// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "storage/segment/variant/hierarchical_data_iterator.h"

#include <algorithm>
#include <memory>
#include <ranges>
#include <span>
#include <utility>

#include "common/status.h"
#include "core/assert_cast.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/typeid_cast.h"
#include "storage/segment/column_reader_cache.h"
#include "storage/segment/variant/nested_group_path.h"

namespace doris::segment_v2 {

Status append_assembled_variant(MutableColumnPtr& dst, VariantAssembledColumn&& assembled) {
    if (!dst || !assembled.values || !assembled.outer_nulls) {
        return Status::InvalidArgument("Variant assembled output is incomplete");
    }
    if (assembled.values->size() != assembled.outer_nulls->size()) {
        return Status::InternalError("Variant assembled values have {} rows, null map has {}",
                                     assembled.values->size(), assembled.outer_nulls->size());
    }
    const auto* assembled_nulls = check_and_get_column<ColumnUInt8>(assembled.outer_nulls.get());
    if (assembled_nulls == nullptr) {
        return Status::InternalError("Variant assembled outer nulls are not UInt8");
    }

    dst = IColumn::mutate(std::move(dst));
    ColumnVariantV2* values = nullptr;
    if (auto* nullable = check_and_get_column<ColumnNullable>(dst.get())) {
        values = check_and_get_column<ColumnVariantV2>(&nullable->get_nested_column());
        if (values == nullptr) {
            return Status::InvalidArgument(
                    "Variant reader requires a nullable ColumnVariantV2 destination");
        }
        values->insert_range_from(*assembled.values, 0, assembled.values->size());
        nullable->get_null_map_column().insert_range_from(*assembled_nulls, 0,
                                                          assembled_nulls->size());
    } else {
        values = check_and_get_column<ColumnVariantV2>(dst.get());
        if (values == nullptr) {
            return Status::InvalidArgument("Variant reader requires a ColumnVariantV2 destination");
        }
        if (std::ranges::any_of(assembled_nulls->get_data(),
                                [](uint8_t value) { return value != 0; })) {
            return Status::Corruption(
                    "Variant storage returned SQL NULL for a non-nullable destination");
        }
        values->insert_range_from(*assembled.values, 0, assembled.values->size());
    }
    return Status::OK();
}

Status HierarchicalDataIterator::_assemble_and_publish_batch(MutableColumnPtr& dst,
                                                             size_t num_rows) {
    DorisVector<const IColumn*> materialized;
    materialized.reserve(_substream_reader.size());
    for (const auto& entry : _substream_reader) {
        materialized.push_back(entry->data.column.get());
    }

    DorisVector<const ColumnMap*> sparse;
    const ColumnMap* doc = nullptr;
    if (_binary_column_reader) {
        const auto* binary = check_and_get_column<ColumnMap>(_binary_column_reader->column.get());
        if (binary == nullptr) {
            return Status::Corruption("Variant binary stream is not Map<String,String>");
        }
        if (_read_type == ReadType::SUBCOLUMNS_AND_SPARSE) {
            sparse.push_back(binary);
        } else {
            doc = binary;
        }
    }

    std::span<const uint8_t> outer_nulls;
    if (_root_reader && is_column_nullable(*_root_reader->column)) {
        const auto& nulls =
                assert_cast<const ColumnNullable&>(*_root_reader->column).get_null_map_data();
        outer_nulls = std::span<const uint8_t>(nulls.data(), nulls.size());
    }

    VariantAssemblerBatchView batch;
    batch.num_rows = num_rows;
    batch.outer_nulls = outer_nulls;
    batch.root_jsonb = _root_reader ? _root_reader->column.get() : nullptr;
    batch.materialized_columns = materialized;
    batch.sparse_buckets = sparse;
    batch.doc_values = doc;
    VariantAssembledColumn assembled;
    RETURN_IF_ERROR(_assembler->assemble(batch, &assembled));
    return append_assembled_variant(dst, std::move(assembled));
}

void HierarchicalDataIterator::_clear_read_columns() {
    for (const auto& entry : _substream_reader) {
        entry->data.column->clear();
    }
    if (_binary_column_reader) {
        _binary_column_reader->column->clear();
    }
    if (_root_reader) {
        _root_reader->column->clear();
    }
}

Status HierarchicalDataIterator::create(ColumnIteratorUPtr* reader, int32_t col_uid,
                                        PathInData path, const SubcolumnColumnMetaInfo::Node* node,
                                        std::unique_ptr<SubstreamIterator>&& binary_column_reader,
                                        std::unique_ptr<SubstreamIterator>&& root_column_reader,
                                        ColumnReaderCache* column_reader_cache,
                                        OlapReaderStatistics* stats, ReadType read_type,
                                        bool null_on_no_match) {
    std::unique_ptr<HierarchicalDataIterator> stream_iter(
            new HierarchicalDataIterator(path, read_type));
    if (node != nullptr && read_type == ReadType::SUBCOLUMNS_AND_SPARSE) {
        std::vector<const SubcolumnColumnMetaInfo::Node*> leaves;
        PathsInData leaves_paths;
        SubcolumnColumnMetaInfo::get_leaves_of_node(node, leaves, leaves_paths);
        for (size_t i = 0; i < leaves_paths.size(); ++i) {
            if (leaves_paths[i].empty()) {
                continue;
            }
            const auto& leaf_path = leaves_paths[i].get_path();
            if (contains_nested_group_marker(leaf_path)) {
                VLOG_DEBUG << "Skipping NestedGroup subcolumn: " << leaf_path;
                continue;
            }
            RETURN_IF_ERROR(
                    stream_iter->add_stream(col_uid, leaves[i], column_reader_cache, stats));
        }
    }
    stream_iter->_root_reader = std::move(root_column_reader);
    stream_iter->_binary_column_reader = std::move(binary_column_reader);
    stream_iter->_stats = stats;

    VariantAssemblerPlanOptions plan_options;
    plan_options.mode = VariantAssemblerMode::HIERARCHICAL;
    plan_options.requested_path = path;
    plan_options.sparse_bucket_count =
            stream_iter->_binary_column_reader && read_type == ReadType::SUBCOLUMNS_AND_SPARSE ? 1
                                                                                               : 0;
    plan_options.has_root = stream_iter->_root_reader != nullptr;
    plan_options.has_doc =
            stream_iter->_binary_column_reader && read_type == ReadType::DOC_VALUE_COLUMN;
    plan_options.merge_root_with_subcolumns = false;
    plan_options.null_on_no_match = null_on_no_match;
    RETURN_IF_ERROR(stream_iter->tranverse([&](SubstreamReaderTree::Node& stream) {
        plan_options.materialized_paths.push_back({.path = stream.path, .type = stream.data.type});
        return Status::OK();
    }));
    std::shared_ptr<const VariantAssemblerPlan> plan;
    RETURN_IF_ERROR(VariantAssemblerPlan::create(std::move(plan_options), &plan));
    stream_iter->_assembler = std::make_unique<VariantAssembler>(std::move(plan));
    *reader = std::move(stream_iter);
    return Status::OK();
}

Status HierarchicalDataIterator::init(const ColumnIteratorOptions& opts) {
    RETURN_IF_ERROR(tranverse([&](SubstreamReaderTree::Node& node) {
        RETURN_IF_ERROR(node.data.iterator->init(opts));
        node.data.inited = true;
        return Status::OK();
    }));
    if (_root_reader && !_root_reader->inited) {
        RETURN_IF_ERROR(_root_reader->iterator->init(opts));
        _root_reader->inited = true;
    }
    if (_binary_column_reader && !_binary_column_reader->inited) {
        RETURN_IF_ERROR(_binary_column_reader->iterator->init(opts));
        _binary_column_reader->inited = true;
    }
    return Status::OK();
}

Status HierarchicalDataIterator::seek_to_ordinal(ordinal_t ord) {
    RETURN_IF_ERROR(tranverse([&](SubstreamReaderTree::Node& node) {
        RETURN_IF_ERROR(node.data.iterator->seek_to_ordinal(ord));
        return Status::OK();
    }));
    if (_root_reader) {
        DCHECK(_root_reader->inited);
        RETURN_IF_ERROR(_root_reader->iterator->seek_to_ordinal(ord));
    }
    if (_binary_column_reader) {
        DCHECK(_binary_column_reader->inited);
        RETURN_IF_ERROR(_binary_column_reader->iterator->seek_to_ordinal(ord));
    }
    return Status::OK();
}

Status HierarchicalDataIterator::next_batch(size_t* n, MutableColumnPtr& dst, bool* has_null) {
    const size_t requested_rows = *n;
    return process_read(
            [&](SubstreamIterator& reader, const PathInData& path,
                const DataTypePtr& type) -> Status {
                CHECK(reader.inited);
                size_t stream_rows = requested_rows;
                RETURN_IF_ERROR(reader.iterator->next_batch(&stream_rows, reader.column, has_null));
                if (stream_rows != reader.column->size()) {
                    return Status::Corruption("Variant stream {} reported {} rows but produced {}",
                                              path.get_path(), stream_rows, reader.column->size());
                }
                VLOG_DEBUG << fmt::format("{} next_batch {} rows, type={}", path.get_path(),
                                          stream_rows, type ? type->get_name() : "null");
                return Status::OK();
            },
            dst, requested_rows, true, n);
}

Status HierarchicalDataIterator::read_by_rowids(const rowid_t* rowids, const size_t count,
                                                MutableColumnPtr& dst) {
    size_t actual_rows = count;
    return process_read(
            [&](SubstreamIterator& reader, const PathInData& path,
                const DataTypePtr& type) -> Status {
                CHECK(reader.inited);
                RETURN_IF_ERROR(reader.iterator->read_by_rowids(rowids, count, reader.column));
                VLOG_DEBUG << fmt::format("{} read_by_rowids {} rows, type={}", path.get_path(),
                                          count, type ? type->get_name() : "null");
                return Status::OK();
            },
            dst, count, false, &actual_rows);
}

Status HierarchicalDataIterator::add_stream(int32_t col_uid,
                                            const SubcolumnColumnMetaInfo::Node* node,
                                            ColumnReaderCache* column_reader_cache,
                                            OlapReaderStatistics* stats) {
    if (_substream_reader.find_leaf(node->path)) {
        VLOG_DEBUG << "Already exist sub column " << node->path.get_path();
        return Status::OK();
    }
    CHECK(node);
    ColumnIteratorUPtr it;
    std::shared_ptr<ColumnReader> column_reader;
    RETURN_IF_ERROR(column_reader_cache->get_path_column_reader(col_uid, node->path, &column_reader,
                                                                stats, node));
    RETURN_IF_ERROR(column_reader->new_iterator(&it, nullptr));
    SubstreamIterator stream(node->data.file_column_type->create_column(), std::move(it),
                             node->data.file_column_type);
    if (!_substream_reader.add(node->path, std::move(stream))) {
        return Status::InternalError("Failed to add node path {}", node->path.get_path());
    }
    VLOG_DEBUG << fmt::format("Add substream {} for {}", node->path.get_path(), _path.get_path());
    return Status::OK();
}

ordinal_t HierarchicalDataIterator::get_current_ordinal() const {
    if (_substream_reader.begin() != _substream_reader.end()) {
        return (*_substream_reader.begin())->data.iterator->get_current_ordinal();
    }
    if (_root_reader) {
        return _root_reader->iterator->get_current_ordinal();
    }
    DCHECK(_binary_column_reader != nullptr);
    return _binary_column_reader->iterator->get_current_ordinal();
}

Status HierarchicalDataIterator::init_prefetcher(const SegmentPrefetchParams& params) {
    RETURN_IF_ERROR(tranverse([&](SubstreamReaderTree::Node& node) {
        RETURN_IF_ERROR(node.data.iterator->init_prefetcher(params));
        return Status::OK();
    }));
    if (_root_reader) {
        DCHECK(_root_reader->inited);
        RETURN_IF_ERROR(_root_reader->iterator->init_prefetcher(params));
    }
    if (_binary_column_reader) {
        DCHECK(_binary_column_reader->inited);
        RETURN_IF_ERROR(_binary_column_reader->iterator->init_prefetcher(params));
    }
    return Status::OK();
}

void HierarchicalDataIterator::collect_prefetchers(
        std::map<PrefetcherInitMethod, std::vector<SegmentPrefetcher*>>& prefetchers,
        PrefetcherInitMethod init_method) {
    static_cast<void>(tranverse([&](SubstreamReaderTree::Node& node) {
        node.data.iterator->collect_prefetchers(prefetchers, init_method);
        return Status::OK();
    }));
    if (_root_reader) {
        DCHECK(_root_reader->inited);
        _root_reader->iterator->collect_prefetchers(prefetchers, init_method);
    }
    if (_binary_column_reader) {
        DCHECK(_binary_column_reader->inited);
        _binary_column_reader->iterator->collect_prefetchers(prefetchers, init_method);
    }
}

} // namespace doris::segment_v2
