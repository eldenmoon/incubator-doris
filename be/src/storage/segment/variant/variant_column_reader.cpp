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

#include "storage/segment/variant/variant_column_reader.h"

#include <fmt/format.h>
#include <gen_cpp/segment_v2.pb.h>

#include <algorithm>
#include <memory>
#include <ranges>
#include <roaring/roaring.hh>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "core/assert_cast.h"
#include "core/column/column_array.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_jsonb.h"
#include "core/data_type/data_type_variant.h"
#include "exec/common/variant_util.h"
#include "io/fs/file_reader.h"
#include "runtime/descriptors.h"
#include "storage/key_coder.h"
#include "storage/olap_common.h"
#include "storage/segment/column_meta_accessor.h"
#include "storage/segment/column_reader.h"
#include "storage/segment/column_reader_cache.h"
#include "storage/segment/segment.h"
#include "storage/segment/variant/binary_column_extract_iterator.h"
#include "storage/segment/variant/binary_column_reader.h"
#include "storage/segment/variant/hierarchical_data_iterator.h"
#include "storage/segment/variant/nested_group_path.h"
#include "storage/segment/variant/sparse_column_merge_iterator.h"
#include "storage/segment/variant/variant_doc_snpashot_compact_iterator.h"
#include "storage/tablet/tablet_schema.h"
#include "util/debug_points.h"
#include "util/json/path_in_data.h"
#include "util/string_util.h"

namespace doris::segment_v2 {

namespace {

void add_variant_search_binding_diagnostic(OlapReaderStatistics* stats,
                                           const std::string& diagnostic) {
    VLOG_DEBUG << diagnostic;
    if (stats != nullptr) {
        stats->inverted_index_stats.add_binding_diagnostic(diagnostic);
    }
}

} // namespace

const SubcolumnColumnMetaInfo::Node* VariantColumnReader::get_subcolumn_meta_by_path(
        const PathInData& relative_path) const {
    std::shared_lock<std::shared_mutex> lock(_subcolumns_meta_mutex);
    const auto* node = _subcolumns_meta_info->find_leaf(relative_path);
    if (node) {
        return node;
    }
    // try rebuild path with hierarchical
    // example path(['a.b']) -> path(['a', 'b'])
    auto path = PathInData(relative_path.get_path());
    node = _subcolumns_meta_info->find_leaf(path);
    return node;
}

bool VariantColumnReader::exist_in_sparse_column(const PathInData& relative_path) const {
    std::shared_lock<std::shared_mutex> lock(_subcolumns_meta_mutex);
    // Check if path exist in sparse column
    bool existed_in_sparse_column =
            !_statistics->sparse_column_non_null_size.empty() &&
            _statistics->sparse_column_non_null_size.contains(relative_path.get_path());
    const std::string& prefix = relative_path.get_path() + ".";
    bool prefix_existed_in_sparse_column =
            !_statistics->sparse_column_non_null_size.empty() &&
            (_statistics->sparse_column_non_null_size.lower_bound(prefix) !=
             _statistics->sparse_column_non_null_size.end()) &&
            _statistics->sparse_column_non_null_size.lower_bound(prefix)->first.starts_with(prefix);
    return existed_in_sparse_column || prefix_existed_in_sparse_column;
}

bool VariantColumnReader::is_exceeded_sparse_column_limit() const {
    std::shared_lock<std::shared_mutex> lock(_subcolumns_meta_mutex);
    return _is_exceeded_sparse_column_limit_unlocked();
}

bool VariantColumnReader::_is_exceeded_sparse_column_limit_unlocked() const {
    const bool exceeded_sparse_column_limit = !_statistics->sparse_column_non_null_size.empty() &&
                                              _statistics->sparse_column_non_null_size.size() >=
                                                      _variant_sparse_column_statistics_size;
    DBUG_EXECUTE_IF("exceeded_sparse_column_limit_must_be_false", {
        if (exceeded_sparse_column_limit) {
            throw doris::Exception(
                    ErrorCode::INTERNAL_ERROR,
                    "exceeded_sparse_column_limit_must_be_false, sparse_column_non_null_size: {} : "
                    " _variant_sparse_column_statistics_size: {}",
                    _statistics->sparse_column_non_null_size.size(),
                    _variant_sparse_column_statistics_size);
        }
    })
    return exceeded_sparse_column_limit;
}

int64_t VariantColumnReader::get_metadata_size() const {
    std::shared_lock<std::shared_mutex> lock(_subcolumns_meta_mutex);
    int64_t size = ColumnReader::get_metadata_size();
    if (_statistics) {
        for (const auto& [path, _] : _statistics->subcolumns_non_null_size) {
            size += path.size() + sizeof(size_t);
        }
        for (const auto& [path, _] : _statistics->sparse_column_non_null_size) {
            size += path.size() + sizeof(size_t);
        }
    }

    for (const auto& reader : *_subcolumns_meta_info) {
        size += reader->path.get_path().size();
        size += sizeof(SubcolumnMeta);
    }
    return size;
}

bool VariantColumnReader::has_prefix_path(const PathInData& relative_path) const {
    std::shared_lock<std::shared_mutex> lock(_subcolumns_meta_mutex);
    return _has_prefix_path_unlocked(relative_path);
}

bool VariantColumnReader::_has_prefix_path_unlocked(const PathInData& relative_path) const {
    if (relative_path.empty()) {
        return true;
    }
    const std::string path = relative_path.get_path();
    const std::string dot_prefix = relative_path.get_path() + ".";

    // 1) exact node exists and has children.
    if (const auto* node = _subcolumns_meta_info->find_exact(relative_path)) {
        if (!node->children.empty()) {
            return true;
        }
    }

    // 2) Check sparse column stats: use lower_bound to test the `p.` prefix range
    // example sparse columns path: a.b.c, a.b.e, access prefix: a.b.
    // then we must read the sparse columns
    if (_statistics->has_prefix_path_in_sparse_column(dot_prefix)) {
        return true;
    }

    // 3) Check external meta store (if available).
    if (_ext_meta_reader && _ext_meta_reader->available()) {
        bool has = false;
        // Pass strict prefix `p.` to avoid false positives like `a.b` matching `a.bc`.
        if (_ext_meta_reader->has_prefix(dot_prefix, &has).ok() && has) {
            return true;
        }
    }

    return false;
}

bool VariantColumnReader::_can_use_nested_group_read_path() const {
    return _nested_group_read_provider != nullptr &&
           _nested_group_read_provider->should_enable_nested_group_read_path();
}

Status VariantColumnReader::init(const ColumnReaderOptions& opts, ColumnMetaAccessor* accessor,
                                 const std::shared_ptr<SegmentFooterPB>& footer, int32_t column_uid,
                                 uint64_t num_rows, io::FileReaderSPtr file_reader) {
    // init sub columns
    _subcolumns_meta_info = std::make_unique<SubcolumnColumnMetaInfo>();
    _statistics = std::make_unique<VariantStatistics>();

    // Prefer external root ColumnMetaPB via ColumnMetaAccessor when available.
    ColumnMetaPB self_column_pb;
    if (opts.tablet_schema == nullptr) {
        return Status::InvalidArgument("Variant reader requires a tablet schema");
    }
    RETURN_IF_ERROR(accessor->get_column_meta_by_uid(*footer, column_uid, &self_column_pb));
    const auto& parent_column = opts.tablet_schema->column_by_uid(self_column_pb.unique_id());
    if (parent_column.variant_enable_nested_group()) {
        return Status::NotSupported(
                "Variant V2 storage reader does not support nested-group layout");
    }
    _nested_group_read_provider = create_nested_group_read_provider();
    {
        // root column
        DataTypePtr root_type = self_column_pb.is_nullable()
                                        ? make_nullable(std::make_unique<DataTypeJsonb>())
                                        : std::make_unique<DataTypeJsonb>();
        int32_t root_footer_ordinal = -1;
        if (self_column_pb.has_column_id()) {
            // Narrow explicitly to avoid implicit narrowing warnings.
            root_footer_ordinal = static_cast<int32_t>(self_column_pb.column_id());
        }
        _subcolumns_meta_info->create_root(SubcolumnMeta {
                .file_column_type = root_type,
                // Use column_id from meta as footer ordinal when inline footer is available.
                .footer_ordinal = root_footer_ordinal});
        RETURN_IF_ERROR(ColumnReader::create(opts, self_column_pb, num_rows, file_reader,
                                             &_root_column_reader));
    }

    _data_type = DataTypeFactory::instance().create_data_type(self_column_pb);
    _root_unique_id = self_column_pb.unique_id();
    const bool should_record_path_stats = variant_util::should_record_variant_path_stats(
            opts.tablet_schema->column_by_uid(self_column_pb.unique_id()));
    const auto& parent_index = opts.tablet_schema->inverted_indexs(self_column_pb.unique_id());
    // record variant_sparse_column_statistics_size from parent column
    _variant_sparse_column_statistics_size =
            opts.tablet_schema->column_by_uid(self_column_pb.unique_id())
                    .variant_max_sparse_column_statistics_size();
    _tablet_schema = opts.tablet_schema;

    // Only extract scalar flags from root stats; sparse/doc maps come from
    // their respective column metas via additive merge methods.
    if (self_column_pb.has_variant_statistics()) {
        _statistics->has_nested_group = self_column_pb.variant_statistics().has_nested_group();
    }

    // collect bucketized binary column readers for this variant column
    std::map<uint32_t, std::shared_ptr<ColumnReader>> tmp_sparse_readers;
    std::map<uint32_t, std::shared_ptr<ColumnReader>> tmp_doc_value_readers;

    // helper to handle sparse meta (single or bucket) from a ColumnMetaPB
    auto handle_sparse_meta = [&](const ColumnMetaPB& col, bool* handled) -> Status {
        *handled = false;
        if (!col.has_column_path_info()) {
            return Status::OK();
        }
        PathInData path;
        path.from_protobuf(col.column_path_info());
        auto relative = path.copy_pop_front();
        if (relative.empty()) {
            return Status::OK();
        }

        // case 1: single sparse column
        std::string rel_str = relative.get_path();
        if (rel_str == SPARSE_COLUMN_PATH) {
            DCHECK(col.has_variant_statistics()) << col.DebugString();
            if (should_record_path_stats) {
                // Always load sparse stats from the sparse column's own meta.
                // This is the authoritative source; root stats may duplicate these
                // but the sparse column meta is canonical.
                _statistics->merge_sparse_from_pb(col.variant_statistics());
            }
            std::shared_ptr<ColumnReader> single_reader;
            RETURN_IF_ERROR(ColumnReader::create(opts, col, footer->num_rows(), file_reader,
                                                 &single_reader));
            // only one sparse column
            if (_binary_column_reader) {
                return Status::AlreadyExist("single sparse column reader already exists");
            }
            _binary_column_reader = std::make_shared<SingleSparseColumnReader>();
            RETURN_IF_ERROR(
                    _binary_column_reader->add_binary_column_reader(std::move(single_reader), 0));
            *handled = true;
            return Status::OK();
        }

        // case 2: bucketized sparse column
        std::string bucket_prefix = std::string(SPARSE_COLUMN_PATH) + ".b";
        if (rel_str.starts_with(bucket_prefix)) {
            uint32_t idx =
                    static_cast<uint32_t>(atoi(rel_str.substr(bucket_prefix.size()).c_str()));
            DCHECK(col.has_variant_statistics()) << col.DebugString();
            if (should_record_path_stats) {
                // Additively merge per-bucket sparse stats into the unified statistics.
                _statistics->merge_sparse_from_pb(col.variant_statistics());
            }
            std::shared_ptr<ColumnReader> reader;
            RETURN_IF_ERROR(ColumnReader::create(opts, col, num_rows, file_reader, &reader));
            tmp_sparse_readers[idx] = std::move(reader);
            *handled = true;
            return Status::OK();
        }

        // case 3: doc snapshot column
        if (rel_str.find(DOC_VALUE_COLUMN_PATH) != std::string::npos) {
            size_t bucket = rel_str.rfind('b');
            uint32_t bucket_value = static_cast<uint32_t>(std::stoi(rel_str.substr(bucket + 1)));
            std::shared_ptr<ColumnReader> column_reader;
            RETURN_IF_ERROR(ColumnReader::create(opts, col, num_rows, file_reader, &column_reader));
            tmp_doc_value_readers[bucket_value] = std::move(column_reader);
            if (should_record_path_stats) {
                // Additively merge per-bucket doc value stats into the unified statistics.
                _statistics->merge_doc_value_from_pb(col.variant_statistics());
            }
            *handled = true;
            return Status::OK();
        }
        return Status::OK();
    };

    // First try initialize sparse from root's embedded children_columns (new segments)
    if (self_column_pb.children_columns_size() > 0) {
        for (int i = 0; i < self_column_pb.children_columns_size(); ++i) {
            const ColumnMetaPB& child_pb = self_column_pb.children_columns(i);
            bool handled = false;
            RETURN_IF_ERROR(handle_sparse_meta(child_pb, &handled));
        }
    }

    // init from inline columns meta
    for (int32_t ordinal = 0; ordinal < footer->columns_size(); ++ordinal) {
        const ColumnMetaPB& column_pb = footer->columns(ordinal);
        // Find all columns belonging to the current variant column
        // 1. not the variant column
        if (!column_pb.has_column_path_info()) {
            continue;
        }

        // 2. other variant root columns
        if (column_pb.type() == (int)FieldType::OLAP_FIELD_TYPE_VARIANT &&
            column_pb.unique_id() != self_column_pb.unique_id()) {
            continue;
        }

        // 3. other variant's subcolumns
        if (column_pb.type() != (int)FieldType::OLAP_FIELD_TYPE_VARIANT &&
            column_pb.column_path_info().parrent_column_unique_id() != self_column_pb.unique_id()) {
            continue;
        }
        DCHECK(column_pb.has_column_path_info());
        PathInData path;
        path.from_protobuf(column_pb.column_path_info());

        // init sparse column readers
        auto relative_sparse = path.copy_pop_front();
        auto rel_str = relative_sparse.get_path();
        {
            bool handled = false;
            RETURN_IF_ERROR(handle_sparse_meta(column_pb, &handled));
            if (handled) {
                continue;
            }
        }

        // init subcolumns
        auto relative_path = path.copy_pop_front();
        if (relative_path.empty()) {
            continue;
        }
        // Skip NestedGroup subcolumns (columns with ___DOR_ng___. prefix in path).
        // NestedGroup columns only contain rows that have the nested array, not all rows.
        // They need special handling via NestedGroupWholeIterator, not regular subcolumns.
        const auto& leaf_path = relative_path.get_path();
        if (contains_nested_group_marker(leaf_path)) {
            VLOG_DEBUG << "Skipping NestedGroup subcolumn: " << leaf_path;
            continue;
        }
        // check the root is already a leaf node
        if (should_record_path_stats && column_pb.has_none_null_size()) {
            _statistics->subcolumns_non_null_size.emplace(relative_path.get_path(),
                                                          column_pb.none_null_size());
        }
        // 3.1.2 may store a flat JSON key like {"a.b": 1} as a single PathInData part.
        // New compaction schema and query path expect a dot-split multi-part shape.
        // Rebuild via the string constructor when the path has neither typed
        // nor nested metadata, so the tree matches the new shape.
        if (!relative_path.get_is_typed() && !relative_path.has_nested_part()) {
            relative_path = PathInData(relative_path.get_path());
        }
        _subcolumns_meta_info->add(
                relative_path,
                SubcolumnMeta {
                        .file_column_type = DataTypeFactory::instance().create_data_type(column_pb),
                        .footer_ordinal = ordinal});
    }

    // finalize bucket readers if any
    // Stats have already been merged additively as each bucket column was processed.
    if (!tmp_sparse_readers.empty()) {
        _binary_column_reader = std::make_shared<MultipleSparseColumnReader>();
        for (auto& [index, reader] : tmp_sparse_readers) {
            RETURN_IF_ERROR(
                    _binary_column_reader->add_binary_column_reader(std::move(reader), index));
        }
    } else if (!tmp_doc_value_readers.empty()) {
        _binary_column_reader = std::make_shared<MultipleDocColumnReader>();
        for (auto& [index, reader] : tmp_doc_value_readers) {
            RETURN_IF_ERROR(
                    _binary_column_reader->add_binary_column_reader(std::move(reader), index));
        }
    }

    // old version variant column without any binary data.
    // if no binary column reader, use dummy binary column reader
    if (_binary_column_reader == nullptr) {
        _binary_column_reader = std::make_shared<DummyBinaryColumnReader>();
    }
    _segment_file_reader = file_reader;
    _num_rows = num_rows;
    // try build external meta readers (optional)
    _ext_meta_reader = std::make_unique<VariantExternalMetaReader>();
    RETURN_IF_ERROR(_ext_meta_reader->init_from_footer(footer, file_reader, _root_unique_id));

    // NestedGroup initialization is provider-driven. Disabled providers keep fallback behavior,
    // while enabled providers populate nested group readers from segment footer.
    if (_can_use_nested_group_read_path()) {
        RETURN_IF_ERROR(_nested_group_read_provider->init_readers(opts, footer, file_reader,
                                                                  accessor, _root_unique_id,
                                                                  num_rows, _nested_group_readers));
    }

    return Status::OK();
}

Status VariantColumnReader::create_reader_from_external_meta(const std::string& path,
                                                             const ColumnReaderOptions& opts,
                                                             const io::FileReaderSPtr& file_reader,
                                                             uint64_t num_rows,
                                                             std::shared_ptr<ColumnReader>* out) {
    if (!_ext_meta_reader || !_ext_meta_reader->available()) {
        return Status::Error<ErrorCode::NOT_FOUND, false>("no external variant meta");
    }
    ColumnMetaPB meta;
    RETURN_IF_ERROR(_ext_meta_reader->lookup_meta_by_path(path, &meta));
    return ColumnReader::create(opts, meta, num_rows, file_reader, out);
}

Status VariantColumnReader::create_path_reader(const PathInData& relative_path,
                                               const ColumnReaderOptions& opts,
                                               ColumnMetaAccessor* accessor,
                                               const SegmentFooterPB& footer,
                                               const io::FileReaderSPtr& file_reader,
                                               uint64_t num_rows,
                                               std::shared_ptr<ColumnReader>* out) {
    // 1) Try inline subcolumn meta if available (footer_ordinal >= 0)
    const auto* node = get_subcolumn_meta_by_path(relative_path);
    if (node != nullptr && node->data.footer_ordinal >= 0) {
        // leaf node, get the column meta by footer ordinal
        const int32_t column_ordinal = node->data.footer_ordinal;
        ColumnMetaPB meta;
        RETURN_IF_ERROR(
                accessor->get_column_meta_by_column_ordinal_id(footer, column_ordinal, &meta));
        return ColumnReader::create(opts, meta, num_rows, file_reader, out);
    }

    // 2) Try external meta layout (if available)
    Status st = create_reader_from_external_meta(relative_path.get_path(), opts, file_reader,
                                                 num_rows, out);
    if (st.is<ErrorCode::NOT_FOUND>()) {
        // 3) Try nested group readers (array-of-objects / nested search paths).
        // relative_path is already popped of the variant root, so it can directly match
        // nested group child names (e.g. "msg", "title").
        auto [group_reader, child_path] = find_nested_group_for_path(relative_path.get_path());
        if (group_reader != nullptr && !child_path.empty()) {
            auto it = group_reader->child_readers.find(child_path);
            if (it != group_reader->child_readers.end() && it->second != nullptr) {
                *out = it->second;
                return Status::OK();
            }
        }
        *out = nullptr;
        return st;
    }
    return st;
}

Status VariantColumnReader::load_external_meta_once() {
    if (!_ext_meta_reader || !_ext_meta_reader->available()) {
        return Status::OK();
    }
    // Ensure only one writer can populate `_subcolumns_meta_info` / `_statistics`
    // while readers of these structures hold shared locks.
    std::unique_lock<std::shared_mutex> lock(_subcolumns_meta_mutex);
    VariantStatistics* stats = variant_util::should_record_variant_path_stats(
                                       _tablet_schema->column_by_uid(_root_unique_id))
                                       ? _statistics.get()
                                       : nullptr;
    return _ext_meta_reader->load_all_once(_subcolumns_meta_info.get(), stats);
}

TabletIndexes VariantColumnReader::find_subcolumn_tablet_indexes(const TabletColumn& column,
                                                                 const DataTypePtr& data_type,
                                                                 OlapReaderStatistics* stats) {
    TabletSchema::SubColumnInfo sub_column_info;
    const auto& parent_index = _tablet_schema->inverted_indexs(column.parent_unique_id());
    auto relative_path = column.path_info_ptr()->copy_pop_front();
    DataTypePtr index_data_type = data_type;
    const std::string logical_path = column.path_info_ptr()->get_path();
    const std::string relative_path_str = relative_path.get_path();

    if (!relative_path.empty()) {
        auto [found, group_chain, child_path] =
                collect_nested_group_chain(relative_path.get_path());
        (void)child_path;
        if (found && !group_chain.empty()) {
            // NestedGroup leaf readers store the flattened element type.
            if (data_type->is_nullable()) {
                auto base = variant_util::get_base_type_of_array(remove_nullable(data_type));
                index_data_type = base->is_nullable() ? base : make_nullable(base);
            } else {
                index_data_type = variant_util::get_base_type_of_array(data_type);
            }
        }
    }

    // if subcolumn has index, add index to _variant_subcolumns_indexes
    if (variant_util::generate_sub_column_info(*_tablet_schema, column.parent_unique_id(),
                                               relative_path.get_path(), &sub_column_info) &&
        !sub_column_info.indexes.empty()) {
        for (const auto& index : sub_column_info.indexes) {
            add_variant_search_binding_diagnostic(
                    stats,
                    fmt::format("[VariantSearchBinding] phase=subcolumn_index_candidates "
                                "source=direct logical_path={} relative_path={} "
                                "materialized_column={} index_id={} suffix={} field_pattern={} "
                                "reason=generated_subcolumn_info",
                                logical_path, relative_path_str, column.name(), index->index_id(),
                                index->get_index_suffix(), index->field_pattern()));
        }
        return sub_column_info.indexes;
    }

    // Otherwise, inherit index from the VARIANT parent column.
    if (!parent_index.empty() &&
        index_data_type->get_primitive_type() != PrimitiveType::TYPE_VARIANT &&
        index_data_type->get_primitive_type() != PrimitiveType::TYPE_MAP /*SPARSE COLUMN*/) {
        // type in column maynot be real type, so use data_type to get the real type
        PathInData index_path {*column.path_info_ptr()};
        TabletColumn target_column =
                variant_util::get_column_by_type(index_data_type, column.name(),
                                                 {.unique_id = -1,
                                                  .parent_unique_id = column.parent_unique_id(),
                                                  .path_info = index_path});
        variant_util::inherit_index(parent_index, sub_column_info.indexes, target_column);
        for (const auto& index : sub_column_info.indexes) {
            add_variant_search_binding_diagnostic(
                    stats,
                    fmt::format("[VariantSearchBinding] phase=subcolumn_index_candidates "
                                "source=parent_inherited logical_path={} relative_path={} "
                                "materialized_column={} index_id={} suffix={} field_pattern={} "
                                "reason=no_direct_subcolumn_index",
                                logical_path, relative_path_str, column.name(), index->index_id(),
                                index->get_index_suffix(), index->field_pattern()));
        }
    } else if (parent_index.empty()) {
        add_variant_search_binding_diagnostic(
                stats,
                fmt::format("[VariantSearchBinding] phase=subcolumn_index_candidates "
                            "source=none logical_path={} relative_path={} materialized_column={} "
                            "reason=parent_index_missing",
                            logical_path, relative_path_str, column.name()));
    } else {
        add_variant_search_binding_diagnostic(
                stats,
                fmt::format("[VariantSearchBinding] phase=subcolumn_index_candidates "
                            "source=none logical_path={} relative_path={} materialized_column={} "
                            "data_type={} reason=unsupported_inherited_index_type",
                            logical_path, relative_path_str, column.name(),
                            index_data_type ? index_data_type->get_name() : "null"));
    }
    // Return shared_ptr directly to maintain object lifetime
    return sub_column_info.indexes;
}

void VariantColumnReader::get_subcolumns_types(
        std::unordered_map<PathInData, DataTypes, PathInData::Hash>* subcolumns_types) const {
    std::shared_lock<std::shared_mutex> lock(_subcolumns_meta_mutex);
    for (const auto& subcolumn_reader : *_subcolumns_meta_info) {
        auto& path_types = (*subcolumns_types)[subcolumn_reader->path];
        path_types.push_back(subcolumn_reader->data.file_column_type);
    }
}

void VariantColumnReader::get_typed_paths(std::unordered_set<std::string>* typed_paths) const {
    std::shared_lock<std::shared_mutex> lock(_subcolumns_meta_mutex);
    for (const auto& entry : *_subcolumns_meta_info) {
        if (entry->path.get_is_typed()) {
            typed_paths->insert(entry->path.get_path());
        }
    }
}

void VariantColumnReader::get_nested_paths(
        std::unordered_set<PathInData, PathInData::Hash>* nested_paths) const {
    std::shared_lock<std::shared_mutex> lock(_subcolumns_meta_mutex);
    for (const auto& entry : *_subcolumns_meta_info) {
        if (entry->path.has_nested_part()) {
            nested_paths->insert(entry->path);
        }
    }
}

Status VariantRootColumnIterator::init(const ColumnIteratorOptions& opts) {
    VariantAssemblerPlanOptions plan_options;
    plan_options.mode = VariantAssemblerMode::ROOT_FLAT;
    plan_options.has_root = true;
    std::shared_ptr<const VariantAssemblerPlan> plan;
    RETURN_IF_ERROR(VariantAssemblerPlan::create(std::move(plan_options), &plan));
    _assembler = std::make_unique<VariantAssembler>(std::move(plan));
    return _inner_iter->init(opts);
}

Status VariantRootColumnIterator::_process_root_column(MutableColumnPtr& dst,
                                                       MutableColumnPtr& root_column) {
    VariantAssemblerBatchView batch;
    batch.num_rows = root_column->size();
    batch.root_jsonb = root_column.get();
    VariantAssembledColumn assembled;
    RETURN_IF_ERROR(_assembler->assemble(batch, &assembled));
    return append_assembled_variant(dst, std::move(assembled));
}

Status VariantRootColumnIterator::next_batch(size_t* n, MutableColumnPtr& dst, bool* has_null) {
    DataTypePtr root_type = std::make_shared<DataTypeJsonb>();
    if (_inner_iter->is_nullable()) {
        root_type = make_nullable(std::move(root_type));
    }
    auto root_column = root_type->create_column();
    RETURN_IF_ERROR(_inner_iter->next_batch(n, root_column, has_null));
    return _process_root_column(dst, root_column);
}

Status VariantRootColumnIterator::read_by_rowids(const rowid_t* rowids, const size_t count,
                                                 MutableColumnPtr& dst) {
    DataTypePtr root_type = std::make_shared<DataTypeJsonb>();
    if (_inner_iter->is_nullable()) {
        root_type = make_nullable(std::move(root_type));
    }
    auto root_column = root_type->create_column();
    RETURN_IF_ERROR(_inner_iter->read_by_rowids(rowids, count, root_column));
    return _process_root_column(dst, root_column);
}

Status VariantRootColumnIterator::init_prefetcher(const SegmentPrefetchParams& params) {
    return _inner_iter->init_prefetcher(params);
}

void VariantRootColumnIterator::collect_prefetchers(
        std::map<PrefetcherInitMethod, std::vector<SegmentPrefetcher*>>& prefetchers,
        PrefetcherInitMethod init_method) {
    _inner_iter->collect_prefetchers(prefetchers, init_method);
}

static void fill_nested_with_defaults(MutableColumnPtr& dst, MutableColumnPtr& sibling_column,
                                      size_t nrows) {
    const auto* sibling_array =
            check_and_get_column<ColumnArray>(remove_nullable(sibling_column->get_ptr()).get());
    const auto* dst_array =
            check_and_get_column<ColumnArray>(remove_nullable(dst->get_ptr()).get());
    if (!dst_array || !sibling_array) {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "Expected array column, but met {} and {}", dst->get_name(),
                               sibling_column->get_name());
    }
    auto new_nested =
            dst_array->get_data_ptr()->clone_resized(sibling_array->get_data_ptr()->size());
    ColumnPtr nested_column = std::move(new_nested);
    auto new_array =
            make_nullable(ColumnArray::create(nested_column, sibling_array->get_offsets_ptr()));
    dst->insert_range_from(*new_array, 0, new_array->size());
#ifndef NDEBUG
    if (!dst_array->has_equal_offsets(*sibling_array)) {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR, "Expected same array offsets");
    }
#endif
}

Status DefaultNestedColumnIterator::next_batch(size_t* n, MutableColumnPtr& dst) {
    bool has_null = false;
    return next_batch(n, dst, &has_null);
}

Status DefaultNestedColumnIterator::next_batch(size_t* n, MutableColumnPtr& dst, bool* has_null) {
    if (_sibling_iter) {
        MutableColumnPtr sibling_column = _file_column_type->create_column();
        RETURN_IF_ERROR(_sibling_iter->next_batch(n, sibling_column, has_null));
        fill_nested_with_defaults(dst, sibling_column, *n);
    } else {
        dst->insert_many_defaults(*n);
    }
    return Status::OK();
}
Status DefaultNestedColumnIterator::read_by_rowids(const rowid_t* rowids, const size_t count,
                                                   MutableColumnPtr& dst) {
    if (_sibling_iter) {
        MutableColumnPtr sibling_column = _file_column_type->create_column();
        RETURN_IF_ERROR(_sibling_iter->read_by_rowids(rowids, count, sibling_column));
        fill_nested_with_defaults(dst, sibling_column, count);
    } else {
        dst->insert_many_defaults(count);
    }
    return Status::OK();
}

const NestedGroupReader* VariantColumnReader::get_nested_group_reader(
        const std::string& array_path) const {
    auto res = find_in_nested_groups(_nested_group_readers, array_path, false);
    return (res.found && res.child_path.empty()) ? res.reader : nullptr;
}

std::pair<const NestedGroupReader*, std::string> VariantColumnReader::find_nested_group_for_path(
        const std::string& path) const {
    auto res = find_in_nested_groups(_nested_group_readers, path, false);
    if (!res.found) {
        return {nullptr, ""};
    }
    if (res.child_path.empty()) {
        return {res.reader, ""};
    }
    if (res.reader && res.reader->child_readers.contains(res.child_path)) {
        return {res.reader, std::move(res.child_path)};
    }
    return {nullptr, ""};
}

std::tuple<bool, std::vector<const NestedGroupReader*>, std::string>
VariantColumnReader::collect_nested_group_chain(const std::string& path) const {
    auto res = find_in_nested_groups(_nested_group_readers, path, true);
    return {res.found, std::move(res.chain), std::move(res.child_path)};
}

} // namespace doris::segment_v2
