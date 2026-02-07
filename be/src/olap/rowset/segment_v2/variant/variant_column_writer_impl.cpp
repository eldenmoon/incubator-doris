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
#include "olap/rowset/segment_v2/variant/variant_column_writer_impl.h"

#include <gen_cpp/segment_v2.pb.h>

#include <algorithm>
#include <memory>

#include "common/config.h"
#include "common/status.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/rowset/rowset_writer_context.h"
#include "olap/rowset/segment_v2/column_writer.h"
#include "olap/rowset/segment_v2/indexed_column_writer.h"
#include "olap/rowset/segment_v2/variant/nested_group_builder.h"
#include "olap/rowset/segment_v2/variant/nested_group_path.h"
#include "olap/rowset/segment_v2/variant/nested_offsets_mapping_index.h"
#include "olap/tablet_schema.h"
#include "olap/types.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_variant.h"
#include "vec/common/variant_util.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/json/path_in_data.h"
#include "vec/olap/olap_data_convertor.h"

namespace doris::segment_v2 {

#include "common/compile_check_begin.h"

// forward declaration for NestedGroup write helper used by both
// VariantColumnWriterImpl and VariantSubcolumnWriter.
static Status write_nested_groups_to_storage(
        const doris::segment_v2::NestedGroupsMap& nested_groups, const TabletColumn* tablet_column,
        const ColumnWriterOptions& opts, vectorized::OlapBlockDataConvertor* converter,
        size_t num_rows, int& column_id,
        std::unordered_map<std::string, NestedGroupWriter>& writers, VariantStatistics& statistics);

template <typename Func>
static Status for_each_nested_group_writer(
        std::unordered_map<std::string, NestedGroupWriter>& writers, Func&& func) {
    for (auto& [_, ngw] : writers) {
        if (ngw.offsets_writer) {
            RETURN_IF_ERROR(func(ngw.offsets_writer.get(), ngw.offsets_opts));
        }
        for (auto& [path, cw] : ngw.child_writers) {
            if (!cw) {
                continue;
            }
            auto it = ngw.child_opts.find(path);
            if (it != ngw.child_opts.end()) {
                RETURN_IF_ERROR(func(cw.get(), it->second));
            }
        }
    }
    return Status::OK();
}

static Status write_nested_offsets_mapping_index(
        std::unordered_map<std::string, NestedGroupWriter>& writers) {
    for (auto& [_, ngw] : writers) {
        if (ngw.offsets_mapping_index_writer) {
            RETURN_IF_ERROR(ngw.offsets_mapping_index_writer->write(
                    ngw.offsets_opts.file_writer, ngw.offsets_opts.meta,
                    ngw.offsets_opts.compression_type));
        }
    }
    return Status::OK();
}

static Status build_nested_groups_from_variant_jsonb(
        const vectorized::ColumnVariant& variant, bool include_jsonb_subcolumns,
        doris::segment_v2::NestedGroupsMap* nested_groups) {
    if (nested_groups == nullptr) {
        return Status::InvalidArgument("nested_groups is null");
    }

    nested_groups->clear();
    doris::segment_v2::NestedGroupBuilder ng_builder;
    ng_builder.set_max_depth(static_cast<size_t>(config::variant_nested_group_max_depth));

    if (variant.get_root_type() &&
        vectorized::remove_nullable(variant.get_root_type())->get_primitive_type() ==
                PrimitiveType::TYPE_JSONB &&
        variant.get_root()) {
        RETURN_IF_ERROR(ng_builder.build_from_jsonb(variant.get_root()->get_ptr(), *nested_groups,
                                                    variant.rows()));
    }

    if (!include_jsonb_subcolumns) {
        return Status::OK();
    }

    for (const auto& entry :
         vectorized::variant_util::get_sorted_subcolumns(variant.get_subcolumns())) {
        if (entry->path.empty()) {
            continue;
        }
        const auto& t = entry->data.get_least_common_type();
        if (!t ||
            vectorized::remove_nullable(t)->get_primitive_type() != PrimitiveType::TYPE_JSONB) {
            continue;
        }
        RETURN_IF_ERROR(
                ng_builder.build_from_jsonb(entry->data.get_finalized_column_ptr()->get_ptr(),
                                            entry->path, *nested_groups, entry->data.size()));
    }

    return Status::OK();
}

void _init_column_meta(ColumnMetaPB* meta, uint32_t column_id, const TabletColumn& column,
                       CompressionTypePB compression_type) {
    meta->Clear();
    meta->set_column_id(column_id);
    meta->set_type(int(column.type()));
    meta->set_length(column.length());
    meta->set_encoding(DEFAULT_ENCODING);
    meta->set_compression(compression_type);
    meta->set_is_nullable(column.is_nullable());
    meta->set_default_value(column.default_value());
    meta->set_precision(column.precision());
    meta->set_frac(column.frac());
    if (column.has_path_info()) {
        column.path_info_ptr()->to_protobuf(meta->mutable_column_path_info(),
                                            column.parent_unique_id());
    }
    meta->set_unique_id(column.unique_id());
    for (uint32_t i = 0; i < column.get_subtype_count(); ++i) {
        _init_column_meta(meta->add_children_columns(), column_id, column.get_sub_column(i),
                          compression_type);
    }
    if (column.is_variant_type()) {
        meta->set_variant_max_subcolumns_count(column.variant_max_subcolumns_count());
    }
}

Status _create_column_writer(uint32_t cid, const TabletColumn& column,
                             const TabletSchemaSPtr& tablet_schema,
                             IndexFileWriter* inverted_index_file_writer,
                             std::unique_ptr<ColumnWriter>* writer,
                             TabletIndexes& subcolumn_indexes, ColumnWriterOptions* opt,
                             int64_t none_null_value_size, bool need_record_none_null_value_size) {
    _init_column_meta(opt->meta, cid, column, opt->compression_type);
    // no need to record none null value size for typed column or nested column, since it's compaction stage
    // will directly pick it as sub column
    if (need_record_none_null_value_size) {
        // record none null value size for statistics
        opt->meta->set_none_null_size(none_null_value_size);
    }
    opt->need_zone_map = tablet_schema->keys_type() != KeysType::AGG_KEYS;
    opt->need_bloom_filter = column.is_bf_column();
    const auto& parent_index = tablet_schema->inverted_indexs(column.parent_unique_id());

    // init inverted index
    // parent_index denotes the index of the entire variant column
    // while subcolumn_index denotes the current subcolumn's index
    if (segment_v2::IndexColumnWriter::check_support_inverted_index(column)) {
        auto init_opt_inverted_index = [&]() {
            DCHECK(!subcolumn_indexes.empty());
            for (const auto& index : subcolumn_indexes) {
                opt->inverted_indexes.push_back(index.get());
            }
            opt->need_inverted_index = true;
            DCHECK(inverted_index_file_writer != nullptr);
            opt->index_file_writer = inverted_index_file_writer;
        };

        // the subcolumn index is already initialized
        if (!subcolumn_indexes.empty()) {
            init_opt_inverted_index();
        }
        // the subcolumn index is not initialized, but the parent index is present
        else if (!parent_index.empty() &&
                 vectorized::variant_util::inherit_index(parent_index, subcolumn_indexes, column)) {
            init_opt_inverted_index();
        }
        // no parent index and no subcolumn index
        else {
            opt->need_inverted_index = false;
        }
    }

#define DISABLE_INDEX_IF_FIELD_TYPE(TYPE, type_name)          \
    if (column.type() == FieldType::OLAP_FIELD_TYPE_##TYPE) { \
        opt->need_zone_map = false;                           \
        opt->need_bloom_filter = false;                       \
    }

    DISABLE_INDEX_IF_FIELD_TYPE(ARRAY, "array")
    DISABLE_INDEX_IF_FIELD_TYPE(JSONB, "jsonb")
    DISABLE_INDEX_IF_FIELD_TYPE(VARIANT, "variant")

#undef DISABLE_INDEX_IF_FIELD_TYPE

    RETURN_IF_ERROR(ColumnWriter::create(*opt, &column, opt->file_writer, writer));
    RETURN_IF_ERROR((*writer)->init());

    return Status::OK();
}

Status convert_and_write_column(vectorized::OlapBlockDataConvertor* converter,
                                const TabletColumn& column, vectorized::DataTypePtr data_type,
                                ColumnWriter* writer,

                                const vectorized::ColumnPtr& src_column, size_t num_rows,
                                int column_id) {
    converter->add_column_data_convertor(column);
    RETURN_IF_ERROR(converter->set_source_content_with_specifid_column({src_column, data_type, ""},
                                                                       0, num_rows, column_id));
    auto [status, converted_column] = converter->convert_column_data(column_id);
    RETURN_IF_ERROR(status);

    const uint8_t* nullmap = converted_column->get_nullmap();
    RETURN_IF_ERROR(writer->append(nullmap, converted_column->get_data(), num_rows));

    converter->clear_source_content(column_id);
    return Status::OK();
}

Status UnifiedSparseColumnWriter::init(const TabletColumn* parent_column, int bucket_num,
                                       int& column_id, const ColumnWriterOptions& base_opts,
                                       SegmentFooterPB* footer) {
    _bucket_num = std::max(1, bucket_num);
    if (_bucket_num <= 1) {
        TabletColumn sparse_column = vectorized::variant_util::create_sparse_column(*parent_column);
        RETURN_IF_ERROR(init_single(sparse_column, column_id, base_opts, footer));
    } else {
        RETURN_IF_ERROR(init_buckets(_bucket_num, *parent_column, column_id, base_opts, footer));
    }
    return Status::OK();
}

Status UnifiedSparseColumnWriter::append_data(const TabletColumn* parent_column,
                                              const vectorized::ColumnVariant& src, size_t num_rows,
                                              vectorized::OlapBlockDataConvertor* converter) {
    if (_single_writer) {
        RETURN_IF_ERROR(append_single_sparse(src, num_rows, converter, *parent_column));
    } else {
        RETURN_IF_ERROR(append_bucket_sparse(src, num_rows, converter, *parent_column));
    }
    return Status::OK();
}

void UnifiedSparseColumnWriter::merge_stats_to(VariantStatistics* stats) const {
    if (stats == nullptr) {
        return;
    }
    for (const auto& [path, cnt] : _stats.sparse_column_non_null_size) {
        stats->sparse_column_non_null_size[path] += cnt;
    }
}

// UnifiedSparseColumnWriter implementation
Status UnifiedSparseColumnWriter::init_single(const TabletColumn& sparse_column, int& column_id,
                                              const ColumnWriterOptions& base_opts,
                                              SegmentFooterPB* footer) {
    _single_opts = base_opts;
    _single_opts.meta = footer->add_columns();
    _init_column_meta(_single_opts.meta, column_id, sparse_column, base_opts.compression_type);
    RETURN_IF_ERROR(ColumnWriter::create_map_writer(_single_opts, &sparse_column,
                                                    base_opts.file_writer, &_single_writer));
    RETURN_IF_ERROR(_single_writer->init());
    _first_column_id = column_id;
    ++column_id;
    return Status::OK();
}

Status UnifiedSparseColumnWriter::init_buckets(int bucket_num, const TabletColumn& parent_column,
                                               int& column_id, const ColumnWriterOptions& base_opts,
                                               SegmentFooterPB* footer) {
    _bucket_writers.clear();
    _bucket_opts.clear();
    _bucket_writers.resize(bucket_num);
    _bucket_opts.resize(bucket_num);
    for (int b = 0; b < bucket_num; ++b) {
        TabletColumn bucket_col =
                vectorized::variant_util::create_sparse_shard_column(parent_column, b);
        _bucket_opts[b] = base_opts;
        _bucket_opts[b].meta = footer->add_columns();
        _init_column_meta(_bucket_opts[b].meta, column_id, bucket_col, base_opts.compression_type);
        RETURN_IF_ERROR(ColumnWriter::create_map_writer(
                _bucket_opts[b], &bucket_col, base_opts.file_writer, &_bucket_writers[b]));
        RETURN_IF_ERROR(_bucket_writers[b]->init());
        if (b == 0) {
            _first_column_id = column_id;
        }
        ++column_id;
    }
    return Status::OK();
}

uint64_t UnifiedSparseColumnWriter::estimate_buffer_size() const {
    uint64_t size = 0;
    if (_single_writer) {
        size += _single_writer->estimate_buffer_size();
    }
    for (const auto& w : _bucket_writers) {
        if (w) {
            size += w->estimate_buffer_size();
        }
    }
    return size;
}

Status UnifiedSparseColumnWriter::finish() {
    if (_single_writer) {
        RETURN_IF_ERROR(_single_writer->finish());
    }
    for (auto& w : _bucket_writers) {
        if (w) {
            RETURN_IF_ERROR(w->finish());
        }
    }
    return Status::OK();
}

Status UnifiedSparseColumnWriter::write_data() {
    if (_single_writer) {
        RETURN_IF_ERROR(_single_writer->write_data());
    }
    for (auto& w : _bucket_writers) {
        if (w) {
            RETURN_IF_ERROR(w->write_data());
        }
    }
    return Status::OK();
}

Status UnifiedSparseColumnWriter::write_ordinal_index() {
    if (_single_writer) {
        RETURN_IF_ERROR(_single_writer->write_ordinal_index());
    }
    for (auto& w : _bucket_writers) {
        if (w) {
            RETURN_IF_ERROR(w->write_ordinal_index());
        }
    }
    return Status::OK();
}

// Single sparse mode path:
// - Convert the pre-serialized sparse ColumnMap from the engine format
//   (src.get_sparse_column()) to storage format using converter, binding
//   to the column id allocated during init_single (stored in _first_column_id).
// - Append to the single writer and populate sparse path statistics into
//   out_stats and the single column meta.
Status UnifiedSparseColumnWriter::append_single_sparse(
        const vectorized::ColumnVariant& src, size_t num_rows,
        vectorized::OlapBlockDataConvertor* converter, const TabletColumn& parent_column) {
    TabletColumn sparse_column = vectorized::variant_util::create_sparse_column(parent_column);
    converter->add_column_data_convertor(sparse_column);
    DCHECK_EQ(src.get_sparse_column()->size(), num_rows);
    RETURN_IF_ERROR(converter->set_source_content_with_specifid_column(
            {src.get_sparse_column(), nullptr, ""}, 0, num_rows, _first_column_id));
    auto [status, column] = converter->convert_column_data(_first_column_id);
    RETURN_IF_ERROR(status);
    RETURN_IF_ERROR(_single_writer->append(column->get_nullmap(), column->get_data(), num_rows));
    converter->clear_source_content(_first_column_id);

    // Build path frequency statistics with upper bound limit to avoid
    // large memory and metadata size. Persist to meta for readers.
    std::unordered_map<StringRef, size_t> path_counts;
    const auto [paths, _] = src.get_sparse_data_paths_and_values();
    size_t limit = parent_column.variant_max_sparse_column_statistics_size();
    for (size_t i = 0; i != paths->size(); ++i) {
        auto k = paths->get_data_at(i);
        if (auto it = path_counts.find(k); it != path_counts.end())
            ++it->second;
        else if (path_counts.size() < limit)
            path_counts.emplace(k, 1);
    }
    segment_v2::VariantStatistics sparse_stats;
    for (const auto& [k, cnt] : path_counts) {
        sparse_stats.sparse_column_non_null_size.emplace(k.to_string(), static_cast<uint32_t>(cnt));
    }
    sparse_stats.to_pb(_single_opts.meta->mutable_variant_statistics());
    _stats.sparse_column_non_null_size = sparse_stats.sparse_column_non_null_size;
    _single_opts.meta->set_num_rows(num_rows);
    return Status::OK();
}

// Bucketized sparse mode path:
// - Materialize N temporary ColumnMap (keys, values, offsets)
// - For each row, distribute (path,value) pairs to the bucket decided by
//   variant_util::variant_sparse_shard_of(path)
// - Convert and append each bucket map to its writer using the column id
//   sequence initialized by init_buckets (starting at _first_column_id)
// - Compute per-bucket path stats and persist into each bucket's meta
Status UnifiedSparseColumnWriter::append_bucket_sparse(
        const vectorized::ColumnVariant& src, size_t num_rows,
        vectorized::OlapBlockDataConvertor* converter, const TabletColumn& parent_column) {
    const int bucket_num = static_cast<int>(_bucket_writers.size());
    const auto [paths_col, values_col] = src.get_sparse_data_paths_and_values();
    const auto& offsets = src.serialized_sparse_column_offsets();

    std::vector<vectorized::MutableColumnPtr> tmp_maps(bucket_num);
    for (int b = 0; b < bucket_num; ++b) {
        tmp_maps[b] = vectorized::ColumnMap::create(
                vectorized::ColumnString::create(), vectorized::ColumnString::create(),
                vectorized::ColumnArray::ColumnOffsets::create());
    }
    for (int b = 0; b < bucket_num; ++b) {
        auto& m = assert_cast<vectorized::ColumnMap&>(*tmp_maps[b]);
        m.get_offsets().reserve(num_rows);
    }
    for (ssize_t row = 0; row < static_cast<ssize_t>(num_rows); ++row) {
        size_t start = offsets[row - 1];
        size_t end = offsets[row];
        for (size_t i = start; i < end; ++i) {
            StringRef path = paths_col->get_data_at(i);
            uint32_t b = vectorized::variant_util::variant_binary_shard_of(path, bucket_num);
            auto& map_col = assert_cast<vectorized::ColumnMap&>(*tmp_maps[b]);
            map_col.get_keys_ptr()->assume_mutable()->insert_from(*paths_col, i);
            map_col.get_values_ptr()->assume_mutable()->insert_from(*values_col, i);
        }
        for (int b = 0; b < bucket_num; ++b) {
            auto& map_col = assert_cast<vectorized::ColumnMap&>(*tmp_maps[b]);
            map_col.get_offsets().push_back(map_col.get_keys().size());
        }
    }
    for (int b = 0; b < bucket_num; ++b) {
        TabletColumn bucket_col =
                vectorized::variant_util::create_sparse_shard_column(parent_column, b);
        converter->add_column_data_convertor(bucket_col);
        int this_col_id = _first_column_id + b;
        RETURN_IF_ERROR(converter->set_source_content_with_specifid_column(
                {tmp_maps[b]->get_ptr(), nullptr, ""}, 0, num_rows, this_col_id));
        auto [st, converted] = converter->convert_column_data(this_col_id);
        RETURN_IF_ERROR(st);
        RETURN_IF_ERROR(_bucket_writers[b]->append(converted->get_nullmap(), converted->get_data(),
                                                   num_rows));
        converter->clear_source_content(this_col_id);
        _bucket_opts[b].meta->set_num_rows(num_rows);
    }
    // per-bucket statistics
    for (int b = 0; b < bucket_num; ++b) {
        auto& map_col = assert_cast<vectorized::ColumnMap&>(*tmp_maps[b]);
        const auto& keys = assert_cast<const vectorized::ColumnString&>(map_col.get_keys());
        std::unordered_map<StringRef, size_t> bucket_path_counts;
        bucket_path_counts.reserve(1024);
        size_t limit = parent_column.variant_max_sparse_column_statistics_size();
        for (size_t i = 0; i < keys.size(); ++i) {
            StringRef k = keys.get_data_at(i);
            if (auto it = bucket_path_counts.find(k); it != bucket_path_counts.end())
                ++it->second;
            else if (bucket_path_counts.size() < limit)
                bucket_path_counts.emplace(k, 1);
        }
        segment_v2::VariantStatistics bucket_stats;
        for (const auto& [k, cnt] : bucket_path_counts) {
            const std::string k_str = k.to_string();
            const uint32_t cnt_u32 = static_cast<uint32_t>(cnt);
            bucket_stats.sparse_column_non_null_size.emplace(k_str, cnt_u32);
            _stats.sparse_column_non_null_size[k_str] += cnt_u32;
        }
        bucket_stats.to_pb(_bucket_opts[b].meta->mutable_variant_statistics());
        _bucket_opts[b].meta->set_num_rows(num_rows);
    }
    return Status::OK();
}

Status VariantDocWriter::init(const TabletColumn* parent_column, int bucket_num, int& column_id,
                              const ColumnWriterOptions& opts, SegmentFooterPB* footer) {
    _bucket_num = bucket_num;
    _first_column_id = column_id;
    _doc_value_column_writers.resize(_bucket_num);
    _doc_value_column_opts.resize(_bucket_num);
    for (int b = 0; b < _bucket_num; ++b) {
        const TabletColumn& bucket_column =
                vectorized::variant_util::create_doc_value_column(*parent_column, b);
        _doc_value_column_opts[b] = opts;
        _doc_value_column_opts[b].meta = footer->add_columns();
        _init_column_meta(_doc_value_column_opts[b].meta, column_id, bucket_column,
                          opts.compression_type);
        RETURN_IF_ERROR(ColumnWriter::create_map_writer(_doc_value_column_opts[b], &bucket_column,
                                                        opts.file_writer,
                                                        &_doc_value_column_writers[b]));
        RETURN_IF_ERROR(_doc_value_column_writers[b]->init());
        ++column_id;
    }
    return Status::OK();
}

Status VariantDocWriter::append_data(const TabletColumn* parent_column,
                                     const vectorized::ColumnVariant& src, size_t num_rows,
                                     vectorized::OlapBlockDataConvertor* converter) {
    _stats.doc_value_column_non_null_size.clear();
    const auto [paths_col, values_col] = src.get_doc_value_data_paths_and_values();
    const auto& offsets = src.serialized_doc_value_column_offsets();

    std::vector<vectorized::MutableColumnPtr> tmp_maps(_bucket_num);
    for (int b = 0; b < _bucket_num; ++b) {
        tmp_maps[b] = vectorized::ColumnVariant::create_binary_column_fn();
        auto& map_col = assert_cast<vectorized::ColumnMap&>(*tmp_maps[b]);
        map_col.get_offsets().reserve(num_rows);
    }

    std::vector<std::unordered_map<StringRef, uint32_t>> bucket_path_counts(_bucket_num);

    for (size_t row = 0; row < num_rows; ++row) {
        const ssize_t srow = static_cast<ssize_t>(row);
        size_t start = offsets[srow - 1];
        size_t end = offsets[srow];
        for (size_t i = start; i < end; ++i) {
            StringRef path = paths_col->get_data_at(i);
            uint32_t bucket = vectorized::variant_util::variant_binary_shard_of(path, _bucket_num);
            auto& map_col = assert_cast<vectorized::ColumnMap&>(*tmp_maps[bucket]);
            map_col.get_keys_ptr()->assume_mutable()->insert_from(*paths_col, i);
            map_col.get_values_ptr()->assume_mutable()->insert_from(*values_col, i);
            bucket_path_counts[bucket][path]++;
        }
        for (int b = 0; b < _bucket_num; ++b) {
            auto& map_col = assert_cast<vectorized::ColumnMap&>(*tmp_maps[b]);
            map_col.get_offsets().push_back(map_col.get_keys().size());
        }
    }

    for (int b = 0; b < _bucket_num; ++b) {
        TabletColumn bucket_column =
                vectorized::variant_util::create_doc_value_column(*parent_column, b);
        converter->add_column_data_convertor(bucket_column);
        int this_col_id = _first_column_id + b;
        RETURN_IF_ERROR(converter->set_source_content_with_specifid_column(
                {tmp_maps[b]->get_ptr(), nullptr, ""}, 0, num_rows, this_col_id));
        auto [status, column] = converter->convert_column_data(this_col_id);
        RETURN_IF_ERROR(status);
        RETURN_IF_ERROR(_doc_value_column_writers[b]->append(column->get_nullmap(),
                                                             column->get_data(), num_rows));
        converter->clear_source_content(this_col_id);
        _doc_value_column_opts[b].meta->set_num_rows(num_rows);
        auto* stats = _doc_value_column_opts[b].meta->mutable_variant_statistics();
        auto* doc_value_column_non_null_size = stats->mutable_doc_value_column_non_null_size();
        for (const auto& [k, cnt] : bucket_path_counts[b]) {
            const std::string k_str = k.to_string();
            (*doc_value_column_non_null_size)[k_str] = cnt;
            _stats.doc_value_column_non_null_size[k_str] += cnt;
        }
        _doc_value_column_opts[b].meta->set_num_rows(num_rows);
    }

    return Status::OK();
}

void VariantDocWriter::merge_stats_to(VariantStatistics* stats) const {
    if (stats == nullptr) {
        return;
    }
    for (const auto& [path, cnt] : _stats.doc_value_column_non_null_size) {
        stats->doc_value_column_non_null_size[path] += cnt;
    }
}

uint64_t VariantDocWriter::estimate_buffer_size() const {
    uint64_t size = 0;
    for (const auto& writer : _doc_value_column_writers) {
        size += writer->estimate_buffer_size();
    }
    return size;
}

Status VariantDocWriter::finish() {
    for (auto& writer : _doc_value_column_writers) {
        RETURN_IF_ERROR(writer->finish());
    }
    return Status::OK();
}

Status VariantDocWriter::write_data() {
    for (auto& writer : _doc_value_column_writers) {
        RETURN_IF_ERROR(writer->write_data());
    }
    return Status::OK();
}

Status VariantDocWriter::write_ordinal_index() {
    for (auto& writer : _doc_value_column_writers) {
        RETURN_IF_ERROR(writer->write_ordinal_index());
    }
    return Status::OK();
}

VariantColumnWriterImpl::VariantColumnWriterImpl(const ColumnWriterOptions& opts,
                                                 const TabletColumn* column) {
    _opts = opts;
    _tablet_column = column;
    _null_column = vectorized::ColumnUInt8::create();
}

Status VariantColumnWriterImpl::init() {
    DCHECK(_tablet_column->variant_max_subcolumns_count() >= 0)
            << "max subcolumns count is: " << _tablet_column->variant_max_subcolumns_count();
    int count = _tablet_column->variant_max_subcolumns_count();
    if (_opts.rowset_ctx->write_type == DataWriteType::TYPE_DIRECT) {
        count = 0;
    }
    _column = vectorized::ColumnVariant::create(count);
    return Status::OK();
}

Status VariantColumnWriterImpl::_process_root_column(vectorized::ColumnVariant* ptr,
                                                     vectorized::OlapBlockDataConvertor* converter,
                                                     size_t num_rows, int& column_id) {
    // root column
    _root_writer = std::make_unique<ScalarColumnWriter>(
            _opts, std::unique_ptr<Field>(FieldFactory::create(*_tablet_column)),
            _opts.file_writer);
    RETURN_IF_ERROR(_root_writer->init());

    // make sure the root type
    auto expected_root_type = vectorized::make_nullable(
            std::make_shared<vectorized::ColumnVariant::MostCommonType>());
    ptr->ensure_root_node_type(expected_root_type);

    DCHECK_EQ(ptr->get_root()->get_ptr()->size(), num_rows);
    converter->add_column_data_convertor(*_tablet_column);
    const uint8_t* nullmap = nullptr;
    auto& nullable_column =
            assert_cast<vectorized::ColumnNullable&>(*ptr->get_root()->assume_mutable());
    auto root_column = nullable_column.get_nested_column_ptr();
    // If the root variant is nullable, then update the root column null column with the outer null column.
    if (_tablet_column->is_nullable()) {
        // use outer null column as final null column
        root_column = vectorized::ColumnNullable::create(
                root_column->get_ptr(), vectorized::ColumnUInt8::create(*_null_column));
        nullmap = _null_column->get_data().data();
    } else {
        // Otherwise setting to all not null.
        root_column = vectorized::ColumnNullable::create(
                root_column->get_ptr(), vectorized::ColumnUInt8::create(root_column->size(), 0));
    }
    // make sure the root_column is nullable
    RETURN_IF_ERROR(converter->set_source_content_with_specifid_column(
            {root_column->get_ptr(), nullptr, ""}, 0, num_rows, column_id));
    auto [status, column] = converter->convert_column_data(column_id);
    if (!status.ok()) {
        return status;
    }
    RETURN_IF_ERROR(_root_writer->append(nullmap, column->get_data(), num_rows));
    converter->clear_source_content(column_id);
    ++column_id;

    _opts.meta->set_num_rows(num_rows);
    return Status::OK();
}

Status VariantColumnWriterImpl::_process_subcolumns(vectorized::ColumnVariant* ptr,
                                                    vectorized::OlapBlockDataConvertor* converter,
                                                    size_t num_rows, int& column_id) {
    // generate column info by entry info
    auto generate_column_info = [&](const auto& entry) {
        const std::string& column_name =
                _tablet_column->name_lower_case() + "." + entry->path.get_path();
        const vectorized::DataTypePtr& final_data_type_from_object =
                entry->data.get_least_common_type();
        vectorized::PathInData full_path;
        if (entry->path.has_nested_part()) {
            vectorized::PathInDataBuilder full_path_builder;
            full_path = full_path_builder.append(_tablet_column->name_lower_case(), false)
                                .append(entry->path.get_parts(), false)
                                .build();
        } else {
            full_path = vectorized::PathInData(column_name);
        }
        // set unique_id and parent_unique_id, will use unique_id to get iterator correct
        auto column = vectorized::variant_util::get_column_by_type(
                final_data_type_from_object, column_name,
                vectorized::variant_util::ExtraInfo {
                        .unique_id = -1,
                        .parent_unique_id = _tablet_column->unique_id(),
                        .path_info = full_path});
        return column;
    };
    _subcolumns_indexes.resize(ptr->get_subcolumns().size());
    // convert sub column data from engine format to storage layer format
    // NOTE: We only keep up to variant_max_subcolumns_count as extracted columns; others are externalized.
    for (const auto& entry :
         vectorized::variant_util::get_sorted_subcolumns(ptr->get_subcolumns())) {
        const auto& least_common_type = entry->data.get_least_common_type();
        if (vectorized::variant_util::get_base_type_of_array(least_common_type)
                    ->get_primitive_type() == PrimitiveType::INVALID_TYPE) {
            continue;
        }
        if (entry->path.empty()) {
            // already handled
            continue;
        }
        CHECK(entry->data.is_finalized());

        // create subcolumn writer if under limit; otherwise externalize ColumnMetaPB via IndexedColumn
        int current_column_id = column_id++;
        TabletColumn tablet_column;
        int64_t none_null_value_size = entry->data.get_non_null_value_size();
        vectorized::ColumnPtr current_column = entry->data.get_finalized_column_ptr()->get_ptr();
        vectorized::DataTypePtr current_type = entry->data.get_least_common_type();
        if (auto current_path = entry->path.get_path();
            _subcolumns_info.find(current_path) != _subcolumns_info.end()) {
            tablet_column = std::move(_subcolumns_info[current_path].column);
            _subcolumns_indexes[current_column_id] =
                    std::move(_subcolumns_info[current_path].indexes);
            if (auto storage_type =
                        vectorized::DataTypeFactory::instance().create_data_type(tablet_column);
                !storage_type->equals(*current_type)) {
                return Status::InvalidArgument("Storage type {} is not equal to current type {}",
                                               storage_type->get_name(), current_type->get_name());
            }
        } else {
            tablet_column = generate_column_info(entry);
        }
        ColumnWriterOptions opts;
        opts.meta = _opts.footer->add_columns();
        opts.index_file_writer = _opts.index_file_writer;
        opts.compression_type = _opts.compression_type;
        opts.rowset_ctx = _opts.rowset_ctx;
        opts.file_writer = _opts.file_writer;
        opts.encoding_preference = _opts.encoding_preference;
        std::unique_ptr<ColumnWriter> writer;
        vectorized::variant_util::inherit_column_attributes(*_tablet_column, tablet_column);

        bool need_record_none_null_value_size =
                (!tablet_column.path_info_ptr()->get_is_typed() ||
                 _tablet_column->variant_enable_typed_paths_to_sparse()) &&
                !tablet_column.path_info_ptr()->has_nested_part();

        RETURN_IF_ERROR(_create_column_writer(
                current_column_id, tablet_column, _opts.rowset_ctx->tablet_schema,
                _opts.index_file_writer, &writer, _subcolumns_indexes[current_column_id], &opts,
                none_null_value_size, need_record_none_null_value_size));
        _subcolumn_writers.push_back(std::move(writer));
        _subcolumn_opts.push_back(opts);
        _subcolumn_opts[current_column_id - 1].meta->set_num_rows(num_rows);

        RETURN_IF_ERROR(convert_and_write_column(converter, tablet_column, current_type,
                                                 _subcolumn_writers[current_column_id - 1].get(),
                                                 current_column, ptr->rows(), current_column_id));
    }
    return Status::OK();
}

Status VariantColumnWriterImpl::_process_binary_column(
        vectorized::ColumnVariant* ptr, vectorized::OlapBlockDataConvertor* converter,
        size_t num_rows, int& column_id) {
    int bucket_num = 1;
    if (_tablet_column->variant_enable_doc_mode()) {
        _binary_writer = std::make_unique<VariantDocWriter>();
        bucket_num = std::max(1, _tablet_column->variant_doc_hash_shard_count());
        ptr->sort_doc_value_column();
    } else {
        _binary_writer = std::make_unique<UnifiedSparseColumnWriter>();
        bucket_num = std::max(1, _tablet_column->variant_sparse_hash_shard_count());
    }

    RETURN_IF_ERROR(
            _binary_writer->init(_tablet_column, bucket_num, column_id, _opts, _opts.footer));
    RETURN_IF_ERROR(_binary_writer->append_data(_tablet_column, *ptr, num_rows, converter));
    return Status::OK();
}

Status VariantColumnWriterImpl::finalize() {
    auto* ptr = _column.get();
    ptr->set_max_subcolumns_count(_tablet_column->variant_max_subcolumns_count());
    ptr->finalize(vectorized::ColumnVariant::FinalizeMode::WRITE_MODE);
    // convert each subcolumns to storage format and add data to sub columns writers buffer
    auto olap_data_convertor = std::make_unique<vectorized::OlapBlockDataConvertor>();

    DCHECK(ptr->is_finalized());

    for (const auto& entry :
         vectorized::variant_util::get_sorted_subcolumns(ptr->get_subcolumns())) {
        if (entry->path.empty()) {
            // already handled
            continue;
        }
        // Not supported nested path to generate sub column info, currently
        if (entry->path.has_nested_part()) {
            continue;
        }
        TabletSchema::SubColumnInfo sub_column_info;
        if (vectorized::variant_util::generate_sub_column_info(
                    *_opts.rowset_ctx->tablet_schema, _tablet_column->unique_id(),
                    entry->path.get_path(), &sub_column_info)) {
            _subcolumns_info.emplace(entry->path.get_path(), std::move(sub_column_info));
        }
    }

    RETURN_IF_ERROR(ptr->convert_typed_path_to_storage_type(_subcolumns_info));

    RETURN_IF_ERROR(ptr->pick_subcolumns_to_sparse_column(
            _subcolumns_info, _tablet_column->variant_enable_typed_paths_to_sparse()));

#ifndef NDEBUG
    ptr->check_consistency();
#endif

    // Build NestedGroups from JSONB columns. Both JSONB and NestedGroup are stored
    // for redundancy: JSONB for Whole reads, NestedGroup for column pruning.
    doris::segment_v2::NestedGroupsMap nested_groups;
    RETURN_IF_ERROR(build_nested_groups_from_variant_jsonb(*ptr, /*include_jsonb_subcolumns=*/true,
                                                           &nested_groups));

    size_t num_rows = _column->size();
    int column_id = 0;

    // convert root column data from engine format to storage layer format
    RETURN_IF_ERROR(_process_root_column(ptr, olap_data_convertor.get(), num_rows, column_id));

    const bool has_extracted_columns =
            std::ranges::any_of(_opts.rowset_ctx->tablet_schema->columns(),
                                [](const auto& column) { return column->is_extracted_column(); });
    if (!has_extracted_columns) {
        // process and append each subcolumns to sub columns writers buffer
        RETURN_IF_ERROR(_process_subcolumns(ptr, olap_data_convertor.get(), num_rows, column_id));

        // process sparse column and append to sparse writer buffer
        RETURN_IF_ERROR(
                _process_binary_column(ptr, olap_data_convertor.get(), num_rows, column_id));

        // Write NestedGroups to segment and persist stats to root meta.
        RETURN_IF_ERROR(write_nested_groups_to_storage(
                nested_groups, _tablet_column, _opts, olap_data_convertor.get(), num_rows,
                column_id, _nested_group_writers, _statistics));
        if (_binary_writer) {
            _binary_writer->merge_stats_to(&_statistics);
        }
        _statistics.to_pb(_opts.meta->mutable_variant_statistics());
    }

    _is_finalized = true;
    return Status::OK();
}

bool VariantColumnWriterImpl::is_finalized() const {
    return _column->is_finalized() && _is_finalized;
}

Status VariantColumnWriterImpl::_for_each_column_writer(
        const std::function<Status(ColumnWriter*)>& func) {
    RETURN_IF_ERROR(func(_root_writer.get()));
    for (auto& writer : _subcolumn_writers) {
        RETURN_IF_ERROR(func(writer.get()));
    }
    RETURN_IF_ERROR(for_each_nested_group_writer(
            _nested_group_writers,
            [&](ColumnWriter* writer, const ColumnWriterOptions&) { return func(writer); }));
    return Status::OK();
}

Status VariantColumnWriterImpl::append_data(const uint8_t** ptr, size_t num_rows) {
    DCHECK(!is_finalized());
    const auto* column = reinterpret_cast<const vectorized::VariantColumnData*>(*ptr);
    const auto& src = *reinterpret_cast<const vectorized::ColumnVariant*>(column->column_data);
    RETURN_IF_ERROR(src.sanitize());
    // TODO: if direct write we could avoid copy
    _column->insert_range_from(src, column->row_pos, num_rows);
    return Status::OK();
}

uint64_t VariantColumnWriterImpl::estimate_buffer_size() {
    if (!is_finalized()) {
        // not accurate
        return _column->byte_size();
    }
    uint64_t size = 0;
    size += _root_writer->estimate_buffer_size();
    for (auto& column_writer : _subcolumn_writers) {
        size += column_writer->estimate_buffer_size();
    }
    if (_binary_writer) {
        size += _binary_writer->estimate_buffer_size();
    }
    for (auto& [_, ngw] : _nested_group_writers) {
        if (ngw.offsets_writer) {
            size += ngw.offsets_writer->estimate_buffer_size();
        }
        for (auto& [__, cw] : ngw.child_writers) {
            if (cw) {
                size += cw->estimate_buffer_size();
            }
        }
    }
    return size;
}

Status VariantColumnWriterImpl::finish() {
    if (!is_finalized()) {
        RETURN_IF_ERROR(finalize());
    }
    RETURN_IF_ERROR(_for_each_column_writer([](ColumnWriter* writer) { return writer->finish(); }));
    if (_binary_writer) {
        RETURN_IF_ERROR(_binary_writer->finish());
    }
    return Status::OK();
}
Status VariantColumnWriterImpl::write_data() {
    if (!is_finalized()) {
        RETURN_IF_ERROR(finalize());
    }
    RETURN_IF_ERROR(
            _for_each_column_writer([](ColumnWriter* writer) { return writer->write_data(); }));
    if (_binary_writer) {
        RETURN_IF_ERROR(_binary_writer->write_data());
    }
    return Status::OK();
}
Status VariantColumnWriterImpl::write_ordinal_index() {
    // write ordinal index after data has been written which should be finalized
    assert(is_finalized());
    RETURN_IF_ERROR(_for_each_column_writer(
            [](ColumnWriter* writer) { return writer->write_ordinal_index(); }));
    if (_binary_writer) {
        RETURN_IF_ERROR(_binary_writer->write_ordinal_index());
    }
    return Status::OK();
}

Status VariantColumnWriterImpl::write_zone_map() {
    assert(is_finalized());
    for (size_t i = 0; i < _subcolumn_writers.size(); ++i) {
        if (_subcolumn_opts[i].need_zone_map) {
            RETURN_IF_ERROR(_subcolumn_writers[i]->write_zone_map());
        }
    }
    RETURN_IF_ERROR(for_each_nested_group_writer(
            _nested_group_writers, [](ColumnWriter* writer, const ColumnWriterOptions& opts) {
                if (opts.need_zone_map) {
                    return writer->write_zone_map();
                }
                return Status::OK();
            }));
    RETURN_IF_ERROR(write_nested_offsets_mapping_index(_nested_group_writers));
    return Status::OK();
}

Status VariantColumnWriterImpl::write_inverted_index() {
    assert(is_finalized());
    for (size_t i = 0; i < _subcolumn_writers.size(); ++i) {
        if (_subcolumn_opts[i].need_inverted_index) {
            RETURN_IF_ERROR(_subcolumn_writers[i]->write_inverted_index());
        }
    }
    RETURN_IF_ERROR(for_each_nested_group_writer(
            _nested_group_writers, [](ColumnWriter* writer, const ColumnWriterOptions& opts) {
                if (opts.need_inverted_index) {
                    return writer->write_inverted_index();
                }
                return Status::OK();
            }));
    return Status::OK();
}
Status VariantColumnWriterImpl::write_bloom_filter_index() {
    assert(is_finalized());
    for (size_t i = 0; i < _subcolumn_writers.size(); ++i) {
        if (_subcolumn_opts[i].need_bloom_filter) {
            RETURN_IF_ERROR(_subcolumn_writers[i]->write_bloom_filter_index());
        }
    }
    RETURN_IF_ERROR(for_each_nested_group_writer(
            _nested_group_writers, [](ColumnWriter* writer, const ColumnWriterOptions& opts) {
                if (opts.need_bloom_filter) {
                    return writer->write_bloom_filter_index();
                }
                return Status::OK();
            }));
    return Status::OK();
}

Status VariantColumnWriterImpl::append_nullable(const uint8_t* null_map, const uint8_t** ptr,
                                                size_t num_rows) {
    if (null_map != nullptr) {
        _null_column->insert_many_raw_data((const char*)null_map, num_rows);
    }
    RETURN_IF_ERROR(append_data(ptr, num_rows));
    return Status::OK();
}

VariantSubcolumnWriter::VariantSubcolumnWriter(const ColumnWriterOptions& opts,
                                               const TabletColumn* column,
                                               std::unique_ptr<Field> field)
        : ColumnWriter(std::move(field), opts.meta->is_nullable(), opts.meta) {
    _tablet_column = column;
    _opts = opts;
    _column = vectorized::ColumnVariant::create(0);
}

Status VariantSubcolumnWriter::init() {
    return Status::OK();
}

Status VariantSubcolumnWriter::append_data(const uint8_t** ptr, size_t num_rows) {
    const auto* column = reinterpret_cast<const vectorized::VariantColumnData*>(*ptr);
    const auto& src = *reinterpret_cast<const vectorized::ColumnVariant*>(column->column_data);
    // TODO: if direct write we could avoid copy
    _column->insert_range_from(src, column->row_pos, num_rows);
    return Status::OK();
}

uint64_t VariantSubcolumnWriter::estimate_buffer_size() {
    if (!is_finalized()) {
        return _column->byte_size();
    }
    uint64_t size = 0;
    if (_writer) {
        size += _writer->estimate_buffer_size();
    }
    for (auto& [_, ngw] : _nested_group_writers) {
        if (ngw.offsets_writer) {
            size += ngw.offsets_writer->estimate_buffer_size();
        }
        for (auto& [__, cw] : ngw.child_writers) {
            if (cw) {
                size += cw->estimate_buffer_size();
            }
        }
    }
    return size;
}

bool VariantSubcolumnWriter::is_finalized() const {
    return _column->is_finalized() && _is_finalized;
}

Status VariantSubcolumnWriter::finalize() {
    auto* ptr = _column.get();
    ptr->finalize();

    DCHECK(ptr->is_finalized());
    const auto& parent_column =
            _opts.rowset_ctx->tablet_schema->column_by_uid(_tablet_column->parent_unique_id());

    TabletColumn flush_column;
    if (ptr->get_subcolumns().get_root()->data.get_least_common_base_type_id() ==
        PrimitiveType::INVALID_TYPE) {
        auto flush_type = vectorized::DataTypeFactory::instance().create_data_type(
                PrimitiveType::TYPE_TINYINT, true /* is_nullable */);
        ptr->ensure_root_node_type(flush_type);
    }
    flush_column = vectorized::variant_util::get_column_by_type(
            ptr->get_root_type(), _tablet_column->name(),
            vectorized::variant_util::ExtraInfo {
                    .unique_id = -1,
                    .parent_unique_id = _tablet_column->parent_unique_id(),
                    .path_info = *_tablet_column->path_info_ptr()});

    int64_t none_null_value_size = ptr->get_subcolumns().get_root()->data.get_non_null_value_size();
    bool need_record_none_null_value_size = (!flush_column.path_info_ptr()->get_is_typed()) &&
                                            !flush_column.path_info_ptr()->has_nested_part();
    ColumnWriterOptions opts = _opts;

    // refresh opts and get writer with flush column
    vectorized::variant_util::inherit_column_attributes(parent_column, flush_column);
    RETURN_IF_ERROR(_create_column_writer(0, flush_column, _opts.rowset_ctx->tablet_schema,
                                          _opts.index_file_writer, &_writer, _indexes, &opts,
                                          none_null_value_size, need_record_none_null_value_size));

    _opts = opts;
    auto olap_data_convertor = std::make_unique<vectorized::OlapBlockDataConvertor>();
    int column_id = 0;
    RETURN_IF_ERROR(convert_and_write_column(olap_data_convertor.get(), flush_column,
                                             ptr->get_root_type(), _writer.get(),
                                             ptr->get_root()->get_ptr(), ptr->rows(), column_id));
    _opts.meta->set_num_rows(ptr->rows());
    ++column_id;

    // also expand array<object> JSONB into NestedGroup for compaction sub-variant writer.
    doris::segment_v2::NestedGroupsMap nested_groups;
    RETURN_IF_ERROR(build_nested_groups_from_variant_jsonb(*ptr, /*include_jsonb_subcolumns=*/false,
                                                           &nested_groups));
    RETURN_IF_ERROR(write_nested_groups_to_storage(
            nested_groups, &flush_column, _opts, olap_data_convertor.get(), ptr->rows(), column_id,
            /*writers=*/_nested_group_writers, /*statistics=*/_statistics));
    _statistics.to_pb(_opts.meta->mutable_variant_statistics());

    _is_finalized = true;
    return Status::OK();
}

Status VariantSubcolumnWriter::finish() {
    if (!is_finalized()) {
        RETURN_IF_ERROR(finalize());
    }
    RETURN_IF_ERROR(_writer->finish());
    RETURN_IF_ERROR(for_each_nested_group_writer(
            _nested_group_writers,
            [](ColumnWriter* writer, const ColumnWriterOptions&) { return writer->finish(); }));
    return Status::OK();
}
Status VariantSubcolumnWriter::write_data() {
    if (!is_finalized()) {
        RETURN_IF_ERROR(finalize());
    }
    RETURN_IF_ERROR(_writer->write_data());
    RETURN_IF_ERROR(for_each_nested_group_writer(
            _nested_group_writers,
            [](ColumnWriter* writer, const ColumnWriterOptions&) { return writer->write_data(); }));
    return Status::OK();
}
Status VariantSubcolumnWriter::write_ordinal_index() {
    assert(is_finalized());
    RETURN_IF_ERROR(_writer->write_ordinal_index());
    RETURN_IF_ERROR(for_each_nested_group_writer(
            _nested_group_writers, [](ColumnWriter* writer, const ColumnWriterOptions&) {
                return writer->write_ordinal_index();
            }));
    return Status::OK();
}

Status VariantSubcolumnWriter::write_zone_map() {
    assert(is_finalized());
    if (_opts.need_zone_map) {
        RETURN_IF_ERROR(_writer->write_zone_map());
    }
    RETURN_IF_ERROR(for_each_nested_group_writer(
            _nested_group_writers, [](ColumnWriter* writer, const ColumnWriterOptions& opts) {
                if (opts.need_zone_map) {
                    return writer->write_zone_map();
                }
                return Status::OK();
            }));
    RETURN_IF_ERROR(write_nested_offsets_mapping_index(_nested_group_writers));
    return Status::OK();
}

Status VariantSubcolumnWriter::write_inverted_index() {
    assert(is_finalized());
    if (_opts.need_inverted_index) {
        RETURN_IF_ERROR(_writer->write_inverted_index());
    }
    RETURN_IF_ERROR(for_each_nested_group_writer(
            _nested_group_writers, [](ColumnWriter* writer, const ColumnWriterOptions& opts) {
                if (opts.need_inverted_index) {
                    return writer->write_inverted_index();
                }
                return Status::OK();
            }));
    return Status::OK();
}
Status VariantSubcolumnWriter::write_bloom_filter_index() {
    assert(is_finalized());
    if (_opts.need_bloom_filter) {
        RETURN_IF_ERROR(_writer->write_bloom_filter_index());
    }
    RETURN_IF_ERROR(for_each_nested_group_writer(
            _nested_group_writers, [](ColumnWriter* writer, const ColumnWriterOptions& opts) {
                if (opts.need_bloom_filter) {
                    return writer->write_bloom_filter_index();
                }
                return Status::OK();
            }));
    return Status::OK();
}

Status VariantSubcolumnWriter::append_nullable(const uint8_t* null_map, const uint8_t** ptr,
                                               size_t num_rows) {
    // the root contains the same nullable info
    RETURN_IF_ERROR(append_data(ptr, num_rows));
    return Status::OK();
}

VariantDocCompactWriter::VariantDocCompactWriter(const ColumnWriterOptions& opts,
                                                 const TabletColumn* column,
                                                 std::unique_ptr<Field> field)
        : ColumnWriter(std::move(field), opts.meta->is_nullable(), opts.meta) {
    _opts = opts;
    _tablet_column = column;
    _column = vectorized::ColumnVariant::create(0);
}

Status VariantDocCompactWriter::init() {
    return Status::OK();
}

Status VariantDocCompactWriter::append_data(const uint8_t** ptr, size_t num_rows) {
    const auto* column = reinterpret_cast<const vectorized::VariantColumnData*>(*ptr);
    const auto& src = *reinterpret_cast<const vectorized::ColumnVariant*>(column->column_data);
    auto* dst_ptr = assert_cast<vectorized::ColumnVariant*>(_column.get());
    // TODO: if direct write we could avoid copy
    dst_ptr->insert_range_from(src, column->row_pos, num_rows);
    return Status::OK();
}

Status VariantDocCompactWriter::finish() {
    if (!is_finalized()) {
        RETURN_IF_ERROR(finalize());
    }
    for (auto& column_writer : _subcolumn_writers) {
        RETURN_IF_ERROR(column_writer->finish());
    }
    RETURN_IF_ERROR(_doc_value_column_writer->finish());
    return Status::OK();
}
Status VariantDocCompactWriter::write_data() {
    if (!is_finalized()) {
        RETURN_IF_ERROR(finalize());
    }
    for (auto& column_writer : _subcolumn_writers) {
        RETURN_IF_ERROR(column_writer->write_data());
    }
    RETURN_IF_ERROR(_doc_value_column_writer->write_data());
    return Status::OK();
}
Status VariantDocCompactWriter::write_ordinal_index() {
    assert(is_finalized());
    for (auto& column_writer : _subcolumn_writers) {
        RETURN_IF_ERROR(column_writer->write_ordinal_index());
    }
    RETURN_IF_ERROR(_doc_value_column_writer->write_ordinal_index());
    return Status::OK();
}

Status VariantDocCompactWriter::write_zone_map() {
    assert(is_finalized());
    for (int i = 0; i < _subcolumn_writers.size(); ++i) {
        if (_subcolumn_opts[i].need_zone_map) {
            RETURN_IF_ERROR(_subcolumn_writers[i]->write_zone_map());
        }
    }
    RETURN_IF_ERROR(_doc_value_column_writer->write_zone_map());

    return Status::OK();
}
Status VariantDocCompactWriter::write_inverted_index() {
    assert(is_finalized());
    for (int i = 0; i < _subcolumn_writers.size(); ++i) {
        if (_subcolumn_opts[i].need_inverted_index) {
            RETURN_IF_ERROR(_subcolumn_writers[i]->write_inverted_index());
        }
    }
    RETURN_IF_ERROR(_doc_value_column_writer->write_inverted_index());
    return Status::OK();
}
Status VariantDocCompactWriter::write_bloom_filter_index() {
    assert(is_finalized());
    for (int i = 0; i < _subcolumn_writers.size(); ++i) {
        if (_subcolumn_opts[i].need_bloom_filter) {
            RETURN_IF_ERROR(_subcolumn_writers[i]->write_bloom_filter_index());
        }
    }
    RETURN_IF_ERROR(_doc_value_column_writer->write_bloom_filter_index());
    return Status::OK();
}
Status VariantDocCompactWriter::append_nullable(const uint8_t* null_map, const uint8_t** ptr,
                                                size_t num_rows) {
    RETURN_IF_ERROR(append_data(ptr, num_rows));
    return Status::OK();
}
Status VariantDocCompactWriter::finalize() {
    auto* variant_column = assert_cast<vectorized::ColumnVariant*>(_column.get());

    const auto& parent_column =
            _opts.rowset_ctx->tablet_schema->column_by_uid(_tablet_column->parent_unique_id());

    size_t num_rows = variant_column->size();
    auto converter = std::make_unique<vectorized::OlapBlockDataConvertor>();
    int column_id = 0;
    int64_t variant_doc_materialization_min_rows =
            parent_column.variant_doc_materialization_min_rows();
    if (num_rows >= static_cast<size_t>(variant_doc_materialization_min_rows)) {
        auto subcolumns =
                vectorized::variant_util::materialize_docs_to_subcolumns_map(*variant_column);

        auto generate_column_info = [&](std::string_view path,
                                        const vectorized::ColumnVariant::Subcolumn& subcolumn) {
            const std::string& column_name =
                    parent_column.name_lower_case() + "." + std::string(path);
            const vectorized::DataTypePtr& final_data_type_from_object =
                    subcolumn.get_least_common_type();
            vectorized::PathInData full_path = vectorized::PathInData(column_name);
            // set unique_id and parent_unique_id, will use unique_id to get iterator correct
            auto column = vectorized::variant_util::get_column_by_type(
                    final_data_type_from_object, column_name,
                    vectorized::variant_util::ExtraInfo {
                            .unique_id = -1,
                            .parent_unique_id = parent_column.unique_id(),
                            .path_info = full_path});
            return column;
        };

        _subcolumns_indexes.resize(subcolumns.size());

        for (auto& [path, subcolumn] : subcolumns) {
            const auto& least_common_type = subcolumn.get_least_common_type();
            if (vectorized::variant_util::get_base_type_of_array(least_common_type)
                        ->get_primitive_type() == PrimitiveType::INVALID_TYPE) {
                continue;
            }
            subcolumn.finalize();
            TabletColumn tablet_column;
            TabletSchema::SubColumnInfo sub_column_info;
            vectorized::ColumnPtr current_column = subcolumn.get_finalized_column_ptr()->get_ptr();
            vectorized::DataTypePtr current_type = subcolumn.get_least_common_type();
            if (vectorized::variant_util::generate_sub_column_info(
                        *_opts.rowset_ctx->tablet_schema, parent_column.unique_id(),
                        std::string(path), &sub_column_info)) {
                tablet_column = std::move(sub_column_info.column);
                _subcolumns_indexes[column_id] = std::move(sub_column_info.indexes);
                vectorized::DataTypePtr storage_type =
                        vectorized::DataTypeFactory::instance().create_data_type(tablet_column);
                if (!storage_type->equals(*current_type)) {
                    RETURN_IF_ERROR(vectorized::variant_util::cast_column(
                            {current_column, current_type, ""}, storage_type, &current_column));
                }
                current_type = std::move(storage_type);
            } else {
                tablet_column = generate_column_info(path, subcolumn);
                const auto& indexes =
                        _opts.rowset_ctx->tablet_schema->inverted_indexs(parent_column.unique_id());
                vectorized::variant_util::inherit_index(indexes, _subcolumns_indexes[column_id],
                                                        tablet_column);
            }

            int current_column_id = column_id++;
            int64_t none_null_value_size = subcolumn.get_non_null_value_size();

            ColumnWriterOptions opts;
            opts.meta = _opts.footer->add_columns();
            opts.index_file_writer = _opts.index_file_writer;
            opts.compression_type = _opts.compression_type;
            opts.rowset_ctx = _opts.rowset_ctx;
            opts.file_writer = _opts.file_writer;
            std::unique_ptr<ColumnWriter> writer;
            vectorized::variant_util::inherit_column_attributes(parent_column, tablet_column);

            bool need_record_none_null_value_size = true;

            RETURN_IF_ERROR(_create_column_writer(
                    current_column_id, tablet_column, _opts.rowset_ctx->tablet_schema,
                    _opts.index_file_writer, &writer, _subcolumns_indexes[current_column_id], &opts,
                    none_null_value_size, need_record_none_null_value_size));
            _subcolumn_writers.push_back(std::move(writer));
            _subcolumn_opts.push_back(opts);
            _subcolumn_opts[current_column_id].meta->set_num_rows(num_rows);

            RETURN_IF_ERROR(convert_and_write_column(converter.get(), tablet_column, current_type,
                                                     _subcolumn_writers[current_column_id].get(),
                                                     current_column, num_rows, current_column_id));
        }
    }

    std::string doc_value_column_path = _tablet_column->path_info_ptr()->get_path();
    size_t pos = doc_value_column_path.rfind("b");
    int bucket_value = std::stoi(doc_value_column_path.substr(pos + 1));
    TabletColumn doc_value_column =
            vectorized::variant_util::create_doc_value_column(parent_column, bucket_value);
    _init_column_meta(_opts.meta, column_id, doc_value_column, _opts.compression_type);
    RETURN_IF_ERROR(ColumnWriter::create_map_writer(_opts, &doc_value_column, _opts.file_writer,
                                                    &_doc_value_column_writer));
    RETURN_IF_ERROR(_doc_value_column_writer->init());

    // convert root column data from engine format to storage layer format
    converter->add_column_data_convertor(doc_value_column);
    // Convert MutableColumnPtr to ColumnPtr by creating a shared pointer from the raw pointer
    // The ownership is maintained by _column, so this is safe
    RETURN_IF_ERROR(converter->set_source_content_with_specifid_column(
            {variant_column->get_doc_value_column(), nullptr, ""}, 0, num_rows, column_id));
    auto [status, column] = converter->convert_column_data(column_id);
    RETURN_IF_ERROR(status);
    RETURN_IF_ERROR(
            _doc_value_column_writer->append(column->get_nullmap(), column->get_data(), num_rows));
    converter->clear_source_content(column_id);

    _opts.meta->set_num_rows(num_rows);

    auto [column_key, column_value] = variant_column->get_doc_value_data_paths_and_values();
    const auto& column_offsets = variant_column->serialized_doc_value_column_offsets();
    std::map<StringRef, uint32_t> column_stats;
    for (int64_t i = 0; i < num_rows; ++i) {
        size_t start = column_offsets[i - 1];
        size_t end = column_offsets[i];
        for (size_t j = start; j < end; ++j) {
            const auto& key = column_key->get_data_at(j);
            column_stats[key] += 1;
        }
    }
    auto* stats = _opts.meta->mutable_variant_statistics();
    auto* doc_value_column_non_null_size = stats->mutable_doc_value_column_non_null_size();
    for (const auto& [k, cnt] : column_stats) {
        (*doc_value_column_non_null_size)[k.to_string()] = cnt;
    }
    _is_finalized = true;
    return Status::OK();
}

uint64_t VariantDocCompactWriter::estimate_buffer_size() {
    return _column->byte_size();
}

static void init_nested_group_column_path_info(ColumnPathInfo* pb, int32_t variant_column_unique_id,
                                               const std::string& parent_path,
                                               const std::string& physical_path, bool is_offsets,
                                               size_t depth) {
    vectorized::PathInData storage_path(physical_path);
    pb->clear_path_part_infos();
    storage_path.to_protobuf(pb, variant_column_unique_id);
    pb->set_nested_group_parent_path(parent_path);
    pb->set_nested_group_depth(static_cast<uint32_t>(depth));
    if (is_offsets) {
        pb->set_is_nested_group_offsets(true);
    }
}

static Status write_nested_group_offsets(
        const doris::segment_v2::NestedGroup* group, const std::string& full_path,
        int32_t variant_column_unique_id, const TabletColumn* tablet_column,
        const ColumnWriterOptions& base_opts, vectorized::OlapBlockDataConvertor* converter,
        int& column_id, std::unordered_map<std::string, NestedGroupWriter>& writers, size_t depth) {
    std::string offsets_col_name =
            build_nested_group_offsets_column_name(tablet_column->name_lower_case(), full_path);

    TabletColumn offsets_column;
    offsets_column.set_name(offsets_col_name);
    offsets_column.set_type(FieldType::OLAP_FIELD_TYPE_BIGINT);
    offsets_column.set_is_nullable(false);
    offsets_column.set_length(sizeof(int64_t));
    offsets_column.set_index_length(sizeof(int64_t));

    auto& group_writer = writers[full_path];
    group_writer.offsets_opts = base_opts;
    group_writer.offsets_opts.need_inverted_index = false;
    group_writer.offsets_opts.inverted_indexes.clear();
    group_writer.offsets_opts.index_file_writer = nullptr;
    group_writer.offsets_opts.need_zone_map = true;
    group_writer.offsets_opts.need_bloom_filter = false;
    group_writer.offsets_opts.meta = base_opts.footer->add_columns();
    _init_column_meta(group_writer.offsets_opts.meta, column_id, offsets_column,
                      base_opts.compression_type);

    auto* path_info = group_writer.offsets_opts.meta->mutable_column_path_info();
    init_nested_group_column_path_info(path_info, variant_column_unique_id, full_path,
                                       offsets_col_name,
                                       /*is_offsets=*/true, depth);

    RETURN_IF_ERROR(ColumnWriter::create(group_writer.offsets_opts, &offsets_column,
                                         base_opts.file_writer, &group_writer.offsets_writer));
    RETURN_IF_ERROR(group_writer.offsets_writer->init());

    vectorized::ColumnPtr offsets_col =
            static_cast<const vectorized::IColumn&>(*group->offsets).get_ptr();
    size_t offsets_num_rows = offsets_col->size();
    {
        const auto* src = assert_cast<const vectorized::ColumnOffset64*>(offsets_col.get());
        auto dst = vectorized::ColumnInt64::create();
        auto& dst_data = dst->get_data();
        const auto& src_data = src->get_data();
        dst_data.resize(src_data.size());
        std::transform(src_data.begin(), src_data.end(), dst_data.begin(),
                       [](auto v) { return static_cast<int64_t>(v); });
        offsets_col = std::move(dst);
        group_writer.offsets_mapping_index_writer =
                std::make_shared<NestedOffsetsMappingIndexWriter>();
        RETURN_IF_ERROR(
                group_writer.offsets_mapping_index_writer->build(src_data.data(), src_data.size()));
    }
    converter->add_column_data_convertor(offsets_column);
    RETURN_IF_ERROR(converter->set_source_content_with_specifid_column(
            {offsets_col, nullptr, ""}, 0, offsets_num_rows, column_id));
    auto [status, converted] = converter->convert_column_data(column_id);
    RETURN_IF_ERROR(status);
    RETURN_IF_ERROR(group_writer.offsets_writer->append(converted->get_nullmap(),
                                                        converted->get_data(), offsets_num_rows));
    converter->clear_source_content(column_id);
    group_writer.offsets_opts.meta->set_num_rows(offsets_num_rows);
    ++column_id;
    return Status::OK();
}

static Status write_nested_group_children(
        const doris::segment_v2::NestedGroup* group, const std::string& full_path,
        const std::string& logical_full_path, int32_t variant_column_unique_id,
        const TabletColumn* tablet_column, const ColumnWriterOptions& base_opts,
        vectorized::OlapBlockDataConvertor* converter, int& column_id,
        std::unordered_map<std::string, NestedGroupWriter>& writers, size_t depth) {
    auto& group_writer = writers[full_path];
    const auto& parent_indexes =
            base_opts.rowset_ctx->tablet_schema->inverted_indexs(variant_column_unique_id);
    for (const auto& [relative_path, subcolumn] : group->children) {
        std::string child_col_name = tablet_column->name_lower_case() +
                                     nested_group_marker_token() + full_path + "." +
                                     relative_path.get_path();

        const auto& child_type = subcolumn.get_least_common_type();
        if (vectorized::variant_util::get_base_type_of_array(child_type)->get_primitive_type() ==
            PrimitiveType::INVALID_TYPE) {
            continue;
        }
        assert(child_type->is_nullable());
        // use logical path to construct indexes
        TabletColumn child_column = vectorized::variant_util::get_column_by_type(
                child_type, child_col_name,
                vectorized::variant_util::ExtraInfo {
                        .unique_id = -1,
                        .parent_unique_id = variant_column_unique_id,
                        .path_info = vectorized::PathInData(logical_full_path + "." +
                                                            relative_path.get_path())});

        ColumnWriterOptions child_opts = base_opts;
        child_opts.need_inverted_index = false;
        child_opts.inverted_indexes.clear();
        child_opts.meta = base_opts.footer->add_columns();
        _init_column_meta(child_opts.meta, column_id, child_column, base_opts.compression_type);

        auto* child_path_info = child_opts.meta->mutable_column_path_info();
        init_nested_group_column_path_info(child_path_info, variant_column_unique_id, full_path,
                                           child_col_name,
                                           /*is_offsets=*/false, depth);

        if (segment_v2::IndexColumnWriter::check_support_inverted_index(child_column) &&
            !parent_indexes.empty()) {
            TabletIndexes child_indexes;
            // use logical path as suffix
            if (vectorized::variant_util::inherit_index(parent_indexes, child_indexes,
                                                        child_column)) {
                group_writer.child_inverted_indexes[relative_path.get_path()] = child_indexes;
                child_opts.need_inverted_index = true;
                DCHECK(base_opts.index_file_writer != nullptr);
                child_opts.index_file_writer = base_opts.index_file_writer;
                for (const auto& index :
                     group_writer.child_inverted_indexes[relative_path.get_path()]) {
                    child_opts.inverted_indexes.push_back(index.get());
                }
            }
        }

        std::unique_ptr<ColumnWriter> child_writer;
        RETURN_IF_ERROR(ColumnWriter::create(child_opts, &child_column, base_opts.file_writer,
                                             &child_writer));
        RETURN_IF_ERROR(child_writer->init());

        if (!subcolumn.is_finalized()) {
            const_cast<vectorized::ColumnVariant::Subcolumn&>(subcolumn).finalize();
        }
        auto child_col = subcolumn.get_finalized_column_ptr();
        size_t child_num_rows = child_col->size();

        converter->add_column_data_convertor(child_column);
        RETURN_IF_ERROR(converter->set_source_content_with_specifid_column(
                {child_col, nullptr, ""}, 0, child_num_rows, column_id));
        auto [child_status, child_converted] = converter->convert_column_data(column_id);
        RETURN_IF_ERROR(child_status);
        RETURN_IF_ERROR(child_writer->append(child_converted->get_nullmap(),
                                             child_converted->get_data(), child_num_rows));
        converter->clear_source_content(column_id);
        child_opts.meta->set_num_rows(child_num_rows);

        group_writer.child_writers[relative_path.get_path()] = std::move(child_writer);
        group_writer.child_opts[relative_path.get_path()] = child_opts;
        ++column_id;
    }
    return Status::OK();
}

// recursively write NestedGroup built from JSONB at finalize stage.
static Status write_nested_group_recursive(
        const doris::segment_v2::NestedGroup* group, const std::string& path_prefix,
        const TabletColumn* tablet_column, const ColumnWriterOptions& base_opts,
        vectorized::OlapBlockDataConvertor* converter, int& column_id,
        std::unordered_map<std::string, NestedGroupWriter>& writers, VariantStatistics& statistics,
        size_t depth) {
    if (!group || group->is_disabled) {
        return Status::OK();
    }

    int32_t variant_column_unique_id = tablet_column->parent_unique_id();
    if (variant_column_unique_id < 0) {
        variant_column_unique_id = tablet_column->unique_id();
    }

    std::string full_path = path_prefix.empty() ? group->path.get_path()
                                                : path_prefix + nested_group_marker_token() +
                                                          group->path.get_path();
    const std::string logical_full_path = strip_nested_group_marker(full_path);

    RETURN_IF_ERROR(write_nested_group_offsets(group, full_path, variant_column_unique_id,
                                               tablet_column, base_opts, converter, column_id,
                                               writers, depth));
    RETURN_IF_ERROR(write_nested_group_children(group, full_path, logical_full_path,
                                                variant_column_unique_id, tablet_column, base_opts,
                                                converter, column_id, writers, depth));

    // 3. Recursively write nested groups within this group
    for (const auto& [_, nested_group] : group->nested_groups) {
        RETURN_IF_ERROR(write_nested_group_recursive(nested_group.get(), full_path, tablet_column,
                                                     base_opts, converter, column_id, writers,
                                                     statistics, depth + 1));
    }

    statistics.nested_group_info[full_path];

    return Status::OK();
}

static Status write_nested_groups_to_storage(
        const doris::segment_v2::NestedGroupsMap& nested_groups, const TabletColumn* tablet_column,
        const ColumnWriterOptions& opts, vectorized::OlapBlockDataConvertor* converter,
        size_t num_rows, int& column_id,
        std::unordered_map<std::string, NestedGroupWriter>& writers,
        VariantStatistics& statistics) {
    if (nested_groups.empty()) {
        return Status::OK();
    }

    std::vector<std::shared_ptr<doris::segment_v2::NestedGroup>> groups;
    groups.reserve(nested_groups.size());
    for (const auto& [_, g] : nested_groups) {
        if (g) {
            groups.push_back(g);
        }
    }
    std::sort(groups.begin(), groups.end(),
              [](const auto& a, const auto& b) { return a->path.get_path() < b->path.get_path(); });
    for (const auto& g : groups) {
        RETURN_IF_ERROR(write_nested_group_recursive(g.get(), "", tablet_column, opts, converter,
                                                     column_id, writers, statistics, 1));
    }
    return Status::OK();
}

#include "common/compile_check_end.h"

} // namespace doris::segment_v2
