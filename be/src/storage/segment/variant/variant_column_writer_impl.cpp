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
#include "storage/segment/variant/variant_column_writer_impl.h"

#include <gen_cpp/segment_v2.pb.h>

#include <algorithm>
#include <charconv>
#include <memory>
#include <span>
#include <string_view>

#include "common/cast_set.h"
#include "common/status.h"
#include "core/column/column.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_factory.hpp"
#include "exec/common/variant_util.h"
#include "storage/iterator/olap_data_convertor.h"
#include "storage/olap_common.h"
#include "storage/olap_define.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/segment/column_writer.h"
#include "storage/segment/encoding_info.h"
#include "storage/segment/variant/variant_shredder.h"
#include "storage/segment/variant/variant_writer_helpers.h"
#include "storage/tablet/tablet_schema.h"
#include "storage/types.h"
#include "util/json/path_in_data.h"
#include "util/slice.h"

namespace doris::segment_v2 {

void _init_column_meta(ColumnMetaPB* meta, uint32_t column_id, const TabletColumn& column,
                       const ColumnWriterOptions& opts) {
    meta->Clear();
    meta->set_column_id(column_id);
    meta->set_type(int(column.type()));
    meta->set_length(column.length());
    meta->set_encoding(EncodingInfo::resolve_default_encoding(opts.storage_format, column));
    meta->set_compression(opts.compression_type);
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
        _init_column_meta(meta->add_children_columns(), column_id, column.get_sub_column(i), opts);
    }
    if (column.is_variant_type()) {
        meta->set_variant_max_subcolumns_count(column.variant_max_subcolumns_count());
        meta->set_variant_enable_doc_mode(column.variant_enable_doc_mode());
    }
}

namespace {
Status finish_and_write_column_writer(ColumnWriter* writer) {
    RETURN_IF_ERROR(writer->finish());
    RETURN_IF_ERROR(writer->write_data());
    return Status::OK();
}

} // namespace

Status UnifiedSparseColumnWriter::init(const TabletColumn* parent_column, int bucket_num,
                                       int& column_id, const ColumnWriterOptions& base_opts,
                                       SegmentFooterPB* footer) {
    _bucket_num = std::max(1, bucket_num);
    if (_bucket_num <= 1) {
        TabletColumn sparse_column = variant_util::create_sparse_column(*parent_column);
        RETURN_IF_ERROR(init_single(sparse_column, column_id, base_opts, footer));
    } else {
        RETURN_IF_ERROR(init_buckets(_bucket_num, *parent_column, column_id, base_opts, footer));
    }
    return Status::OK();
}

Status UnifiedSparseColumnWriter::append_shredded(const TabletColumn* parent_column,
                                                  const VariantShreddedColumns& shredded,
                                                  size_t num_rows,
                                                  OlapBlockDataConvertor* converter) {
    if (shredded.sparse_buckets.size() != cast_set<size_t>(_bucket_num)) {
        return Status::InvalidArgument("Variant shredder produced {} sparse buckets, expected {}",
                                       shredded.sparse_buckets.size(), _bucket_num);
    }
    converter->resize(_first_column_id + _bucket_num);
    for (int bucket = 0; bucket < _bucket_num; ++bucket) {
        const auto& source = shredded.sparse_buckets[bucket];
        if (source.column->size() != num_rows) {
            return Status::InvalidArgument("Variant sparse bucket {} has {} rows, expected {}",
                                           bucket, source.column->size(), num_rows);
        }
        TabletColumn bucket_column =
                _bucket_num == 1 ? variant_util::create_sparse_column(*parent_column)
                                 : variant_util::create_sparse_shard_column(*parent_column, bucket);
        const int column_id = _first_column_id + bucket;
        if (num_rows > 0) {
            converter->add_column_data_convertor_at(bucket_column, column_id);
            RETURN_IF_ERROR(converter->set_source_content_with_specifid_column(
                    {source.column, shredded.sparse_type, ""}, 0, num_rows, column_id));
            auto [status, converted] = converter->convert_column_data(column_id);
            RETURN_IF_ERROR(status);
            ColumnWriter* writer =
                    _bucket_num == 1 ? _single_writer.get() : _bucket_writers[bucket].get();
            RETURN_IF_ERROR(
                    writer->append(converted->get_nullmap(), converted->get_data(), num_rows));
            converter->clear_source_content(column_id);
        }

        ColumnWriterOptions& opts = _bucket_num == 1 ? _single_opts : _bucket_opts[bucket];
        source.statistics.to_pb(opts.meta->mutable_variant_statistics());
        opts.meta->set_num_rows(num_rows);
    }
    return Status::OK();
}

// UnifiedSparseColumnWriter implementation
Status UnifiedSparseColumnWriter::init_single(const TabletColumn& sparse_column, int& column_id,
                                              const ColumnWriterOptions& base_opts,
                                              SegmentFooterPB* footer) {
    _single_opts = base_opts;
    _single_opts.meta = footer->add_columns();
    _init_column_meta(_single_opts.meta, column_id, sparse_column, base_opts);
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
        TabletColumn bucket_col = variant_util::create_sparse_shard_column(parent_column, b);
        _bucket_opts[b] = base_opts;
        _bucket_opts[b].meta = footer->add_columns();
        _init_column_meta(_bucket_opts[b].meta, column_id, bucket_col, base_opts);
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

Status UnifiedSparseColumnWriter::write_zone_map() {
    return Status::OK();
}

Status UnifiedSparseColumnWriter::write_inverted_index() {
    return Status::OK();
}

Status UnifiedSparseColumnWriter::write_bloom_filter_index() {
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
                variant_util::create_doc_value_column(*parent_column, b);
        _doc_value_column_opts[b] = opts;
        _doc_value_column_opts[b].meta = footer->add_columns();
        _init_column_meta(_doc_value_column_opts[b].meta, column_id, bucket_column, opts);
        RETURN_IF_ERROR(ColumnWriter::create_map_writer(_doc_value_column_opts[b], &bucket_column,
                                                        opts.file_writer,
                                                        &_doc_value_column_writers[b]));
        RETURN_IF_ERROR(_doc_value_column_writers[b]->init());
        ++column_id;
    }
    return Status::OK();
}

Status VariantDocWriter::append_shredded(const TabletColumn* parent_column,
                                         const VariantShreddedColumns& shredded, size_t num_rows,
                                         OlapBlockDataConvertor* converter) {
    if (shredded.doc_buckets.size() != cast_set<size_t>(_bucket_num)) {
        return Status::InvalidArgument("Variant shredder produced {} doc buckets, expected {}",
                                       shredded.doc_buckets.size(), _bucket_num);
    }
    converter->resize(_first_column_id + _bucket_num);
    for (int bucket = 0; bucket < _bucket_num; ++bucket) {
        const auto& source = shredded.doc_buckets[bucket];
        if (source.column->size() != num_rows) {
            return Status::InvalidArgument("Variant doc bucket {} has {} rows, expected {}", bucket,
                                           source.column->size(), num_rows);
        }
        TabletColumn bucket_column = variant_util::create_doc_value_column(*parent_column, bucket);
        const int column_id = _first_column_id + bucket;
        if (num_rows > 0) {
            converter->add_column_data_convertor_at(bucket_column, column_id);
            RETURN_IF_ERROR(converter->set_source_content_with_specifid_column(
                    {source.column, shredded.doc_type, ""}, 0, num_rows, column_id));
            auto [status, converted] = converter->convert_column_data(column_id);
            RETURN_IF_ERROR(status);
            RETURN_IF_ERROR(_doc_value_column_writers[bucket]->append(
                    converted->get_nullmap(), converted->get_data(), num_rows));
            converter->clear_source_content(column_id);
        }

        source.statistics.to_pb(_doc_value_column_opts[bucket].meta->mutable_variant_statistics());
        _doc_value_column_opts[bucket].meta->set_num_rows(num_rows);
    }
    return Status::OK();
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

Status VariantDocWriter::write_zone_map() {
    for (int i = 0; i < _doc_value_column_writers.size(); ++i) {
        if (_doc_value_column_opts[i].need_zone_map) {
            RETURN_IF_ERROR(_doc_value_column_writers[i]->write_zone_map());
        }
    }
    return Status::OK();
}

Status VariantDocWriter::write_inverted_index() {
    for (int i = 0; i < _doc_value_column_writers.size(); ++i) {
        if (_doc_value_column_opts[i].need_inverted_index) {
            RETURN_IF_ERROR(_doc_value_column_writers[i]->write_inverted_index());
        }
    }
    return Status::OK();
}

Status VariantDocWriter::write_bloom_filter_index() {
    for (int i = 0; i < _doc_value_column_writers.size(); ++i) {
        if (_doc_value_column_opts[i].need_bloom_filter) {
            RETURN_IF_ERROR(_doc_value_column_writers[i]->write_bloom_filter_index());
        }
    }
    return Status::OK();
}

VariantColumnWriterImpl::~VariantColumnWriterImpl() = default;

VariantColumnWriterImpl::VariantColumnWriterImpl(const ColumnWriterOptions& opts,
                                                 const TabletColumn* column) {
    _opts = opts;
    _tablet_column = column;
    _null_column = ColumnUInt8::create();
}

Status VariantColumnWriterImpl::init() {
    if (_tablet_column->variant_enable_nested_group()) {
        return Status::NotSupported(
                "Variant V2 storage writer does not support nested-group layout");
    }
    DCHECK(_tablet_column->variant_max_subcolumns_count() >= 0)
            << "max subcolumns count is: " << _tablet_column->variant_max_subcolumns_count();
    _v2_column = ColumnVariantV2::create();
    return Status::OK();
}

bool VariantColumnWriterImpl::_has_extracted_variant_columns() const {
    const int current_variant_uid = _tablet_column->unique_id();
    return std::ranges::any_of(_opts.rowset_ctx->tablet_schema->columns(),
                               [current_variant_uid](const auto& column) {
                                   return column->is_extracted_column() &&
                                          column->parent_unique_id() == current_variant_uid;
                               });
}

Status VariantColumnWriterImpl::_write_v2_root(const VariantShreddedColumns& shredded,
                                               size_t num_rows, int& column_id) {
    if (!shredded.root_jsonb || shredded.root_jsonb->size() != num_rows) {
        return Status::InvalidArgument("Variant shredder root has {} rows, expected {}",
                                       !shredded.root_jsonb ? 0 : shredded.root_jsonb->size(),
                                       num_rows);
    }
    _root_writer = std::make_unique<ScalarColumnWriter>(
            _opts, std::make_shared<TabletColumn>(*_tablet_column), _opts.file_writer);
    RETURN_IF_ERROR(_root_writer->init());

    const auto& nullable = assert_cast<const ColumnNullable&>(*shredded.root_jsonb);
    const auto& values = assert_cast<const ColumnString&>(nullable.get_nested_column());
    DorisVector<Slice> slices(num_rows);
    for (size_t row = 0; row < num_rows; ++row) {
        slices[row] = values.get_data_at(row).to_slice();
    }
    const uint8_t* outer_nulls =
            _tablet_column->is_nullable() ? _null_column->get_data().data() : nullptr;
    if (num_rows > 0) {
        RETURN_IF_ERROR(_root_writer->append(outer_nulls, slices.data(), num_rows));
    }
    _opts.meta->set_num_rows(num_rows);
    ++column_id;
    return Status::OK();
}

Status VariantColumnWriterImpl::_write_v2_materialized(const VariantShreddedColumns& shredded,
                                                       OlapBlockDataConvertor* converter,
                                                       size_t num_rows, int& column_id) {
    for (const VariantPathColumn& path_column : shredded.materialized) {
        if (!path_column.column || path_column.column->size() != num_rows) {
            return Status::InvalidArgument("Variant materialized path {} has {} rows, expected {}",
                                           path_column.path.get_path(),
                                           !path_column.column ? 0 : path_column.column->size(),
                                           num_rows);
        }
        TabletIndexes indexes;
        ColumnWriterOptions opts;
        std::unique_ptr<ColumnWriter> writer;
        TabletColumn tablet_column;
        const int current_column_id = column_id++;
        RETURN_IF_ERROR(variant_writer_helpers::prepare_subcolumn_writer_target(
                _opts, *_tablet_column, current_column_id, path_column.path, path_column.type,
                path_column.non_null_rows, num_rows, nullptr, true, &indexes, &opts, &writer,
                &tablet_column));
        RETURN_IF_ERROR(variant_writer_helpers::convert_and_write_column(
                converter, tablet_column, path_column.type, writer.get(), path_column.column,
                num_rows, current_column_id));
        _subcolumns_indexes.push_back(std::move(indexes));
        _subcolumn_opts.push_back(opts);
        _subcolumn_writers.push_back(std::move(writer));
    }
    return Status::OK();
}

Status VariantColumnWriterImpl::_write_v2_binary(const VariantShreddedColumns& shredded,
                                                 OlapBlockDataConvertor* converter, size_t num_rows,
                                                 int& column_id) {
    if (_tablet_column->variant_enable_doc_mode()) {
        auto writer = std::make_unique<VariantDocWriter>();
        const int bucket_count = std::max(1, _tablet_column->variant_doc_hash_shard_count());
        RETURN_IF_ERROR(writer->init(_tablet_column, bucket_count, column_id, _opts, _opts.footer));
        RETURN_IF_ERROR(writer->append_shredded(_tablet_column, shredded, num_rows, converter));
        _binary_writer = std::move(writer);
        return Status::OK();
    }

    auto writer = std::make_unique<UnifiedSparseColumnWriter>();
    const int bucket_count = std::max(1, _tablet_column->variant_sparse_hash_shard_count());
    RETURN_IF_ERROR(writer->init(_tablet_column, bucket_count, column_id, _opts, _opts.footer));
    RETURN_IF_ERROR(writer->append_shredded(_tablet_column, shredded, num_rows, converter));
    _binary_writer = std::move(writer);
    return Status::OK();
}

Status VariantColumnWriterImpl::finalize() {
    if (_is_finalized) {
        return Status::OK();
    }
    DORIS_CHECK(static_cast<bool>(_v2_column));
    const size_t num_rows = _v2_column->size();
    DORIS_CHECK_EQ(_null_column->size(), num_rows);
    _v2_column->ensure_encoded();

    VariantShredder shredder(
            {.tablet_schema = _opts.rowset_ctx->tablet_schema.get(),
             .parent_column_unique_id = _tablet_column->unique_id(),
             .physical_layout = _tablet_column->variant_enable_doc_mode()
                                        ? VariantShredderPhysicalLayout::DOC
                                        : VariantShredderPhysicalLayout::ORDINARY,
             .max_subcolumns_count =
                     cast_set<size_t>(_tablet_column->variant_max_subcolumns_count()),
             .typed_paths_to_sparse = _tablet_column->variant_enable_typed_paths_to_sparse(),
             .sparse_bucket_count = cast_set<uint32_t>(
                     std::max(1, _tablet_column->variant_sparse_hash_shard_count())),
             .max_sparse_column_statistics_size =
                     cast_set<size_t>(_tablet_column->variant_max_sparse_column_statistics_size()),
             .doc_bucket_count = cast_set<uint32_t>(
                     std::max(1, _tablet_column->variant_doc_hash_shard_count())),
             .doc_materialization_min_rows = cast_set<size_t>(std::max<int64_t>(
                     0, _tablet_column->variant_doc_materialization_min_rows()))});
    if (num_rows > 0) {
        RETURN_IF_ERROR(shredder.append(
                _v2_column->read_view(), 0, num_rows,
                std::span<const uint8_t>(_null_column->get_data().data(), num_rows)));
    }
    VariantShreddedColumns shredded;
    RETURN_IF_ERROR(shredder.finish(&shredded));

    auto olap_data_convertor = std::make_unique<OlapBlockDataConvertor>();
    int column_id = 0;
    RETURN_IF_ERROR(_write_v2_root(shredded, num_rows, column_id));
    // Reserve converter id 0 for the physical root writer. Materialized and binary writers append
    // convertors in their persisted column-id order.
    olap_data_convertor->add_column_data_convertor(*_tablet_column);

    if (!_has_extracted_variant_columns()) {
        if (_tablet_column->variant_enable_doc_mode()) {
            RETURN_IF_ERROR(
                    _write_v2_binary(shredded, olap_data_convertor.get(), num_rows, column_id));
            RETURN_IF_ERROR(_write_v2_materialized(shredded, olap_data_convertor.get(), num_rows,
                                                   column_id));
        } else {
            RETURN_IF_ERROR(_write_v2_materialized(shredded, olap_data_convertor.get(), num_rows,
                                                   column_id));
            RETURN_IF_ERROR(
                    _write_v2_binary(shredded, olap_data_convertor.get(), num_rows, column_id));
        }
    }
    shredded.statistics.to_pb(_opts.meta->mutable_variant_statistics());

    _is_finalized = true;
    return Status::OK();
}

bool VariantColumnWriterImpl::is_finalized() const {
    return _is_finalized;
}

Status VariantColumnWriterImpl::_for_each_column_writer(
        const std::function<Status(ColumnWriter*)>& func) {
    RETURN_IF_ERROR(func(_root_writer.get()));
    for (auto& writer : _subcolumn_writers) {
        RETURN_IF_ERROR(func(writer.get()));
    }
    return Status::OK();
}

Status VariantColumnWriterImpl::_ensure_materialized_variant_finalized() {
    if (is_finalized()) {
        return Status::OK();
    }
    return finalize();
}

void VariantColumnWriterImpl::_assert_ready_for_index_writes() const {
    assert(is_finalized());
}

Status VariantColumnWriterImpl::append_data(const uint8_t** ptr, size_t num_rows) {
    const auto* column = reinterpret_cast<const VariantColumnData*>(*ptr);
    if (column == nullptr || column->column_data == nullptr) {
        return Status::InvalidArgument("Variant V2 writer received null column data");
    }
    DCHECK(!is_finalized());
    _v2_column->insert_range_from(*column->column_data, column->row_pos, num_rows);
    _null_column->insert_many_defaults(num_rows);
    return Status::OK();
}

uint64_t VariantColumnWriterImpl::estimate_buffer_size() {
    if (!is_finalized()) {
        return _v2_column->byte_size() + _null_column->byte_size();
    }
    uint64_t size = 0;
    size += _root_writer->estimate_buffer_size();
    for (auto& column_writer : _subcolumn_writers) {
        size += column_writer->estimate_buffer_size();
    }
    if (_binary_writer) {
        size += _binary_writer->estimate_buffer_size();
    }
    return size;
}

Status VariantColumnWriterImpl::finish() {
    RETURN_IF_ERROR(_ensure_materialized_variant_finalized());
    RETURN_IF_ERROR(_for_each_column_writer([](ColumnWriter* writer) { return writer->finish(); }));
    if (_binary_writer) {
        RETURN_IF_ERROR(_binary_writer->finish());
    }
    return Status::OK();
}
Status VariantColumnWriterImpl::write_data() {
    RETURN_IF_ERROR(_ensure_materialized_variant_finalized());
    RETURN_IF_ERROR(
            _for_each_column_writer([](ColumnWriter* writer) { return writer->write_data(); }));
    if (_binary_writer) {
        RETURN_IF_ERROR(_binary_writer->write_data());
    }
    return Status::OK();
}
Status VariantColumnWriterImpl::write_ordinal_index() {
    _assert_ready_for_index_writes();
    RETURN_IF_ERROR(_for_each_column_writer(
            [](ColumnWriter* writer) { return writer->write_ordinal_index(); }));
    if (_binary_writer) {
        RETURN_IF_ERROR(_binary_writer->write_ordinal_index());
    }
    return Status::OK();
}

Status VariantColumnWriterImpl::write_zone_map() {
    _assert_ready_for_index_writes();
    for (size_t i = 0; i < _subcolumn_writers.size(); ++i) {
        if (_subcolumn_opts[i].need_zone_map) {
            RETURN_IF_ERROR(_subcolumn_writers[i]->write_zone_map());
        }
    }
    if (_binary_writer) {
        RETURN_IF_ERROR(_binary_writer->write_zone_map());
    }
    return Status::OK();
}

Status VariantColumnWriterImpl::write_inverted_index() {
    _assert_ready_for_index_writes();
    for (size_t i = 0; i < _subcolumn_writers.size(); ++i) {
        if (_subcolumn_opts[i].need_inverted_index) {
            RETURN_IF_ERROR(_subcolumn_writers[i]->write_inverted_index());
        }
    }
    if (_binary_writer) {
        RETURN_IF_ERROR(_binary_writer->write_inverted_index());
    }
    return Status::OK();
}
Status VariantColumnWriterImpl::write_bloom_filter_index() {
    _assert_ready_for_index_writes();
    for (size_t i = 0; i < _subcolumn_writers.size(); ++i) {
        if (_subcolumn_opts[i].need_bloom_filter) {
            RETURN_IF_ERROR(_subcolumn_writers[i]->write_bloom_filter_index());
        }
    }
    if (_binary_writer) {
        RETURN_IF_ERROR(_binary_writer->write_bloom_filter_index());
    }
    return Status::OK();
}

Status VariantColumnWriterImpl::append_nullable(const uint8_t* null_map, const uint8_t** ptr,
                                                size_t num_rows) {
    const auto* column = reinterpret_cast<const VariantColumnData*>(*ptr);
    if (column == nullptr || column->column_data == nullptr) {
        return Status::InvalidArgument("Variant V2 writer received null column data");
    }
    DCHECK(!is_finalized());
    _v2_column->insert_range_from(*column->column_data, column->row_pos, num_rows);
    if (null_map == nullptr) {
        _null_column->insert_many_defaults(num_rows);
    } else {
        _null_column->insert_many_raw_data(reinterpret_cast<const char*>(null_map), num_rows);
    }
    return Status::OK();
}

VariantSubcolumnWriter::VariantSubcolumnWriter(const ColumnWriterOptions& opts,
                                               TabletColumnPtr column)
        : ColumnWriter(std::move(column), opts.meta->is_nullable(), opts.meta) {
    _opts = opts;
    _column = ColumnVariantV2::create();
    _null_column = ColumnUInt8::create();
}

Status VariantSubcolumnWriter::init() {
    const auto& parent_column =
            _opts.rowset_ctx->tablet_schema->column_by_uid(get_column()->parent_unique_id());
    if (parent_column.variant_enable_nested_group()) {
        return Status::NotSupported(
                "Variant V2 subcolumn writer does not support nested-group layout");
    }
    return Status::OK();
}

Status VariantSubcolumnWriter::append_data(const uint8_t** ptr, size_t num_rows) {
    const auto* column = reinterpret_cast<const VariantColumnData*>(*ptr);
    if (column == nullptr || column->column_data == nullptr) {
        return Status::InvalidArgument("Variant V2 subcolumn writer received null column data");
    }
    _column->insert_range_from(*column->column_data, column->row_pos, num_rows);
    _null_column->insert_many_defaults(num_rows);
    _next_rowid += num_rows;
    return Status::OK();
}

uint64_t VariantSubcolumnWriter::estimate_buffer_size() {
    if (!is_finalized()) {
        return _column->byte_size() + _null_column->byte_size();
    }
    return _writer ? _writer->estimate_buffer_size() : 0;
}

bool VariantSubcolumnWriter::is_finalized() const {
    return _is_finalized;
}

Status VariantSubcolumnWriter::finalize() {
    const auto& parent_column =
            _opts.rowset_ctx->tablet_schema->column_by_uid(get_column()->parent_unique_id());
    DORIS_CHECK(!parent_column.variant_enable_nested_group());
    const size_t num_rows = _column->size();
    DORIS_CHECK_EQ(_null_column->size(), num_rows);
    _column->ensure_encoded();

    const PathInData relative_path = get_column()->path_info_ptr()->copy_pop_front();
    VariantShredder shredder({.tablet_schema = _opts.rowset_ctx->tablet_schema.get(),
                              .parent_column_unique_id = parent_column.unique_id(),
                              .logical_root_path = relative_path,
                              .max_subcolumns_count = 0,
                              .sparse_bucket_count = 1});
    if (num_rows > 0) {
        RETURN_IF_ERROR(shredder.append(
                _column->read_view(), 0, num_rows,
                std::span<const uint8_t>(_null_column->get_data().data(), num_rows)));
    }
    VariantShreddedColumns shredded;
    RETURN_IF_ERROR(shredder.finish(&shredded));

    const std::string& relative_path_string = relative_path.get_path();
    const auto selected = std::ranges::find_if(
            shredded.materialized, [&relative_path_string](const VariantPathColumn& candidate) {
                return candidate.path.get_path().empty() ||
                       candidate.path.get_path() == relative_path_string;
            });

    DataTypePtr flush_type;
    ColumnPtr flush_values;
    int64_t non_null_value_size = 0;
    if (selected == shredded.materialized.end()) {
        flush_type = DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_TINYINT,
                                                                  true /* is_nullable */);
        MutableColumnPtr defaults = flush_type->create_column();
        defaults->insert_many_defaults(num_rows);
        flush_values = std::move(defaults);
    } else {
        flush_type = selected->type;
        flush_values = selected->column;
        non_null_value_size = selected->non_null_rows;
    }

    TabletColumn flush_column = variant_util::get_column_by_type(
            flush_type, get_column()->name(),
            variant_util::ExtraInfo {.unique_id = -1,
                                     .parent_unique_id = get_column()->parent_unique_id(),
                                     .path_info = *get_column()->path_info_ptr()});

    bool need_record_none_null_value_size = (!flush_column.path_info_ptr()->get_is_typed()) &&
                                            !flush_column.path_info_ptr()->has_nested_part();
    ColumnWriterOptions opts = _opts;

    // refresh opts and get writer with flush column
    variant_util::inherit_column_attributes(parent_column, flush_column);
    RETURN_IF_ERROR(variant_writer_helpers::create_column_writer(
            0, flush_column, _opts.rowset_ctx->tablet_schema, _opts.index_file_writer, &_writer,
            _indexes, &opts, non_null_value_size, need_record_none_null_value_size));

    _opts = opts;
    if (num_rows > 0) {
        auto olap_data_convertor = std::make_unique<OlapBlockDataConvertor>();
        RETURN_IF_ERROR(variant_writer_helpers::convert_and_write_column(
                olap_data_convertor.get(), flush_column, flush_type, _writer.get(), flush_values,
                num_rows, 0));
    }
    _opts.meta->set_num_rows(num_rows);
    none_null_size = cast_set<size_t>(non_null_value_size);

    _is_finalized = true;
    return Status::OK();
}

Status VariantSubcolumnWriter::finish() {
    if (!is_finalized()) {
        RETURN_IF_ERROR(finalize());
    }
    return _writer->finish();
}
Status VariantSubcolumnWriter::write_data() {
    if (!is_finalized()) {
        RETURN_IF_ERROR(finalize());
    }
    return _writer->write_data();
}
Status VariantSubcolumnWriter::write_ordinal_index() {
    assert(is_finalized());
    return _writer->write_ordinal_index();
}

Status VariantSubcolumnWriter::write_zone_map() {
    assert(is_finalized());
    if (_opts.need_zone_map) {
        return _writer->write_zone_map();
    }
    return Status::OK();
}

Status VariantSubcolumnWriter::write_inverted_index() {
    assert(is_finalized());
    if (_opts.need_inverted_index) {
        return _writer->write_inverted_index();
    }
    return Status::OK();
}
Status VariantSubcolumnWriter::write_bloom_filter_index() {
    assert(is_finalized());
    if (_opts.need_bloom_filter) {
        return _writer->write_bloom_filter_index();
    }
    return Status::OK();
}

Status VariantSubcolumnWriter::append_nullable(const uint8_t* null_map, const uint8_t** ptr,
                                               size_t num_rows) {
    const auto* column = reinterpret_cast<const VariantColumnData*>(*ptr);
    if (column == nullptr || column->column_data == nullptr) {
        return Status::InvalidArgument("Variant V2 subcolumn writer received null column data");
    }
    _column->insert_range_from(*column->column_data, column->row_pos, num_rows);
    if (null_map == nullptr) {
        _null_column->insert_many_defaults(num_rows);
    } else {
        _null_column->insert_many_raw_data(reinterpret_cast<const char*>(null_map), num_rows);
    }
    _next_rowid += num_rows;
    return Status::OK();
}

VariantDocCompactWriter::VariantDocCompactWriter(const ColumnWriterOptions& opts,
                                                 TabletColumnPtr column)
        : ColumnWriter(std::move(column), opts.meta->is_nullable(), opts.meta) {
    _opts = opts;
    _column = ColumnVariantV2::create();
    _null_column = ColumnUInt8::create();
}

Status VariantDocCompactWriter::init() {
    const auto& parent_column =
            _opts.rowset_ctx->tablet_schema->column_by_uid(get_column()->parent_unique_id());
    if (parent_column.variant_enable_nested_group()) {
        return Status::NotSupported(
                "Variant V2 doc compact writer does not support nested-group layout");
    }
    return Status::OK();
}

Status VariantDocCompactWriter::append_data(const uint8_t** ptr, size_t num_rows) {
    const auto* column = reinterpret_cast<const VariantColumnData*>(*ptr);
    if (column == nullptr || column->column_data == nullptr) {
        return Status::InvalidArgument("Variant V2 doc compact writer received null column data");
    }
    _column->insert_range_from(*column->column_data, column->row_pos, num_rows);
    _null_column->insert_many_defaults(num_rows);
    _next_rowid += num_rows;
    return Status::OK();
}

Status VariantDocCompactWriter::finish() {
    if (!is_finalized()) {
        RETURN_IF_ERROR(finalize());
    }
    if (_data_written) {
        return Status::OK();
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
    if (_data_written) {
        return Status::OK();
    }
    for (auto& column_writer : _subcolumn_writers) {
        RETURN_IF_ERROR(column_writer->write_data());
    }
    RETURN_IF_ERROR(_doc_value_column_writer->write_data());
    _data_written = true;
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
    const auto* column = reinterpret_cast<const VariantColumnData*>(*ptr);
    if (column == nullptr || column->column_data == nullptr) {
        return Status::InvalidArgument("Variant V2 doc compact writer received null column data");
    }
    _column->insert_range_from(*column->column_data, column->row_pos, num_rows);
    if (null_map == nullptr) {
        _null_column->insert_many_defaults(num_rows);
    } else {
        _null_column->insert_many_raw_data(reinterpret_cast<const char*>(null_map), num_rows);
    }
    _next_rowid += num_rows;
    return Status::OK();
}

Status VariantDocCompactWriter::_write_materialized_subcolumns(
        const TabletColumn& parent_column, const VariantShreddedColumns& shredded,
        OlapBlockDataConvertor* converter, size_t num_rows, int& column_id) {
    for (const VariantPathColumn& path_column : shredded.materialized) {
        if (!path_column.column || path_column.column->size() != num_rows) {
            return Status::InvalidArgument(
                    "Variant doc compact materialized path {} has {} rows, expected {}",
                    path_column.path.get_path(),
                    !path_column.column ? 0 : path_column.column->size(), num_rows);
        }
        TabletIndexes indexes;
        ColumnWriterOptions opts;
        std::unique_ptr<ColumnWriter> writer;
        TabletColumn tablet_column;
        const int current_column_id = column_id++;
        RETURN_IF_ERROR(variant_writer_helpers::prepare_subcolumn_writer_target(
                _opts, parent_column, current_column_id, path_column.path, path_column.type,
                path_column.non_null_rows, num_rows, nullptr, true, &indexes, &opts, &writer,
                &tablet_column));
        RETURN_IF_ERROR(variant_writer_helpers::convert_and_write_column(
                converter, tablet_column, path_column.type, writer.get(), path_column.column,
                num_rows, current_column_id));
        RETURN_IF_ERROR(finish_and_write_column_writer(writer.get()));
        _subcolumns_indexes.push_back(std::move(indexes));
        _subcolumn_opts.push_back(opts);
        _subcolumn_writers.push_back(std::move(writer));
    }
    return Status::OK();
}

Status VariantDocCompactWriter::_write_doc_value_column(const TabletColumn& parent_column,
                                                        const VariantShreddedColumns& shredded,
                                                        OlapBlockDataConvertor* converter,
                                                        int column_id, size_t num_rows) {
    const std::string doc_value_column_path = get_column()->path_info_ptr()->get_path();
    const std::string marker = "." + DOC_VALUE_COLUMN_PATH + ".b";
    const size_t marker_pos = doc_value_column_path.rfind(marker);
    if (marker_pos == std::string::npos) {
        return Status::Corruption("Invalid Variant doc compact path {}", doc_value_column_path);
    }
    const std::string_view suffix(doc_value_column_path.data() + marker_pos + marker.size(),
                                  doc_value_column_path.size() - marker_pos - marker.size());
    int bucket_value = -1;
    const auto [end, error] =
            std::from_chars(suffix.data(), suffix.data() + suffix.size(), bucket_value);
    if (error != std::errc() || end != suffix.data() + suffix.size() || bucket_value < 0 ||
        cast_set<size_t>(bucket_value) >= shredded.doc_buckets.size()) {
        return Status::Corruption("Invalid Variant doc compact bucket in path {}",
                                  doc_value_column_path);
    }

    for (size_t bucket = 0; bucket < shredded.doc_buckets.size(); ++bucket) {
        const auto& source = shredded.doc_buckets[bucket];
        const auto* map = check_and_get_column<ColumnMap>(source.column.get());
        if (map == nullptr || map->size() != num_rows) {
            return Status::Corruption("Variant doc compact bucket {} is not a {}-row map", bucket,
                                      num_rows);
        }
        const bool has_values =
                !map->get_offsets().empty() && map->get_offsets().back() != map->get_offsets()[-1];
        if (bucket != cast_set<size_t>(bucket_value) &&
            (has_values || !source.statistics.doc_value_column_non_null_size.empty())) {
            return Status::InvalidArgument(
                    "Variant doc compact input for bucket {} contains data for bucket {}",
                    bucket_value, bucket);
        }
    }

    const auto& source = shredded.doc_buckets[bucket_value];
    TabletColumn doc_value_column =
            variant_util::create_doc_value_column(parent_column, bucket_value);
    _init_column_meta(_opts.meta, column_id, doc_value_column, _opts);
    RETURN_IF_ERROR(ColumnWriter::create_map_writer(_opts, &doc_value_column, _opts.file_writer,
                                                    &_doc_value_column_writer));
    RETURN_IF_ERROR(_doc_value_column_writer->init());

    if (num_rows > 0) {
        converter->add_column_data_convertor(doc_value_column);
        RETURN_IF_ERROR(converter->set_source_content_with_specifid_column(
                {source.column, shredded.doc_type, ""}, 0, num_rows, column_id));
        auto [status, column] = converter->convert_column_data(column_id);
        RETURN_IF_ERROR(status);
        RETURN_IF_ERROR(_doc_value_column_writer->append(column->get_nullmap(), column->get_data(),
                                                         num_rows));
        converter->clear_source_content(column_id);
    }
    source.statistics.to_pb(_opts.meta->mutable_variant_statistics());
    _opts.meta->set_num_rows(num_rows);
    return Status::OK();
}
Status VariantDocCompactWriter::finalize() {
    const auto& parent_column =
            _opts.rowset_ctx->tablet_schema->column_by_uid(get_column()->parent_unique_id());
    DORIS_CHECK(!parent_column.variant_enable_nested_group());

    const size_t num_rows = _column->size();
    DORIS_CHECK_EQ(_null_column->size(), num_rows);
    _column->ensure_encoded();

    VariantShredder shredder(
            {.tablet_schema = _opts.rowset_ctx->tablet_schema.get(),
             .parent_column_unique_id = parent_column.unique_id(),
             .physical_layout = VariantShredderPhysicalLayout::DOC,
             .max_subcolumns_count = cast_set<size_t>(parent_column.variant_max_subcolumns_count()),
             .typed_paths_to_sparse = parent_column.variant_enable_typed_paths_to_sparse(),
             .sparse_bucket_count = cast_set<uint32_t>(
                     std::max(1, parent_column.variant_sparse_hash_shard_count())),
             .max_sparse_column_statistics_size =
                     cast_set<size_t>(parent_column.variant_max_sparse_column_statistics_size()),
             .doc_bucket_count =
                     cast_set<uint32_t>(std::max(1, parent_column.variant_doc_hash_shard_count())),
             .doc_materialization_min_rows = cast_set<size_t>(
                     std::max<int64_t>(0, parent_column.variant_doc_materialization_min_rows()))});
    if (num_rows > 0) {
        RETURN_IF_ERROR(shredder.append(
                _column->read_view(), 0, num_rows,
                std::span<const uint8_t>(_null_column->get_data().data(), num_rows)));
    }
    VariantShreddedColumns shredded;
    RETURN_IF_ERROR(shredder.finish(&shredded));

    auto converter = std::make_unique<OlapBlockDataConvertor>();
    int column_id = 0;

    _subcolumn_writers.clear();
    _subcolumns_indexes.clear();
    _subcolumn_opts.clear();
    RETURN_IF_ERROR(_write_materialized_subcolumns(parent_column, shredded, converter.get(),
                                                   num_rows, column_id));
    RETURN_IF_ERROR(
            _write_doc_value_column(parent_column, shredded, converter.get(), column_id, num_rows));
    RETURN_IF_ERROR(finish_and_write_column_writer(_doc_value_column_writer.get()));

    _column = ColumnVariantV2::create();
    _null_column = ColumnUInt8::create();
    _data_written = true;
    _is_finalized = true;
    return Status::OK();
}

uint64_t VariantDocCompactWriter::estimate_buffer_size() {
    return _column->byte_size();
}

} // namespace doris::segment_v2
