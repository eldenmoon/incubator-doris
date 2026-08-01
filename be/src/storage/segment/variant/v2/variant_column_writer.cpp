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

#include "storage/segment/variant/v2/variant_column_writer.h"

#include <algorithm>
#include <ranges>
#include <utility>

#include "common/exception.h"
#include "core/assert_cast.h"
#include "core/column/column_string.h"
#include "storage/iterator/olap_data_convertor.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/segment/variant/v2/variant_shredder.h"
#include "storage/segment/variant/variant_column_writer_impl.h"
#include "storage/segment/variant/variant_writer_helpers.h"
#include "util/slice.h"

namespace doris::segment_v2 {

VariantV2ColumnWriter::VariantV2ColumnWriter(ColumnWriterOptions opts, const TabletColumn* column)
        : _opts(std::move(opts)), _tablet_column(column), _outer_nulls(ColumnUInt8::create()) {}

VariantV2ColumnWriter::~VariantV2ColumnWriter() = default;

Status VariantV2ColumnWriter::init() {
    const VariantShredderPhysicalLayout physical_layout =
            _tablet_column->variant_enable_doc_mode() ? VariantShredderPhysicalLayout::DOC
                                                      : VariantShredderPhysicalLayout::ORDINARY;
    VariantShredderOptions options;
    RETURN_IF_ERROR(variant_writer_helpers::make_variant_shredder_options(
            *_opts.rowset_ctx->tablet_schema, *_tablet_column, physical_layout, {}, &options));
    _shredder = std::make_unique<VariantShredder>(std::move(options));
    return Status::OK();
}

Status VariantV2ColumnWriter::append(const VariantColumnData& column, size_t num_rows,
                                     std::span<const uint8_t> outer_nulls) {
    if (_is_finalized) {
        return Status::InternalError("Cannot append ColumnVariantV2 after writer finalization");
    }
    RETURN_IF_ERROR(variant_writer_helpers::append_variant_v2_to_shredder(_shredder.get(), column,
                                                                          num_rows, outer_nulls));
    if (outer_nulls.empty()) {
        _outer_nulls->insert_many_defaults(num_rows);
    } else {
        _outer_nulls->insert_many_raw_data(reinterpret_cast<const char*>(outer_nulls.data()),
                                           num_rows);
    }
    _num_rows += num_rows;
    return Status::OK();
}

bool VariantV2ColumnWriter::_has_extracted_variant_columns() const {
    const int current_variant_uid = _tablet_column->unique_id();
    return std::ranges::any_of(_opts.rowset_ctx->tablet_schema->columns(),
                               [current_variant_uid](const auto& column) {
                                   return column->is_extracted_column() &&
                                          column->parent_unique_id() == current_variant_uid;
                               });
}

Status VariantV2ColumnWriter::_write_root(const VariantShreddedColumns& shredded, int& column_id) {
    if (!shredded.root_jsonb || shredded.root_jsonb->size() != _num_rows) {
        return Status::InvalidArgument("Variant shredder root has {} rows, expected {}",
                                       shredded.root_jsonb ? shredded.root_jsonb->size() : 0,
                                       _num_rows);
    }
    DORIS_CHECK_EQ(_outer_nulls->size(), _num_rows);
    _root_writer = std::make_unique<ScalarColumnWriter>(
            _opts, std::make_shared<TabletColumn>(*_tablet_column), _opts.file_writer);
    RETURN_IF_ERROR(_root_writer->init());

    const auto& values = assert_cast<const ColumnString&>(*shredded.root_jsonb);
    DorisVector<Slice> slices(_num_rows);
    for (size_t row = 0; row < _num_rows; ++row) {
        slices[row] = values.get_data_at(row).to_slice();
    }
    const uint8_t* outer_nulls =
            _tablet_column->is_nullable() ? _outer_nulls->get_data().data() : nullptr;
    if (_num_rows > 0) {
        RETURN_IF_ERROR(_root_writer->append(outer_nulls, slices.data(), _num_rows));
    }
    _opts.meta->set_num_rows(_num_rows);
    ++column_id;
    return Status::OK();
}

Status VariantV2ColumnWriter::_write_materialized(const VariantShreddedColumns& shredded,
                                                  OlapBlockDataConvertor* converter,
                                                  int& column_id) {
    for (const VariantPathColumn& path_column : shredded.materialized) {
        if (!path_column.column || path_column.column->size() != _num_rows) {
            return Status::InvalidArgument("Variant materialized path {} has {} rows, expected {}",
                                           path_column.path.get_path(),
                                           path_column.column ? path_column.column->size() : 0,
                                           _num_rows);
        }
        TabletIndexes indexes;
        ColumnWriterOptions opts;
        std::unique_ptr<ColumnWriter> writer;
        TabletColumn tablet_column;
        const int current_column_id = column_id++;
        RETURN_IF_ERROR(variant_writer_helpers::prepare_subcolumn_writer_target(
                _opts, *_tablet_column, current_column_id, path_column.path, path_column.type,
                path_column.non_null_rows, _num_rows, nullptr, true, &indexes, &opts, &writer,
                &tablet_column));
        RETURN_IF_ERROR(variant_writer_helpers::convert_and_write_column(
                converter, tablet_column, path_column.type, writer.get(), path_column.column,
                _num_rows, current_column_id));
        _subcolumn_indexes.push_back(std::move(indexes));
        _subcolumn_opts.push_back(opts);
        _subcolumn_writers.push_back(std::move(writer));
    }
    return Status::OK();
}

Status VariantV2ColumnWriter::_write_binary(const VariantShreddedColumns& shredded,
                                            OlapBlockDataConvertor* converter, int& column_id) {
    if (_tablet_column->variant_enable_doc_mode()) {
        auto writer = std::make_unique<VariantDocWriter>();
        const int bucket_count = std::max(1, _tablet_column->variant_doc_hash_shard_count());
        RETURN_IF_ERROR(writer->init(_tablet_column, bucket_count, column_id, _opts, _opts.footer));
        RETURN_IF_ERROR(writer->append_shredded(_tablet_column, shredded, _num_rows, converter));
        _binary_writer = std::move(writer);
        return Status::OK();
    }

    auto writer = std::make_unique<UnifiedSparseColumnWriter>();
    const int bucket_count = std::max(1, _tablet_column->variant_sparse_hash_shard_count());
    RETURN_IF_ERROR(writer->init(_tablet_column, bucket_count, column_id, _opts, _opts.footer));
    RETURN_IF_ERROR(writer->append_shredded(_tablet_column, shredded, _num_rows, converter));
    _binary_writer = std::move(writer);
    return Status::OK();
}

Status VariantV2ColumnWriter::finalize() {
    if (_is_finalized) {
        return Status::OK();
    }
    DORIS_CHECK(_shredder != nullptr);
    VariantShreddedColumns shredded;
    RETURN_IF_ERROR(_shredder->finish(&shredded));
    DORIS_CHECK_EQ(shredded.num_rows, _num_rows);
    _shredder.reset();

    auto converter = std::make_unique<OlapBlockDataConvertor>();
    int column_id = 0;
    RETURN_IF_ERROR(_write_root(shredded, column_id));
    converter->add_column_data_convertor(*_tablet_column);

    const bool has_extracted_columns = _has_extracted_variant_columns();
    if (!has_extracted_columns) {
        if (_tablet_column->variant_enable_doc_mode()) {
            RETURN_IF_ERROR(_write_binary(shredded, converter.get(), column_id));
            RETURN_IF_ERROR(_write_materialized(shredded, converter.get(), column_id));
        } else {
            RETURN_IF_ERROR(_write_materialized(shredded, converter.get(), column_id));
            RETURN_IF_ERROR(_write_binary(shredded, converter.get(), column_id));
        }
        shredded.statistics.to_pb(_opts.meta->mutable_variant_statistics());
    }
    _is_finalized = true;
    return Status::OK();
}

Status VariantV2ColumnWriter::_for_each_column_writer(
        const std::function<Status(ColumnWriter*)>& function) {
    DORIS_CHECK(_root_writer != nullptr);
    RETURN_IF_ERROR(function(_root_writer.get()));
    for (auto& writer : _subcolumn_writers) {
        RETURN_IF_ERROR(function(writer.get()));
    }
    return Status::OK();
}

Status VariantV2ColumnWriter::finish() {
    RETURN_IF_ERROR(finalize());
    RETURN_IF_ERROR(_for_each_column_writer([](ColumnWriter* writer) { return writer->finish(); }));
    if (_binary_writer != nullptr) {
        RETURN_IF_ERROR(_binary_writer->finish());
    }
    return Status::OK();
}

Status VariantV2ColumnWriter::write_data() {
    RETURN_IF_ERROR(finalize());
    RETURN_IF_ERROR(
            _for_each_column_writer([](ColumnWriter* writer) { return writer->write_data(); }));
    if (_binary_writer != nullptr) {
        RETURN_IF_ERROR(_binary_writer->write_data());
    }
    return Status::OK();
}

Status VariantV2ColumnWriter::write_ordinal_index() {
    DORIS_CHECK(_is_finalized);
    RETURN_IF_ERROR(_for_each_column_writer(
            [](ColumnWriter* writer) { return writer->write_ordinal_index(); }));
    if (_binary_writer != nullptr) {
        RETURN_IF_ERROR(_binary_writer->write_ordinal_index());
    }
    return Status::OK();
}

Status VariantV2ColumnWriter::write_zone_map() {
    DORIS_CHECK(_is_finalized);
    for (size_t index = 0; index < _subcolumn_writers.size(); ++index) {
        if (_subcolumn_opts[index].need_zone_map) {
            RETURN_IF_ERROR(_subcolumn_writers[index]->write_zone_map());
        }
    }
    if (_binary_writer != nullptr) {
        RETURN_IF_ERROR(_binary_writer->write_zone_map());
    }
    return Status::OK();
}

Status VariantV2ColumnWriter::write_inverted_index() {
    DORIS_CHECK(_is_finalized);
    for (size_t index = 0; index < _subcolumn_writers.size(); ++index) {
        if (_subcolumn_opts[index].need_inverted_index) {
            RETURN_IF_ERROR(_subcolumn_writers[index]->write_inverted_index());
        }
    }
    if (_binary_writer != nullptr) {
        RETURN_IF_ERROR(_binary_writer->write_inverted_index());
    }
    return Status::OK();
}

Status VariantV2ColumnWriter::write_bloom_filter_index() {
    DORIS_CHECK(_is_finalized);
    for (size_t index = 0; index < _subcolumn_writers.size(); ++index) {
        if (_subcolumn_opts[index].need_bloom_filter) {
            RETURN_IF_ERROR(_subcolumn_writers[index]->write_bloom_filter_index());
        }
    }
    if (_binary_writer != nullptr) {
        RETURN_IF_ERROR(_binary_writer->write_bloom_filter_index());
    }
    return Status::OK();
}

uint64_t VariantV2ColumnWriter::estimate_buffer_size() {
    if (!_is_finalized) {
        DORIS_CHECK(_shredder != nullptr);
        return _outer_nulls->byte_size() + _shredder->byte_size();
    }
    uint64_t size = _root_writer->estimate_buffer_size();
    for (const auto& writer : _subcolumn_writers) {
        size += writer->estimate_buffer_size();
    }
    if (_binary_writer != nullptr) {
        size += _binary_writer->estimate_buffer_size();
    }
    return size;
}

} // namespace doris::segment_v2
