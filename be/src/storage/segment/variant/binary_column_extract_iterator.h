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

#pragma once

#include <parallel_hashmap/phmap.h>

#include <memory>
#include <string_view>
#include <unordered_map>
#include <utility>

#include "common/exception.h"
#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/column_with_type_and_name.h"
#include "core/block/columns_with_type_and_name.h"
#include "core/column/column.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/column/subcolumn_tree.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_variant.h"
#include "core/types.h"
#include "exprs/function/function_helpers.h"
#include "io/io_common.h"
#include "storage/iterators.h"
#include "storage/schema.h"
#include "storage/segment/column_reader.h"
#include "storage/segment/stream_reader.h"
#include "storage/segment/variant/variant_column_reader.h"
#include "storage/tablet/tablet_schema.h"
#include "util/json/path_in_data.h"

namespace doris::segment_v2 {

// Base class for sparse column processors with common functionality
class BaseBinaryColumnProcessor : public ColumnIterator {
protected:
    const StorageReadOptions* _read_opts;
    BinaryColumnCacheSPtr _sparse_column_cache;
    // Pure virtual method for data processing when encounter existing sparse columns(to be implemented by subclasses)
    virtual void _process_data_with_existing_sparse_column(MutableColumnPtr& dst,
                                                           size_t num_rows) = 0;

    // Pure virtual method for data processing when no sparse columns(to be implemented by subclasses)
    virtual void _process_data_without_sparse_column(MutableColumnPtr& dst, size_t num_rows) = 0;

public:
    BaseBinaryColumnProcessor(BinaryColumnCacheSPtr sparse_column_cache,
                              const StorageReadOptions* opts)
            : _read_opts(opts), _sparse_column_cache(std::move(sparse_column_cache)) {}

    // Common initialization for all processors
    Status init(const ColumnIteratorOptions& opts) override {
        return _sparse_column_cache->init(opts);
    }

    Status seek_to_ordinal(ordinal_t ord) override {
        return _sparse_column_cache->seek_to_ordinal(ord);
    }

    ordinal_t get_current_ordinal() const override {
        throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR, "not implement");
    }

    // Template method pattern for batch processing
    template <typename ReadMethod>
    Status _process_batch(ReadMethod&& read_method, size_t nrows, MutableColumnPtr& dst) {
        {
            SCOPED_RAW_TIMER(&_read_opts->stats->variant_scan_sparse_column_timer_ns);
            int64_t before_size = _read_opts->stats->uncompressed_bytes_read;
            RETURN_IF_ERROR(read_method());
            _read_opts->stats->variant_scan_sparse_column_bytes +=
                    _read_opts->stats->uncompressed_bytes_read - before_size;
        }

        SCOPED_RAW_TIMER(&_read_opts->stats->variant_fill_path_from_sparse_column_timer_ns);
        const auto& offsets =
                assert_cast<const ColumnMap&>(*_sparse_column_cache->binary_column).get_offsets();
        if (offsets.back() == offsets[-1]) {
            // no sparse column in this batch
            _process_data_without_sparse_column(dst, nrows);
        } else {
            // merge subcolumns to existing sparse columns
            _process_data_with_existing_sparse_column(dst, nrows);
        }
        return Status::OK();
    }
};

// Implementation for path extraction processor
class BinaryColumnExtractIterator : public BaseBinaryColumnProcessor {
public:
    BinaryColumnExtractIterator(std::string_view path, BinaryColumnCacheSPtr sparse_column_cache,
                                const StorageReadOptions* opts)
            : BaseBinaryColumnProcessor(std::move(sparse_column_cache), opts), _path(path) {}

    Status init(const ColumnIteratorOptions& opts) override {
        VariantAssemblerPlanOptions plan_options;
        plan_options.mode = VariantAssemblerMode::BINARY_EXTRACT;
        std::shared_ptr<const VariantAssemblerPlan> plan;
        RETURN_IF_ERROR(VariantAssemblerPlan::create(std::move(plan_options), &plan));
        _assembler = std::make_unique<VariantAssembler>(std::move(plan));
        return BaseBinaryColumnProcessor::init(opts);
    }

    // Batch processing using template method
    Status next_batch(size_t* n, MutableColumnPtr& dst, bool* has_null) override {
        return _process_variant_batch(
                [&]() { return _sparse_column_cache->next_batch(n, has_null); }, n, dst);
    }

    // RowID-based read using template method
    Status read_by_rowids(const rowid_t* rowids, const size_t count,
                          MutableColumnPtr& dst) override {
        size_t rows = count;
        return _process_variant_batch(
                [&]() { return _sparse_column_cache->read_by_rowids(rowids, count); }, &rows, dst);
    }

private:
    std::string _path;
    std::unique_ptr<VariantAssembler> _assembler;

    template <typename ReadMethod>
    Status _process_variant_batch(ReadMethod&& read_method, size_t* num_rows,
                                  MutableColumnPtr& dst) {
        {
            SCOPED_RAW_TIMER(&_read_opts->stats->variant_scan_sparse_column_timer_ns);
            const int64_t before_size = _read_opts->stats->uncompressed_bytes_read;
            RETURN_IF_ERROR(read_method());
            _read_opts->stats->variant_scan_sparse_column_bytes +=
                    _read_opts->stats->uncompressed_bytes_read - before_size;
        }
        SCOPED_RAW_TIMER(&_read_opts->stats->variant_fill_path_from_sparse_column_timer_ns);
        if (_sparse_column_cache->binary_column->size() != *num_rows) {
            return Status::Corruption("Variant sparse reader returned {} rows, expected {}",
                                      _sparse_column_cache->binary_column->size(), *num_rows);
        }
        if (*num_rows == 0) {
            return _fill_missing_column(dst, 0);
        }
        const auto& offsets =
                assert_cast<const ColumnMap&>(*_sparse_column_cache->binary_column).get_offsets();
        if (offsets.back() == offsets[-1]) {
            return _fill_missing_column(dst, *num_rows);
        }
        return _fill_path_column(dst, *num_rows);
    }

    // Fill column by finding path in sparse column
    void _process_data_with_existing_sparse_column(MutableColumnPtr& dst,
                                                   size_t num_rows) override {
        THROW_IF_ERROR(_fill_path_column(dst, num_rows));
    }

    Status _fill_path_column(MutableColumnPtr& dst, size_t num_rows) {
        const auto* map =
                check_and_get_column<ColumnMap>(_sparse_column_cache->binary_column.get());
        if (map == nullptr || map->size() != num_rows) {
            return Status::Corruption("Variant sparse input must be a {}-row Map<String,String>",
                                      num_rows);
        }
        const auto* paths = check_and_get_column<ColumnString>(&map->get_keys());
        const auto* values = check_and_get_column<ColumnString>(&map->get_values());
        if (paths == nullptr || values == nullptr || paths->size() != values->size()) {
            return Status::Corruption("Variant sparse input is not Map<String,String>");
        }

        auto cells = ColumnString::create();
        auto missing = ColumnUInt8::create();
        const auto& offsets = map->get_offsets();
        size_t previous_end = 0;
        for (size_t row = 0; row < num_rows; ++row) {
            const size_t end = offsets[ssize_t(row)];
            if (end < previous_end || end > paths->size()) {
                return Status::Corruption("Variant sparse row {} has invalid offset {}", row, end);
            }
            for (size_t index = previous_end + 1; index < end; ++index) {
                if (paths->get_data_at(index - 1).compare(paths->get_data_at(index)) >= 0) {
                    return Status::Corruption(
                            "Variant sparse row {} paths are not strictly sorted at {}", row,
                            paths->get_data_at(index).to_string());
                }
            }
            previous_end = end;
        }
        if (previous_end != paths->size()) {
            return Status::Corruption("Variant sparse offsets consume {} of {} cells", previous_end,
                                      paths->size());
        }
        const StringRef requested {_path.data(), _path.size()};
        for (size_t row = 0; row < num_rows; ++row) {
            size_t lower = offsets[ssize_t(row) - 1];
            size_t upper = offsets[ssize_t(row)];
            while (lower < upper) {
                const size_t middle = lower + (upper - lower) / 2;
                if (paths->get_data_at(middle).compare(requested) < 0) {
                    lower = middle + 1;
                } else {
                    upper = middle;
                }
            }
            const size_t end = offsets[ssize_t(row)];
            if (lower < end && paths->get_data_at(lower) == requested) {
                cells->insert_from(*values, lower);
                missing->insert_value(0);
            } else {
                cells->insert_default();
                missing->insert_value(1);
            }
        }

        VariantAssemblerBatchView batch;
        batch.num_rows = num_rows;
        batch.binary_values = cells.get();
        batch.binary_missing = std::span<const uint8_t>(missing->get_data().data(), num_rows);
        VariantAssembledColumn assembled;
        RETURN_IF_ERROR(_assembler->assemble(batch, &assembled));
        return append_assembled_variant(dst, std::move(assembled));
    }

    Status _fill_missing_column(MutableColumnPtr& dst, size_t num_rows) {
        auto cells = ColumnString::create();
        cells->insert_many_defaults(num_rows);
        auto missing = ColumnUInt8::create(num_rows, 1);
        VariantAssemblerBatchView batch;
        batch.num_rows = num_rows;
        batch.binary_values = cells.get();
        batch.binary_missing = std::span<const uint8_t>(missing->get_data().data(), num_rows);
        VariantAssembledColumn assembled;
        RETURN_IF_ERROR(_assembler->assemble(batch, &assembled));
        return append_assembled_variant(dst, std::move(assembled));
    }

    void _process_data_without_sparse_column(MutableColumnPtr& dst, size_t num_rows) override {
        THROW_IF_ERROR(_fill_missing_column(dst, num_rows));
    }
};

} // namespace doris::segment_v2
