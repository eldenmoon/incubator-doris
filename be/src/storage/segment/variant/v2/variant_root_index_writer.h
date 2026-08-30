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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <unordered_set>
#include <vector>

#include "common/status.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/value/variant/variant_value.h"
#include "storage/index/snii/snii_index_writer.h"
#include "util/slice.h"

namespace doris {

class PathInData;
class ColumnVariant;
class TabletIndex;

namespace segment_v2 {

class IndexFileWriter;

// Owns the one logical SNII document stream for a VARIANT parent. The ordinary V2 shredder calls
// begin/add/end while it already traverses object leaves. append() supplies the same semantics for
// root-only batches whose extracted columns are written separately.
class VariantRootIndexWriter final {
public:
    VariantRootIndexWriter(IndexFileWriter* index_file_writer, const TabletIndex* index_meta,
                           bool is_direct_load, bool check_duplicate_json_path);
    ~VariantRootIndexWriter();

    Status init();
    Status begin_document(bool sql_null);
    Status add_leaf(std::string_view relative_path, const VariantRef& value, bool is_root_value);
    Status end_document();
    Status append(const ColumnVariantV2::ReadView& view, size_t begin, size_t length,
                  std::span<const uint8_t> outer_nulls);
    Status finish();
    void close_on_error();
    size_t size() const;

private:
    struct AnalyzedValue {
        std::string prefix;
        Slice value;
    };

    struct OwnedAnalyzedValue {
        std::string prefix;
        std::string value;
    };

    Status _mark_leaf(std::string_view relative_path, bool* inserted);

    IndexFileWriter* _index_file_writer = nullptr;
    const TabletIndex* _index_meta = nullptr;
    bool _is_direct_load = false;
    bool _check_duplicate_json_path = false;
    bool _all_values = false;
    bool _should_analyze = false;
    uint32_t _ignore_above = 0;
    bool _document_open = false;
    bool _sql_null = false;
    std::unique_ptr<SniiIndexColumnWriter> _writer;
    std::vector<std::string> _exact_terms;
    std::vector<AnalyzedValue> _analyzed_values;
    std::vector<OwnedAnalyzedValue> _owned_analyzed_values;
    std::unordered_set<std::string> _seen_paths;
};

// Appends one logical Variant document stream to every writer while traversing each input row
// once. Each writer still owns an independent SNII docid domain and analyzer identity.
Status append_variant_root_indexes(std::span<VariantRootIndexWriter*> writers,
                                   const ColumnVariantV2::ReadView& view, size_t begin,
                                   size_t length, std::span<const uint8_t> outer_nulls);

Status append_variant_root_indexes(std::span<VariantRootIndexWriter*> writers,
                                   const ColumnVariant& column, size_t begin, size_t length,
                                   std::span<const uint8_t> outer_nulls);

Status finish_variant_root_indexes(std::span<VariantRootIndexWriter*> writers);

} // namespace segment_v2
} // namespace doris
