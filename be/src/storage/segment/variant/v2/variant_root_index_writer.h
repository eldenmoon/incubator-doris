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
#include <vector>

#include "common/status.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/value/variant/variant_value.h"
#include "storage/index/snii/snii_index_writer.h"
#include "util/slice.h"

namespace doris {

class ColumnVariant;
class TabletIndex;
struct VariantLeaf;

namespace segment_v2 {

class IndexFileWriter;

// Owns the one logical SNII document stream of a VARIANT root index (contract 1 in
// variant_root_index.h). A document is one row: begin_document(), add_leaf() for every scalar
// leaf the caller's traversal reaches, end_document(). The ordinary V2 shredder feeds leaves
// while it already traverses the object; append_variant_root_indexes() traverses rows itself for
// callers that hold a whole column. Every leaf becomes one path-less typed term (exact index)
// or one canonical text for the analyzer (token index).
class VariantRootIndexWriter final {
public:
    VariantRootIndexWriter(IndexFileWriter* index_file_writer, const TabletIndex* index_meta,
                           bool is_direct_load);
    ~VariantRootIndexWriter();

    Status init();
    Status begin_document(bool sql_null);
    // Adds one classified leaf. The leaf borrows the document until end_document(); string
    // leaves are referenced, not copied, until then.
    Status add_leaf(const VariantLeaf& leaf);
    Status end_document();
    Status finish();
    void close_on_error();
    size_t size() const;

private:
    IndexFileWriter* _index_file_writer = nullptr;
    const TabletIndex* _index_meta = nullptr;
    bool _is_direct_load = false;
    bool _should_analyze = false;
    uint32_t _ignore_above = 0;
    bool _document_open = false;
    bool _sql_null = false;
    bool _has_unspellable_leaf = false;
    std::unique_ptr<SniiIndexColumnWriter> _writer;
    std::vector<std::string> _exact_terms;
    // String leaves borrow the document's bytes until end_document(); the canonical texts of
    // number and boolean leaves are owned here.
    std::vector<Slice> _analyzed_values;
    std::vector<std::string> _owned_texts;
};

// Visits the scalar leaves below `value` once (objects and arrays recursed) and hands each one
// to every writer. Each writer still owns its independent term and docid stream.
Status append_variant_root_index_leaves(std::span<VariantRootIndexWriter*> writers,
                                        const VariantRef& value);

// Appends one document per row of [begin, begin + length) to every writer while traversing each
// input row once. `outer_nulls`, when not empty, marks the SQL NULL rows.
Status append_variant_root_indexes(std::span<VariantRootIndexWriter*> writers,
                                   const ColumnVariantV2::ReadView& view, size_t begin,
                                   size_t length, std::span<const uint8_t> outer_nulls);

// The legacy row model (ColumnVariant) reaches the index through its JSON text; rows that fail
// to parse back are an error rather than a document indexed as one string.
Status append_variant_root_indexes(std::span<VariantRootIndexWriter*> writers,
                                   const ColumnVariant& column, size_t begin, size_t length,
                                   std::span<const uint8_t> outer_nulls);

Status finish_variant_root_indexes(std::span<VariantRootIndexWriter*> writers);

} // namespace segment_v2
} // namespace doris
