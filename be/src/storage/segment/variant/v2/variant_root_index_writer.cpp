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

#include "storage/segment/variant/v2/variant_root_index_writer.h"

#include "common/cast_set.h"
#include "common/exception.h"
#include "core/column/column_variant.h"
#include "core/value/variant/variant_leaf_visitor.h"
#include "exprs/function/parse/variant_string_parse.h"
#include "storage/index/inverted/analyzer/analyzer.h"
#include "storage/index/inverted/inverted_index_parser.h"
#include "storage/index/inverted/variant_root_index.h"
#include "storage/index/inverted/variant_term_codec.h"
#include "storage/tablet/tablet_schema.h"

namespace doris::segment_v2 {

VariantRootIndexWriter::VariantRootIndexWriter(IndexFileWriter* index_file_writer,
                                               const TabletIndex* index_meta, bool is_direct_load)
        : _index_file_writer(index_file_writer),
          _index_meta(index_meta),
          _is_direct_load(is_direct_load) {}

VariantRootIndexWriter::~VariantRootIndexWriter() {
    close_on_error();
}

Status VariantRootIndexWriter::init() {
    DORIS_CHECK(_index_file_writer != nullptr);
    DORIS_CHECK(_index_meta != nullptr);
    DORIS_CHECK(variant_root_index::is_root_index(*_index_meta));
    if (get_parser_phrase_support_string_from_properties(_index_meta->properties()) ==
        INVERTED_INDEX_PARSER_PHRASE_SUPPORT_YES) {
        return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>(
                "VARIANT root index does not support phrase positions");
    }
    _ignore_above = cast_set<uint32_t>(
            std::stoul(get_parser_ignore_above_value_from_properties(_index_meta->properties())));
    _should_analyze =
            inverted_index::InvertedIndexAnalyzer::should_analyzer(_index_meta->properties());
    _writer = std::make_unique<SniiIndexColumnWriter>(_index_file_writer, _index_meta,
                                                      FieldType::OLAP_FIELD_TYPE_VARCHAR);
    RETURN_IF_ERROR(_writer->init());
    _writer->set_direct_load(_is_direct_load);
    return Status::OK();
}

Status VariantRootIndexWriter::begin_document(bool sql_null) {
    DORIS_CHECK(_writer != nullptr);
    DORIS_CHECK(!_document_open);
    _document_open = true;
    _sql_null = sql_null;
    _exact_terms.clear();
    _analyzed_values.clear();
    _owned_texts.clear();
    _has_unspellable_leaf = false;
    return Status::OK();
}

Status VariantRootIndexWriter::add_leaf(const VariantLeaf& leaf) {
    DORIS_CHECK(_document_open);
    DORIS_CHECK(!_sql_null);
    if (leaf.kind == VariantLeafKind::OTHER) {
        // A non-null scalar the codec cannot spell has no value term. The exact index records
        // one marker per document instead, so an equality over the binary storage still lists
        // the document as a candidate for its residual.
        if (!_should_analyze && !_has_unspellable_leaf &&
            leaf.value.basic_type() == VariantBasicType::PRIMITIVE && !leaf.value.is_null()) {
            _has_unspellable_leaf = true;
            _exact_terms.push_back(variant_root_index::unspellable_marker_term());
        }
        return Status::OK();
    }
    if (!_should_analyze) {
        if (leaf.kind != VariantLeafKind::STRING || leaf.string_value.size <= _ignore_above) {
            _exact_terms.push_back(variant_root_index::leaf_term(leaf));
        }
        return Status::OK();
    }
    if (leaf.kind == VariantLeafKind::STRING) {
        _analyzed_values.emplace_back(leaf.string_value.data, leaf.string_value.size);
        return Status::OK();
    }
    // Numbers and booleans are searchable by their canonical text, the same text the scalar
    // MATCH fallback tokenizes. Slices into _owned_texts are taken in end_document(), after the
    // vector stopped growing.
    std::string text;
    const bool has_text = variant_root_index::canonical_leaf_text(leaf, &text);
    DORIS_CHECK(has_text);
    _owned_texts.push_back(std::move(text));
    return Status::OK();
}

Status VariantRootIndexWriter::end_document() {
    DORIS_CHECK(_document_open);
    Status status;
    if (_sql_null) {
        status = _writer->add_nulls(1);
    } else {
        for (const std::string& text : _owned_texts) {
            _analyzed_values.emplace_back(text);
        }
        status = _writer->add_document(_exact_terms, variant_term_codec::token_term_prefix(),
                                       _analyzed_values);
    }
    _document_open = false;
    return status;
}

// NOLINTNEXTLINE(readability-make-member-function-const): finishing the owned SNII writer is a mutation.
Status VariantRootIndexWriter::finish() {
    DORIS_CHECK(_writer != nullptr);
    DORIS_CHECK(!_document_open);
    return _writer->finish();
}

void VariantRootIndexWriter::close_on_error() {
    if (_writer != nullptr) {
        _writer->close_on_error();
    }
}

size_t VariantRootIndexWriter::size() const {
    size_t result = _analyzed_values.capacity() * sizeof(Slice);
    for (const std::string& term : _exact_terms) {
        result += term.capacity();
    }
    for (const std::string& text : _owned_texts) {
        result += text.capacity();
    }
    return result;
}

Status append_variant_root_index_leaves(std::span<VariantRootIndexWriter*> writers,
                                        const VariantRef& value) {
    for (VariantRootIndexWriter* writer : writers) {
        DORIS_CHECK(writer != nullptr);
    }
    return visit_variant_leaves(value, {}, [&](const VariantLeaf& leaf) {
        for (VariantRootIndexWriter* writer : writers) {
            RETURN_IF_ERROR(writer->add_leaf(leaf));
        }
        return Status::OK();
    });
}

Status append_variant_root_indexes(std::span<VariantRootIndexWriter*> writers,
                                   const ColumnVariantV2::ReadView& view, size_t begin,
                                   size_t length, std::span<const uint8_t> outer_nulls) {
    DORIS_CHECK(!writers.empty());
    if (view.is_typed()) {
        return Status::InvalidArgument(
                "VARIANT root index requires encoded E-state input; caller must ensure_encoded");
    }
    if (begin > view.size() || length > view.size() - begin) {
        return Status::InvalidArgument("VARIANT root index range [{}, {}) exceeds input size {}",
                                       begin, begin + length, view.size());
    }
    if (!outer_nulls.empty() && outer_nulls.size() != length) {
        return Status::InvalidArgument(
                "VARIANT root index outer-null span has {} rows, expected {}", outer_nulls.size(),
                length);
    }
    try {
        for (size_t offset = 0; offset < length; ++offset) {
            const bool outer_null = !outer_nulls.empty() && outer_nulls[offset] != 0;
            for (VariantRootIndexWriter* writer : writers) {
                DORIS_CHECK(writer != nullptr);
                RETURN_IF_ERROR(writer->begin_document(outer_null));
            }
            if (!outer_null) {
                RETURN_IF_ERROR(
                        append_variant_root_index_leaves(writers, view.value_at(begin + offset)));
            }
            for (VariantRootIndexWriter* writer : writers) {
                RETURN_IF_ERROR(writer->end_document());
            }
        }
        return Status::OK();
    } catch (const Exception& exception) {
        return exception.to_status();
    }
}

Status append_variant_root_indexes(std::span<VariantRootIndexWriter*> writers,
                                   const ColumnVariant& column, size_t begin, size_t length,
                                   std::span<const uint8_t> outer_nulls) {
    DORIS_CHECK(!writers.empty());
    if (begin > column.size() || length > column.size() - begin) {
        return Status::InvalidArgument("VARIANT root index range [{}, {}) exceeds input size {}",
                                       begin, begin + length, column.size());
    }
    if (!outer_nulls.empty() && outer_nulls.size() != length) {
        return Status::InvalidArgument(
                "VARIANT root index outer-null span has {} rows, expected {}", outer_nulls.size(),
                length);
    }
    try {
        // Every string is escaped (control characters included) so the text parses back to the
        // same leaves, and text that still fails to parse is an error rather than a document
        // silently indexed as one string.
        JsonToVariantOptions encoder_options = JsonToVariantOptions::current_config();
        encoder_options.throw_on_invalid_json = true;
        JsonStringToVariantEncoder encoder(encoder_options);
        DataTypeSerDe::FormatOptions format_options;
        format_options.escape_char = '\\';
        for (size_t offset = 0; offset < length; ++offset) {
            const bool outer_null = !outer_nulls.empty() && outer_nulls[offset] != 0;
            std::string json;
            if (outer_null) {
                json = "{}";
            } else {
                column.serialize_one_row_to_string(begin + offset, &json, format_options);
            }
            RETURN_IF_ERROR(encoder.try_add_json({json.data(), json.size()}));
        }
        VariantBatchBuilder batch = encoder.finish_batch();
        auto encoded = ColumnVariantV2::create();
        encoded->insert_encoded_batch(batch);
        return append_variant_root_indexes(writers, encoded->read_view(), 0, length, outer_nulls);
    } catch (const Exception& exception) {
        for (VariantRootIndexWriter* writer : writers) {
            DORIS_CHECK(writer != nullptr);
            writer->close_on_error();
        }
        return exception.to_status();
    }
}

Status finish_variant_root_indexes(std::span<VariantRootIndexWriter*> writers) {
    for (VariantRootIndexWriter* writer : writers) {
        DORIS_CHECK(writer != nullptr);
        const Status status = writer->finish();
        if (!status.ok()) {
            for (VariantRootIndexWriter* writer_to_close : writers) {
                writer_to_close->close_on_error();
            }
            return status;
        }
    }
    return Status::OK();
}

} // namespace doris::segment_v2
