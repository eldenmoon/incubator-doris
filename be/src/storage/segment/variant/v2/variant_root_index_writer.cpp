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

#include <array>
#include <limits>

#include "common/cast_set.h"
#include "common/exception.h"
#include "core/column/column_variant.h"
#include "exprs/function/parse/variant_string_parse.h"
#include "storage/index/inverted/analyzer/analyzer.h"
#include "storage/index/inverted/inverted_index_parser.h"
#include "storage/index/inverted/variant_root_index.h"
#include "storage/tablet/tablet_schema.h"
#include "util/json/path_in_data.h"

namespace doris::segment_v2 {

VariantRootIndexWriter::VariantRootIndexWriter(IndexFileWriter* index_file_writer,
                                               const TabletIndex* index_meta, bool is_direct_load,
                                               bool check_duplicate_json_path)
        : _index_file_writer(index_file_writer),
          _index_meta(index_meta),
          _is_direct_load(is_direct_load),
          _check_duplicate_json_path(check_duplicate_json_path) {}

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
    _seen_paths.clear();
    return Status::OK();
}

Status VariantRootIndexWriter::_mark_leaf(std::string_view relative_path, bool* inserted) {
    DORIS_CHECK(_document_open);
    DORIS_CHECK(!_sql_null);
    DORIS_CHECK(inserted != nullptr);
    const auto [unused, was_inserted] = _seen_paths.emplace(relative_path);
    static_cast<void>(unused);
    *inserted = was_inserted;
    if (!was_inserted) {
        if (_check_duplicate_json_path) {
            return Status::OK();
        }
        return Status::InvalidArgument("may contains duplicated entry : {}", relative_path);
    }
    return Status::OK();
}

Status VariantRootIndexWriter::add_leaf(std::string_view relative_path, const VariantRef& value) {
    bool inserted = false;
    RETURN_IF_ERROR(_mark_leaf(relative_path, &inserted));
    if (!inserted) {
        return Status::OK();
    }

    const bool is_string = value.basic_type() == VariantBasicType::SHORT_STRING ||
                           (value.basic_type() == VariantBasicType::PRIMITIVE && !value.is_null() &&
                            value.primitive_id() == VariantPrimitiveId::STRING);
    if (!_should_analyze && (!is_string || value.get_string().size <= _ignore_above)) {
        RETURN_IF_ERROR(variant_root_index::append_variant_value_terms(relative_path, value,
                                                                       &_exact_terms));
    }
    if (is_string && _should_analyze) {
        const StringRef string = value.get_string();
        _analyzed_values.push_back(
                {.prefix = variant_root_index::encode_token_term(relative_path, ""),
                 .value = Slice(string.data, string.size)});
    }
    return Status::OK();
}

Status VariantRootIndexWriter::end_document() {
    DORIS_CHECK(_document_open);
    Status status;
    if (_sql_null) {
        status = _writer->add_nulls(1);
    } else {
        std::vector<SniiIndexColumnWriter::PrefixedAnalyzedValue> analyzed_values;
        analyzed_values.reserve(_analyzed_values.size());
        for (const AnalyzedValue& value : _analyzed_values) {
            analyzed_values.push_back({.term_prefix = value.prefix, .value = value.value});
        }
        status = _writer->add_document(_exact_terms, analyzed_values);
    }
    _document_open = false;
    return status;
}

namespace {

Status visit_root_index_writers(std::span<VariantRootIndexWriter*> writers, const VariantRef& value,
                                const PathInData& relative_path) {
    if (value.basic_type() != VariantBasicType::OBJECT) {
        if (relative_path.empty()) {
            return Status::OK();
        }
        for (VariantRootIndexWriter* writer : writers) {
            DORIS_CHECK(writer != nullptr);
            RETURN_IF_ERROR(writer->add_leaf(relative_path.get_path(), value));
        }
        return Status::OK();
    }
    const VariantRef::ObjectView object = value.object_view();
    for (uint32_t index = 0; index < object.size(); ++index) {
        uint32_t field = 0;
        const VariantRef child = object.value_at(index, &field);
        PathInDataBuilder builder;
        if (!relative_path.empty()) {
            builder.append(relative_path.get_parts(), false);
        }
        PathInData child_path =
                builder.append(value.metadata.key_at(field).to_string_view(), false).build();
        child_path = PathInData(child_path.get_path());
        RETURN_IF_ERROR(visit_root_index_writers(writers, child, child_path));
    }
    return Status::OK();
}

} // namespace

Status append_variant_root_indexes(std::span<VariantRootIndexWriter*> writers,
                                   const ColumnVariantV2::ReadView& view, size_t begin,
                                   size_t length, std::span<const uint8_t> outer_nulls) {
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
    for (VariantRootIndexWriter* writer : writers) {
        DORIS_CHECK(writer != nullptr);
    }
    try {
        for (size_t offset = 0; offset < length; ++offset) {
            const bool outer_null = !outer_nulls.empty() && outer_nulls[offset] != 0;
            for (VariantRootIndexWriter* writer : writers) {
                RETURN_IF_ERROR(writer->begin_document(outer_null));
            }
            if (!outer_null) {
                RETURN_IF_ERROR(
                        visit_root_index_writers(writers, view.value_at(begin + offset), {}));
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
        JsonStringToVariantEncoder encoder;
        DataTypeSerDe::FormatOptions format_options;
        for (size_t offset = 0; offset < length; ++offset) {
            const bool outer_null = !outer_nulls.empty() && outer_nulls[offset] != 0;
            std::string json;
            if (outer_null) {
                json = "{}";
            } else {
                column.serialize_one_row_to_string(begin + offset, &json, format_options);
            }
            encoder.add_json({json.data(), json.size()});
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

Status VariantRootIndexWriter::append(const ColumnVariantV2::ReadView& view, size_t begin,
                                      size_t length, std::span<const uint8_t> outer_nulls) {
    std::array<VariantRootIndexWriter*, 1> writers = {this};
    return append_variant_root_indexes(writers, view, begin, length, outer_nulls);
}

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
    size_t result = 0;
    for (const std::string& term : _exact_terms) {
        result += term.capacity();
    }
    for (const AnalyzedValue& value : _analyzed_values) {
        result += value.prefix.capacity();
    }
    for (const std::string& path : _seen_paths) {
        result += path.capacity();
    }
    return result;
}

} // namespace doris::segment_v2
