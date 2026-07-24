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

#include <fcntl.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iterator>
#include <regex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <tuple>
#include <utility>
#include <vector>

#include "core/field.h"
#include "core/string_ref.h"
#include "core/value/variant/variant_batch_builder.h"
#include "core/value/variant/variant_field.h"
#include "core/value/variant/variant_parquet_encoding.h"
#include "util/json/json_parser.h"
#include "util/json/simd_json_parser.h"
#include "util/utf8_check.h"

namespace doris {
namespace {

constexpr std::string_view REGEN_ENV = "DORIS_REGEN_VARIANT_SEMANTICS_SNAPSHOT";
constexpr std::string_view SNAPSHOT_RELATIVE_PATH =
        "docs/design/variant_v2/baseline/semantics/internal.tsv";

const std::vector<std::string_view>& snapshot_header() {
    static const std::vector<std::string_view> header {
            "# Licensed to the Apache Software Foundation (ASF) under one",
            "# or more contributor license agreements.  See the NOTICE file",
            "# distributed with this work for additional information",
            "# regarding copyright ownership.  The ASF licenses this file",
            "# to you under the Apache License, Version 2.0 (the",
            "# \"License\"); you may not use this file except in compliance",
            "# with the License.  You may obtain a copy of the License at",
            "#",
            "#   http://www.apache.org/licenses/LICENSE-2.0",
            "#",
            "# Unless required by applicable law or agreed to in writing,",
            "# software distributed under the License is distributed on an",
            "# \"AS IS\" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY",
            "# KIND, either express or implied.  See the License for the",
            "# specific language governing permissions and limitations",
            "# under the License.",
            "#",
            "# variant-semantics-internal-v1",
            "# base_head=e8a13279055752c7c698b772adecda3e6f5673a0",
            "# "
            "columns=case_id\\tsource\\tinput\\tconfig\\tpath\\tpath_parts\\tfield_"
            "shape\\tobservation",
    };
    return header;
}

struct SnapshotRecord {
    std::string case_id;
    std::string source;
    std::string input;
    std::string config;
    std::string path;
    std::string path_parts;
    std::string field_shape;
    std::string observation;

    auto primary_key() const { return std::tie(case_id, path, path_parts); }

    auto key() const {
        return std::tie(case_id, path, path_parts, source, input, config, field_shape, observation);
    }

    auto operator<=>(const SnapshotRecord& other) const { return key() <=> other.key(); }
    bool operator==(const SnapshotRecord& other) const { return key() == other.key(); }
};

std::string bool_string(bool value) {
    return value ? "true" : "false";
}

std::filesystem::path repo_root() {
    const char* root = std::getenv("ROOT");
    if (root == nullptr) {
        throw std::runtime_error("ROOT is required to locate the Variant semantics snapshot");
    }
    const std::filesystem::path configured(root);
    if (!configured.is_absolute()) {
        throw std::runtime_error("ROOT must be an absolute path");
    }
    const std::filesystem::path canonical = std::filesystem::canonical(configured);
    if (configured.lexically_normal() != canonical ||
        !std::filesystem::exists(canonical / ".git") ||
        !std::filesystem::exists(canonical / "be/src/common/config.h") ||
        !std::filesystem::is_directory(canonical / "docs/design/variant_v2/baseline/semantics")) {
        throw std::runtime_error("ROOT must name the canonical Doris repository root");
    }
    return canonical;
}

std::string read_file(const std::filesystem::path& path) {
    std::ifstream input(path, std::ios::binary);
    if (!input) {
        throw std::runtime_error("Cannot open snapshot input " + path.string());
    }
    std::ostringstream contents;
    contents << input.rdbuf();
    if (input.bad()) {
        throw std::runtime_error("Failed while reading snapshot input " + path.string());
    }
    return contents.str();
}

std::string escape_field(std::string_view value) {
    constexpr char HEX[] = "0123456789ABCDEF";
    std::string result;
    result.reserve(value.size());
    for (const unsigned char byte : value) {
        switch (byte) {
        case '\\':
            result.append("\\\\");
            break;
        case '\t':
            result.append("\\t");
            break;
        case '\n':
            result.append("\\n");
            break;
        case '\r':
            result.append("\\r");
            break;
        default:
            if (byte < 0x20 || byte == 0x7F) {
                result.append("\\x");
                result.push_back(HEX[byte >> 4]);
                result.push_back(HEX[byte & 0x0F]);
            } else {
                result.push_back(static_cast<char>(byte));
            }
        }
    }
    return result;
}

uint8_t uppercase_hex_digit(char value) {
    if (value >= '0' && value <= '9') {
        return static_cast<uint8_t>(value - '0');
    }
    if (value >= 'A' && value <= 'F') {
        return static_cast<uint8_t>(value - 'A' + 10);
    }
    throw std::runtime_error("\\x escape must use uppercase hexadecimal digits");
}

std::string unescape_field(std::string_view value) {
    std::string result;
    result.reserve(value.size());
    for (size_t index = 0; index < value.size(); ++index) {
        const unsigned char byte = value[index];
        if (byte != '\\') {
            if (byte < 0x20 || byte == 0x7F) {
                throw std::runtime_error("raw control byte in snapshot field");
            }
            result.push_back(static_cast<char>(byte));
            continue;
        }
        if (++index == value.size()) {
            throw std::runtime_error("trailing backslash in snapshot field");
        }
        switch (value[index]) {
        case '\\':
            result.push_back('\\');
            break;
        case 't':
            result.push_back('\t');
            break;
        case 'n':
            result.push_back('\n');
            break;
        case 'r':
            result.push_back('\r');
            break;
        case 'x': {
            if (index + 2 >= value.size()) {
                throw std::runtime_error("short \\x escape in snapshot field");
            }
            const uint8_t high = uppercase_hex_digit(value[index + 1]);
            const uint8_t low = uppercase_hex_digit(value[index + 2]);
            result.push_back(static_cast<char>((high << 4) | low));
            index += 2;
            break;
        }
        default:
            throw std::runtime_error("unknown escape in snapshot field");
        }
    }
    if (escape_field(result) != value) {
        throw std::runtime_error("noncanonical escape in snapshot field");
    }
    return result;
}

std::vector<std::string_view> split_tabs(std::string_view line) {
    std::vector<std::string_view> result;
    size_t begin = 0;
    while (true) {
        const size_t delimiter = line.find('\t', begin);
        if (delimiter == std::string_view::npos) {
            result.push_back(line.substr(begin));
            return result;
        }
        result.push_back(line.substr(begin, delimiter - begin));
        begin = delimiter + 1;
    }
}

[[noreturn]] void snapshot_error(const std::filesystem::path& path, size_t line,
                                 std::string_view reason) {
    throw std::runtime_error(path.string() + ":" + std::to_string(line) + ": " +
                             std::string(reason));
}

std::vector<SnapshotRecord> parse_snapshot(std::string_view contents,
                                           const std::filesystem::path& path) {
    if (contents.starts_with("\xEF\xBB\xBF")) {
        throw std::runtime_error(path.string() + ": UTF-8 BOM is forbidden");
    }
    if (!validate_utf8(contents.data(), contents.size())) {
        throw std::runtime_error(path.string() + ": snapshot is not valid UTF-8");
    }
    if (contents.empty() || contents.back() != '\n' || contents.find('\r') != std::string::npos) {
        throw std::runtime_error(path.string() + ": snapshot must use LF and end with LF");
    }

    std::vector<std::string_view> lines;
    size_t begin = 0;
    while (begin < contents.size()) {
        const size_t end = contents.find('\n', begin);
        lines.push_back(contents.substr(begin, end - begin));
        begin = end + 1;
    }
    const auto& expected_header = snapshot_header();
    if (lines.size() <= expected_header.size()) {
        throw std::runtime_error(path.string() + ": snapshot contains no records");
    }
    for (size_t index = 0; index < expected_header.size(); ++index) {
        if (lines[index] != expected_header[index]) {
            snapshot_error(path, index + 1, "unexpected or missing snapshot header");
        }
    }

    std::vector<SnapshotRecord> records;
    for (size_t index = expected_header.size(); index < lines.size(); ++index) {
        const std::string_view line = lines[index];
        if (line.empty() || line.starts_with('#')) {
            snapshot_error(path, index + 1, "blank lines and extra comments are forbidden");
        }
        const std::vector<std::string_view> encoded = split_tabs(line);
        if (encoded.size() != 8) {
            snapshot_error(path, index + 1, "record must contain exactly eight columns");
        }
        std::array<std::string, 8> decoded;
        try {
            for (size_t field = 0; field < encoded.size(); ++field) {
                decoded[field] = unescape_field(encoded[field]);
                if (decoded[field].empty()) {
                    throw std::runtime_error("record columns must be non-empty");
                }
            }
        } catch (const std::exception& error) {
            snapshot_error(path, index + 1, error.what());
        }
        SnapshotRecord record {
                .case_id = std::move(decoded[0]),
                .source = std::move(decoded[1]),
                .input = std::move(decoded[2]),
                .config = std::move(decoded[3]),
                .path = std::move(decoded[4]),
                .path_parts = std::move(decoded[5]),
                .field_shape = std::move(decoded[6]),
                .observation = std::move(decoded[7]),
        };
        if (!records.empty() && records.back().primary_key() == record.primary_key()) {
            snapshot_error(path, index + 1,
                           records.back() == record ? "duplicate snapshot record"
                                                    : "ambiguous snapshot primary key");
        }
        if (!records.empty() && !(records.back() < record)) {
            snapshot_error(path, index + 1, "snapshot records are not sorted");
        }
        records.push_back(std::move(record));
    }
    return records;
}

std::string render_snapshot(std::vector<SnapshotRecord> records) {
    std::ranges::sort(records);
    for (size_t index = 1; index < records.size(); ++index) {
        if (records[index - 1].primary_key() == records[index].primary_key()) {
            throw std::runtime_error(
                    records[index - 1] == records[index]
                            ? "collector produced a duplicate snapshot record"
                            : "collector produced an ambiguous snapshot primary key");
        }
    }
    std::string result;
    for (const std::string_view line : snapshot_header()) {
        result.append(line);
        result.push_back('\n');
    }
    for (const SnapshotRecord& record : records) {
        const std::array<std::string_view, 8> fields {
                record.case_id, record.source,     record.input,       record.config,
                record.path,    record.path_parts, record.field_shape, record.observation,
        };
        for (size_t index = 0; index < fields.size(); ++index) {
            if (fields[index].empty()) {
                throw std::runtime_error("collector produced an empty snapshot field");
            }
            if (index != 0) {
                result.push_back('\t');
            }
            result.append(escape_field(fields[index]));
        }
        result.push_back('\n');
    }
    return result;
}

std::string hex_bytes(const char* data, size_t size) {
    constexpr char HEX[] = "0123456789abcdef";
    std::string result;
    result.reserve(size * 2);
    for (size_t index = 0; index < size; ++index) {
        const auto byte = static_cast<uint8_t>(data[index]);
        result.push_back(HEX[byte >> 4]);
        result.push_back(HEX[byte & 0x0F]);
    }
    return result;
}

std::string quoted_path_key(std::string_view key) {
    std::string result = "\"";
    for (const char value : key) {
        if (value == '\\' || value == '\"') {
            result.push_back('\\');
        }
        result.push_back(value);
    }
    result.push_back('\"');
    return result;
}

std::string path_parts_shape(const PathInData& path) {
    std::string result = "[";
    for (size_t index = 0; index < path.get_parts().size(); ++index) {
        if (index != 0) {
            result.push_back(';');
        }
        const auto& part = path.get_parts()[index];
        result.append("key=");
        result.append(quoted_path_key(part.key));
        result.append(",nested=");
        result.append(bool_string(part.is_nested));
        result.append(",anonymous_array_level=");
        result.append(std::to_string(part.anonymous_array_level));
    }
    result.push_back(']');
    return result;
}

std::string field_shape(const Field& field) {
    switch (field.get_type()) {
    case TYPE_NULL:
        return "NULL";
    case TYPE_BIGINT:
        return "BIGINT(" + std::to_string(field.get<TYPE_BIGINT>()) + ")";
    case TYPE_ARRAY: {
        std::string result = "ARRAY[";
        const Array& values = field.get<TYPE_ARRAY>();
        for (size_t index = 0; index < values.size(); ++index) {
            if (index != 0) {
                result.push_back(',');
            }
            result.append(field_shape(values[index]));
        }
        result.push_back(']');
        return result;
    }
    case TYPE_JSONB: {
        const JsonbField& value = field.get<TYPE_JSONB>();
        return "JSONB(hex=" + hex_bytes(value.get_value(), value.get_size()) + ")";
    }
    default:
        return field.get_type_name();
    }
}

void collect_json_parser_records(std::vector<SnapshotRecord>& records) {
    struct ParserSample {
        constexpr ParserSample(std::string_view case_id_, std::string_view input_,
                               bool flatten_nested_)
                : case_id(case_id_), input(input_), flatten_nested(flatten_nested_) {}

        std::string_view case_id;
        std::string_view input;
        bool flatten_nested;
    };
    constexpr std::array samples {
            ParserSample {"json_array_object_member_no_flatten", R"({"a":[{"x":1},{}]})", false},
            ParserSample {"json_array_object_member_flatten", R"({"a":[{"x":1},{}]})", true},
            ParserSample {"json_top_array_object_no_flatten", R"([{"x":1},{}])", false},
            ParserSample {"json_top_array_object_flatten", R"([{"x":1},{}])", true},
            ParserSample {"json_empty_root_object", R"({})", false},
            ParserSample {"json_empty_member_object", R"({"a":{}})", false},
            ParserSample {"json_empty_root_array", R"([])", false},
            ParserSample {"json_empty_member_array", R"({"a":[]})", false},
            ParserSample {"json_single_empty_object_array_no_flatten", R"([{}])", false},
            ParserSample {"json_single_empty_object_array_flatten", R"([{}])", true},
    };
    for (const ParserSample& sample : samples) {
        JSONDataParser<SimdJSONParser> parser;
        ParseConfig config;
        config.deprecated_enable_flatten_nested = sample.flatten_nested;
        const std::string config_shape =
                "deprecated_enable_flatten_nested=" + bool_string(sample.flatten_nested) +
                ";check_duplicate_json_path=false;parse_to=OnlySubcolumns";
        const auto parsed = parser.parse(sample.input.data(), sample.input.size(), config);
        if (!parsed.has_value()) {
            records.push_back({std::string(sample.case_id), "JSONDataParser<SimdJSONParser>",
                               std::string(sample.input), config_shape, "<none>", "[]", "no_field",
                               "parse=failed;records=0"});
            continue;
        }
        if (parsed->paths.size() != parsed->values.size()) {
            throw std::runtime_error("JSONDataParser returned mismatched path/value counts");
        }
        if (parsed->paths.empty()) {
            records.push_back({std::string(sample.case_id), "JSONDataParser<SimdJSONParser>",
                               std::string(sample.input), config_shape, "<none>", "[]", "no_field",
                               "parse=success;records=0"});
            continue;
        }
        for (size_t index = 0; index < parsed->paths.size(); ++index) {
            const PathInData& path = parsed->paths[index];
            records.push_back({std::string(sample.case_id), "JSONDataParser<SimdJSONParser>",
                               std::string(sample.input), config_shape,
                               path.get_path().empty() ? "$" : path.get_path(),
                               path_parts_shape(path), field_shape(parsed->values[index]),
                               "parse=success;records=" + std::to_string(parsed->paths.size())});
        }
    }
}

std::string basic_type_name(VariantBasicType type) {
    switch (type) {
    case VariantBasicType::PRIMITIVE:
        return "PRIMITIVE";
    case VariantBasicType::SHORT_STRING:
        return "SHORT_STRING";
    case VariantBasicType::OBJECT:
        return "OBJECT";
    case VariantBasicType::ARRAY:
        return "ARRAY";
    }
    throw std::runtime_error("unknown Variant basic type");
}

void collect_codec_record(std::vector<SnapshotRecord>& records, std::string case_id,
                          std::string input,
                          const std::function<void(VariantBatchBuilder::Row&)>& append_value) {
    VariantBatchBuilder builder;
    auto row = builder.begin_row();
    append_value(row);
    row.finish();
    VariantBatchBuilder batch = builder.finish_batch();
    const VariantMetadataRef metadata = batch.metadata_ref();
    const VariantRef value_ref = batch.value_at(0);
    const VariantField field = VariantField::from_ref(value_ref);
    const StringRef field_bytes = field.bytes();
    records.push_back(
            {std::move(case_id), "VariantBatchBuilder/VariantField", std::move(input),
             "VARIANT_ENCODING_VERSION=" + std::to_string(VARIANT_ENCODING_VERSION), "$", "[]",
             "VARIANT(metadata_hex=" + hex_bytes(metadata.data, metadata.size) +
                     ";value_hex=" + hex_bytes(value_ref.value.data, value_ref.value.size) +
                     ";field_hex=" + hex_bytes(field_bytes.data, field_bytes.size) + ")",
             "metadata_size=" + std::to_string(metadata.size) +
                     ";value_size=" + std::to_string(value_ref.value.size) +
                     ";field_size=" + std::to_string(field_bytes.size) +
                     ";root_basic_type=" + basic_type_name(value_ref.basic_type())});
}

void collect_codec_records(std::vector<SnapshotRecord>& records) {
    collect_codec_record(records, "codec_null", "builder.add_null()",
                         [](VariantBatchBuilder::Row& builder) { builder.add_null(); });
    collect_codec_record(records, "codec_empty_object", "start_object();finish()",
                         [](VariantBatchBuilder::Row& builder) {
                             auto object = builder.start_object();
                             object.finish();
                         });
    collect_codec_record(records, "codec_empty_array", "start_array();finish()",
                         [](VariantBatchBuilder::Row& builder) {
                             auto array = builder.start_array();
                             array.finish();
                         });
}

size_t regex_count(const std::string& contents, const std::regex& pattern) {
    return static_cast<size_t>(
            std::distance(std::sregex_iterator(contents.begin(), contents.end(), pattern),
                          std::sregex_iterator()));
}

std::string nested_array_json(size_t depth) {
    std::string result;
    result.reserve(depth * 2 + 1);
    result.append(depth, '[');
    result.push_back('0');
    result.append(depth, ']');
    return result;
}

bool simdjson_parse_depth(size_t depth) {
    const std::string input = nested_array_json(depth);
    SimdJSONParser parser;
    SimdJSONParser::Element result;
    return parser.parse(input.data(), input.size(), result);
}

std::pair<bool, size_t> json_data_parser_depth(size_t depth) {
    const std::string input = nested_array_json(depth);
    JSONDataParser<SimdJSONParser> parser;
    ParseConfig config;
    const auto result = parser.parse(input.data(), input.size(), config);
    return {result.has_value(), result.has_value() ? result->paths.size() : 0};
}

void collect_depth_records(const std::filesystem::path& root,
                           std::vector<SnapshotRecord>& records) {
    const std::string config_h = read_file(root / "be/src/common/config.h");
    const std::string config_cpp = read_file(root / "be/src/common/config.cpp");
    const std::regex declaration(R"(\bDECLARE_[A-Za-z0-9_]+\s*\(\s*variant_max_depth\s*\)\s*;)");
    const std::regex definition(R"(\bDEFINE_[A-Za-z0-9_]+\s*\(\s*variant_max_depth\s*,)");
    const std::regex token(R"(\bvariant_max_depth\b)");
    records.push_back(
            {"depth_config_source_scan", "be/src/common/config.h|be/src/common/config.cpp",
             "variant_max_depth", "exact macro/token regex scan", "<none>", "[]", "not_applicable",
             "declaration_matches=" + std::to_string(regex_count(config_h, declaration)) +
                     ";definition_matches=" + std::to_string(regex_count(config_cpp, definition)) +
                     ";config_h_token_matches=" + std::to_string(regex_count(config_h, token)) +
                     ";config_cpp_token_matches=" +
                     std::to_string(regex_count(config_cpp, token))});
    records.push_back({"depth_codec_constant",
                       "be/src/core/value/variant/variant_parquet_encoding.h",
                       "VARIANT_MAX_NESTING_DEPTH", "compiled constant", "<none>", "[]",
                       "not_applicable", "value=" + std::to_string(VARIANT_MAX_NESTING_DEPTH)});
    records.push_back({"depth_simdjson_constant", "simdjson/base.h", "simdjson::DEFAULT_MAX_DEPTH",
                       "compiled dependency constant", "<none>", "[]", "not_applicable",
                       "value=" + std::to_string(simdjson::DEFAULT_MAX_DEPTH)});

    const std::array<size_t, 3> simdjson_depths {
            simdjson::DEFAULT_MAX_DEPTH - 1,
            simdjson::DEFAULT_MAX_DEPTH,
            simdjson::DEFAULT_MAX_DEPTH + 1,
    };
    for (const size_t depth : simdjson_depths) {
        records.push_back({"depth_simdjson_parse_" + std::to_string(depth), "SimdJSONParser::parse",
                           "nested_array_depth=" + std::to_string(depth), "fresh parser", "$", "[]",
                           "not_collected",
                           "parse_success=" + bool_string(simdjson_parse_depth(depth))});
    }
    for (const size_t depth : {size_t {128}, size_t {129}}) {
        const auto [success, record_count] = json_data_parser_depth(depth);
        records.push_back({"depth_json_data_parser_" + std::to_string(depth),
                           "JSONDataParser<SimdJSONParser>::parse",
                           "nested_array_depth=" + std::to_string(depth),
                           "fresh parser;deprecated_enable_flatten_nested=false", "$", "[]",
                           "not_collected",
                           "parse_success=" + bool_string(success) +
                                   ";records=" + std::to_string(record_count)});
    }
}

std::vector<SnapshotRecord> archived_legacy_records(
        const std::vector<SnapshotRecord>& snapshot_records) {
    std::vector<SnapshotRecord> archived;
    std::ranges::copy_if(
            snapshot_records, std::back_inserter(archived),
            [](const SnapshotRecord& record) { return record.case_id.starts_with("legacy_"); });
    if (archived.empty()) {
        throw std::runtime_error("internal snapshot is missing archived legacy records");
    }
    return archived;
}

std::vector<SnapshotRecord> collect_internal_snapshot(
        const std::filesystem::path& root, std::vector<SnapshotRecord> archived_records) {
    std::vector<SnapshotRecord> records = std::move(archived_records);
    collect_json_parser_records(records);
    collect_codec_records(records);
    collect_depth_records(root, records);
    return records;
}

void atomic_write(const std::filesystem::path& target, std::string_view contents) {
    const std::filesystem::path temporary =
            target.string() + ".tmp." + std::to_string(static_cast<long long>(::getpid()));
    int descriptor = ::open(temporary.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0644);
    if (descriptor == -1) {
        throw std::system_error(errno, std::generic_category(),
                                "Cannot create snapshot temporary file " + temporary.string());
    }
    FILE* output = ::fdopen(descriptor, "wb");
    if (output == nullptr) {
        const int error = errno;
        ::close(descriptor);
        ::unlink(temporary.c_str());
        throw std::system_error(
                error, std::generic_category(),
                "Cannot open stream for snapshot temporary file " + temporary.string());
    }
    auto fail = [&](std::string_view operation, int error) -> void {
        if (output != nullptr) {
            ::fclose(output);
            output = nullptr;
        }
        ::unlink(temporary.c_str());
        throw std::system_error(
                error, std::generic_category(),
                std::string(operation) + " snapshot temporary file " + temporary.string());
    };
    if (::fwrite(contents.data(), 1, contents.size(), output) != contents.size()) {
        fail("Cannot write", errno == 0 ? EIO : errno);
    }
    if (::fflush(output) != 0) {
        fail("Cannot flush", errno);
    }
    const int output_descriptor = ::fileno(output);
    if (output_descriptor == -1) {
        fail("Cannot obtain descriptor for", errno);
    }
    if (::fsync(output_descriptor) != 0) {
        fail("Cannot fsync", errno);
    }
    if (::fclose(output) != 0) {
        const int error = errno;
        output = nullptr;
        ::unlink(temporary.c_str());
        throw std::system_error(error, std::generic_category(),
                                "Cannot close snapshot temporary file " + temporary.string());
    }
    output = nullptr;
    if (::rename(temporary.c_str(), target.c_str()) != 0) {
        const int error = errno;
        ::unlink(temporary.c_str());
        throw std::system_error(error, std::generic_category(),
                                "Cannot atomically replace snapshot " + target.string());
    }
}

TEST(VariantSemanticsSnapshotTest, CanonicalTsvCodec) {
    std::string decoded = "slash/and\\";
    decoded.push_back('\t');
    decoded.push_back('\n');
    decoded.push_back('\r');
    decoded.push_back('\x01');
    decoded.push_back('\x1F');
    decoded.push_back('\x7F');
    const std::string encoded = R"(slash/and\\\t\n\r\x01\x1F\x7F)";
    EXPECT_EQ(escape_field(decoded), encoded);
    EXPECT_EQ(unescape_field(encoded), decoded);

    EXPECT_THROW((void)unescape_field("\\unknown"), std::runtime_error);
    EXPECT_THROW((void)unescape_field("\\x1f"), std::runtime_error);
    EXPECT_THROW((void)unescape_field("\\x09"), std::runtime_error);
    EXPECT_THROW((void)unescape_field("\\x0A"), std::runtime_error);
    EXPECT_THROW((void)unescape_field("\\x0D"), std::runtime_error);
    EXPECT_THROW((void)unescape_field("\\x41"), std::runtime_error);

    const SnapshotRecord record {
            .case_id = "codec_self_test",
            .source = "unit test",
            .input = "render then parse",
            .config = "in_memory=true",
            .path = "$",
            .path_parts = "[]",
            .field_shape = "not_applicable",
            .observation = decoded,
    };
    const std::string rendered = render_snapshot({record});
    const std::vector<SnapshotRecord> parsed = parse_snapshot(rendered, "<in-memory>");
    ASSERT_EQ(parsed.size(), 1);
    EXPECT_EQ(parsed.front(), record);
}

TEST(VariantSemanticsSnapshotTest, VerifyInternalGolden) {
    const std::filesystem::path root = repo_root();
    const std::filesystem::path snapshot = root / SNAPSHOT_RELATIVE_PATH;
    const std::string expected = read_file(snapshot);
    const std::vector<SnapshotRecord> expected_records = parse_snapshot(expected, snapshot);
    const std::vector<SnapshotRecord> actual_records =
            collect_internal_snapshot(root, archived_legacy_records(expected_records));
    EXPECT_EQ(expected_records.size(), actual_records.size());
    EXPECT_EQ(expected, render_snapshot(actual_records));
}

TEST(VariantSemanticsSnapshotTest, GenerateInternalGolden) {
    const char* regenerate = std::getenv(REGEN_ENV.data());
    if (regenerate == nullptr) {
        GTEST_SKIP() << "Set " << REGEN_ENV << "=1 to regenerate the snapshot";
    }
    ASSERT_STREQ(regenerate, "1") << REGEN_ENV << " must be exactly 1 when set";

    const std::filesystem::path root = repo_root();
    const std::filesystem::path snapshot = root / SNAPSHOT_RELATIVE_PATH;
    const std::vector<SnapshotRecord> existing_records =
            parse_snapshot(read_file(snapshot), snapshot);
    const std::string rendered = render_snapshot(
            collect_internal_snapshot(root, archived_legacy_records(existing_records)));
    const std::vector<SnapshotRecord> validated = parse_snapshot(rendered, "<generated>");
    ASSERT_FALSE(validated.empty());
    ASSERT_NO_THROW(atomic_write(snapshot, rendered));
}

} // namespace
} // namespace doris
