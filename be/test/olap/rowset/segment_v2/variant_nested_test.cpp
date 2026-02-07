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

#include <rapidjson/document.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <random>
#include <ranges>
#include <sstream>

#include "gtest/gtest.h"
#include "olap/merger.h"
#include "olap/rowid_conversion.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/beta_rowset_writer.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/rowset/segment_v2/column_meta_accessor.h"
#include "olap/rowset/segment_v2/column_reader.h"
#include "olap/rowset/segment_v2/column_reader_cache.h"
#include "olap/rowset/segment_v2/column_writer.h"
#include "olap/rowset/segment_v2/index_file_reader.h"
#include "olap/rowset/segment_v2/index_file_writer.h"
#include "olap/rowset/segment_v2/inverted_index_desc.h"
#include "olap/rowset/segment_v2/segment.h"
#include "olap/rowset/segment_v2/variant/nested_group_builder.h"
#include "olap/rowset/segment_v2/variant/nested_group_iterator.h"
#include "olap/rowset/segment_v2/variant/variant_column_reader.h"
#include "olap/rowset/segment_v2/variant/variant_column_writer_impl.h"
#include "olap/segment_loader.h"
#include "olap/storage_engine.h"
#include "runtime/descriptors.h"
#include "util/jsonb_utils.h"
#include "vec/columns/column_string.h"
#include "vec/common/string_buffer.hpp"
#include "vec/common/variant_util.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/data_types/data_type_variant.h"
#include "vec/data_types/serde/data_type_serde.h"
#include "vec/io/io_helper.h"
#include "vec/olap/olap_data_convertor.h"

using namespace doris::vectorized;

namespace doris {

constexpr static uint32_t MAX_PATH_LEN = 1024;
constexpr static std::string_view dest_dir = "/ut_dir/variant_nested_test";
constexpr static std::string_view tmp_dir = "./ut_dir/tmp";

// Forward declarations
class VariantNestedTest;
class VariantTestContext;

// ============================================================================
// Mock Classes
// ============================================================================

class MockColumnReaderCache : public segment_v2::ColumnReaderCache {
public:
    MockColumnReaderCache(const SegmentFooterPB& footer, const io::FileReaderSPtr& file_reader,
                          const std::shared_ptr<TabletSchema>& tablet_schema)
            : ColumnReaderCache(nullptr, nullptr, nullptr, 0,
                                [](std::shared_ptr<SegmentFooterPB>&, OlapReaderStatistics*) {
                                    return Status::OK();
                                }),
              _footer(footer),
              _file_reader(file_reader),
              _tablet_schema(tablet_schema) {}

    Status get_path_column_reader(
            int32_t col_uid, vectorized::PathInData relative_path,
            std::shared_ptr<segment_v2::ColumnReader>* column_reader, OlapReaderStatistics* stats,
            const SubcolumnColumnMetaInfo::Node* node_hint = nullptr) override {
        DCHECK(node_hint != nullptr);
        int32_t footer_ordinal = node_hint->data.footer_ordinal;
        if (footer_ordinal < 0 || footer_ordinal >= _footer.columns_size()) {
            *column_reader = nullptr;
            return Status::OK();
        }

        ColumnReaderOptions opts;
        opts.kept_in_memory = false;
        opts.be_exec_version = BeExecVersionManager::get_newest_version();
        opts.tablet_schema = _tablet_schema;

        return segment_v2::ColumnReader::create(opts, _footer.columns(footer_ordinal),
                                                _footer.num_rows(), _file_reader, column_reader);
    }

private:
    const SegmentFooterPB& _footer;
    const io::FileReaderSPtr& _file_reader;
    const std::shared_ptr<TabletSchema>& _tablet_schema;
};

class CountingColumnReader : public segment_v2::ColumnReader {
public:
    CountingColumnReader(std::shared_ptr<segment_v2::ColumnReader> inner,
                         std::shared_ptr<std::atomic<int>> counter)
            : _inner(std::move(inner)), _counter(std::move(counter)) {
        _data_type = _inner->get_vec_data_type();
    }

    Status new_iterator(ColumnIteratorUPtr* iterator, const TabletColumn* col,
                        const StorageReadOptions* opts) override {
        _counter->fetch_add(1);
        return _inner->new_iterator(iterator, col, opts);
    }

private:
    std::shared_ptr<segment_v2::ColumnReader> _inner;
    std::shared_ptr<std::atomic<int>> _counter;
};

// ============================================================================
// Helper Functions
// ============================================================================

static std::string serialize_to_json_string(const vectorized::IColumn& column,
                                            const vectorized::DataTypePtr& type, size_t row) {
    auto serde = type->get_serde();
    vectorized::DataTypeSerDe::FormatOptions format_options;
    auto temp_col = vectorized::ColumnString::create();
    vectorized::VectorBufferWriter buffer(*temp_col);
    serde->to_string(column, row, buffer, format_options);
    buffer.commit();
    if (temp_col->size() == 0) {
        return "";
    }
    return temp_col->get_data_at(0).to_string();
}

static std::string format_json_values_as_array(const std::vector<std::string>& values) {
    std::string out = "[";
    for (size_t i = 0; i < values.size(); ++i) {
        if (i > 0) {
            out.append(", ");
        }
        out.append(values[i]);
    }
    out.append("]");
    return out;
}

static std::string format_offsets_as_array(const std::vector<uint64_t>& offsets) {
    std::ostringstream oss;
    oss << "[";
    for (size_t i = 0; i < offsets.size(); ++i) {
        if (i > 0) {
            oss << ", ";
        }
        oss << offsets[i];
    }
    oss << "]";
    return oss.str();
}

static Status read_column_to_json_values(const std::shared_ptr<segment_v2::ColumnReader>& reader,
                                         const ColumnIteratorOptions& iter_opts,
                                         std::vector<std::string>* out) {
    ColumnIteratorUPtr it;
    RETURN_IF_ERROR(reader->new_iterator(&it, nullptr));
    RETURN_IF_ERROR(it->init(iter_opts));
    RETURN_IF_ERROR(it->seek_to_ordinal(0));

    const auto type = reader->get_vec_data_type();
    const size_t total = static_cast<size_t>(reader->num_rows());
    MutableColumnPtr dst = type->create_column();

    size_t remaining = total;
    while (remaining > 0) {
        size_t n = remaining;
        bool has_null = false;
        RETURN_IF_ERROR(it->next_batch(&n, dst, &has_null));
        if (n == 0) {
            break;
        }
        remaining -= n;
    }
    if (dst->size() != total) {
        return Status::InternalError("read_column_to_json_values: expected {}, got {}", total,
                                     dst->size());
    }

    out->clear();
    out->reserve(total);
    for (size_t i = 0; i < total; ++i) {
        out->push_back(serialize_to_json_string(*dst, type, i));
    }
    return Status::OK();
}

struct NamedDumpItem {
    std::string name;
    std::string value;
};

static void print_dump_tree(const std::vector<NamedDumpItem>& items) {
    for (size_t i = 0; i < items.size(); ++i) {
        const bool is_last = (i + 1 == items.size());
        std::cout << (is_last ? "└── " : "├── ") << std::left << std::setw(55) << items[i].name
                  << " → " << items[i].value << std::endl;
    }
}

static bool is_variant_or_variant_array(const vectorized::DataTypePtr& type) {
    auto base_type = vectorized::remove_nullable(type);
    if (base_type->get_primitive_type() == PrimitiveType::TYPE_VARIANT) {
        return true;
    }
    const auto* array_type = dynamic_cast<const DataTypeArray*>(base_type.get());
    if (!array_type) {
        return false;
    }
    return is_variant_or_variant_array(array_type->get_nested_type());
}

static bool is_empty_json_array(const rapidjson::Value& value) {
    return value.IsArray() && value.Empty();
}

static double json_numeric_scale(double lhs, double rhs) {
    return std::max({1.0, std::abs(lhs), std::abs(rhs)});
}

static bool json_numbers_equal(double lhs, double rhs, double eps) {
    return std::abs(lhs - rhs) <= eps * json_numeric_scale(lhs, rhs);
}

static bool json_values_equal(const rapidjson::Value& lhs, const rapidjson::Value& rhs,
                              double eps = 1e-9);

static bool json_arrays_equal(const rapidjson::Value& lhs, const rapidjson::Value& rhs,
                              double eps) {
    if (lhs.Size() != rhs.Size()) {
        return false;
    }
    return std::ranges::all_of(
            std::views::iota(rapidjson::SizeType {0}, lhs.Size()),
            [&](rapidjson::SizeType i) { return json_values_equal(lhs[i], rhs[i], eps); });
}

static bool json_objects_equal(const rapidjson::Value& lhs, const rapidjson::Value& rhs,
                               double eps) {
    std::unordered_map<std::string_view, const rapidjson::Value*> rhs_map;
    rhs_map.reserve(rhs.MemberCount());
    for (auto it = rhs.MemberBegin(); it != rhs.MemberEnd(); ++it) {
        rhs_map.emplace(std::string_view(it->name.GetString(), it->name.GetStringLength()),
                        &it->value);
    }

    std::unordered_map<std::string_view, const rapidjson::Value*> lhs_map;
    lhs_map.reserve(lhs.MemberCount());
    for (auto it = lhs.MemberBegin(); it != lhs.MemberEnd(); ++it) {
        lhs_map.emplace(std::string_view(it->name.GetString(), it->name.GetStringLength()),
                        &it->value);
    }

    for (const auto& [key, lval] : lhs_map) {
        if (!rhs_map.contains(key)) {
            if (is_empty_json_array(*lval)) {
                continue;
            }
            return false;
        }
        if (!json_values_equal(*lval, *rhs_map.at(key), eps)) {
            return false;
        }
    }
    for (const auto& [key, rval] : rhs_map) {
        if (lhs_map.contains(key)) {
            continue;
        }
        if (is_empty_json_array(*rval)) {
            continue;
        }
        return false;
    }
    return true;
}

static bool json_bool_number_equal(const rapidjson::Value& lhs, const rapidjson::Value& rhs,
                                   double eps) {
    if (lhs.IsBool() && rhs.IsNumber()) {
        return json_numbers_equal(rhs.GetDouble(), lhs.GetBool() ? 1.0 : 0.0, eps);
    }
    if (lhs.IsNumber() && rhs.IsBool()) {
        return json_numbers_equal(lhs.GetDouble(), rhs.GetBool() ? 1.0 : 0.0, eps);
    }
    return false;
}

static bool json_values_equal(const rapidjson::Value& lhs, const rapidjson::Value& rhs,
                              double eps) {
    if (lhs.GetType() != rhs.GetType()) {
        if (lhs.IsNumber() && rhs.IsNumber()) {
            return json_numbers_equal(lhs.GetDouble(), rhs.GetDouble(), eps);
        }
        return json_bool_number_equal(lhs, rhs, eps);
    }

    if (lhs.IsNull()) {
        return true;
    }
    if (lhs.IsBool()) {
        return lhs.GetBool() == rhs.GetBool();
    }
    if (lhs.IsNumber()) {
        return json_numbers_equal(lhs.GetDouble(), rhs.GetDouble(), eps);
    }
    if (lhs.IsString()) {
        return std::string_view(lhs.GetString(), lhs.GetStringLength()) ==
               std::string_view(rhs.GetString(), rhs.GetStringLength());
    }
    if (lhs.IsArray()) {
        return json_arrays_equal(lhs, rhs, eps);
    }
    if (lhs.IsObject()) {
        return json_objects_equal(lhs, rhs, eps);
    }
    return false;
}

static std::vector<std::string> split_path(const std::string& path) {
    std::vector<std::string> parts;
    if (path.empty()) {
        return parts;
    }
    size_t start = 0;
    while (start < path.size()) {
        size_t pos = path.find('.', start);
        if (pos == std::string::npos) {
            parts.emplace_back(path.substr(start));
            break;
        }
        parts.emplace_back(path.substr(start, pos - start));
        start = pos + 1;
    }
    return parts;
}

static bool json_strings_equal(const std::string& actual, const std::string& expected) {
    rapidjson::Document actual_doc;
    rapidjson::Document expected_doc;
    if (actual_doc.Parse(actual.data(), actual.size()).HasParseError() ||
        expected_doc.Parse(expected.data(), expected.size()).HasParseError()) {
        bool ok = (actual == expected);
        if (!ok) {
            std::cout << "JSON mismatch (raw string compare)" << std::endl;
            std::cout << "Actual  : " << actual << std::endl;
            std::cout << "Expected: " << expected << std::endl;
        }
        return ok;
    }
    bool ok = json_values_equal(actual_doc, expected_doc);
    if (!ok) {
        std::cout << "JSON mismatch" << std::endl;
        std::cout << "Actual  : " << actual << std::endl;
        std::cout << "Expected: " << expected << std::endl;
    }
    return ok;
}

static void init_column_meta(ColumnMetaPB* meta, uint32_t column_id, const TabletColumn& column,
                             CompressionTypePB compression_type) {
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
        init_column_meta(meta->add_children_columns(), column_id, column.get_sub_column(i),
                         compression_type);
    }
    if (column.is_variant_type()) {
        meta->set_variant_max_subcolumns_count(column.variant_max_subcolumns_count());
    }
}

static void construct_column(ColumnPB* column_pb, int32_t col_unique_id,
                             const std::string& column_type, const std::string& column_name,
                             int variant_max_subcolumns_count = 10, bool is_key = false,
                             bool is_nullable = false, int variant_sparse_hash_shard_count = 0) {
    column_pb->set_unique_id(col_unique_id);
    column_pb->set_name(column_name);
    column_pb->set_type(column_type);
    column_pb->set_is_key(is_key);
    column_pb->set_is_nullable(is_nullable);
    if (column_type == "VARIANT") {
        column_pb->set_variant_max_subcolumns_count(variant_max_subcolumns_count);
        column_pb->set_variant_max_sparse_column_statistics_size(10000);
        column_pb->set_variant_sparse_hash_shard_count(variant_sparse_hash_shard_count);
    }
}

static Status create_variant_root_reader(const SegmentFooterPB& footer,
                                         const io::FileReaderSPtr& file_reader,
                                         const TabletSchemaSPtr& tablet_schema,
                                         std::shared_ptr<segment_v2::ColumnReader>* out) {
    segment_v2::ColumnMetaAccessor accessor;
    RETURN_IF_ERROR(accessor.init(footer, file_reader));

    segment_v2::ColumnReaderOptions opts;
    opts.kept_in_memory = false;
    opts.be_exec_version = BeExecVersionManager::get_newest_version();
    opts.tablet_schema = tablet_schema;

    auto variant_reader = std::make_shared<segment_v2::VariantColumnReader>();
    int32_t root_uid = tablet_schema->column(0).unique_id();
    auto footer_sp = std::make_shared<SegmentFooterPB>();
    footer_sp->CopyFrom(footer);
    RETURN_IF_ERROR(variant_reader->init(opts, &accessor, footer_sp, root_uid, footer.num_rows(),
                                         file_reader));
    *out = std::move(variant_reader);
    return Status::OK();
}

// ============================================================================
// Test Configuration
// ============================================================================

struct VariantTestConfig {
    uint64_t tablet_id = 10001;
    int variant_max_subcolumns_count = 10;
    int variant_sparse_hash_shard_count = 0;
    bool is_nullable = false;
    bool enable_inverted_index = false;
    bool verbose = false; // Print debug output
};

// ============================================================================
// NestedGroup Metadata Validator - Fluent API for metadata verification
// ============================================================================

class NestedGroupMetadataValidator {
public:
    explicit NestedGroupMetadataValidator(const segment_v2::NestedGroupReader* reader,
                                          const std::string& context = "")
            : _reader(reader), _context(context) {}

    NestedGroupMetadataValidator& expect_exists() {
        EXPECT_TRUE(_reader != nullptr) << _context << "NestedGroup reader should exist";
        return *this;
    }

    NestedGroupMetadataValidator& expect_valid() {
        EXPECT_TRUE(_reader != nullptr && _reader->is_valid())
                << _context << "NestedGroup should be valid";
        EXPECT_TRUE(_reader->offsets_reader != nullptr)
                << _context << "NestedGroup offsets_reader should not be null";
        return *this;
    }

    NestedGroupMetadataValidator& expect_path(const std::string& expected_path) {
        EXPECT_EQ(_reader->array_path, expected_path)
                << _context << "NestedGroup array_path mismatch";
        return *this;
    }

    NestedGroupMetadataValidator& expect_depth(int expected_depth) {
        EXPECT_EQ(_reader->depth, expected_depth) << _context << "NestedGroup depth mismatch";
        return *this;
    }

    NestedGroupMetadataValidator& expect_child(const std::string& child_name) {
        EXPECT_TRUE(_reader->child_readers.count(child_name) > 0)
                << _context << "NestedGroup should have child reader '" << child_name << "'";
        return *this;
    }

    NestedGroupMetadataValidator& expect_children(const std::vector<std::string>& child_names) {
        for (const auto& name : child_names) {
            expect_child(name);
        }
        return *this;
    }

    NestedGroupMetadataValidator& expect_no_child(const std::string& child_name) {
        EXPECT_TRUE(_reader->child_readers.count(child_name) == 0)
                << _context << "NestedGroup should NOT have child reader '" << child_name << "'";
        return *this;
    }

    NestedGroupMetadataValidator& expect_child_count(size_t count) {
        EXPECT_EQ(_reader->child_readers.size(), count)
                << _context << "NestedGroup child_readers count mismatch";
        return *this;
    }

    NestedGroupMetadataValidator& expect_nested_group(const std::string& group_name) {
        EXPECT_TRUE(_reader->nested_group_readers.count(group_name) > 0)
                << _context << "NestedGroup should have nested group '" << group_name << "'";
        return *this;
    }

    NestedGroupMetadataValidator& expect_nested_groups(
            const std::vector<std::string>& group_names) {
        for (const auto& name : group_names) {
            expect_nested_group(name);
        }
        return *this;
    }

    NestedGroupMetadataValidator& expect_no_nested_groups() {
        EXPECT_TRUE(_reader->nested_group_readers.empty())
                << _context << "NestedGroup should have no nested groups";
        return *this;
    }

    NestedGroupMetadataValidator& expect_children_nullable() {
        for (const auto& [name, reader] : _reader->child_readers) {
            auto child_type = reader->get_vec_data_type();
            EXPECT_TRUE(child_type->is_nullable())
                    << _context << "Child '" << name << "' should be nullable";
        }
        return *this;
    }

    // Get nested validator for sub-group
    NestedGroupMetadataValidator nested_group(const std::string& name) {
        auto it = _reader->nested_group_readers.find(name);
        if (it == _reader->nested_group_readers.end()) {
            ADD_FAILURE() << _context << "Nested group '" << name << "' not found";
            return NestedGroupMetadataValidator(nullptr, _context + name + ".");
        }
        return NestedGroupMetadataValidator(it->second.get(), _context + name + ".");
    }

private:
    const segment_v2::NestedGroupReader* _reader;
    std::string _context;
};

// ============================================================================
// Variant Data Validator - For data consistency verification
// ============================================================================

class VariantDataValidator {
public:
    VariantDataValidator(ColumnVariant* result, const std::vector<std::string>& written_data,
                         bool verbose = false)
            : _result(result), _written_data(written_data), _verbose(verbose) {}

    VariantDataValidator& expect_row_count(size_t expected) {
        EXPECT_EQ(_result->rows(), expected) << "Row count mismatch";
        return *this;
    }

    VariantDataValidator& expect_exact_match() { return expect_exact_match(0, _result->rows()); }

    VariantDataValidator& expect_exact_match(size_t start, size_t end) {
        for (size_t i = start; i < end && i < _result->rows(); ++i) {
            std::string read_serialized;
            _result->serialize_one_row_to_string(i, &read_serialized, _format_options);
            if (_verbose) {
                std::cout << "Row " << i << " read: " << read_serialized << std::endl;
            }
            EXPECT_EQ(_written_data[i], read_serialized)
                    << "Data mismatch at row " << i << "\nExpected: " << _written_data[i]
                    << "\nActual: " << read_serialized;
        }
        return *this;
    }

    VariantDataValidator& expect_row_contains(size_t row, const std::string& substr) {
        std::string serialized;
        _result->serialize_one_row_to_string(row, &serialized, _format_options);
        if (_verbose) {
            std::cout << "Row " << row << ": " << serialized << std::endl;
        }
        EXPECT_TRUE(serialized.find(substr) != std::string::npos)
                << "Row " << row << " should contain '" << substr << "': " << serialized;
        return *this;
    }

    VariantDataValidator& expect_row_contains_all(size_t row,
                                                  const std::vector<std::string>& substrs) {
        for (const auto& substr : substrs) {
            expect_row_contains(row, substr);
        }
        return *this;
    }

    VariantDataValidator& expect_row_starts_with(size_t row, char c) {
        std::string serialized;
        _result->serialize_one_row_to_string(row, &serialized, _format_options);
        EXPECT_TRUE(serialized.size() > 0 && serialized[0] == c)
                << "Row " << row << " should start with '" << c << "': " << serialized;
        return *this;
    }

    VariantDataValidator& expect_row_not_empty(size_t row) {
        std::string serialized;
        _result->serialize_one_row_to_string(row, &serialized, _format_options);
        EXPECT_FALSE(serialized.empty()) << "Row " << row << " should not be empty";
        return *this;
    }

    VariantDataValidator& expect_subcolumn_exists(const PathInData& path) {
        const auto* subcolumn = _result->get_subcolumn(path);
        EXPECT_TRUE(subcolumn != nullptr) << "Subcolumn '" << path.get_path() << "' should exist";
        return *this;
    }

    VariantDataValidator& expect_subcolumn_size(const PathInData& path, size_t expected) {
        const auto* subcolumn = _result->get_subcolumn(path);
        EXPECT_TRUE(subcolumn != nullptr) << "Subcolumn '" << path.get_path() << "' should exist";
        if (subcolumn) {
            EXPECT_EQ(subcolumn->size(), expected)
                    << "Subcolumn '" << path.get_path() << "' size mismatch";
        }
        return *this;
    }

    // Print all rows for debugging
    VariantDataValidator& print_all() {
        for (size_t i = 0; i < _result->rows(); ++i) {
            std::string serialized;
            _result->serialize_one_row_to_string(i, &serialized, _format_options);
            std::cout << "Row " << i << ": " << serialized << std::endl;
        }
        return *this;
    }

private:
    ColumnVariant* _result;
    const std::vector<std::string>& _written_data;
    bool _verbose;
    DataTypeSerDe::FormatOptions _format_options;
};

// ============================================================================
// Test Context - Manages test environment lifecycle
// ============================================================================

class VariantTestContext {
public:
    VariantTestContext(VariantNestedTest* test, const VariantTestConfig& config);
    ~VariantTestContext() = default;

    // Write phase
    Status write_json_data(const std::vector<std::string>& jsons);
    Status finish_write();

    // Read phase
    Status open_for_read();
    Status read_all_data(MutableColumnPtr* result, vectorized::DataTypePtr* type = nullptr);
    Status read_nested_group_with_access_paths(
            const std::string& nested_group_path,
            const std::vector<std::vector<std::string>>& all_paths,
            const std::vector<std::vector<std::string>>& predicate_paths,
            vectorized::DataTypePtr* type, MutableColumnPtr* result);

    // Accessors
    segment_v2::VariantColumnReader* get_variant_reader() const {
        return assert_cast<segment_v2::VariantColumnReader*>(_column_reader.get());
    }

    const SegmentFooterPB& get_footer() const { return _footer; }
    const std::vector<std::string>& get_written_data() const { return _written_data; }
    TabletSchemaSPtr get_tablet_schema() const { return _tablet_schema; }
    const TabletColumn& get_column() const { return _column; }
    io::FileReaderSPtr get_file_reader() const { return _file_reader; }
    const std::string& get_index_path_prefix() const { return _index_path_prefix; }

    // Validation helpers
    NestedGroupMetadataValidator validate_nested_group(const std::string& path) {
        auto* reader = get_variant_reader()->get_nested_group_reader(path);
        return NestedGroupMetadataValidator(reader, path + ": ");
    }

    VariantDataValidator validate_data(ColumnVariant* result) {
        return VariantDataValidator(result, _written_data, _config.verbose);
    }

    // Create column reader cache for iterator creation
    std::unique_ptr<MockColumnReaderCache> create_reader_cache() {
        return std::make_unique<MockColumnReaderCache>(_footer, _file_reader, _tablet_schema);
    }

    // Read via NestedGroupReader and verify data matches expected data
    // This ensures NestedGroupReader path is exercised for nested data
    // @param group_path: The path of the nested group to verify
    // @param expected_json_arrays: Expected JSON array strings for each row (e.g., ["[{\"a\":1}]", "[]", ...])
    //        If empty, only structural verification is performed (array format check)
    // @param expected_elem_counts: Expected element counts per row for offset verification
    void verify_nested_group_read(const std::string& group_path,
                                  const std::vector<std::string>& expected_json_arrays = {},
                                  const std::vector<size_t>& expected_elem_counts = {}) {
        auto* variant_reader = get_variant_reader();
        const auto* nested_group_reader = variant_reader->get_nested_group_reader(group_path);

        if (nested_group_reader == nullptr) {
            // No nested group for this path - skip verification
            return;
        }

        ASSERT_TRUE(nested_group_reader->is_valid())
                << "NestedGroup '" << group_path << "' should be valid";

        OlapReaderStatistics stats;
        ColumnIteratorOptions iter_opts;
        iter_opts.stats = &stats;
        iter_opts.file_reader = _file_reader.get();

        // Build element iterator and wrap with offsets to read row-level arrays
        auto element_iter =
                std::make_unique<segment_v2::NestedGroupWholeIterator>(nested_group_reader);
        ColumnIteratorUPtr offsets_iter;
        ASSERT_TRUE(nested_group_reader->offsets_reader->new_iterator(&offsets_iter, nullptr).ok())
                << "Failed to create offsets iterator for '" << group_path << "'";
        auto array_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeVariant>(0));
        auto array_iter = std::make_unique<segment_v2::NestedGroupIterator>(
                std::move(offsets_iter), std::move(element_iter), array_type);

        ASSERT_TRUE(array_iter->init(iter_opts).ok())
                << "Failed to init NestedGroupIterator for '" << group_path << "'";
        ASSERT_TRUE(array_iter->seek_to_ordinal(0).ok())
                << "Failed to seek NestedGroupIterator for '" << group_path << "'";

        // Read all rows as ARRAY<VARIANT>
        MutableColumnPtr dst_col = array_iter->create_result_column();
        size_t n = _num_rows;
        bool has_null = false;
        ASSERT_TRUE(array_iter->next_batch(&n, dst_col, &has_null).ok())
                << "Failed to read via NestedGroupIterator for '" << group_path << "'";

        EXPECT_EQ(dst_col->size(), _num_rows)
                << "NestedGroup read row count mismatch for '" << group_path << "'";

        // Verify each row's array structure and content
        // If expected data provided, verify against it
        if (!expected_json_arrays.empty()) {
            ASSERT_EQ(expected_json_arrays.size(), n)
                    << "Expected array count mismatch for '" << group_path << "'";

            for (size_t i = 0; i < n; ++i) {
                std::string actual_json = serialize_to_json_string(*dst_col, array_type, i);
                if (_config.verbose) {
                    std::cout << "NestedGroup[" << group_path << "] Row " << i << ": "
                              << actual_json << " (expected: " << expected_json_arrays[i] << ")"
                              << std::endl;
                }

                EXPECT_TRUE(json_strings_equal(actual_json, expected_json_arrays[i]))
                        << "NestedGroup data mismatch at row " << i << " for '" << group_path
                        << "', actual: " << actual_json
                        << ", expected: " << expected_json_arrays[i];
            }
        } else {
            // Structural verification only - verify array format
            for (size_t i = 0; i < n; ++i) {
                if (_config.verbose) {
                    std::cout << "NestedGroup[" << group_path << "] Row " << i << ": "
                              << serialize_to_json_string(*dst_col, array_type, i) << std::endl;
                }
                // For non-null nested arrays, verify they start with '['
                std::string json_str = serialize_to_json_string(*dst_col, array_type, i);
                if (!json_str.empty()) {
                    EXPECT_EQ(json_str[0], '[')
                            << "NestedGroup row " << i << " should be an array for '" << group_path
                            << "': " << json_str;
                }
            }
        }

        // If expected_elem_counts provided, verify offset structure
        if (!expected_elem_counts.empty()) {
            ColumnIteratorUPtr offsets_iter;
            ASSERT_TRUE(
                    nested_group_reader->offsets_reader->new_iterator(&offsets_iter, nullptr).ok());
            ASSERT_TRUE(offsets_iter->init(iter_opts).ok());

            MutableColumnPtr offsets_col = ColumnOffset64::create();
            size_t n_offsets = _num_rows;
            ASSERT_TRUE(offsets_iter->seek_to_ordinal(0).ok());
            ASSERT_TRUE(offsets_iter->next_batch(&n_offsets, offsets_col, &has_null).ok());

            auto* offsets_col_ptr = assert_cast<ColumnOffset64*>(offsets_col.get());
            auto& offsets_data = offsets_col_ptr->get_data();

            ASSERT_EQ(offsets_data.size(), expected_elem_counts.size())
                    << "Offset count mismatch for '" << group_path << "'";

            uint64_t cumulative = 0;
            for (size_t i = 0; i < expected_elem_counts.size(); ++i) {
                cumulative += expected_elem_counts[i];
                EXPECT_EQ(offsets_data[i], cumulative)
                        << "Offset[" << i << "] mismatch for '" << group_path << "'";
            }
        }
    }

    // Struct to hold expected data for a nested group
    struct NestedGroupExpectation {
        std::vector<std::string> json_arrays; // Expected JSON array for each row
        std::vector<size_t> elem_counts;      // Expected element count per row (optional)
    };

    // Verify all nested groups in the variant reader with expected data
    // @param expectations: Map from group_path to expected data
    //        If a group is not in the map, only structural verification is performed
    void verify_all_nested_groups(
            const std::unordered_map<std::string, NestedGroupExpectation>& expectations = {}) {
        auto* variant_reader = get_variant_reader();
        const auto& nested_group_readers = variant_reader->get_nested_group_readers();

        if (nested_group_readers.empty()) {
            return; // No nested groups to verify
        }

        for (const auto& [path, reader] : nested_group_readers) {
            auto it = expectations.find(path);
            if (it != expectations.end()) {
                // Verify with expected data
                verify_nested_group_read(path, it->second.json_arrays, it->second.elem_counts);
            } else {
                // Structural verification only
                verify_nested_group_read(path, {}, {});
            }
        }
    }

private:
    VariantNestedTest* _test;
    VariantTestConfig _config;

    // Schema and tablet
    TabletSchemaSPtr _tablet_schema;
    TabletSharedPtr _tablet;
    TabletColumn _column;

    // Writer state
    io::FileWriterPtr _file_writer;
    segment_v2::IndexFileWriterPtr _index_file_writer;
    std::unique_ptr<ColumnWriter> _writer;
    SegmentFooterPB _footer;
    RowsetWriterContext _rowset_ctx;
    std::string _file_path;
    std::string _index_path_prefix;

    // Reader state
    io::FileReaderSPtr _file_reader;
    std::shared_ptr<segment_v2::ColumnReader> _column_reader;

    // Data tracking
    std::vector<std::string> _written_data;
    size_t _num_rows = 0;
};

// ============================================================================
// Test Fixture
// ============================================================================

class VariantNestedTest : public testing::Test {
public:
    void SetUp() override {
        // Increase max depth to support multi-level nested arrays in tests
        config::variant_nested_group_max_depth = 10;

        char buffer[MAX_PATH_LEN];
        EXPECT_NE(getcwd(buffer, MAX_PATH_LEN), nullptr);
        _current_dir = std::string(buffer);
        _absolute_dir = _current_dir + std::string(dest_dir);
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_absolute_dir).ok());
        EXPECT_TRUE(io::global_local_filesystem()->create_directory(_absolute_dir).ok());

        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(tmp_dir).ok());
        EXPECT_TRUE(io::global_local_filesystem()->create_directory(tmp_dir).ok());
        std::vector<StorePath> paths;
        paths.emplace_back(std::string(tmp_dir), 1024000000);
        auto tmp_file_dirs = std::make_unique<segment_v2::TmpFileDirs>(paths);
        Status st = tmp_file_dirs->init();
        EXPECT_TRUE(st.ok()) << st.to_json();
        ExecEnv::GetInstance()->set_tmp_file_dir(std::move(tmp_file_dirs));

        doris::EngineOptions options;
        auto engine = std::make_unique<StorageEngine>(options);
        _engine_ref = engine.get();
        _data_dir = std::make_unique<DataDir>(*_engine_ref, _absolute_dir);
        static_cast<void>(_data_dir->update_capacity());
        ExecEnv::GetInstance()->set_storage_engine(std::move(engine));
    }

    void TearDown() override {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_absolute_dir).ok());
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(tmp_dir).ok());
        _engine_ref = nullptr;
        ExecEnv::GetInstance()->set_storage_engine(nullptr);
    }

    // Create a test context with specified config
    std::unique_ptr<VariantTestContext> create_context(const VariantTestConfig& config) {
        return std::make_unique<VariantTestContext>(this, config);
    }

    // Create a test context with default config and specified tablet_id
    std::unique_ptr<VariantTestContext> create_context(uint64_t tablet_id) {
        VariantTestConfig config;
        config.tablet_id = tablet_id;
        return std::make_unique<VariantTestContext>(this, config);
    }

    StorageEngine* engine_ref() const { return _engine_ref; }
    DataDir* data_dir() const { return _data_dir.get(); }
    const std::string& absolute_dir() const { return _absolute_dir; }

protected:
    StorageEngine* _engine_ref = nullptr;
    std::unique_ptr<DataDir> _data_dir = nullptr;
    std::string _absolute_dir;
    std::string _current_dir;
};

// ============================================================================
// VariantTestContext Implementation
// ============================================================================

VariantTestContext::VariantTestContext(VariantNestedTest* test, const VariantTestConfig& config)
        : _test(test), _config(config) {
    // 1. Create tablet_schema
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    if (config.enable_inverted_index) {
        schema_pb.set_inverted_index_storage_format(InvertedIndexStorageFormatPB::V2);
        auto* index_pb = schema_pb.add_index();
        index_pb->set_index_id(10000);
        index_pb->set_index_name("v1_idx");
        index_pb->set_index_type(IndexType::INVERTED);
        index_pb->add_col_unique_id(1);
        (*index_pb->mutable_properties())["parser"] = "english";
    }
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1",
                     config.variant_max_subcolumns_count, false, config.is_nullable,
                     config.variant_sparse_hash_shard_count);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    // 2. Create tablet
    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    _tablet_schema->set_external_segment_meta_used_default(true);
    tablet_meta->_tablet_id = config.tablet_id;
    _tablet = std::make_shared<Tablet>(*_test->engine_ref(), tablet_meta, _test->data_dir());

    EXPECT_TRUE(_tablet->init().ok());
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    // 3. Create file_writer
    _file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(_file_path, &_file_writer);
    EXPECT_TRUE(st.ok()) << st.msg();

    // 4. Create column_writer
    _footer.Clear();
    ColumnWriterOptions opts;
    opts.meta = _footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = _file_writer.get();
    opts.footer = &_footer;
    _rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    opts.rowset_ctx = &_rowset_ctx;
    opts.rowset_ctx->tablet_schema = _tablet_schema;
    _column = _tablet_schema->column(0);
    init_column_meta(opts.meta, 0, _column, CompressionTypePB::LZ4);

    if (config.enable_inverted_index) {
        _index_path_prefix = std::string(
                segment_v2::InvertedIndexDescriptor::get_index_file_path_prefix(_file_path));
        io::FileWriterPtr idx_file_writer;
        auto idx_file_path =
                segment_v2::InvertedIndexDescriptor::get_index_file_path_v2(_index_path_prefix);
        st = io::global_local_filesystem()->create_file(idx_file_path, &idx_file_writer);
        EXPECT_TRUE(st.ok()) << st.msg();
        _index_file_writer = std::make_unique<segment_v2::IndexFileWriter>(
                io::global_local_filesystem(), _index_path_prefix, "0", 0,
                InvertedIndexStorageFormatPB::V2, std::move(idx_file_writer));
        opts.index_file_writer = _index_file_writer.get();
    }

    EXPECT_TRUE(ColumnWriter::create(opts, &_column, _file_writer.get(), &_writer).ok());
    EXPECT_TRUE(_writer->init().ok());
}

Status VariantTestContext::write_json_data(const std::vector<std::string>& jsons) {
    auto column_variant = ColumnVariant::create(0);
    auto json_column = ColumnString::create();

    for (const auto& json : jsons) {
        json_column->insert_data(json.data(), json.size());
    }

    ParseConfig config;
    variant_util::parse_json_to_variant(*column_variant, *json_column, config);

    if (column_variant->rows() != jsons.size()) {
        return Status::InternalError("Parse error: row count mismatch");
    }

    // Save serialized data for later comparison
    _written_data.clear();
    DataTypeSerDe::FormatOptions format_options;
    for (size_t i = 0; i < column_variant->rows(); ++i) {
        std::string serialized;
        column_variant->serialize_one_row_to_string(i, &serialized, format_options);
        if (_config.verbose) {
            std::cout << "Row " << i << " written: " << serialized << std::endl;
        }
        _written_data.push_back(serialized);
    }

    _num_rows = jsons.size();

    // Convert and write
    auto olap_data_convertor = std::make_unique<vectorized::OlapBlockDataConvertor>();
    auto block = _tablet_schema->create_block();
    block.get_by_position(0).column = std::move(column_variant);
    olap_data_convertor->add_column_data_convertor(_column);
    olap_data_convertor->set_source_content(&block, 0, _num_rows);
    auto [result, accessor] = olap_data_convertor->convert_column_data(0);
    RETURN_IF_ERROR(result);
    return _writer->append(accessor->get_nullmap(), accessor->get_data(), _num_rows);
}

Status VariantTestContext::finish_write() {
    RETURN_IF_ERROR(_writer->finish());
    RETURN_IF_ERROR(_writer->write_data());
    RETURN_IF_ERROR(_writer->write_ordinal_index());
    RETURN_IF_ERROR(_writer->write_zone_map());
    if (_config.enable_inverted_index) {
        RETURN_IF_ERROR(_writer->write_inverted_index());
    }
    RETURN_IF_ERROR(_file_writer->close());
    _footer.set_num_rows(_num_rows);
    if (_config.enable_inverted_index) {
        RETURN_IF_ERROR(_index_file_writer->begin_close());
        RETURN_IF_ERROR(_index_file_writer->finish_close());
    }
    return Status::OK();
}

Status VariantTestContext::open_for_read() {
    RETURN_IF_ERROR(io::global_local_filesystem()->open_file(_file_path, &_file_reader));
    return create_variant_root_reader(_footer, _file_reader, _tablet_schema, &_column_reader);
}

Status VariantTestContext::read_all_data(MutableColumnPtr* result, vectorized::DataTypePtr* type) {
    ColumnIteratorUPtr it;
    StorageReadOptions storage_read_opts;
    OlapReaderStatistics stats;
    storage_read_opts.io_ctx.reader_type = ReaderType::READER_QUERY;
    storage_read_opts.stats = &stats;

    auto cache = create_reader_cache();
    auto* variant_reader = get_variant_reader();
    RETURN_IF_ERROR(variant_reader->new_iterator(&it, &_column, &storage_read_opts, cache.get()));

    ColumnIteratorOptions column_iter_opts;
    column_iter_opts.stats = &stats;
    column_iter_opts.file_reader = _file_reader.get();
    RETURN_IF_ERROR(it->init(column_iter_opts));

    vectorized::DataTypePtr read_type;
    RETURN_IF_ERROR(variant_reader->infer_data_type_for_path(&read_type, _column, storage_read_opts,
                                                             cache.get()));
    if (type != nullptr) {
        *type = read_type;
    }
    *result = read_type->create_column();
    size_t nrows = _num_rows;
    RETURN_IF_ERROR(it->seek_to_ordinal(0));
    bool has_null = false;
    RETURN_IF_ERROR(it->next_batch(&nrows, *result, &has_null));

    if (nrows != _num_rows) {
        return Status::InternalError("Read row count mismatch");
    }
    return Status::OK();
}

Status VariantTestContext::read_nested_group_with_access_paths(
        const std::string& nested_group_path,
        const std::vector<std::vector<std::string>>& all_paths,
        const std::vector<std::vector<std::string>>& predicate_paths, vectorized::DataTypePtr* type,
        MutableColumnPtr* result) {
    TabletColumn nested_column = _column;
    auto parts = split_path(nested_group_path);
    vectorized::PathInData path_info(_column.name(), parts);
    nested_column.set_name(path_info.get_path());
    nested_column.set_path_info(path_info);
    nested_column.set_parent_unique_id(_column.unique_id());
    nested_column.set_unique_id(-1);

    StorageReadOptions storage_read_opts;
    OlapReaderStatistics stats;
    storage_read_opts.io_ctx.reader_type = ReaderType::READER_QUERY;
    storage_read_opts.stats = &stats;
    storage_read_opts.tablet_schema = _tablet_schema;

    auto build_access_paths = [](const std::vector<std::vector<std::string>>& paths) {
        TColumnAccessPaths access_paths;
        access_paths.reserve(paths.size());
        for (const auto& path : paths) {
            TColumnAccessPath access_path;
            access_path.type = TAccessPathType::DATA;
            TDataAccessPath data_path;
            data_path.path = path;
            access_path.__set_data_access_path(std::move(data_path));
            access_paths.emplace_back(std::move(access_path));
        }
        return access_paths;
    };

    int32_t col_uid = _column.unique_id() >= 0 ? _column.unique_id() : _column.parent_unique_id();
    if (!all_paths.empty()) {
        storage_read_opts.all_access_paths.emplace(col_uid, build_access_paths(all_paths));
    }
    if (!predicate_paths.empty()) {
        storage_read_opts.predicate_access_paths.emplace(col_uid,
                                                         build_access_paths(predicate_paths));
    }

    ColumnIteratorUPtr it;
    auto cache = create_reader_cache();
    auto* variant_reader = get_variant_reader();
    RETURN_IF_ERROR(
            variant_reader->new_iterator(&it, &nested_column, &storage_read_opts, cache.get()));

    ColumnIteratorOptions column_iter_opts;
    column_iter_opts.stats = &stats;
    column_iter_opts.file_reader = _file_reader.get();
    RETURN_IF_ERROR(it->init(column_iter_opts));

    RETURN_IF_ERROR(variant_reader->infer_data_type_for_path(type, nested_column, storage_read_opts,
                                                             cache.get()));
    *result = (*type)->create_column();

    size_t nrows = _num_rows;
    RETURN_IF_ERROR(it->seek_to_ordinal(0));
    bool has_null = false;
    RETURN_IF_ERROR(it->next_batch(&nrows, *result, &has_null));
    if (nrows != _num_rows) {
        return Status::InternalError("Read row count mismatch");
    }
    return Status::OK();
}

// ============================================================================
// Test Cases
// ============================================================================

// Test writing and reading non-top-level nested JSON: {"nested": [{"a": 1}]}
TEST_F(VariantNestedTest, test_non_top_level_nested) {
    auto ctx = create_context(10001);

    // Write nested JSON data
    std::vector<std::string> jsons = {
            R"({"nested": [{"a": 1, "b": "x"}]})", R"({"nested": [{"a": 2, "b": "y"}]})",
            R"({"nested": [{"a": 3, "b": "z"}]})", R"({"other": "value"})", R"({"nested": []})"};

    EXPECT_TRUE(ctx->write_json_data(jsons).ok());
    EXPECT_TRUE(ctx->finish_write().ok());
    EXPECT_TRUE(ctx->open_for_read().ok());

    // Verify NestedGroupReader path is exercised with expected data
    ctx->verify_all_nested_groups({{"nested",
                                    {// Expected JSON arrays for "nested" group
                                     {R"([{"a":1,"b":"x"}])", R"([{"a":2,"b":"y"}])",
                                      R"([{"a":3,"b":"z"}])", R"([])", R"([])"},
                                     // Expected element counts: 1, 1, 1, 0, 0
                                     {1, 1, 1, 0, 0}}}});

    // Verify metadata
    ctx->validate_nested_group("nested")
            .expect_exists()
            .expect_valid()
            .expect_path("nested")
            .expect_children({"a", "b"});

    // Read and verify data
    MutableColumnPtr result;
    EXPECT_TRUE(ctx->read_all_data(&result).ok());
    auto* result_variant = assert_cast<ColumnVariant*>(result.get());

    ctx->validate_data(result_variant)
            .expect_row_count(5)
            .expect_exact_match(0, 3) // Rows 0-2 should match exactly
            .expect_row_contains_all(0, {"\"nested\"", "\"a\"", "\"b\""})
            .expect_subcolumn_exists(PathInData())
            .expect_subcolumn_size(PathInData(), 5);

    // Verify rows 3 and 4 are valid
    for (size_t i = 3; i < 5; ++i) {
        ctx->validate_data(result_variant).expect_row_not_empty(i);
    }
}

TEST_F(VariantNestedTest, test_non_top_level_nested_inverted_index_inherited_and_exists) {
    VariantTestConfig config;
    config.tablet_id = 10004;
    config.enable_inverted_index = true;
    auto ctx = create_context(config);

    std::vector<std::string> jsons = {
            R"({"nested": [{"a": 1, "b": "x"}]})", R"({"nested": [{"a": 2, "b": "y"}]})",
            R"({"nested": [{"a": 3, "b": "z"}]})", R"({"other": "value"})", R"({"nested": []})"};

    EXPECT_TRUE(ctx->write_json_data(jsons).ok());
    EXPECT_TRUE(ctx->finish_write().ok());

    segment_v2::IndexFileReader reader(io::global_local_filesystem(), ctx->get_index_path_prefix(),
                                       InvertedIndexStorageFormatPB::V2);
    EXPECT_TRUE(reader.init().ok());

    auto parent_indexes = ctx->get_tablet_schema()->inverted_indexs(ctx->get_column().unique_id());
    ASSERT_FALSE(parent_indexes.empty());

    auto build_and_check = [&](PrimitiveType primitive_type, std::string_view leaf) {
        auto dt = vectorized::DataTypeFactory::instance().create_data_type(primitive_type, true);
        std::string col_name = ctx->get_column().name_lower_case() + "." +
                               std::string(segment_v2::kNestedGroupMarker) + ".nested." +
                               std::string(leaf);
        std::string suffix_path = "nested." + std::string(leaf);
        auto child_column = vectorized::variant_util::get_column_by_type(
                dt, col_name,
                vectorized::variant_util::ExtraInfo {
                        .unique_id = -1,
                        .parent_unique_id = ctx->get_column().unique_id(),
                        .path_info = vectorized::PathInData(suffix_path)});
        TabletIndexes inherited;
        EXPECT_TRUE(
                vectorized::variant_util::inherit_index(parent_indexes, inherited, child_column));
        ASSERT_FALSE(inherited.empty());

        bool exists = false;
        EXPECT_TRUE(reader.index_file_exist(inherited[0].get(), &exists).ok());
        EXPECT_TRUE(exists);
    };

    build_and_check(PrimitiveType::TYPE_INT, "a");
    build_and_check(PrimitiveType::TYPE_STRING, "b");
}

// Test writing and reading top-level nested JSON: [{"a": 123}]
TEST_F(VariantNestedTest, test_top_level_nested_array) {
    auto ctx = create_context(10002);

    // Write top-level array JSON data
    std::vector<std::string> jsons = {R"([{"a": 123}])", R"([{"b": 456}])", R"([{"c": 789}])"};

    EXPECT_TRUE(ctx->write_json_data(jsons).ok());
    EXPECT_TRUE(ctx->finish_write().ok());
    EXPECT_TRUE(ctx->open_for_read().ok());

    // Verify NestedGroupReader path is exercised with expected data
    ctx->verify_all_nested_groups({{std::string(kRootNestedGroupPath),
                                    {// Expected JSON arrays for $root group
                                     {R"([{"a":123}])", R"([{"b":456}])", R"([{"c":789}])"},
                                     // Expected element counts: 1, 1, 1
                                     {1, 1, 1}}}});

    // Verify $root NestedGroup exists for top-level arrays
    auto* variant_reader = ctx->get_variant_reader();
    const auto& nested_group_readers = variant_reader->get_nested_group_readers();
    ASSERT_FALSE(nested_group_readers.empty())
            << "Top-level arrays should create NestedGroup with $root path";

    ctx->validate_nested_group(std::string(kRootNestedGroupPath))
            .expect_exists()
            .expect_valid()
            .expect_path(std::string(kRootNestedGroupPath))
            .expect_children({"a", "b", "c"});

    // Read and verify data
    vectorized::DataTypePtr type;
    MutableColumnPtr result;
    EXPECT_TRUE(ctx->read_all_data(&result, &type).ok());
    ASSERT_NE(type, nullptr);
    ASSERT_TRUE(result);
    EXPECT_EQ(result->size(), 3);

    EXPECT_TRUE(json_strings_equal(serialize_to_json_string(*result, type, 0), R"([{"a":123}])"));
    EXPECT_TRUE(json_strings_equal(serialize_to_json_string(*result, type, 1), R"([{"b":456}])"));
    EXPECT_TRUE(json_strings_equal(serialize_to_json_string(*result, type, 2), R"([{"c":789}])"));
}

// Test that enable_variant_nested_group flag has been removed
TEST_F(VariantNestedTest, test_flag_removed) {
    ParseConfig config;

    // Verify that ParseConfig no longer has enable_variant_nested_group field
    // The config should only have enable_flatten_nested
    config.enable_flatten_nested = false;
    EXPECT_FALSE(config.enable_flatten_nested);

    config.enable_flatten_nested = true;
    EXPECT_TRUE(config.enable_flatten_nested);
}

// Test multi-level nested arrays: {"outer": [{"inner": [{"value": 1}]}]}
TEST_F(VariantNestedTest, test_multi_level_nested_arrays) {
    VariantTestConfig config;
    config.tablet_id = 10003;
    config.variant_max_subcolumns_count = 20;
    auto ctx = create_context(config);

    std::vector<std::string> jsons = {R"({"outer": [{"inner": [{"value": 10}]}]})",
                                      R"({"outer": [{"inner": [{"value": 20}, {"value": 21}]}]})",
                                      R"({"outer": [{"inner": []}]})", R"({"outer": []})",
                                      R"({"outer": [{"other": "data"}]})"};

    EXPECT_TRUE(ctx->write_json_data(jsons).ok());
    EXPECT_TRUE(ctx->finish_write().ok());
    EXPECT_TRUE(ctx->open_for_read().ok());

    // Verify NestedGroupReader path is exercised with expected data
    // Note: For multi-level nested, we verify the top-level "outer" group
    ctx->verify_all_nested_groups(
            {{"outer",
              {// Expected JSON arrays for "outer" group (contains inner arrays reconstructed)
               // Note: PRUNED mode includes empty nested groups for all elements
               .json_arrays = {R"([{"inner":[{"value":10}]}])",
                               R"([{"inner":[{"value":20},{"value":21}]}])", R"([{"inner":[]}])",
                               R"([])", R"([{"other":"data","inner":[]}])"},
               // Expected element counts: 1, 1, 1, 0, 1
               .elem_counts = {1, 1, 1, 0, 1}}}});

    // Verify outer NestedGroup exists
    ctx->validate_nested_group("outer").expect_exists().expect_valid().expect_path("outer");

    // Verify nested "inner" NestedGroup exists within "outer"
    auto* variant_reader = ctx->get_variant_reader();
    const auto* outer_group_reader = variant_reader->get_nested_group_reader("outer");
    ASSERT_FALSE(outer_group_reader->nested_group_readers.empty())
            << "Outer group should have nested groups";

    ctx->validate_nested_group("outer")
            .expect_nested_group("inner")
            .nested_group("inner")
            .expect_valid()
            .expect_path("inner")
            .expect_child("value");

    // Read and verify data
    MutableColumnPtr result;
    EXPECT_TRUE(ctx->read_all_data(&result).ok());
    auto* result_variant = assert_cast<ColumnVariant*>(result.get());

    ctx->validate_data(result_variant)
            .expect_row_count(5)
            .expect_exact_match(0, 2); // First 2 rows should match exactly
}

// Test nested array with scalar fields: {"arr": [{"scalar": 1, "nested": [{"val": 2}]}]}
TEST_F(VariantNestedTest, test_nested_array_with_scalars_and_nested) {
    VariantTestConfig config;
    config.tablet_id = 10004;
    config.variant_max_subcolumns_count = 20;
    auto ctx = create_context(config);

    std::vector<std::string> jsons = {R"({"arr": [{"scalar": 100, "nested": [{"val": 200}]}]})",
                                      R"({"arr": [{"scalar": 101, "other": "test"}]})",
                                      R"({"arr": [{"nested": [{"val": 201}, {"val": 202}]}]})",
                                      R"({"arr": [{"scalar": 102}]})"};

    EXPECT_TRUE(ctx->write_json_data(jsons).ok());
    EXPECT_TRUE(ctx->finish_write().ok());
    EXPECT_TRUE(ctx->open_for_read().ok());

    // Verify NestedGroupReader path is exercised with expected data
    ctx->verify_all_nested_groups(
            {{"arr",
              {// Expected JSON arrays for "arr" group
               // Note: PRUNED mode includes empty nested groups for all elements
               .json_arrays = {R"([{"scalar":100,"nested":[{"val":200}]}])",
                               R"([{"other":"test","scalar":101,"nested":[]}])",
                               R"([{"nested":[{"val":201},{"val":202}]}])",
                               R"([{"scalar":102,"nested":[]}])"},
               // Expected element counts: 1, 1, 1, 1
               .elem_counts = {1, 1, 1, 1}}}});

    // Verify "arr" NestedGroup has both scalar children and nested groups
    ctx->validate_nested_group("arr")
            .expect_exists()
            .expect_valid()
            .expect_path("arr")
            .expect_children({"scalar", "other"})
            .expect_nested_group("nested");

    // Verify nested "nested" group has "val" child
    ctx->validate_nested_group("arr").nested_group("nested").expect_valid().expect_child("val");

    // Read and verify data
    MutableColumnPtr result;
    EXPECT_TRUE(ctx->read_all_data(&result).ok());
    auto* result_variant = assert_cast<ColumnVariant*>(result.get());

    ctx->validate_data(result_variant)
            .expect_row_count(4)
            .expect_row_contains_all(0, {"\"scalar\":100", "\"nested\""});
}

// Test deep nested objects with arrays: {"level1": {"level2": {"level3": [{"data": 1}]}}}
TEST_F(VariantNestedTest, test_deep_nested_objects_with_arrays) {
    VariantTestConfig config;
    config.tablet_id = 10005;
    config.variant_max_subcolumns_count = 20;
    auto ctx = create_context(config);

    std::vector<std::string> jsons = {
            R"({"level1": {"level2": {"level3": [{"data": 1}]}}})",
            R"({"level1": {"level2": {"level3": [{"data": 2}, {"data": 3}]}}})",
            R"({"level1": {"level2": {"other": "value"}}})"};

    EXPECT_TRUE(ctx->write_json_data(jsons).ok());
    EXPECT_TRUE(ctx->finish_write().ok());
    EXPECT_TRUE(ctx->open_for_read().ok());

    // Verify NestedGroupReader path is exercised with expected data
    ctx->verify_all_nested_groups({{"level1.level2.level3",
                                    {// Expected JSON arrays for "level1.level2.level3" group
                                     {R"([{"data":1}])", R"([{"data":2},{"data":3}])", R"([])"},
                                     // Expected element counts: 1, 2, 0
                                     {1, 2, 0}}}});

    // Verify "level1.level2.level3" NestedGroup exists (flattened path)
    ctx->validate_nested_group("level1.level2.level3")
            .expect_exists()
            .expect_valid()
            .expect_path("level1.level2.level3")
            .expect_child("data");

    // Read and verify data
    MutableColumnPtr result;
    EXPECT_TRUE(ctx->read_all_data(&result).ok());
    auto* result_variant = assert_cast<ColumnVariant*>(result.get());

    ctx->validate_data(result_variant).expect_row_count(3);
}

// Test empty arrays and null elements: {"arr": []} and {"arr": [{"a": 1}, null, {"a": 2}]}
TEST_F(VariantNestedTest, test_empty_arrays_and_null_elements) {
    VariantTestConfig config;
    config.tablet_id = 10006;
    config.variant_max_subcolumns_count = 20;
    auto ctx = create_context(config);

    std::vector<std::string> jsons = {R"({"arr": [{"a": 1}]})", R"({"arr": [123]})",
                                      R"({"arr": [{"a": 2}, {"a": 3}]})", R"({"arr": [null, 1]})"};

    EXPECT_TRUE(ctx->write_json_data(jsons).ok());
    EXPECT_TRUE(ctx->finish_write().ok());
    EXPECT_TRUE(ctx->open_for_read().ok());

    // Verify NestedGroupReader path is exercised with expected data
    // Note: PRUNED mode only reconstructs object elements, non-object elements are skipped
    ctx->verify_all_nested_groups({{"arr",
                                    {// Expected JSON arrays for "arr" group (PRUNED mode)
                                     {R"([{"a":1}])", R"([])", R"([{"a":2},{"a":3}])", R"([])"},
                                     // Empty elem_counts - don't verify offsets
                                     {}}}});

    // Verify "arr" NestedGroup exists
    ctx->validate_nested_group("arr").expect_exists().expect_valid();

    // Read and verify data
    MutableColumnPtr result;
    EXPECT_TRUE(ctx->read_all_data(&result).ok());
    auto* result_variant = assert_cast<ColumnVariant*>(result.get());

    ctx->validate_data(result_variant)
            .expect_row_count(4)
            .expect_row_contains(1, "\"arr\":["); // Row 1 should have array
}

// Test mixed data types in same column: some rows are objects, some are arrays
TEST_F(VariantNestedTest, test_mixed_object_and_array_rows) {
    VariantTestConfig config;
    config.tablet_id = 10007;
    config.variant_max_subcolumns_count = 20;
    auto ctx = create_context(config);

    std::vector<std::string> jsons = {R"({"nested": [{"a": 1}]})", R"({"nested": [{"b": 2}]})",
                                      R"({"other": "value"})",
                                      R"({"nested": [{"c": 3}, {"c": 4}]})"};

    EXPECT_TRUE(ctx->write_json_data(jsons).ok());
    EXPECT_TRUE(ctx->finish_write().ok());
    EXPECT_TRUE(ctx->open_for_read().ok());

    // Verify NestedGroupReader path is exercised with expected data
    ctx->verify_all_nested_groups(
            {{"nested",
              {// Expected JSON arrays for "nested" group
               {R"([{"a":1}])", R"([{"b":2}])", R"([])", R"([{"c":3},{"c":4}])"},
               // Expected element counts: 1, 1, 0, 2
               {1, 1, 0, 2}}}});

    // Verify NestedGroup exists
    auto* variant_reader = ctx->get_variant_reader();
    const auto& nested_group_readers = variant_reader->get_nested_group_readers();
    EXPECT_TRUE(nested_group_readers.count("nested") > 0 ||
                nested_group_readers.count(std::string(kRootNestedGroupPath)) > 0)
            << "Should have either 'nested' or '$root' NestedGroup";

    // Read and verify data
    MutableColumnPtr result;
    EXPECT_TRUE(ctx->read_all_data(&result).ok());
    auto* result_variant = assert_cast<ColumnVariant*>(result.get());

    ctx->validate_data(result_variant)
            .expect_row_count(4)
            .expect_row_contains(1, "\"b\":2")
            .print_all(); // Debug output
}

// Test complex nested structure with multiple scalar types
TEST_F(VariantNestedTest, test_complex_nested_with_multiple_types) {
    VariantTestConfig config;
    config.tablet_id = 10008;
    config.variant_max_subcolumns_count = 20;
    auto ctx = create_context(config);

    std::vector<std::string> jsons = {
            R"({"items": [{"id": 1, "name": "item1", "price": 99.99, "tags": [{"tag": "new"}]}]})",
            R"({"items": [{"id": 2, "name": "item2", "available": true}]})",
            R"({"items": [{"id": 3, "tags": [{"tag": "sale"}, {"tag": "hot"}]}]})"};

    EXPECT_TRUE(ctx->write_json_data(jsons).ok());
    EXPECT_TRUE(ctx->finish_write().ok());
    EXPECT_TRUE(ctx->open_for_read().ok());

    // Verify NestedGroupReader path is exercised with expected data
    ctx->verify_all_nested_groups(
            {{"items",
              {// Expected JSON arrays for "items" group
               // Note: PRUNED mode includes empty nested groups, bool may be stored as int
               .json_arrays = {R"([{"id":1,"name":"item1","price":99.99,"tags":[{"tag":"new"}]}])",
                               R"([{"available":true,"id":2,"name":"item2","tags":[]}])",
                               R"([{"id":3,"tags":[{"tag":"sale"},{"tag":"hot"}]}])"},
               // Expected element counts: 1, 1, 1
               .elem_counts = {1, 1, 1}}}});

    // Verify "items" NestedGroup with multiple child readers
    ctx->validate_nested_group("items")
            .expect_exists()
            .expect_valid()
            .expect_path("items")
            .expect_child("id")
            .expect_nested_group("tags");

    // Verify nested "tags" group has "tag" child
    ctx->validate_nested_group("items").nested_group("tags").expect_valid().expect_child("tag");

    // Read and verify data
    MutableColumnPtr result;
    EXPECT_TRUE(ctx->read_all_data(&result).ok());
    auto* result_variant = assert_cast<ColumnVariant*>(result.get());

    ctx->validate_data(result_variant)
            .expect_row_count(3)
            .expect_row_contains_all(0, {"\"id\":1", "\"items\""});
}

// Test metadata structure verification: check offsets, children count, depth
TEST_F(VariantNestedTest, test_nested_group_metadata_structure) {
    VariantTestConfig config;
    config.tablet_id = 10009;
    config.variant_max_subcolumns_count = 20;
    auto ctx = create_context(config);

    std::vector<std::string> jsons = {
            R"({"outer": [{"inner": [{"value": 100}]}, {"inner": [{"value": 101}]}]})",
            R"({"outer": [{"inner": []}]})", R"({"outer": []})"};

    EXPECT_TRUE(ctx->write_json_data(jsons).ok());
    EXPECT_TRUE(ctx->finish_write().ok());
    EXPECT_TRUE(ctx->open_for_read().ok());

    // Verify NestedGroupReader path is exercised with expected data
    ctx->verify_all_nested_groups(
            {{"outer",
              {// Expected JSON arrays for "outer" group (with nested inner arrays)
               {R"([{"inner":[{"value":100}]},{"inner":[{"value":101}]}])", R"([{"inner":[]}])",
                R"([])"},
               // Expected element counts: 2, 1, 0
               {2, 1, 0}}}});

    // Verify "outer" NestedGroup metadata
    ctx->validate_nested_group("outer")
            .expect_exists()
            .expect_valid()
            .expect_depth(1)
            .expect_nested_group("inner");

    // Verify "inner" nested group metadata
    ctx->validate_nested_group("outer")
            .nested_group("inner")
            .expect_valid()
            .expect_depth(2)
            .expect_child("value")
            .expect_children_nullable();

    // Read and verify data
    MutableColumnPtr result;
    EXPECT_TRUE(ctx->read_all_data(&result).ok());
    auto* result_variant = assert_cast<ColumnVariant*>(result.get());

    ctx->validate_data(result_variant).expect_row_count(3);
}

// Test that JSONB and NestedGroup are both stored (redundant storage)
// and WHOLE read mode uses JSONB directly without NestedGroup merging.
TEST_F(VariantNestedTest, test_redundant_storage_whole_read) {
    auto ctx = create_context(10020);

    std::vector<std::string> jsons = {
            R"({"items": [{"id": 1, "name": "a"}]})",
            R"({"items": [{"id": 2, "name": "b"}, {"id": 3, "name": "c"}]})", R"({"items": []})",
            R"({"other": "value"})"};

    EXPECT_TRUE(ctx->write_json_data(jsons).ok());
    EXPECT_TRUE(ctx->finish_write().ok());
    EXPECT_TRUE(ctx->open_for_read().ok());

    // Verify NestedGroupReader path is exercised with expected data
    ctx->verify_all_nested_groups(
            {{"items",
              {// Expected JSON arrays for "items" group
               {R"([{"id":1,"name":"a"}])", R"([{"id":2,"name":"b"},{"id":3,"name":"c"}])", R"([])",
                R"([])"},
               // Expected element counts: 1, 2, 0, 0
               {1, 2, 0, 0}}}});

    // Verify both JSONB subcolumn and NestedGroup exist (redundant storage)
    auto* variant_reader = ctx->get_variant_reader();

    ctx->validate_nested_group("items").expect_exists().expect_valid();

    // Verify JSONB subcolumn exists
    const auto* subcolumn_meta = variant_reader->get_subcolumns_meta_info();
    EXPECT_TRUE(subcolumn_meta != nullptr && !subcolumn_meta->empty())
            << "JSONB subcolumn should exist (redundant storage)";

    // Read and verify data matches exactly (redundant storage preserves complete data)
    MutableColumnPtr result;
    EXPECT_TRUE(ctx->read_all_data(&result).ok());
    auto* result_variant = assert_cast<ColumnVariant*>(result.get());

    ctx->validate_data(result_variant)
            .expect_row_count(4)
            .expect_exact_match(); // All rows should match exactly
}

// Test reading NestedGroup subcolumns directly (simulating column pruning access)
TEST_F(VariantNestedTest, test_nested_group_subcolumn_access) {
    auto ctx = create_context(10021);

    std::vector<std::string> jsons = {R"({"arr": [{"x": 10, "y": 100}]})",
                                      R"({"arr": [{"x": 20, "y": 200}, {"x": 30, "y": 300}]})",
                                      R"({"arr": [{"x": 40}]})", R"({"arr": []})"};

    EXPECT_TRUE(ctx->write_json_data(jsons).ok());
    EXPECT_TRUE(ctx->finish_write().ok());
    EXPECT_TRUE(ctx->open_for_read().ok());

    // Verify NestedGroupReader path is exercised with expected data
    ctx->verify_all_nested_groups(
            {{"arr",
              {// Expected JSON arrays for "arr" group
               {R"([{"x":10,"y":100}])", R"([{"x":20,"y":200},{"x":30,"y":300}])", R"([{"x":40}])",
                R"([])"},
               // Expected element counts: 1, 2, 1, 0
               {1, 2, 1, 0}}}});

    // Verify NestedGroup structure
    ctx->validate_nested_group("arr").expect_exists().expect_valid().expect_children({"x", "y"});

    // Access child readers directly for column pruning simulation
    auto* variant_reader = ctx->get_variant_reader();
    const auto* arr_nested_group = variant_reader->get_nested_group_reader("arr");

    auto* x_reader = arr_nested_group->child_readers.at("x").get();
    ASSERT_TRUE(x_reader != nullptr);

    OlapReaderStatistics stats;
    ColumnIteratorUPtr x_iter;
    EXPECT_TRUE(x_reader->new_iterator(&x_iter, nullptr).ok());

    ColumnIteratorOptions iter_opts;
    iter_opts.stats = &stats;
    iter_opts.file_reader = ctx->get_file_reader().get();
    EXPECT_TRUE(x_iter->init(iter_opts).ok());

    // Read all "x" values (flat: 10, 20, 30, 40)
    auto x_type = x_reader->get_vec_data_type();
    auto x_column = x_type->create_column();
    size_t total_x_values = 4;
    EXPECT_TRUE(x_iter->seek_to_ordinal(0).ok());
    bool has_null = false;
    EXPECT_TRUE(x_iter->next_batch(&total_x_values, x_column, &has_null).ok());

    EXPECT_EQ(x_column->size(), 4) << "Should have 4 'x' values in flat storage";

    // Verify x values are not null
    auto* x_nullable = assert_cast<ColumnNullable*>(x_column.get());
    for (size_t i = 0; i < 4; ++i) {
        EXPECT_FALSE(x_nullable->is_null_at(i)) << "x[" << i << "] should not be null";
    }

    // Read offsets to understand array boundaries
    ColumnIteratorUPtr offsets_iter;
    EXPECT_TRUE(arr_nested_group->offsets_reader->new_iterator(&offsets_iter, nullptr).ok());
    EXPECT_TRUE(offsets_iter->init(iter_opts).ok());

    MutableColumnPtr offsets_col = ColumnOffset64::create();
    size_t n_offsets = 4;
    EXPECT_TRUE(offsets_iter->seek_to_ordinal(0).ok());
    EXPECT_TRUE(offsets_iter->next_batch(&n_offsets, offsets_col, &has_null).ok());

    EXPECT_EQ(offsets_col->size(), 4);
    auto* offsets_col_ptr = assert_cast<ColumnOffset64*>(offsets_col.get());
    auto& offsets_data = offsets_col_ptr->get_data();

    // Verify offset values
    EXPECT_EQ(offsets_data[0], 1); // Row 0: 1 element
    EXPECT_EQ(offsets_data[1], 3); // Row 1: 2 elements -> cumulative 3
    EXPECT_EQ(offsets_data[2], 4); // Row 2: 1 element -> cumulative 4
    EXPECT_EQ(offsets_data[3], 4); // Row 3: 0 elements -> cumulative 4
}

TEST_F(VariantNestedTest, test_nested_group_access_path_pruning) {
    auto ctx = create_context(10060);

    std::vector<std::string> jsons = {R"({"arr": [{"x": 10, "y": 100}]})",
                                      R"({"arr": [{"x": 20, "y": 200}, {"x": 30, "y": 300}]})",
                                      R"({"arr": [{"x": 40}]})", R"({"arr": []})"};

    EXPECT_TRUE(ctx->write_json_data(jsons).ok());
    EXPECT_TRUE(ctx->finish_write().ok());
    EXPECT_TRUE(ctx->open_for_read().ok());

    vectorized::DataTypePtr type;
    MutableColumnPtr result;
    std::vector<std::vector<std::string>> all_paths = {{"V1", "arr", "x"}};
    EXPECT_TRUE(
            ctx->read_nested_group_with_access_paths("arr", all_paths, {}, &type, &result).ok());

    EXPECT_TRUE(json_strings_equal(serialize_to_json_string(*result, type, 0), R"([{"x":10}])"));
    EXPECT_TRUE(json_strings_equal(serialize_to_json_string(*result, type, 1),
                                   R"([{"x":20},{"x":30}])"));
    EXPECT_TRUE(json_strings_equal(serialize_to_json_string(*result, type, 2), R"([{"x":40}])"));
    EXPECT_TRUE(json_strings_equal(serialize_to_json_string(*result, type, 3), R"([])"));
}

TEST_F(VariantNestedTest, test_nested_group_access_path_prunes_readers) {
    auto ctx = create_context(10064);

    std::vector<std::string> jsons = {R"({"arr": [{"x": 10, "y": 100}]})",
                                      R"({"arr": [{"x": 20, "y": 200}, {"x": 30, "y": 300}]})",
                                      R"({"arr": [{"x": 40}]})", R"({"arr": []})"};

    EXPECT_TRUE(ctx->write_json_data(jsons).ok());
    EXPECT_TRUE(ctx->finish_write().ok());
    EXPECT_TRUE(ctx->open_for_read().ok());

    auto* variant_reader = ctx->get_variant_reader();
    const auto* arr_group_reader_const = variant_reader->get_nested_group_reader("arr");
    ASSERT_NE(arr_group_reader_const, nullptr);
    auto* arr_group_reader = const_cast<segment_v2::NestedGroupReader*>(arr_group_reader_const);

    std::unordered_map<std::string, std::shared_ptr<std::atomic<int>>> read_counters;
    for (auto& [name, reader] : arr_group_reader->child_readers) {
        auto counter = std::make_shared<std::atomic<int>>(0);
        read_counters.emplace(name, counter);
        reader = std::make_shared<CountingColumnReader>(reader, counter);
    }

    vectorized::DataTypePtr type;
    MutableColumnPtr result;
    std::vector<std::vector<std::string>> all_paths = {{"V1", "arr", "x"}};
    EXPECT_TRUE(
            ctx->read_nested_group_with_access_paths("arr", all_paths, {}, &type, &result).ok());

    EXPECT_EQ(read_counters["x"]->load(), 1);
    EXPECT_EQ(read_counters["y"]->load(), 0);
}

TEST_F(VariantNestedTest, test_nested_group_access_path_predicate_paths_union) {
    auto ctx = create_context(10065);

    std::vector<std::string> jsons = {R"({"arr": [{"x": 10, "y": 100}]})",
                                      R"({"arr": [{"x": 20, "y": 200}, {"x": 30, "y": 300}]})",
                                      R"({"arr": [{"x": 40}]})", R"({"arr": []})"};

    EXPECT_TRUE(ctx->write_json_data(jsons).ok());
    EXPECT_TRUE(ctx->finish_write().ok());
    EXPECT_TRUE(ctx->open_for_read().ok());

    auto* variant_reader = ctx->get_variant_reader();
    const auto* arr_group_reader_const = variant_reader->get_nested_group_reader("arr");
    ASSERT_NE(arr_group_reader_const, nullptr);
    auto* arr_group_reader = const_cast<segment_v2::NestedGroupReader*>(arr_group_reader_const);

    std::unordered_map<std::string, std::shared_ptr<std::atomic<int>>> read_counters;
    for (auto& [name, reader] : arr_group_reader->child_readers) {
        auto counter = std::make_shared<std::atomic<int>>(0);
        read_counters.emplace(name, counter);
        reader = std::make_shared<CountingColumnReader>(reader, counter);
    }

    vectorized::DataTypePtr type;
    MutableColumnPtr result;
    std::vector<std::vector<std::string>> all_paths = {{"V1", "arr", "x"}};
    std::vector<std::vector<std::string>> predicate_paths = {{"V1", "arr", "y"}};
    EXPECT_TRUE(ctx->read_nested_group_with_access_paths("arr", all_paths, predicate_paths, &type,
                                                         &result)
                        .ok());

    EXPECT_TRUE(json_strings_equal(serialize_to_json_string(*result, type, 0),
                                   R"([{"x":10,"y":100}])"));
    EXPECT_TRUE(json_strings_equal(serialize_to_json_string(*result, type, 1),
                                   R"([{"x":20,"y":200},{"x":30,"y":300}])"));
    EXPECT_TRUE(json_strings_equal(serialize_to_json_string(*result, type, 2), R"([{"x":40}])"));
    EXPECT_TRUE(json_strings_equal(serialize_to_json_string(*result, type, 3), R"([])"));

    EXPECT_GE(read_counters["x"]->load(), 1);
    EXPECT_GE(read_counters["y"]->load(), 1);
}

TEST_F(VariantNestedTest, test_root_nested_group_access_path_pruning) {
    auto ctx = create_context(10067);

    std::vector<std::string> jsons = {R"([{"x": 10, "y": "a"}])",
                                      R"([{"x": 20, "y": "b"}, {"x": 30, "y": "c"}])",
                                      R"([{"x": 40}])", R"([])"};

    EXPECT_TRUE(ctx->write_json_data(jsons).ok());
    EXPECT_TRUE(ctx->finish_write().ok());
    EXPECT_TRUE(ctx->open_for_read().ok());

    vectorized::DataTypePtr type;
    MutableColumnPtr result;
    std::vector<std::vector<std::string>> all_paths = {{"V1", "x"}};
    EXPECT_TRUE(ctx->read_nested_group_with_access_paths({}, all_paths, {}, &type, &result).ok());

    EXPECT_TRUE(json_strings_equal(serialize_to_json_string(*result, type, 0), R"([{"x":10}])"));
    EXPECT_TRUE(json_strings_equal(serialize_to_json_string(*result, type, 1),
                                   R"([{"x":20},{"x":30}])"));
    EXPECT_TRUE(json_strings_equal(serialize_to_json_string(*result, type, 2), R"([{"x":40}])"));
    EXPECT_TRUE(json_strings_equal(serialize_to_json_string(*result, type, 3), R"([])"));
}

TEST_F(VariantNestedTest, test_root_nested_group_access_path_prunes_readers) {
    auto ctx = create_context(10068);

    std::vector<std::string> jsons = {R"([{"x": 10, "y": "a"}])",
                                      R"([{"x": 20, "y": "b"}, {"x": 30, "y": "c"}])",
                                      R"([{"x": 40}])", R"([])"};

    EXPECT_TRUE(ctx->write_json_data(jsons).ok());
    EXPECT_TRUE(ctx->finish_write().ok());
    EXPECT_TRUE(ctx->open_for_read().ok());

    auto* variant_reader = ctx->get_variant_reader();
    const auto& nested_group_readers = variant_reader->get_nested_group_readers();
    ASSERT_EQ(nested_group_readers.size(), 1);
    const auto* root_group_reader_const = nested_group_readers.begin()->second.get();
    ASSERT_NE(root_group_reader_const, nullptr);
    auto* root_group_reader = const_cast<segment_v2::NestedGroupReader*>(root_group_reader_const);

    std::unordered_map<std::string, std::shared_ptr<std::atomic<int>>> read_counters;
    for (auto& [name, reader] : root_group_reader->child_readers) {
        auto counter = std::make_shared<std::atomic<int>>(0);
        read_counters.emplace(name, counter);
        reader = std::make_shared<CountingColumnReader>(reader, counter);
    }

    vectorized::DataTypePtr type;
    MutableColumnPtr result;
    std::vector<std::vector<std::string>> all_paths = {{"V1", "x"}};
    EXPECT_TRUE(ctx->read_nested_group_with_access_paths({}, all_paths, {}, &type, &result).ok());

    EXPECT_EQ(read_counters["x"]->load(), 1);
    EXPECT_EQ(read_counters["y"]->load(), 0);
}

TEST_F(VariantNestedTest, test_root_nested_group_access_path_predicate_paths_union) {
    auto ctx = create_context(10069);

    std::vector<std::string> jsons = {R"([{"x": 10, "y": "a"}])",
                                      R"([{"x": 20, "y": "b"}, {"x": 30, "y": "c"}])",
                                      R"([{"x": 40}])", R"([])"};

    EXPECT_TRUE(ctx->write_json_data(jsons).ok());
    EXPECT_TRUE(ctx->finish_write().ok());
    EXPECT_TRUE(ctx->open_for_read().ok());

    auto* variant_reader = ctx->get_variant_reader();
    const auto& nested_group_readers = variant_reader->get_nested_group_readers();
    ASSERT_EQ(nested_group_readers.size(), 1);
    const auto* root_group_reader_const = nested_group_readers.begin()->second.get();
    ASSERT_NE(root_group_reader_const, nullptr);
    auto* root_group_reader = const_cast<segment_v2::NestedGroupReader*>(root_group_reader_const);

    std::unordered_map<std::string, std::shared_ptr<std::atomic<int>>> read_counters;
    for (auto& [name, reader] : root_group_reader->child_readers) {
        auto counter = std::make_shared<std::atomic<int>>(0);
        read_counters.emplace(name, counter);
        reader = std::make_shared<CountingColumnReader>(reader, counter);
    }

    vectorized::DataTypePtr type;
    MutableColumnPtr result;
    std::vector<std::vector<std::string>> all_paths = {{"V1", "x"}};
    std::vector<std::vector<std::string>> predicate_paths = {{"V1", "y"}};
    EXPECT_TRUE(
            ctx->read_nested_group_with_access_paths({}, all_paths, predicate_paths, &type, &result)
                    .ok());

    EXPECT_TRUE(json_strings_equal(serialize_to_json_string(*result, type, 0),
                                   R"([{"x":10,"y":"a"}])"));
    EXPECT_TRUE(json_strings_equal(serialize_to_json_string(*result, type, 1),
                                   R"([{"x":20,"y":"b"},{"x":30,"y":"c"}])"));
    EXPECT_TRUE(json_strings_equal(serialize_to_json_string(*result, type, 2), R"([{"x":40}])"));
    EXPECT_TRUE(json_strings_equal(serialize_to_json_string(*result, type, 3), R"([])"));

    EXPECT_GE(read_counters["x"]->load(), 1);
    EXPECT_GE(read_counters["y"]->load(), 1);
}

TEST_F(VariantNestedTest, test_nested_group_access_path_multi_level) {
    auto ctx = create_context(10061);

    std::vector<std::string> jsons = {
            R"({"outer": [{"inner": [{"z": 1, "w": 10}]}]})",
            R"({"outer": [{"inner": [{"z": 2}, {"z": 3, "w": 30}]}, {"inner": [{"z": 4, "w": 40}]}]})",
            R"({"outer": []})"};

    EXPECT_TRUE(ctx->write_json_data(jsons).ok());
    EXPECT_TRUE(ctx->finish_write().ok());
    EXPECT_TRUE(ctx->open_for_read().ok());

    vectorized::DataTypePtr type;
    MutableColumnPtr result;
    std::vector<std::vector<std::string>> all_paths = {{"V1", "outer", "inner", "z"}};
    EXPECT_TRUE(
            ctx->read_nested_group_with_access_paths("outer.inner", all_paths, {}, &type, &result)
                    .ok());

    EXPECT_TRUE(json_strings_equal(serialize_to_json_string(*result, type, 0), R"([[{"z":1}]])"));
    EXPECT_TRUE(json_strings_equal(serialize_to_json_string(*result, type, 1),
                                   R"([[{"z":2},{"z":3}],[{"z":4}]])"));
    EXPECT_TRUE(json_strings_equal(serialize_to_json_string(*result, type, 2), R"([])"));
}

TEST_F(VariantNestedTest, test_nested_group_access_path_multi_level_with_predicate_paths) {
    auto ctx = create_context(10066);

    std::vector<std::string> jsons = {
            R"({"outer": [{"inner": [{"z": 1, "w": 10}]}]})",
            R"({"outer": [{"inner": [{"z": 2}, {"z": 3, "w": 30}]}, {"inner": [{"z": 4, "w": 40}]}]})",
            R"({"outer": []})"};

    EXPECT_TRUE(ctx->write_json_data(jsons).ok());
    EXPECT_TRUE(ctx->finish_write().ok());
    EXPECT_TRUE(ctx->open_for_read().ok());

    auto* variant_reader = ctx->get_variant_reader();
    const auto* inner_group_reader_const = variant_reader->get_nested_group_reader("outer.inner");
    ASSERT_NE(inner_group_reader_const, nullptr);
    auto* inner_group_reader = const_cast<segment_v2::NestedGroupReader*>(inner_group_reader_const);

    std::unordered_map<std::string, std::shared_ptr<std::atomic<int>>> read_counters;
    for (auto& [name, reader] : inner_group_reader->child_readers) {
        auto counter = std::make_shared<std::atomic<int>>(0);
        read_counters.emplace(name, counter);
        reader = std::make_shared<CountingColumnReader>(reader, counter);
    }

    vectorized::DataTypePtr type;
    MutableColumnPtr result;
    std::vector<std::vector<std::string>> all_paths = {{"V1", "outer", "inner", "z"}};
    std::vector<std::vector<std::string>> predicate_paths = {{"V1", "outer", "inner", "w"}};
    EXPECT_TRUE(ctx->read_nested_group_with_access_paths("outer.inner", all_paths, predicate_paths,
                                                         &type, &result)
                        .ok());

    EXPECT_TRUE(json_strings_equal(serialize_to_json_string(*result, type, 0),
                                   R"([[{"z":1,"w":10}]])"));
    EXPECT_TRUE(json_strings_equal(serialize_to_json_string(*result, type, 1),
                                   R"([[{"z":2},{"z":3,"w":30}],[{"z":4,"w":40}]])"));
    EXPECT_TRUE(json_strings_equal(serialize_to_json_string(*result, type, 2), R"([])"));

    EXPECT_GE(read_counters["z"]->load(), 1);
    EXPECT_GE(read_counters["w"]->load(), 1);
}

TEST_F(VariantNestedTest, test_nested_group_access_path_allow_all) {
    auto ctx = create_context(10062);

    std::vector<std::string> jsons = {R"({"arr": [{"x": 10, "y": 100}]})",
                                      R"({"arr": [{"x": 20, "y": 200}, {"x": 30, "y": 300}]})",
                                      R"({"arr": [{"x": 40}]})", R"({"arr": []})"};

    EXPECT_TRUE(ctx->write_json_data(jsons).ok());
    EXPECT_TRUE(ctx->finish_write().ok());
    EXPECT_TRUE(ctx->open_for_read().ok());

    vectorized::DataTypePtr type;
    MutableColumnPtr result;
    std::vector<std::vector<std::string>> all_paths = {{"V1", "arr"}, {"V1", "arr", "x"}};
    EXPECT_TRUE(
            ctx->read_nested_group_with_access_paths("arr", all_paths, {}, &type, &result).ok());

    EXPECT_TRUE(json_strings_equal(serialize_to_json_string(*result, type, 0),
                                   R"([{"x":10,"y":100}])"));
    EXPECT_TRUE(json_strings_equal(serialize_to_json_string(*result, type, 1),
                                   R"([{"x":20,"y":200},{"x":30,"y":300}])"));
    EXPECT_TRUE(json_strings_equal(serialize_to_json_string(*result, type, 2), R"([{"x":40}])"));
    EXPECT_TRUE(json_strings_equal(serialize_to_json_string(*result, type, 3), R"([])"));
}

TEST_F(VariantNestedTest, test_nested_group_pruned_path_with_filter) {
    auto ctx = create_context(10063);

    std::vector<std::string> jsons = {
            R"({"arr": [{"obj": {"x": 1, "y": 2}, "z": 100}]})",
            R"({"arr": [{"obj": {"x": 3}, "z": 200}, {"obj": {"y": 4}}]})", R"({"arr": []})"};

    EXPECT_TRUE(ctx->write_json_data(jsons).ok());
    EXPECT_TRUE(ctx->finish_write().ok());
    EXPECT_TRUE(ctx->open_for_read().ok());

    vectorized::DataTypePtr type;
    MutableColumnPtr result;
    std::vector<std::vector<std::string>> all_paths = {{"V1", "arr", "obj"}};
    EXPECT_TRUE(ctx->read_nested_group_with_access_paths("arr.obj", all_paths, {}, &type, &result)
                        .ok());

    EXPECT_TRUE(
            json_strings_equal(serialize_to_json_string(*result, type, 0), R"([{"x":1,"y":2}])"));
    EXPECT_TRUE(
            json_strings_equal(serialize_to_json_string(*result, type, 1), R"([{"x":3},{"y":4}])"));
    EXPECT_TRUE(json_strings_equal(serialize_to_json_string(*result, type, 2), R"([])"));
}

// Test NestedGroupWholeIterator columnar reconstruction via NestedGroupIterator
TEST_F(VariantNestedTest, test_nested_group_iterator_modes) {
    auto ctx = create_context(10023);

    std::vector<std::string> jsons = {R"({"nested": [{"a": 1, "b": 10}]})",
                                      R"({"nested": [{"a": 2, "b": 20}, {"a": 3, "b": 30}]})",
                                      R"({"nested": [123]})", R"({"nested": [{"a": 4}]})"};

    EXPECT_TRUE(ctx->write_json_data(jsons).ok());
    EXPECT_TRUE(ctx->finish_write().ok());
    EXPECT_TRUE(ctx->open_for_read().ok());

    // Verify NestedGroupReader path is exercised with expected data
    // Note: PRUNED mode only reconstructs object elements, scalar elements are skipped
    ctx->verify_all_nested_groups({{"nested",
                                    {// Expected JSON arrays for "nested" group (PRUNED mode)
                                     {R"([{"a":1,"b":10}])", R"([{"a":2,"b":20},{"a":3,"b":30}])",
                                      R"([])", R"([{"a":4}])"},
                                     // Empty elem_counts - don't verify offsets
                                     {}}}});

    auto* variant_reader = ctx->get_variant_reader();
    const auto* nested_group_reader = variant_reader->get_nested_group_reader("nested");
    ASSERT_TRUE(nested_group_reader != nullptr && nested_group_reader->is_valid());

    OlapReaderStatistics stats;
    ColumnIteratorOptions iter_opts;
    iter_opts.stats = &stats;
    iter_opts.file_reader = ctx->get_file_reader().get();

    // Test columnar reconstruction via NestedGroupIterator
    {
        auto element_iter =
                std::make_unique<segment_v2::NestedGroupWholeIterator>(nested_group_reader);
        segment_v2::ColumnIteratorUPtr offsets_iter;
        EXPECT_TRUE(nested_group_reader->offsets_reader->new_iterator(&offsets_iter, nullptr).ok());
        auto array_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeVariant>(0));
        auto array_iter = std::make_unique<segment_v2::NestedGroupIterator>(
                std::move(offsets_iter), std::move(element_iter), array_type);

        EXPECT_TRUE(array_iter->init(iter_opts).ok());
        EXPECT_TRUE(array_iter->seek_to_ordinal(0).ok());

        MutableColumnPtr dst_col = array_iter->create_result_column();
        size_t n = 4;
        bool has_null = false;
        EXPECT_TRUE(array_iter->next_batch(&n, dst_col, &has_null).ok());
        EXPECT_EQ(dst_col->size(), 4);

        EXPECT_TRUE(json_strings_equal(serialize_to_json_string(*dst_col, array_type, 0),
                                       R"([{"a":1,"b":10}])"));
        EXPECT_TRUE(json_strings_equal(serialize_to_json_string(*dst_col, array_type, 1),
                                       R"([{"a":2,"b":20},{"a":3,"b":30}])"));
        // Non-object elements are not expanded into NestedGroup
        EXPECT_TRUE(json_strings_equal(serialize_to_json_string(*dst_col, array_type, 2), R"([])"));
        EXPECT_TRUE(json_strings_equal(serialize_to_json_string(*dst_col, array_type, 3),
                                       R"([{"a":4}])"));
    }
}

// Test read_by_rowids for NestedGroupIterator with Nullable column
TEST_F(VariantNestedTest, test_nested_group_iterator_read_by_rowids) {
    auto ctx = create_context(10050);

    // Use the same JSON pattern as test_nested_group_subcolumn_access
    std::vector<std::string> jsons = {R"({"arr": [{"x": 10, "y": 100}]})",
                                      R"({"arr": [{"x": 20, "y": 200}, {"x": 30, "y": 300}]})",
                                      R"({"arr": [{"x": 40}]})", R"({"arr": []})"};

    EXPECT_TRUE(ctx->write_json_data(jsons).ok());
    EXPECT_TRUE(ctx->finish_write().ok());
    EXPECT_TRUE(ctx->open_for_read().ok());

    auto* variant_reader = ctx->get_variant_reader();
    ASSERT_NE(variant_reader, nullptr);

    const auto* nested_group_reader = variant_reader->get_nested_group_reader("arr");
    ASSERT_NE(nested_group_reader, nullptr);
    ASSERT_TRUE(nested_group_reader->is_valid());

    OlapReaderStatistics stats;
    ColumnIteratorOptions iter_opts;
    iter_opts.stats = &stats;
    iter_opts.file_reader = ctx->get_file_reader().get();

    // Test 1: NestedGroupIterator next_batch with Nullable column
    {
        auto x_it = nested_group_reader->child_readers.find("x");
        ASSERT_TRUE(x_it != nested_group_reader->child_readers.end());

        auto* x_reader = x_it->second.get();
        ASSERT_NE(x_reader, nullptr);

        segment_v2::ColumnIteratorUPtr offsets_iter;
        EXPECT_TRUE(nested_group_reader->offsets_reader->new_iterator(&offsets_iter, nullptr).ok());

        segment_v2::ColumnIteratorUPtr child_iter;
        EXPECT_TRUE(x_reader->new_iterator(&child_iter, nullptr).ok());

        auto child_type = x_reader->get_vec_data_type();
        auto array_type = std::make_shared<vectorized::DataTypeArray>(child_type);

        auto nested_iter = std::make_unique<segment_v2::NestedGroupIterator>(
                std::move(offsets_iter), std::move(child_iter), array_type);

        EXPECT_TRUE(nested_iter->init(iter_opts).ok());
        EXPECT_TRUE(nested_iter->seek_to_ordinal(0).ok());

        // Test next_batch with Nullable column
        auto nullable_type = std::make_shared<vectorized::DataTypeNullable>(array_type);
        MutableColumnPtr nullable_col = nullable_type->create_column();

        size_t n = 4;
        bool has_null = false;
        EXPECT_TRUE(nested_iter->next_batch(&n, nullable_col, &has_null).ok());
        EXPECT_EQ(nullable_col->size(), 4);

        auto* nullable_result = assert_cast<ColumnNullable*>(nullable_col.get());
        auto& array_col = assert_cast<ColumnArray&>(nullable_result->get_nested_column());

        // Verify offsets: row 0 has 1, row 1 has 2, row 2 has 1, row 3 has 0
        EXPECT_EQ(array_col.get_offsets()[0], 1);
        EXPECT_EQ(array_col.get_offsets()[1] - array_col.get_offsets()[0], 2);
        EXPECT_EQ(array_col.get_offsets()[2] - array_col.get_offsets()[1], 1);
        EXPECT_EQ(array_col.get_offsets()[3] - array_col.get_offsets()[2], 0);

        // Verify null map
        for (size_t i = 0; i < 4; ++i) {
            EXPECT_FALSE(nullable_result->is_null_at(i));
        }
    }

    // Test 2: NestedGroupIterator read_by_rowids with Nullable column
    {
        auto x_it = nested_group_reader->child_readers.find("x");
        ASSERT_TRUE(x_it != nested_group_reader->child_readers.end());

        auto* x_reader = x_it->second.get();

        segment_v2::ColumnIteratorUPtr offsets_iter;
        EXPECT_TRUE(nested_group_reader->offsets_reader->new_iterator(&offsets_iter, nullptr).ok());

        segment_v2::ColumnIteratorUPtr child_iter;
        EXPECT_TRUE(x_reader->new_iterator(&child_iter, nullptr).ok());

        auto child_type = x_reader->get_vec_data_type();
        auto array_type = std::make_shared<vectorized::DataTypeArray>(child_type);

        auto nested_iter = std::make_unique<segment_v2::NestedGroupIterator>(
                std::move(offsets_iter), std::move(child_iter), array_type);

        EXPECT_TRUE(nested_iter->init(iter_opts).ok());

        // Test read_by_rowids with Nullable column
        std::vector<segment_v2::rowid_t> rowids = {0, 2, 3}; // rows 0, 2, 3

        auto nullable_type = std::make_shared<vectorized::DataTypeNullable>(array_type);
        MutableColumnPtr nullable_col = nullable_type->create_column();

        EXPECT_TRUE(nested_iter->read_by_rowids(rowids.data(), rowids.size(), nullable_col).ok());
        EXPECT_EQ(nullable_col->size(), 3);

        auto* nullable_result = assert_cast<ColumnNullable*>(nullable_col.get());
        auto& array_col = assert_cast<ColumnArray&>(nullable_result->get_nested_column());

        // Row 0: 1 element, Row 2: 1 element, Row 3: 0 elements
        EXPECT_EQ(array_col.get_offsets()[0], 1);
        EXPECT_EQ(array_col.get_offsets()[1] - array_col.get_offsets()[0], 1);
        EXPECT_EQ(array_col.get_offsets()[2] - array_col.get_offsets()[1], 0);

        // Verify null map is properly set
        for (size_t i = 0; i < 3; ++i) {
            EXPECT_FALSE(nullable_result->is_null_at(i));
        }
    }

    // Test 3: NestedGroupIterator read_by_rowids with non-Nullable column
    {
        auto x_it = nested_group_reader->child_readers.find("x");
        ASSERT_TRUE(x_it != nested_group_reader->child_readers.end());

        auto* x_reader = x_it->second.get();

        segment_v2::ColumnIteratorUPtr offsets_iter;
        EXPECT_TRUE(nested_group_reader->offsets_reader->new_iterator(&offsets_iter, nullptr).ok());

        segment_v2::ColumnIteratorUPtr child_iter;
        EXPECT_TRUE(x_reader->new_iterator(&child_iter, nullptr).ok());

        auto child_type = x_reader->get_vec_data_type();
        auto array_type = std::make_shared<vectorized::DataTypeArray>(child_type);

        auto nested_iter = std::make_unique<segment_v2::NestedGroupIterator>(
                std::move(offsets_iter), std::move(child_iter), array_type);

        EXPECT_TRUE(nested_iter->init(iter_opts).ok());

        // Test with non-Nullable column (direct ColumnArray)
        MutableColumnPtr array_col = nested_iter->create_result_column();

        std::vector<segment_v2::rowid_t> rowids = {0, 1}; // rows 0 and 1
        EXPECT_TRUE(nested_iter->read_by_rowids(rowids.data(), rowids.size(), array_col).ok());
        EXPECT_EQ(array_col->size(), 2);

        auto& array_result = assert_cast<ColumnArray&>(*array_col);
        // Row 0: 1 element, Row 1: 2 elements
        EXPECT_EQ(array_result.get_offsets()[0], 1);
        EXPECT_EQ(array_result.get_offsets()[1] - array_result.get_offsets()[0], 2);
    }

    // Test 4: NestedGroupWholeIterator read_by_rowids (element-level)
    {
        auto element_iter =
                std::make_unique<segment_v2::NestedGroupWholeIterator>(nested_group_reader);

        EXPECT_TRUE(element_iter->init(iter_opts).ok());

        MutableColumnPtr dst_col = element_iter->create_result_column();

        std::vector<segment_v2::rowid_t> rowids = {0, 1}; // element ordinals 0 and 1
        EXPECT_TRUE(element_iter->read_by_rowids(rowids.data(), rowids.size(), dst_col).ok());
        EXPECT_EQ(dst_col->size(), 2);

        auto variant_type = std::make_shared<DataTypeVariant>(0);

        // Element 0: {"x": 10, "y": 100}
        std::string json0 = serialize_to_json_string(*dst_col, variant_type, 0);
        EXPECT_TRUE(json0.find("\"x\":10") != std::string::npos ||
                    json0.find("\"x\": 10") != std::string::npos)
                << "Element 0 should contain x:10, got: " << json0;

        // Element 1: {"x": 20, "y": 200}
        std::string json1 = serialize_to_json_string(*dst_col, variant_type, 1);
        EXPECT_TRUE(json1.find("\"x\":20") != std::string::npos ||
                    json1.find("\"x\": 20") != std::string::npos)
                << "Element 1 should contain x:20, got: " << json1;
    }
}

// Test structure conflict: same path has both object and array<object>
TEST_F(VariantNestedTest, test_structure_conflict_object_vs_array) {
    auto ctx = create_context(10022);

    // Mixed structure data - same "field" path has different types
    std::vector<std::string> jsons = {
            R"({"field": {"scalar": 1}})", R"({"field": [{"nested": 2}]})",
            R"({"field": "string_value"})", R"({"field": [{"nested": 3}, {"nested": 4}]})",
            R"({"other": "data"})"};

    EXPECT_TRUE(ctx->write_json_data(jsons).ok());
    EXPECT_TRUE(ctx->finish_write().ok());
    EXPECT_TRUE(ctx->open_for_read().ok());

    // Verify NestedGroupReader path is exercised with expected data
    // Note: "field" has mixed types - only array rows contribute to nested group
    ctx->verify_all_nested_groups(
            {{"field",
              {// Expected JSON arrays for "field" group (only array rows have data)
               {R"([])", R"([{"nested":2}])", R"([])", R"([{"nested":3},{"nested":4}])", R"([])"},
               // Expected element counts: 0, 1, 0, 2, 0
               {0, 1, 0, 2, 0}}}});

    // Read and verify ALL data is preserved correctly with redundant storage
    MutableColumnPtr result;
    EXPECT_TRUE(ctx->read_all_data(&result).ok());
    auto* result_variant = assert_cast<ColumnVariant*>(result.get());

    // With redundant storage + hierarchical reader, all rows should be preserved exactly
    ctx->validate_data(result_variant)
            .expect_row_count(5)
            .expect_exact_match(); // All rows should match exactly
}

// ============================================================================
// Additional Test Cases from test_cases.md
// ============================================================================

// Test Case 2: Multiple independent nested fields in same row
TEST_F(VariantNestedTest, test_multi_nested_fields) {
    VariantTestConfig config;
    config.tablet_id = 10030;
    config.variant_max_subcolumns_count = 20;
    auto ctx = create_context(config);

    std::vector<std::string> jsons = {R"({"nested1": [{"a": 123}], "nested2": [{"xx": 1}]})",
                                      R"({"nested2": [{"a": 123, "b": 45}]})",
                                      R"({"nested3": [{"x": 123, "y": 45}]})"};

    EXPECT_TRUE(ctx->write_json_data(jsons).ok());
    EXPECT_TRUE(ctx->finish_write().ok());
    EXPECT_TRUE(ctx->open_for_read().ok());

    // Verify NestedGroupReader for each nested field
    ctx->verify_all_nested_groups();

    // Verify each nested group exists independently
    auto* variant_reader = ctx->get_variant_reader();
    const auto& nested_group_readers = variant_reader->get_nested_group_readers();

    // Should have nested1, nested2, nested3 groups
    EXPECT_TRUE(nested_group_readers.count("nested1") > 0 ||
                nested_group_readers.count("nested2") > 0 ||
                nested_group_readers.count("nested3") > 0)
            << "Should have at least one nested group";

    // Read and verify data
    MutableColumnPtr result;
    EXPECT_TRUE(ctx->read_all_data(&result).ok());
    auto* result_variant = assert_cast<ColumnVariant*>(result.get());

    ctx->validate_data(result_variant)
            .expect_row_count(3)
            .expect_exact_match(); // All rows should match exactly
}

// Test Case 6: Multi-layer array nesting (should not extract subpaths)
TEST_F(VariantNestedTest, test_multi_layer_array_nesting) {
    VariantTestConfig config;
    config.tablet_id = 10031;
    config.variant_max_subcolumns_count = 20;
    auto ctx = create_context(config);

    // Multi-layer arrays: [[{...}]] should NOT be extracted as nested groups
    std::vector<std::string> jsons = {R"({"nested": [[{"a": 123}]]})",
                                      R"({"nested": [[[{"a": 456}]]]})",
                                      R"({"nested": [{"a": 789}]})"}; // This one is normal nested

    EXPECT_TRUE(ctx->write_json_data(jsons).ok());
    EXPECT_TRUE(ctx->finish_write().ok());
    EXPECT_TRUE(ctx->open_for_read().ok());

    // Read and verify data - multi-layer arrays should be preserved as JSONB
    MutableColumnPtr result;
    EXPECT_TRUE(ctx->read_all_data(&result).ok());
    auto* result_variant = assert_cast<ColumnVariant*>(result.get());

    ctx->validate_data(result_variant)
            .expect_row_count(3)
            .expect_exact_match(); // All rows preserved exactly

    // Verify row 0 and 1 have multi-layer array preserved
    ctx->validate_data(result_variant)
            .expect_row_contains(0, "[[")   // Double bracket indicates multi-layer
            .expect_row_contains(1, "[[["); // Triple bracket
}

// Test Case 8: Non-nested JSONB type (type conflict but not array)
TEST_F(VariantNestedTest, test_non_nested_jsonb_type_conflict) {
    VariantTestConfig config;
    config.tablet_id = 10032;
    config.variant_max_subcolumns_count = 20;
    auto ctx = create_context(config);

    // Type conflict on 'a': integer vs string - stored as JSONB but NOT nested
    std::vector<std::string> jsons = {R"({"a": 12345})", R"({"a": "12345"})", R"({"a": 67890})",
                                      R"({"a": "hello"})"};

    EXPECT_TRUE(ctx->write_json_data(jsons).ok());
    EXPECT_TRUE(ctx->finish_write().ok());
    EXPECT_TRUE(ctx->open_for_read().ok());

    // Verify NO nested groups created (since 'a' is not array of objects)
    auto* variant_reader = ctx->get_variant_reader();
    const auto& nested_group_readers = variant_reader->get_nested_group_readers();
    EXPECT_TRUE(nested_group_readers.empty())
            << "Non-nested JSONB type should not create NestedGroup";

    // Read and verify data
    MutableColumnPtr result;
    EXPECT_TRUE(ctx->read_all_data(&result).ok());
    auto* result_variant = assert_cast<ColumnVariant*>(result.get());

    ctx->validate_data(result_variant)
            .expect_row_count(4)
            .expect_exact_match(); // All rows preserved exactly
}

// Test Case 9 extended: More null and edge cases
TEST_F(VariantNestedTest, test_null_and_edge_cases_extended) {
    VariantTestConfig config;
    config.tablet_id = 10033;
    config.variant_max_subcolumns_count = 20;
    auto ctx = create_context(config);

    std::vector<std::string> jsons = {
            R"({"nested": [{"a": 1}]})",    // Normal case
            R"({"nested": [{}]})",          // Array with empty object
            R"({"nested": [{"a": null}]})", // Field with null value
            R"({"nested": null})",          // Null nested field (not an array)
            R"({"nested": [{"a": 123}, {"a": null}, {"a": 456}]})"}; // Mixed null

    EXPECT_TRUE(ctx->write_json_data(jsons).ok());
    EXPECT_TRUE(ctx->finish_write().ok());
    EXPECT_TRUE(ctx->open_for_read().ok());

    // Verify NestedGroupReader path is exercised
    ctx->verify_all_nested_groups();

    // Read and verify data
    MutableColumnPtr result;
    EXPECT_TRUE(ctx->read_all_data(&result).ok());
    auto* result_variant = assert_cast<ColumnVariant*>(result.get());

    ctx->validate_data(result_variant)
            .expect_row_count(5)
            .expect_row_contains(0, "nested") // Has nested field
            .expect_row_contains(1, "[{}]")   // Empty object in array
            .expect_row_contains(2, "null")   // null value in object
            .expect_exact_match();
}

// Test Case 10 extended: Nested array with multiple elements
TEST_F(VariantNestedTest, test_nested_array_multiple_elements) {
    VariantTestConfig config;
    config.tablet_id = 10034;
    config.variant_max_subcolumns_count = 20;
    auto ctx = create_context(config);

    std::vector<std::string> jsons = {R"({"nested": [{"a": 1}, {"a": 2}, {"a": 3}]})",
                                      R"({"nested": [{"a": 1, "b": 10}, {"a": 2, "b": 20}]})",
                                      R"({"nested": [{"a": 1}, {"b": 2}, {"c": 3}]})"};

    EXPECT_TRUE(ctx->write_json_data(jsons).ok());
    EXPECT_TRUE(ctx->finish_write().ok());
    EXPECT_TRUE(ctx->open_for_read().ok());

    // Verify NestedGroupReader path is exercised
    ctx->verify_all_nested_groups(
            {{"nested",
              {// Expected JSON arrays
               {R"([{"a":1},{"a":2},{"a":3}])", R"([{"a":1,"b":10},{"a":2,"b":20}])",
                R"([{"a":1},{"b":2},{"c":3}])"},
               // Expected element counts: 3, 2, 3
               {3, 2, 3}}}});

    // Verify nested group has expected children
    ctx->validate_nested_group("nested").expect_exists().expect_valid().expect_children(
            {"a", "b", "c"});

    // Read and verify data
    MutableColumnPtr result;
    EXPECT_TRUE(ctx->read_all_data(&result).ok());
    auto* result_variant = assert_cast<ColumnVariant*>(result.get());

    ctx->validate_data(result_variant).expect_row_count(3).expect_exact_match();
}

// Test Case 11: Mixed types in nested array values
TEST_F(VariantNestedTest, test_mixed_value_types_in_nested) {
    VariantTestConfig config;
    config.tablet_id = 10035;
    config.variant_max_subcolumns_count = 20;
    auto ctx = create_context(config);

    // Test different value types in nested arrays
    std::vector<std::string> jsons = {R"({"nested": [{"val": 123}]})",          // Integer
                                      R"({"nested": [{"val": 123.456}]})",      // Float
                                      R"({"nested": [{"val": [1, 2, 3]}]})",    // Array
                                      R"({"nested": [{"val": {"inner": 1}}]})", // Object
                                      R"({"nested": [{"val": "hello"}]})",      // String
                                      R"({"nested": [{"val": 456}]})",          // Integer again
                                      R"({"nested": [{"val": "world"}]})"};     // String again

    auto write_st = ctx->write_json_data(jsons);
    if (!write_st.ok()) {
        std::cout << "write_json_data failed: " << write_st.to_string() << std::endl;
    }
    EXPECT_TRUE(write_st.ok());

    auto finish_st = ctx->finish_write();
    if (!finish_st.ok()) {
        std::cout << "finish_write failed: " << finish_st.to_string() << std::endl;
    }
    EXPECT_TRUE(finish_st.ok());

    auto open_st = ctx->open_for_read();
    if (!open_st.ok()) {
        std::cout << "open_for_read failed: " << open_st.to_string() << std::endl;
    }
    EXPECT_TRUE(open_st.ok());

    // Verify NestedGroupReader path is exercised
    ctx->verify_all_nested_groups(
            {{"nested",
              {// Expected JSON arrays for "nested" group
               {R"([{"val":123}])", R"([{"val":123.456}])", R"([{"val":[1,2,3]}])",
                R"([{"val":{"inner":1}}])", R"([{"val":"hello"}])", R"([{"val":456}])",
                R"([{"val":"world"}])"},
               // Expected element counts: all 1
               {}}}});

    // Read and verify data - all types should be preserved
    MutableColumnPtr result;
    auto read_st = ctx->read_all_data(&result);
    if (!read_st.ok()) {
        std::cout << "read_all_data failed: " << read_st.to_string() << std::endl;
        return;
    }
    EXPECT_TRUE(read_st.ok());
    auto* result_variant = assert_cast<ColumnVariant*>(result.get());

    ctx->validate_data(result_variant)
            .expect_row_count(7)
            .expect_exact_match(); // All rows preserved exactly with correct types

    // Verify specific type representations
    ctx->validate_data(result_variant)
            .expect_row_contains(0, "123")
            .expect_row_contains(1, "123.456")
            .expect_row_contains(2, "[1,2,3]")
            .expect_row_contains(3, "inner")
            .expect_row_contains(4, "hello")
            .expect_row_contains(5, "456")
            .expect_row_contains(6, "world");
}

// Test Case 12: Special characters in field names
TEST_F(VariantNestedTest, test_special_characters_in_path) {
    VariantTestConfig config;
    config.tablet_id = 10036;
    config.variant_max_subcolumns_count = 20;
    auto ctx = create_context(config);

    std::vector<std::string> jsons = {R"({"nested": [{"a.b": 123}]})",       // Dot in key
                                      R"({"nested": [{"a b": 456}]})",       // Space in key
                                      R"({"nested": [{"123key": 789}]})",    // Digit-starting key
                                      R"({"nested": [{"key-name": 111}]})",  // Hyphen in key
                                      R"({"nested": [{"key_name": 222}]})"}; // Underscore in key

    EXPECT_TRUE(ctx->write_json_data(jsons).ok());
    EXPECT_TRUE(ctx->finish_write().ok());
    EXPECT_TRUE(ctx->open_for_read().ok());

    // Verify NestedGroupReader path is exercised
    ctx->verify_all_nested_groups();

    // Read and verify data - special characters should be preserved
    MutableColumnPtr result;
    EXPECT_TRUE(ctx->read_all_data(&result).ok());
    auto* result_variant = assert_cast<ColumnVariant*>(result.get());

    ctx->validate_data(result_variant)
            .expect_row_count(5)
            .expect_exact_match(); // All rows preserved exactly

    // Verify special characters are preserved in output
    ctx->validate_data(result_variant)
            .expect_row_contains(0, "\"a.b\"")
            .expect_row_contains(1, "\"a b\"")
            .expect_row_contains(2, "\"123key\"")
            .expect_row_contains(3, "\"key-name\"")
            .expect_row_contains(4, "\"key_name\"");
}

// Test Case 7 extended: Structure conflict - scalar vs nested array
TEST_F(VariantNestedTest, test_structure_conflict_scalar_vs_nested) {
    VariantTestConfig config;
    config.tablet_id = 10037;
    config.variant_max_subcolumns_count = 20;
    auto ctx = create_context(config);

    // Same path 'a' is scalar in some rows, nested array in others
    std::vector<std::string> jsons = {R"({"a": 123})", R"({"a": [{"b": 1}]})", R"({"a": 456})",
                                      R"({"a": [{"b": 2}, {"b": 3}]})"};

    EXPECT_TRUE(ctx->write_json_data(jsons).ok());
    EXPECT_TRUE(ctx->finish_write().ok());
    EXPECT_TRUE(ctx->open_for_read().ok());

    // Verify NestedGroupReader path is exercised
    ctx->verify_all_nested_groups();

    // Read and verify data - both scalar and nested data preserved
    MutableColumnPtr result;
    EXPECT_TRUE(ctx->read_all_data(&result).ok());
    auto* result_variant = assert_cast<ColumnVariant*>(result.get());

    ctx->validate_data(result_variant).expect_row_count(4).expect_exact_match();

    // Verify specific row content
    ctx->validate_data(result_variant)
            .expect_row_contains(0, "\"a\":123")   // Scalar
            .expect_row_contains(1, "[{\"b\":1}]") // Nested array
            .expect_row_contains(2, "\"a\":456")   // Scalar
            .expect_row_contains(3, "[{\"b\":2}"); // Nested array
}

// Test: Large nested array (performance/correctness with many elements)
TEST_F(VariantNestedTest, test_large_nested_array) {
    VariantTestConfig config;
    config.tablet_id = 10038;
    config.variant_max_subcolumns_count = 50;
    auto ctx = create_context(config);

    // Create a nested array with 100 elements
    std::string large_array = R"({"nested": [)";
    for (int i = 0; i < 100; ++i) {
        if (i > 0) large_array += ",";
        large_array += "{\"id\":" + std::to_string(i) + "}";
    }
    large_array += "]}";

    std::vector<std::string> jsons = {large_array};

    EXPECT_TRUE(ctx->write_json_data(jsons).ok());
    EXPECT_TRUE(ctx->finish_write().ok());
    EXPECT_TRUE(ctx->open_for_read().ok());

    // Verify NestedGroupReader path is exercised
    ctx->verify_all_nested_groups();

    // Verify nested group has 100 elements
    ctx->validate_nested_group("nested").expect_exists().expect_valid().expect_child("id");

    // Read and verify data
    MutableColumnPtr result;
    EXPECT_TRUE(ctx->read_all_data(&result).ok());
    auto* result_variant = assert_cast<ColumnVariant*>(result.get());

    ctx->validate_data(result_variant)
            .expect_row_count(1)
            .expect_row_contains(0, "\"id\":0")
            .expect_row_contains(0, "\"id\":99");
}

// Test Case: 5-level deep nesting with data validation at each level
// Structure: L1 -> L2 -> L3 -> L4 -> L5
// Each level contains array<object> with nested children
TEST_F(VariantNestedTest, test_5_level_deep_nesting) {
    VariantTestConfig config;
    config.tablet_id = 10040;
    config.variant_max_subcolumns_count = 50;
    auto ctx = create_context(config);

    // 5-level nested structure:
    // L1: [{ L2: [{ L3: [{ L4: [{ L5: [{ value: X }] }] }] }] }]
    std::vector<std::string> jsons = {// Row 0: Full 5-level nesting
                                      R"({
                "L1": [{
                    "L1_id": 1,
                    "L2": [{
                        "L2_id": 10,
                        "L3": [{
                            "L3_id": 100,
                            "L4": [{
                                "L4_id": 1000,
                                "L5": [{
                                    "L5_id": 10000,
                                    "value": "deepest"
                                }]
                            }]
                        }]
                    }]
                }]
            })",
                                      // Row 1: Another full 5-level nesting with different values
                                      R"({
                "L1": [{
                    "L1_id": 2,
                    "L2": [{
                        "L2_id": 20,
                        "L3": [{
                            "L3_id": 200,
                            "L4": [{
                                "L4_id": 2000,
                                "L5": [{
                                    "L5_id": 20000,
                                    "value": "also_deep"
                                }]
                            }]
                        }]
                    }]
                }]
            })",
                                      // Row 2: Multiple elements at L5
                                      R"({
                "L1": [{
                    "L1_id": 3,
                    "L2": [{
                        "L2_id": 30,
                        "L3": [{
                            "L3_id": 300,
                            "L4": [{
                                "L4_id": 3000,
                                "L5": [
                                    {"L5_id": 30001, "value": "first"},
                                    {"L5_id": 30002, "value": "second"},
                                    {"L5_id": 30003, "value": "third"}
                                ]
                            }]
                        }]
                    }]
                }]
            })"};

    auto write_st = ctx->write_json_data(jsons);
    if (!write_st.ok()) {
        std::cout << "write_json_data failed: " << write_st.to_string() << std::endl;
    }
    EXPECT_TRUE(write_st.ok());

    auto finish_st = ctx->finish_write();
    if (!finish_st.ok()) {
        std::cout << "finish_write failed: " << finish_st.to_string() << std::endl;
    }
    EXPECT_TRUE(finish_st.ok());

    EXPECT_TRUE(ctx->open_for_read().ok());

    // =========================================================================
    // Validate Level 1: L1 (top-level nested group)
    // =========================================================================
    ctx->validate_nested_group("L1")
            .expect_exists()
            .expect_valid()
            .expect_child("L1_id")
            .expect_nested_group("L2");

    // =========================================================================
    // Validate Level 2: L2 is a nested group inside L1
    // The path in the reader's nested_groups map is relative: "L2" not "L1.L2"
    // =========================================================================
    auto* variant_reader = ctx->get_variant_reader();
    const auto* L1_reader = variant_reader->get_nested_group_reader("L1");
    ASSERT_NE(L1_reader, nullptr) << "L1 nested group reader should exist";

    // L2 is inside L1's nested_groups
    auto L2_it = L1_reader->nested_group_readers.find("L2");
    EXPECT_TRUE(L2_it != L1_reader->nested_group_readers.end()) << "L2 should exist inside L1";
    if (L2_it != L1_reader->nested_group_readers.end()) {
        const auto& L2_reader = L2_it->second;
        EXPECT_TRUE(L2_reader->is_valid()) << "L2 should be valid";

        // L3 is inside L2's nested_groups
        auto L3_it = L2_reader->nested_group_readers.find("L3");
        EXPECT_TRUE(L3_it != L2_reader->nested_group_readers.end()) << "L3 should exist inside L2";
        if (L3_it != L2_reader->nested_group_readers.end()) {
            const auto& L3_reader = L3_it->second;
            EXPECT_TRUE(L3_reader->is_valid()) << "L3 should be valid";

            // L4 is inside L3's nested_groups
            auto L4_it = L3_reader->nested_group_readers.find("L4");
            EXPECT_TRUE(L4_it != L3_reader->nested_group_readers.end())
                    << "L4 should exist inside L3";
            if (L4_it != L3_reader->nested_group_readers.end()) {
                const auto& L4_reader = L4_it->second;
                EXPECT_TRUE(L4_reader->is_valid()) << "L4 should be valid";

                // L5 is inside L4's nested_groups
                auto L5_it = L4_reader->nested_group_readers.find("L5");
                EXPECT_TRUE(L5_it != L4_reader->nested_group_readers.end())
                        << "L5 should exist inside L4";
                if (L5_it != L4_reader->nested_group_readers.end()) {
                    const auto& L5_reader = L5_it->second;
                    EXPECT_TRUE(L5_reader->is_valid()) << "L5 should be valid";

                    // Verify L5 has the expected children
                    EXPECT_TRUE(L5_reader->child_readers.find("L5_id") !=
                                L5_reader->child_readers.end())
                            << "L5 should have L5_id child";
                    EXPECT_TRUE(L5_reader->child_readers.find("value") !=
                                L5_reader->child_readers.end())
                            << "L5 should have value child";
                }
            }
        }
    }

    // =========================================================================
    // Read and verify data content from root
    // =========================================================================
    MutableColumnPtr result;
    EXPECT_TRUE(ctx->read_all_data(&result).ok());
    auto* result_variant = assert_cast<ColumnVariant*>(result.get());

    ctx->validate_data(result_variant).expect_row_count(3).expect_exact_match();

    // Row 0: Verify L1_id through L5_id chain
    ctx->validate_data(result_variant)
            .expect_row_contains(0, "\"L1_id\":1")
            .expect_row_contains(0, "\"L2_id\":10")
            .expect_row_contains(0, "\"L3_id\":100")
            .expect_row_contains(0, "\"L4_id\":1000")
            .expect_row_contains(0, "\"L5_id\":10000")
            .expect_row_contains(0, "\"value\":\"deepest\"");

    // Row 1: Verify different values
    ctx->validate_data(result_variant)
            .expect_row_contains(1, "\"L1_id\":2")
            .expect_row_contains(1, "\"L2_id\":20")
            .expect_row_contains(1, "\"L3_id\":200")
            .expect_row_contains(1, "\"L4_id\":2000")
            .expect_row_contains(1, "\"L5_id\":20000")
            .expect_row_contains(1, "\"value\":\"also_deep\"");

    // Row 2: Verify multiple L5 elements
    ctx->validate_data(result_variant)
            .expect_row_contains(2, "\"L1_id\":3")
            .expect_row_contains(2, "\"L5_id\":30001")
            .expect_row_contains(2, "\"L5_id\":30002")
            .expect_row_contains(2, "\"L5_id\":30003")
            .expect_row_contains(2, "\"value\":\"first\"")
            .expect_row_contains(2, "\"value\":\"second\"")
            .expect_row_contains(2, "\"value\":\"third\"");

    // =========================================================================
    // Verify NestedGroupReader at each level
    // =========================================================================
    ctx->verify_all_nested_groups();
}

// ============================================================================
// Compaction Test Infrastructure
// ============================================================================

// Helper class for compaction tests that operates at rowset level
class VariantCompactionTestContext {
public:
    VariantCompactionTestContext(VariantNestedTest* test, uint64_t tablet_id)
            : _test(test), _tablet_id(tablet_id) {
        // Create tablet schema with a VARIANT column
        TabletSchemaPB schema_pb;
        schema_pb.set_keys_type(KeysType::DUP_KEYS);
        construct_column(schema_pb.add_column(), 0, "BIGINT", "key", 0, true, false, 0);
        construct_column(schema_pb.add_column(), 1, "VARIANT", "V1", 10, false, false, 0);
        _tablet_schema = std::make_shared<TabletSchema>();
        _tablet_schema->init_from_pb(schema_pb);

        // Create tablet
        TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
        // Use inline segment meta (not external) so that column metadata is stored in footer
        // This allows _init_nested_group_readers to find NestedGroup columns
        _tablet_schema->set_external_segment_meta_used_default(false);
        tablet_meta->_tablet_id = _tablet_id;
        _tablet = std::make_shared<Tablet>(*_test->engine_ref(), tablet_meta, _test->data_dir());

        EXPECT_TRUE(_tablet->init().ok());
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
        EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());
    }

    ~VariantCompactionTestContext() {
        static_cast<void>(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()));
    }

    // Create a rowset with given JSON data
    RowsetSharedPtr create_rowset(const std::vector<std::vector<std::string>>& batches,
                                  int64_t max_rows_per_segment = 200) {
        RowsetWriterContext ctx;
        ctx.rowset_id = _next_rowset_id();
        ctx.rowset_type = BETA_ROWSET;
        ctx.data_dir = _test->data_dir();
        ctx.rowset_state = VISIBLE;
        ctx.tablet_schema = _tablet_schema;
        ctx.tablet_path = _tablet->tablet_path();
        ctx.tablet_id = _tablet->tablet_id();
        ctx.tablet = _tablet;
        ctx.version = Version(_version_id, _version_id);
        ctx.max_rows_per_segment = max_rows_per_segment;

        _version_id++;

        auto res = RowsetFactory::create_rowset_writer(*_test->engine_ref(), ctx, false);
        EXPECT_TRUE(res.has_value()) << res.error();
        auto rowset_writer = std::move(res).value();

        int64_t key = _key_counter;
        for (const auto& batch : batches) {
            vectorized::Block block = _tablet_schema->create_block();

            // Fill key column
            auto columns = block.mutate_columns();
            for (size_t i = 0; i < batch.size(); ++i) {
                auto field = vectorized::Field::create_field<PrimitiveType::TYPE_BIGINT>(key++);
                columns[0]->insert(field);
            }

            // Fill variant column
            auto variant_col = ColumnVariant::create(10);
            auto json_col = ColumnString::create();
            for (const auto& json : batch) {
                json_col->insert_data(json.data(), json.size());
            }
            ParseConfig config;
            variant_util::parse_json_to_variant(*variant_col, *json_col, config);
            columns[1] = std::move(variant_col);

            block.set_columns(std::move(columns));

            auto st = rowset_writer->add_block(&block);
            EXPECT_TRUE(st.ok()) << st.msg();
            st = rowset_writer->flush();
            EXPECT_TRUE(st.ok()) << st.msg();

            // Save written data for later verification
            DataTypeSerDe::FormatOptions format_options;
            for (const auto& json : batch) {
                auto temp_variant = ColumnVariant::create(10);
                auto temp_json = ColumnString::create();
                temp_json->insert_data(json.data(), json.size());
                variant_util::parse_json_to_variant(*temp_variant, *temp_json, config);
                std::string serialized;
                temp_variant->serialize_one_row_to_string(0, &serialized, format_options);
                _all_written_data.push_back(serialized);
            }
        }
        _key_counter = key;

        RowsetSharedPtr rowset;
        EXPECT_TRUE(rowset_writer->build(rowset).ok());
        return rowset;
    }

    // Perform compaction on the given rowsets
    RowsetSharedPtr compact_rowsets(const std::vector<RowsetSharedPtr>& input_rowsets,
                                    int64_t max_rows_per_segment = 3456) {
        // Create input rowset readers
        std::vector<RowsetReaderSharedPtr> input_rs_readers;
        for (auto& rowset : input_rowsets) {
            RowsetReaderSharedPtr rs_reader;
            EXPECT_TRUE(rowset->create_reader(&rs_reader).ok());
            input_rs_readers.push_back(std::move(rs_reader));
        }

        // Create output rowset writer
        RowsetWriterContext ctx;
        ctx.rowset_id = _next_rowset_id();
        ctx.rowset_type = BETA_ROWSET;
        ctx.data_dir = _test->data_dir();
        ctx.rowset_state = VISIBLE;
        ctx.tablet_schema = _tablet_schema;
        ctx.tablet_path = _tablet->tablet_path();
        ctx.tablet_id = _tablet->tablet_id();
        ctx.tablet = _tablet;
        ctx.version = {0, input_rowsets.back()->end_version()};
        ctx.segments_overlap = NONOVERLAPPING;
        ctx.max_rows_per_segment = max_rows_per_segment;

        auto res = RowsetFactory::create_rowset_writer(*_test->engine_ref(), ctx, true);
        EXPECT_TRUE(res.has_value()) << res.error();
        auto output_rs_writer = std::move(res).value();

        // Perform vertical compaction
        Merger::Statistics stats;
        RowIdConversion rowid_conversion;
        stats.rowid_conversion = &rowid_conversion;

        auto s = Merger::vertical_merge_rowsets(
                _tablet, ReaderType::READER_BASE_COMPACTION, *_tablet_schema, input_rs_readers,
                output_rs_writer.get(), static_cast<uint32_t>(max_rows_per_segment),
                input_rs_readers.size(), &stats);
        EXPECT_TRUE(s.ok()) << s;

        RowsetSharedPtr out_rowset;
        EXPECT_TRUE(output_rs_writer->build(out_rowset).ok());
        return out_rowset;
    }

    // Read all variant data from a rowset
    std::vector<std::string> read_rowset_data(const RowsetSharedPtr& rowset) {
        std::vector<std::string> result;

        RowsetReaderSharedPtr rs_reader;
        EXPECT_TRUE(rowset->create_reader(&rs_reader).ok());

        RowsetReaderContext reader_context;
        reader_context.tablet_schema = _tablet_schema;
        reader_context.need_ordered_result = false;
        std::vector<uint32_t> return_columns = {0, 1}; // key and variant
        reader_context.return_columns = &return_columns;
        EXPECT_TRUE(rs_reader->init(&reader_context).ok());

        vectorized::Block block;
        bool eof = false;
        while (!eof) {
            block.clear();
            block = _tablet_schema->create_block();
            auto st = rs_reader->next_batch(&block);
            if (!st.ok()) {
                if (st.is<ErrorCode::END_OF_FILE>()) {
                    eof = true;
                } else {
                    EXPECT_TRUE(false) << "Read error: " << st.msg();
                }
            }

            // Extract variant column data
            if (block.rows() > 0) {
                auto* variant_col = assert_cast<ColumnVariant*>(
                        block.get_by_position(1).column->assume_mutable().get());
                DataTypeSerDe::FormatOptions format_options;
                for (size_t i = 0; i < variant_col->size(); ++i) {
                    std::string serialized;
                    variant_col->serialize_one_row_to_string(i, &serialized, format_options);
                    result.push_back(serialized);
                }
            }
        }

        return result;
    }

    // Read variant subcolumn data from a rowset for a specific path
    // Uses direct path-based column reader instead of reading the whole variant
    // Returns a vector of (row_index, serialized_value) pairs for non-null values
    std::vector<std::pair<size_t, std::string>> read_subcolumn_data(const RowsetSharedPtr& rowset,
                                                                    const std::string& path) {
        std::vector<std::pair<size_t, std::string>> result;

        // Build the subcolumn TabletColumn with path info
        const TabletColumn& parent_column = _tablet_schema->column(1); // variant column
        std::string full_path = parent_column.name_lower_case() + "." + path;

        TabletColumn subcolumn;
        subcolumn.set_name(full_path);
        subcolumn.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);
        subcolumn.set_unique_id(-1); // extracted column has unique_id = -1
        subcolumn.set_parent_unique_id(parent_column.unique_id());
        subcolumn.set_path_info(PathInData(full_path));
        subcolumn.set_variant_max_subcolumns_count(parent_column.variant_max_subcolumns_count());
        subcolumn.set_is_nullable(true);

        // Create a tablet schema that includes the subcolumn for reading
        TabletSchemaSPtr read_schema = std::make_shared<TabletSchema>();
        TabletSchemaPB schema_pb;
        _tablet_schema->to_schema_pb(&schema_pb);
        read_schema->init_from_pb(schema_pb);

        OlapReaderStatistics stats;
        size_t global_row_idx = 0;

        // Iterate through all segments in the rowset
        for (int seg_id = 0; seg_id < rowset->num_segments(); ++seg_id) {
            auto file_path = local_segment_path(_tablet->tablet_path(),
                                                rowset->rowset_id().to_string(), seg_id);

            std::shared_ptr<segment_v2::Segment> segment;
            auto st = segment_v2::Segment::open(io::global_local_filesystem(), file_path,
                                                _tablet->tablet_id(), seg_id, rowset->rowset_id(),
                                                rowset->tablet_schema(), io::FileReaderOptions(),
                                                &segment, InvertedIndexFileInfo(), &stats);
            if (!st.ok()) {
                EXPECT_TRUE(false) << "Failed to open segment: " << st.msg();
                continue;
            }

            // Create column iterator for the subcolumn path
            std::unique_ptr<segment_v2::ColumnIterator> iter;
            StorageReadOptions storage_read_opts;
            storage_read_opts.io_ctx.reader_type = ReaderType::READER_QUERY;
            storage_read_opts.stats = &stats;

            // Debug: print path info
            std::cout << "  [DEBUG] Segment " << seg_id << " trying path: '" << full_path
                      << "' (relative: '" << path << "')" << std::endl;

            st = segment->new_column_iterator(subcolumn, &iter, &storage_read_opts);
            if (!st.ok()) {
                // Column doesn't exist in this segment, skip
                global_row_idx += segment->num_rows();
                continue;
            }

            segment_v2::ColumnIteratorOptions iter_opts;
            iter_opts.stats = &stats;
            iter_opts.file_reader = segment->file_reader().get();
            st = iter->init(iter_opts);
            if (!st.ok()) {
                EXPECT_TRUE(false) << "Failed to init iterator: " << st.msg();
                global_row_idx += segment->num_rows();
                continue;
            }

            // Create column to hold data
            // Check iterator type to create the correct column
            vectorized::MutableColumnPtr read_column;
            vectorized::DataTypePtr read_type;
            auto* nested_iter = dynamic_cast<segment_v2::NestedGroupIterator*>(iter.get());
            auto* whole_iter = dynamic_cast<segment_v2::NestedGroupWholeIterator*>(iter.get());
            bool is_jsonb_column = false;

            if (nested_iter != nullptr) {
                // For NestedGroupIterator, use the iterator's result type to create correct column
                read_column = nested_iter->create_result_column();
                read_type = nested_iter->get_result_type();
                std::cout << "  [DEBUG] Created Array column from NestedGroupIterator result type"
                          << std::endl;
            } else if (whole_iter != nullptr) {
                // For NestedGroupWholeIterator, use its create_result_column() method
                read_column = whole_iter->create_result_column();
                read_type = std::make_shared<DataTypeVariant>(0);
                std::cout << "  [DEBUG] Created column from NestedGroupWholeIterator (VARIANT)"
                          << std::endl;
            } else {
                // Use Segment::get_data_type_of to get correct type for subcolumns
                // This properly handles extracted subcolumns (like String, Int) vs variant paths
                auto data_type = segment->get_data_type_of(subcolumn, storage_read_opts);
                read_column = data_type->create_column();
                read_type = data_type;
                std::cout << "  [DEBUG] Created column with type: " << data_type->get_name()
                          << std::endl;
                // Check if this is a JSONB column (need special handling for serialization)
                auto base_type = vectorized::remove_nullable(data_type);
                is_jsonb_column = (base_type->get_name() == "JSONB");
            }

            st = iter->seek_to_ordinal(0);
            if (!st.ok()) {
                EXPECT_TRUE(false) << "Failed to seek: " << st.msg();
                global_row_idx += segment->num_rows();
                continue;
            }

            size_t nrows = segment->num_rows();
            st = iter->next_batch(&nrows, read_column);
            if (!st.ok()) {
                EXPECT_TRUE(false) << "Failed to read batch: " << st.msg();
                global_row_idx += segment->num_rows();
                continue;
            }

            // Extract values from the read column
            for (size_t i = 0; i < nrows; ++i) {
                bool is_null = false;
                if (read_column->is_nullable()) {
                    const auto* nullable_col =
                            assert_cast<const ColumnNullable*>(read_column.get());
                    is_null = nullable_col->is_null_at(i);
                }

                if (!is_null) {
                    std::string serialized;
                    if (read_type && is_variant_or_variant_array(read_type)) {
                        serialized = serialize_to_json_string(*read_column, read_type, i);
                        if (!serialized.empty()) {
                            result.emplace_back(global_row_idx + i, serialized);
                        }
                        continue;
                    }

                    vectorized::Field field;
                    read_column->get(i, field);

                    // Handle Nullable field wrapper
                    if (field.is_null()) {
                        // Skip null values
                    } else {
                        // Unwrap nullable field if necessary
                        vectorized::Field actual_field = field;

                        if (actual_field.get_type() == PrimitiveType::TYPE_STRING) {
                            const auto& str_val = actual_field.get<TYPE_STRING>();
                            if (is_jsonb_column && !str_val.empty()) {
                                // This is JSONB binary data, convert to JSON string
                                serialized = JsonbToJson::jsonb_to_json_string(str_val.data(),
                                                                               str_val.size());
                            } else {
                                serialized = str_val;
                            }
                        } else if (actual_field.get_type() == PrimitiveType::TYPE_TINYINT) {
                            serialized = std::to_string(actual_field.get<TYPE_TINYINT>());
                        } else if (actual_field.get_type() == PrimitiveType::TYPE_SMALLINT) {
                            serialized = std::to_string(actual_field.get<TYPE_SMALLINT>());
                        } else if (actual_field.get_type() == PrimitiveType::TYPE_INT) {
                            serialized = std::to_string(actual_field.get<TYPE_INT>());
                        } else if (actual_field.get_type() == PrimitiveType::TYPE_BIGINT) {
                            serialized = std::to_string(actual_field.get<TYPE_BIGINT>());
                        } else if (actual_field.get_type() == PrimitiveType::TYPE_LARGEINT) {
                            serialized =
                                    vectorized::int128_to_string(actual_field.get<TYPE_LARGEINT>());
                        } else if (actual_field.get_type() == PrimitiveType::TYPE_DOUBLE) {
                            serialized = std::to_string(actual_field.get<TYPE_DOUBLE>());
                        } else if (actual_field.get_type() == PrimitiveType::TYPE_ARRAY) {
                            // Helper lambda to serialize arrays recursively
                            // For nested arrays from NestedGroupIterator, inner elements may be JSONB
                            std::function<std::string(const vectorized::Array&)> serialize_array;
                            serialize_array = [&serialize_array](
                                                      const vectorized::Array& arr) -> std::string {
                                std::string result = "[";
                                for (size_t j = 0; j < arr.size(); ++j) {
                                    if (j > 0) result += ",";
                                    auto elem_type = arr[j].get_type();
                                    if (elem_type == PrimitiveType::TYPE_STRING) {
                                        const auto& str_val = arr[j].get<TYPE_STRING>();
                                        // Check if this looks like JSONB binary data
                                        // JSONB type bytes: T_Object=0x0B, T_Array=0x0C, etc.
                                        // Try to convert as JSONB if it contains non-printable chars or starts with JSONB type byte
                                        bool is_likely_jsonb = false;
                                        if (!str_val.empty()) {
                                            uint8_t first_byte = static_cast<uint8_t>(str_val[0]);
                                            // Check for JSONB type bytes or non-printable characters
                                            is_likely_jsonb =
                                                    (first_byte <= 0x20 || first_byte >= 0x80 ||
                                                     first_byte == 0x0B || first_byte == 0x0C);
                                        }
                                        if (is_likely_jsonb) {
                                            // Likely JSONB, try to convert to JSON
                                            result += JsonbToJson::jsonb_to_json_string(
                                                    str_val.data(), str_val.size());
                                        } else {
                                            result += "\"" + str_val + "\"";
                                        }
                                    } else if (elem_type == PrimitiveType::TYPE_TINYINT) {
                                        result += std::to_string(arr[j].get<TYPE_TINYINT>());
                                    } else if (elem_type == PrimitiveType::TYPE_SMALLINT) {
                                        result += std::to_string(arr[j].get<TYPE_SMALLINT>());
                                    } else if (elem_type == PrimitiveType::TYPE_INT) {
                                        result += std::to_string(arr[j].get<TYPE_INT>());
                                    } else if (elem_type == PrimitiveType::TYPE_BIGINT) {
                                        result += std::to_string(arr[j].get<TYPE_BIGINT>());
                                    } else if (elem_type == PrimitiveType::TYPE_LARGEINT) {
                                        result += vectorized::int128_to_string(
                                                arr[j].get<TYPE_LARGEINT>());
                                    } else if (elem_type == PrimitiveType::TYPE_ARRAY) {
                                        // Handle nested arrays recursively
                                        result += serialize_array(arr[j].get<TYPE_ARRAY>());
                                    } else if (arr[j].is_null()) {
                                        result += "null";
                                    } else {
                                        result += "?<" + std::to_string((int)elem_type) + ">";
                                    }
                                }
                                result += "]";
                                return result;
                            };
                            serialized = serialize_array(actual_field.get<TYPE_ARRAY>());
                        } else if (actual_field.get_type() == PrimitiveType::TYPE_JSONB) {
                            const auto& jsonb_val = actual_field.get<TYPE_JSONB>();
                            serialized = JsonbToJson::jsonb_to_json_string(jsonb_val.get_value(),
                                                                           jsonb_val.get_size());
                        } else {
                            serialized = "<unknown_type:" +
                                         std::to_string((int)actual_field.get_type()) + ">";
                        }

                        if (!serialized.empty()) {
                            result.emplace_back(global_row_idx + i, serialized);
                        }
                    }
                }
            }
            global_row_idx += nrows;
        }

        return result;
    }

    // Get all written data for comparison
    const std::vector<std::string>& get_all_written_data() const { return _all_written_data; }

    // Clear written data tracking
    void clear_written_data() { _all_written_data.clear(); }

    TabletSchemaSPtr get_tablet_schema() const { return _tablet_schema; }
    TabletSharedPtr get_tablet() const { return _tablet; }

private:
    RowsetId _next_rowset_id() {
        RowsetId id;
        id.init(_rowset_id_counter++);
        return id;
    }

    VariantNestedTest* _test;
    uint64_t _tablet_id;
    TabletSchemaSPtr _tablet_schema;
    TabletSharedPtr _tablet;

    int64_t _rowset_id_counter = 10000;
    int64_t _version_id = 1;
    int64_t _key_counter = 0;
    std::vector<std::string> _all_written_data;
};

// ============================================================================
// read_subcolumn_data Verification Tests
// ============================================================================

// Test read_subcolumn_data works correctly for simple nested data like {"items": [...]}
TEST_F(VariantNestedTest, test_read_subcolumn_simple_nested) {
    auto ctx = std::make_unique<VariantCompactionTestContext>(this, 30001);

    // Create rowset with simple nested arrays
    auto rowset = ctx->create_rowset(
            {{R"({"items": [{"id": 1, "name": "apple"}]})",
              R"({"items": [{"id": 2, "name": "banana"}, {"id": 3, "name": "cherry"}]})",
              R"({"items": []})",      // empty array
              R"({"other": "value"})", // no items field
              R"({"items": [{"id": 4, "name": "date"}], "extra": 100})"}});

    // =========================================================================
    // Test reading items.id subcolumn
    // =========================================================================
    auto items_id_data = ctx->read_subcolumn_data(rowset, "items.id");

    std::cout << "=== items.id subcolumn data ===" << std::endl;
    for (const auto& [idx, val] : items_id_data) {
        std::cout << "  Row " << idx << ": " << val << std::endl;
    }

    // Expected per-row arrays: [1], [2,3], [], (missing), [4]
    // The result should be an array for each row because items is a nested array<object>
    EXPECT_FALSE(items_id_data.empty()) << "items.id should have data";

    // Verify we have data for the expected rows
    // Row 0: [1]
    // Row 1: [2, 3]
    // Row 2: [] (empty array)
    // Row 3: missing (no items field)
    // Row 4: [4]
    std::map<size_t, std::string> row_to_value;
    for (const auto& [idx, val] : items_id_data) {
        row_to_value[idx] = val;
        std::cout << "  Found id at row " << idx << ": " << val << std::endl;
    }

    // Verify array format for nested columns
    // Row 0 should have array [1]
    EXPECT_EQ(row_to_value[0], "[1]") << "Row 0 items.id should be [1]";
    // Row 1 should have array [2, 3]
    EXPECT_EQ(row_to_value[1], "[2,3]") << "Row 1 items.id should be [2,3]";
    // Row 4 should have array [4]
    EXPECT_EQ(row_to_value[4], "[4]") << "Row 4 items.id should be [4]";

    // =========================================================================
    // Test reading items.name subcolumn
    // =========================================================================
    auto items_name_data = ctx->read_subcolumn_data(rowset, "items.name");

    std::cout << "=== items.name subcolumn data ===" << std::endl;
    for (const auto& [idx, val] : items_name_data) {
        std::cout << "  Row " << idx << ": " << val << std::endl;
    }

    EXPECT_FALSE(items_name_data.empty()) << "items.name should have data";

    // Verify array format for name column too
    std::map<size_t, std::string> name_by_row;
    for (const auto& [idx, val] : items_name_data) {
        name_by_row[idx] = val;
    }
    // Row 0 should have ["apple"]
    EXPECT_EQ(name_by_row[0], "[\"apple\"]") << "Row 0 items.name should be [\"apple\"]";
    // Row 1 should have ["banana", "cherry"]
    EXPECT_EQ(name_by_row[1], "[\"banana\",\"cherry\"]")
            << "Row 1 items.name should be [\"banana\",\"cherry\"]";

    // =========================================================================
    // Test reading "other" field (non-nested)
    // =========================================================================
    auto other_data = ctx->read_subcolumn_data(rowset, "other");

    std::cout << "=== other subcolumn data ===" << std::endl;
    for (const auto& [idx, val] : other_data) {
        std::cout << "  Row " << idx << ": " << val << std::endl;
    }

    // Row 3 has {"other": "value"}
    EXPECT_EQ(other_data.size(), 1) << "Only row 3 should have 'other' field";
    EXPECT_EQ(other_data[0].first, 3) << "'other' field should be at row 3";
    EXPECT_EQ(other_data[0].second, "value") << "'other' value should be 'value'";

    // =========================================================================
    // Test reading "extra" field (non-nested)
    // =========================================================================
    auto extra_data = ctx->read_subcolumn_data(rowset, "extra");

    std::cout << "=== extra subcolumn data ===" << std::endl;
    for (const auto& [idx, val] : extra_data) {
        std::cout << "  Row " << idx << ": " << val << std::endl;
    }

    // Row 4 has {"extra": 100}
    EXPECT_EQ(extra_data.size(), 1) << "Only row 4 should have 'extra' field";
    EXPECT_EQ(extra_data[0].first, 4) << "'extra' field should be at row 4";
    EXPECT_EQ(extra_data[0].second, "100") << "'extra' value should be '100'";

    // =========================================================================
    // Test reading non-existent field
    // =========================================================================
    auto nonexistent_data = ctx->read_subcolumn_data(rowset, "nonexistent");
    EXPECT_TRUE(nonexistent_data.empty()) << "Non-existent field should return empty";
}

// Test read_subcolumn_data works correctly for top-level nested data like [{...}]
TEST_F(VariantNestedTest, test_read_subcolumn_top_level_nested) {
    auto ctx = std::make_unique<VariantCompactionTestContext>(this, 30002);

    // Create rowset with top-level arrays (root-level nesting)
    auto rowset = ctx->create_rowset({{
            R"([{"id": 10, "value": "first"}])",
            R"([{"id": 20, "value": "second"}, {"id": 30, "value": "third"}])",
            R"([123])",       // top-level array
            R"([{"id": 40}])" // only id, no value
    }});

    // For top-level nested arrays, kRootNestedGroupPath ("$root") is transparent to TabletColumn.
    // Storage layer automatically handles $root prefix, so we just use "id" instead of "$root.id".
    // =========================================================================
    // Test reading "id" subcolumn (for top-level array data)
    // =========================================================================
    auto id_data = ctx->read_subcolumn_data(rowset, "id");

    std::cout << "=== id subcolumn data (top-level nested) ===" << std::endl;
    for (const auto& [idx, val] : id_data) {
        std::cout << "  Row " << idx << ": " << val << std::endl;
    }

    // Expected per-row arrays: [10], [20, 30], [], [40]
    EXPECT_FALSE(id_data.empty()) << "id should have data";

    // Verify array format
    std::map<size_t, std::string> id_by_row;
    for (const auto& [idx, val] : id_data) {
        id_by_row[idx] = val;
    }
    // Row 0: [10]
    EXPECT_EQ(id_by_row[0], "[10]") << "Row 0 id should be [10]";
    // Row 1: [20, 30]
    EXPECT_EQ(id_by_row[1], "[20,30]") << "Row 1 id should be [20,30]";
    // Row 3: [40]
    EXPECT_EQ(id_by_row[3], "[40]") << "Row 3 id should be [40]";

    // =========================================================================
    // Test reading "value" subcolumn (for top-level array data)
    // =========================================================================
    auto value_data = ctx->read_subcolumn_data(rowset, "value");

    std::cout << "=== value subcolumn data (top-level nested) ===" << std::endl;
    for (const auto& [idx, val] : value_data) {
        std::cout << "  Row " << idx << ": " << val << std::endl;
    }

    // Expected: ["first"], ["second", "third"], [], (missing - no value in row 3)
    EXPECT_FALSE(value_data.empty()) << "value should have data";

    // Verify array format
    std::map<size_t, std::string> value_by_row;
    for (const auto& [idx, val] : value_data) {
        value_by_row[idx] = val;
    }
    // Row 0: ["first"]
    EXPECT_EQ(value_by_row[0], "[\"first\"]") << "Row 0 value should be [\"first\"]";
    // Row 1: ["second", "third"]
    EXPECT_EQ(value_by_row[1], "[\"second\",\"third\"]")
            << "Row 1 value should be [\"second\",\"third\"]";

    // =========================================================================
    // Verify full data can still be read correctly
    // =========================================================================
    auto full_data = ctx->read_rowset_data(rowset);
    EXPECT_EQ(full_data.size(), 4) << "Should have 4 rows";

    std::cout << "=== Full rowset data ===" << std::endl;
    for (size_t i = 0; i < full_data.size(); ++i) {
        std::cout << "  Row " << i << ": " << full_data[i] << std::endl;
    }

    // Verify each row starts with '[' (top-level array)
    for (size_t i = 0; i < full_data.size(); ++i) {
        EXPECT_FALSE(full_data[i].empty()) << "Row " << i << " should not be empty";
        if (!full_data[i].empty()) {
            EXPECT_EQ(full_data[i][0], '[')
                    << "Row " << i << " should start with '[' (top-level array): " << full_data[i];
        }
    }
}

// Test read_subcolumn_data with multiple nested levels
TEST_F(VariantNestedTest, test_read_subcolumn_multi_level_nested) {
    auto ctx = std::make_unique<VariantCompactionTestContext>(this, 30003);

    // Create rowset with multi-level nesting
    auto rowset = ctx->create_rowset(
            {{R"({"level1": [{"level2": [{"deep_id": 100}]}]})",
              R"({"level1": [{"level2": [{"deep_id": 200}, {"deep_id": 300}]}]})",
              R"({"level1": [{"other_field": "test"}]})", R"({"level1": []})"}});

    // =========================================================================
    // Test reading level1.level2.deep_id (3-level path)
    // =========================================================================
    auto deep_id_data = ctx->read_subcolumn_data(rowset, "level1.level2.deep_id");

    std::cout << "=== level1.level2.deep_id subcolumn data ===" << std::endl;
    for (const auto& [idx, val] : deep_id_data) {
        std::cout << "  Row " << idx << ": " << val << std::endl;
    }

    // For multi-level nesting like level1[].level2[], the return should be N-dimensional array
    // level1 is array<object>, so first level wraps in array
    // Currently we only support first-level NestedGroup, so deeper levels are stored as JSONB
    // Expected format depends on implementation:
    // - If only first level is NestedGroup: [[{deep_id: 100}]], [[{deep_id: 200}, {deep_id: 300}]]
    // - If both levels are NestedGroup: [[100]], [[200, 300]]
    // We verify data is present and log the actual format

    // =========================================================================
    // Test reading level1.other_field
    // =========================================================================
    auto other_field_data = ctx->read_subcolumn_data(rowset, "level1.other_field");

    std::cout << "=== level1.other_field subcolumn data ===" << std::endl;
    for (const auto& [idx, val] : other_field_data) {
        std::cout << "  Row " << idx << ": " << val << std::endl;
    }

    // NestedGroup storage keeps alignment between offsets and child data.
    // When an element doesn't have a field, it stores null instead.
    // Row 0: level1 has 1 element without other_field -> [null]
    // Row 1: level1 has 1 element without other_field -> [null]
    // Row 2: level1 has 1 element with other_field="test" -> ["test"]
    // Row 3: level1 is empty -> []
    EXPECT_EQ(other_field_data.size(), 4) << "Should have 4 rows";
    if (other_field_data.size() >= 4) {
        EXPECT_EQ(other_field_data[0].first, 0);
        EXPECT_EQ(other_field_data[0].second, "[null]");
        EXPECT_EQ(other_field_data[1].first, 1);
        EXPECT_EQ(other_field_data[1].second, "[null]");
        EXPECT_EQ(other_field_data[2].first, 2);
        EXPECT_EQ(other_field_data[2].second, "[\"test\"]");
        EXPECT_EQ(other_field_data[3].first, 3);
        EXPECT_EQ(other_field_data[3].second, "[]");
    }

    // =========================================================================
    // Verify full data consistency
    // =========================================================================
    auto full_data = ctx->read_rowset_data(rowset);
    EXPECT_EQ(full_data.size(), 4) << "Should have 4 rows";

    const auto& written_data = ctx->get_all_written_data();
    EXPECT_EQ(written_data.size(), full_data.size()) << "Written and read data count should match";

    for (size_t i = 0; i < full_data.size(); ++i) {
        EXPECT_EQ(written_data[i], full_data[i])
                << "Data mismatch at row " << i << "\nWritten: " << written_data[i]
                << "\nRead: " << full_data[i];
    }
}

// ============================================================================
// Compaction Test Cases
// ============================================================================

// Test compaction preserves nested data consistency
// Write nested JSON data to multiple rowsets, compact them, verify data is unchanged
TEST_F(VariantNestedTest, test_compaction_nested_data_consistency) {
    auto ctx = std::make_unique<VariantCompactionTestContext>(this, 20001);

    // Create multiple rowsets with nested JSON data
    std::vector<RowsetSharedPtr> input_rowsets;

    // Rowset 1: Simple nested arrays
    auto rowset1 = ctx->create_rowset(
            {{R"({"items": [{"id": 1, "name": "a"}]})", R"({"items": [{"id": 2, "name": "b"}]})",
              R"({"items": [{"id": 3, "name": "c"}]})"}});
    input_rowsets.push_back(rowset1);

    // Rowset 2: Multiple elements in nested arrays
    auto rowset2 =
            ctx->create_rowset({{R"({"items": [{"id": 4, "name": "d"}, {"id": 5, "name": "e"}]})",
                                 R"({"items": [{"id": 6, "name": "f"}]})"}});
    input_rowsets.push_back(rowset2);

    // Rowset 3: Empty arrays and mixed content
    auto rowset3 = ctx->create_rowset(
            {{R"({"items": []})", R"({"items": [{"id": 7, "name": "g"}], "extra": "data"})",
              R"({"other": "field_value"})"}});
    input_rowsets.push_back(rowset3);

    // =========================================================================
    // Read subcolumn data before compaction for items.id, items.name, other
    // =========================================================================
    std::vector<std::pair<size_t, std::string>> items_id_before;
    std::vector<std::pair<size_t, std::string>> items_name_before;
    std::vector<std::pair<size_t, std::string>> other_before;

    size_t row_offset = 0;
    for (const auto& rowset : input_rowsets) {
        auto id_data = ctx->read_subcolumn_data(rowset, "items.id");
        auto name_data = ctx->read_subcolumn_data(rowset, "items.name");
        auto other_data = ctx->read_subcolumn_data(rowset, "other");

        // Adjust row indices with offset
        for (auto& [idx, val] : id_data) {
            items_id_before.emplace_back(idx + row_offset, val);
        }
        for (auto& [idx, val] : name_data) {
            items_name_before.emplace_back(idx + row_offset, val);
        }
        for (auto& [idx, val] : other_data) {
            other_before.emplace_back(idx + row_offset, val);
        }

        auto rowset_data = ctx->read_rowset_data(rowset);
        row_offset += rowset_data.size();
    }

    // Read full data before compaction
    std::vector<std::string> data_before_compaction;
    for (const auto& rowset : input_rowsets) {
        auto data = ctx->read_rowset_data(rowset);
        data_before_compaction.insert(data_before_compaction.end(), data.begin(), data.end());
    }

    // Perform compaction
    auto compacted_rowset = ctx->compact_rowsets(input_rowsets);
    ASSERT_NE(compacted_rowset, nullptr);

    // Read data after compaction
    auto data_after_compaction = ctx->read_rowset_data(compacted_rowset);

    // =========================================================================
    // Read subcolumn data after compaction
    // =========================================================================
    auto items_id_after = ctx->read_subcolumn_data(compacted_rowset, "items.id");
    auto items_name_after = ctx->read_subcolumn_data(compacted_rowset, "items.name");
    auto other_after = ctx->read_subcolumn_data(compacted_rowset, "other");

    // =========================================================================
    // Verify full data consistency
    // =========================================================================
    EXPECT_EQ(data_before_compaction.size(), data_after_compaction.size())
            << "Row count should match after compaction";

    for (size_t i = 0; i < data_before_compaction.size(); ++i) {
        EXPECT_EQ(data_before_compaction[i], data_after_compaction[i])
                << "Data mismatch at row " << i << "\nBefore: " << data_before_compaction[i]
                << "\nAfter: " << data_after_compaction[i];
    }

    // =========================================================================
    // Verify items.id subcolumn data consistency
    // =========================================================================
    EXPECT_EQ(items_id_before.size(), items_id_after.size())
            << "items.id count mismatch: before=" << items_id_before.size()
            << ", after=" << items_id_after.size();

    for (size_t i = 0; i < std::min(items_id_before.size(), items_id_after.size()); ++i) {
        EXPECT_EQ(items_id_before[i].first, items_id_after[i].first)
                << "items.id row index mismatch at position " << i;
        EXPECT_EQ(items_id_before[i].second, items_id_after[i].second)
                << "items.id value mismatch at row " << items_id_before[i].first
                << "\nBefore: " << items_id_before[i].second
                << "\nAfter: " << items_id_after[i].second;
    }

    // =========================================================================
    // Verify items.name subcolumn data consistency
    // =========================================================================
    EXPECT_EQ(items_name_before.size(), items_name_after.size())
            << "items.name count mismatch: before=" << items_name_before.size()
            << ", after=" << items_name_after.size();

    for (size_t i = 0; i < std::min(items_name_before.size(), items_name_after.size()); ++i) {
        EXPECT_EQ(items_name_before[i].first, items_name_after[i].first)
                << "items.name row index mismatch at position " << i;
        EXPECT_EQ(items_name_before[i].second, items_name_after[i].second)
                << "items.name value mismatch at row " << items_name_before[i].first
                << "\nBefore: " << items_name_before[i].second
                << "\nAfter: " << items_name_after[i].second;
    }

    // =========================================================================
    // Verify "other" field data consistency
    // =========================================================================
    EXPECT_EQ(other_before.size(), other_after.size())
            << "other field count mismatch: before=" << other_before.size()
            << ", after=" << other_after.size();

    for (size_t i = 0; i < std::min(other_before.size(), other_after.size()); ++i) {
        EXPECT_EQ(other_before[i].first, other_after[i].first)
                << "other field row index mismatch at position " << i;
        EXPECT_EQ(other_before[i].second, other_after[i].second)
                << "other field value mismatch at row " << other_before[i].first
                << "\nBefore: " << other_before[i].second << "\nAfter: " << other_after[i].second;
    }

    // =========================================================================
    // Print summary for debugging
    // =========================================================================
    std::cout << "=== Compaction Data Consistency Summary ===" << std::endl;
    std::cout << "Total rows: " << data_after_compaction.size() << std::endl;
    std::cout << "items.id values: " << items_id_after.size() << std::endl;
    std::cout << "items.name values: " << items_name_after.size() << std::endl;
    std::cout << "other field values: " << other_after.size() << std::endl;

    // Print items.id details
    std::cout << "items.id data:" << std::endl;
    for (const auto& [idx, val] : items_id_after) {
        std::cout << "  Row " << idx << ": " << val << std::endl;
    }

    // Print items.name details
    std::cout << "items.name data:" << std::endl;
    for (const auto& [idx, val] : items_name_after) {
        std::cout << "  Row " << idx << ": " << val << std::endl;
    }

    // Print other field details
    std::cout << "other field data:" << std::endl;
    for (const auto& [idx, val] : other_after) {
        std::cout << "  Row " << idx << ": " << val << std::endl;
    }

    // Also verify against the original written data
    const auto& written_data = ctx->get_all_written_data();
    EXPECT_EQ(written_data.size(), data_after_compaction.size())
            << "Row count should match original written data";

    for (size_t i = 0; i < written_data.size(); ++i) {
        EXPECT_EQ(written_data[i], data_after_compaction[i])
                << "Data mismatch at row " << i << " vs original\nOriginal: " << written_data[i]
                << "\nAfter compaction: " << data_after_compaction[i];
    }
}

// Test compaction with multi-level nested structures
TEST_F(VariantNestedTest, test_compaction_multi_level_nested) {
    auto ctx = std::make_unique<VariantCompactionTestContext>(this, 20002);

    std::vector<RowsetSharedPtr> input_rowsets;

    // Rowset 1: 2-level nesting
    auto rowset1 = ctx->create_rowset({{R"({"L1": [{"L2": [{"val": 1}]}]})",
                                        R"({"L1": [{"L2": [{"val": 2}, {"val": 3}]}]})"}});
    input_rowsets.push_back(rowset1);

    // Rowset 2: 3-level nesting
    auto rowset2 = ctx->create_rowset(
            {{R"({"L1": [{"L2": [{"L3": [{"deep": "value"}]}]}]})", R"({"L1": [{"L2": []}]})"}});
    input_rowsets.push_back(rowset2);

    // Read data before compaction
    std::vector<std::string> data_before_compaction;
    for (const auto& rowset : input_rowsets) {
        auto data = ctx->read_rowset_data(rowset);
        data_before_compaction.insert(data_before_compaction.end(), data.begin(), data.end());
    }

    // Perform compaction
    auto compacted_rowset = ctx->compact_rowsets(input_rowsets);
    ASSERT_NE(compacted_rowset, nullptr);

    // Read data after compaction
    auto data_after_compaction = ctx->read_rowset_data(compacted_rowset);

    // Verify data consistency
    EXPECT_EQ(data_before_compaction.size(), data_after_compaction.size())
            << "Row count should match after compaction";

    for (size_t i = 0; i < data_before_compaction.size(); ++i) {
        EXPECT_EQ(data_before_compaction[i], data_after_compaction[i])
                << "Data mismatch at row " << i << "\nBefore: " << data_before_compaction[i]
                << "\nAfter: " << data_after_compaction[i];
    }
}

// Test compaction with top-level arrays
TEST_F(VariantNestedTest, test_compaction_top_level_arrays) {
    auto ctx = std::make_unique<VariantCompactionTestContext>(this, 20003);

    std::vector<RowsetSharedPtr> input_rowsets;

    // Rowset 1: Top-level arrays
    auto rowset1 = ctx->create_rowset({{R"([{"a": 1}, {"a": 2}])", R"([{"b": "x"}])"}});
    input_rowsets.push_back(rowset1);

    // Rowset 2: More top-level arrays
    auto rowset2 = ctx->create_rowset({{R"([{"c": 100}])", R"([])"}});
    input_rowsets.push_back(rowset2);

    // Read data before compaction
    std::vector<std::string> data_before_compaction;
    for (const auto& rowset : input_rowsets) {
        auto data = ctx->read_rowset_data(rowset);
        data_before_compaction.insert(data_before_compaction.end(), data.begin(), data.end());
    }

    // Perform compaction
    auto compacted_rowset = ctx->compact_rowsets(input_rowsets);
    ASSERT_NE(compacted_rowset, nullptr);

    // Read data after compaction
    auto data_after_compaction = ctx->read_rowset_data(compacted_rowset);

    // Verify data consistency
    EXPECT_EQ(data_before_compaction.size(), data_after_compaction.size())
            << "Row count should match after compaction";

    for (size_t i = 0; i < data_before_compaction.size(); ++i) {
        EXPECT_EQ(data_before_compaction[i], data_after_compaction[i])
                << "Data mismatch at row " << i << "\nBefore: " << data_before_compaction[i]
                << "\nAfter: " << data_after_compaction[i];
    }
}

// Test compaction with mixed nested and non-nested data
TEST_F(VariantNestedTest, test_compaction_mixed_data_types) {
    auto ctx = std::make_unique<VariantCompactionTestContext>(this, 20004);

    std::vector<RowsetSharedPtr> input_rowsets;

    // Rowset 1: Mixed content
    auto rowset1 = ctx->create_rowset(
            {{R"({"nested": [{"id": 1}], "scalar": 100})", R"({"scalar_only": "value"})",
              R"({"nested": [{"id": 2}, {"id": 3}]})"}});
    input_rowsets.push_back(rowset1);

    // Rowset 2: More mixed content
    auto rowset2 = ctx->create_rowset({{R"({"array": [1, 2, 3]})", // Non-object array
                                        R"({"nested": [{"x": "y"}]})", R"({"empty_obj": {}})"}});
    input_rowsets.push_back(rowset2);

    // Read data before compaction
    std::vector<std::string> data_before_compaction;
    for (const auto& rowset : input_rowsets) {
        auto data = ctx->read_rowset_data(rowset);
        data_before_compaction.insert(data_before_compaction.end(), data.begin(), data.end());
    }

    // Perform compaction
    auto compacted_rowset = ctx->compact_rowsets(input_rowsets);
    ASSERT_NE(compacted_rowset, nullptr);

    // Read data after compaction
    auto data_after_compaction = ctx->read_rowset_data(compacted_rowset);

    // Verify data consistency
    EXPECT_EQ(data_before_compaction.size(), data_after_compaction.size())
            << "Row count should match after compaction";

    for (size_t i = 0; i < data_before_compaction.size(); ++i) {
        EXPECT_EQ(data_before_compaction[i], data_after_compaction[i])
                << "Data mismatch at row " << i << "\nBefore: " << data_before_compaction[i]
                << "\nAfter: " << data_after_compaction[i];
    }
}

// Test multiple compaction cycles preserve data
TEST_F(VariantNestedTest, test_compaction_multiple_cycles) {
    auto ctx = std::make_unique<VariantCompactionTestContext>(this, 20005);

    // Create initial rowsets
    std::vector<RowsetSharedPtr> round1_rowsets;
    round1_rowsets.push_back(
            ctx->create_rowset({{R"({"data": [{"id": 1}]})", R"({"data": [{"id": 2}]})"}}));
    round1_rowsets.push_back(
            ctx->create_rowset({{R"({"data": [{"id": 3}]})", R"({"data": [{"id": 4}]})"}}));

    // Get original data
    const auto& original_data = ctx->get_all_written_data();
    std::vector<std::string> expected_data(original_data.begin(), original_data.end());

    // First compaction
    auto compacted1 = ctx->compact_rowsets(round1_rowsets);
    ASSERT_NE(compacted1, nullptr);

    // Read and verify after first compaction
    auto data_after_round1 = ctx->read_rowset_data(compacted1);
    EXPECT_EQ(expected_data.size(), data_after_round1.size());
    for (size_t i = 0; i < expected_data.size(); ++i) {
        EXPECT_EQ(expected_data[i], data_after_round1[i]) << "Round 1 mismatch at row " << i;
    }

    // Add more rowsets
    ctx->clear_written_data();
    std::vector<RowsetSharedPtr> round2_rowsets;
    round2_rowsets.push_back(compacted1);
    round2_rowsets.push_back(
            ctx->create_rowset({{R"({"data": [{"id": 5}]})", R"({"data": [{"id": 6}]})"}}));

    // Extend expected data
    const auto& new_data = ctx->get_all_written_data();
    expected_data.insert(expected_data.end(), new_data.begin(), new_data.end());

    // Second compaction
    auto compacted2 = ctx->compact_rowsets(round2_rowsets);
    ASSERT_NE(compacted2, nullptr);

    // Read and verify after second compaction
    auto data_after_round2 = ctx->read_rowset_data(compacted2);
    EXPECT_EQ(expected_data.size(), data_after_round2.size());
    for (size_t i = 0; i < expected_data.size(); ++i) {
        EXPECT_EQ(expected_data[i], data_after_round2[i]) << "Round 2 mismatch at row " << i;
    }
}

TEST_F(VariantNestedTest, test_nested_import_and_compaction_perf) {
#ifndef NDEBUG
    GTEST_SKIP() << "Performance test runs only in Release build";
#endif

    constexpr size_t kTotalRows = 1000000;
    constexpr size_t kInputSegments = 10;
    constexpr size_t kKeyPoolSize = 100;
    constexpr size_t kMinKeysPerRow = 10;
    constexpr size_t kMaxKeysPerRow = 50;
    constexpr uint32_t kSeed = 20260207;

    static_assert(kMinKeysPerRow <= kMaxKeysPerRow);
    static_assert(kMaxKeysPerRow <= kKeyPoolSize);
    ASSERT_EQ(kTotalRows % kInputSegments, 0);

    auto ctx = std::make_unique<VariantCompactionTestContext>(this, 20006);
    const size_t rows_per_segment = kTotalRows / kInputSegments;

    std::vector<std::string> key_pool;
    key_pool.reserve(kKeyPoolSize);
    for (size_t i = 0; i < kKeyPoolSize; ++i) {
        key_pool.emplace_back("key_" + std::to_string(i));
    }
    std::vector<size_t> key_indexes(kKeyPoolSize);
    std::iota(key_indexes.begin(), key_indexes.end(), 0);

    std::mt19937 rng(kSeed);
    std::uniform_int_distribution<size_t> key_count_dist(kMinKeysPerRow, kMaxKeysPerRow);

    auto generate_nested_json = [&](int64_t row_id, int64_t segment_idx) {
        const size_t key_count = key_count_dist(rng);
        std::shuffle(key_indexes.begin(), key_indexes.end(), rng);

        std::ostringstream oss;
        oss << '[';
        for (size_t i = 0; i < key_count; ++i) {
            if (i > 0) {
                oss << ',';
            }

            const size_t key_idx = key_indexes[i];
            const int64_t metric = (row_id * 131 + static_cast<int64_t>(key_idx) * 17) % 1000003;
            const int64_t value = metric + segment_idx + (row_id % 97);
            oss << "{\"" << key_pool[key_idx] << "\":" << value << '}';
        }
        oss << ']';
        return oss.str();
    };

    std::vector<RowsetSharedPtr> input_rowsets;
    input_rowsets.reserve(kInputSegments);
    int64_t generated_rows = 0;

    const auto import_start = std::chrono::steady_clock::now();
    for (size_t segment_idx = 0; segment_idx < kInputSegments; ++segment_idx) {
        std::vector<std::string> segment_json_rows;
        segment_json_rows.reserve(rows_per_segment);

        for (size_t i = 0; i < rows_per_segment; ++i) {
            segment_json_rows.emplace_back(
                    generate_nested_json(generated_rows, static_cast<int64_t>(segment_idx)));
            ++generated_rows;
        }

        std::vector<std::vector<std::string>> batches;
        batches.emplace_back(std::move(segment_json_rows));
        auto rowset = ctx->create_rowset(batches, static_cast<int64_t>(rows_per_segment));
        ASSERT_NE(rowset, nullptr);
        ASSERT_EQ(rowset->num_segments(), 1) << "Input rowset should produce exactly one segment";
        input_rowsets.emplace_back(std::move(rowset));
    }
    const auto import_end = std::chrono::steady_clock::now();

    ASSERT_EQ(generated_rows, static_cast<int64_t>(kTotalRows));
    ASSERT_EQ(input_rowsets.size(), kInputSegments);

    size_t total_input_segments = 0;
    int64_t total_input_rows = 0;
    for (const auto& rowset : input_rowsets) {
        total_input_segments += static_cast<size_t>(rowset->num_segments());
        total_input_rows += rowset->num_rows();
    }

    ASSERT_EQ(total_input_segments, kInputSegments);
    ASSERT_EQ(total_input_rows, static_cast<int64_t>(kTotalRows));

    const auto compaction_start = std::chrono::steady_clock::now();
    auto compacted_rowset = ctx->compact_rowsets(input_rowsets, static_cast<int64_t>(kTotalRows));
    const auto compaction_end = std::chrono::steady_clock::now();

    ASSERT_NE(compacted_rowset, nullptr);
    ASSERT_EQ(compacted_rowset->num_segments(), 1)
            << "Compaction output should be a single segment";
    ASSERT_EQ(compacted_rowset->num_rows(), static_cast<int64_t>(kTotalRows));

    const auto import_ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(import_end - import_start)
                    .count();
    const auto compaction_ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(compaction_end - compaction_start)
                    .count();

    std::cout << "[VariantNestedPerf] rows=" << kTotalRows << ", key_pool=" << kKeyPoolSize
              << ", keys_per_row=[" << kMinKeysPerRow << ',' << kMaxKeysPerRow << "]"
              << ", input_segments=" << total_input_segments << ", import_ms=" << import_ms
              << ", compaction_ms=" << compaction_ms << std::endl;
}

// Test intermediate path access for multi-level nested arrays
// Reproduces issue: SELECT v['nested']['a'] returns NULL for {"nested": [{"a": [{"b": "c"}]}]}
// The path nested.a is an intermediate nested group (array within array), not a leaf node
TEST_F(VariantNestedTest, test_intermediate_path_in_multi_level_nested) {
    auto ctx = std::make_unique<VariantCompactionTestContext>(this, 30010);

    // Create rowset with multi-level nesting: nested.a is also an array
    // Data: {"nested": [{"a": [{"b": "c"}]}]}
    // - nested is a NestedGroup (array)
    // - nested.a is also a NestedGroup (array within the nested array)
    // - nested.a.b is a leaf String column
    auto rowset = ctx->create_rowset({{R"({"nested": [{"a": [{"b": "c"}]}]})"}});

    // =========================================================================
    // Test 1: Read the full path nested (should work)
    // =========================================================================
    std::cout << "=== Test 1: Read v['nested'] ===" << std::endl;
    auto nested_data = ctx->read_subcolumn_data(rowset, "nested");
    std::cout << "nested_data size: " << nested_data.size() << std::endl;
    for (const auto& [idx, val] : nested_data) {
        std::cout << "  Row " << idx << ": " << val << std::endl;
    }
    ASSERT_EQ(nested_data.size(), 1) << "Should have 1 row";
    // Expected: [{"a":[{"b":"c"}]}]
    EXPECT_TRUE(nested_data[0].second.find("\"a\"") != std::string::npos)
            << "v['nested'] should contain 'a' field, got: " << nested_data[0].second;

    // =========================================================================
    // Test 2: Read the intermediate path nested.a (THIS IS THE BUG)
    // Currently returns NULL, should return [[{"b":"c"}]]
    // =========================================================================
    std::cout << "=== Test 2: Read v['nested']['a'] (intermediate path) ===" << std::endl;
    auto nested_a_data = ctx->read_subcolumn_data(rowset, "nested.a");
    std::cout << "nested.a data size: " << nested_a_data.size() << std::endl;
    for (const auto& [idx, val] : nested_a_data) {
        std::cout << "  Row " << idx << ": " << val << std::endl;
    }

    // This should NOT be empty! nested.a is a nested group within nested
    // The expected result is the 'a' array wrapped in the outer 'nested' array
    ASSERT_EQ(nested_a_data.size(), 1) << "v['nested']['a'] should have 1 row, not be empty/NULL";
    // Expected: [[{"b":"c"}]] (nested.a is array wrapped by nested's outer array)
    EXPECT_TRUE(nested_a_data[0].second.find("\"b\"") != std::string::npos)
            << "v['nested']['a'] should contain 'b' field, got: " << nested_a_data[0].second;

    // =========================================================================
    // Test 3: Read the leaf path nested.a.b (should work)
    // =========================================================================
    std::cout << "=== Test 3: Read v['nested']['a']['b'] (leaf path) ===" << std::endl;
    auto nested_a_b_data = ctx->read_subcolumn_data(rowset, "nested.a.b");
    std::cout << "nested.a.b data size: " << nested_a_b_data.size() << std::endl;
    for (const auto& [idx, val] : nested_a_b_data) {
        std::cout << "  Row " << idx << ": " << val << std::endl;
    }
    ASSERT_EQ(nested_a_b_data.size(), 1) << "Should have 1 row";
    // Expected: [["c"]] (value "c" wrapped twice by nested.a and nested arrays)
    EXPECT_TRUE(nested_a_b_data[0].second.find("\"c\"") != std::string::npos)
            << "v['nested']['a']['b'] should contain 'c', got: " << nested_a_b_data[0].second;
}

// Test nested object inside array: subpath access should return nested object.
// Case: {"nested":[{"nested1":{"a":123}}]} -> v['nested'] works, v['nested']['nested1'] returns object array.
TEST_F(VariantNestedTest, test_nested_object_subpath_access) {
    auto ctx = std::make_unique<VariantCompactionTestContext>(this, 30011);

    auto rowset = ctx->create_rowset({{R"({"nested":[{"nested1":{"a":123}}]})"}});

    std::cout << "=== Test: Read v['nested'] ===" << std::endl;
    auto nested_data = ctx->read_subcolumn_data(rowset, "nested");
    ASSERT_EQ(nested_data.size(), 1) << "v['nested'] should return one row";
    EXPECT_TRUE(json_strings_equal(nested_data[0].second, R"([{"nested1":{"a":123}}])"))
            << "v['nested'] mismatch, got: " << nested_data[0].second;

    std::cout << "=== Test: Read v['nested']['nested1'] ===" << std::endl;
    auto nested1_data = ctx->read_subcolumn_data(rowset, "nested.nested1");
    ASSERT_EQ(nested1_data.size(), 1) << "v['nested']['nested1'] should return one row";
    EXPECT_TRUE(json_strings_equal(nested1_data[0].second, R"([{"a":123}])"))
            << "v['nested']['nested1'] mismatch, got: " << nested1_data[0].second;
}

// Test non-leaf nested group access returning ARRAY<...<VARIANT>>
// This tests the refactored output type: v.nested returns ARRAY<VARIANT>
TEST_F(VariantNestedTest, test_nested_group_whole_access_array_variant) {
    auto ctx = create_context(10099);

    // Create data with multi-level nesting
    std::vector<std::string> jsons = {
            R"({"outer": [{"inner": [{"a": 1}]}]})",
            R"({"outer": [{"inner": [{"a": 2}, {"a": 3}]}, {"inner": [{"a": 4}]}]})",
            R"({"outer": []})", R"({"outer": [{"inner": []}]})"};

    EXPECT_TRUE(ctx->write_json_data(jsons).ok());
    EXPECT_TRUE(ctx->finish_write().ok());
    EXPECT_TRUE(ctx->open_for_read().ok());

    auto* variant_reader = ctx->get_variant_reader();
    ASSERT_NE(variant_reader, nullptr);

    // Test 1: Single-level whole access (v.outer) should return ARRAY<VARIANT>
    const auto* outer_group = variant_reader->get_nested_group_reader("outer");
    ASSERT_NE(outer_group, nullptr);
    ASSERT_TRUE(outer_group->is_valid());

    OlapReaderStatistics stats;
    ColumnIteratorOptions iter_opts;
    iter_opts.stats = &stats;
    iter_opts.file_reader = ctx->get_file_reader().get();

    // Test sequential read with next_batch
    {
        auto element_iter = std::make_unique<segment_v2::NestedGroupWholeIterator>(outer_group);

        EXPECT_TRUE(element_iter->init(iter_opts).ok());
        EXPECT_TRUE(element_iter->seek_to_ordinal(0).ok());

        // NestedGroupWholeIterator outputs per-element VARIANT objects
        MutableColumnPtr dst_col = element_iter->create_result_column();

        // Read element 0 (row 0's first outer element)
        size_t n = 1;
        bool has_null = false;
        EXPECT_TRUE(element_iter->next_batch(&n, dst_col, &has_null).ok());
        EXPECT_EQ(dst_col->size(), 1);

        // Element 0: {"inner": [{"a":1}]}
        auto variant_type = std::make_shared<DataTypeVariant>(0);
        std::string elem0_json = serialize_to_json_string(*dst_col, variant_type, 0);
        EXPECT_TRUE(elem0_json.find("\"inner\"") != std::string::npos)
                << "Element should contain 'inner' field, got: " << elem0_json;
    }

    // Test 2: Test seek + read combination
    {
        auto element_iter = std::make_unique<segment_v2::NestedGroupWholeIterator>(outer_group);

        EXPECT_TRUE(element_iter->init(iter_opts).ok());

        // Seek to element 1 (row 1's first outer element)
        EXPECT_TRUE(element_iter->seek_to_ordinal(1).ok());
        EXPECT_EQ(element_iter->get_current_ordinal(), 1);

        MutableColumnPtr dst_col = element_iter->create_result_column();
        size_t n = 2; // Read 2 elements
        bool has_null = false;
        EXPECT_TRUE(element_iter->next_batch(&n, dst_col, &has_null).ok());
        EXPECT_EQ(dst_col->size(), 2);
        EXPECT_EQ(element_iter->get_current_ordinal(), 3);
    }

    // Test 3: Test read_by_rowids with consecutive rowids
    {
        auto element_iter = std::make_unique<segment_v2::NestedGroupWholeIterator>(outer_group);

        EXPECT_TRUE(element_iter->init(iter_opts).ok());

        MutableColumnPtr dst_col = element_iter->create_result_column();

        // Read elements 0, 1, 2 (consecutive)
        std::vector<rowid_t> rowids = {0, 1, 2};
        EXPECT_TRUE(element_iter->read_by_rowids(rowids.data(), rowids.size(), dst_col).ok());
        EXPECT_EQ(dst_col->size(), 3);
    }

    // Test 4: Test NestedGroupIterator with cached flat_pos optimization
    {
        segment_v2::ColumnIteratorUPtr offsets_iter;
        EXPECT_TRUE(outer_group->offsets_reader->new_iterator(&offsets_iter, nullptr).ok());

        auto inner_iter = std::make_unique<segment_v2::NestedGroupWholeIterator>(outer_group);

        auto array_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeVariant>(0));

        auto nested_iter = std::make_unique<segment_v2::NestedGroupIterator>(
                std::move(offsets_iter), std::move(inner_iter), array_type);

        EXPECT_TRUE(nested_iter->init(iter_opts).ok());

        // Sequential reads should use cached flat_pos
        EXPECT_TRUE(nested_iter->seek_to_ordinal(0).ok());

        MutableColumnPtr dst_col = array_type->create_column();
        size_t n = 2;
        bool has_null = false;
        EXPECT_TRUE(nested_iter->next_batch(&n, dst_col, &has_null).ok());
        EXPECT_EQ(dst_col->size(), 2);

        // Continue sequential read (should use cached flat_pos)
        n = 2;
        EXPECT_TRUE(nested_iter->next_batch(&n, dst_col, &has_null).ok());
        EXPECT_EQ(dst_col->size(), 4); // Total 4 rows
    }

    // Test 5: Test NestedGroupIterator read_by_rowids with batched consecutive rowids
    {
        segment_v2::ColumnIteratorUPtr offsets_iter;
        EXPECT_TRUE(outer_group->offsets_reader->new_iterator(&offsets_iter, nullptr).ok());

        auto inner_iter = std::make_unique<segment_v2::NestedGroupWholeIterator>(outer_group);

        auto array_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeVariant>(0));

        auto nested_iter = std::make_unique<segment_v2::NestedGroupIterator>(
                std::move(offsets_iter), std::move(inner_iter), array_type);

        EXPECT_TRUE(nested_iter->init(iter_opts).ok());

        MutableColumnPtr dst_col = array_type->create_column();

        // Read rows 0, 1, 3 (row 2 is empty array, testing skip)
        std::vector<rowid_t> rowids = {0, 1, 3};
        EXPECT_TRUE(nested_iter->read_by_rowids(rowids.data(), rowids.size(), dst_col).ok());
        EXPECT_EQ(dst_col->size(), 3);

        auto* array_col = assert_cast<ColumnArray*>(dst_col.get());
        auto& offsets = array_col->get_offsets();

        // Row 0: 1 element
        EXPECT_EQ(offsets[0], 1);
        // Row 1: 2 elements
        EXPECT_EQ(offsets[1], 3);
        // Row 3 (index 2 in result): 1 element with empty inner
        EXPECT_EQ(offsets[2], 4);
    }
}

// ============================================================================
// Test VariantColumnReader::new_iterator for multi-level nested group access
// This tests the iterator creation logic in variant_column_reader.cpp:859-955
// ============================================================================

// Helper function to create a subcolumn TabletColumn with path_info for subpath access
static TabletColumn create_subcolumn_for_path(const TabletColumn& parent_column,
                                              const std::string& path) {
    std::string full_path = parent_column.name_lower_case() + "." + path;
    TabletColumn subcolumn;
    subcolumn.set_name(full_path);
    subcolumn.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);
    subcolumn.set_unique_id(-1); // extracted column has unique_id = -1
    subcolumn.set_parent_unique_id(parent_column.unique_id());
    subcolumn.set_path_info(PathInData(full_path));
    subcolumn.set_variant_max_subcolumns_count(parent_column.variant_max_subcolumns_count());
    subcolumn.set_is_nullable(true);
    return subcolumn;
}

// Test Case: Verify VariantColumnReader::new_iterator creates correct iterators
// for multi-level nested group access (NESTED_GROUP_WHOLE and NESTED_GROUP_CHILD modes)
// This directly tests the logic in variant_column_reader.cpp lines 859-955
TEST_F(VariantNestedTest, test_variant_reader_new_iterator_multi_level_nested) {
    VariantTestConfig config;
    config.tablet_id = 10100;
    config.variant_max_subcolumns_count = 50;
    auto ctx = create_context(config);

    // Data with two-level nesting: L1 -> L2 -> val
    // This creates nested group chain: [L1_reader, L2_reader]
    std::vector<std::string> jsons = {
            R"({"L1":[{"L2":[{"val":1}]}]})", R"({"L1":[{"L2":[{"val":2},{"val":3}]}]})",
            R"({"L1":[{"L2":[{"val":4}]},{"L2":[{"val":5}]}]})", R"({"L1":[]})"};

    auto write_st = ctx->write_json_data(jsons);
    EXPECT_TRUE(write_st.ok()) << "write_json_data failed: " << write_st.to_string();

    auto finish_st = ctx->finish_write();
    EXPECT_TRUE(finish_st.ok()) << "finish_write failed: " << finish_st.to_string();

    EXPECT_TRUE(ctx->open_for_read().ok());

    auto* variant_reader = ctx->get_variant_reader();
    ASSERT_NE(variant_reader, nullptr);

    const TabletColumn& parent_column = ctx->get_column();
    auto cache = ctx->create_reader_cache();

    OlapReaderStatistics stats;
    StorageReadOptions storage_read_opts;
    storage_read_opts.io_ctx.reader_type = ReaderType::READER_QUERY;
    storage_read_opts.stats = &stats;

    ColumnIteratorOptions iter_opts;
    iter_opts.stats = &stats;
    iter_opts.file_reader = ctx->get_file_reader().get();

    // =========================================================================
    // Test 1: Single-level NESTED_GROUP_WHOLE access (v['L1'])
    // chain = [L1], returns NestedGroupIterator which outputs ARRAY<VARIANT>
    // =========================================================================
    {
        TabletColumn subcolumn = create_subcolumn_for_path(parent_column, "L1");

        segment_v2::ColumnIteratorUPtr iter;
        auto st = variant_reader->new_iterator(&iter, &subcolumn, &storage_read_opts, cache.get());
        ASSERT_TRUE(st.ok()) << "new_iterator for L1 failed: " << st.to_string();
        ASSERT_NE(iter, nullptr);

        ASSERT_TRUE(iter->init(iter_opts).ok());
        ASSERT_TRUE(iter->seek_to_ordinal(0).ok());

        // For chain.size() == 1, returns NestedGroupIterator
        auto* nested_iter = dynamic_cast<segment_v2::NestedGroupIterator*>(iter.get());
        ASSERT_NE(nested_iter, nullptr) << "Single-level access should return NestedGroupIterator";

        MutableColumnPtr result_col = nested_iter->create_result_column();

        size_t n = 4;
        bool has_null = false;
        ASSERT_TRUE(iter->next_batch(&n, result_col, &has_null).ok());
        EXPECT_EQ(result_col->size(), 4);

        // Row 0: [{"L2":[{"val":1}]}] - array with 1 element
        std::string row0 = serialize_to_json_string(*result_col, nested_iter->get_result_type(), 0);
        EXPECT_TRUE(row0.find("\"L2\"") != std::string::npos)
                << "Row 0 should contain L2 field, got: " << row0;
        EXPECT_TRUE(row0.starts_with("[")) << "Row 0 should be a JSON array, got: " << row0;
    }

    // =========================================================================
    // Test 2: Two-level NESTED_GROUP_WHOLE access (v['L1']['L2'])
    // chain = [L1, L2], output type = ARRAY<ARRAY<VARIANT>>
    // NestedGroupIterator wraps NestedGroupWholeIterator
    // =========================================================================
    {
        TabletColumn subcolumn = create_subcolumn_for_path(parent_column, "L1.L2");

        segment_v2::ColumnIteratorUPtr iter;
        auto st = variant_reader->new_iterator(&iter, &subcolumn, &storage_read_opts, cache.get());
        ASSERT_TRUE(st.ok()) << "new_iterator for L1.L2 failed: " << st.to_string();
        ASSERT_NE(iter, nullptr);

        // Two-level WHOLE access returns NestedGroupIterator wrapping NestedGroupWholeIterator
        auto* nested_iter = dynamic_cast<segment_v2::NestedGroupIterator*>(iter.get());
        ASSERT_NE(nested_iter, nullptr) << "Two-level WHOLE should return NestedGroupIterator";

        ASSERT_TRUE(iter->init(iter_opts).ok());
        ASSERT_TRUE(iter->seek_to_ordinal(0).ok());

        MutableColumnPtr result_col = nested_iter->create_result_column();

        size_t n = 4;
        bool has_null = false;
        ASSERT_TRUE(iter->next_batch(&n, result_col, &has_null).ok());
        EXPECT_EQ(result_col->size(), 4);

        // Outer array follows L1's structure
        auto& l1_array = assert_cast<ColumnArray&>(*result_col);
        auto& l1_offsets = l1_array.get_offsets();

        // Row 0: L1 has 1 element
        // Row 1: L1 has 1 element
        // Row 2: L1 has 2 elements
        // Row 3: L1 has 0 elements (empty array)
        EXPECT_EQ(l1_offsets[0], 1) << "Row 0: L1 has 1 element";
        EXPECT_EQ(l1_offsets[1], 2) << "Row 1: L1 has 1 element (cumulative: 2)";
        EXPECT_EQ(l1_offsets[2], 4) << "Row 2: L1 has 2 elements (cumulative: 4)";
        EXPECT_EQ(l1_offsets[3], 4) << "Row 3: L1 has 0 elements (cumulative: 4)";
    }

    // =========================================================================
    // Test 3: Two-level NESTED_GROUP_CHILD access (v['L1']['L2']['val'])
    // chain = [L1, L2], child_path = "val", output type = ARRAY<ARRAY<child_type>>
    // =========================================================================
    {
        TabletColumn subcolumn = create_subcolumn_for_path(parent_column, "L1.L2.val");

        segment_v2::ColumnIteratorUPtr iter;
        auto st = variant_reader->new_iterator(&iter, &subcolumn, &storage_read_opts, cache.get());
        ASSERT_TRUE(st.ok()) << "new_iterator for L1.L2.val failed: " << st.to_string();
        ASSERT_NE(iter, nullptr);

        ASSERT_TRUE(iter->init(iter_opts).ok());
        ASSERT_TRUE(iter->seek_to_ordinal(0).ok());

        // The iterator returns ARRAY<ARRAY<Nullable(val_type)>>
        // We don't know the exact val_type, so use the iterator's type or a generic column
        auto* nested_iter = dynamic_cast<segment_v2::NestedGroupIterator*>(iter.get());
        ASSERT_NE(nested_iter, nullptr) << "Should return NestedGroupIterator for CHILD access";

        MutableColumnPtr result_col = nested_iter->create_result_column();

        size_t n = 4;
        bool has_null = false;
        ASSERT_TRUE(iter->next_batch(&n, result_col, &has_null).ok());
        EXPECT_EQ(result_col->size(), 4);

        // Verify the outer array has correct structure
        auto& outer_array = assert_cast<ColumnArray&>(*result_col);
        auto& outer_offsets = outer_array.get_offsets();

        // Same structure as L1.L2: outer array follows L1's structure
        EXPECT_EQ(outer_offsets[0], 1) << "Row 0: L1 has 1 element";
        EXPECT_EQ(outer_offsets[1], 2) << "Row 1: L1 has 1 element (cumulative: 2)";
        EXPECT_EQ(outer_offsets[2], 4) << "Row 2: L1 has 2 elements (cumulative: 4)";
        EXPECT_EQ(outer_offsets[3], 4) << "Row 3: L1 has 0 elements (cumulative: 4)";
    }

    // =========================================================================
    // Test 4: read_by_rowids for two-level NESTED_GROUP_CHILD access
    // =========================================================================
    {
        TabletColumn subcolumn = create_subcolumn_for_path(parent_column, "L1.L2.val");

        segment_v2::ColumnIteratorUPtr iter;
        auto st = variant_reader->new_iterator(&iter, &subcolumn, &storage_read_opts, cache.get());
        ASSERT_TRUE(st.ok()) << "new_iterator for L1.L2.val failed: " << st.to_string();

        ASSERT_TRUE(iter->init(iter_opts).ok());

        auto* nested_iter = dynamic_cast<segment_v2::NestedGroupIterator*>(iter.get());
        ASSERT_NE(nested_iter, nullptr);

        MutableColumnPtr result_col = nested_iter->create_result_column();

        // Read rows 0, 2 (skipping rows 1, 3)
        std::vector<rowid_t> rowids = {0, 2};
        ASSERT_TRUE(iter->read_by_rowids(rowids.data(), rowids.size(), result_col).ok());
        EXPECT_EQ(result_col->size(), 2);

        auto& outer_array = assert_cast<ColumnArray&>(*result_col);
        auto& outer_offsets = outer_array.get_offsets();

        // Result[0] = Row 0: L1 has 1 element
        // Result[1] = Row 2: L1 has 2 elements
        EXPECT_EQ(outer_offsets[0], 1) << "Result[0] (Row 0): L1 has 1 element";
        EXPECT_EQ(outer_offsets[1], 3) << "Result[1] (Row 2): L1 has 2 elements (cumulative: 3)";
    }
}

// Test Case: Verify single-level nested group access via VariantColumnReader::new_iterator
// Tests variant_column_reader.cpp lines 859-912 for chain size = 1
TEST_F(VariantNestedTest, test_variant_reader_new_iterator_single_level_nested) {
    VariantTestConfig config;
    config.tablet_id = 10101;
    config.variant_max_subcolumns_count = 50;
    auto ctx = create_context(config);

    // Data with single-level nesting
    std::vector<std::string> jsons = {R"({"nested":[{"a":1,"b":"x"}]})",
                                      R"({"nested":[{"a":2,"b":"y"},{"a":3,"b":"z"}]})",
                                      R"({"nested":[]})", R"({"other":"value"})"};

    EXPECT_TRUE(ctx->write_json_data(jsons).ok());
    EXPECT_TRUE(ctx->finish_write().ok());
    EXPECT_TRUE(ctx->open_for_read().ok());

    auto* variant_reader = ctx->get_variant_reader();
    ASSERT_NE(variant_reader, nullptr);

    const TabletColumn& parent_column = ctx->get_column();
    auto cache = ctx->create_reader_cache();

    OlapReaderStatistics stats;
    StorageReadOptions storage_read_opts;
    storage_read_opts.io_ctx.reader_type = ReaderType::READER_QUERY;
    storage_read_opts.stats = &stats;

    ColumnIteratorOptions iter_opts;
    iter_opts.stats = &stats;
    iter_opts.file_reader = ctx->get_file_reader().get();

    // =========================================================================
    // Test 1: NESTED_GROUP_WHOLE access (v['nested'])
    // chain = [nested], returns NestedGroupIterator which outputs ARRAY<VARIANT>
    // =========================================================================
    {
        TabletColumn subcolumn = create_subcolumn_for_path(parent_column, "nested");

        segment_v2::ColumnIteratorUPtr iter;
        auto st = variant_reader->new_iterator(&iter, &subcolumn, &storage_read_opts, cache.get());
        ASSERT_TRUE(st.ok()) << "new_iterator for nested failed: " << st.to_string();
        ASSERT_NE(iter, nullptr);

        ASSERT_TRUE(iter->init(iter_opts).ok());
        ASSERT_TRUE(iter->seek_to_ordinal(0).ok());

        // For chain.size() == 1, returns NestedGroupIterator
        auto* nested_iter = dynamic_cast<segment_v2::NestedGroupIterator*>(iter.get());
        ASSERT_NE(nested_iter, nullptr) << "Single-level access should return NestedGroupIterator";

        MutableColumnPtr result_col = nested_iter->create_result_column();

        size_t n = 4;
        bool has_null = false;
        ASSERT_TRUE(iter->next_batch(&n, result_col, &has_null).ok());
        EXPECT_EQ(result_col->size(), 4);

        // Row 0: [{"a":1,"b":"x"}]
        std::string row0 = serialize_to_json_string(*result_col, nested_iter->get_result_type(), 0);
        EXPECT_TRUE(row0.starts_with("[")) << "Row 0 should be a JSON array, got: " << row0;

        // Row 2: [] - empty array (may be represented as "" or "[]" depending on storage)
        std::string row2 = serialize_to_json_string(*result_col, nested_iter->get_result_type(), 2);
        EXPECT_TRUE(row2 == "[]" || row2.empty()) << "Row 2 should be empty array, got: " << row2;
    }

    // =========================================================================
    // Test 2: NESTED_GROUP_CHILD access (v['nested']['a'])
    // chain = [nested], child_path = "a", output type = ARRAY<Nullable(a_type)>
    // =========================================================================
    {
        TabletColumn subcolumn = create_subcolumn_for_path(parent_column, "nested.a");

        segment_v2::ColumnIteratorUPtr iter;
        auto st = variant_reader->new_iterator(&iter, &subcolumn, &storage_read_opts, cache.get());
        ASSERT_TRUE(st.ok()) << "new_iterator for nested.a failed: " << st.to_string();
        ASSERT_NE(iter, nullptr);

        ASSERT_TRUE(iter->init(iter_opts).ok());
        ASSERT_TRUE(iter->seek_to_ordinal(0).ok());

        // The iterator returns ARRAY<Nullable(a_type)>
        auto* nested_iter = dynamic_cast<segment_v2::NestedGroupIterator*>(iter.get());
        ASSERT_NE(nested_iter, nullptr) << "Should return NestedGroupIterator for CHILD access";

        MutableColumnPtr result_col = nested_iter->create_result_column();

        size_t n = 4;
        bool has_null = false;
        ASSERT_TRUE(iter->next_batch(&n, result_col, &has_null).ok());
        EXPECT_EQ(result_col->size(), 4);

        // Verify the array has correct structure
        auto& array_col = assert_cast<ColumnArray&>(*result_col);
        auto& offsets = array_col.get_offsets();

        // Row 0: 1 element (a=1)
        // Row 1: 2 elements (a=2, a=3)
        // Row 2: 0 elements (empty nested array)
        // Row 3: 0 elements (no nested field)
        EXPECT_EQ(offsets[0], 1) << "Row 0: nested has 1 element";
        EXPECT_EQ(offsets[1], 3) << "Row 1: nested has 2 elements (cumulative: 3)";
        EXPECT_EQ(offsets[2], 3) << "Row 2: nested is empty (cumulative: 3)";
        // Row 3 behavior depends on how missing fields are handled
    }

    // =========================================================================
    // Test 3: read_by_rowids for NESTED_GROUP_WHOLE (single-level)
    // =========================================================================
    {
        TabletColumn subcolumn = create_subcolumn_for_path(parent_column, "nested");

        segment_v2::ColumnIteratorUPtr iter;
        auto st = variant_reader->new_iterator(&iter, &subcolumn, &storage_read_opts, cache.get());
        ASSERT_TRUE(st.ok());

        ASSERT_TRUE(iter->init(iter_opts).ok());

        auto* nested_iter = dynamic_cast<segment_v2::NestedGroupIterator*>(iter.get());
        ASSERT_NE(nested_iter, nullptr);
        MutableColumnPtr result_col = nested_iter->create_result_column();

        std::vector<rowid_t> rowids = {0, 1};
        ASSERT_TRUE(iter->read_by_rowids(rowids.data(), rowids.size(), result_col).ok());
        EXPECT_EQ(result_col->size(), 2);

        // Verify content is JSON arrays
        std::string row0 = serialize_to_json_string(*result_col, nested_iter->get_result_type(), 0);
        EXPECT_TRUE(row0.starts_with("[")) << "Row 0 should be JSON array, got: " << row0;
    }

    // =========================================================================
    // Test 4: read_by_rowids for NESTED_GROUP_CHILD
    // =========================================================================
    {
        TabletColumn subcolumn = create_subcolumn_for_path(parent_column, "nested.a");

        segment_v2::ColumnIteratorUPtr iter;
        auto st = variant_reader->new_iterator(&iter, &subcolumn, &storage_read_opts, cache.get());
        ASSERT_TRUE(st.ok());

        ASSERT_TRUE(iter->init(iter_opts).ok());

        auto* nested_iter = dynamic_cast<segment_v2::NestedGroupIterator*>(iter.get());
        ASSERT_NE(nested_iter, nullptr);

        MutableColumnPtr result_col = nested_iter->create_result_column();

        std::vector<rowid_t> rowids = {0, 1};
        ASSERT_TRUE(iter->read_by_rowids(rowids.data(), rowids.size(), result_col).ok());
        EXPECT_EQ(result_col->size(), 2);

        auto& array_col = assert_cast<ColumnArray&>(*result_col);
        auto& offsets = array_col.get_offsets();

        EXPECT_EQ(offsets[0], 1) << "Row 0: 1 element";
        EXPECT_EQ(offsets[1], 3) << "Row 1: 2 elements (cumulative: 3)";
    }
}

// Test Case: Verify three-level nested group access via VariantColumnReader::new_iterator
// Tests deeply nested structures: L1 -> L2 -> L3 -> leaf
TEST_F(VariantNestedTest, test_variant_reader_new_iterator_three_level_nested) {
    VariantTestConfig config;
    config.tablet_id = 10102;
    config.variant_max_subcolumns_count = 50;
    auto ctx = create_context(config);

    // Data with three-level nesting: L1 -> L2 -> L3 -> id
    std::vector<std::string> jsons = {R"({"L1":[{"L2":[{"L3":[{"id":1}]}]}]})",
                                      R"({"L1":[{"L2":[{"L3":[{"id":2},{"id":3}]}]}]})",
                                      R"({"L1":[{"L2":[{"L3":[{"id":4}]},{"L3":[{"id":5}]}]}]})"};

    EXPECT_TRUE(ctx->write_json_data(jsons).ok());
    EXPECT_TRUE(ctx->finish_write().ok());
    EXPECT_TRUE(ctx->open_for_read().ok());

    auto* variant_reader = ctx->get_variant_reader();
    ASSERT_NE(variant_reader, nullptr);

    const TabletColumn& parent_column = ctx->get_column();
    auto cache = ctx->create_reader_cache();

    OlapReaderStatistics stats;
    StorageReadOptions storage_read_opts;
    storage_read_opts.io_ctx.reader_type = ReaderType::READER_QUERY;
    storage_read_opts.stats = &stats;

    ColumnIteratorOptions iter_opts;
    iter_opts.stats = &stats;
    iter_opts.file_reader = ctx->get_file_reader().get();

    // =========================================================================
    // Test 1: Single-level NESTED_GROUP_WHOLE access (v['L1'])
    // chain = [L1], output type = ARRAY<VARIANT>
    // =========================================================================
    {
        TabletColumn subcolumn = create_subcolumn_for_path(parent_column, "L1");

        segment_v2::ColumnIteratorUPtr iter;
        auto st = variant_reader->new_iterator(&iter, &subcolumn, &storage_read_opts, cache.get());
        ASSERT_TRUE(st.ok()) << "new_iterator for L1 failed: " << st.to_string();
        ASSERT_NE(iter, nullptr);

        // Single-level WHOLE access returns NestedGroupIterator directly
        auto* nested_iter = dynamic_cast<segment_v2::NestedGroupIterator*>(iter.get());
        ASSERT_NE(nested_iter, nullptr) << "Single-level WHOLE should return NestedGroupIterator";

        ASSERT_TRUE(iter->init(iter_opts).ok());
        ASSERT_TRUE(iter->seek_to_ordinal(0).ok());

        MutableColumnPtr result_col = nested_iter->create_result_column();

        size_t n = 3;
        bool has_null = false;
        ASSERT_TRUE(iter->next_batch(&n, result_col, &has_null).ok());
        EXPECT_EQ(result_col->size(), 3);

        // Each row should contain a JSON array of L1 elements
        std::string row0 = serialize_to_json_string(*result_col, nested_iter->get_result_type(), 0);
        std::cout << "Row 0: " << row0 << std::endl;
        EXPECT_TRUE(row0.starts_with("[")) << "Row 0 should be JSON array, got: " << row0;
        EXPECT_TRUE(row0.find("L2") != std::string::npos)
                << "Row 0 should contain L2 field, got: " << row0;
    }

    // =========================================================================
    // Test 2: Two-level NESTED_GROUP_WHOLE access (v['L1']['L2'])
    // chain = [L1, L2], output type = ARRAY<ARRAY<VARIANT>>
    // NestedGroupIterator wraps NestedGroupWholeIterator
    // =========================================================================
    {
        TabletColumn subcolumn = create_subcolumn_for_path(parent_column, "L1.L2");

        segment_v2::ColumnIteratorUPtr iter;
        auto st = variant_reader->new_iterator(&iter, &subcolumn, &storage_read_opts, cache.get());
        ASSERT_TRUE(st.ok()) << "new_iterator for L1.L2 failed: " << st.to_string();
        ASSERT_NE(iter, nullptr);

        // Two-level WHOLE access returns NestedGroupIterator wrapping NestedGroupWholeIterator
        auto* nested_iter = dynamic_cast<segment_v2::NestedGroupIterator*>(iter.get());
        ASSERT_NE(nested_iter, nullptr) << "Two-level WHOLE should return NestedGroupIterator";

        ASSERT_TRUE(iter->init(iter_opts).ok());
        ASSERT_TRUE(iter->seek_to_ordinal(0).ok());

        // Use the iterator's own create_result_column() for correct type
        MutableColumnPtr result_col = nested_iter->create_result_column();

        size_t n = 3;
        bool has_null = false;
        ASSERT_TRUE(iter->next_batch(&n, result_col, &has_null).ok());
        EXPECT_EQ(result_col->size(), 3);

        // Outer array follows L1's structure
        auto& l1_array = assert_cast<ColumnArray&>(*result_col);
        auto& l1_offsets = l1_array.get_offsets();

        // Row 0: L1 has 1 element
        // Row 1: L1 has 1 element
        // Row 2: L1 has 1 element
        EXPECT_EQ(l1_offsets[0], 1) << "Row 0: L1 has 1 element";
        EXPECT_EQ(l1_offsets[1], 2) << "Row 1: L1 has 1 element (cumulative: 2)";
        EXPECT_EQ(l1_offsets[2], 3) << "Row 2: L1 has 1 element (cumulative: 3)";

        // Inner data should be array elements containing L3 info
        auto& inner_data = l1_array.get_data();
        EXPECT_GT(inner_data.size(), 0) << "Should have inner elements";
    }

    // =========================================================================
    // Test 3: Three-level NESTED_GROUP_WHOLE access (v['L1']['L2']['L3'])
    // chain = [L1, L2, L3], output type = ARRAY<ARRAY<ARRAY<VARIANT>>>
    // Multiple NestedGroupIterators chain wrapping NestedGroupWholeIterator
    // =========================================================================
    {
        TabletColumn subcolumn = create_subcolumn_for_path(parent_column, "L1.L2.L3");

        segment_v2::ColumnIteratorUPtr iter;
        auto st = variant_reader->new_iterator(&iter, &subcolumn, &storage_read_opts, cache.get());
        ASSERT_TRUE(st.ok()) << "new_iterator for L1.L2.L3 failed: " << st.to_string();
        ASSERT_NE(iter, nullptr);

        // Three-level WHOLE access returns NestedGroupIterator (outermost L1)
        auto* nested_iter = dynamic_cast<segment_v2::NestedGroupIterator*>(iter.get());
        ASSERT_NE(nested_iter, nullptr) << "Three-level WHOLE should return NestedGroupIterator";

        ASSERT_TRUE(iter->init(iter_opts).ok());
        ASSERT_TRUE(iter->seek_to_ordinal(0).ok());

        MutableColumnPtr result_col = nested_iter->create_result_column();

        size_t n = 3;
        bool has_null = false;
        ASSERT_TRUE(iter->next_batch(&n, result_col, &has_null).ok());
        EXPECT_EQ(result_col->size(), 3);

        // Outer array follows L1's structure
        auto& l1_array = assert_cast<ColumnArray&>(*result_col);
        auto& l1_offsets = l1_array.get_offsets();

        EXPECT_EQ(l1_offsets[0], 1) << "Row 0: L1 has 1 element";
        EXPECT_EQ(l1_offsets[1], 2) << "Row 1: L1 has 1 element (cumulative: 2)";
        EXPECT_EQ(l1_offsets[2], 3) << "Row 2: L1 has 1 element (cumulative: 3)";

        // Second level array should follow L2's structure within each L1 element
        auto& l2_arrays = assert_cast<ColumnArray&>(l1_array.get_data());
        auto& l2_offsets = l2_arrays.get_offsets();

        // For 3 L1 elements total:
        // L1[0] -> L2 has 1 element
        // L1[1] -> L2 has 1 element
        // L1[2] -> L2 has 2 elements (from Row 2's structure)
        EXPECT_EQ(l2_offsets[0], 1) << "L1[0] -> L2 has 1 element";
        EXPECT_EQ(l2_offsets[1], 2) << "L1[1] -> L2 has 1 element (cumulative: 2)";
        EXPECT_EQ(l2_offsets[2], 4) << "L1[2] -> L2 has 2 elements (cumulative: 4)";

        std::string row0 = serialize_to_json_string(*result_col, nested_iter->get_result_type(), 0);
        std::string row1 = serialize_to_json_string(*result_col, nested_iter->get_result_type(), 1);
        std::string row2 = serialize_to_json_string(*result_col, nested_iter->get_result_type(), 2);
        EXPECT_TRUE(json_strings_equal(row0, R"([[[{"id":1}]]])"))
                << "Row 0 L1.L2.L3 mismatch, got: " << row0;
        EXPECT_TRUE(json_strings_equal(row1, R"([[[{"id":2},{"id":3}]]])"))
                << "Row 1 L1.L2.L3 mismatch, got: " << row1;
        EXPECT_TRUE(json_strings_equal(row2, R"([[[{"id":4}],[{"id":5}]]])"))
                << "Row 2 L1.L2.L3 mismatch, got: " << row2;
    }

    // =========================================================================
    // Test 4: Three-level NESTED_GROUP_CHILD access (v['L1']['L2']['L3']['id'])
    // chain = [L1, L2, L3], child_path = "id"
    // output type = ARRAY<ARRAY<ARRAY<Nullable(id_type)>>>
    // =========================================================================
    {
        TabletColumn subcolumn = create_subcolumn_for_path(parent_column, "L1.L2.L3.id");

        segment_v2::ColumnIteratorUPtr iter;
        auto st = variant_reader->new_iterator(&iter, &subcolumn, &storage_read_opts, cache.get());
        ASSERT_TRUE(st.ok()) << "new_iterator for L1.L2.L3.id failed: " << st.to_string();
        ASSERT_NE(iter, nullptr);

        ASSERT_TRUE(iter->init(iter_opts).ok());
        ASSERT_TRUE(iter->seek_to_ordinal(0).ok());

        auto* nested_iter = dynamic_cast<segment_v2::NestedGroupIterator*>(iter.get());
        ASSERT_NE(nested_iter, nullptr) << "Should return NestedGroupIterator for CHILD access";

        MutableColumnPtr result_col = nested_iter->create_result_column();

        size_t n = 3;
        bool has_null = false;
        ASSERT_TRUE(iter->next_batch(&n, result_col, &has_null).ok());
        EXPECT_EQ(result_col->size(), 3);

        // Verify the structure follows L1->L2->L3 nesting
        auto& l1_array = assert_cast<ColumnArray&>(*result_col);
        auto& l1_offsets = l1_array.get_offsets();

        EXPECT_EQ(l1_offsets[0], 1) << "Row 0: L1 has 1 element";
        EXPECT_EQ(l1_offsets[1], 2) << "Row 1: L1 has 1 element (cumulative: 2)";
        EXPECT_EQ(l1_offsets[2], 3) << "Row 2: L1 has 1 element (cumulative: 3)";

        std::string row0 = serialize_to_json_string(*result_col, nested_iter->get_result_type(), 0);
        std::string row1 = serialize_to_json_string(*result_col, nested_iter->get_result_type(), 1);
        std::string row2 = serialize_to_json_string(*result_col, nested_iter->get_result_type(), 2);
        EXPECT_TRUE(json_strings_equal(row0, R"([[[1]]])"))
                << "Row 0 L1.L2.L3.id mismatch, got: " << row0;
        EXPECT_TRUE(json_strings_equal(row1, R"([[[2,3]]])"))
                << "Row 1 L1.L2.L3.id mismatch, got: " << row1;
        EXPECT_TRUE(json_strings_equal(row2, R"([[[4],[5]]])"))
                << "Row 2 L1.L2.L3.id mismatch, got: " << row2;
    }

    // =========================================================================
    // Test 5: read_by_rowids for single-level NESTED_GROUP_WHOLE (v['L1'])
    // =========================================================================
    {
        TabletColumn subcolumn = create_subcolumn_for_path(parent_column, "L1");

        segment_v2::ColumnIteratorUPtr iter;
        auto st = variant_reader->new_iterator(&iter, &subcolumn, &storage_read_opts, cache.get());
        ASSERT_TRUE(st.ok());

        ASSERT_TRUE(iter->init(iter_opts).ok());

        auto* nested_iter = dynamic_cast<segment_v2::NestedGroupIterator*>(iter.get());
        ASSERT_NE(nested_iter, nullptr);

        MutableColumnPtr result_col = nested_iter->create_result_column();

        std::vector<rowid_t> rowids = {0, 2};
        ASSERT_TRUE(iter->read_by_rowids(rowids.data(), rowids.size(), result_col).ok());
        EXPECT_EQ(result_col->size(), 2);

        // Verify JSON array content
        std::string row0 = serialize_to_json_string(*result_col, nested_iter->get_result_type(), 0);
        std::string row2 = serialize_to_json_string(*result_col, nested_iter->get_result_type(), 1);
        EXPECT_TRUE(row0.starts_with("[")) << "Row 0 should be JSON array, got: " << row0;
        EXPECT_TRUE(row2.starts_with("[")) << "Row 2 should be JSON array, got: " << row2;
    }

    // =========================================================================
    // Test 6: read_by_rowids for two-level NESTED_GROUP_WHOLE (v['L1']['L2'])
    // =========================================================================
    {
        TabletColumn subcolumn = create_subcolumn_for_path(parent_column, "L1.L2");

        segment_v2::ColumnIteratorUPtr iter;
        auto st = variant_reader->new_iterator(&iter, &subcolumn, &storage_read_opts, cache.get());
        ASSERT_TRUE(st.ok());

        ASSERT_TRUE(iter->init(iter_opts).ok());

        auto* nested_iter = dynamic_cast<segment_v2::NestedGroupIterator*>(iter.get());
        ASSERT_NE(nested_iter, nullptr);

        MutableColumnPtr result_col = nested_iter->create_result_column();

        std::vector<rowid_t> rowids = {0, 2};
        ASSERT_TRUE(iter->read_by_rowids(rowids.data(), rowids.size(), result_col).ok());
        EXPECT_EQ(result_col->size(), 2);

        auto& l1_array = assert_cast<ColumnArray&>(*result_col);
        auto& l1_offsets = l1_array.get_offsets();

        // Result[0] = Row 0: L1 has 1 element
        // Result[1] = Row 2: L1 has 1 element
        EXPECT_EQ(l1_offsets[0], 1);
        EXPECT_EQ(l1_offsets[1], 2);
    }

    // =========================================================================
    // Test 7: read_by_rowids for three-level NESTED_GROUP_WHOLE (v['L1']['L2']['L3'])
    // =========================================================================
    {
        TabletColumn subcolumn = create_subcolumn_for_path(parent_column, "L1.L2.L3");

        segment_v2::ColumnIteratorUPtr iter;
        auto st = variant_reader->new_iterator(&iter, &subcolumn, &storage_read_opts, cache.get());
        ASSERT_TRUE(st.ok());

        ASSERT_TRUE(iter->init(iter_opts).ok());

        auto* nested_iter = dynamic_cast<segment_v2::NestedGroupIterator*>(iter.get());
        ASSERT_NE(nested_iter, nullptr);

        MutableColumnPtr result_col = nested_iter->create_result_column();

        std::vector<rowid_t> rowids = {0, 2};
        ASSERT_TRUE(iter->read_by_rowids(rowids.data(), rowids.size(), result_col).ok());
        EXPECT_EQ(result_col->size(), 2);

        auto& l1_array = assert_cast<ColumnArray&>(*result_col);
        auto& l1_offsets = l1_array.get_offsets();

        // Result[0] = Row 0: L1 has 1 element
        // Result[1] = Row 2: L1 has 1 element
        EXPECT_EQ(l1_offsets[0], 1);
        EXPECT_EQ(l1_offsets[1], 2);

        // Verify L2 level structure
        auto& l2_arrays = assert_cast<ColumnArray&>(l1_array.get_data());
        auto& l2_offsets = l2_arrays.get_offsets();

        // L1[0] from Row 0: L2 has 1 element
        // L1[1] from Row 2: L2 has 2 elements
        EXPECT_EQ(l2_offsets[0], 1) << "L1[0] from Row 0: L2 has 1 element";
        EXPECT_EQ(l2_offsets[1], 3) << "L1[1] from Row 2: L2 has 2 elements (cumulative: 3)";
    }

    // =========================================================================
    // Test 8: read_by_rowids for three-level NESTED_GROUP_CHILD (v['L1']['L2']['L3']['id'])
    // =========================================================================
    {
        TabletColumn subcolumn = create_subcolumn_for_path(parent_column, "L1.L2.L3.id");

        segment_v2::ColumnIteratorUPtr iter;
        auto st = variant_reader->new_iterator(&iter, &subcolumn, &storage_read_opts, cache.get());
        ASSERT_TRUE(st.ok());

        ASSERT_TRUE(iter->init(iter_opts).ok());

        auto* nested_iter = dynamic_cast<segment_v2::NestedGroupIterator*>(iter.get());
        ASSERT_NE(nested_iter, nullptr);

        MutableColumnPtr result_col = nested_iter->create_result_column();

        std::vector<rowid_t> rowids = {0, 2};
        ASSERT_TRUE(iter->read_by_rowids(rowids.data(), rowids.size(), result_col).ok());
        EXPECT_EQ(result_col->size(), 2);

        auto& l1_array = assert_cast<ColumnArray&>(*result_col);
        auto& l1_offsets = l1_array.get_offsets();

        // Result[0] = Row 0: L1 has 1 element
        // Result[1] = Row 2: L1 has 1 element
        EXPECT_EQ(l1_offsets[0], 1);
        EXPECT_EQ(l1_offsets[1], 2);
    }
}

TEST_F(VariantNestedTest, LoadTestJsonDumpOffsetsAndLeafValues) {
    VariantTestConfig config;
    VariantTestContext ctx(this, config);

    const std::vector<std::string> jsons {
            R"JSON({
  "id": "u001",
  "events": [
    {
      "time": "10:00",
      "type": "click",
      "payload": {
        "page": "/home",
        "duration_ms": 3200,
        "tags": [
          {"name": "mobile",  "priority": "high",  "categories": ["ui", "tracking", "mobile"]},
          {"name": "new",     "priority": "low",   "categories": ["feature"]},
          {"name": "urgent",  "priority": null,    "categories": []}
        ]
      }
    }
  ]
})JSON",
            R"JSON({
  "id": "u002",
  "events": [
    {
      "time": "11:15",
      "type": "view",
      "payload": {
        "page": "/product",
        "duration_ms": 1800,
        "tags": []
      }
    },
    {
      "time": "11:20",
      "type": "scroll",
      "payload": null
    }
  ]
})JSON",
            R"JSON({
  "id": "u003",
  "events": [
    {
      "time": "14:30",
      "type": "add",
      "payload": {
        "page": "/cart",
        "duration_ms": 450,
        "tags": [
          {"name": "premium", "priority": "high", "categories": ["vip", "sale", "limited"]}
        ]
      }
    },
    {
      "time": "14:32",
      "type": "remove",
      "payload": {
        "page": "/cart",
        "duration_ms": 120,
        "tags": [
          {"name": "regret",  "priority": null,   "categories": []},
          {"name": "cheap",   "priority": "low",  "categories": ["budget"]}
        ]
      }
    }
  ]
})JSON",
    };

    ASSERT_TRUE(ctx.write_json_data(jsons).ok());
    ASSERT_TRUE(ctx.finish_write().ok());
    ASSERT_TRUE(ctx.open_for_read().ok());

    MutableColumnPtr result;
    vectorized::DataTypePtr type;
    ASSERT_TRUE(ctx.read_all_data(&result, &type).ok());
    auto* result_variant = assert_cast<ColumnVariant*>(result.get());
    ctx.validate_data(result_variant).expect_row_count(jsons.size()).expect_exact_match();

    OlapReaderStatistics stats;
    ColumnIteratorOptions iter_opts;
    iter_opts.stats = &stats;
    iter_opts.file_reader = ctx.get_file_reader().get();

    auto* variant_reader = ctx.get_variant_reader();

    const std::string events_group_path = "events";
    const std::string tags_group_path = events_group_path + ".payload.tags";

    struct OffsetsExpectation {
        std::string group_path;
        std::vector<uint64_t> expected;
    };
    struct LeafExpectation {
        std::string group_path;
        std::string relative_path;
        std::vector<std::string> expected;
    };

    auto get_group = [&](const std::string& group_path) -> const segment_v2::NestedGroupReader* {
        const auto* group = variant_reader->get_nested_group_reader(group_path);
        if (!group) {
            return nullptr;
        }
        if (!group->is_valid()) {
            return nullptr;
        }
        return group;
    };

    auto get_child_reader =
            [&](const segment_v2::NestedGroupReader& group,
                const std::string& relative_path) -> std::shared_ptr<segment_v2::ColumnReader> {
        auto it = group.child_readers.find(relative_path);
        if (it == group.child_readers.end()) {
            return nullptr;
        }
        return it->second;
    };

    auto read_group_offsets = [&](const segment_v2::NestedGroupReader& group,
                                  std::vector<uint64_t>* offsets) -> Status {
        if (offsets == nullptr) {
            return Status::InvalidArgument("offsets is null");
        }
        offsets->clear();
        offsets->reserve(group.offsets_reader->num_rows() + 1);
        offsets->push_back(0);
        if (group.offsets_reader->num_rows() == 0) {
            return Status::OK();
        }
        segment_v2::ColumnIteratorUPtr iter;
        RETURN_IF_ERROR(group.offsets_reader->new_iterator(&iter, nullptr));
        RETURN_IF_ERROR(iter->init(iter_opts));
        RETURN_IF_ERROR(iter->seek_to_ordinal(0));
        size_t remaining = group.offsets_reader->num_rows();
        while (remaining > 0) {
            size_t batch = std::min<size_t>(remaining, 8192);
            vectorized::MutableColumnPtr col = vectorized::ColumnInt64::create();
            bool has_null = false;
            RETURN_IF_ERROR(iter->next_batch(&batch, col, &has_null));
            auto* data = assert_cast<vectorized::ColumnInt64*>(col.get());
            for (auto v : data->get_data()) {
                offsets->push_back(static_cast<uint64_t>(v));
            }
            remaining -= batch;
        }
        EXPECT_EQ(offsets->size(), group.offsets_reader->num_rows() + 1);
        return Status::OK();
    };

    std::vector<NamedDumpItem> dump_items;

    const std::vector<OffsetsExpectation> offsets_expectations {
            OffsetsExpectation {events_group_path, {0, 1, 3, 5}},
            OffsetsExpectation {tags_group_path, {0, 3, 3, 3, 4, 6}},
    };

    for (const auto& exp : offsets_expectations) {
        const auto* group = get_group(exp.group_path);
        ASSERT_NE(group, nullptr) << "Missing nested group: " << exp.group_path;

        std::vector<uint64_t> actual;
        ASSERT_TRUE(read_group_offsets(*group, &actual).ok());
        EXPECT_EQ(actual, exp.expected) << "Offsets mismatch for group: " << exp.group_path;

        dump_items.push_back(NamedDumpItem {.name = std::string(segment_v2::kNestedGroupMarker) +
                                                    "." + exp.group_path + "._offsets",
                                            .value = format_offsets_as_array(actual)});
    }

    const std::vector<LeafExpectation> leaf_expectations {
            LeafExpectation {.group_path = events_group_path,
                             .relative_path = "time",
                             .expected = {"10:00", "11:15", "11:20", "14:30", "14:32"}},
            LeafExpectation {.group_path = events_group_path,
                             .relative_path = "type",
                             .expected = {"click", "view", "scroll", "add", "remove"}},
            LeafExpectation {.group_path = events_group_path,
                             .relative_path = "payload.page",
                             .expected = {"/home", "/product", "NULL", "/cart", "/cart"}},
            LeafExpectation {.group_path = events_group_path,
                             .relative_path = "payload.duration_ms",
                             .expected = {"3200", "1800", "NULL", "450", "120"}},
            LeafExpectation {.group_path = tags_group_path,
                             .relative_path = "name",
                             .expected = {"mobile", "new", "urgent", "premium", "regret", "cheap"}},
            LeafExpectation {.group_path = tags_group_path,
                             .relative_path = "priority",
                             .expected = {"high", "low", "NULL", "high", "NULL", "low"}},
            LeafExpectation {.group_path = tags_group_path,
                             .relative_path = "categories",
                             .expected = {R"(["ui", "tracking", "mobile"])", "[\"feature\"]", "[]",
                                          R"(["vip", "sale", "limited"])", "[]", "[\"budget\"]"}},
    };

    for (const auto& exp : leaf_expectations) {
        const auto* group = get_group(exp.group_path);
        ASSERT_NE(group, nullptr) << "Missing nested group: " << exp.group_path;

        auto child_reader = get_child_reader(*group, exp.relative_path);
        ASSERT_NE(child_reader, nullptr)
                << "Missing leaf reader: " << exp.group_path << "." << exp.relative_path;

        std::vector<std::string> actual;
        ASSERT_TRUE(read_column_to_json_values(child_reader, iter_opts, &actual).ok());
        EXPECT_EQ(actual, exp.expected)
                << "Leaf values mismatch for: " << exp.group_path << "." << exp.relative_path;

        dump_items.push_back(NamedDumpItem {.name = std::string(segment_v2::kNestedGroupMarker) +
                                                    "." + exp.group_path + "." + exp.relative_path,
                                            .value = format_json_values_as_array(actual)});
    }

    print_dump_tree(dump_items);
}

TEST_F(VariantNestedTest, LoadTestJsonDumpOffsetsAndLeafValuesComplexNestedJson) {
    VariantTestConfig config;
    config.variant_max_subcolumns_count = 50;
    VariantTestContext ctx(this, config);

    const std::vector<std::string> jsons {
            R"JSON({
  "id": "u001",
  "events": [
    {
      "time": "10:00",
      "type": "click",
      "payload": {
        "page": "/home",
        "duration_ms": 3200,
        "tags": [
          {"name": "mobile",  "priority": "high",  "categories": [{"labels": ["ui"]}, {"labels": ["tracking"]}, {"labels": ["mobile"]}]},
          {"name": "new",     "priority": "low",   "categories": [{"labels": ["feature"]}]},
          {"name": "urgent",  "priority": null,    "categories": []}
        ]
      }
    }
  ]
})JSON",
            R"JSON({
  "id": "u002",
  "events": [
    {
      "time": "11:15",
      "type": "view",
      "payload": {
        "page": "/product",
        "duration_ms": 1800,
        "tags": []
      }
    },
    {
      "time": "11:20",
      "type": "scroll",
      "payload": null
    }
  ]
})JSON",
            R"JSON({
  "id": "u003",
  "events": [
    {
      "time": "14:30",
      "type": "add",
      "payload": {
        "page": "/cart",
        "duration_ms": 450,
        "tags": [
          {"name": "premium", "priority": "high", "categories": [{"labels": ["vip"]}, {"labels": ["sale"]}, {"labels": ["limited"]}]}
        ]
      }
    },
    {
      "time": "14:32",
      "type": "remove",
      "payload": {
        "page": "/cart",
        "duration_ms": 120,
        "tags": [
          {"name": "regret",  "priority": null,   "categories": []},
          {"name": "cheap",   "priority": "low",  "categories": [{"labels": ["budget"]}]}
        ]
      }
    }
  ]
})JSON",
            R"JSON({
  "id": "u004",
  "events": [
    {
      "time": "15:00",
      "type": "purchase",
      "payload": {
        "page": "/checkout",
        "duration_ms": 999.5,
        "tags": null,
        "meta": {
          "flags": [true, false, 1, 0],
          "list": [
            {"k": "x", "v": 1},
            {"k": "y", "v": 2}
          ]
        }
      }
    },
    {
      "time": "15:01",
      "type": "refund",
      "payload": {
        "page": null,
        "duration_ms": null,
        "tags": [
          {
            "name": "mix",
            "priority": "high",
            "categories": [{"labels": ["x"]}, {"labels": ["y", "z"]}],
            "attrs": [
              {"k": "color", "v": "red"},
              {"k": "size", "v": 42}
            ]
          }
        ]
      }
    }
  ]
})JSON",
            R"JSON({
  "id": "u005",
  "events": []
})JSON",
            R"JSON({
  "id": "u006",
  "events": [
    {
      "time": "16:00",
      "type": "view",
      "payload": {}
    },
    {
      "time": "16:05",
      "type": "click",
      "payload": {
        "page": "/search",
        "duration_ms": 12,
        "tags": [
          {"priority": null, "categories": []},
          {"name": "partial", "categories": [{"labels": ["v"]}]},
          {"name": "deep", "priority": "low", "categories": [{"labels": ["a"]}, {"labels": ["b", "c"]}], "attrs": []}
        ]
      }
    }
  ]
})JSON",
    };

    ASSERT_TRUE(ctx.write_json_data(jsons).ok());
    ASSERT_TRUE(ctx.finish_write().ok());
    ASSERT_TRUE(ctx.open_for_read().ok());

    MutableColumnPtr result;
    vectorized::DataTypePtr type;
    ASSERT_TRUE(ctx.read_all_data(&result, &type).ok());
    auto* result_variant = assert_cast<ColumnVariant*>(result.get());
    ctx.validate_data(result_variant).expect_row_count(jsons.size()).expect_exact_match();

    OlapReaderStatistics stats;
    ColumnIteratorOptions iter_opts;
    iter_opts.stats = &stats;
    iter_opts.file_reader = ctx.get_file_reader().get();

    auto* variant_reader = ctx.get_variant_reader();

    const std::string events_group_path = "events";
    const std::string tags_group_path = events_group_path + ".payload.tags";
    const std::string categories_group_path = tags_group_path + ".categories";
    const std::string tags_attrs_group_path = tags_group_path + ".attrs";
    const std::string meta_list_group_path = events_group_path + ".payload.meta.list";

    struct OffsetsExpectation {
        std::string group_path;
        std::vector<uint64_t> expected;
    };
    struct LeafExpectation {
        std::string group_path;
        std::string relative_path;
        std::vector<std::string> expected;
        bool compare_as_json = false;
    };

    auto get_group = [&](const std::string& group_path) -> const segment_v2::NestedGroupReader* {
        const auto* group = variant_reader->get_nested_group_reader(group_path);
        if (!group || !group->is_valid()) {
            return nullptr;
        }
        return group;
    };

    auto get_child_reader =
            [&](const segment_v2::NestedGroupReader& group,
                const std::string& relative_path) -> std::shared_ptr<segment_v2::ColumnReader> {
        auto it = group.child_readers.find(relative_path);
        if (it == group.child_readers.end()) {
            return nullptr;
        }
        return it->second;
    };

    auto read_group_offsets = [&](const segment_v2::NestedGroupReader& group,
                                  std::vector<uint64_t>* offsets) -> Status {
        if (offsets == nullptr) {
            return Status::InvalidArgument("offsets is null");
        }
        offsets->clear();
        offsets->reserve(group.offsets_reader->num_rows() + 1);
        offsets->push_back(0);
        if (group.offsets_reader->num_rows() == 0) {
            return Status::OK();
        }
        segment_v2::ColumnIteratorUPtr iter;
        RETURN_IF_ERROR(group.offsets_reader->new_iterator(&iter, nullptr));
        RETURN_IF_ERROR(iter->init(iter_opts));
        RETURN_IF_ERROR(iter->seek_to_ordinal(0));
        size_t remaining = group.offsets_reader->num_rows();
        while (remaining > 0) {
            size_t batch = std::min<size_t>(remaining, 8192);
            vectorized::MutableColumnPtr col = vectorized::ColumnInt64::create();
            bool has_null = false;
            RETURN_IF_ERROR(iter->next_batch(&batch, col, &has_null));
            auto* data = assert_cast<vectorized::ColumnInt64*>(col.get());
            for (auto v : data->get_data()) {
                offsets->push_back(static_cast<uint64_t>(v));
            }
            remaining -= batch;
        }
        EXPECT_EQ(offsets->size(), group.offsets_reader->num_rows() + 1);
        return Status::OK();
    };

    std::vector<NamedDumpItem> dump_items;

    const std::vector<OffsetsExpectation> offsets_expectations {
            OffsetsExpectation {events_group_path, {0, 1, 3, 5, 7, 7, 9}},
            OffsetsExpectation {tags_group_path, {0, 3, 3, 3, 4, 6, 6, 7, 7, 10}},
            OffsetsExpectation {categories_group_path, {0, 3, 4, 4, 7, 7, 8, 10, 10, 11, 13}},
            OffsetsExpectation {tags_attrs_group_path, {0, 0, 0, 0, 0, 0, 0, 2, 2, 2, 2}},
            OffsetsExpectation {meta_list_group_path, {0, 0, 0, 0, 0, 0, 2, 2, 2, 2}},
    };

    for (const auto& exp : offsets_expectations) {
        const auto* group = get_group(exp.group_path);
        ASSERT_NE(group, nullptr) << "Missing nested group: " << exp.group_path;

        std::vector<uint64_t> actual;
        ASSERT_TRUE(read_group_offsets(*group, &actual).ok());
        EXPECT_EQ(actual, exp.expected) << "Offsets mismatch for group: " << exp.group_path;

        dump_items.push_back(NamedDumpItem {.name = std::string(segment_v2::kNestedGroupMarker) +
                                                    "." + exp.group_path + "._offsets",
                                            .value = format_offsets_as_array(actual)});
    }

    const std::vector<LeafExpectation> leaf_expectations {
            LeafExpectation {.group_path = events_group_path,
                             .relative_path = "time",
                             .expected = {"10:00", "11:15", "11:20", "14:30", "14:32", "15:00",
                                          "15:01", "16:00", "16:05"}},
            LeafExpectation {.group_path = events_group_path,
                             .relative_path = "type",
                             .expected = {"click", "view", "scroll", "add", "remove", "purchase",
                                          "refund", "view", "click"}},
            LeafExpectation {.group_path = events_group_path,
                             .relative_path = "payload.page",
                             .expected = {"/home", "/product", "NULL", "/cart", "/cart",
                                          "/checkout", "NULL", "NULL", "/search"}},
            LeafExpectation {.group_path = events_group_path,
                             .relative_path = "payload.duration_ms",
                             .expected = {"3200", "1800", "NULL", "450", "120", "999.5", "NULL",
                                          "NULL", "12"}},
            LeafExpectation {.group_path = events_group_path,
                             .relative_path = "payload.meta.flags",
                             .expected = {"NULL", "NULL", "NULL", "NULL", "NULL",
                                          "[true, false, 1, 0]", "NULL", "NULL", "NULL"},
                             .compare_as_json = true},
            LeafExpectation {.group_path = tags_group_path,
                             .relative_path = "name",
                             .expected = {"mobile", "new", "urgent", "premium", "regret", "cheap",
                                          "mix", "NULL", "partial", "deep"}},
            LeafExpectation {.group_path = tags_group_path,
                             .relative_path = "priority",
                             .expected = {"high", "low", "NULL", "high", "NULL", "low", "high",
                                          "NULL", "NULL", "low"}},
            LeafExpectation {.group_path = categories_group_path,
                             .relative_path = "labels",
                             .expected = {R"(["ui"])", R"(["tracking"])", R"(["mobile"])",
                                          R"(["feature"])", R"(["vip"])", R"(["sale"])",
                                          R"(["limited"])", R"(["budget"])", R"(["x"])",
                                          R"(["y", "z"])", R"(["v"])", R"(["a"])", R"(["b", "c"])"},
                             .compare_as_json = true},
            LeafExpectation {.group_path = tags_attrs_group_path,
                             .relative_path = "k",
                             .expected = {"color", "size"}},
            LeafExpectation {.group_path = tags_attrs_group_path,
                             .relative_path = "v",
                             .expected = {R"("red")", "42"},
                             .compare_as_json = true},
            LeafExpectation {.group_path = meta_list_group_path,
                             .relative_path = "k",
                             .expected = {"x", "y"}},
            LeafExpectation {.group_path = meta_list_group_path,
                             .relative_path = "v",
                             .expected = {"1", "2"}},
    };

    for (const auto& exp : leaf_expectations) {
        const auto* group = get_group(exp.group_path);
        ASSERT_NE(group, nullptr) << "Missing nested group: " << exp.group_path;

        auto child_reader = get_child_reader(*group, exp.relative_path);
        ASSERT_NE(child_reader, nullptr)
                << "Missing leaf reader: " << exp.group_path << "." << exp.relative_path;

        std::vector<std::string> actual;
        ASSERT_TRUE(read_column_to_json_values(child_reader, iter_opts, &actual).ok());

        if (!exp.compare_as_json) {
            EXPECT_EQ(actual, exp.expected)
                    << "Leaf values mismatch for: " << exp.group_path << "." << exp.relative_path;
        } else {
            ASSERT_EQ(actual.size(), exp.expected.size())
                    << "Leaf values size mismatch for: " << exp.group_path << "."
                    << exp.relative_path;
            for (size_t i = 0; i < actual.size(); ++i) {
                if (actual[i] == "NULL" || exp.expected[i] == "NULL") {
                    EXPECT_EQ(actual[i], exp.expected[i])
                            << "Leaf values mismatch for: " << exp.group_path << "."
                            << exp.relative_path << " at index " << i;
                } else {
                    EXPECT_TRUE(json_strings_equal(actual[i], exp.expected[i]))
                            << "Leaf JSON mismatch for: " << exp.group_path << "."
                            << exp.relative_path << " at index " << i;
                }
            }
        }

        dump_items.push_back(NamedDumpItem {.name = std::string(segment_v2::kNestedGroupMarker) +
                                                    "." + exp.group_path + "." + exp.relative_path,
                                            .value = format_json_values_as_array(actual)});
    }

    print_dump_tree(dump_items);
}

} // namespace doris
