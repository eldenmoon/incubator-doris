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

#include <gtest/gtest.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <map>
#include <memory>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/check.h"
#include "core/block/block.h"
#include "core/column/column_map.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_jsonb.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type_serde/data_type_serde.h"
#include "core/value/variant/variant_batch_builder.h"
#include "exec/common/variant_util.h"
#include "exprs/function/parse/variant_string_parse.h"
#include "io/fs/local_file_system.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/segment/column_reader.h"
#include "storage/segment/segment.h"
#include "storage/segment/segment_writer.h"
#include "storage/segment/variant/hierarchical_data_iterator.h"
#include "storage/segment/variant/variant_column_reader.h"
#include "storage/segment/vertical_segment_writer.h"
#include "storage/tablet/tablet_schema.h"
#include "testutil/variant_util.h"

namespace doris {
namespace {

constexpr std::string_view kSamplesPath = "docs/design/variant_v2/baseline/segments/samples";
constexpr std::string_view kLegacySamplesPath =
        "docs/design/variant_v2/baseline/segments/legacy_samples";
constexpr std::string_view kSourceHeader = "case_id\trow\tpath\tsource_type\tvalue\n";
constexpr std::string_view kManifestHeader =
        "kind\tcase_id\tordinal\trow\tpath\tstorage_type\tfield_type\traw\tobservation\n";
struct CaseConfig {
    std::string_view id;
    int32_t max_subcolumns;
    int32_t sparse_buckets = 0;
    bool doc_mode = false;
    int64_t doc_threshold_rows = 0;
    int32_t doc_buckets = 0;
};
enum class CaseKind { ORDINARY, DOC, BUCKETED };
constexpr std::array kCaseKinds {CaseKind::ORDINARY, CaseKind::DOC, CaseKind::BUCKETED};
constexpr std::array kCaseConfigs {
        CaseConfig {.id = "ordinary_sparse", .max_subcolumns = 2},
        CaseConfig {.id = "doc",
                    .max_subcolumns = 3,
                    .doc_mode = true,
                    .doc_threshold_rows = 9,
                    .doc_buckets = 1},
        CaseConfig {.id = "bucketed_sparse", .max_subcolumns = 1, .sparse_buckets = 3}};
class VariantSegmentSnapshotTest : public testing::Test {
protected:
    static testing::AssertionResult status_ok(Status status) {
        if (status.ok()) {
            return testing::AssertionSuccess();
        }
        return testing::AssertionFailure() << status.to_string();
    }
    void SetUp() override {
        fs = io::global_local_filesystem();
        dir = std::filesystem::temp_directory_path() /
              ("variant_segment_snapshot_" + std::to_string(::getpid()));
        ASSERT_TRUE(status_ok(fs->delete_directory(dir.string())));
        ASSERT_TRUE(status_ok(fs->create_directory(dir.string())));
    }
    void TearDown() override { EXPECT_TRUE(status_ok(fs->delete_directory(dir.string()))); }
    Status run_case(CaseKind kind, const std::string& path, std::string* source,
                    std::string* manifest, uint64_t* file_size);
    Status generate_samples(const std::filesystem::path& output);
    io::FileSystemSPtr fs;
    std::filesystem::path dir;
};
std::filesystem::path repo_root() {
    const char* root = std::getenv("ROOT");
    if (root == nullptr) {
        throw std::runtime_error("ROOT is not set");
    }
    return std::filesystem::canonical(root);
}
TabletSchemaSPtr make_schema(const CaseConfig& config) {
    TabletSchemaPB pb;
    pb.set_keys_type(KeysType::DUP_KEYS);
    pb.set_num_short_key_columns(1);
    auto* key = pb.add_column();
    key->set_unique_id(1);
    key->set_name("k");
    key->set_type("INT");
    key->set_is_key(true);
    key->set_is_nullable(false);
    key->set_length(sizeof(Int32));
    key->set_index_length(sizeof(Int32));
    auto* variant = pb.add_column();
    variant->set_unique_id(2);
    variant->set_name("v");
    variant->set_type("VARIANT");
    variant->set_is_key(false);
    variant->set_is_nullable(false);
    variant->set_variant_max_subcolumns_count(config.max_subcolumns);
    variant->set_variant_max_sparse_column_statistics_size(100);
    if (config.sparse_buckets > 0) {
        variant->set_variant_sparse_hash_shard_count(config.sparse_buckets);
    }
    if (config.doc_mode) {
        variant->set_variant_enable_doc_mode(true);
        variant->set_variant_doc_materialization_min_rows(config.doc_threshold_rows);
        if (config.doc_buckets > 0) {
            variant->set_variant_doc_hash_shard_count(config.doc_buckets);
        }
    }
    auto schema = std::make_shared<TabletSchema>();
    schema->init_from_pb(pb);
    schema->set_storage_format(TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2);
    schema->set_storage_page_size(4096);
    return schema;
}
Status write_and_open(const io::FileSystemSPtr& fs, const std::string& path,
                      const TabletSchemaSPtr& schema, Block* block, uint64_t* file_size,
                      std::shared_ptr<segment_v2::Segment>* segment) {
    io::FileWriterPtr file_writer;
    RETURN_IF_ERROR(fs->create_file(path, &file_writer));
    RowsetWriterContext context;
    context.write_type = DataWriteType::TYPE_DIRECT;
    context.tablet_schema = schema;
    SegmentWriterOptions options;
    options.compression_type = CompressionTypePB::LZ4;
    options.num_rows_per_block = 1024;
    options.rowset_ctx = &context;
    options.write_type = DataWriteType::TYPE_DIRECT;
    segment_v2::SegmentWriter writer(file_writer.get(), 0, schema, nullptr, nullptr, options,
                                     nullptr);
    RETURN_IF_ERROR(writer.init());
    RETURN_IF_ERROR(writer.append_block(block, 0, block->rows()));
    uint64_t index_size = 0;
    RETURN_IF_ERROR(writer.finalize(file_size, &index_size));
    RETURN_IF_ERROR(file_writer->close());
    RowsetId rowset_id;
    rowset_id.init(1);
    return segment_v2::Segment::open(fs, path, 1, 0, rowset_id, schema, io::FileReaderOptions {},
                                     segment);
}
Status write_vertical_and_open(const io::FileSystemSPtr& fs, const std::string& path,
                               const TabletSchemaSPtr& schema, Block* block, uint64_t* file_size,
                               std::shared_ptr<segment_v2::Segment>* segment) {
    io::FileWriterPtr file_writer;
    RETURN_IF_ERROR(fs->create_file(path, &file_writer));
    RowsetWriterContext context;
    context.write_type = DataWriteType::TYPE_DIRECT;
    context.tablet_schema = schema;
    segment_v2::VerticalSegmentWriterOptions options;
    options.compression_type = CompressionTypePB::LZ4;
    options.num_rows_per_block = 1024;
    options.rowset_ctx = &context;
    options.write_type = DataWriteType::TYPE_DIRECT;
    segment_v2::VerticalSegmentWriter writer(file_writer.get(), 0, schema, nullptr, nullptr,
                                             options, nullptr);
    RETURN_IF_ERROR(writer.init());
    RETURN_IF_ERROR(writer.batch_block(block, 0, block->rows()));
    RETURN_IF_ERROR(writer.write_batch());
    uint64_t index_size = 0;
    RETURN_IF_ERROR(writer.finalize(file_size, &index_size));
    RowsetId rowset_id;
    rowset_id.init(2);
    return segment_v2::Segment::open(fs, path, 1, 0, rowset_id, schema, io::FileReaderOptions {},
                                     segment);
}
Block v2_public_writer_block(const TabletSchemaSPtr& schema) {
    Block block = schema->create_block();
    auto columns_guard = block.mutate_columns_scoped();
    auto& columns = columns_guard.mutable_columns();
    columns[0]->insert(Field::create_field<TYPE_INT>(1));

    VariantBatchBuilder builder({.rows = 1, .metadata_keys = 1});
    auto row = builder.begin_row();
    auto object = row.start_object();
    object.add_key(StringRef("a"));
    row.add_int(1);
    object.finish();
    row.finish();
    auto encoded = builder.finish_batch();
    assert_cast<ColumnVariantV2&>(*columns[1]).insert_encoded_batch(encoded);

    columns_guard.restore();
    return block;
}
Field jsonb_field(std::string_view json) {
    DataTypeJsonb type;
    auto column = type.create_column();
    Slice slice(json.data(), json.size());
    DataTypeSerDe::FormatOptions options;
    options.converted_from_string = true;
    auto status = type.get_serde()->deserialize_one_cell_from_json(*column, slice, options);
    if (!status.ok()) {
        throw Exception(status.code(), status.to_string());
    }
    const StringRef bytes = column->get_data_at(0);
    return Field::create_field<TYPE_JSONB>(JsonbField(bytes.data, bytes.size));
}
void add_field(std::vector<std::pair<std::string, Field>>* fields, std::vector<std::string>* source,
               std::string_view case_id, int row, std::string path, std::string type,
               std::string value, Field field) {
    source->push_back(fmt::format("{}\t{}\t{}\t{}\t{}", case_id, row, path, type, value));
    fields->emplace_back(path == "$" ? "" : std::move(path), std::move(field));
}
void sort_and_append(std::vector<std::string>* lines, std::string* output) {
    std::ranges::sort(*lines);
    for (const auto& line : *lines) {
        output->append(line).push_back('\n');
    }
}
Block ordinary_block(const TabletSchemaSPtr& schema, std::string* rendered_source) {
    Block block = schema->create_block();
    auto columns_guard = block.mutate_columns_scoped();
    auto& columns = columns_guard.mutable_columns();
    VariantBatchBuilder builder({.rows = 13, .metadata_keys = 10});
    std::vector<std::string> source;
    for (Int32 row = 0; row < 13; ++row) {
        columns[0]->insert(Field::create_field<TYPE_INT>(row));
        auto variant_row = builder.begin_row();
        std::vector<std::pair<std::string, Field>> fields;
        const std::string root =
                row == 12 ? "42"
                          : fmt::format(R"({{"collision":{},"root_only":{}}})", 1000 + row, row);
        auto add = [&](std::string path, std::string type, std::string value, Field field) {
            add_field(&fields, &source, "ordinary_sparse", row, std::move(path), std::move(type),
                      std::move(value), std::move(field));
        };
        add("$", "JSONB", root, jsonb_field(root));
        if (row == 12) {
            variant_row.add_int(42);
        } else {
            // The source rows below retain the legacy raw provenance. The V2 byte gate encodes
            // the committed assembled semantics: the old root-only JSONB sidecar, its shadowed
            // collision value, and sparse NULL are not visible; the nested object remains visible.
            auto object = variant_row.start_object();
            if (row < 12) {
                add("hot", "BIGINT", std::to_string(row), Field::create_field<TYPE_BIGINT>(row));
                object.add_key(StringRef("hot"));
                variant_row.add_int(row);
            }
            if (row < 11) {
                add("collision", "BIGINT", std::to_string(2000 + row),
                    Field::create_field<TYPE_BIGINT>(2000 + row));
                object.add_key(StringRef("collision"));
                variant_row.add_int(2000 + row);
            }
            if (row < 10) {
                Field integer;
                const auto value = static_cast<Int64>(row + 1);
                switch (row % 4) {
                case 0:
                    integer = Field::create_field<TYPE_TINYINT>(value);
                    break;
                case 1:
                    integer = Field::create_field<TYPE_SMALLINT>(value);
                    break;
                case 2:
                    integer = Field::create_field<TYPE_INT>(value);
                    break;
                default:
                    integer = Field::create_field<TYPE_BIGINT>(value);
                    break;
                }
                const auto integer_type = integer.get_type_name();
                add("lct_int", integer_type, std::to_string(value), std::move(integer));
                object.add_key(StringRef("lct_int"));
                variant_row.add_int(value);
            }
            if (row < 9) {
                Field number = row % 2 == 0 ? Field::create_field<TYPE_INT>(row)
                                            : Field::create_field<TYPE_DOUBLE>(row + 0.5);
                const auto number_type = number.get_type_name();
                add("lct_double", number_type,
                    row % 2 == 0 ? std::to_string(row) : fmt::format("{:.1f}", row + 0.5),
                    std::move(number));
                object.add_key(StringRef("lct_double"));
                if (row % 2 == 0) {
                    variant_row.add_int(row);
                } else {
                    variant_row.add_double(row + 0.5);
                }
            }
            if (row < 8) {
                add("flag", "BOOL", row % 2 == 0 ? "false" : "true",
                    Field::create_field<TYPE_BOOLEAN>(row % 2));
                object.add_key(StringRef("flag"));
                variant_row.add_bool(row % 2 != 0);
            }
            if (row < 7) {
                const std::string value = "s" + std::to_string(row);
                add("text", "STRING", value, Field::create_field<TYPE_STRING>(String(value)));
                object.add_key(StringRef("text"));
                variant_row.add_string(StringRef(value));
            }
            if (row < 6) {
                Array array {Field::create_field<TYPE_BIGINT>(row),
                             Field::create_field<TYPE_BIGINT>(row + 1)};
                add("array", "ARRAY", fmt::format("[{},{}]", row, row + 1),
                    Field::create_field<TYPE_ARRAY>(std::move(array)));
                object.add_key(StringRef("array"));
                auto encoded_array = variant_row.start_array();
                variant_row.add_int(row);
                variant_row.add_int(row + 1);
                encoded_array.finish();
            }
            if (row < 5) {
                const std::string value = fmt::format("{{\"x\":{}}}", row);
                add("object", "JSONB", value, jsonb_field(value));
                object.add_key(StringRef("object"));
                auto nested = variant_row.start_object();
                nested.add_key(StringRef("x"));
                variant_row.add_int(row);
                nested.finish();
            }
            if (row < 4) {
                add("nullish", "NULL", "null", Field());
            }
            object.finish();
        }
        variant_row.finish();
    }
    VariantBatchBuilder encoded = builder.finish_batch();
    assert_cast<ColumnVariantV2&>(*columns[1]).insert_encoded_batch(encoded);
    *rendered_source = kSourceHeader;
    sort_and_append(&source, rendered_source);
    columns_guard.restore();
    return block;
}
Block doc_block(const TabletSchemaSPtr& schema, std::string* rendered_source) {
    Block block = schema->create_block();
    auto columns_guard = block.mutate_columns_scoped();
    auto& columns = columns_guard.mutable_columns();
    JsonStringToVariantEncoder encoder;
    *rendered_source = kSourceHeader;
    for (Int32 row = 0; row < 8; ++row) {
        columns[0]->insert(Field::create_field<TYPE_INT>(row));
        const std::string value = fmt::format(
                "{{\"flag\":{},\"id\":{},\"name\":\"n{}\",\"nested\":{{\"x\":{}}},"
                "\"nullish\":null,\"ratio\":{:.1f}}}",
                row % 2 == 0 ? "false" : "true", row, row, row + 100, row + 0.5);
        encoder.add_json(StringRef(value));
        rendered_source->append(fmt::format("doc\t{}\t$\tJSON\t{}\n", row, value));
    }
    VariantBatchBuilder encoded = encoder.finish_batch();
    assert_cast<ColumnVariantV2&>(*columns[1]).insert_encoded_batch(encoded);
    columns_guard.restore();
    return block;
}
std::array<std::string, 3> bucket_paths() {
    std::array<std::string, 3> selected;
    for (int candidate_id = 0; candidate_id < 32; ++candidate_id) {
        const std::string candidate = fmt::format("cold_{:02}", candidate_id);
        const StringRef ref {candidate.data(), candidate.size()};
        const auto bucket = variant_util::variant_binary_shard_of(ref, selected.size());
        if (selected[bucket].empty()) {
            selected[bucket] = candidate;
        }
    }
    for (const auto& path : selected) {
        DORIS_CHECK(!path.empty());
    }
    return selected;
}
Block bucketed_sparse_block(const TabletSchemaSPtr& schema, std::string* rendered_source) {
    Block block = schema->create_block();
    auto columns_guard = block.mutate_columns_scoped();
    auto& columns = columns_guard.mutable_columns();
    const auto paths = bucket_paths();
    VariantBatchBuilder builder({.rows = 12, .metadata_keys = paths.size() + 1});
    std::vector<std::string> source;
    for (Int32 row = 0; row < 12; ++row) {
        columns[0]->insert(Field::create_field<TYPE_INT>(row));
        auto variant_row = builder.begin_row();
        auto object = variant_row.start_object();
        std::vector<std::pair<std::string, Field>> fields;
        auto add = [&](std::string path, std::string type, std::string value, Field field) {
            add_field(&fields, &source, "bucketed_sparse", row, std::move(path), std::move(type),
                      std::move(value), std::move(field));
        };
        add("hot", "BIGINT", std::to_string(row), Field::create_field<TYPE_BIGINT>(row));
        object.add_key(StringRef("hot"));
        variant_row.add_int(row);
        if (row < 9) {
            add(paths[0], "BIGINT", std::to_string(row + 10),
                Field::create_field<TYPE_BIGINT>(row + 10));
            object.add_key(StringRef(paths[0]));
            variant_row.add_int(row + 10);
        }
        if (row < 8) {
            const std::string value = "b1_" + std::to_string(row);
            add(paths[1], "STRING", value, Field::create_field<TYPE_STRING>(String(value)));
            object.add_key(StringRef(paths[1]));
            variant_row.add_string(StringRef(value));
        }
        if (row < 7) {
            add(paths[2], "BOOL", row % 2 == 0 ? "false" : "true",
                Field::create_field<TYPE_BOOLEAN>(row % 2));
            object.add_key(StringRef(paths[2]));
            variant_row.add_bool(row % 2 != 0);
        }
        object.finish();
        variant_row.finish();
    }
    VariantBatchBuilder encoded = builder.finish_batch();
    assert_cast<ColumnVariantV2&>(*columns[1]).insert_encoded_batch(encoded);
    *rendered_source = kSourceHeader;
    sort_and_append(&source, rendered_source);
    columns_guard.restore();
    return block;
}
using CaseBuilder = Block (*)(const TabletSchemaSPtr&, std::string*);
constexpr std::array<CaseBuilder, 3> kCaseBuilders {ordinary_block, doc_block,
                                                    bucketed_sparse_block};
std::string relative_path(const ColumnMetaPB& meta) {
    if (!meta.has_column_path_info()) {
        return "$key";
    }
    PathInData path;
    path.from_protobuf(meta.column_path_info());
    const auto relative = path.copy_pop_front().get_path();
    if (!relative.empty()) {
        return relative;
    }
    return static_cast<FieldType>(meta.type()) == FieldType::OLAP_FIELD_TYPE_VARIANT ? "$variant"
                                                                                     : "$root";
}
std::string hex(StringRef bytes) {
    static constexpr char digits[] = "0123456789abcdef";
    std::string result(bytes.size * 2, '0');
    for (size_t i = 0; i < bytes.size; ++i) {
        const auto byte = static_cast<uint8_t>(bytes.data[i]);
        result[2 * i] = digits[byte >> 4];
        result[2 * i + 1] = digits[byte & 0x0f];
    }
    return result;
}
struct BinaryMapInspection {
    std::map<std::string, std::set<FieldType>> types;
    std::map<std::string, size_t> counts;
    std::vector<std::string> cell_paths;
    std::vector<std::string> records;
};
std::string render_binary_summary(const std::map<std::string, std::set<FieldType>>& types,
                                  const std::map<std::string, size_t>& counts) {
    std::set<std::string> paths;
    for (const auto& [path, _] : types) {
        paths.insert(path);
    }
    for (const auto& [path, _] : counts) {
        paths.insert(path);
    }
    std::string output;
    for (const auto& path : paths) {
        if (!output.empty()) {
            output.push_back(';');
        }
        const auto count = counts.find(path);
        output.append(fmt::format("{}:count={},types=[", path,
                                  count == counts.end() ? 0 : count->second));
        const auto type_set = types.find(path);
        if (type_set != types.end()) {
            bool first = true;
            for (const auto type : type_set->second) {
                if (!first) {
                    output.push_back(',');
                }
                first = false;
                output.append(std::to_string(static_cast<int>(type)));
            }
        }
        output.push_back(']');
    }
    return output;
}
Status inspect_binary_map(std::string_view case_id, const ColumnMetaPB& meta,
                          const std::shared_ptr<segment_v2::Segment>& segment,
                          const TabletSchemaSPtr& schema, BinaryMapInspection* output) {
    segment_v2::ColumnReaderOptions reader_options;
    reader_options.tablet_schema = schema;
    reader_options.be_exec_version = BeExecVersionManager::get_newest_version();
    std::shared_ptr<segment_v2::ColumnReader> reader;
    RETURN_IF_ERROR(segment_v2::ColumnReader::create(reader_options, meta, segment->num_rows(),
                                                     segment->file_reader(), &reader));
    segment_v2::ColumnIteratorUPtr iterator;
    RETURN_IF_ERROR(reader->new_iterator(&iterator, nullptr));
    OlapReaderStatistics read_stats;
    segment_v2::ColumnIteratorOptions iterator_options;
    iterator_options.file_reader = segment->file_reader().get();
    iterator_options.stats = &read_stats;
    RETURN_IF_ERROR(iterator->init(iterator_options));
    RETURN_IF_ERROR(iterator->seek_to_ordinal(0));
    MutableColumnPtr binary = DataTypeFactory::instance().create_data_type(meta)->create_column();
    size_t rows = segment->num_rows();
    RETURN_IF_ERROR(iterator->next_batch(&rows, binary));
    if (rows != segment->num_rows()) {
        return Status::Corruption("partial binary-map read");
    }
    const auto& map = assert_cast<const ColumnMap&>(*binary);
    const auto& keys = assert_cast<const ColumnString&>(map.get_keys());
    const auto& values = assert_cast<const ColumnString&>(map.get_values());
    const auto& offsets = map.get_offsets();
    output->types.clear();
    output->counts.clear();
    output->cell_paths.clear();
    output->records.clear();
    for (size_t row = 0; row < rows; ++row) {
        const size_t begin = row == 0 ? 0 : offsets[row - 1];
        std::string previous;
        for (size_t i = begin; i < offsets[row]; ++i) {
            const std::string path = keys.get_data_at(i).to_string();
            if (!previous.empty() && previous >= path) {
                return Status::Corruption("{} row {} binary paths are not ordered", case_id, row);
            }
            previous = path;
            const StringRef bytes = values.get_data_at(i);
            if (bytes.size == 0) {
                return Status::Corruption("{} binary cell is empty", case_id);
            }
            Field decoded;
            FieldInfo info;
            const auto* begin_data = reinterpret_cast<const uint8_t*>(bytes.data);
            const auto* end = DataTypeSerDe::deserialize_binary_to_field(begin_data, decoded, info);
            if (end != begin_data + bytes.size) {
                return Status::Corruption("{} payload length mismatch at row {} path {}", case_id,
                                          row, path);
            }
            const auto type = static_cast<FieldType>(*begin_data);
            output->types[path].insert(type);
            ++output->counts[path];
            output->cell_paths.push_back(path);
            output->records.push_back(fmt::format("cell\t{}\t-1\t{}\t{}\t-\t{}\t{}\tdecoded={}",
                                                  case_id, row, path, static_cast<int>(type),
                                                  hex(bytes), decoded.get_type_name()));
        }
    }
    std::ranges::sort(output->records);
    return Status::OK();
}
struct AssembledInspection {
    std::vector<std::string> rows;
    std::vector<std::string> records;
    OlapReaderStatistics stats;
    bool hierarchical = false;
};
struct SnapshotJsonWriter {
    void write(const char* data, size_t size) { value.append(data, size); }

    std::string value;
};
Status inspect_assembled(std::string_view case_id,
                         const std::shared_ptr<segment_v2::Segment>& segment,
                         const TabletSchemaSPtr& schema, AssembledInspection* output) {
    StorageReadOptions storage_options;
    storage_options.stats = &output->stats;
    segment_v2::ColumnIteratorUPtr iterator;
    RETURN_IF_ERROR(segment->new_column_iterator(schema->column(1), &iterator, &storage_options));
    output->hierarchical =
            dynamic_cast<segment_v2::HierarchicalDataIterator*>(iterator.get()) != nullptr;
    segment_v2::ColumnIteratorOptions options;
    options.file_reader = segment->file_reader().get();
    options.stats = &output->stats;
    RETURN_IF_ERROR(iterator->init(options));
    RETURN_IF_ERROR(iterator->seek_to_ordinal(0));
    MutableColumnPtr assembled =
            DataTypeFactory::instance().create_data_type(schema->column(1), false)->create_column();
    size_t rows = segment->num_rows();
    RETURN_IF_ERROR(iterator->next_batch(&rows, assembled));
    if (rows != segment->num_rows()) {
        return Status::Corruption("partial assembled read");
    }
    const auto& variant = assert_cast<const ColumnVariantV2&>(*assembled);
    for (size_t row = 0; row < rows; ++row) {
        SnapshotJsonWriter writer;
        to_json(variant.get_value_ref(row), writer);
        output->rows.push_back(writer.value);
        output->records.push_back(
                fmt::format("assembled\t{}\t-1\t{}\t$\t-\t-\t-\t{}", case_id, row, writer.value));
    }
    return Status::OK();
}
struct PhysicalInspection {
    std::vector<ColumnMetaPB> metas;
    std::map<std::string, FieldType> types;
    std::map<std::string, const ColumnMetaPB*> by_path;
    std::vector<std::string> records;
};
Status inspect_physical(std::string_view case_id,
                        const std::shared_ptr<segment_v2::Segment>& segment,
                        const std::map<std::string, FieldType>& expected,
                        PhysicalInspection* output) {
    RETURN_IF_ERROR(segment->traverse_column_meta_pbs(
            [&](const ColumnMetaPB& meta) { output->metas.push_back(meta); }));
    if (output->metas.size() != expected.size()) {
        return Status::Corruption("{} expected {} physical columns, got {}", case_id,
                                  expected.size(), output->metas.size());
    }
    for (size_t ordinal = 0; ordinal < output->metas.size(); ++ordinal) {
        const auto& meta = output->metas[ordinal];
        const auto path = relative_path(meta);
        const int64_t column_id = meta.has_column_id() ? meta.column_id() : -1;
        const int64_t unique_id =
                meta.has_unique_id() ? static_cast<int32_t>(meta.unique_id()) : -1;
        const int64_t parent_uid =
                meta.has_column_path_info() &&
                                meta.column_path_info().has_parrent_column_unique_id()
                        ? meta.column_path_info().parrent_column_unique_id()
                        : -1;
        std::string_view nullable = "-";
        if (meta.has_is_nullable()) {
            nullable = meta.is_nullable() ? "true" : "false";
        }
        output->types[path] = static_cast<FieldType>(meta.type());
        output->by_path[path] = &meta;
        output->records.push_back(fmt::format(
                "column\t{}\t{}\t-1\t{}\t{}\t-\t-\tcolumn_id={};unique_id={};parent_uid={};"
                "encoding={};compression={};nullable={};rows={};raw={};compressed={};uncompressed={"
                "}",
                case_id, ordinal, path, meta.type(), column_id, unique_id, parent_uid,
                meta.has_encoding() ? segment_v2::EncodingTypePB_Name(meta.encoding()) : "-",
                meta.has_compression() ? segment_v2::CompressionTypePB_Name(meta.compression())
                                       : "-",
                nullable, meta.num_rows(), meta.raw_data_bytes(), meta.compressed_data_bytes(),
                meta.uncompressed_data_bytes()));
    }
    if (output->types == expected) {
        return Status::OK();
    }
    std::string rendered;
    for (const auto& [path, type] : output->types) {
        rendered.append(fmt::format("{}={};", path, static_cast<int>(type)));
    }
    return Status::Corruption("{} physical layout changed: {}", case_id, rendered);
}
void render_manifest(std::vector<std::string>* records, std::string* manifest) {
    *manifest = kManifestHeader;
    sort_and_append(records, manifest);
}
Status inspect_ordinary(const std::shared_ptr<segment_v2::Segment>& segment,
                        const TabletSchemaSPtr& schema, std::string* manifest,
                        bool legacy_persisted_layout = false) {
    const std::map<std::string, FieldType> expected_physical {
            {"$key", FieldType::OLAP_FIELD_TYPE_INT},
            {"$variant", FieldType::OLAP_FIELD_TYPE_VARIANT},
            {"collision", FieldType::OLAP_FIELD_TYPE_BIGINT},
            {"hot", FieldType::OLAP_FIELD_TYPE_BIGINT},
            {"__DORIS_VARIANT_SPARSE__", FieldType::OLAP_FIELD_TYPE_MAP}};
    PhysicalInspection physical;
    RETURN_IF_ERROR(inspect_physical("ordinary_sparse", segment, expected_physical, &physical));
    std::vector<std::string> records = std::move(physical.records);
    BinaryMapInspection sparse;
    RETURN_IF_ERROR(inspect_binary_map("ordinary_sparse",
                                       *physical.by_path.at("__DORIS_VARIANT_SPARSE__"), segment,
                                       schema, &sparse));
    records.insert(records.end(), sparse.records.begin(), sparse.records.end());
    decltype(sparse.types) expected_types {{"array", {FieldType::OLAP_FIELD_TYPE_ARRAY}},
                                           {"flag", {FieldType::OLAP_FIELD_TYPE_BOOL}},
                                           {"lct_double", {FieldType::OLAP_FIELD_TYPE_DOUBLE}},
                                           {"lct_int", {FieldType::OLAP_FIELD_TYPE_BIGINT}},
                                           {"text", {FieldType::OLAP_FIELD_TYPE_STRING}}};
    decltype(sparse.counts) expected_counts {
            {"array", 6}, {"flag", 8}, {"lct_double", 9}, {"lct_int", 10}, {"text", 7}};
    if (legacy_persisted_layout) {
        expected_types.emplace("object", std::set {FieldType::OLAP_FIELD_TYPE_JSONB});
        expected_counts.emplace("object", 5);
    } else {
        expected_types.emplace("object.x", std::set {FieldType::OLAP_FIELD_TYPE_BIGINT});
        expected_counts.emplace("object.x", 5);
    }
    if (sparse.types != expected_types || sparse.counts != expected_counts) {
        return Status::Corruption("ordinary sparse cells changed: expected={}; actual={}",
                                  render_binary_summary(expected_types, expected_counts),
                                  render_binary_summary(sparse.types, sparse.counts));
    }
    AssembledInspection assembled;
    RETURN_IF_ERROR(inspect_assembled("ordinary_sparse", segment, schema, &assembled));
    records.insert(records.end(), assembled.records.begin(), assembled.records.end());
    if (assembled.rows[0] !=
                R"({"array":[0,1],"collision":2000,"flag":false,"hot":0,"lct_double":0,"lct_int":1,"object":{"x":0},"text":"s0"})" ||
        assembled.rows[11] != R"({"hot":11})" || assembled.rows[12] != "42") {
        return Status::Corruption("ordinary assembled rows changed: row0={};row11={};row12={}",
                                  assembled.rows[0], assembled.rows[11], assembled.rows[12]);
    }
    if (!assembled.hierarchical || assembled.stats.variant_subtree_hierarchical_iter_count != 1) {
        return Status::Corruption("ordinary did not use one hierarchical iterator");
    }
    render_manifest(&records, manifest);
    return Status::OK();
}
Status inspect_doc(const std::shared_ptr<segment_v2::Segment>& segment,
                   const TabletSchemaSPtr& schema, std::string* manifest) {
    const std::map<std::string, FieldType> expected_physical {
            {"$key", FieldType::OLAP_FIELD_TYPE_INT},
            {"$variant", FieldType::OLAP_FIELD_TYPE_VARIANT},
            {"__DORIS_VARIANT_DOC_VALUE__.b0", FieldType::OLAP_FIELD_TYPE_MAP}};
    PhysicalInspection physical;
    RETURN_IF_ERROR(inspect_physical("doc", segment, expected_physical, &physical));
    std::vector<std::string> records = std::move(physical.records);
    BinaryMapInspection doc;
    RETURN_IF_ERROR(inspect_binary_map(
            "doc", *physical.by_path.at("__DORIS_VARIANT_DOC_VALUE__.b0"), segment, schema, &doc));
    records.insert(records.end(), doc.records.begin(), doc.records.end());
    const std::map<std::string, FieldType> expected_types {
            {"flag", FieldType::OLAP_FIELD_TYPE_BOOL},
            {"id", FieldType::OLAP_FIELD_TYPE_BIGINT},
            {"name", FieldType::OLAP_FIELD_TYPE_STRING},
            {"nested.x", FieldType::OLAP_FIELD_TYPE_BIGINT},
            {"ratio", FieldType::OLAP_FIELD_TYPE_DOUBLE}};
    for (const auto& [path, type] : expected_types) {
        if (doc.types[path] != std::set<FieldType> {type}) {
            return Status::Corruption("doc path {} has unexpected FieldType set", path);
        }
    }
    if (doc.types.contains("nullish")) {
        return Status::Corruption("doc nullish was persisted");
    }
    AssembledInspection assembled;
    RETURN_IF_ERROR(inspect_assembled("doc", segment, schema, &assembled));
    records.insert(records.end(), assembled.records.begin(), assembled.records.end());
    if (!assembled.hierarchical || assembled.stats.variant_doc_value_column_iter_count != 1 ||
        assembled.stats.variant_subtree_hierarchical_iter_count != 1) {
        return Status::Corruption("doc did not use one HIERARCHICAL_DOC iterator");
    }
    if (assembled.rows.size() != 8 ||
        assembled.rows[0].find(R"("nested":{"x":100})") == std::string::npos ||
        assembled.rows[7].find(R"("name":"n7")") == std::string::npos) {
        return Status::Corruption("doc assembled rows changed");
    }
    render_manifest(&records, manifest);
    return Status::OK();
}
Status inspect_bucketed_sparse(const std::shared_ptr<segment_v2::Segment>& segment,
                               const TabletSchemaSPtr& schema, std::string* manifest) {
    constexpr size_t kBuckets = 3;
    std::map<std::string, FieldType> expected_physical {
            {"$key", FieldType::OLAP_FIELD_TYPE_INT},
            {"$variant", FieldType::OLAP_FIELD_TYPE_VARIANT},
            {"hot", FieldType::OLAP_FIELD_TYPE_BIGINT}};
    for (size_t bucket = 0; bucket < kBuckets; ++bucket) {
        expected_physical.emplace(fmt::format("__DORIS_VARIANT_SPARSE__.b{}", bucket),
                                  FieldType::OLAP_FIELD_TYPE_MAP);
    }
    PhysicalInspection physical;
    RETURN_IF_ERROR(inspect_physical("bucketed_sparse", segment, expected_physical, &physical));
    std::vector<std::string> records = std::move(physical.records);
    const auto paths = bucket_paths();
    const std::array<size_t, kBuckets> expected_counts {9, 8, 7};
    const std::array<FieldType, kBuckets> expected_types {FieldType::OLAP_FIELD_TYPE_BIGINT,
                                                          FieldType::OLAP_FIELD_TYPE_STRING,
                                                          FieldType::OLAP_FIELD_TYPE_BOOL};
    for (size_t bucket = 0; bucket < kBuckets; ++bucket) {
        const auto sparse_path = fmt::format("__DORIS_VARIANT_SPARSE__.b{}", bucket);
        const auto* sparse_meta = physical.by_path.at(sparse_path);
        BinaryMapInspection sparse;
        RETURN_IF_ERROR(
                inspect_binary_map("bucketed_sparse", *sparse_meta, segment, schema, &sparse));
        records.insert(records.end(), sparse.records.begin(), sparse.records.end());
        if (sparse.counts !=
                    std::map<std::string, size_t> {{paths[bucket], expected_counts[bucket]}} ||
            sparse.types != std::map<std::string, std::set<FieldType>> {
                                    {paths[bucket], {expected_types[bucket]}}}) {
            return Status::Corruption("bucket {} decoded stats or types changed", bucket);
        }
        for (const auto& path : sparse.cell_paths) {
            const StringRef ref {path.data(), path.size()};
            if (variant_util::variant_binary_shard_of(ref, kBuckets) != bucket) {
                return Status::Corruption("path {} persisted in wrong bucket {}", path, bucket);
            }
        }
        std::map<std::string, uint32_t> persisted_stats;
        for (const auto& [path, count] :
             sparse_meta->variant_statistics().sparse_column_non_null_size()) {
            persisted_stats[path] = count;
        }
        const std::map<std::string, uint32_t> expected_stats {
                {paths[bucket], static_cast<uint32_t>(expected_counts[bucket])}};
        if (persisted_stats != expected_stats) {
            return Status::Corruption("bucket {} persisted statistics changed", bucket);
        }
    }
    AssembledInspection assembled;
    RETURN_IF_ERROR(inspect_assembled("bucketed_sparse", segment, schema, &assembled));
    records.insert(records.end(), assembled.records.begin(), assembled.records.end());
    if (!assembled.hierarchical || assembled.stats.variant_subtree_hierarchical_iter_count != 1 ||
        assembled.rows.size() != 12 || assembled.rows[11].find("\"hot\":11") == std::string::npos ||
        assembled.rows[11].find("cold_") != std::string::npos) {
        return Status::Corruption("bucketed sparse assembled rows changed");
    }
    for (const auto& path : paths) {
        if (assembled.rows[0].find("\"" + path + "\":") == std::string::npos) {
            return Status::Corruption("bucketed sparse row 0 lost path {}", path);
        }
    }
    render_manifest(&records, manifest);
    return Status::OK();
}
Status inspect_case(CaseKind kind, const std::shared_ptr<segment_v2::Segment>& segment,
                    const TabletSchemaSPtr& schema, std::string* manifest,
                    bool legacy_persisted_layout = false) {
    switch (kind) {
    case CaseKind::ORDINARY:
        return inspect_ordinary(segment, schema, manifest, legacy_persisted_layout);
    case CaseKind::DOC:
        return inspect_doc(segment, schema, manifest);
    case CaseKind::BUCKETED:
        return inspect_bucketed_sparse(segment, schema, manifest);
    }
    __builtin_unreachable();
}
Status VariantSegmentSnapshotTest::run_case(CaseKind kind, const std::string& path,
                                            std::string* source, std::string* manifest,
                                            uint64_t* file_size) {
    const auto ordinal = static_cast<size_t>(kind);
    auto schema = make_schema(kCaseConfigs[ordinal]);
    Block block = kCaseBuilders[ordinal](schema, source);
    std::shared_ptr<segment_v2::Segment> segment;
    RETURN_IF_ERROR(write_and_open(fs, path, schema, &block, file_size, &segment));
    if (segment->num_rows() != block.rows()) {
        return Status::Corruption("segment row count changed: expected {}, got {}", block.rows(),
                                  segment->num_rows());
    }
    return inspect_case(kind, segment, schema, manifest);
}
Status VariantSegmentSnapshotTest::generate_samples(const std::filesystem::path& output) {
    if (!std::filesystem::is_directory(output) || !std::filesystem::is_empty(output)) {
        return Status::InvalidArgument("snapshot output must be an existing empty directory: {}",
                                       output.string());
    }
    std::string combined_source(kSourceHeader);
    std::vector<std::string> manifest_records;
    for (const auto kind : kCaseKinds) {
        const auto config = kCaseConfigs[static_cast<size_t>(kind)];
        std::string source;
        std::string manifest;
        uint64_t file_size = 0;
        RETURN_IF_ERROR(run_case(kind, (output / (std::string(config.id) + ".dat")).string(),
                                 &source, &manifest, &file_size));
        if (file_size == 0 || file_size > 64 * 1024 || !source.starts_with(kSourceHeader) ||
            !manifest.starts_with(kManifestHeader)) {
            return Status::Corruption("{} generated invalid snapshot metadata", config.id);
        }
        combined_source.append(source.substr(kSourceHeader.size()));
        size_t offset = kManifestHeader.size();
        while (offset < manifest.size()) {
            const auto end = manifest.find('\n', offset);
            if (end == std::string::npos) {
                return Status::Corruption("{} manifest lacks final newline", config.id);
            }
            manifest_records.emplace_back(manifest.substr(offset, end - offset));
            offset = end + 1;
        }
        manifest_records.push_back(
                fmt::format("segment\t{}\t-1\t-1\t$segment\t-\t-\t-\tfile_size={};storage_format="
                            "V2;footer=inline",
                            config.id, file_size));
    }
    manifest_records.emplace_back(
            "deferred\tnested_group\t-1\t-1\t$\t-\t-\t-\tstatus=deferred;task=T5.9");
    std::string combined_manifest(kManifestHeader);
    sort_and_append(&manifest_records, &combined_manifest);
    const std::array<std::pair<std::string_view, std::string_view>, 2> text_outputs {
            {{"source.tsv", combined_source}, {"manifest.tsv", combined_manifest}}};
    for (const auto& [name, contents] : text_outputs) {
        std::ofstream file(output / name, std::ios::binary | std::ios::trunc);
        if (!file.is_open()) {
            return Status::IOError("failed to open generated {}", name);
        }
        file.write(contents.data(), static_cast<std::streamsize>(contents.size()));
        if (!file.good()) {
            return Status::IOError("failed to write generated {}", name);
        }
    }
    return Status::OK();
}
Status compare_directories(const std::filesystem::path& generated,
                           const std::filesystem::path& committed) {
    if (!std::filesystem::is_directory(generated)) {
        return Status::NotFound("generated snapshot directory is missing: {}", generated.string());
    }
    if (!std::filesystem::is_directory(committed)) {
        return Status::NotFound("committed snapshot directory is missing: {}", committed.string());
    }
    std::set<std::string> generated_names;
    std::set<std::string> committed_names;
    for (const auto& entry : std::filesystem::directory_iterator(generated)) {
        generated_names.insert(entry.path().filename().string());
    }
    for (const auto& entry : std::filesystem::directory_iterator(committed)) {
        committed_names.insert(entry.path().filename().string());
    }
    if (generated_names != committed_names) {
        std::string difference;
        for (const auto& name : committed_names) {
            if (!generated_names.contains(name)) {
                difference.append("missing ").append(name).append(";");
            }
        }
        for (const auto& name : generated_names) {
            if (!committed_names.contains(name)) {
                difference.append("extra ").append(name).append(";");
            }
        }
        return Status::Corruption("snapshot filename set changed: {}", difference);
    }
    for (const auto& name : generated_names) {
        const auto generated_path = generated / name;
        const auto committed_path = committed / name;
        if (!std::filesystem::is_regular_file(generated_path) ||
            !std::filesystem::is_regular_file(committed_path)) {
            return Status::Corruption("snapshot {} is not a regular file", name);
        }
        std::ifstream generated_file(generated_path, std::ios::binary);
        std::ifstream committed_file(committed_path, std::ios::binary);
        if (!generated_file.is_open() || !committed_file.is_open()) {
            return Status::IOError("failed to open snapshot {} for comparison", name);
        }
        const std::string generated_bytes((std::istreambuf_iterator<char>(generated_file)), {});
        const std::string committed_bytes((std::istreambuf_iterator<char>(committed_file)), {});
        if (generated_bytes != committed_bytes) {
            size_t offset = 0;
            while (offset < std::min(generated_bytes.size(), committed_bytes.size()) &&
                   generated_bytes[offset] == committed_bytes[offset]) {
                ++offset;
            }
            return Status::Corruption(
                    "snapshot {} byte drift at offset {} (generated={}, committed={})", name,
                    offset, generated_bytes.size(), committed_bytes.size());
        }
    }
    return Status::OK();
}
TEST_F(VariantSegmentSnapshotTest, GenerateOrVerifyGolden) {
    const char* output = std::getenv("DORIS_REGEN_VARIANT_SEGMENT_SNAPSHOT_OUTPUT");
    if (output != nullptr) {
        ASSERT_TRUE(status_ok(generate_samples(output)));
        return;
    }
    const auto generated = dir / "generated";
    ASSERT_TRUE(status_ok(fs->create_directory(generated.string())));
    ASSERT_TRUE(status_ok(generate_samples(generated)));
    ASSERT_TRUE(status_ok(compare_directories(generated, repo_root() / kSamplesPath)));
}
TEST_F(VariantSegmentSnapshotTest, LegacyPersistedSegmentsRemainReadable) {
    constexpr std::array<size_t, 3> expected_rows {13, 8, 12};
    for (const auto kind : kCaseKinds) {
        const auto ordinal = static_cast<size_t>(kind);
        const auto config = kCaseConfigs[ordinal];
        SCOPED_TRACE(config.id);

        auto schema = make_schema(config);
        const auto path = repo_root() / kLegacySamplesPath / (std::string(config.id) + ".dat");
        RowsetId rowset_id;
        rowset_id.init(static_cast<int64_t>(100 + ordinal));
        std::shared_ptr<segment_v2::Segment> segment;
        ASSERT_TRUE(status_ok(segment_v2::Segment::open(fs, path.string(), 1, 0, rowset_id, schema,
                                                        io::FileReaderOptions {}, &segment)));
        ASSERT_EQ(segment->num_rows(), expected_rows[ordinal]);

        std::string manifest;
        ASSERT_TRUE(status_ok(inspect_case(kind, segment, schema, &manifest,
                                           /*legacy_persisted_layout=*/true)));
        ASSERT_TRUE(manifest.starts_with(kManifestHeader));
    }
}
TEST_F(VariantSegmentSnapshotTest, PublicWriterRoundTrips) {
    for (const auto kind : kCaseKinds) {
        const auto config = kCaseConfigs[static_cast<size_t>(kind)];
        SCOPED_TRACE(config.id);
        std::string source;
        std::string manifest;
        uint64_t file_size = 0;
        const auto path = (dir / (std::string(config.id) + ".dat")).string();
        ASSERT_TRUE(status_ok(run_case(kind, path, &source, &manifest, &file_size)));
        ASSERT_GT(file_size, 0);
        ASSERT_LE(file_size, 64 * 1024);
        ASSERT_TRUE(source.starts_with(kSourceHeader));
        ASSERT_TRUE(manifest.starts_with(kManifestHeader));
    }
}
TEST_F(VariantSegmentSnapshotTest, V2PublicWriterRoundTrips) {
    auto schema = make_schema(kCaseConfigs[0]);
    Block block = v2_public_writer_block(schema);
    uint64_t file_size = 0;
    std::shared_ptr<segment_v2::Segment> segment;
    ASSERT_TRUE(status_ok(write_and_open(fs, (dir / "v2_public.dat").string(), schema, &block,
                                         &file_size, &segment)));
    ASSERT_EQ(segment->num_rows(), 1);
    ASSERT_GT(file_size, 0);
}
TEST_F(VariantSegmentSnapshotTest, DirectWriteRecordsExtractedSparsePathStatistics) {
    constexpr int kSparseBuckets = 3;
    auto schema = make_schema(
            CaseConfig {.id = "direct_sparse_stats", .max_subcolumns = 0, .sparse_buckets = 3});
    for (int bucket = 0; bucket < kSparseBuckets; ++bucket) {
        schema->append_column(variant_util::create_sparse_shard_column(schema->column(1), bucket));
    }

    Block block = schema->create_block();
    auto columns_guard = block.mutate_columns_scoped();
    auto& columns = columns_guard.mutable_columns();
    for (Int32 key = 0; key < 2; ++key) {
        columns[0]->insert(Field::create_field<TYPE_INT>(key));
    }

    VariantBatchBuilder builder({.rows = 2, .metadata_keys = 2});
    for (Int32 value = 1; value <= 2; ++value) {
        auto row = builder.begin_row();
        auto object = row.start_object();
        object.add_key(StringRef(""));
        row.add_int(value);
        object.add_key(StringRef("hot"));
        row.add_int(value * 10);
        object.finish();
        row.finish();
    }
    auto variant = ColumnVariantV2::create();
    variant->insert_encoded_batch(builder.finish_batch());
    columns[1] = std::move(variant);

    const std::array<std::string_view, 2> sparse_paths {"", "hot"};
    for (int bucket = 0; bucket < kSparseBuckets; ++bucket) {
        auto keys = ColumnString::create();
        auto values = ColumnString::create();
        auto offsets = ColumnArray::ColumnOffsets::create();
        for (size_t row = 0; row < 2; ++row) {
            for (const std::string_view path : sparse_paths) {
                const StringRef ref {path.data(), path.size()};
                if (variant_util::variant_binary_shard_of(ref, kSparseBuckets) != bucket) {
                    continue;
                }
                keys->insert_data(path.data(), path.size());
                values->insert_data("encoded", 7);
            }
            offsets->insert_value(keys->size());
        }
        columns[2 + bucket] =
                ColumnMap::create(std::move(keys), std::move(values), std::move(offsets));
    }
    columns_guard.restore();

    uint64_t file_size = 0;
    std::shared_ptr<segment_v2::Segment> segment;
    ASSERT_TRUE(status_ok(write_and_open(fs, (dir / "direct_sparse_stats.dat").string(), schema,
                                         &block, &file_size, &segment)));
    ASSERT_GT(file_size, 0);

    uint64_t vertical_file_size = 0;
    std::shared_ptr<segment_v2::Segment> vertical_segment;
    ASSERT_TRUE(status_ok(
            write_vertical_and_open(fs, (dir / "vertical_direct_sparse_stats.dat").string(), schema,
                                    &block, &vertical_file_size, &vertical_segment)));
    ASSERT_GT(vertical_file_size, 0);

    for (const auto& stored_segment : {segment, vertical_segment}) {
        OlapReaderStatistics reader_stats;
        std::shared_ptr<segment_v2::ColumnReader> column_reader;
        ASSERT_TRUE(status_ok(stored_segment->get_column_reader(2, &column_reader, &reader_stats)));
        auto* variant_reader = assert_cast<segment_v2::VariantColumnReader*>(column_reader.get());
        ASSERT_NE(variant_reader, nullptr);

        PathInData::Parts empty_key_parts;
        empty_key_parts.emplace_back("", false, 0);
        EXPECT_TRUE(variant_reader->exist_in_sparse_column(PathInData(std::move(empty_key_parts))));
        EXPECT_TRUE(variant_reader->exist_in_sparse_column(PathInData("hot")));
    }
}
} // namespace
} // namespace doris
