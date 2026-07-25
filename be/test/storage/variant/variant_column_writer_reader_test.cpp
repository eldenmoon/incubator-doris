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

#include <atomic>
#include <cstdlib>
#include <set>
#include <thread>

#include "common/config.h"
#include "core/column/column_array.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_jsonb.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type_serde/data_type_serde.h"
#include "core/value/variant/variant_batch_builder.h"
#include "exec/common/variant_util.h"
#include "gtest/gtest.h"
#include "runtime/runtime_state.h"
#include "storage/index/index_file_reader.h"
#include "storage/index/index_file_writer.h"
#include "storage/index/inverted/inverted_index_desc.h"
#include "storage/index/inverted/inverted_index_reader.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/segment/column_meta_accessor.h"
#include "storage/segment/column_reader.h"
#include "storage/segment/column_reader_cache.h"
#include "storage/segment/variant/binary_column_extract_iterator.h"
#include "storage/segment/variant/hierarchical_data_iterator.h"
#include "storage/segment/variant/nested_group_path.h"
#include "storage/segment/variant/nested_group_provider.h"
#include "storage/segment/variant/nested_group_streaming_write_plan.h"
#include "storage/segment/variant/sparse_column_merge_iterator.h"
#include "storage/segment/variant/variant_column_reader.h"
#include "storage/segment/variant/variant_column_writer_impl.h"
#include "storage/segment/variant_column_writer_reader_test_fixture.h"
#include "storage/storage_engine.h"
#include "testutil/variant_util.h"
#include "util/defer_op.h"

using namespace doris;

namespace doris {

static void construct_tablet_index(TabletIndexPB* tablet_index, int64_t index_id,
                                   const std::string& index_name, int32_t col_unique_id) {
    tablet_index->set_index_id(index_id);
    tablet_index->set_index_name(index_name);
    tablet_index->set_index_type(IndexType::INVERTED);
    tablet_index->add_col_unique_id(col_unique_id);
}

static void fill_nullable_variant_block(Block* block,
                                        std::unordered_map<int, std::string>* inserted_jsonstr,
                                        variant_util::PathToNoneNullValues* path_with_size) {
    auto jsons = ColumnString::create();
    auto nulls = ColumnUInt8::create();
    const auto append_nulls = [&](size_t count) {
        constexpr std::string_view empty_object = "{}";
        for (size_t i = 0; i < count; ++i) {
            jsons->insert_data(empty_object.data(), empty_object.size());
            nulls->insert_value(1);
        }
    };
    const auto append_values = [&](int count) {
        auto batch = ColumnString::create();
        std::unordered_map<int, std::string> inserted;
        const auto stats = VariantUtil::fill_string_column_with_test_data(batch, count, &inserted);
        path_with_size->insert(stats.begin(), stats.end());
        for (int row = 0; row < count; ++row) {
            const StringRef json = batch->get_data_at(row);
            jsons->insert_data(json.data, json.size);
            nulls->insert_value(0);
            (*inserted_jsonstr)[row] = inserted[row];
        }
    };

    for (int idx = 0; idx < 10; idx++) {
        append_nulls(1);
        append_values(80);
        append_nulls(17);
        append_values(2);
    }
    block->replace_by_position(
            0, ColumnNullable::create(encode_json_column_v2(*jsons), std::move(nulls)));
}

void check_column_meta(const ColumnMetaPB& column_meta, auto& path_with_size) {
    EXPECT_TRUE(column_meta.has_column_path_info());
    auto path = std::make_shared<PathInData>();
    path->from_protobuf(column_meta.column_path_info());
    EXPECT_EQ(column_meta.column_path_info().parrent_column_unique_id(), 1);
    EXPECT_EQ(column_meta.none_null_size(), path_with_size[path->copy_pop_front().get_path()]);
}

void check_sparse_column_meta(const ColumnMetaPB& column_meta, auto& path_with_size) {
    EXPECT_TRUE(column_meta.has_column_path_info());
    auto path = std::make_shared<PathInData>();
    path->from_protobuf(column_meta.column_path_info());
    EXPECT_EQ(column_meta.column_path_info().parrent_column_unique_id(), 1);
    for (const auto& [pat, size] : column_meta.variant_statistics().sparse_column_non_null_size()) {
        EXPECT_EQ(size, path_with_size[pat]);
    }
    auto base_path = path->copy_pop_front().get_path();
    EXPECT_TRUE(base_path == "__DORIS_VARIANT_SPARSE__" ||
                base_path.rfind("__DORIS_VARIANT_SPARSE__.b", 0) == 0);
}

static const ColumnMetaPB* find_footer_column_meta_by_relative_path(
        const SegmentFooterPB& footer, std::string_view relative_path) {
    for (int i = 0; i < footer.columns_size(); ++i) {
        const auto& column_meta = footer.columns(i);
        if (!column_meta.has_column_path_info()) {
            continue;
        }
        PathInData path;
        path.from_protobuf(column_meta.column_path_info());
        if (path.copy_pop_front().get_path() == relative_path) {
            return &column_meta;
        }
    }
    return nullptr;
}

static TabletColumn make_int_typed_path_template(
        std::string_view path, PatternTypePB pattern_type = PatternTypePB::MATCH_NAME) {
    ColumnPB column_pb;
    column_pb.set_unique_id(-1);
    column_pb.set_name(std::string(path));
    column_pb.set_type("INT");
    column_pb.set_is_nullable(true);
    column_pb.set_pattern_type(pattern_type);

    TabletColumn column;
    column.init_from_pb(column_pb);
    return column;
}

static std::string make_nested_variant_row(size_t row) {
    return R"({"a":{"b":[{"c":{"d":)" + std::to_string(row) + R"(,"e":"a@b"}}]},"x":"y"})";
}

static std::string make_nested_variant_array_item(size_t row) {
    return R"({"c":{"d":)" + std::to_string(row) + R"(,"e":"a@b"}})";
}

// Regression test for legacy flat-dot-key compatibility.
//
// Old versions (e.g. cloud-4.1.2 with variant_max_subcolumns_count=0) stored
// a flat JSON key like {"a.b": 1} as a single PathInData part "a.b" in the
// segment's ColumnPathInfo protobuf. New master compaction schema builds
// query paths by splitting on dots (3+ parts including root), which does not
// match the 1-part tree node and causes silent data loss during compaction.
//
// This test writes a normal variant segment via the writer, then *mutates*
// the resulting footer to turn a subcolumn's `column_path_info` into the
// legacy 1-part form, then calls `VariantColumnReader::init()` and verifies
// that the normalization inside init() rebuilds a multi-level tree that can
// be queried via both `get_subcolumn_meta_by_path` and prefix-path lookup.
TEST_F(VariantColumnWriterReaderTest, test_legacy_flat_dot_key_reader_init) {
    // 1. create tablet_schema with a variant column that has nested subcolumns
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1", /*max_subcolumns=*/10);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    // 2. create tablet
    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    _tablet_schema->set_storage_format(TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2);
    tablet_meta->_tablet_id = 20000;
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    EXPECT_TRUE(_tablet->init().ok());
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    // 3. create file_writer
    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    EXPECT_TRUE(st.ok()) << st.msg();

    // 4. create column_writer
    SegmentFooterPB footer;
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    opts.rowset_ctx = &rowset_ctx;
    opts.rowset_ctx->tablet_schema = _tablet_schema;
    TabletColumn column = _tablet_schema->column(0);
    opts.compression_type = CompressionTypePB::LZ4;
    opts.storage_format = TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2;
    _init_column_meta(opts.meta, 0, column, opts);

    std::unique_ptr<ColumnWriter> writer;
    EXPECT_TRUE(ColumnWriter::create(opts, &column, file_writer.get(), &writer).ok());
    EXPECT_TRUE(writer->init().ok());

    // 5. write nested json so the writer naturally creates a subcolumn "a.b"
    // with a 2-part path ["a", "b"].
    std::vector<std::string> jsons;
    const int kNumRows = 8;
    for (int i = 0; i < kNumRows; ++i) {
        jsons.push_back(R"({"a": {"b": "v)" + std::to_string(i) + R"("}})");
    }
    EXPECT_TRUE(append_json_batch(writer.get(), jsons).ok());
    EXPECT_TRUE(writer->finish().ok());
    EXPECT_TRUE(writer->write_data().ok());
    EXPECT_TRUE(writer->write_ordinal_index().ok());
    EXPECT_TRUE(writer->write_zone_map().ok());
    EXPECT_TRUE(file_writer->close().ok());
    footer.set_num_rows(kNumRows);

    // 6. Locate the "V1.a.b" subcolumn in the footer and mutate its
    // column_path_info into the legacy 1-part form: pb.path = "V1.a.b" but
    // path_part_infos = [{"V1"}, {"a.b"}]. This is exactly what cloud-4.1.2
    // wrote for JSON key {"a.b": ...}.
    int target_idx = -1;
    for (int i = 1; i < footer.columns_size(); ++i) {
        const auto& col_meta = footer.columns(i);
        if (!col_meta.has_column_path_info()) {
            continue;
        }
        if (col_meta.column_path_info().path() == "v1.a.b") {
            target_idx = i;
            break;
        }
    }
    ASSERT_GT(target_idx, 0) << "failed to locate subcolumn V1.a.b in footer";

    auto* target_path_info = footer.mutable_columns(target_idx)->mutable_column_path_info();
    target_path_info->clear_path_part_infos();
    auto* root_part = target_path_info->add_path_part_infos();
    root_part->set_key("v1");
    root_part->set_is_nested(false);
    root_part->set_anonymous_array_level(0);
    auto* legacy_part = target_path_info->add_path_part_infos();
    legacy_part->set_key("a.b"); // single legacy part containing a dot
    legacy_part->set_is_nested(false);
    legacy_part->set_anonymous_array_level(0);
    target_path_info->set_has_nested(false);

    // 7. Now initialize a fresh VariantColumnReader with the mutated footer.
    // The init() path calls _subcolumns_meta_info->add() for each subcolumn;
    // our fix normalizes the legacy 1-part relative path "a.b" into a
    // 2-part path ["a", "b"] so the tree has root -> "a" -> "b".
    io::FileReaderSPtr file_reader;
    st = io::global_local_filesystem()->open_file(file_path, &file_reader);
    ASSERT_TRUE(st.ok()) << st.msg();

    std::shared_ptr<segment_v2::ColumnReader> column_reader;
    st = create_variant_root_reader(footer, file_reader, _tablet_schema, &column_reader);
    ASSERT_TRUE(st.ok()) << st.msg();
    auto* variant_reader = assert_cast<segment_v2::VariantColumnReader*>(column_reader.get());
    ASSERT_NE(variant_reader, nullptr);

    // 8. Verify that queries against the normalized tree succeed.
    //    - Leaf lookup "a.b" (PathInData splits into 2 parts) should hit.
    //    - Intermediate lookup "a" should return the TUPLE parent, which
    //      has exactly one child "b".
    const auto* leaf_node = variant_reader->get_subcolumn_meta_by_path(PathInData("a.b"));
    ASSERT_NE(leaf_node, nullptr)
            << "normalized tree should be able to find leaf 'a.b' via multi-part query";
    EXPECT_TRUE(leaf_node->is_scalar());
    EXPECT_GE(leaf_node->data.footer_ordinal, 0);

    const auto* subtree = variant_reader->get_subcolumns_meta_info();
    ASSERT_NE(subtree, nullptr);
    const auto* intermediate = subtree->find_exact(PathInData("a"));
    ASSERT_NE(intermediate, nullptr)
            << "normalized tree should expose intermediate node 'a' as a TUPLE";
    EXPECT_FALSE(intermediate->is_scalar());
    EXPECT_EQ(intermediate->children.size(), 1U);
}

TEST_F(VariantColumnWriterReaderTest, test_statics) {
    // VariantStatisticsPB stats_pb;
    // auto* subcolumns_stats = stats_pb.mutable_sparse_column_non_null_size();
    // (*subcolumns_stats)["key0"] = 500;  // 50% of rows have key0
    // (*subcolumns_stats)["key1"] = 500;  // 50% of rows have key1
    // (*subcolumns_stats)["key2"] = 333;  // 33.3% of rows have key2
    // (*subcolumns_stats)["key3"] = 200;  // 20% of rows have key3
    // (*subcolumns_stats)["key4"] = 1000; // 100% of rows have key4

    // auto* sparse_stats = stats_pb.mutable_sparse_column_non_null_size();
    // (*sparse_stats)["key5"] = 100;
    // (*sparse_stats)["key6"] = 200;
    // (*sparse_stats)["key7"] = 300;

    // // 6.2 Test from_pb
    // segment_v2::VariantStatistics stats;
    // stats.from_pb(stats_pb);

    // // 6.3 Verify statistics
    // EXPECT_EQ(stats.sparse_column_non_null_size["key0"], 500);
    // EXPECT_EQ(stats.sparse_column_non_null_size["key1"], 500);
    // EXPECT_EQ(stats.sparse_column_non_null_size["key2"], 333);
    // EXPECT_EQ(stats.sparse_column_non_null_size["key3"], 200);
    // EXPECT_EQ(stats.sparse_column_non_null_size["key4"], 1000);

    // EXPECT_EQ(stats.sparse_column_non_null_size["key5"], 100);
    // EXPECT_EQ(stats.sparse_column_non_null_size["key6"], 200);
    // EXPECT_EQ(stats.sparse_column_non_null_size["key7"], 300);
}

TEST_F(VariantColumnWriterReaderTest, test_empty_v2_root_writer_finalize) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1", 3);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    const std::string file_path = _absolute_dir + "/empty_root.dat";
    io::FileWriterPtr file_writer;
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    ASSERT_TRUE(st.ok()) << st.to_string();

    SegmentFooterPB footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    rowset_ctx.tablet_schema = _tablet_schema;

    const TabletColumn& root = _tablet_schema->column(0);
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    opts.rowset_ctx = &rowset_ctx;
    opts.storage_format = TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2;
    _init_column_meta(opts.meta, 0, root, opts);

    std::unique_ptr<ColumnWriter> writer;
    ASSERT_TRUE(ColumnWriter::create(opts, &root, file_writer.get(), &writer).ok());
    ASSERT_TRUE(writer->init().ok());
    // SegmentFlusher returns before segment finalization when row_num == 0. This gate exercises
    // the internal writer boundary only; a zero-row segment never writes an ordinal index.
    ASSERT_TRUE(writer->finish().ok());
    ASSERT_TRUE(writer->write_data().ok());
    ASSERT_TRUE(file_writer->close().ok());
    EXPECT_EQ(footer.columns(0).num_rows(), 0);
}

TEST_F(VariantColumnWriterReaderTest, test_empty_v2_subcolumn_writer_finalize) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1", 3);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    const TabletColumn& root = _tablet_schema->column(0);
    TabletColumn extracted;
    extracted.set_name(root.name_lower_case() + ".missing");
    extracted.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);
    extracted.set_parent_unique_id(root.unique_id());
    extracted.set_path_info(PathInData(extracted.name()));
    extracted.set_is_nullable(true);
    _tablet_schema->append_column(extracted);

    const std::string file_path = _absolute_dir + "/empty_subcolumn.dat";
    io::FileWriterPtr file_writer;
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    ASSERT_TRUE(st.ok()) << st.to_string();

    SegmentFooterPB footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_COMPACTION;
    rowset_ctx.tablet_schema = _tablet_schema;

    const TabletColumn& subcolumn = _tablet_schema->column(1);
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    opts.rowset_ctx = &rowset_ctx;
    opts.storage_format = TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2;
    _init_column_meta(opts.meta, 0, subcolumn, opts);

    std::unique_ptr<ColumnWriter> writer;
    ASSERT_TRUE(ColumnWriter::create(opts, &subcolumn, file_writer.get(), &writer).ok());
    ASSERT_TRUE(writer->init().ok());
    // See the root-writer zero-row gate above for the SegmentFlusher boundary.
    ASSERT_TRUE(writer->finish().ok());
    ASSERT_TRUE(writer->write_data().ok());
    ASSERT_TRUE(file_writer->close().ok());
    EXPECT_EQ(footer.columns(0).num_rows(), 0);
}

TEST_F(VariantColumnWriterReaderTest, test_compaction_subcolumn_preserves_typed_float_path) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1", 1);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    ColumnPB typed_path_pb;
    typed_path_pb.set_unique_id(-1);
    typed_path_pb.set_name("float_1");
    typed_path_pb.set_type("FLOAT");
    typed_path_pb.set_is_nullable(true);
    typed_path_pb.set_pattern_type(PatternTypePB::MATCH_NAME);
    TabletColumn typed_path;
    typed_path.init_from_pb(typed_path_pb);
    _tablet_schema->mutable_column_by_uid(1).add_sub_column(typed_path);

    const TabletColumn& root = _tablet_schema->column(0);
    TabletColumn extracted;
    extracted.set_name(root.name_lower_case() + ".float_1");
    extracted.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);
    extracted.set_parent_unique_id(root.unique_id());
    extracted.set_path_info(PathInData(extracted.name(), true));
    extracted.set_is_nullable(true);
    _tablet_schema->append_column(extracted);

    const std::string file_path = _absolute_dir + "/typed_float_subcolumn.dat";
    io::FileWriterPtr file_writer;
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    ASSERT_TRUE(st.ok()) << st.to_string();

    SegmentFooterPB footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_COMPACTION;
    rowset_ctx.tablet_schema = _tablet_schema;

    const TabletColumn& subcolumn = _tablet_schema->column(1);
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    opts.rowset_ctx = &rowset_ctx;
    opts.storage_format = TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2;
    _init_column_meta(opts.meta, 0, subcolumn, opts);

    std::unique_ptr<ColumnWriter> writer;
    ASSERT_TRUE(ColumnWriter::create(opts, &subcolumn, file_writer.get(), &writer).ok());
    ASSERT_TRUE(writer->init().ok());

    auto floats = ColumnFloat32::create();
    floats->insert_value(8.12F);
    auto nulls = ColumnUInt8::create();
    nulls->insert_value(0);
    auto typed_values = ColumnVariantV2::create_typed(
            ColumnNullable::create(std::move(floats), std::move(nulls)),
            std::make_shared<DataTypeFloat32>());
    VariantColumnData variant_data(typed_values.get(), 0);
    const uint8_t* data = reinterpret_cast<const uint8_t*>(&variant_data);
    ASSERT_TRUE(writer->append_data(&data, 1).ok());
    ASSERT_TRUE(writer->finish().ok());
    ASSERT_TRUE(writer->write_data().ok());
    ASSERT_TRUE(file_writer->close().ok());

    ASSERT_EQ(footer.columns_size(), 1);
    EXPECT_EQ(footer.columns(0).type(), static_cast<int>(FieldType::OLAP_FIELD_TYPE_FLOAT));
    EXPECT_EQ(footer.columns(0).num_rows(), 1);
    EXPECT_EQ(assert_cast<VariantSubcolumnWriter*>(writer.get())->get_non_null_size(), 1);
}

TEST_F(VariantColumnWriterReaderTest, test_write_data_normal) {
    // 1. create tablet_schema
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    int variant_sparse_hash_shard_count = rand() % 10 + 1;
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1", 3, false, false,
                     variant_sparse_hash_shard_count);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    // 2. create tablet
    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    bool external_segment_meta_used_default = rand() % 2 == 0;
    std::cout << "external_segment_meta_used_default: " << external_segment_meta_used_default
              << std::endl;
    _tablet_schema->set_storage_format(external_segment_meta_used_default
                                               ? TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V3
                                               : TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2);
    tablet_meta->_tablet_id = 10000;
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());

    EXPECT_TRUE(_tablet->init().ok());
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    // 3. create file_writer
    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    EXPECT_TRUE(st.ok()) << st.msg();

    // 4. create column_writer
    SegmentFooterPB footer;
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    opts.rowset_ctx = &rowset_ctx;
    opts.rowset_ctx->tablet_schema = _tablet_schema;
    TabletColumn column = _tablet_schema->column(0);
    opts.compression_type = CompressionTypePB::LZ4;
    opts.storage_format = TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2;
    _init_column_meta(opts.meta, 0, column, opts);

    std::unique_ptr<ColumnWriter> writer;
    EXPECT_TRUE(ColumnWriter::create(opts, &column, file_writer.get(), &writer).ok());
    EXPECT_TRUE(writer->init().ok());
    EXPECT_TRUE(assert_cast<VariantColumnWriter*>(writer.get()) != nullptr);

    // 5. write data
    auto olap_data_convertor = std::make_unique<OlapBlockDataConvertor>();
    auto block = _tablet_schema->create_block();
    std::unordered_map<int, std::string> inserted_jsonstr;
    auto json_column = ColumnString::create();
    auto path_with_size =
            VariantUtil::fill_string_column_with_test_data(json_column, 1000, &inserted_jsonstr);
    block.replace_by_position(0, encode_json_column_v2(*json_column));
    olap_data_convertor->add_column_data_convertor(column);
    olap_data_convertor->set_source_content(&block, 0, 1000);
    auto [result, accessor] = olap_data_convertor->convert_column_data(0);
    EXPECT_TRUE(result.ok());
    EXPECT_TRUE(accessor != nullptr);
    EXPECT_TRUE(writer->append(accessor->get_nullmap(), accessor->get_data(), 1000).ok());
    st = writer->finish();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_data();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_ordinal_index();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_zone_map();
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(file_writer->close().ok());
    footer.set_num_rows(1000);

    // 6. check footer
    int expected_sparse_cols =
            variant_sparse_hash_shard_count > 1 ? variant_sparse_hash_shard_count : 1;
    EXPECT_EQ(footer.columns_size(), 1 + 3 + expected_sparse_cols);
    auto column_meta = footer.columns(0);
    EXPECT_EQ(column_meta.type(), (int)FieldType::OLAP_FIELD_TYPE_VARIANT);

    for (int i = 1; i < footer.columns_size() - 1; ++i) {
        auto column_met = footer.columns(i);
        check_column_meta(column_met, path_with_size);
    }
    check_sparse_column_meta(footer.columns(footer.columns_size() - 1), path_with_size);

    // 7. check variant reader
    io::FileReaderSPtr file_reader;
    st = io::global_local_filesystem()->open_file(file_path, &file_reader);
    EXPECT_TRUE(st.ok()) << st.msg();
    std::shared_ptr<ColumnReader> column_reader;
    // create root variant reader using ColumnMetaAccessor (supports inline/external meta)
    st = create_variant_root_reader(footer, file_reader, _tablet_schema, &column_reader);
    EXPECT_TRUE(st.ok()) << st.msg();

    MockColumnReaderCache column_reader_cache(footer, file_reader, _tablet_schema);

    auto variant_column_reader = assert_cast<VariantColumnReader*>(column_reader.get());
    EXPECT_TRUE(variant_column_reader != nullptr);

    auto subcolumn_meta = variant_column_reader->get_subcolumn_meta_by_path(PathInData("key0"));
    EXPECT_TRUE(subcolumn_meta != nullptr);
    subcolumn_meta = variant_column_reader->get_subcolumn_meta_by_path(PathInData("key1"));
    EXPECT_TRUE(subcolumn_meta != nullptr);
    subcolumn_meta = variant_column_reader->get_subcolumn_meta_by_path(PathInData("key2"));
    EXPECT_TRUE(subcolumn_meta != nullptr);
    EXPECT_TRUE(variant_column_reader->exist_in_sparse_column(PathInData("key3")));
    EXPECT_TRUE(variant_column_reader->exist_in_sparse_column(PathInData("key4")));
    EXPECT_TRUE(variant_column_reader->exist_in_sparse_column(PathInData("key5")));
    EXPECT_TRUE(variant_column_reader->exist_in_sparse_column(PathInData("key6")));
    EXPECT_TRUE(variant_column_reader->exist_in_sparse_column(PathInData("key7")));
    EXPECT_TRUE(variant_column_reader->exist_in_sparse_column(PathInData("key8")));
    EXPECT_TRUE(variant_column_reader->exist_in_sparse_column(PathInData("key9")));
    auto size = variant_column_reader->get_metadata_size();
    EXPECT_GT(size, 0);

    // 8. check statistics
    auto statistics = variant_column_reader->get_stats();
    for (const auto& [path, siz] : statistics->subcolumns_non_null_size) {
        EXPECT_EQ(path_with_size[path], siz);
    }
    for (const auto& [path, siz] : statistics->sparse_column_non_null_size) {
        EXPECT_EQ(path_with_size[path], siz);
    }

    // 9. check hier reader
    ColumnIteratorUPtr it;
    TabletColumn parent_column = _tablet_schema->column(0);
    StorageReadOptions storage_read_opts;
    OlapReaderStatistics stats;
    storage_read_opts.io_ctx.reader_type = ReaderType::READER_QUERY;
    storage_read_opts.stats = &stats;
    st = variant_column_reader->new_iterator(&it, &parent_column, &storage_read_opts,
                                             &column_reader_cache);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(assert_cast<HierarchicalDataIterator*>(it.get()) != nullptr);
    ColumnIteratorOptions column_iter_opts;
    column_iter_opts.stats = &stats;
    column_iter_opts.file_reader = file_reader.get();
    st = it->init(column_iter_opts);
    EXPECT_TRUE(st.ok()) << st.msg();

    MutableColumnPtr new_column_object = ColumnVariantV2::create();
    size_t nrows = 1000;
    st = it->seek_to_ordinal(0);
    EXPECT_TRUE(st.ok()) << st.msg();
    st = it->next_batch(&nrows, new_column_object);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(stats.bytes_read > 0);

    // seek_to_first for HierarchicalDataIterator no need to implement
    {
        auto iter = assert_cast<HierarchicalDataIterator*>(it.get());
        std::shared_ptr<ColumnReader> column_reader1;
        st = create_variant_root_reader(footer, file_reader, _tablet_schema, &column_reader1);
        EXPECT_TRUE(st.ok()) << st.msg();
        std::cout << "hier:" << iter->get_current_ordinal() << std::endl;
        //  now we can find exist
        auto exist_node = std::make_unique<SubcolumnColumnMetaInfo::Node>(
                SubcolumnColumnMetaInfo::Node::Kind::SCALAR);
        exist_node->path = PathInData("key0");
        OlapReaderStatistics stats;
        Status sts = iter->add_stream(0, exist_node.get(), &column_reader_cache, &stats);
        EXPECT_TRUE(sts.ok());
        auto jsonb_type = std::make_shared<DataTypeJsonb>();
        // if node path is emtpy we will meet error
        auto variant_column_reader1 = assert_cast<VariantColumnReader*>(column_reader1.get());
        EXPECT_TRUE(variant_column_reader1 != nullptr);
        auto r = variant_column_reader1->get_subcolumns_meta_info()->get_leaves()[1];
        r->path = PathInData("");
        // if we clear the parts manually, we will meet error, but it can be handled, and should not happen
        r->path.parts.clear();
        sts = iter->add_stream(0, r.get(), &column_reader_cache, &stats);
        EXPECT_FALSE(sts.ok());
    }

    for (int i = 0; i < 1000; ++i) {
        EXPECT_EQ(variant_json_at(*new_column_object, i), inserted_jsonstr[i]);
    }

    std::vector<rowid_t> row_ids;
    for (int i = 0; i < 1000; ++i) {
        if (i % 7 == 0) {
            row_ids.push_back(i);
        }
    }
    new_column_object = ColumnVariantV2::create();
    st = it->read_by_rowids(row_ids.data(), row_ids.size(), new_column_object);
    EXPECT_TRUE(st.ok()) << st.msg();
    for (int i = 0; i < row_ids.size(); ++i) {
        EXPECT_EQ(variant_json_at(*new_column_object, i), inserted_jsonstr[row_ids[i]]);
    }

    auto read_to_column_object = [&](ColumnIteratorUPtr& it) {
        new_column_object =
                ColumnNullable::create(ColumnVariantV2::create(), ColumnUInt8::create());
        nrows = 1000;
        st = it->seek_to_ordinal(0);
        EXPECT_TRUE(st.ok()) << st.msg();
        st = it->next_batch(&nrows, new_column_object);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_TRUE(stats.bytes_read > 0);
        EXPECT_EQ(nrows, 1000);
    };

    // 10. check sparse extract reader
    PathToBinaryColumnCacheUPtr sparse_column_cache =
            std::make_unique<std::unordered_map<std::string, BinaryColumnCacheSPtr>>();
    stats.bytes_read = 0;
    for (int i = 3; i < 10; ++i) {
        std::string key = ".key" + std::to_string(i);
        const std::string json_key_needle = "\"" + key.substr(1) + "\":";
        TabletColumn subcolumn_in_sparse;
        subcolumn_in_sparse.set_name(parent_column.name_lower_case() + key);
        subcolumn_in_sparse.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);
        subcolumn_in_sparse.set_parent_unique_id(parent_column.unique_id());
        subcolumn_in_sparse.set_path_info(PathInData(parent_column.name_lower_case() + key));
        subcolumn_in_sparse.set_variant_max_subcolumns_count(
                parent_column.variant_max_subcolumns_count());
        subcolumn_in_sparse.set_is_nullable(true);

        ColumnIteratorUPtr it;
        st = variant_column_reader->new_iterator(&it, &subcolumn_in_sparse, &storage_read_opts,
                                                 &column_reader_cache, sparse_column_cache.get());
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_TRUE(assert_cast<BinaryColumnExtractIterator*>(it.get()) != nullptr);
        st = it->init(column_iter_opts);
        EXPECT_TRUE(st.ok()) << st.msg();

        if (i == 3) {
            MutableColumnPtr non_nullable_dst = ColumnVariantV2::create();
            size_t raw_rows = 1000;
            ASSERT_TRUE(it->seek_to_ordinal(0).ok());
            const Status raw_status = it->next_batch(&raw_rows, non_nullable_dst);
            EXPECT_TRUE(raw_status.is<ErrorCode::CORRUPTION>()) << raw_status;
            EXPECT_NE(raw_status.msg().find(
                              "Variant storage returned SQL NULL for a non-nullable destination"),
                      std::string::npos);
            EXPECT_EQ(non_nullable_dst->size(), 0);
        }

        int64_t before_bytes_read = stats.bytes_read;
        read_to_column_object(it);
        // In bucketized mode, different keys may map to different buckets and trigger extra IO.
        if (variant_sparse_hash_shard_count <= 1 && before_bytes_read != 0) {
            EXPECT_EQ(stats.bytes_read, before_bytes_read);
        }

        for (int row = 0; row < 1000; ++row) {
            const std::string value = variant_json_at(*new_column_object, row);
            if (inserted_jsonstr[row].find(json_key_needle) != std::string::npos) {
                if (i % 2 == 0) {
                    EXPECT_EQ(value, "88");
                } else {
                    EXPECT_EQ(value, "\"str99\"");
                }
            } else {
                EXPECT_EQ(value, "NULL");
            }
        }
    }

    // 11. check leaf reader
    auto check_leaf_reader = [&]() {
        for (int i = 0; i < 3; ++i) {
            std::string key = ".key" + std::to_string(i);
            TabletColumn subcolumn;
            subcolumn.set_name(parent_column.name_lower_case() + key);
            subcolumn.set_type((FieldType)(int)footer.columns(i + 1).type());
            subcolumn.set_parent_unique_id(parent_column.unique_id());
            subcolumn.set_path_info(PathInData(parent_column.name_lower_case() + key));
            subcolumn.set_variant_max_subcolumns_count(
                    parent_column.variant_max_subcolumns_count());
            subcolumn.set_is_nullable(true);

            ColumnIteratorUPtr it;
            st = variant_column_reader->new_iterator(&it, &subcolumn, &storage_read_opts,
                                                     &column_reader_cache);
            EXPECT_TRUE(st.ok()) << st.msg();
            std::cout << "key " << key << std::endl;
            EXPECT_TRUE(dynamic_cast<FileColumnIterator*>(it.get()) != nullptr);
            st = it->init(column_iter_opts);
            EXPECT_TRUE(st.ok()) << st.msg();

            auto column_type = DataTypeFactory::instance().create_data_type(subcolumn, false);
            auto read_column = column_type->create_column();
            nrows = 1000;
            st = it->seek_to_ordinal(0);
            EXPECT_TRUE(st.ok()) << st.msg();
            st = it->next_batch(&nrows, read_column);
            EXPECT_TRUE(st.ok()) << st.msg();
            EXPECT_TRUE(stats.bytes_read > 0);

            for (int row = 0; row < 1000; ++row) {
                const std::string& value = column_type->to_string(*read_column, row);
                if (inserted_jsonstr[row].find(key) != std::string::npos) {
                    if (i % 2 == 0) {
                        EXPECT_EQ(value, "88");
                    } else {
                        EXPECT_EQ(value, "str99");
                    }
                }
            }
        }
    };
    check_leaf_reader();

    // 12. check empty
    TabletColumn subcolumn;
    subcolumn.set_name(parent_column.name_lower_case() + ".key10");
    subcolumn.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);
    subcolumn.set_parent_unique_id(parent_column.unique_id());
    subcolumn.set_path_info(PathInData(parent_column.name_lower_case() + ".key10"));
    subcolumn.set_is_nullable(true);
    ColumnIteratorUPtr it1;
    st = variant_column_reader->new_iterator(&it1, &subcolumn, &storage_read_opts,
                                             &column_reader_cache);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(assert_cast<DefaultValueColumnIterator*>(it1.get()) != nullptr);

    // 13. check statistics size == limit
    auto& variant_stats = variant_column_reader->_statistics;
    EXPECT_TRUE(variant_stats->sparse_column_non_null_size.size() <
                variant_column_reader->_variant_sparse_column_statistics_size);
    auto limit = variant_column_reader->_variant_sparse_column_statistics_size -
                 variant_stats->sparse_column_non_null_size.size();
    for (int i = 0; i < limit; ++i) {
        std::string key = parent_column.name_lower_case() + ".key10" + std::to_string(i);
        variant_stats->sparse_column_non_null_size[key] = 10000;
    }
    EXPECT_TRUE(variant_stats->sparse_column_non_null_size.size() ==
                variant_column_reader->_variant_sparse_column_statistics_size);
    EXPECT_TRUE(variant_column_reader->is_exceeded_sparse_column_limit());

    ColumnIteratorUPtr it2;
    st = variant_column_reader->new_iterator(&it2, &subcolumn, &storage_read_opts,
                                             &column_reader_cache);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(assert_cast<HierarchicalDataIterator*>(it2.get()) != nullptr);
    st = it2->init(column_iter_opts);
    EXPECT_TRUE(st.ok()) << st.msg();

    auto check_empty_column = [&]() {
        for (int row = 0; row < 1000; ++row) {
            EXPECT_EQ(variant_json_at(*new_column_object, row), "NULL");
        }
    };

    read_to_column_object(it2);
    check_empty_column();

    // construct tablet schema for compaction
    storage_read_opts.io_ctx.reader_type = ReaderType::READER_BASE_COMPACTION;
    storage_read_opts.tablet_schema = _tablet_schema;
    std::unordered_map<int32_t, TabletSchema::PathsSetInfo> uid_to_paths_set_info;
    TabletSchema::PathsSetInfo paths_set_info;
    paths_set_info.sub_path_set.insert("key0");
    paths_set_info.sub_path_set.insert("key3");
    paths_set_info.sub_path_set.insert("key4");
    paths_set_info.sparse_path_set.insert("key1");
    paths_set_info.sparse_path_set.insert("key2");
    paths_set_info.sparse_path_set.insert("key5");
    paths_set_info.sparse_path_set.insert("key6");
    paths_set_info.sparse_path_set.insert("key7");
    paths_set_info.sparse_path_set.insert("key8");
    paths_set_info.sparse_path_set.insert("key9");
    uid_to_paths_set_info[parent_column.unique_id()] = paths_set_info;
    _tablet_schema->set_path_set_info(std::move(uid_to_paths_set_info));

    // mock a subcolumn in compaction
    TabletColumn subcolumn_in_compaction;
    subcolumn_in_compaction.set_name(parent_column.name_lower_case() + ".key10");
    subcolumn_in_compaction.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);
    subcolumn_in_compaction.set_parent_unique_id(parent_column.unique_id());
    subcolumn_in_compaction.set_path_info(PathInData(parent_column.name_lower_case() + ".key10"));
    subcolumn_in_compaction.set_is_nullable(true);
    _tablet_schema->append_column(subcolumn_in_compaction);

    // 14. check compaction subcolumn reader
    check_leaf_reader();
    // 15. check compaction root reader
    ColumnIteratorUPtr it3;
    st = variant_column_reader->new_iterator(&it3, &parent_column, &storage_read_opts,
                                             &column_reader_cache);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(assert_cast<VariantRootColumnIterator*>(it3.get()) != nullptr);
    st = it3->init(column_iter_opts);
    EXPECT_TRUE(st.ok()) << st.msg();
    // test VariantRootColumnIterator for next_batch and read_by_rowids
    {
        auto iter = assert_cast<VariantRootColumnIterator*>(it3.get());
        MutableColumnPtr root_column_object =
                ColumnNullable::create(ColumnVariantV2::create(), ColumnUInt8::create());
        nrows = 1000;
        st = iter->seek_to_ordinal(0);
        EXPECT_TRUE(st.ok()) << st.msg();
        st = iter->next_batch(&nrows, root_column_object);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_TRUE(stats.bytes_read > 0);

        std::vector<rowid_t> row_ids1 = {0, 10, 100};
        root_column_object->clear();
        st = iter->read_by_rowids(row_ids1.data(), row_ids1.size(), root_column_object);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_TRUE(root_column_object->size() == row_ids1.size());
        auto row_id = iter->get_current_ordinal();
        std::cout << "current row id: " << row_id << std::endl;
    }

    // 16. check compacton sparse column
    TabletColumn sparse_column =
            variant_sparse_hash_shard_count > 1
                    ? variant_util::create_sparse_shard_column(parent_column, 0)
                    : variant_util::create_sparse_column(parent_column);
    ColumnIteratorUPtr it4;
    st = variant_column_reader->new_iterator(&it4, &sparse_column, &storage_read_opts,
                                             &column_reader_cache);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(assert_cast<SparseColumnMergeIterator*>(it4.get()) != nullptr);
    st = it4->init(column_iter_opts);
    EXPECT_TRUE(st.ok()) << st.msg();
    auto column_type = DataTypeFactory::instance().create_data_type(sparse_column, false);
    auto read_column = column_type->create_column();
    nrows = 1000;
    st = it4->seek_to_ordinal(0);
    EXPECT_TRUE(st.ok()) << st.msg();
    st = it4->next_batch(&nrows, read_column);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(stats.bytes_read > 0);

    {
        // test SparseColumnMergeIterator seek_to_first
        auto iter = assert_cast<SparseColumnMergeIterator*>(it4.get());
        EXPECT_ANY_THROW(iter->get_current_ordinal());
        // and test read_by_rowids for 0 -> 1000
        std::vector<rowid_t> row_ids1;
        for (int i = 0; i < 1000; ++i) {
            row_ids1.push_back(i);
        }
        auto column_type1 = DataTypeFactory::instance().create_data_type(sparse_column, false);
        auto read_column1 = column_type1->create_column();
        st = iter->read_by_rowids(row_ids1.data(), row_ids1.size(), read_column1);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_TRUE(read_column1->size() == row_ids1.size());
        // test _process_data_without_sparse_column
        std::cout << "_iter._src_subcolumn_map size : " << iter->_src_subcolumns_for_sparse.size()
                  << std::endl;
        std::cout << "_iter.root  " << iter->_src_subcolumns_for_sparse.empty() << std::endl;
        // fill with dst SparseMap
        MutableColumnPtr sparse_dst =
                ColumnMap::create(ColumnString::create(), ColumnString::create(),
                                  ColumnArray::ColumnOffsets::create());
        iter->_process_data_without_sparse_column(sparse_dst, 1);
        EXPECT_TRUE(sparse_dst->size() == 1);
    }
    //
    //    {
    //        // read with opt
    //        auto iter = assert_cast<SparseColumnMergeIterator*>(it4);
    //        StorageReadOptions storage_read_opts1;
    //        storage_read_opts1.io_ctx.reader_type = ReaderType::READER_QUERY;
    //        iter->_read_opts = &storage_read_opts1;
    //        auto read_column1 = column_type->create_column();
    //        st = iter->next_batch(&nrows, read_column1, nullptr);
    //        EXPECT_TRUE(st.ok()) << st.msg();
    //        EXPECT_TRUE(stats.bytes_read > 0);
    //        iter->_read_opts->io_ctx.reader_type = ReaderType::READER_BASE_COMPACTION;
    //        st = iter->next_batch(&nrows, read_column1, nullptr);
    //        EXPECT_TRUE(st.ok()) << st.msg();
    //    }

    for (int row = 0; row < 1000; ++row) {
        const std::string& value = column_type->to_string(*read_column, row);
        EXPECT_TRUE(value.find("key0") == std::string::npos)
                << "row: " << row << ", value: " << value;
        EXPECT_TRUE(value.find("key3") == std::string::npos)
                << "row: " << row << ", value: " << value;
        EXPECT_TRUE(value.find("key4") == std::string::npos)
                << "row: " << row << ", value: " << value;
    }

    // 17. check limit = 10000
    subcolumn.set_name(parent_column.name_lower_case() + ".key10");
    subcolumn.set_path_info(PathInData(parent_column.name_lower_case() + ".key10"));
    ColumnIteratorUPtr it5;
    st = variant_column_reader->new_iterator(&it5, &subcolumn, &storage_read_opts,
                                             &column_reader_cache);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(assert_cast<BinaryColumnExtractIterator*>(it5.get()) != nullptr);
    EXPECT_TRUE(it5->init(column_iter_opts).ok());

    {
        // test BinaryColumnExtractIterator seek_to_first
        auto iter = assert_cast<BinaryColumnExtractIterator*>(it5.get());
        EXPECT_TRUE(st.ok()) << st.msg();
        // and test read_by_rowids
        std::vector<rowid_t> row_ids1;
        for (int i = 0; i < 1000; ++i) {
            row_ids1.push_back(i);
        }
        MutableColumnPtr sparse_dst1 = ColumnVariantV2::create();
        const Status raw_rowids_status =
                iter->read_by_rowids(row_ids1.data(), row_ids1.size(), sparse_dst1);
        EXPECT_TRUE(raw_rowids_status.is<ErrorCode::CORRUPTION>()) << raw_rowids_status;
        EXPECT_NE(raw_rowids_status.msg().find(
                          "Variant storage returned SQL NULL for a non-nullable destination"),
                  std::string::npos);
        EXPECT_EQ(sparse_dst1->size(), 0);
        // test to nullable column object
        std::cout << "test 2 " << std::endl;
        MutableColumnPtr sparse_dst2 =
                ColumnNullable::create(ColumnVariantV2::create(), ColumnUInt8::create());
        st = iter->read_by_rowids(row_ids1.data(), row_ids1.size(), sparse_dst2);
        EXPECT_TRUE(st.ok()) << st.msg();
        ASSERT_EQ(sparse_dst2->size(), row_ids1.size());
        for (size_t row = 0; row < sparse_dst2->size(); ++row) {
            EXPECT_EQ(variant_json_at(*sparse_dst2, row), "NULL");
        }
        std::cout << "test 3" << std::endl;
        MutableColumnPtr sparse_dst3 = ColumnVariantV2::create();
        size_t rs = 1000;
        bool has_null = false;
        st = iter->seek_to_ordinal(0);
        EXPECT_TRUE(st.ok()) << st.msg();
        const Status raw_batch_status = iter->next_batch(&rs, sparse_dst3, &has_null);
        EXPECT_TRUE(raw_batch_status.is<ErrorCode::CORRUPTION>()) << raw_batch_status;
        EXPECT_NE(raw_batch_status.msg().find(
                          "Variant storage returned SQL NULL for a non-nullable destination"),
                  std::string::npos);
        EXPECT_EQ(sparse_dst3->size(), 0);

        MutableColumnPtr sparse_dst4 =
                ColumnNullable::create(ColumnVariantV2::create(), ColumnUInt8::create());
        rs = 1000;
        has_null = false;
        st = iter->seek_to_ordinal(0);
        EXPECT_TRUE(st.ok()) << st.msg();
        st = iter->next_batch(&rs, sparse_dst4, &has_null);
        EXPECT_TRUE(st.ok()) << st.msg();
        ASSERT_EQ(sparse_dst4->size(), row_ids1.size());
        for (size_t row = 0; row < sparse_dst4->size(); ++row) {
            EXPECT_EQ(variant_json_at(*sparse_dst4, row), "NULL");
        }
    }

    for (int i = 0; i < limit; ++i) {
        std::string key = parent_column.name_lower_case() + ".key10" + std::to_string(i);
        variant_stats->sparse_column_non_null_size.erase(key);
    }

    // 18. check compacton sparse extract column
    ColumnIteratorUPtr it6;
    subcolumn.set_name(parent_column.name_lower_case() + ".key3");
    subcolumn.set_path_info(PathInData(parent_column.name_lower_case() + ".key3"));
    st = variant_column_reader->new_iterator(&it6, &subcolumn, &storage_read_opts,
                                             &column_reader_cache);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(assert_cast<BinaryColumnExtractIterator*>(it6.get()) != nullptr);

    // 19. check compaction default column
    subcolumn.set_name(parent_column.name_lower_case() + ".key10");
    subcolumn.set_path_info(PathInData(parent_column.name_lower_case() + ".key10"));
    ColumnIteratorUPtr it7;
    st = variant_column_reader->new_iterator(&it7, &subcolumn, &storage_read_opts,
                                             &column_reader_cache);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(assert_cast<DefaultValueColumnIterator*>(it7.get()) != nullptr);
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
}

TEST_F(VariantColumnWriterReaderTest, test_v2_write_materialized_and_sparse) {
    constexpr int kRows = 4;

    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1",
                     /*variant_max_subcolumns_count=*/2,
                     /*is_key=*/false,
                     /*is_nullable=*/false,
                     /*variant_sparse_hash_shard_count=*/0,
                     /*variant_enable_doc_mode=*/false);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    _tablet_schema->set_storage_format(TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2);
    tablet_meta->_tablet_id = 33003;
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    ASSERT_TRUE(_tablet->init().ok());
    ASSERT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    ASSERT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    ASSERT_TRUE(st.ok()) << st.msg();

    SegmentFooterPB footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    rowset_ctx.tablet_schema = _tablet_schema;

    TabletColumn parent_column = _tablet_schema->column(0);
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    opts.rowset_ctx = &rowset_ctx;
    opts.storage_format = TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2;
    _init_column_meta(opts.meta, 0, parent_column, opts);

    std::unique_ptr<ColumnWriter> writer;
    ASSERT_TRUE(ColumnWriter::create(opts, &parent_column, file_writer.get(), &writer).ok());
    ASSERT_TRUE(writer->init().ok());

    const std::vector<std::string> jsons = {
            R"({"hot":1,"warm":10,"cold0":100})",
            R"({"hot":2,"warm":20,"cold1":101})",
            R"({"hot":3,"warm":30,"cold2":102})",
            R"({"hot":4,"warm":40,"cold3":103})",
    };

    Block block = _tablet_schema->create_block();
    auto columns = std::move(block).mutate_columns();
    auto json_column = ColumnString::create();
    for (const auto& json : jsons) {
        json_column->insert_data(json.data(), json.size());
    }
    columns[0] = encode_json_column_v2(*json_column);
    block.set_columns(std::move(columns));
    ASSERT_NE(check_and_get_column<ColumnVariantV2>(*block.get_by_position(0).column), nullptr);

    auto converter = std::make_unique<OlapBlockDataConvertor>();
    converter->add_column_data_convertor(parent_column);
    converter->set_source_content(&block, 0, kRows);
    auto [convert_status, accessor] = converter->convert_column_data(0);
    ASSERT_TRUE(convert_status.ok()) << convert_status.to_string();
    ASSERT_NE(accessor, nullptr);
    ASSERT_TRUE(writer->append(accessor->get_nullmap(), accessor->get_data(), kRows).ok());

    ASSERT_TRUE(writer->finish().ok());
    ASSERT_TRUE(writer->write_data().ok());
    ASSERT_TRUE(writer->write_ordinal_index().ok());
    ASSERT_TRUE(writer->write_zone_map().ok());
    ASSERT_TRUE(file_writer->close().ok());
    footer.set_num_rows(kRows);

    EXPECT_EQ(footer.columns_size(), 4);

    io::FileReaderSPtr file_reader;
    st = io::global_local_filesystem()->open_file(file_path, &file_reader);
    ASSERT_TRUE(st.ok()) << st.msg();
    std::shared_ptr<ColumnReader> column_reader;
    st = create_variant_root_reader(footer, file_reader, _tablet_schema, &column_reader);
    ASSERT_TRUE(st.ok()) << st.msg();
    auto* variant_column_reader = assert_cast<VariantColumnReader*>(column_reader.get());
    ASSERT_NE(variant_column_reader, nullptr);

    EXPECT_NE(variant_column_reader->get_subcolumn_meta_by_path(PathInData("hot")), nullptr);
    EXPECT_NE(variant_column_reader->get_subcolumn_meta_by_path(PathInData("warm")), nullptr);
    for (int i = 0; i < kRows; ++i) {
        EXPECT_TRUE(variant_column_reader->exist_in_sparse_column(
                PathInData("cold" + std::to_string(i))));
    }

    const auto* stats = variant_column_reader->get_stats();
    ASSERT_NE(stats, nullptr);
    EXPECT_EQ(stats->subcolumns_non_null_size.at("hot"), kRows);
    EXPECT_EQ(stats->subcolumns_non_null_size.at("warm"), kRows);
    for (int i = 0; i < kRows; ++i) {
        EXPECT_EQ(stats->sparse_column_non_null_size.at("cold" + std::to_string(i)), 1);
    }

    std::vector<std::string> actual_rows;
    st = read_root_rows(footer, file_path, &actual_rows);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(actual_rows, normalize_json_rows(jsons));

    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
}

TEST_F(VariantColumnWriterReaderTest, test_v2_sparse_stats_limit_keeps_exact_missing_path_null) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1",
                     /*variant_max_subcolumns_count=*/1);
    schema_pb.mutable_column(0)->set_variant_max_sparse_column_statistics_size(2);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);
    init_tablet_from_current_schema(33022);

    const std::vector<std::string> jsons = {
            R"({"b":10})",
            R"({"b":{"c":11}})",
            R"({"obj":{"child":1}})",
            R"({"obj":{"child":2}})",
            R"({"obj":{"child":3}})",
            R"({"u":{"child":12}})",
            R"({"b":{"c":{"d":13}}})",
    };
    SegmentFooterPB footer;
    std::string file_path;
    auto st = write_v2_segment(jsons, "sparse_stats_exact_missing", &footer, &file_path);
    ASSERT_TRUE(st.ok()) << st.to_string();

    io::FileReaderSPtr file_reader;
    st = io::global_local_filesystem()->open_file(file_path, &file_reader);
    ASSERT_TRUE(st.ok()) << st.to_string();
    std::shared_ptr<ColumnReader> column_reader;
    st = create_variant_root_reader(footer, file_reader, _tablet_schema, &column_reader);
    ASSERT_TRUE(st.ok()) << st.to_string();
    auto* variant_reader = assert_cast<VariantColumnReader*>(column_reader.get());
    ASSERT_NE(variant_reader, nullptr);
    EXPECT_EQ(variant_reader->get_subcolumn_meta_by_path(PathInData("b.c")), nullptr);
    EXPECT_NE(variant_reader->get_subcolumn_meta_by_path(PathInData("obj.child")), nullptr);
    EXPECT_TRUE(variant_reader->is_exceeded_sparse_column_limit());
    EXPECT_TRUE(variant_reader->exist_in_sparse_column(PathInData("b.c")));
    EXPECT_FALSE(variant_reader->has_prefix_path(PathInData("b.c")));
    EXPECT_FALSE(variant_reader->exist_in_sparse_column(PathInData("u")));
    EXPECT_FALSE(variant_reader->has_prefix_path(PathInData("u")));
    EXPECT_TRUE(variant_reader->has_prefix_path(PathInData("obj")));

    const TabletColumn& parent_column = _tablet_schema->column(0);
    MockColumnReaderCache column_reader_cache(footer, file_reader, _tablet_schema);
    StorageReadOptions storage_read_opts;
    storage_read_opts.io_ctx.reader_type = ReaderType::READER_QUERY;
    OlapReaderStatistics stats;
    storage_read_opts.stats = &stats;
    const auto make_path_column = [&](std::string_view relative_path) {
        TabletColumn path_column;
        const std::string full_path =
                parent_column.name_lower_case() + "." + std::string(relative_path);
        path_column.set_name(full_path);
        path_column.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);
        path_column.set_parent_unique_id(parent_column.unique_id());
        path_column.set_path_info(PathInData(full_path));
        path_column.set_is_nullable(true);
        return path_column;
    };

    auto path_column = make_path_column("b.c");
    ColumnIteratorUPtr iterator;
    st = variant_reader->new_iterator(&iterator, &path_column, &storage_read_opts,
                                      &column_reader_cache);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_NE(assert_cast<HierarchicalDataIterator*>(iterator.get()), nullptr);

    path_column = make_path_column("u");
    st = variant_reader->new_iterator(&iterator, &path_column, &storage_read_opts,
                                      &column_reader_cache);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_NE(assert_cast<HierarchicalDataIterator*>(iterator.get()), nullptr);

    path_column = make_path_column("obj");
    st = variant_reader->new_iterator(&iterator, &path_column, &storage_read_opts,
                                      &column_reader_cache);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_NE(assert_cast<HierarchicalDataIterator*>(iterator.get()), nullptr);

    std::vector<std::string> path_rows;
    st = read_variant_path_rows(footer, file_path, "b.c", FieldType::OLAP_FIELD_TYPE_VARIANT,
                                &path_rows);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(path_rows, (std::vector<std::string> {"NULL", "11", "NULL", "NULL", "NULL", "NULL",
                                                    R"({"d":13})"}));

    st = read_variant_path_rows(footer, file_path, "u", FieldType::OLAP_FIELD_TYPE_VARIANT,
                                &path_rows);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(path_rows, (std::vector<std::string> {"NULL", "NULL", "NULL", "NULL", "NULL",
                                                    R"({"child":12})", "NULL"}));

    st = read_variant_path_rows(footer, file_path, "obj", FieldType::OLAP_FIELD_TYPE_VARIANT,
                                &path_rows);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(path_rows, (std::vector<std::string> {"{}", "{}", R"({"child":1})", R"({"child":2})",
                                                    R"({"child":3})", "{}", "{}"}));

    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
}

TEST_F(VariantColumnWriterReaderTest,
       test_v2_new_write_omits_json_null_subpaths_in_ordinary_and_doc) {
    const std::vector<std::string> jsons = {
            R"({"null_then_scalar":null,"scalar_then_null":1,"present_null":null,"present":1})",
            R"({"null_then_scalar":2,"scalar_then_null":null,"present":1})",
            R"({"present":9})",
    };

    struct LayoutCase {
        bool doc_mode;
        int64_t tablet_id;
        std::string_view rowset_id;
    };
    const std::array layouts = {
            LayoutCase {.doc_mode = false, .tablet_id = 33020, .rowset_id = "json_null_ordinary"},
            LayoutCase {.doc_mode = true, .tablet_id = 33021, .rowset_id = "json_null_doc"},
    };

    for (const LayoutCase& layout : layouts) {
        SCOPED_TRACE(layout.doc_mode ? "DOC" : "ordinary");
        TabletSchemaPB schema_pb;
        schema_pb.set_keys_type(KeysType::DUP_KEYS);
        construct_column(schema_pb.add_column(), 1, "VARIANT", "V1",
                         /*variant_max_subcolumns_count=*/1,
                         /*is_key=*/false,
                         /*is_nullable=*/false,
                         /*variant_sparse_hash_shard_count=*/1,
                         /*variant_enable_doc_mode=*/layout.doc_mode,
                         /*variant_doc_materialization_min_rows=*/100,
                         /*variant_doc_hash_shard_count=*/1);
        _tablet_schema = std::make_shared<TabletSchema>();
        _tablet_schema->init_from_pb(schema_pb);
        init_tablet_from_current_schema(layout.tablet_id);

        SegmentFooterPB footer;
        std::string file_path;
        auto st = write_v2_segment(jsons, layout.rowset_id, &footer, &file_path);
        ASSERT_TRUE(st.ok()) << st.to_string();

        std::vector<std::string> actual_rows;
        st = read_root_rows(footer, file_path, &actual_rows);
        ASSERT_TRUE(st.ok()) << st.to_string();
        const std::vector<std::string> expected_rows = {
                R"({"scalar_then_null":1,"present":1})",
                R"({"null_then_scalar":2,"present":1})",
                R"({"present":9})",
        };
        EXPECT_EQ(actual_rows, normalize_json_rows(expected_rows));

        const auto expect_path_rows = [&](std::string_view path,
                                          std::vector<std::string> expected) {
            std::vector<std::string> path_rows;
            const Status read_status = read_variant_path_rows(
                    footer, file_path, path, FieldType::OLAP_FIELD_TYPE_VARIANT, &path_rows);
            ASSERT_TRUE(read_status.ok()) << path << ": " << read_status.to_string();
            EXPECT_EQ(path_rows, expected) << path;
        };
        expect_path_rows("present_null", {"NULL", "NULL", "NULL"});
        expect_path_rows("null_then_scalar", {"NULL", "2", "NULL"});
        expect_path_rows("scalar_then_null", {"1", "NULL", "NULL"});

        io::FileReaderSPtr file_reader;
        st = io::global_local_filesystem()->open_file(file_path, &file_reader);
        ASSERT_TRUE(st.ok()) << st.to_string();
        std::shared_ptr<ColumnReader> column_reader;
        st = create_variant_root_reader(footer, file_reader, _tablet_schema, &column_reader);
        ASSERT_TRUE(st.ok()) << st.to_string();
        const auto* variant_reader = assert_cast<const VariantColumnReader*>(column_reader.get());
        ASSERT_NE(variant_reader, nullptr);
        const auto* stats = variant_reader->get_stats();
        ASSERT_NE(stats, nullptr);
        if (layout.doc_mode) {
            EXPECT_FALSE(stats->doc_value_column_non_null_size.contains("present_null"));
            EXPECT_EQ(stats->doc_value_column_non_null_size.at("null_then_scalar"), 1);
            EXPECT_EQ(stats->doc_value_column_non_null_size.at("scalar_then_null"), 1);
        } else {
            EXPECT_FALSE(variant_reader->exist_in_sparse_column(PathInData("present_null")));
            EXPECT_FALSE(stats->sparse_column_non_null_size.contains("present_null"));
            EXPECT_EQ(stats->sparse_column_non_null_size.at("null_then_scalar"), 1);
            EXPECT_EQ(stats->sparse_column_non_null_size.at("scalar_then_null"), 1);
        }

        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    }
}

TEST_F(VariantColumnWriterReaderTest, test_v2_empty_key_is_persisted_as_a_structured_subcolumn) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1",
                     /*variant_max_subcolumns_count=*/10,
                     /*is_key=*/false,
                     /*is_nullable=*/false,
                     /*variant_sparse_hash_shard_count=*/1,
                     /*variant_enable_doc_mode=*/false);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);
    init_tablet_from_current_schema(33022);

    const std::vector<std::string> jsons = {
            R"({"":1,"sibling":2})",
            R"({"":"text","sibling":3})",
    };
    SegmentFooterPB footer;
    std::string file_path;
    auto st = write_v2_segment(jsons, "empty_key", &footer, &file_path);
    ASSERT_TRUE(st.ok()) << st.to_string();

    io::FileReaderSPtr file_reader;
    st = io::global_local_filesystem()->open_file(file_path, &file_reader);
    ASSERT_TRUE(st.ok()) << st.to_string();
    std::shared_ptr<ColumnReader> column_reader;
    st = create_variant_root_reader(footer, file_reader, _tablet_schema, &column_reader);
    ASSERT_TRUE(st.ok()) << st.to_string();
    const auto* variant_reader = assert_cast<const VariantColumnReader*>(column_reader.get());
    ASSERT_NE(variant_reader, nullptr);

    PathInData::Parts empty_key_parts;
    empty_key_parts.emplace_back("", false, 0);
    const PathInData empty_key_path(std::move(empty_key_parts));
    ASSERT_FALSE(empty_key_path.empty());
    EXPECT_NE(variant_reader->get_subcolumn_meta_by_path(empty_key_path), nullptr);

    std::vector<std::string> actual_rows;
    st = read_root_rows(footer, file_path, &actual_rows);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(actual_rows, normalize_json_rows(jsons));

    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
}

TEST_F(VariantColumnWriterReaderTest, test_v2_empty_key_is_read_from_sparse_storage) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1",
                     /*variant_max_subcolumns_count=*/0,
                     /*is_key=*/false,
                     /*is_nullable=*/false,
                     /*variant_sparse_hash_shard_count=*/3,
                     /*variant_enable_doc_mode=*/false);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);
    init_tablet_from_current_schema(33023);

    const std::vector<std::string> jsons = {
            R"({"hot":1,"":"before"})",
            R"({"hot":2,"":42})",
            R"({"hot":3,"":"after"})",
    };
    SegmentFooterPB footer;
    std::string file_path;
    auto st = write_v2_segment(jsons, "empty_key_sparse", &footer, &file_path);
    ASSERT_TRUE(st.ok()) << st.to_string();

    io::FileReaderSPtr file_reader;
    st = io::global_local_filesystem()->open_file(file_path, &file_reader);
    ASSERT_TRUE(st.ok()) << st.to_string();
    std::shared_ptr<ColumnReader> column_reader;
    st = create_variant_root_reader(footer, file_reader, _tablet_schema, &column_reader);
    ASSERT_TRUE(st.ok()) << st.to_string();
    const auto* variant_reader = assert_cast<const VariantColumnReader*>(column_reader.get());
    ASSERT_NE(variant_reader, nullptr);
    PathInData::Parts empty_key_parts;
    empty_key_parts.emplace_back("", false, 0);
    const PathInData empty_key_path(std::move(empty_key_parts));
    ASSERT_NE(variant_reader->get_subcolumn_meta_by_path(empty_key_path), nullptr);

    std::vector<std::string> path_rows;
    st = read_variant_path_rows(footer, file_path, "", FieldType::OLAP_FIELD_TYPE_VARIANT,
                                &path_rows);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(path_rows, (std::vector<std::string> {R"("before")", "42", R"("after")"}));

    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
}

TEST_F(VariantColumnWriterReaderTest, test_v2_write_typed_path_materialized_with_storage_type) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1",
                     /*variant_max_subcolumns_count=*/1,
                     /*is_key=*/false,
                     /*is_nullable=*/false,
                     /*variant_sparse_hash_shard_count=*/0,
                     /*variant_enable_doc_mode=*/false);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    auto typed_path = make_int_typed_path_template("typed_i");
    _tablet_schema->mutable_column_by_uid(1).add_sub_column(typed_path);
    init_tablet_from_current_schema(33007);

    const std::vector<std::string> jsons = {
            R"({"typed_i":1,"hot":"a","cold0":100})",
            R"({"typed_i":2,"hot":"b","cold1":101})",
            R"({"hot":"c","cold2":102})",
    };

    SegmentFooterPB footer;
    std::string file_path;
    auto st = write_v2_segment(jsons, "typed_materialized", &footer, &file_path);
    ASSERT_TRUE(st.ok()) << st.to_string();

    const auto* typed_meta = find_footer_column_meta_by_relative_path(footer, "typed_i");
    ASSERT_NE(typed_meta, nullptr);
    EXPECT_EQ(typed_meta->type(), static_cast<int>(FieldType::OLAP_FIELD_TYPE_INT));
    EXPECT_TRUE(typed_meta->is_nullable());
    EXPECT_FALSE(typed_meta->has_none_null_size());

    const auto* hot_meta = find_footer_column_meta_by_relative_path(footer, "hot");
    ASSERT_NE(hot_meta, nullptr);
    EXPECT_EQ(hot_meta->none_null_size(), jsons.size());

    io::FileReaderSPtr file_reader;
    st = io::global_local_filesystem()->open_file(file_path, &file_reader);
    ASSERT_TRUE(st.ok()) << st.msg();
    std::shared_ptr<ColumnReader> column_reader;
    st = create_variant_root_reader(footer, file_reader, _tablet_schema, &column_reader);
    ASSERT_TRUE(st.ok()) << st.msg();
    auto* variant_column_reader = assert_cast<VariantColumnReader*>(column_reader.get());
    ASSERT_NE(variant_column_reader, nullptr);
    EXPECT_NE(variant_column_reader->get_subcolumn_meta_by_path(PathInData("typed_i")), nullptr);
    EXPECT_NE(variant_column_reader->get_subcolumn_meta_by_path(PathInData("hot")), nullptr);
    EXPECT_TRUE(variant_column_reader->exist_in_sparse_column(PathInData("cold0")));
    EXPECT_TRUE(variant_column_reader->exist_in_sparse_column(PathInData("cold1")));
    EXPECT_TRUE(variant_column_reader->exist_in_sparse_column(PathInData("cold2")));
    const auto* stats = variant_column_reader->get_stats();
    ASSERT_NE(stats, nullptr);
    EXPECT_FALSE(stats->subcolumns_non_null_size.contains("typed_i"));
    EXPECT_EQ(stats->subcolumns_non_null_size.at("hot"), jsons.size());

    std::vector<std::string> actual_rows;
    st = read_root_rows(footer, file_path, &actual_rows);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(actual_rows, normalize_json_rows(jsons));

    std::vector<std::string> typed_values;
    st = read_variant_path_rows(footer, file_path, "typed_i", FieldType::OLAP_FIELD_TYPE_INT,
                                &typed_values);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(typed_values, (std::vector<std::string> {"1", "2", "NULL"}));

    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
}

TEST_F(VariantColumnWriterReaderTest, test_v2_write_preserves_typed_temporal_primitives) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1",
                     /*variant_max_subcolumns_count=*/0);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    ColumnPB date_pb;
    date_pb.set_unique_id(-1);
    date_pb.set_name("a");
    date_pb.set_type("DATEV2");
    date_pb.set_is_nullable(true);
    date_pb.set_pattern_type(PatternTypePB::MATCH_NAME);
    TabletColumn date;
    date.init_from_pb(date_pb);
    _tablet_schema->mutable_column_by_uid(1).add_sub_column(date);

    ColumnPB datetime_pb;
    datetime_pb.set_unique_id(-1);
    datetime_pb.set_name("c");
    datetime_pb.set_type("DATETIMEV2");
    datetime_pb.set_is_nullable(true);
    datetime_pb.set_frac(0);
    datetime_pb.set_pattern_type(PatternTypePB::MATCH_NAME);
    TabletColumn datetime;
    datetime.init_from_pb(datetime_pb);
    _tablet_schema->mutable_column_by_uid(1).add_sub_column(datetime);
    init_tablet_from_current_schema(33013);

    constexpr int32_t date_days = 20'194;
    constexpr int64_t datetime_micros = static_cast<int64_t>(date_days + 1) * 86'400'000'000 +
                                        17 * 3'600'000'000 + 9 * 60'000'000 + 9'000'000;
    VariantBatchBuilder builder({.rows = 1, .metadata_keys = 2});
    auto row = builder.begin_row();
    auto object = row.start_object();
    object.add_key({"a", 1});
    row.add_date(date_days);
    object.add_key({"c", 1});
    row.add_timestamp_micros(datetime_micros, false);
    object.finish();
    row.finish();
    VariantBatchBuilder encoded = builder.finish_batch();
    auto input = ColumnVariantV2::create();
    input->insert_encoded_batch(encoded);
    const std::string expected = R"({"a":"2025-04-16","c":"2025-04-17 17:09:09.000000"})";
    EXPECT_EQ(variant_json_at(*input, 0), expected);

    const std::string file_path = local_segment_path(_tablet->tablet_path(), "typed_temporal", 0);
    io::FileWriterPtr file_writer;
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    ASSERT_TRUE(st.ok()) << st.to_string();

    SegmentFooterPB footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    rowset_ctx.tablet_schema = _tablet_schema;
    rowset_ctx.tablet = _tablet;
    rowset_ctx.tablet_path = _tablet->tablet_path();

    TabletColumn parent = _tablet_schema->column(0);
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    opts.rowset_ctx = &rowset_ctx;
    opts.storage_format = TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2;
    _init_column_meta(opts.meta, 0, parent, opts);

    std::unique_ptr<ColumnWriter> writer;
    ASSERT_TRUE(ColumnWriter::create(opts, &parent, file_writer.get(), &writer).ok());
    ASSERT_TRUE(writer->init().ok());

    Block block = _tablet_schema->create_block();
    block.replace_by_position(0, std::move(input));
    OlapBlockDataConvertor converter;
    converter.add_column_data_convertor(parent);
    converter.set_source_content(&block, 0, 1);
    auto [convert_status, accessor] = converter.convert_column_data(0);
    ASSERT_TRUE(convert_status.ok()) << convert_status.to_string();
    ASSERT_TRUE(writer->append(accessor->get_nullmap(), accessor->get_data(), 1).ok());
    ASSERT_TRUE(writer->finish().ok());
    ASSERT_TRUE(writer->write_data().ok());
    ASSERT_TRUE(writer->write_ordinal_index().ok());
    ASSERT_TRUE(writer->write_zone_map().ok());
    ASSERT_TRUE(file_writer->close().ok());
    footer.set_num_rows(1);

    const auto* date_meta = find_footer_column_meta_by_relative_path(footer, "a");
    ASSERT_NE(date_meta, nullptr);
    EXPECT_EQ(date_meta->type(), static_cast<int>(FieldType::OLAP_FIELD_TYPE_DATEV2));
    const auto* datetime_meta = find_footer_column_meta_by_relative_path(footer, "c");
    ASSERT_NE(datetime_meta, nullptr);
    EXPECT_EQ(datetime_meta->type(), static_cast<int>(FieldType::OLAP_FIELD_TYPE_DATETIMEV2));
    EXPECT_EQ(datetime_meta->frac(), 0);

    std::vector<std::string> actual_rows;
    st = read_root_rows(footer, file_path, &actual_rows);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(actual_rows, (std::vector<std::string> {expected}));
}

TEST_F(VariantColumnWriterReaderTest, test_v2_write_typed_path_sparse_fallback) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    auto* root_pb = schema_pb.add_column();
    construct_column(root_pb, 1, "VARIANT", "V1",
                     /*variant_max_subcolumns_count=*/1,
                     /*is_key=*/false,
                     /*is_nullable=*/false,
                     /*variant_sparse_hash_shard_count=*/0,
                     /*variant_enable_doc_mode=*/false);
    root_pb->set_variant_enable_typed_paths_to_sparse(true);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    auto typed_path = make_int_typed_path_template("typed_i");
    _tablet_schema->mutable_column_by_uid(1).add_sub_column(typed_path);
    init_tablet_from_current_schema(33008);

    const std::vector<std::string> jsons = {
            R"({"typed_i":1,"hot":"a"})",
            R"({"typed_i":2,"hot":"b"})",
            R"({"hot":"c"})",
    };

    SegmentFooterPB footer;
    std::string file_path;
    auto st = write_v2_segment(jsons, "typed_sparse", &footer, &file_path);
    ASSERT_TRUE(st.ok()) << st.to_string();

    EXPECT_EQ(find_footer_column_meta_by_relative_path(footer, "typed_i"), nullptr);
    const auto* hot_meta = find_footer_column_meta_by_relative_path(footer, "hot");
    ASSERT_NE(hot_meta, nullptr);
    EXPECT_EQ(hot_meta->none_null_size(), jsons.size());

    io::FileReaderSPtr file_reader;
    st = io::global_local_filesystem()->open_file(file_path, &file_reader);
    ASSERT_TRUE(st.ok()) << st.msg();
    std::shared_ptr<ColumnReader> column_reader;
    st = create_variant_root_reader(footer, file_reader, _tablet_schema, &column_reader);
    ASSERT_TRUE(st.ok()) << st.msg();
    auto* variant_column_reader = assert_cast<VariantColumnReader*>(column_reader.get());
    ASSERT_NE(variant_column_reader, nullptr);
    EXPECT_EQ(variant_column_reader->get_subcolumn_meta_by_path(PathInData("typed_i")), nullptr);
    EXPECT_NE(variant_column_reader->get_subcolumn_meta_by_path(PathInData("hot")), nullptr);
    EXPECT_TRUE(variant_column_reader->exist_in_sparse_column(PathInData("typed_i")));

    const auto* stats = variant_column_reader->get_stats();
    ASSERT_NE(stats, nullptr);
    EXPECT_EQ(stats->subcolumns_non_null_size.at("hot"), jsons.size());
    EXPECT_EQ(stats->sparse_column_non_null_size.at("typed_i"), 2);

    std::vector<std::string> actual_rows;
    st = read_root_rows(footer, file_path, &actual_rows);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(actual_rows, normalize_json_rows(jsons));

    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
}

TEST_F(VariantColumnWriterReaderTest,
       test_v2_write_glob_typed_path_materialized_with_storage_type) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1",
                     /*variant_max_subcolumns_count=*/1,
                     /*is_key=*/false,
                     /*is_nullable=*/false,
                     /*variant_sparse_hash_shard_count=*/0,
                     /*variant_enable_doc_mode=*/false);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    auto typed_path = make_int_typed_path_template("typed_*", PatternTypePB::MATCH_NAME_GLOB);
    _tablet_schema->mutable_column_by_uid(1).add_sub_column(typed_path);
    init_tablet_from_current_schema(33010);

    const std::vector<std::string> jsons = {
            R"({"typed_g":1,"hot":"a","cold0":100})",
            R"({"typed_g":2,"hot":"b","cold1":101})",
            R"({"hot":"c","cold2":102})",
    };

    SegmentFooterPB footer;
    std::string file_path;
    auto st = write_v2_segment(jsons, "glob_typed_materialized", &footer, &file_path);
    ASSERT_TRUE(st.ok()) << st.to_string();

    const auto* typed_meta = find_footer_column_meta_by_relative_path(footer, "typed_g");
    ASSERT_NE(typed_meta, nullptr);
    EXPECT_EQ(typed_meta->type(), static_cast<int>(FieldType::OLAP_FIELD_TYPE_INT));
    EXPECT_TRUE(typed_meta->is_nullable());
    EXPECT_FALSE(typed_meta->has_none_null_size());

    const auto* hot_meta = find_footer_column_meta_by_relative_path(footer, "hot");
    ASSERT_NE(hot_meta, nullptr);
    EXPECT_EQ(hot_meta->none_null_size(), jsons.size());

    std::vector<std::string> actual_rows;
    st = read_root_rows(footer, file_path, &actual_rows);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(actual_rows, normalize_json_rows(jsons));

    io::FileReaderSPtr file_reader;
    st = io::global_local_filesystem()->open_file(file_path, &file_reader);
    ASSERT_TRUE(st.ok()) << st.msg();
    std::shared_ptr<ColumnReader> column_reader;
    st = create_variant_root_reader(footer, file_reader, _tablet_schema, &column_reader);
    ASSERT_TRUE(st.ok()) << st.msg();
    auto* variant_column_reader = assert_cast<VariantColumnReader*>(column_reader.get());
    ASSERT_NE(variant_column_reader, nullptr);
    const auto* stats = variant_column_reader->get_stats();
    ASSERT_NE(stats, nullptr);
    EXPECT_FALSE(stats->subcolumns_non_null_size.contains("typed_g"));
    EXPECT_EQ(stats->subcolumns_non_null_size.at("hot"), jsons.size());

    std::vector<std::string> typed_values;
    st = read_variant_path_rows(footer, file_path, "typed_g", FieldType::OLAP_FIELD_TYPE_INT,
                                &typed_values);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(typed_values, (std::vector<std::string> {"1", "2", "NULL"}));

    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
}

TEST_F(VariantColumnWriterReaderTest, test_v2_write_glob_typed_path_sparse_fallback) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    auto* root_pb = schema_pb.add_column();
    construct_column(root_pb, 1, "VARIANT", "V1",
                     /*variant_max_subcolumns_count=*/1,
                     /*is_key=*/false,
                     /*is_nullable=*/false,
                     /*variant_sparse_hash_shard_count=*/0,
                     /*variant_enable_doc_mode=*/false);
    root_pb->set_variant_enable_typed_paths_to_sparse(true);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    auto typed_path = make_int_typed_path_template("typed_*", PatternTypePB::MATCH_NAME_GLOB);
    _tablet_schema->mutable_column_by_uid(1).add_sub_column(typed_path);
    init_tablet_from_current_schema(33011);

    const std::vector<std::string> jsons = {
            R"({"typed_g":1,"hot":"a"})",
            R"({"typed_g":2,"hot":"b"})",
            R"({"hot":"c"})",
    };

    SegmentFooterPB footer;
    std::string file_path;
    auto st = write_v2_segment(jsons, "glob_typed_sparse", &footer, &file_path);
    ASSERT_TRUE(st.ok()) << st.to_string();

    EXPECT_EQ(find_footer_column_meta_by_relative_path(footer, "typed_g"), nullptr);
    const auto* hot_meta = find_footer_column_meta_by_relative_path(footer, "hot");
    ASSERT_NE(hot_meta, nullptr);
    EXPECT_EQ(hot_meta->none_null_size(), jsons.size());

    io::FileReaderSPtr file_reader;
    st = io::global_local_filesystem()->open_file(file_path, &file_reader);
    ASSERT_TRUE(st.ok()) << st.msg();
    std::shared_ptr<ColumnReader> column_reader;
    st = create_variant_root_reader(footer, file_reader, _tablet_schema, &column_reader);
    ASSERT_TRUE(st.ok()) << st.msg();
    auto* variant_column_reader = assert_cast<VariantColumnReader*>(column_reader.get());
    ASSERT_NE(variant_column_reader, nullptr);
    EXPECT_TRUE(variant_column_reader->exist_in_sparse_column(PathInData("typed_g")));

    const auto* stats = variant_column_reader->get_stats();
    ASSERT_NE(stats, nullptr);
    EXPECT_EQ(stats->subcolumns_non_null_size.at("hot"), jsons.size());
    EXPECT_EQ(stats->sparse_column_non_null_size.at("typed_g"), 2);

    std::vector<std::string> actual_rows;
    st = read_root_rows(footer, file_path, &actual_rows);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(actual_rows, normalize_json_rows(jsons));

    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
}

TEST_F(VariantColumnWriterReaderTest, test_v2_write_parent_index_topn_materialized_only) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    schema_pb.set_inverted_index_storage_format(InvertedIndexStorageFormatPB::V2);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1",
                     /*variant_max_subcolumns_count=*/1,
                     /*is_key=*/false,
                     /*is_nullable=*/false,
                     /*variant_sparse_hash_shard_count=*/0,
                     /*variant_enable_doc_mode=*/false);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    TabletIndexPB parent_index_pb;
    construct_tablet_index(&parent_index_pb, 10007, "idx_v1",
                           _tablet_schema->column(0).unique_id());
    TabletIndex parent_index;
    parent_index.init_from_pb(parent_index_pb);
    _tablet_schema->append_index(std::move(parent_index));
    init_tablet_from_current_schema(33009);

    const std::vector<std::string> jsons = {
            R"({"hot":"a","cold0":"x"})",
            R"({"hot":"b","cold1":"y"})",
            R"({"hot":"c","cold2":"z"})",
    };

    SegmentFooterPB footer;
    std::string file_path;
    auto st = write_v2_segment(jsons, "parent_index", &footer, &file_path,
                               true /* write_inverted_index */);
    ASSERT_TRUE(st.ok()) << st.to_string();

    const auto* hot_meta = find_footer_column_meta_by_relative_path(footer, "hot");
    ASSERT_NE(hot_meta, nullptr);
    EXPECT_EQ(hot_meta->none_null_size(), jsons.size());
    EXPECT_EQ(find_footer_column_meta_by_relative_path(footer, "cold0"), nullptr);

    io::FileReaderSPtr file_reader;
    st = io::global_local_filesystem()->open_file(file_path, &file_reader);
    ASSERT_TRUE(st.ok()) << st.msg();
    std::shared_ptr<ColumnReader> column_reader;
    st = create_variant_root_reader(footer, file_reader, _tablet_schema, &column_reader);
    ASSERT_TRUE(st.ok()) << st.msg();
    auto* variant_column_reader = assert_cast<VariantColumnReader*>(column_reader.get());
    ASSERT_NE(variant_column_reader, nullptr);
    EXPECT_NE(variant_column_reader->get_subcolumn_meta_by_path(PathInData("hot")), nullptr);
    EXPECT_TRUE(variant_column_reader->exist_in_sparse_column(PathInData("cold0")));
    EXPECT_TRUE(variant_column_reader->exist_in_sparse_column(PathInData("cold1")));
    EXPECT_TRUE(variant_column_reader->exist_in_sparse_column(PathInData("cold2")));

    TabletColumn hot_subcolumn;
    hot_subcolumn.set_name("v1.hot");
    hot_subcolumn.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
    hot_subcolumn.set_parent_unique_id(_tablet_schema->column(0).unique_id());
    hot_subcolumn.set_path_info(PathInData("v1.hot"));
    hot_subcolumn.set_is_nullable(true);
    auto indexes = variant_column_reader->find_subcolumn_tablet_indexes(
            hot_subcolumn, std::make_shared<DataTypeString>());
    ASSERT_EQ(indexes.size(), 1);
    EXPECT_EQ(indexes[0]->index_id(), 10007);
    EXPECT_EQ(indexes[0]->get_index_suffix(), "v1%2Ehot");

    const std::string index_path_prefix =
            std::string(InvertedIndexDescriptor::get_index_file_path_prefix(file_path));
    auto index_file_reader = std::make_shared<IndexFileReader>(
            io::global_local_filesystem(), index_path_prefix, InvertedIndexStorageFormatPB::V2);
    st = index_file_reader->init();
    ASSERT_TRUE(st.ok()) << st.to_string();
    auto inverted_reader =
            StringTypeInvertedIndexReader::create_shared(indexes[0].get(), index_file_reader);
    ASSERT_NE(inverted_reader, nullptr);

    auto query_cache = std::unique_ptr<InvertedIndexQueryCache>(
            InvertedIndexQueryCache::create_global_cache(1024 * 1024, 1));
    auto searcher_cache = std::unique_ptr<InvertedIndexSearcherCache>(
            InvertedIndexSearcherCache::create_global_instance(1024 * 1024, 1));
    auto* previous_query_cache = ExecEnv::GetInstance()->get_inverted_index_query_cache();
    auto* previous_searcher_cache = ExecEnv::GetInstance()->get_inverted_index_searcher_cache();
    ExecEnv::GetInstance()->set_inverted_index_query_cache(query_cache.get());
    ExecEnv::GetInstance()->set_inverted_index_searcher_cache(searcher_cache.get());
    DEFER({
        ExecEnv::GetInstance()->set_inverted_index_query_cache(previous_query_cache);
        ExecEnv::GetInstance()->set_inverted_index_searcher_cache(previous_searcher_cache);
    });

    OlapReaderStatistics index_stats;
    RuntimeState runtime_state;
    TQueryOptions query_options;
    query_options.enable_inverted_index_query_cache = false;
    query_options.enable_inverted_index_searcher_cache = false;
    runtime_state.set_query_options(query_options);
    io::IOContext io_ctx;
    auto query_context = std::make_shared<IndexQueryContext>();
    query_context->io_ctx = &io_ctx;
    query_context->stats = &index_stats;
    query_context->runtime_state = &runtime_state;

    const std::string field_name =
            std::to_string(hot_subcolumn.parent_unique_id()) + "." + hot_subcolumn.name();
    for (size_t row = 0; row < jsons.size(); ++row) {
        auto bitmap = std::make_shared<roaring::Roaring>();
        Field query_value =
                Field::create_field<TYPE_STRING>(std::string(1, static_cast<char>('a' + row)));
        st = inverted_reader->query(query_context, field_name, query_value,
                                    InvertedIndexQueryType::EQUAL_QUERY, bitmap);
        ASSERT_TRUE(st.ok()) << st.to_string();
        EXPECT_EQ(bitmap->cardinality(), 1);
        EXPECT_TRUE(bitmap->contains(static_cast<uint32_t>(row)));
    }

    std::vector<std::string> actual_rows;
    st = read_root_rows(footer, file_path, &actual_rows);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(actual_rows, normalize_json_rows(jsons));

    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
}

TEST_F(VariantColumnWriterReaderTest,
       test_compaction_schema_excludes_materialized_typed_paths_from_topn_sparse_paths) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    schema_pb.set_inverted_index_storage_format(InvertedIndexStorageFormatPB::V2);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1",
                     /*variant_max_subcolumns_count=*/1,
                     /*is_key=*/false,
                     /*is_nullable=*/false,
                     /*variant_sparse_hash_shard_count=*/0,
                     /*variant_enable_doc_mode=*/false);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    auto typed_path = make_int_typed_path_template("a");
    _tablet_schema->mutable_column_by_uid(1).add_sub_column(typed_path);

    TabletIndexPB parent_index_pb;
    construct_tablet_index(&parent_index_pb, 10008, "idx_v1",
                           _tablet_schema->column(0).unique_id());
    TabletIndex parent_index;
    parent_index.init_from_pb(parent_index_pb);
    _tablet_schema->append_index(std::move(parent_index));
    init_tablet_from_current_schema(33012);

    auto rowset = create_variant_rowset({{R"({"a":1,"b":"x"})", R"({"a":2,"b":"y","c":"z"})"}}, 1);
    std::vector<RowsetSharedPtr> input_rowsets {rowset};

    auto compaction_schema = std::make_shared<TabletSchema>(*_tablet_schema);
    auto st = variant_util::VariantCompactionUtil::get_extended_compaction_schema(
            input_rowsets, compaction_schema);
    ASSERT_TRUE(st.ok()) << st.to_string();

    const auto* path_set_info = compaction_schema->try_path_set_info(1);
    ASSERT_NE(path_set_info, nullptr);
    ASSERT_TRUE(path_set_info->typed_path_set.contains("a"));
    EXPECT_FALSE(path_set_info->sub_path_set.contains(StringRef("a")));
    EXPECT_FALSE(path_set_info->sparse_path_set.contains(StringRef("a")));
    EXPECT_FALSE(path_set_info->subcolumn_indexes.contains("a"));
    EXPECT_TRUE(path_set_info->sub_path_set.contains(StringRef("b")));
    EXPECT_TRUE(path_set_info->sparse_path_set.contains(StringRef("c")));

    size_t typed_path_count = 0;
    size_t dynamic_path_count = 0;
    size_t sparse_path_count = 0;
    for (const auto& column : compaction_schema->columns()) {
        if (!column->is_extracted_column() || column->parent_unique_id() != 1) {
            continue;
        }
        const auto relative_path = column->path_info_ptr()->copy_pop_front().get_path();
        if (relative_path == "a") {
            ++typed_path_count;
            EXPECT_TRUE(column->path_info_ptr()->get_is_typed());
        } else if (relative_path == "b") {
            ++dynamic_path_count;
            EXPECT_FALSE(column->path_info_ptr()->get_is_typed());
        } else if (relative_path == "c") {
            ++sparse_path_count;
        }
    }
    EXPECT_EQ(typed_path_count, 1);
    EXPECT_EQ(dynamic_path_count, 1);
    EXPECT_EQ(sparse_path_count, 0);

    const auto& typed_info = path_set_info->typed_path_set.at("a");
    ASSERT_EQ(typed_info.indexes.size(), 1);
    EXPECT_EQ(typed_info.indexes[0]->index_id(), 10008);
    EXPECT_EQ(typed_info.indexes[0]->get_index_suffix(), "v1%2Ea");
}

TEST_F(VariantColumnWriterReaderTest,
       test_doc_value_staging_root_writer_skips_payload_with_extracted_columns) {
    constexpr int kRows = 2;

    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1",
                     /*variant_max_subcolumns_count=*/2,
                     /*is_key=*/false,
                     /*is_nullable=*/false,
                     /*variant_sparse_hash_shard_count=*/0,
                     /*variant_enable_doc_mode=*/false);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    TabletColumn parent_column = _tablet_schema->column(0);
    TabletColumn extracted;
    extracted.set_name(parent_column.name_lower_case() + ".hot");
    extracted.set_type(FieldType::OLAP_FIELD_TYPE_BIGINT);
    extracted.set_parent_unique_id(parent_column.unique_id());
    extracted.set_path_info(PathInData(parent_column.name_lower_case() + ".hot"));
    extracted.set_is_nullable(true);
    _tablet_schema->append_column(extracted);

    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    _tablet_schema->set_storage_format(TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2);
    tablet_meta->_tablet_id = 33006;
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    ASSERT_TRUE(_tablet->init().ok());
    ASSERT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    ASSERT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    ASSERT_TRUE(st.ok()) << st.msg();

    SegmentFooterPB footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    rowset_ctx.tablet_schema = _tablet_schema;

    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    opts.rowset_ctx = &rowset_ctx;
    opts.storage_format = TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2;
    _init_column_meta(opts.meta, 0, parent_column, opts);

    std::unique_ptr<ColumnWriter> writer;
    ASSERT_TRUE(ColumnWriter::create(opts, &parent_column, file_writer.get(), &writer).ok());
    ASSERT_TRUE(writer->init().ok());

    auto strings = ColumnString::create();
    const std::vector<std::string> jsons = {R"({"hot":1,"cold":10})", R"({"hot":2,"cold":20})"};
    for (const auto& json : jsons) {
        strings->insert_data(json.data(), json.size());
    }

    auto variant_column = encode_json_column_v2(*strings);

    auto variant_data = std::make_unique<VariantColumnData>();
    variant_data->column_data = variant_column.get();
    variant_data->row_pos = 0;
    const auto* data = reinterpret_cast<const uint8_t*>(variant_data.get());
    ASSERT_TRUE(writer->append_data(&data, kRows).ok());

    ASSERT_TRUE(writer->finish().ok());
    ASSERT_TRUE(writer->write_data().ok());
    ASSERT_TRUE(writer->write_ordinal_index().ok());
    ASSERT_TRUE(writer->write_zone_map().ok());
    ASSERT_TRUE(file_writer->close().ok());
    footer.set_num_rows(kRows);

    EXPECT_EQ(footer.columns_size(), 1);
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
}

TEST_F(VariantColumnWriterReaderTest, test_write_data_advanced) {
    // 1. create tablet_schema
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    int variant_sparse_hash_shard_count = rand() % 10 + 1;
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1", 10, false, false,
                     variant_sparse_hash_shard_count);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    // 2. create tablet
    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    bool external_segment_meta_used_default = rand() % 2 == 0;
    std::cout << "external_segment_meta_used_default: " << external_segment_meta_used_default
              << std::endl;
    _tablet_schema->set_storage_format(external_segment_meta_used_default
                                               ? TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V3
                                               : TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2);
    tablet_meta->_tablet_id = 10000;
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    EXPECT_TRUE(_tablet->init().ok());
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    // 3. create file_writer
    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    EXPECT_TRUE(st.ok()) << st.msg();

    // 4. create column_writer
    SegmentFooterPB footer;
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    opts.rowset_ctx = &rowset_ctx;
    opts.rowset_ctx->tablet_schema = _tablet_schema;
    TabletColumn column = _tablet_schema->column(0);
    opts.compression_type = CompressionTypePB::LZ4;
    opts.storage_format = TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2;
    _init_column_meta(opts.meta, 0, column, opts);

    std::unique_ptr<ColumnWriter> writer;
    EXPECT_TRUE(ColumnWriter::create(opts, &column, file_writer.get(), &writer).ok());
    EXPECT_TRUE(writer->init().ok());
    EXPECT_TRUE(assert_cast<VariantColumnWriter*>(writer.get()) != nullptr);

    // 5. write data
    auto olap_data_convertor = std::make_unique<OlapBlockDataConvertor>();
    auto block = _tablet_schema->create_block();
    std::unordered_map<int, std::string> inserted_jsonstr;
    auto json_column = ColumnString::create();
    auto path_with_size = VariantUtil::fill_string_column_with_nested_test_data(json_column, 1000,
                                                                                &inserted_jsonstr);
    block.replace_by_position(0, encode_json_column_v2(*json_column));
    olap_data_convertor->add_column_data_convertor(column);
    olap_data_convertor->set_source_content(&block, 0, 1000);
    auto [result, accessor] = olap_data_convertor->convert_column_data(0);
    EXPECT_TRUE(result.ok());
    EXPECT_TRUE(accessor != nullptr);
    EXPECT_TRUE(writer->append(accessor->get_nullmap(), accessor->get_data(), 1000).ok());
    st = writer->finish();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_data();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_ordinal_index();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_zone_map();
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(file_writer->close().ok());
    footer.set_num_rows(1000);

    // 6. check footer
    int expected_sparse_cols =
            variant_sparse_hash_shard_count > 1 ? variant_sparse_hash_shard_count : 1;
    EXPECT_EQ(footer.columns_size(), 1 + 10 + expected_sparse_cols);
    auto column_meta = footer.columns(0);
    EXPECT_EQ(column_meta.type(), (int)FieldType::OLAP_FIELD_TYPE_VARIANT);

    for (int i = 1; i < footer.columns_size() - 1; ++i) {
        auto column_met = footer.columns(i);
        check_column_meta(column_met, path_with_size);
    }
    check_sparse_column_meta(footer.columns(footer.columns_size() - 1), path_with_size);

    // 7. check variant reader
    io::FileReaderSPtr file_reader;
    st = io::global_local_filesystem()->open_file(file_path, &file_reader);
    EXPECT_TRUE(st.ok()) << st.msg();
    std::shared_ptr<ColumnReader> column_reader;
    st = create_variant_root_reader(footer, file_reader, _tablet_schema, &column_reader);
    EXPECT_TRUE(st.ok()) << st.msg();

    auto variant_column_reader = assert_cast<VariantColumnReader*>(column_reader.get());
    EXPECT_TRUE(variant_column_reader != nullptr);

    // 8. check statistics
    auto statistics = variant_column_reader->get_stats();
    for (const auto& [path, size] : statistics->subcolumns_non_null_size) {
        EXPECT_EQ(path_with_size[path], size);
    }
    for (const auto& [path, size] : statistics->sparse_column_non_null_size) {
        EXPECT_EQ(path_with_size[path], size);
    }

    MockColumnReaderCache column_reader_cache(footer, file_reader, _tablet_schema);

    // 9. check root
    ColumnIteratorUPtr it;
    TabletColumn parent_column = _tablet_schema->column(0);
    StorageReadOptions storage_read_opts;
    storage_read_opts.io_ctx.reader_type = ReaderType::READER_QUERY;
    OlapReaderStatistics stats;
    storage_read_opts.stats = &stats;
    st = variant_column_reader->new_iterator(&it, &parent_column, &storage_read_opts,
                                             &column_reader_cache);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(assert_cast<HierarchicalDataIterator*>(it.get()) != nullptr);
    ColumnIteratorOptions column_iter_opts;
    column_iter_opts.stats = &stats;
    column_iter_opts.file_reader = file_reader.get();
    st = it->init(column_iter_opts);
    EXPECT_TRUE(st.ok()) << st.msg();

    MutableColumnPtr new_column_object = ColumnVariantV2::create();
    size_t nrows = 1000;
    st = it->seek_to_ordinal(0);
    EXPECT_TRUE(st.ok()) << st.msg();
    st = it->next_batch(&nrows, new_column_object);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(stats.bytes_read > 0);

    for (int i = 0; i < 1000; ++i) {
        EXPECT_EQ(variant_json_at(*new_column_object, i), inserted_jsonstr[i]);
    }

    auto read_to_column_object = [&](ColumnIteratorUPtr& it) {
        new_column_object = ColumnVariantV2::create();
        nrows = 1000;
        st = it->seek_to_ordinal(0);
        EXPECT_TRUE(st.ok()) << st.msg();
        st = it->next_batch(&nrows, new_column_object);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_TRUE(stats.bytes_read > 0);
        EXPECT_EQ(nrows, 1000);
    };

    auto check_key_stats = [&](const std::string& key_num) {
        std::string key = ".key" + key_num;
        TabletColumn subcolumn_in_nested;
        subcolumn_in_nested.set_name(parent_column.name_lower_case() + key);
        subcolumn_in_nested.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);
        subcolumn_in_nested.set_parent_unique_id(parent_column.unique_id());
        subcolumn_in_nested.set_path_info(PathInData(parent_column.name_lower_case() + key));
        subcolumn_in_nested.set_variant_max_subcolumns_count(
                parent_column.variant_max_subcolumns_count());
        subcolumn_in_nested.set_is_nullable(true);

        ColumnIteratorUPtr it1;
        st = variant_column_reader->new_iterator(&it1, &subcolumn_in_nested, &storage_read_opts,
                                                 &column_reader_cache);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_TRUE(assert_cast<HierarchicalDataIterator*>(it1.get()) != nullptr);
        st = it1->init(column_iter_opts);
        EXPECT_TRUE(st.ok()) << st.msg();
        read_to_column_object(it1);

        size_t key_count = 0;
        size_t key_nested_count = 0;
        for (int row = 0; row < 1000; ++row) {
            const std::string value = variant_json_at(*new_column_object, row);
            if (value.find("nested" + key_num) != std::string::npos) {
                key_nested_count++;
            } else if (value.find("88") != std::string::npos) {
                key_count++;
            }
        }
        EXPECT_EQ(key_count, path_with_size["key" + key_num]);
        EXPECT_EQ(key_nested_count, path_with_size["key" + key_num + ".nested" + key_num]);
    };

    for (int i = 3; i < 10; ++i) {
        check_key_stats(std::to_string(i));
    }

    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
}

TEST_F(VariantColumnWriterReaderTest, test_write_sub_index) {
    // 1. create tablet_schema
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    schema_pb.set_inverted_index_storage_format(InvertedIndexStorageFormatPB::V2);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "v", 2, false);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);
    TabletColumn& variant = _tablet_schema->mutable_column_by_uid(1);
    // add subcolumn
    TabletColumn subcolumn2;
    subcolumn2.set_name("v.b");
    subcolumn2.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
    variant.add_sub_column(subcolumn2);
    variant.set_is_bf_column(true);

    // 2. create tablet
    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    bool external_segment_meta_used_default = rand() % 2 == 0;
    std::cout << "external_segment_meta_used_default: " << external_segment_meta_used_default
              << std::endl;
    _tablet_schema->set_storage_format(external_segment_meta_used_default
                                               ? TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V3
                                               : TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2);
    tablet_meta->_tablet_id = 10000;
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    EXPECT_TRUE(_tablet->init().ok());
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    // 3. create file_writer
    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    EXPECT_TRUE(st.ok()) << st.msg();

    // 4. create column_writer
    SegmentFooterPB footer;
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    opts.rowset_ctx = &rowset_ctx;
    opts.rowset_ctx->tablet_schema = _tablet_schema;
    TabletColumn column = _tablet_schema->column(0);
    opts.compression_type = CompressionTypePB::LZ4;
    opts.storage_format = TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2;
    _init_column_meta(opts.meta, 0, column, opts);

    std::unique_ptr<ColumnWriter> writer;
    EXPECT_TRUE(ColumnWriter::create(opts, &column, file_writer.get(), &writer).ok());
    EXPECT_TRUE(writer->init().ok());
    EXPECT_TRUE(assert_cast<VariantColumnWriter*>(writer.get()) != nullptr);

    // 5. write data
    // The legacy fixture combined a scalar and an object at the same path, which canonical V2
    // cannot represent. This test only exercises the v.b sub-index writer lifecycle, so use ten
    // legal V2 objects that preserve the row count and the STRING type of v.b.
    auto strings = ColumnString::create();
    constexpr std::string_view json = R"({"b":"20"})";
    for (size_t row = 0; row < 10; ++row) {
        strings->insert_data(json.data(), json.size());
    }
    auto column_object = encode_json_column_v2(*strings);
    auto vw = assert_cast<VariantColumnWriter*>(writer.get());

    std::unique_ptr<VariantColumnData> _variant_column_data = std::make_unique<VariantColumnData>();
    // pass the real ColumnVariantV2 pointer instead of address of shared_ptr
    _variant_column_data->column_data = column_object.get();
    _variant_column_data->row_pos = 0;
    const uint8_t* data = (const uint8_t*)_variant_column_data.get();
    st = vw->append_data(&data, 10);
    EXPECT_TRUE(st.ok()) << st.msg();
    st = vw->finish();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = vw->write_data();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = vw->write_ordinal_index();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = vw->write_zone_map();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = vw->write_bloom_filter_index();
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(file_writer->close().ok());
    footer.set_num_rows(10);

    // 6. check footer
    std::cout << footer.columns_size() << std::endl;
    auto column_meta = footer.columns(0);
    EXPECT_EQ(column_meta.type(), (int)FieldType::OLAP_FIELD_TYPE_VARIANT);
}

TEST_F(VariantColumnWriterReaderTest, test_find_subcolumn_tablet_indexes_inherits_full_path) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    schema_pb.set_inverted_index_storage_format(InvertedIndexStorageFormatPB::V2);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "v", 10, false);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    _tablet_schema->set_storage_format(TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2);
    tablet_meta->_tablet_id = 10001;
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    ASSERT_TRUE(_tablet->init().ok());
    ASSERT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    ASSERT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    ASSERT_TRUE(st.ok()) << st.msg();

    SegmentFooterPB footer;
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    rowset_ctx.tablet_schema = _tablet_schema;
    opts.rowset_ctx = &rowset_ctx;
    TabletColumn column = _tablet_schema->column(0);
    opts.compression_type = CompressionTypePB::LZ4;
    opts.storage_format = TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2;
    _init_column_meta(opts.meta, 0, column, opts);

    std::unique_ptr<ColumnWriter> writer;
    ASSERT_TRUE(ColumnWriter::create(opts, &column, file_writer.get(), &writer).ok());
    ASSERT_TRUE(writer->init().ok());
    ASSERT_TRUE(append_json_batch(writer.get(), {R"({"a": "x"})"}).ok());
    ASSERT_TRUE(writer->finish().ok());
    ASSERT_TRUE(writer->write_data().ok());
    ASSERT_TRUE(writer->write_ordinal_index().ok());
    ASSERT_TRUE(writer->write_zone_map().ok());
    ASSERT_TRUE(file_writer->close().ok());
    footer.set_num_rows(1);

    TabletIndexPB index_pb;
    construct_tablet_index(&index_pb, 10001, "idx_v", column.unique_id());
    TabletIndex parent_index;
    parent_index.init_from_pb(index_pb);
    _tablet_schema->append_index(std::move(parent_index));

    io::FileReaderSPtr file_reader;
    st = io::global_local_filesystem()->open_file(file_path, &file_reader);
    ASSERT_TRUE(st.ok()) << st.msg();
    std::shared_ptr<segment_v2::ColumnReader> column_reader;
    st = create_variant_root_reader(footer, file_reader, _tablet_schema, &column_reader);
    ASSERT_TRUE(st.ok()) << st.msg();
    auto* variant_reader = assert_cast<segment_v2::VariantColumnReader*>(column_reader.get());
    ASSERT_NE(variant_reader, nullptr);

    TabletColumn subcolumn;
    subcolumn.set_name("v.a");
    subcolumn.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
    subcolumn.set_parent_unique_id(column.unique_id());
    subcolumn.set_path_info(PathInData("v.a"));
    subcolumn.set_is_nullable(true);

    auto indexes = variant_reader->find_subcolumn_tablet_indexes(
            subcolumn, std::make_shared<DataTypeString>());
    ASSERT_EQ(indexes.size(), 1);
    EXPECT_EQ(indexes[0]->index_id(), 10001);
    EXPECT_EQ(indexes[0]->get_index_suffix(), "v%2Ea");
    EXPECT_NE(indexes[0]->get_index_suffix(), "a");
}

TEST_F(VariantColumnWriterReaderTest, test_nested_group_logical_index_path_uses_variant_root) {
    EXPECT_EQ(segment_v2::build_nested_group_logical_child_path("v", "arr", "x"), "v.arr.x");
    EXPECT_EQ(segment_v2::build_nested_group_logical_child_path("v", "arr.inner", "z"),
              "v.arr.inner.z");
    EXPECT_EQ(segment_v2::build_nested_group_logical_child_path(
                      "v", std::string(segment_v2::kRootNestedGroupPath), "x"),
              "v.x");
    EXPECT_EQ(segment_v2::build_nested_group_logical_child_path(
                      "v", std::string(segment_v2::kRootNestedGroupPath) + ".inner", "z"),
              "v.inner.z");
}

TEST_F(VariantColumnWriterReaderTest, test_find_subcolumn_tablet_indexes_branch_coverage) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    schema_pb.set_inverted_index_storage_format(InvertedIndexStorageFormatPB::V2);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "v", 10, false);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    _tablet_schema->set_storage_format(TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2);
    tablet_meta->_tablet_id = 10002;
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    ASSERT_TRUE(_tablet->init().ok());
    ASSERT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    ASSERT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    ASSERT_TRUE(st.ok()) << st.msg();

    SegmentFooterPB footer;
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    rowset_ctx.tablet_schema = _tablet_schema;
    opts.rowset_ctx = &rowset_ctx;
    TabletColumn root_column = _tablet_schema->column(0);
    opts.compression_type = CompressionTypePB::LZ4;
    opts.storage_format = TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2;
    _init_column_meta(opts.meta, 0, root_column, opts);

    std::unique_ptr<ColumnWriter> writer;
    ASSERT_TRUE(ColumnWriter::create(opts, &root_column, file_writer.get(), &writer).ok());
    ASSERT_TRUE(writer->init().ok());
    ASSERT_TRUE(append_json_batch(writer.get(), {R"({"a": "x", "own": "y"})"}).ok());
    ASSERT_TRUE(writer->finish().ok());
    ASSERT_TRUE(writer->write_data().ok());
    ASSERT_TRUE(writer->write_ordinal_index().ok());
    ASSERT_TRUE(writer->write_zone_map().ok());
    ASSERT_TRUE(file_writer->close().ok());
    footer.set_num_rows(1);

    io::FileReaderSPtr file_reader;
    st = io::global_local_filesystem()->open_file(file_path, &file_reader);
    ASSERT_TRUE(st.ok()) << st.msg();
    std::shared_ptr<segment_v2::ColumnReader> column_reader;
    st = create_variant_root_reader(footer, file_reader, _tablet_schema, &column_reader);
    ASSERT_TRUE(st.ok()) << st.msg();
    auto* variant_reader = assert_cast<segment_v2::VariantColumnReader*>(column_reader.get());
    ASSERT_NE(variant_reader, nullptr);

    const int32_t root_unique_id = root_column.unique_id();
    auto make_subcolumn = [&](std::string name, FieldType type, std::string path,
                              int32_t parent_unique_id) {
        TabletColumn subcolumn;
        subcolumn.set_name(std::move(name));
        subcolumn.set_type(type);
        subcolumn.set_parent_unique_id(parent_unique_id);
        subcolumn.set_unique_id(2001);
        subcolumn.set_path_info(PathInData(std::move(path)));
        subcolumn.set_is_nullable(true);
        return subcolumn;
    };

    {
        auto no_parent_index = variant_reader->find_subcolumn_tablet_indexes(
                make_subcolumn("v.missing", FieldType::OLAP_FIELD_TYPE_STRING, "v.missing",
                               root_unique_id),
                std::make_shared<DataTypeString>());
        EXPECT_TRUE(no_parent_index.empty());
    }

    TabletIndexPB parent_index_pb;
    construct_tablet_index(&parent_index_pb, 10002, "idx_v", root_column.unique_id());
    TabletIndex parent_index;
    parent_index.init_from_pb(parent_index_pb);
    _tablet_schema->append_index(std::move(parent_index));

    {
        auto inherited = variant_reader->find_subcolumn_tablet_indexes(
                make_subcolumn("v.a", FieldType::OLAP_FIELD_TYPE_STRING, "v.a", root_unique_id),
                std::make_shared<DataTypeString>());
        ASSERT_EQ(inherited.size(), 1);
        EXPECT_EQ(inherited[0]->index_id(), 10002);
        EXPECT_EQ(inherited[0]->get_index_suffix(), "v%2Ea");
    }

    {
        auto plain_array = variant_reader->find_subcolumn_tablet_indexes(
                make_subcolumn("v.plainarr", FieldType::OLAP_FIELD_TYPE_ARRAY, "v.plainarr",
                               root_unique_id),
                std::make_shared<DataTypeNullable>(
                        std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())));
        ASSERT_EQ(plain_array.size(), 1);
        EXPECT_EQ(plain_array[0]->index_id(), 10002);
        EXPECT_EQ(plain_array[0]->get_index_suffix(), "v%2Eplainarr");
        EXPECT_NE(plain_array[0]->get_index_suffix(), "plainarr");
    }

    {
        auto variant_type = variant_reader->find_subcolumn_tablet_indexes(
                make_subcolumn("v.object", FieldType::OLAP_FIELD_TYPE_VARIANT, "v.object",
                               root_unique_id),
                std::make_shared<DataTypeVariant>(10, false));
        EXPECT_TRUE(variant_type.empty());
    }

    {
        auto sparse_map_type = variant_reader->find_subcolumn_tablet_indexes(
                make_subcolumn("v.__sparse", FieldType::OLAP_FIELD_TYPE_MAP, "v.__sparse",
                               root_unique_id),
                std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(),
                                              std::make_shared<DataTypeString>()));
        EXPECT_TRUE(sparse_map_type.empty());
    }

    TabletColumn indexed_subcolumn;
    indexed_subcolumn.set_name("own");
    indexed_subcolumn.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
    _tablet_schema->mutable_column_by_uid(root_column.unique_id())
            .add_sub_column(indexed_subcolumn);

    TabletIndexPB own_index_pb;
    construct_tablet_index(&own_index_pb, 10003, "idx_v_own", root_column.unique_id());
    (*own_index_pb.mutable_properties())["field_pattern"] = "own";
    TabletIndex own_index;
    own_index.init_from_pb(own_index_pb);
    _tablet_schema->append_index(std::move(own_index));

    {
        auto own = variant_reader->find_subcolumn_tablet_indexes(
                make_subcolumn("v.own", FieldType::OLAP_FIELD_TYPE_STRING, "v.own", root_unique_id),
                std::make_shared<DataTypeString>());
        ASSERT_EQ(own.size(), 1);
        EXPECT_EQ(own[0]->index_id(), 10003);
        EXPECT_EQ(own[0]->get_index_suffix(), "v%2Eown");
    }

    auto group_reader = std::make_unique<segment_v2::NestedGroupReader>();
    group_reader->array_path = "arr";
    group_reader->offsets_reader = std::make_shared<segment_v2::ColumnReader>();
    group_reader->child_readers.emplace("x", nullptr);
    auto& nested_group_readers =
            const_cast<segment_v2::NestedGroupReaders&>(variant_reader->get_nested_group_readers());
    nested_group_readers.emplace("arr", std::move(group_reader));

    {
        auto nested = variant_reader->find_subcolumn_tablet_indexes(
                make_subcolumn("v.arr.x", FieldType::OLAP_FIELD_TYPE_ARRAY, "v.arr.x",
                               root_unique_id),
                std::make_shared<DataTypeNullable>(
                        std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())));
        ASSERT_EQ(nested.size(), 1);
        EXPECT_EQ(nested[0]->index_id(), 10002);
        EXPECT_EQ(nested[0]->get_index_suffix(), "v%2Earr%2Ex");
        EXPECT_NE(nested[0]->get_index_suffix(), "arr%2Ex");
    }

    auto nested_group_reader = std::make_unique<segment_v2::NestedGroupReader>();
    nested_group_reader->array_path = "inner";
    nested_group_reader->offsets_reader = std::make_shared<segment_v2::ColumnReader>();
    nested_group_reader->child_readers.emplace("z", nullptr);
    nested_group_readers.at("arr")->nested_group_readers.emplace("inner",
                                                                 std::move(nested_group_reader));

    {
        auto nested = variant_reader->find_subcolumn_tablet_indexes(
                make_subcolumn("v.arr.inner.z", FieldType::OLAP_FIELD_TYPE_ARRAY, "v.arr.inner.z",
                               root_unique_id),
                std::make_shared<DataTypeNullable>(std::make_shared<DataTypeArray>(
                        std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()))));
        ASSERT_EQ(nested.size(), 1);
        EXPECT_EQ(nested[0]->index_id(), 10002);
        EXPECT_EQ(nested[0]->get_index_suffix(), "v%2Earr%2Einner%2Ez");
        EXPECT_NE(nested[0]->get_index_suffix(), "arr%2Einner%2Ez");
    }
}

TEST_F(VariantColumnWriterReaderTest, nullable_v2_chunk_uses_converter_adjusted_nullmap) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1", 10, false, true);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);
    _tablet_schema->set_storage_format(TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2);

    io::FileWriterPtr file_writer;
    const std::string file_path = _absolute_dir + "/nullable_v2_chunk.dat";
    ASSERT_TRUE(io::global_local_filesystem()->create_file(file_path, &file_writer).ok());

    SegmentFooterPB footer;
    RowsetWriterContext rowset_context;
    rowset_context.write_type = DataWriteType::TYPE_DIRECT;
    rowset_context.tablet_schema = _tablet_schema;
    ColumnWriterOptions options;
    options.meta = footer.add_columns();
    options.compression_type = CompressionTypePB::LZ4;
    options.file_writer = file_writer.get();
    options.footer = &footer;
    options.rowset_ctx = &rowset_context;
    options.storage_format = TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2;

    TabletColumn column = _tablet_schema->column(0);
    _init_column_meta(options.meta, 0, column, options);
    std::unique_ptr<ColumnWriter> writer;
    ASSERT_TRUE(ColumnWriter::create(options, &column, file_writer.get(), &writer).ok());
    ASSERT_TRUE(writer->init().ok());

    auto jsons = ColumnString::create();
    for (int row = 0; row < 10; ++row) {
        const std::string json = fmt::format(R"({{"row":{}}})", row);
        jsons->insert_data(json.data(), json.size());
    }
    auto nested = encode_json_column_v2(*jsons);
    auto nulls = ColumnUInt8::create();
    const std::vector<UInt8> source_nulls {0, 0, 0, 1, 1, 0, 1, 0, 0, 0};
    for (const UInt8 value : source_nulls) {
        nulls->insert_value(value);
    }
    Block block = _tablet_schema->create_block();
    block.replace_by_position(0, ColumnNullable::create(std::move(nested), std::move(nulls)));

    OlapBlockDataConvertor converter;
    converter.add_column_data_convertor(column);
    constexpr size_t kRowPos = 2;
    constexpr size_t kNumRows = 3;
    converter.set_source_content(&block, kRowPos, kNumRows);
    auto [convert_status, accessor] = converter.convert_column_data(0);
    ASSERT_TRUE(convert_status.ok()) << convert_status;
    ASSERT_NE(accessor, nullptr);
    ASSERT_NE(accessor->get_nullmap(), nullptr);
    ASSERT_EQ(accessor->get_nullmap()[0], source_nulls[kRowPos]);
    ASSERT_EQ(accessor->get_nullmap()[1], source_nulls[kRowPos + 1]);
    ASSERT_EQ(accessor->get_nullmap()[2], source_nulls[kRowPos + 2]);

    ASSERT_TRUE(writer->append(accessor->get_nullmap(), accessor->get_data(), kNumRows).ok());
    ASSERT_TRUE(writer->finish().ok());
    ASSERT_TRUE(writer->write_data().ok());
    ASSERT_TRUE(writer->write_ordinal_index().ok());
    ASSERT_TRUE(writer->write_zone_map().ok());
    ASSERT_TRUE(file_writer->close().ok());
    footer.set_num_rows(kNumRows);

    io::FileReaderSPtr file_reader;
    ASSERT_TRUE(io::global_local_filesystem()->open_file(file_path, &file_reader).ok());
    std::shared_ptr<ColumnReader> column_reader;
    ASSERT_TRUE(
            create_variant_root_reader(footer, file_reader, _tablet_schema, &column_reader).ok());
    const auto* variant_reader = assert_cast<const VariantColumnReader*>(column_reader.get());
    ASSERT_NE(variant_reader, nullptr);
    const auto& root_reader = variant_reader->get_root_column_reader();
    ASSERT_NE(root_reader, nullptr);
    ColumnIteratorUPtr iterator = std::make_unique<FileColumnIterator>(root_reader);
    OlapReaderStatistics stats;
    ColumnIteratorOptions iterator_options;
    iterator_options.file_reader = file_reader.get();
    iterator_options.stats = &stats;
    ASSERT_TRUE(iterator->init(iterator_options).ok());
    ASSERT_TRUE(iterator->seek_to_ordinal(0).ok());
    MutableColumnPtr root = make_nullable(std::make_shared<DataTypeJsonb>())->create_column();
    size_t rows = kNumRows;
    ASSERT_TRUE(iterator->next_batch(&rows, root).ok());
    ASSERT_EQ(rows, kNumRows);
    const auto& actual_nulls = assert_cast<const ColumnNullable&>(*root).get_null_map_data();
    EXPECT_EQ(actual_nulls[0], source_nulls[kRowPos]);
    EXPECT_EQ(actual_nulls[1], source_nulls[kRowPos + 1]);
    EXPECT_EQ(actual_nulls[2], source_nulls[kRowPos + 2]);
}

TEST_F(VariantColumnWriterReaderTest, test_write_data_nullable) {
    // 1. create tablet_schema
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    // make nullable tablet_column
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1", 10, true, true);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    // 2. create tablet
    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    bool external_segment_meta_used_default = rand() % 2 == 0;
    std::cout << "external_segment_meta_used_default: " << external_segment_meta_used_default
              << std::endl;
    _tablet_schema->set_storage_format(external_segment_meta_used_default
                                               ? TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V3
                                               : TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2);
    tablet_meta->_tablet_id = 10000;
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    EXPECT_TRUE(_tablet->init().ok());
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    // 3. create file_writer
    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    EXPECT_TRUE(st.ok()) << st.msg();

    // 4. create column_writer
    SegmentFooterPB footer;
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    opts.rowset_ctx = &rowset_ctx;
    opts.rowset_ctx->tablet_schema = _tablet_schema;
    // nullable variant column
    TabletColumn column = _tablet_schema->column(0);
    opts.compression_type = CompressionTypePB::LZ4;
    opts.storage_format = TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2;
    _init_column_meta(opts.meta, 0, column, opts);

    std::unique_ptr<ColumnWriter> writer;
    EXPECT_TRUE(ColumnWriter::create(opts, &column, file_writer.get(), &writer).ok());
    EXPECT_TRUE(writer->init().ok());
    EXPECT_TRUE(assert_cast<VariantColumnWriter*>(writer.get()) != nullptr);

    // 5. write data
    auto olap_data_convertor = std::make_unique<OlapBlockDataConvertor>();
    // here is nullable variant
    auto block = _tablet_schema->create_block();
    std::unordered_map<int, std::string> inserted_jsonstr;
    variant_util::PathToNoneNullValues path_with_size;
    fill_nullable_variant_block(&block, &inserted_jsonstr, &path_with_size);
    // sort path_with_size with value
    olap_data_convertor->add_column_data_convertor(column);
    olap_data_convertor->set_source_content(&block, 0, 1000);
    auto [result, accessor] = olap_data_convertor->convert_column_data(0);
    EXPECT_TRUE(result.ok());
    EXPECT_TRUE(accessor != nullptr);
    // variant do not implement append_nulls
    auto* vw = assert_cast<VariantColumnWriter*>(writer.get());
    const auto* ptr = (const uint8_t*)accessor->get_data();
    st = vw->append_nullable(accessor->get_nullmap(), &ptr, 1000);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_EQ(vw->get_next_rowid(), 1000);
    st = vw->finish();
    EXPECT_TRUE(st.ok()) << st.msg();
    auto size = vw->estimate_buffer_size();
    std::cout << "size: " << size << std::endl;
    st = vw->write_data();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = vw->write_ordinal_index();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = vw->write_zone_map();
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(file_writer->close().ok());
    footer.set_num_rows(1000);

    // 6. check footer
    EXPECT_EQ(footer.columns_size(), 12);
    auto column_meta = footer.columns(0);
    EXPECT_EQ(column_meta.type(), (int)FieldType::OLAP_FIELD_TYPE_VARIANT);

    for (int i = 1; i < footer.columns_size() - 1; ++i) {
        auto column_meta = footer.columns(i);
        EXPECT_TRUE(column_meta.has_column_path_info());
        auto path = std::make_shared<PathInData>();
        EXPECT_EQ(column_meta.column_path_info().parrent_column_unique_id(), 1);
        EXPECT_GT(column_meta.none_null_size(), path_with_size[path->copy_pop_front().get_path()]);
    }
    check_sparse_column_meta(footer.columns(footer.columns_size() - 1), path_with_size);

    // 7. check variant reader
    io::FileReaderSPtr file_reader;
    st = io::global_local_filesystem()->open_file(file_path, &file_reader);
    EXPECT_TRUE(st.ok()) << st.msg();
    std::shared_ptr<ColumnReader> column_reader;
    st = create_variant_root_reader(footer, file_reader, _tablet_schema, &column_reader);
    EXPECT_TRUE(st.ok()) << st.msg();

    auto variant_column_reader = assert_cast<VariantColumnReader*>(column_reader.get());
    EXPECT_TRUE(variant_column_reader != nullptr);

    // 8. check statistics
    auto statistics = variant_column_reader->get_stats();
    for (const auto& [path, size] : statistics->subcolumns_non_null_size) {
        EXPECT_GT(size, path_with_size[path]);
    }
    for (const auto& [path, size] : statistics->sparse_column_non_null_size) {
        EXPECT_EQ(path_with_size[path], size);
    }

    MockColumnReaderCache column_reader_cache(footer, file_reader, _tablet_schema);

    // 9. check root
    ColumnIteratorUPtr it;
    TabletColumn parent_column = _tablet_schema->column(0);
    StorageReadOptions storage_read_opts;
    OlapReaderStatistics stats;
    storage_read_opts.stats = &stats;
    storage_read_opts.io_ctx.reader_type = ReaderType::READER_QUERY;
    st = variant_column_reader->new_iterator(&it, &parent_column, &storage_read_opts,
                                             &column_reader_cache);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(assert_cast<HierarchicalDataIterator*>(it.get()) != nullptr);
    ColumnIteratorOptions column_iter_opts;
    column_iter_opts.stats = &stats;
    column_iter_opts.file_reader = file_reader.get();
    st = it->init(column_iter_opts);
    EXPECT_TRUE(st.ok()) << st.msg();

    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
}

TEST_F(VariantColumnWriterReaderTest, test_write_data_nullable_without_finalize) {
    // 1. create tablet_schema
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    // make nullable tablet_column
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1", 10, true, true);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    // 2. create tablet
    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    bool external_segment_meta_used_default = rand() % 2 == 0;
    std::cout << "external_segment_meta_used_default: " << external_segment_meta_used_default
              << std::endl;
    _tablet_schema->set_storage_format(external_segment_meta_used_default
                                               ? TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V3
                                               : TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2);
    tablet_meta->_tablet_id = 10000;
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    EXPECT_TRUE(_tablet->init().ok());
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    // 3. create file_writer
    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    EXPECT_TRUE(st.ok()) << st.msg();

    // 4. create column_writer
    SegmentFooterPB footer;
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    opts.rowset_ctx = &rowset_ctx;
    opts.rowset_ctx->tablet_schema = _tablet_schema;
    // nullable variant column
    TabletColumn column = _tablet_schema->column(0);
    opts.compression_type = CompressionTypePB::LZ4;
    opts.storage_format = TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2;
    _init_column_meta(opts.meta, 0, column, opts);

    std::unique_ptr<ColumnWriter> writer;
    EXPECT_TRUE(ColumnWriter::create(opts, &column, file_writer.get(), &writer).ok());
    EXPECT_TRUE(writer->init().ok());
    EXPECT_TRUE(assert_cast<VariantColumnWriter*>(writer.get()) != nullptr);

    // 5. write data
    auto olap_data_convertor = std::make_unique<OlapBlockDataConvertor>();
    // here is nullable variant
    auto block = _tablet_schema->create_block();
    std::unordered_map<int, std::string> inserted_jsonstr;
    variant_util::PathToNoneNullValues path_with_size;
    fill_nullable_variant_block(&block, &inserted_jsonstr, &path_with_size);
    // sort path_with_size with value
    olap_data_convertor->add_column_data_convertor(column);
    olap_data_convertor->set_source_content(&block, 0, 1000);
    auto [result, accessor] = olap_data_convertor->convert_column_data(0);
    EXPECT_TRUE(result.ok());
    EXPECT_TRUE(accessor != nullptr);
    // variant do not implement append_nulls
    auto* vw = assert_cast<VariantColumnWriter*>(writer.get());
    const auto* ptr = (const uint8_t*)accessor->get_data();
    st = vw->append_nullable(accessor->get_nullmap(), &ptr, 1000);
    EXPECT_TRUE(st.ok()) << st.msg();
    st = vw->write_data();
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(file_writer->close().ok());
    footer.set_num_rows(1000);

    // 6. check footer
    EXPECT_EQ(footer.columns_size(), 12);
    auto column_meta = footer.columns(0);
    EXPECT_EQ(column_meta.type(), (int)FieldType::OLAP_FIELD_TYPE_VARIANT);
}

TEST_F(VariantColumnWriterReaderTest, test_write_bm_with_finalize) {
    // 1. create tablet_schema
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    // make nullable tablet_column
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1", 10, true, true);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    // 2. create tablet
    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    bool external_segment_meta_used_default = rand() % 2 == 0;
    std::cout << "external_segment_meta_used_default: " << external_segment_meta_used_default
              << std::endl;
    _tablet_schema->set_storage_format(external_segment_meta_used_default
                                               ? TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V3
                                               : TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2);
    tablet_meta->_tablet_id = 10000;
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    EXPECT_TRUE(_tablet->init().ok());
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    // 3. create file_writer
    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    EXPECT_TRUE(st.ok()) << st.msg();

    // 4. create column_writer
    SegmentFooterPB footer;
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    opts.rowset_ctx = &rowset_ctx;
    opts.rowset_ctx->tablet_schema = _tablet_schema;
    // nullable variant column
    TabletColumn column = _tablet_schema->column(0);
    opts.compression_type = CompressionTypePB::LZ4;
    opts.storage_format = TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2;
    _init_column_meta(opts.meta, 0, column, opts);

    std::unique_ptr<ColumnWriter> writer;
    EXPECT_TRUE(ColumnWriter::create(opts, &column, file_writer.get(), &writer).ok());
    EXPECT_TRUE(writer->init().ok());
    EXPECT_TRUE(assert_cast<VariantColumnWriter*>(writer.get()) != nullptr);

    // 5. write data
    auto olap_data_convertor = std::make_unique<OlapBlockDataConvertor>();
    // here is nullable variant
    auto block = _tablet_schema->create_block();
    std::unordered_map<int, std::string> inserted_jsonstr;
    variant_util::PathToNoneNullValues path_with_size;
    fill_nullable_variant_block(&block, &inserted_jsonstr, &path_with_size);
    // sort path_with_size with value
    olap_data_convertor->add_column_data_convertor(column);
    olap_data_convertor->set_source_content(&block, 0, 1000);
    auto [result, accessor] = olap_data_convertor->convert_column_data(0);
    EXPECT_TRUE(result.ok());
    EXPECT_TRUE(accessor != nullptr);
    // variant do not implement append_nulls
    auto* vw = assert_cast<VariantColumnWriter*>(writer.get());
    const auto* ptr = (const uint8_t*)accessor->get_data();
    st = vw->append_nullable(accessor->get_nullmap(), &ptr, 1000);
    EXPECT_TRUE(st.ok()) << st.msg();
    st = vw->_impl->finalize();
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(file_writer->close().ok());
    footer.set_num_rows(1000);

    // 6. check footer
    EXPECT_EQ(footer.columns_size(), 12);
    auto column_meta = footer.columns(0);
    EXPECT_EQ(column_meta.type(), (int)FieldType::OLAP_FIELD_TYPE_VARIANT);
}

TEST_F(VariantColumnWriterReaderTest, test_write_bf_with_finalize) {
    // 1. create tablet_schema
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    // make nullable tablet_column
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1", 10, true, true);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    // 2. create tablet
    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    bool external_segment_meta_used_default = rand() % 2 == 0;
    std::cout << "external_segment_meta_used_default: " << external_segment_meta_used_default
              << std::endl;
    _tablet_schema->set_storage_format(external_segment_meta_used_default
                                               ? TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V3
                                               : TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2);
    tablet_meta->_tablet_id = 10000;
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    EXPECT_TRUE(_tablet->init().ok());
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    // 3. create file_writer
    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    EXPECT_TRUE(st.ok()) << st.msg();

    // 4. create column_writer
    SegmentFooterPB footer;
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    opts.rowset_ctx = &rowset_ctx;
    opts.rowset_ctx->tablet_schema = _tablet_schema;
    // nullable variant column
    TabletColumn column = _tablet_schema->column(0);
    opts.compression_type = CompressionTypePB::LZ4;
    opts.storage_format = TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2;
    _init_column_meta(opts.meta, 0, column, opts);

    std::unique_ptr<ColumnWriter> writer;
    EXPECT_TRUE(ColumnWriter::create(opts, &column, file_writer.get(), &writer).ok());
    EXPECT_TRUE(writer->init().ok());
    EXPECT_TRUE(assert_cast<VariantColumnWriter*>(writer.get()) != nullptr);

    // 5. write data
    auto olap_data_convertor = std::make_unique<OlapBlockDataConvertor>();
    // here is nullable variant
    auto block = _tablet_schema->create_block();
    std::unordered_map<int, std::string> inserted_jsonstr;
    variant_util::PathToNoneNullValues path_with_size;
    fill_nullable_variant_block(&block, &inserted_jsonstr, &path_with_size);
    // sort path_with_size with value
    olap_data_convertor->add_column_data_convertor(column);
    olap_data_convertor->set_source_content(&block, 0, 1000);
    auto [result, accessor] = olap_data_convertor->convert_column_data(0);
    EXPECT_TRUE(result.ok());
    EXPECT_TRUE(accessor != nullptr);
    // variant do not implement append_nulls
    auto* vw = assert_cast<VariantColumnWriter*>(writer.get());
    const auto* ptr = (const uint8_t*)accessor->get_data();
    st = vw->append_nullable(accessor->get_nullmap(), &ptr, 1000);
    EXPECT_TRUE(st.ok()) << st.msg();
    st = vw->_impl->finalize();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = vw->write_bloom_filter_index();
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(file_writer->close().ok());
    footer.set_num_rows(1000);

    // 6. check footer
    EXPECT_EQ(footer.columns_size(), 12);
    auto column_meta = footer.columns(0);
    EXPECT_EQ(column_meta.type(), (int)FieldType::OLAP_FIELD_TYPE_VARIANT);
}

TEST_F(VariantColumnWriterReaderTest, test_write_zm_with_finalize) {
    // 1. create tablet_schema
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    // make nullable tablet_column
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1", 10, true, true);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    // 2. create tablet
    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    bool external_segment_meta_used_default = rand() % 2 == 0;
    std::cout << "external_segment_meta_used_default: " << external_segment_meta_used_default
              << std::endl;
    _tablet_schema->set_storage_format(external_segment_meta_used_default
                                               ? TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V3
                                               : TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2);
    tablet_meta->_tablet_id = 10000;
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    EXPECT_TRUE(_tablet->init().ok());
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    // 3. create file_writer
    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    EXPECT_TRUE(st.ok()) << st.msg();

    // 4. create column_writer
    SegmentFooterPB footer;
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    opts.rowset_ctx = &rowset_ctx;
    opts.rowset_ctx->tablet_schema = _tablet_schema;
    // nullable variant column
    TabletColumn column = _tablet_schema->column(0);
    opts.compression_type = CompressionTypePB::LZ4;
    opts.storage_format = TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2;
    _init_column_meta(opts.meta, 0, column, opts);

    std::unique_ptr<ColumnWriter> writer;
    EXPECT_TRUE(ColumnWriter::create(opts, &column, file_writer.get(), &writer).ok());
    EXPECT_TRUE(writer->init().ok());
    EXPECT_TRUE(assert_cast<VariantColumnWriter*>(writer.get()) != nullptr);

    // 5. write data
    auto olap_data_convertor = std::make_unique<OlapBlockDataConvertor>();
    // here is nullable variant
    auto block = _tablet_schema->create_block();
    std::unordered_map<int, std::string> inserted_jsonstr;
    variant_util::PathToNoneNullValues path_with_size;
    fill_nullable_variant_block(&block, &inserted_jsonstr, &path_with_size);
    // sort path_with_size with value
    olap_data_convertor->add_column_data_convertor(column);
    olap_data_convertor->set_source_content(&block, 0, 1000);
    auto [result, accessor] = olap_data_convertor->convert_column_data(0);
    EXPECT_TRUE(result.ok());
    EXPECT_TRUE(accessor != nullptr);
    // variant do not implement append_nulls
    auto* vw = assert_cast<VariantColumnWriter*>(writer.get());
    const auto* ptr = (const uint8_t*)accessor->get_data();
    st = vw->append_nullable(accessor->get_nullmap(), &ptr, 1000);
    EXPECT_TRUE(st.ok()) << st.msg();
    st = vw->_impl->finalize();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = vw->write_zone_map();
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(file_writer->close().ok());
    footer.set_num_rows(1000);

    // 6. check footer
    EXPECT_EQ(footer.columns_size(), 12);
    auto column_meta = footer.columns(0);
    EXPECT_EQ(column_meta.type(), (int)FieldType::OLAP_FIELD_TYPE_VARIANT);
}

TEST_F(VariantColumnWriterReaderTest, test_write_inverted_with_finalize) {
    // 1. create tablet_schema
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    // make nullable tablet_column
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1", 10, true, true);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    // 2. create tablet
    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    bool external_segment_meta_used_default = rand() % 2 == 0;
    std::cout << "external_segment_meta_used_default: " << external_segment_meta_used_default
              << std::endl;
    _tablet_schema->set_storage_format(external_segment_meta_used_default
                                               ? TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V3
                                               : TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2);
    tablet_meta->_tablet_id = 10000;
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    EXPECT_TRUE(_tablet->init().ok());
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    // 3. create file_writer
    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    EXPECT_TRUE(st.ok()) << st.msg();

    // 4. create column_writer
    SegmentFooterPB footer;
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    opts.rowset_ctx = &rowset_ctx;
    opts.rowset_ctx->tablet_schema = _tablet_schema;
    // nullable variant column
    TabletColumn column = _tablet_schema->column(0);
    opts.compression_type = CompressionTypePB::LZ4;
    opts.storage_format = TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2;
    _init_column_meta(opts.meta, 0, column, opts);

    std::unique_ptr<ColumnWriter> writer;
    EXPECT_TRUE(ColumnWriter::create(opts, &column, file_writer.get(), &writer).ok());
    EXPECT_TRUE(writer->init().ok());
    EXPECT_TRUE(assert_cast<VariantColumnWriter*>(writer.get()) != nullptr);

    // 5. write data
    auto olap_data_convertor = std::make_unique<OlapBlockDataConvertor>();
    // here is nullable variant
    auto block = _tablet_schema->create_block();
    std::unordered_map<int, std::string> inserted_jsonstr;
    variant_util::PathToNoneNullValues path_with_size;
    fill_nullable_variant_block(&block, &inserted_jsonstr, &path_with_size);
    // sort path_with_size with value
    olap_data_convertor->add_column_data_convertor(column);
    olap_data_convertor->set_source_content(&block, 0, 1000);
    auto [result, accessor] = olap_data_convertor->convert_column_data(0);
    EXPECT_TRUE(result.ok());
    EXPECT_TRUE(accessor != nullptr);
    // variant do not implement append_nulls
    auto* vw = assert_cast<VariantColumnWriter*>(writer.get());
    const auto* ptr = (const uint8_t*)accessor->get_data();
    st = vw->append_nullable(accessor->get_nullmap(), &ptr, 1000);
    EXPECT_TRUE(st.ok()) << st.msg();
    st = vw->_impl->finalize();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = vw->write_inverted_index();
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(file_writer->close().ok());
    footer.set_num_rows(1000);

    // 6. check footer
    EXPECT_EQ(footer.columns_size(), 12);
    auto column_meta = footer.columns(0);
    EXPECT_EQ(column_meta.type(), (int)FieldType::OLAP_FIELD_TYPE_VARIANT);
}

TEST_F(VariantColumnWriterReaderTest, test_no_sub_in_sparse_column) {
    // 1. create tablet_schema
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1");
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    // 2. create tablet
    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    bool external_segment_meta_used_default = rand() % 2 == 0;
    std::cout << "external_segment_meta_used_default: " << external_segment_meta_used_default
              << std::endl;
    _tablet_schema->set_storage_format(external_segment_meta_used_default
                                               ? TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V3
                                               : TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2);
    tablet_meta->_tablet_id = 10001;
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    EXPECT_TRUE(_tablet->init().ok());
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    // 3. create file_writer
    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    EXPECT_TRUE(st.ok()) << st.msg();

    // 4. create column_writer
    SegmentFooterPB footer;
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    opts.rowset_ctx = &rowset_ctx;
    opts.rowset_ctx->tablet_schema = _tablet_schema;
    TabletColumn column = _tablet_schema->column(0);
    opts.compression_type = CompressionTypePB::LZ4;
    opts.storage_format = TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2;
    _init_column_meta(opts.meta, 0, column, opts);

    std::unique_ptr<ColumnWriter> writer;
    EXPECT_TRUE(ColumnWriter::create(opts, &column, file_writer.get(), &writer).ok());
    EXPECT_TRUE(writer->init().ok());
    EXPECT_TRUE(assert_cast<VariantColumnWriter*>(writer.get()) != nullptr);

    // 5. write data
    auto olap_data_convertor = std::make_unique<OlapBlockDataConvertor>();
    auto block = _tablet_schema->create_block();
    auto type_string = std::make_shared<DataTypeString>();
    auto json_column = type_string->create_column();
    auto column_string = assert_cast<ColumnString*>(json_column.get());
    // for some test data in json string to insert variant column
    // make list for json string
    for (int i = 0; i < 1000; ++i) {
        std::string inserted_jsonstr =
                (R"({"a": {"b": )" + std::to_string(i) + R"(, "c": )" + std::to_string(i) +
                 R"(}, "d": )" + std::to_string(i) + R"(})");
        // insert json string to variant column
        column_string->insert_data(inserted_jsonstr.data(), inserted_jsonstr.size());
    }

    block.replace_by_position(0, encode_json_column_v2(*column_string));

    olap_data_convertor->add_column_data_convertor(column);
    olap_data_convertor->set_source_content(&block, 0, 1000);
    auto [result, accessor] = olap_data_convertor->convert_column_data(0);
    EXPECT_TRUE(result.ok());
    EXPECT_TRUE(accessor != nullptr);
    EXPECT_TRUE(writer->append(accessor->get_nullmap(), accessor->get_data(), 1000).ok());
    st = writer->finish();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_data();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_ordinal_index();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_zone_map();
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(file_writer->close().ok());
    footer.set_num_rows(1000);

    // check footer
    EXPECT_EQ(footer.columns_size(), 5);
    auto column_meta = footer.columns(0);
    EXPECT_EQ(column_meta.type(), (int)FieldType::OLAP_FIELD_TYPE_VARIANT);

    // 6. create reader
    io::FileReaderSPtr file_reader;
    st = io::global_local_filesystem()->open_file(file_path, &file_reader);
    EXPECT_TRUE(st.ok()) << st.msg();

    std::shared_ptr<ColumnReader> reader;
    st = create_variant_root_reader(footer, file_reader, _tablet_schema, &reader);
    EXPECT_TRUE(st.ok()) << st.msg();
    auto variant_column_reader = assert_cast<VariantColumnReader*>(reader.get());
    EXPECT_TRUE(variant_column_reader != nullptr);

    // 7. test exist_in_sparse_column
    auto* variant_reader = assert_cast<VariantColumnReader*>(reader.get());
    PathInData non_existent_path("non.existent.path");
    EXPECT_FALSE(variant_reader->exist_in_sparse_column(non_existent_path));

    // 8. test prefix_exist_in_sparse_column = true which means we have prefix in sparse column
    for (auto& path : variant_reader->get_stats()->sparse_column_non_null_size) {
        std::cout << "sparse_column_non_null_size path: " << path.first << ", size: " << path.second
                  << std::endl;
    }
    for (auto& path : variant_reader->get_stats()->subcolumns_non_null_size) {
        std::cout << "subcolumns_non_null_size path: " << path.first << ", size: " << path.second
                  << std::endl;
    }
    PathInData prefix_path("a");
    EXPECT_FALSE(variant_reader->exist_in_sparse_column(prefix_path));

    // 9. test get_metadata_size with null statistics
    EXPECT_GT(variant_reader->get_metadata_size(), 0);

    MockColumnReaderCache column_reader_cache(footer, file_reader, _tablet_schema);

    // 10. test hierarchical reader with empty statistics
    ColumnIteratorUPtr iterator;
    StorageReadOptions read_opts;
    OlapReaderStatistics stats;
    read_opts.stats = &stats;
    st = variant_reader->new_iterator(&iterator, &column, &read_opts, &column_reader_cache);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(iterator != nullptr);
}

TEST_F(VariantColumnWriterReaderTest, test_prefix_in_sub_and_sparse) {
    // 1. create tablet_schema
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1");
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    // 2. create tablet
    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    bool external_segment_meta_used_default = rand() % 2 == 0;
    std::cout << "external_segment_meta_used_default: " << external_segment_meta_used_default
              << std::endl;
    _tablet_schema->set_storage_format(external_segment_meta_used_default
                                               ? TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V3
                                               : TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2);
    tablet_meta->_tablet_id = 10001;
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    EXPECT_TRUE(_tablet->init().ok());
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    // 3. create file_writer
    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    EXPECT_TRUE(st.ok()) << st.msg();

    // 4. create column_writer
    SegmentFooterPB footer;
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    opts.rowset_ctx = &rowset_ctx;
    opts.rowset_ctx->tablet_schema = _tablet_schema;
    TabletColumn column = _tablet_schema->column(0);
    opts.compression_type = CompressionTypePB::LZ4;
    opts.storage_format = TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2;
    _init_column_meta(opts.meta, 0, column, opts);

    std::unique_ptr<ColumnWriter> writer;
    EXPECT_TRUE(ColumnWriter::create(opts, &column, file_writer.get(), &writer).ok());
    EXPECT_TRUE(writer->init().ok());
    EXPECT_TRUE(assert_cast<VariantColumnWriter*>(writer.get()) != nullptr);

    // 5. write data
    auto olap_data_convertor = std::make_unique<OlapBlockDataConvertor>();
    auto block = _tablet_schema->create_block();
    auto type_string = std::make_shared<DataTypeString>();
    auto json_column = type_string->create_column();
    auto column_string = assert_cast<ColumnString*>(json_column.get());
    // for some test data in json string to insert variant column
    // insert some test data to json string
    for (int i = 0; i < 1000; ++i) {
        std::string inserted_jsonstr =
                (R"({"a": {"b": )" + std::to_string(i) + R"(, "c": )" + std::to_string(i) +
                 R"(}, "d": )" + std::to_string(i) + R"(})");
        // add some rand key for sparse column with 'a.b' prefix : {"a": {"b": 1, "c": 1, "e": 1}, "d": 1}
        if (i % 17 == 0) {
            inserted_jsonstr =
                    (R"({"a": {"b": )" + std::to_string(i) + R"(, "c": )" + std::to_string(i) +
                     R"(, "e": )" + std::to_string(i) + R"(}, "d": )" + std::to_string(i) + R"(})");
        }
        // add some rand key for spare column without prefix: {"a": {"b": 1, "c": 1}, "d": 1, "e": 1}
        if (i % 177 == 0) {
            inserted_jsonstr =
                    (R"({"a": {"b": )" + std::to_string(i) + R"(, "c": )" + std::to_string(i) +
                     R"(}, "d": )" + std::to_string(i) + R"(, "e": )" + std::to_string(i) + R"(})");
        }
        // insert json string to variant column
        column_string->insert_data(inserted_jsonstr.data(), inserted_jsonstr.size());
    }

    block.replace_by_position(0, encode_json_column_v2(*column_string));

    olap_data_convertor->add_column_data_convertor(column);
    olap_data_convertor->set_source_content(&block, 0, 1000);
    auto [result, accessor] = olap_data_convertor->convert_column_data(0);
    EXPECT_TRUE(result.ok());
    EXPECT_TRUE(accessor != nullptr);
    EXPECT_TRUE(writer->append(accessor->get_nullmap(), accessor->get_data(), 1000).ok());
    st = writer->finish();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_data();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_ordinal_index();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_zone_map();
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(file_writer->close().ok());
    footer.set_num_rows(1000);

    // check footer
    EXPECT_EQ(footer.columns_size(), 5);
    auto column_meta = footer.columns(0);
    EXPECT_EQ(column_meta.type(), (int)FieldType::OLAP_FIELD_TYPE_VARIANT);

    // 6. create reader
    io::FileReaderSPtr file_reader;
    st = io::global_local_filesystem()->open_file(file_path, &file_reader);
    EXPECT_TRUE(st.ok()) << st.msg();

    std::shared_ptr<ColumnReader> reader;
    st = create_variant_root_reader(footer, file_reader, _tablet_schema, &reader);
    EXPECT_TRUE(st.ok()) << st.msg();
    auto variant_column_reader = assert_cast<VariantColumnReader*>(reader.get());
    EXPECT_TRUE(variant_column_reader != nullptr);

    // 7. test exist_in_sparse_column
    auto* variant_reader = assert_cast<VariantColumnReader*>(reader.get());
    PathInData non_existent_path("non.existent.path");
    EXPECT_FALSE(variant_reader->exist_in_sparse_column(non_existent_path));

    // 8. test prefix_exist_in_sparse_column = true which means we have prefix in sparse column
    for (auto& path : variant_reader->get_stats()->sparse_column_non_null_size) {
        std::cout << "sparse_column_non_null_size path: " << path.first << ", size: " << path.second
                  << std::endl;
    }
    for (auto& path : variant_reader->get_stats()->subcolumns_non_null_size) {
        std::cout << "subcolumns_non_null_size path: " << path.first << ", size: " << path.second
                  << std::endl;
    }
    PathInData prefix_path("a");
    EXPECT_TRUE(variant_reader->exist_in_sparse_column(prefix_path));

    // 9. test get_metadata_size with null statistics
    EXPECT_GT(variant_reader->get_metadata_size(), 0);

    MockColumnReaderCache column_reader_cache(footer, file_reader, _tablet_schema);

    // 10. test hierarchical reader with empty statistics
    ColumnIteratorUPtr iterator;
    StorageReadOptions read_opts;
    OlapReaderStatistics stats;
    read_opts.stats = &stats;
    st = variant_reader->new_iterator(&iterator, &column, &read_opts, &column_reader_cache);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(iterator != nullptr);
}

void test_write_variant_column(StorageEngine* _engine_ref, std::string _absolute_dir,
                               std::string& file_path, SegmentFooterPB& footer,
                               std::shared_ptr<TabletSchema> _tablet_schema,
                               bool nullable = false) {
    // 1. create tablet_schema
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "v", 3, false, nullable);
    _tablet_schema->init_from_pb(schema_pb);

    // 2. create tablet
    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    bool external_segment_meta_used_default = rand() % 2 == 0;
    std::cout << "external_segment_meta_used_default: " << external_segment_meta_used_default
              << std::endl;
    _tablet_schema->set_storage_format(external_segment_meta_used_default
                                               ? TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V3
                                               : TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2);
    tablet_meta->_tablet_id = 10000;
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_absolute_dir).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_absolute_dir).ok());
    std::unique_ptr<DataDir> _data_dir = std::make_unique<DataDir>(*_engine_ref, _absolute_dir);
    static_cast<void>(_data_dir->update_capacity());
    Status st1 = _data_dir->init(true);
    EXPECT_TRUE(st1.ok()) << st1.msg();
    std::shared_ptr<Tablet> _tablet =
            std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    EXPECT_TRUE(_tablet->init().ok());
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    // 3. create file_writer
    io::FileWriterPtr file_writer;
    file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    EXPECT_TRUE(st.ok()) << st.msg();

    // 4. create column_writer
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    opts.rowset_ctx = &rowset_ctx;
    opts.rowset_ctx->tablet_schema = _tablet_schema;
    TabletColumn tablet_column = _tablet_schema->column(0);
    opts.compression_type = CompressionTypePB::LZ4;
    opts.storage_format = TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2;
    _init_column_meta(opts.meta, 0, tablet_column, opts);

    std::unique_ptr<ColumnWriter> writer;
    EXPECT_TRUE(ColumnWriter::create(opts, &tablet_column, file_writer.get(), &writer).ok());
    EXPECT_TRUE(writer->init().ok());
    EXPECT_TRUE(assert_cast<VariantColumnWriter*>(writer.get()) != nullptr);

    // 5. make test data for column_object
    auto olap_data_convertor = std::make_unique<OlapBlockDataConvertor>();
    auto block = _tablet_schema->create_block();
    VariantUtil::VariantStringCreator simple_column_object = [](ColumnString* column_string,
                                                                size_t size) {
        for (size_t i = 0; i < size; ++i) {
            std::string inserted_jsonstr = make_nested_variant_row(i);
            column_string->insert_data(inserted_jsonstr.data(), inserted_jsonstr.size());
        }
    };
    auto json_column = ColumnString::create();
    simple_column_object(json_column.get(), 1000);
    MutableColumnPtr column_object = encode_json_column_v2(*json_column);
    if (nullable) {
        auto nulls = ColumnUInt8::create();
        nulls->insert_many_defaults(1000);
        column_object = ColumnNullable::create(std::move(column_object), std::move(nulls));
    }
    EXPECT_TRUE(column_object->size() == 1000);
    block.replace_by_position(0, std::move(column_object));
    olap_data_convertor->add_column_data_convertor(tablet_column);
    olap_data_convertor->set_source_content(&block, 0, 1000);
    auto [result, accessor] = olap_data_convertor->convert_column_data(0);
    EXPECT_TRUE(result.ok());
    EXPECT_TRUE(accessor != nullptr);
    EXPECT_TRUE(writer->append(accessor->get_nullmap(), accessor->get_data(), 1000).ok());
    st = writer->finish();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_data();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_ordinal_index();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_zone_map();
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(file_writer->close().ok());
    footer.set_num_rows(1000);

    // The V2 shredder materializes the complete array value at a.b. It must not restore the
    // legacy per-leaf a.b.c.d/e layout, and nested-group layout remains disabled.
    ASSERT_EQ(footer.columns_size(), 4);
    const auto& root_meta = footer.columns(0);
    EXPECT_EQ(root_meta.type(), static_cast<int>(FieldType::OLAP_FIELD_TYPE_VARIANT));
    EXPECT_EQ(root_meta.is_nullable(), nullable);
    ASSERT_TRUE(root_meta.has_column_path_info());
    PathInData root_path;
    root_path.from_protobuf(root_meta.column_path_info());
    EXPECT_EQ(root_path.get_path(), "v");

    const auto* array_meta = find_footer_column_meta_by_relative_path(footer, "a.b");
    ASSERT_NE(array_meta, nullptr);
    EXPECT_EQ(array_meta->type(), static_cast<int>(FieldType::OLAP_FIELD_TYPE_ARRAY));
    EXPECT_TRUE(array_meta->is_nullable());
    EXPECT_EQ(array_meta->none_null_size(), 1000);
    ASSERT_EQ(array_meta->children_columns_size(), 3);
    EXPECT_EQ(array_meta->children_columns(0).type(),
              static_cast<int>(FieldType::OLAP_FIELD_TYPE_JSONB));

    const auto* string_meta = find_footer_column_meta_by_relative_path(footer, "x");
    ASSERT_NE(string_meta, nullptr);
    EXPECT_EQ(string_meta->type(), static_cast<int>(FieldType::OLAP_FIELD_TYPE_STRING));
    EXPECT_TRUE(string_meta->is_nullable());
    EXPECT_EQ(string_meta->none_null_size(), 1000);

    const auto* sparse_meta = find_footer_column_meta_by_relative_path(footer, SPARSE_COLUMN_PATH);
    ASSERT_NE(sparse_meta, nullptr);
    EXPECT_EQ(sparse_meta->type(), static_cast<int>(FieldType::OLAP_FIELD_TYPE_MAP));
    EXPECT_FALSE(sparse_meta->is_nullable());

    for (const auto& column_meta : footer.columns()) {
        EXPECT_FALSE(column_meta.variant_statistics().has_nested_group());
        if (column_meta.has_column_path_info()) {
            PathInData path;
            path.from_protobuf(column_meta.column_path_info());
            EXPECT_FALSE(segment_v2::contains_nested_group_marker(path.get_path()));
        }
    }
}

TEST_F(VariantColumnWriterReaderTest, test_nested_subcolumn) {
    // write data
    std::string absolute_dir = _current_dir + std::string("/ut_dir/variant_test_nested_subcolumn");
    // declare file_path and footer
    std::string file_path;
    SegmentFooterPB footer;
    std::shared_ptr<TabletSchema> _tablet_schema = std::make_shared<TabletSchema>();
    test_write_variant_column(_engine_ref, absolute_dir, file_path, footer, _tablet_schema);
    // reader data
    // check variant reader
    io::FileReaderSPtr file_reader;
    Status st = io::global_local_filesystem()->open_file(file_path, &file_reader);
    EXPECT_TRUE(st.ok()) << st.msg();
    std::shared_ptr<ColumnReader> column_reader;
    st = create_variant_root_reader(footer, file_reader, _tablet_schema, &column_reader);
    EXPECT_TRUE(st.ok()) << st.msg();

    auto variant_column_reader = assert_cast<VariantColumnReader*>(column_reader.get());
    EXPECT_TRUE(variant_column_reader != nullptr);
    // test read situation for compaction with should flat all sub column
    EXPECT_FALSE(variant_column_reader->get_subcolumns_meta_info()->empty());

    // create a nested column array<struct> which not exists in subcolumn
    TabletColumn struct_column;
    struct_column.set_name("b");
    struct_column.set_type(FieldType::OLAP_FIELD_TYPE_STRUCT);
    TabletColumn int_column;
    int_column.set_name("i");
    int_column.set_type(FieldType::OLAP_FIELD_TYPE_INT);
    TabletColumn string_column;
    string_column.set_name("s");
    string_column.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
    struct_column.add_sub_column(int_column);
    struct_column.add_sub_column(string_column);

    TabletColumn target_column;
    target_column.set_name("a");
    target_column.set_type(FieldType::OLAP_FIELD_TYPE_ARRAY);
    target_column.add_sub_column(struct_column);

    // {"a" : {"b" : [{"i" : 1, "s": "abs"}]}}
    // DefaultNestedColumnIterator with sibling_iter
    PathInDataBuilder builder;
    builder.append("v", false); // First part is variant
    builder.append("a", false); //  Second part is struct
    builder.append("b", false); // Third part is struct
    builder.append("i", true); // Fourth part is int as array for b.i array<int> , b.s array<string>
    // this will be a.b.i and a.b.s
    PathInData path = builder.build();
    EXPECT_TRUE(path.has_nested_part());
    target_column.set_path_info(path);
    EXPECT_TRUE(target_column.is_nested_subcolumn())
            << target_column._column_path->has_nested_part();

    StorageReadOptions storageReadOptions;
    storageReadOptions.io_ctx.reader_type = ReaderType::READER_CUMULATIVE_COMPACTION;

    // DefaultNestedColumnIterator with nullptr parameter
    PathInDataBuilder builder1;
    builder1.append("v", false); // First part is variant
    builder1.append("v", false); // First part is variant
    builder1.append("a", false); //  Second part is struct
    builder1.append("b", false); // Third part is struct
    builder1.append("i",
                    true); // Fourth part is int as array for b.i array<int> , b.s array<string>
    // this will be a.b.i and a.b.s
    PathInData path1 = builder1.build();
    EXPECT_TRUE(path1.has_nested_part());
    target_column.set_path_info(path1);
    EXPECT_TRUE(target_column.is_nested_subcolumn())
            << target_column._column_path->has_nested_part();
}

TEST_F(VariantColumnWriterReaderTest, test_nested_iter) {
    // write data
    std::string absolute_dir = _current_dir + std::string("/ut_dir/variant_test_nested_iter");
    // declare file_path and footer
    std::string file_path;
    SegmentFooterPB footer;
    std::shared_ptr<TabletSchema> _tablet_schema = std::make_shared<TabletSchema>();
    test_write_variant_column(_engine_ref, absolute_dir, file_path, footer, _tablet_schema);
    // reader data
    // check variant reader
    io::FileReaderSPtr file_reader;
    Status st = io::global_local_filesystem()->open_file(file_path, &file_reader);
    EXPECT_TRUE(st.ok()) << st.msg();
    std::shared_ptr<ColumnReader> column_reader;
    MockColumnReaderCache column_reader_cache(footer, file_reader, _tablet_schema);
    st = create_variant_root_reader(footer, file_reader, _tablet_schema, &column_reader);
    EXPECT_TRUE(st.ok()) << st.msg();

    auto variant_column_reader = assert_cast<VariantColumnReader*>(column_reader.get());
    EXPECT_TRUE(variant_column_reader != nullptr);
    // test read situation for compaction with should flat all sub column
    EXPECT_FALSE(variant_column_reader->get_subcolumns_meta_info()->empty());

    StorageReadOptions storageReadOptions;
    storageReadOptions.io_ctx.reader_type = ReaderType::READER_QUERY;
    OlapReaderStatistics stats;
    storageReadOptions.stats = &stats;

    ColumnIteratorUPtr nested_column_iter;
    st = variant_column_reader->new_iterator(&nested_column_iter, &_tablet_schema->column(0),
                                             &storageReadOptions, &column_reader_cache);
    EXPECT_TRUE(st.ok()) << st.msg();
    // this is nested column root
    auto* nested_iter = dynamic_cast<HierarchicalDataIterator*>(nested_column_iter.get());
    ASSERT_NE(nested_iter, nullptr);
    ColumnIteratorOptions column_iter_opts;
    column_iter_opts.stats = &stats;
    column_iter_opts.file_reader = file_reader.get();
    st = nested_iter->init(column_iter_opts);
    EXPECT_TRUE(st.ok()) << st.msg();
    // fill with nullable ColumnVariantV2 target
    MutableColumnPtr new_column_object1 = ColumnVariantV2::create();
    MutableColumnPtr null_object =
            ColumnNullable::create(std::move(new_column_object1), ColumnUInt8::create());
    size_t n = 1000;
    st = nested_iter->seek_to_ordinal(0);
    EXPECT_TRUE(st.ok()) << st.msg();
    bool has_null = false;
    st = nested_iter->next_batch(&n, null_object, &has_null);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(stats.bytes_read > 0);
    ASSERT_EQ(n, 1000);
    ASSERT_EQ(null_object->size(), 1000);
    for (size_t row = 0; row < n; ++row) {
        EXPECT_EQ(variant_json_at(*null_object, row), make_nested_variant_row(row));
    }
    {
        // fill with nullable ColumnVariantV2 target
        MutableColumnPtr new_column_object12 = ColumnVariantV2::create();
        MutableColumnPtr null_object12 =
                ColumnNullable::create(std::move(new_column_object12), ColumnUInt8::create());
        st = nested_iter->seek_to_ordinal(0);
        EXPECT_TRUE(st.ok()) << st.msg();
        n = 1000;
        st = nested_iter->next_batch(&n, null_object12, &has_null);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_TRUE(stats.bytes_read > 0);
        ASSERT_EQ(n, 1000);
        EXPECT_EQ(variant_json_at(*null_object12, 0), make_nested_variant_row(0));
        EXPECT_EQ(variant_json_at(*null_object12, n - 1), make_nested_variant_row(n - 1));
    }
    // Direct a.b reads use the exact physical file type: Nullable(Array(Nullable(JSONB))).
    {
        ColumnIteratorUPtr array_column_iter;
        TabletColumn target_column;
        target_column.set_name("b");
        target_column.set_type(FieldType::OLAP_FIELD_TYPE_ARRAY);
        target_column.set_parent_unique_id(_tablet_schema->column(0).unique_id());
        target_column.set_is_nullable(true);
        PathInDataBuilder builder;
        builder.append("v", false);
        builder.append("a", false);
        builder.append("b", false);
        PathInData path = builder.build();
        target_column.set_path_info(path);

        const auto* array_meta = find_footer_column_meta_by_relative_path(footer, "a.b");
        ASSERT_NE(array_meta, nullptr);
        DataTypePtr file_column_type = DataTypeFactory::instance().create_data_type(*array_meta);
        ASSERT_NE(file_column_type, nullptr);
        ASSERT_TRUE(file_column_type->is_nullable());
        const auto* array_type =
                typeid_cast<const DataTypeArray*>(remove_nullable(file_column_type).get());
        ASSERT_NE(array_type, nullptr);
        ASSERT_TRUE(array_type->get_nested_type()->is_nullable());
        EXPECT_EQ(remove_nullable(array_type->get_nested_type())->get_primitive_type(),
                  PrimitiveType::TYPE_JSONB);

        DataTypePtr inferred_type;
        st = variant_column_reader->infer_data_type_for_path(
                &inferred_type, target_column, storageReadOptions, &column_reader_cache);
        ASSERT_TRUE(st.ok()) << st.msg();
        ASSERT_NE(inferred_type, nullptr);
        EXPECT_TRUE(inferred_type->equals(*file_column_type))
                << "inferred=" << inferred_type->get_name()
                << " file=" << file_column_type->get_name();

        st = variant_column_reader->new_iterator(&array_column_iter, &target_column,
                                                 &storageReadOptions, &column_reader_cache);
        ASSERT_TRUE(st.ok()) << st.msg();
        auto* array_iter = dynamic_cast<ArrayFileColumnIterator*>(array_column_iter.get());
        ASSERT_NE(array_iter, nullptr);
        st = array_iter->init(column_iter_opts);
        ASSERT_TRUE(st.ok()) << st.msg();

        MutableColumnPtr array_column = file_column_type->create_column();
        size_t nrows = 1000;
        st = array_iter->seek_to_ordinal(0);
        ASSERT_TRUE(st.ok()) << st.msg();
        st = array_iter->next_batch(&nrows, array_column, &has_null);
        ASSERT_TRUE(st.ok()) << st.msg();
        ASSERT_EQ(nrows, 1000);
        ASSERT_EQ(array_column->size(), 1000);

        const auto& nullable_array = assert_cast<const ColumnNullable&>(*array_column);
        const auto& array = assert_cast<const ColumnArray&>(nullable_array.get_nested_column());
        const auto& nullable_items = assert_cast<const ColumnNullable&>(array.get_data());
        const auto& jsonb_items =
                assert_cast<const ColumnString&>(nullable_items.get_nested_column());
        const DataTypeJsonb jsonb_type;
        ASSERT_EQ(jsonb_items.size(), 1000);
        for (size_t row = 0; row < nrows; ++row) {
            EXPECT_FALSE(nullable_array.is_null_at(row));
            EXPECT_EQ(array.get_offsets()[row], row + 1);
            EXPECT_FALSE(nullable_items.is_null_at(row));
            EXPECT_EQ(jsonb_type.to_string(jsonb_items, row), make_nested_variant_array_item(row));
        }
    }
}

TEST_F(VariantColumnWriterReaderTest, test_nested_iter_nullable) {
    // write data
    std::string absolute_dir = _current_dir + std::string("/ut_dir/variant_test_nested_iter");
    // declare file_path and footer
    std::string file_path;
    SegmentFooterPB footer;
    std::shared_ptr<TabletSchema> _tablet_schema = std::make_shared<TabletSchema>();
    test_write_variant_column(_engine_ref, absolute_dir, file_path, footer, _tablet_schema, true);
    // reader data
    // check variant reader
    io::FileReaderSPtr file_reader;
    Status st = io::global_local_filesystem()->open_file(file_path, &file_reader);
    EXPECT_TRUE(st.ok()) << st.msg();
    std::shared_ptr<ColumnReader> column_reader;
    st = create_variant_root_reader(footer, file_reader, _tablet_schema, &column_reader);
    EXPECT_TRUE(st.ok()) << st.msg();

    auto variant_column_reader = assert_cast<VariantColumnReader*>(column_reader.get());
    EXPECT_TRUE(variant_column_reader != nullptr);
    // test read situation for compaction with should flat all sub column
    EXPECT_FALSE(variant_column_reader->get_subcolumns_meta_info()->empty());

    StorageReadOptions storageReadOptions;
    storageReadOptions.io_ctx.reader_type = ReaderType::READER_QUERY;
    OlapReaderStatistics stats;
    storageReadOptions.stats = &stats;

    ColumnIteratorUPtr nested_column_iter;
    MockColumnReaderCache column_reader_cache(footer, file_reader, _tablet_schema);

    st = variant_column_reader->new_iterator(&nested_column_iter, &_tablet_schema->column(0),
                                             &storageReadOptions, &column_reader_cache);
    EXPECT_TRUE(st.ok()) << st.msg();
    // this is nested column root
    auto* nested_iter = dynamic_cast<HierarchicalDataIterator*>(nested_column_iter.get());
    ASSERT_NE(nested_iter, nullptr);
    ColumnIteratorOptions column_iter_opts;
    column_iter_opts.stats = &stats;
    column_iter_opts.file_reader = file_reader.get();
    st = nested_iter->init(column_iter_opts);
    EXPECT_TRUE(st.ok()) << st.msg();
    // fill with nullable ColumnVariantV2 target
    MutableColumnPtr new_column_object1 = ColumnVariantV2::create();
    MutableColumnPtr null_object =
            ColumnNullable::create(std::move(new_column_object1), ColumnUInt8::create());
    size_t nrows = 1000;
    st = nested_iter->seek_to_ordinal(0);
    EXPECT_TRUE(st.ok()) << st.msg();
    bool has_null = false;
    st = nested_iter->next_batch(&nrows, null_object, &has_null);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(stats.bytes_read > 0);
    ASSERT_EQ(nrows, 1000);
    ASSERT_EQ(null_object->size(), 1000);
    const auto& nullable_root = assert_cast<const ColumnNullable&>(*null_object);
    for (size_t row = 0; row < nrows; ++row) {
        EXPECT_FALSE(nullable_root.is_null_at(row));
        EXPECT_EQ(variant_json_at(*null_object, row), make_nested_variant_row(row));
    }
}

TEST_F(VariantColumnWriterReaderTest, test_read_with_checksum) {
    auto fill_string_column_with_test_data =
            [&](auto& column_string, int size,
                std::unordered_map<int, std::string>* inserted_jsonstr,
                variant_util::PathToNoneNullValues* path_with_size) {
                for (int i = 0; i < size; ++i) {
                    std::string jsonstr;
                    if (i % 2 == 0) {
                        jsonstr = R"({"b" : 3})";
                        (*path_with_size)["b"] += 1;
                    } else {
                        jsonstr = R"({"b" : {"c" : 5}})";
                        (*path_with_size)["b.c"] += 1;
                    }
                    inserted_jsonstr->emplace(i, jsonstr);
                    column_string->insert_data(jsonstr.c_str(), jsonstr.size());
                }
            };

    // 1. create tablet_schema
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1");
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    // 2. create tablet
    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    bool external_segment_meta_used_default = rand() % 2 == 0;
    std::cout << "external_segment_meta_used_default: " << external_segment_meta_used_default
              << std::endl;
    _tablet_schema->set_storage_format(external_segment_meta_used_default
                                               ? TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V3
                                               : TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2);
    tablet_meta->_tablet_id = 10000;
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());

    EXPECT_TRUE(_tablet->init().ok());
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    // 3. create file_writer
    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    EXPECT_TRUE(st.ok()) << st.msg();

    // 4. create column_writer
    SegmentFooterPB footer;
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    opts.rowset_ctx = &rowset_ctx;
    opts.rowset_ctx->tablet_schema = _tablet_schema;
    TabletColumn column = _tablet_schema->column(0);
    opts.compression_type = CompressionTypePB::LZ4;
    opts.storage_format = TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2;
    _init_column_meta(opts.meta, 0, column, opts);

    std::unique_ptr<ColumnWriter> writer;
    EXPECT_TRUE(ColumnWriter::create(opts, &column, file_writer.get(), &writer).ok());
    EXPECT_TRUE(writer->init().ok());
    EXPECT_TRUE(assert_cast<VariantColumnWriter*>(writer.get()) != nullptr);

    // 5. write data
    auto olap_data_convertor = std::make_unique<OlapBlockDataConvertor>();
    auto block = _tablet_schema->create_block();
    variant_util::PathToNoneNullValues path_with_size;
    std::unordered_map<int, std::string> inserted_jsonstr;
    auto json_column = ColumnString::create();
    fill_string_column_with_test_data(json_column, 1000, &inserted_jsonstr, &path_with_size);
    block.replace_by_position(0, encode_json_column_v2(*json_column));

    olap_data_convertor->add_column_data_convertor(column);
    olap_data_convertor->set_source_content(&block, 0, 1000);
    auto [result, accessor] = olap_data_convertor->convert_column_data(0);
    EXPECT_TRUE(result.ok());
    EXPECT_TRUE(accessor != nullptr);
    EXPECT_TRUE(writer->append(accessor->get_nullmap(), accessor->get_data(), 1000).ok());
    st = writer->finish();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_data();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_ordinal_index();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_zone_map();
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(file_writer->close().ok());
    footer.set_num_rows(1000);

    // 6. check footer
    EXPECT_EQ(footer.columns_size(), 4);
    auto column_meta = footer.columns(0);
    EXPECT_EQ(column_meta.type(), (int)FieldType::OLAP_FIELD_TYPE_VARIANT);

    for (int i = 1; i < footer.columns_size() - 1; ++i) {
        auto column_met = footer.columns(i);
        check_column_meta(column_met, path_with_size);
    }
    check_sparse_column_meta(footer.columns(footer.columns_size() - 1), path_with_size);

    // 7. check variant reader
    io::FileReaderSPtr file_reader;
    st = io::global_local_filesystem()->open_file(file_path, &file_reader);
    EXPECT_TRUE(st.ok()) << st.msg();
    std::shared_ptr<ColumnReader> column_reader;
    st = create_variant_root_reader(footer, file_reader, _tablet_schema, &column_reader);
    EXPECT_TRUE(st.ok()) << st.msg();

    MockColumnReaderCache column_reader_cache(footer, file_reader, _tablet_schema);

    auto* variant_column_reader = assert_cast<VariantColumnReader*>(column_reader.get());
    EXPECT_TRUE(variant_column_reader != nullptr);

    const auto* subcolumn_meta = variant_column_reader->get_subcolumn_meta_by_path(PathInData("b"));
    EXPECT_TRUE(subcolumn_meta != nullptr);
    subcolumn_meta = variant_column_reader->get_subcolumn_meta_by_path(PathInData("b.c"));
    EXPECT_TRUE(subcolumn_meta != nullptr);

    TabletColumn parent_column = _tablet_schema->column(0);
    StorageReadOptions storage_read_opts;

    storage_read_opts.tablet_schema = _tablet_schema;

    TabletColumn subcolumn;
    subcolumn.set_name(parent_column.name_lower_case() + ".b");
    subcolumn.set_type((FieldType)(int)footer.columns(1).type());
    subcolumn.set_parent_unique_id(parent_column.unique_id());
    subcolumn.set_path_info(PathInData(parent_column.name_lower_case() + ".b"));
    subcolumn.set_variant_max_subcolumns_count(parent_column.variant_max_subcolumns_count());
    subcolumn.set_is_nullable(true);
    _tablet_schema->append_column(subcolumn);
    storage_read_opts.io_ctx.reader_type = ReaderType::READER_QUERY;
    OlapReaderStatistics stats;
    storage_read_opts.stats = &stats;
    ColumnIteratorUPtr hierarchical_it;
    st = variant_column_reader->new_iterator(&hierarchical_it, &subcolumn, &storage_read_opts,
                                             &column_reader_cache);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(dynamic_cast<HierarchicalDataIterator*>(hierarchical_it.get()) != nullptr);

    storage_read_opts.io_ctx.reader_type = ReaderType::READER_CHECKSUM;
    ColumnIteratorUPtr it;
    st = variant_column_reader->new_iterator(&it, &subcolumn, &storage_read_opts,
                                             &column_reader_cache);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(dynamic_cast<FileColumnIterator*>(it.get()) != nullptr);
    ColumnIteratorOptions column_iter_opts;
    column_iter_opts.stats = &stats;
    column_iter_opts.file_reader = file_reader.get();
    st = it->init(column_iter_opts);
    EXPECT_TRUE(st.ok()) << st.msg();

    auto column_type = DataTypeFactory::instance().create_data_type(subcolumn, true);
    auto read_column = column_type->create_column();
    size_t nrows = 1000;
    st = it->seek_to_ordinal(0);
    EXPECT_TRUE(st.ok()) << st.msg();
    st = it->next_batch(&nrows, read_column);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(stats.bytes_read > 0);

    for (int row = 0; row < 1000; ++row) {
        const std::string& value = column_type->to_string(*read_column, row);
        if (row % 2 == 0) {
            EXPECT_EQ(value, "3");
        }
    }
}

// Concurrently trigger external meta loading and subcolumn meta access to guard against
// data races between `load_external_meta_once` writer and readers like
// `get_subcolumn_meta_by_path` / `get_metadata_size`. This roughly simulates the
// production crash stack where one thread was loading external meta while another
// thread was reading from `_subcolumns_meta_info`.
TEST_F(VariantColumnWriterReaderTest, test_concurrent_load_external_meta_and_get_subcolumn_meta) {
    // 1. create tablet_schema
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1");
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    // 2. create tablet with external segment meta explicitly enabled so that
    // VariantColumnReader builds a VariantExternalMetaReader.
    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    bool external_segment_meta_used_default = true;
    _tablet_schema->set_storage_format(external_segment_meta_used_default
                                               ? TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V3
                                               : TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2);
    tablet_meta->_tablet_id = 20000;
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());

    EXPECT_TRUE(_tablet->init().ok());
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    // 3. create file_writer
    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    EXPECT_TRUE(st.ok()) << st.msg();

    // 4. create column_writer
    SegmentFooterPB footer;
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    opts.rowset_ctx = &rowset_ctx;
    opts.rowset_ctx->tablet_schema = _tablet_schema;
    TabletColumn column = _tablet_schema->column(0);
    opts.compression_type = CompressionTypePB::LZ4;
    opts.storage_format = TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2;
    _init_column_meta(opts.meta, 0, column, opts);

    std::unique_ptr<ColumnWriter> writer;
    EXPECT_TRUE(ColumnWriter::create(opts, &column, file_writer.get(), &writer).ok());
    EXPECT_TRUE(writer->init().ok());
    EXPECT_TRUE(assert_cast<VariantColumnWriter*>(writer.get()) != nullptr);

    // 5. write a small amount of data to build some subcolumns
    auto olap_data_convertor = std::make_unique<OlapBlockDataConvertor>();
    auto block = _tablet_schema->create_block();
    std::unordered_map<int, std::string> inserted_jsonstr;
    auto json_column = ColumnString::create();
    auto path_with_size =
            VariantUtil::fill_string_column_with_test_data(json_column, 200, &inserted_jsonstr);
    block.replace_by_position(0, encode_json_column_v2(*json_column));
    olap_data_convertor->add_column_data_convertor(column);
    olap_data_convertor->set_source_content(&block, 0, 200);
    auto [result, accessor] = olap_data_convertor->convert_column_data(0);
    EXPECT_TRUE(result.ok());
    EXPECT_TRUE(accessor != nullptr);
    EXPECT_TRUE(writer->append(accessor->get_nullmap(), accessor->get_data(), 200).ok());
    st = writer->finish();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_data();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_ordinal_index();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_zone_map();
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(file_writer->close().ok());
    footer.set_num_rows(200);

    // 6. open a VariantColumnReader on this segment
    io::FileReaderSPtr file_reader;
    st = io::global_local_filesystem()->open_file(file_path, &file_reader);
    EXPECT_TRUE(st.ok()) << st.msg();
    std::shared_ptr<ColumnReader> column_reader;
    st = create_variant_root_reader(footer, file_reader, _tablet_schema, &column_reader);
    EXPECT_TRUE(st.ok()) << st.msg();

    auto* variant_column_reader = assert_cast<VariantColumnReader*>(column_reader.get());
    EXPECT_TRUE(variant_column_reader != nullptr);

    // 7. run load_external_meta_once and subcolumn meta access concurrently.
    const int rounds = 200;
    std::atomic<bool> failed {false};
    Status writer_status = Status::OK();

    std::thread writer_thread([&] {
        for (int i = 0; i < rounds && !failed.load(); ++i) {
            Status s = variant_column_reader->load_external_meta_once();
            if (!s.ok()) {
                writer_status = s;
                failed.store(true);
                break;
            }
        }
    });

    std::thread reader_thread([&] {
        for (int i = 0; i < rounds && !failed.load(); ++i) {
            // Access subcolumn meta and metadata size repeatedly.
            auto* node = variant_column_reader->get_subcolumn_meta_by_path(PathInData("key0"));
            (void)node;
            auto meta_size = variant_column_reader->get_metadata_size();
            (void)meta_size;
        }
    });

    writer_thread.join();
    reader_thread.join();

    EXPECT_TRUE(writer_status.ok());
}

// Regression test: compaction on no-key duplicate table with variant column uid=0.
// TabletColumn::is_extracted_column() used "_parent_col_unique_id > 0" which
// incorrectly returned false for subcolumns whose parent has uid=0, causing
// VariantColumnWriterImpl to duplicate sparse column entries in segment footer.
// Without fix: DCHECK(uid >= 0) fires in segment_iterator.cpp because
// is_extracted_column() wrongly returns false for extracted cols with parent uid=0,
// making them take the non-extracted path where uid=-1 violates the check.
TEST_F(VariantColumnWriterReaderTest, test_compaction_nokey_variant_uid0) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    construct_column(schema_pb.add_column(), /*col_unique_id=*/0, "VARIANT", "v1",
                     /*variant_max_subcolumns_count=*/3);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    tablet_meta->_tablet_id = 99900;
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    ASSERT_TRUE(_tablet->init().ok());
    ASSERT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    ASSERT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    auto rs0 = create_variant_rowset(
            {{R"({"name":"Alice","age":30})", R"({"name":"Bob","age":25})"}}, 2);
    auto rs1 =
            create_variant_rowset({{R"({"name":"u1","age":10})", R"({"name":"u2","age":20})"}}, 3);

    std::vector<RowsetSharedPtr> input_rowsets {rs0, rs1};
    auto input_readers = create_rowset_readers(input_rowsets);

    auto compaction_schema = std::make_shared<TabletSchema>(*_tablet_schema);
    auto st = variant_util::VariantCompactionUtil::get_extended_compaction_schema(
            input_rowsets, compaction_schema);
    ASSERT_TRUE(st.ok()) << st.to_string();

    RowsetWriterContext ctx;
    RowsetId rowset_id;
    rowset_id.init(9999);
    ctx.rowset_id = rowset_id;
    ctx.rowset_type = BETA_ROWSET;
    ctx.data_dir = _data_dir.get();
    ctx.rowset_state = VISIBLE;
    ctx.tablet_schema = compaction_schema;
    ctx.tablet_path = _tablet->tablet_path();
    ctx.tablet_id = _tablet->tablet_id();
    ctx.tablet = _tablet;
    ctx.version = Version(2, 3);
    ctx.write_type = DataWriteType::TYPE_COMPACTION;
    auto res = RowsetFactory::create_rowset_writer(*_engine_ref, ctx, true);
    ASSERT_TRUE(res.has_value()) << res.error();
    auto output_writer = std::move(res).value();

    Merger::Statistics stats;
    st = Merger::vertical_merge_rowsets(_tablet, ReaderType::READER_CUMULATIVE_COMPACTION,
                                        *compaction_schema, input_readers, output_writer.get(),
                                        10000, 2, &stats);
    ASSERT_TRUE(st.ok()) << st.to_string();

    RowsetSharedPtr output_rowset;
    ASSERT_TRUE(output_writer->build(output_rowset).ok());
    ASSERT_EQ(output_rowset->num_rows(), 4);
}

} // namespace doris
