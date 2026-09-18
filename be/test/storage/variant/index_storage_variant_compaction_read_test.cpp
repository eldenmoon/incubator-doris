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

#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type_serde/data_type_variant_v2_serde.h"
#include "storage/index/index_file_reader.h"
#include "storage/index/inverted/inverted_index_desc.h"
#include "storage/index/inverted/variant_root_index.h"
#include "storage/index/inverted/variant_term_codec.h"
#include "storage/index/snii/query/term_query.h"
#include "storage/segment/variant/nested_group_provider.h"
#include "storage/variant/index_storage_variant_test_base.h"
#include "util/debug_points.h"
#include "util/defer_op.h"

namespace doris::index_storage_test {

static bool nested_group_write_path_available() {
    auto provider = segment_v2::create_nested_group_read_provider();
    return provider != nullptr && provider->should_enable_nested_group_read_path();
}

class IndexStorageVariantCompactionReadTest : public IndexStorageTestFixture {
protected:
    void run_deep_sparse_variant_lifecycle(bool external_segment_meta, int64_t tablet_id);
    void run_nested_group_variant_lifecycle(bool external_segment_meta, int64_t tablet_id);
    // Two root indexes (exact + english) on one VARIANT column merge natively and
    // independently under the given compaction kind.
    void run_direct_merge_of_root_indexes(IndexCompactionKind kind, int64_t tablet_id,
                                          int64_t first_index_id);
};

void IndexStorageVariantCompactionReadTest::run_deep_sparse_variant_lifecycle(
        bool external_segment_meta, int64_t tablet_id) {
    VariantColumnSpec variant;
    variant.unique_id = 2;
    variant.name = "v";
    variant.max_subcolumns_count = 1;
    variant.sparse_hash_shard_count = 2;

    IndexTabletOptions options;
    options.tablet_id = tablet_id;
    options.external_segment_meta = external_segment_meta;
    options.variant_columns = {std::move(variant)};
    ASSERT_TRUE(create_tablet(options).ok());

    IndexRowsetSpec rowset0;
    rowset0.version = 0;
    rowset0.batches.push_back(IndexBatch::single_variant(
            {R"({"hot": "h0", "deep": {"rare_a": "a0"}, "cold0": "c0"})",
             R"({"hot": "h1", "deep": {"rare_b": "b0"}, "cold1": "c1"})"},
            0));
    auto rowset0_result = write_rowset(rowset0);
    ASSERT_TRUE(rowset0_result.has_value()) << rowset0_result.error();

    auto rowset0_probe = probe_rowset(rowset0_result.value());
    ASSERT_TRUE(rowset0_probe.has_value()) << rowset0_probe.error();
    EXPECT_TRUE(has_variant_layout(rowset0_probe.value(), 2, "hot"));
    EXPECT_TRUE(has_sparse_path_stat(rowset0_probe.value(), "deep.rare_a"));
    EXPECT_TRUE(has_sparse_path_stat(rowset0_probe.value(), "deep.rare_b"));

    IndexRowsetSpec rowset1;
    rowset1.version = 1;
    rowset1.batches.push_back(IndexBatch::single_variant(
            {R"({"hot": "h2", "deep": {"rare_a": "a1"}, "cold2": "c2"})",
             R"({"hot": "h3", "deep": {"rare_c": "c0"}, "cold3": "c3"})"},
            100));
    auto rowset1_result = write_rowset(rowset1);
    ASSERT_TRUE(rowset1_result.has_value()) << rowset1_result.error();

    auto read_result = read_rowsets({rowset0_result.value(), rowset1_result.value()});
    ASSERT_TRUE(read_result.has_value()) << read_result.error();
    EXPECT_EQ(read_result->rows_read, 4);

    auto compacted = compact_rowsets(IndexCompactionKind::CUMULATIVE,
                                     {rowset0_result.value(), rowset1_result.value()});
    ASSERT_TRUE(compacted.has_value()) << compacted.error();
    ASSERT_NE(compacted.value(), nullptr);
    EXPECT_EQ(compacted.value()->num_rows(), 4);

    auto compacted_probe = probe_rowset(compacted.value());
    ASSERT_TRUE(compacted_probe.has_value()) << compacted_probe.error();
    EXPECT_TRUE(has_variant_layout(compacted_probe.value(), 2, "hot"));
    EXPECT_TRUE(has_sparse_path_stat(compacted_probe.value(), "deep.rare_a"));
    EXPECT_TRUE(has_sparse_path_stat(compacted_probe.value(), "deep.rare_b"));
    EXPECT_TRUE(has_sparse_path_stat(compacted_probe.value(), "deep.rare_c"));

    auto compacted_read = read_rowsets({compacted.value()});
    ASSERT_TRUE(compacted_read.has_value()) << compacted_read.error();
    EXPECT_EQ(compacted_read->rows_read, 4);
}

void IndexStorageVariantCompactionReadTest::run_nested_group_variant_lifecycle(
        bool external_segment_meta, int64_t tablet_id) {
    if (!nested_group_write_path_available()) {
        GTEST_SKIP() << "NestedGroup write path is not available in this build";
    }

    VariantColumnSpec variant;
    variant.unique_id = 2;
    variant.name = "v";
    variant.max_subcolumns_count = 1;
    variant.sparse_hash_shard_count = 2;
    variant.enable_nested_group = true;

    IndexTabletOptions options;
    options.tablet_id = tablet_id;
    options.external_segment_meta = external_segment_meta;
    options.variant_columns = {std::move(variant)};
    ASSERT_TRUE(create_tablet(options).ok());

    IndexRowsetSpec rowset0;
    rowset0.version = 0;
    rowset0.batches.push_back(IndexBatch::single_variant(
            {R"({"owner": "alice", "profile": {"region": "us"}, "items": [{"sku": "a", "qty": 1}]})",
             R"({"owner": "bob", "profile": {"region": "eu"}, "items": [{"sku": "b", "qty": 2}]})"},
            0));
    auto rowset0_result = write_rowset(rowset0);
    ASSERT_TRUE(rowset0_result.has_value()) << rowset0_result.error();

    IndexRowsetSpec rowset1;
    rowset1.version = 1;
    rowset1.batches.push_back(IndexBatch::single_variant(
            {R"({"owner": "carol", "profile": {"region": "apac"}, "items": [{"sku": "c", "qty": 3}]})",
             R"({"owner": "dave", "profile": {"region": "us"}, "items": [{"sku": "d", "qty": 4}]})"},
            100));
    auto rowset1_result = write_rowset(rowset1);
    ASSERT_TRUE(rowset1_result.has_value()) << rowset1_result.error();

    auto compacted = compact_rowsets(IndexCompactionKind::CUMULATIVE,
                                     {rowset0_result.value(), rowset1_result.value()});
    ASSERT_TRUE(compacted.has_value()) << compacted.error();
    ASSERT_NE(compacted.value(), nullptr);
    EXPECT_EQ(compacted.value()->num_rows(), 4);
    ASSERT_TRUE(compacted.value()->tablet_schema()->has_column_unique_id(2));
    EXPECT_TRUE(compacted.value()->tablet_schema()->column_by_uid(2).variant_enable_nested_group());

    auto compacted_probe = probe_rowset(compacted.value());
    ASSERT_TRUE(compacted_probe.has_value()) << compacted_probe.error();
    EXPECT_TRUE(has_variant_parent(compacted_probe.value(), 2));
    EXPECT_TRUE(has_variant_layout(compacted_probe.value(), 2, "owner"));
    EXPECT_TRUE(has_variant_layout(compacted_probe.value(), 2, "profile.region"));

    auto compacted_read = read_rowsets({compacted.value()});
    ASSERT_TRUE(compacted_read.has_value()) << compacted_read.error();
    EXPECT_EQ(compacted_read->rows_read, 4);
}

TEST_F(IndexStorageVariantCompactionReadTest, WriteReadProbeAndCumulativeCompact) {
    IndexTabletOptions options;
    options.tablet_id = 110002;
    options.external_segment_meta = true;
    options.variant_columns = {VariantColumnSpec {}};
    options.variant_columns[0].unique_id = 2;
    options.variant_columns[0].name = "v";
    options.variant_columns[0].max_subcolumns_count = 4;
    options.variant_columns[0].sparse_hash_shard_count = 2;
    options.variant_columns[0].predefined_paths = {
            VariantPathSpec {.path = "a",
                             .type = FieldType::OLAP_FIELD_TYPE_INT,
                             .nullable = true,
                             .pattern_type = PatternTypePB::MATCH_NAME,
                             .array_item_type = {},
                             .array_item_nullable = true},
            VariantPathSpec {.path = "b",
                             .type = FieldType::OLAP_FIELD_TYPE_STRING,
                             .nullable = true,
                             .pattern_type = PatternTypePB::MATCH_NAME,
                             .array_item_type = {},
                             .array_item_nullable = true},
    };
    ASSERT_TRUE(create_tablet(options).ok());

    IndexRowsetSpec rowset0;
    rowset0.version = 0;
    rowset0.batches.push_back(
            IndexBatch::single_variant({R"({"a": 1, "b": "one"})", R"({"a": 2, "c": 20})"}, 0));
    auto rowset0_result = write_rowset(rowset0);
    ASSERT_TRUE(rowset0_result.has_value()) << rowset0_result.error();

    IndexRowsetSpec rowset1;
    rowset1.version = 1;
    rowset1.batches.push_back(
            IndexBatch::single_variant({R"({"a": 3, "b": "three"})", R"({"a": 4, "d": 40})"}, 100));
    auto rowset1_result = write_rowset(rowset1);
    ASSERT_TRUE(rowset1_result.has_value()) << rowset1_result.error();

    auto read_result = read_rowsets({rowset0_result.value(), rowset1_result.value()});
    ASSERT_TRUE(read_result.has_value()) << read_result.error();
    EXPECT_EQ(read_result->rows_read, 4);

    auto probe_result = probe_rowset(rowset0_result.value());
    ASSERT_TRUE(probe_result.has_value()) << probe_result.error();
    EXPECT_EQ(probe_result->num_rows, 2);
    EXPECT_EQ(probe_result->num_segments, 1);
    EXPECT_TRUE(probe_result->contains_relative_path("a"));
    EXPECT_TRUE(probe_result->contains_relative_path("b"));
    expect_index_files(probe_result.value(), false);

    auto compacted = compact_rowsets(IndexCompactionKind::CUMULATIVE,
                                     {rowset0_result.value(), rowset1_result.value()});
    ASSERT_TRUE(compacted.has_value()) << compacted.error();
    ASSERT_NE(compacted.value(), nullptr);
    EXPECT_EQ(compacted.value()->num_rows(), 4);

    auto compacted_probe = probe_rowset(compacted.value());
    ASSERT_TRUE(compacted_probe.has_value()) << compacted_probe.error();
    EXPECT_TRUE(compacted_probe->contains_relative_path("a"));
    EXPECT_TRUE(compacted_probe->contains_relative_path("b"));
}

TEST_F(IndexStorageVariantCompactionReadTest, DeepSparseVariantCompactsWithExternalSegmentMeta) {
    run_deep_sparse_variant_lifecycle(true, 110023);
}

TEST_F(IndexStorageVariantCompactionReadTest, DeepSparseVariantCompactsWithoutExternalSegmentMeta) {
    run_deep_sparse_variant_lifecycle(false, 110024);
}

TEST_F(IndexStorageVariantCompactionReadTest, NestedGroupVariantCompactsWithExternalSegmentMeta) {
    run_nested_group_variant_lifecycle(true, 110025);
}

TEST_F(IndexStorageVariantCompactionReadTest,
       NestedGroupVariantCompactsWithoutExternalSegmentMeta) {
    run_nested_group_variant_lifecycle(false, 110026);
}

TEST_F(IndexStorageVariantCompactionReadTest, VariantDocModeWritesDocValueColumnsAfterCompaction) {
    VariantColumnSpec variant;
    variant.unique_id = 2;
    variant.name = "v";
    variant.max_subcolumns_count = 4;
    variant.enable_doc_mode = true;
    variant.doc_materialization_min_rows = 100000;
    variant.doc_hash_shard_count = 2;

    IndexTabletOptions options;
    options.tablet_id = 110012;
    options.variant_columns = {std::move(variant)};
    ASSERT_TRUE(create_tablet(options).ok());

    IndexRowsetSpec rowset0;
    rowset0.version = 0;
    rowset0.batches.push_back(
            IndexBatch::single_variant({R"({"a": "one", "b": 1})", R"({"a": "two", "c": 2})"}, 0));
    auto rowset0_result = write_rowset(rowset0);
    ASSERT_TRUE(rowset0_result.has_value()) << rowset0_result.error();

    IndexRowsetSpec rowset1;
    rowset1.version = 1;
    rowset1.batches.push_back(IndexBatch::single_variant(
            {R"({"a": "three", "d": 3})", R"({"a": "four", "e": 4})"}, 100));
    auto rowset1_result = write_rowset(rowset1);
    ASSERT_TRUE(rowset1_result.has_value()) << rowset1_result.error();

    auto before_compaction_read = read_rowsets({rowset0_result.value(), rowset1_result.value()});
    ASSERT_TRUE(before_compaction_read.has_value()) << before_compaction_read.error();
    EXPECT_EQ(before_compaction_read->rows_read, 4);

    auto rowset0_probe = probe_rowset(rowset0_result.value());
    ASSERT_TRUE(rowset0_probe.has_value()) << rowset0_probe.error();
    EXPECT_TRUE(has_doc_value_column(rowset0_probe.value()));

    auto compacted = compact_rowsets(IndexCompactionKind::CUMULATIVE,
                                     {rowset0_result.value(), rowset1_result.value()});
    ASSERT_TRUE(compacted.has_value()) << compacted.error();
    ASSERT_NE(compacted.value(), nullptr);
    EXPECT_EQ(compacted.value()->num_rows(), 4);

    auto compacted_probe = probe_rowset(compacted.value());
    ASSERT_TRUE(compacted_probe.has_value()) << compacted_probe.error();
    EXPECT_TRUE(has_doc_value_column(compacted_probe.value()));

    auto compacted_read = read_rowsets({compacted.value()});
    ASSERT_TRUE(compacted_read.has_value()) << compacted_read.error();
    EXPECT_EQ(compacted_read->rows_read, 4);
}

TEST_F(IndexStorageVariantCompactionReadTest,
       FullCompactionRebuildsExactAndTokenRootIndexesFromLogicalVariant) {
    VariantColumnSpec variant;
    variant.unique_id = 2;
    variant.name = "v";
    variant.max_subcolumns_count = 2;

    IndexTabletOptions options;
    options.tablet_id = 110056;
    options.index_storage_format = InvertedIndexStorageFormatPB::SNII;
    options.variant_columns = {std::move(variant)};
    ASSERT_TRUE(create_tablet(options).ok());

    IndexRowsetSpec rowset0;
    rowset0.version = 0;
    rowset0.batches.push_back(IndexBatch::single_variant(
            {R"({"action":"opened","body":"Root Index"})", R"({"action":"closed","body":"Other"})"},
            0));
    IndexRowsetSpec rowset1;
    rowset1.version = 1;
    rowset1.batches.push_back(IndexBatch::single_variant(
            {R"({"action":"opened","body":"Another Root"})", R"({"missing":1})"}, 100));
    auto rowsets = write_rowsets({rowset0, rowset1});
    ASSERT_TRUE(rowsets.has_value()) << rowsets.error();

    const auto root_properties = [](std::string parser) {
        return std::map<std::string, std::string> {
                {"parser", std::move(parser)},
                {std::string(segment_v2::variant_root_index::VARIANT_INDEX_MODE_KEY),
                 std::string(segment_v2::variant_root_index::VARIANT_INDEX_MODE_ALL_VALUES)},
                {std::string(segment_v2::variant_root_index::VARIANT_ROOT_FORMAT_VERSION_KEY),
                 std::string(segment_v2::variant_root_index::VARIANT_ROOT_FORMAT_VERSION_CURRENT)}};
    };
    IndexSchemaPatch patch;
    patch.add_inverted_indexes = {
            IndexSpec::column_index(10056, "idx_v_root_exact", 2, root_properties("none")),
            IndexSpec::column_index(10057, "idx_v_root_english", 2, root_properties("english")),
    };
    auto indexed_schema = build_patched_tablet_schema(*tablet_schema(), patch);
    ASSERT_NE(indexed_schema, nullptr);
    auto indexed_rowsets =
            inject_reader_schema_for_rowsets(rowsets.value(), std::move(indexed_schema));
    ASSERT_TRUE(indexed_rowsets.has_value()) << indexed_rowsets.error();

    IndexReadOptions read_options;
    read_options.return_columns = {0, 1};
    read_options.collect_variant_values = true;
    auto before = read_rowsets(indexed_rowsets.value(), read_options);
    ASSERT_TRUE(before.has_value()) << before.error();

    auto compacted = compact_rowsets(IndexCompactionKind::FULL, indexed_rowsets.value());
    ASSERT_TRUE(compacted.has_value()) << compacted.error();
    ASSERT_NE(compacted.value(), nullptr);
    EXPECT_EQ(compacted.value()->num_rows(), 4);

    auto after = read_rowsets({compacted.value()}, read_options);
    ASSERT_TRUE(after.has_value()) << after.error();
    EXPECT_EQ(after->variant_values_by_uid.at(2), before->variant_values_by_uid.at(2));

    auto segment_path = compacted.value()->segment_path(0);
    ASSERT_TRUE(segment_path.has_value()) << segment_path.error();
    const std::string prefix(InvertedIndexDescriptor::get_index_file_path_prefix(*segment_path));
    IndexFileReader index_file_reader(
            compacted.value()->rowset_meta()->fs(), prefix, InvertedIndexStorageFormatPB::SNII,
            compacted.value()->segment(0).inverted_index_file_info(), options.tablet_id);
    ASSERT_TRUE(index_file_reader.init().ok());
    const auto root_indexes = compacted.value()->tablet_schema()->inverted_indexs(2);
    ASSERT_EQ(root_indexes.size(), 2);
    auto exact = index_file_reader.open_snii_index(root_indexes[0]);
    auto token = index_file_reader.open_snii_index(root_indexes[1]);
    ASSERT_TRUE(exact.has_value()) << exact.error();
    ASSERT_TRUE(token.has_value()) << token.error();
    EXPECT_EQ((*exact)->stats().doc_count, 4U);
    EXPECT_EQ((*token)->stats().doc_count, 4U);

    std::vector<uint32_t> docids;
    ASSERT_TRUE(snii::query::term_query(
                        **exact, segment_v2::variant_term_codec::term_string("opened"), &docids)
                        .ok());
    EXPECT_EQ(docids, (std::vector<uint32_t> {0, 2}));
    docids.clear();
    ASSERT_TRUE(snii::query::term_query(**token, segment_v2::variant_term_codec::term_token("root"),
                                        &docids)
                        .ok());
    EXPECT_EQ(docids, (std::vector<uint32_t> {0, 2}));
}

TEST_F(IndexStorageVariantCompactionReadTest,
       FullCompactionRebuildKeepsLeavesWithControlCharacters) {
    // A GitHub IssuesEvent row whose body holds U+0001 / U+0003. Rebuilt through the legacy
    // ColumnVariant JSON text round trip the row degraded to one string and lost every value
    // term; the compaction must assemble and re-shred it as Variant V2.
    VariantColumnSpec variant;
    variant.unique_id = 2;
    variant.name = "v";
    variant.nullable = true;
    variant.max_subcolumns_count = 1024;

    IndexTabletOptions options;
    options.tablet_id = 110059;
    options.index_storage_format = InvertedIndexStorageFormatPB::SNII;
    options.variant_columns = {std::move(variant)};
    ASSERT_TRUE(create_tablet(options).ok());

    IndexRowsetSpec rowset0;
    rowset0.version = 0;
    rowset0.batches.push_back(IndexBatch::single_variant(
            {R"json({"action":"opened","issue":{"body":"Ошибка появляется начиная с версии 4.2.50 в любом окне с контролом Chart.  \\r\\n\\r\\nException: Your trial of SciChart has expired.  \\r\\nSciChart must be activated on this machine using a purchased serial key to allow development.\\r\\nPlease contact support@scichart.com or visit www.scichart.com/buy-now\\r\\nStackTrace:\\r\\n at Abt.Licensing.Core.Credentials.\u0001_","comments":0,"comments_url":"https://api.github.com/repos/stocksharp/stocksharp/issues/1/comments","created_at":"2015-01-03T11:22:17Z","events_url":"https://api.github.com/repos/stocksharp/stocksharp/issues/1/events","html_url":"https://github.com/stocksharp/stocksharp/issues/1","id":53301496,"labels_url":"https://api.github.com/repos/stocksharp/stocksharp/issues/1/labels{/name}","locked":false,"number":1,"state":"open","title":"Your trial of SciChart has expired","updated_at":"2015-01-03T11:22:17Z","url":"https://api.github.com/repos/stocksharp/stocksharp/issues/1","user":{"avatar_url":"https://avatars.githubusercontent.com/u/10364235?v=3","events_url":"https://api.github.com/users/Sergey-Dvortsov/events{/privacy}","followers_url":"https://api.github.com/users/Sergey-Dvortsov/followers","following_url":"https://api.github.com/users/Sergey-Dvortsov/following{/other_user}","gists_url":"https://api.github.com/users/Sergey-Dvortsov/gists{/gist_id}","gravatar_id":"","html_url":"https://github.com/Sergey-Dvortsov","id":10364235,"login":"Sergey-Dvortsov","organizations_url":"https://api.github.com/users/Sergey-Dvortsov/orgs","received_events_url":"https://api.github.com/users/Sergey-Dvortsov/received_events","repos_url":"https://api.github.com/users/Sergey-Dvortsov/repos","site_admin":false,"starred_url":"https://api.github.com/users/Sergey-Dvortsov/starred{/owner}{/repo}","subscriptions_url":"https://api.github.com/users/Sergey-Dvortsov/subscriptions","type":"User","url":"https://api.github.com/users/Sergey-Dvortsov"}}})json"},
            0));
    IndexRowsetSpec rowset1;
    rowset1.version = 1;
    rowset1.batches.push_back(IndexBatch::single_variant(
            {R"({"action":"opened","issue":{"number":5,"state":"open","title":"t","labels":[{"name":"bug"}]}})"},
            100));
    auto rowsets = write_rowsets({rowset0, rowset1});
    ASSERT_TRUE(rowsets.has_value()) << rowsets.error();

    const auto root_properties = [](std::string parser) {
        return std::map<std::string, std::string> {
                {"parser", std::move(parser)},
                {std::string(segment_v2::variant_root_index::VARIANT_INDEX_MODE_KEY),
                 std::string(segment_v2::variant_root_index::VARIANT_INDEX_MODE_ALL_VALUES)},
                {std::string(segment_v2::variant_root_index::VARIANT_ROOT_FORMAT_VERSION_KEY),
                 std::string(segment_v2::variant_root_index::VARIANT_ROOT_FORMAT_VERSION_CURRENT)}};
    };
    IndexSchemaPatch patch;
    patch.add_inverted_indexes = {
            IndexSpec::column_index(10062, "idx_v_root_exact", 2, root_properties("none")),
            IndexSpec::column_index(10063, "idx_v_root_english", 2, root_properties("english")),
    };
    auto indexed_schema = build_patched_tablet_schema(*tablet_schema(), patch);
    ASSERT_NE(indexed_schema, nullptr);
    auto indexed_rowsets =
            inject_reader_schema_for_rowsets(rowsets.value(), std::move(indexed_schema));
    ASSERT_TRUE(indexed_rowsets.has_value()) << indexed_rowsets.error();

    IndexReadOptions read_options;
    read_options.return_columns = {0, 1};
    read_options.collect_variant_values = true;
    auto before = read_rowsets(indexed_rowsets.value(), read_options);
    ASSERT_TRUE(before.has_value()) << before.error();

    const bool old_index_compaction = config::inverted_index_compaction_enable;
    config::inverted_index_compaction_enable = false;
    auto compacted = compact_rowsets(IndexCompactionKind::FULL, indexed_rowsets.value());
    config::inverted_index_compaction_enable = old_index_compaction;
    ASSERT_TRUE(compacted.has_value()) << compacted.error();
    ASSERT_NE(compacted.value(), nullptr);
    EXPECT_EQ(compacted.value()->num_rows(), 2);

    auto after = read_rowsets({compacted.value()}, read_options);
    ASSERT_TRUE(after.has_value()) << after.error();
    EXPECT_EQ(after->variant_values_by_uid.at(2), before->variant_values_by_uid.at(2));

    auto segment_path = compacted.value()->segment_path(0);
    ASSERT_TRUE(segment_path.has_value()) << segment_path.error();
    const std::string prefix(InvertedIndexDescriptor::get_index_file_path_prefix(*segment_path));
    IndexFileReader index_file_reader(
            compacted.value()->rowset_meta()->fs(), prefix, InvertedIndexStorageFormatPB::SNII,
            compacted.value()->segment(0).inverted_index_file_info(), options.tablet_id);
    ASSERT_TRUE(index_file_reader.init().ok());
    const auto root_indexes = compacted.value()->tablet_schema()->inverted_indexs(2);
    ASSERT_EQ(root_indexes.size(), 2);
    auto exact = index_file_reader.open_snii_index(root_indexes[0]);
    auto token = index_file_reader.open_snii_index(root_indexes[1]);
    ASSERT_TRUE(exact.has_value()) << exact.error();
    ASSERT_TRUE(token.has_value()) << token.error();
    EXPECT_EQ((*exact)->stats().doc_count, 2U);
    EXPECT_EQ((*token)->stats().doc_count, 2U);

    std::vector<uint32_t> docids;
    ASSERT_TRUE(snii::query::term_query(
                        **exact, segment_v2::variant_term_codec::term_string("opened"), &docids)
                        .ok());
    EXPECT_EQ(docids, (std::vector<uint32_t> {0, 1}));
    docids.clear();
    ASSERT_TRUE(snii::query::term_query(
                        **exact, segment_v2::variant_term_codec::term_int64(53301496), &docids)
                        .ok());
    EXPECT_EQ(docids, (std::vector<uint32_t> {0}));
    // Key names are not values: the token index must not carry them.
    docids.clear();
    ASSERT_TRUE(snii::query::term_query(
                        **token, segment_v2::variant_term_codec::term_token("gravatar"), &docids)
                        .ok());
    EXPECT_EQ(docids, (std::vector<uint32_t> {}));
    docids.clear();
    ASSERT_TRUE(snii::query::term_query(
                        **token, segment_v2::variant_term_codec::term_token("stocksharp"), &docids)
                        .ok());
    EXPECT_EQ(docids, (std::vector<uint32_t> {0}));
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity,readability-function-size): one fixture pins the whole native merge, from tablet setup to term lookup.
void IndexStorageVariantCompactionReadTest::run_direct_merge_of_root_indexes(
        IndexCompactionKind kind, int64_t tablet_id, int64_t first_index_id) {
    const auto root_properties = [](std::string parser) {
        return std::map<std::string, std::string> {
                {"parser", std::move(parser)},
                {std::string(segment_v2::variant_root_index::VARIANT_INDEX_MODE_KEY),
                 std::string(segment_v2::variant_root_index::VARIANT_INDEX_MODE_ALL_VALUES)},
                {std::string(segment_v2::variant_root_index::VARIANT_ROOT_FORMAT_VERSION_KEY),
                 std::string(segment_v2::variant_root_index::VARIANT_ROOT_FORMAT_VERSION_CURRENT)}};
    };

    VariantColumnSpec variant;
    variant.unique_id = 2;
    variant.name = "v";
    variant.max_subcolumns_count = 1;

    IndexTabletOptions options;
    options.tablet_id = tablet_id;
    options.index_storage_format = InvertedIndexStorageFormatPB::SNII;
    options.variant_columns = {std::move(variant)};
    options.inverted_indexes = {
            IndexSpec::column_index(first_index_id, "idx_v_root_exact", 2, root_properties("none")),
            IndexSpec::column_index(first_index_id + 1, "idx_v_root_english", 2,
                                    root_properties("english")),
    };
    ASSERT_TRUE(create_tablet(options).ok());

    const auto make_variant_v2 = [](std::initializer_list<std::string_view> jsons) {
        auto values = ColumnVariantV2::create();
        DataTypeVariantV2SerDe serde;
        DataTypeSerDe::FormatOptions format_options;
        for (const std::string_view json : jsons) {
            Slice slice(json.data(), json.size());
            DORIS_CHECK(serde.deserialize_one_cell_from_json(*values, slice, format_options).ok());
        }
        return values->get_ptr();
    };

    IndexRowsetSpec rowset0;
    rowset0.version = 0;
    rowset0.batches.push_back(IndexBatch::single_variant_column(
            make_variant_v2(
                    {R"({"action":"opened","payload":{"repo":"Root Index","arr":[1,{"z":2}]}})",
                     R"({"action":"closed","note":"Other"})"}),
            0));
    IndexRowsetSpec rowset1;
    rowset1.version = 1;
    rowset1.batches.push_back(IndexBatch::single_variant_column(
            make_variant_v2(
                    {R"({"action":"opened","payload":{"repo":"Another Root","nested":{"v":3}}})",
                     R"({"action":"missing"})"}),
            100));
    auto rowsets = write_rowsets({rowset0, rowset1});
    ASSERT_TRUE(rowsets.has_value()) << rowsets.error();

    IndexReadOptions read_options;
    read_options.return_columns = {0, 1};
    read_options.collect_variant_values = true;
    auto before = read_rowsets(rowsets.value(), read_options);
    ASSERT_TRUE(before.has_value()) << before.error();

    const bool old_debug_points = config::enable_debug_points;
    const bool old_index_compaction = config::inverted_index_compaction_enable;
    config::enable_debug_points = true;
    config::inverted_index_compaction_enable = true;
    constexpr std::string_view kSessionPoint = "Compaction::before_add_snii_destination_session";
    constexpr std::string_view kValidationPoint =
            "Compaction::snii_validated_rowid_conversion_created";
    DEFER({
        DebugPoints::instance()->remove(std::string(kSessionPoint));
        DebugPoints::instance()->remove(std::string(kValidationPoint));
        config::enable_debug_points = old_debug_points;
        config::inverted_index_compaction_enable = old_index_compaction;
    });
    size_t session_count = 0;
    size_t validation_count = 0;
    std::function<void(size_t, Status*)> count_session = [&session_count](size_t, Status*) {
        ++session_count;
    };
    std::function<void()> count_validation = [&validation_count]() { ++validation_count; };
    DebugPoints::instance()->add_with_callback(std::string(kSessionPoint), count_session);
    DebugPoints::instance()->add_with_callback(std::string(kValidationPoint), count_validation);

    auto compacted = compact_rowsets(kind, rowsets.value());
    ASSERT_TRUE(compacted.has_value()) << compacted.error();
    ASSERT_NE(compacted.value(), nullptr);
    EXPECT_EQ(compacted.value()->num_rows(), 4);
    EXPECT_EQ(compacted.value()->num_segments(), 1);
    EXPECT_EQ(session_count, 2);
    EXPECT_EQ(validation_count, 1);

    auto after = read_rowsets({compacted.value()}, read_options);
    ASSERT_TRUE(after.has_value()) << after.error();
    EXPECT_EQ(after->variant_values_by_uid.at(2), before->variant_values_by_uid.at(2));
    auto compacted_probe = probe_rowset(compacted.value());
    ASSERT_TRUE(compacted_probe.has_value()) << compacted_probe.error();
    EXPECT_TRUE(has_sparse_path_stat(compacted_probe.value(), "payload.repo"));
    EXPECT_TRUE(has_sparse_path_stat(compacted_probe.value(), "payload.arr"));

    auto segment_path = compacted.value()->segment_path(0);
    ASSERT_TRUE(segment_path.has_value()) << segment_path.error();
    const std::string prefix(InvertedIndexDescriptor::get_index_file_path_prefix(*segment_path));
    IndexFileReader index_file_reader(
            compacted.value()->rowset_meta()->fs(), prefix, InvertedIndexStorageFormatPB::SNII,
            compacted.value()->segment(0).inverted_index_file_info(), options.tablet_id);
    ASSERT_TRUE(index_file_reader.init().ok());
    const auto root_indexes = compacted.value()->tablet_schema()->inverted_indexs(2);
    ASSERT_EQ(root_indexes.size(), 2);
    auto exact = index_file_reader.open_snii_index(root_indexes[0]);
    auto token = index_file_reader.open_snii_index(root_indexes[1]);
    ASSERT_TRUE(exact.has_value()) << exact.error();
    ASSERT_TRUE(token.has_value()) << token.error();
    EXPECT_EQ((*exact)->stats().doc_count, 4U);
    EXPECT_EQ((*token)->stats().doc_count, 4U);

    std::vector<uint32_t> docids;
    ASSERT_TRUE(snii::query::term_query(
                        **exact, segment_v2::variant_term_codec::term_string("opened"), &docids)
                        .ok());
    EXPECT_EQ(docids, (std::vector<uint32_t> {0, 2}));
    docids.clear();
    ASSERT_TRUE(snii::query::term_query(**token, segment_v2::variant_term_codec::term_token("root"),
                                        &docids)
                        .ok());
    EXPECT_EQ(docids, (std::vector<uint32_t> {0, 2}));
}

TEST_F(IndexStorageVariantCompactionReadTest,
       FullCompactionDirectMergesExactAndTokenRootIndexesIndependently) {
    run_direct_merge_of_root_indexes(IndexCompactionKind::FULL, 110057, 10058);
}

// Cumulative compaction shares merge_input_rowsets() with full compaction: the same native
// merge, planned per (column, index), applies to it.
TEST_F(IndexStorageVariantCompactionReadTest,
       CumulativeCompactionDirectMergesExactAndTokenRootIndexesIndependently) {
    run_direct_merge_of_root_indexes(IndexCompactionKind::CUMULATIVE, 110079, 10079);
}

TEST_F(IndexStorageVariantCompactionReadTest,
       FullCompactionDirectMergesOneRootSiblingAndRebuildsTheOther) {
    const auto root_properties = [](std::string parser) {
        return std::map<std::string, std::string> {
                {"parser", std::move(parser)},
                {std::string(segment_v2::variant_root_index::VARIANT_INDEX_MODE_KEY),
                 std::string(segment_v2::variant_root_index::VARIANT_INDEX_MODE_ALL_VALUES)},
                {std::string(segment_v2::variant_root_index::VARIANT_ROOT_FORMAT_VERSION_KEY),
                 std::string(segment_v2::variant_root_index::VARIANT_ROOT_FORMAT_VERSION_CURRENT)}};
    };

    VariantColumnSpec variant;
    variant.unique_id = 2;
    variant.name = "v";
    variant.max_subcolumns_count = 1;

    IndexTabletOptions options;
    options.tablet_id = 110068;
    options.index_storage_format = InvertedIndexStorageFormatPB::SNII;
    options.variant_columns = {std::move(variant)};
    options.inverted_indexes = {
            IndexSpec::column_index(10060, "idx_v_root_exact", 2, root_properties("none"))};
    ASSERT_TRUE(create_tablet(options).ok());

    const auto make_variant_v2 = [](std::initializer_list<std::string_view> jsons) {
        auto values = ColumnVariantV2::create();
        DataTypeVariantV2SerDe serde;
        DataTypeSerDe::FormatOptions format_options;
        for (const std::string_view json : jsons) {
            Slice slice(json.data(), json.size());
            DORIS_CHECK(serde.deserialize_one_cell_from_json(*values, slice, format_options).ok());
        }
        return values->get_ptr();
    };

    IndexRowsetSpec rowset0;
    rowset0.version = 0;
    rowset0.batches.push_back(IndexBatch::single_variant_column(
            make_variant_v2(
                    {R"({"action":"opened","payload":{"repo":"Root Index","arr":[1,{"z":2}]}})",
                     R"({"action":"closed","note":"Other"})"}),
            0));
    IndexRowsetSpec rowset1;
    rowset1.version = 1;
    rowset1.batches.push_back(IndexBatch::single_variant_column(
            make_variant_v2(
                    {R"({"action":"opened","payload":{"repo":"Another Root","nested":{"v":3}}})",
                     R"({"action":"missing"})"}),
            100));
    auto rowsets = write_rowsets({rowset0, rowset1});
    ASSERT_TRUE(rowsets.has_value()) << rowsets.error();

    IndexSchemaPatch patch;
    patch.add_inverted_indexes = {
            IndexSpec::column_index(10061, "idx_v_root_english", 2, root_properties("english"))};
    auto indexed_schema = build_patched_tablet_schema(*tablet_schema(), patch);
    ASSERT_NE(indexed_schema, nullptr);
    auto indexed_rowsets =
            inject_reader_schema_for_rowsets(rowsets.value(), std::move(indexed_schema));
    ASSERT_TRUE(indexed_rowsets.has_value()) << indexed_rowsets.error();

    IndexReadOptions read_options;
    read_options.return_columns = {0, 1};
    read_options.collect_variant_values = true;
    auto before = read_rowsets(indexed_rowsets.value(), read_options);
    ASSERT_TRUE(before.has_value()) << before.error();

    const bool old_debug_points = config::enable_debug_points;
    const bool old_index_compaction = config::inverted_index_compaction_enable;
    config::enable_debug_points = true;
    config::inverted_index_compaction_enable = true;
    constexpr std::string_view kSessionPoint = "Compaction::before_add_snii_destination_session";
    DEFER({
        DebugPoints::instance()->remove(std::string(kSessionPoint));
        config::enable_debug_points = old_debug_points;
        config::inverted_index_compaction_enable = old_index_compaction;
    });
    size_t session_count = 0;
    std::function<void(size_t, Status*)> count_session = [&session_count](size_t, Status*) {
        ++session_count;
    };
    DebugPoints::instance()->add_with_callback(std::string(kSessionPoint), count_session);

    auto compacted = compact_rowsets(IndexCompactionKind::FULL, indexed_rowsets.value());
    ASSERT_TRUE(compacted.has_value()) << compacted.error();
    ASSERT_NE(compacted.value(), nullptr);
    EXPECT_EQ(compacted.value()->num_rows(), 4);
    EXPECT_EQ(compacted.value()->num_segments(), 1);
    EXPECT_EQ(session_count, 1);

    auto after = read_rowsets({compacted.value()}, read_options);
    ASSERT_TRUE(after.has_value()) << after.error();
    EXPECT_EQ(after->variant_values_by_uid.at(2), before->variant_values_by_uid.at(2));
    auto compacted_probe = probe_rowset(compacted.value());
    ASSERT_TRUE(compacted_probe.has_value()) << compacted_probe.error();
    EXPECT_TRUE(has_sparse_path_stat(compacted_probe.value(), "payload.repo"));
    EXPECT_TRUE(has_sparse_path_stat(compacted_probe.value(), "payload.arr"));

    auto segment_path = compacted.value()->segment_path(0);
    ASSERT_TRUE(segment_path.has_value()) << segment_path.error();
    const std::string prefix(InvertedIndexDescriptor::get_index_file_path_prefix(*segment_path));
    IndexFileReader index_file_reader(
            compacted.value()->rowset_meta()->fs(), prefix, InvertedIndexStorageFormatPB::SNII,
            compacted.value()->segment(0).inverted_index_file_info(), options.tablet_id);
    ASSERT_TRUE(index_file_reader.init().ok());
    const auto root_indexes = compacted.value()->tablet_schema()->inverted_indexs(2);
    ASSERT_EQ(root_indexes.size(), 2);
    auto exact = index_file_reader.open_snii_index(root_indexes[0]);
    auto token = index_file_reader.open_snii_index(root_indexes[1]);
    ASSERT_TRUE(exact.has_value()) << exact.error();
    ASSERT_TRUE(token.has_value()) << token.error();
    EXPECT_EQ((*exact)->stats().doc_count, 4U);
    EXPECT_EQ((*token)->stats().doc_count, 4U);

    std::vector<uint32_t> docids;
    ASSERT_TRUE(snii::query::term_query(
                        **exact, segment_v2::variant_term_codec::term_string("opened"), &docids)
                        .ok());
    EXPECT_EQ(docids, (std::vector<uint32_t> {0, 2}));
    docids.clear();
    ASSERT_TRUE(snii::query::term_query(**token, segment_v2::variant_term_codec::term_token("root"),
                                        &docids)
                        .ok());
    EXPECT_EQ(docids, (std::vector<uint32_t> {0, 2}));
}

} // namespace doris::index_storage_test
