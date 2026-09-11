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

// BUILD INDEX / DROP INDEX for the VARIANT value-first index: the index is derived from the
// logical document, so it can be added to existing rowsets and removed again.

#include <gtest/gtest.h>

#include <map>
#include <string>
#include <vector>

#include "storage/index/index_file_reader.h"
#include "storage/index/inverted/inverted_index_desc.h"
#include "storage/index/inverted/variant_root_index.h"
#include "storage/index/snii/query/term_query.h"
#include "testutil/index_storage_test_util.h"

namespace doris::index_storage_test {
namespace {

std::map<std::string, std::string> root_properties(std::string parser) {
    return {{"parser", std::move(parser)},
            {std::string(segment_v2::variant_root_index::VARIANT_INDEX_MODE_KEY),
             std::string(segment_v2::variant_root_index::VARIANT_INDEX_MODE_ROOT)},
            {std::string(segment_v2::variant_root_index::VARIANT_ROOT_FORMAT_VERSION_KEY),
             std::string(segment_v2::variant_root_index::VARIANT_ROOT_FORMAT_VERSION_CURRENT)}};
}

} // namespace

class IndexStorageVariantRootIndexLifecycleTest : public IndexStorageTestFixture {};

TEST_F(IndexStorageVariantRootIndexLifecycleTest, BuildAndDropRootIndexesAfterExistingRows) {
    VariantColumnSpec variant;
    variant.unique_id = 2;
    variant.name = "v";
    IndexTabletOptions options;
    options.tablet_id = 110040;
    options.index_storage_format = InvertedIndexStorageFormatPB::SNII;
    options.variant_columns = {std::move(variant)};
    ASSERT_TRUE(create_tablet(options).ok());

    IndexRowsetSpec rowset0;
    rowset0.version = 0;
    rowset0.batches.push_back(
            IndexBatch::single_variant({R"({"action":"opened","body":"Root Index","n":5})",
                                        R"({"action":"closed","body":"Other","tags":["a","b"]})"},
                                       0));
    IndexRowsetSpec rowset1;
    rowset1.version = 1;
    rowset1.batches.push_back(IndexBatch::single_variant(
            {R"({"action":"opened","body":"Another Root","n":7.5})", R"({"missing":1})"}, 100));
    auto rowsets = write_rowsets({rowset0, rowset1});
    ASSERT_TRUE(rowsets.has_value()) << rowsets.error();

    const auto exact =
            IndexSpec::column_index(10060, "idx_v_root_exact", 2, root_properties("none"));
    const auto token =
            IndexSpec::column_index(10061, "idx_v_root_english", 2, root_properties("english"));
    auto built = build_inverted_indexes_and_reload({exact, token});
    ASSERT_TRUE(built.has_value()) << built.error();
    ASSERT_EQ(built->size(), 2);

    std::vector<std::vector<uint32_t>> opened_rows;
    for (const auto& rowset : built.value()) {
        auto probe = probe_rowset(rowset);
        ASSERT_TRUE(probe.has_value()) << probe.error();
        expect_index_files(probe.value(), true);

        auto segment_path = rowset->segment_path(0);
        ASSERT_TRUE(segment_path.has_value()) << segment_path.error();
        const std::string prefix(
                InvertedIndexDescriptor::get_index_file_path_prefix(*segment_path));
        IndexFileReader index_file_reader(
                rowset->rowset_meta()->fs(), prefix, InvertedIndexStorageFormatPB::SNII,
                rowset->segment(0).inverted_index_file_info(), options.tablet_id);
        ASSERT_TRUE(index_file_reader.init().ok());
        const auto root_indexes = rowset->tablet_schema()->inverted_indexs(2);
        ASSERT_EQ(root_indexes.size(), 2);
        auto exact_reader = index_file_reader.open_snii_index(root_indexes[0]);
        auto token_reader = index_file_reader.open_snii_index(root_indexes[1]);
        ASSERT_TRUE(exact_reader.has_value()) << exact_reader.error();
        ASSERT_TRUE(token_reader.has_value()) << token_reader.error();
        EXPECT_EQ((*exact_reader)->stats().doc_count, 2U);
        EXPECT_EQ((*token_reader)->stats().doc_count, 2U);
        std::vector<uint32_t> docids;
        ASSERT_TRUE(snii::query::term_query(
                            **exact_reader,
                            segment_v2::variant_root_index::encode_string_term("action", "opened"),
                            &docids)
                            .ok());
        opened_rows.push_back(docids);
        docids.clear();
        ASSERT_TRUE(snii::query::term_query(
                            **token_reader,
                            segment_v2::variant_root_index::encode_token_term("body", "root"),
                            &docids)
                            .ok());
        EXPECT_EQ(docids, opened_rows.back());
    }
    // rowset 0: row 0 opened; rowset 1: row 0 opened.
    EXPECT_EQ(opened_rows, (std::vector<std::vector<uint32_t>> {{0}, {0}}));
    {
        auto segment_path = built->front()->segment_path(0);
        ASSERT_TRUE(segment_path.has_value());
        const std::string prefix(
                InvertedIndexDescriptor::get_index_file_path_prefix(*segment_path));
        IndexFileReader index_file_reader(
                built->front()->rowset_meta()->fs(), prefix, InvertedIndexStorageFormatPB::SNII,
                built->front()->segment(0).inverted_index_file_info(), options.tablet_id);
        ASSERT_TRUE(index_file_reader.init().ok());
        auto exact_reader = index_file_reader.open_snii_index(
                built->front()->tablet_schema()->inverted_indexs(2)[0]);
        ASSERT_TRUE(exact_reader.has_value()) << exact_reader.error();
        std::vector<uint32_t> docids;
        // Numbers are typed and arrays are recursed, exactly as on the load path.
        ASSERT_TRUE(snii::query::term_query(
                            **exact_reader,
                            segment_v2::variant_root_index::encode_int64_term("n", 5), &docids)
                            .ok());
        EXPECT_EQ(docids, (std::vector<uint32_t> {0}));
        docids.clear();
        ASSERT_TRUE(snii::query::term_query(
                            **exact_reader,
                            segment_v2::variant_root_index::encode_string_term("tags", "b"),
                            &docids)
                            .ok());
        EXPECT_EQ(docids, (std::vector<uint32_t> {1}));
    }

    auto dropped = drop_inverted_indexes_and_reload({exact, token});
    ASSERT_TRUE(dropped.has_value()) << dropped.error();
    ASSERT_EQ(dropped->size(), 2);
    for (const auto& rowset : dropped.value()) {
        auto probe = probe_rowset(rowset);
        ASSERT_TRUE(probe.has_value()) << probe.error();
        expect_index_files(probe.value(), false);
        EXPECT_TRUE(rowset->tablet_schema()->inverted_indexs(2).empty());
    }
    IndexReadOptions read_options;
    read_options.return_columns = {0, 1};
    read_options.collect_variant_values = true;
    auto after_drop = read_rowsets(dropped.value(), read_options);
    ASSERT_TRUE(after_drop.has_value()) << after_drop.error();
    EXPECT_EQ(after_drop->rows_read, 4);
}

} // namespace doris::index_storage_test
