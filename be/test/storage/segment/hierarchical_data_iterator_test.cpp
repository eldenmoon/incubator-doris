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

#include "storage/segment/variant/hierarchical_data_iterator.h"

#include <gtest/gtest.h>

#include <cstring>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "core/column/column_map.h"
#include "core/column/column_string.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type/data_type_string.h"
#include "storage/segment/variant/binary_column_extract_iterator.h"

namespace doris::segment_v2 {

class DummySparseIterator final : public ColumnIterator {
public:
    Status init(const ColumnIteratorOptions&) override { return Status::OK(); }
    Status seek_to_ordinal(ordinal_t) override { return Status::OK(); }
    ordinal_t get_current_ordinal() const override { return 0; }

    Status next_batch(size_t* rows, MutableColumnPtr& dst, bool*) override {
        if (*rows < 2) {
            return Status::InvalidArgument("Dummy sparse reader requires room for two rows");
        }
        *rows = 2;
        return fill(dst);
    }

    Status read_by_rowids(const rowid_t*, const size_t count, MutableColumnPtr& dst) override {
        if (count != 2) {
            return Status::InvalidArgument("Dummy sparse reader requires two rows");
        }
        return fill(dst);
    }

private:
    static Status fill(MutableColumnPtr& dst) {
        auto* map = check_and_get_column<ColumnMap>(dst.get());
        if (map == nullptr) {
            return Status::InvalidArgument("Dummy sparse destination is not a map");
        }
        auto& keys = assert_cast<ColumnString&>(map->get_keys());
        auto& values = assert_cast<ColumnString&>(map->get_values());
        auto& offsets = map->get_offsets();

        DataTypePtr string_type = std::make_shared<DataTypeString>();
        auto strings = string_type->create_column();
        auto serde = string_type->get_serde();
        strings->insert_data("abcvalues", strlen("abcvalues"));
        strings->insert_data("abdvalues", strlen("abdvalues"));
        strings->insert_data("abcvalues", strlen("abcvalues"));
        strings->insert_data("abevalues", strlen("abevalues"));
        strings->insert_data("axvalues", strlen("axvalues"));
        ColumnString::Chars& chars = values.get_chars();
        for (size_t index = 0; index < 5; ++index) {
            serde->write_one_cell_to_binary(*strings, chars, index);
            values.get_offsets().push_back(chars.size());
        }

        keys.insert_data("a.b.c", strlen("a.b.c"));
        keys.insert_data("a.b.d", strlen("a.b.d"));
        offsets.push_back(keys.size());
        keys.insert_data("a.b.c", strlen("a.b.c"));
        keys.insert_data("a.b.e", strlen("a.b.e"));
        keys.insert_data("a.x", strlen("a.x"));
        offsets.push_back(keys.size());
        return Status::OK();
    }
};

class MalformedSparseIterator final : public ColumnIterator {
public:
    explicit MalformedSparseIterator(std::vector<std::string> paths) : _paths(std::move(paths)) {}

    Status init(const ColumnIteratorOptions&) override { return Status::OK(); }
    Status seek_to_ordinal(ordinal_t) override { return Status::OK(); }
    ordinal_t get_current_ordinal() const override { return 0; }

    Status next_batch(size_t* rows, MutableColumnPtr& dst, bool*) override {
        if (*rows == 0) {
            return Status::InvalidArgument("Malformed sparse reader requires one row");
        }
        *rows = 1;
        return fill(dst);
    }

    Status read_by_rowids(const rowid_t*, const size_t count, MutableColumnPtr& dst) override {
        if (count != 1) {
            return Status::InvalidArgument("Malformed sparse reader requires one row");
        }
        return fill(dst);
    }

private:
    Status fill(MutableColumnPtr& dst) const {
        auto* map = check_and_get_column<ColumnMap>(dst.get());
        if (map == nullptr) {
            return Status::InvalidArgument("Malformed sparse destination is not a map");
        }
        auto& keys = assert_cast<ColumnString&>(map->get_keys());
        auto& values = assert_cast<ColumnString&>(map->get_values());
        for (const std::string& path : _paths) {
            keys.insert_data(path.data(), path.size());
            values.insert_data("x", 1);
        }
        map->get_offsets().push_back(keys.size());
        return Status::OK();
    }

    std::vector<std::string> _paths;
};

static BinaryColumnCacheSPtr make_binary_cache(ColumnIteratorUPtr iterator) {
    return std::make_shared<BinaryColumnCache>(
            std::move(iterator), ColumnMap::create(ColumnString::create(), ColumnString::create(),
                                                   ColumnArray::ColumnOffsets::create()));
}

TEST(HierarchicalDataIteratorTest, ProcessSparseExtractSubpathsIntoVariantV2) {
    auto sparse = std::make_unique<SubstreamIterator>(
            ColumnMap::create(ColumnString::create(), ColumnString::create(),
                              ColumnArray::ColumnOffsets::create()),
            std::make_unique<DummySparseIterator>(), nullptr);
    ColumnIteratorUPtr iterator;
    OlapReaderStatistics stats;
    ASSERT_TRUE(HierarchicalDataIterator::create(
                        &iterator, 0, PathInData("a.b"), nullptr, std::move(sparse), nullptr,
                        nullptr, &stats, HierarchicalDataIterator::ReadType::SUBCOLUMNS_AND_SPARSE)
                        .ok());

    ColumnIteratorOptions options;
    options.stats = &stats;
    ASSERT_TRUE(iterator->init(options).ok());
    ASSERT_TRUE(iterator->seek_to_ordinal(0).ok());
    MutableColumnPtr dst = ColumnVariantV2::create();
    size_t rows = 2;
    bool has_null = false;
    ASSERT_TRUE(iterator->next_batch(&rows, dst, &has_null).ok());

    const auto& variant = assert_cast<const ColumnVariantV2&>(*dst);
    VariantRef value;
    const VariantRef row0 = variant.get_value_ref(0);
    ASSERT_TRUE(row0.object_find(StringRef("c"), &value));
    EXPECT_EQ(value.get_string(), StringRef("abcvalues"));
    ASSERT_TRUE(row0.object_find(StringRef("d"), &value));
    EXPECT_EQ(value.get_string(), StringRef("abdvalues"));
    EXPECT_FALSE(row0.object_find(StringRef("e"), &value));

    const VariantRef row1 = variant.get_value_ref(1);
    ASSERT_TRUE(row1.object_find(StringRef("c"), &value));
    EXPECT_EQ(value.get_string(), StringRef("abcvalues"));
    ASSERT_TRUE(row1.object_find(StringRef("e"), &value));
    EXPECT_EQ(value.get_string(), StringRef("abevalues"));
    EXPECT_FALSE(row1.object_find(StringRef("x"), &value));
}

TEST(HierarchicalDataIteratorTest, NextBatchUsesActualShortFinalBatchSize) {
    auto sparse = std::make_unique<SubstreamIterator>(
            ColumnMap::create(ColumnString::create(), ColumnString::create(),
                              ColumnArray::ColumnOffsets::create()),
            std::make_unique<DummySparseIterator>(), nullptr);
    ColumnIteratorUPtr iterator;
    OlapReaderStatistics stats;
    ASSERT_TRUE(HierarchicalDataIterator::create(
                        &iterator, 0, PathInData("a.b"), nullptr, std::move(sparse), nullptr,
                        nullptr, &stats, HierarchicalDataIterator::ReadType::SUBCOLUMNS_AND_SPARSE)
                        .ok());

    ColumnIteratorOptions options;
    options.stats = &stats;
    ASSERT_TRUE(iterator->init(options).ok());
    ASSERT_TRUE(iterator->seek_to_ordinal(0).ok());
    MutableColumnPtr dst = ColumnVariantV2::create();
    size_t rows = 8;
    ASSERT_TRUE(iterator->next_batch(&rows, dst).ok());
    EXPECT_EQ(rows, 2);
    EXPECT_EQ(dst->size(), 2);
}

TEST(BinaryColumnExtractIteratorTest, NextBatchUsesActualShortFinalBatchSize) {
    OlapReaderStatistics stats;
    StorageReadOptions read_options;
    read_options.stats = &stats;
    BinaryColumnExtractIterator iterator(
            "a.b.c", make_binary_cache(std::make_unique<DummySparseIterator>()), &read_options);
    ColumnIteratorOptions iterator_options;
    iterator_options.stats = &stats;
    ASSERT_TRUE(iterator.init(iterator_options).ok());
    ASSERT_TRUE(iterator.seek_to_ordinal(0).ok());

    MutableColumnPtr dst = ColumnVariantV2::create();
    size_t rows = 8;
    ASSERT_TRUE(iterator.next_batch(&rows, dst, nullptr).ok());
    EXPECT_EQ(rows, 2);
    EXPECT_EQ(dst->size(), 2);
    EXPECT_EQ(assert_cast<const ColumnVariantV2&>(*dst).get_value_ref(0).get_string(),
              StringRef("abcvalues"));
}

TEST(BinaryColumnExtractIteratorTest, RejectsUnsortedAndDuplicateSparsePaths) {
    for (const std::vector<std::string>& paths :
         {std::vector<std::string> {"b", "a"}, std::vector<std::string> {"a", "a"}}) {
        SCOPED_TRACE(testing::PrintToString(paths));
        OlapReaderStatistics stats;
        StorageReadOptions read_options;
        read_options.stats = &stats;
        BinaryColumnExtractIterator iterator(
                "a", make_binary_cache(std::make_unique<MalformedSparseIterator>(paths)),
                &read_options);
        ColumnIteratorOptions iterator_options;
        iterator_options.stats = &stats;
        ASSERT_TRUE(iterator.init(iterator_options).ok());
        ASSERT_TRUE(iterator.seek_to_ordinal(0).ok());

        MutableColumnPtr dst = ColumnVariantV2::create();
        size_t rows = 1;
        const Status status = iterator.next_batch(&rows, dst, nullptr);
        EXPECT_TRUE(status.is<ErrorCode::CORRUPTION>()) << status;
        EXPECT_EQ(dst->size(), 0);
    }
}

} // namespace doris::segment_v2
