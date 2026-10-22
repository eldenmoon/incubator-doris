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

#include <array>
#include <cstdlib>
#include <string>
#include <vector>

#include "agent/be_exec_version_manager.h"
#include "core/block/block.h"
#include "core/column/column_const.h"
#include "core/column/column_varbinary.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type/data_type_variant_v2.h"
#include "core/value/variant/variant_batch_builder.h"

namespace doris {
namespace {

ColumnVariantV2::MutablePtr string_variant(const std::string& payload, size_t rows) {
    VariantBatchBuilder builder;
    for (size_t index = 0; index < rows; ++index) {
        auto row = builder.begin_row();
        row.add_string(StringRef(payload));
        row.finish();
    }
    auto batch = builder.finish_batch();
    auto column = ColumnVariantV2::create();
    column->insert_encoded_batch(batch);
    return column;
}

TEST(VariantBinaryStorageTest, AppendKeepsLongPayloadInItsArenaChunk) {
    const std::string payload(32768, 'x');
    auto source = string_variant(payload, 1);
    auto destination = ColumnVariantV2::create();
    destination->insert_range_from(*source, 0, 1);
    const auto first = destination->get_value_ref(0).value;
    for (size_t row = 0; row < 64; ++row) {
        destination->insert_range_from(*source, 0, 1);
    }
    EXPECT_EQ(destination->get_value_ref(0).value.data, first.data);
    EXPECT_EQ(destination->get_value_ref(64).get_string(), StringRef(payload));
}

TEST(VariantBinaryStorageTest, RepeatedInPlaceFilterDoesNotCopyLongValues) {
    auto column = ColumnVarbinary::create();
    const std::string payload(32768, 'x');
    for (size_t row = 0; row < 64; ++row) {
        column->insert_data(payload.data(), payload.size());
    }
    const size_t allocated = column->allocated_bytes();
    const char* first = column->get_data_at(0).data;
    IColumn::Filter filter(64, 1);
    for (size_t iteration = 0; iteration < 4; ++iteration) {
        EXPECT_EQ(column->filter(filter), 64);
        EXPECT_EQ(column->allocated_bytes(), allocated);
        EXPECT_EQ(column->get_data_at(0).data, first);
    }
    filter[1] = 0;
    EXPECT_EQ(column->filter(filter), 63);
    column->erase(1, 61);
    ASSERT_EQ(column->size(), 2);
    EXPECT_EQ(column->get_data_at(1), StringRef(payload));
}

void expect_binary_roundtrip(const IColumn& source, int version, std::vector<char>& bytes) {
    const DataTypeVariantV2 type;
    bytes.resize(type.get_uncompressed_serialized_bytes(source, version));
    const char* end = type.serialize(source, bytes.data(), version);
    ASSERT_EQ(end, bytes.data() + bytes.size());
    MutableColumnPtr restored = type.create_column();
    EXPECT_EQ(type.deserialize(bytes.data(), &restored, version), end);
    ASSERT_EQ(restored->size(), source.size());
    const auto materialized = restored->convert_to_full_column_if_const();
    const auto expected = source.convert_to_full_column_if_const();
    EXPECT_EQ(materialized->compare_at(0, 0, *expected, 1), 0);
    EXPECT_EQ(materialized->compare_at(source.size() - 1, source.size() - 1, *expected, 1), 0);
}

TEST(VariantBinaryStorageTest, EncodedAndConstValuesUseOneBinaryLayout) {
    auto column = string_variant(std::string(32768, 'x'), 4);
    for (const bool constant : {false, true}) {
        ColumnPtr source = column->get_ptr();
        if (constant) {
            source = ColumnConst::create(column->clone_resized(1), 17);
        }
        std::vector<char> first_encoding;
        expect_binary_roundtrip(*source, 10, first_encoding);
        std::vector<char> current_encoding;
        expect_binary_roundtrip(*source, BeExecVersionManager::get_newest_version(),
                                current_encoding);
        EXPECT_EQ(current_encoding, first_encoding);
    }
}

TEST(VariantBinaryStorageTest, JoinBuildMergeRetainsPayloadAndCopyOnWrite) {
    const auto type = std::make_shared<DataTypeVariantV2>();
    Block output {{ColumnVariantV2::create(), type, "v"}};
    const std::string payload(65536, 'x');
    auto input = string_variant(payload, 32);
    Block source {{input->get_ptr(), type, "v"}};
    {
        ScopedMutableBlock scoped(&output);
        for (size_t batch = 0; batch < 8; ++batch) {
            const auto status = scoped.mutable_block().merge_ignore_overflow(source);
            ASSERT_TRUE(status.ok()) << status;
        }
    }
    ASSERT_EQ(output.rows(), 256);
    const auto& original = assert_cast<const ColumnVariantV2&>(*output.get_by_position(0).column);
    auto selected = original.filter(IColumn::Filter(256, 1), 256);
    auto detached = IColumn::mutate(std::move(selected));
    detached->erase(1, 255);
    EXPECT_EQ(detached->size(), 1);
    EXPECT_EQ(original.size(), 256);
    EXPECT_EQ(original.get_value_ref(255).get_string(), StringRef(payload));
}

// Opt in because this exercises real uint32 boundaries and retains several GiB of tracked data.
// DORIS_VARIANT_BINARY_LARGE_TEST=1 ./run-be-ut.sh --run --filter=VariantBinaryStorageTest.LargeBatch
TEST(VariantBinaryStorageTest, LargeBatch) {
    if (std::getenv("DORIS_VARIANT_BINARY_LARGE_TEST") == nullptr) {
        GTEST_SKIP() << "Set DORIS_VARIANT_BINARY_LARGE_TEST=1 for the real >4-GiB boundary";
    }
    constexpr size_t ROWS = 4096;
    const std::string payload(1024 * 1024, 'x');
    auto column = string_variant(payload, ROWS);
    EXPECT_GT(column->byte_size(), uint64_t {1} << 32);
    for (const size_t row : {size_t {0}, ROWS / 2, ROWS - 1}) {
        EXPECT_EQ(column->get_value_ref(row).get_string(), StringRef(payload));
    }
    auto extra = string_variant(payload, 1);
    column->insert_range_from(*extra, 0, 1);
    EXPECT_EQ(column->size(), ROWS + 1);
    EXPECT_EQ(column->get_value_ref(ROWS).get_string(), StringRef(payload));

    const DataTypeVariantV2 type;
    const int VERSION = BeExecVersionManager::get_newest_version();
    DorisVector<char> bytes(type.get_uncompressed_serialized_bytes(*column, VERSION));
    const char* end = type.serialize(*column, bytes.data(), VERSION);
    ASSERT_EQ(end, bytes.data() + bytes.size());
    MutableColumnPtr restored = type.create_column();
    EXPECT_EQ(type.deserialize(bytes.data(), &restored, VERSION), end);
    const auto& restored_values = assert_cast<const ColumnVariantV2&>(*restored);
    ASSERT_EQ(restored_values.size(), ROWS + 1);
    EXPECT_EQ(restored_values.get_value_ref(ROWS).get_string(), StringRef(payload));
}

} // namespace
} // namespace doris
