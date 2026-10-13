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

#include "storage/index/inverted/variant_term_codec.h"

#include <gtest/gtest.h>
namespace doris::segment_v2::variant_term_codec {
TEST(VariantTermCodecTest, TokenEncodingRetainsFormatFour) {
    EXPECT_EQ(token_term_prefix(), std::string_view("\x07", 1));
    EXPECT_EQ(term_token("hello"), std::string("\x07hello", 6));
    EXPECT_EQ(term_token(std::string_view("a\0b", 3)), std::string("\x07"
                                                                   "a\0b",
                                                                   4));
}
} // namespace doris::segment_v2::variant_term_codec
