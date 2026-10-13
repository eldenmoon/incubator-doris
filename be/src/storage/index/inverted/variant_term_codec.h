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

#pragma once
#include <string>
#include <string_view>
namespace doris::segment_v2::variant_term_codec {
// Format 4 TOKEN encoding: prefix 0x07 followed by raw analyzer token bytes.
inline std::string_view token_term_prefix() {
    static constexpr char prefix[] = {'\x07'};
    return {prefix, 1};
}
inline std::string term_token(std::string_view value) {
    std::string term(token_term_prefix());
    term.append(value);
    return term;
}
} // namespace doris::segment_v2::variant_term_codec
