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

#include <cstdlib>
#include <functional>
#include <string>
#include <unordered_map>

#include "core/column/column_string.h"

namespace doris {

class VariantUtil {
public:
    using VariantStringCreator = std::function<void(ColumnString*, size_t)>;

    static std::unordered_map<std::string, int> fill_string_column_with_test_data(
            auto& column_string, int size, std::unordered_map<int, std::string>* inserted_jsonstr) {
        std::unordered_map<std::string, int> all_path_stats;
        std::srand(42);
        for (int i = 0; i < size; i++) {
            std::string json_str = "{";
            int num_pairs = std::rand() % 10 + 1;
            for (int j = 0; j < num_pairs; j++) {
                std::string key = "key" + std::to_string(j);
                if (j % 2 == 0) {
                    int value = 88;
                    json_str += "\"" + key + "\":" + std::to_string(value);
                } else {
                    std::string value = "str" + std::to_string(99);
                    json_str += "\"" + key + "\":\"" + value + "\"";
                }
                if (j < num_pairs - 1) {
                    json_str += ",";
                }
                all_path_stats[key] += 1;
            }
            json_str += "}";
            column_string->insert_data(json_str.data(), json_str.size());
            (*inserted_jsonstr)[i] = json_str;
        }
        return all_path_stats;
    }

    static std::unordered_map<std::string, int> fill_string_column_with_nested_test_data(
            auto& column_string, int size, std::unordered_map<int, std::string>* inserted_jsonstr) {
        std::unordered_map<std::string, int> all_path_stats;
        std::srand(42);
        for (int i = 0; i < size; i++) {
            std::string json_str = "{";

            int num_paths = std::rand() % 9 + 2;
            int current_path = 0;

            json_str += "\"key0\":{";
            json_str += "\"key1\":{";
            json_str += "\"key2\":" + std::to_string(88) + ",";
            json_str += R"("key3":")" + std::to_string(88) + "\"";
            json_str += "},";
            json_str += "\"key4\":" + std::to_string(88);
            json_str += "},";

            all_path_stats["key0.key1.key2"] += 1;
            all_path_stats["key0.key1.key3"] += 1;
            all_path_stats["key0.key4"] += 1;
            current_path += 3;

            while (current_path < num_paths) {
                std::string key = "key" + std::to_string(current_path);
                if (std::rand() % 2 == 0) {
                    json_str += "\"" + key + "\":{";
                    json_str +=
                            "\"nested" + std::to_string(current_path) + "\":" + std::to_string(88);
                    json_str += "},";
                    all_path_stats[key + ".nested" + std::to_string(current_path)] += 1;
                } else {
                    json_str += "\"" + key + "\":\"" + std::to_string(88) + "\",";
                    all_path_stats[key] += 1;
                }
                current_path++;
            }

            json_str = json_str.substr(0, json_str.length() - 1);
            json_str += "}";

            column_string->insert_data(json_str.data(), json_str.size());
            (*inserted_jsonstr)[i] = json_str;
        }
        return all_path_stats;
    }

    static void fill_string_column_with_nested_data(auto& column_string, int size) {
        for (int i = 0; i < size; ++i) {
            std::string inserted_jsonstr = R"({"a": {"b": [{"c": {"d": )" + std::to_string(i) +
                                           R"(, "e": ")" + std::to_string(i) + R"("}}]}, "x": ")" +
                                           std::to_string(i) + R"("})";
            if (i % 17 == 0) {
                inserted_jsonstr = R"({"a": {"b": [{"c": {"d": )" + std::to_string(i) +
                                   R"(, "e": ")" + std::to_string(i) + R"(", "f": )" +
                                   std::to_string(i) + R"(}}]}, "x": ")" + std::to_string(i) +
                                   R"("})";
            }
            if (i % 177 == 0) {
                inserted_jsonstr = R"({"a": {"b": [{"c": {"d": )" + std::to_string(i) +
                                   R"(, "e": ")" + std::to_string(i) + R"("}}]}, "x": ")" +
                                   std::to_string(i) + R"(", "z": )" + std::to_string(i) + R"("})";
            }
            column_string->insert_data(inserted_jsonstr.data(), inserted_jsonstr.size());
        }
    }
};

} // namespace doris
