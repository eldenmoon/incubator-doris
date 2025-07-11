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

#include "paimon_jni_reader.h"

#include <map>

#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "runtime/types.h"
#include "vec/core/types.h"
namespace doris {
class RuntimeProfile;
class RuntimeState;
namespace vectorized {
class Block;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {
#include "common/compile_check_begin.h"

const std::string PaimonJniReader::PAIMON_OPTION_PREFIX = "paimon.";
const std::string PaimonJniReader::HADOOP_OPTION_PREFIX = "hadoop.";

PaimonJniReader::PaimonJniReader(const std::vector<SlotDescriptor*>& file_slot_descs,
                                 RuntimeState* state, RuntimeProfile* profile,
                                 const TFileRangeDesc& range,
                                 const TFileScanRangeParams* range_params)
        : JniReader(file_slot_descs, state, profile) {
    std::vector<std::string> column_names;
    std::vector<std::string> column_types;
    for (const auto& desc : _file_slot_descs) {
        column_names.emplace_back(desc->col_name());
        column_types.emplace_back(JniConnector::get_jni_type_with_different_string(desc->type()));
    }
    std::map<String, String> params;
    params["db_name"] = range.table_format_params.paimon_params.db_name;
    params["table_name"] = range.table_format_params.paimon_params.table_name;
    params["paimon_split"] = range.table_format_params.paimon_params.paimon_split;
    params["paimon_column_names"] = range.table_format_params.paimon_params.paimon_column_names;
    params["paimon_predicate"] = range.table_format_params.paimon_params.paimon_predicate;
    params["ctl_id"] = std::to_string(range.table_format_params.paimon_params.ctl_id);
    params["db_id"] = std::to_string(range.table_format_params.paimon_params.db_id);
    params["tbl_id"] = std::to_string(range.table_format_params.paimon_params.tbl_id);
    params["last_update_time"] =
            std::to_string(range.table_format_params.paimon_params.last_update_time);
    params["required_fields"] = join(column_names, ",");
    params["columns_types"] = join(column_types, "#");
    if (range_params->__isset.serialized_table) {
        params["serialized_table"] = range_params->serialized_table;
    }
    if (range.table_format_params.__isset.table_level_row_count) {
        _remaining_table_level_row_count = range.table_format_params.table_level_row_count;
    } else {
        _remaining_table_level_row_count = -1;
    }

    // Used to create paimon option
    for (const auto& kv : range.table_format_params.paimon_params.paimon_options) {
        params[PAIMON_OPTION_PREFIX + kv.first] = kv.second;
    }
    if (range.table_format_params.paimon_params.__isset.hadoop_conf) {
        for (const auto& kv : range.table_format_params.paimon_params.hadoop_conf) {
            params[HADOOP_OPTION_PREFIX + kv.first] = kv.second;
        }
    }
    int64_t self_split_weight = range.__isset.self_split_weight ? range.self_split_weight : -1;
    _jni_connector = std::make_unique<JniConnector>("org/apache/doris/paimon/PaimonJniScanner",
                                                    params, column_names, self_split_weight);
}

Status PaimonJniReader::get_next_block(Block* block, size_t* read_rows, bool* eof) {
    if (_push_down_agg_type == TPushAggOp::type::COUNT && _remaining_table_level_row_count >= 0) {
        auto rows = std::min(_remaining_table_level_row_count,
                             (int64_t)_state->query_options().batch_size);
        _remaining_table_level_row_count -= rows;
        auto mutate_columns = block->mutate_columns();
        for (auto& col : mutate_columns) {
            col->resize(rows);
        }
        block->set_columns(std::move(mutate_columns));
        *read_rows = rows;
        if (_remaining_table_level_row_count == 0) {
            *eof = true;
        }

        return Status::OK();
    }
    return _jni_connector->get_next_block(block, read_rows, eof);
}

Status PaimonJniReader::init_reader(
        const std::unordered_map<std::string, ColumnValueRangeType>* colname_to_value_range) {
    _colname_to_value_range = colname_to_value_range;
    RETURN_IF_ERROR(_jni_connector->init(colname_to_value_range));
    return _jni_connector->open(_state, _profile);
}
#include "common/compile_check_end.h"
} // namespace doris::vectorized
