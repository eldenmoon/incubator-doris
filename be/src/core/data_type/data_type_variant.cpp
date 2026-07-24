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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/DataTypes/DataTypeObject.cpp
// and modified by Doris

#include "core/data_type/data_type_variant.h"

#include <memory>

#include "common/exception.h"
#include "core/column/column.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type_serde/data_type_variant_v2_serde.h"
#include "core/typeid_cast.h"

namespace doris {
class IColumn;
} // namespace doris

namespace doris {

DataTypeVariant::DataTypeVariant(int32_t max_subcolumns_count)
        : DataTypeVariant(max_subcolumns_count, false) {}

DataTypeVariant::DataTypeVariant(int32_t max_subcolumns_count, bool enable_doc_mode)
        : _max_subcolumns_count(max_subcolumns_count), _enable_doc_mode(enable_doc_mode) {
    name = fmt::format("Variant(max subcolumns count = {}, enable doc mode = {})",
                       max_subcolumns_count, enable_doc_mode);
}
bool DataTypeVariant::equals(const IDataType& rhs) const {
    return typeid_cast<const DataTypeVariant*>(&rhs) != nullptr;
}

int64_t DataTypeVariant::get_uncompressed_serialized_bytes(const IColumn& column,
                                                           int be_exec_version) const {
    return DataTypeVariantV2SerDe::get_uncompressed_serialized_bytes(column, be_exec_version);
}

char* DataTypeVariant::serialize(const IColumn& column, char* buf, int be_exec_version) const {
    return DataTypeVariantV2SerDe::serialize(column, buf, be_exec_version);
}

Field DataTypeVariant::get_field(const TExprNode& node) const {
    if (node.node_type == TExprNodeType::NULL_LITERAL) {
        return {};
    }
    throw Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                    "Variant literals other than NULL must be bound through an explicit CAST");
}

const char* DataTypeVariant::deserialize(const char* buf, MutableColumnPtr* column,
                                         int be_exec_version) const {
    return DataTypeVariantV2SerDe::deserialize(buf, column, be_exec_version);
}

void DataTypeVariant::to_pb_column_meta(PColumnMeta* col_meta) const {
    IDataType::to_pb_column_meta(col_meta);
    col_meta->set_variant_max_subcolumns_count(_max_subcolumns_count);
    col_meta->set_variant_enable_doc_mode(_enable_doc_mode);
}

MutableColumnPtr DataTypeVariant::create_column() const {
    return ColumnVariantV2::create();
}

} // namespace doris
