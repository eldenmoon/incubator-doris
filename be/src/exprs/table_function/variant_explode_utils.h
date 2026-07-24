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

#include "common/status.h"
#include "core/column/column.h"

namespace doris::variant_explode_internal {

// Materializes encoded Variant array elements into one Array<Nullable<Variant>> column. SQL NULL,
// typed-state values, and encoded non-arrays contribute an empty array. Variant null is retained as
// a non-SQL-null nested Variant value.
Status materialize_variant_array(const ColumnPtr& source, ColumnPtr* output);

} // namespace doris::variant_explode_internal
