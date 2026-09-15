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

#include "core/value/variant/variant_leaf_visitor.h"

#include <cmath>
#include <limits>

#include "common/exception.h"

namespace doris {

std::optional<VariantCanonicalNumber> canonical_numeric_from_int64(int64_t value) {
    VariantCanonicalNumber number;
    number.kind = VariantLeafKind::INT64;
    number.int64_value = value;
    return number;
}

std::optional<VariantCanonicalNumber> canonical_numeric_from_uint64(uint64_t value) {
    if (value <= static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
        return canonical_numeric_from_int64(static_cast<int64_t>(value));
    }
    VariantCanonicalNumber number;
    number.kind = VariantLeafKind::UINT64;
    number.uint64_value = value;
    return number;
}

std::optional<VariantCanonicalNumber> canonical_numeric_from_double(double value) {
    if (std::isnan(value)) {
        return std::nullopt;
    }
    // Mixed numeric VARIANT paths are promoted to FLOAT/DOUBLE only when the floating mantissa can
    // represent the integer width losslessly, so folding integral floating values into the same
    // signed/unsigned domain keeps cross-type equality one term.
    if (std::isfinite(value) && std::trunc(value) == value) {
        if (value >= -0x1p63 && value < 0x1p63) {
            return canonical_numeric_from_int64(static_cast<int64_t>(value));
        }
        if (value >= 0x1p63 && value < 0x1p64) {
            return canonical_numeric_from_uint64(static_cast<uint64_t>(value));
        }
    }
    VariantCanonicalNumber number;
    number.kind = VariantLeafKind::DOUBLE;
    number.double_value = value == 0.0 ? 0.0 : value; // fold -0.0
    return number;
}

namespace {

void apply_number(VariantLeaf* leaf, const std::optional<VariantCanonicalNumber>& number) {
    if (!number.has_value()) {
        leaf->kind = VariantLeafKind::OTHER;
        return;
    }
    leaf->kind = number->kind;
    leaf->int64_value = number->int64_value;
    leaf->uint64_value = number->uint64_value;
    leaf->double_value = number->double_value;
}

} // namespace

VariantLeaf classify_variant_leaf(std::string_view path, const VariantRef& value) {
    VariantLeaf leaf;
    leaf.path = path;
    leaf.value = value;
    leaf.kind = VariantLeafKind::OTHER;
    switch (value.basic_type()) {
    case VariantBasicType::SHORT_STRING:
        leaf.kind = VariantLeafKind::STRING;
        leaf.string_value = value.get_string();
        return leaf;
    case VariantBasicType::PRIMITIVE:
        break;
    default:
        return leaf; // OBJECT / ARRAY
    }
    if (value.is_null()) {
        return leaf;
    }
    switch (value.primitive_id()) {
    case VariantPrimitiveId::TRUE_VALUE:
    case VariantPrimitiveId::FALSE_VALUE:
        leaf.kind = VariantLeafKind::BOOL;
        leaf.bool_value = value.get_bool();
        return leaf;
    case VariantPrimitiveId::INT8:
    case VariantPrimitiveId::INT16:
    case VariantPrimitiveId::INT32:
    case VariantPrimitiveId::INT64:
        apply_number(&leaf, canonical_numeric_from_int64(value.get_int()));
        return leaf;
    case VariantPrimitiveId::FLOAT:
        apply_number(&leaf, canonical_numeric_from_double(static_cast<double>(value.get_float())));
        return leaf;
    case VariantPrimitiveId::DOUBLE:
        apply_number(&leaf, canonical_numeric_from_double(value.get_double()));
        return leaf;
    case VariantPrimitiveId::STRING:
        leaf.kind = VariantLeafKind::STRING;
        leaf.string_value = value.get_string();
        return leaf;
    default:
        return leaf; // decimal, temporal, binary, UUID, ...: path exists, no indexable value
    }
}

namespace {

class LeafWalker {
public:
    LeafWalker(const VariantVisitOptions& options, const VariantLeafCallback& callback)
            : _options(options), _callback(callback), _path(options.path_prefix) {}

    Status walk(const VariantRef& value) {
        switch (value.basic_type()) {
        case VariantBasicType::OBJECT:
            return walk_object(value);
        case VariantBasicType::ARRAY:
            if (_options.recurse_arrays) {
                return walk_array(value);
            }
            if (value.num_elements() == 0) {
                return Status::OK(); // empty containers never call back
            }
            return emit(value);
        default:
            if (value.is_null()) {
                return Status::OK();
            }
            return emit(value);
        }
    }

private:
    Status emit(const VariantRef& value) { return _callback(classify_variant_leaf(_path, value)); }

    Status walk_object(const VariantRef& value) {
        const VariantRef::ObjectView object = value.object_view();
        const size_t saved = _path.size();
        for (uint32_t index = 0; index < object.size(); ++index) {
            uint32_t field = 0;
            const VariantRef child = object.value_at(index, &field);
            if (!_path.empty()) {
                _path.push_back('.');
            }
            const StringRef key = value.metadata.key_at(field);
            _path.append(key.data, key.size);
            RETURN_IF_ERROR(walk(child));
            _path.resize(saved);
        }
        return Status::OK();
    }

    Status walk_array(const VariantRef& value) {
        const uint32_t count = value.num_elements();
        for (uint32_t index = 0; index < count; ++index) {
            RETURN_IF_ERROR(walk(value.array_at(index)));
        }
        return Status::OK();
    }

    const VariantVisitOptions& _options;
    const VariantLeafCallback& _callback;
    std::string _path;
};

} // namespace

Status visit_variant_leaves(const VariantRef& root, const VariantVisitOptions& options,
                            const VariantLeafCallback& callback) {
    try {
        LeafWalker walker(options, callback);
        return walker.walk(root);
    } catch (const Exception& exception) {
        return exception.to_status();
    }
}

} // namespace doris
