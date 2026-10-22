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

#include "core/column/column_varbinary.h"

#include <crc32c/crc32c.h>
#include <glog/logging.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <vector>

#include "core/arena.h"
#include "core/assert_cast.h"
#include "core/column/column.h"
#include "core/column/column_string.h"
#include "core/column/columns_common.h"
#include "core/data_type/primitive_type.h"
#include "exec/sort/sort_block.h"
#include "util/hash_util.hpp"

namespace doris {

void ColumnVarbinary::insert_many_continuous_binary_data(const char* data, const uint32_t* offsets,
                                                         size_t num) {
    reserve(size() + num);
    for (size_t row = 0; row < num; ++row) {
        insert_data(data + offsets[row], offsets[row + 1] - offsets[row]);
    }
}

void ColumnVarbinary::insert_many_dict_data(const int32_t* data_array, size_t start_index,
                                            const StringRef* dict, size_t data_num,
                                            uint32_t dict_num) {
    reserve(size() + data_num);
    // Decoder pages can be released after the call; copy long dictionary entries into our arena.
    for (size_t row = start_index; row < start_index + data_num; ++row) {
        const auto& value = dict[data_array[row]];
        insert_data(value.data, value.size);
    }
}

MutableColumnPtr ColumnVarbinary::clone_resized(size_t size) const {
    auto res = create();
    if (size == 0) {
        return res;
    }
    auto& new_col = *res;
    auto& new_data = new_col.get_data();
    const size_t count = std::min(this->size(), size);
    new_data.reserve(size);
    new_data.resize(count);
    // A short value owns its bytes inside the descriptor, so copying descriptors already clones it.
    // Only long values need their payload copied into the new arena.
    if (count != 0) {
        std::copy_n(_data.data(), count, new_data.data());
    }
    for (size_t row = 0; row < count; ++row) {
        auto& value = new_data[row];
        if (!value.isInline()) {
            const char* dst = new_col._arena.insert(value.data(), value.size());
            value = StringView(dst, value.size());
        }
    }
    if (size > count) {
        new_col.insert_many_defaults(size - count);
    }
    return res;
}

void ColumnVarbinary::insert_range_from(const IColumn& src, size_t start, size_t length) {
    if (length == 0) {
        return;
    }
    const auto& src_col = assert_cast<const ColumnVarbinary&>(src);

    if (start + length > src_col.size()) {
        throw doris::Exception(doris::ErrorCode::INTERNAL_ERROR,
                               "Parameters start = {}, length = {} are out of bound in "
                               "ColumnVarbinary::insert_range_from method (data.size() = {})",
                               start, length, src_col.size());
    }

    const size_t old_size = _data.size();
    if (static_cast<const void*>(&src) == static_cast<const void*>(this)) {
        // Self-append keeps the source descriptors in this column; reserve first so the resize
        // below cannot move them out from under the bulk copy.
        _data.reserve(old_size + length);
    }
    _data.resize(old_size + length);
    // Copy the descriptors first: short values are already complete, long values are rewritten
    // below after their payload is copied into this column's arena. Avoids rebuilding every
    // descriptor through insert_data(), which dominated small-value range appends.
    std::copy_n(src_col._data.data() + start, length, _data.data() + old_size);
    for (size_t row = old_size; row < old_size + length; ++row) {
        auto& value = _data[row];
        if (!value.isInline()) {
            const char* dst = _arena.insert(value.data(), value.size());
            value = StringView(dst, value.size());
        }
    }
}

void ColumnVarbinary::insert_indices_from(const IColumn& src, const uint32_t* indices_begin,
                                          const uint32_t* indices_end) {
    const Self& src_vec = assert_cast<const Self&>(src);
    const size_t count = indices_end - indices_begin;
    const size_t old_size = _data.size();
    _data.resize(old_size + count);

    for (size_t row = 0; row < count; ++row) {
        auto& value = _data[old_size + row] = src_vec._data[indices_begin[row]];
        if (!value.isInline()) {
            const char* dst = _arena.insert(value.data(), value.size());
            value = StringView(dst, value.size());
        }
    }
}

ColumnPtr ColumnVarbinary::filter(const IColumn::Filter& filt, ssize_t result_size_hint) const {
    size_t size = this->size();
    column_match_filter_size(size, filt.size());

    if (_data.size() == 0) {
        return doris::ColumnVarbinary::create();
    }
    auto res = doris::ColumnVarbinary::create();
    Container& res_data = res->get_data();

    if (result_size_hint) {
        res_data.reserve(result_size_hint > 0 ? result_size_hint : size);
    }

    for (size_t row = 0; row < size; ++row) {
        if (filt[row]) {
            res_data.push_back(_data[row]);
        }
    }
    // Descriptors were copied as-is; only long values need their payload in the new arena.
    for (auto& value : res_data) {
        if (!value.isInline()) {
            const char* dst = res->_arena.insert(value.data(), value.size());
            value = StringView(dst, value.size());
        }
    }
    return res;
};

void ColumnVarbinary::reserve_bytes(size_t bytes) {
    // A batch hint must not turn the arena back into a multi-GiB contiguous allocation.
    const size_t reserve_size = std::min(bytes, size_t {128 * 1024 * 1024});
    if (reserve_size > _arena.remaining_space_in_current_chunk()) {
        static_cast<void>(_arena.alloc(reserve_size));
        _arena.rollback(reserve_size);
    }
}

bool ColumnVarbinary::owns_data(StringRef value) const {
    const auto begin = reinterpret_cast<uintptr_t>(_data.data());
    const auto address = reinterpret_cast<uintptr_t>(value.data);
    const size_t bytes = _data.size() * sizeof(value_type);
    return (address >= begin && address - begin < bytes &&
            value.size <= bytes - (address - begin)) ||
           _arena.contains(value.data, value.size);
}

namespace {

// Erasing rows drops descriptors only: an Arena cannot release the payload of individual values, so
// a consume-and-refill analytic sink would retain every erased long value and grow without bound.
// Once the garbage dominates the live payload, rebuild the arena from the surviving values instead.
// Values at or below the inline threshold live inside their descriptor and are never affected.
constexpr size_t COMPACTION_MIN_ARENA_BYTES = 1 << 20;

void compact_arena_if_needed(ColumnVarbinary::Container& data, Arena& arena) {
    if (arena.size() <= COMPACTION_MIN_ARENA_BYTES) {
        return;
    }
    size_t live_bytes = 0;
    for (const auto& value : data) {
        if (!value.isInline()) {
            live_bytes += value.size();
        }
    }
    // More than half of the arena is garbage. The extra copy below is paid back by the reclaim and
    // keeps the retained memory proportional to the live payload instead of the appended history.
    if (arena.size() <= live_bytes * 2) {
        return;
    }
    if (live_bytes == 0) {
        arena.clear(true);
        return;
    }
    // Stage the survivors outside the arena, release every chunk, then copy them into one
    // right-sized chunk. Views must be rewritten because the arena address changes.
    std::vector<char> staged(live_bytes);
    char* cursor = staged.data();
    for (auto& value : data) {
        if (value.isInline()) {
            continue;
        }
        std::memcpy(cursor, value.data(), value.size());
        value = StringView(cursor, value.size());
        cursor += value.size();
    }
    arena.clear(true);
    char* compacted = arena.alloc(live_bytes);
    std::memcpy(compacted, staged.data(), live_bytes);
    cursor = compacted;
    for (auto& value : data) {
        if (value.isInline()) {
            continue;
        }
        value = StringView(cursor, value.size());
        cursor += value.size();
    }
}

} // namespace

void ColumnVarbinary::erase(size_t start, size_t length) {
    DORIS_CHECK_LE(start, size());
    DORIS_CHECK_LE(length, size() - start);
    if (length == 0) {
        // Empty analytic partitions do not have a backing view array.
        return;
    }
    std::memmove(_data.data() + start, _data.data() + start + length,
                 (size() - start - length) * sizeof(value_type));
    _data.resize(size() - length);
    compact_arena_if_needed(_data, _arena);
}

size_t ColumnVarbinary::filter(const IColumn::Filter& filter) {
    column_match_filter_size(size(), filter.size());
    size_t pos = 0;
    for (size_t row = 0; row < filter.size(); ++row) {
        if (filter[row]) {
            // The arena remains owned by this column. Moving views preserves the payload; copying
            // long values into the same arena would retain the old bytes and grow on every filter.
            _data[pos++] = _data[row];
        }
    }
    resize(pos);
    // In-place filtering on a freshly built column starts from an arena that already holds about
    // twice the live payload, so garbage always looks dominant here. Reclaiming it would copy the
    // survivors on every filter and turn a descriptor-only selection into a payload copy; keep the
    // arena and let erase() own the reclamation decision instead.
    return pos;
}

MutableColumnPtr ColumnVarbinary::permute(const IColumn::Permutation& perm, size_t limit) const {
    size_t size = _data.size();

    limit = limit ? std::min(size, limit) : size;

    if (perm.size() < limit) {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "Size of permutation is less than required.");
        __builtin_unreachable();
    }

    auto res = doris::ColumnVarbinary::create(limit);
    typename Self::Container& res_data = res->get_data();
    for (size_t i = 0; i < limit; ++i) {
        auto val = _data[perm[i]];
        if (val.isInline()) {
            res_data[i] = val;
            continue;
        }
        const auto* dst = res->_arena.insert(val.data(), val.size());
        res_data[i] = doris::StringView(dst, val.size());
    }

    return res;
};

void ColumnVarbinary::replace_column_data(const IColumn& rhs, size_t row, size_t self_row) {
    DCHECK(this->size() > self_row);
    const auto& rhs_col = assert_cast<const Self&, TypeCheckOnRelease::DISABLE>(rhs);
    auto val = rhs_col.get_data()[row];
    if (val.isInline()) {
        _data[self_row] = val;
        return;
    }
    const auto* dst = _arena.insert(val.data(), val.size());
    _data[self_row] = doris::StringView(dst, val.size());
}

size_t ColumnVarbinary::get_max_row_byte_size() const {
    uint32_t max_size = 0;
    size_t num_rows = size();
    for (size_t i = 0; i < num_rows; ++i) {
        max_size = std::max(max_size, _data[i].size());
    }
    return max_size + sizeof(uint32_t);
}

void ColumnVarbinary::update_crcs_with_value(uint32_t* __restrict hashes, PrimitiveType /*type*/,
                                             uint32_t rows, uint32_t /*offset*/,
                                             const uint8_t* __restrict null_data) const {
    DCHECK(rows == size());
    if (null_data == nullptr) {
        for (size_t i = 0; i < rows; ++i) {
            hashes[i] = HashUtil::zlib_crc_hash(_data[i].data(), _data[i].size(), hashes[i]);
        }
    } else {
        for (size_t i = 0; i < rows; ++i) {
            if (null_data[i] == 0) {
                hashes[i] = HashUtil::zlib_crc_hash(_data[i].data(), _data[i].size(), hashes[i]);
            }
        }
    }
}

void ColumnVarbinary::update_crc_with_value(size_t start, size_t end, uint32_t& hash,
                                            const uint8_t* __restrict null_data) const {
    if (null_data == nullptr) {
        for (size_t i = start; i < end; ++i) {
            hash = HashUtil::zlib_crc_hash(_data[i].data(), _data[i].size(), hash);
        }
    } else {
        for (size_t i = start; i < end; ++i) {
            if (null_data[i] == 0) {
                hash = HashUtil::zlib_crc_hash(_data[i].data(), _data[i].size(), hash);
            }
        }
    }
}

void ColumnVarbinary::update_crc32c_batch(uint32_t* __restrict hashes,
                                          const uint8_t* __restrict null_map) const {
    const size_t rows = size();
    if (null_map == nullptr) {
        for (size_t i = 0; i < rows; ++i) {
            hashes[i] = crc32c_extend(hashes[i], reinterpret_cast<const uint8_t*>(_data[i].data()),
                                      _data[i].size());
        }
    } else {
        for (size_t i = 0; i < rows; ++i) {
            if (null_map[i] == 0) {
                hashes[i] =
                        crc32c_extend(hashes[i], reinterpret_cast<const uint8_t*>(_data[i].data()),
                                      _data[i].size());
            }
        }
    }
}

void ColumnVarbinary::update_crc32c_single(size_t start, size_t end, uint32_t& hash,
                                           const uint8_t* __restrict null_map) const {
    if (null_map == nullptr) {
        for (size_t i = start; i < end; ++i) {
            hash = crc32c_extend(hash, reinterpret_cast<const uint8_t*>(_data[i].data()),
                                 _data[i].size());
        }
    } else {
        for (size_t i = start; i < end; ++i) {
            if (null_map[i] == 0) {
                hash = crc32c_extend(hash, reinterpret_cast<const uint8_t*>(_data[i].data()),
                                     _data[i].size());
            }
        }
    }
}

void ColumnVarbinary::serialize(StringRef* keys, size_t num_rows) const {
    for (size_t i = 0; i < num_rows; ++i) {
        keys[i].size += serialize_impl(const_cast<char*>(keys[i].data + keys[i].size), i);
    }
}

void ColumnVarbinary::deserialize(StringRef* keys, const size_t num_rows) {
    for (size_t i = 0; i != num_rows; ++i) {
        auto sz = deserialize_impl(keys[i].data);
        keys[i].data += sz;
        keys[i].size -= sz;
    }
}

template <bool positive>
struct ColumnVarbinary::less {
    const ColumnVarbinary& parent;
    explicit less(const ColumnVarbinary& parent_) : parent(parent_) {}
    bool operator()(size_t lhs, size_t rhs) const {
        int res = parent._data[lhs].compare(parent._data[rhs]);
        return positive ? (res < 0) : (res > 0);
    }
};

void ColumnVarbinary::get_permutation(bool reverse, size_t limit, int /*nan_direction_hint*/,
                                      HybridSorter& sorter, IColumn::Permutation& res) const {
    size_t s = _data.size();
    res.resize(s);
    for (size_t i = 0; i < s; ++i) {
        res[i] = i;
    }

    if (reverse) {
        sorter.sort(res.begin(), res.end(), less<false>(*this));
    } else {
        sorter.sort(res.begin(), res.end(), less<true>(*this));
    }
}

void ColumnVarbinary::insert_many_strings(const StringRef* strings, size_t num) {
    for (size_t i = 0; i < num; i++) {
        insert_data(strings[i].data, strings[i].size);
    }
}

void ColumnVarbinary::insert_many_strings_overflow(const StringRef* strings, size_t num,
                                                   size_t max_length) {
    insert_many_strings(strings, num);
}

void ColumnVarbinary::sort_column(const ColumnSorter* sorter, EqualFlags& flags,
                                  IColumn::Permutation& perms, EqualRange& range,
                                  bool last_column) const {
    sorter->sort_column(*this, flags, perms, range, last_column);
}

} // namespace doris
