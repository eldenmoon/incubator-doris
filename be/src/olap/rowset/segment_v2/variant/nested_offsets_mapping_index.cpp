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

#include "olap/rowset/segment_v2/variant/nested_offsets_mapping_index.h"

#include <algorithm>
#include <limits>
#include <utility>

#include <roaring/roaring.hh>

#include "olap/rowset/segment_v2/encoding_info.h"
#include "olap/rowset/segment_v2/indexed_column_reader.h"
#include "olap/rowset/segment_v2/indexed_column_writer.h"
#include "olap/types.h"
#include "util/coding.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"

namespace doris::segment_v2 {

static inline int128_t encode_end_offset_key(uint64_t end_offset, uint32_t block_id) {
    uint128_t key = (static_cast<uint128_t>(end_offset) << 32) | static_cast<uint128_t>(block_id);
    return static_cast<int128_t>(key);
}

static inline uint64_t decode_end_offset_from_key(int128_t key) {
    uint128_t u = static_cast<uint128_t>(key);
    return static_cast<uint64_t>(u >> 32);
}

Status NestedOffsetsMappingIndexWriter::build(const uint64_t* offsets, size_t num_rows) {
    if (_block_size == 0) {
        return Status::InvalidArgument("block_size is 0");
    }
    _block_end_offset_keys.clear();
    _block_cumulative_offsets.clear();

    if (offsets == nullptr && num_rows != 0) {
        return Status::InvalidArgument("offsets is null");
    }
    const uint64_t rows = num_rows;
    if (rows == 0) {
        return Status::OK();
    }

    const uint64_t num_blocks = (rows + _block_size - 1) / _block_size;
    _block_end_offset_keys.reserve(num_blocks);
    _block_cumulative_offsets.reserve(num_blocks);

    for (uint64_t block_id = 0; block_id < num_blocks; ++block_id) {
        const uint64_t block_row_start = block_id * _block_size;
        const uint64_t block_row_end_exclusive = std::min<uint64_t>(rows, block_row_start + _block_size);
        if (block_row_start >= block_row_end_exclusive) {
            break;
        }

        const uint64_t block_row_end = block_row_end_exclusive - 1;
        const uint64_t block_end_offset = offsets[block_row_end];
        _block_end_offset_keys.emplace_back(
                encode_end_offset_key(block_end_offset, static_cast<uint32_t>(block_id)));

        const uint64_t block_start_offset = (block_row_start == 0) ? 0 : offsets[block_row_start - 1];
        uint64_t prev = block_start_offset;

        std::string payload;
        payload.resize(static_cast<size_t>((block_row_end_exclusive - block_row_start) * sizeof(uint32_t)));
        uint8_t* dst = reinterpret_cast<uint8_t*>(payload.data());

        for (uint64_t r = block_row_start; r < block_row_end_exclusive; ++r) {
            const uint64_t cur = offsets[r];
            if (UNLIKELY(cur < prev)) {
                return Status::Corruption("nested offsets not non-decreasing");
            }
            const uint64_t local = cur - block_start_offset;
            if (UNLIKELY(local > std::numeric_limits<uint32_t>::max())) {
                return Status::NotSupported("nested offsets block elements exceed uint32 max: {}",
                                            local);
            }
            encode_fixed32_le(dst, static_cast<uint32_t>(local));
            dst += sizeof(uint32_t);
            prev = cur;
        }
        _block_cumulative_offsets.emplace_back(std::move(payload));
    }

    return Status::OK();
}

Status NestedOffsetsMappingIndexWriter::write(io::FileWriter* file_writer, ColumnMetaPB* offsets_meta,
                                              CompressionTypePB compression_type) const {
    if (file_writer == nullptr) {
        return Status::InvalidArgument("file_writer is null");
    }
    if (offsets_meta == nullptr) {
        return Status::InvalidArgument("offsets_meta is null");
    }
    if (_block_size == 0) {
        return Status::InvalidArgument("block_size is 0");
    }
    if (_block_end_offset_keys.size() != _block_cumulative_offsets.size()) {
        return Status::InternalError("block_end_offsets and cumulative_offsets size mismatch");
    }
    if (_block_end_offset_keys.empty()) {
        return Status::OK();
    }

    auto* idx_meta = offsets_meta->add_indexes();
    idx_meta->set_type(NESTED_OFFSETS_INDEX);
    auto* nested_meta = idx_meta->mutable_nested_offsets_index();
    nested_meta->set_version(1);
    nested_meta->set_block_size(_block_size);

    {
        const auto* type_info = get_scalar_type_info<FieldType::OLAP_FIELD_TYPE_LARGEINT>();
        IndexedColumnWriterOptions options;
        options.write_ordinal_index = true;
        options.write_value_index = true;
        options.encoding = EncodingInfo::get_default_encoding(type_info->type(), {}, false);
        options.compression = compression_type;

        IndexedColumnWriter writer(options, type_info, file_writer);
        RETURN_IF_ERROR(writer.init());

        for (const auto& v : _block_end_offset_keys) {
            RETURN_IF_ERROR(writer.add(&v));
        }
        RETURN_IF_ERROR(writer.finish(nested_meta->mutable_block_end_offsets()));
    }

    {
        const auto* type_info = get_scalar_type_info<FieldType::OLAP_FIELD_TYPE_STRING>();
        IndexedColumnWriterOptions options;
        options.write_ordinal_index = true;
        options.write_value_index = false;
        options.encoding = PLAIN_ENCODING_V2;
        options.compression = compression_type;

        IndexedColumnWriter writer(options, type_info, file_writer);
        RETURN_IF_ERROR(writer.init());

        for (const auto& payload : _block_cumulative_offsets) {
            Slice s(payload);
            RETURN_IF_ERROR(writer.add(&s));
        }
        RETURN_IF_ERROR(writer.finish(nested_meta->mutable_block_cumulative_offsets()));
    }

    return Status::OK();
}

Status NestedOffsetsMappingIndexReader::_load(bool use_page_cache, OlapReaderStatistics* stats) const {
    if (!_pb.has_block_end_offsets() || !_pb.has_block_cumulative_offsets()) {
        return Status::Corruption("nested offsets index v2 missing required metas");
    }
    _block_end_offsets_reader =
            std::make_unique<IndexedColumnReader>(_file_reader, _pb.block_end_offsets());
    _block_cumulative_offsets_reader =
            std::make_unique<IndexedColumnReader>(_file_reader, _pb.block_cumulative_offsets());

    RETURN_IF_ERROR(_block_end_offsets_reader->load(use_page_cache, _kept_in_memory, stats));
    RETURN_IF_ERROR(_block_cumulative_offsets_reader->load(use_page_cache, _kept_in_memory, stats));

    if (_block_end_offsets_reader->num_values() != _block_cumulative_offsets_reader->num_values()) {
        return Status::Corruption(
                "nested offsets index v2 meta mismatch. end_offsets={} cumulative_offsets={}",
                _block_end_offsets_reader->num_values(), _block_cumulative_offsets_reader->num_values());
    }
    return Status::OK();
}

Status NestedOffsetsMappingIndexReader::get_total_elements(const ColumnIteratorOptions& opts,
                                                          uint64_t* total_elements) const {
    if (total_elements == nullptr) {
        return Status::InvalidArgument("total_elements is null");
    }
    *total_elements = 0;
    if (_pb.block_size() == 0) {
        return Status::Corruption("nested offsets index v2 block_size is 0");
    }
    RETURN_IF_ERROR(_load_once.call([this, &opts] { return _load(opts.use_page_cache, opts.stats); }));
    if (_block_end_offsets_reader == nullptr) {
        return Status::InternalError("nested offsets index v2 end_offsets reader not initialized");
    }
    const uint64_t blocks = _block_end_offsets_reader->num_values();
    if (blocks == 0) {
        return Status::OK();
    }

    IndexedColumnIterator end_iter(_block_end_offsets_reader.get(), opts.stats);
    RETURN_IF_ERROR(end_iter.seek_to_ordinal(static_cast<ordinal_t>(blocks - 1)));
    size_t one = 1;
    vectorized::MutableColumnPtr col = vectorized::ColumnInt128::create();
    RETURN_IF_ERROR(end_iter.next_batch(&one, col));
    if (one != 1) {
        return Status::Corruption("failed to read block_end_offsets at last ordinal");
    }
    auto* data = assert_cast<vectorized::ColumnInt128*>(col.get());
    *total_elements = decode_end_offset_from_key(data->get_data()[0]);
    return Status::OK();
}

Status NestedOffsetsMappingIndexReader::map_elements_to_parent_ords(const ColumnIteratorOptions& opts,
                                                                   const roaring::Roaring& element_bitmap,
                                                                   roaring::Roaring* parent_bitmap) const {
    if (parent_bitmap == nullptr) {
        return Status::InvalidArgument("parent_bitmap is null");
    }
    *parent_bitmap = roaring::Roaring();
    if (element_bitmap.isEmpty()) {
        return Status::OK();
    }

    if (_pb.block_size() == 0) {
        return Status::Corruption("nested offsets index v2 block_size is 0");
    }
    const uint32_t block_size = _pb.block_size();
    RETURN_IF_ERROR(_load_once.call([this, &opts] { return _load(opts.use_page_cache, opts.stats); }));

    if (_block_end_offsets_reader == nullptr || _block_cumulative_offsets_reader == nullptr) {
        return Status::InternalError("nested offsets index v2 readers not initialized");
    }
    if (_block_end_offsets_reader->num_values() == 0) {
        return Status::OK();
    }

    IndexedColumnIterator end_seek_iter(_block_end_offsets_reader.get(), opts.stats);
    IndexedColumnIterator end_ord_iter(_block_end_offsets_reader.get(), opts.stats);
    IndexedColumnIterator payload_iter(_block_cumulative_offsets_reader.get(), opts.stats);

    uint64_t current_block_idx = std::numeric_limits<uint64_t>::max();
    uint64_t current_block_start_offset = 0;
    uint64_t current_block_end_offset = 0;
    uint32_t current_expected_rows = 0;
    Slice current_payload;
    uint32_t current_row_in_block = 0;
    uint32_t current_row_end_offset = 0;

    uint64_t last_added_parent = std::numeric_limits<uint64_t>::max();

    uint64_t prev_block_idx = std::numeric_limits<uint64_t>::max();
    uint64_t prev_block_end_offset = 0;

    vectorized::MutableColumnPtr end_col = vectorized::ColumnInt128::create();
    vectorized::MutableColumnPtr payload_col = vectorized::ColumnString::create();

    auto get_cumulative_at = [&](uint32_t idx) -> uint32_t {
        return decode_fixed32_le(reinterpret_cast<const uint8_t*>(current_payload.data) +
                                 static_cast<size_t>(idx) * sizeof(uint32_t));
    };

    auto upper_bound_payload = [&](uint32_t from, uint32_t to, uint32_t value) -> uint32_t {
        uint32_t lo = from;
        uint32_t hi = to;
        while (lo < hi) {
            uint32_t mid = lo + ((hi - lo) >> 1);
            if (get_cumulative_at(mid) > value) {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }
        return lo;
    };

    auto read_block_end_offset = [&](uint64_t block_idx, uint64_t* out) -> Status {
        end_col->clear();
        RETURN_IF_ERROR(end_ord_iter.seek_to_ordinal(static_cast<ordinal_t>(block_idx)));
        size_t one = 1;
        RETURN_IF_ERROR(end_ord_iter.next_batch(&one, end_col));
        if (one != 1) {
            return Status::Corruption("failed to read block_end_offsets at ordinal {}", block_idx);
        }
        auto* data = assert_cast<vectorized::ColumnInt128*>(end_col.get());
        *out = decode_end_offset_from_key(data->get_data()[0]);
        return Status::OK();
    };

    auto load_block_cumulative = [&](uint64_t block_idx) -> Status {
        const uint64_t block_row_start = block_idx * block_size;
        const uint64_t remaining_rows =
                (block_row_start >= _offsets_num_rows) ? 0 : (_offsets_num_rows - block_row_start);
        current_expected_rows = static_cast<uint32_t>(std::min<uint64_t>(block_size, remaining_rows));

        payload_col->clear();
        RETURN_IF_ERROR(payload_iter.seek_to_ordinal(static_cast<ordinal_t>(block_idx)));
        size_t one = 1;
        RETURN_IF_ERROR(payload_iter.next_batch(&one, payload_col));
        if (one != 1) {
            return Status::Corruption("failed to read block_cumulative_offsets at ordinal {}", block_idx);
        }
        const auto& s = payload_col->get_data_at(0);
        if (s.size != static_cast<size_t>(current_expected_rows) * sizeof(uint32_t)) {
            return Status::Corruption("nested offsets index v2 payload size mismatch at ordinal {}",
                                      block_idx);
        }
        current_payload = Slice(s.data, s.size);
        current_row_in_block = 0;
        current_row_end_offset = (current_expected_rows > 0) ? get_cumulative_at(0) : 0;
        return Status::OK();
    };

    for (uint32_t elem : element_bitmap) {
        const uint64_t target = static_cast<uint64_t>(elem) + 1;

        if (current_block_idx == std::numeric_limits<uint64_t>::max() || target > current_block_end_offset) {
            bool exact = false;
            const int128_t search_key = encode_end_offset_key(target, 0);
            Status st = end_seek_iter.seek_at_or_after(&search_key, &exact);
            if (st.is<ErrorCode::ENTRY_NOT_FOUND>()) {
                return Status::OK();
            }
            RETURN_IF_ERROR(st);

            current_block_idx = static_cast<uint64_t>(end_seek_iter.get_current_ordinal());
            RETURN_IF_ERROR(read_block_end_offset(current_block_idx, &current_block_end_offset));
            if (current_block_idx == 0) {
                current_block_start_offset = 0;
            } else if (prev_block_idx != std::numeric_limits<uint64_t>::max() &&
                       current_block_idx == prev_block_idx + 1) {
                current_block_start_offset = prev_block_end_offset;
            } else {
                RETURN_IF_ERROR(read_block_end_offset(current_block_idx - 1, &current_block_start_offset));
            }
            prev_block_idx = current_block_idx;
            prev_block_end_offset = current_block_end_offset;
            RETURN_IF_ERROR(load_block_cumulative(current_block_idx));
        }

        if (UNLIKELY(current_expected_rows == 0 || current_payload.data == nullptr)) {
            return Status::Corruption("nested offsets index v2 missing cumulative for block {}",
                                      current_block_idx);
        }
        const uint64_t local_elem = static_cast<uint64_t>(elem) - current_block_start_offset;
        if (UNLIKELY(local_elem > std::numeric_limits<uint32_t>::max())) {
            return Status::Corruption("nested offsets index v2 local elem overflow: {}", local_elem);
        }
        const uint32_t local = static_cast<uint32_t>(local_elem);

        if (current_row_end_offset <= local) {
            constexpr uint32_t kLinearSteps = 16;
            uint32_t steps = 0;
            while (current_row_in_block < current_expected_rows &&
                   current_row_end_offset <= local && steps < kLinearSteps) {
                ++current_row_in_block;
                ++steps;
                if (current_row_in_block >= current_expected_rows) {
                    break;
                }
                current_row_end_offset = get_cumulative_at(current_row_in_block);
            }
            if (current_row_in_block < current_expected_rows && current_row_end_offset <= local) {
                current_row_in_block =
                        upper_bound_payload(current_row_in_block, current_expected_rows, local);
                if (current_row_in_block >= current_expected_rows) {
                    return Status::Corruption("failed to locate row for element {} in block {}", elem,
                                              current_block_idx);
                }
                current_row_end_offset = get_cumulative_at(current_row_in_block);
            }
        }

        if (UNLIKELY(current_row_in_block >= current_expected_rows)) {
            return Status::Corruption("failed to locate row for element {} in block {}", elem,
                                      current_block_idx);
        }

        const uint64_t parent = current_block_idx * block_size + current_row_in_block;
        if (parent != last_added_parent) {
            parent_bitmap->add(static_cast<uint32_t>(parent));
            last_added_parent = parent;
        }
    }

    return Status::OK();
}

} // namespace doris::segment_v2
