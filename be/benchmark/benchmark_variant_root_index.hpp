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

#include <benchmark/benchmark.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <limits>
#include <map>
#include <memory>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#include "common/cast_set.h"
#include "common/exception.h"
#include "common/status.h"
#include "core/block/block.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_variant.h"
#include "core/data_type/data_type_variant_v2.h"
#include "core/data_type/primitive_type.h"
#include "core/string_ref.h"
#include "exec/common/variant_util.h"
#include "exprs/function/parse/variant_string_parse.h"
#include "exprs/vcast_expr.h"
#include "exprs/vectorized_fn_call.h"
#include "exprs/vexpr_context.h"
#include "exprs/vliteral.h"
#include "exprs/vmatch_predicate.h"
#include "exprs/vslot_ref.h"
#include "io/fs/local_file_system.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/memory/cache_manager.h"
#include "runtime/runtime_state.h"
#include "storage/cache/page_cache.h"
#include "storage/compaction/full_compaction.h"
#include "storage/data_dir.h"
#include "storage/index/index_writer.h"
#include "storage/index/inverted/analyzer/analyzer.h"
#include "storage/index/inverted/inverted_index_cache.h"
#include "storage/index/inverted/inverted_index_desc.h"
#include "storage/index/inverted/variant_root_index.h"
#include "storage/index/snii/bkd/bkd_reader.h"
#include "storage/index/snii/reader/logical_index_reader.h"
#include "storage/index/snii/reader/snii_segment_reader.h"
#include "storage/index/snii/snii_doris_adapter.h"
#include "storage/options.h"
#include "storage/predicate/predicate_creator.h"
#include "storage/rowset/beta_rowset.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_reader.h"
#include "storage/rowset/rowset_reader_context.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/schema.h"
#include "storage/segment/segment.h"
#include "storage/segment/segment_loader.h"
#include "storage/storage_engine.h"
#include "storage/tablet/tablet.h"
#include "storage/tablet/tablet_column_object_pool.h"
#include "storage/tablet/tablet_meta.h"
#include "storage/tablet/tablet_schema.h"
#include "storage/tablet/tablet_schema_cache.h"

namespace doris::variant_root_benchmark {
namespace {

// Raw JSON generation is setup. Timed import starts at JSON parsing and goes through
// RowsetWriter -> SegmentWriter. Lower the row count only for smoke tests.
constexpr uint32_t DEFAULT_ROWS = 1'000'000;
constexpr uint32_t BATCH_ROWS = 4'096;
constexpr uint32_t COMPACTION_INPUT_ROWSETS = 4;
constexpr uint32_t TOTAL_FIELDS = 100;
constexpr uint32_t INTEGER_FIELDS = 50;
constexpr uint32_t TEXT_FIELDS = 50;
constexpr uint32_t PRESENT_INTEGER_FIELDS = 25;
constexpr uint32_t PRESENT_TEXT_FIELDS = 25;
constexpr uint32_t TOTAL_PATHS = TOTAL_FIELDS + 1;
constexpr int32_t KEY_UID = 0;
constexpr int32_t ROOT_UID = 1;
constexpr int64_t INDEX_ID = 9'001;
constexpr uint64_t FNV_OFFSET = 1'469'598'103'934'665'603ULL;
constexpr uint64_t FNV_PRIME = 1'099'511'628'211ULL;
constexpr uint64_t SYNTHETIC_SEED = 0x4f1bbcdc5a77e2d9ULL;
constexpr std::string_view ROOT_NAME = "v";
constexpr std::string_view STRING_QUERY_RELATIVE_PATH = "payload.t00";
constexpr std::string_view INTEGER_QUERY_RELATIVE_PATH = "actor.i00";
constexpr std::string_view ARRAY_QUERY_RELATIVE_PATH = "tags";
constexpr std::string_view EXACT_QUERY_VALUE = "PushEvent repository-0 ref-heads-main-0";
constexpr std::string_view ENGLISH_QUERY_VALUE = "benchmark";
constexpr std::string_view ENGLISH_MATCH_ALL_QUERY_VALUE = "automated performance";
constexpr std::string_view LIKE_QUERY_VALUE = "%benchmark%";
constexpr std::string_view STARTS_WITH_QUERY_VALUE = "The benchmark";
constexpr std::string_view ARRAY_QUERY_VALUE = "arrayhit";

enum class IndexLayout : uint8_t { ROOT, ALL_VALUES, CHILDREN, NO_INDEX };
enum class TextMode : uint8_t { EXACT, ENGLISH };
enum class QueryShape : uint8_t {
    PATH_STRING_EQ,
    PATH_STRING_IN,
    PATH_NUMERIC_EQ,
    PATH_NUMERIC_IN,
    PATH_ENGLISH_MATCH_ANY,
    PATH_ENGLISH_MATCH_ALL,
    PATH_LIKE_SUBSTRING,
    PATH_STARTS_WITH,
    PATH_ARRAY_CONTAINS,
    WHOLE_ROOT_ENGLISH_MATCH_ANY,
    WHOLE_ROOT_LIKE_SUBSTRING
};

std::string_view layout_name(IndexLayout layout) {
    switch (layout) {
    case IndexLayout::ROOT:
        return "Root";
    case IndexLayout::ALL_VALUES:
        return "AllValues";
    case IndexLayout::CHILDREN:
        return "Children";
    case IndexLayout::NO_INDEX:
        return "NoIndex";
    }
    __builtin_unreachable();
}

bool is_single_root_layout(IndexLayout layout) {
    return layout == IndexLayout::ROOT || layout == IndexLayout::ALL_VALUES;
}

bool has_inverted_index(IndexLayout layout) {
    return layout != IndexLayout::NO_INDEX;
}

std::string_view text_mode_name(TextMode mode) {
    return mode == TextMode::EXACT ? "Exact" : "English";
}

std::string_view query_shape_name(QueryShape shape) {
    switch (shape) {
    case QueryShape::PATH_STRING_EQ:
        return "PathStringEq";
    case QueryShape::PATH_STRING_IN:
        return "PathStringIn";
    case QueryShape::PATH_NUMERIC_EQ:
        return "PathNumericEq";
    case QueryShape::PATH_NUMERIC_IN:
        return "PathNumericIn";
    case QueryShape::PATH_ENGLISH_MATCH_ANY:
        return "PathEnglishMatchAnyCrossPathStress";
    case QueryShape::PATH_ENGLISH_MATCH_ALL:
        return "PathEnglishMatchAllCrossPathStress";
    case QueryShape::PATH_LIKE_SUBSTRING:
        return "PathLikeSubstringScan";
    case QueryShape::PATH_STARTS_WITH:
        return "PathStartsWithScan";
    case QueryShape::PATH_ARRAY_CONTAINS:
        return "PathArrayContains";
    case QueryShape::WHOLE_ROOT_ENGLISH_MATCH_ANY:
        return "WholeRootEnglishMatchAny";
    case QueryShape::WHOLE_ROOT_LIKE_SUBSTRING:
        return "WholeRootLikeSubstringScan";
    }
    __builtin_unreachable();
}

bool is_whole_root_query(QueryShape shape) {
    return shape == QueryShape::WHOLE_ROOT_ENGLISH_MATCH_ANY ||
           shape == QueryShape::WHOLE_ROOT_LIKE_SUBSTRING;
}

bool is_english_query(QueryShape shape) {
    return shape == QueryShape::PATH_ENGLISH_MATCH_ANY ||
           shape == QueryShape::PATH_ENGLISH_MATCH_ALL ||
           shape == QueryShape::PATH_LIKE_SUBSTRING || shape == QueryShape::PATH_STARTS_WITH ||
           shape == QueryShape::WHOLE_ROOT_ENGLISH_MATCH_ANY ||
           shape == QueryShape::WHOLE_ROOT_LIKE_SUBSTRING;
}

bool expects_inverted_index(IndexLayout layout, QueryShape shape) {
    if (!has_inverted_index(layout)) {
        return false;
    }
    if (shape == QueryShape::WHOLE_ROOT_ENGLISH_MATCH_ANY) {
        return layout == IndexLayout::ALL_VALUES;
    }
    if (shape == QueryShape::PATH_ARRAY_CONTAINS) {
        return layout == IndexLayout::CHILDREN;
    }
    return shape != QueryShape::PATH_LIKE_SUBSTRING && shape != QueryShape::PATH_STARTS_WITH &&
           shape != QueryShape::WHOLE_ROOT_LIKE_SUBSTRING;
}

bool expects_warm_searcher_cache(IndexLayout layout, QueryShape shape) {
    return expects_inverted_index(layout, shape);
}

size_t analyzed_english_term_count(std::string_view query) {
    const std::map<std::string, std::string> properties {
            {INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_ENGLISH},
            {INVERTED_INDEX_PARSER_LOWERCASE_KEY, INVERTED_INDEX_PARSER_TRUE},
            {INVERTED_INDEX_PARSER_STOPWORDS_KEY, ""}};
    return segment_v2::inverted_index::InvertedIndexAnalyzer::get_analyse_result(std::string(query),
                                                                                 properties)
            .size();
}

int64_t configured_query_iterations() {
    constexpr int64_t DEFAULT_QUERY_ITERATIONS = 5;
    const char* value = std::getenv("DORIS_VARIANT_ROOT_BENCHMARK_QUERY_ITERATIONS");
    if (value == nullptr) {
        return DEFAULT_QUERY_ITERATIONS;
    }
    int64_t iterations = 0;
    const std::string_view text(value);
    const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), iterations);
    if (error != std::errc {} || end != text.data() + text.size() || iterations <= 0) {
        return DEFAULT_QUERY_ITERATIONS;
    }
    return iterations;
}

uint32_t configured_rows() {
    const char* value = std::getenv("DORIS_VARIANT_ROOT_BENCHMARK_ROWS");
    if (value == nullptr) {
        return DEFAULT_ROWS;
    }
    uint32_t rows = 0;
    const std::string_view text(value);
    const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), rows);
    if (error != std::errc {} || end != text.data() + text.size() || rows == 0) {
        return 0;
    }
    return rows;
}

uint64_t next_random(uint64_t* state) {
    *state += 0x9e3779b97f4a7c15ULL;
    uint64_t value = *state;
    value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9ULL;
    value = (value ^ (value >> 27)) * 0x94d049bb133111ebULL;
    return value ^ (value >> 31);
}

uint64_t select_fields(uint32_t row, uint64_t salt, uint32_t selected_count) {
    std::array<uint8_t, 50> fields;
    for (uint8_t field = 0; field < fields.size(); ++field) {
        fields[field] = field;
    }
    uint64_t state = SYNTHETIC_SEED ^ (static_cast<uint64_t>(row) << 1) ^ salt;
    uint64_t selected = 0;
    for (uint32_t index = 0; index < selected_count; ++index) {
        const uint32_t picked =
                index + cast_set<uint32_t>(next_random(&state) % (fields.size() - index));
        std::swap(fields[index], fields[picked]);
        selected |= uint64_t {1} << fields[index];
    }
    return selected;
}

void append_field_name(std::string* json, char prefix, uint32_t field) {
    json->push_back('"');
    json->push_back(prefix);
    json->push_back(static_cast<char>('0' + field / 10));
    json->push_back(static_cast<char>('0' + field % 10));
    json->append("\":");
}

void append_integer_group(std::string* json, std::string_view group, uint64_t selected,
                          uint32_t first_field, uint32_t end_field, uint32_t row) {
    json->push_back('"');
    json->append(group);
    json->append("\":{");
    bool first = true;
    for (uint32_t field = first_field; field < end_field; ++field) {
        if ((selected & (uint64_t {1} << field)) == 0) {
            continue;
        }
        if (!first) {
            json->push_back(',');
        }
        first = false;
        append_field_name(json, 'i', field);
        json->append(std::to_string(static_cast<uint64_t>(row % 10'000) * INTEGER_FIELDS + field));
    }
    json->push_back('}');
}

constexpr std::array<std::string_view, 24> ENGLISH_TEXT = {
        "The contributor opened a pull request to improve variant storage performance.",
        "A reviewer requested tests for integer paths strings and sparse nested objects.",
        "The continuous integration job compiled the backend and executed regression tests.",
        "This issue describes an unexpected result after compaction merged several rowsets.",
        "The repository maintainer approved the change after checking compatibility and memory.",
        "A commit updated the parser configuration and regenerated the inverted index metadata.",
        "The release note explains how users can query GitHub events with exact predicates.",
        "An automated bot labeled the pull request and assigned it to the storage team.",
        "The actor pushed multiple commits to the feature branch before requesting another review.",
        "A discussion compared root indexing with per path indexes for large JSON documents.",
        "The benchmark loads one million sparse rows and measures import time and disk footprint.",
        "Full compaction rewrites data segments while preserving every logical index and document.",
        "The query searched issue comments for matching words across several English paragraphs.",
        "A developer reproduced the failure with deterministic data and attached a detailed "
        "profile.",
        "The merged patch reduced index storage without changing row counts or extracted values.",
        "The project archived an event containing repository actor payload and organization "
        "fields.",
        "A nightly workflow uploaded benchmark artifacts and recorded CPU affinity for every "
        "repetition.",
        "The incident report connected a failed deployment with a schema change from the previous "
        "release.",
        "Several maintainers discussed backward compatibility before merging the storage format "
        "update.",
        "The pull request description listed measured import latency compaction time and total "
        "bytes.",
        "A test fixture generated deterministic event documents so repeated runs used identical "
        "values.",
        "The organization scheduled a release candidate after all required checks completed "
        "successfully.",
        "An issue comment included reproduction steps logs expected results and a proposed narrow "
        "fix.",
        "The repository received watch fork release and member events during the same archive "
        "hour."};

void append_text_group(std::string* json, std::string_view group, uint64_t selected,
                       uint32_t first_field, uint32_t end_field, uint32_t row, TextMode mode) {
    json->push_back('"');
    json->append(group);
    json->append("\":{");
    bool first = true;
    for (uint32_t field = first_field; field < end_field; ++field) {
        if ((selected & (uint64_t {1} << field)) == 0) {
            continue;
        }
        if (!first) {
            json->push_back(',');
        }
        first = false;
        append_field_name(json, 't', field);
        json->push_back('"');
        if (mode == TextMode::EXACT) {
            json->append("PushEvent repository-");
            json->append(std::to_string(field));
            json->append(" ref-heads-main-");
            json->append(std::to_string(row % 256));
        } else {
            const size_t first_sentence = (row + field * 7) % ENGLISH_TEXT.size();
            json->append(ENGLISH_TEXT[first_sentence]);
            if ((row + field) % 3 == 0) {
                json->push_back(' ');
                json->append(ENGLISH_TEXT[(first_sentence + 7) % ENGLISH_TEXT.size()]);
            }
            if ((row + field) % 11 == 0) {
                json->append("\\n\\n");
                json->append(ENGLISH_TEXT[(first_sentence + 13) % ENGLISH_TEXT.size()]);
            }
        }
        json->push_back('"');
    }
    json->push_back('}');
}

std::string make_synthetic_json(uint32_t row, TextMode mode) {
    const uint64_t integers = select_fields(row, 0x13f9375b21ULL, PRESENT_INTEGER_FIELDS);
    const uint64_t text = select_fields(row, 0xa87d6e4c19ULL, PRESENT_TEXT_FIELDS);
    std::string json;
    json.reserve(mode == TextMode::EXACT ? 1'600 : 2'600);
    json.push_back('{');
    append_integer_group(&json, "actor", integers, 0, 25, row);
    json.push_back(',');
    append_integer_group(&json, "repo", integers, 25, INTEGER_FIELDS, row);
    json.push_back(',');
    append_text_group(&json, "payload", text, 0, 25, row, mode);
    json.push_back(',');
    append_text_group(&json, "meta", text, 25, TEXT_FIELDS, row, mode);
    json.append(",\"tags\":[\"");
    json.append(row % 64 == 0 ? ARRAY_QUERY_VALUE : std::string_view("arraymiss"));
    json.append("\",\"storage\"]");
    json.push_back('}');
    return json;
}

struct SyntheticData {
    Status status = Status::OK();
    uint32_t rows = configured_rows();

    SyntheticData() {
        if (rows == 0) {
            status = Status::InvalidArgument(
                    "DORIS_VARIANT_ROOT_BENCHMARK_ROWS must be a positive uint32");
        } else if (rows % COMPACTION_INPUT_ROWSETS != 0) {
            status = Status::InvalidArgument("synthetic row count {} is not divisible by {}", rows,
                                             COMPACTION_INPUT_ROWSETS);
        }
    }
};

SyntheticData& synthetic_data() {
    static SyntheticData data;
    return data;
}

TabletSchemaSPtr make_schema(IndexLayout layout, TextMode mode) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    schema_pb.set_num_short_key_columns(1);
    schema_pb.set_num_rows_per_row_block(1'024);
    schema_pb.set_compress_kind(COMPRESS_NONE);
    schema_pb.set_compression_type(LZ4F);
    schema_pb.set_inverted_index_storage_format(InvertedIndexStorageFormatPB::SNII);
    schema_pb.set_next_column_unique_id(ROOT_UID + 1);

    auto* key = schema_pb.add_column();
    key->set_unique_id(KEY_UID);
    key->set_name("k");
    key->set_type("BIGINT");
    key->set_is_key(true);
    key->set_is_nullable(false);

    auto* variant = schema_pb.add_column();
    variant->set_unique_id(ROOT_UID);
    variant->set_name(std::string(ROOT_NAME));
    variant->set_type("VARIANT");
    variant->set_is_key(false);
    variant->set_is_nullable(false);
    variant->set_variant_max_subcolumns_count(10'000);
    variant->set_variant_max_sparse_column_statistics_size(10'000);
    variant->set_variant_sparse_hash_shard_count(16);
    variant->set_variant_enable_doc_mode(false);
    variant->set_variant_doc_materialization_min_rows(std::numeric_limits<int64_t>::max());
    variant->set_variant_doc_hash_shard_count(16);

    if (has_inverted_index(layout)) {
        auto* index = schema_pb.add_index();
        index->set_index_id(INDEX_ID);
        index->set_index_name("idx_v_" + std::string(layout_name(layout)) + "_" +
                              std::string(text_mode_name(mode)));
        index->set_index_type(IndexType::INVERTED);
        index->add_col_unique_id(ROOT_UID);
        (*index->mutable_properties())["parser"] = mode == TextMode::EXACT ? "none" : "english";
        (*index->mutable_properties())["support_phrase"] = "false";
        if (is_single_root_layout(layout)) {
            (*index->mutable_properties())[std::string(
                    segment_v2::variant_root_index::VARIANT_INDEX_MODE_KEY)] =
                    layout == IndexLayout::ROOT
                            ? segment_v2::variant_root_index::VARIANT_INDEX_MODE_ROOT
                            : segment_v2::variant_root_index::VARIANT_INDEX_MODE_ALL_VALUES;
            (*index->mutable_properties())[std::string(
                    segment_v2::variant_root_index::VARIANT_ROOT_FORMAT_VERSION_KEY)] =
                    segment_v2::variant_root_index::VARIANT_ROOT_FORMAT_VERSION_V1;
        }
    }

    auto schema = std::make_shared<TabletSchema>();
    schema->init_from_pb(schema_pb);
    schema->set_storage_format(TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V3);
    return schema;
}

void ensure_runtime_services() {
    ExecEnv* env = ExecEnv::GetInstance();
    if (env->get_cache_manager() == nullptr) {
        env->set_cache_manager(CacheManager::create_global_instance());
    }
    if (env->get_storage_page_cache() == nullptr) {
        constexpr size_t CACHE_CAPACITY = 512UL << 20;
        env->set_storage_page_cache(StoragePageCache::create_global_cache(CACHE_CAPACITY, 10, 0));
    }
    if (env->segment_loader() == nullptr) {
        static const std::unique_ptr<SegmentLoader> loader =
                std::make_unique<SegmentLoader>(512UL << 20, 4'096);
        env->set_segment_loader(loader.get());
    }
    if (env->get_tablet_schema_cache() == nullptr) {
        env->set_tablet_schema_cache(TabletSchemaCache::create_global_schema_cache(
                config::tablet_schema_cache_capacity));
    }
    if (env->get_tablet_column_object_pool() == nullptr) {
        env->set_tablet_column_object_pool(TabletColumnObjectPool::create_global_column_cache(
                config::tablet_schema_cache_capacity));
    }
    if (env->get_inverted_index_searcher_cache() == nullptr) {
        constexpr size_t SEARCHER_CACHE_CAPACITY = 1UL << 30;
        static const std::unique_ptr<segment_v2::InvertedIndexSearcherCache> searcher_cache(
                segment_v2::InvertedIndexSearcherCache::create_global_instance(
                        SEARCHER_CACHE_CAPACITY, 16));
        env->set_inverted_index_searcher_cache(searcher_cache.get());
    }
    if (env->get_inverted_index_query_cache() == nullptr) {
        constexpr size_t QUERY_CACHE_CAPACITY = 128UL << 20;
        static const std::unique_ptr<segment_v2::InvertedIndexQueryCache> query_cache(
                segment_v2::InvertedIndexQueryCache::create_global_cache(QUERY_CACHE_CAPACITY, 16));
        env->set_inverted_index_query_cache(query_cache.get());
    }
}

uint64_t next_fixture_id() {
    static uint64_t next_id = 0;
    return ++next_id;
}

uint64_t update_hash(uint64_t hash, std::string_view value) {
    for (const unsigned char byte : value) {
        hash ^= byte;
        hash *= FNV_PRIME;
    }
    hash ^= 0xff;
    hash *= FNV_PRIME;
    return hash;
}

std::string variant_index_suffix(std::string_view relative_path) {
    TabletIndex index;
    index.set_escaped_escaped_index_suffix_path(std::string(ROOT_NAME) + "." +
                                                std::string(relative_path));
    return index.get_index_suffix();
}

std::string synthetic_field_path(bool integer, uint32_t field) {
    const std::string_view group =
            integer ? (field < 25 ? "actor" : "repo") : (field < 25 ? "payload" : "meta");
    std::string path(group);
    path.append(integer ? ".i" : ".t");
    path.push_back(static_cast<char>('0' + field / 10));
    path.push_back(static_cast<char>('0' + field % 10));
    return path;
}

struct DataFingerprint {
    uint64_t rows = 0;
    uint64_t int_values = 0;
    uint64_t string_values = 0;
    uint64_t array_rows = 0;
    uint64_t array_hash_sum = 0;
    uint64_t array_hash_xor = 0;
    uint64_t hash_sum = 0;
    uint64_t hash_xor = 0;

    bool operator==(const DataFingerprint&) const = default;
};

struct IndexFingerprint {
    uint64_t document_rows = 0;
    uint64_t indexed_documents = 0;
    uint64_t null_documents = 0;
    uint64_t logical_stats_hash_sum = 0;
    uint64_t logical_stats_hash_mix = 0;
    uint64_t term_document_frequency = 0;
    uint64_t term_df_hash_sum = 0;
    uint64_t term_df_hash_mix = 0;
    uint64_t integer_documents = 0;
    uint64_t integer_points = 0;
    uint64_t integer_doc_hash_sum = 0;
    uint64_t integer_point_hash_sum = 0;

    bool operator==(const IndexFingerprint&) const = default;
};

struct PhysicalIndexStats {
    uint32_t segments = 0;
    uint64_t logical_indexes = 0;
    uint64_t physical_data_bytes = 0;
    uint64_t physical_index_bytes = 0;
    uint64_t physical_total_bytes = 0;
    IndexFingerprint fingerprint;
};

VExprContextSPtr make_match_context(const ReadSchema& read_schema, std::string query,
                                    bool match_all) {
    constexpr int32_t MATCH_COLUMN_ORDINAL = 1;
    const DataTypePtr slot_type = read_schema.data_type(MATCH_COLUMN_ORDINAL);
    const DataTypePtr result_type = slot_type->is_nullable()
                                            ? make_nullable(std::make_shared<DataTypeUInt8>())
                                            : std::make_shared<DataTypeUInt8>();

    TFunctionName function_name;
    function_name.__set_db_name("");
    function_name.__set_function_name(match_all ? "match_all" : "match_any");

    TFunction function;
    function.__set_name(function_name);
    function.__set_binary_type(TFunctionBinaryType::BUILTIN);
    function.__set_arg_types({create_type_desc(remove_nullable(slot_type)->get_primitive_type()),
                              create_type_desc(PrimitiveType::TYPE_STRING)});
    function.__set_ret_type(result_type->to_thrift());
    function.__set_has_var_args(false);

    TMatchPredicate match_predicate;
    match_predicate.__set_analyzer_name("english");
    match_predicate.__set_parser_type("english");
    match_predicate.__set_parser_mode("");
    match_predicate.__set_parser_lowercase(true);
    match_predicate.__set_parser_stopwords("");

    TExprNode node;
    node.__set_node_type(TExprNodeType::MATCH_PRED);
    node.__set_opcode(match_all ? TExprOpcode::MATCH_ALL : TExprOpcode::MATCH_ANY);
    node.__set_type(result_type->to_thrift());
    node.__set_num_children(2);
    node.__set_is_nullable(result_type->is_nullable());
    node.__set_fn(function);
    node.__set_match_predicate(match_predicate);

    auto predicate = VMatchPredicate::create_shared(node);
    predicate->add_child(VSlotRef::create_shared(
            -1, MATCH_COLUMN_ORDINAL, read_schema.column(MATCH_COLUMN_ORDINAL)->unique_id(),
            slot_type, read_schema.column(MATCH_COLUMN_ORDINAL)->name()));
    predicate->add_child(
            VLiteral::create_shared(std::make_shared<DataTypeString>(),
                                    Field::create_field<TYPE_STRING>(std::move(query))));
    return VExprContext::create_shared(std::move(predicate));
}

VExprContextSPtr make_string_function_context(const ReadSchema& read_schema,
                                              std::string function_name, std::string argument) {
    constexpr int32_t QUERY_COLUMN_ORDINAL = 1;
    const DataTypePtr slot_type = read_schema.data_type(QUERY_COLUMN_ORDINAL);
    const DataTypePtr string_type = std::make_shared<DataTypeString>();
    const DataTypePtr result_type = make_nullable(std::make_shared<DataTypeUInt8>());

    TFunctionName name;
    name.__set_db_name("");
    name.__set_function_name(std::move(function_name));
    TFunction function;
    function.__set_name(name);
    function.__set_binary_type(TFunctionBinaryType::BUILTIN);
    function.__set_arg_types({string_type->to_thrift(), string_type->to_thrift()});
    function.__set_ret_type(result_type->to_thrift());
    function.__set_has_var_args(false);

    TExprNode node;
    node.__set_node_type(TExprNodeType::FUNCTION_CALL);
    node.__set_type(result_type->to_thrift());
    node.__set_num_children(2);
    node.__set_output_scale(-1);
    node.__set_is_nullable(true);
    node.__set_fn(function);
    auto predicate = VectorizedFnCall::create_shared(node);
    predicate->add_child(VSlotRef::create_shared(
            -1, QUERY_COLUMN_ORDINAL, read_schema.column(QUERY_COLUMN_ORDINAL)->unique_id(),
            slot_type, read_schema.column(QUERY_COLUMN_ORDINAL)->name()));
    predicate->add_child(VLiteral::create_shared(
            string_type, Field::create_field<TYPE_STRING>(std::move(argument))));
    return VExprContext::create_shared(std::move(predicate));
}

VExprContextSPtr make_array_contains_context(const ReadSchema& read_schema, std::string value) {
    constexpr int32_t QUERY_COLUMN_ORDINAL = 1;
    const DataTypePtr array_type = read_schema.data_type(QUERY_COLUMN_ORDINAL);
    const DataTypePtr string_type = std::make_shared<DataTypeString>();

    TFunctionName name;
    name.__set_db_name("");
    name.__set_function_name("array_contains");
    TFunction function;
    function.__set_name(name);
    function.__set_binary_type(TFunctionBinaryType::BUILTIN);
    function.__set_arg_types({create_type_desc(PrimitiveType::TYPE_ARRAY),
                              create_type_desc(PrimitiveType::TYPE_STRING)});
    function.__set_ret_type(create_type_desc(PrimitiveType::TYPE_BOOLEAN));
    function.__set_has_var_args(false);

    TExprNode node;
    node.__set_type(create_type_desc(PrimitiveType::TYPE_BOOLEAN));
    node.__set_node_type(TExprNodeType::FUNCTION_CALL);
    node.__set_fn(function);
    node.__set_num_children(2);
    node.__set_is_nullable(true);
    auto predicate = VectorizedFnCall::create_shared(node);
    predicate->add_child(VSlotRef::create_shared(
            -1, QUERY_COLUMN_ORDINAL, read_schema.column(QUERY_COLUMN_ORDINAL)->unique_id(),
            array_type, read_schema.column(QUERY_COLUMN_ORDINAL)->name()));
    predicate->add_child(VLiteral::create_shared(
            string_type, Field::create_field<TYPE_STRING>(std::move(value))));
    return VExprContext::create_shared(std::move(predicate));
}

VExprContextSPtr make_whole_root_like_context(const ReadSchema& read_schema, std::string pattern) {
    constexpr int32_t QUERY_COLUMN_ORDINAL = 1;
    const DataTypePtr slot_type = read_schema.data_type(QUERY_COLUMN_ORDINAL);
    const DataTypePtr string_type = std::make_shared<DataTypeString>();
    const DataTypePtr nullable_string = make_nullable(string_type);
    const DataTypePtr nullable_boolean = make_nullable(std::make_shared<DataTypeUInt8>());

    TExprNode cast_node;
    cast_node.__set_node_type(TExprNodeType::CAST_EXPR);
    cast_node.__set_opcode(TExprOpcode::CAST);
    cast_node.__set_type(nullable_string->to_thrift());
    cast_node.__set_num_children(1);
    cast_node.__set_output_scale(-1);
    cast_node.__set_is_nullable(true);
    auto cast = VCastExpr::create_shared(cast_node);
    cast->add_child(VSlotRef::create_shared(
            -1, QUERY_COLUMN_ORDINAL, read_schema.column(QUERY_COLUMN_ORDINAL)->unique_id(),
            slot_type, read_schema.column(QUERY_COLUMN_ORDINAL)->name()));

    TFunctionName name;
    name.__set_db_name("");
    name.__set_function_name("like");
    TFunction function;
    function.__set_name(name);
    function.__set_binary_type(TFunctionBinaryType::BUILTIN);
    function.__set_arg_types({string_type->to_thrift(), string_type->to_thrift()});
    function.__set_ret_type(nullable_boolean->to_thrift());
    function.__set_has_var_args(false);

    TExprNode like_node;
    like_node.__set_node_type(TExprNodeType::FUNCTION_CALL);
    like_node.__set_type(nullable_boolean->to_thrift());
    like_node.__set_num_children(2);
    like_node.__set_output_scale(-1);
    like_node.__set_is_nullable(true);
    like_node.__set_fn(function);
    auto like = VectorizedFnCall::create_shared(like_node);
    like->add_child(std::move(cast));
    like->add_child(VLiteral::create_shared(string_type,
                                            Field::create_field<TYPE_STRING>(std::move(pattern))));
    return VExprContext::create_shared(std::move(like));
}

struct QueryPlan {
    TabletSchemaSPtr tablet_schema;
    ReadSchemaSPtr read_schema;
    std::vector<std::shared_ptr<ColumnPredicate>> predicates;
    std::map<std::string, DataTypePtr> target_cast_type_for_variants;
    std::unique_ptr<RuntimeState> runtime_state;
    VExprContextSPtrs common_expr_ctxs_push_down;
};

struct QueryResult {
    uint64_t rows = 0;
    uint64_t hash_sum = 0;
    uint64_t hash_xor = 0;
    int64_t raw_rows_read = 0;
    int64_t rows_inverted_index_filtered = 0;
    int64_t rows_vec_cond_filtered = 0;
    int64_t rows_conditions_filtered = 0;
    int64_t rows_expr_cond_filtered = 0;
    int64_t expr_cond_input_rows = 0;
    int64_t inverted_index_downgrade_count = 0;
    int64_t inverted_index_filter_timer = 0;
    int64_t inverted_index_query_timer = 0;
    int64_t inverted_index_searcher_cache_hit = 0;
    int64_t inverted_index_searcher_cache_miss = 0;
    int64_t inverted_index_query_cache_lookup = 0;
    int64_t inverted_index_query_cache_hit = 0;

    bool same_output(const QueryResult& other) const {
        return rows == other.rows && hash_sum == other.hash_sum && hash_xor == other.hash_xor;
    }
};

struct BenchmarkResult {
    uint64_t raw_json_bytes = 0;
    uint64_t input_data_bytes = 0;
    uint64_t input_index_bytes = 0;
    uint64_t input_total_bytes = 0;
    uint64_t output_data_bytes = 0;
    uint64_t output_index_bytes = 0;
    uint64_t output_total_bytes = 0;
    uint64_t input_physical_data_bytes = 0;
    uint64_t input_physical_index_bytes = 0;
    uint64_t input_physical_total_bytes = 0;
    uint64_t output_physical_data_bytes = 0;
    uint64_t output_physical_index_bytes = 0;
    uint64_t output_physical_total_bytes = 0;
    uint32_t input_segments = 0;
    uint32_t output_segments = 0;
    uint64_t input_logical_indexes = 0;
    uint64_t output_logical_indexes = 0;
    int64_t compaction_prepare_time_ns = 0;
    int64_t compaction_execute_time_ns = 0;
    int64_t merge_rowsets_time_ns = 0;
    int64_t merge_row_data_time_ns = 0;
    int64_t inverted_index_compaction_time_ns = 0;
    int64_t build_output_rowset_time_ns = 0;
    DataFingerprint input_data_fingerprint;
    DataFingerprint output_data_fingerprint;
    IndexFingerprint input_index_fingerprint;
    IndexFingerprint output_index_fingerprint;
};

class HighLevelVariantFixture {
public:
    HighLevelVariantFixture(IndexLayout layout, TextMode text_mode)
            : _layout(layout),
              _text_mode(text_mode),
              _fixture_id(next_fixture_id()),
              _directory(std::filesystem::path("./ut_dir") /
                         ("variant_root_high_level_" + std::string(layout_name(layout)) + "_" +
                          std::string(text_mode_name(text_mode)) + "_" + std::to_string(getpid()) +
                          "_" + std::to_string(_fixture_id))),
              _tmp_directory(_directory / "tmp"),
              _previous_ordered_compaction(config::enable_ordered_data_compaction),
              _previous_compaction_checksum(config::enable_compaction_checksum),
              _previous_vertical_compaction(config::enable_vertical_compaction),
              _previous_vertical_variant_compaction(
                      config::enable_vertical_compact_variant_subcolumns),
              _previous_index_compaction(config::inverted_index_compaction_enable) {}

    ~HighLevelVariantFixture() {
        _compaction.reset();
        _input_rowsets.clear();
        _tablet.reset();
        _schema.reset();
        _data_dir.reset();
        _engine = nullptr;
        if (_runtime_installed) {
            ExecEnv* env = ExecEnv::GetInstance();
            env->set_storage_engine(std::move(_previous_storage_engine));
            env->set_tmp_file_dir(std::move(_previous_tmp_file_dirs));
        }
        WARN_IF_ERROR(io::global_local_filesystem()->delete_directory(_directory.string()),
                      "Failed to clean high-level Variant root benchmark directory");
        config::enable_ordered_data_compaction = _previous_ordered_compaction;
        config::enable_compaction_checksum = _previous_compaction_checksum;
        config::enable_vertical_compaction = _previous_vertical_compaction;
        config::enable_vertical_compact_variant_subcolumns = _previous_vertical_variant_compaction;
        config::inverted_index_compaction_enable = _previous_index_compaction;
    }

    Status setup() {
        const SyntheticData& data = synthetic_data();
        RETURN_IF_ERROR(data.status);
        if (config::variant_storage_parse_mode != 0) {
            return Status::InvalidArgument(
                    "high-level Variant root benchmark requires variant_storage_parse_mode=0, "
                    "actual={}",
                    config::variant_storage_parse_mode);
        }

        ensure_runtime_services();
        config::enable_ordered_data_compaction = false;
        config::enable_compaction_checksum = false;
        config::enable_vertical_compaction = true;
        config::enable_vertical_compact_variant_subcolumns = true;
        config::inverted_index_compaction_enable = true;
        RETURN_IF_ERROR(io::global_local_filesystem()->delete_directory(_directory.string()));
        RETURN_IF_ERROR(io::global_local_filesystem()->create_directory(_directory.string()));
        RETURN_IF_ERROR(io::global_local_filesystem()->create_directory(_tmp_directory.string()));

        ExecEnv* env = ExecEnv::GetInstance();
        _previous_storage_engine = std::move(env->_storage_engine);
        _previous_tmp_file_dirs = std::move(env->_tmp_file_dirs);
        _runtime_installed = true;

        std::vector<StorePath> tmp_paths;
        tmp_paths.emplace_back(_tmp_directory.string(), 100ULL << 30);
        auto tmp_file_dirs = std::make_unique<segment_v2::TmpFileDirs>(tmp_paths);
        RETURN_IF_ERROR(tmp_file_dirs->init());
        env->set_tmp_file_dir(std::move(tmp_file_dirs));

        EngineOptions engine_options;
        engine_options.backend_uid =
                UniqueId(static_cast<int64_t>(getpid()), static_cast<int64_t>(_fixture_id));
        auto engine = std::make_unique<StorageEngine>(engine_options);
        _engine = engine.get();
        _data_dir = std::make_unique<DataDir>(*_engine, _directory.string());
        RETURN_IF_ERROR(_data_dir->init(true));
        env->set_storage_engine(std::move(engine));

        _schema = make_schema(_layout, _text_mode);
        auto tablet_meta = std::make_shared<TabletMeta>(_schema);
        const int64_t tablet_id = 300'000 + static_cast<int64_t>(_fixture_id);
        tablet_meta->_tablet_id = tablet_id;
        tablet_meta->set_tablet_uid(TabletUid(tablet_id, tablet_id + 1));
        _tablet = std::make_shared<Tablet>(*_engine, tablet_meta, _data_dir.get());
        RETURN_IF_ERROR(_tablet->init());
        RETURN_IF_ERROR(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()));
        return io::global_local_filesystem()->create_directory(_tablet->tablet_path());
    }

    Status write_import_rowset(benchmark::State* state, RowsetSharedPtr* rowset) {
        return write_rowset(/*rowset_index=*/0, /*rowset_count=*/1, state, rowset);
    }

    Status prepare_compaction_inputs(DataFingerprint* before, PhysicalIndexStats* index_stats) {
        DORIS_CHECK(before != nullptr);
        DORIS_CHECK(index_stats != nullptr);
        _input_rowsets.reserve(COMPACTION_INPUT_ROWSETS);
        for (uint32_t rowset_index = 0; rowset_index < COMPACTION_INPUT_ROWSETS; ++rowset_index) {
            RowsetSharedPtr rowset;
            RETURN_IF_ERROR(write_rowset(rowset_index, COMPACTION_INPUT_ROWSETS, nullptr, &rowset));
            RETURN_IF_ERROR(_tablet->add_rowset(rowset));
            _input_rowsets.push_back(std::move(rowset));
        }
        RETURN_IF_ERROR(fingerprint(_input_rowsets, before));
        RETURN_IF_ERROR(inspect_indexes(_input_rowsets, index_stats));
        RETURN_IF_ERROR(validate_fingerprint(*before));
        RETURN_IF_ERROR(validate_index_stats(*index_stats));
        _compaction = std::make_unique<FullCompaction>(*_engine, _tablet);
        return Status::OK();
    }

    Status compact(BenchmarkResult* result) {
        DORIS_CHECK(_compaction != nullptr);
        DORIS_CHECK(result != nullptr);
        const auto prepare_start = std::chrono::steady_clock::now();
        RETURN_IF_ERROR(_compaction->prepare_compact());
        const auto execute_start = std::chrono::steady_clock::now();
        const Status status = _compaction->execute_compact();
        const auto execute_end = std::chrono::steady_clock::now();
        result->compaction_prepare_time_ns =
                std::chrono::duration_cast<std::chrono::nanoseconds>(execute_start - prepare_start)
                        .count();
        result->compaction_execute_time_ns =
                std::chrono::duration_cast<std::chrono::nanoseconds>(execute_end - execute_start)
                        .count();
        RETURN_IF_ERROR(status);

        RuntimeProfile* profile = _compaction->runtime_profile();
        DORIS_CHECK(profile != nullptr);
        auto counter_value = [profile](const char* name) {
            RuntimeProfile::Counter* counter = profile->get_counter(name);
            DORIS_CHECK(counter != nullptr);
            return counter->value();
        };
        result->merge_rowsets_time_ns = counter_value("merge_rowsets_latency");
        result->merge_row_data_time_ns = counter_value("merge_row_data_latency");
        result->inverted_index_compaction_time_ns =
                counter_value("inverted_index_compaction_latency");
        result->build_output_rowset_time_ns = counter_value("build_output_rowset_latency");
        return Status::OK();
    }

    Status validate_import(const RowsetSharedPtr& rowset, BenchmarkResult* result) const {
        DORIS_CHECK(rowset != nullptr);
        DORIS_CHECK(result != nullptr);
        DataFingerprint fingerprint_result;
        PhysicalIndexStats index_stats;
        RETURN_IF_ERROR(fingerprint({rowset}, &fingerprint_result));
        RETURN_IF_ERROR(inspect_indexes({rowset}, &index_stats));
        RETURN_IF_ERROR(validate_fingerprint(fingerprint_result));
        RETURN_IF_ERROR(validate_index_stats(index_stats));
        result->raw_json_bytes = _raw_json_bytes;
        result->output_data_bytes = rowset->data_disk_size();
        result->output_index_bytes = rowset->index_disk_size();
        result->output_total_bytes = rowset->total_disk_size();
        result->output_physical_data_bytes = index_stats.physical_data_bytes;
        result->output_physical_index_bytes = index_stats.physical_index_bytes;
        result->output_physical_total_bytes = index_stats.physical_total_bytes;
        result->output_segments = index_stats.segments;
        result->output_logical_indexes = index_stats.logical_indexes;
        result->output_data_fingerprint = fingerprint_result;
        result->output_index_fingerprint = index_stats.fingerprint;
        return Status::OK();
    }

    Status validate_compaction(const DataFingerprint& before,
                               const PhysicalIndexStats& before_indexes,
                               BenchmarkResult* result) const {
        DORIS_CHECK(result != nullptr);
        if (_compaction == nullptr) {
            return Status::InternalError("FullCompaction did not produce an output rowset");
        }
        RowsetSharedPtr output;
        {
            std::shared_lock lock(_tablet->get_header_lock());
            output = _tablet->get_rowset_by_version(Version(0, COMPACTION_INPUT_ROWSETS - 1));
        }
        if (output == nullptr) {
            return Status::InternalError("FullCompaction output version [0,{}] is missing",
                                         COMPACTION_INPUT_ROWSETS - 1);
        }
        DataFingerprint after;
        PhysicalIndexStats after_indexes;
        RETURN_IF_ERROR(fingerprint({output}, &after));
        RETURN_IF_ERROR(inspect_indexes({output}, &after_indexes));
        RETURN_IF_ERROR(validate_fingerprint(after));
        RETURN_IF_ERROR(validate_index_stats(after_indexes));
        if (!(before == after)) {
            return Status::Corruption(
                    "FullCompaction changed data fingerprint: rows {}/{}, int {}/{}, string "
                    "{}/{}, arrays {}/{}, array_sum {}/{}, array_xor {}/{}, sum {}/{}, xor {}/{}",
                    before.rows, after.rows, before.int_values, after.int_values,
                    before.string_values, after.string_values, before.array_rows, after.array_rows,
                    before.array_hash_sum, after.array_hash_sum, before.array_hash_xor,
                    after.array_hash_xor, before.hash_sum, after.hash_sum, before.hash_xor,
                    after.hash_xor);
        }
        if (!(before_indexes.fingerprint == after_indexes.fingerprint)) {
            return Status::Corruption(
                    "FullCompaction changed index fingerprint: rows {}/{}, indexed {}/{}, "
                    "nulls {}/{}, logical_stats {}/{}, logical_mix {}/{}, term_df {}/{}, "
                    "term_hash {}/{}, term_mix {}/{}, integer_docs {}/{}, integer_points {}/{}, "
                    "integer_doc_hash {}/{}, integer_point_hash {}/{}",
                    before_indexes.fingerprint.document_rows,
                    after_indexes.fingerprint.document_rows,
                    before_indexes.fingerprint.indexed_documents,
                    after_indexes.fingerprint.indexed_documents,
                    before_indexes.fingerprint.null_documents,
                    after_indexes.fingerprint.null_documents,
                    before_indexes.fingerprint.logical_stats_hash_sum,
                    after_indexes.fingerprint.logical_stats_hash_sum,
                    before_indexes.fingerprint.logical_stats_hash_mix,
                    after_indexes.fingerprint.logical_stats_hash_mix,
                    before_indexes.fingerprint.term_document_frequency,
                    after_indexes.fingerprint.term_document_frequency,
                    before_indexes.fingerprint.term_df_hash_sum,
                    after_indexes.fingerprint.term_df_hash_sum,
                    before_indexes.fingerprint.term_df_hash_mix,
                    after_indexes.fingerprint.term_df_hash_mix,
                    before_indexes.fingerprint.integer_documents,
                    after_indexes.fingerprint.integer_documents,
                    before_indexes.fingerprint.integer_points,
                    after_indexes.fingerprint.integer_points,
                    before_indexes.fingerprint.integer_doc_hash_sum,
                    after_indexes.fingerprint.integer_doc_hash_sum,
                    before_indexes.fingerprint.integer_point_hash_sum,
                    after_indexes.fingerprint.integer_point_hash_sum);
        }
        result->raw_json_bytes = _raw_json_bytes;
        for (const auto& rowset : _input_rowsets) {
            result->input_data_bytes += rowset->data_disk_size();
            result->input_index_bytes += rowset->index_disk_size();
            result->input_total_bytes += rowset->total_disk_size();
        }
        result->output_data_bytes = output->data_disk_size();
        result->output_index_bytes = output->index_disk_size();
        result->output_total_bytes = output->total_disk_size();
        result->input_physical_data_bytes = before_indexes.physical_data_bytes;
        result->input_physical_index_bytes = before_indexes.physical_index_bytes;
        result->input_physical_total_bytes = before_indexes.physical_total_bytes;
        result->output_physical_data_bytes = after_indexes.physical_data_bytes;
        result->output_physical_index_bytes = after_indexes.physical_index_bytes;
        result->output_physical_total_bytes = after_indexes.physical_total_bytes;
        result->input_segments = before_indexes.segments;
        result->output_segments = after_indexes.segments;
        result->input_logical_indexes = before_indexes.logical_indexes;
        result->output_logical_indexes = after_indexes.logical_indexes;
        result->input_data_fingerprint = before;
        result->output_data_fingerprint = after;
        result->input_index_fingerprint = before_indexes.fingerprint;
        result->output_index_fingerprint = after_indexes.fingerprint;
        return Status::OK();
    }

    Status prepare_query_rowset(RowsetSharedPtr* rowset) {
        DORIS_CHECK(rowset != nullptr);
        RETURN_IF_ERROR(write_rowset(/*rowset_index=*/0, /*rowset_count=*/1, nullptr, rowset));
        return _tablet->add_rowset(*rowset);
    }

    Status prepare_query_plan(QueryShape shape, bool enable_inverted_index, QueryPlan* result,
                              std::string_view match_query_override = {}) const {
        DORIS_CHECK(result != nullptr);
        if (is_english_query(shape) != (_text_mode == TextMode::ENGLISH)) {
            return Status::InvalidArgument("query shape {} is incompatible with text mode {}",
                                           query_shape_name(shape), text_mode_name(_text_mode));
        }
        if (shape == QueryShape::WHOLE_ROOT_ENGLISH_MATCH_ANY &&
            _layout != IndexLayout::ALL_VALUES) {
            return Status::InvalidArgument(
                    "whole-root MATCH is only valid for the all_values layout");
        }
        if (shape == QueryShape::WHOLE_ROOT_LIKE_SUBSTRING && _layout != IndexLayout::ALL_VALUES &&
            _layout != IndexLayout::NO_INDEX) {
            return Status::InvalidArgument(
                    "whole-root LIKE is only benchmarked for all_values and no-index layouts");
        }
        if (shape == QueryShape::PATH_ARRAY_CONTAINS && _layout != IndexLayout::CHILDREN &&
            _layout != IndexLayout::NO_INDEX) {
            return Status::InvalidArgument(
                    "array_contains is only benchmarked for children and no-index layouts");
        }
        if ((shape == QueryShape::PATH_LIKE_SUBSTRING || shape == QueryShape::PATH_STARTS_WITH) &&
            _layout != IndexLayout::NO_INDEX) {
            return Status::InvalidArgument("{} is a no-index scan baseline",
                                           query_shape_name(shape));
        }

        result->tablet_schema =
                is_whole_root_query(shape) ? make_whole_root_query_schema() : make_query_schema();
        const int32_t key_index = result->tablet_schema->field_index(KEY_UID);
        DORIS_CHECK_GE(key_index, 0);
        int32_t query_column_index = result->tablet_schema->field_index(ROOT_UID);
        if (!is_whole_root_query(shape)) {
            std::string_view relative_path = STRING_QUERY_RELATIVE_PATH;
            if (shape == QueryShape::PATH_NUMERIC_EQ || shape == QueryShape::PATH_NUMERIC_IN) {
                relative_path = INTEGER_QUERY_RELATIVE_PATH;
            } else if (shape == QueryShape::PATH_ARRAY_CONTAINS) {
                relative_path = ARRAY_QUERY_RELATIVE_PATH;
            }
            const std::string full_path = std::string(ROOT_NAME) + "." + std::string(relative_path);
            query_column_index = result->tablet_schema->field_index(full_path);
        }
        DORIS_CHECK_GE(query_column_index, 0);
        const std::vector<ColumnId> projection {cast_set<ColumnId>(key_index),
                                                cast_set<ColumnId>(query_column_index)};
        result->read_schema = std::make_shared<ReadSchema>(
                project_columns_by_ordinal(result->tablet_schema->columns(), projection));

        const TabletColumn* query_column = result->read_schema->column(1);
        DORIS_CHECK(query_column != nullptr);
        if (shape == QueryShape::PATH_STRING_EQ) {
            result->predicates.push_back(create_comparison_predicate<PredicateType::EQ>(
                    1, query_column->name(), std::make_shared<DataTypeString>(),
                    Field::create_field<TYPE_STRING>(std::string(EXACT_QUERY_VALUE)), false));
        } else if (shape == QueryShape::PATH_STRING_IN) {
            auto values = build_set<TYPE_STRING>();
            for (const std::string_view value :
                 {EXACT_QUERY_VALUE, std::string_view("PushEvent repository-0 ref-heads-main-1")}) {
                StringRef reference(value.data(), value.size());
                values->insert(&reference);
            }
            result->predicates.push_back(create_in_list_predicate<PredicateType::IN_LIST>(
                    1, query_column->name(), std::make_shared<DataTypeString>(), values, false));
        } else if (shape == QueryShape::PATH_NUMERIC_EQ) {
            result->predicates.push_back(create_comparison_predicate<PredicateType::EQ>(
                    1, query_column->name(), std::make_shared<DataTypeInt64>(),
                    Field::create_field<TYPE_BIGINT>(int64_t {50}), false));
        } else if (shape == QueryShape::PATH_NUMERIC_IN) {
            auto values = build_set<TYPE_BIGINT>();
            for (int64_t value : {50, 100, 750, 800}) {
                values->insert(&value);
            }
            result->predicates.push_back(create_in_list_predicate<PredicateType::IN_LIST>(
                    1, query_column->name(), std::make_shared<DataTypeInt64>(), values, false));
        } else if (shape == QueryShape::PATH_ENGLISH_MATCH_ANY ||
                   shape == QueryShape::WHOLE_ROOT_ENGLISH_MATCH_ANY) {
            const std::string_view query =
                    match_query_override.empty() ? ENGLISH_QUERY_VALUE : match_query_override;
            result->common_expr_ctxs_push_down.push_back(
                    make_match_context(*result->read_schema, std::string(query), false));
        } else if (shape == QueryShape::PATH_ENGLISH_MATCH_ALL) {
            const std::string_view query = match_query_override.empty()
                                                   ? ENGLISH_MATCH_ALL_QUERY_VALUE
                                                   : match_query_override;
            result->common_expr_ctxs_push_down.push_back(
                    make_match_context(*result->read_schema, std::string(query), true));
        } else if (shape == QueryShape::PATH_LIKE_SUBSTRING) {
            result->common_expr_ctxs_push_down.push_back(make_string_function_context(
                    *result->read_schema, "like", std::string(LIKE_QUERY_VALUE)));
        } else if (shape == QueryShape::PATH_STARTS_WITH) {
            result->common_expr_ctxs_push_down.push_back(make_string_function_context(
                    *result->read_schema, "starts_with", std::string(STARTS_WITH_QUERY_VALUE)));
        } else if (shape == QueryShape::PATH_ARRAY_CONTAINS) {
            result->common_expr_ctxs_push_down.push_back(make_array_contains_context(
                    *result->read_schema, std::string(ARRAY_QUERY_VALUE)));
        } else if (shape == QueryShape::WHOLE_ROOT_LIKE_SUBSTRING) {
            result->common_expr_ctxs_push_down.push_back(make_whole_root_like_context(
                    *result->read_schema, std::string(LIKE_QUERY_VALUE)));
        }
        if (!is_whole_root_query(shape)) {
            result->target_cast_type_for_variants[query_column->name()] =
                    result->read_schema->data_type(1);
        } else if (shape == QueryShape::WHOLE_ROOT_LIKE_SUBSTRING) {
            result->target_cast_type_for_variants[query_column->name()] =
                    make_nullable(std::make_shared<DataTypeString>());
        }

        TQueryOptions query_options;
        query_options.__set_enable_inverted_index_query(enable_inverted_index &&
                                                        expects_inverted_index(_layout, shape));
        query_options.__set_enable_fallback_on_missing_inverted_index(true);
        query_options.__set_enable_no_need_read_data_opt(true);
        query_options.__set_enable_common_expr_pushdown(true);
        query_options.__set_enable_inverted_index_query_cache(false);
        query_options.__set_enable_inverted_index_searcher_cache(true);
        query_options.__set_enable_file_cache(false);
        query_options.__set_disable_file_cache(true);
        query_options.__set_enable_segment_cache(false);
        query_options.__set_enable_match_without_inverted_index(true);
        query_options.__set_inverted_index_skip_threshold(0);
        query_options.__set_batch_size(BATCH_ROWS);

        result->runtime_state = std::make_unique<RuntimeState>();
        result->runtime_state->set_exec_env(ExecEnv::GetInstance());
        result->runtime_state->set_query_options(query_options);
        TupleDescriptor tuple_desc;
        RowDescriptor row_desc(&tuple_desc);
        for (const auto& expr_ctx : result->common_expr_ctxs_push_down) {
            RETURN_IF_ERROR(expr_ctx->prepare(result->runtime_state.get(), row_desc));
            RETURN_IF_ERROR(expr_ctx->open(result->runtime_state.get()));
        }
        return Status::OK();
    }

    Status run_query(const RowsetSharedPtr& rowset, const QueryPlan& plan,
                     QueryResult* result) const {
        DORIS_CHECK(rowset != nullptr);
        DORIS_CHECK(result != nullptr);
        *result = QueryResult {};

        RowsetReaderSharedPtr reader;
        RETURN_IF_ERROR(rowset->create_reader(&reader));
        OlapReaderStatistics statistics;
        RowsetReaderContext context;
        context.reader_type = ReaderType::READER_QUERY;
        context.tablet_schema = plan.tablet_schema;
        context.need_ordered_result = true;
        context.read_schema = plan.read_schema;
        context.predicates = &plan.predicates;
        context.stats = &statistics;
        context.runtime_state = plan.runtime_state.get();
        context.common_expr_ctxs_push_down = plan.common_expr_ctxs_push_down;
        context.target_cast_type_for_variants = plan.target_cast_type_for_variants;
        context.use_page_cache = false;
        context.batch_size = BATCH_ROWS;
        RETURN_IF_ERROR(reader->init(&context));

        while (true) {
            Block block = plan.read_schema->create_read_block();
            Status status = reader->next_batch(&block);
            if (status.is<ErrorCode::END_OF_FILE>()) {
                break;
            }
            RETURN_IF_ERROR(status);
            const auto& key_column =
                    assert_cast<const ColumnInt64&>(*block.get_by_position(0).column);
            for (size_t row = 0; row < block.rows(); ++row) {
                const uint64_t key = cast_set<uint64_t>(key_column.get_element(row));
                const uint64_t row_hash = (key ^ FNV_OFFSET) * FNV_PRIME;
                result->hash_sum += row_hash;
                result->hash_xor ^= row_hash;
            }
            result->rows += block.rows();
        }

        result->raw_rows_read = statistics.raw_rows_read;
        result->rows_inverted_index_filtered = statistics.rows_inverted_index_filtered;
        result->rows_vec_cond_filtered = statistics.rows_vec_cond_filtered;
        result->rows_conditions_filtered = statistics.rows_conditions_filtered;
        result->rows_expr_cond_filtered = statistics.rows_expr_cond_filtered;
        result->expr_cond_input_rows = statistics.expr_cond_input_rows;
        result->inverted_index_downgrade_count = statistics.inverted_index_downgrade_count;
        result->inverted_index_filter_timer = statistics.inverted_index_filter_timer;
        result->inverted_index_query_timer = statistics.inverted_index_query_timer;
        result->inverted_index_searcher_cache_hit = statistics.inverted_index_searcher_cache_hit;
        result->inverted_index_searcher_cache_miss = statistics.inverted_index_searcher_cache_miss;
        result->inverted_index_query_cache_lookup = statistics.inverted_index_query_cache_lookup;
        result->inverted_index_query_cache_hit = statistics.inverted_index_query_cache_hit;
        return Status::OK();
    }

private:
    Status make_input_block(uint32_t rowset_index, uint32_t rowset_count, uint32_t first_local_row,
                            uint32_t rows, benchmark::State* state, Block* block) {
        DORIS_CHECK(block != nullptr);
        auto keys = ColumnInt64::create();
        keys->reserve(rows);
        auto values = ColumnVariantV2::create();
        std::vector<std::string> json_rows;
        if (state != nullptr) {
            state->PauseTiming();
        }
        json_rows.reserve(rows);
        for (uint32_t local = 0; local < rows; ++local) {
            const uint32_t local_row = first_local_row + local;
            const uint32_t global_row =
                    rowset_count == 1 ? local_row : local_row * rowset_count + rowset_index;
            json_rows.push_back(make_synthetic_json(global_row, _text_mode));
            _raw_json_bytes += json_rows.back().size();
        }
        if (state != nullptr) {
            state->ResumeTiming();
        }
        RETURN_IF_CATCH_EXCEPTION({
            JsonStringToVariantEncoder encoder(JsonToVariantOptions::current_config());
            for (uint32_t local = 0; local < rows; ++local) {
                const uint32_t local_row = first_local_row + local;
                const uint32_t global_row =
                        rowset_count == 1 ? local_row : local_row * rowset_count + rowset_index;
                keys->insert_value(global_row);
                const std::string& json = json_rows[local];
                encoder.add_json({json.data(), json.size()});
            }
            VariantBatchBuilder encoded = encoder.finish_batch();
            values->insert_encoded_batch(encoded);
        });
        block->insert({std::move(keys), std::make_shared<DataTypeInt64>(), "k"});
        block->insert({std::move(values), std::make_shared<DataTypeVariantV2>(10'000, false),
                       std::string(ROOT_NAME)});
        if (state != nullptr) {
            state->PauseTiming();
        }
        std::vector<std::string>().swap(json_rows);
        if (state != nullptr) {
            state->ResumeTiming();
        }
        return Status::OK();
    }

    Status write_rowset(uint32_t rowset_index, uint32_t rowset_count, benchmark::State* state,
                        RowsetSharedPtr* rowset) {
        DORIS_CHECK(rowset != nullptr);
        const uint32_t rows = synthetic_data().rows / rowset_count;
        RowsetWriterContext context;
        RowsetId rowset_id;
        rowset_id.init(static_cast<int64_t>(_fixture_id) * 100 + rowset_index + 1);
        context.rowset_id = rowset_id;
        context.rowset_type = BETA_ROWSET;
        context.data_dir = _data_dir.get();
        context.rowset_state = VISIBLE;
        context.tablet_schema = _schema;
        context.tablet_path = _tablet->tablet_path();
        context.tablet_id = _tablet->tablet_id();
        context.tablet_uid = _tablet->tablet_uid();
        context.tablet = _tablet;
        context.version = Version(rowset_index, rowset_index);
        context.segments_overlap = NONOVERLAPPING;
        context.max_rows_per_segment = rows;
        context.write_type = DataWriteType::TYPE_DIRECT;

        auto writer_result = RowsetFactory::create_rowset_writer(*_engine, context, false);
        if (!writer_result.has_value()) {
            return writer_result.error();
        }
        auto writer = std::move(writer_result).value();
        for (uint32_t first = 0; first < rows; first += BATCH_ROWS) {
            Block block;
            RETURN_IF_ERROR(make_input_block(rowset_index, rowset_count, first,
                                             std::min(BATCH_ROWS, rows - first), state, &block));
            RETURN_IF_ERROR(writer->add_block(&block));
        }
        RETURN_IF_ERROR(writer->flush());
        RETURN_IF_ERROR(writer->build(*rowset));
        return Status::OK();
    }

    TabletSchemaSPtr make_query_schema() const {
        TabletSchemaPB schema_pb;
        _schema->to_schema_pb(&schema_pb);
        auto query_schema = std::make_shared<TabletSchema>();
        query_schema->init_from_pb(schema_pb);
        query_schema->set_storage_format(_schema->storage_format());
        const int32_t root_index = query_schema->field_index(ROOT_UID);
        DORIS_CHECK_GE(root_index, 0);
        query_schema->mutable_column(root_index).set_variant_is_v2(false);
        const TabletColumn root = query_schema->column(root_index);
        const auto append_path = [&](std::string_view relative_path, DataTypePtr type) {
            const std::string full_path = root.name_lower_case() + "." + std::string(relative_path);
            TabletColumn path_column = variant_util::get_column_by_type(
                    std::move(type), full_path,
                    variant_util::ExtraInfo {.parent_unique_id = root.unique_id(),
                                             .path_info = PathInData(full_path)});
            path_column.set_is_nullable(true);
            variant_util::inherit_column_attributes(root, path_column);
            query_schema->append_column(path_column, TabletSchema::ColumnType::VARIANT);
        };
        for (uint32_t field = 0; field < INTEGER_FIELDS; ++field) {
            append_path(synthetic_field_path(true, field), std::make_shared<DataTypeInt64>());
        }
        for (uint32_t field = 0; field < TEXT_FIELDS; ++field) {
            append_path(synthetic_field_path(false, field), std::make_shared<DataTypeString>());
        }
        append_path(
                ARRAY_QUERY_RELATIVE_PATH,
                std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeString>())));
        return query_schema;
    }

    TabletSchemaSPtr make_whole_root_query_schema() const {
        TabletSchemaPB schema_pb;
        _schema->to_schema_pb(&schema_pb);
        auto query_schema = std::make_shared<TabletSchema>();
        query_schema->init_from_pb(schema_pb);
        query_schema->set_storage_format(_schema->storage_format());
        const int32_t root_index = query_schema->field_index(ROOT_UID);
        DORIS_CHECK_GE(root_index, 0);
        query_schema->mutable_column(root_index).set_variant_is_v2(true);
        return query_schema;
    }

    Status fingerprint(const std::vector<RowsetSharedPtr>& rowsets, DataFingerprint* result) const {
        DORIS_CHECK(result != nullptr);
        *result = DataFingerprint {};
        const TabletSchemaSPtr query_schema = make_query_schema();
        const int32_t root_index = query_schema->field_index(ROOT_UID);
        DORIS_CHECK_GE(root_index, 0);
        const ColumnId first_path = cast_set<ColumnId>(query_schema->num_columns() - TOTAL_PATHS);
        std::vector<ColumnId> projection {cast_set<ColumnId>(KEY_UID)};
        projection.reserve(TOTAL_PATHS + 1);
        for (uint32_t field = 0; field < TOTAL_PATHS; ++field) {
            projection.push_back(first_path + field);
        }
        auto read_schema = std::make_shared<ReadSchema>(
                project_columns_by_ordinal(query_schema->columns(), projection));

        for (const auto& rowset : rowsets) {
            RowsetReaderSharedPtr reader;
            RETURN_IF_ERROR(rowset->create_reader(&reader));
            OlapReaderStatistics statistics;
            RowsetReaderContext context;
            context.reader_type = ReaderType::READER_QUERY;
            context.tablet_schema = query_schema;
            context.need_ordered_result = true;
            context.read_schema = read_schema;
            context.stats = &statistics;
            RETURN_IF_ERROR(reader->init(&context));

            while (true) {
                Block block = read_schema->create_read_block();
                Status status = reader->next_batch(&block);
                if (status.is<ErrorCode::END_OF_FILE>()) {
                    break;
                }
                RETURN_IF_ERROR(status);
                const auto& key = block.get_by_position(0);
                for (size_t row = 0; row < block.rows(); ++row) {
                    const std::string key_value = key.type->to_string(*key.column, row);
                    uint64_t row_hash = FNV_OFFSET;
                    row_hash = update_hash(row_hash, key_value);
                    for (uint32_t field = 0; field < TOTAL_PATHS; ++field) {
                        const auto& value = block.get_by_position(field + 1);
                        const bool is_null = value.column->is_null_at(row);
                        row_hash = update_hash(row_hash, is_null ? "N" : "V");
                        std::string serialized_value;
                        if (!is_null) {
                            serialized_value = value.type->to_string(*value.column, row);
                            row_hash = update_hash(row_hash, serialized_value);
                            if (field < INTEGER_FIELDS) {
                                ++result->int_values;
                            } else if (field < TOTAL_FIELDS) {
                                ++result->string_values;
                            } else {
                                ++result->array_rows;
                            }
                        }
                        if (field == TOTAL_FIELDS) {
                            uint64_t array_hash = update_hash(FNV_OFFSET, key_value);
                            array_hash = update_hash(array_hash, is_null ? "N" : "V");
                            if (!is_null) {
                                array_hash = update_hash(array_hash, serialized_value);
                            }
                            result->array_hash_sum += array_hash;
                            result->array_hash_xor ^= array_hash;
                        }
                    }
                    result->hash_sum += row_hash;
                    result->hash_xor ^= row_hash;
                }
                result->rows += block.rows();
            }
        }
        return Status::OK();
    }

    Status accumulate_text_index(const snii::reader::LogicalIndexReader& index,
                                 std::string_view logical_name,
                                 IndexFingerprint* fingerprint_result) const {
        DORIS_CHECK(fingerprint_result != nullptr);
        fingerprint_result->document_rows += index.stats().doc_count;
        fingerprint_result->indexed_documents += index.stats().indexed_doc_count;
        fingerprint_result->null_documents += index.stats().null_count;
        const uint64_t logical_hash = update_hash(FNV_OFFSET, logical_name);
        const uint64_t doc_hash = update_hash(logical_hash, "documents");
        const uint64_t indexed_hash = update_hash(logical_hash, "indexed");
        const uint64_t null_hash = update_hash(logical_hash, "nulls");
        fingerprint_result->logical_stats_hash_sum +=
                doc_hash * index.stats().doc_count +
                indexed_hash * index.stats().indexed_doc_count +
                null_hash * index.stats().null_count;
        fingerprint_result->logical_stats_hash_mix +=
                (doc_hash * doc_hash + FNV_PRIME) * index.stats().doc_count +
                (indexed_hash * indexed_hash + FNV_PRIME) * index.stats().indexed_doc_count +
                (null_hash * null_hash + FNV_PRIME) * index.stats().null_count;
        return index.visit_prefix_terms("", [&](snii::reader::LogicalIndexReader::PrefixHit&& hit,
                                                bool* stop) {
            DORIS_CHECK(stop != nullptr);
            const uint64_t term_hash = update_hash(update_hash(FNV_OFFSET, logical_name), hit.term);
            fingerprint_result->term_document_frequency += hit.entry.df;
            fingerprint_result->term_df_hash_sum += term_hash * hit.entry.df;
            fingerprint_result->term_df_hash_mix +=
                    (term_hash * term_hash + FNV_PRIME) * hit.entry.df;
            return Status::OK();
        });
    }

    Status accumulate_integer_index(segment_v2::snii_doris::DorisSniiFileReader* adapter,
                                    const snii::format::LogicalIndexMetadataRef& entry,
                                    std::string_view logical_name, uint64_t segment_rows,
                                    IndexFingerprint* fingerprint_result) const {
        DORIS_CHECK(adapter != nullptr);
        DORIS_CHECK(fingerprint_result != nullptr);
        snii::bkd::BkdSections sections;
        bool has_data = false;
        bool has_index = false;
        for (const auto& file : entry.files) {
            if (file.name == "bkd_data") {
                sections.data_offset = file.offset;
                sections.data_length = file.length;
                has_data = true;
            } else if (file.name == "bkd_index") {
                sections.index_offset = file.offset;
                sections.index_length = file.length;
                has_index = true;
            }
        }
        if (!has_data || !has_index) {
            return Status::Corruption("integer SNII index is missing data or index sections");
        }
        std::unique_ptr<snii::bkd::BkdReader> bkd;
        RETURN_IF_ERROR(snii::bkd::BkdReader::open(adapter, sections, &bkd));
        if (bkd->doc_count() == 0 || bkd->doc_count() > segment_rows) {
            return Status::Corruption("integer SNII index doc_count={}, segment_rows={}",
                                      bkd->doc_count(), segment_rows);
        }
        fingerprint_result->integer_documents += bkd->doc_count();
        fingerprint_result->integer_points += bkd->point_count();
        const uint64_t logical_hash = update_hash(FNV_OFFSET, logical_name);
        fingerprint_result->integer_doc_hash_sum += logical_hash * bkd->doc_count();
        fingerprint_result->integer_point_hash_sum += logical_hash * bkd->point_count();
        return Status::OK();
    }

    Status inspect_indexes(const std::vector<RowsetSharedPtr>& rowsets,
                           PhysicalIndexStats* result) const {
        DORIS_CHECK(result != nullptr);
        *result = PhysicalIndexStats {};
        if (!has_inverted_index(_layout) && !_schema->inverted_indexes().empty()) {
            return Status::Corruption("no-index layout unexpectedly contains a TabletIndex");
        }
        for (const auto& rowset : rowsets) {
            auto beta_rowset = std::static_pointer_cast<BetaRowset>(rowset);
            std::vector<segment_v2::SegmentSharedPtr> segments;
            RETURN_IF_ERROR(beta_rowset->load_segments(&segments));
            if (segments.size() != rowset->num_segments()) {
                return Status::Corruption("loaded {} segments for rowset with {} segments",
                                          segments.size(), rowset->num_segments());
            }
            uint64_t rowset_data_bytes = 0;
            uint64_t rowset_index_bytes = 0;
            for (uint32_t segment_id = 0; segment_id < rowset->num_segments(); ++segment_id) {
                auto segment_path = rowset->segment_path(segment_id);
                if (!segment_path.has_value()) {
                    return segment_path.error();
                }
                int64_t data_file_size = 0;
                RETURN_IF_ERROR(io::global_local_filesystem()->file_size(segment_path.value(),
                                                                         &data_file_size));
                if (data_file_size <= 0) {
                    return Status::Corruption("segment {} data file has size {}", segment_id,
                                              data_file_size);
                }
                rowset_data_bytes += cast_set<uint64_t>(data_file_size);
                ++result->segments;

                const std::string prefix(
                        segment_v2::InvertedIndexDescriptor::get_index_file_path_prefix(
                                segment_path.value()));
                const std::string index_path =
                        segment_v2::InvertedIndexDescriptor::get_index_file_path_v2(prefix);
                bool index_exists = false;
                RETURN_IF_ERROR(io::global_local_filesystem()->exists(index_path, &index_exists));
                if (!has_inverted_index(_layout)) {
                    if (index_exists) {
                        return Status::Corruption("no-index segment {} unexpectedly contains {}",
                                                  segment_id, index_path);
                    }
                    continue;
                }
                if (!index_exists) {
                    return Status::Corruption("{} segment {} is missing {}", layout_name(_layout),
                                              segment_id, index_path);
                }
                int64_t index_file_size = 0;
                RETURN_IF_ERROR(
                        io::global_local_filesystem()->file_size(index_path, &index_file_size));
                if (index_file_size <= 0) {
                    return Status::Corruption("segment {} index file has size {}", segment_id,
                                              index_file_size);
                }
                rowset_index_bytes += cast_set<uint64_t>(index_file_size);

                io::FileReaderSPtr file;
                RETURN_IF_ERROR(io::global_local_filesystem()->open_file(index_path, &file));
                segment_v2::snii_doris::DorisSniiFileReader adapter(file);
                snii::reader::SniiSegmentReader reader;
                RETURN_IF_ERROR(snii::reader::SniiSegmentReader::open(&adapter, &reader));
                result->logical_indexes += reader.n_logical_indexes();

                const uint64_t segment_rows = segments[segment_id]->num_rows();
                bool root_exists = false;
                RETURN_IF_ERROR(reader.index_exists(INDEX_ID, "", &root_exists));
                if (is_single_root_layout(_layout)) {
                    if (reader.n_logical_indexes() != 1 || !root_exists) {
                        return Status::Corruption(
                                "{} segment {} has {} logical indexes, root_exists={}",
                                layout_name(_layout), segment_id, reader.n_logical_indexes(),
                                root_exists);
                    }
                    snii::reader::LogicalIndexReader root_index;
                    RETURN_IF_ERROR(reader.open_index(INDEX_ID, "", &root_index));
                    if (root_index.stats().doc_count != segment_rows) {
                        return Status::Corruption("root segment {} index doc_count={}, rows={}",
                                                  segment_id, root_index.stats().doc_count,
                                                  segment_rows);
                    }
                    RETURN_IF_ERROR(accumulate_text_index(root_index, "", &result->fingerprint));
                    continue;
                }

                if (root_exists) {
                    return Status::Corruption("children segment {} contains a root logical index",
                                              segment_id);
                }
                if (reader.n_logical_indexes() != TOTAL_PATHS) {
                    return Status::Corruption(
                            "children segment {} has {} logical indexes, expected={}", segment_id,
                            reader.n_logical_indexes(), TOTAL_PATHS);
                }
                for (uint32_t field = 0; field < INTEGER_FIELDS; ++field) {
                    const std::string suffix =
                            variant_index_suffix(synthetic_field_path(true, field));
                    bool exists = false;
                    RETURN_IF_ERROR(reader.index_exists(INDEX_ID, suffix, &exists));
                    if (!exists) {
                        return Status::Corruption("children segment {} is missing integer field {}",
                                                  segment_id, synthetic_field_path(true, field));
                    }
                    const snii::format::LogicalIndexMetadataRef* integer_index = nullptr;
                    RETURN_IF_ERROR(reader.blob_entry(INDEX_ID, suffix, &integer_index));
                    DORIS_CHECK(integer_index != nullptr);
                    RETURN_IF_ERROR(accumulate_integer_index(&adapter, *integer_index, suffix,
                                                             segment_rows, &result->fingerprint));
                }
                for (uint32_t field = 0; field < TEXT_FIELDS; ++field) {
                    const std::string suffix =
                            variant_index_suffix(synthetic_field_path(false, field));
                    bool exists = false;
                    RETURN_IF_ERROR(reader.index_exists(INDEX_ID, suffix, &exists));
                    if (!exists) {
                        return Status::Corruption("children segment {} is missing text field {}",
                                                  segment_id, synthetic_field_path(false, field));
                    }
                    snii::reader::LogicalIndexReader string_index;
                    RETURN_IF_ERROR(reader.open_index(INDEX_ID, suffix, &string_index));
                    if (string_index.stats().doc_count != segment_rows) {
                        return Status::Corruption(
                                "children segment {} text field {} doc_count={}, rows={}",
                                segment_id, synthetic_field_path(false, field),
                                string_index.stats().doc_count, segment_rows);
                    }
                    RETURN_IF_ERROR(
                            accumulate_text_index(string_index, suffix, &result->fingerprint));
                }
                const std::string array_suffix = variant_index_suffix(ARRAY_QUERY_RELATIVE_PATH);
                bool array_exists = false;
                RETURN_IF_ERROR(reader.index_exists(INDEX_ID, array_suffix, &array_exists));
                if (!array_exists) {
                    return Status::Corruption("children segment {} is missing array field {}",
                                              segment_id, ARRAY_QUERY_RELATIVE_PATH);
                }
                snii::reader::LogicalIndexReader array_index;
                RETURN_IF_ERROR(reader.open_index(INDEX_ID, array_suffix, &array_index));
                if (array_index.stats().doc_count != segment_rows) {
                    return Status::Corruption(
                            "children segment {} array field {} doc_count={}, rows={}", segment_id,
                            ARRAY_QUERY_RELATIVE_PATH, array_index.stats().doc_count, segment_rows);
                }
                RETURN_IF_ERROR(
                        accumulate_text_index(array_index, array_suffix, &result->fingerprint));
            }
            const uint64_t rowset_total_bytes = rowset_data_bytes + rowset_index_bytes;
            if (rowset_data_bytes != rowset->data_disk_size() ||
                rowset_index_bytes != rowset->index_disk_size() ||
                rowset_total_bytes != rowset->total_disk_size()) {
                return Status::Corruption(
                        "{} physical/meta byte mismatch: data={}/{}, index={}/{}, total={}/{}",
                        layout_name(_layout), rowset_data_bytes, rowset->data_disk_size(),
                        rowset_index_bytes, rowset->index_disk_size(), rowset_total_bytes,
                        rowset->total_disk_size());
            }
            result->physical_data_bytes += rowset_data_bytes;
            result->physical_index_bytes += rowset_index_bytes;
            result->physical_total_bytes += rowset_total_bytes;
        }
        return Status::OK();
    }

    Status validate_fingerprint(const DataFingerprint& fingerprint_result) const {
        const uint64_t expected = synthetic_data().rows;
        const uint64_t expected_integer_values = expected * PRESENT_INTEGER_FIELDS;
        const uint64_t expected_string_values = expected * PRESENT_TEXT_FIELDS;
        if (fingerprint_result.rows != expected ||
            fingerprint_result.int_values != expected_integer_values ||
            fingerprint_result.string_values != expected_string_values ||
            fingerprint_result.array_rows != expected) {
            return Status::Corruption(
                    "typed Variant fingerprint coverage is rows={}, int={}, string={}, arrays={}; "
                    "expected {}, {}, {}, {}",
                    fingerprint_result.rows, fingerprint_result.int_values,
                    fingerprint_result.string_values, fingerprint_result.array_rows, expected,
                    expected_integer_values, expected_string_values, expected);
        }
        return Status::OK();
    }

    Status validate_index_stats(const PhysicalIndexStats& stats) const {
        if (stats.segments == 0) {
            return Status::Corruption("rowsets contain no segments");
        }
        if (!has_inverted_index(_layout)) {
            if (stats.logical_indexes != 0 || stats.physical_index_bytes != 0 ||
                !(stats.fingerprint == IndexFingerprint {})) {
                return Status::Corruption(
                        "no-index layout has logical_indexes={}, physical_index_bytes={} or a "
                        "non-empty index fingerprint",
                        stats.logical_indexes, stats.physical_index_bytes);
            }
            return Status::OK();
        }
        if (is_single_root_layout(_layout) && stats.logical_indexes != stats.segments) {
            return Status::Corruption("{} layout has {} logical indexes across {} segments",
                                      layout_name(_layout), stats.logical_indexes, stats.segments);
        }
        if (_layout == IndexLayout::CHILDREN &&
            stats.logical_indexes != static_cast<uint64_t>(stats.segments) * TOTAL_PATHS) {
            return Status::Corruption(
                    "children layout has {} logical indexes across {} segments, expected={}",
                    stats.logical_indexes, stats.segments,
                    static_cast<uint64_t>(stats.segments) * TOTAL_PATHS);
        }
        const uint64_t expected_document_rows =
                static_cast<uint64_t>(synthetic_data().rows) *
                (is_single_root_layout(_layout) ? 1 : TEXT_FIELDS + 1);
        if (stats.fingerprint.document_rows != expected_document_rows ||
            stats.fingerprint.term_document_frequency == 0) {
            return Status::Corruption(
                    "index fingerprint has document_rows={}, term_df={}, expected_rows={}",
                    stats.fingerprint.document_rows, stats.fingerprint.term_document_frequency,
                    expected_document_rows);
        }
        if (_layout == IndexLayout::CHILDREN &&
            (stats.fingerprint.integer_documents == 0 || stats.fingerprint.integer_points == 0)) {
            return Status::Corruption("children integer fingerprint has documents={}, points={}",
                                      stats.fingerprint.integer_documents,
                                      stats.fingerprint.integer_points);
        }
        return Status::OK();
    }

    IndexLayout _layout;
    TextMode _text_mode;
    uint64_t _fixture_id;
    uint64_t _raw_json_bytes = 0;
    std::filesystem::path _directory;
    std::filesystem::path _tmp_directory;
    bool _previous_ordered_compaction;
    bool _previous_compaction_checksum;
    bool _previous_vertical_compaction;
    bool _previous_vertical_variant_compaction;
    bool _previous_index_compaction;
    bool _runtime_installed = false;
    std::unique_ptr<BaseStorageEngine> _previous_storage_engine;
    std::unique_ptr<segment_v2::TmpFileDirs> _previous_tmp_file_dirs;
    StorageEngine* _engine = nullptr;
    std::unique_ptr<DataDir> _data_dir;
    TabletSchemaSPtr _schema;
    TabletSharedPtr _tablet;
    std::vector<RowsetSharedPtr> _input_rowsets;
    std::unique_ptr<FullCompaction> _compaction;
};

bool benchmark_status(benchmark::State& state, const Status& status) {
    if (status.ok()) {
        return true;
    }
    const std::string message = status.to_string();
    state.SkipWithError(message);
    return false;
}

void add_u64_counters(benchmark::State& state, std::string_view name, uint64_t value) {
    state.counters[std::string(name) + "_hi32"] = static_cast<double>(value >> 32);
    state.counters[std::string(name) + "_lo32"] =
            static_cast<double>(value & std::numeric_limits<uint32_t>::max());
}

void add_common_counters(benchmark::State& state, IndexLayout layout, TextMode text_mode,
                         const BenchmarkResult& result) {
    const SyntheticData& data = synthetic_data();
    state.counters["rows_per_run"] =
            benchmark::Counter(data.rows, benchmark::Counter::kIsIterationInvariant);
    state.counters["candidate_fields"] = TOTAL_FIELDS;
    state.counters["candidate_paths"] = TOTAL_PATHS;
    state.counters["fields_per_row"] = PRESENT_INTEGER_FIELDS + PRESENT_TEXT_FIELDS;
    state.counters["integer_fields"] = INTEGER_FIELDS;
    state.counters["text_fields"] = TEXT_FIELDS;
    state.counters["synthetic_seed"] = static_cast<double>(SYNTHETIC_SEED);
    state.counters["raw_json_bytes"] = result.raw_json_bytes;
    state.counters["input_data_bytes"] = result.input_data_bytes;
    state.counters["input_index_bytes"] = result.input_index_bytes;
    state.counters["input_total_bytes"] = result.input_total_bytes;
    state.counters["output_data_bytes"] = result.output_data_bytes;
    state.counters["output_index_bytes"] = result.output_index_bytes;
    state.counters["output_total_bytes"] = result.output_total_bytes;
    state.counters["input_physical_data_bytes"] = result.input_physical_data_bytes;
    state.counters["input_physical_index_bytes"] = result.input_physical_index_bytes;
    state.counters["input_physical_total_bytes"] = result.input_physical_total_bytes;
    state.counters["output_physical_data_bytes"] = result.output_physical_data_bytes;
    state.counters["output_physical_index_bytes"] = result.output_physical_index_bytes;
    state.counters["output_physical_total_bytes"] = result.output_physical_total_bytes;
    state.counters["input_segments"] = result.input_segments;
    state.counters["output_segments"] = result.output_segments;
    state.counters["input_logical_indexes"] = result.input_logical_indexes;
    state.counters["output_logical_indexes"] = result.output_logical_indexes;
    state.counters["input_index_term_df"] =
            static_cast<double>(result.input_index_fingerprint.term_document_frequency);
    state.counters["output_index_term_df"] =
            static_cast<double>(result.output_index_fingerprint.term_document_frequency);
    state.counters["input_integer_index_docs"] =
            static_cast<double>(result.input_index_fingerprint.integer_documents);
    state.counters["output_integer_index_docs"] =
            static_cast<double>(result.output_index_fingerprint.integer_documents);
    state.counters["root_layout"] = layout == IndexLayout::ROOT;
    state.counters["all_values_layout"] = layout == IndexLayout::ALL_VALUES;
    state.counters["children_layout"] = layout == IndexLayout::CHILDREN;
    state.counters["no_index_layout"] = layout == IndexLayout::NO_INDEX;
    state.counters["english_text_dataset"] = text_mode == TextMode::ENGLISH;
    state.counters["phrase_search_supported"] = 0;
    state.counters["lz4f_data_compression"] = 1;
    state.counters["all_101_variant_paths_validated"] = 1;
    state.counters["all_logical_indexes_validated"] = 1;
    state.counters["input_data_fingerprint_rows"] = result.input_data_fingerprint.rows;
    state.counters["output_data_fingerprint_rows"] = result.output_data_fingerprint.rows;
    state.counters["input_data_fingerprint_array_rows"] = result.input_data_fingerprint.array_rows;
    state.counters["output_data_fingerprint_array_rows"] =
            result.output_data_fingerprint.array_rows;
    add_u64_counters(state, "input_array_hash_sum", result.input_data_fingerprint.array_hash_sum);
    add_u64_counters(state, "input_array_hash_xor", result.input_data_fingerprint.array_hash_xor);
    add_u64_counters(state, "output_array_hash_sum", result.output_data_fingerprint.array_hash_sum);
    add_u64_counters(state, "output_array_hash_xor", result.output_data_fingerprint.array_hash_xor);
    add_u64_counters(state, "input_data_hash_sum", result.input_data_fingerprint.hash_sum);
    add_u64_counters(state, "input_data_hash_xor", result.input_data_fingerprint.hash_xor);
    add_u64_counters(state, "output_data_hash_sum", result.output_data_fingerprint.hash_sum);
    add_u64_counters(state, "output_data_hash_xor", result.output_data_fingerprint.hash_xor);
    if (result.input_total_bytes > 0) {
        state.counters["compaction_total_ratio"] =
                static_cast<double>(result.output_total_bytes) / result.input_total_bytes;
        state.counters["compaction_data_ratio"] =
                static_cast<double>(result.output_data_bytes) / result.input_data_bytes;
        state.counters["compaction_data_saved_bytes"] =
                static_cast<double>(result.input_data_bytes) - result.output_data_bytes;
        state.counters["compaction_saved_bytes"] =
                static_cast<double>(result.input_total_bytes) - result.output_total_bytes;
    }
    if (result.input_index_bytes > 0) {
        state.counters["compaction_index_ratio"] =
                static_cast<double>(result.output_index_bytes) / result.input_index_bytes;
        state.counters["compaction_index_saved_bytes"] =
                static_cast<double>(result.input_index_bytes) - result.output_index_bytes;
    }
    state.SetLabel("synthetic-gharchive-v2;101-paths;50-scalars-present;fixed-seed");
}

void BM_VariantRootHighLevelImport(benchmark::State& state, IndexLayout layout,
                                   TextMode text_mode) {
    BenchmarkResult result;
    bool completed = false;
    for (auto _ : state) {
        benchmark::DoNotOptimize(_);
        state.PauseTiming();
        auto fixture = std::make_unique<HighLevelVariantFixture>(layout, text_mode);
        Status status = fixture->setup();
        const bool setup_ok = benchmark_status(state, status);
        state.ResumeTiming();
        if (!setup_ok) {
            break;
        }

        RowsetSharedPtr rowset;
        status = fixture->write_import_rowset(&state, &rowset);

        state.PauseTiming();
        if (status.ok()) {
            status = fixture->validate_import(rowset, &result);
        }
        const bool ok = benchmark_status(state, status);
        fixture.reset();
        state.ResumeTiming();
        if (!ok) {
            break;
        }
        completed = true;
    }
    if (!completed) {
        return;
    }
    add_common_counters(state, layout, text_mode, result);
    state.counters["rowset_writer_path"] = 1;
    state.counters["post_import_readable"] = 1;
    state.SetItemsProcessed(static_cast<int64_t>(synthetic_data().rows) * state.iterations());
    state.SetBytesProcessed(static_cast<int64_t>(result.raw_json_bytes) * state.iterations());
}

void BM_VariantRootHighLevelFullCompaction(benchmark::State& state, IndexLayout layout,
                                           TextMode text_mode) {
    BenchmarkResult result;
    bool completed = false;
    for (auto _ : state) {
        benchmark::DoNotOptimize(_);
        state.PauseTiming();
        auto fixture = std::make_unique<HighLevelVariantFixture>(layout, text_mode);
        Status status = fixture->setup();
        DataFingerprint before;
        PhysicalIndexStats before_indexes;
        if (status.ok()) {
            status = fixture->prepare_compaction_inputs(&before, &before_indexes);
        }
        const bool setup_ok = benchmark_status(state, status);
        state.ResumeTiming();
        if (!setup_ok) {
            break;
        }

        status = fixture->compact(&result);

        state.PauseTiming();
        if (status.ok()) {
            status = fixture->validate_compaction(before, before_indexes, &result);
        }
        const bool ok = benchmark_status(state, status);
        fixture.reset();
        state.ResumeTiming();
        if (!ok) {
            break;
        }
        completed = true;
    }
    if (!completed) {
        return;
    }
    add_common_counters(state, layout, text_mode, result);
    state.counters["full_compaction_framework"] = 1;
    state.counters["pre_compaction_correct"] = 1;
    state.counters["post_compaction_correct"] = 1;
    state.counters["pre_post_fingerprint_equal"] = 1;
    state.counters["pre_post_index_fingerprint_equal"] = 1;
    constexpr double NS_PER_MS = 1'000'000.0;
    state.counters["compaction_prepare_ms"] = result.compaction_prepare_time_ns / NS_PER_MS;
    state.counters["compaction_execute_ms"] = result.compaction_execute_time_ns / NS_PER_MS;
    state.counters["merge_rowsets_ms"] = result.merge_rowsets_time_ns / NS_PER_MS;
    state.counters["merge_row_data_ms"] = result.merge_row_data_time_ns / NS_PER_MS;
    state.counters["inverted_index_compaction_ms"] =
            result.inverted_index_compaction_time_ns / NS_PER_MS;
    state.counters["build_output_rowset_ms"] = result.build_output_rowset_time_ns / NS_PER_MS;
    state.SetItemsProcessed(static_cast<int64_t>(synthetic_data().rows) * state.iterations());
    state.SetBytesProcessed(static_cast<int64_t>(result.input_total_bytes) * state.iterations());
}

Status validate_scan_metrics(QueryShape shape, const QueryResult& scan) {
    if (scan.inverted_index_downgrade_count != 0 || scan.rows_inverted_index_filtered != 0 ||
        scan.inverted_index_filter_timer != 0 || scan.inverted_index_query_timer != 0 ||
        scan.inverted_index_searcher_cache_hit != 0 ||
        scan.inverted_index_searcher_cache_miss != 0 ||
        scan.inverted_index_query_cache_lookup != 0 || scan.inverted_index_query_cache_hit != 0) {
        return Status::Corruption(
                "{} scan unexpectedly used index work: downgrade={}, filtered={}, filter_ns={}, "
                "query_ns={}, searcher_hit={}, searcher_miss={}, query_cache_lookup={}, "
                "query_cache_hit={}",
                query_shape_name(shape), scan.inverted_index_downgrade_count,
                scan.rows_inverted_index_filtered, scan.inverted_index_filter_timer,
                scan.inverted_index_query_timer, scan.inverted_index_searcher_cache_hit,
                scan.inverted_index_searcher_cache_miss, scan.inverted_index_query_cache_lookup,
                scan.inverted_index_query_cache_hit);
    }
    return Status::OK();
}

Status validate_query_oracle(IndexLayout layout, QueryShape shape, const QueryResult& primary,
                             const QueryResult& scalar_scan, bool require_warm_searcher) {
    if (!primary.same_output(scalar_scan)) {
        return Status::Corruption(
                "{} index ON/OFF results differ: rows={}/{}, hash_sum={}/{}, hash_xor={}/{}",
                query_shape_name(shape), primary.rows, scalar_scan.rows, primary.hash_sum,
                scalar_scan.hash_sum, primary.hash_xor, scalar_scan.hash_xor);
    }
    if (primary.rows == 0) {
        return Status::Corruption("{} produced no matching rows", query_shape_name(shape));
    }
    RETURN_IF_ERROR(validate_scan_metrics(shape, scalar_scan));
    if (!expects_inverted_index(layout, shape)) {
        RETURN_IF_ERROR(validate_scan_metrics(shape, primary));
        if (layout == IndexLayout::NO_INDEX && shape == QueryShape::WHOLE_ROOT_LIKE_SUBSTRING &&
            primary.raw_rows_read != synthetic_data().rows) {
            return Status::Corruption("whole-root no-index LIKE read {} raw rows, expected {}",
                                      primary.raw_rows_read, synthetic_data().rows);
        }
        return Status::OK();
    }
    if (primary.inverted_index_downgrade_count != 0) {
        return Status::Corruption("{} downgraded the inverted index {} times",
                                  query_shape_name(shape), primary.inverted_index_downgrade_count);
    }
    if (primary.rows_inverted_index_filtered <= 0) {
        return Status::Corruption("{} did not filter rows with the inverted index",
                                  query_shape_name(shape));
    }
    if (primary.inverted_index_query_cache_lookup != 0 ||
        primary.inverted_index_query_cache_hit != 0) {
        return Status::Corruption(
                "{} used the disabled query cache: lookups={}, hits={}", query_shape_name(shape),
                primary.inverted_index_query_cache_lookup, primary.inverted_index_query_cache_hit);
    }
    if (require_warm_searcher && expects_warm_searcher_cache(layout, shape) &&
        (primary.inverted_index_searcher_cache_hit <= 0 ||
         primary.inverted_index_searcher_cache_miss != 0)) {
        return Status::Corruption(
                "{} did not exclusively use the warmed searcher cache: hits={}, misses={}",
                query_shape_name(shape), primary.inverted_index_searcher_cache_hit,
                primary.inverted_index_searcher_cache_miss);
    }
    return Status::OK();
}

void add_query_counters(benchmark::State& state, IndexLayout layout, TextMode text_mode,
                        QueryShape shape, const BenchmarkResult& storage_result,
                        const QueryResult& result, const QueryResult& scalar_scan) {
    add_common_counters(state, layout, text_mode, storage_result);
    state.counters["query_result_rows"] = result.rows;
    state.counters["query_selectivity"] = static_cast<double>(result.rows) / synthetic_data().rows;
    add_u64_counters(state, "query_result_hash_sum", result.hash_sum);
    add_u64_counters(state, "query_result_hash_xor", result.hash_xor);
    state.counters["raw_rows_read"] = result.raw_rows_read;
    state.counters["rows_inverted_index_filtered"] = result.rows_inverted_index_filtered;
    state.counters["rows_vec_cond_filtered"] = result.rows_vec_cond_filtered;
    state.counters["rows_conditions_filtered"] = result.rows_conditions_filtered;
    state.counters["rows_expr_cond_filtered"] = result.rows_expr_cond_filtered;
    state.counters["expr_cond_input_rows"] = result.expr_cond_input_rows;
    state.counters["inverted_index_downgrade_count"] = result.inverted_index_downgrade_count;
    state.counters["inverted_index_filter_ns"] = result.inverted_index_filter_timer;
    state.counters["inverted_index_query_ns"] = result.inverted_index_query_timer;
    state.counters["inverted_index_searcher_cache_hit"] = result.inverted_index_searcher_cache_hit;
    state.counters["inverted_index_searcher_cache_miss"] =
            result.inverted_index_searcher_cache_miss;
    state.counters["inverted_index_query_cache_lookup"] = result.inverted_index_query_cache_lookup;
    state.counters["inverted_index_query_cache_hit"] = result.inverted_index_query_cache_hit;
    state.counters["scalar_scan_raw_rows_read"] = scalar_scan.raw_rows_read;
    state.counters["index_on_off_oracle_equal"] = 1;
    state.counters["index_filter_expected"] = expects_inverted_index(layout, shape);
    state.counters["scan_baseline"] = !expects_inverted_index(layout, shape);
    state.counters["candidate_recheck_expected"] =
            shape != QueryShape::WHOLE_ROOT_ENGLISH_MATCH_ANY;
    state.counters["whole_root_like_cast_scan"] = shape == QueryShape::WHOLE_ROOT_LIKE_SUBSTRING;
    state.counters["cross_path_amplification_stress"] =
            shape == QueryShape::PATH_ENGLISH_MATCH_ANY ||
            shape == QueryShape::PATH_ENGLISH_MATCH_ALL;
    state.counters["match_all_analyzed_terms"] =
            shape == QueryShape::PATH_ENGLISH_MATCH_ALL
                    ? analyzed_english_term_count(ENGLISH_MATCH_ALL_QUERY_VALUE)
                    : 0;
    state.counters["match_all_same_query_any_strict_superset"] =
            shape == QueryShape::PATH_ENGLISH_MATCH_ALL;
    state.counters["warm_os_cache"] = 1;
    state.counters["warm_searcher_cache"] = expects_warm_searcher_cache(layout, shape);
    state.counters["query_iterations_per_process"] = state.iterations();
    const std::string label =
            "warm-cache;production-rowset-reader;" + std::string(query_shape_name(shape));
    state.SetLabel(label);
}

void BM_VariantRootHighLevelQuery(benchmark::State& state, IndexLayout layout, TextMode text_mode,
                                  QueryShape shape) {
    auto fixture = std::make_unique<HighLevelVariantFixture>(layout, text_mode);
    Status status = fixture->setup();
    RowsetSharedPtr rowset;
    BenchmarkResult storage_result;
    QueryPlan indexed_plan;
    QueryPlan scalar_scan_plan;
    QueryResult indexed_oracle;
    QueryResult scalar_scan_oracle;
    QueryResult warmup;
    if (status.ok()) {
        status = fixture->prepare_query_rowset(&rowset);
    }
    if (status.ok()) {
        status = fixture->validate_import(rowset, &storage_result);
    }
    if (status.ok()) {
        status = fixture->prepare_query_plan(shape, true, &indexed_plan);
    }
    if (status.ok()) {
        status = fixture->prepare_query_plan(shape, false, &scalar_scan_plan);
    }
    if (status.ok()) {
        status = fixture->run_query(rowset, indexed_plan, &indexed_oracle);
    }
    if (status.ok()) {
        status = fixture->run_query(rowset, scalar_scan_plan, &scalar_scan_oracle);
    }
    if (status.ok()) {
        status = validate_query_oracle(layout, shape, indexed_oracle, scalar_scan_oracle, false);
    }
    if (status.ok() && shape == QueryShape::PATH_ENGLISH_MATCH_ALL) {
        QueryPlan any_plan;
        QueryPlan any_scan_plan;
        QueryResult any_result;
        QueryResult any_scan_result;
        const size_t analyzed_terms = analyzed_english_term_count(ENGLISH_MATCH_ALL_QUERY_VALUE);
        if (analyzed_terms != 2) {
            status = Status::Corruption("MATCH_ALL query analyzed to {} terms, expected 2",
                                        analyzed_terms);
        }
        if (status.ok()) {
            status = fixture->prepare_query_plan(QueryShape::PATH_ENGLISH_MATCH_ANY, true,
                                                 &any_plan, ENGLISH_MATCH_ALL_QUERY_VALUE);
        }
        if (status.ok()) {
            status = fixture->prepare_query_plan(QueryShape::PATH_ENGLISH_MATCH_ANY, false,
                                                 &any_scan_plan, ENGLISH_MATCH_ALL_QUERY_VALUE);
        }
        if (status.ok()) {
            status = fixture->run_query(rowset, any_plan, &any_result);
        }
        if (status.ok()) {
            status = fixture->run_query(rowset, any_scan_plan, &any_scan_result);
        }
        if (status.ok()) {
            status = validate_query_oracle(layout, QueryShape::PATH_ENGLISH_MATCH_ANY, any_result,
                                           any_scan_result, false);
        }
        if (status.ok() && any_result.rows <= indexed_oracle.rows) {
            status = Status::Corruption(
                    "same-query two-term MATCH_ANY must be a strict superset of MATCH_ALL: "
                    "any_rows={}, all_rows={}",
                    any_result.rows, indexed_oracle.rows);
        }
    }
    if (status.ok() && shape == QueryShape::WHOLE_ROOT_LIKE_SUBSTRING &&
        layout == IndexLayout::ALL_VALUES) {
        QueryPlan match_plan;
        QueryPlan match_scan_plan;
        QueryResult match_result;
        QueryResult match_scan_result;
        status = fixture->prepare_query_plan(QueryShape::WHOLE_ROOT_ENGLISH_MATCH_ANY, true,
                                             &match_plan);
        if (status.ok()) {
            status = fixture->prepare_query_plan(QueryShape::WHOLE_ROOT_ENGLISH_MATCH_ANY, false,
                                                 &match_scan_plan);
        }
        if (status.ok()) {
            status = fixture->run_query(rowset, match_plan, &match_result);
        }
        if (status.ok()) {
            status = fixture->run_query(rowset, match_scan_plan, &match_scan_result);
        }
        if (status.ok()) {
            status = validate_query_oracle(layout, QueryShape::WHOLE_ROOT_ENGLISH_MATCH_ANY,
                                           match_result, match_scan_result, false);
        }
        if (status.ok() && !match_result.same_output(indexed_oracle)) {
            status = Status::Corruption(
                    "synthetic-v2 whole-root MATCH and CAST(String) LIKE differ: rows={}/{}",
                    match_result.rows, indexed_oracle.rows);
        }
    }
    if (status.ok()) {
        status = fixture->run_query(rowset, indexed_plan, &warmup);
    }
    if (status.ok() && !warmup.same_output(indexed_oracle)) {
        status = Status::Corruption("{} warmup result differs from the validated oracle",
                                    query_shape_name(shape));
    }
    const bool setup_ok = benchmark_status(state, status);
    if (!setup_ok) {
        return;
    }

    QueryResult result;
    bool completed = false;
    for (auto _ : state) {
        benchmark::DoNotOptimize(_);
        status = fixture->run_query(rowset, indexed_plan, &result);
        if (!benchmark_status(state, status)) {
            completed = false;
            break;
        }
        benchmark::DoNotOptimize(result.hash_sum);
        benchmark::DoNotOptimize(result.hash_xor);
        completed = true;
    }

    if (completed) {
        status = validate_query_oracle(layout, shape, result, scalar_scan_oracle, true);
        if (status.ok() && !result.same_output(indexed_oracle)) {
            status = Status::Corruption("{} timed result differs from the setup oracle",
                                        query_shape_name(shape));
        }
        completed = benchmark_status(state, status);
    }
    if (completed) {
        add_query_counters(state, layout, text_mode, shape, storage_result, result,
                           scalar_scan_oracle);
        state.SetItemsProcessed(static_cast<int64_t>(synthetic_data().rows) * state.iterations());
    }
    fixture.reset();
}

BENCHMARK_CAPTURE(BM_VariantRootHighLevelImport, ExactRoot, IndexLayout::ROOT, TextMode::EXACT)
        ->Iterations(1)
        ->Unit(benchmark::kMillisecond)
        ->UseRealTime();
BENCHMARK_CAPTURE(BM_VariantRootHighLevelImport, ExactAllValues, IndexLayout::ALL_VALUES,
                  TextMode::EXACT)
        ->Iterations(1)
        ->Unit(benchmark::kMillisecond)
        ->UseRealTime();
BENCHMARK_CAPTURE(BM_VariantRootHighLevelImport, ExactChildren, IndexLayout::CHILDREN,
                  TextMode::EXACT)
        ->Iterations(1)
        ->Unit(benchmark::kMillisecond)
        ->UseRealTime();
BENCHMARK_CAPTURE(BM_VariantRootHighLevelImport, ExactNoIndex, IndexLayout::NO_INDEX,
                  TextMode::EXACT)
        ->Iterations(1)
        ->Unit(benchmark::kMillisecond)
        ->UseRealTime();
BENCHMARK_CAPTURE(BM_VariantRootHighLevelImport, EnglishRoot, IndexLayout::ROOT, TextMode::ENGLISH)
        ->Iterations(1)
        ->Unit(benchmark::kMillisecond)
        ->UseRealTime();
BENCHMARK_CAPTURE(BM_VariantRootHighLevelImport, EnglishAllValues, IndexLayout::ALL_VALUES,
                  TextMode::ENGLISH)
        ->Iterations(1)
        ->Unit(benchmark::kMillisecond)
        ->UseRealTime();
BENCHMARK_CAPTURE(BM_VariantRootHighLevelImport, EnglishChildren, IndexLayout::CHILDREN,
                  TextMode::ENGLISH)
        ->Iterations(1)
        ->Unit(benchmark::kMillisecond)
        ->UseRealTime();
BENCHMARK_CAPTURE(BM_VariantRootHighLevelImport, EnglishNoIndex, IndexLayout::NO_INDEX,
                  TextMode::ENGLISH)
        ->Iterations(1)
        ->Unit(benchmark::kMillisecond)
        ->UseRealTime();
BENCHMARK_CAPTURE(BM_VariantRootHighLevelFullCompaction, ExactRoot, IndexLayout::ROOT,
                  TextMode::EXACT)
        ->Iterations(1)
        ->Unit(benchmark::kMillisecond)
        ->UseRealTime();
BENCHMARK_CAPTURE(BM_VariantRootHighLevelFullCompaction, ExactAllValues, IndexLayout::ALL_VALUES,
                  TextMode::EXACT)
        ->Iterations(1)
        ->Unit(benchmark::kMillisecond)
        ->UseRealTime();
BENCHMARK_CAPTURE(BM_VariantRootHighLevelFullCompaction, ExactChildren, IndexLayout::CHILDREN,
                  TextMode::EXACT)
        ->Iterations(1)
        ->Unit(benchmark::kMillisecond)
        ->UseRealTime();
BENCHMARK_CAPTURE(BM_VariantRootHighLevelFullCompaction, ExactNoIndex, IndexLayout::NO_INDEX,
                  TextMode::EXACT)
        ->Iterations(1)
        ->Unit(benchmark::kMillisecond)
        ->UseRealTime();
BENCHMARK_CAPTURE(BM_VariantRootHighLevelFullCompaction, EnglishRoot, IndexLayout::ROOT,
                  TextMode::ENGLISH)
        ->Iterations(1)
        ->Unit(benchmark::kMillisecond)
        ->UseRealTime();
BENCHMARK_CAPTURE(BM_VariantRootHighLevelFullCompaction, EnglishAllValues, IndexLayout::ALL_VALUES,
                  TextMode::ENGLISH)
        ->Iterations(1)
        ->Unit(benchmark::kMillisecond)
        ->UseRealTime();
BENCHMARK_CAPTURE(BM_VariantRootHighLevelFullCompaction, EnglishChildren, IndexLayout::CHILDREN,
                  TextMode::ENGLISH)
        ->Iterations(1)
        ->Unit(benchmark::kMillisecond)
        ->UseRealTime();
BENCHMARK_CAPTURE(BM_VariantRootHighLevelFullCompaction, EnglishNoIndex, IndexLayout::NO_INDEX,
                  TextMode::ENGLISH)
        ->Iterations(1)
        ->Unit(benchmark::kMillisecond)
        ->UseRealTime();

#define REGISTER_VARIANT_QUERY(name, layout, mode, shape)                                  \
    BENCHMARK_CAPTURE(BM_VariantRootHighLevelQuery, name, layout, mode, QueryShape::shape) \
            ->Iterations(configured_query_iterations())                                    \
            ->Unit(benchmark::kMillisecond)                                                \
            ->UseRealTime()

REGISTER_VARIANT_QUERY(StringEqRoot, IndexLayout::ROOT, TextMode::EXACT, PATH_STRING_EQ);
REGISTER_VARIANT_QUERY(StringEqAllValues, IndexLayout::ALL_VALUES, TextMode::EXACT, PATH_STRING_EQ);
REGISTER_VARIANT_QUERY(StringEqChildren, IndexLayout::CHILDREN, TextMode::EXACT, PATH_STRING_EQ);
REGISTER_VARIANT_QUERY(StringEqNoIndex, IndexLayout::NO_INDEX, TextMode::EXACT, PATH_STRING_EQ);
REGISTER_VARIANT_QUERY(StringInRoot, IndexLayout::ROOT, TextMode::EXACT, PATH_STRING_IN);
REGISTER_VARIANT_QUERY(StringInAllValues, IndexLayout::ALL_VALUES, TextMode::EXACT, PATH_STRING_IN);
REGISTER_VARIANT_QUERY(StringInChildren, IndexLayout::CHILDREN, TextMode::EXACT, PATH_STRING_IN);
REGISTER_VARIANT_QUERY(StringInNoIndex, IndexLayout::NO_INDEX, TextMode::EXACT, PATH_STRING_IN);
REGISTER_VARIANT_QUERY(NumericEqRoot, IndexLayout::ROOT, TextMode::EXACT, PATH_NUMERIC_EQ);
REGISTER_VARIANT_QUERY(NumericEqAllValues, IndexLayout::ALL_VALUES, TextMode::EXACT,
                       PATH_NUMERIC_EQ);
REGISTER_VARIANT_QUERY(NumericEqChildren, IndexLayout::CHILDREN, TextMode::EXACT, PATH_NUMERIC_EQ);
REGISTER_VARIANT_QUERY(NumericEqNoIndex, IndexLayout::NO_INDEX, TextMode::EXACT, PATH_NUMERIC_EQ);
REGISTER_VARIANT_QUERY(NumericInRoot, IndexLayout::ROOT, TextMode::EXACT, PATH_NUMERIC_IN);
REGISTER_VARIANT_QUERY(NumericInAllValues, IndexLayout::ALL_VALUES, TextMode::EXACT,
                       PATH_NUMERIC_IN);
REGISTER_VARIANT_QUERY(NumericInChildren, IndexLayout::CHILDREN, TextMode::EXACT, PATH_NUMERIC_IN);
REGISTER_VARIANT_QUERY(NumericInNoIndex, IndexLayout::NO_INDEX, TextMode::EXACT, PATH_NUMERIC_IN);
REGISTER_VARIANT_QUERY(MatchAnyRoot, IndexLayout::ROOT, TextMode::ENGLISH, PATH_ENGLISH_MATCH_ANY);
REGISTER_VARIANT_QUERY(MatchAnyAllValues, IndexLayout::ALL_VALUES, TextMode::ENGLISH,
                       PATH_ENGLISH_MATCH_ANY);
REGISTER_VARIANT_QUERY(MatchAnyChildren, IndexLayout::CHILDREN, TextMode::ENGLISH,
                       PATH_ENGLISH_MATCH_ANY);
REGISTER_VARIANT_QUERY(MatchAnyNoIndex, IndexLayout::NO_INDEX, TextMode::ENGLISH,
                       PATH_ENGLISH_MATCH_ANY);
REGISTER_VARIANT_QUERY(MatchAllRoot, IndexLayout::ROOT, TextMode::ENGLISH, PATH_ENGLISH_MATCH_ALL);
REGISTER_VARIANT_QUERY(MatchAllAllValues, IndexLayout::ALL_VALUES, TextMode::ENGLISH,
                       PATH_ENGLISH_MATCH_ALL);
REGISTER_VARIANT_QUERY(MatchAllChildren, IndexLayout::CHILDREN, TextMode::ENGLISH,
                       PATH_ENGLISH_MATCH_ALL);
REGISTER_VARIANT_QUERY(MatchAllNoIndex, IndexLayout::NO_INDEX, TextMode::ENGLISH,
                       PATH_ENGLISH_MATCH_ALL);
REGISTER_VARIANT_QUERY(PathLikeNoIndex, IndexLayout::NO_INDEX, TextMode::ENGLISH,
                       PATH_LIKE_SUBSTRING);
REGISTER_VARIANT_QUERY(PathStartsWithNoIndex, IndexLayout::NO_INDEX, TextMode::ENGLISH,
                       PATH_STARTS_WITH);
REGISTER_VARIANT_QUERY(ArrayContainsChildren, IndexLayout::CHILDREN, TextMode::EXACT,
                       PATH_ARRAY_CONTAINS);
REGISTER_VARIANT_QUERY(ArrayContainsNoIndex, IndexLayout::NO_INDEX, TextMode::EXACT,
                       PATH_ARRAY_CONTAINS);
REGISTER_VARIANT_QUERY(WholeRootMatchAllValues, IndexLayout::ALL_VALUES, TextMode::ENGLISH,
                       WHOLE_ROOT_ENGLISH_MATCH_ANY);
REGISTER_VARIANT_QUERY(WholeRootLikeAllValues, IndexLayout::ALL_VALUES, TextMode::ENGLISH,
                       WHOLE_ROOT_LIKE_SUBSTRING);
REGISTER_VARIANT_QUERY(WholeRootLikeNoIndex, IndexLayout::NO_INDEX, TextMode::ENGLISH,
                       WHOLE_ROOT_LIKE_SUBSTRING);

#undef REGISTER_VARIANT_QUERY

} // namespace
} // namespace doris::variant_root_benchmark
