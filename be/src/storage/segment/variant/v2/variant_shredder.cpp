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

#include "storage/segment/variant/v2/variant_shredder.h"

#include <algorithm>
#include <limits>
#include <numeric>
#include <utility>

#include "common/exception.h"
#include "core/assert_cast.h"
#include "core/column/column_map.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/typeid_cast.h"
#include "exec/common/variant_util.h"
#include "exprs/function/parse/variant_jsonb_parse.h"
#include "storage/tablet/tablet_schema.h"
#include "util/jsonb_writer.h"

namespace doris::segment_v2 {
namespace {

DataTypePtr normalize_integer_widths(const DataTypePtr& type) {
    const DataTypePtr base = remove_nullable(type);
    if (const auto* array = typeid_cast<const DataTypeArray*>(base.get())) {
        return std::make_shared<DataTypeArray>(normalize_integer_widths(array->get_nested_type()));
    }
    switch (base->get_primitive_type()) {
    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT:
    case TYPE_BIGINT:
        return std::make_shared<DataTypeInt64>();
    default:
        return base;
    }
}

bool contains_nothing(const DataTypePtr& type) {
    const DataTypePtr base = remove_nullable(type);
    if (base->get_primitive_type() == INVALID_TYPE) {
        return true;
    }
    if (const auto* array = typeid_cast<const DataTypeArray*>(base.get())) {
        return contains_nothing(array->get_nested_type());
    }
    return false;
}

PathInData normalize_doc_publication_path(const PathInData& path) {
    if (path.empty()) {
        return path;
    }
    for (const PathInData::Part& part : path.get_parts()) {
        if (part.is_nested || part.anonymous_array_level != 0) {
            return path;
        }
    }
    return PathInData(path.get_path(), path.get_is_typed());
}

} // namespace

struct VariantShredder::Impl {
    enum class State : uint8_t { COLLECTING, FINISHED, FAILED };

    struct SparsePlan {
        VariantPathBuilder* builder = nullptr;
        uint32_t bucket = 0;
        const std::string* path = nullptr;
        bool track_statistics = false;
    };

    struct DocPlan {
        VariantPathBuilder* builder = nullptr;
        uint32_t bucket = 0;
        const std::string* path = nullptr;
        size_t candidate_index = 0;
    };

    explicit Impl(VariantShredderOptions options_) : options(std::move(options_)) {
        if (options.physical_layout == VariantShredderPhysicalLayout::ORDINARY &&
            options.sparse_bucket_count == 0) {
            failure = Status::InvalidArgument(
                    "Variant shredder sparse bucket count must be positive");
            state = State::FAILED;
        } else if (options.physical_layout == VariantShredderPhysicalLayout::DOC &&
                   options.doc_bucket_count == 0) {
            failure = Status::InvalidArgument("Variant shredder doc bucket count must be positive");
            state = State::FAILED;
        } else if (options.tablet_schema != nullptr && options.parent_column_unique_id < 0) {
            failure = Status::InvalidArgument(
                    "Variant shredder tablet schema requires a parent column unique id");
            state = State::FAILED;
        }
    }

    Status require_collecting() const {
        if (state == State::FAILED) {
            return failure;
        }
        if (state == State::FINISHED) {
            return Status::InvalidArgument("Variant shredder is already finished");
        }
        return Status::OK();
    }

    Status fail(Status status) {
        if (state != State::FAILED) {
            failure = std::move(status);
            state = State::FAILED;
        }
        return failure;
    }

    PathInData logical_path(uint32_t path_id) const {
        const PathInData& discovered_path = path_plan.path(path_id);
        if (options.logical_root_path.empty()) {
            return discovered_path;
        }
        if (discovered_path.empty()) {
            return options.logical_root_path;
        }
        PathInDataBuilder builder;
        return builder.append(options.logical_root_path.get_parts(), false)
                .append(discovered_path.get_parts(), false)
                .build();
    }

    VariantPathBuilder* get_or_create_builder(uint32_t path_id) {
        if (builders.size() <= path_id) {
            builders.resize(path_id + 1);
        }
        if (!builders[path_id]) {
            builders[path_id] = std::make_unique<VariantPathBuilder>(logical_path(path_id), rows);
        }
        return builders[path_id].get();
    }

    Status validate_doc_path(uint32_t path_id) const {
        if (options.physical_layout != VariantShredderPhysicalLayout::DOC) {
            return Status::OK();
        }
        const auto& parts = path_plan.path(path_id).get_parts();
        if (parts.empty()) {
            return Status::Corruption("Variant doc path must not be empty");
        }
        return Status::OK();
    }

    Status append_leaf(VariantRef value, uint32_t path_id, size_t row) {
        if (last_path_rows.size() <= path_id) {
            last_path_rows.resize(path_id + 1, 0);
        }
        const size_t row_marker = row + 1;
        if (last_path_rows[path_id] == row_marker) {
            if (!options.check_duplicate_json_path) {
                return Status::InvalidArgument("may contains duplicated entry : {}",
                                               path_plan.path(path_id).get_path());
            }
            return Status::OK();
        }
        last_path_rows[path_id] = row_marker;
        if (value.is_null()) {
            return Status::OK();
        }
        return get_or_create_builder(path_id)->append(value, row);
    }

    Status visit(VariantRef value, uint32_t metadata_plan_id, uint32_t path_id, size_t row) {
        if (value.is_null()) {
            return options.check_duplicate_json_path ? append_leaf(value, path_id, row)
                                                     : Status::OK();
        }
        if (path_id == 0 && !options.logical_root_path.empty()) {
            return append_leaf(value, path_id, row);
        }
        if (value.basic_type() != VariantBasicType::OBJECT) {
            return append_leaf(value, path_id, row);
        }
        const uint32_t children = value.num_elements();
        for (uint32_t index = 0; index < children; ++index) {
            uint32_t field_id = 0;
            VariantRef child = value.object_value_at(index, &field_id);
            uint32_t child_path_id = 0;
            RETURN_IF_ERROR(
                    path_plan.resolve_child(path_id, metadata_plan_id, field_id, &child_path_id));
            RETURN_IF_ERROR(validate_doc_path(child_path_id));
            RETURN_IF_ERROR(visit(child, metadata_plan_id, child_path_id, row));
        }
        return Status::OK();
    }

    Status complete_builder_rows(size_t completed_rows) {
        for (const auto& builder : builders) {
            if (builder) {
                RETURN_IF_ERROR(builder->complete_rows(completed_rows));
            }
        }
        return Status::OK();
    }

    void append_default_root() { root_values->insert_default(); }

    Status append_root(VariantRef value) {
        // V1 reconstructs objects exclusively from shredded paths, so JSON-null and unresolved
        // leaves remain absent. Keep the root only for scalar and array values. The physical
        // writer applies SQL NULL through the column's ordinary nullable map, so the shredder
        // does not need a separate root-null state.
        if (value.is_null() || value.basic_type() == VariantBasicType::OBJECT) {
            append_default_root();
            return Status::OK();
        }
        variant_to_jsonb(value, root_writer);
        root_values->insert_data(root_writer.getOutput()->getBuffer(),
                                 root_writer.getOutput()->getSize());
        return Status::OK();
    }

    Status prepare_logical_candidates(DorisVector<VariantPathSelectionCandidate>* candidates,
                                      DorisVector<VariantPathBuilder*>* candidate_builders,
                                      DorisVector<DataTypePtr>* storage_types) {
        candidates->reserve(builders.size());
        candidate_builders->reserve(builders.size());
        storage_types->reserve(builders.size());
        for (const auto& builder_owner : builders) {
            if (!builder_owner) {
                continue;
            }
            VariantPathBuilder* builder = builder_owner.get();
            bool is_typed_path = false;
            DataTypePtr storage_type;
            if (options.tablet_schema != nullptr) {
                TabletSchema::SubColumnInfo info;
                is_typed_path = variant_util::generate_sub_column_info(
                        *options.tablet_schema, options.parent_column_unique_id,
                        builder->path().get_path(), &info);
                if (is_typed_path) {
                    storage_type = DataTypeFactory::instance().create_data_type(info.column);
                }
            }
            if (builder->non_null_rows() == 0) {
                if (!is_typed_path || options.typed_paths_to_sparse) {
                    continue;
                }
                RETURN_IF_ERROR(builder->convert_to(storage_type));
            } else {
                RETURN_IF_ERROR(builder->convert_to(normalize_integer_widths(builder->type())));
            }
            if (builder->non_null_rows() != 0 && contains_nothing(builder->type()) &&
                (storage_type == nullptr || contains_nothing(storage_type))) {
                continue;
            }
            candidate_builders->push_back(builder);
            candidates->push_back(VariantPathSelectionCandidate {.builder = builder,
                                                                 .is_typed_path = is_typed_path});
            storage_types->push_back(std::move(storage_type));
        }
        return Status::OK();
    }

    Status convert_typed_candidates(std::span<const size_t> selected,
                                    const DorisVector<VariantPathBuilder*>& candidate_builders,
                                    const DorisVector<DataTypePtr>& storage_types) const {
        for (size_t index : selected) {
            if (storage_types[index] != nullptr) {
                RETURN_IF_ERROR(candidate_builders[index]->convert_to(storage_types[index]));
            }
        }
        return Status::OK();
    }

    Status publish_root_and_materialized(
            const VariantPathSelection& selection,
            const DorisVector<VariantPathSelectionCandidate>& candidates,
            const DorisVector<VariantPathBuilder*>& candidate_builders,
            VariantShreddedColumns* result) {
        result->num_rows = rows;
        result->root_jsonb = std::move(root_values);
        result->materialized.reserve(selection.materialized.size());
        for (size_t selected : selection.materialized) {
            VariantPathBuilder& builder = *candidate_builders[selected];
            ColumnPtr materialized;
            RETURN_IF_ERROR(builder.materialize(&materialized));
            const PathInData& raw_path = builder.path();
            PathInData publication_path =
                    options.physical_layout == VariantShredderPhysicalLayout::DOC
                            ? normalize_doc_publication_path(raw_path)
                            : raw_path;
            result->materialized.push_back({.path = publication_path,
                                            .type = builder.type(),
                                            .column = std::move(materialized),
                                            .non_null_rows = builder.non_null_rows(),
                                            .is_typed_path = candidates[selected].is_typed_path});
        }
        return Status::OK();
    }

    DorisVector<SparsePlan> build_sparse_plan(
            const VariantPathSelection& selection,
            const DorisVector<VariantPathBuilder*>& candidate_builders) const {
        DorisVector<SparsePlan> sparse_plan;
        sparse_plan.reserve(selection.sparse.size());
        for (size_t selected : selection.sparse) {
            VariantPathBuilder* builder = candidate_builders[selected];
            const std::string& path = builder->path().get_path();
            sparse_plan.push_back({.builder = builder,
                                   .bucket = variant_util::variant_binary_shard_of(
                                           {path.data(), path.size()}, options.sparse_bucket_count),
                                   .path = &path,
                                   .track_statistics = false});
        }
        return sparse_plan;
    }

    void select_sparse_statistics(DorisVector<SparsePlan>* sparse_plan) const {
        if (options.max_sparse_column_statistics_size == 0) {
            return;
        }
        DorisVector<size_t> encounter_order(sparse_plan->size());
        std::iota(encounter_order.begin(), encounter_order.end(), 0);
        std::ranges::sort(encounter_order, [&](size_t left, size_t right) {
            const auto left_rowids = (*sparse_plan)[left].builder->rowids();
            const auto right_rowids = (*sparse_plan)[right].builder->rowids();
            DORIS_CHECK(!left_rowids.empty());
            DORIS_CHECK(!right_rowids.empty());
            if (left_rowids.front() != right_rowids.front()) {
                return left_rowids.front() < right_rowids.front();
            }
            // The plan is path-sorted, matching the old inner loop's tie-break at a row.
            return left < right;
        });

        DorisVector<size_t> tracked_paths_per_bucket(options.sparse_bucket_count, 0);
        for (size_t index : encounter_order) {
            SparsePlan& plan = (*sparse_plan)[index];
            if (tracked_paths_per_bucket[plan.bucket] < options.max_sparse_column_statistics_size) {
                plan.track_statistics = true;
                ++tracked_paths_per_bucket[plan.bucket];
            }
        }
    }

    template <typename BinaryPlan>
    Status append_binary_rows(const DorisVector<BinaryPlan>& binary_plan,
                              const DorisVector<ColumnMap*>& maps) const {
        struct BinaryCell {
            size_t plan_index = 0;
            size_t value_index = 0;
        };

        // Build a compact row index in two passes. Each path contributes only its present values,
        // and paths are visited in publication order so cells within one row preserve path order.
        DorisVector<size_t> row_offsets(rows + 1, 0);
        for (const BinaryPlan& plan : binary_plan) {
            for (uint32_t row : plan.builder->rowids()) {
                if (row >= rows) {
                    return Status::InternalError("Variant path {} row {} exceeds {} rows",
                                                 *plan.path, row, rows);
                }
                ++row_offsets[row + 1];
            }
        }
        std::partial_sum(row_offsets.begin(), row_offsets.end(), row_offsets.begin());
        DorisVector<BinaryCell> cells(row_offsets.back());
        DorisVector<size_t> next_cell = row_offsets;
        for (size_t plan_index = 0; plan_index < binary_plan.size(); ++plan_index) {
            const auto rowids = binary_plan[plan_index].builder->rowids();
            for (size_t value_index = 0; value_index < rowids.size(); ++value_index) {
                cells[next_cell[rowids[value_index]]++] = {.plan_index = plan_index,
                                                           .value_index = value_index};
            }
        }

        for (size_t row = 0; row < rows; ++row) {
            for (size_t cell_index = row_offsets[row]; cell_index < row_offsets[row + 1];
                 ++cell_index) {
                const BinaryCell& cell = cells[cell_index];
                const BinaryPlan& plan = binary_plan[cell.plan_index];
                auto& keys = assert_cast<ColumnString&>(maps[plan.bucket]->get_keys());
                auto& values = assert_cast<ColumnString&>(maps[plan.bucket]->get_values());
                keys.insert_data(plan.path->data(), plan.path->size());
                RETURN_IF_ERROR(
                        plan.builder->write_sparse_cell(cell.value_index, &values.get_chars()));
                values.get_offsets().push_back(values.get_chars().size());
            }
            for (ColumnMap* map : maps) {
                map->get_offsets().push_back(map->get_keys().size());
            }
        }
        return Status::OK();
    }

    void publish_sparse_statistics(const DorisVector<SparsePlan>& sparse_plan,
                                   DorisVector<VariantStatistics>* bucket_statistics,
                                   VariantShreddedColumns* result) const {
        for (const SparsePlan& plan : sparse_plan) {
            if (!plan.track_statistics) {
                continue;
            }
            const uint32_t count = plan.builder->non_null_rows();
            (*bucket_statistics)[plan.bucket].sparse_column_non_null_size[*plan.path] = count;
            result->statistics.sparse_column_non_null_size[*plan.path] += count;
        }
    }

    Status publish_sparse(const VariantPathSelection& selection,
                          const DorisVector<VariantPathBuilder*>& candidate_builders,
                          VariantShreddedColumns* result) const {
        const DataTypePtr string_type = std::make_shared<DataTypeString>();
        result->sparse_type = std::make_shared<DataTypeMap>(string_type, string_type);
        DorisVector<MutableColumnPtr> sparse_owners;
        DorisVector<ColumnMap*> sparse_maps;
        sparse_owners.reserve(options.sparse_bucket_count);
        sparse_maps.reserve(options.sparse_bucket_count);
        result->sparse_buckets.reserve(options.sparse_bucket_count);
        for (uint32_t bucket = 0; bucket < options.sparse_bucket_count; ++bucket) {
            auto map = ColumnMap::create(ColumnString::create(), ColumnString::create(),
                                         ColumnArray::ColumnOffsets::create());
            sparse_maps.push_back(map.get());
            sparse_owners.emplace_back(std::move(map));
        }

        DorisVector<SparsePlan> sparse_plan = build_sparse_plan(selection, candidate_builders);
        select_sparse_statistics(&sparse_plan);
        RETURN_IF_ERROR(append_binary_rows(sparse_plan, sparse_maps));
        DorisVector<VariantStatistics> bucket_statistics(options.sparse_bucket_count);
        publish_sparse_statistics(sparse_plan, &bucket_statistics, result);
        for (uint32_t bucket = 0; bucket < options.sparse_bucket_count; ++bucket) {
            result->sparse_buckets.push_back({.column = std::move(sparse_owners[bucket]),
                                              .statistics = std::move(bucket_statistics[bucket])});
        }
        return Status::OK();
    }

    Status build_doc_plan(const DorisVector<VariantPathBuilder*>& candidate_builders,
                          DorisVector<DocPlan>& plan) const {
        plan.reserve(candidate_builders.size());
        for (size_t index = 0; index < candidate_builders.size(); ++index) {
            VariantPathBuilder* builder = candidate_builders[index];
            const PathInData publication_path = normalize_doc_publication_path(builder->path());
            if (publication_path.get_parts().empty()) {
                return Status::Corruption("Variant doc path must not be empty");
            }
            for (const PathInData::Part& part : publication_path.get_parts()) {
                if (part.key.find('.') != std::string_view::npos) {
                    return Status::Corruption("Variant doc path has an ambiguous dotted part");
                }
            }
            const std::string& path = builder->path().get_path();
            plan.push_back({.builder = builder,
                            .bucket = variant_util::variant_binary_shard_of(
                                    {path.data(), path.size()}, options.doc_bucket_count),
                            .path = &path,
                            .candidate_index = index});
        }
        std::ranges::sort(plan, [](const DocPlan& left, const DocPlan& right) {
            return *left.path < *right.path;
        });
        for (size_t index = 1; index < plan.size(); ++index) {
            if (*plan[index - 1].path == *plan[index].path) {
                return Status::Corruption("Variant structured doc paths collide at {}",
                                          *plan[index].path);
            }
        }
        return Status::OK();
    }

    Status publish_doc(const DorisVector<DocPlan>& plan, VariantShreddedColumns* result) const {
        const DataTypePtr string_type = std::make_shared<DataTypeString>();
        result->doc_type = std::make_shared<DataTypeMap>(string_type, string_type);
        DorisVector<MutableColumnPtr> owners;
        DorisVector<ColumnMap*> maps;
        owners.reserve(options.doc_bucket_count);
        maps.reserve(options.doc_bucket_count);
        for (uint32_t bucket = 0; bucket < options.doc_bucket_count; ++bucket) {
            auto map = ColumnMap::create(ColumnString::create(), ColumnString::create(),
                                         ColumnArray::ColumnOffsets::create());
            maps.push_back(map.get());
            owners.emplace_back(std::move(map));
        }
        RETURN_IF_ERROR(append_binary_rows(plan, maps));

        DorisVector<VariantStatistics> statistics(options.doc_bucket_count);
        for (const DocPlan& entry : plan) {
            const uint32_t count = entry.builder->non_null_rows();
            statistics[entry.bucket].doc_value_column_non_null_size[*entry.path] = count;
            result->statistics.doc_value_column_non_null_size[*entry.path] += count;
        }
        result->doc_buckets.reserve(options.doc_bucket_count);
        for (uint32_t bucket = 0; bucket < options.doc_bucket_count; ++bucket) {
            result->doc_buckets.push_back({.column = std::move(owners[bucket]),
                                           .statistics = std::move(statistics[bucket])});
        }
        return Status::OK();
    }

    void publish_debug(VariantShreddedColumns* result) const {
        result->debug.metadata_plans = path_plan.metadata_plan_count();
        result->debug.path_plans = path_plan.path_plan_count();
        for (const auto& builder : builders) {
            if (builder) {
                result->debug.promotions += builder->promotion_count();
            }
        }
    }

    Status finish_ordinary(const DorisVector<VariantPathSelectionCandidate>& candidates,
                           const DorisVector<VariantPathBuilder*>& candidate_builders,
                           const DorisVector<DataTypePtr>& storage_types,
                           VariantShreddedColumns* result) {
        DorisVector<size_t> all(candidate_builders.size());
        std::iota(all.begin(), all.end(), 0);
        RETURN_IF_ERROR(convert_typed_candidates(all, candidate_builders, storage_types));
        const VariantPathSelection selection = select_variant_paths(
                candidates, options.max_subcolumns_count, options.typed_paths_to_sparse);
        RETURN_IF_ERROR(
                publish_root_and_materialized(selection, candidates, candidate_builders, result));
        RETURN_IF_ERROR(publish_sparse(selection, candidate_builders, result));
        return Status::OK();
    }

    Status finish_doc(const DorisVector<VariantPathSelectionCandidate>& candidates,
                      const DorisVector<VariantPathBuilder*>& candidate_builders,
                      const DorisVector<DataTypePtr>& storage_types,
                      VariantShreddedColumns* result) {
        DorisVector<DocPlan> doc_plan;
        RETURN_IF_ERROR(build_doc_plan(candidate_builders, doc_plan));
        RETURN_IF_ERROR(publish_doc(doc_plan, result));

        VariantPathSelection selection;
        if (rows >= options.doc_materialization_min_rows) {
            selection.materialized.reserve(doc_plan.size());
            for (const DocPlan& entry : doc_plan) {
                selection.materialized.push_back(entry.candidate_index);
            }
            RETURN_IF_ERROR(convert_typed_candidates(selection.materialized, candidate_builders,
                                                     storage_types));
        }
        return publish_root_and_materialized(selection, candidates, candidate_builders, result);
    }

    Status finish_impl(VariantShreddedColumns* result) {
        RETURN_IF_ERROR(complete_builder_rows(rows));
        DorisVector<VariantPathSelectionCandidate> candidates;
        DorisVector<VariantPathBuilder*> candidate_builders;
        DorisVector<DataTypePtr> storage_types;
        RETURN_IF_ERROR(
                prepare_logical_candidates(&candidates, &candidate_builders, &storage_types));
        if (options.physical_layout == VariantShredderPhysicalLayout::DOC) {
            RETURN_IF_ERROR(finish_doc(candidates, candidate_builders, storage_types, result));
        } else {
            RETURN_IF_ERROR(finish_ordinary(candidates, candidate_builders, storage_types, result));
        }
        publish_debug(result);
        return Status::OK();
    }

    VariantShredderOptions options;
    State state = State::COLLECTING;
    Status failure;
    size_t rows = 0;
    VariantMetadataPathPlan path_plan;
    DorisVector<std::unique_ptr<VariantPathBuilder>> builders;
    DorisVector<size_t> last_path_rows;
    ColumnString::MutablePtr root_values = ColumnString::create();
    JsonbWriter root_writer;
};

VariantShredder::VariantShredder(VariantShredderOptions options)
        : _impl(std::make_unique<Impl>(std::move(options))) {}

VariantShredder::~VariantShredder() = default;
VariantShredder::VariantShredder(VariantShredder&&) noexcept = default;
VariantShredder& VariantShredder::operator=(VariantShredder&&) noexcept = default;

Status VariantShredder::append(const ColumnVariantV2::ReadView& view, size_t begin, size_t length,
                               std::span<const uint8_t> outer_nulls) {
    RETURN_IF_ERROR(_impl->require_collecting());
    if (view.is_typed()) {
        return _impl->fail(Status::InvalidArgument(
                "Variant shredder requires encoded E-state input; caller must ensure_encoded"));
    }
    if (begin > view.size() || length > view.size() - begin) {
        return _impl->fail(
                Status::InvalidArgument("Variant shredder range [{}, {}) exceeds input size {}",
                                        begin, begin + length, view.size()));
    }
    if (!outer_nulls.empty() && outer_nulls.size() != length) {
        return _impl->fail(
                Status::InvalidArgument("Variant shredder outer-null span has {} rows, expected {}",
                                        outer_nulls.size(), length));
    }
    if (length > std::numeric_limits<size_t>::max() - _impl->rows) {
        return _impl->fail(Status::InvalidArgument("Variant shredder row count overflows size_t"));
    }
    try {
        DorisVector<uint32_t> metadata_plans(view.metadata_count());
        for (size_t metadata_id = 0; metadata_id < view.metadata_count(); ++metadata_id) {
            Status status = _impl->path_plan.intern_metadata(
                    view.metadata_at(static_cast<uint32_t>(metadata_id)),
                    &metadata_plans[metadata_id]);
            if (!status.ok()) {
                return _impl->fail(std::move(status));
            }
        }

        for (size_t offset = 0; offset < length; ++offset) {
            const bool outer_null = !outer_nulls.empty() && outer_nulls[offset] != 0;
            if (outer_null) {
                _impl->append_default_root();
                ++_impl->rows;
                continue;
            }
            const size_t input_row = begin + offset;
            const uint32_t metadata_id = view.metadata_id_at(input_row);
            if (metadata_id >= metadata_plans.size()) {
                return _impl->fail(
                        Status::Corruption("Variant row {} metadata id {} exceeds {} plans",
                                           input_row, metadata_id, metadata_plans.size()));
            }
            VariantRef value = view.value_at(input_row);
            Status status = _impl->append_root(value);
            if (!status.ok()) {
                return _impl->fail(std::move(status));
            }
            if (value.basic_type() == VariantBasicType::OBJECT) {
                status = _impl->visit(value, metadata_plans[metadata_id], 0, _impl->rows);
                if (!status.ok()) {
                    return _impl->fail(std::move(status));
                }
            } else if (!_impl->options.logical_root_path.empty()) {
                status = _impl->visit(value, metadata_plans[metadata_id], 0, _impl->rows);
                if (!status.ok()) {
                    return _impl->fail(std::move(status));
                }
            }
            ++_impl->rows;
        }
        return Status::OK();
    } catch (const Exception& exception) {
        return _impl->fail(exception.to_status());
    }
}

Status VariantShredder::finish(VariantShreddedColumns* output) {
    RETURN_IF_ERROR(_impl->require_collecting());
    if (output == nullptr) {
        return _impl->fail(Status::InvalidArgument("Variant shredder output must not be null"));
    }

    try {
        VariantShreddedColumns result;
        Status status = _impl->finish_impl(&result);
        if (!status.ok()) {
            return _impl->fail(std::move(status));
        }
        *output = std::move(result);
        _impl->state = Impl::State::FINISHED;
        return Status::OK();
    } catch (const Exception& exception) {
        return _impl->fail(exception.to_status());
    }
}

size_t VariantShredder::byte_size() const {
    size_t size = sizeof(Impl) + _impl->path_plan.byte_size();
    size += _impl->builders.capacity() * sizeof(std::unique_ptr<VariantPathBuilder>);
    for (const auto& builder : _impl->builders) {
        if (builder != nullptr) {
            size += builder->byte_size();
        }
    }
    size += _impl->last_path_rows.capacity() * sizeof(size_t);
    if (_impl->root_values) {
        size += _impl->root_values->allocated_bytes();
    }
    size += sizeof(JsonbOutStream) + _impl->root_writer.getOutput()->allocated_bytes();
    return size;
}

} // namespace doris::segment_v2
