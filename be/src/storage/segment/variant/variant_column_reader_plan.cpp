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

#include <fmt/format.h>
#include <gen_cpp/segment_v2.pb.h>

#include <algorithm>
#include <memory>
#include <ranges>
#include <roaring/roaring.hh>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "core/assert_cast.h"
#include "core/column/column_array.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_variant.h"
#include "exec/common/variant_util.h"
#include "io/fs/file_reader.h"
#include "runtime/descriptors.h"
#include "storage/key_coder.h"
#include "storage/olap_common.h"
#include "storage/segment/column_meta_accessor.h"
#include "storage/segment/column_reader.h"
#include "storage/segment/column_reader_cache.h"
#include "storage/segment/segment.h"
#include "storage/segment/variant/binary_column_extract_iterator.h"
#include "storage/segment/variant/binary_column_reader.h"
#include "storage/segment/variant/hierarchical_data_iterator.h"
#include "storage/segment/variant/nested_group_path.h"
#include "storage/segment/variant/sparse_column_merge_iterator.h"
#include "storage/segment/variant/variant_column_reader.h"
#include "storage/segment/variant/variant_doc_snpashot_compact_iterator.h"
#include "storage/tablet/tablet_schema.h"
#include "util/debug_points.h"
#include "util/json/path_in_data.h"
#include "util/string_util.h"

namespace doris::segment_v2 {

namespace {

MutableColumnPtr create_binary_variant_map_column() {
    return ColumnMap::create(ColumnString::create(), ColumnString::create(),
                             ColumnArray::ColumnOffsets::create());
}

bool is_compaction_or_checksum_reader(const StorageReadOptions* opts) {
    return opts != nullptr && (ColumnReader::is_compaction_reader_type(opts->io_ctx.reader_type) ||
                               opts->io_ctx.reader_type == ReaderType::READER_CHECKSUM);
}

// Nested-group whole/root-merge iterators dereference NestedGroupReader state that is owned by
// VariantColumnReader. Hold the owning reader until the iterator itself is destroyed so query-time
// iterator initialization cannot outlive the reader and hit a UAF.
class ReaderOwnedColumnIterator final : public ColumnIterator {
public:
    ReaderOwnedColumnIterator(ColumnIteratorUPtr inner, std::shared_ptr<ColumnReader> owner)
            : _inner(std::move(inner)), _owner(std::move(owner)) {
        DCHECK(_inner != nullptr);
        set_column_name(_inner->column_name());
        set_read_requirement(_inner->read_requirement());
    }

    Status init(const ColumnIteratorOptions& opts) override { return _inner->init(opts); }

    Status seek_to_ordinal(ordinal_t ord) override { return _inner->seek_to_ordinal(ord); }

    Status next_batch(size_t* n, MutableColumnPtr& dst, bool* has_null) override {
        return _inner->next_batch(n, dst, has_null);
    }

    Status next_batch_of_zone_map(size_t* n, MutableColumnPtr& dst) override {
        return _inner->next_batch_of_zone_map(n, dst);
    }

    Status read_by_rowids(const rowid_t* rowids, const size_t count,
                          MutableColumnPtr& dst) override {
        return _inner->read_by_rowids(rowids, count, dst);
    }

    ordinal_t get_current_ordinal() const override { return _inner->get_current_ordinal(); }

    Status get_row_ranges_by_zone_map(
            const AndBlockColumnPredicate* col_predicates,
            const std::vector<std::shared_ptr<const ColumnPredicate>>* delete_predicates,
            RowRanges* row_ranges) override {
        return _inner->get_row_ranges_by_zone_map(col_predicates, delete_predicates, row_ranges);
    }

    Status get_row_ranges_by_bloom_filter(const AndBlockColumnPredicate* col_predicates,
                                          RowRanges* row_ranges) override {
        return _inner->get_row_ranges_by_bloom_filter(col_predicates, row_ranges);
    }

    Status get_row_ranges_by_dict(const AndBlockColumnPredicate* col_predicates,
                                  RowRanges* row_ranges) override {
        return _inner->get_row_ranges_by_dict(col_predicates, row_ranges);
    }

    bool is_all_dict_encoding() const override { return _inner->is_all_dict_encoding(); }

    Status set_access_paths(const TColumnAccessPaths& all_access_paths,
                            const TColumnAccessPaths& predicate_access_paths) override {
        RETURN_IF_ERROR(_inner->set_access_paths(all_access_paths, predicate_access_paths));
        ColumnIterator::set_read_requirement_self(_inner->read_requirement());
        return Status::OK();
    }

    void set_read_requirement(ReadRequirement requirement) override {
        _inner->set_read_requirement(requirement);
        ColumnIterator::set_read_requirement_self(_inner->read_requirement());
    }

    void set_read_requirement_self(ReadRequirement requirement) override {
        _inner->set_read_requirement_self(requirement);
        ColumnIterator::set_read_requirement_self(_inner->read_requirement());
    }

    void set_lazy_output_requirement() override {
        _inner->set_lazy_output_requirement();
        ColumnIterator::set_read_requirement_self(_inner->read_requirement());
    }

    void set_read_phase(ReadPhase mode) override {
        ColumnIterator::set_read_phase(mode);
        _inner->set_read_phase(mode);
    }

    void finalize_lazy_phase(MutableColumnPtr& dst) override { _inner->finalize_lazy_phase(dst); }

    bool has_lazy_read_target() const override { return _inner->has_lazy_read_target(); }

    bool need_to_read() const override { return _inner->need_to_read(); }

    void remove_pruned_sub_iterators() override { _inner->remove_pruned_sub_iterators(); }

    Status init_prefetcher(const SegmentPrefetchParams& params) override {
        return _inner->init_prefetcher(params);
    }

    void collect_prefetchers(
            std::map<PrefetcherInitMethod, std::vector<SegmentPrefetcher*>>& prefetchers,
            PrefetcherInitMethod init_method) override {
        _inner->collect_prefetchers(prefetchers, init_method);
    }

private:
    ColumnIteratorUPtr _inner;
    std::shared_ptr<ColumnReader> _owner;
};

} // namespace

Status VariantColumnReader::_create_hierarchical_reader(
        ColumnIteratorUPtr* reader, int32_t col_uid, PathInData path,
        const SubcolumnColumnMetaInfo::Node* node, const SubcolumnColumnMetaInfo::Node* root,
        ColumnReaderCache* column_reader_cache, OlapReaderStatistics* stats,
        HierarchicalDataIterator::ReadType read_type, bool null_on_no_match) {
    // make sure external meta is loaded otherwise can't find any meta data for extracted columns
    // TODO(lhy): this will load all external meta if not loaded, and memory will be consumed.
    RETURN_IF_ERROR(load_external_meta_once());

    stats->variant_subtree_hierarchical_iter_count++;
    // After external meta is loaded, protect reads from `_statistics` and
    // `_subcolumns_meta_info` against concurrent writers.
    std::shared_lock<std::shared_mutex> lock(_subcolumns_meta_mutex);

    // Node contains column with children columns or has correspoding sparse columns
    // Create reader with hirachical data.
    std::unique_ptr<SubstreamIterator> sparse_iter;
    ColumnIteratorUPtr iter;
    // if read from subcolumns, but the binary column reader is multiple doc value,
    // use dummy binary column reader to insert default values to binary column.
    if (read_type == HierarchicalDataIterator::ReadType::SUBCOLUMNS_AND_SPARSE &&
        _binary_column_reader->get_type() == BinaryColumnType::MULTIPLE_DOC_VALUE) {
        DummyBinaryColumnReader dummy_binary_column_reader;
        RETURN_IF_ERROR(dummy_binary_column_reader.new_binary_column_iterator(&iter));
    } else {
        RETURN_IF_ERROR(_binary_column_reader->new_binary_column_iterator(&iter));
    }

    sparse_iter = std::make_unique<SubstreamIterator>(create_binary_variant_map_column(),
                                                      std::move(iter), nullptr);
    if (node == nullptr) {
        node = _subcolumns_meta_info->find_exact(path);
    }
    // Make sure the root node is in strem_cache, so that child can merge data with root
    // Eg. {"a" : "b" : {"c" : 1}}, access the `a.b` path and merge with root path so that
    // we could make sure the data could be fully merged, since some column may not be extracted but remains in root
    // like {"a" : "b" : {"e" : 1.1}} in jsonb format
    std::unique_ptr<SubstreamIterator> root_column_reader;
    if (path == root->path) {
        root_column_reader = std::make_unique<SubstreamIterator>(
                root->data.file_column_type->create_column(),
                std::make_unique<FileColumnIterator>(_root_column_reader),
                root->data.file_column_type);
    }
    RETURN_IF_ERROR(HierarchicalDataIterator::create(
            reader, col_uid, path, node, std::move(sparse_iter), std::move(root_column_reader),
            column_reader_cache, stats, read_type, null_on_no_match));
    return Status::OK();
}

Status VariantColumnReader::_create_sparse_merge_reader(ColumnIteratorUPtr* iterator,
                                                        const StorageReadOptions* opts,
                                                        const TabletColumn& target_col,
                                                        BinaryColumnCacheSPtr sparse_column_cache,
                                                        ColumnReaderCache* column_reader_cache,
                                                        std::optional<uint32_t> bucket_index) {
    std::shared_lock<std::shared_mutex> lock(_subcolumns_meta_mutex);
    // Get subcolumns path set from tablet schema
    const auto& path_set_info = opts->tablet_schema->path_set_info(target_col.parent_unique_id());

    // Build substream reader tree for merging subcolumns into sparse column
    SubstreamReaderTree src_subcolumns_for_sparse;
    for (const auto& subcolumn_reader : *_subcolumns_meta_info) {
        // NOTE: Skip the root node (empty parts). Do NOT skip "empty key" subcolumns where
        // path.get_path() may also be "" but parts are not empty. Otherwise v[''] data will be lost.
        if (subcolumn_reader->path.empty()) {
            continue;
        }
        const auto& path = subcolumn_reader->path.get_path();
        if (path_set_info.sparse_path_set.find(StringRef(path)) ==
            path_set_info.sparse_path_set.end()) {
            // The subcolumn is not a sparse column, skip it
            continue;
        }
        // If bucketized sparse column is requested (per-bucket sparse output column),
        // only collect subcolumns that belong to this bucket to avoid extra IO.
        if (bucket_index.has_value()) {
            CHECK(_binary_column_reader->get_type() == BinaryColumnType::MULTIPLE_SPARSE);
            uint32_t N = _binary_column_reader->num_buckets();
            if (N > 1) {
                uint32_t b = variant_util::variant_binary_shard_of(
                        StringRef {path.data(), path.size()}, N);
                if (b != bucket_index.value()) {
                    continue; // prune subcolumns of other buckets early
                }
            }
        }
        // Create subcolumn iterator
        std::shared_ptr<ColumnReader> column_reader;
        RETURN_IF_ERROR(column_reader_cache->get_path_column_reader(
                target_col.parent_unique_id(), subcolumn_reader->path, &column_reader, opts->stats,
                subcolumn_reader.get()));
        ColumnIteratorUPtr it;
        RETURN_IF_ERROR(column_reader->new_iterator(&it, nullptr));
        // Create substream reader and add to tree
        SubstreamIterator reader(subcolumn_reader->data.file_column_type->create_column(),
                                 std::move(it), subcolumn_reader->data.file_column_type);
        if (!src_subcolumns_for_sparse.add(subcolumn_reader->path, std::move(reader))) {
            return Status::InternalError("Failed to add node path {}", path);
        }
    }
    VLOG_DEBUG << "subcolumns to merge " << src_subcolumns_for_sparse.size();
    // Create sparse column merge reader
    *iterator = std::make_unique<SparseColumnMergeIterator>(
            path_set_info, std::move(sparse_column_cache), std::move(src_subcolumns_for_sparse),
            opts);
    return Status::OK();
}

Status VariantColumnReader::_new_default_iter_with_same_nested(
        ColumnIteratorUPtr* iterator, const TabletColumn& tablet_column,
        const StorageReadOptions* opt, ColumnReaderCache* column_reader_cache) {
    std::shared_lock<std::shared_mutex> lock(_subcolumns_meta_mutex);
    auto relative_path = tablet_column.path_info_ptr()->copy_pop_front();
    // We find node that represents the same Nested type as path.
    const auto* parent = _subcolumns_meta_info->find_best_match(relative_path);
    VLOG_DEBUG << "find with path " << tablet_column.path_info_ptr()->get_path() << " parent "
               << (parent ? parent->path.get_path() : "nullptr") << ", type "
               << ", parent is nested " << (parent ? parent->is_nested() : false) << ", "
               << TabletColumn::get_string_by_field_type(tablet_column.type()) << ", relative_path "
               << relative_path.get_path();
    // find it's common parent with nested part
    // why not use parent->path->has_nested_part? because parent may not be a leaf node
    // none leaf node may not contain path info
    // Example:
    // {"payload" : {"commits" : [{"issue" : {"id" : 123, "email" : "a@b"}}]}}
    // nested node path          : payload.commits(NESTED)
    // tablet_column path_info   : payload.commits.issue.id(SCALAR)
    // parent path node          : payload.commits.issue(TUPLE)
    // leaf path_info            : payload.commits.issue.email(SCALAR)
    if (parent && SubcolumnColumnMetaInfo::find_parent(
                          parent, [](const auto& node) { return node.is_nested(); })) {
        /// Find any leaf of Nested subcolumn.
        const auto* leaf = SubcolumnColumnMetaInfo::find_leaf(
                parent, [](const auto& node) { return node.path.has_nested_part(); });
        assert(leaf);
        std::unique_ptr<ColumnIterator> sibling_iter;
        std::shared_ptr<ColumnReader> column_reader;
        RETURN_IF_ERROR(column_reader_cache->get_path_column_reader(
                tablet_column.parent_unique_id(), leaf->path, &column_reader, opt->stats, leaf));
        RETURN_IF_ERROR(column_reader->new_iterator(&sibling_iter, nullptr));
        *iterator = std::make_unique<DefaultNestedColumnIterator>(std::move(sibling_iter),
                                                                  leaf->data.file_column_type);
    } else {
        *iterator = std::make_unique<DefaultNestedColumnIterator>(nullptr, nullptr);
    }
    return Status::OK();
}

Result<BinaryColumnCacheSPtr> VariantColumnReader::_get_binary_column_cache(
        PathToBinaryColumnCache* binary_column_cache_ptr, const std::string& path,
        std::shared_ptr<ColumnReader> binary_column_reader) {
    if (!binary_column_cache_ptr || !binary_column_cache_ptr->contains(path)) {
        ColumnIteratorUPtr inner_iter;
        RETURN_IF_ERROR_RESULT(binary_column_reader->new_iterator(&inner_iter, nullptr));
        MutableColumnPtr binary_column = create_binary_variant_map_column();
        auto binary_column_cache = std::make_shared<BinaryColumnCache>(std::move(inner_iter),
                                                                       std::move(binary_column));
        // if binary_column_cache_ptr is nullptr, means the binary column cache is not used
        if (binary_column_cache_ptr) {
            binary_column_cache_ptr->emplace(path, binary_column_cache);
        }
        return binary_column_cache;
    }
    return binary_column_cache_ptr->at(path);
}

DataTypePtr create_variant_type(const TabletColumn& target_col) {
    return target_col.is_nullable()
                   ? make_nullable(std::make_shared<DataTypeVariant>(
                             target_col.variant_max_subcolumns_count(),
                             target_col.variant_enable_doc_mode()))
                   : std::make_shared<DataTypeVariant>(target_col.variant_max_subcolumns_count(),
                                                       target_col.variant_enable_doc_mode());
}

Status VariantColumnReader::_build_read_plan_flat_leaves(
        ReadPlan* plan, const TabletColumn& target_col, const StorageReadOptions* opts,
        ColumnReaderCache* column_reader_cache, PathToBinaryColumnCache* binary_column_cache_ptr) {
    // make sure external meta is loaded otherwise can't find any meta data for extracted columns
    // TODO(lhy): this will load all external meta if not loaded, and memory will be consumed.
    RETURN_IF_ERROR(load_external_meta_once());

    std::shared_lock<std::shared_mutex> lock(_subcolumns_meta_mutex);

    DCHECK(opts != nullptr);
    int32_t col_uid =
            target_col.unique_id() >= 0 ? target_col.unique_id() : target_col.parent_unique_id();
    auto relative_path = target_col.path_info_ptr()->copy_pop_front();
    const auto* node = (!relative_path.empty() && target_col.has_path_info())
                               ? _subcolumns_meta_info->find_leaf(relative_path)
                               : nullptr;

    if (!relative_path.empty() && _can_use_nested_group_read_path() &&
        _try_fill_nested_group_plan(plan, target_col, opts, col_uid, relative_path)) {
        return Status::OK();
    }

    // compaction need to read flat leaves nodes data to prevent from amplification
    if (!node) {
        // Handle sparse column reads in flat-leaf compaction.
        const std::string rel = relative_path.get_path();
        // Case 1: single sparse column path
        if (rel == SPARSE_COLUMN_PATH &&
            _binary_column_reader->get_type() == BinaryColumnType::SINGLE_SPARSE) {
            plan->kind = ReadKind::SPARSE_MERGE;
            plan->type = DataTypeFactory::instance().create_data_type(target_col);
            plan->relative_path = relative_path;
            plan->binary_column_reader = _binary_column_reader->select_reader(0);
            plan->binary_cache_key = SPARSE_COLUMN_PATH;
            plan->bucket_index.reset();
            return Status::OK();
        }
        // Case 2: bucketized sparse column path: __DORIS_VARIANT_SPARSE__.b{i}
        if (rel.rfind(std::string(SPARSE_COLUMN_PATH) + ".b", 0) == 0) {
            CHECK(_binary_column_reader->get_type() == BinaryColumnType::MULTIPLE_SPARSE);
            // parse bucket index
            uint32_t bucket_index = static_cast<uint32_t>(
                    atoi(rel.substr(std::string(SPARSE_COLUMN_PATH).size() + 2).c_str()));
            const auto& reader = _binary_column_reader->select_reader(bucket_index);
            if (!reader) {
                return Status::NotFound("bucket sparse column reader not found: {}", rel);
            }
            plan->kind = ReadKind::SPARSE_MERGE;
            plan->type = DataTypeFactory::instance().create_data_type(target_col);
            plan->relative_path = relative_path;
            plan->binary_column_reader = _binary_column_reader->select_reader(bucket_index);
            plan->binary_cache_key =
                    std::string(SPARSE_COLUMN_PATH) + ".b" + std::to_string(bucket_index);
            plan->bucket_index = bucket_index;
            return Status::OK();
        }

        // case 3: doc snapshot column
        if (rel.find(DOC_VALUE_COLUMN_PATH) != std::string::npos) {
            CHECK(_binary_column_reader->get_type() == BinaryColumnType::MULTIPLE_DOC_VALUE);
            size_t bucket = rel.rfind('b');
            uint32_t bucket_value = static_cast<uint32_t>(std::stoul(rel.substr(bucket + 1)));
            plan->kind = ReadKind::DOC_COMPACT;
            plan->type = DataTypeFactory::instance().create_data_type(target_col);
            plan->binary_column_reader = _binary_column_reader->select_reader(bucket_value);
            return Status::OK();
        }

        if (target_col.is_nested_subcolumn()) {
            plan->kind = ReadKind::DEFAULT_NESTED;
            plan->type = DataTypeFactory::instance().create_data_type(target_col);
            plan->relative_path = relative_path;
            return Status::OK();
        }

        if (relative_path.empty()) {
            // root path, use VariantRootColumnIterator
            plan->kind = ReadKind::ROOT_FLAT;
            plan->type = create_variant_type(target_col);
            plan->relative_path = relative_path;
            plan->needs_root_merge = _needs_root_nested_group_merge(relative_path);
            return Status::OK();
        }

        // If the path is typed, it means the path is not a sparse column, so we can't read the sparse column
        // even if the sparse column size is reached limit
        bool existed_in_sparse_column =
                _statistics->existed_in_sparse_column(relative_path.get_path());
        bool exceeded_sparse_column_limit = is_exceeded_sparse_column_limit();
        if (existed_in_sparse_column || exceeded_sparse_column_limit) {
            // Sparse column exists or reached sparse size limit, read sparse column
            auto [reader, cache_key] =
                    _binary_column_reader->select_reader_and_cache_key(relative_path.get_path());
            DCHECK(reader != nullptr);
            plan->kind = ReadKind::BINARY_EXTRACT;
            plan->type = create_variant_type(target_col);
            plan->relative_path = relative_path;
            plan->binary_column_reader = std::move(reader);
            plan->binary_cache_key = std::move(cache_key);
            plan->bucket_index.reset();
            return Status::OK();
        }

        VLOG_DEBUG << "new_default_iter: " << target_col.path_info_ptr()->get_path();
        plan->kind = ReadKind::DEFAULT_FILL;
        plan->type = DataTypeFactory::instance().create_data_type(target_col);
        plan->relative_path = relative_path;
        return Status::OK();
    }
    VLOG_DEBUG << "new iterator: " << target_col.path_info_ptr()->get_path();
    std::shared_ptr<ColumnReader> column_reader;
    RETURN_IF_ERROR(column_reader_cache->get_path_column_reader(
            target_col.parent_unique_id(), node->path, &column_reader, opts->stats, node));
    plan->kind = ReadKind::LEAF;
    plan->type = column_reader->get_vec_data_type();
    plan->relative_path = relative_path;
    plan->leaf_column_reader = std::move(column_reader);
    return Status::OK();
}

bool VariantColumnReader::_need_read_flat_leaves(const StorageReadOptions* opts) {
    return opts != nullptr && opts->tablet_schema != nullptr &&
           std::ranges::any_of(opts->tablet_schema->columns(),
                               [](const auto& column) { return column->is_extracted_column(); }) &&
           is_compaction_or_checksum_reader(opts);
}

bool VariantColumnReader::_needs_root_nested_group_merge(const PathInData& relative_path) const {
    return relative_path.empty() && _nested_group_read_provider != nullptr &&
           !_nested_group_readers.empty();
}

Status VariantColumnReader::_validate_access_paths_debug(const TabletColumn& target_col,
                                                         const StorageReadOptions* opt,
                                                         int32_t col_uid,
                                                         const PathInData& relative_path) const {
    DBUG_EXECUTE_IF("VariantColumnReader.build_read_plan.access_paths", {
        if (opt != nullptr && opt->io_ctx.reader_type == ReaderType::READER_QUERY) {
            auto split_csv = [](const std::string& s) {
                std::vector<std::string> out;
                out.reserve(8);
                size_t pos = 0;
                while (pos < s.size()) {
                    size_t comma = s.find(',', pos);
                    if (comma == std::string::npos) {
                        comma = s.size();
                    }
                    size_t l = pos;
                    size_t r = comma;
                    while (l < r && s[l] == ' ') {
                        ++l;
                    }
                    while (r > l && s[r - 1] == ' ') {
                        --r;
                    }
                    if (r > l) {
                        out.emplace_back(s.substr(l, r - l));
                    }
                    pos = comma + 1;
                }
                return out;
            };

            const std::string root_name = _tablet_schema->column_by_uid(col_uid).name();
            bool allow_all = false;
            std::unordered_set<std::string> rel_paths;
            auto dump_paths = [&]() -> std::string {
                std::string out;
                bool first = true;
                for (const auto& p : rel_paths) {
                    if (!first) {
                        out += ",";
                    }
                    first = false;
                    out += p;
                }
                return out;
            };

            auto collect = [&](const TColumnAccessPaths& access_paths) {
                for (const auto& access_path : access_paths) {
                    if (access_path.type != TAccessPathType::DATA ||
                        !access_path.__isset.data_access_path) {
                        continue;
                    }
                    const auto& parts = access_path.data_access_path.path;
                    if (parts.empty()) {
                        continue;
                    }
                    size_t start = 0;
                    if (StringCaseEqual()(parts[0], root_name)) {
                        start = 1;
                    }
                    if (start >= parts.size()) {
                        allow_all = true;
                        return;
                    }
                    for (size_t i = start; i < parts.size(); ++i) {
                        if (parts[i] == "*") {
                            allow_all = true;
                            return;
                        }
                    }
                    std::string rel = parts[start];
                    for (size_t i = start + 1; i < parts.size(); ++i) {
                        rel += ".";
                        rel += parts[i];
                    }
                    if (rel.empty()) {
                        allow_all = true;
                        return;
                    }
                    rel_paths.emplace(std::move(rel));
                }
            };

            if (auto it = opt->all_access_paths.find(col_uid); it != opt->all_access_paths.end()) {
                collect(it->second);
            }
            if (auto it = opt->predicate_access_paths.find(col_uid);
                it != opt->predicate_access_paths.end()) {
                collect(it->second);
            }

            auto require = split_csv(dp->param<std::string>("require", ""));
            auto forbid = split_csv(dp->param<std::string>("forbid", ""));
            const bool expect_allow_all = dp->param<bool>("expect_allow_all", false);

            if (expect_allow_all != allow_all) {
                return Status::InternalError(
                        "DebugPoint {} expect_allow_all={} but allow_all={} col_uid={} root={} "
                        "relative_path={} paths={}",
                        DP_NAME, expect_allow_all, allow_all, col_uid, root_name,
                        relative_path.get_path(), dump_paths());
            }

            if (!allow_all) {
                for (const auto& r : require) {
                    if (!r.empty() && !rel_paths.contains(r)) {
                        return Status::InternalError(
                                "DebugPoint {} missing required path {} col_uid={} root={} "
                                "paths={}",
                                DP_NAME, r, col_uid, root_name, dump_paths());
                    }
                }
                for (const auto& f : forbid) {
                    if (!f.empty() && rel_paths.contains(f)) {
                        return Status::InternalError(
                                "DebugPoint {} hit forbidden path {} col_uid={} root={} paths={}",
                                DP_NAME, f, col_uid, root_name, dump_paths());
                    }
                }
            }
        }
    });
    return Status::OK();
}

bool VariantColumnReader::_try_fill_nested_group_plan(ReadPlan* plan,
                                                      const TabletColumn& target_col,
                                                      const StorageReadOptions* opt,
                                                      int32_t col_uid,
                                                      const PathInData& relative_path) const {
    DCHECK(_nested_group_read_provider != nullptr);

    bool is_whole = false;
    DataTypePtr out_type;
    PathInData out_relative_path;
    std::string out_child_path;
    std::string out_pruned_path;
    std::vector<const NestedGroupReader*> out_chain;
    std::optional<NestedGroupPathFilter> out_path_filter;

    if (!_nested_group_read_provider->try_build_read_plan(
                _tablet_schema.get(), _nested_group_readers, target_col, opt, col_uid,
                relative_path, &is_whole, &out_type, &out_relative_path, &out_child_path,
                &out_pruned_path, &out_chain, &out_path_filter)) {
        return false;
    }
    plan->kind = is_whole ? ReadKind::NESTED_GROUP_WHOLE : ReadKind::NESTED_GROUP_CHILD;
    plan->type = std::move(out_type);
    plan->relative_path = std::move(out_relative_path);
    plan->nested_child_path = std::move(out_child_path);
    plan->nested_group_pruned_path = std::move(out_pruned_path);
    plan->nested_group_chain = std::move(out_chain);
    plan->nested_group_path_filter = std::move(out_path_filter);
    return true;
}

bool VariantColumnReader::_try_build_nested_group_plan(ReadPlan* plan,
                                                       const TabletColumn& target_col,
                                                       const StorageReadOptions* opt,
                                                       int32_t col_uid,
                                                       const PathInData& relative_path) const {
    const bool is_compaction_or_checksum = is_compaction_or_checksum_reader(opt);

    // Root path in compaction/checksum must reconstruct full Variant rows for re-write.
    // Query root reads can still use NestedGroup whole read for top-level array shape.
    if (relative_path.empty() && is_compaction_or_checksum) {
        return false;
    }
    if (!_can_use_nested_group_read_path()) {
        return false;
    }

    if (_need_read_flat_leaves(opt)) {
        return false;
    }
    return _try_fill_nested_group_plan(plan, target_col, opt, col_uid, relative_path);
}

Status VariantColumnReader::_try_build_leaf_plan(ReadPlan* plan, int32_t col_uid,
                                                 const PathInData& relative_path,
                                                 const SubcolumnColumnMetaInfo::Node* node,
                                                 ColumnReaderCache* column_reader_cache,
                                                 OlapReaderStatistics* stats) {
    if (node == nullptr) {
        return Status::OK();
    }

    DCHECK(node->is_leaf_node());
    const auto* leaf_node = _subcolumns_meta_info->find_leaf(relative_path);

    std::shared_ptr<ColumnReader> leaf_column_reader;
    RETURN_IF_ERROR(column_reader_cache->get_path_column_reader(
            col_uid, leaf_node->path, &leaf_column_reader, stats, leaf_node));
    plan->kind = ReadKind::LEAF;
    plan->type = leaf_column_reader->get_vec_data_type();
    plan->relative_path = relative_path;
    plan->leaf_column_reader = std::move(leaf_column_reader);
    return Status::OK();
}

Status VariantColumnReader::_try_build_external_leaf_plan(ReadPlan* plan, int32_t col_uid,
                                                          const PathInData& relative_path,
                                                          ColumnReaderCache* column_reader_cache,
                                                          OlapReaderStatistics* stats) {
    if (!_ext_meta_reader || !_ext_meta_reader->available()) {
        return Status::OK();
    }

    std::shared_ptr<ColumnReader> leaf_column_reader;
    Status st = column_reader_cache->get_path_column_reader(col_uid, relative_path,
                                                            &leaf_column_reader, stats, nullptr);
    DCHECK(!_has_prefix_path_unlocked(relative_path));
    if (st.ok()) {
        plan->kind = ReadKind::LEAF;
        plan->type = leaf_column_reader->get_vec_data_type();
        plan->relative_path = relative_path;
        plan->leaf_column_reader = std::move(leaf_column_reader);
        return Status::OK();
    }
    if (!st.is<ErrorCode::NOT_FOUND>()) {
        return st;
    }
    return Status::OK();
}

Status VariantColumnReader::_build_read_plan(ReadPlan* plan, const TabletColumn& target_col,
                                             const StorageReadOptions* opt,
                                             ColumnReaderCache* column_reader_cache,
                                             PathToBinaryColumnCache* binary_column_cache_ptr) {
    // root column use unique id, leaf column use parent_unique_id
    int32_t col_uid =
            target_col.unique_id() >= 0 ? target_col.unique_id() : target_col.parent_unique_id();
    // root column use unique id, leaf column use parent_unique_id
    auto relative_path = target_col.path_info_ptr()->copy_pop_front();

    RETURN_IF_ERROR(_validate_access_paths_debug(target_col, opt, col_uid, relative_path));

    // If the variant column has extracted columns and is a compaction reader, then read flat leaves
    // Otherwise read hierarchical data, since the variant subcolumns are flattened in
    // variant_util::get_compaction_schema. For checksum reader, we need to read flat leaves to
    // get the correct data if has extracted columns.
    // Flat-leaf compaction/checksum mode: delegate to dedicated planner which handles locking
    // and external meta loading internally.
    if (_need_read_flat_leaves(opt)) {
        return _build_read_plan_flat_leaves(plan, target_col, opt, column_reader_cache,
                                            binary_column_cache_ptr);
    }

    std::shared_lock<std::shared_mutex> lock(_subcolumns_meta_mutex);
    const auto* root = _subcolumns_meta_info->get_root();
    const auto* node =
            target_col.has_path_info() ? _subcolumns_meta_info->find_exact(relative_path) : nullptr;

    // try rebuild path with hierarchical
    // example path(['a.b']) -> path(['a', 'b'])
    if (node == nullptr) {
        relative_path = PathInData(relative_path.get_path());
        node = _subcolumns_meta_info->find_exact(relative_path);
    }

    // NestedGroup path resolution must happen before doc/sparse/hierarchical fallbacks.
    // This keeps query/compaction behavior consistent for array<object> paths.
    if (_try_build_nested_group_plan(plan, target_col, opt, col_uid, relative_path)) {
        return Status::OK();
    }

    // read root: from doc value column
    if (root->path == relative_path && _statistics->has_doc_value_column_non_null_size()) {
        plan->kind = ReadKind::HIERARCHICAL_DOC;
        plan->type = create_variant_type(target_col);
        plan->relative_path = relative_path;
        plan->root = root;
        plan->needs_root_merge = _needs_root_nested_group_merge(relative_path);
        return Status::OK();
    }

    // Check if path exist in sparse column
    bool existed_in_sparse_column = _statistics->existed_in_sparse_column(relative_path.get_path());

    DBUG_EXECUTE_IF("exist_in_sparse_column_must_be_false", {
        if (existed_in_sparse_column) {
            return Status::Error<ErrorCode::INTERNAL_ERROR>(
                    "exist_in_sparse_column_must_be_false, relative_path: {}",
                    relative_path.get_path());
        }
    })

    // Otherwise the prefix is not exist and the sparse column size is reached limit
    // which means the path maybe exist in sparse_column
    bool exceeded_sparse_column_limit = _is_exceeded_sparse_column_limit_unlocked();

    const std::string dot_prefix = relative_path.get_path() + ".";
    if (target_col.variant_enable_doc_mode() &&
        _statistics->has_prefix_path_in_doc_value_column(dot_prefix)) {
        plan->kind = ReadKind::HIERARCHICAL_DOC;
        plan->type = create_variant_type(target_col);
        plan->relative_path = relative_path;
        plan->root = root;
        plan->needs_root_merge = _needs_root_nested_group_merge(relative_path);
        return Status::OK();
    }

    // Check if path is prefix, example sparse columns path: a.b.c, a.b.e, access prefix: a.b.
    // Or access root path.
    const bool has_prefix_path = _has_prefix_path_unlocked(relative_path);
    const bool sparse_stats_may_have_unrecorded_children =
            node == nullptr && exceeded_sparse_column_limit && existed_in_sparse_column;
    if (has_prefix_path || sparse_stats_may_have_unrecorded_children) {
        // Example {"b" : {"c":456,"e":7.111}}
        // b.c is sparse column, b.e is subcolumn, so b is both the prefix of sparse column and
        // subcolumn
        plan->kind = ReadKind::HIERARCHICAL;
        plan->type = create_variant_type(target_col);
        plan->relative_path = relative_path;
        plan->node = node;
        plan->root = root;
        plan->null_on_no_match = !has_prefix_path && sparse_stats_may_have_unrecorded_children;
        plan->needs_root_merge = _needs_root_nested_group_merge(relative_path);
        return Status::OK();
    }

    // if path exists in sparse column, read sparse column with extract reader
    if (existed_in_sparse_column && !node) {
        // node should be nullptr, example
        // {"b" : {"c":456}}   b.c in subcolumn
        // {"b" : 123}         b in sparse column
        // Then we should use hierarchical reader to read b
        auto [reader, cache_key] =
                _binary_column_reader->select_reader_and_cache_key(relative_path.get_path());
        DCHECK(reader);
        plan->kind = ReadKind::BINARY_EXTRACT;
        plan->type = create_variant_type(target_col);
        plan->relative_path = relative_path;
        plan->binary_column_reader = std::move(reader);
        plan->binary_cache_key = std::move(cache_key);
        plan->bucket_index.reset();
        return Status::OK();
    }

    RETURN_IF_ERROR(_try_build_leaf_plan(plan, col_uid, relative_path, node, column_reader_cache,
                                         opt->stats));
    if (plan->kind == ReadKind::LEAF) {
        return Status::OK();
    }
    if (node == nullptr) {
        RETURN_IF_ERROR(_try_build_external_leaf_plan(plan, col_uid, relative_path,
                                                      column_reader_cache, opt->stats));
        if (plan->kind == ReadKind::LEAF) {
            return Status::OK();
        }

        if (_statistics->has_prefix_path_in_doc_value_column(dot_prefix)) {
            plan->kind = ReadKind::HIERARCHICAL_DOC;
            plan->type = create_variant_type(target_col);
            plan->relative_path = relative_path;
            plan->root = root;
            plan->needs_root_merge = _needs_root_nested_group_merge(relative_path);
            return Status::OK();
        }

        // find if path exists in doc snapshot column
        bool existed_in_doc_column =
                _statistics->existed_in_doc_value_column(relative_path.get_path());
        if (existed_in_doc_column) {
            auto [reader, cache_key] =
                    _binary_column_reader->select_reader_and_cache_key(relative_path.get_path());
            DCHECK(reader);
            plan->kind = ReadKind::BINARY_EXTRACT;
            plan->type = create_variant_type(target_col);
            plan->relative_path = relative_path;
            plan->binary_column_reader = std::move(reader);
            plan->binary_cache_key = std::move(cache_key);
            return Status::OK();
        }

        if (exceeded_sparse_column_limit) {
            // Sparse stats are truncated, so a missing stat entry does not prove that the exact
            // path is absent. Read the sparse subtree, but make rows with no exact/descendant
            // physical match SQL NULL instead of manufacturing an empty object.
            plan->kind = ReadKind::HIERARCHICAL;
            plan->type = create_variant_type(target_col);
            plan->relative_path = relative_path;
            plan->node = node;
            plan->root = root;
            plan->null_on_no_match = true;
            plan->needs_root_merge = _needs_root_nested_group_merge(relative_path);
            return Status::OK();
        }

        // Sparse column not exists and not reached stats limit, then the target path is not
        // exist, get a default iterator
        plan->kind = ReadKind::DEFAULT_FILL;
        plan->type = DataTypeFactory::instance().create_data_type(target_col);
        plan->relative_path = relative_path;
        return Status::OK();
    }
    return Status::OK();
}

Status VariantColumnReader::_create_iterator_from_plan(
        ColumnIteratorUPtr* iterator, const ReadPlan& plan, const TabletColumn& target_col,
        const StorageReadOptions* opt, ColumnReaderCache* column_reader_cache,
        PathToBinaryColumnCache* binary_column_cache_ptr) {
    switch (plan.kind) {
    case ReadKind::ROOT_FLAT: {
        // ROOT_FLAT reads the persisted root column itself. It does not rebuild root `v` from
        // regular extracted columns such as `v.keep` / `v.owner`; only the optional root-merge
        // wrapper below may fold NestedGroup data back into the root view.
        *iterator = std::make_unique<VariantRootColumnIterator>(
                std::make_unique<FileColumnIterator>(_root_column_reader));
        return _maybe_wrap_root_merge_iterator(iterator, plan, opt);
    }
    case ReadKind::HIERARCHICAL: {
        // HIERARCHICAL reconstructs the requested object from extracted subcolumns plus sparse
        // state. Reading root `v` through this branch may therefore read regular children such as
        // `v.keep` / `v.owner` and merge them into the final variant result.
        int32_t col_uid = target_col.unique_id() >= 0 ? target_col.unique_id()
                                                      : target_col.parent_unique_id();
        RETURN_IF_ERROR(_create_hierarchical_reader(
                iterator, col_uid, plan.relative_path, plan.node, plan.root, column_reader_cache,
                opt->stats, HierarchicalDataIterator::ReadType::SUBCOLUMNS_AND_SPARSE,
                plan.null_on_no_match));
        return _maybe_wrap_root_merge_iterator(iterator, plan, opt);
    }
    case ReadKind::LEAF: {
        DCHECK(plan.leaf_column_reader != nullptr);
        RETURN_IF_ERROR(plan.leaf_column_reader->new_iterator(iterator, nullptr));
        if (opt && opt->stats) {
            opt->stats->variant_subtree_leaf_iter_count++;
        }
        return Status::OK();
    }
    case ReadKind::BINARY_EXTRACT: {
        DCHECK(plan.binary_column_reader != nullptr);
        BinaryColumnCacheSPtr binary_column_cache = DORIS_TRY(_get_binary_column_cache(
                binary_column_cache_ptr, plan.binary_cache_key, plan.binary_column_reader));
        *iterator = std::make_unique<BinaryColumnExtractIterator>(
                plan.relative_path.get_path(), std::move(binary_column_cache), opt);
        if (opt && opt->stats) {
            opt->stats->variant_subtree_sparse_iter_count++;
        }
        return Status::OK();
    }
    case ReadKind::SPARSE_MERGE: {
        DCHECK(plan.binary_column_reader != nullptr);
        BinaryColumnCacheSPtr sparse_column_cache = DORIS_TRY(_get_binary_column_cache(
                binary_column_cache_ptr, plan.binary_cache_key, plan.binary_column_reader));
        RETURN_IF_ERROR(_create_sparse_merge_reader(iterator, opt, target_col, sparse_column_cache,
                                                    column_reader_cache, plan.bucket_index));
        return Status::OK();
    }
    case ReadKind::DEFAULT_NESTED: {
        RETURN_IF_ERROR(
                _new_default_iter_with_same_nested(iterator, target_col, opt, column_reader_cache));
        return Status::OK();
    }
    case ReadKind::DEFAULT_FILL: {
        RETURN_IF_ERROR(Segment::new_default_iterator(target_col, iterator));
        if (opt && opt->stats) {
            opt->stats->variant_subtree_default_iter_count++;
        }
        return Status::OK();
    }
    case ReadKind::DOC_COMPACT: {
        DCHECK(plan.binary_column_reader);
        ColumnIteratorUPtr inner_iter;
        RETURN_IF_ERROR(plan.binary_column_reader->new_iterator(&inner_iter, nullptr));
        *iterator = std::make_unique<VariantDocValueCompactIterator>(std::move(inner_iter));
        return Status::OK();
    }
    case ReadKind::HIERARCHICAL_DOC: {
        int32_t col_uid = target_col.unique_id() >= 0 ? target_col.unique_id()
                                                      : target_col.parent_unique_id();
        RETURN_IF_ERROR(_create_hierarchical_reader(
                iterator, col_uid, plan.relative_path, plan.node, plan.root, column_reader_cache,
                opt->stats, HierarchicalDataIterator::ReadType::DOC_VALUE_COLUMN));
        if (opt && opt->stats) {
            opt->stats->variant_doc_value_column_iter_count++;
        }
        return _maybe_wrap_root_merge_iterator(iterator, plan, opt);
    }
    case ReadKind::NESTED_GROUP_WHOLE:
    case ReadKind::NESTED_GROUP_CHILD: {
        // Delegate iterator creation to the read provider.
        DCHECK(!plan.nested_group_chain.empty());
        bool is_whole = (plan.kind == ReadKind::NESTED_GROUP_WHOLE);
        DataTypePtr out_type;
        RETURN_IF_ERROR(_nested_group_read_provider->create_nested_group_iterator(
                is_whole, plan.nested_group_chain, plan.nested_child_path,
                plan.nested_group_pruned_path, plan.nested_group_path_filter, iterator, &out_type));

        DCHECK(plan.type->equals(*make_nullable(out_type)))
                << "Type mismatch in NESTED_GROUP: plan.type=" << plan.type->get_name()
                << ", iterator_type=" << make_nullable(out_type)->get_name();

        if (!is_whole && opt && opt->stats) {
            opt->stats->variant_subtree_leaf_iter_count++;
        }
        return Status::OK();
    }
    default:
        return Status::InternalError("unknown variant read kind");
    }
}

Status VariantColumnReader::_maybe_wrap_root_merge_iterator(ColumnIteratorUPtr* iterator,
                                                            const ReadPlan& plan,
                                                            const StorageReadOptions* opt) {
    if (!plan.needs_root_merge) {
        return Status::OK();
    }

    // The planner may reach this point through ROOT_FLAT, HIERARCHICAL or HIERARCHICAL_DOC.
    // Wrapping once here prevents those branches from duplicating the same root merge logic.
    ColumnIteratorUPtr merged_iterator;
    RETURN_IF_ERROR(_nested_group_read_provider->create_root_merge_iterator(
            std::move(*iterator), _nested_group_readers, opt, &merged_iterator));
    *iterator = std::move(merged_iterator);
    return Status::OK();
}

Status VariantColumnReader::new_iterator(ColumnIteratorUPtr* iterator,
                                         const TabletColumn* target_col,
                                         const StorageReadOptions* opt) {
    // return new_iterator(iterator, target_col, opt, nullptr);
    return Status::NotSupported("Not implemented");
}

Status VariantColumnReader::new_iterator(ColumnIteratorUPtr* iterator,
                                         const TabletColumn* target_col,
                                         const StorageReadOptions* opt,
                                         ColumnReaderCache* column_reader_cache,
                                         PathToBinaryColumnCache* binary_column_cache_ptr) {
    ReadPlan plan;
    RETURN_IF_ERROR(_build_read_plan(&plan, *target_col, opt, column_reader_cache,
                                     binary_column_cache_ptr));
    // Caller of this overload does not need the storage type; only iterator is used.
    RETURN_IF_ERROR(_create_iterator_from_plan(iterator, plan, *target_col, opt,
                                               column_reader_cache, binary_column_cache_ptr));
    const bool needs_reader_owner = plan.needs_root_merge ||
                                    plan.kind == ReadKind::NESTED_GROUP_WHOLE ||
                                    plan.kind == ReadKind::NESTED_GROUP_CHILD;
    if (needs_reader_owner) {
        *iterator = std::make_unique<ReaderOwnedColumnIterator>(std::move(*iterator),
                                                                shared_from_this());
    }
    return Status::OK();
}

Status VariantColumnReader::infer_data_type_for_path(DataTypePtr* type, const TabletColumn& column,
                                                     const StorageReadOptions& opts,
                                                     ColumnReaderCache* column_reader_cache) {
    DCHECK(column.has_path_info());
    ReadPlan plan;
    RETURN_IF_ERROR(_build_read_plan(&plan, column, &opts, column_reader_cache, nullptr));
    *type = plan.type;
    return Status::OK();
}

} // namespace doris::segment_v2
