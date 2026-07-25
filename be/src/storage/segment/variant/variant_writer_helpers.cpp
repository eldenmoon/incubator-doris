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

#include "storage/segment/variant/variant_writer_helpers.h"

#include <memory>
#include <string>
#include <utility>

#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_jsonb.h"
#include "exec/common/variant_util.h"
#include "storage/index/indexed_column_writer.h"
#include "storage/iterator/olap_data_convertor.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/segment/variant/variant_column_writer_impl.h"
#include "storage/types.h"

namespace doris::segment_v2::variant_writer_helpers {

Status create_column_writer(uint32_t cid, const TabletColumn& column,
                            const TabletSchemaSPtr& tablet_schema,
                            IndexFileWriter* inverted_index_file_writer,
                            std::unique_ptr<ColumnWriter>* writer, TabletIndexes& subcolumn_indexes,
                            ColumnWriterOptions* opt, int64_t none_null_value_size,
                            bool need_record_none_null_value_size) {
    _init_column_meta(opt->meta, cid, column, *opt);
    // no need to record none null value size for typed column or nested column, since it's compaction stage
    // will directly pick it as sub column
    if (need_record_none_null_value_size) {
        // record none null value size for statistics
        opt->meta->set_none_null_size(none_null_value_size);
    }
    opt->need_zone_map = tablet_schema->keys_type() != KeysType::AGG_KEYS;
    opt->need_bloom_filter = column.is_bf_column();
    const auto& parent_index = tablet_schema->inverted_indexs(column.parent_unique_id());

    // init inverted index
    // parent_index denotes the index of the entire variant column
    // while subcolumn_index denotes the current subcolumn's index
    if (segment_v2::IndexColumnWriter::check_support_inverted_index(column)) {
        auto init_opt_inverted_index = [&]() {
            DCHECK(!subcolumn_indexes.empty());
            for (const auto& index : subcolumn_indexes) {
                opt->inverted_indexes.push_back(index.get());
            }
            opt->need_inverted_index = true;
            DCHECK(inverted_index_file_writer != nullptr);
            opt->index_file_writer = inverted_index_file_writer;
        };

        // the subcolumn index is already initialized
        if (!subcolumn_indexes.empty()) {
            init_opt_inverted_index();
        }
        // the subcolumn index is not initialized, but the parent index is present
        else if (!parent_index.empty() &&
                 variant_util::inherit_index(parent_index, subcolumn_indexes, column)) {
            init_opt_inverted_index();
        }
        // no parent index and no subcolumn index
        else {
            opt->need_inverted_index = false;
        }
    }

#define DISABLE_INDEX_IF_FIELD_TYPE(TYPE)                     \
    if (column.type() == FieldType::OLAP_FIELD_TYPE_##TYPE) { \
        opt->need_zone_map = false;                           \
        opt->need_bloom_filter = false;                       \
    }

    DISABLE_INDEX_IF_FIELD_TYPE(ARRAY)
    DISABLE_INDEX_IF_FIELD_TYPE(JSONB)
    DISABLE_INDEX_IF_FIELD_TYPE(VARIANT)

#undef DISABLE_INDEX_IF_FIELD_TYPE

    RETURN_IF_ERROR(ColumnWriter::create(*opt, &column, opt->file_writer, writer));
    RETURN_IF_ERROR((*writer)->init());

    return Status::OK();
}

Status convert_and_write_column(OlapBlockDataConvertor* converter, const TabletColumn& column,
                                DataTypePtr data_type, ColumnWriter* writer,
                                const ColumnPtr& src_column, size_t num_rows, int column_id) {
    converter->add_column_data_convertor(column);
    RETURN_IF_ERROR(converter->set_source_content_with_specifid_column({src_column, data_type, ""},
                                                                       0, num_rows, column_id));
    auto [status, converted_column] = converter->convert_column_data(column_id);
    RETURN_IF_ERROR(status);

    const uint8_t* nullmap = converted_column->get_nullmap();
    RETURN_IF_ERROR(writer->append(nullmap, converted_column->get_data(), num_rows));

    converter->clear_source_content(column_id);
    return Status::OK();
}

void maybe_remove_root_jsonb_with_empty_defaults(MutableColumnPtr* root_column, size_t num_rows,
                                                 bool remove_root_jsonb) {
    if (!remove_root_jsonb) {
        return;
    }
    auto bare_jsonb_type = std::make_shared<DataTypeJsonb>();
    auto bare_jsonb_col = bare_jsonb_type->create_column();
    bare_jsonb_col->insert_many_defaults(num_rows);
    *root_column = std::move(bare_jsonb_col);
}

Status prepare_subcolumn_writer_target(
        const ColumnWriterOptions& base_opts, const TabletColumn& parent_column,
        int current_column_id, const PathInData& relative_path, const DataTypePtr& current_type,
        int64_t none_null_value_size, size_t num_rows,
        const TabletSchema::SubColumnInfo* existing_subcolumn_info, bool check_storage_type,
        // Written through by move-assignment below; the checker misses the alias write.
        TabletIndexes* out_subcolumn_indexes, // NOLINT(readability-non-const-parameter)
        ColumnWriterOptions* out_subcolumn_opts, std::unique_ptr<ColumnWriter>* out_writer,
        TabletColumn* out_tablet_column) {
    if (out_subcolumn_indexes == nullptr || out_subcolumn_opts == nullptr ||
        out_writer == nullptr || out_tablet_column == nullptr) {
        return Status::InvalidArgument("subcolumn writer target output is null");
    }

    TabletColumn tablet_column;
    TabletIndexes subcolumn_indexes;
    bool resolved_from_schema = false;
    if (existing_subcolumn_info != nullptr) {
        tablet_column = existing_subcolumn_info->column;
        subcolumn_indexes = existing_subcolumn_info->indexes;
        resolved_from_schema = true;
    } else {
        TabletSchema::SubColumnInfo sub_column_info;
        if (variant_util::generate_sub_column_info(*base_opts.rowset_ctx->tablet_schema,
                                                   parent_column.unique_id(),
                                                   relative_path.get_path(), &sub_column_info)) {
            tablet_column = std::move(sub_column_info.column);
            subcolumn_indexes = std::move(sub_column_info.indexes);
            resolved_from_schema = true;
        } else {
            const std::string column_name =
                    parent_column.name_lower_case() + "." + relative_path.get_path();
            PathInData full_path;
            if (relative_path.has_nested_part()) {
                PathInDataBuilder full_path_builder;
                full_path = full_path_builder.append(parent_column.name_lower_case(), false)
                                    .append(relative_path.get_parts(), false)
                                    .build();
            } else {
                full_path = PathInData(column_name);
            }
            tablet_column = variant_util::get_column_by_type(
                    current_type, column_name,
                    variant_util::ExtraInfo {.unique_id = -1,
                                             .parent_unique_id = parent_column.unique_id(),
                                             .path_info = full_path});
            const auto& indexes =
                    base_opts.rowset_ctx->tablet_schema->inverted_indexs(parent_column.unique_id());
            variant_util::inherit_index(indexes, subcolumn_indexes, tablet_column);
        }
    }

    if (resolved_from_schema && check_storage_type) {
        auto storage_type = DataTypeFactory::instance().create_data_type(tablet_column);
        if (!storage_type->equals(*current_type)) {
            return Status::InvalidArgument(
                    "Storage type {} is not equal to current type {} for path {}",
                    storage_type->get_name(), current_type->get_name(), relative_path.get_path());
        }
    }

    ColumnWriterOptions opts;
    opts.meta = base_opts.footer->add_columns();
    opts.index_file_writer = base_opts.index_file_writer;
    opts.compression_type = base_opts.compression_type;
    opts.rowset_ctx = base_opts.rowset_ctx;
    opts.file_writer = base_opts.file_writer;
    opts.storage_format = base_opts.storage_format;
    variant_util::inherit_column_attributes(parent_column, tablet_column);

    bool need_record_none_null_value_size =
            (!tablet_column.path_info_ptr()->get_is_typed() ||
             parent_column.variant_enable_typed_paths_to_sparse()) &&
            !tablet_column.path_info_ptr()->has_nested_part() &&
            variant_util::should_record_variant_path_stats(parent_column);

    std::unique_ptr<ColumnWriter> writer;
    RETURN_IF_ERROR(create_column_writer(
            current_column_id, tablet_column, base_opts.rowset_ctx->tablet_schema,
            base_opts.index_file_writer, &writer, subcolumn_indexes, &opts, none_null_value_size,
            need_record_none_null_value_size));
    opts.meta->set_num_rows(num_rows);
    *out_subcolumn_indexes = std::move(subcolumn_indexes);
    *out_subcolumn_opts = opts;
    *out_writer = std::move(writer);
    *out_tablet_column = std::move(tablet_column);
    return Status::OK();
}

} // namespace doris::segment_v2::variant_writer_helpers
