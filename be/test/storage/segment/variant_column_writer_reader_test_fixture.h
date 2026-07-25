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

#include <atomic>
#include <cstdlib>
#include <set>
#include <thread>

#include "common/config.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type_serde/data_type_serde.h"
#include "exprs/function/parse/variant_string_parse.h"
#include "gtest/gtest.h"
#include "storage/index/index_file_writer.h"
#include "storage/index/inverted/inverted_index_desc.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/segment/column_meta_accessor.h"
#include "storage/segment/column_reader.h"
#include "storage/segment/column_reader_cache.h"
#include "storage/segment/variant/binary_column_extract_iterator.h"
#include "storage/segment/variant/hierarchical_data_iterator.h"
#include "storage/segment/variant/nested_group_path.h"
#include "storage/segment/variant/nested_group_provider.h"
#include "storage/segment/variant/nested_group_streaming_write_plan.h"
#include "storage/segment/variant/sparse_column_merge_iterator.h"
#include "storage/segment/variant/variant_column_reader.h"
#include "storage/segment/variant/variant_column_writer_impl.h"
#include "storage/storage_engine.h"
#include "testutil/variant_util.h"

namespace doris {

inline constexpr uint32_t MAX_PATH_LEN = 1024;
inline constexpr std::string_view dest_dir = "/ut_dir/variant_column_writer_test";
inline constexpr std::string_view tmp_dir = "./ut_dir/tmp";

inline void construct_column(ColumnPB* column_pb, int32_t col_unique_id,
                             const std::string& column_type, const std::string& column_name,
                             int variant_max_subcolumns_count = 3, bool is_key = false,
                             bool is_nullable = false, int variant_sparse_hash_shard_count = 0,
                             bool variant_enable_doc_mode = false,
                             int64_t variant_doc_materialization_min_rows = 0,
                             int variant_doc_hash_shard_count = 0,
                             bool variant_enable_nested_group = false) {
    column_pb->set_unique_id(col_unique_id);
    column_pb->set_name(column_name);
    column_pb->set_type(column_type);
    column_pb->set_is_key(is_key);
    column_pb->set_is_nullable(is_nullable);
    if (column_type == "VARIANT") {
        column_pb->set_variant_max_subcolumns_count(variant_max_subcolumns_count);
        column_pb->set_variant_max_sparse_column_statistics_size(10000);
        // 5 sparse hash shard
        column_pb->set_variant_sparse_hash_shard_count(variant_sparse_hash_shard_count);
        column_pb->set_variant_enable_doc_mode(variant_enable_doc_mode);
        column_pb->set_variant_doc_materialization_min_rows(variant_doc_materialization_min_rows);
        if (variant_doc_hash_shard_count > 0) {
            column_pb->set_variant_doc_hash_shard_count(variant_doc_hash_shard_count);
        }
        column_pb->set_variant_enable_nested_group(variant_enable_nested_group);
    }
}

inline ColumnVariantV2::MutablePtr encode_json_column_v2(const ColumnString& jsons) {
    JsonStringToVariantEncoder encoder;
    for (size_t row = 0; row < jsons.size(); ++row) {
        encoder.add_json(jsons.get_data_at(row));
    }
    VariantBatchBuilder block = encoder.finish_batch();
    auto result = ColumnVariantV2::create();
    result->insert_encoded_batch(block);
    return result;
}

struct VariantJsonWriter {
    void write(const char* data, size_t size) { value.append(data, size); }

    std::string value;
};

inline std::string variant_json_at(const IColumn& column, size_t row) {
    const IColumn* data = &column;
    if (const auto* nullable = check_and_get_column<ColumnNullable>(column); nullable != nullptr) {
        if (nullable->is_null_at(row)) {
            return "NULL";
        }
        data = &nullable->get_nested_column();
    }
    const auto& variant = assert_cast<const ColumnVariantV2&>(*data);
    VariantJsonWriter writer;
    to_json(variant.get_value_ref(row), writer);
    return writer.value;
}

// MockColumnReaderCache class for testing
class MockColumnReaderCache : public segment_v2::ColumnReaderCache {
public:
    MockColumnReaderCache(const SegmentFooterPB& footer, const io::FileReaderSPtr& file_reader,
                          const std::shared_ptr<TabletSchema>& tablet_schema)
            : ColumnReaderCache(nullptr, nullptr, nullptr, 0,
                                [](std::shared_ptr<SegmentFooterPB>&, OlapReaderStatistics*) {
                                    return Status::OK();
                                }),
              _footer(footer),
              _file_reader(file_reader),
              _tablet_schema(tablet_schema) {}

    Status get_path_column_reader(
            int32_t col_uid, PathInData relative_path,
            std::shared_ptr<segment_v2::ColumnReader>* column_reader, OlapReaderStatistics* stats,
            const SubcolumnColumnMetaInfo::Node* node_hint = nullptr) override {
        DCHECK(node_hint != nullptr);
        // Use node_hint's footer_ordinal to locate the specific ColumnMeta
        int32_t footer_ordinal = node_hint->data.footer_ordinal;
        if (footer_ordinal < 0 || footer_ordinal >= _footer.columns_size()) {
            *column_reader = nullptr;
            return Status::OK();
        }

        // Create ColumnReaderOptions
        ColumnReaderOptions opts;
        opts.kept_in_memory = false;
        opts.be_exec_version = BeExecVersionManager::get_newest_version();
        opts.tablet_schema = _tablet_schema;

        // Use ColumnReader::create to generate the corresponding ColumnReader
        return segment_v2::ColumnReader::create(opts, _footer.columns(footer_ordinal),
                                                _footer.num_rows(), _file_reader, column_reader);
    }

private:
    const SegmentFooterPB& _footer;
    const io::FileReaderSPtr& _file_reader;
    const std::shared_ptr<TabletSchema>& _tablet_schema;
};

// Helper to create a root VariantColumnReader using ColumnMetaAccessor, which
// hides inline vs external column meta layout (V2 vs V3).
inline Status create_variant_root_reader(const SegmentFooterPB& footer,
                                         const io::FileReaderSPtr& file_reader,
                                         const TabletSchemaSPtr& tablet_schema,
                                         std::shared_ptr<segment_v2::ColumnReader>* out) {
    segment_v2::ColumnMetaAccessor accessor;
    RETURN_IF_ERROR(accessor.init(footer, file_reader));

    segment_v2::ColumnReaderOptions opts;
    opts.kept_in_memory = false;
    opts.be_exec_version = BeExecVersionManager::get_newest_version();
    opts.tablet_schema = tablet_schema;

    auto variant_reader = std::make_shared<segment_v2::VariantColumnReader>();
    int32_t root_uid = tablet_schema->column(0).unique_id();
    auto footer_sp = std::make_shared<SegmentFooterPB>();
    footer_sp->CopyFrom(footer);
    RETURN_IF_ERROR(variant_reader->init(opts, &accessor, footer_sp, root_uid, footer.num_rows(),
                                         file_reader));
    *out = std::move(variant_reader);
    return Status::OK();
}

inline std::vector<std::string> normalize_json_rows(const std::vector<std::string>& jsons) {
    auto json_col = ColumnString::create();
    for (const auto& json : jsons) {
        json_col->insert_data(json.data(), json.size());
    }
    auto variant_col = encode_json_column_v2(*json_col);

    std::vector<std::string> normalized;
    normalized.reserve(jsons.size());
    for (size_t i = 0; i < jsons.size(); ++i) {
        normalized.push_back(variant_json_at(*variant_col, i));
    }
    return normalized;
}

class VariantColumnWriterReaderTest : public testing::Test {
public:
    void SetUp() override {
        // absolute dir
        char buffer[MAX_PATH_LEN];
        EXPECT_NE(getcwd(buffer, MAX_PATH_LEN), nullptr);
        _current_dir = std::string(buffer);
        _absolute_dir = _current_dir + std::string(dest_dir);
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_absolute_dir).ok());
        EXPECT_TRUE(io::global_local_filesystem()->create_directory(_absolute_dir).ok());

        // tmp dir
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(tmp_dir).ok());
        EXPECT_TRUE(io::global_local_filesystem()->create_directory(tmp_dir).ok());
        std::vector<StorePath> paths;
        paths.emplace_back(std::string(tmp_dir), 1024000000);
        auto tmp_file_dirs = std::make_unique<segment_v2::TmpFileDirs>(paths);
        Status st = tmp_file_dirs->init();
        EXPECT_TRUE(st.ok()) << st.to_json();
        ExecEnv::GetInstance()->set_tmp_file_dir(std::move(tmp_file_dirs));

        // storage engine
        doris::EngineOptions options;
        auto engine = std::make_unique<StorageEngine>(options);
        _engine_ref = engine.get();
        _data_dir = std::make_unique<DataDir>(*_engine_ref, _absolute_dir);
        static_cast<void>(_data_dir->update_capacity());
        ExecEnv::GetInstance()->set_storage_engine(std::move(engine));
    }

    void TearDown() override {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_absolute_dir).ok());
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(tmp_dir).ok());
        _engine_ref = nullptr;
        ExecEnv::GetInstance()->set_storage_engine(nullptr);
    }

    VariantColumnWriterReaderTest() = default;
    ~VariantColumnWriterReaderTest() override = default;

protected:
    void init_variant_tablet(int64_t tablet_id, int variant_max_subcolumns_count = 10,
                             bool variant_enable_nested_group = false, bool is_nullable = false) {
        TabletSchemaPB schema_pb;
        schema_pb.set_keys_type(KeysType::DUP_KEYS);
        construct_column(schema_pb.add_column(), 1, "VARIANT", "V1", variant_max_subcolumns_count,
                         false, is_nullable, 0, false, 0, 0, variant_enable_nested_group);
        _tablet_schema = std::make_shared<TabletSchema>();
        _tablet_schema->init_from_pb(schema_pb);

        TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
        _tablet_schema->set_storage_format(TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V3);
        tablet_meta->_tablet_id = tablet_id;
        _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());

        EXPECT_TRUE(_tablet->init().ok());
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
        EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());
    }

    void init_tablet_from_current_schema(int64_t tablet_id,
                                         TabletStorageFormatPB storage_format =
                                                 TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2) {
        TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
        _tablet_schema->set_storage_format(storage_format);
        tablet_meta->_tablet_id = tablet_id;
        _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
        ASSERT_TRUE(_tablet->init().ok());
        ASSERT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
        ASSERT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());
    }

    RowsetSharedPtr create_variant_rowset(const std::vector<std::vector<std::string>>& batches,
                                          int64_t version, int64_t max_rows_per_segment = 200) {
        RowsetWriterContext ctx;
        RowsetId rowset_id;
        rowset_id.init(version + 1000);
        ctx.rowset_id = rowset_id;
        ctx.rowset_type = BETA_ROWSET;
        ctx.data_dir = _data_dir.get();
        ctx.rowset_state = VISIBLE;
        ctx.tablet_schema = _tablet_schema;
        ctx.tablet_path = _tablet->tablet_path();
        ctx.tablet_id = _tablet->tablet_id();
        ctx.tablet = _tablet;
        ctx.version = Version(version, version);
        ctx.segments_overlap = NONOVERLAPPING;
        ctx.max_rows_per_segment = max_rows_per_segment;
        ctx.write_type = DataWriteType::TYPE_DIRECT;

        auto res = RowsetFactory::create_rowset_writer(*_engine_ref, ctx, false);
        EXPECT_TRUE(res.has_value()) << res.error();
        auto rowset_writer = std::move(res).value();

        for (const auto& batch : batches) {
            Block block = _tablet_schema->create_block();
            auto columns = std::move(block).mutate_columns();
            auto json_col = ColumnString::create();
            for (const auto& json : batch) {
                json_col->insert_data(json.data(), json.size());
            }
            columns[0] = encode_json_column_v2(*json_col);
            block.set_columns(std::move(columns));

            auto st = rowset_writer->add_block(&block);
            EXPECT_TRUE(st.ok()) << st.to_string();
            st = rowset_writer->flush();
            EXPECT_TRUE(st.ok()) << st.to_string();
        }

        RowsetSharedPtr rowset;
        EXPECT_TRUE(rowset_writer->build(rowset).ok());
        return rowset;
    }

    std::vector<RowsetReaderSharedPtr> create_rowset_readers(
            const std::vector<RowsetSharedPtr>& rowsets) const {
        std::vector<RowsetReaderSharedPtr> readers;
        readers.reserve(rowsets.size());
        for (const auto& rowset : rowsets) {
            RowsetReaderSharedPtr reader;
            EXPECT_TRUE(rowset->create_reader(&reader).ok());
            readers.push_back(std::move(reader));
        }
        return readers;
    }

    Status append_json_batch(ColumnWriter* writer, const std::vector<std::string>& jsons) {
        if (writer == nullptr) {
            return Status::InvalidArgument("writer is null");
        }

        Block block = _tablet_schema->create_block();
        auto columns = std::move(block).mutate_columns();
        auto json_col = ColumnString::create();
        for (const auto& json : jsons) {
            json_col->insert_data(json.data(), json.size());
        }
        columns[0] = encode_json_column_v2(*json_col);
        block.set_columns(std::move(columns));

        auto converter = std::make_unique<OlapBlockDataConvertor>();
        converter->add_column_data_convertor(_tablet_schema->column(0));
        converter->set_source_content(&block, 0, jsons.size());
        auto [status, accessor] = converter->convert_column_data(0);
        RETURN_IF_ERROR(status);
        return writer->append(accessor->get_nullmap(), accessor->get_data(), jsons.size());
    }

    Status read_root_rows(const SegmentFooterPB& footer, const std::string& file_path,
                          std::vector<std::string>* out_rows) {
        io::FileReaderSPtr file_reader;
        RETURN_IF_ERROR(io::global_local_filesystem()->open_file(file_path, &file_reader));

        std::shared_ptr<ColumnReader> column_reader;
        RETURN_IF_ERROR(
                create_variant_root_reader(footer, file_reader, _tablet_schema, &column_reader));

        auto* variant_column_reader = assert_cast<VariantColumnReader*>(column_reader.get());
        MockColumnReaderCache column_reader_cache(footer, file_reader, _tablet_schema);

        TabletColumn parent_column = _tablet_schema->column(0);
        StorageReadOptions storage_read_opts;
        storage_read_opts.io_ctx.reader_type = ReaderType::READER_QUERY;
        OlapReaderStatistics stats;
        storage_read_opts.stats = &stats;

        ColumnIteratorUPtr iterator;
        RETURN_IF_ERROR(variant_column_reader->new_iterator(
                &iterator, &parent_column, &storage_read_opts, &column_reader_cache));

        ColumnIteratorOptions column_iter_opts;
        column_iter_opts.stats = &stats;
        column_iter_opts.file_reader = file_reader.get();
        RETURN_IF_ERROR(iterator->init(column_iter_opts));

        MutableColumnPtr dst = ColumnVariantV2::create();
        size_t nrows = footer.num_rows();
        RETURN_IF_ERROR(iterator->seek_to_ordinal(0));
        RETURN_IF_ERROR(iterator->next_batch(&nrows, dst));

        out_rows->clear();
        out_rows->reserve(nrows);
        for (size_t i = 0; i < nrows; ++i) {
            out_rows->push_back(variant_json_at(*dst, i));
        }
        return Status::OK();
    }

    Status read_variant_path_rows(const SegmentFooterPB& footer, const std::string& file_path,
                                  std::string_view relative_path, FieldType field_type,
                                  std::vector<std::string>* out_rows) {
        io::FileReaderSPtr file_reader;
        RETURN_IF_ERROR(io::global_local_filesystem()->open_file(file_path, &file_reader));

        std::shared_ptr<ColumnReader> column_reader;
        RETURN_IF_ERROR(
                create_variant_root_reader(footer, file_reader, _tablet_schema, &column_reader));

        auto* variant_column_reader = assert_cast<VariantColumnReader*>(column_reader.get());
        MockColumnReaderCache column_reader_cache(footer, file_reader, _tablet_schema);

        const TabletColumn& parent_column = _tablet_schema->column(0);
        const std::string full_path =
                parent_column.name_lower_case() + "." + std::string(relative_path);
        TabletColumn path_column;
        path_column.set_name(full_path);
        path_column.set_type(field_type);
        path_column.set_parent_unique_id(parent_column.unique_id());
        path_column.set_path_info(PathInData(full_path));
        path_column.set_is_nullable(true);

        StorageReadOptions storage_read_opts;
        storage_read_opts.io_ctx.reader_type = ReaderType::READER_QUERY;
        OlapReaderStatistics stats;
        storage_read_opts.stats = &stats;

        DataTypePtr storage_type;
        RETURN_IF_ERROR(variant_column_reader->infer_data_type_for_path(
                &storage_type, path_column, storage_read_opts, &column_reader_cache));

        ColumnIteratorUPtr iterator;
        RETURN_IF_ERROR(variant_column_reader->new_iterator(
                &iterator, &path_column, &storage_read_opts, &column_reader_cache));

        ColumnIteratorOptions column_iter_opts;
        column_iter_opts.stats = &stats;
        column_iter_opts.file_reader = file_reader.get();
        RETURN_IF_ERROR(iterator->init(column_iter_opts));

        MutableColumnPtr dst = storage_type->create_column();
        size_t nrows = footer.num_rows();
        RETURN_IF_ERROR(iterator->seek_to_ordinal(0));
        RETURN_IF_ERROR(iterator->next_batch(&nrows, dst));

        auto data_type = DataTypeFactory::instance().create_data_type(path_column, false);
        ColumnPtr result = dst->get_ptr();
        if (!storage_type->equals(*data_type)) {
            RETURN_IF_ERROR(variant_util::cast_column({result, storage_type, path_column.name()},
                                                      data_type, &result));
        }
        out_rows->clear();
        out_rows->reserve(nrows);
        for (size_t i = 0; i < nrows; ++i) {
            out_rows->push_back(data_type->to_string(*result, i));
        }
        return Status::OK();
    }

    Status write_v2_segment(const std::vector<std::string>& jsons, std::string_view rowset_id,
                            SegmentFooterPB* footer, std::string* file_path,
                            bool write_inverted_index = false) {
        if (footer == nullptr || file_path == nullptr) {
            return Status::InvalidArgument("footer or file_path is null");
        }
        const size_t num_rows = jsons.size();
        *file_path = local_segment_path(_tablet->tablet_path(), rowset_id, 0);
        static_cast<void>(io::global_local_filesystem()->delete_file(*file_path));

        io::FileWriterPtr file_writer;
        RETURN_IF_ERROR(io::global_local_filesystem()->create_file(*file_path, &file_writer));

        std::unique_ptr<segment_v2::IndexFileWriter> index_file_writer;
        if (_tablet_schema->has_inverted_index()) {
            const std::string index_path_prefix = std::string(
                    segment_v2::InvertedIndexDescriptor::get_index_file_path_prefix(*file_path));
            io::FileWriterPtr index_v2_file_writer;
            if (_tablet_schema->get_inverted_index_storage_format() !=
                InvertedIndexStorageFormatPB::V1) {
                RETURN_IF_ERROR(io::global_local_filesystem()->create_file(
                        segment_v2::InvertedIndexDescriptor::get_index_file_path_v2(
                                index_path_prefix),
                        &index_v2_file_writer));
            }
            index_file_writer = std::make_unique<segment_v2::IndexFileWriter>(
                    io::global_local_filesystem(), index_path_prefix, std::string(rowset_id),
                    0 /* seg_id */, _tablet_schema->get_inverted_index_storage_format(),
                    std::move(index_v2_file_writer), true /* can_use_ram_dir */,
                    _tablet->tablet_id());
        }

        footer->Clear();
        RowsetWriterContext rowset_ctx;
        rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
        rowset_ctx.tablet_schema = _tablet_schema;
        rowset_ctx.tablet = _tablet;
        rowset_ctx.tablet_path = _tablet->tablet_path();

        TabletColumn parent_column = _tablet_schema->column(0);
        ColumnWriterOptions opts;
        opts.meta = footer->add_columns();
        opts.index_file_writer = index_file_writer.get();
        opts.compression_type = CompressionTypePB::LZ4;
        opts.file_writer = file_writer.get();
        opts.footer = footer;
        opts.rowset_ctx = &rowset_ctx;
        opts.storage_format = TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2;
        _init_column_meta(opts.meta, 0, parent_column, opts);

        std::unique_ptr<ColumnWriter> writer;
        RETURN_IF_ERROR(ColumnWriter::create(opts, &parent_column, file_writer.get(), &writer));
        RETURN_IF_ERROR(writer->init());

        Block block = _tablet_schema->create_block();
        auto columns = std::move(block).mutate_columns();
        auto json_column = ColumnString::create();
        for (const auto& json : jsons) {
            json_column->insert_data(json.data(), json.size());
        }
        columns[0] = encode_json_column_v2(*json_column);
        block.set_columns(std::move(columns));

        auto converter = std::make_unique<OlapBlockDataConvertor>();
        converter->add_column_data_convertor(parent_column);
        converter->set_source_content(&block, 0, num_rows);
        auto [convert_status, accessor] = converter->convert_column_data(0);
        RETURN_IF_ERROR(convert_status);
        RETURN_IF_ERROR(writer->append(accessor->get_nullmap(), accessor->get_data(), num_rows));

        RETURN_IF_ERROR(writer->finish());
        RETURN_IF_ERROR(writer->write_data());
        RETURN_IF_ERROR(writer->write_ordinal_index());
        RETURN_IF_ERROR(writer->write_zone_map());
        if (write_inverted_index) {
            RETURN_IF_ERROR(writer->write_inverted_index());
            if (index_file_writer != nullptr) {
                RETURN_IF_ERROR(index_file_writer->begin_close());
                RETURN_IF_ERROR(index_file_writer->finish_close());
            }
        }
        RETURN_IF_ERROR(file_writer->close());
        footer->set_num_rows(num_rows);
        return Status::OK();
    }

    TabletSchemaSPtr _tablet_schema = nullptr;
    StorageEngine* _engine_ref = nullptr;
    std::unique_ptr<DataDir> _data_dir = nullptr;
    TabletSharedPtr _tablet = nullptr;
    std::string _absolute_dir;
    std::string _current_dir;
};

} // namespace doris
