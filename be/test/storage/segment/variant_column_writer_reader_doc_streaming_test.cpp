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

#include "exec/common/variant_util.h"
#include "storage/segment/variant/variant_doc_snpashot_compact_iterator.h"
#include "storage/segment/variant_column_writer_reader_test_fixture.h"
#include "testutil/variant_util.h"

namespace doris {

static bool nested_group_write_path_available() {
    auto provider = segment_v2::create_nested_group_read_provider();
    return provider != nullptr && provider->should_enable_nested_group_read_path();
}

static void fill_variant_column_v2(MutableColumnPtr& column_object, int num_rows,
                                   std::unordered_map<int, std::string>* inserted) {
    auto type_string = std::make_shared<DataTypeString>();
    auto json_column = type_string->create_column();
    auto* column_string = assert_cast<ColumnString*>(json_column.get());
    VariantUtil::fill_string_column_with_test_data(column_string, num_rows, inserted);

    auto encoded = encode_json_column_v2(*column_string);
    column_object->insert_range_from(*encoded, 0, encoded->size());
}

// DOC_COMPACT reads only one doc bucket column (e.g. "__DORIS_VARIANT_DOC_VALUE__.b0"), so it
// naturally returns only the subset of keys mapped into that bucket.
// This helper derives the expected JSON string for a given bucket from the full JSON produced by
// VariantUtil::fill_string_column_with_test_data, without parsing JSON.
static std::string expected_doc_bucket_json_from_full(const std::string& full_json, int bucket_num,
                                                      int bucket_index) {
    auto bucket_of = [&](const std::string& key) -> uint32_t {
        StringRef ref {key.data(), key.size()};
        return variant_util::variant_binary_shard_of(ref, bucket_num);
    };

    std::string out;
    out.reserve(full_json.size());
    out.push_back('{');

    bool first = true;
    // fill_string_column_with_test_data generates keys "key0".."key9" at most.
    for (int j = 0; j < 10; ++j) {
        const std::string key = "key" + std::to_string(j);
        const std::string needle = "\"" + key + "\":";
        if (full_json.find(needle) == std::string::npos) {
            continue;
        }
        if (bucket_of(key) != static_cast<uint32_t>(bucket_index)) {
            continue;
        }
        if (!first) {
            out.push_back(',');
        }
        first = false;
        out.append("\"");
        out.append(key);
        out.append("\":");
        if (j % 2 == 0) {
            out.append("88");
        } else {
            out.append("\"str99\"");
        }
    }

    out.push_back('}');
    return out;
}

static std::set<std::string> collect_regular_paths(
        const segment_v2::NestedGroupStreamingWritePlan& plan) {
    std::set<std::string> paths;
    for (const auto& entry : plan.regular_subcolumns) {
        paths.insert(entry.path);
    }
    return paths;
}

TEST_F(VariantColumnWriterReaderTest, test_write_doc_and_read_hierarchical_doc) {
    constexpr int kRows = 200;
    constexpr int kDocBuckets = 2;

    // 1. create tablet_schema (enable doc mode, small shard count to keep footer small)
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1", 3, false, false,
                     /*variant_sparse_hash_shard_count=*/0,
                     /*variant_enable_doc_mode=*/true,
                     /*variant_doc_materialization_min_rows=*/100000,
                     /*variant_doc_hash_shard_count=*/kDocBuckets);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    // 2. create tablet
    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    bool external_segment_meta_used_default = false;
    _tablet_schema->set_storage_format(external_segment_meta_used_default
                                               ? TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V3
                                               : TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2);
    tablet_meta->_tablet_id = 31000;
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    EXPECT_TRUE(_tablet->init().ok());
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    // 3. create file_writer
    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    EXPECT_TRUE(st.ok()) << st.msg();

    // 4. create variant writer
    SegmentFooterPB footer;
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    opts.rowset_ctx = &rowset_ctx;
    opts.rowset_ctx->tablet_schema = _tablet_schema;
    TabletColumn parent_column = _tablet_schema->column(0);
    opts.compression_type = CompressionTypePB::LZ4;
    opts.storage_format = TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2;
    _init_column_meta(opts.meta, 0, parent_column, opts);

    std::unique_ptr<ColumnWriter> writer;
    EXPECT_TRUE(ColumnWriter::create(opts, &parent_column, file_writer.get(), &writer).ok());
    EXPECT_TRUE(writer->init().ok());
    EXPECT_TRUE(assert_cast<VariantColumnWriter*>(writer.get()) != nullptr);

    // 5. write doc-value-only data into variant
    auto olap_data_convertor = std::make_unique<OlapBlockDataConvertor>();
    auto block = _tablet_schema->create_block();
    auto column_object = (*std::move(block.get_by_position(0).column)).mutate();
    std::unordered_map<int, std::string> inserted_jsonstr;
    fill_variant_column_v2(column_object, kRows, &inserted_jsonstr);
    olap_data_convertor->add_column_data_convertor(parent_column);
    olap_data_convertor->set_source_content(&block, 0, kRows);
    auto [result, accessor] = olap_data_convertor->convert_column_data(0);
    EXPECT_TRUE(result.ok());
    EXPECT_TRUE(accessor != nullptr);
    EXPECT_TRUE(writer->append(accessor->get_nullmap(), accessor->get_data(), kRows).ok());
    st = writer->finish();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_data();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_ordinal_index();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_zone_map();
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(file_writer->close().ok());
    footer.set_num_rows(kRows);

    // 6. validate footer contains doc snapshot bucket columns and per-bucket stats
    EXPECT_EQ(footer.columns_size(), 1 + kDocBuckets);
    for (int i = 1; i < footer.columns_size(); ++i) {
        const auto& col = footer.columns(i);
        EXPECT_TRUE(col.has_column_path_info());
        PathInData path;
        path.from_protobuf(col.column_path_info());
        auto rel = path.copy_pop_front().get_path();
        EXPECT_TRUE(rel.find(DOC_VALUE_COLUMN_PATH) != std::string::npos) << rel;
        EXPECT_TRUE(col.has_variant_statistics());
        EXPECT_GT(col.variant_statistics().doc_value_column_non_null_size_size(), 0);
    }

    // 7. open a VariantColumnReader on this segment
    io::FileReaderSPtr file_reader;
    st = io::global_local_filesystem()->open_file(file_path, &file_reader);
    EXPECT_TRUE(st.ok()) << st.msg();
    std::shared_ptr<ColumnReader> column_reader;
    st = create_variant_root_reader(footer, file_reader, _tablet_schema, &column_reader);
    EXPECT_TRUE(st.ok()) << st.msg();
    auto* variant_column_reader = assert_cast<VariantColumnReader*>(column_reader.get());
    EXPECT_TRUE(variant_column_reader != nullptr);
    EXPECT_TRUE(variant_column_reader->get_stats()->has_doc_column_non_null_size());
    EXPECT_TRUE(variant_column_reader->get_subcolumn_meta_by_path(PathInData("key0")) == nullptr);

    MockColumnReaderCache column_reader_cache(footer, file_reader, _tablet_schema);

    // 8. Read root with QUERY reader type: should choose ReadKind::HIERARCHICAL_DOC
    StorageReadOptions storage_read_opts;
    storage_read_opts.io_ctx.reader_type = ReaderType::READER_QUERY;
    OlapReaderStatistics stats;
    storage_read_opts.stats = &stats;
    ColumnIteratorUPtr it;
    st = variant_column_reader->new_iterator(&it, &parent_column, &storage_read_opts,
                                             &column_reader_cache);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(dynamic_cast<HierarchicalDataIterator*>(it.get()) != nullptr);
    EXPECT_EQ(stats.variant_doc_value_column_iter_count, 1);

    ColumnIteratorOptions column_iter_opts;
    column_iter_opts.stats = &stats;
    column_iter_opts.file_reader = file_reader.get();
    st = it->init(column_iter_opts);
    EXPECT_TRUE(st.ok()) << st.msg();

    MutableColumnPtr dst = ColumnVariantV2::create();
    size_t nrows = kRows;
    st = it->seek_to_ordinal(0);
    EXPECT_TRUE(st.ok()) << st.msg();
    st = it->next_batch(&nrows, dst);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_EQ(nrows, kRows);

    for (int i = 0; i < kRows; ++i) {
        EXPECT_EQ(variant_json_at(*dst, i), inserted_jsonstr[i]);
    }
}

TEST_F(VariantColumnWriterReaderTest,
       test_write_doc_materialized_by_min_rows_and_read_metadata_and_data) {
    constexpr int kRows = 200;
    constexpr int kDocBuckets = 2;

    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1", 3, false, false,
                     /*variant_sparse_hash_shard_count=*/0,
                     /*variant_enable_doc_mode=*/true,
                     /*variant_doc_materialization_min_rows=*/0,
                     /*variant_doc_hash_shard_count=*/kDocBuckets);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    _tablet_schema->set_storage_format(TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2);
    tablet_meta->_tablet_id = 31002;
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    EXPECT_TRUE(_tablet->init().ok());
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    EXPECT_TRUE(st.ok()) << st.msg();

    SegmentFooterPB footer;
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    opts.rowset_ctx = &rowset_ctx;
    opts.rowset_ctx->tablet_schema = _tablet_schema;
    TabletColumn parent_column = _tablet_schema->column(0);
    opts.compression_type = CompressionTypePB::LZ4;
    opts.storage_format = TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2;
    _init_column_meta(opts.meta, 0, parent_column, opts);

    std::unique_ptr<ColumnWriter> writer;
    EXPECT_TRUE(ColumnWriter::create(opts, &parent_column, file_writer.get(), &writer).ok());
    EXPECT_TRUE(writer->init().ok());

    auto olap_data_convertor = std::make_unique<OlapBlockDataConvertor>();
    auto block = _tablet_schema->create_block();
    auto column_object = (*std::move(block.get_by_position(0).column)).mutate();
    std::unordered_map<int, std::string> inserted_jsonstr;
    fill_variant_column_v2(column_object, kRows, &inserted_jsonstr);
    olap_data_convertor->add_column_data_convertor(parent_column);
    olap_data_convertor->set_source_content(&block, 0, kRows);
    auto [result, accessor] = olap_data_convertor->convert_column_data(0);
    EXPECT_TRUE(result.ok());
    EXPECT_TRUE(accessor != nullptr);
    EXPECT_TRUE(writer->append(accessor->get_nullmap(), accessor->get_data(), kRows).ok());
    st = writer->finish();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_data();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_ordinal_index();
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(file_writer->close().ok());
    footer.set_num_rows(kRows);

    EXPECT_GT(footer.columns_size(), 1 + kDocBuckets);

    io::FileReaderSPtr file_reader;
    st = io::global_local_filesystem()->open_file(file_path, &file_reader);
    EXPECT_TRUE(st.ok()) << st.msg();
    std::shared_ptr<ColumnReader> column_reader;
    st = create_variant_root_reader(footer, file_reader, _tablet_schema, &column_reader);
    EXPECT_TRUE(st.ok()) << st.msg();
    auto* variant_column_reader = assert_cast<VariantColumnReader*>(column_reader.get());
    EXPECT_TRUE(variant_column_reader != nullptr);
    EXPECT_TRUE(variant_column_reader->get_subcolumn_meta_by_path(PathInData("key0")) != nullptr);

    MockColumnReaderCache column_reader_cache(footer, file_reader, _tablet_schema);

    StorageReadOptions query_read_opts;
    query_read_opts.io_ctx.reader_type = ReaderType::READER_QUERY;
    OlapReaderStatistics query_stats;
    query_read_opts.stats = &query_stats;
    ColumnIteratorUPtr root_it;
    st = variant_column_reader->new_iterator(&root_it, &parent_column, &query_read_opts,
                                             &column_reader_cache);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(dynamic_cast<HierarchicalDataIterator*>(root_it.get()) != nullptr);

    ColumnIteratorOptions root_iter_opts;
    root_iter_opts.stats = &query_stats;
    root_iter_opts.file_reader = file_reader.get();
    st = root_it->init(root_iter_opts);
    EXPECT_TRUE(st.ok()) << st.msg();

    MutableColumnPtr dst = ColumnVariantV2::create();
    size_t nrows = kRows;
    st = root_it->seek_to_ordinal(0);
    EXPECT_TRUE(st.ok()) << st.msg();
    st = root_it->next_batch(&nrows, dst);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_EQ(nrows, kRows);
    for (int i = 0; i < kRows; ++i) {
        EXPECT_EQ(variant_json_at(*dst, i), inserted_jsonstr[i]);
    }

    StorageReadOptions compact_read_opts;
    compact_read_opts.io_ctx.reader_type = ReaderType::READER_BASE_COMPACTION;
    compact_read_opts.tablet_schema = _tablet_schema;
    OlapReaderStatistics compact_stats;
    compact_read_opts.stats = &compact_stats;
    TabletColumn doc_bucket_col = variant_util::create_doc_value_column(parent_column, 0);
    ColumnIteratorUPtr bucket_it;
    st = variant_column_reader->new_iterator(&bucket_it, &doc_bucket_col, &compact_read_opts,
                                             &column_reader_cache);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(bucket_it != nullptr);

    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
}

// Regression: materialized subcolumns in V3 doc-mode tablets must inherit the parent's
// storage_format and resolve V3 default encodings (e.g. integer family = PLAIN, not BIT_SHUFFLE).
// Without propagating base_opts.storage_format into the per-subcolumn ColumnWriterOptions,
// `_init_column_meta` falls back to the V2 default map and writes V2 encodings even for V3
// tablets, defeating the storage-format-based encoding policy.
TEST_F(VariantColumnWriterReaderTest, test_write_doc_materialized_v3_uses_v3_encoding) {
    constexpr int kRows = 200;
    constexpr int kDocBuckets = 2;

    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1", 3, false, false,
                     /*variant_sparse_hash_shard_count=*/0,
                     /*variant_enable_doc_mode=*/true,
                     /*variant_doc_materialization_min_rows=*/0,
                     /*variant_doc_hash_shard_count=*/kDocBuckets);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    _tablet_schema->set_storage_format(TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V3);
    tablet_meta->_tablet_id = 31003;
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    EXPECT_TRUE(_tablet->init().ok());
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    EXPECT_TRUE(st.ok()) << st.msg();

    SegmentFooterPB footer;
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    opts.rowset_ctx = &rowset_ctx;
    opts.rowset_ctx->tablet_schema = _tablet_schema;
    TabletColumn parent_column = _tablet_schema->column(0);
    opts.storage_format = TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V3;
    _init_column_meta(opts.meta, 0, parent_column, opts);

    std::unique_ptr<ColumnWriter> writer;
    EXPECT_TRUE(ColumnWriter::create(opts, &parent_column, file_writer.get(), &writer).ok());
    EXPECT_TRUE(writer->init().ok());

    auto olap_data_convertor = std::make_unique<OlapBlockDataConvertor>();
    auto block = _tablet_schema->create_block();
    auto column_object = (*std::move(block.get_by_position(0).column)).mutate();
    std::unordered_map<int, std::string> inserted_jsonstr;
    fill_variant_column_v2(column_object, kRows, &inserted_jsonstr);
    olap_data_convertor->add_column_data_convertor(parent_column);
    olap_data_convertor->set_source_content(&block, 0, kRows);
    auto [result, accessor] = olap_data_convertor->convert_column_data(0);
    EXPECT_TRUE(result.ok());
    EXPECT_TRUE(accessor != nullptr);
    EXPECT_TRUE(writer->append(accessor->get_nullmap(), accessor->get_data(), kRows).ok());
    EXPECT_TRUE(writer->finish().ok());
    EXPECT_TRUE(writer->write_data().ok());
    EXPECT_TRUE(writer->write_ordinal_index().ok());
    EXPECT_TRUE(file_writer->close().ok());

    // Materialization must have produced extra subcolumns beyond the doc-bucket columns.
    EXPECT_GT(footer.columns_size(), 1 + kDocBuckets) << "no subcolumns were materialized";

    // Locate materialized subcolumns. Doc bucket columns have DOC_VALUE_COLUMN_PATH in their
    // path; everything else (other than the root variant at index 0) is a materialized subcolumn.
    int integer_subcolumns_checked = 0;
    int string_subcolumns_checked = 0;
    for (int i = 1; i < footer.columns_size(); ++i) {
        const auto& col = footer.columns(i);
        if (!col.has_column_path_info()) continue;
        PathInData path;
        path.from_protobuf(col.column_path_info());
        std::string rel = path.copy_pop_front().get_path();
        if (rel.find(DOC_VALUE_COLUMN_PATH) != std::string::npos) continue;
        const auto field_type = static_cast<FieldType>(col.type());
        switch (field_type) {
        case FieldType::OLAP_FIELD_TYPE_TINYINT:
        case FieldType::OLAP_FIELD_TYPE_SMALLINT:
        case FieldType::OLAP_FIELD_TYPE_INT:
        case FieldType::OLAP_FIELD_TYPE_BIGINT:
        case FieldType::OLAP_FIELD_TYPE_LARGEINT:
            EXPECT_EQ(col.encoding(), EncodingTypePB::PLAIN_ENCODING)
                    << "V3 integer subcolumn '" << rel << "' got "
                    << EncodingTypePB_Name(col.encoding()) << " instead of PLAIN_ENCODING";
            ++integer_subcolumns_checked;
            break;
        case FieldType::OLAP_FIELD_TYPE_CHAR:
        case FieldType::OLAP_FIELD_TYPE_VARCHAR:
        case FieldType::OLAP_FIELD_TYPE_STRING:
            EXPECT_EQ(col.encoding(), EncodingTypePB::DICT_ENCODING)
                    << "V3 string subcolumn '" << rel << "' got "
                    << EncodingTypePB_Name(col.encoding()) << " instead of DICT_ENCODING";
            ++string_subcolumns_checked;
            break;
        default:
            break;
        }
    }
    EXPECT_GT(integer_subcolumns_checked + string_subcolumns_checked, 0)
            << "no scalar materialized subcolumns were found to verify";

    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
}

TEST_F(VariantColumnWriterReaderTest, test_read_doc_compact_from_doc_value_bucket) {
    constexpr int kRows = 200;
    constexpr int kDocBuckets = 4;

    // 1. create tablet_schema (enable doc mode)
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1", 3, false, false,
                     /*variant_sparse_hash_shard_count=*/0,
                     /*variant_enable_doc_mode=*/true,
                     /*variant_doc_materialization_min_rows=*/0,
                     /*variant_doc_hash_shard_count=*/kDocBuckets);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    // 2. create tablet
    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    _tablet_schema->set_storage_format(TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2);
    tablet_meta->_tablet_id = 32000;
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    EXPECT_TRUE(_tablet->init().ok());
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    // 3. write doc-value-only segment
    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    EXPECT_TRUE(st.ok()) << st.msg();

    SegmentFooterPB footer;
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    opts.rowset_ctx = &rowset_ctx;
    opts.rowset_ctx->tablet_schema = _tablet_schema;
    TabletColumn parent_column = _tablet_schema->column(0);
    opts.compression_type = CompressionTypePB::LZ4;
    opts.storage_format = TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2;
    _init_column_meta(opts.meta, 0, parent_column, opts);

    std::unique_ptr<ColumnWriter> writer;
    EXPECT_TRUE(ColumnWriter::create(opts, &parent_column, file_writer.get(), &writer).ok());
    EXPECT_TRUE(writer->init().ok());

    auto olap_data_convertor = std::make_unique<OlapBlockDataConvertor>();
    auto block = _tablet_schema->create_block();
    auto column_object = (*std::move(block.get_by_position(0).column)).mutate();
    std::unordered_map<int, std::string> inserted_jsonstr;
    fill_variant_column_v2(column_object, kRows, &inserted_jsonstr);
    olap_data_convertor->add_column_data_convertor(parent_column);
    olap_data_convertor->set_source_content(&block, 0, kRows);
    auto [result, accessor] = olap_data_convertor->convert_column_data(0);
    EXPECT_TRUE(result.ok());
    EXPECT_TRUE(accessor != nullptr);
    EXPECT_TRUE(writer->append(accessor->get_nullmap(), accessor->get_data(), kRows).ok());
    st = writer->finish();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_data();
    EXPECT_TRUE(st.ok()) << st.msg();
    st = writer->write_ordinal_index();
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(file_writer->close().ok());
    footer.set_num_rows(kRows);

    // 4. open reader
    io::FileReaderSPtr file_reader;
    st = io::global_local_filesystem()->open_file(file_path, &file_reader);
    EXPECT_TRUE(st.ok()) << st.msg();
    std::shared_ptr<ColumnReader> column_reader;
    st = create_variant_root_reader(footer, file_reader, _tablet_schema, &column_reader);
    EXPECT_TRUE(st.ok()) << st.msg();
    auto* variant_column_reader = assert_cast<VariantColumnReader*>(column_reader.get());
    EXPECT_TRUE(variant_column_reader != nullptr);

    MockColumnReaderCache column_reader_cache(footer, file_reader, _tablet_schema);

    // 5. trigger flat-leaf planning by using compaction reader type + schema with extracted columns
    auto compaction_schema = std::make_shared<TabletSchema>();
    compaction_schema->init_from_pb(schema_pb);
    TabletColumn extracted;
    extracted.set_name(parent_column.name_lower_case() + ".dummy");
    extracted.set_type(FieldType::OLAP_FIELD_TYPE_INT);
    extracted.set_parent_unique_id(parent_column.unique_id());
    extracted.set_path_info(PathInData(parent_column.name_lower_case() + ".dummy"));
    extracted.set_is_nullable(true);
    compaction_schema->append_column(extracted);

    StorageReadOptions storage_read_opts;
    storage_read_opts.io_ctx.reader_type = ReaderType::READER_BASE_COMPACTION;
    storage_read_opts.tablet_schema = compaction_schema;
    OlapReaderStatistics stats;
    storage_read_opts.stats = &stats;

    ColumnIteratorUPtr root_it;
    st = variant_column_reader->new_iterator(&root_it, &parent_column, &storage_read_opts,
                                             &column_reader_cache);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(dynamic_cast<VariantRootColumnIterator*>(root_it.get()) != nullptr);

    // 6. Read and validate each doc value bucket column: should choose ReadKind::DOC_COMPACT.
    for (int bucket = 0; bucket < kDocBuckets; ++bucket) {
        TabletColumn doc_bucket_col = variant_util::create_doc_value_column(parent_column, bucket);
        ColumnIteratorUPtr it;
        st = variant_column_reader->new_iterator(&it, &doc_bucket_col, &storage_read_opts,
                                                 &column_reader_cache);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_TRUE(dynamic_cast<segment_v2::VariantDocValueCompactIterator*>(it.get()) != nullptr);

        ColumnIteratorOptions column_iter_opts;
        column_iter_opts.stats = &stats;
        column_iter_opts.file_reader = file_reader.get();
        st = it->init(column_iter_opts);
        EXPECT_TRUE(st.ok()) << st.msg();

        MutableColumnPtr dst = ColumnVariantV2::create();
        size_t nrows = kRows;
        st = it->seek_to_ordinal(0);
        EXPECT_TRUE(st.ok()) << st.msg();
        st = it->next_batch(&nrows, dst);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_EQ(nrows, kRows);

        for (int i = 0; i < kRows; ++i) {
            const std::string expected =
                    expected_doc_bucket_json_from_full(inserted_jsonstr[i], kDocBuckets, bucket);
            EXPECT_EQ(variant_json_at(*dst, i), expected);
        }
    }
}

TEST_F(VariantColumnWriterReaderTest, test_write_doc_compact_writer_and_read_doc_compact) {
    constexpr int kRows = 200;
    constexpr int kDocBuckets = 4;
    constexpr int kBucket = 0;

    // 1. create tablet_schema: root variant is in doc mode; plus one extracted doc bucket column
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1", 3, false, false,
                     /*variant_sparse_hash_shard_count=*/0,
                     /*variant_enable_doc_mode=*/true,
                     /*variant_doc_materialization_min_rows=*/0,
                     /*variant_doc_hash_shard_count=*/kDocBuckets);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    TabletColumn parent_column = _tablet_schema->column(0);
    TabletColumn extracted_doc_bucket =
            variant_util::create_doc_value_column(parent_column, kBucket);
    // This matches VariantCompactionUtil::get_extended_compaction_schema behavior:
    // extracted doc bucket columns are represented as VARIANT to trigger VariantDocCompactWriter.
    extracted_doc_bucket.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);
    extracted_doc_bucket.set_is_nullable(false);
    _tablet_schema->append_column(extracted_doc_bucket);

    // 2. create tablet
    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    _tablet_schema->set_storage_format(TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2);
    tablet_meta->_tablet_id = 33000;
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    EXPECT_TRUE(_tablet->init().ok());
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    // 3. create file_writer
    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    EXPECT_TRUE(st.ok()) << st.msg();

    // 4. create column writers: root VariantColumnWriter + extracted VariantDocCompactWriter
    SegmentFooterPB footer;

    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    rowset_ctx.tablet_schema = _tablet_schema;

    ColumnWriterOptions root_opts;
    root_opts.meta = footer.add_columns();
    root_opts.compression_type = CompressionTypePB::LZ4;
    root_opts.file_writer = file_writer.get();
    root_opts.footer = &footer;
    root_opts.rowset_ctx = &rowset_ctx;
    root_opts.compression_type = CompressionTypePB::LZ4;
    root_opts.storage_format = TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2;
    _init_column_meta(root_opts.meta, 0, parent_column, root_opts);

    std::unique_ptr<ColumnWriter> root_writer;
    EXPECT_TRUE(
            ColumnWriter::create(root_opts, &parent_column, file_writer.get(), &root_writer).ok());
    EXPECT_TRUE(root_writer->init().ok());

    TabletColumn extracted_doc_bucket_col = _tablet_schema->column(1);
    ColumnWriterOptions doc_compact_opts = root_opts;
    doc_compact_opts.meta = footer.add_columns();
    doc_compact_opts.compression_type = CompressionTypePB::LZ4;
    doc_compact_opts.storage_format = TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2;
    _init_column_meta(doc_compact_opts.meta, 0, extracted_doc_bucket_col, doc_compact_opts);
    std::unique_ptr<ColumnWriter> doc_compact_writer;
    EXPECT_TRUE(ColumnWriter::create(doc_compact_opts, &extracted_doc_bucket_col, file_writer.get(),
                                     &doc_compact_writer)
                        .ok());
    EXPECT_TRUE(doc_compact_writer->init().ok());

    // 5. build doc-value-only data:
    // - root column uses the full JSON (doc values only is enough for this test)
    // - extracted doc bucket column uses bucket-filtered JSON so that doc bucket data matches
    //   the bucket index expected by VariantDocCompactWriter.
    std::unordered_map<int, std::string> inserted_full_json;
    auto type_string = std::make_shared<DataTypeString>();
    auto full_json_column = type_string->create_column();
    auto* full_strings = assert_cast<ColumnString*>(full_json_column.get());
    VariantUtil::fill_string_column_with_test_data(full_strings, kRows, &inserted_full_json);

    std::unordered_map<int, std::string> expected_bucket_json;
    auto bucket_json_column = type_string->create_column();
    auto* bucket_strings = assert_cast<ColumnString*>(bucket_json_column.get());
    for (int i = 0; i < kRows; ++i) {
        const std::string& full = inserted_full_json[i];
        std::string bucket_json = expected_doc_bucket_json_from_full(full, kDocBuckets, kBucket);
        expected_bucket_json.emplace(i, bucket_json);
        bucket_strings->insert_data(bucket_json.data(), bucket_json.size());
    }

    auto root_variant = encode_json_column_v2(*full_strings);
    auto bucket_variant = encode_json_column_v2(*bucket_strings);

    // 6. append and write
    {
        auto root_data = std::make_unique<VariantColumnData>();
        root_data->column_data = root_variant.get();
        root_data->row_pos = 0;
        const auto* data = reinterpret_cast<const uint8_t*>(root_data.get());
        EXPECT_TRUE(root_writer->append_data(&data, kRows).ok());
    }
    {
        auto bucket_data = std::make_unique<VariantColumnData>();
        bucket_data->column_data = bucket_variant.get();
        bucket_data->row_pos = 0;
        const auto* data = reinterpret_cast<const uint8_t*>(bucket_data.get());
        EXPECT_TRUE(doc_compact_writer->append_data(&data, kRows).ok());
    }

    EXPECT_TRUE(root_writer->finish().ok());
    EXPECT_TRUE(doc_compact_writer->finish().ok());
    EXPECT_TRUE(root_writer->write_data().ok());
    EXPECT_TRUE(doc_compact_writer->write_data().ok());
    EXPECT_TRUE(root_writer->write_ordinal_index().ok());
    EXPECT_TRUE(doc_compact_writer->write_ordinal_index().ok());
    EXPECT_TRUE(file_writer->close().ok());
    footer.set_num_rows(kRows);

    // 7. open reader and validate:
    // - doc bucket can be read via DOC_COMPACT iterator in flat-leaf compaction mode
    // - materialized leaf meta exists for at least one key in this bucket
    io::FileReaderSPtr file_reader;
    st = io::global_local_filesystem()->open_file(file_path, &file_reader);
    EXPECT_TRUE(st.ok()) << st.msg();
    std::shared_ptr<ColumnReader> column_reader;
    st = create_variant_root_reader(footer, file_reader, _tablet_schema, &column_reader);
    EXPECT_TRUE(st.ok()) << st.msg();
    auto* variant_column_reader = assert_cast<VariantColumnReader*>(column_reader.get());
    EXPECT_TRUE(variant_column_reader != nullptr);

    bool checked_one_key = false;
    for (int j = 0; j < 10; ++j) {
        const std::string key = "key" + std::to_string(j);
        StringRef ref {key.data(), key.size()};
        if (variant_util::variant_binary_shard_of(ref, kDocBuckets) ==
            static_cast<uint32_t>(kBucket)) {
            EXPECT_TRUE(variant_column_reader->get_subcolumn_meta_by_path(PathInData(key)) !=
                        nullptr);
            checked_one_key = true;
            break;
        }
    }
    EXPECT_TRUE(checked_one_key);

    MockColumnReaderCache column_reader_cache(footer, file_reader, _tablet_schema);
    StorageReadOptions storage_read_opts;
    storage_read_opts.io_ctx.reader_type = ReaderType::READER_BASE_COMPACTION;
    storage_read_opts.tablet_schema = _tablet_schema;
    OlapReaderStatistics stats;
    storage_read_opts.stats = &stats;

    TabletColumn doc_bucket_map = variant_util::create_doc_value_column(parent_column, kBucket);
    ColumnIteratorUPtr it;
    st = variant_column_reader->new_iterator(&it, &doc_bucket_map, &storage_read_opts,
                                             &column_reader_cache);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_TRUE(dynamic_cast<segment_v2::VariantDocValueCompactIterator*>(it.get()) != nullptr);

    ColumnIteratorOptions column_iter_opts;
    column_iter_opts.stats = &stats;
    column_iter_opts.file_reader = file_reader.get();
    st = it->init(column_iter_opts);
    EXPECT_TRUE(st.ok()) << st.msg();

    MutableColumnPtr dst = ColumnVariantV2::create();
    size_t nrows = kRows;
    st = it->seek_to_ordinal(0);
    EXPECT_TRUE(st.ok()) << st.msg();
    st = it->next_batch(&nrows, dst);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_EQ(nrows, kRows);

    for (int i = 0; i < kRows; ++i) {
        EXPECT_EQ(variant_json_at(*dst, i), expected_bucket_json[i]);
    }

    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
}

TEST_F(VariantColumnWriterReaderTest, test_write_empty_doc_compact_column) {
    constexpr int kDocBuckets = 4;
    constexpr int kBucket = 0;

    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1", 3, false, false,
                     /*variant_sparse_hash_shard_count=*/0,
                     /*variant_enable_doc_mode=*/true,
                     /*variant_doc_materialization_min_rows=*/0,
                     /*variant_doc_hash_shard_count=*/kDocBuckets);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    const TabletColumn& parent_column = _tablet_schema->column(0);
    TabletColumn extracted_doc_bucket =
            variant_util::create_doc_value_column(parent_column, kBucket);
    extracted_doc_bucket.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);
    extracted_doc_bucket.set_is_nullable(false);
    _tablet_schema->append_column(extracted_doc_bucket);
    init_tablet_from_current_schema(33002);

    io::FileWriterPtr file_writer;
    const auto file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    ASSERT_TRUE(st.ok()) << st.msg();

    SegmentFooterPB footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    rowset_ctx.tablet_schema = _tablet_schema;

    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    opts.rowset_ctx = &rowset_ctx;
    opts.storage_format = TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2;
    const TabletColumn& doc_bucket_column = _tablet_schema->column(1);
    _init_column_meta(opts.meta, 0, doc_bucket_column, opts);

    std::unique_ptr<ColumnWriter> writer;
    ASSERT_TRUE(ColumnWriter::create(opts, &doc_bucket_column, file_writer.get(), &writer).ok());
    ASSERT_TRUE(writer->init().ok());
    ASSERT_TRUE(writer->finish().ok());
    ASSERT_TRUE(writer->write_data().ok());
    ASSERT_TRUE(file_writer->close().ok());

    ASSERT_EQ(footer.columns_size(), 1);
    EXPECT_EQ(footer.columns(0).num_rows(), 0);
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
}

TEST_F(VariantColumnWriterReaderTest, test_doc_compact_sparse_write_array_gap) {
    constexpr int kRows = 2;
    constexpr int kDocBuckets = 1;
    constexpr int kBucket = 0;

    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1", 3, false, false,
                     /*variant_sparse_hash_shard_count=*/0,
                     /*variant_enable_doc_mode=*/true,
                     /*variant_doc_materialization_min_rows=*/0,
                     /*variant_doc_hash_shard_count=*/kDocBuckets);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    TabletColumn parent_column = _tablet_schema->column(0);
    TabletColumn extracted_doc_bucket =
            variant_util::create_doc_value_column(parent_column, kBucket);
    extracted_doc_bucket.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);
    extracted_doc_bucket.set_is_nullable(false);
    _tablet_schema->append_column(extracted_doc_bucket);

    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    _tablet_schema->set_storage_format(TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2);
    tablet_meta->_tablet_id = 33001;
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    EXPECT_TRUE(_tablet->init().ok());
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    EXPECT_TRUE(st.ok()) << st.msg();

    SegmentFooterPB footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    rowset_ctx.tablet_schema = _tablet_schema;

    TabletColumn extracted_doc_bucket_col = _tablet_schema->column(1);
    ColumnWriterOptions doc_compact_opts;
    doc_compact_opts.meta = footer.add_columns();
    doc_compact_opts.compression_type = CompressionTypePB::LZ4;
    doc_compact_opts.file_writer = file_writer.get();
    doc_compact_opts.footer = &footer;
    doc_compact_opts.rowset_ctx = &rowset_ctx;
    doc_compact_opts.compression_type = CompressionTypePB::LZ4;
    doc_compact_opts.storage_format = TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2;
    _init_column_meta(doc_compact_opts.meta, 0, extracted_doc_bucket_col, doc_compact_opts);

    std::unique_ptr<ColumnWriter> doc_compact_writer;
    EXPECT_TRUE(ColumnWriter::create(doc_compact_opts, &extracted_doc_bucket_col, file_writer.get(),
                                     &doc_compact_writer)
                        .ok());
    EXPECT_TRUE(doc_compact_writer->init().ok());

    auto type_string = std::make_shared<DataTypeString>();
    auto json_column = type_string->create_column();
    auto* strings = assert_cast<ColumnString*>(json_column.get());
    const std::string row0 = R"({"arr":[1,2]})";
    const std::string row1 = R"({})";
    strings->insert_data(row0.data(), row0.size());
    strings->insert_data(row1.data(), row1.size());

    auto bucket_variant = encode_json_column_v2(*strings);

    auto bucket_data = std::make_unique<VariantColumnData>();
    bucket_data->column_data = bucket_variant.get();
    bucket_data->row_pos = 0;
    const auto* data = reinterpret_cast<const uint8_t*>(bucket_data.get());
    EXPECT_TRUE(doc_compact_writer->append_data(&data, kRows).ok());

    EXPECT_TRUE(doc_compact_writer->finish().ok());
    EXPECT_TRUE(doc_compact_writer->write_data().ok());
    EXPECT_TRUE(doc_compact_writer->write_ordinal_index().ok());
    EXPECT_TRUE(file_writer->close().ok());
    footer.set_num_rows(kRows);

    bool found_arr = false;
    for (int i = 0; i < footer.columns_size(); ++i) {
        const auto& col = footer.columns(i);
        if (!col.has_column_path_info()) {
            continue;
        }
        PathInData path;
        path.from_protobuf(col.column_path_info());
        if (path.copy_pop_front().get_path() == "arr") {
            EXPECT_EQ(col.type(), (int)FieldType::OLAP_FIELD_TYPE_ARRAY);
            EXPECT_TRUE(col.is_nullable());
            found_arr = true;
            break;
        }
    }
    EXPECT_TRUE(found_arr);

    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
}

TEST_F(VariantColumnWriterReaderTest, test_write_doc_sparse_write_array_gap_and_read) {
    constexpr int kRows = 2;
    constexpr int kDocBuckets = 1;

    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    construct_column(schema_pb.add_column(), 1, "VARIANT", "V1", 3, false, false,
                     /*variant_sparse_hash_shard_count=*/0,
                     /*variant_enable_doc_mode=*/true,
                     /*variant_doc_materialization_min_rows=*/0,
                     /*variant_doc_hash_shard_count=*/kDocBuckets);
    _tablet_schema = std::make_shared<TabletSchema>();
    _tablet_schema->init_from_pb(schema_pb);

    TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
    _tablet_schema->set_storage_format(TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2);
    tablet_meta->_tablet_id = 33002;
    _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
    EXPECT_TRUE(_tablet->init().ok());
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    EXPECT_TRUE(st.ok()) << st.msg();

    SegmentFooterPB footer;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
    rowset_ctx.tablet_schema = _tablet_schema;

    TabletColumn parent_column = _tablet_schema->column(0);
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;
    opts.rowset_ctx = &rowset_ctx;
    opts.compression_type = CompressionTypePB::LZ4;
    opts.storage_format = TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2;
    _init_column_meta(opts.meta, 0, parent_column, opts);

    std::unique_ptr<ColumnWriter> writer;
    EXPECT_TRUE(ColumnWriter::create(opts, &parent_column, file_writer.get(), &writer).ok());
    EXPECT_TRUE(writer->init().ok());

    auto type_string = std::make_shared<DataTypeString>();
    auto json_column = type_string->create_column();
    auto* strings = assert_cast<ColumnString*>(json_column.get());
    std::unordered_map<int, std::string> inserted_json;
    inserted_json.emplace(0, R"({"arr":[1,2]})");
    inserted_json.emplace(1, R"({})");
    strings->insert_data(inserted_json[0].data(), inserted_json[0].size());
    strings->insert_data(inserted_json[1].data(), inserted_json[1].size());

    auto variant_column = encode_json_column_v2(*strings);

    auto variant_data = std::make_unique<VariantColumnData>();
    variant_data->column_data = variant_column.get();
    variant_data->row_pos = 0;
    const auto* data = reinterpret_cast<const uint8_t*>(variant_data.get());
    EXPECT_TRUE(writer->append_data(&data, kRows).ok());

    EXPECT_TRUE(writer->finish().ok());
    EXPECT_TRUE(writer->write_data().ok());
    EXPECT_TRUE(writer->write_ordinal_index().ok());
    EXPECT_TRUE(file_writer->close().ok());
    footer.set_num_rows(kRows);

    io::FileReaderSPtr file_reader;
    st = io::global_local_filesystem()->open_file(file_path, &file_reader);
    EXPECT_TRUE(st.ok()) << st.msg();
    std::shared_ptr<ColumnReader> column_reader;
    st = create_variant_root_reader(footer, file_reader, _tablet_schema, &column_reader);
    EXPECT_TRUE(st.ok()) << st.msg();
    auto* variant_column_reader = assert_cast<VariantColumnReader*>(column_reader.get());
    EXPECT_TRUE(variant_column_reader != nullptr);

    const auto* arr_node = variant_column_reader->get_subcolumn_meta_by_path(PathInData("arr"));
    EXPECT_TRUE(arr_node != nullptr);

    bool found_arr_meta = false;
    for (int i = 0; i < footer.columns_size(); ++i) {
        const auto& col = footer.columns(i);
        if (!col.has_column_path_info()) {
            continue;
        }
        PathInData path;
        path.from_protobuf(col.column_path_info());
        if (path.copy_pop_front().get_path() == "arr") {
            EXPECT_EQ(col.type(), (int)FieldType::OLAP_FIELD_TYPE_ARRAY);
            EXPECT_TRUE(col.is_nullable());
            found_arr_meta = true;
            break;
        }
    }
    EXPECT_TRUE(found_arr_meta);

    MockColumnReaderCache column_reader_cache(footer, file_reader, _tablet_schema);
    StorageReadOptions storage_read_opts;
    storage_read_opts.io_ctx.reader_type = ReaderType::READER_QUERY;
    OlapReaderStatistics stats;
    storage_read_opts.stats = &stats;
    ColumnIteratorUPtr it;
    st = variant_column_reader->new_iterator(&it, &parent_column, &storage_read_opts,
                                             &column_reader_cache);
    EXPECT_TRUE(st.ok()) << st.msg();

    ColumnIteratorOptions column_iter_opts;
    column_iter_opts.stats = &stats;
    column_iter_opts.file_reader = file_reader.get();
    st = it->init(column_iter_opts);
    EXPECT_TRUE(st.ok()) << st.msg();

    MutableColumnPtr dst = ColumnVariantV2::create();
    size_t nrows = kRows;
    st = it->seek_to_ordinal(0);
    EXPECT_TRUE(st.ok()) << st.msg();
    st = it->next_batch(&nrows, dst);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_EQ(nrows, kRows);

    for (int i = 0; i < kRows; ++i) {
        if (i == 0) {
            EXPECT_EQ(variant_json_at(*dst, i), "{\"arr\":[1,2]}");
        } else {
            EXPECT_EQ(variant_json_at(*dst, i), "{}");
        }
    }

    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
}

TEST_F(VariantColumnWriterReaderTest,
       test_streaming_write_plan_collects_regular_paths_from_rowset_metadata) {
    if (!nested_group_write_path_available()) {
        GTEST_SKIP() << "NestedGroup write path is not available in this build";
    }

    init_variant_tablet(41000, 10, true);

    std::vector<RowsetSharedPtr> input_rowsets;
    input_rowsets.push_back(
            create_variant_rowset({{R"({"session_id": 1, "tags": ["a"], "score": 10})",
                                    R"({"session_id": 2, "tags": []})"}},
                                  1));
    input_rowsets.push_back(
            create_variant_rowset({{R"({"score": 30})", R"({"session_id": 4})"}}, 2));

    auto readers = create_rowset_readers(input_rowsets);
    segment_v2::NestedGroupStreamingWritePlan plan;
    auto st = segment_v2::build_nested_group_streaming_write_plan(readers,
                                                                  _tablet_schema->column(0), &plan);
    ASSERT_TRUE(st.ok()) << st.to_string();

    EXPECT_FALSE(plan.has_conflict_paths);
    EXPECT_FALSE(plan.has_root_nested_group);
    EXPECT_EQ(plan.conflict_policy, segment_v2::get_nested_group_conflict_policy());
    EXPECT_TRUE(plan.nested_groups.empty());
    EXPECT_EQ(collect_regular_paths(plan), std::set<std::string>({"score", "session_id", "tags"}));
    ASSERT_EQ(plan.regular_subcolumns.size(), 3);
    EXPECT_EQ(plan.regular_subcolumns[0].path, "score");
    EXPECT_EQ(plan.regular_subcolumns[1].path, "session_id");
    EXPECT_EQ(plan.regular_subcolumns[2].path, "tags");
    ASSERT_NE(plan.regular_subcolumns[2].data_type, nullptr);
    EXPECT_NE(plan.regular_subcolumns[2].data_type->get_name().find("Array"), std::string::npos);
}

TEST_F(VariantColumnWriterReaderTest,
       test_nested_group_writer_rejected_before_physical_writer_init) {
    init_variant_tablet(41001, 10, true);

    io::FileWriterPtr file_writer;
    auto file_path = local_segment_path(_tablet->tablet_path(), "streaming_compaction", 0);
    auto st = io::global_local_filesystem()->create_file(file_path, &file_writer);
    ASSERT_TRUE(st.ok()) << st.msg();

    SegmentFooterPB footer;
    ColumnWriterOptions opts;
    opts.meta = footer.add_columns();
    opts.compression_type = CompressionTypePB::LZ4;
    opts.file_writer = file_writer.get();
    opts.footer = &footer;

    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_COMPACTION;
    rowset_ctx.tablet_schema = _tablet_schema;
    opts.rowset_ctx = &rowset_ctx;

    TabletColumn column = _tablet_schema->column(0);
    opts.compression_type = CompressionTypePB::LZ4;
    opts.storage_format = TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2;
    _init_column_meta(opts.meta, 0, column, opts);

    std::unique_ptr<ColumnWriter> writer;
    ASSERT_TRUE(ColumnWriter::create(opts, &column, file_writer.get(), &writer).ok());
    st = writer->init();
    EXPECT_TRUE(st.is<ErrorCode::NOT_IMPLEMENTED_ERROR>()) << st.to_string();
    EXPECT_NE(st.to_string().find("nested-group"), std::string::npos);
    ASSERT_TRUE(file_writer->close().ok());
}

} // namespace doris
