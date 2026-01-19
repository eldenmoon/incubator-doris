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

#include "vec/functions/function_search.h"

#include <gtest/gtest.h>
#include <crc32c/crc32c.h>

#include <cstdlib>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include <unistd.h>

#include "common/config.h"
#include "olap/data_dir.h"
#include "olap/rowset/segment_v2/column_writer.h"
#include "olap/rowset/segment_v2/index_file_writer.h"
#include "olap/rowset/segment_v2/inverted_index_desc.h"
#include "olap/rowset/segment_v2/inverted_index_cache.h"
#include "olap/rowset/segment_v2/index_writer.h"
#include "olap/rowset/segment_v2/segment.h"
#include "olap/rowset/segment_v2/segment_writer.h"
#include "olap/rowset/segment_v2/variant/nested_offsets_mapping_index.h"
#include "util/stopwatch.hpp"
#include "olap/rowset/segment_v2/variant/variant_column_reader.h"
#include "olap/storage_engine.h"
#include "olap/tablet.h"
#include "olap/tablet_meta.h"
#include "runtime/exec_env.h"
#include "testutil/variant_util.h"
#include "util/coding.h"
#include "vec/columns/column_string.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/olap/olap_data_convertor.h"

namespace doris::vectorized {

namespace {

constexpr uint32_t MAX_PATH_LEN = 1024;
constexpr std::string_view dest_dir = "/ut_dir/function_search_variant_nested_test";
constexpr std::string_view tmp_dir = "./ut_dir/tmp";

struct TestConfig {
    uint64_t tablet_id = 11001;
    int variant_max_subcolumns_count = 20;
    bool enable_inverted_index = true;
};

static void construct_column(ColumnPB* column_pb, int32_t col_unique_id,
                             const std::string& column_type, const std::string& column_name,
                             int variant_max_subcolumns_count, bool is_nullable) {
    column_pb->set_unique_id(col_unique_id);
    column_pb->set_name(column_name);
    column_pb->set_type(column_type);
    column_pb->set_is_key(false);
    column_pb->set_is_nullable(is_nullable);
    if (column_type == "VARIANT") {
        column_pb->set_variant_max_subcolumns_count(variant_max_subcolumns_count);
        column_pb->set_variant_max_sparse_column_statistics_size(10000);
        column_pb->set_variant_sparse_hash_shard_count(0);
    }
}

static void init_column_meta(ColumnMetaPB* meta, uint32_t column_id, const TabletColumn& column,
                             CompressionTypePB compression_type) {
    meta->set_column_id(column_id);
    meta->set_type(int(column.type()));
    meta->set_length(column.length());
    meta->set_encoding(DEFAULT_ENCODING);
    meta->set_compression(compression_type);
    meta->set_is_nullable(column.is_nullable());
    meta->set_default_value(column.default_value());
    meta->set_precision(column.precision());
    meta->set_frac(column.frac());
    if (column.has_path_info()) {
        column.path_info_ptr()->to_protobuf(meta->mutable_column_path_info(),
                                            column.parent_unique_id());
    }
    meta->set_unique_id(column.unique_id());
    for (uint32_t i = 0; i < column.get_subtype_count(); ++i) {
        init_column_meta(meta->add_children_columns(), column_id, column.get_sub_column(i),
                         compression_type);
    }
    if (column.is_variant_type()) {
        meta->set_variant_max_subcolumns_count(column.variant_max_subcolumns_count());
    }
}

static TSearchClause make_term(std::string field, std::string value) {
    TSearchClause clause;
    clause.clause_type = "TERM";
    clause.__set_field_name(std::move(field));
    clause.__set_value(std::move(value));
    return clause;
}

static TSearchClause make_any(std::string field, std::string value) {
    TSearchClause clause;
    clause.clause_type = "ANY";
    clause.__set_field_name(std::move(field));
    clause.__set_value(std::move(value));
    return clause;
}

static TSearchClause make_and(std::vector<TSearchClause> children) {
    TSearchClause clause;
    clause.clause_type = "AND";
    clause.__set_children(std::move(children));
    return clause;
}

static TSearchClause make_or(std::vector<TSearchClause> children) {
    TSearchClause clause;
    clause.clause_type = "OR";
    clause.__set_children(std::move(children));
    return clause;
}

static TSearchClause make_not(std::vector<TSearchClause> children) {
    TSearchClause clause;
    clause.clause_type = "NOT";
    clause.__set_children(std::move(children));
    return clause;
}

static TSearchClause with_occur(TSearchClause clause, TSearchOccur::type occur) {
    clause.__set_occur(occur);
    return clause;
}

static TSearchClause make_occur_boolean(std::vector<TSearchClause> children,
                                        std::optional<int32_t> minimum_should_match = std::nullopt) {
    TSearchClause clause;
    clause.clause_type = "OCCUR_BOOLEAN";
    if (minimum_should_match.has_value()) {
        clause.__set_minimum_should_match(minimum_should_match.value());
    }
    clause.__set_children(std::move(children));
    return clause;
}

static TSearchClause make_nested(std::string nested_path, TSearchClause inner) {
    TSearchClause clause;
    clause.clause_type = "NESTED";
    clause.__set_nested_path(std::move(nested_path));
    std::vector<TSearchClause> children;
    children.emplace_back(std::move(inner));
    clause.__set_children(std::move(children));
    return clause;
}

static std::vector<uint32_t> to_vec(const roaring::Roaring& bitmap) {
    std::vector<uint32_t> out;
    out.reserve(bitmap.cardinality());
    for (auto it = bitmap.begin(); it != bitmap.end(); ++it) {
        out.emplace_back(*it);
    }
    return out;
}

class VariantSegmentContext {
public:
    VariantSegmentContext(StorageEngine* engine, DataDir* data_dir, std::string absolute_dir,
                          TestConfig config)
            : _engine(engine),
              _data_dir(data_dir),
              _absolute_dir(std::move(absolute_dir)),
              _config(config) {
        TabletSchemaPB schema_pb;
        schema_pb.set_keys_type(KeysType::DUP_KEYS);
        if (_config.enable_inverted_index) {
            schema_pb.set_inverted_index_storage_format(InvertedIndexStorageFormatPB::V2);
            auto* index_pb = schema_pb.add_index();
            index_pb->set_index_id(10000);
            index_pb->set_index_name("v1_idx");
            index_pb->set_index_type(IndexType::INVERTED);
            index_pb->add_col_unique_id(1);
            (*index_pb->mutable_properties())["parser"] = "english";
        }
        construct_column(schema_pb.add_column(), 1, "VARIANT", "data",
                         _config.variant_max_subcolumns_count, false);
        _tablet_schema = std::make_shared<TabletSchema>();
        _tablet_schema->init_from_pb(schema_pb);

        TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
        _tablet_schema->set_external_segment_meta_used_default(true);
        tablet_meta->_tablet_id = _config.tablet_id;
        _tablet = std::make_shared<Tablet>(*_engine, tablet_meta, _data_dir);

        EXPECT_TRUE(_tablet->init().ok());
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
        EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

        _file_path = local_segment_path(_tablet->tablet_path(), "0", 0);
        Status st = io::global_local_filesystem()->create_file(_file_path, &_file_writer);
        EXPECT_TRUE(st.ok()) << st.msg();

        _footer.Clear();
        ColumnWriterOptions opts;
        opts.meta = _footer.add_columns();
        opts.compression_type = CompressionTypePB::LZ4;
        opts.file_writer = _file_writer.get();
        opts.footer = &_footer;
        _rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
        opts.rowset_ctx = &_rowset_ctx;
        opts.rowset_ctx->tablet_schema = _tablet_schema;
        _column = _tablet_schema->column(0);
        init_column_meta(opts.meta, 0, _column, CompressionTypePB::LZ4);

        if (_config.enable_inverted_index) {
            _index_path_prefix =
                    std::string(segment_v2::InvertedIndexDescriptor::get_index_file_path_prefix(
                            _file_path));
            io::FileWriterPtr idx_file_writer;
            auto idx_file_path =
                    segment_v2::InvertedIndexDescriptor::get_index_file_path_v2(_index_path_prefix);
            st = io::global_local_filesystem()->create_file(idx_file_path, &idx_file_writer);
            EXPECT_TRUE(st.ok()) << st.msg();
            _index_file_writer = std::make_unique<segment_v2::IndexFileWriter>(
                    io::global_local_filesystem(), _index_path_prefix, "0", 0,
                    InvertedIndexStorageFormatPB::V2, std::move(idx_file_writer));
            opts.index_file_writer = _index_file_writer.get();
        }

        EXPECT_TRUE(ColumnWriter::create(opts, &_column, _file_writer.get(), &_writer).ok());
        EXPECT_TRUE(_writer->init().ok());
    }

    Status write_json_data(const std::vector<std::string>& jsons) {
        auto column_variant = ColumnVariant::create(0);
        auto json_column = ColumnString::create();
        for (const auto& json : jsons) {
            json_column->insert_data(json.data(), json.size());
        }

        ParseConfig config;
        variant_util::parse_json_to_variant(*column_variant, *json_column, config);
        _num_rows = jsons.size();

        auto olap_data_convertor = std::make_unique<vectorized::OlapBlockDataConvertor>();
        auto block = _tablet_schema->create_block();
        block.get_by_position(0).column = std::move(column_variant);
        olap_data_convertor->add_column_data_convertor(_column);
        olap_data_convertor->set_source_content(&block, 0, _num_rows);
        auto [result, accessor] = olap_data_convertor->convert_column_data(0);
        RETURN_IF_ERROR(result);
        return _writer->append(accessor->get_nullmap(), accessor->get_data(), _num_rows);
    }

    Status finish_write() {
        RETURN_IF_ERROR(_writer->finish());
        RETURN_IF_ERROR(_writer->write_data());
        RETURN_IF_ERROR(_writer->write_ordinal_index());
        RETURN_IF_ERROR(_writer->write_zone_map());
        if (_config.enable_inverted_index) {
            RETURN_IF_ERROR(_writer->write_inverted_index());
        }
        _footer.set_num_rows(_num_rows);

        // Write footer
        std::string footer_buf;
        if (!_footer.SerializeToString(&footer_buf)) {
             return Status::InternalError("failed to serialize segment footer");
        }

        std::string fixed_buf;
        // footer's size
        put_fixed32_le(&fixed_buf, static_cast<uint32_t>(footer_buf.size()));
        // footer's checksum
        uint32_t checksum = crc32c::Crc32c(footer_buf.data(), footer_buf.size());
        put_fixed32_le(&fixed_buf, checksum);
        // magic number
        fixed_buf.append(segment_v2::k_segment_magic, segment_v2::k_segment_magic_length);

        RETURN_IF_ERROR(_file_writer->append(footer_buf));
        RETURN_IF_ERROR(_file_writer->append(fixed_buf));

        RETURN_IF_ERROR(_file_writer->close());
        if (_config.enable_inverted_index) {
            RETURN_IF_ERROR(_index_file_writer->begin_close());
            RETURN_IF_ERROR(_index_file_writer->finish_close());
        }
        return Status::OK();
    }

    Result<std::shared_ptr<segment_v2::Segment>> open_segment() {
        RowsetId rowset_id;
        rowset_id.init(1);
        OlapReaderStatistics stats;
        std::shared_ptr<segment_v2::Segment> segment;
        auto st = segment_v2::Segment::open(io::global_local_filesystem(), _file_path, _tablet->tablet_id(),
                                            0, rowset_id, _tablet_schema, io::FileReaderOptions(),
                                            &segment, InvertedIndexFileInfo(), &stats);
        if (!st.ok()) {
            return ResultError(st);
        }
        return segment;
    }

    const TabletSchemaSPtr& tablet_schema() const { return _tablet_schema; }
    const TabletColumn& column() const { return _column; }
    size_t num_rows() const { return _num_rows; }

private:
    StorageEngine* _engine = nullptr;
    DataDir* _data_dir = nullptr;
    std::string _absolute_dir;
    TestConfig _config;

    TabletSchemaSPtr _tablet_schema;
    TabletSharedPtr _tablet;
    TabletColumn _column;

    io::FileWriterPtr _file_writer;
    std::unique_ptr<segment_v2::IndexFileWriter> _index_file_writer;
    std::unique_ptr<ColumnWriter> _writer;
    SegmentFooterPB _footer;
    RowsetWriterContext _rowset_ctx;
    std::string _file_path;
    std::string _index_path_prefix;

    size_t _num_rows = 0;
};

} // namespace

class FunctionSearchVariantNestedTest : public testing::Test {
public:
    void SetUp() override {
        config::variant_nested_group_max_depth = 10;
        char buffer[MAX_PATH_LEN];
        EXPECT_NE(getcwd(buffer, MAX_PATH_LEN), nullptr);
        _current_dir = std::string(buffer);
        _absolute_dir = _current_dir + std::string(dest_dir);
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_absolute_dir).ok());
        EXPECT_TRUE(io::global_local_filesystem()->create_directory(_absolute_dir).ok());

        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(tmp_dir).ok());
        EXPECT_TRUE(io::global_local_filesystem()->create_directory(tmp_dir).ok());
        std::vector<StorePath> paths;
        paths.emplace_back(std::string(tmp_dir), 1024000000);
        auto tmp_file_dirs = std::make_unique<segment_v2::TmpFileDirs>(paths);
        Status st = tmp_file_dirs->init();
        EXPECT_TRUE(st.ok()) << st.to_json();
        ExecEnv::GetInstance()->set_tmp_file_dir(std::move(tmp_file_dirs));

        doris::EngineOptions options;
        auto engine = std::make_unique<StorageEngine>(options);
        _engine_ref = engine.get();
        _data_dir = std::make_unique<DataDir>(*_engine_ref, _absolute_dir);
        static_cast<void>(_data_dir->update_capacity());
        ExecEnv::GetInstance()->set_storage_engine(std::move(engine));
        if (ExecEnv::GetInstance()->get_inverted_index_query_cache() == nullptr) {
            ExecEnv::GetInstance()->set_inverted_index_query_cache(
                    segment_v2::InvertedIndexQueryCache::create_global_cache(1 << 20, 1));
        }
    }

    void TearDown() override {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_absolute_dir).ok());
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(tmp_dir).ok());
        _engine_ref = nullptr;
        ExecEnv::GetInstance()->set_storage_engine(nullptr);
    }

protected:
    StorageEngine* _engine_ref = nullptr;
    std::unique_ptr<DataDir> _data_dir;
    std::string _absolute_dir;
    std::string _current_dir;
};

TEST_F(FunctionSearchVariantNestedTest, nested_and_or_any_queries) {
    TestConfig cfg;
    cfg.tablet_id = 11001;
    VariantSegmentContext ctx(_engine_ref, _data_dir.get(), _absolute_dir, cfg);

    std::vector<std::string> jsons = {
            R"({"items":[{"subitems":[{"msg":"hello","title":"news","tags":"java kotlin"},{"msg":"foo","title":"bar","tags":"cpp"}]},{"subitems":[{"msg":"hello","title":"sports","tags":"python"}]}]})",
            R"({"items":[{"subitems":[{"msg":"hello","tags":"java"},{"title":"news","tags":"python"}]}]})",
            R"({"items":[{"subitems":[{"msg":"hello","title":"news","tags":"scala"}]},{"subitems":[]}]} )",
            R"({"items":[{"subitems":null}]})",
            R"(null)"};

    auto write_st = ctx.write_json_data(jsons);
    ASSERT_TRUE(write_st.ok()) << write_st.msg();
    write_st = ctx.finish_write();
    ASSERT_TRUE(write_st.ok()) << write_st.msg();

    auto segment_res = ctx.open_segment();
    ASSERT_TRUE(segment_res.has_value()) << segment_res.error().msg();
    auto segment = segment_res.value();

    StorageReadOptions read_opts;
    OlapReaderStatistics stats;
    read_opts.io_ctx.reader_type = ReaderType::READER_QUERY;
    read_opts.stats = &stats;
    ASSERT_TRUE(ctx.tablet_schema() != nullptr);

    std::vector<ColumnId> col_ids = {0};
    std::vector<std::unique_ptr<segment_v2::IndexIterator>> index_iters;
    std::vector<vectorized::IndexFieldNameAndTypePair> storage_name_and_type;
    storage_name_and_type.emplace_back("1", nullptr);
    std::unordered_map<ColumnId, std::unordered_map<const vectorized::VExpr*, bool>> expr_status;
    segment_v2::ColumnIteratorOptions col_iter_opts;
    col_iter_opts.stats = &stats;
    col_iter_opts.file_reader = segment->file_reader().get();

    IndexExecContext index_exec_ctx(col_ids, index_iters, storage_name_and_type, expr_status, nullptr,
                                   segment.get(), col_iter_opts);

    FunctionSearch fn;

    auto iterator_ctx = std::make_shared<segment_v2::IndexQueryContext>();
    iterator_ctx->io_ctx = &read_opts.io_ctx;
    iterator_ctx->stats = &stats;
    std::shared_ptr<segment_v2::ColumnReader> root_reader;
    EXPECT_TRUE(segment->get_column_reader(ctx.column(), &root_reader, &stats).ok());
    ASSERT_TRUE(root_reader != nullptr);
    auto* variant_reader = dynamic_cast<segment_v2::VariantColumnReader*>(root_reader.get());
    ASSERT_TRUE(variant_reader != nullptr);

    std::unordered_map<std::string, DataTypePtr> inferred_types;
    auto build_materialized = [&](const std::string& field) -> TabletColumn {
        auto dot = field.find('.');
        EXPECT_NE(dot, std::string::npos);
        std::string root = field.substr(0, dot);
        std::vector<std::string> paths;
        for (size_t start = dot + 1; start < field.size();) {
            auto next = field.find('.', start);
            if (next == std::string::npos) {
                paths.emplace_back(field.substr(start));
                break;
            }
            paths.emplace_back(field.substr(start, next - start));
            start = next + 1;
        }
        return TabletColumn::create_materialized_variant_column(
                root, paths, ctx.column().unique_id(), ctx.column().variant_max_subcolumns_count());
    };
    auto infer_type_for_field = [&](const std::string& field,
                                   const TabletColumn& materialized) -> DataTypePtr {
        auto it = inferred_types.find(field);
        if (it != inferred_types.end()) {
            return it->second;
        }
        DataTypePtr inferred_type;
        auto st = variant_reader->infer_data_type_for_path(&inferred_type, materialized, read_opts,
                                                           nullptr);
        EXPECT_TRUE(st.ok()) << st.msg();
        if (inferred_type == nullptr) {
            ADD_FAILURE() << "Inferred storage type is null for field: " << field;
            inferred_type =
                    DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_STRING, true);
        }
        std::cout << "Inferred storage type for " << field << ": " << inferred_type->get_name()
                  << std::endl;
        inferred_types.emplace(field, inferred_type);
        return inferred_type;
    };

    std::unordered_map<std::string, std::unique_ptr<segment_v2::IndexIterator>> owned_iters;
    auto get_or_create_iter = [&](const std::string& field) -> segment_v2::IndexIterator* {
        auto it = owned_iters.find(field);
        if (it != owned_iters.end()) {
            return it->second.get();
        }
        auto dot = field.find('.');
        if (dot == std::string::npos) {
            ADD_FAILURE() << "Invalid field name: " << field;
            return nullptr;
        }
        std::string root = field.substr(0, dot);
        std::vector<std::string> paths;
        for (size_t start = dot + 1; start < field.size();) {
            auto next = field.find('.', start);
            if (next == std::string::npos) {
                paths.emplace_back(field.substr(start));
                break;
            }
            paths.emplace_back(field.substr(start, next - start));
            start = next + 1;
        }
        TabletColumn materialized =
                TabletColumn::create_materialized_variant_column(root, paths, ctx.column().unique_id(),
                                                                 ctx.column().variant_max_subcolumns_count());
        auto dt = infer_type_for_field(field, materialized);
        auto index_holder = variant_reader->find_subcolumn_tablet_indexes(materialized, dt);
        if (index_holder.empty()) {
            ADD_FAILURE() << "No inverted index metadata for field: " << field;
            return nullptr;
        }
        std::unique_ptr<segment_v2::IndexIterator> index_iter;
        EXPECT_TRUE(segment
                            ->new_index_iterator(materialized, index_holder[0].get(), read_opts,
                                                 &index_iter)
                            .ok());
        if (index_iter == nullptr) {
            ADD_FAILURE() << "Failed to create index iterator for field: " << field;
            return nullptr;
        }
        index_iter->set_context(iterator_ctx);
        auto* raw = index_iter.get();
        owned_iters.emplace(field, std::move(index_iter));
        return raw;
    };

    auto run = [&](const TSearchParam& search_param,
                   const std::unordered_map<std::string, vectorized::IndexFieldNameAndTypePair>&
                           types,
                   std::vector<uint32_t> expected_rows) {
        std::unordered_map<std::string, segment_v2::IndexIterator*> iters;
        for (const auto& [field, _] : types) {
            iters[field] = get_or_create_iter(field);
            ASSERT_TRUE(iters[field] != nullptr);
        }
        std::unordered_map<std::string, int> field_name_to_column_id;

        segment_v2::InvertedIndexResultBitmap bitmap;
        auto st = fn.evaluate_inverted_index_with_search_param(search_param, types, std::move(iters),
                                                               ctx.num_rows(), bitmap, &index_exec_ctx,
                                                               field_name_to_column_id);
        EXPECT_TRUE(st.ok()) << st.msg();
        ASSERT_TRUE(bitmap.get_data_bitmap() != nullptr);
        EXPECT_EQ(expected_rows, to_vec(*bitmap.get_data_bitmap()));
    };

    auto types_for_fields = [&](std::vector<std::string> fields) {
        std::unordered_map<std::string, vectorized::IndexFieldNameAndTypePair> m;
        for (const auto& f : fields) {
            auto dot = f.find('.');
            std::string sub_path = (dot == std::string::npos) ? f : f.substr(dot + 1);
            auto materialized = build_materialized(f);
            auto dt = infer_type_for_field(f, materialized);
            m.emplace(f, std::make_pair("1." + sub_path, dt));
        }
        return m;
    };

    auto bindings_for_fields = [&](std::vector<std::string> fields) {
        std::vector<TSearchFieldBinding> bindings;
        for (const auto& f : fields) {
            TSearchFieldBinding b;
            b.field_name = f;
            b.slot_index = 0;
            b.__set_parent_field_name("data");
            auto dot = f.find('.');
            if (dot != std::string::npos) {
                b.__set_subcolumn_path(f.substr(dot + 1));
            }
            b.__set_is_variant_subcolumn(true);
            bindings.emplace_back(std::move(b));
        }
        return bindings;
    };

    {
        auto fields = std::vector<std::string> {"data.items.subitems.msg",
                                                "data.items.subitems.title"};
        auto types = types_for_fields(fields);
        TSearchParam p;
        p.__set_original_dsl("NESTED(data.items.subitems, msg=hello AND title=news)");
        p.__set_field_bindings(bindings_for_fields(fields));
        p.__set_root(make_nested("data.items.subitems",
                                 make_and({make_term("data.items.subitems.msg", "hello"),
                                           make_term("data.items.subitems.title", "news")})));
        run(p, types, {0, 2});
    }

    {
        auto fields = std::vector<std::string> {"data.items.subitems.title",
                                                "data.items.subitems.tags"};
        auto types = types_for_fields(fields);
        TSearchParam p;
        p.__set_original_dsl("NESTED(data.items.subitems, title=news AND tags ANY(java kotlin))");
        p.__set_field_bindings(bindings_for_fields(fields));
        p.__set_root(make_nested(
                "data.items.subitems",
                make_and({make_term("data.items.subitems.title", "news"),
                          make_any("data.items.subitems.tags", "java kotlin")})));
        run(p, types, {0});
    }

    {
        auto fields = std::vector<std::string> {"data.items.subitems.msg",
                                                "data.items.subitems.title"};
        auto types = types_for_fields(fields);
        TSearchParam p;
        p.__set_original_dsl(
                "NESTED(data.items.subitems, (title=sports OR title=news) AND msg=hello)");
        p.__set_field_bindings(bindings_for_fields(fields));
        p.__set_root(make_nested(
                "data.items.subitems",
                make_and({make_or({make_term("data.items.subitems.title", "sports"),
                                   make_term("data.items.subitems.title", "news")}),
                          make_term("data.items.subitems.msg", "hello")})));
        run(p, types, {0, 2});
    }

    {
        auto fields = std::vector<std::string> {"data.items.subitems.msg",
                                                "data.items.subitems.tags"};
        auto types = types_for_fields(fields);
        TSearchParam p;
        p.__set_original_dsl(
                "NESTED(data.items.subitems, (tags ANY(python) OR tags ANY(scala)) AND msg=hello)");
        p.__set_field_bindings(bindings_for_fields(fields));
        p.__set_root(make_nested(
                "data.items.subitems",
                make_and({make_or({make_any("data.items.subitems.tags", "python"),
                                   make_any("data.items.subitems.tags", "scala")}),
                          make_term("data.items.subitems.msg", "hello")})));
        run(p, types, {0, 2});
    }

    {
        auto fields = std::vector<std::string> {"data.items.subitems.msg",
                                                "data.items.subitems.tags"};
        auto types = types_for_fields(fields);
        TSearchParam p;
        p.__set_original_dsl("NESTED(data.items.subitems, msg=hello AND tags ANY(cpp))");
        p.__set_field_bindings(bindings_for_fields(fields));
        p.__set_root(make_nested("data.items.subitems",
                                 make_and({make_term("data.items.subitems.msg", "hello"),
                                           make_any("data.items.subitems.tags", "cpp")})));
        run(p, types, {});
    }

    {
        auto fields = std::vector<std::string> {"data.items.subitems.title",
                                                "data.items.subitems.tags"};
        auto types = types_for_fields(fields);
        TSearchParam p;
        p.__set_original_dsl("NESTED(data.items.subitems, title=news AND tags ANY(python))");
        p.__set_field_bindings(bindings_for_fields(fields));
        p.__set_root(make_nested("data.items.subitems",
                                 make_and({make_term("data.items.subitems.title", "news"),
                                           make_any("data.items.subitems.tags", "python")})));
        run(p, types, {1});
    }

    {
        auto fields = std::vector<std::string> {"data.items.subitems.msg",
                                                "data.items.subitems.title"};
        auto types = types_for_fields(fields);
        TSearchParam p;
        p.__set_original_dsl(
                "NESTED(data.items.subitems, (msg=hello AND title=news) OR (msg=foo AND title=bar))");
        p.__set_field_bindings(bindings_for_fields(fields));
        p.__set_root(make_nested(
                "data.items.subitems",
                make_or({make_and({make_term("data.items.subitems.msg", "hello"),
                                   make_term("data.items.subitems.title", "news")}),
                         make_and({make_term("data.items.subitems.msg", "foo"),
                                   make_term("data.items.subitems.title", "bar")})})));
        run(p, types, {0, 2});
    }

    {
        auto fields = std::vector<std::string> {"data.items.subitems.msg",
                                                "data.items.subitems.title",
                                                "data.items.subitems.tags"};
        auto types = types_for_fields(fields);
        TSearchParam p;
        p.__set_original_dsl(
                "NESTED(data.items.subitems, ((msg=hello AND tags ANY(java)) OR (msg=foo AND tags ANY(cpp))) AND (title=news OR title=bar))");
        p.__set_field_bindings(bindings_for_fields(fields));
        p.__set_root(make_nested(
                "data.items.subitems",
                make_and({make_or({make_and({make_term("data.items.subitems.msg", "hello"),
                                             make_any("data.items.subitems.tags", "java")}),
                                   make_and({make_term("data.items.subitems.msg", "foo"),
                                             make_any("data.items.subitems.tags", "cpp")})}),
                          make_or({make_term("data.items.subitems.title", "news"),
                                   make_term("data.items.subitems.title", "bar")})})));
        run(p, types, {0});
    }
}

TEST_F(FunctionSearchVariantNestedTest, nested_offsets_multilevel_composition) {
    TestConfig cfg;
    cfg.tablet_id = 11002;
    VariantSegmentContext ctx(_engine_ref, _data_dir.get(), _absolute_dir, cfg);

    std::vector<std::string> jsons = {
            R"({"items":[{"subitems":[{"msg":"a"},{"msg":"b"}]},{"subitems":[{"msg":"c"}]}]})",
            R"({"items":[{"subitems":[{"msg":"c"},{"msg":"d"},{"msg":"c"}]}]})",
            R"({"items":[]})",
            R"({"items":[{"subitems":null}]})",
            R"(null)"};

    auto write_st = ctx.write_json_data(jsons);
    ASSERT_TRUE(write_st.ok()) << write_st.msg();
    write_st = ctx.finish_write();
    ASSERT_TRUE(write_st.ok()) << write_st.msg();

    auto segment_res = ctx.open_segment();
    ASSERT_TRUE(segment_res.has_value()) << segment_res.error().msg();
    auto segment = segment_res.value();

    StorageReadOptions read_opts;
    OlapReaderStatistics stats;
    read_opts.io_ctx.reader_type = ReaderType::READER_QUERY;
    read_opts.stats = &stats;

    std::vector<ColumnId> col_ids = {0};
    std::vector<std::unique_ptr<segment_v2::IndexIterator>> index_iters;
    std::vector<vectorized::IndexFieldNameAndTypePair> storage_name_and_type;
    storage_name_and_type.emplace_back("1", nullptr);
    std::unordered_map<ColumnId, std::unordered_map<const vectorized::VExpr*, bool>> expr_status;
    segment_v2::ColumnIteratorOptions col_iter_opts;
    col_iter_opts.stats = &stats;
    col_iter_opts.file_reader = segment->file_reader().get();
    IndexExecContext index_exec_ctx(col_ids, index_iters, storage_name_and_type, expr_status, nullptr,
                                   segment.get(), col_iter_opts);

    FunctionSearch fn;

    auto iterator_ctx = std::make_shared<segment_v2::IndexQueryContext>();
    iterator_ctx->io_ctx = &read_opts.io_ctx;
    iterator_ctx->stats = &stats;
    std::shared_ptr<segment_v2::ColumnReader> root_reader;
    EXPECT_TRUE(segment->get_column_reader(ctx.column(), &root_reader, &stats).ok());
    ASSERT_TRUE(root_reader != nullptr);
    auto* variant_reader = dynamic_cast<segment_v2::VariantColumnReader*>(root_reader.get());
    ASSERT_TRUE(variant_reader != nullptr);

    std::unordered_map<std::string, DataTypePtr> inferred_types;
    auto build_materialized = [&](const std::string& field) -> TabletColumn {
        auto dot = field.find('.');
        EXPECT_NE(dot, std::string::npos);
        std::string root = field.substr(0, dot);
        std::vector<std::string> paths;
        for (size_t start = dot + 1; start < field.size();) {
            auto next = field.find('.', start);
            if (next == std::string::npos) {
                paths.emplace_back(field.substr(start));
                break;
            }
            paths.emplace_back(field.substr(start, next - start));
            start = next + 1;
        }
        return TabletColumn::create_materialized_variant_column(
                root, paths, ctx.column().unique_id(), ctx.column().variant_max_subcolumns_count());
    };
    auto infer_type_for_field = [&](const std::string& field,
                                   const TabletColumn& materialized) -> DataTypePtr {
        auto it = inferred_types.find(field);
        if (it != inferred_types.end()) {
            return it->second;
        }
        DataTypePtr inferred_type;
        auto st = variant_reader->infer_data_type_for_path(&inferred_type, materialized, read_opts,
                                                           nullptr);
        EXPECT_TRUE(st.ok()) << st.msg();
        if (inferred_type == nullptr) {
            ADD_FAILURE() << "Inferred storage type is null for field: " << field;
            inferred_type =
                    DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_STRING, true);
        }
        std::cout << "Inferred storage type for " << field << ": " << inferred_type->get_name()
                  << std::endl;
        inferred_types.emplace(field, inferred_type);
        return inferred_type;
    };

    std::unordered_map<std::string, std::unique_ptr<segment_v2::IndexIterator>> owned_iters;
    auto get_or_create_iter = [&](const std::string& field) -> segment_v2::IndexIterator* {
        auto it = owned_iters.find(field);
        if (it != owned_iters.end()) {
            return it->second.get();
        }
        auto dot = field.find('.');
        if (dot == std::string::npos) {
            return nullptr;
        }
        std::string root = field.substr(0, dot);
        std::vector<std::string> paths;
        for (size_t start = dot + 1; start < field.size();) {
            auto next = field.find('.', start);
            if (next == std::string::npos) {
                paths.emplace_back(field.substr(start));
                break;
            }
            paths.emplace_back(field.substr(start, next - start));
            start = next + 1;
        }
        TabletColumn materialized =
                TabletColumn::create_materialized_variant_column(root, paths, ctx.column().unique_id(),
                                                                 ctx.column().variant_max_subcolumns_count());
        auto dt = infer_type_for_field(field, materialized);
        auto index_holder = variant_reader->find_subcolumn_tablet_indexes(materialized, dt);
        if (index_holder.empty()) {
            return nullptr;
        }
        std::unique_ptr<segment_v2::IndexIterator> index_iter;
        EXPECT_TRUE(segment
                            ->new_index_iterator(materialized, index_holder[0].get(), read_opts,
                                                 &index_iter)
                            .ok());
        if (index_iter == nullptr) {
            return nullptr;
        }
        index_iter->set_context(iterator_ctx);
        auto* raw = index_iter.get();
        owned_iters.emplace(field, std::move(index_iter));
        return raw;
    };

    auto run = [&](const TSearchParam& search_param, std::string field, std::string value,
                   std::vector<uint32_t> expected_rows) {
        std::unordered_map<std::string, vectorized::IndexFieldNameAndTypePair> types;
        auto materialized = build_materialized(field);
        auto dt = infer_type_for_field(field, materialized);
        types.emplace(field, std::make_pair("1." + field.substr(field.find('.') + 1), dt));

        std::unordered_map<std::string, segment_v2::IndexIterator*> iters;
        iters[field] = get_or_create_iter(field);
        ASSERT_TRUE(iters[field] != nullptr);

        std::unordered_map<std::string, int> field_name_to_column_id;

        segment_v2::InvertedIndexResultBitmap bitmap;
        auto st = fn.evaluate_inverted_index_with_search_param(search_param, types, std::move(iters),
                                                               ctx.num_rows(), bitmap, &index_exec_ctx,
                                                               field_name_to_column_id);
        EXPECT_TRUE(st.ok()) << st.msg();
        ASSERT_TRUE(bitmap.get_data_bitmap() != nullptr);
        EXPECT_EQ(expected_rows, to_vec(*bitmap.get_data_bitmap()));
    };

    auto build_param = [&](std::string val) {
        TSearchParam p;
        p.__set_original_dsl("NESTED(data.items.subitems, msg=" + val + ")");
        std::vector<TSearchFieldBinding> bindings;
        TSearchFieldBinding b;
        b.field_name = "data.items.subitems.msg";
        b.slot_index = 0;
        b.__set_parent_field_name("data");
        b.__set_subcolumn_path("items.subitems.msg");
        b.__set_is_variant_subcolumn(true);
        bindings.emplace_back(std::move(b));
        p.__set_field_bindings(std::move(bindings));
        p.__set_root(make_nested("data.items.subitems", make_term("data.items.subitems.msg", val)));
        return p;
    };

    run(build_param("c"), "data.items.subitems.msg", "c", {0, 1});
    run(build_param("d"), "data.items.subitems.msg", "d", {1});
}

TEST_F(FunctionSearchVariantNestedTest, nested_offsets_multi_page_sparse_hits) {
    TestConfig cfg;
    cfg.tablet_id = 11003;
    VariantSegmentContext ctx(_engine_ref, _data_dir.get(), _absolute_dir, cfg);

    const size_t num_rows = 20000;
    std::vector<std::string> jsons;
    jsons.reserve(num_rows);
    for (size_t i = 0; i < num_rows; ++i) {
        const bool hit = (i == 0 || i == 8191 || i == 8192 || i == 15000);
        if (hit) {
            jsons.emplace_back(R"({"items":[{"subitems":[{"msg":"hit"}]}]})");
        } else {
            jsons.emplace_back(R"({"items":[{"subitems":[{"msg":"miss"}]}]})");
        }
    }

    auto write_st = ctx.write_json_data(jsons);
    ASSERT_TRUE(write_st.ok()) << write_st.msg();
    write_st = ctx.finish_write();
    ASSERT_TRUE(write_st.ok()) << write_st.msg();

    auto segment_res = ctx.open_segment();
    ASSERT_TRUE(segment_res.has_value()) << segment_res.error().msg();
    auto segment = segment_res.value();

    StorageReadOptions read_opts;
    OlapReaderStatistics stats;
    read_opts.io_ctx.reader_type = ReaderType::READER_QUERY;
    read_opts.stats = &stats;

    std::vector<ColumnId> col_ids = {0};
    std::vector<std::unique_ptr<segment_v2::IndexIterator>> index_iters;
    std::vector<vectorized::IndexFieldNameAndTypePair> storage_name_and_type;
    storage_name_and_type.emplace_back("1", nullptr);
    std::unordered_map<ColumnId, std::unordered_map<const vectorized::VExpr*, bool>> expr_status;
    segment_v2::ColumnIteratorOptions col_iter_opts;
    col_iter_opts.stats = &stats;
    col_iter_opts.file_reader = segment->file_reader().get();
    IndexExecContext index_exec_ctx(col_ids, index_iters, storage_name_and_type, expr_status, nullptr,
                                   segment.get(), col_iter_opts);

    FunctionSearch fn;

    auto iterator_ctx = std::make_shared<segment_v2::IndexQueryContext>();
    iterator_ctx->io_ctx = &read_opts.io_ctx;
    iterator_ctx->stats = &stats;
    std::shared_ptr<segment_v2::ColumnReader> root_reader;
    EXPECT_TRUE(segment->get_column_reader(ctx.column(), &root_reader, &stats).ok());
    ASSERT_TRUE(root_reader != nullptr);
    auto* variant_reader = dynamic_cast<segment_v2::VariantColumnReader*>(root_reader.get());
    ASSERT_TRUE(variant_reader != nullptr);

    std::unordered_map<std::string, DataTypePtr> inferred_types;
    auto build_materialized = [&](const std::string& field) -> TabletColumn {
        auto dot = field.find('.');
        EXPECT_NE(dot, std::string::npos);
        std::string root = field.substr(0, dot);
        std::vector<std::string> paths;
        for (size_t start = dot + 1; start < field.size();) {
            auto next = field.find('.', start);
            if (next == std::string::npos) {
                paths.emplace_back(field.substr(start));
                break;
            }
            paths.emplace_back(field.substr(start, next - start));
            start = next + 1;
        }
        return TabletColumn::create_materialized_variant_column(
                root, paths, ctx.column().unique_id(), ctx.column().variant_max_subcolumns_count());
    };

    auto infer_type_for_field = [&](const std::string& field,
                                   const TabletColumn& materialized) -> DataTypePtr {
        auto it = inferred_types.find(field);
        if (it != inferred_types.end()) {
            return it->second;
        }
        DataTypePtr inferred_type;
        auto st = variant_reader->infer_data_type_for_path(&inferred_type, materialized, read_opts,
                                                           nullptr);
        EXPECT_TRUE(st.ok()) << st.msg();
        inferred_types.emplace(field, inferred_type);
        return inferred_type;
    };

    std::unordered_map<std::string, std::unique_ptr<segment_v2::IndexIterator>> owned_iters;
    auto get_or_create_iter = [&](const std::string& field) -> segment_v2::IndexIterator* {
        auto it = owned_iters.find(field);
        if (it != owned_iters.end()) {
            return it->second.get();
        }
        auto materialized = build_materialized(field);
        auto dt = infer_type_for_field(field, materialized);
        auto index_holder = variant_reader->find_subcolumn_tablet_indexes(materialized, dt);
        if (index_holder.empty()) {
            ADD_FAILURE() << "No inverted index metadata for field: " << field;
            return nullptr;
        }
        std::unique_ptr<segment_v2::IndexIterator> index_iter;
        EXPECT_TRUE(segment
                            ->new_index_iterator(materialized, index_holder[0].get(), read_opts,
                                                 &index_iter)
                            .ok());
        if (index_iter == nullptr) {
            ADD_FAILURE() << "Failed to create index iterator for field: " << field;
            return nullptr;
        }
        index_iter->set_context(iterator_ctx);
        auto* raw = index_iter.get();
        owned_iters.emplace(field, std::move(index_iter));
        return raw;
    };

    auto types_for_fields = [&](std::vector<std::string> fields) {
        std::unordered_map<std::string, vectorized::IndexFieldNameAndTypePair> m;
        for (const auto& f : fields) {
            auto dot = f.find('.');
            std::string sub_path = (dot == std::string::npos) ? f : f.substr(dot + 1);
            auto materialized = build_materialized(f);
            auto dt = infer_type_for_field(f, materialized);
            m.emplace(f, std::make_pair("1." + sub_path, dt));
        }
        return m;
    };

    auto bindings_for_fields = [&](std::vector<std::string> fields) {
        std::vector<TSearchFieldBinding> bindings;

        for (const auto& f : fields) {
            TSearchFieldBinding b;
            b.field_name = f;
            b.slot_index = 0;
            b.__set_parent_field_name("data");
            auto dot = f.find('.');
            if (dot != std::string::npos) {
                b.__set_subcolumn_path(f.substr(dot + 1));
            }
            b.__set_is_variant_subcolumn(true);
            bindings.emplace_back(std::move(b));
        }
        return bindings;
    };

    auto fields = std::vector<std::string> {"data.items.subitems.msg"};
    auto types = types_for_fields(fields);
    TSearchParam p;
    p.__set_original_dsl("NESTED(data.items.subitems, msg=hit)");
    p.__set_field_bindings(bindings_for_fields(fields));
    p.__set_root(make_nested("data.items.subitems", make_term("data.items.subitems.msg", "hit")));

    std::unordered_map<std::string, segment_v2::IndexIterator*> iters;
    for (const auto& [field, _] : types) {
        iters[field] = get_or_create_iter(field);
        ASSERT_TRUE(iters[field] != nullptr);
    }
    std::unordered_map<std::string, int> field_name_to_column_id;

    segment_v2::InvertedIndexResultBitmap bitmap;
    auto st = fn.evaluate_inverted_index_with_search_param(p, types, std::move(iters), ctx.num_rows(),
                                                           bitmap, &index_exec_ctx,
                                                           field_name_to_column_id);
    EXPECT_TRUE(st.ok()) << st.msg();
    ASSERT_TRUE(bitmap.get_data_bitmap() != nullptr);
    EXPECT_EQ((std::vector<uint32_t> {0, 8191, 8192, 15000}),
              to_vec(*bitmap.get_data_bitmap()));
}

TEST_F(FunctionSearchVariantNestedTest, nested_offsets_mapping_index_handles_empty_blocks) {
    TestConfig cfg;
    cfg.tablet_id = 11010;
    VariantSegmentContext ctx(_engine_ref, _data_dir.get(), _absolute_dir, cfg);

    const size_t num_rows = 9000;
    const size_t hit_row = 5000;
    std::vector<std::string> jsons;
    jsons.reserve(num_rows);
    for (size_t i = 0; i < num_rows; ++i) {
        if (i == hit_row) {
            jsons.emplace_back(R"({"items":[{"subitems":[{"msg":"hit"}]}]})");
        } else {
            jsons.emplace_back(R"({"items":[]})");
        }
    }
    ASSERT_TRUE(ctx.write_json_data(jsons).ok());
    ASSERT_TRUE(ctx.finish_write().ok());

    auto segment_res = ctx.open_segment();
    ASSERT_TRUE(segment_res.has_value()) << segment_res.error().msg();
    auto segment = segment_res.value();

    bool found_items_offsets_index = false;
    ASSERT_TRUE(segment->traverse_column_meta_pbs([&](const ColumnMetaPB& meta) {
        if (!meta.has_column_path_info()) {
            return;
        }
        const auto& path_info = meta.column_path_info();
        if (!path_info.has_nested_group_parent_path()) {
            return;
        }
        if (!path_info.has_is_nested_group_offsets() || !path_info.is_nested_group_offsets()) {
            return;
        }
        if (path_info.nested_group_parent_path() != "items") {
            return;
        }
        for (int i = 0; i < meta.indexes_size(); ++i) {
            const auto& idx = meta.indexes(i);
            if (idx.type() == NESTED_OFFSETS_INDEX && idx.has_nested_offsets_index() &&
                idx.nested_offsets_index().block_size() == 4096) {
                found_items_offsets_index = true;
                return;
            }
        }
    }).ok());
    EXPECT_TRUE(found_items_offsets_index);

    StorageReadOptions read_opts;
    OlapReaderStatistics stats;
    read_opts.io_ctx.reader_type = ReaderType::READER_QUERY;
    read_opts.stats = &stats;

    std::vector<ColumnId> col_ids = {0};
    std::vector<std::unique_ptr<segment_v2::IndexIterator>> index_iters;
    std::vector<vectorized::IndexFieldNameAndTypePair> storage_name_and_type;
    storage_name_and_type.emplace_back("1", nullptr);
    std::unordered_map<ColumnId, std::unordered_map<const vectorized::VExpr*, bool>> expr_status;
    segment_v2::ColumnIteratorOptions col_iter_opts;
    col_iter_opts.stats = &stats;
    col_iter_opts.file_reader = segment->file_reader().get();
    IndexExecContext index_exec_ctx(col_ids, index_iters, storage_name_and_type, expr_status, nullptr,
                                   segment.get(), col_iter_opts);

    FunctionSearch fn;
    std::shared_ptr<segment_v2::ColumnReader> root_reader;
    {
        auto st = segment->get_column_reader(ctx.column(), &root_reader, &stats);
        EXPECT_TRUE(st.ok()) << st.msg();
    }
    ASSERT_TRUE(root_reader != nullptr);
    auto* variant_reader = dynamic_cast<segment_v2::VariantColumnReader*>(root_reader.get());
    ASSERT_TRUE(variant_reader != nullptr);
    {
        auto [found, chain, _] = variant_reader->collect_nested_group_chain("items.subitems");
        EXPECT_TRUE(found);
        ASSERT_FALSE(chain.empty());
        EXPECT_TRUE(chain.back()->offsets_mapping_index != nullptr);
    }

    const std::string field = "data.items.subitems.msg";
    auto dot = field.find('.');
    ASSERT_NE(dot, std::string::npos);
    std::string root = field.substr(0, dot);
    std::vector<std::string> paths;
    for (size_t start = dot + 1; start < field.size();) {
        auto next = field.find('.', start);
        if (next == std::string::npos) {
            paths.emplace_back(field.substr(start));
            break;
        }
        paths.emplace_back(field.substr(start, next - start));
        start = next + 1;
    }
    TabletColumn materialized =
            TabletColumn::create_materialized_variant_column(root, paths, ctx.column().unique_id(),
                                                             ctx.column().variant_max_subcolumns_count());
    DataTypePtr inferred_type;
    auto infer_st =
            variant_reader->infer_data_type_for_path(&inferred_type, materialized, read_opts, nullptr);
    EXPECT_TRUE(infer_st.ok()) << infer_st.msg();
    ASSERT_TRUE(inferred_type != nullptr);

    std::unordered_map<std::string, vectorized::IndexFieldNameAndTypePair> types;
    types.emplace(field, std::make_pair("1.items.subitems.msg", inferred_type));

    TSearchParam p;
    p.__set_original_dsl("NESTED(data.items.subitems, msg=hit)");
    std::vector<TSearchFieldBinding> bindings;
    TSearchFieldBinding b;
    b.field_name = field;
    b.slot_index = 0;
    b.__set_parent_field_name("data");
    b.__set_subcolumn_path("items.subitems.msg");
    b.__set_is_variant_subcolumn(true);
    bindings.emplace_back(std::move(b));
    p.__set_field_bindings(std::move(bindings));
    p.__set_root(make_nested("data.items.subitems", make_term(field, "hit")));

    std::unordered_map<std::string, int> field_name_to_column_id;

    auto iterator_ctx = std::make_shared<segment_v2::IndexQueryContext>();
    iterator_ctx->io_ctx = &read_opts.io_ctx;
    iterator_ctx->stats = &stats;
    auto index_holder = variant_reader->find_subcolumn_tablet_indexes(materialized, inferred_type);
    ASSERT_FALSE(index_holder.empty());
    std::unique_ptr<segment_v2::IndexIterator> index_iter;
    EXPECT_TRUE(segment->new_index_iterator(materialized, index_holder[0].get(), read_opts, &index_iter)
                        .ok());
    ASSERT_TRUE(index_iter != nullptr);
    index_iter->set_context(iterator_ctx);

    std::unordered_map<std::string, segment_v2::IndexIterator*> iters;
    iters[field] = index_iter.get();

    segment_v2::InvertedIndexResultBitmap bitmap;
    auto st = fn.evaluate_inverted_index_with_search_param(p, types, std::move(iters), ctx.num_rows(),
                                                           bitmap, &index_exec_ctx,
                                                           field_name_to_column_id);
    EXPECT_TRUE(st.ok()) << st.msg();
    ASSERT_TRUE(bitmap.get_data_bitmap() != nullptr);
    EXPECT_EQ((std::vector<uint32_t> {static_cast<uint32_t>(hit_row)}),
              to_vec(*bitmap.get_data_bitmap()));
}

TEST_F(FunctionSearchVariantNestedTest, nested_offsets_mapping_index_exists) {
    auto has_index = [](const std::shared_ptr<segment_v2::Segment>& segment) {
        bool found = false;
        auto st = segment->traverse_column_meta_pbs([&](const ColumnMetaPB& meta) {
            if (!meta.has_column_path_info()) {
                return;
            }
            const auto& path_info = meta.column_path_info();
            if (!path_info.has_nested_group_parent_path()) {
                return;
            }
            if (!path_info.has_is_nested_group_offsets() || !path_info.is_nested_group_offsets()) {
                return;
            }
            if (path_info.nested_group_parent_path() != "items") {
                return;
            }
            for (int i = 0; i < meta.indexes_size(); ++i) {
                const auto& idx = meta.indexes(i);
                if (idx.type() == NESTED_OFFSETS_INDEX && idx.has_nested_offsets_index()) {
                    found = true;
                    return;
                }
            }
        });
        EXPECT_TRUE(st.ok()) << st.msg();
        return found;
    };

    TestConfig cfg;
    cfg.tablet_id = 11015;
    VariantSegmentContext ctx(_engine_ref, _data_dir.get(), _absolute_dir, cfg);
    std::vector<std::string> jsons = {R"({"items":[{"subitems":[{"msg":"hit"}]}]})",
                                      R"({"items":[]})",
                                      R"({"items":[{"subitems":[{"msg":"hit"}]}]})"};
    ASSERT_TRUE(ctx.write_json_data(jsons).ok());
    ASSERT_TRUE(ctx.finish_write().ok());
    auto segment_res = ctx.open_segment();
    ASSERT_TRUE(segment_res.has_value()) << segment_res.error().msg();
    EXPECT_TRUE(has_index(segment_res.value()));
}

TEST_F(FunctionSearchVariantNestedTest, nested_query_missing_index_returns_empty) {
    TestConfig cfg;
    cfg.tablet_id = 11009;
    VariantSegmentContext ctx(_engine_ref, _data_dir.get(), _absolute_dir, cfg);

    std::vector<std::string> jsons = {
            R"({"items":[{"subitems":[{"msg":"hello"}]}]})",
            R"({"items":[{"subitems":[{"msg":"foo"}]}]})",
            R"({"items":[]})",
            R"(null)"};
    EXPECT_TRUE(ctx.write_json_data(jsons).ok());
    EXPECT_TRUE(ctx.finish_write().ok());

    auto segment_res = ctx.open_segment();
    ASSERT_TRUE(segment_res.has_value()) << segment_res.error().msg();
    auto segment = segment_res.value();

    StorageReadOptions read_opts;
    OlapReaderStatistics stats;
    read_opts.io_ctx.reader_type = ReaderType::READER_QUERY;
    read_opts.stats = &stats;

    std::vector<ColumnId> col_ids = {0};
    std::vector<std::unique_ptr<segment_v2::IndexIterator>> index_iters;
    std::vector<vectorized::IndexFieldNameAndTypePair> storage_name_and_type;
    storage_name_and_type.emplace_back("1", nullptr);
    std::unordered_map<ColumnId, std::unordered_map<const vectorized::VExpr*, bool>> expr_status;
    segment_v2::ColumnIteratorOptions col_iter_opts;
    col_iter_opts.stats = &stats;
    col_iter_opts.file_reader = segment->file_reader().get();
    IndexExecContext index_exec_ctx(col_ids, index_iters, storage_name_and_type, expr_status, nullptr,
                                   segment.get(), col_iter_opts);

    FunctionSearch fn;

    std::shared_ptr<segment_v2::ColumnReader> root_reader;
    EXPECT_TRUE(segment->get_column_reader(ctx.column(), &root_reader, &stats).ok());
    ASSERT_TRUE(root_reader != nullptr);
    auto* variant_reader = dynamic_cast<segment_v2::VariantColumnReader*>(root_reader.get());
    ASSERT_TRUE(variant_reader != nullptr);

    const std::string field = "data.items.subitems.msg";
    auto dot = field.find('.');
    ASSERT_NE(dot, std::string::npos);
    std::string root = field.substr(0, dot);
    std::vector<std::string> paths;
    for (size_t start = dot + 1; start < field.size();) {
        auto next = field.find('.', start);
        if (next == std::string::npos) {
            paths.emplace_back(field.substr(start));
            break;
        }
        paths.emplace_back(field.substr(start, next - start));
        start = next + 1;
    }
    TabletColumn materialized =
            TabletColumn::create_materialized_variant_column(root, paths, ctx.column().unique_id(),
                                                             ctx.column().variant_max_subcolumns_count());
    DataTypePtr inferred_type;
    auto infer_st =
            variant_reader->infer_data_type_for_path(&inferred_type, materialized, read_opts, nullptr);
    EXPECT_TRUE(infer_st.ok()) << infer_st.msg();
    ASSERT_TRUE(inferred_type != nullptr);

    std::unordered_map<std::string, vectorized::IndexFieldNameAndTypePair> types;
    types.emplace(field, std::make_pair("1.items.subitems.msg", inferred_type));

    TSearchParam p;
    p.__set_original_dsl("NESTED(data.items.subitems, msg=hello)");
    std::vector<TSearchFieldBinding> bindings;
    TSearchFieldBinding b;
    b.field_name = "data.items.subitems.msg";
    b.slot_index = 0;
    b.__set_parent_field_name("data");
    b.__set_subcolumn_path("items.subitems.msg");
    b.__set_is_variant_subcolumn(true);
    bindings.emplace_back(std::move(b));
    p.__set_field_bindings(std::move(bindings));
    p.__set_root(make_nested("data.items.subitems", make_term("data.items.subitems.msg", "hello")));

    std::unordered_map<std::string, int> field_name_to_column_id;

    auto iterator_ctx = std::make_shared<segment_v2::IndexQueryContext>();
    iterator_ctx->io_ctx = &read_opts.io_ctx;
    iterator_ctx->stats = &stats;
    auto index_holder = variant_reader->find_subcolumn_tablet_indexes(materialized, inferred_type);
    ASSERT_FALSE(index_holder.empty());
    std::unique_ptr<segment_v2::IndexIterator> index_iter;
    EXPECT_TRUE(segment->new_index_iterator(materialized, index_holder[0].get(), read_opts, &index_iter)
                        .ok());
    ASSERT_TRUE(index_iter != nullptr);
    index_iter->set_context(iterator_ctx);

    {
        std::unordered_map<std::string, segment_v2::IndexIterator*> iters;
        iters[field] = index_iter.get();
        segment_v2::InvertedIndexResultBitmap bitmap;
        auto st = fn.evaluate_inverted_index_with_search_param(p, types, std::move(iters), ctx.num_rows(),
                                                               bitmap, &index_exec_ctx,
                                                               field_name_to_column_id);
        EXPECT_TRUE(st.ok()) << st.msg();
        ASSERT_TRUE(bitmap.get_data_bitmap() != nullptr);
        EXPECT_EQ(std::vector<uint32_t>({0}), to_vec(*bitmap.get_data_bitmap()));
    }

    segment_v2::InvertedIndexResultBitmap bitmap;
    std::unordered_map<std::string, segment_v2::IndexIterator*> iters;
    iters[field] = nullptr;
    auto st = fn.evaluate_inverted_index_with_search_param(p, types, std::move(iters), ctx.num_rows(),
                                                           bitmap, &index_exec_ctx,
                                                           field_name_to_column_id);
    EXPECT_TRUE(st.ok()) << st.msg();
    ASSERT_TRUE(bitmap.get_data_bitmap() != nullptr);
    EXPECT_TRUE(bitmap.get_data_bitmap()->isEmpty());
}

TEST_F(FunctionSearchVariantNestedTest, nested_root_array_query_patches_stored_field_name) {
    TestConfig cfg;
    cfg.tablet_id = 11004;
    VariantSegmentContext ctx(_engine_ref, _data_dir.get(), _absolute_dir, cfg);

    std::vector<std::string> jsons = {
            R"([{"msg":"hello"},{"msg":"foo"}])",
            R"([{"msg":"bar"}])",
            R"([])",
            R"(null)",
    };
    EXPECT_TRUE(ctx.write_json_data(jsons).ok());
    EXPECT_TRUE(ctx.finish_write().ok());

    auto segment_res = ctx.open_segment();
    ASSERT_TRUE(segment_res.has_value()) << segment_res.error().msg();
    auto segment = segment_res.value();

    StorageReadOptions read_opts;
    OlapReaderStatistics stats;
    read_opts.io_ctx.reader_type = ReaderType::READER_QUERY;
    read_opts.stats = &stats;

    std::vector<ColumnId> col_ids = {0};
    std::vector<std::unique_ptr<segment_v2::IndexIterator>> index_iters;
    std::vector<vectorized::IndexFieldNameAndTypePair> storage_name_and_type;
    storage_name_and_type.emplace_back("1", nullptr);
    std::unordered_map<ColumnId, std::unordered_map<const vectorized::VExpr*, bool>> expr_status;
    segment_v2::ColumnIteratorOptions col_iter_opts;
    col_iter_opts.stats = &stats;
    col_iter_opts.file_reader = segment->file_reader().get();

    IndexExecContext index_exec_ctx(col_ids, index_iters, storage_name_and_type, expr_status, nullptr,
                                   segment.get(), col_iter_opts);

    FunctionSearch fn;

    auto iterator_ctx = std::make_shared<segment_v2::IndexQueryContext>();
    iterator_ctx->io_ctx = &read_opts.io_ctx;
    iterator_ctx->stats = &stats;
    std::shared_ptr<segment_v2::ColumnReader> root_reader;
    EXPECT_TRUE(segment->get_column_reader(ctx.column(), &root_reader, &stats).ok());
    ASSERT_TRUE(root_reader != nullptr);
    auto* variant_reader = dynamic_cast<segment_v2::VariantColumnReader*>(root_reader.get());
    ASSERT_TRUE(variant_reader != nullptr);

    auto build_materialized = [&](const std::string& field) -> TabletColumn {
        auto dot = field.find('.');
        EXPECT_NE(dot, std::string::npos);
        std::string root = field.substr(0, dot);
        std::vector<std::string> paths;
        for (size_t start = dot + 1; start < field.size();) {
            auto next = field.find('.', start);
            if (next == std::string::npos) {
                paths.emplace_back(field.substr(start));
                break;
            }
            paths.emplace_back(field.substr(start, next - start));
            start = next + 1;
        }
        return TabletColumn::create_materialized_variant_column(
                root, paths, ctx.column().unique_id(), ctx.column().variant_max_subcolumns_count());
    };

    std::unordered_map<std::string, DataTypePtr> inferred_types;
    auto infer_type_for_field = [&](const std::string& field,
                                   const TabletColumn& materialized) -> DataTypePtr {
        auto it = inferred_types.find(field);
        if (it != inferred_types.end()) {
            return it->second;
        }
        DataTypePtr inferred_type;
        auto st = variant_reader->infer_data_type_for_path(&inferred_type, materialized, read_opts,
                                                           nullptr);
        EXPECT_TRUE(st.ok()) << st.msg();
        if (inferred_type == nullptr) {
            inferred_type =
                    DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_STRING, true);
        }
        inferred_types.emplace(field, inferred_type);
        return inferred_type;
    };

    std::unordered_map<std::string, std::unique_ptr<segment_v2::IndexIterator>> owned_iters;
    auto get_or_create_iter = [&](const std::string& field) -> segment_v2::IndexIterator* {
        auto it = owned_iters.find(field);
        if (it != owned_iters.end()) {
            return it->second.get();
        }
        auto materialized = build_materialized(field);
        auto dt = infer_type_for_field(field, materialized);
        auto index_holder = variant_reader->find_subcolumn_tablet_indexes(materialized, dt);
        if (index_holder.empty()) {
            return nullptr;
        }
        std::unique_ptr<segment_v2::IndexIterator> index_iter;
        EXPECT_TRUE(segment
                            ->new_index_iterator(materialized, index_holder[0].get(), read_opts,
                                                 &index_iter)
                            .ok());
        if (index_iter == nullptr) {
            return nullptr;
        }
        index_iter->set_context(iterator_ctx);
        auto* raw = index_iter.get();
        owned_iters.emplace(field, std::move(index_iter));
        return raw;
    };

    const std::string field = "data.msg";
    std::unordered_map<std::string, vectorized::IndexFieldNameAndTypePair> types;
    {
        auto materialized = build_materialized(field);
        auto dt = infer_type_for_field(field, materialized);
        types.emplace(field, std::make_pair("1." + field.substr(field.find('.') + 1), dt));
    }
    std::unordered_map<std::string, segment_v2::IndexIterator*> iters;
    iters[field] = get_or_create_iter(field);
    ASSERT_TRUE(iters[field] != nullptr);

    std::unordered_map<std::string, int> field_name_to_column_id;

    TSearchParam p;
    p.__set_original_dsl("NESTED(data, msg=hello)");
    std::vector<TSearchFieldBinding> bindings;
    TSearchFieldBinding b;
    b.field_name = field;
    b.slot_index = 0;
    b.__set_parent_field_name("data");
    b.__set_subcolumn_path("msg");
    b.__set_is_variant_subcolumn(true);
    bindings.emplace_back(std::move(b));
    p.__set_field_bindings(std::move(bindings));
    p.__set_root(make_nested("data", make_term(field, "hello")));

    segment_v2::InvertedIndexResultBitmap bitmap;
    auto st = fn.evaluate_inverted_index_with_search_param(p, types, std::move(iters), ctx.num_rows(),
                                                           bitmap, &index_exec_ctx,
                                                           field_name_to_column_id);
    EXPECT_TRUE(st.ok()) << st.msg();
    ASSERT_TRUE(bitmap.get_data_bitmap() != nullptr);
    EXPECT_EQ(std::vector<uint32_t>({0}), to_vec(*bitmap.get_data_bitmap()));
}

TEST_F(FunctionSearchVariantNestedTest, nested_not_excludes_null_elements) {
    TestConfig cfg;
    cfg.tablet_id = 11005;
    VariantSegmentContext ctx(_engine_ref, _data_dir.get(), _absolute_dir, cfg);

    std::vector<std::string> jsons = {
            R"({"items":[{"subitems":[{"msg":null}]}]})",
            R"({"items":[{"subitems":[{"msg":"hello"}]}]})",
            R"({"items":[{"subitems":[{"msg":"foo"}]}]})",
            R"({"items":[{"subitems":[{"msg":"hello"},{"msg":null}]}]})",
            R"({"items":[]})",
    };
    EXPECT_TRUE(ctx.write_json_data(jsons).ok());
    EXPECT_TRUE(ctx.finish_write().ok());

    auto segment_res = ctx.open_segment();
    ASSERT_TRUE(segment_res.has_value()) << segment_res.error().msg();
    auto segment = segment_res.value();

    StorageReadOptions read_opts;
    OlapReaderStatistics stats;
    read_opts.io_ctx.reader_type = ReaderType::READER_QUERY;
    read_opts.stats = &stats;

    std::vector<ColumnId> col_ids = {0};
    std::vector<std::unique_ptr<segment_v2::IndexIterator>> index_iters;
    std::vector<vectorized::IndexFieldNameAndTypePair> storage_name_and_type;
    storage_name_and_type.emplace_back("1", nullptr);
    std::unordered_map<ColumnId, std::unordered_map<const vectorized::VExpr*, bool>> expr_status;
    segment_v2::ColumnIteratorOptions col_iter_opts;
    col_iter_opts.stats = &stats;
    col_iter_opts.file_reader = segment->file_reader().get();

    IndexExecContext index_exec_ctx(col_ids, index_iters, storage_name_and_type, expr_status, nullptr,
                                   segment.get(), col_iter_opts);

    FunctionSearch fn;

    auto iterator_ctx = std::make_shared<segment_v2::IndexQueryContext>();
    iterator_ctx->io_ctx = &read_opts.io_ctx;
    iterator_ctx->stats = &stats;
    std::shared_ptr<segment_v2::ColumnReader> root_reader;
    EXPECT_TRUE(segment->get_column_reader(ctx.column(), &root_reader, &stats).ok());
    ASSERT_TRUE(root_reader != nullptr);
    auto* variant_reader = dynamic_cast<segment_v2::VariantColumnReader*>(root_reader.get());
    ASSERT_TRUE(variant_reader != nullptr);

    std::unordered_map<std::string, DataTypePtr> inferred_types;
    auto build_materialized = [&](const std::string& field) -> TabletColumn {
        auto dot = field.find('.');
        EXPECT_NE(dot, std::string::npos);
        std::string root = field.substr(0, dot);
        std::vector<std::string> paths;
        for (size_t start = dot + 1; start < field.size();) {
            auto next = field.find('.', start);
            if (next == std::string::npos) {
                paths.emplace_back(field.substr(start));
                break;
            }
            paths.emplace_back(field.substr(start, next - start));
            start = next + 1;
        }
        return TabletColumn::create_materialized_variant_column(
                root, paths, ctx.column().unique_id(), ctx.column().variant_max_subcolumns_count());
    };
    auto infer_type_for_field = [&](const std::string& field,
                                   const TabletColumn& materialized) -> DataTypePtr {
        auto it = inferred_types.find(field);
        if (it != inferred_types.end()) {
            return it->second;
        }
        DataTypePtr inferred_type;
        auto st = variant_reader->infer_data_type_for_path(&inferred_type, materialized, read_opts,
                                                           nullptr);
        EXPECT_TRUE(st.ok()) << st.msg();
        if (inferred_type == nullptr) {
            inferred_type =
                    DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_STRING, true);
        }
        inferred_types.emplace(field, inferred_type);
        return inferred_type;
    };

    std::unordered_map<std::string, std::unique_ptr<segment_v2::IndexIterator>> owned_iters;
    auto get_or_create_iter = [&](const std::string& field) -> segment_v2::IndexIterator* {
        auto it = owned_iters.find(field);
        if (it != owned_iters.end()) {
            return it->second.get();
        }
        auto dot = field.find('.');
        if (dot == std::string::npos) {
            return nullptr;
        }
        std::string root = field.substr(0, dot);
        std::vector<std::string> paths;
        for (size_t start = dot + 1; start < field.size();) {
            auto next = field.find('.', start);
            if (next == std::string::npos) {
                paths.emplace_back(field.substr(start));
                break;
            }
            paths.emplace_back(field.substr(start, next - start));
            start = next + 1;
        }
        TabletColumn materialized =
                TabletColumn::create_materialized_variant_column(root, paths, ctx.column().unique_id(),
                                                                 ctx.column().variant_max_subcolumns_count());
        auto dt = infer_type_for_field(field, materialized);
        auto index_holder = variant_reader->find_subcolumn_tablet_indexes(materialized, dt);
        if (index_holder.empty()) {
            return nullptr;
        }
        std::unique_ptr<segment_v2::IndexIterator> index_iter;
        EXPECT_TRUE(segment
                            ->new_index_iterator(materialized, index_holder[0].get(), read_opts,
                                                 &index_iter)
                            .ok());
        if (index_iter == nullptr) {
            return nullptr;
        }
        index_iter->set_context(iterator_ctx);
        auto* raw = index_iter.get();
        owned_iters.emplace(field, std::move(index_iter));
        return raw;
    };

    const std::string field = "data.items.subitems.msg";
    std::unordered_map<std::string, vectorized::IndexFieldNameAndTypePair> types;
    {
        auto materialized = build_materialized(field);
        auto dt = infer_type_for_field(field, materialized);
        types.emplace(field, std::make_pair("1." + field.substr(field.find('.') + 1), dt));
    }

    std::unordered_map<std::string, segment_v2::IndexIterator*> iters;
    iters[field] = get_or_create_iter(field);
    ASSERT_TRUE(iters[field] != nullptr);

    std::unordered_map<std::string, int> field_name_to_column_id;

    TSearchParam p;
    p.__set_original_dsl("NESTED(data.items.subitems, NOT msg=hello)");
    std::vector<TSearchFieldBinding> bindings;
    TSearchFieldBinding b;
    b.field_name = field;
    b.slot_index = 0;
    b.__set_parent_field_name("data");
    b.__set_subcolumn_path("items.subitems.msg");
    b.__set_is_variant_subcolumn(true);
    bindings.emplace_back(std::move(b));
    p.__set_field_bindings(std::move(bindings));
    p.__set_root(make_nested("data.items.subitems", make_not({make_term(field, "hello")})));

    segment_v2::InvertedIndexResultBitmap bitmap;
    auto st = fn.evaluate_inverted_index_with_search_param(p, types, std::move(iters), ctx.num_rows(),
                                                           bitmap, &index_exec_ctx,
                                                           field_name_to_column_id);
    EXPECT_TRUE(st.ok()) << st.msg();
    ASSERT_TRUE(bitmap.get_data_bitmap() != nullptr);
    EXPECT_EQ(std::vector<uint32_t>({2}), to_vec(*bitmap.get_data_bitmap()));
}

TEST_F(FunctionSearchVariantNestedTest, nested_occur_boolean_minimum_should_match) {
    TestConfig cfg;
    cfg.tablet_id = 11008;
    VariantSegmentContext ctx(_engine_ref, _data_dir.get(), _absolute_dir, cfg);

    std::vector<std::string> jsons = {
            R"({"items":[{"subitems":[{"msg":"hello","title":"news"}]}]})",
            R"({"items":[{"subitems":[{"msg":"hello","title":"sports"}]}]})",
            R"({"items":[{"subitems":[{"msg":"foo","title":"news"}]}]})",
            R"({"items":[{"subitems":[{"msg":"hello","title":"news"},{"msg":"foo","title":"news"}]}]})",
    };
    EXPECT_TRUE(ctx.write_json_data(jsons).ok());
    EXPECT_TRUE(ctx.finish_write().ok());

    auto segment_res = ctx.open_segment();
    ASSERT_TRUE(segment_res.has_value()) << segment_res.error().msg();
    auto segment = segment_res.value();

    StorageReadOptions read_opts;
    OlapReaderStatistics stats;
    read_opts.io_ctx.reader_type = ReaderType::READER_QUERY;
    read_opts.stats = &stats;

    std::vector<ColumnId> col_ids = {0};
    std::vector<std::unique_ptr<segment_v2::IndexIterator>> index_iters;
    std::vector<vectorized::IndexFieldNameAndTypePair> storage_name_and_type;
    storage_name_and_type.emplace_back("1", nullptr);
    std::unordered_map<ColumnId, std::unordered_map<const vectorized::VExpr*, bool>> expr_status;
    segment_v2::ColumnIteratorOptions col_iter_opts;
    col_iter_opts.stats = &stats;
    col_iter_opts.file_reader = segment->file_reader().get();

    IndexExecContext index_exec_ctx(col_ids, index_iters, storage_name_and_type, expr_status, nullptr,
                                   segment.get(), col_iter_opts);

    FunctionSearch fn;

    auto iterator_ctx = std::make_shared<segment_v2::IndexQueryContext>();
    iterator_ctx->io_ctx = &read_opts.io_ctx;
    iterator_ctx->stats = &stats;
    std::shared_ptr<segment_v2::ColumnReader> root_reader;
    EXPECT_TRUE(segment->get_column_reader(ctx.column(), &root_reader, &stats).ok());
    ASSERT_TRUE(root_reader != nullptr);
    auto* variant_reader = dynamic_cast<segment_v2::VariantColumnReader*>(root_reader.get());
    ASSERT_TRUE(variant_reader != nullptr);

    std::unordered_map<std::string, DataTypePtr> inferred_types;
    auto build_materialized = [&](const std::string& field) -> TabletColumn {
        auto dot = field.find('.');
        EXPECT_NE(dot, std::string::npos);
        std::string root = field.substr(0, dot);
        std::vector<std::string> paths;
        for (size_t start = dot + 1; start < field.size();) {
            auto next = field.find('.', start);
            if (next == std::string::npos) {
                paths.emplace_back(field.substr(start));
                break;
            }
            paths.emplace_back(field.substr(start, next - start));
            start = next + 1;
        }
        return TabletColumn::create_materialized_variant_column(
                root, paths, ctx.column().unique_id(), ctx.column().variant_max_subcolumns_count());
    };
    auto infer_type_for_field = [&](const std::string& field,
                                   const TabletColumn& materialized) -> DataTypePtr {
        auto it = inferred_types.find(field);
        if (it != inferred_types.end()) {
            return it->second;
        }
        DataTypePtr inferred_type;
        auto st = variant_reader->infer_data_type_for_path(&inferred_type, materialized, read_opts,
                                                           nullptr);
        EXPECT_TRUE(st.ok()) << st.msg();
        if (inferred_type == nullptr) {
            inferred_type =
                    DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_STRING, true);
        }
        inferred_types.emplace(field, inferred_type);
        return inferred_type;
    };

    std::unordered_map<std::string, std::unique_ptr<segment_v2::IndexIterator>> owned_iters;
    auto get_or_create_iter = [&](const std::string& field) -> segment_v2::IndexIterator* {
        auto it = owned_iters.find(field);
        if (it != owned_iters.end()) {
            return it->second.get();
        }
        auto materialized = build_materialized(field);
        auto dt = infer_type_for_field(field, materialized);
        auto index_holder = variant_reader->find_subcolumn_tablet_indexes(materialized, dt);
        if (index_holder.empty()) {
            return nullptr;
        }
        std::unique_ptr<segment_v2::IndexIterator> index_iter;
        EXPECT_TRUE(segment
                            ->new_index_iterator(materialized, index_holder[0].get(), read_opts,
                                                 &index_iter)
                            .ok());
        if (index_iter == nullptr) {
            return nullptr;
        }
        index_iter->set_context(iterator_ctx);
        auto* raw = index_iter.get();
        owned_iters.emplace(field, std::move(index_iter));
        return raw;
    };

    const std::string field_msg = "data.items.subitems.msg";
    const std::string field_title = "data.items.subitems.title";
    std::unordered_map<std::string, vectorized::IndexFieldNameAndTypePair> types;
    {
        auto materialized = build_materialized(field_msg);
        auto dt = infer_type_for_field(field_msg, materialized);
        types.emplace(field_msg, std::make_pair("1." + field_msg.substr(field_msg.find('.') + 1), dt));
    }
    {
        auto materialized = build_materialized(field_title);
        auto dt = infer_type_for_field(field_title, materialized);
        types.emplace(field_title,
                      std::make_pair("1." + field_title.substr(field_title.find('.') + 1), dt));
    }

    std::unordered_map<std::string, segment_v2::IndexIterator*> iters;
    iters[field_msg] = get_or_create_iter(field_msg);
    iters[field_title] = get_or_create_iter(field_title);
    ASSERT_TRUE(iters[field_msg] != nullptr);
    ASSERT_TRUE(iters[field_title] != nullptr);

    std::unordered_map<std::string, int> field_name_to_column_id;

    TSearchParam p;
    p.__set_original_dsl(
            "NESTED(data.items.subitems, OCCUR_BOOLEAN((msg=hello) (title=news), minimum_should_match=2))");
    std::vector<TSearchFieldBinding> bindings;

    TSearchFieldBinding b1;
    b1.field_name = field_msg;
    b1.slot_index = 0;
    b1.__set_parent_field_name("data");
    b1.__set_subcolumn_path("items.subitems.msg");
    b1.__set_is_variant_subcolumn(true);
    bindings.emplace_back(std::move(b1));

    TSearchFieldBinding b2;
    b2.field_name = field_title;
    b2.slot_index = 0;
    b2.__set_parent_field_name("data");
    b2.__set_subcolumn_path("items.subitems.title");
    b2.__set_is_variant_subcolumn(true);
    bindings.emplace_back(std::move(b2));

    p.__set_field_bindings(std::move(bindings));
    p.__set_root(make_nested(
            "data.items.subitems",
            make_occur_boolean({with_occur(make_term(field_msg, "hello"), TSearchOccur::SHOULD),
                                with_occur(make_term(field_title, "news"), TSearchOccur::SHOULD)},
                               2)));

    segment_v2::InvertedIndexResultBitmap bitmap;
    auto st = fn.evaluate_inverted_index_with_search_param(p, types, std::move(iters), ctx.num_rows(),
                                                           bitmap, &index_exec_ctx,
                                                           field_name_to_column_id);
    EXPECT_TRUE(st.ok()) << st.msg();
    ASSERT_TRUE(bitmap.get_data_bitmap() != nullptr);
    EXPECT_EQ(std::vector<uint32_t>({0, 3}), to_vec(*bitmap.get_data_bitmap()));
}

TEST_F(FunctionSearchVariantNestedTest, nested_query_without_offsets_binding) {
    TestConfig cfg;
    cfg.tablet_id = 11006;
    VariantSegmentContext ctx(_engine_ref, _data_dir.get(), _absolute_dir, cfg);

    std::vector<std::string> jsons = {R"({"items":[{"subitems":[{"msg":"hello"}]}]})"};
    EXPECT_TRUE(ctx.write_json_data(jsons).ok());
    EXPECT_TRUE(ctx.finish_write().ok());

    auto segment_res = ctx.open_segment();
    ASSERT_TRUE(segment_res.has_value()) << segment_res.error().msg();
    auto segment = segment_res.value();

    StorageReadOptions read_opts;
    OlapReaderStatistics stats;
    read_opts.io_ctx.reader_type = ReaderType::READER_QUERY;
    read_opts.stats = &stats;

    std::vector<ColumnId> col_ids = {0};
    std::vector<std::unique_ptr<segment_v2::IndexIterator>> index_iters;
    std::vector<vectorized::IndexFieldNameAndTypePair> storage_name_and_type;
    storage_name_and_type.emplace_back("1", nullptr);
    std::unordered_map<ColumnId, std::unordered_map<const vectorized::VExpr*, bool>> expr_status;
    segment_v2::ColumnIteratorOptions col_iter_opts;
    col_iter_opts.stats = &stats;
    col_iter_opts.file_reader = segment->file_reader().get();

    IndexExecContext index_exec_ctx(col_ids, index_iters, storage_name_and_type, expr_status, nullptr,
                                   segment.get(), col_iter_opts);

    FunctionSearch fn;

    auto iterator_ctx = std::make_shared<segment_v2::IndexQueryContext>();
    iterator_ctx->io_ctx = &read_opts.io_ctx;
    iterator_ctx->stats = &stats;
    std::shared_ptr<segment_v2::ColumnReader> root_reader;
    EXPECT_TRUE(segment->get_column_reader(ctx.column(), &root_reader, &stats).ok());
    ASSERT_TRUE(root_reader != nullptr);
    auto* variant_reader = dynamic_cast<segment_v2::VariantColumnReader*>(root_reader.get());
    ASSERT_TRUE(variant_reader != nullptr);

    const std::string field = "data.items.subitems.msg";
    auto dot = field.find('.');
    ASSERT_NE(dot, std::string::npos);
    std::string root = field.substr(0, dot);
    std::vector<std::string> paths;
    for (size_t start = dot + 1; start < field.size();) {
        auto next = field.find('.', start);
        if (next == std::string::npos) {
            paths.emplace_back(field.substr(start));
            break;
        }
        paths.emplace_back(field.substr(start, next - start));
        start = next + 1;
    }
    TabletColumn materialized =
            TabletColumn::create_materialized_variant_column(root, paths, ctx.column().unique_id(),
                                                             ctx.column().variant_max_subcolumns_count());
    DataTypePtr inferred_type;
    auto infer_st =
            variant_reader->infer_data_type_for_path(&inferred_type, materialized, read_opts, nullptr);
    EXPECT_TRUE(infer_st.ok()) << infer_st.msg();
    ASSERT_TRUE(inferred_type != nullptr);

    std::unordered_map<std::string, vectorized::IndexFieldNameAndTypePair> types;
    types.emplace(field, std::make_pair("1.items.subitems.msg", inferred_type));

    auto index_holder = variant_reader->find_subcolumn_tablet_indexes(materialized, inferred_type);
    ASSERT_FALSE(index_holder.empty());
    std::unique_ptr<segment_v2::IndexIterator> index_iter;
    EXPECT_TRUE(segment->new_index_iterator(materialized, index_holder[0].get(), read_opts, &index_iter)
                        .ok());
    ASSERT_TRUE(index_iter != nullptr);
    index_iter->set_context(iterator_ctx);

    std::unordered_map<std::string, segment_v2::IndexIterator*> iters;
    iters[field] = index_iter.get();

    TSearchParam p;
    p.__set_original_dsl("NESTED(data.items.subitems, msg=hello)");
    std::vector<TSearchFieldBinding> bindings;
    TSearchFieldBinding b;
    b.field_name = field;
    b.slot_index = 0;
    b.__set_parent_field_name("data");
    b.__set_subcolumn_path("items.subitems.msg");
    b.__set_is_variant_subcolumn(true);
    bindings.emplace_back(std::move(b));
    p.__set_field_bindings(std::move(bindings));
    p.__set_root(make_nested("data.items.subitems", make_term(field, "hello")));

    std::unordered_map<std::string, int> field_name_to_column_id;

    segment_v2::InvertedIndexResultBitmap bitmap;
    auto st = fn.evaluate_inverted_index_with_search_param(p, types, std::move(iters), ctx.num_rows(),
                                                           bitmap, &index_exec_ctx,
                                                           field_name_to_column_id);
    EXPECT_TRUE(st.ok()) << st.msg();
    ASSERT_TRUE(bitmap.get_data_bitmap() != nullptr);
    EXPECT_EQ(std::vector<uint32_t>({0}), to_vec(*bitmap.get_data_bitmap()));
}

TEST_F(FunctionSearchVariantNestedTest, nested_clause_missing_fields_returns_error) {
    TestConfig cfg;
    cfg.tablet_id = 11007;
    VariantSegmentContext ctx(_engine_ref, _data_dir.get(), _absolute_dir, cfg);

    std::vector<std::string> jsons = {R"({"items":[{"subitems":[{"msg":"hello"}]}]})"};
    EXPECT_TRUE(ctx.write_json_data(jsons).ok());
    EXPECT_TRUE(ctx.finish_write().ok());

    auto segment_res = ctx.open_segment();
    ASSERT_TRUE(segment_res.has_value()) << segment_res.error().msg();
    auto segment = segment_res.value();

    StorageReadOptions read_opts;
    OlapReaderStatistics stats;
    read_opts.io_ctx.reader_type = ReaderType::READER_QUERY;
    read_opts.stats = &stats;

    std::vector<ColumnId> col_ids = {0};
    std::vector<std::unique_ptr<segment_v2::IndexIterator>> index_iters;
    std::vector<vectorized::IndexFieldNameAndTypePair> storage_name_and_type;
    storage_name_and_type.emplace_back("1", nullptr);
    std::unordered_map<ColumnId, std::unordered_map<const vectorized::VExpr*, bool>> expr_status;
    segment_v2::ColumnIteratorOptions col_iter_opts;
    col_iter_opts.stats = &stats;
    col_iter_opts.file_reader = segment->file_reader().get();

    IndexExecContext index_exec_ctx(col_ids, index_iters, storage_name_and_type, expr_status, nullptr,
                                   segment.get(), col_iter_opts);

    FunctionSearch fn;

    std::shared_ptr<segment_v2::ColumnReader> root_reader;
    EXPECT_TRUE(segment->get_column_reader(ctx.column(), &root_reader, &stats).ok());
    ASSERT_TRUE(root_reader != nullptr);
    auto* variant_reader = dynamic_cast<segment_v2::VariantColumnReader*>(root_reader.get());
    ASSERT_TRUE(variant_reader != nullptr);

    const std::string field = "data.items.subitems.msg";
    auto dot = field.find('.');
    ASSERT_NE(dot, std::string::npos);
    std::string root = field.substr(0, dot);
    std::vector<std::string> paths;
    for (size_t start = dot + 1; start < field.size();) {
        auto next = field.find('.', start);
        if (next == std::string::npos) {
            paths.emplace_back(field.substr(start));
            break;
        }
        paths.emplace_back(field.substr(start, next - start));
        start = next + 1;
    }
    TabletColumn materialized =
            TabletColumn::create_materialized_variant_column(root, paths, ctx.column().unique_id(),
                                                             ctx.column().variant_max_subcolumns_count());
    DataTypePtr inferred_type;
    auto infer_st =
            variant_reader->infer_data_type_for_path(&inferred_type, materialized, read_opts, nullptr);
    EXPECT_TRUE(infer_st.ok()) << infer_st.msg();
    ASSERT_TRUE(inferred_type != nullptr);

    std::unordered_map<std::string, vectorized::IndexFieldNameAndTypePair> types;
    types.emplace(field, std::make_pair("1.items.subitems.msg", inferred_type));

    auto index_holder = variant_reader->find_subcolumn_tablet_indexes(materialized, inferred_type);
    ASSERT_FALSE(index_holder.empty());
    std::unique_ptr<segment_v2::IndexIterator> index_iter;
    EXPECT_TRUE(segment->new_index_iterator(materialized, index_holder[0].get(), read_opts, &index_iter)
                        .ok());
    ASSERT_TRUE(index_iter != nullptr);
    auto iterator_ctx = std::make_shared<segment_v2::IndexQueryContext>();
    iterator_ctx->io_ctx = &read_opts.io_ctx;
    iterator_ctx->stats = &stats;
    index_iter->set_context(iterator_ctx);

    std::unordered_map<std::string, segment_v2::IndexIterator*> iters;
    iters[field] = index_iter.get();

    std::unordered_map<std::string, int> field_name_to_column_id;

    {
        TSearchParam p;
        p.__set_original_dsl("NESTED(data.items.subitems, msg=hello)");
        std::vector<TSearchFieldBinding> bindings;
        TSearchFieldBinding b;
        b.field_name = field;
        b.slot_index = 0;
        b.__set_parent_field_name("data");
        b.__set_subcolumn_path("items.subitems.msg");
        b.__set_is_variant_subcolumn(true);
        bindings.emplace_back(std::move(b));
        p.__set_field_bindings(std::move(bindings));

        TSearchClause nested;
        nested.clause_type = "NESTED";
        std::vector<TSearchClause> children;
        children.emplace_back(make_term(field, "hello"));
        nested.__set_children(std::move(children));
        p.__set_root(std::move(nested));

        segment_v2::InvertedIndexResultBitmap bitmap;
        auto st = fn.evaluate_inverted_index_with_search_param(p, types, iters, ctx.num_rows(), bitmap,
                                                               &index_exec_ctx,
                                                               field_name_to_column_id);
        EXPECT_FALSE(st.ok());
    }

    {
        TSearchParam p;
        p.__set_original_dsl("NESTED(data.items.subitems, )");
        std::vector<TSearchFieldBinding> bindings;
        TSearchFieldBinding b;
        b.field_name = field;
        b.slot_index = 0;
        b.__set_parent_field_name("data");
        b.__set_subcolumn_path("items.subitems.msg");
        b.__set_is_variant_subcolumn(true);
        bindings.emplace_back(std::move(b));
        p.__set_field_bindings(std::move(bindings));

        TSearchClause nested;
        nested.clause_type = "NESTED";
        nested.__set_nested_path("data.items.subitems");
        p.__set_root(std::move(nested));

        segment_v2::InvertedIndexResultBitmap bitmap;
        auto st = fn.evaluate_inverted_index_with_search_param(p, types, iters, ctx.num_rows(), bitmap,
                                                               &index_exec_ctx,
                                                               field_name_to_column_id);
        EXPECT_FALSE(st.ok());
    }
}

TEST_F(FunctionSearchVariantNestedTest, nested_and_requires_same_element) {
    TestConfig cfg;
    cfg.tablet_id = 11011;
    VariantSegmentContext ctx(_engine_ref, _data_dir.get(), _absolute_dir, cfg);

    std::vector<std::string> jsons = {
            R"({"items":[{"subitems":[{"msg":"hello"},{"title":"news"}]}]})",
            R"({"items":[{"subitems":[{"msg":"hello","title":"news"}]}]})",
            R"({"items":[{"subitems":[{"msg":"hello","title":"sports"}]}]})",
    };
    ASSERT_TRUE(ctx.write_json_data(jsons).ok());
    ASSERT_TRUE(ctx.finish_write().ok());

    auto segment_res = ctx.open_segment();
    ASSERT_TRUE(segment_res.has_value()) << segment_res.error().msg();
    auto segment = segment_res.value();

    StorageReadOptions read_opts;
    OlapReaderStatistics stats;
    read_opts.io_ctx.reader_type = ReaderType::READER_QUERY;
    read_opts.stats = &stats;

    std::vector<ColumnId> col_ids = {0};
    std::vector<std::unique_ptr<segment_v2::IndexIterator>> index_iters;
    std::vector<vectorized::IndexFieldNameAndTypePair> storage_name_and_type;
    storage_name_and_type.emplace_back("1", nullptr);
    std::unordered_map<ColumnId, std::unordered_map<const vectorized::VExpr*, bool>> expr_status;
    segment_v2::ColumnIteratorOptions col_iter_opts;
    col_iter_opts.stats = &stats;
    col_iter_opts.file_reader = segment->file_reader().get();
    IndexExecContext index_exec_ctx(col_ids, index_iters, storage_name_and_type, expr_status, nullptr,
                                   segment.get(), col_iter_opts);

    FunctionSearch fn;

    std::shared_ptr<segment_v2::ColumnReader> root_reader;
    ASSERT_TRUE(segment->get_column_reader(ctx.column(), &root_reader, &stats).ok());
    ASSERT_TRUE(root_reader != nullptr);
    auto* variant_reader = dynamic_cast<segment_v2::VariantColumnReader*>(root_reader.get());
    ASSERT_TRUE(variant_reader != nullptr);

    auto build_materialized = [&](const std::string& field) -> TabletColumn {
        auto dot = field.find('.');
        EXPECT_NE(dot, std::string::npos);
        std::string root = field.substr(0, dot);
        std::vector<std::string> paths;
        for (size_t start = dot + 1; start < field.size();) {
            auto next = field.find('.', start);
            if (next == std::string::npos) {
                paths.emplace_back(field.substr(start));
                break;
            }
            paths.emplace_back(field.substr(start, next - start));
            start = next + 1;
        }
        return TabletColumn::create_materialized_variant_column(
                root, paths, ctx.column().unique_id(), ctx.column().variant_max_subcolumns_count());
    };

    auto infer_type_for_field = [&](const std::string& field, const TabletColumn& materialized) {
        DataTypePtr inferred_type;
        auto st = variant_reader->infer_data_type_for_path(&inferred_type, materialized, read_opts,
                                                           nullptr);
        EXPECT_TRUE(st.ok()) << st.msg();
        return inferred_type;
    };

    const std::string field_msg = "data.items.subitems.msg";
    const std::string field_title = "data.items.subitems.title";

    auto materialized_msg = build_materialized(field_msg);
    auto materialized_title = build_materialized(field_title);
    auto dt_msg = infer_type_for_field(field_msg, materialized_msg);
    auto dt_title = infer_type_for_field(field_title, materialized_title);
    ASSERT_TRUE(dt_msg != nullptr);
    ASSERT_TRUE(dt_title != nullptr);

    std::unordered_map<std::string, vectorized::IndexFieldNameAndTypePair> types;
    types.emplace(field_msg, std::make_pair("1.items.subitems.msg", dt_msg));
    types.emplace(field_title, std::make_pair("1.items.subitems.title", dt_title));

    auto iterator_ctx = std::make_shared<segment_v2::IndexQueryContext>();
    iterator_ctx->io_ctx = &read_opts.io_ctx;
    iterator_ctx->stats = &stats;

    auto make_iter = [&](const std::string& field, const TabletColumn& materialized,
                         const DataTypePtr& dt) -> std::unique_ptr<segment_v2::IndexIterator> {
        auto index_holder = variant_reader->find_subcolumn_tablet_indexes(materialized, dt);
        EXPECT_FALSE(index_holder.empty());
        if (index_holder.empty()) {
            return nullptr;
        }
        std::unique_ptr<segment_v2::IndexIterator> index_iter;
        EXPECT_TRUE(segment->new_index_iterator(materialized, index_holder[0].get(), read_opts, &index_iter)
                            .ok());
        if (index_iter == nullptr) {
            return nullptr;
        }
        index_iter->set_context(iterator_ctx);
        return index_iter;
    };

    auto iter_msg = make_iter(field_msg, materialized_msg, dt_msg);
    auto iter_title = make_iter(field_title, materialized_title, dt_title);
    ASSERT_TRUE(iter_msg != nullptr);
    ASSERT_TRUE(iter_title != nullptr);

    std::unordered_map<std::string, segment_v2::IndexIterator*> iters;
    iters[field_msg] = iter_msg.get();
    iters[field_title] = iter_title.get();

    TSearchParam p;
    p.__set_original_dsl("NESTED(data.items.subitems, msg=hello AND title=news)");
    std::vector<TSearchFieldBinding> bindings;
    {
        TSearchFieldBinding b;
        b.field_name = field_msg;
        b.slot_index = 0;
        b.__set_parent_field_name("data");
        b.__set_subcolumn_path("items.subitems.msg");
        b.__set_is_variant_subcolumn(true);
        bindings.emplace_back(std::move(b));
    }
    {
        TSearchFieldBinding b;
        b.field_name = field_title;
        b.slot_index = 0;
        b.__set_parent_field_name("data");
        b.__set_subcolumn_path("items.subitems.title");
        b.__set_is_variant_subcolumn(true);
        bindings.emplace_back(std::move(b));
    }
    p.__set_field_bindings(std::move(bindings));
    p.__set_root(make_nested("data.items.subitems", make_and({make_term(field_msg, "hello"),
                                                             make_term(field_title, "news")})));

    std::unordered_map<std::string, int> field_name_to_column_id;

    segment_v2::InvertedIndexResultBitmap bitmap;
    auto st = fn.evaluate_inverted_index_with_search_param(p, types, std::move(iters), ctx.num_rows(),
                                                           bitmap, &index_exec_ctx,
                                                           field_name_to_column_id);
    EXPECT_TRUE(st.ok()) << st.msg();
    ASSERT_TRUE(bitmap.get_data_bitmap() != nullptr);
    EXPECT_EQ((std::vector<uint32_t> {1}), to_vec(*bitmap.get_data_bitmap()));
}

TEST_F(FunctionSearchVariantNestedTest, nested_clause_must_be_top_level) {
    TestConfig cfg;
    cfg.tablet_id = 11012;
    VariantSegmentContext ctx(_engine_ref, _data_dir.get(), _absolute_dir, cfg);

    std::vector<std::string> jsons = {R"({"items":[{"subitems":[{"msg":"hello"}]}]})"};
    ASSERT_TRUE(ctx.write_json_data(jsons).ok());
    ASSERT_TRUE(ctx.finish_write().ok());

    auto segment_res = ctx.open_segment();
    ASSERT_TRUE(segment_res.has_value()) << segment_res.error().msg();
    auto segment = segment_res.value();

    StorageReadOptions read_opts;
    OlapReaderStatistics stats;
    read_opts.io_ctx.reader_type = ReaderType::READER_QUERY;
    read_opts.stats = &stats;

    std::vector<ColumnId> col_ids = {0};
    std::vector<std::unique_ptr<segment_v2::IndexIterator>> index_iters;
    std::vector<vectorized::IndexFieldNameAndTypePair> storage_name_and_type;
    storage_name_and_type.emplace_back("1", nullptr);
    std::unordered_map<ColumnId, std::unordered_map<const vectorized::VExpr*, bool>> expr_status;
    segment_v2::ColumnIteratorOptions col_iter_opts;
    col_iter_opts.stats = &stats;
    col_iter_opts.file_reader = segment->file_reader().get();
    IndexExecContext index_exec_ctx(col_ids, index_iters, storage_name_and_type, expr_status, nullptr,
                                   segment.get(), col_iter_opts);

    FunctionSearch fn;

    std::shared_ptr<segment_v2::ColumnReader> root_reader;
    ASSERT_TRUE(segment->get_column_reader(ctx.column(), &root_reader, &stats).ok());
    ASSERT_TRUE(root_reader != nullptr);
    auto* variant_reader = dynamic_cast<segment_v2::VariantColumnReader*>(root_reader.get());
    ASSERT_TRUE(variant_reader != nullptr);

    const std::string field = "data.items.subitems.msg";
    auto dot = field.find('.');
    ASSERT_NE(dot, std::string::npos);
    std::string root = field.substr(0, dot);
    std::vector<std::string> paths;
    for (size_t start = dot + 1; start < field.size();) {
        auto next = field.find('.', start);
        if (next == std::string::npos) {
            paths.emplace_back(field.substr(start));
            break;
        }
        paths.emplace_back(field.substr(start, next - start));
        start = next + 1;
    }
    TabletColumn materialized =
            TabletColumn::create_materialized_variant_column(root, paths, ctx.column().unique_id(),
                                                             ctx.column().variant_max_subcolumns_count());
    DataTypePtr inferred_type;
    auto infer_st =
            variant_reader->infer_data_type_for_path(&inferred_type, materialized, read_opts, nullptr);
    ASSERT_TRUE(infer_st.ok()) << infer_st.msg();
    ASSERT_TRUE(inferred_type != nullptr);

    std::unordered_map<std::string, vectorized::IndexFieldNameAndTypePair> types;
    types.emplace(field, std::make_pair("1.items.subitems.msg", inferred_type));

    auto iterator_ctx = std::make_shared<segment_v2::IndexQueryContext>();
    iterator_ctx->io_ctx = &read_opts.io_ctx;
    iterator_ctx->stats = &stats;
    auto index_holder = variant_reader->find_subcolumn_tablet_indexes(materialized, inferred_type);
    ASSERT_FALSE(index_holder.empty());
    std::unique_ptr<segment_v2::IndexIterator> index_iter;
    ASSERT_TRUE(segment->new_index_iterator(materialized, index_holder[0].get(), read_opts, &index_iter)
                        .ok());
    ASSERT_TRUE(index_iter != nullptr);
    index_iter->set_context(iterator_ctx);

    std::unordered_map<std::string, segment_v2::IndexIterator*> iters;
    iters[field] = index_iter.get();

    TSearchParam p;
    p.__set_original_dsl("AND(NESTED(data.items.subitems, msg=hello), msg=hello)");
    std::vector<TSearchFieldBinding> bindings;
    TSearchFieldBinding b;
    b.field_name = field;
    b.slot_index = 0;
    b.__set_parent_field_name("data");
    b.__set_subcolumn_path("items.subitems.msg");
    b.__set_is_variant_subcolumn(true);
    bindings.emplace_back(std::move(b));
    p.__set_field_bindings(std::move(bindings));

    p.__set_root(make_and({make_nested("data.items.subitems", make_term(field, "hello")),
                           make_term(field, "hello")}));

    std::unordered_map<std::string, int> field_name_to_column_id;

    segment_v2::InvertedIndexResultBitmap bitmap;
    auto st = fn.evaluate_inverted_index_with_search_param(p, types, std::move(iters), ctx.num_rows(),
                                                           bitmap, &index_exec_ctx,
                                                           field_name_to_column_id);
    EXPECT_FALSE(st.ok());
    EXPECT_NE(st.to_string().find("top level"), std::string::npos);
}

TEST_F(FunctionSearchVariantNestedTest, nested_variable_offsets_large_data) {
    TestConfig cfg;
    cfg.tablet_id = 11013;
    VariantSegmentContext ctx(_engine_ref, _data_dir.get(), _absolute_dir, cfg);

    const size_t num_rows = 12000;
    std::vector<std::string> jsons;
    jsons.reserve(num_rows);
    std::vector<uint32_t> expected_rows;

    for (size_t i = 0; i < num_rows; ++i) {
        const size_t n = i % 5;
        if (i % 97 == 0 && n != 0) {
            expected_rows.emplace_back(static_cast<uint32_t>(i));
        }
        std::string s;
        s.append(R"({"items":[{"subitems":[)");
        for (size_t j = 0; j < n; ++j) {
            if (j != 0) {
                s.push_back(',');
            }
            const bool hit = (i % 97 == 0) && (j + 1 == n);
            if (hit) {
                s.append(R"({"msg":"hit"})");
            } else {
                s.append(R"({"msg":"miss"})");
            }
        }
        s.append(R"(]}]})");
        jsons.emplace_back(std::move(s));
    }

    ASSERT_TRUE(ctx.write_json_data(jsons).ok());
    ASSERT_TRUE(ctx.finish_write().ok());

    auto segment_res = ctx.open_segment();
    ASSERT_TRUE(segment_res.has_value()) << segment_res.error().msg();
    auto segment = segment_res.value();

    StorageReadOptions read_opts;
    OlapReaderStatistics stats;
    read_opts.io_ctx.reader_type = ReaderType::READER_QUERY;
    read_opts.stats = &stats;

    std::vector<ColumnId> col_ids = {0};
    std::vector<std::unique_ptr<segment_v2::IndexIterator>> index_iters;
    std::vector<vectorized::IndexFieldNameAndTypePair> storage_name_and_type;
    storage_name_and_type.emplace_back("1", nullptr);
    std::unordered_map<ColumnId, std::unordered_map<const vectorized::VExpr*, bool>> expr_status;
    segment_v2::ColumnIteratorOptions col_iter_opts;
    col_iter_opts.stats = &stats;
    col_iter_opts.file_reader = segment->file_reader().get();
    IndexExecContext index_exec_ctx(col_ids, index_iters, storage_name_and_type, expr_status, nullptr,
                                   segment.get(), col_iter_opts);

    FunctionSearch fn;

    std::shared_ptr<segment_v2::ColumnReader> root_reader;
    ASSERT_TRUE(segment->get_column_reader(ctx.column(), &root_reader, &stats).ok());
    ASSERT_TRUE(root_reader != nullptr);
    auto* variant_reader = dynamic_cast<segment_v2::VariantColumnReader*>(root_reader.get());
    ASSERT_TRUE(variant_reader != nullptr);

    const std::string field = "data.items.subitems.msg";
    auto dot = field.find('.');
    ASSERT_NE(dot, std::string::npos);
    std::string root = field.substr(0, dot);
    std::vector<std::string> paths;
    for (size_t start = dot + 1; start < field.size();) {
        auto next = field.find('.', start);
        if (next == std::string::npos) {
            paths.emplace_back(field.substr(start));
            break;
        }
        paths.emplace_back(field.substr(start, next - start));
        start = next + 1;
    }
    TabletColumn materialized =
            TabletColumn::create_materialized_variant_column(root, paths, ctx.column().unique_id(),
                                                             ctx.column().variant_max_subcolumns_count());
    DataTypePtr inferred_type;
    auto infer_st =
            variant_reader->infer_data_type_for_path(&inferred_type, materialized, read_opts, nullptr);
    ASSERT_TRUE(infer_st.ok()) << infer_st.msg();
    ASSERT_TRUE(inferred_type != nullptr);

    std::unordered_map<std::string, vectorized::IndexFieldNameAndTypePair> types;
    types.emplace(field, std::make_pair("1.items.subitems.msg", inferred_type));

    auto iterator_ctx = std::make_shared<segment_v2::IndexQueryContext>();
    iterator_ctx->io_ctx = &read_opts.io_ctx;
    iterator_ctx->stats = &stats;
    auto index_holder = variant_reader->find_subcolumn_tablet_indexes(materialized, inferred_type);
    ASSERT_FALSE(index_holder.empty());
    std::unique_ptr<segment_v2::IndexIterator> index_iter;
    ASSERT_TRUE(segment->new_index_iterator(materialized, index_holder[0].get(), read_opts, &index_iter)
                        .ok());
    ASSERT_TRUE(index_iter != nullptr);
    index_iter->set_context(iterator_ctx);

    std::unordered_map<std::string, segment_v2::IndexIterator*> iters;
    iters[field] = index_iter.get();

    TSearchParam p;
    p.__set_original_dsl("NESTED(data.items.subitems, msg=hit)");
    std::vector<TSearchFieldBinding> bindings;
    TSearchFieldBinding b;
    b.field_name = field;
    b.slot_index = 0;
    b.__set_parent_field_name("data");
    b.__set_subcolumn_path("items.subitems.msg");
    b.__set_is_variant_subcolumn(true);
    bindings.emplace_back(std::move(b));
    p.__set_field_bindings(std::move(bindings));
    p.__set_root(make_nested("data.items.subitems", make_term(field, "hit")));

    std::unordered_map<std::string, int> field_name_to_column_id;

    segment_v2::InvertedIndexResultBitmap bitmap;
    auto st = fn.evaluate_inverted_index_with_search_param(p, types, std::move(iters), ctx.num_rows(),
                                                           bitmap, &index_exec_ctx,
                                                           field_name_to_column_id);
    EXPECT_TRUE(st.ok()) << st.msg();
    ASSERT_TRUE(bitmap.get_data_bitmap() != nullptr);
    EXPECT_EQ(expected_rows, to_vec(*bitmap.get_data_bitmap()));
}

TEST_F(FunctionSearchVariantNestedTest, nested_offsets_mapping_index_micro_bench) {
    const char* env = std::getenv("DORIS_RUN_FUNCTION_SEARCH_MICRO_BENCH");
    if (env == nullptr || std::string(env) != "1") {
        GTEST_SKIP();
    }

    const std::string empty = R"({"items":[]})";
    const std::string hit = R"({"items":[{"subitems":[{"msg":"hit"},{"msg":"hit"},{"msg":"hit"},{"msg":"hit"}]}]})";
    const std::string rare =
            R"({"items":[{"subitems":[{"msg":"rare"},{"msg":"hit"},{"msg":"hit"},{"msg":"hit"}]}]})";

    const size_t num_rows = 2000000;
    std::vector<std::string> jsons;
    jsons.reserve(num_rows);
    for (size_t i = 0; i < num_rows; ++i) {
        if (i % 5 == 0) {
            jsons.emplace_back(empty);
            continue;
        }
        jsons.emplace_back((i % 16 == 0) ? rare : hit);
    }

    const std::string field = "data.items.subitems.msg";
    const int iters_hit = 20;
    const int iters_rare = 100;

    auto run_for_segment = [&](int tablet_id, const char* tag) {
        TestConfig cfg;
        cfg.tablet_id = tablet_id;
        VariantSegmentContext ctx(_engine_ref, _data_dir.get(), _absolute_dir, cfg);

        ASSERT_TRUE(ctx.write_json_data(jsons).ok());
        ASSERT_TRUE(ctx.finish_write().ok());

        auto segment_res = ctx.open_segment();
        ASSERT_TRUE(segment_res.has_value()) << segment_res.error().msg();
        auto segment = segment_res.value();

        StorageReadOptions read_opts;
        OlapReaderStatistics stats;
        read_opts.io_ctx.reader_type = ReaderType::READER_QUERY;
        read_opts.stats = &stats;

        std::vector<ColumnId> col_ids = {0};
        std::vector<std::unique_ptr<segment_v2::IndexIterator>> index_iters;
        std::vector<vectorized::IndexFieldNameAndTypePair> storage_name_and_type;
        storage_name_and_type.emplace_back("1", nullptr);
        std::unordered_map<ColumnId, std::unordered_map<const vectorized::VExpr*, bool>> expr_status;
        segment_v2::ColumnIteratorOptions col_iter_opts;
        col_iter_opts.stats = &stats;
        col_iter_opts.file_reader = segment->file_reader().get();
        IndexExecContext index_exec_ctx(col_ids, index_iters, storage_name_and_type, expr_status, nullptr,
                                       segment.get(), col_iter_opts);

        std::shared_ptr<segment_v2::ColumnReader> root_reader;
        ASSERT_TRUE(segment->get_column_reader(ctx.column(), &root_reader, &stats).ok());
        ASSERT_TRUE(root_reader != nullptr);
        auto* variant_reader = dynamic_cast<segment_v2::VariantColumnReader*>(root_reader.get());
        ASSERT_TRUE(variant_reader != nullptr);

        TabletColumn materialized = TabletColumn::create_materialized_variant_column(
                "data", {"items", "subitems", "msg"}, ctx.column().unique_id(),
                ctx.column().variant_max_subcolumns_count());
        DataTypePtr inferred_type;
        auto infer_st =
                variant_reader->infer_data_type_for_path(&inferred_type, materialized, read_opts, nullptr);
        ASSERT_TRUE(infer_st.ok()) << infer_st.msg();
        ASSERT_TRUE(inferred_type != nullptr);

        std::unordered_map<std::string, vectorized::IndexFieldNameAndTypePair> types;
        types.emplace(field, std::make_pair("1.items.subitems.msg", inferred_type));

        auto iterator_ctx = std::make_shared<segment_v2::IndexQueryContext>();
        iterator_ctx->io_ctx = &read_opts.io_ctx;
        iterator_ctx->stats = &stats;
        auto index_holder = variant_reader->find_subcolumn_tablet_indexes(materialized, inferred_type);
        ASSERT_FALSE(index_holder.empty());
        std::unique_ptr<segment_v2::IndexIterator> index_iter;
        ASSERT_TRUE(
                segment->new_index_iterator(materialized, index_holder[0].get(), read_opts, &index_iter)
                        .ok());
        ASSERT_TRUE(index_iter != nullptr);
        index_iter->set_context(iterator_ctx);

        std::unordered_map<std::string, segment_v2::IndexIterator*> base_iters;
        base_iters[field] = index_iter.get();

        auto make_param = [&](const std::string& term) {
            TSearchParam p;
            p.__set_original_dsl("NESTED(data.items.subitems, msg=" + term + ")");
            std::vector<TSearchFieldBinding> bindings;
            TSearchFieldBinding b;
            b.field_name = field;
            b.slot_index = 0;
            b.__set_parent_field_name("data");
            b.__set_subcolumn_path("items.subitems.msg");
            b.__set_is_variant_subcolumn(true);
            bindings.emplace_back(std::move(b));
            p.__set_field_bindings(std::move(bindings));
            p.__set_root(make_nested("data.items.subitems", make_term(field, term)));
            return p;
        };

        TSearchParam p_hit = make_param("hit");
        TSearchParam p_rare = make_param("rare");

        std::unordered_map<std::string, int> field_name_to_column_id;

        FunctionSearch fn;

        auto run_case = [&](const char* name, const TSearchParam& p, int iters) {
            {
                auto iters_map = base_iters;
                segment_v2::InvertedIndexResultBitmap bitmap;
                auto st = fn.evaluate_inverted_index_with_search_param(
                        p, types, std::move(iters_map), ctx.num_rows(), bitmap, &index_exec_ctx,
                        field_name_to_column_id);
                ASSERT_TRUE(st.ok()) << st.msg();
            }

            uint64_t checksum = 0;
            MonotonicStopWatch watch;
            watch.start();
            for (int i = 0; i < iters; ++i) {
                auto iters_map = base_iters;
                segment_v2::InvertedIndexResultBitmap bitmap;
                auto st = fn.evaluate_inverted_index_with_search_param(
                        p, types, std::move(iters_map), ctx.num_rows(), bitmap, &index_exec_ctx,
                        field_name_to_column_id);
                ASSERT_TRUE(st.ok()) << st.msg();
                checksum += bitmap.get_data_bitmap() ? bitmap.get_data_bitmap()->cardinality() : 0;
            }
            watch.stop();

            const double secs = static_cast<double>(watch.elapsed_time()) / 1e9;
            std::cerr << "[micro-bench] " << tag << "_" << name << " iters=" << iters
                      << " rows=" << num_rows << " secs=" << secs
                      << " qps=" << (static_cast<double>(iters) / secs)
                      << " avg_hit_rows=" << (static_cast<double>(checksum) / iters) << "\n";
        };

        std::cout << "----start bench (" << tag << ")----" << std::endl;
        run_case("nested_msg_hit", p_hit, iters_hit);
        run_case("nested_msg_rare", p_rare, iters_rare);
    };

    run_for_segment(11017, "v2");
}

} // namespace doris::vectorized
