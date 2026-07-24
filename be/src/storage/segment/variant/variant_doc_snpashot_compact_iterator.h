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

#include "core/column/column_map.h"
#include "storage/segment/column_reader.h"
#include "storage/segment/variant/hierarchical_data_iterator.h"

namespace doris::segment_v2 {

class VariantDocValueCompactIterator : public ColumnIterator {
public:
    VariantDocValueCompactIterator(ColumnIteratorUPtr&& column_iterator)
            : _doc_value_iterator(std::move(column_iterator)) {}

    Status init(const ColumnIteratorOptions& opts) override {
        VariantAssemblerPlanOptions plan_options;
        plan_options.mode = VariantAssemblerMode::HIERARCHICAL;
        plan_options.has_doc = true;
        std::shared_ptr<const VariantAssemblerPlan> plan;
        RETURN_IF_ERROR(VariantAssemblerPlan::create(std::move(plan_options), &plan));
        _assembler = std::make_unique<VariantAssembler>(std::move(plan));
        return _doc_value_iterator->init(opts);
    }

    Status seek_to_ordinal(ordinal_t ord) override {
        return _doc_value_iterator->seek_to_ordinal(ord);
    }

    Status next_batch(size_t* n, MutableColumnPtr& dst, bool* has_null) override {
        MutableColumnPtr doc_value_column =
                ColumnMap::create(ColumnString::create(), ColumnString::create(),
                                  ColumnArray::ColumnOffsets::create());
        RETURN_IF_ERROR(_doc_value_iterator->next_batch(n, doc_value_column, has_null));
        return _set_doc_value_into_variant(dst, doc_value_column, *n);
    }

    Status read_by_rowids(const rowid_t* rowids, const size_t count,
                          MutableColumnPtr& dst) override {
        MutableColumnPtr doc_value_column =
                ColumnMap::create(ColumnString::create(), ColumnString::create(),
                                  ColumnArray::ColumnOffsets::create());
        RETURN_IF_ERROR(_doc_value_iterator->read_by_rowids(rowids, count, doc_value_column));
        return _set_doc_value_into_variant(dst, doc_value_column, count);
    }

    ordinal_t get_current_ordinal() const override {
        return _doc_value_iterator->get_current_ordinal();
    }

private:
    Status _set_doc_value_into_variant(MutableColumnPtr& dst,
                                       const MutableColumnPtr& doc_value_column, size_t count) {
        const auto* doc = check_and_get_column<ColumnMap>(doc_value_column.get());
        if (doc == nullptr) {
            return Status::Corruption("Variant doc-value stream is not Map<String,String>");
        }
        VariantAssemblerBatchView batch;
        batch.num_rows = count;
        batch.doc_values = doc;
        VariantAssembledColumn assembled;
        RETURN_IF_ERROR(_assembler->assemble(batch, &assembled));
        return append_assembled_variant(dst, std::move(assembled));
    }

    ColumnIteratorUPtr _doc_value_iterator;
    std::unique_ptr<VariantAssembler> _assembler;
};

} // namespace doris::segment_v2
