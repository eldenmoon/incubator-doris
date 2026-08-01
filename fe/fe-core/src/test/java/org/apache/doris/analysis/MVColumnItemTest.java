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

package org.apache.doris.analysis;

import org.apache.doris.catalog.AggStateType;
import org.apache.doris.catalog.AggregateFunction;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.FunctionName;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarFunction;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.catalog.VariantType;
import org.apache.doris.common.Config;
import org.apache.doris.thrift.TExpr;
import org.apache.doris.thrift.TFunction;
import org.apache.doris.thrift.TTypeDesc;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

class MVColumnItemTest {
    @Test
    void testExplicitTypeDoesNotKeepVariantV2ExecutionType() throws Exception {
        VariantType type = (VariantType) org.apache.doris.nereids.types.VariantType.COMPUTE_V2_INSTANCE
                .toCatalogDataType();
        SlotRef defineExpr = new SlotRef(type, true);

        MVColumnItem item = new MVColumnItem("v", type, null, defineExpr);
        Column column = item.toMVColumn(null);

        Assertions.assertFalse(((VariantType) item.getType()).isComputeV2());
        Assertions.assertFalse(((VariantType) column.getType()).isComputeV2());
    }

    @Test
    void testExpressionTypeDoesNotKeepVariantV2ExecutionType() throws Exception {
        VariantType type = (VariantType) org.apache.doris.nereids.types.VariantType.COMPUTE_V2_INSTANCE
                .toCatalogDataType();
        SlotRef defineExpr = new SlotRef(type, true);

        MVColumnItem item = new MVColumnItem("v", defineExpr);
        Column column = item.toMVColumn(null);

        Assertions.assertFalse(((VariantType) item.getType()).isComputeV2());
        Assertions.assertFalse(((VariantType) column.getType()).isComputeV2());
    }

    @Test
    void testDefineExpressionTreeDoesNotKeepVariantV2ExecutionType() {
        VariantType type = (VariantType) org.apache.doris.nereids.types.VariantType.COMPUTE_V2_INSTANCE
                .toCatalogDataType();
        SlotRef slot = new SlotRef(type, true);
        ScalarFunction function = new ScalarFunction(new FunctionName("element_at"),
                List.of(type, Type.STRING), type, false, true);
        FunctionCallExpr defineExpr = new FunctionCallExpr(function,
                new FunctionParams(List.of(slot, new StringLiteral("k"))), true);

        MVColumnItem item = new MVColumnItem("v", type, null, defineExpr);

        Assertions.assertFalse(((VariantType) item.getDefineExpr().getType()).isComputeV2());
        Assertions.assertFalse(((VariantType) item.getDefineExpr().getChild(0).getType()).isComputeV2());
        TExpr thriftExpr = ExprToThriftVisitor.treeToThrift(item.getDefineExpr());
        Assertions.assertFalse(isVariantV2(thriftExpr.getNodes().get(0).getType()));
        Assertions.assertFalse(isVariantV2(thriftExpr.getNodes().get(1).getType()));
        TFunction thriftFunction = thriftExpr.getNodes().get(0).getFn();
        Assertions.assertFalse(isVariantV2(thriftFunction.getArgTypes().get(0)));
        Assertions.assertFalse(isVariantV2(thriftFunction.getRetType()));
    }

    @Test
    void testAggregateStateDoesNotKeepVariantV2ExecutionType() {
        VariantType type = (VariantType) org.apache.doris.nereids.types.VariantType.COMPUTE_V2_INSTANCE
                .toCatalogDataType();
        AggStateType aggStateType = new AggStateType("any_value", true, List.of(type), List.of(true));
        SlotRef slot = new SlotRef(type, true);
        AggregateFunction function = new AggregateFunction(new FunctionName("any_value_state"),
                new Type[] {type}, aggStateType, false, aggStateType, null,
                "init", "update", "merge", null, null, null, null);
        FunctionCallExpr defineExpr = new FunctionCallExpr(function,
                new FunctionParams(List.of(slot)), true);

        MVColumnItem item = new MVColumnItem("v", aggStateType, null, defineExpr);

        AggStateType itemType = (AggStateType) item.getType();
        Assertions.assertFalse(((VariantType) itemType.getSubTypes().get(0)).isComputeV2());
        TFunction thriftFunction = ExprToThriftVisitor.treeToThrift(item.getDefineExpr())
                .getNodes().get(0).getFn();
        Assertions.assertFalse(isVariantV2(thriftFunction.getArgTypes().get(0)));
        Assertions.assertFalse(isVariantV2(thriftFunction.getRetType().getSubTypes().get(0)));
        Assertions.assertFalse(isVariantV2(thriftFunction.getAggregateFn()
                .getIntermediateType().getSubTypes().get(0)));
    }

    private static boolean isVariantV2(TTypeDesc type) {
        return type.getTypes().get(0).getScalarType().isVariantIsV2();
    }

    @Test
    void testClearingVariantMarkerDoesNotConvertLegacyTypes() throws Exception {
        boolean originalDateConversion = Config.enable_date_conversion;
        boolean originalDecimalConversion = Config.enable_decimal_conversion;
        try {
            Config.enable_date_conversion = true;
            Config.enable_decimal_conversion = true;

            SlotRef dateExpr = new SlotRef(Type.DATE, true);
            MVColumnItem dateItem = new MVColumnItem("d", Type.DATE, null, dateExpr);
            Assertions.assertSame(Type.DATE, dateItem.getType());

            Type decimalV2 = ScalarType.createDecimalType(PrimitiveType.DECIMALV2, 10, 2);
            SlotRef decimalExpr = new SlotRef(decimalV2, true);
            MVColumnItem decimalItem = new MVColumnItem("n", decimalExpr);
            Assertions.assertSame(decimalV2, decimalItem.getType());
        } finally {
            Config.enable_date_conversion = originalDateConversion;
            Config.enable_decimal_conversion = originalDecimalConversion;
        }
    }
}
