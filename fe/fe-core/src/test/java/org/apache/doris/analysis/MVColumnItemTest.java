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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.catalog.VariantType;
import org.apache.doris.common.Config;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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
