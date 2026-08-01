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

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.VariantType;
import org.apache.doris.common.Config;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.types.DateType;
import org.apache.doris.nereids.types.DecimalV2Type;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class BaseViewInfoTest {
    @Test
    void testCreateFinalColsDoesNotPersistVariantV2ExecutionType() throws Exception {
        BaseViewInfo viewInfo = new BaseViewInfo(null, "", ImmutableList.of());

        viewInfo.createFinalCols(ImmutableList.of(
                SlotReference.of("v", org.apache.doris.nereids.types.VariantType.COMPUTE_V2_INSTANCE)));

        VariantType type = (VariantType) viewInfo.finalCols.get(0).getType();
        Assertions.assertFalse(type.isComputeV2());
    }

    @Test
    void testCreateFinalColsWithExplicitNamesDoesNotPersistVariantV2ExecutionType() throws Exception {
        BaseViewInfo viewInfo = new BaseViewInfo(
                null, "", ImmutableList.of(new SimpleColumnDefinition("renamed", "")));

        viewInfo.createFinalCols(ImmutableList.of(
                SlotReference.of("v", org.apache.doris.nereids.types.VariantType.COMPUTE_V2_INSTANCE)));

        VariantType type = (VariantType) viewInfo.finalCols.get(0).getType();
        Assertions.assertEquals("renamed", viewInfo.finalCols.get(0).getName());
        Assertions.assertFalse(type.isComputeV2());
    }

    @Test
    void testCreateFinalColsOnlyClearsNestedVariantMarker() throws Exception {
        boolean originalDateConversion = Config.enable_date_conversion;
        boolean originalDecimalConversion = Config.enable_decimal_conversion;
        try {
            Config.enable_date_conversion = true;
            Config.enable_decimal_conversion = true;
            BaseViewInfo viewInfo = new BaseViewInfo(null, "", ImmutableList.of());

            viewInfo.createFinalCols(ImmutableList.of(
                    SlotReference.of("v", org.apache.doris.nereids.types.ArrayType.of(
                            org.apache.doris.nereids.types.VariantType.COMPUTE_V2_INSTANCE)),
                    SlotReference.of("d", DateType.INSTANCE),
                    SlotReference.of("n", DecimalV2Type.createDecimalV2Type(10, 2))));

            ArrayType array = (ArrayType) viewInfo.finalCols.get(0).getType();
            Assertions.assertFalse(((VariantType) array.getItemType()).isComputeV2());
            Assertions.assertEquals(
                    PrimitiveType.DATE, viewInfo.finalCols.get(1).getType().getPrimitiveType());
            Assertions.assertEquals(
                    PrimitiveType.DECIMALV2, viewInfo.finalCols.get(2).getType().getPrimitiveType());
        } finally {
            Config.enable_date_conversion = originalDateConversion;
            Config.enable_decimal_conversion = originalDecimalConversion;
        }
    }
}
