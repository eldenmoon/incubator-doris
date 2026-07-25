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

package org.apache.doris.nereids.types;

import org.apache.doris.nereids.util.TypeCoercionUtils;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class VariantTypeTest {

    @Test
    public void storagePropertiesAndPredefinedFieldsDoNotChangeTheComputeType() {
        VariantType sparse = new VariantType(37);
        VariantType docMode = new VariantType(
                ImmutableList.of(), 2048, true, 4096, 8, true, 100, 16, false);
        VariantType predefined = new VariantType(ImmutableList.of(
                new VariantField("a", IntegerType.INSTANCE, "")));

        Assertions.assertEquals(sparse, docMode);
        Assertions.assertEquals(docMode, predefined);
        Assertions.assertEquals(sparse.hashCode(), docMode.hashCode());
        Assertions.assertEquals(docMode.hashCode(), predefined.hashCode());
        Assertions.assertTrue(sparse.equalsForRecursiveCte(predefined));

        Assertions.assertNotEquals(sparse.toSql(), docMode.toSql());
        Assertions.assertNotEquals(docMode.toSql(), predefined.toSql());
    }

    @Test
    public void computedCommonTypesDropStoragePropertiesRecursively() {
        VariantType sparse = new VariantType(37);
        VariantType docMode = new VariantType(
                ImmutableList.of(), 2048, true, 4096, 8, true, 100, 16, false);

        DataType common = TypeCoercionUtils.findWiderTypeForTwo(sparse, docMode, false, true)
                .orElseThrow();
        Assertions.assertSame(VariantType.INSTANCE, common);

        StructType nested = new StructType(ImmutableList.of(
                new StructField("items", ArrayType.of(sparse), true, ""),
                new StructField("lookup", MapType.of(StringType.INSTANCE, docMode), true, "")));
        StructType normalized =
                (StructType) TypeCoercionUtils.normalizeVariantForCompute(nested);
        ArrayType items = (ArrayType) normalized.getField("items").getDataType();
        MapType lookup = (MapType) normalized.getField("lookup").getDataType();
        Assertions.assertSame(VariantType.INSTANCE, items.getItemType());
        Assertions.assertSame(VariantType.INSTANCE, lookup.getValueType());

        Assertions.assertEquals(37, sparse.getVariantMaxSubcolumnsCount());
        Assertions.assertTrue(docMode.getEnableVariantDocMode());
    }
}
