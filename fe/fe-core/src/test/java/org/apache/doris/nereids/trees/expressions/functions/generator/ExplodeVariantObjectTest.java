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

package org.apache.doris.nereids.trees.expressions.functions.generator;

import org.apache.doris.catalog.BuiltinTableGeneratingFunctions;
import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.types.VariantType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.stream.Collectors;

public class ExplodeVariantObjectTest {

    @Test
    public void testFunctionsAreRegistered() {
        Set<String> names = BuiltinTableGeneratingFunctions.INSTANCE.tableGeneratingFunctions.stream()
                .flatMap(function -> function.names.stream())
                .collect(Collectors.toSet());
        Assertions.assertTrue(names.contains("explode_variant_object"));
        Assertions.assertTrue(names.contains("explode_variant_object_outer"));
        Assertions.assertTrue(BuiltinTableGeneratingFunctions.RETURN_MULTI_COLUMNS_FUNCTIONS
                .contains("explode_variant_object"));
        Assertions.assertTrue(BuiltinTableGeneratingFunctions.RETURN_MULTI_COLUMNS_FUNCTIONS
                .contains("explode_variant_object_outer"));
    }

    @Test
    public void testSignatureKeepsExactVariantConfiguration() {
        VariantType configuredVariant = new VariantType(7);
        ExplodeVariantObject function = new ExplodeVariantObject(
                SlotReference.of("v", configuredVariant));

        FunctionSignature signature = function.getSignatures().get(0);
        Assertions.assertEquals(configuredVariant, signature.getArgType(0));
        Assertions.assertTrue(signature.returnType.isStructType());
        StructType result = (StructType) signature.returnType;
        Assertions.assertEquals(2, result.getFields().size());
        Assertions.assertTrue(result.getFields().get(0).getDataType().isStringType());
        Assertions.assertTrue(result.getFields().get(1).getDataType().isJsonType());

        ExplodeVariantObjectOuter outer = new ExplodeVariantObjectOuter(
                SlotReference.of("v", configuredVariant));
        FunctionSignature outerSignature = outer.getSignatures().get(0);
        Assertions.assertEquals(configuredVariant, outerSignature.getArgType(0));
        Assertions.assertEquals(signature.returnType, outerSignature.returnType);
    }

    @Test
    public void testRejectsNonVariantInput() {
        ExplodeVariantObject function = new ExplodeVariantObject(
                SlotReference.of("i", IntegerType.INSTANCE));
        Assertions.assertThrows(AnalysisException.class, function::getSignatures);
    }
}
