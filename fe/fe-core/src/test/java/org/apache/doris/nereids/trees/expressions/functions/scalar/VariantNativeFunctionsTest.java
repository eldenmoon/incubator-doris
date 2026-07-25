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

package org.apache.doris.nereids.trees.expressions.functions.scalar;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.CaseWhen;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.WhenClause;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.VariantType;
import org.apache.doris.nereids.util.TypeCoercionUtils;
import org.apache.doris.qe.GlobalVariable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class VariantNativeFunctionsTest {

    @Test
    public void testVariantGetUsesLogicalVariantType() {
        VariantType configured = new VariantType(37);
        VariantGet function = new VariantGet(
                SlotReference.of("v", configured), new VarcharLiteral("$.a"));

        FunctionSignature signature = function.computeSignature(function.getSignatures().get(0));
        Assertions.assertSame(VariantType.INSTANCE, signature.getArgType(0));
        Assertions.assertSame(VariantType.INSTANCE, signature.returnType);
    }

    @Test
    public void testJsonPathsMustBeConstant() {
        SlotReference variant = SlotReference.of("v", VariantType.INSTANCE);
        SlotReference dynamicPath = SlotReference.of("p", StringType.INSTANCE);

        VariantGet get = new VariantGet(variant, dynamicPath);
        Assertions.assertThrows(AnalysisException.class, get::checkLegalityBeforeTypeCoercion);

        VariantExistsPath exists = new VariantExistsPath(variant, dynamicPath);
        Assertions.assertThrows(AnalysisException.class, exists::checkLegalityBeforeTypeCoercion);

        JsonbExtract extractAlias = new JsonbExtract(variant, dynamicPath);
        Assertions.assertThrows(AnalysisException.class, extractAlias::rewriteWhenAnalyze);

        JsonbExtractInt typedAlias = new JsonbExtractInt(variant, dynamicPath);
        Assertions.assertThrows(AnalysisException.class, typedAlias::rewriteWhenAnalyze);

        JsonbExistsPath existsAlias = new JsonbExistsPath(variant, dynamicPath);
        Assertions.assertThrows(AnalysisException.class, existsAlias::rewriteWhenAnalyze);
    }

    @Test
    public void testLegacyCoercionDropsVariantStorageProperties() {
        boolean original = GlobalVariable.enableNewTypeCoercionBehavior;
        GlobalVariable.enableNewTypeCoercionBehavior = false;
        try {
            VariantType sparse = new VariantType(37);
            VariantType docMode = new VariantType(
                    com.google.common.collect.ImmutableList.of(), 2048, true, 4096, 8,
                    true, 100, 16, false);
            SlotReference condition = SlotReference.of("condition", BooleanType.INSTANCE);

            FunctionSignature ifSignature = new If(condition,
                    SlotReference.of("sparse", sparse),
                    SlotReference.of("doc_mode", docMode)).customSignature();
            Assertions.assertSame(VariantType.INSTANCE, ifSignature.returnType);

            FunctionSignature coalesceSignature = new Coalesce(
                    SlotReference.of("sparse", sparse),
                    SlotReference.of("doc_mode", docMode)).customSignature();
            Assertions.assertSame(VariantType.INSTANCE, coalesceSignature.returnType);

            FunctionSignature nestedSignature = new If(condition,
                    SlotReference.of("sparse_array", ArrayType.of(sparse)),
                    SlotReference.of("doc_mode_array", ArrayType.of(docMode))).customSignature();
            Assertions.assertSame(VariantType.INSTANCE,
                    ((ArrayType) nestedSignature.returnType).getItemType());

            CaseWhen singleArm = new CaseWhen(com.google.common.collect.ImmutableList.of(
                    new WhenClause(condition, SlotReference.of("single", sparse))));
            Expression normalizedSingleArm = TypeCoercionUtils.processCaseWhen(singleArm);
            Assertions.assertSame(VariantType.INSTANCE, normalizedSingleArm.getDataType());

            CaseWhen equalArms = new CaseWhen(com.google.common.collect.ImmutableList.of(
                    new WhenClause(condition, SlotReference.of("first", sparse)),
                    new WhenClause(condition, SlotReference.of("second", docMode))));
            Expression normalizedEqualArms = TypeCoercionUtils.processCaseWhen(equalArms);
            Assertions.assertSame(VariantType.INSTANCE, normalizedEqualArms.getDataType());

            CaseWhen reversedArms = new CaseWhen(com.google.common.collect.ImmutableList.of(
                    new WhenClause(condition, SlotReference.of("canonical", VariantType.INSTANCE)),
                    new WhenClause(condition, SlotReference.of("configured", sparse))),
                    SlotReference.of("configured_default", docMode));
            CaseWhen normalizedReversedArms = (CaseWhen) TypeCoercionUtils.processCaseWhen(reversedArms);
            Assertions.assertSame(VariantType.INSTANCE,
                    normalizedReversedArms.getWhenClauses().get(1).getResult().getDataType());
            Assertions.assertSame(VariantType.INSTANCE,
                    normalizedReversedArms.getDefaultValue().orElseThrow().getDataType());
        } finally {
            GlobalVariable.enableNewTypeCoercionBehavior = original;
        }
    }
}
