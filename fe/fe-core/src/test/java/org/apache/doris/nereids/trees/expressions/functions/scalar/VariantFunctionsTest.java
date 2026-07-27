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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.types.VariantType;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Unit tests for Variant V2 scalar function analysis. */
public class VariantFunctionsTest implements MemoPatternMatchSupported {

    @Test
    public void testVariantGetRequiresConstantPath() {
        VariantGet variantGet = new VariantGet(
                new SlotReference("v", VariantType.COMPUTE_V2_INSTANCE),
                new SlotReference("path", VarcharType.SYSTEM_DEFAULT));

        AnalysisException exception = Assertions.assertThrows(
                AnalysisException.class, variantGet::checkLegalityBeforeTypeCoercion);
        Assertions.assertTrue(exception.getMessage().contains("variant_get path must be constant"));
    }

    @Test
    public void testVariantGetPreservesVariantType() {
        VariantGet computeVariantGet = new VariantGet(
                new SlotReference("v", VariantType.COMPUTE_V2_INSTANCE), new VarcharLiteral("$.a[0]"));
        FunctionSignature computeSignature = computeVariantGet.computeSignature(VariantGet.SIGNATURES.get(0));
        Assertions.assertTrue(((VariantType) computeSignature.getArgType(0)).isComputeV2());
        Assertions.assertTrue(((VariantType) computeSignature.returnType).isComputeV2());
        Assertions.assertTrue(computeVariantGet.nullable());
    }

    @Test
    public void testVariantFunctionsRejectStoredVariant() {
        VariantType storedVariant = new VariantType(100);
        VariantGet variantGet = new VariantGet(
                new SlotReference("v", storedVariant), new VarcharLiteral("$.a[0]"));
        AnalysisException getException = Assertions.assertThrows(
                AnalysisException.class, variantGet::checkLegalityBeforeTypeCoercion);
        Assertions.assertTrue(getException.getMessage().contains("requires a Variant V2 input"));

        VariantTypeof variantTypeof = new VariantTypeof(new SlotReference("v", storedVariant));
        AnalysisException typeofException = Assertions.assertThrows(
                AnalysisException.class, variantTypeof::checkLegalityBeforeTypeCoercion);
        Assertions.assertTrue(typeofException.getMessage().contains("requires a Variant V2 input"));
    }

    @Test
    public void testVariantFunctionsPropagateBareNull() {
        VariantGet variantGet = new VariantGet(NullLiteral.INSTANCE, new VarcharLiteral("$"));
        Assertions.assertDoesNotThrow(variantGet::checkLegalityBeforeTypeCoercion);
        FunctionSignature getSignature = variantGet.computeSignature(VariantGet.SIGNATURES.get(0));
        Assertions.assertTrue(((VariantType) getSignature.returnType).isComputeV2());

        VariantTypeof variantTypeof = new VariantTypeof(NullLiteral.INSTANCE);
        Assertions.assertDoesNotThrow(variantTypeof::checkLegalityBeforeTypeCoercion);
    }

    @Test
    public void testVariantGetChecksPathForBareNullInput() {
        VariantGet variantGet = new VariantGet(
                NullLiteral.INSTANCE, new SlotReference("path", VarcharType.SYSTEM_DEFAULT));

        AnalysisException exception = Assertions.assertThrows(
                AnalysisException.class, variantGet::checkLegalityBeforeTypeCoercion);
        Assertions.assertTrue(exception.getMessage().contains("variant_get path must be constant"));
    }

    @Test
    public void testVariantTypeofSignature() {
        VariantTypeof variantTypeof = new VariantTypeof(
                new SlotReference("v", VariantType.COMPUTE_V2_INSTANCE));
        FunctionSignature signature = variantTypeof.computeSignature(VariantTypeof.SIGNATURES.get(0));

        Assertions.assertTrue(((VariantType) signature.getArgType(0)).isComputeV2());
        Assertions.assertEquals(StringType.INSTANCE, signature.returnType);
        Assertions.assertTrue(variantTypeof.nullable());
    }

    @Test
    public void testVariantFunctionsAreRegistered() {
        ConnectContext connectContext = MemoTestUtils.createConnectContext();
        connectContext.getSessionVariable().enableVariantV2 = true;
        try {
            PlanChecker.from(connectContext)
                    .analyze("select variant_get(parse_to_variant('{\"a\":[1]}'), '$.a[0]'), "
                            + "variant_typeof(parse_to_variant('{\"a\":[1]}'))")
                    .matches(
                            logicalOneRowRelation().when(oneRowRelation -> {
                                Expression variantGet = oneRowRelation.getProjects().get(0).child(0);
                                Expression variantTypeof = oneRowRelation.getProjects().get(1).child(0);
                                Assertions.assertInstanceOf(VariantGet.class, variantGet);
                                Assertions.assertInstanceOf(VariantTypeof.class, variantTypeof);
                                Assertions.assertTrue(((VariantType) variantGet.getDataType()).isComputeV2());
                                Assertions.assertEquals(StringType.INSTANCE, variantTypeof.getDataType());
                                return true;
                            })
                    );
            PlanChecker.from(connectContext)
                    .analyze("select variant_typeof(variant_get(NULL, '$'))")
                    .matches(
                            logicalOneRowRelation().when(oneRowRelation -> {
                                Expression variantTypeof = oneRowRelation.getProjects().get(0).child(0);
                                Assertions.assertInstanceOf(VariantTypeof.class, variantTypeof);
                                Expression variantGet = variantTypeof.child(0);
                                Assertions.assertInstanceOf(VariantGet.class, variantGet);
                                Assertions.assertTrue(((VariantType) variantGet.getDataType()).isComputeV2());
                                return true;
                            })
                    );
            PlanChecker.from(connectContext)
                    .analyze("select variant_get(NULL, '$'), variant_typeof(NULL)")
                    .rewrite()
                    .matches(
                            logicalResultSink(
                                    logicalOneRowRelation().when(oneRowRelation -> {
                                        Assertions.assertInstanceOf(
                                                NullLiteral.class,
                                                oneRowRelation.getProjects().get(0).child(0));
                                        Assertions.assertInstanceOf(
                                                NullLiteral.class,
                                                oneRowRelation.getProjects().get(1).child(0));
                                        return true;
                                    }))
                    );
        } finally {
            connectContext.getSessionVariable().enableVariantV2 = false;
        }
    }
}
