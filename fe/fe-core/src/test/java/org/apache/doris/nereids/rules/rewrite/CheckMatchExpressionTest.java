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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.common.Config;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.MatchAll;
import org.apache.doris.nereids.trees.expressions.MatchAny;
import org.apache.doris.nereids.trees.expressions.MatchPhrase;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.VariantType;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;

class CheckMatchExpressionTest {

    private CheckMatchExpression checkMatchExpression;
    private Method checkChildrenMethod;
    private boolean originalEnableVariantV2;

    @BeforeEach
    void setUp() throws Exception {
        originalEnableVariantV2 = Config.enable_variant_v2;
        Config.enable_variant_v2 = true;
        checkMatchExpression = new CheckMatchExpression();
        checkChildrenMethod = CheckMatchExpression.class.getDeclaredMethod("checkChildren", LogicalFilter.class);
        checkChildrenMethod.setAccessible(true);
    }

    @AfterEach
    void tearDown() {
        Config.enable_variant_v2 = originalEnableVariantV2;
    }

    @Test
    void testAllowsRootVariantMatchAnyAndAll() {
        SlotReference rootVariantSlot = new SlotReference("response", VariantType.INSTANCE, true, Arrays.asList());
        Assertions.assertDoesNotThrow(
                () -> invokeCheck(new MatchAny(rootVariantSlot, new StringLiteral("doris"))));
        Assertions.assertDoesNotThrow(
                () -> invokeCheck(new MatchAll(rootVariantSlot, new StringLiteral("apache doris"))));
    }

    @Test
    void testAllowsRootVariantMatchNestedInOr() {
        SlotReference textSlot = new SlotReference("response_body", StringType.INSTANCE, true);
        SlotReference rootVariantSlot = new SlotReference("response", VariantType.INSTANCE, true, Arrays.asList());
        Or match = new Or(
                new MatchAny(textSlot, new StringLiteral("doris")),
                new MatchAny(rootVariantSlot, new StringLiteral("doris")));

        Assertions.assertDoesNotThrow(() -> invokeCheck(match));
    }

    @Test
    void testRejectsCastOnRootVariantMatch() {
        SlotReference rootVariantSlot = new SlotReference("response", VariantType.INSTANCE, true, Arrays.asList());
        MatchAny match = new MatchAny(new Cast(rootVariantSlot, StringType.INSTANCE), new StringLiteral("doris"));

        AnalysisException exception = Assertions.assertThrows(AnalysisException.class, () -> invokeCheck(match));
        Assertions.assertTrue(exception.getMessage().contains("must use the root column directly"),
                exception.getMessage());
    }

    @Test
    void testAllowsAliasOnRootVariantMatch() {
        SlotReference rootVariantSlot = new SlotReference("response", VariantType.INSTANCE, true, Arrays.asList());
        MatchAny match = new MatchAny(new Alias(rootVariantSlot, "response.trace_id"), new StringLiteral("doris"));

        Assertions.assertDoesNotThrow(() -> invokeCheck(match));
    }

    @Test
    void testRejectsRootVariantPhraseMatch() {
        SlotReference rootVariantSlot = new SlotReference("response", VariantType.INSTANCE, true, Arrays.asList());
        MatchPhrase match = new MatchPhrase(rootVariantSlot, new StringLiteral("apache doris"));

        AnalysisException exception = Assertions.assertThrows(AnalysisException.class, () -> invokeCheck(match));
        Assertions.assertTrue(exception.getMessage().contains("supports only MATCH"), exception.getMessage());
    }

    @Test
    void testAllowsVariantSubcolumnMatch() {
        SlotReference variantSubcolumnSlot = new SlotReference("response", VariantType.INSTANCE, true, Arrays.asList())
                .withSubPath(Arrays.asList("msg"));
        MatchAny match = new MatchAny(variantSubcolumnSlot, new StringLiteral("doris"));

        Assertions.assertDoesNotThrow(() -> invokeCheck(match));
    }

    @Test
    void testAllowsAliasOnVariantSubcolumnMatch() {
        SlotReference variantSubcolumnSlot = new SlotReference("response", VariantType.INSTANCE, true, Arrays.asList())
                .withSubPath(Arrays.asList("trace_id"));
        MatchAny match = new MatchAny(new Alias(variantSubcolumnSlot, "response.trace_id"),
                new StringLiteral("doris"));

        Assertions.assertDoesNotThrow(() -> invokeCheck(match));
    }

    @Test
    void testAllowsCastOnAliasVariantSubcolumnMatch() {
        SlotReference variantSubcolumnSlot = new SlotReference("response", VariantType.INSTANCE, true, Arrays.asList())
                .withSubPath(Arrays.asList("trace_id"));
        MatchAny match = new MatchAny(new Cast(new Alias(variantSubcolumnSlot, "response.trace_id"),
                StringType.INSTANCE), new StringLiteral("doris"));

        Assertions.assertDoesNotThrow(() -> invokeCheck(match));
    }

    @Test
    void testAllowsAliasAndCastChainOnVariantSubcolumnMatch() {
        SlotReference variantSubcolumnSlot = new SlotReference("response", VariantType.INSTANCE, true, Arrays.asList())
                .withSubPath(Arrays.asList("trace_id"));
        Expression left = new Cast(new Alias(new Cast(variantSubcolumnSlot, StringType.INSTANCE),
                "response.trace_id"), StringType.INSTANCE);
        MatchAny match = new MatchAny(left, new StringLiteral("doris"));

        Assertions.assertDoesNotThrow(() -> invokeCheck(match));
    }

    @Test
    void testRejectsAliasOnExpressionMatch() {
        Expression aliasOnExpression = new Alias(new Add(new IntegerLiteral(1), new IntegerLiteral(2)),
                "response.trace_id");
        MatchAny match = new MatchAny(aliasOnExpression, new StringLiteral("doris"));

        AnalysisException exception = Assertions.assertThrows(AnalysisException.class, () -> invokeCheck(match));
        Assertions.assertTrue(exception.getMessage().contains("Only support match left operand is SlotRef"),
                exception.getMessage());
    }

    private void invokeCheck(Expression expression) throws Throwable {
        LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        LogicalFilter<LogicalOlapScan> filter = new LogicalFilter<>(ImmutableSet.of(expression), scan);
        try {
            checkChildrenMethod.invoke(checkMatchExpression, filter);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }
}
