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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.datasource.doris.RemoteOlapTable;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.analyzer.UnboundTableSinkCreator;
import org.apache.doris.nereids.jobs.executor.Analyzer;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ParseToVariant;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.info.DMLCommandType;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertUtils;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapTableSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.types.VariantType;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.thrift.TPartialUpdateNewRowPolicy;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

class BindSinkVariantV2Test extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("bind_sink_variant_v2_test");
        connectContext.setDatabase("bind_sink_variant_v2_test");
        boolean originalEnableFlattenNested =
                connectContext.getSessionVariable().getEnableVariantFlattenNested();
        try {
            connectContext.getSessionVariable().setEnableVariantFlattenNested(true);
            createTables(
                    "CREATE TABLE variant_source (k INT, v VARIANT) DUPLICATE KEY(k) "
                            + "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES(\"replication_num\"=\"1\")",
                    "CREATE TABLE variant_target (k INT, v VARIANT) DUPLICATE KEY(k) "
                            + "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES(\"replication_num\"=\"1\")",
                    "CREATE TABLE variant_partial_target (k INT, v VARIANT, other INT DEFAULT \"0\") "
                            + "UNIQUE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES("
                            + "\"replication_num\"=\"1\", "
                            + "\"enable_unique_key_merge_on_write\"=\"true\")",
                    "CREATE TABLE variant_nested_target (k INT, v VARIANT<PROPERTIES("
                            + "\"variant_enable_nested_group\"=\"true\")>) DUPLICATE KEY(k) "
                            + "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES(\"replication_num\"=\"1\")",
                    "CREATE TABLE variant_flatten_target (k INT, v VARIANT) DUPLICATE KEY(k) "
                            + "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES("
                            + "\"replication_num\"=\"1\", "
                            + "\"deprecated_variant_enable_flatten_nested\"=\"true\")"
            );
        } finally {
            connectContext.getSessionVariable()
                    .setEnableVariantFlattenNested(originalEnableFlattenNested);
        }
        createMvByNereids(
                "CREATE MATERIALIZED VIEW variant_mtmv BUILD DEFERRED REFRESH COMPLETE ON MANUAL "
                        + "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES(\"replication_num\"=\"1\") "
                        + "AS SELECT k, v FROM variant_source");
    }

    @Test
    void testVariantV2InsertSelectSinkTypes() {
        LogicalOlapTableSink<?> sink = assertSinkVariantType(true, true, () -> analyzeInsertSink(
                "INSERT INTO variant_target SELECT k, v FROM variant_source"));
        assertNoVariantRepresentationCast(sink);
    }

    @Test
    void testLegacyVariantInsertSelectSinkTypesWhenDisabled() {
        LogicalOlapTableSink<?> sink = assertSinkVariantType(false, false, () -> analyzeInsertSink(
                "INSERT INTO variant_target SELECT k, v FROM variant_source"));
        assertNoVariantRepresentationCast(sink);
    }

    @Test
    void testValuesSinkUsesLegacyVariant() {
        LogicalOlapTableSink<?> sink = assertSinkVariantType(true, false, () -> analyzeValuesSink(
                "INSERT INTO variant_target VALUES (1, '{\"a\": 1}')"));
        assertNoVariantRepresentationCast(sink);
    }

    @Test
    void testVariantValuesSinkUsesLegacyVariant() {
        LogicalOlapTableSink<?> singleRowSink = analyzeWithVariantV2(true, () -> analyzeValuesSink(
                "INSERT INTO variant_target VALUES (1, parse_to_variant('{\"a\": 1}'))"));
        assertVariantExecutionType(singleRowSink, false);
        assertOnlyVariantV2ToV1RepresentationCast(singleRowSink);

        LogicalOlapTableSink<?> multiRowSink = analyzeWithVariantV2(true, () -> analyzeValuesSink(
                "INSERT INTO variant_target VALUES (1, parse_to_variant('{\"a\": 1}')), "
                        + "(2, parse_to_variant('{\"a\": 2}'))"));
        assertVariantExecutionType(multiRowSink, false);
        assertOnlyVariantV2ToV1RepresentationCast(multiRowSink);
    }

    @Test
    void testVariantConstantSelectSinkUsesV2() {
        LogicalOlapTableSink<?> singleRowSink = analyzeWithVariantV2(true, () -> analyzeInsertSink(
                "INSERT INTO variant_target SELECT 1, parse_to_variant('{\"a\": 1}')"));
        assertVariantExecutionType(singleRowSink, true);
        assertNoVariantRepresentationCast(singleRowSink);

        LogicalOlapTableSink<?> unionSink = analyzeWithVariantV2(true, () -> analyzeInsertSink(
                "INSERT INTO variant_target "
                        + "SELECT 1, parse_to_variant('{\"a\": 1}') UNION ALL "
                        + "SELECT 2, parse_to_variant('{\"a\": 2}')"));
        assertVariantExecutionType(unionSink, true);
        assertNoVariantRepresentationCast(unionSink);
    }

    @Test
    void testStringSelectSinkUsesLegacyVariant() {
        LogicalOlapTableSink<?> sink = assertSinkVariantType(true, false, () -> analyzeInsertSink(
                "INSERT INTO variant_target SELECT k, CAST(v AS STRING) FROM variant_source"));
        assertStandardStringVariantCast(sink);
        assertNoVariantRepresentationCast(sink);
    }

    @Test
    void testLoadSinkUsesLegacyVariant() {
        LogicalOlapTableSink<?> sink = assertSinkVariantType(true, false, () -> analyzeCustomSink(
                "variant_target", false, DMLCommandType.LOAD, ImmutableList.of(),
                "SELECT k, v FROM variant_source"));
        assertOnlyVariantV2ToV1RepresentationCast(sink);
    }

    @Test
    void testMtmvSinkUsesLegacyVariant() {
        LogicalOlapTableSink<?> sink = assertSinkVariantType(true, false, () -> analyzeCustomSink(
                "variant_mtmv", false, DMLCommandType.NONE, ImmutableList.of(),
                "SELECT k, v FROM variant_source"));
        assertOnlyVariantV2ToV1RepresentationCast(sink);
    }

    @Test
    void testPartialUpdateSinkUsesLegacyVariant() {
        LogicalOlapTableSink<?> sink = assertSinkVariantType(true, false, () -> analyzeCustomSink(
                "variant_partial_target", true, DMLCommandType.INSERT, ImmutableList.of("k", "v"),
                "SELECT k, v FROM variant_source"));
        assertOnlyVariantV2ToV1RepresentationCast(sink);
    }

    @Test
    void testNestedGroupSinkUsesLegacyVariant() {
        LogicalOlapTableSink<?> sink = assertSinkVariantType(true, false, () -> analyzeCustomSink(
                "variant_nested_target", false, DMLCommandType.INSERT, ImmutableList.of(),
                "SELECT k, v FROM variant_source"));
        assertOnlyVariantV2ToV1RepresentationCast(sink);
    }

    @Test
    void testFlattenNestedSinkUsesLegacyVariant() {
        LogicalOlapTableSink<?> sink = assertSinkVariantType(true, false, () -> analyzeCustomSink(
                "variant_flatten_target", false, DMLCommandType.INSERT, ImmutableList.of(),
                "SELECT k, v FROM variant_source"));
        assertOnlyVariantV2ToV1RepresentationCast(sink);
    }

    @Test
    void testMergeSinkUsesLegacyVariant() {
        LogicalOlapTableSink<?> sink = assertSinkVariantType(true, false, () -> analyzeCustomSink(
                "variant_partial_target", false, DMLCommandType.INSERT,
                ImmutableList.of("k", "v", "other", Column.DELETE_SIGN),
                "SELECT k, v, 0, CAST(0 AS TINYINT) FROM variant_source"));
        assertOnlyVariantV2ToV1RepresentationCast(sink);
    }

    @Test
    void testRemoteTargetIsNotEligibleForVariantV2Sink() {
        Assertions.assertTrue(BindSink.isVariantV2SinkEligible(
                new OlapTable(), DMLCommandType.INSERT, false, false, true));
        Assertions.assertFalse(BindSink.isVariantV2SinkEligible(
                new RemoteOlapTable(), DMLCommandType.INSERT, false, false, true));
    }

    private LogicalOlapTableSink<?> assertSinkVariantType(boolean enableVariantV2,
            boolean expectedComputeV2, Supplier<LogicalOlapTableSink<?>> sinkSupplier) {
        LogicalOlapTableSink<?> sink = analyzeWithVariantV2(enableVariantV2, sinkSupplier);
        assertVariantExecutionType(sink, expectedComputeV2);
        assertNoParseToVariant(sink);
        return sink;
    }

    private LogicalOlapTableSink<?> analyzeWithVariantV2(boolean enableVariantV2,
            Supplier<LogicalOlapTableSink<?>> sinkSupplier) {
        boolean originalEnableVariantV2 = connectContext.getSessionVariable().enableVariantV2;
        try {
            connectContext.getSessionVariable().enableVariantV2 = enableVariantV2;
            return sinkSupplier.get();
        } finally {
            connectContext.getSessionVariable().enableVariantV2 = originalEnableVariantV2;
        }
    }

    private LogicalOlapTableSink<?> analyzeInsertSink(String sql) {
        LogicalPlan parsed = new NereidsParser().parseSingle(sql);
        Assertions.assertInstanceOf(InsertIntoTableCommand.class, parsed);
        return analyzeSink(((InsertIntoTableCommand) parsed).getLogicalQuery());
    }

    private LogicalOlapTableSink<?> analyzeValuesSink(String sql) {
        LogicalPlan parsed = new NereidsParser().parseSingle(sql);
        Assertions.assertInstanceOf(InsertIntoTableCommand.class, parsed);
        LogicalPlan sinkPlan = ((InsertIntoTableCommand) parsed).getLogicalQuery();
        TableIf targetTable = InsertUtils.getTargetTable(sinkPlan, connectContext);
        sinkPlan = (LogicalPlan) InsertUtils.normalizePlan(
                sinkPlan, targetTable, Optional.empty(), Optional.empty());
        return analyzeSink(sinkPlan);
    }

    private LogicalOlapTableSink<?> analyzeCustomSink(String targetTable, boolean isPartialUpdate,
            DMLCommandType commandType, List<String> targetColumns, String querySql) {
        LogicalPlan query = new NereidsParser().parseSingle(querySql);
        LogicalPlan sinkPlan = (LogicalPlan) UnboundTableSinkCreator.createUnboundTableSink(
                ImmutableList.of(targetTable), targetColumns, ImmutableList.of(),
                false, ImmutableList.of(), isPartialUpdate, TPartialUpdateNewRowPolicy.APPEND,
                commandType, query);
        return analyzeSink(sinkPlan);
    }

    private LogicalOlapTableSink<?> analyzeSink(LogicalPlan sinkPlan) {
        CascadesContext cascadesContext = CascadesContext.initContext(
                MemoTestUtils.createStatementContext(connectContext, ""),
                sinkPlan, PhysicalProperties.ANY);
        Analyzer.buildCustomAnalyzer(cascadesContext, ImmutableList.of(
                Analyzer.bottomUp(new BindRelation(), new BindExpression()),
                Analyzer.topDown(new BindSink(false))
        )).analyze();
        Assertions.assertInstanceOf(LogicalOlapTableSink.class, cascadesContext.getRewritePlan());
        return (LogicalOlapTableSink<?>) cascadesContext.getRewritePlan();
    }

    private void assertVariantExecutionType(LogicalOlapTableSink<?> sink, boolean expectedComputeV2) {
        Slot childVariant = sink.child().getOutput().get(1);
        Slot targetVariant = sink.getTargetTableSlots().get(1);
        Assertions.assertInstanceOf(VariantType.class, childVariant.getDataType());
        Assertions.assertInstanceOf(VariantType.class, targetVariant.getDataType());
        Assertions.assertEquals(expectedComputeV2,
                ((VariantType) childVariant.getDataType()).isComputeV2());
        Assertions.assertEquals(expectedComputeV2,
                ((VariantType) targetVariant.getDataType()).isComputeV2());
    }

    private void assertStandardStringVariantCast(Plan plan) {
        Assertions.assertTrue(containsExpression(plan, expression -> expression.anyMatch(node -> {
            if (!(node instanceof Cast)) {
                return false;
            }
            Cast cast = (Cast) node;
            return cast.child().getDataType().isStringLikeType()
                    && cast.getDataType() instanceof VariantType
                    && !((VariantType) cast.getDataType()).isComputeV2();
        })));
    }

    private void assertNoParseToVariant(Plan plan) {
        Assertions.assertFalse(containsExpression(
                plan, expression -> expression.containsType(ParseToVariant.class)));
    }

    private void assertNoVariantRepresentationCast(Plan plan) {
        Assertions.assertFalse(containsVariantRepresentationCast(plan, true, false));
        Assertions.assertFalse(containsVariantRepresentationCast(plan, false, true));
    }

    private void assertOnlyVariantV2ToV1RepresentationCast(Plan plan) {
        Assertions.assertTrue(containsVariantRepresentationCast(plan, true, false));
        Assertions.assertFalse(containsVariantRepresentationCast(plan, false, true));
    }

    private boolean containsVariantRepresentationCast(
            Plan plan, boolean sourceComputeV2, boolean targetComputeV2) {
        return containsExpression(plan, expression -> expression.anyMatch(node -> {
            if (!(node instanceof Cast)) {
                return false;
            }
            Cast cast = (Cast) node;
            if (!(cast.child().getDataType() instanceof VariantType)
                    || !(cast.getDataType() instanceof VariantType)) {
                return false;
            }
            return ((VariantType) cast.child().getDataType()).isComputeV2() == sourceComputeV2
                    && ((VariantType) cast.getDataType()).isComputeV2() == targetComputeV2;
        }));
    }

    private boolean containsExpression(
            Plan plan, java.util.function.Predicate<Expression> predicate) {
        for (Expression expression : plan.getExpressions()) {
            if (predicate.test(expression)) {
                return true;
            }
        }
        for (Plan child : plan.children()) {
            if (containsExpression(child, predicate)) {
                return true;
            }
        }
        return false;
    }
}
