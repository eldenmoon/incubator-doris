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

import org.apache.doris.analysis.ColumnAccessPath;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.MatchPredicate;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SearchExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ElementAt;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

public class VariantPruningLogicTest extends TestWithFeService implements MemoPatternMatchSupported {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test_variant_pruning_logic");
        useDatabase("test_variant_pruning_logic");
        createTable("create table variant_tbl(\n"
                + "  id int,\n"
                + "  v variant\n"
                + ") properties ('replication_num'='1')");
        createTable("create table variant_msg_tbl(\n"
                + "  id int,\n"
                + "  msg variant<properties(\"variant_max_subcolumns_count\"=\"0\")>,\n"
                + "  index idx_msg(msg) using inverted properties(\n"
                + "    \"parser\"=\"unicode\", \"lower_case\"=\"true\", \"support_phrase\"=\"true\")\n"
                + ") properties ('replication_num'='1', 'inverted_index_storage_format'='V2')");
        connectContext.getSessionVariable().setDisableNereidsRules(RuleType.PRUNE_EMPTY_PARTITION.name());
        connectContext.getSessionVariable().enableNereidsTimeout = false;
        connectContext.getSessionVariable().enablePruneNestedColumns = true;
    }

    @Test
    public void testVariantNumericIndexSubPath() throws Exception {
        assertVariantSubColumnSlots(
                "select v['arr'][0]['x'] from variant_tbl",
                ImmutableList.of(ImmutableList.of("arr", "0", "x")));
        assertAllAccessPathsContain(
                "select v['arr'][0]['x'] from variant_tbl",
                ImmutableList.of(path("v", "arr", "0", "x")),
                ImmutableList.of()
        );
    }

    @Test
    public void testVariantArraySubscriptUsesPrunedSubPath() throws Exception {
        String sql = "select cast(v['items']['type'] as array<string>)[1] from variant_tbl";
        String explain = getSQLPlanOrErrorMsg(sql, true);
        Assertions.assertTrue(explain.contains("final projections: element_at(CAST(v AS array<text>), 1)"),
                explain);
        Assertions.assertTrue(explain.contains("subColPath=[items, type]"),
                explain);
        Assertions.assertFalse(explain.contains("element_at(CAST(element_at(element_at("),
                explain);
        assertVariantSubColumnSlots(sql, ImmutableList.of(ImmutableList.of("items", "type")));
    }

    @Test
    public void testVariantOrPredicatePaths() throws Exception {
        assertPredicateAccessPathsEqual(
                "select 1 from variant_tbl where v['a'] = 1 or v['b']['c'] = 2",
                ImmutableList.of(path("v", "a"), path("v", "b", "c"))
        );
        assertVariantSubColumnSlots(
                "select 1 from variant_tbl where v['a'] = 1 or v['b']['c'] = 2",
                ImmutableList.of(ImmutableList.of("a"), ImmutableList.of("b", "c")));
    }

    @Test
    public void testVariantIfExpressionPaths() throws Exception {
        assertVariantSubColumnSlots(
                "select if(v['a'] is null, v['b']['c'], v['d']) from variant_tbl",
                ImmutableList.of(
                        ImmutableList.of("a"),
                        ImmutableList.of("b", "c"),
                        ImmutableList.of("d")));
        assertAllAccessPathsContain(
                "select if(v['a'] is null, v['b']['c'], v['d']) from variant_tbl",
                ImmutableList.of(path("v", "a"), path("v", "b", "c"), path("v", "d")),
                ImmutableList.of()
        );
    }

    @Test
    public void testExplodeWholeVariantAccessPaths() throws Exception {
        assertAllAccessPathsContain(
                "select x['k'] from variant_tbl lateral view explode(v) tmp as x",
                ImmutableList.of(path("v", "k")),
                ImmutableList.of()
        );
    }

    @Test
    public void testExplodeVariantArrayWithOuterFilterAccessPaths() throws Exception {
        assertAllAccessPathsContain(
                "select x['x'] from variant_tbl lateral view explode(v['arr']) tmp as x "
                        + "where v['filter']['k'] = 1 and x['y'] is not null",
                ImmutableList.of(
                        path("v", "arr", "x"),
                        path("v", "arr", "y"),
                        path("v", "filter", "k")
                ),
                ImmutableList.of()
        );
    }

    @Test
    public void testExplodeVariantDeepNestedAccessPaths() throws Exception {
        assertAllAccessPathsContain(
                "select x['a']['b'][0]['c'] from variant_tbl lateral view explode(v['arr']) tmp as x",
                ImmutableList.of(path("v", "arr", "a", "b", "0", "c")),
                ImmutableList.of()
        );
    }

    @Test
    public void testExplodeSubqueryJoinAggAccessPaths() throws Exception {
        assertAllAccessPathsContain(
                "select cast(t2.v['k'] as string) as k, count(*) from (select id, v from variant_tbl) t1 "
                        + "lateral view explode(t1.v['arr']) tmp as x "
                        + "join variant_tbl t2 on t1.id=t2.id "
                        + "where x['a']['b'] = 1 and t2.v['k'] is not null "
                        + "group by cast(t2.v['k'] as string)",
                ImmutableList.of(
                        path("v", "arr", "a", "b"),
                        path("v", "k")
                ),
                ImmutableList.of()
        );
    }

    @Test
    public void testExplodeAggAndFilterAccessPaths() throws Exception {
        assertAllAccessPathsContain(
                "select sum(cast(x['metric'] as int)) from variant_tbl lateral view explode(v['arr']) tmp as x "
                        + "where x['metric'] is not null",
                ImmutableList.of(path("v", "arr", "metric")),
                ImmutableList.of()
        );
    }

    @Test
    public void testExplodeOuterAccessPaths() throws Exception {
        assertAllAccessPathsContain(
                "select x['k'] from variant_tbl lateral view explode_outer(v['arr']) tmp as x",
                ImmutableList.of(path("v", "arr", "k")),
                ImmutableList.of()
        );
    }

    @Test
    public void testMatchOnIndexedDotVariantSubColumnUsesSlotRefInScanPredicate() throws Exception {
        String sql = "select id from variant_msg_tbl "
                + "where cast(msg.trace_id as string) match_phrase_prefix 'abc'";
        List<OlapScanNode> olapScanNodes = collectOlapScanNodes(sql);
        Assertions.assertEquals(1, olapScanNodes.size());

        List<MatchPredicate> matchPredicates = new ArrayList<>();
        Expr.collectList(olapScanNodes.get(0).getConjuncts(), MatchPredicate.class, matchPredicates);
        Assertions.assertEquals(1, matchPredicates.size());

        Expr leftWithoutCast = matchPredicates.get(0).getChildWithoutCast(0);
        Assertions.assertInstanceOf(SlotRef.class, leftWithoutCast, matchPredicates.get(0).toString());
        SlotRef leftSlot = (SlotRef) leftWithoutCast;
        Assertions.assertEquals(ImmutableList.of("trace_id"), leftSlot.getDesc().getSubColLables());
    }

    @Test
    public void testSearchOnDotVariantSubColumnUsesSlotRefInScanPredicate() throws Exception {
        String sql = "select id from variant_msg_tbl where search('msg.trace_id:abc')";
        assertSearchRewrite(sql, ImmutableList.of(ImmutableList.of("trace_id")));
    }

    @Test
    public void testMatchWithoutInvertedIndexPrunesAllStaticVariantElementAt() throws Exception {
        String sql = "select v['display'] from variant_tbl "
                + "where cast(v.trace_id as string) match_phrase_prefix 'abc' and v['ordinary'] = 'x'";
        assertVariantRootAndSubColumnSlots(
                sql,
                ImmutableList.of(
                        ImmutableList.of("display"),
                        ImmutableList.of("ordinary"),
                        ImmutableList.of("trace_id")),
                0);
        String explain = getSQLPlanOrErrorMsg(sql, true);
        Assertions.assertTrue(explain.contains("subColPath=[display]"), explain);
        Assertions.assertTrue(explain.contains("subColPath=[trace_id]"), explain);
        Assertions.assertTrue(explain.contains("subColPath=[ordinary]"), explain);
    }

    @Test
    public void testIndexedMatchPrunesAllStaticVariantElementAt() throws Exception {
        String sql = "select msg['display'] from variant_msg_tbl "
                + "where cast(msg.trace_id as string) match_phrase_prefix 'abc' and msg['ordinary'] = 'x'";
        assertVariantRootAndSubColumnSlots(
                sql,
                ImmutableList.of(
                        ImmutableList.of("display"),
                        ImmutableList.of("ordinary"),
                        ImmutableList.of("trace_id")),
                0);
        String explain = getSQLPlanOrErrorMsg(sql, true);
        Assertions.assertTrue(explain.contains("subColPath=[display]"), explain);
        Assertions.assertTrue(explain.contains("subColPath=[trace_id]"), explain);
        Assertions.assertTrue(explain.contains("subColPath=[ordinary]"), explain);
    }

    @Test
    public void testSearchPrunesAllStaticVariantElementAt() throws Exception {
        String sql = "select msg['display'] from variant_msg_tbl "
                + "where search('msg.trace_id:abc') and msg['ordinary'] = 'x'";
        List<List<String>> expectedSubPaths = ImmutableList.of(
                ImmutableList.of("display"),
                ImmutableList.of("ordinary"),
                ImmutableList.of("trace_id"));
        assertSearchRewrite(sql, expectedSubPaths);
    }

    private Pair<PhysicalPlan, List<SlotDescriptor>> collectVariantSlots(String sql) throws Exception {
        NereidsPlanner planner = plan(sql);
        List<SlotDescriptor> variantSlots = new ArrayList<>();
        PhysicalPlan physicalPlan = planner.getPhysicalPlan();
        for (OlapScanNode olapScanNode : collectOlapScanNodes(planner)) {
            List<SlotDescriptor> slots = olapScanNode.getTupleDesc().getSlots();
            for (SlotDescriptor slot : slots) {
                Type type = slot.getType();
                if (type.isVariantType()) {
                    variantSlots.add(slot);
                }
            }
        }
        return Pair.of(physicalPlan, variantSlots);
    }

    private List<OlapScanNode> collectOlapScanNodes(String sql) throws Exception {
        return collectOlapScanNodes(plan(sql));
    }

    private List<OlapScanNode> collectOlapScanNodes(NereidsPlanner planner) {
        List<OlapScanNode> olapScanNodes = new ArrayList<>();
        for (PlanFragment fragment : planner.getFragments()) {
            olapScanNodes.addAll(fragment.getPlanRoot().collectInCurrentFragment(OlapScanNode.class::isInstance));
        }
        return olapScanNodes;
    }

    private NereidsPlanner plan(String sql) throws Exception {
        return (NereidsPlanner) executeNereidsSql(sql).planner();
    }

    private void assertVariantSubColumnSlots(String sql, List<List<String>> expectedSubColPaths) throws Exception {
        Pair<PhysicalPlan, List<SlotDescriptor>> result = collectVariantSlots(sql);
        TreeSet<String> actualSubColPaths = new TreeSet<>();
        for (SlotDescriptor slotDescriptor : result.second) {
            List<String> subColPath = slotDescriptor.getSubColLables();
            if (subColPath == null || subColPath.isEmpty()) {
                continue;
            }
            actualSubColPaths.add(String.join(".", subColPath));
        }

        TreeSet<String> expectedSubColPathSet = new TreeSet<>();
        for (List<String> expected : expectedSubColPaths) {
            expectedSubColPathSet.add(String.join(".", expected));
        }

        Assertions.assertEquals(expectedSubColPathSet, actualSubColPaths);
    }

    private void assertVariantRootAndSubColumnSlots(
            String sql, List<List<String>> expectedSubColPaths, int expectedRootSlotCount) throws Exception {
        Pair<PhysicalPlan, List<SlotDescriptor>> result = collectVariantSlots(sql);
        TreeSet<String> actualSubColPaths = new TreeSet<>();
        int actualRootSlotCount = 0;
        for (SlotDescriptor slotDescriptor : result.second) {
            List<String> subColPath = slotDescriptor.getSubColLables();
            if (subColPath == null || subColPath.isEmpty()) {
                actualRootSlotCount++;
            } else {
                actualSubColPaths.add(String.join(".", subColPath));
            }
        }

        TreeSet<String> expectedSubColPathSet = new TreeSet<>();
        for (List<String> expected : expectedSubColPaths) {
            expectedSubColPathSet.add(String.join(".", expected));
        }
        Assertions.assertEquals(expectedRootSlotCount, actualRootSlotCount);
        Assertions.assertEquals(expectedSubColPathSet, actualSubColPaths);
    }

    private void assertSearchRewrite(String sql, List<List<String>> expectedSubPaths) {
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(logicalFilter(
                        logicalOlapScan().when(scan -> hasVariantRootAndSubPaths(scan, expectedSubPaths))
                ).when(this::hasSearchSlotWithoutElementAt));
    }

    private boolean hasVariantRootAndSubPaths(LogicalOlapScan scan, List<List<String>> expectedSubPaths) {
        int rootSlots = 0;
        TreeSet<String> subPaths = new TreeSet<>();
        for (Slot slot : scan.getOutput()) {
            if (!slot.getDataType().isVariantType()) {
                continue;
            }
            List<String> subPath = ((SlotReference) slot).getSubPath();
            if (subPath.isEmpty()) {
                rootSlots++;
            } else {
                subPaths.add(String.join(".", subPath));
            }
        }
        TreeSet<String> expected = new TreeSet<>();
        for (List<String> expectedSubPath : expectedSubPaths) {
            expected.add(String.join(".", expectedSubPath));
        }
        return rootSlots == 1 && subPaths.equals(expected);
    }

    private boolean hasSearchSlotWithoutElementAt(LogicalFilter<? extends Plan> filter) {
        List<SearchExpression> searches = new ArrayList<>();
        List<ElementAt> elementAts = new ArrayList<>();
        for (Expression expression : filter.getExpressions()) {
            searches.addAll(expression.collectToList(SearchExpression.class::isInstance));
            elementAts.addAll(expression.collectToList(ElementAt.class::isInstance));
        }
        if (searches.size() != 1 || searches.get(0).getSlotChildren().size() != 1
                || !(searches.get(0).getSlotChildren().get(0) instanceof SlotReference)) {
            return false;
        }
        SlotReference searchSlot = (SlotReference) searches.get(0).getSlotChildren().get(0);
        if (!searchSlot.getSubPath().equals(ImmutableList.of("trace_id"))) {
            return false;
        }

        return elementAts.isEmpty();
    }

    private void assertPredicateAccessPathsEqual(String sql, List<ColumnAccessPath> expected) throws Exception {
        Pair<PhysicalPlan, List<SlotDescriptor>> result = collectVariantSlots(sql);
        TreeSet<ColumnAccessPath> actualSet = new TreeSet<>();
        for (SlotDescriptor slotDescriptor : result.second) {
            List<ColumnAccessPath> predicate = slotDescriptor.getPredicateAccessPaths();
            if (predicate != null) {
                actualSet.addAll(predicate);
            }
        }

        TreeSet<ColumnAccessPath> expectedSet = new TreeSet<>(expected);
        Assertions.assertEquals(expectedSet, actualSet);
    }

    private void assertAllAccessPathsContain(
            String sql, List<ColumnAccessPath> expectedContain, List<ColumnAccessPath> expectedNotContain)
            throws Exception {
        Pair<PhysicalPlan, List<SlotDescriptor>> result = collectVariantSlots(sql);
        TreeSet<ColumnAccessPath> allAccessPaths = new TreeSet<>();
        for (SlotDescriptor slotDescriptor : result.second) {
            allAccessPaths.addAll(slotDescriptor.getAllAccessPaths());
        }
        for (ColumnAccessPath accessPath : expectedContain) {
            Assertions.assertTrue(allAccessPaths.contains(accessPath));
        }
        for (ColumnAccessPath accessPath : expectedNotContain) {
            Assertions.assertFalse(allAccessPaths.contains(accessPath));
        }
    }

    private ColumnAccessPath path(String... path) {
        return ColumnAccessPath.data(ImmutableList.copyOf(path));
    }
}
