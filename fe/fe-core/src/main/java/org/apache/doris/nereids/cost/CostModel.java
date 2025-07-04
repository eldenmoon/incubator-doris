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

package org.apache.doris.nereids.cost;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.nereids.PlanContext;
import org.apache.doris.nereids.hint.Hint;
import org.apache.doris.nereids.hint.UseMvHint;
import org.apache.doris.nereids.processor.post.RuntimeFilterGenerator;
import org.apache.doris.nereids.properties.DistributionSpec;
import org.apache.doris.nereids.properties.DistributionSpecGather;
import org.apache.doris.nereids.properties.DistributionSpecHash;
import org.apache.doris.nereids.properties.DistributionSpecReplicated;
import org.apache.doris.nereids.stats.HboPlanStatisticsProvider;
import org.apache.doris.nereids.stats.HboUtils;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanNodeAndHash;
import org.apache.doris.nereids.trees.plans.algebra.OlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalAssertNumRows;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDeferMaterializeOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDeferMaterializeTopN;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribute;
import org.apache.doris.nereids.trees.plans.physical.PhysicalEsScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFileScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalGenerate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalIntersect;
import org.apache.doris.nereids.trees.plans.physical.PhysicalJdbcScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalNestedLoopJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOdbcScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPartitionTopN;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalQuickSort;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSchemaScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalStorageLayerAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTopN;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.Statistics;
import org.apache.doris.statistics.hbo.PlanStatistics;
import org.apache.doris.statistics.hbo.RecentRunsPlanStatistics;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

class CostModel extends PlanVisitor<Cost, PlanContext> {
    static final double RANDOM_SHUFFLE_TO_HASH_SHUFFLE_FACTOR = 0.1;
    // The cost of using external tables should be somewhat higher than using internal tables,
    // so when encountering a scan of an external table, a coefficient should be applied.
    static final double EXTERNAL_TABLE_SCAN_FACTOR = 5;
    private static final Logger LOG = LogManager.getLogger(CostModel.class);
    private final int beNumber;
    private final int parallelInstance;
    private final HboPlanStatisticsProvider hboPlanStatisticsProvider;

    public CostModel(ConnectContext connectContext) {
        SessionVariable sessionVariable = connectContext.getSessionVariable();
        if (sessionVariable.getBeNumberForTest() != -1) {
            // shape test, fix the BE number and instance number
            beNumber = sessionVariable.getBeNumberForTest();
            parallelInstance = 8;
        } else {
            beNumber = Math.max(1, connectContext.getEnv().getClusterInfo().getBackendsNumber(true));
            parallelInstance = Math.max(1, connectContext.getSessionVariable().getParallelExecInstanceNum());
        }
        this.hboPlanStatisticsProvider = Objects.requireNonNull(Env.getCurrentEnv().getHboPlanStatisticsManager()
                .getHboPlanStatisticsProvider(), "HboPlanStatisticsProvider is null");
    }

    public static Cost addChildCost(SessionVariable sessionVariable, Cost planCost, Cost childCost) {
        Preconditions.checkArgument(childCost instanceof Cost && planCost instanceof Cost);
        return new Cost(sessionVariable,
                childCost.getCpuCost() + planCost.getCpuCost(),
                childCost.getMemoryCost() + planCost.getMemoryCost(),
                childCost.getNetworkCost() + planCost.getNetworkCost());
    }

    @Override
    public Cost visit(Plan plan, PlanContext context) {
        return Cost.zero();
    }

    @Override
    public Cost visitPhysicalOlapScan(PhysicalOlapScan physicalOlapScan, PlanContext context) {
        OlapTable table = physicalOlapScan.getTable();
        Statistics statistics = context.getStatisticsWithCheck();
        double rows = statistics.getRowCount();
        double aggMvBonus = 0.0;
        if (table.getBaseIndexId() != physicalOlapScan.getSelectedIndexId()) {
            Optional<UseMvHint> useMvHint = ConnectContext.get().getStatementContext().getUseMvHint("USE_MV");
            if (useMvHint.isPresent()) {
                List<String> mvQualifier = new ArrayList<>();
                for (String qualifier : table.getFullQualifiers()) {
                    mvQualifier.add(qualifier);
                }
                mvQualifier.add(table.getIndexNameById(physicalOlapScan.getSelectedIndexId()));
                if (useMvHint.get().getUseMvTableColumnMap().containsKey(mvQualifier)) {
                    useMvHint.get().getUseMvTableColumnMap().put(mvQualifier, true);
                    useMvHint.get().setStatus(Hint.HintStatus.SUCCESS);
                    return Cost.ofCpu(context.getSessionVariable(), Double.NEGATIVE_INFINITY);
                }
            }
            if (table.getIndexMetaByIndexId(physicalOlapScan.getSelectedIndexId())
                    .getKeysType().equals(KeysType.AGG_KEYS)) {
                aggMvBonus = rows > 1.0 ? 1.0 : rows * 0.5;
            }
        }
        if (table instanceof MTMV) {
            Optional<UseMvHint> useMvHint = ConnectContext.get().getStatementContext().getUseMvHint("USE_MV");
            if (useMvHint.isPresent() && useMvHint.get().getUseMvTableColumnMap()
                    .containsKey(table.getFullQualifiers())) {
                useMvHint.get().getUseMvTableColumnMap().put(table.getFullQualifiers(), true);
                useMvHint.get().setStatus(Hint.HintStatus.SUCCESS);
                return Cost.ofCpu(context.getSessionVariable(), Double.NEGATIVE_INFINITY);
            }
        }
        return Cost.ofCpu(context.getSessionVariable(), rows - aggMvBonus);
    }

    private Set<Column> getColumnForRangePredicate(Set<Expression> expressions) {
        Set<Column> columns = Sets.newHashSet();
        for (Expression expr : expressions) {
            if (expr instanceof ComparisonPredicate) {
                ComparisonPredicate compare = (ComparisonPredicate) expr;
                boolean hasLiteral = compare.left() instanceof Literal || compare.right() instanceof Literal;
                boolean hasSlot = compare.left() instanceof SlotReference || compare.right() instanceof SlotReference;
                if (hasSlot && hasLiteral) {
                    if (compare.left() instanceof SlotReference) {
                        if (((SlotReference) compare.left()).getOriginalColumn().isPresent()) {
                            columns.add(((SlotReference) compare.left()).getOriginalColumn().get());
                        }
                    } else {
                        if (((SlotReference) compare.right()).getOriginalColumn().isPresent()) {
                            columns.add(((SlotReference) compare.right()).getOriginalColumn().get());
                        }
                    }
                }
            }
        }
        return columns;
    }

    @Override
    public Cost visitPhysicalFilter(PhysicalFilter<? extends Plan> filter, PlanContext context) {
        double exprCost = expressionTreeCost(filter.getExpressions());
        double filterCostFactor = 0.0001;
        if (ConnectContext.get() != null) {
            filterCostFactor = ConnectContext.get().getSessionVariable().filterCostFactor;
        }
        int prefixIndexMatched = 0;
        if (filter.getGroupExpression().isPresent()) {
            OlapScan olapScan = (OlapScan) filter.getGroupExpression().get().getFirstChildPlan(OlapScan.class);
            if (olapScan != null) {
                // check prefix index
                long idxId = olapScan.getSelectedIndexId();
                List<Column> keyColumns = olapScan.getTable().getIndexMetaByIndexId(idxId).getPrefixKeyColumns();
                Set<Column> predicateColumns = getColumnForRangePredicate(filter.getConjuncts());
                for (Column col : keyColumns) {
                    if (predicateColumns.contains(col)) {
                        prefixIndexMatched++;
                    } else {
                        break;
                    }
                }
            }
        }
        return Cost.ofCpu(context.getSessionVariable(),
                (filter.getConjuncts().size() - prefixIndexMatched + exprCost) * filterCostFactor);
    }

    @Override
    public Cost visitPhysicalDeferMaterializeOlapScan(PhysicalDeferMaterializeOlapScan deferMaterializeOlapScan,
            PlanContext context) {
        return visitPhysicalOlapScan(deferMaterializeOlapScan.getPhysicalOlapScan(), context);
    }

    public Cost visitPhysicalSchemaScan(PhysicalSchemaScan physicalSchemaScan, PlanContext context) {
        Statistics statistics = context.getStatisticsWithCheck();
        return Cost.ofCpu(context.getSessionVariable(), statistics.getRowCount());
    }

    @Override
    public Cost visitPhysicalStorageLayerAggregate(
            PhysicalStorageLayerAggregate storageLayerAggregate, PlanContext context) {
        Cost costValue = (Cost) storageLayerAggregate.getRelation().accept(this, context);
        // multiply a factor less than 1, so we can select PhysicalStorageLayerAggregate as far as possible
        return new Cost(context.getSessionVariable(), costValue.getCpuCost() * 0.7, costValue.getMemoryCost(),
                costValue.getNetworkCost());
    }

    @Override
    public Cost visitPhysicalFileScan(PhysicalFileScan physicalFileScan, PlanContext context) {
        Statistics statistics = context.getStatisticsWithCheck();
        return Cost.ofCpu(context.getSessionVariable(), statistics.getRowCount() * EXTERNAL_TABLE_SCAN_FACTOR);
    }

    @Override
    public Cost visitPhysicalProject(PhysicalProject<? extends Plan> physicalProject, PlanContext context) {
        boolean trival = true;
        for (Expression expr : physicalProject.getProjects()) {
            if (!(expr instanceof Alias && expr.child(0) instanceof SlotReference)) {
                trival = false;
            }
        }
        if (trival) {
            return Cost.zero();
        }
        double exprCost = expressionTreeCost(physicalProject.getProjects());
        return Cost.ofCpu(context.getSessionVariable(), exprCost + 1);
    }

    @Override
    public Cost visitPhysicalJdbcScan(PhysicalJdbcScan physicalJdbcScan, PlanContext context) {
        Statistics statistics = context.getStatisticsWithCheck();
        return Cost.ofCpu(context.getSessionVariable(), statistics.getRowCount() * EXTERNAL_TABLE_SCAN_FACTOR);
    }

    @Override
    public Cost visitPhysicalOdbcScan(PhysicalOdbcScan physicalOdbcScan, PlanContext context) {
        Statistics statistics = context.getStatisticsWithCheck();
        return Cost.ofCpu(context.getSessionVariable(), statistics.getRowCount() * EXTERNAL_TABLE_SCAN_FACTOR);
    }

    @Override
    public Cost visitPhysicalEsScan(PhysicalEsScan physicalEsScan, PlanContext context) {
        Statistics statistics = context.getStatisticsWithCheck();
        return Cost.ofCpu(context.getSessionVariable(), statistics.getRowCount() * EXTERNAL_TABLE_SCAN_FACTOR);
    }

    @Override
    public Cost visitPhysicalQuickSort(
            PhysicalQuickSort<? extends Plan> physicalQuickSort, PlanContext context) {
        // TODO: consider two-phase sort and enforcer.
        Statistics statistics = context.getStatisticsWithCheck();
        Statistics childStatistics = context.getChildStatistics(0);

        double childRowCount = childStatistics.getRowCount();
        double rowCount = statistics.getRowCount();
        if (physicalQuickSort.getSortPhase().isGather()) {
            // Now we do more like two-phase sort, so penalise one-phase sort
            rowCount *= 100;
        }
        return Cost.of(context.getSessionVariable(), childRowCount, rowCount, childRowCount);
    }

    @Override
    public Cost visitPhysicalTopN(PhysicalTopN<? extends Plan> topN, PlanContext context) {
        // TODO: consider two-phase sort and enforcer.
        Statistics statistics = context.getStatisticsWithCheck();
        Statistics childStatistics = context.getChildStatistics(0);

        double childRowCount = childStatistics.getRowCount();
        double rowCount = statistics.getRowCount();
        if (topN.getSortPhase().isGather()) {
            // Now we do more like two-phase sort, so penalise one-phase sort
            rowCount = rowCount * 100 + 100;
        }
        return Cost.of(context.getSessionVariable(), childRowCount, rowCount, childRowCount);
    }

    @Override
    public Cost visitPhysicalDeferMaterializeTopN(PhysicalDeferMaterializeTopN<? extends Plan> topN,
            PlanContext context) {
        return visitPhysicalTopN(topN.getPhysicalTopN(), context);
    }

    @Override
    public Cost visitPhysicalPartitionTopN(PhysicalPartitionTopN<? extends Plan> partitionTopN, PlanContext context) {
        Statistics statistics = context.getStatisticsWithCheck();
        Statistics childStatistics = context.getChildStatistics(0);
        return Cost.of(context.getSessionVariable(),
                childStatistics.getRowCount(),
                statistics.getRowCount(),
                childStatistics.getRowCount());
    }

    @Override
    public Cost visitPhysicalDistribute(
            PhysicalDistribute<? extends Plan> distribute, PlanContext context) {
        Statistics childStatistics = context.getChildStatistics(0);
        double intputRowCount = childStatistics.getRowCount();
        DistributionSpec spec = distribute.getDistributionSpec();
        // cost model is trained by clusters with more than 3 BE.
        int beNumForDist = Math.max(3, beNumber);
        double dataSizeFactor = childStatistics.dataSizeFactor(distribute.child().getOutput());
        // shuffle
        if (spec instanceof DistributionSpecHash) {
            return Cost.of(context.getSessionVariable(),
                    intputRowCount / beNumForDist,
                    0,
                    intputRowCount * dataSizeFactor / beNumForDist
                    );
        }

        // replicate
        if (spec instanceof DistributionSpecReplicated) {
            // estimate broadcast cost by an experience formula: beNumber^0.5 * rowCount
            // - sender number and receiver number is not available at RBO stage now, so we use beNumber
            // - senders and receivers work in parallel, that why we use square of beNumber
            return Cost.of(context.getSessionVariable(),
                    0,
                    0,
                    intputRowCount * dataSizeFactor);

        }

        // gather
        if (spec instanceof DistributionSpecGather) {
            return Cost.of(context.getSessionVariable(),
                    0,
                    0,
                    intputRowCount * dataSizeFactor / beNumForDist);
        }

        // any
        // cost of random shuffle is lower than hash shuffle.
        return Cost.of(context.getSessionVariable(),
                0,
                0,
                intputRowCount * dataSizeFactor
                        * RANDOM_SHUFFLE_TO_HASH_SHUFFLE_FACTOR / beNumForDist);
    }

    private double expressionTreeCost(List<? extends Expression> expressions) {
        double exprCost = 0.0;
        ExpressionCostEvaluator expressionCostEvaluator = new ExpressionCostEvaluator();
        for (Expression expr : expressions) {
            if (!(expr instanceof SlotReference)) {
                exprCost += expr.accept(expressionCostEvaluator, null);
            }
        }
        return exprCost;
    }

    @Override
    public Cost visitPhysicalHashAggregate(
            PhysicalHashAggregate<? extends Plan> aggregate, PlanContext context) {
        Statistics inputStatistics = context.getChildStatistics(0);
        double exprCost = expressionTreeCost(aggregate.getExpressions());
        if (aggregate.getAggPhase().isLocal()) {
            return Cost.of(context.getSessionVariable(),
                    exprCost / 100 + inputStatistics.getRowCount() / beNumber,
                    inputStatistics.getRowCount() / beNumber, 0);
        } else {
            // global
            return Cost.of(context.getSessionVariable(), exprCost / 100
                            + inputStatistics.getRowCount(),
                    inputStatistics.getRowCount(), 0);
        }
    }

    @Override
    public Cost visitPhysicalHashJoin(
            PhysicalHashJoin<? extends Plan, ? extends Plan> physicalHashJoin, PlanContext context) {
        Preconditions.checkState(context.arity() == 2);
        Statistics outputStats = context.getStatisticsWithCheck();
        double outputRowCount = outputStats.getRowCount();

        Statistics probeStats = context.getChildStatistics(0);
        Statistics buildStats = context.getChildStatistics(1);

        double leftRowCount = probeStats.getRowCount();
        double rightRowCount = buildStats.getRowCount();
        if ((long) leftRowCount == (long) rightRowCount) {
            // reorder by connectivity to be friendly to runtime filter.
            if (physicalHashJoin.getGroupExpression().isPresent()
                    && physicalHashJoin.getGroupExpression().get().getOwnerGroup() != null
                    && !physicalHashJoin.getGroupExpression().get().getOwnerGroup().isStatsReliable()) {
                int leftConnectivity = computeConnectivity(physicalHashJoin.left(), context);
                int rightConnectivity = computeConnectivity(physicalHashJoin.right(), context);
                if (rightConnectivity < leftConnectivity) {
                    leftRowCount += 1;
                }
            } else if (probeStats.getWidthInJoinCluster() < buildStats.getWidthInJoinCluster()) {
                leftRowCount += 1;
            }
            if (probeStats.getWidthInJoinCluster() == buildStats.getWidthInJoinCluster()
                    && probeStats.computeTupleSize(physicalHashJoin.left().getOutput())
                    < buildStats.computeTupleSize(physicalHashJoin.right().getOutput())) {
                // When the number of rows and the width on both sides of the join are the same,
                // we need to consider the cost of materializing the output.
                // When there is more data on the build side, a greater penalty will be given.
                leftRowCount += 1e-3;
            }
        }
        if (!physicalHashJoin.getGroupExpression().get().getOwnerGroup().isStatsReliable()
                && !(RuntimeFilterGenerator.DENIED_JOIN_TYPES.contains(physicalHashJoin.getJoinType()))
                && !physicalHashJoin.isMarkJoin()
                && buildStats.getWidthInJoinCluster() == 1) {
            // we prefer join order: A-B-filter(C) to A-filter(C)-B,
            // since filter(C) may generate effective RF to B, and RF(C->B) makes RF(B->A) more effective.
            double filterFactor = computeFilterFactor(buildStats);
            if (filterFactor > 1.0) {
                double bonus = filterFactor * probeStats.getWidthInJoinCluster();
                if (leftRowCount > bonus) {
                    leftRowCount -= bonus;
                }
            }
        }

        /*
        pattern1: L join1 (Agg1() join2 Agg2())
        result number of join2 may much less than Agg1.
        but Agg1 and Agg2 are slow. so we need to punish this pattern1.

        pattern2: (L join1 Agg1) join2 agg2
        in pattern2, join1 and join2 takes more time, but Agg1 and agg2 can be processed in parallel.
        */
        if (physicalHashJoin.getJoinType().isCrossJoin()) {
            return Cost.of(context.getSessionVariable(), leftRowCount + rightRowCount + outputRowCount,
                    0,
                    leftRowCount + rightRowCount
            );
        }
        double probeShortcutFactor = 1.0;
        if (ConnectContext.get() != null && ConnectContext.get().getStatementContext() != null
                && !ConnectContext.get().getStatementContext().isHasUnknownColStats()
                && physicalHashJoin.getJoinType().isLeftSemiOrAntiJoin()
                && physicalHashJoin.getOtherJoinConjuncts().isEmpty()
                && physicalHashJoin.getMarkJoinConjuncts().isEmpty()) {
            // left semi/anti has short-cut opt, add probe side factor for distinguishing from the right ones
            probeShortcutFactor = context.getSessionVariable().getLeftSemiOrAntiProbeFactor();
        }
        if (context.isBroadcastJoin()) {
            // compared with shuffle join, bc join will be taken a penalty for both build and probe side;
            // currently we use the following factor as the penalty factor:
            // build side factor: totalInstanceNumber to the power of 2, standing for the additional effort for
            //                    bigger cost for building hash table, taken on rightRowCount
            // probe side factor: totalInstanceNumber to the power of 2, standing for the additional effort for
            //                    bigger cost for ProbeWhenBuildSideOutput effort and ProbeWhenSearchHashTableTime
            //                    on the output rows, taken on outputRowCount()
            double probeSideFactor = 1.0;
            double buildSideFactor = context.getSessionVariable().getBroadcastRightTableScaleFactor();
            int totalInstanceNumber = parallelInstance * Math.max(3, beNumber);
            if (buildSideFactor <= 1.0) {
                if (buildStats.computeSize(physicalHashJoin.right().getOutput()) < 1024 * 1024) {
                    // no penalty to broadcast if build side is small
                    buildSideFactor = 1.0;
                } else {
                    // use totalInstanceNumber to the power of 2 as the default factor value
                    buildSideFactor = Math.pow(totalInstanceNumber, 0.5);
                }
            }

            // hbo to adjust bc cost parameter to reduce bc cost
            if (context.getSessionVariable() != null
                    && context.getSessionVariable().isEnableHboOptimization()) {
                PlanNodeAndHash planNodeAndHash = null;
                try {
                    planNodeAndHash = HboUtils.getPlanNodeHash(physicalHashJoin);
                } catch (IllegalStateException e) {
                    LOG.warn("failed to get plan node hash", e);
                }
                if (planNodeAndHash != null) {
                    RecentRunsPlanStatistics planStatistics = hboPlanStatisticsProvider.getHboPlanStats(
                            planNodeAndHash);
                    PlanStatistics matchedPlanStatistics = HboUtils.getMatchedPlanStatistics(planStatistics,
                            context.getStatementContext().getConnectContext());
                    if (matchedPlanStatistics != null) {
                        int builderSkewRatio = matchedPlanStatistics.getJoinBuilderSkewRatio();
                        int probeSkewRatio = matchedPlanStatistics.getJoinProbeSkewRatio();
                        int hboSkewRatioThreshold = context.getSessionVariable().getHboSkewRatioThreshold();
                        if (builderSkewRatio >= hboSkewRatioThreshold || probeSkewRatio >= hboSkewRatioThreshold) {
                            probeShortcutFactor = probeShortcutFactor * 0.1;
                        }
                    }
                }
            }

            return Cost.of(context.getSessionVariable(),
                    leftRowCount * probeShortcutFactor + rightRowCount * probeShortcutFactor * buildSideFactor
                            + outputRowCount * probeSideFactor,
                    rightRowCount,
                    0
            );
        }
        return Cost.of(context.getSessionVariable(),
                leftRowCount * probeShortcutFactor + rightRowCount * probeShortcutFactor + outputRowCount,
                        rightRowCount, 0
        );
    }

    private double computeFilterFactor(Statistics stats) {
        double factor = 1.0;
        double maxBaseTableRowCount = 0.0;
        for (Expression expr : stats.columnStatistics().keySet()) {
            ColumnStatistic colStats = stats.findColumnStatistics(expr);
            if (colStats.isUnKnown) {
                maxBaseTableRowCount = Math.max(maxBaseTableRowCount, colStats.count);
            } else {
                break;
            }
        }
        if (maxBaseTableRowCount != 0) {
            factor = Math.max(1, Math.min(2, maxBaseTableRowCount / stats.getRowCount()));
        }
        return factor;
    }

    /*
    in a join cluster graph, if a node has higher connectivity, it is more likely to be reduced
    by runtime filters, and it is also more likely to produce effective runtime filters.
    Thus, we prefer to put the node with higher connectivity on the join right side.
     */
    private int computeConnectivity(
            Plan plan, PlanContext context) {
        int connectCount = 0;
        for (Expression expr : context.getStatementContext().getJoinFilters()) {
            connectCount += Collections.disjoint(expr.getInputSlots(), plan.getOutputSet()) ? 0 : 1;
        }
        return connectCount;
    }

    @Override
    public Cost visitPhysicalNestedLoopJoin(
            PhysicalNestedLoopJoin<? extends Plan, ? extends Plan> nestedLoopJoin,
            PlanContext context) {
        // TODO: copy from physicalHashJoin, should update according to physical nested loop join properties.
        Preconditions.checkState(context.arity() == 2);
        Statistics leftStatistics = context.getChildStatistics(0);
        Statistics rightStatistics = context.getChildStatistics(1);
        /*
         * nljPenalty:
         * The row count estimation for nested loop join (NLJ) results often has significant errors.
         * When the estimated row count is higher than the actual value, the cost benefits of subsequent
         * operators (e.g., aggregation) may be overestimated. This can lead the optimizer to choose a
         * plan where a small table joins a large table, severely impacting the overall SQL execution efficiency.
         *
         * For example, if the subsequent operator is an aggregation (AGG) and the GROUP BY key aligns with
         * the distribution key of the small table, the optimizer might prioritize avoiding shuffling the NLJ
         * results by choosing to join the small table to the large table, even if this is suboptimal.
         */
        double nljPenalty = 1.0;
        if (leftStatistics.getRowCount() < 10 * rightStatistics.getRowCount()) {
            nljPenalty = Math.min(leftStatistics.getRowCount(), rightStatistics.getRowCount());
        }
        return Cost.of(context.getSessionVariable(),
                leftStatistics.getRowCount() * rightStatistics.getRowCount(),
                rightStatistics.getRowCount() * nljPenalty,
                0);
    }

    @Override
    public Cost visitPhysicalAssertNumRows(PhysicalAssertNumRows<? extends Plan> assertNumRows,
            PlanContext context) {
        return Cost.of(context.getSessionVariable(),
                assertNumRows.getAssertNumRowsElement().getDesiredNumOfRows(),
                assertNumRows.getAssertNumRowsElement().getDesiredNumOfRows(),
                0
        );
    }

    @Override
    public Cost visitPhysicalIntersect(PhysicalIntersect physicalIntersect, PlanContext context) {
        double cpuCost = 0.0;
        for (int i = 0; i < physicalIntersect.children().size(); i++) {
            cpuCost += context.getChildStatistics(i).getRowCount();
        }
        double memoryCost = context.getChildStatistics(0).computeSize(
                physicalIntersect.child(0).getOutput());
        return Cost.of(context.getSessionVariable(), cpuCost, memoryCost, 0);
    }

    @Override
    public Cost visitPhysicalGenerate(PhysicalGenerate<? extends Plan> generate, PlanContext context) {
        Statistics statistics = context.getStatisticsWithCheck();
        return Cost.of(context.getSessionVariable(),
                statistics.getRowCount(),
                statistics.getRowCount(),
                0
        );
    }
}
