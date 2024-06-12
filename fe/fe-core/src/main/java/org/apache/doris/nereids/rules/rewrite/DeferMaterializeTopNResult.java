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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalDeferMaterializeOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalDeferMaterializeResultSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalDeferMaterializeTopN;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalResultSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * rewrite simple top n query to defer materialize slot not use for sort or predicate
 */
public class DeferMaterializeTopNResult implements RewriteRuleFactory {

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                RuleType.DEFER_MATERIALIZE_TOP_N_RESULT.build(
                        logicalResultSink(logicalTopN(logicalOlapScan()))
                                .when(r -> r.child().getLimit() < getTopNOptLimitThreshold())
                                .whenNot(r -> r.child().getOrderKeys().isEmpty())
                                .when(r -> r.child().getOrderKeys().stream().map(OrderKey::getExpr)
                                        .allMatch(Expression::isColumnFromTable))
                                .when(r -> r.child().child().getTable().getEnableLightSchemaChange())
                                .when(r -> r.child().child().getTable().isDupKeysOrMergeOnWrite())
                                .then(r -> deferMaterialize(r, Optional.of(r.child()), Optional.empty(),
                                        Optional.empty(), Optional.empty(), r.child().child()))
                ),
                RuleType.DEFER_MATERIALIZE_TOP_N_RESULT.build(
                        logicalResultSink(logicalTopN(logicalFilter(logicalOlapScan())))
                                .when(r -> r.child().getLimit() < getTopNOptLimitThreshold())
                                .whenNot(r -> r.child().getOrderKeys().isEmpty())
                                .when(r -> r.child().getOrderKeys().stream().map(OrderKey::getExpr)
                                        .allMatch(Expression::isColumnFromTable))
                                .when(r -> r.child().child().child().getTable().getEnableLightSchemaChange())
                                .when(r -> r.child().child().child().getTable().isDupKeysOrMergeOnWrite())
                                .then(r -> {
                                    LogicalFilter<LogicalOlapScan> filter = r.child().child();
                                    return deferMaterialize(r, Optional.of(r.child()), Optional.empty(),
                                            Optional.of(filter), Optional.empty(), filter.child());
                                })
                ),
                RuleType.DEFER_MATERIALIZE_TOP_N_RESULT.build(
                        logicalResultSink(logicalProject(logicalFilter(logicalProject(logicalOlapScan()))))
                                .when(r -> ConnectContext.get().getSessionVariable().enableTwoPhaseReadOpt)
                                .when(r -> r.child().child().child().child().getTable().getEnableLightSchemaChange())
                                .when(r -> r.child().child().child().child().getTable().isDupKeysOrMergeOnWrite())
                                .when(r -> r.child().child().child().child().getTable().storeRowColumn())
                                .then(r -> deferMaterialize(r, Optional.empty(), Optional.empty(),
                                        Optional.of(r.child().child()),
                                        Optional.of(r.child().child().child()), r.child().child().child().child()))
                ),
                RuleType.DEFER_MATERIALIZE_TOP_N_RESULT.build(
                        logicalResultSink(
                                logicalLimit(logicalLimit(
                                        logicalProject(logicalFilter(logicalProject(logicalOlapScan()))))))
                                .when(r -> ConnectContext.get().getSessionVariable().enableTwoPhaseReadOpt)
                                .when(r -> r.child().child().child().child()
                                        .child().child().getTable().getEnableLightSchemaChange())
                                .when(r -> r.child().child().child().child()
                                        .child().child().getTable().isDupKeysOrMergeOnWrite())
                                .when(r -> r.child().child().child().child()
                                        .child().child().getTable().storeRowColumn())
                                .then(r -> deferMaterialize(r, Optional.empty(), Optional.of(r.child()),
                                        Optional.of(r.child().child().child().child()),
                                        Optional.of(r.child().child().child().child().child()),
                                        r.child().child().child().child().child().child()))
                )
        );
    }

    private Plan deferMaterialize(LogicalResultSink<? extends Plan> logicalResultSink,
            Optional<LogicalTopN<? extends Plan>> logicalTopN,
            Optional<LogicalLimit<? extends Plan>> logicalLimit,
            Optional<LogicalFilter<? extends Plan>> logicalFilter,
            Optional<LogicalProject<? extends Plan>> project,
            LogicalOlapScan logicalOlapScan) {
        Column rowId = new Column(Column.ROWID_COL, Type.STRING, false, null, false, "", "rowid column");
        SlotReference columnId = SlotReference.fromColumn(
                logicalOlapScan.getTable(), rowId, logicalOlapScan.getQualifier());
        Set<ExprId> deferredMaterializedExprIds = Sets.newHashSet(logicalOlapScan.getOutputExprIdSet());
        logicalFilter.ifPresent(filter -> filter.getConjuncts()
                .forEach(e -> deferredMaterializedExprIds.removeAll(e.getInputSlotExprIds())));
        LogicalDeferMaterializeOlapScan deferOlapScan = new LogicalDeferMaterializeOlapScan(
                logicalOlapScan, deferredMaterializedExprIds, columnId);
        Plan scan = project.map(f -> f.withChildren(deferOlapScan)).orElse(deferOlapScan);
        Plan filter = logicalFilter.map(f -> f.withChildren(scan)).orElse(deferOlapScan);
        Plan root = null;
        if (logicalTopN.isPresent()) {
            logicalTopN.get().getOrderKeys().stream()
                .map(OrderKey::getExpr)
                .map(Slot.class::cast)
                .map(NamedExpression::getExprId)
                .filter(Objects::nonNull)
                .forEach(deferredMaterializedExprIds::remove);
            root = new LogicalDeferMaterializeTopN<>(
                    (LogicalTopN<? extends Plan>) logicalTopN.get().withChildren(filter),
                    deferredMaterializedExprIds, columnId);
        }
        if (logicalLimit.isPresent()) {
            root = logicalLimit.map(f -> f.withChildren(filter)).orElse(filter);
        }
        root = logicalResultSink.withChildren(root);
        return new LogicalDeferMaterializeResultSink<>((LogicalResultSink<? extends Plan>) root,
                logicalOlapScan.getTable(), logicalOlapScan.getSelectedIndexId());
    }

    private long getTopNOptLimitThreshold() {
        if (ConnectContext.get() != null && ConnectContext.get().getSessionVariable() != null) {
            if (!ConnectContext.get().getSessionVariable().enableTwoPhaseReadOpt) {
                return -1;
            }
            return ConnectContext.get().getSessionVariable().topnOptLimitThreshold;
        }
        return -1;
    }
}
