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

package org.apache.doris.planner;

import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.catalog.Column;
import org.apache.doris.common.AnalysisException;

import com.google.common.base.Preconditions;

import java.util.Collection;
import java.util.Map;

public class RangePartitionPruneByCachedLiteralMap extends PartitionPrunerV2Base {
    private RangeMap<LiteralExpr, Long> cachedPartitionRangeMapByLiteral;

    RangePartitionPruneByCachedLiteralMap(RangeMap<LiteralExpr, Long> cachedPartitionRangeMapByLiteral) {
        super(null, null, null);
        this.cachedPartitionRangeMapByLiteral = cachedPartitionRangeMapByLiteral;
    }

    @Override
    public Collection<Long> prune() throws AnalysisException {

    }

    @Override
    FinalFilters getFinalFilters(ColumnRange columnRange,
            Column column) throws AnalysisException {
        throw new AnalysisException("Not implemented");
    }

    @Override
    void genSingleColumnRangeMap() {
        Preconditions.checkState(false);
    }

    @Override
    Collection<Long> pruneMultipleColumnPartition(Map<Column, FinalFilters> columnToFilters) throws AnalysisException {
        throw new AnalysisException("Not implemented");
    }
}

