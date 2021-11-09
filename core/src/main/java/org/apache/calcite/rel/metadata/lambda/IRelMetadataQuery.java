/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.rel.metadata.lambda;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * The interface that RelMetadataQuery would ideally be.
 *
 * An interface that abstracts away convenience methods from functional methods so that only unique
 * operations must be defined by concrete implementations.
 */
interface IRelMetadataQuery {

  @Nullable Multimap<Class<? extends RelNode>, RelNode> getNodeTypes(RelNode rel);
  @Nullable Double getMaxRowCount(RelNode rel);
  @Nullable Double getMinRowCount(RelNode rel);
  @Nullable RelOptCost getCumulativeCost(RelNode rel);
  @Nullable RelOptCost getNonCumulativeCost(RelNode rel);
  @Nullable Double getPercentageOriginalRows(RelNode rel);
  @Nullable Set<RelColumnOrigin> getColumnOrigins(RelNode rel, int column);
  @Nullable Set<RexNode> getExpressionLineage(RelNode rel, RexNode expression);
  @Nullable Set<RexTableInputRef.RelTableRef> getTableReferences(RelNode rel);
  @Nullable Double getSelectivity(RelNode rel, @Nullable RexNode predicate);
  @Nullable Set<ImmutableBitSet> getUniqueKeys(RelNode rel, boolean ignoreNulls);
  @Nullable Boolean areColumnsUnique(RelNode rel, ImmutableBitSet columns, boolean ignoreNulls);
  @Nullable ImmutableList<RelCollation> collations(RelNode rel);
  @Nullable Double getPopulationSize(RelNode rel, ImmutableBitSet groupKey);
  @Nullable Double getAverageRowSize(RelNode rel);
  @Nullable List<@Nullable Double> getAverageColumnSizes(RelNode rel);
  @Nullable Boolean isPhaseTransition(RelNode rel);
  @Nullable Integer splitCount(RelNode rel);
  @Nullable Double memory(RelNode rel);
  @Nullable Double cumulativeMemoryWithinPhase(RelNode rel);
  @Nullable Double cumulativeMemoryWithinPhaseSplit(RelNode rel);
  @Nullable Double getDistinctRowCount(RelNode rel, ImmutableBitSet groupKey,
      @Nullable RexNode predicate);
  @Nullable RelOptPredicateList getAllPredicates(RelNode rel);
  @Nullable RelDistribution getDistribution(RelNode rel);
  @Nullable RelOptCost getLowerBoundCost(RelNode rel, VolcanoPlanner planner);

  Double getRowCount(RelNode rel);
  RelOptPredicateList getPulledUpPredicates(RelNode rel);
  Boolean isVisibleInExplain(RelNode rel, SqlExplainLevel explainLevel);

  boolean clearCache(RelNode rel);

  default List<@Nullable Double> getAverageColumnSizesNotNull(RelNode rel) {
    @Nullable List<@Nullable Double> averageColumnSizes = getAverageColumnSizes(rel);
    return averageColumnSizes == null
        ? Collections.nCopies(rel.getRowType().getFieldCount(), null)
        : averageColumnSizes;
  }

  default RelDistribution distribution(RelNode rel) {
    return getDistribution(rel);
  }

  default @Nullable Set<ImmutableBitSet> getUniqueKeys(RelNode rel) {
    return getUniqueKeys(rel, false);
  }
  default @Nullable Boolean areRowsUnique(RelNode rel, boolean ignoreNulls) {
    Double maxRowCount = this.getMaxRowCount(rel);
    if (maxRowCount != null && maxRowCount <= 1D) {
      return true;
    }
    ImmutableBitSet columns =
        ImmutableBitSet.range(rel.getRowType().getFieldCount());
    return areColumnsUnique(rel, columns, ignoreNulls);
  }

  default @Nullable Boolean areRowsUnique(RelNode rel) {
    return areRowsUnique(rel, false);
  }

  default @Nullable Boolean areColumnsUnique(RelNode rel, ImmutableBitSet columns) {
    return areColumnsUnique(rel, columns, false);
  }
  default @Nullable RelColumnOrigin getColumnOrigin(RelNode rel, int column) {
    Set<RelColumnOrigin> origins = getColumnOrigins(rel, column);
    if (origins == null || origins.size() != 1) {
      return null;
    }
    RelColumnOrigin origin = Iterables.getOnlyElement(origins);
    return origin;
  }

  default @Nullable RelOptTable getTableOrigin(RelNode rel) {
    // Determine the simple origin of the first column in the
    // RelNode.  If it's simple, then that means that the underlying
    // table is also simple, even if the column itself is derived.
    if (rel.getRowType().getFieldCount() == 0) {
      return null;
    }
    Set<RelColumnOrigin> colOrigins = getColumnOrigins(rel, 0);
    if (colOrigins == null || colOrigins.size() == 0) {
      return null;
    }
    return colOrigins.iterator().next().getOriginTable();
  }

}
