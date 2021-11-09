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
import com.google.common.collect.Multimap;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Set;

/**
 * A base class that simply delegates all calls to this to a delegate IRelMetadataQuery.
 */
abstract class DelegatingIRMQ implements IRelMetadataQuery {
  private final IRelMetadataQuery delegate;

  DelegatingIRMQ(IRelMetadataQuery delegate) {
    this.delegate = delegate;
  }

  @Override public @Nullable Multimap<Class<? extends RelNode>, RelNode> getNodeTypes(
      final RelNode rel) {
    return delegate.getNodeTypes(rel);
  }

  @Override public Double getRowCount(final RelNode rel) {
    return delegate.getRowCount(rel);
  }

  @Override public boolean clearCache(final RelNode rel) {
    return delegate.clearCache(rel);
  }

  @Override @Nullable public Double getMaxRowCount(
      final RelNode rel) {
    return delegate.getMaxRowCount(rel);
  }

  @Override @Nullable public Double getMinRowCount(
      final RelNode rel) {
    return delegate.getMinRowCount(rel);
  }

  @Override public @Nullable RelOptCost getCumulativeCost(
      final RelNode rel) {
    return delegate.getCumulativeCost(rel);
  }

  @Override public @Nullable RelOptCost getNonCumulativeCost(
      final RelNode rel) {
    return delegate.getNonCumulativeCost(rel);
  }

  @Override @Nullable public Double getPercentageOriginalRows(
      final RelNode rel) {
    return delegate.getPercentageOriginalRows(rel);
  }

  @Override public @Nullable Set<RelColumnOrigin> getColumnOrigins(
      final RelNode rel, final int column) {
    return delegate.getColumnOrigins(rel, column);
  }

  @Override public @Nullable Set<RexNode> getExpressionLineage(
      final RelNode rel, final RexNode expression) {
    return delegate.getExpressionLineage(rel, expression);
  }

  @Override public @Nullable Set<RexTableInputRef.RelTableRef> getTableReferences(
      final RelNode rel) {
    return delegate.getTableReferences(rel);
  }

  @Override @Nullable public Double getSelectivity(
      final RelNode rel,
      final @Nullable RexNode predicate) {
    return delegate.getSelectivity(rel, predicate);
  }

  @Override public @Nullable Set<ImmutableBitSet> getUniqueKeys(
      final RelNode rel, final boolean ignoreNulls) {
    return delegate.getUniqueKeys(rel, ignoreNulls);
  }

  @Override @Nullable public Boolean areColumnsUnique(
      final RelNode rel, final ImmutableBitSet columns,
      final boolean ignoreNulls) {
    return delegate.areColumnsUnique(rel, columns, ignoreNulls);
  }

  @Override public @Nullable ImmutableList<RelCollation> collations(
      final RelNode rel) {
    return delegate.collations(rel);
  }

  @Override public RelDistribution distribution(final RelNode rel) {
    return delegate.distribution(rel);
  }

  @Override @Nullable public Double getPopulationSize(
      final RelNode rel,
      final ImmutableBitSet groupKey) {
    return delegate.getPopulationSize(rel, groupKey);
  }

  @Override @Nullable public Double getAverageRowSize(
      final RelNode rel) {
    return delegate.getAverageRowSize(rel);
  }

  @Override public @Nullable List<@Nullable Double> getAverageColumnSizes(
      final RelNode rel) {
    return delegate.getAverageColumnSizes(rel);
  }

  @Override @Nullable public Boolean isPhaseTransition(
      final RelNode rel) {
    return delegate.isPhaseTransition(rel);
  }

  @Override @Nullable public Integer splitCount(
      final RelNode rel) {
    return delegate.splitCount(rel);
  }

  @Override @Nullable public Double memory(
      final RelNode rel) {
    return delegate.memory(rel);
  }

  @Override @Nullable public Double cumulativeMemoryWithinPhase(
      final RelNode rel) {
    return delegate.cumulativeMemoryWithinPhase(rel);
  }

  @Override @Nullable public Double cumulativeMemoryWithinPhaseSplit(
      final RelNode rel) {
    return delegate.cumulativeMemoryWithinPhaseSplit(rel);
  }

  @Override @Nullable public Double getDistinctRowCount(
      final RelNode rel,
      final ImmutableBitSet groupKey,
      final @Nullable RexNode predicate) {
    return delegate.getDistinctRowCount(rel, groupKey, predicate);
  }

  @Override public RelOptPredicateList getPulledUpPredicates(
      final RelNode rel) {
    return delegate.getPulledUpPredicates(rel);
  }

  @Override public @Nullable RelOptPredicateList getAllPredicates(
      final RelNode rel) {
    return delegate.getAllPredicates(rel);
  }

  @Override public Boolean isVisibleInExplain(final RelNode rel,
      final SqlExplainLevel explainLevel) {
    return delegate.isVisibleInExplain(rel, explainLevel);
  }

  @Override public @Nullable RelDistribution getDistribution(
      final RelNode rel) {
    return delegate.getDistribution(rel);
  }

  @Override public @Nullable RelOptCost getLowerBoundCost(
      final RelNode rel,
      final VolcanoPlanner planner) {
    return delegate.getLowerBoundCost(rel, planner);
  }

  @Override public List<@Nullable Double> getAverageColumnSizesNotNull(
      final RelNode rel) {
    return delegate.getAverageColumnSizesNotNull(rel);
  }

  @Override public @Nullable Set<ImmutableBitSet> getUniqueKeys(
      final RelNode rel) {
    return delegate.getUniqueKeys(rel);
  }

  @Override @Nullable public Boolean areRowsUnique(
      final RelNode rel, final boolean ignoreNulls) {
    return delegate.areRowsUnique(rel, ignoreNulls);
  }

  @Override @Nullable public Boolean areRowsUnique(
      final RelNode rel) {
    return delegate.areRowsUnique(rel);
  }

  @Override @Nullable public Boolean areColumnsUnique(
      final RelNode rel, final ImmutableBitSet columns) {
    return delegate.areColumnsUnique(rel, columns);
  }

  @Override public @Nullable RelColumnOrigin getColumnOrigin(
      final RelNode rel, final int column) {
    return delegate.getColumnOrigin(rel, column);
  }

  @Override public @Nullable RelOptTable getTableOrigin(
      final RelNode rel) {
    return delegate.getTableOrigin(rel);
  }
}
