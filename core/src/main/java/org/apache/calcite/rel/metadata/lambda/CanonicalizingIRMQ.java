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

import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.ImmutableBitSet;

import org.checkerframework.checker.nullness.qual.Nullable;

import static org.apache.calcite.linq4j.Nullness.castNonNull;

/**
 * Canonicalizes methods from null to other standard outputs.
 */
class CanonicalizingIRMQ extends DelegatingIRMQ {
  CanonicalizingIRMQ(IRelMetadataQuery delegate) {
    super(delegate);
  }

  @Override public @Nullable RelDistribution distribution(final RelNode rel) {
    RelDistribution distribution = super.distribution(rel);
    if (distribution == null) {
      return RelDistributions.ANY;
    }
    return distribution;
  }

  public Double getRowCount(RelNode rel) {
    return RelMdUtil.validateResult(castNonNull(super.getRowCount(rel)));
  }

  public @Nullable Double getPopulationSize(RelNode rel, ImmutableBitSet groupKey) {
    return RelMdUtil.validateResult(super.getPopulationSize(rel, groupKey));
  }

  public @Nullable Double getPercentageOriginalRows(RelNode rel) {
    Double result = super.getPercentageOriginalRows(rel);
    return RelMdUtil.validatePercentage(result);
  }

  public @Nullable Double getSelectivity(RelNode rel, @Nullable RexNode predicate) {
    return RelMdUtil.validatePercentage(super.getSelectivity(rel, predicate));
  }

  public @Nullable Double getDistinctRowCount(
      RelNode rel,
      ImmutableBitSet groupKey,
      @Nullable RexNode predicate) {
    return RelMdUtil.validateResult(super.getDistinctRowCount(rel, groupKey, predicate));
  }

  public RelOptPredicateList getPulledUpPredicates(RelNode rel) {
    RelOptPredicateList result = super.getPulledUpPredicates(rel);
    return result != null ? result : RelOptPredicateList.EMPTY;
  }

  public Boolean isVisibleInExplain(RelNode rel,
      SqlExplainLevel explainLevel) {
    Boolean b = super.isVisibleInExplain(rel, explainLevel);
    return b == null || b;
  }
}
