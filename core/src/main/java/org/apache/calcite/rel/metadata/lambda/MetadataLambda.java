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
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;

import org.apiguardian.api.API;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Set;
import javax.annotation.concurrent.ThreadSafe;

/**
   *
 * A marker interface that describes a lambda for Metadata retrieval.
 */
@API(since = "1.29", status = API.Status.EXPERIMENTAL)
@ThreadSafe
public interface MetadataLambda {
  /**
   * Metadata Lambda for NodeTypes.
   * @param <R> Specific RelNode subclass this interface captures.
   */
  @FunctionalInterface interface NodeTypes<R extends RelNode>
      extends MetadataLambda, Arg0Lambda<R, Multimap<Class<? extends RelNode>, RelNode>> { }

  /**
   * Metadata Lambda for NodeTypes.
   * @param <R> Specific RelNode subclass this interface captures.
   */
  @FunctionalInterface
  interface AverageColumnSizes<R extends RelNode>
      extends MetadataLambda, Arg0Lambda<R, List<@Nullable Double>> { }

  /**
   * Metadata Lambda for RowCount.
   * @param <R> Specific RelNode subclass this interface captures.
   */
  @FunctionalInterface interface RowCount<R extends RelNode>
      extends MetadataLambda, Arg0Lambda<R, Double> { }

  /**
   * Metadata Lambda for MaxRowCount.
   * @param <R> Specific RelNode subclass this interface captures.
   */
  @FunctionalInterface interface MaxRowCount<R extends RelNode>
      extends MetadataLambda, Arg0Lambda<R, Double> { }

  /**
   * Metadata Lambda for MinRowCount.
   * @param <R> Specific RelNode subclass this interface captures.
   */
  @FunctionalInterface interface MinRowCount<R extends RelNode>
      extends MetadataLambda, Arg0Lambda<R, Double> { }

  /**
   * Metadata Lambda for CumulativeCost.
   * @param <R> Specific RelNode subclass this interface captures.
   */
  @FunctionalInterface interface CumulativeCost<R extends RelNode>
      extends MetadataLambda, Arg0Lambda<R, RelOptCost> { }

  /**
   * Metadata Lambda for NonCumulativeCost.
   * @param <R> Specific RelNode subclass this interface captures.
   */
  @FunctionalInterface interface NonCumulativeCost<R extends RelNode>
      extends MetadataLambda, Arg0Lambda<R, RelOptCost> { }

  /**
   * Metadata Lambda for PercentageOriginalRows.
   * @param <R> Specific RelNode subclass this interface captures.
   */
  @FunctionalInterface interface PercentageOriginalRows<R extends RelNode>
      extends MetadataLambda, Arg0Lambda<R, Double> { }

  /**
   * Metadata Lambda for ColumnOrigins.
   * @param <R> Specific RelNode subclass this interface captures.
   */
  @FunctionalInterface interface ColumnOrigins<R extends RelNode>
      extends MetadataLambda, Arg1Lambda<R, Integer, Set<RelColumnOrigin>> { }

  /**
   * Metadata Lambda for ExpressionLineage.
   * @param <R> Specific RelNode subclass this interface captures.
   */
  @FunctionalInterface interface ExpressionLineage<R extends RelNode>
      extends MetadataLambda, Arg1Lambda<R, RexNode, Set<RexNode>> { }

  /**
   * Metadata Lambda for TableReferences.
   * @param <R> Specific RelNode subclass this interface captures.
   */
  @FunctionalInterface interface TableReferences<R extends RelNode>
      extends MetadataLambda, Arg0Lambda<R, Set<RexTableInputRef.RelTableRef>> { }

  /**
   * Metadata Lambda for Selectivity.
   * @param <R> Specific RelNode subclass this interface captures.
   */
  @FunctionalInterface interface Selectivity<R extends RelNode>
      extends MetadataLambda, Arg1Lambda<R, RexNode, Double> { }

  /**
   * Metadata Lambda for UniqueKeys.
   * @param <R> Specific RelNode subclass this interface captures.
   */
  @FunctionalInterface interface UniqueKeys<R extends RelNode>
      extends MetadataLambda, Arg1Lambda<R, Boolean, Set<ImmutableBitSet>> { }

  /**
   * Metadata Lambda for ColumnsUnique.
   * @param <R> Specific RelNode subclass this interface captures.
   */
  @FunctionalInterface interface ColumnsUnique<R extends RelNode>
      extends MetadataLambda, Arg2Lambda<R, ImmutableBitSet, Boolean, Boolean> { }

  /**
   * Metadata Lambda for Collations.
   * @param <R> Specific RelNode subclass this interface captures.
   */
  @FunctionalInterface interface Collations<R extends RelNode>
      extends MetadataLambda, Arg0Lambda<R, ImmutableList<RelCollation>> { }

  /**
   * Metadata Lambda for PopulationSize.
   * @param <R> Specific RelNode subclass this interface captures.
   */
  @FunctionalInterface interface PopulationSize<R extends RelNode>
      extends MetadataLambda, Arg1Lambda<R, ImmutableBitSet, Double> { }

  /**
   * Metadata Lambda for AverageRowSize.
   * @param <R> Specific RelNode subclass this interface captures.
   */
  @FunctionalInterface interface AverageRowSize<R extends RelNode>
      extends MetadataLambda, Arg0Lambda<R, Double> { }

  /**
   * Metadata Lambda for PhaseTransition.
   * @param <R> Specific RelNode subclass this interface captures.
   */
  @FunctionalInterface interface PhaseTransition<R extends RelNode>
      extends MetadataLambda, Arg0Lambda<R, Boolean> { }

  /**
   * Metadata Lambda for SplitCount.
   * @param <R> Specific RelNode subclass this interface captures.
   */
  @FunctionalInterface interface SplitCount<R extends RelNode>
      extends MetadataLambda, Arg0Lambda<R, Integer> { }

  /**
   * Metadata Lambda for Memory.
   * @param <R> Specific RelNode subclass this interface captures.
   */
  @FunctionalInterface interface Memory<R extends RelNode>
      extends MetadataLambda, Arg0Lambda<R, Double> { }

  /**
   * Metadata Lambda for CumulativeMemoryWithinPhase.
   * @param <R> Specific RelNode subclass this interface captures.
   */
  @FunctionalInterface interface CumulativeMemoryWithinPhase<R extends RelNode>
      extends MetadataLambda, Arg0Lambda<R, Double> { }

  /**
   * Metadata Lambda for CumulativeMemoryWithinPhaseSplit.
   * @param <R> Specific RelNode subclass this interface captures.
   */
  @FunctionalInterface interface CumulativeMemoryWithinPhaseSplit<R extends RelNode>
      extends MetadataLambda, Arg0Lambda<R, Double> { }

  /**
   * Metadata Lambda for DistinctRowCount.
   * @param <R> Specific RelNode subclass this interface captures.
   */
  @FunctionalInterface interface DistinctRowCount<R extends RelNode>
      extends MetadataLambda, Arg2Lambda<R, ImmutableBitSet, @Nullable RexNode, Double> { }

  /**
   * Metadata Lambda for PulledUpPredicates.
   * @param <R> Specific RelNode subclass this interface captures.
   */
  @FunctionalInterface interface PulledUpPredicates<R extends RelNode>
      extends MetadataLambda, Arg0Lambda<R, RelOptPredicateList> { }

  /**
   * Metadata Lambda for AllPredicates.
   * @param <R> Specific RelNode subclass this interface captures.
   */
  @FunctionalInterface interface AllPredicates<R extends RelNode>
      extends MetadataLambda, Arg0Lambda<R, RelOptPredicateList> { }

  /**
   * Metadata Lambda for VisibleInExplain.
   * @param <R> Specific RelNode subclass this interface captures.
   */
  @FunctionalInterface interface VisibleInExplain<R extends RelNode>
      extends MetadataLambda, Arg1Lambda<R, SqlExplainLevel, Boolean> { }

  /**
   * Metadata Lambda for Distribution.
   * @param <R> Specific RelNode subclass this interface captures.
   */
  @FunctionalInterface interface Distribution<R extends RelNode>
      extends MetadataLambda, Arg0Lambda<R, RelDistribution> { }

  /**
   * Metadata Lambda for LowerBoundCost.
   * @param <R> Specific RelNode subclass this interface captures.
   */
  @FunctionalInterface interface LowerBoundCost<R extends RelNode>
      extends MetadataLambda, Arg1Lambda<R, VolcanoPlanner, RelOptCost> { }

  /**
   * Base Lambda interface for zero argument metadata call.
   * @param <T> Specific RelNode subclass this interface captures.
   * @param <R> Return data type.
   */
  interface Arg0Lambda<T extends RelNode, R> {

    /** Call with rel, mq and 0 arguments. */
    R call(T rel, RelMetadataQuery mq);
  }

  /**
   * Base Lambda interface for one argument metadata call.
   * @param <T> Specific RelNode subclass this interface captures.
   * @param <A0> First argument type.
   * @param <R> Return data type.
   */
  interface Arg1Lambda<T extends RelNode, A0, R> {
    /** Call with rel, mq and 1 argument. */
    R call(T rel, RelMetadataQuery mq, A0 arg0);
  }

  /**
   * Base Lambda interface for two argument metadata call.
   * @param <T> Specific RelNode subclass this interface captures.
   * @param <A0> First argument type.
   * @param <A1> Second argument type.
   * @param <R> Return data type.
   */
  interface Arg2Lambda<T extends RelNode, A0, A1, R> {
    /** Call with rel, mq and 2 arguments. */
    R call(T rel, RelMetadataQuery mq, A0 arg0, A1 arg1);
  }
}
