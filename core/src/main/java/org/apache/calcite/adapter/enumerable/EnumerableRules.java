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
package org.apache.calcite.adapter.enumerable;

import org.apache.calcite.linq4j.function.Experimental;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalMatch;
import org.apache.calcite.rel.logical.LogicalRepeatUnion;
import org.apache.calcite.rel.logical.LogicalTableSpool;
import org.apache.calcite.util.trace.CalciteTrace;

import com.google.common.collect.ImmutableList;

import org.slf4j.Logger;

import java.util.List;

/**
 * Rules and relational operators for the
 * {@link EnumerableConvention enumerable calling convention}.
 */
public class EnumerableRules {
  protected static final Logger LOGGER = CalciteTrace.getPlannerTracer();

  public static final boolean BRIDGE_METHODS = true;

  private EnumerableRules() {
  }

  /** Rule that converts a
   * {@link org.apache.calcite.rel.logical.LogicalJoin} to
   * {@link EnumerableConvention enumerable calling convention}. */
  public static final RelOptRule ENUMERABLE_JOIN_RULE =
      EnumerableJoinRule.DEFAULT_CONFIG.toRule(EnumerableJoinRule.class);

  /** Rule that converts a
   * {@link org.apache.calcite.rel.logical.LogicalAsofJoin} to
   * {@link EnumerableConvention enumerable calling convention}. */
  public static final RelOptRule ENUMERABLE_ASOFJOIN_RULE =
      EnumerableAsofJoinRule.DEFAULT_CONFIG.toRule(EnumerableAsofJoinRule.class);

  /** Rule that converts a
   * {@link org.apache.calcite.rel.logical.LogicalJoin} to
   * {@link EnumerableConvention enumerable calling convention}. */
  public static final RelOptRule ENUMERABLE_MERGE_JOIN_RULE =
      EnumerableMergeJoinRule.DEFAULT_CONFIG
          .toRule(EnumerableMergeJoinRule.class);

  public static final RelOptRule ENUMERABLE_CORRELATE_RULE =
      EnumerableCorrelateRule.DEFAULT_CONFIG
          .toRule(EnumerableCorrelateRule.class);

  /** Rule that converts a
   * {@link org.apache.calcite.rel.logical.LogicalJoin} into an
   * {@link org.apache.calcite.adapter.enumerable.EnumerableBatchNestedLoopJoin}. */
  public static final RelOptRule ENUMERABLE_BATCH_NESTED_LOOP_JOIN_RULE =
      EnumerableBatchNestedLoopJoinRule.Config.DEFAULT.toRule();

  /** Rule that converts a
   * {@link org.apache.calcite.rel.logical.LogicalProject} to an
   * {@link EnumerableProject}. */
  public static final EnumerableProjectRule ENUMERABLE_PROJECT_RULE =
      EnumerableProjectRule.DEFAULT_CONFIG.toRule(EnumerableProjectRule.class);

  public static final EnumerableFilterRule ENUMERABLE_FILTER_RULE =
      EnumerableFilterRule.DEFAULT_CONFIG.toRule(EnumerableFilterRule.class);

  public static final EnumerableCalcRule ENUMERABLE_CALC_RULE =
      EnumerableCalcRule.DEFAULT_CONFIG.toRule(EnumerableCalcRule.class);

  public static final EnumerableAggregateRule ENUMERABLE_AGGREGATE_RULE =
      EnumerableAggregateRule.DEFAULT_CONFIG
          .toRule(EnumerableAggregateRule.class);

  /** Rule that converts a {@link org.apache.calcite.rel.core.Sort} to an
   * {@link EnumerableSort}. */
  public static final EnumerableSortRule ENUMERABLE_SORT_RULE =
      EnumerableSortRule.DEFAULT_CONFIG.toRule(EnumerableSortRule.class);

  public static final EnumerableLimitSortRule ENUMERABLE_LIMIT_SORT_RULE =
      EnumerableLimitSortRule.Config.DEFAULT.toRule();

  public static final EnumerableLimitRule ENUMERABLE_LIMIT_RULE =
      EnumerableLimitRule.Config.DEFAULT.toRule();

  /** Rule that converts a {@link org.apache.calcite.rel.logical.LogicalUnion}
   * to an {@link EnumerableUnion}. */
  public static final EnumerableUnionRule ENUMERABLE_UNION_RULE =
      EnumerableUnionRule.DEFAULT_CONFIG.toRule(EnumerableUnionRule.class);

  /** Rule that converts a {@link org.apache.calcite.rel.core.Combine}
   * to an {@link EnumerableCombine}. */
  public static final EnumerableCombineRule ENUMERABLE_COMBINE_RULE =
      EnumerableCombineRule.DEFAULT_CONFIG.toRule(EnumerableCombineRule.class);

  /** Rule that converts a {@link LogicalRepeatUnion} into an
   * {@link EnumerableRepeatUnion}. */
  public static final EnumerableRepeatUnionRule ENUMERABLE_REPEAT_UNION_RULE =
      EnumerableRepeatUnionRule.DEFAULT_CONFIG
          .toRule(EnumerableRepeatUnionRule.class);

  /** Rule to convert a {@link org.apache.calcite.rel.logical.LogicalSort} on top of a
   * {@link org.apache.calcite.rel.logical.LogicalUnion} into a {@link EnumerableMergeUnion}. */
  public static final EnumerableMergeUnionRule ENUMERABLE_MERGE_UNION_RULE =
      EnumerableMergeUnionRule.Config.DEFAULT_CONFIG.toRule();

  /** Rule that converts a {@link LogicalTableSpool} into an
   * {@link EnumerableTableSpool}. */
  @Experimental
  public static final EnumerableTableSpoolRule ENUMERABLE_TABLE_SPOOL_RULE =
      EnumerableTableSpoolRule.DEFAULT_CONFIG
          .toRule(EnumerableTableSpoolRule.class);

  /** Rule that converts a
   * {@link org.apache.calcite.rel.logical.LogicalIntersect} to an
   * {@link EnumerableIntersect}. */
  public static final EnumerableIntersectRule ENUMERABLE_INTERSECT_RULE =
      EnumerableIntersectRule.DEFAULT_CONFIG
          .toRule(EnumerableIntersectRule.class);

  /** Rule that converts a
   * {@link org.apache.calcite.rel.logical.LogicalMinus} to an
   * {@link EnumerableMinus}. */
  public static final EnumerableMinusRule ENUMERABLE_MINUS_RULE =
      EnumerableMinusRule.DEFAULT_CONFIG.toRule(EnumerableMinusRule.class);

  /** Rule that converts a
   * {@link org.apache.calcite.rel.logical.LogicalTableModify} to
   * {@link EnumerableConvention enumerable calling convention}. */
  public static final EnumerableTableModifyRule ENUMERABLE_TABLE_MODIFICATION_RULE =
      EnumerableTableModifyRule.DEFAULT_CONFIG
          .toRule(EnumerableTableModifyRule.class);

  /** Rule that converts a
   * {@link org.apache.calcite.rel.logical.LogicalValues} to
   * {@link EnumerableConvention enumerable calling convention}. */
  public static final EnumerableValuesRule ENUMERABLE_VALUES_RULE =
      EnumerableValuesRule.DEFAULT_CONFIG.toRule(EnumerableValuesRule.class);

  /** Rule that converts a {@link org.apache.calcite.rel.logical.LogicalWindow}
   * to an {@link org.apache.calcite.adapter.enumerable.EnumerableWindow}. */
  public static final EnumerableWindowRule ENUMERABLE_WINDOW_RULE =
      EnumerableWindowRule.DEFAULT_CONFIG.toRule(EnumerableWindowRule.class);

  /** Rule that converts an {@link org.apache.calcite.rel.core.Collect}
   * to an {@link EnumerableCollect}. */
  public static final EnumerableCollectRule ENUMERABLE_COLLECT_RULE =
      EnumerableCollectRule.DEFAULT_CONFIG.toRule(EnumerableCollectRule.class);

  /** Rule that converts an {@link org.apache.calcite.rel.core.Uncollect}
   * to an {@link EnumerableUncollect}. */
  public static final EnumerableUncollectRule ENUMERABLE_UNCOLLECT_RULE =
      EnumerableUncollectRule.DEFAULT_CONFIG
          .toRule(EnumerableUncollectRule.class);

  public static final EnumerableFilterToCalcRule ENUMERABLE_FILTER_TO_CALC_RULE =
      EnumerableFilterToCalcRule.Config.DEFAULT.toRule();

  /** Variant of {@link org.apache.calcite.rel.rules.ProjectToCalcRule} for
   * {@link EnumerableConvention enumerable calling convention}. */
  public static final EnumerableProjectToCalcRule ENUMERABLE_PROJECT_TO_CALC_RULE =
      EnumerableProjectToCalcRule.Config.DEFAULT.toRule();

  /** Rule that converts a
   * {@link org.apache.calcite.rel.logical.LogicalTableScan} to
   * {@link EnumerableConvention enumerable calling convention}. */
  public static final EnumerableTableScanRule ENUMERABLE_TABLE_SCAN_RULE =
      EnumerableTableScanRule.DEFAULT_CONFIG
          .toRule(EnumerableTableScanRule.class);

  /** Rule that converts a
   * {@link org.apache.calcite.rel.logical.LogicalTableFunctionScan} to
   * {@link EnumerableConvention enumerable calling convention}. */
  public static final EnumerableTableFunctionScanRule ENUMERABLE_TABLE_FUNCTION_SCAN_RULE =
      EnumerableTableFunctionScanRule.DEFAULT_CONFIG
          .toRule(EnumerableTableFunctionScanRule.class);

  /** Rule that converts a {@link LogicalMatch} to an
   * {@link EnumerableMatch}. */
  public static final EnumerableMatchRule ENUMERABLE_MATCH_RULE =
      EnumerableMatchRule.DEFAULT_CONFIG.toRule(EnumerableMatchRule.class);

  /** Rule to convert a {@link LogicalAggregate}
   * to an {@link EnumerableSortedAggregate}. */
  public static final EnumerableSortedAggregateRule ENUMERABLE_SORTED_AGGREGATE_RULE =
      EnumerableSortedAggregateRule.DEFAULT_CONFIG
          .toRule(EnumerableSortedAggregateRule.class);

  /** Rule that converts any enumerable relational expression to bindable. */
  public static final EnumerableBindable.EnumerableToBindableConverterRule TO_BINDABLE =
      EnumerableBindable.EnumerableToBindableConverterRule.DEFAULT_CONFIG
          .toRule(EnumerableBindable.EnumerableToBindableConverterRule.class);

  /**
   * Rule that converts {@link org.apache.calcite.interpreter.BindableRel}
   * to {@link org.apache.calcite.adapter.enumerable.EnumerableRel} by creating
   * an {@link org.apache.calcite.adapter.enumerable.EnumerableInterpreter}. */
  public static final EnumerableInterpreterRule TO_INTERPRETER =
      EnumerableInterpreterRule.DEFAULT_CONFIG
          .toRule(EnumerableInterpreterRule.class);

  public static final List<RelOptRule> ENUMERABLE_RULES =
      ImmutableList.of(EnumerableRules.ENUMERABLE_JOIN_RULE,
          EnumerableRules.ENUMERABLE_ASOFJOIN_RULE,
          EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE,
          EnumerableRules.ENUMERABLE_CORRELATE_RULE,
          EnumerableRules.ENUMERABLE_PROJECT_RULE,
          EnumerableRules.ENUMERABLE_FILTER_RULE,
          EnumerableRules.ENUMERABLE_CALC_RULE,
          EnumerableRules.ENUMERABLE_AGGREGATE_RULE,
          EnumerableRules.ENUMERABLE_SORT_RULE,
          EnumerableRules.ENUMERABLE_LIMIT_RULE,
          EnumerableRules.ENUMERABLE_COLLECT_RULE,
          EnumerableRules.ENUMERABLE_UNCOLLECT_RULE,
          EnumerableRules.ENUMERABLE_MERGE_UNION_RULE,
          EnumerableRules.ENUMERABLE_UNION_RULE,
          EnumerableRules.ENUMERABLE_COMBINE_RULE,
          EnumerableRules.ENUMERABLE_REPEAT_UNION_RULE,
          EnumerableRules.ENUMERABLE_TABLE_SPOOL_RULE,
          EnumerableRules.ENUMERABLE_INTERSECT_RULE,
          EnumerableRules.ENUMERABLE_MINUS_RULE,
          EnumerableRules.ENUMERABLE_TABLE_MODIFICATION_RULE,
          EnumerableRules.ENUMERABLE_VALUES_RULE,
          EnumerableRules.ENUMERABLE_WINDOW_RULE,
          EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE,
          EnumerableRules.ENUMERABLE_TABLE_FUNCTION_SCAN_RULE,
          EnumerableRules.ENUMERABLE_MATCH_RULE);

  public static List<RelOptRule> rules() {
    return ENUMERABLE_RULES;
  }
}
