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
package org.apache.calcite.rel.rules;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Intersect;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Minus;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalExchange;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalMatch;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSortExchange;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.rel.rules.materialize.MaterializedViewRules;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.schema.impl.StarTable;

/** Rules that perform logical transformations on relational expressions.
 *
 * @see MaterializedViewRules */
public class CoreRules {

  private CoreRules() {}

  /** Rule that recognizes an {@link Aggregate}
   * on top of a {@link Project} and if possible
   * aggregates through the Project or removes the Project. */
  public static final AggregateProjectMergeRule AGGREGATE_PROJECT_MERGE =
      AggregateProjectMergeRule.Config.DEFAULT.toRule();

  /** Rule that removes constant keys from an {@link Aggregate}. */
  public static final AggregateProjectPullUpConstantsRule
      AGGREGATE_PROJECT_PULL_UP_CONSTANTS =
      AggregateProjectPullUpConstantsRule.Config.DEFAULT.toRule();

  /** More general form of {@link #AGGREGATE_PROJECT_PULL_UP_CONSTANTS}
   * that matches any relational expression. */
  public static final AggregateProjectPullUpConstantsRule
      AGGREGATE_ANY_PULL_UP_CONSTANTS =
      AggregateProjectPullUpConstantsRule.Config.DEFAULT
          .withOperandFor(LogicalAggregate.class, RelNode.class)
          .toRule();

  /** Rule that matches an {@link Aggregate} on
   * a {@link StarTable.StarTableScan}. */
  public static final AggregateStarTableRule AGGREGATE_STAR_TABLE =
      AggregateStarTableRule.Config.DEFAULT.toRule();

  /** Variant of {@link #AGGREGATE_STAR_TABLE} that accepts a {@link Project}
   * between the {@link Aggregate} and its {@link StarTable.StarTableScan}
   * input. */
  public static final AggregateProjectStarTableRule AGGREGATE_PROJECT_STAR_TABLE =
      AggregateProjectStarTableRule.Config.DEFAULT.toRule();

  /** Rule that reduces aggregate functions in
   * an {@link Aggregate} to simpler forms. */
  public static final AggregateReduceFunctionsRule AGGREGATE_REDUCE_FUNCTIONS =
      AggregateReduceFunctionsRule.Config.DEFAULT.toRule();

  /** Rule that matches an {@link Aggregate} on an {@link Aggregate},
   * and merges into a single Aggregate if the top aggregate's group key is a
   * subset of the lower aggregate's group key, and the aggregates are
   * expansions of rollups. */
  public static final AggregateMergeRule AGGREGATE_MERGE =
      AggregateMergeRule.Config.DEFAULT.toRule();

  /** Rule that removes an {@link Aggregate}
   * if it computes no aggregate functions
   * (that is, it is implementing {@code SELECT DISTINCT}),
   * or all the aggregate functions are splittable,
   * and the underlying relational expression is already distinct. */
  public static final AggregateRemoveRule AGGREGATE_REMOVE =
      AggregateRemoveRule.Config.DEFAULT.toRule();

  /** Rule that expands distinct aggregates
   * (such as {@code COUNT(DISTINCT x)}) from a
   * {@link Aggregate}.
   * This instance operates only on logical expressions. */
  public static final AggregateExpandDistinctAggregatesRule
      AGGREGATE_EXPAND_DISTINCT_AGGREGATES =
      AggregateExpandDistinctAggregatesRule.Config.DEFAULT.toRule();

  /** As {@link #AGGREGATE_EXPAND_DISTINCT_AGGREGATES} but generates a Join. */
  public static final AggregateExpandDistinctAggregatesRule
      AGGREGATE_EXPAND_DISTINCT_AGGREGATES_TO_JOIN =
      AggregateExpandDistinctAggregatesRule.Config.JOIN.toRule();

  /** Rule that rewrites a {@link LogicalAggregate} that contains
   * {@code WITHIN DISTINCT} aggregate functions to one that does not. */
  public static final AggregateExpandWithinDistinctRule
      AGGREGATE_EXPAND_WITHIN_DISTINCT =
      AggregateExpandWithinDistinctRule.Config.DEFAULT.toRule();

  /** Rule that matches an {@link Aggregate}
   * on a {@link Filter} and transposes them,
   * pushing the aggregate below the filter. */
  public static final AggregateFilterTransposeRule AGGREGATE_FILTER_TRANSPOSE =
      AggregateFilterTransposeRule.Config.DEFAULT.toRule();

  /** Rule that matches an {@link Aggregate}
   * on a {@link Join} and removes the left input
   * of the join provided that the left input is also a left join if
   * possible. */
  public static final AggregateJoinJoinRemoveRule AGGREGATE_JOIN_JOIN_REMOVE =
      AggregateJoinJoinRemoveRule.Config.DEFAULT.toRule();

  /** Rule that matches an {@link Aggregate}
   * on a {@link Join} and removes the join
   * provided that the join is a left join or right join and it computes no
   * aggregate functions or all the aggregate calls have distinct. */
  public static final AggregateJoinRemoveRule AGGREGATE_JOIN_REMOVE =
      AggregateJoinRemoveRule.Config.DEFAULT.toRule();

  /** Rule that pushes an {@link Aggregate}
   * past a {@link Join}. */
  public static final AggregateJoinTransposeRule AGGREGATE_JOIN_TRANSPOSE =
      AggregateJoinTransposeRule.Config.DEFAULT.toRule();

  /** As {@link #AGGREGATE_JOIN_TRANSPOSE}, but extended to push down aggregate
   * functions. */
  public static final AggregateJoinTransposeRule AGGREGATE_JOIN_TRANSPOSE_EXTENDED =
      AggregateJoinTransposeRule.Config.EXTENDED.toRule();

  /** Rule that pushes an {@link Aggregate}
   * past a non-distinct {@link Union}. */
  public static final AggregateUnionTransposeRule AGGREGATE_UNION_TRANSPOSE =
      AggregateUnionTransposeRule.Config.DEFAULT.toRule();

  /** Rule that matches an {@link Aggregate} whose input is a {@link Union}
   * one of whose inputs is an {@code Aggregate}.
   *
   * <p>Because it matches {@link RelNode} for each input of {@code Union}, it
   * will create O(N ^ 2) matches, which may cost too much during the popMatch
   * phase in VolcanoPlanner. If efficiency is a concern, we recommend that you
   * use {@link #AGGREGATE_UNION_AGGREGATE_FIRST}
   * and {@link #AGGREGATE_UNION_AGGREGATE_SECOND} instead. */
  public static final AggregateUnionAggregateRule AGGREGATE_UNION_AGGREGATE =
      AggregateUnionAggregateRule.Config.DEFAULT.toRule();

  /** As {@link #AGGREGATE_UNION_AGGREGATE}, but matches an {@code Aggregate}
   * only as the left input of the {@code Union}. */
  public static final AggregateUnionAggregateRule AGGREGATE_UNION_AGGREGATE_FIRST =
      AggregateUnionAggregateRule.Config.AGG_FIRST.toRule();

  /** As {@link #AGGREGATE_UNION_AGGREGATE}, but matches an {@code Aggregate}
   * only as the right input of the {@code Union}. */
  public static final AggregateUnionAggregateRule AGGREGATE_UNION_AGGREGATE_SECOND =
      AggregateUnionAggregateRule.Config.AGG_SECOND.toRule();

  /** Rule that converts CASE-style filtered aggregates into true filtered
   * aggregates. */
  public static final AggregateCaseToFilterRule AGGREGATE_CASE_TO_FILTER =
      AggregateCaseToFilterRule.Config.DEFAULT.toRule();

  /** Rule that merges a {@link Calc} onto a {@code Calc}. */
  public static final CalcMergeRule CALC_MERGE =
      CalcMergeRule.Config.DEFAULT.toRule();

  /** Rule that removes a trivial {@link LogicalCalc}. */
  public static final CalcRemoveRule CALC_REMOVE =
      CalcRemoveRule.Config.DEFAULT.toRule();

  /** Rule that reduces operations on the DECIMAL type, such as casts or
   * arithmetic, into operations involving more primitive types such as BIGINT
   * and DOUBLE. */
  public static final ReduceDecimalsRule CALC_REDUCE_DECIMALS =
      ReduceDecimalsRule.Config.DEFAULT.toRule();

  /** Rule that reduces constants inside a {@link LogicalCalc}.
   *
   * @see #FILTER_REDUCE_EXPRESSIONS */
  public static final ReduceExpressionsRule.CalcReduceExpressionsRule
      CALC_REDUCE_EXPRESSIONS =
      ReduceExpressionsRule.CalcReduceExpressionsRule.CalcReduceExpressionsRuleConfig.DEFAULT
          .toRule();

  /** Rule that converts a {@link Calc} to a {@link Project} and
   * {@link Filter}. */
  public static final CalcSplitRule CALC_SPLIT =
      CalcSplitRule.Config.DEFAULT.toRule();

  /** Rule that transforms a {@link Calc}
   * that contains windowed aggregates to a mixture of
   * {@link LogicalWindow} and {@code Calc}. */
  public static final ProjectToWindowRule.CalcToWindowRule CALC_TO_WINDOW =
      ProjectToWindowRule.CalcToWindowRule.CalcToWindowRuleConfig.DEFAULT.toRule();

  /** Rule that pre-casts inputs to a particular type. This can assist operator
   * implementations that impose requirements on their input types. */
  public static final CoerceInputsRule COERCE_INPUTS =
      CoerceInputsRule.Config.DEFAULT.toRule();

  /** Rule that removes constants inside a {@link LogicalExchange}. */
  public static final ExchangeRemoveConstantKeysRule EXCHANGE_REMOVE_CONSTANT_KEYS =
      ExchangeRemoveConstantKeysRule.Config.DEFAULT.toRule();

  /** Rule that removes constants inside a {@link LogicalSortExchange}. */
  public static final ExchangeRemoveConstantKeysRule SORT_EXCHANGE_REMOVE_CONSTANT_KEYS =
      ExchangeRemoveConstantKeysRule.Config.SORT.toRule();

  /** Rule that tries to push filter expressions into a join
   * condition and into the inputs of the join. */
  public static final FilterJoinRule.FilterIntoJoinRule FILTER_INTO_JOIN =
      FilterJoinRule.FilterIntoJoinRule.FilterIntoJoinRuleConfig.DEFAULT.toRule();

  /** Dumber version of {@link #FILTER_INTO_JOIN}. Not intended for production
   * use, but keeps some tests working for which {@code FILTER_INTO_JOIN} is too
   * smart. */
  public static final FilterJoinRule.FilterIntoJoinRule FILTER_INTO_JOIN_DUMB =
      FILTER_INTO_JOIN.config
          .withSmart(false)
          .as(FilterJoinRule.FilterIntoJoinRule.FilterIntoJoinRuleConfig.class)
          .toRule();

  /** Rule that combines two {@link LogicalFilter}s. */
  public static final FilterMergeRule FILTER_MERGE =
      FilterMergeRule.Config.DEFAULT.toRule();

  /** Rule that merges a {@link Filter} and a {@link LogicalCalc}. The
   * result is a {@link LogicalCalc} whose filter condition is the logical AND
   * of the two.
   *
   * @see #PROJECT_CALC_MERGE */
  public static final FilterCalcMergeRule FILTER_CALC_MERGE =
      FilterCalcMergeRule.Config.DEFAULT.toRule();

  /** Rule that converts a {@link LogicalFilter} to a {@link LogicalCalc}.
   *
   * @see #PROJECT_TO_CALC */
  public static final FilterToCalcRule FILTER_TO_CALC =
      FilterToCalcRule.Config.DEFAULT.toRule();

  /** Rule that pushes a {@link Filter} past an {@link Aggregate}.
   *
   * @see #AGGREGATE_FILTER_TRANSPOSE */
  public static final FilterAggregateTransposeRule FILTER_AGGREGATE_TRANSPOSE =
      FilterAggregateTransposeRule.Config.DEFAULT.toRule();

  /** The default instance of
   * {@link org.apache.calcite.rel.rules.FilterProjectTransposeRule}.
   *
   * <p>It does not allow a Filter to be pushed past the Project if
   * {@link RexUtil#containsCorrelation there is a correlation condition})
   * anywhere in the Filter, since in some cases it can prevent a
   * {@link Correlate} from being de-correlated.
   */
  public static final FilterProjectTransposeRule FILTER_PROJECT_TRANSPOSE =
      FilterProjectTransposeRule.Config.DEFAULT.toRule();

  /** Rule that pushes a {@link LogicalFilter}
   * past a {@link LogicalTableFunctionScan}. */
  public static final FilterTableFunctionTransposeRule
      FILTER_TABLE_FUNCTION_TRANSPOSE =
      FilterTableFunctionTransposeRule.Config.DEFAULT.toRule();

  /** Rule that matches a {@link Filter} on a {@link TableScan}. */
  public static final FilterTableScanRule FILTER_SCAN =
      FilterTableScanRule.Config.DEFAULT.toRule();

  /** Rule that matches a {@link Filter} on an
   * {@link org.apache.calcite.adapter.enumerable.EnumerableInterpreter} on a
   * {@link TableScan}. */
  public static final FilterTableScanRule FILTER_INTERPRETER_SCAN =
      FilterTableScanRule.Config.INTERPRETER.toRule();

  /** Rule that pushes a {@link Filter} above a {@link Correlate} into the
   * inputs of the {@code Correlate}. */
  public static final FilterCorrelateRule FILTER_CORRELATE =
      FilterCorrelateRule.Config.DEFAULT.toRule();

  /** Rule that merges a {@link Filter} into a {@link MultiJoin},
   * creating a richer {@code MultiJoin}.
   *
   * @see #PROJECT_MULTI_JOIN_MERGE */
  public static final FilterMultiJoinMergeRule FILTER_MULTI_JOIN_MERGE =
      FilterMultiJoinMergeRule.Config.DEFAULT.toRule();

  /** Rule that replaces {@code IS NOT DISTINCT FROM}
   * in a {@link Filter} with logically equivalent operations. */
  public static final FilterRemoveIsNotDistinctFromRule
      FILTER_EXPAND_IS_NOT_DISTINCT_FROM =
      FilterRemoveIsNotDistinctFromRule.Config.DEFAULT.toRule();

  /** Rule that pushes a {@link Filter} past a {@link SetOp}. */
  public static final FilterSetOpTransposeRule FILTER_SET_OP_TRANSPOSE =
      FilterSetOpTransposeRule.Config.DEFAULT.toRule();

  /** Rule that reduces constants inside a {@link LogicalFilter}.
   *
   * @see #JOIN_REDUCE_EXPRESSIONS
   * @see #PROJECT_REDUCE_EXPRESSIONS
   * @see #CALC_REDUCE_EXPRESSIONS
   * @see #WINDOW_REDUCE_EXPRESSIONS
   */
  public static final ReduceExpressionsRule.FilterReduceExpressionsRule
      FILTER_REDUCE_EXPRESSIONS =
      ReduceExpressionsRule.FilterReduceExpressionsRule.FilterReduceExpressionsRuleConfig.DEFAULT
          .toRule();

  /** Rule that flattens an {@link Intersect} on an {@code Intersect}
   * into a single {@code Intersect}. */
  public static final UnionMergeRule INTERSECT_MERGE =
      UnionMergeRule.Config.INTERSECT.toRule();

  /** Rule that translates a distinct
   * {@link Intersect} into a group of operators
   * composed of {@link Union}, {@link Aggregate}, etc. */
  public static final IntersectToDistinctRule INTERSECT_TO_DISTINCT =
      IntersectToDistinctRule.Config.DEFAULT.toRule();

  /** Rule that converts a {@link LogicalMatch} to the result of calling
   * {@link LogicalMatch#copy}. */
  public static final MatchRule MATCH = MatchRule.Config.DEFAULT.toRule();

  /** Rule that flattens a {@link Minus} on a {@code Minus}
   * into a single {@code Minus}. */
  public static final UnionMergeRule MINUS_MERGE =
      UnionMergeRule.Config.MINUS.toRule();

  /** Rule that matches a {@link Project} on an {@link Aggregate},
   * projecting away aggregate calls that are not used. */
  public static final ProjectAggregateMergeRule PROJECT_AGGREGATE_MERGE =
      ProjectAggregateMergeRule.Config.DEFAULT.toRule();

  /** Rule that merges a {@link LogicalProject} and a {@link LogicalCalc}.
   *
   * @see #FILTER_CALC_MERGE */
  public static final ProjectCalcMergeRule PROJECT_CALC_MERGE =
      ProjectCalcMergeRule.Config.DEFAULT.toRule();

  /** Rule that matches a {@link Project} on a {@link Correlate} and
   * pushes the projections to the Correlate's left and right inputs. */
  public static final ProjectCorrelateTransposeRule PROJECT_CORRELATE_TRANSPOSE =
      ProjectCorrelateTransposeRule.Config.DEFAULT.toRule();

  /** Rule that pushes a {@link Project} past a {@link Filter}.
   *
   * @see #PROJECT_FILTER_TRANSPOSE_WHOLE_PROJECT_EXPRESSIONS
   * @see #PROJECT_FILTER_TRANSPOSE_WHOLE_EXPRESSIONS */
  public static final ProjectFilterTransposeRule PROJECT_FILTER_TRANSPOSE =
      ProjectFilterTransposeRule.Config.DEFAULT.toRule();

  /** As {@link #PROJECT_FILTER_TRANSPOSE}, but pushes down project and filter
   * expressions whole, not field references. */
  public static final ProjectFilterTransposeRule
      PROJECT_FILTER_TRANSPOSE_WHOLE_EXPRESSIONS =
      ProjectFilterTransposeRule.Config.PROJECT_FILTER.toRule();

  /** As {@link #PROJECT_FILTER_TRANSPOSE},
   * pushes down field references for filters,
   * but pushes down project expressions whole. */
  public static final ProjectFilterTransposeRule
      PROJECT_FILTER_TRANSPOSE_WHOLE_PROJECT_EXPRESSIONS =
      ProjectFilterTransposeRule.Config.PROJECT.toRule();

  /** Rule that reduces constants inside a {@link LogicalProject}.
   *
   * @see #FILTER_REDUCE_EXPRESSIONS */
  public static final ReduceExpressionsRule.ProjectReduceExpressionsRule
      PROJECT_REDUCE_EXPRESSIONS =
      ReduceExpressionsRule.ProjectReduceExpressionsRule.ProjectReduceExpressionsRuleConfig
          .DEFAULT.toRule();

  /** Rule that converts sub-queries from project expressions into
   * {@link Correlate} instances.
   *
   * @see #FILTER_SUB_QUERY_TO_CORRELATE
   * @see #JOIN_SUB_QUERY_TO_CORRELATE */
  public static final SubQueryRemoveRule PROJECT_SUB_QUERY_TO_CORRELATE =
      SubQueryRemoveRule.Config.PROJECT.toRule();

  /** Rule that converts a sub-queries from filter expressions into
   * {@link Correlate} instances.
   *
   * @see #PROJECT_SUB_QUERY_TO_CORRELATE
   * @see #JOIN_SUB_QUERY_TO_CORRELATE */
  public static final SubQueryRemoveRule FILTER_SUB_QUERY_TO_CORRELATE =
      SubQueryRemoveRule.Config.FILTER.toRule();

  /** Rule that converts sub-queries from join expressions into
   * {@link Correlate} instances.
   *
   * @see #PROJECT_SUB_QUERY_TO_CORRELATE
   * @see #FILTER_SUB_QUERY_TO_CORRELATE */
  public static final SubQueryRemoveRule JOIN_SUB_QUERY_TO_CORRELATE =
      SubQueryRemoveRule.Config.JOIN.toRule();

  /** Rule that transforms a {@link Project}
   *  into a mixture of {@code LogicalProject}
   * and {@link LogicalWindow}. */
  public static final ProjectToWindowRule.ProjectToLogicalProjectAndWindowRule
      PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW =
      ProjectToWindowRule.ProjectToLogicalProjectAndWindowRule.
          ProjectToLogicalProjectAndWindowRuleConfig.DEFAULT.toRule();

  /** Rule that creates a {@link Join#isSemiJoin semi-join} from a
   * {@link Project} on top of a {@link Join} with an {@link Aggregate} as its
   * right input.
   *
   * @see #JOIN_TO_SEMI_JOIN */
  public static final SemiJoinRule.ProjectToSemiJoinRule PROJECT_TO_SEMI_JOIN =
      SemiJoinRule.ProjectToSemiJoinRule.ProjectToSemiJoinRuleConfig.DEFAULT.toRule();

  /** Rule that matches an {@link Project} on a {@link Join} and removes the
   * left input of the join provided that the left input is also a left join. */
  public static final ProjectJoinJoinRemoveRule PROJECT_JOIN_JOIN_REMOVE =
      ProjectJoinJoinRemoveRule.Config.DEFAULT.toRule();

  /** Rule that matches an {@link Project} on a {@link Join} and removes the
   * join provided that the join is a left join or right join and the join keys
   * are unique. */
  public static final ProjectJoinRemoveRule PROJECT_JOIN_REMOVE =
      ProjectJoinRemoveRule.Config.DEFAULT.toRule();

  /** Rule that pushes a {@link LogicalProject} past a {@link LogicalJoin}
   * by splitting the projection into a projection on top of each child of
   * the join. */
  public static final ProjectJoinTransposeRule PROJECT_JOIN_TRANSPOSE =
      ProjectJoinTransposeRule.Config.DEFAULT.toRule();

  /** Rule that merges a {@link Project} into another {@link Project},
   * provided the projects are not projecting identical sets of input
   * references. */
  public static final ProjectMergeRule PROJECT_MERGE =
      ProjectMergeRule.Config.DEFAULT.toRule();

  /** Rule that pushes a {@link LogicalProject} past a {@link SetOp}.
   *
   * <p>The children of the {@code SetOp} will project
   * only the {@link RexInputRef}s referenced in the original
   * {@code LogicalProject}. */
  public static final ProjectSetOpTransposeRule PROJECT_SET_OP_TRANSPOSE =
      ProjectSetOpTransposeRule.Config.DEFAULT.toRule();

  /** Rule that pushes a {@link Project} into a {@link MultiJoin},
   * creating a richer {@code MultiJoin}.
   *
   * @see #FILTER_MULTI_JOIN_MERGE */
  public static final ProjectMultiJoinMergeRule PROJECT_MULTI_JOIN_MERGE =
      ProjectMultiJoinMergeRule.Config.DEFAULT.toRule();

  /** Rule that, given a {@link Project} node that merely returns its input,
   *  converts the node into its input. */
  public static final ProjectRemoveRule PROJECT_REMOVE =
      ProjectRemoveRule.Config.DEFAULT.toRule();

  /** Rule that converts a {@link Project} on a {@link TableScan}
   * of a {@link org.apache.calcite.schema.ProjectableFilterableTable}
   * to a {@link org.apache.calcite.interpreter.Bindables.BindableTableScan}.
   *
   * @see #PROJECT_INTERPRETER_TABLE_SCAN */
  public static final ProjectTableScanRule PROJECT_TABLE_SCAN =
      ProjectTableScanRule.Config.DEFAULT.toRule();

  /** As {@link #PROJECT_TABLE_SCAN}, but with an intervening
   * {@link org.apache.calcite.adapter.enumerable.EnumerableInterpreter}. */
  public static final ProjectTableScanRule PROJECT_INTERPRETER_TABLE_SCAN =
      ProjectTableScanRule.Config.INTERPRETER.toRule();

  /** Rule that converts a {@link LogicalProject} to a {@link LogicalCalc}.
   *
   * @see #FILTER_TO_CALC */
  public static final ProjectToCalcRule PROJECT_TO_CALC =
      ProjectToCalcRule.Config.DEFAULT.toRule();

  /** Rule that pushes a {@link LogicalProject} past a {@link LogicalWindow}. */
  public static final ProjectWindowTransposeRule PROJECT_WINDOW_TRANSPOSE =
      ProjectWindowTransposeRule.Config.DEFAULT.toRule();

  /** Rule that pushes predicates in a Join into the inputs to the join. */
  public static final FilterJoinRule.JoinConditionPushRule JOIN_CONDITION_PUSH =
      FilterJoinRule.JoinConditionPushRule.JoinConditionPushRuleConfig.DEFAULT.toRule();

  /** Rule to add a semi-join into a {@link Join}. */
  public static final JoinAddRedundantSemiJoinRule JOIN_ADD_REDUNDANT_SEMI_JOIN =
      JoinAddRedundantSemiJoinRule.Config.DEFAULT.toRule();

  /** Rule that changes a join based on the associativity rule,
   * ((a JOIN b) JOIN c) &rarr; (a JOIN (b JOIN c)). */
  public static final JoinAssociateRule JOIN_ASSOCIATE =
      JoinAssociateRule.Config.DEFAULT.toRule();

  /** Rule that permutes the inputs to an inner {@link Join}. */
  public static final JoinCommuteRule JOIN_COMMUTE =
      JoinCommuteRule.Config.DEFAULT.toRule();

  /** As {@link #JOIN_COMMUTE} but swaps outer joins as well as inner joins. */
  public static final JoinCommuteRule JOIN_COMMUTE_OUTER =
      JoinCommuteRule.Config.DEFAULT.withSwapOuter(true).toRule();

  /** Rule to convert an
   * {@link LogicalJoin inner join} to a
   * {@link LogicalFilter filter} on top of a
   * {@link LogicalJoin cartesian inner join}. */
  public static final JoinExtractFilterRule JOIN_EXTRACT_FILTER =
      JoinExtractFilterRule.Config.DEFAULT.toRule();

  /** Rule that matches a {@link LogicalJoin} whose inputs are
   * {@link LogicalProject}s, and pulls the project expressions up. */
  public static final JoinProjectTransposeRule JOIN_PROJECT_BOTH_TRANSPOSE =
      JoinProjectTransposeRule.Config.DEFAULT.toRule();

  /** As {@link #JOIN_PROJECT_BOTH_TRANSPOSE} but only the left input is
   * a {@link LogicalProject}. */
  public static final JoinProjectTransposeRule JOIN_PROJECT_LEFT_TRANSPOSE =
      JoinProjectTransposeRule.Config.LEFT.toRule();

  /** As {@link #JOIN_PROJECT_BOTH_TRANSPOSE} but only the right input is
   * a {@link LogicalProject}. */
  public static final JoinProjectTransposeRule JOIN_PROJECT_RIGHT_TRANSPOSE =
      JoinProjectTransposeRule.Config.RIGHT.toRule();

  /** As {@link #JOIN_PROJECT_BOTH_TRANSPOSE} but match outer as well as
   * inner join. */
  public static final JoinProjectTransposeRule
      JOIN_PROJECT_BOTH_TRANSPOSE_INCLUDE_OUTER =
      JoinProjectTransposeRule.Config.OUTER.toRule();

  /** As {@link #JOIN_PROJECT_LEFT_TRANSPOSE} but match outer as well as
   * inner join. */
  public static final JoinProjectTransposeRule
      JOIN_PROJECT_LEFT_TRANSPOSE_INCLUDE_OUTER =
      JoinProjectTransposeRule.Config.LEFT_OUTER.toRule();

  /** As {@link #JOIN_PROJECT_RIGHT_TRANSPOSE} but match outer as well as
   * inner join. */
  public static final JoinProjectTransposeRule
      JOIN_PROJECT_RIGHT_TRANSPOSE_INCLUDE_OUTER =
      JoinProjectTransposeRule.Config.RIGHT_OUTER.toRule();

  /** Rule that matches a {@link Join} and pushes down expressions on either
   * side of "equal" conditions. */
  public static final JoinPushExpressionsRule JOIN_PUSH_EXPRESSIONS =
      JoinPushExpressionsRule.Config.DEFAULT.toRule();

  /** Rule that infers predicates from on a {@link Join} and creates
   * {@link Filter}s if those predicates can be pushed to its inputs. */
  public static final JoinPushTransitivePredicatesRule
      JOIN_PUSH_TRANSITIVE_PREDICATES =
      JoinPushTransitivePredicatesRule.Config.DEFAULT.toRule();

  /** Rule that derives IS NOT NULL predicates from a inner {@link Join} and creates
   * {@link Filter}s with those predicates as new inputs of the {@link Join}. */
  public static final JoinDeriveIsNotNullFilterRule JOIN_DERIVE_IS_NOT_NULL_FILTER_RULE =
      JoinDeriveIsNotNullFilterRule.Config.DEFAULT.toRule();

  /** Rule that reduces constants inside a {@link Join}.
   *
   * @see #FILTER_REDUCE_EXPRESSIONS
   * @see #PROJECT_REDUCE_EXPRESSIONS */
  public static final ReduceExpressionsRule.JoinReduceExpressionsRule
      JOIN_REDUCE_EXPRESSIONS =
      ReduceExpressionsRule.JoinReduceExpressionsRule.JoinReduceExpressionsRuleConfig
          .DEFAULT.toRule();

  /** Rule that creates a {@link Join#isSemiJoin semi-join} from a
   * {@link Join}, and it's right keys are unique in its right input.
   */
  public static final SemiJoinRule.JoinOnUniqueToSemiJoinRule JOIN_ON_UNIQUE_TO_SEMI_JOIN =
      SemiJoinRule.JoinOnUniqueToSemiJoinRule.JoinOnUniqueToSemiJoinRuleConfig.DEFAULT.toRule();

  /** Rule that converts a {@link LogicalJoin}
   * into a {@link LogicalCorrelate}. */
  public static final JoinToCorrelateRule JOIN_TO_CORRELATE =
      JoinToCorrelateRule.Config.DEFAULT.toRule();

  /** Rule that flattens a tree of {@link LogicalJoin}s
   * into a single {@link MultiJoin} with N inputs. */
  public static final JoinToMultiJoinRule JOIN_TO_MULTI_JOIN =
      JoinToMultiJoinRule.Config.DEFAULT.toRule();

  /** Rule that creates a {@link Join#isSemiJoin semi-join} from a
   * {@link Join} with an empty {@link Aggregate} as its right input.
   *
   * @see #PROJECT_TO_SEMI_JOIN */
  public static final SemiJoinRule.JoinToSemiJoinRule JOIN_TO_SEMI_JOIN =
      SemiJoinRule.JoinToSemiJoinRule.JoinToSemiJoinRuleConfig.DEFAULT.toRule();

  /** Rule that pushes a {@link Join}
   * past a non-distinct {@link Union} as its left input. */
  public static final JoinUnionTransposeRule JOIN_LEFT_UNION_TRANSPOSE =
      JoinUnionTransposeRule.Config.LEFT.toRule();

  /** Rule that pushes a {@link Join}
   * past a non-distinct {@link Union} as its right input. */
  public static final JoinUnionTransposeRule JOIN_RIGHT_UNION_TRANSPOSE =
      JoinUnionTransposeRule.Config.RIGHT.toRule();

  /** Rule that re-orders a {@link Join} using a heuristic planner.
   *
   * <p>It is triggered by the pattern
   * {@link LogicalProject} ({@link MultiJoin}).
   *
   * @see #JOIN_TO_MULTI_JOIN
   * @see #MULTI_JOIN_OPTIMIZE_BUSHY */
  public static final LoptOptimizeJoinRule MULTI_JOIN_OPTIMIZE =
      LoptOptimizeJoinRule.Config.DEFAULT.toRule();

  /** Rule that finds an approximately optimal ordering for join operators
   * using a heuristic algorithm and can handle bushy joins.
   *
   * <p>It is triggered by the pattern
   * {@link LogicalProject} ({@link MultiJoin}).
   *
   * @see #MULTI_JOIN_OPTIMIZE
   */
  public static final MultiJoinOptimizeBushyRule MULTI_JOIN_OPTIMIZE_BUSHY =
      MultiJoinOptimizeBushyRule.Config.DEFAULT.toRule();

  /** Rule that matches a {@link LogicalJoin} whose inputs are both a
   * {@link MultiJoin} with intervening {@link LogicalProject}s,
   * and pulls the Projects up above the Join. */
  public static final MultiJoinProjectTransposeRule MULTI_JOIN_BOTH_PROJECT =
      MultiJoinProjectTransposeRule.Config.BOTH_PROJECT.toRule();

  /** As {@link #MULTI_JOIN_BOTH_PROJECT} but only the left input has
   * an intervening Project. */
  public static final MultiJoinProjectTransposeRule MULTI_JOIN_LEFT_PROJECT =
      MultiJoinProjectTransposeRule.Config.LEFT_PROJECT.toRule();

  /** As {@link #MULTI_JOIN_BOTH_PROJECT} but only the right input has
   * an intervening Project. */
  public static final MultiJoinProjectTransposeRule MULTI_JOIN_RIGHT_PROJECT =
      MultiJoinProjectTransposeRule.Config.RIGHT_PROJECT.toRule();

  /** Rule that pushes a {@link Join#isSemiJoin semi-join} down in a tree past
   * a {@link Filter}.
   *
   * @see #SEMI_JOIN_PROJECT_TRANSPOSE
   * @see #SEMI_JOIN_JOIN_TRANSPOSE */
  public static final SemiJoinFilterTransposeRule SEMI_JOIN_FILTER_TRANSPOSE =
      SemiJoinFilterTransposeRule.Config.DEFAULT.toRule();

  /** Rule that pushes a {@link Join#isSemiJoin semi-join} down in a tree past
   * a {@link Project}.
   *
   * @see #SEMI_JOIN_FILTER_TRANSPOSE
   * @see #SEMI_JOIN_JOIN_TRANSPOSE */
  public static final SemiJoinProjectTransposeRule SEMI_JOIN_PROJECT_TRANSPOSE =
      SemiJoinProjectTransposeRule.Config.DEFAULT.toRule();

  /** Rule that pushes a {@link Join#isSemiJoin semi-join} down in a tree past a
   * {@link Join}.
   *
   * @see #SEMI_JOIN_FILTER_TRANSPOSE
   * @see #SEMI_JOIN_PROJECT_TRANSPOSE */
  public static final SemiJoinJoinTransposeRule SEMI_JOIN_JOIN_TRANSPOSE =
      SemiJoinJoinTransposeRule.Config.DEFAULT.toRule();

  /** Rule that removes a {@link Join#isSemiJoin semi-join} from a join tree. */
  public static final SemiJoinRemoveRule SEMI_JOIN_REMOVE =
      SemiJoinRemoveRule.Config.DEFAULT.toRule();

  /** Rule that pushes a {@link Sort} past a {@link Union}.
   *
   * <p>This rule instance is for a Union implementation that does not preserve
   * the ordering of its inputs. Thus, it makes no sense to match this rule
   * if the Sort does not have a limit, i.e., {@link Sort#fetch} is null.
   *
   * @see #SORT_UNION_TRANSPOSE_MATCH_NULL_FETCH */
  public static final SortUnionTransposeRule SORT_UNION_TRANSPOSE =
      SortUnionTransposeRule.Config.DEFAULT.toRule();

  /** As {@link #SORT_UNION_TRANSPOSE}, but for a Union implementation that
   * preserves the ordering of its inputs. It is still worth applying this rule
   * even if the Sort does not have a limit, for the merge of already sorted
   * inputs that the Union can do is usually cheap. */
  public static final SortUnionTransposeRule SORT_UNION_TRANSPOSE_MATCH_NULL_FETCH =
      SortUnionTransposeRule.Config.DEFAULT.withMatchNullFetch(true).toRule();

  /** Rule that copies a {@link Sort} past a {@link Join} without its limit and
   * offset. The original {@link Sort} is preserved but can potentially be
   * removed by {@link #SORT_REMOVE} if redundant. */
  public static final SortJoinCopyRule SORT_JOIN_COPY =
      SortJoinCopyRule.Config.DEFAULT.toRule();

  /** Rule that removes a {@link Sort} if its input is already sorted. */
  public static final SortRemoveRule SORT_REMOVE =
      SortRemoveRule.Config.DEFAULT.toRule();

  /** Rule that removes keys from a {@link Sort}
   * if those keys are known to be constant, or removes the entire Sort if all
   * keys are constant. */
  public static final SortRemoveConstantKeysRule SORT_REMOVE_CONSTANT_KEYS =
      SortRemoveConstantKeysRule.Config.DEFAULT.toRule();

  /** Rule that pushes a {@link Sort} past a {@link Join}. */
  public static final SortJoinTransposeRule SORT_JOIN_TRANSPOSE =
      SortJoinTransposeRule.Config.DEFAULT.toRule();

  /** Rule that pushes a {@link Sort} past a {@link Project}. */
  public static final SortProjectTransposeRule SORT_PROJECT_TRANSPOSE =
      SortProjectTransposeRule.Config.DEFAULT.toRule();

  /** Rule that flattens a {@link Union} on a {@code Union}
   * into a single {@code Union}. */
  public static final UnionMergeRule UNION_MERGE =
      UnionMergeRule.Config.DEFAULT.toRule();

  /** Rule that removes a {@link Union} if it has only one input.
   *
   * @see PruneEmptyRules#UNION_INSTANCE */
  public static final UnionEliminatorRule UNION_REMOVE =
      UnionEliminatorRule.Config.DEFAULT.toRule();

  /** Rule that pulls up constants through a Union operator. */
  public static final UnionPullUpConstantsRule UNION_PULL_UP_CONSTANTS =
       UnionPullUpConstantsRule.Config.DEFAULT.toRule();

  /** Rule that translates a distinct {@link Union}
   * (<code>all</code> = <code>false</code>)
   * into an {@link Aggregate} on top of a non-distinct {@link Union}
   * (<code>all</code> = <code>true</code>). */
  public static final UnionToDistinctRule UNION_TO_DISTINCT =
      UnionToDistinctRule.Config.DEFAULT.toRule();

  /** Rule that applies an {@link Aggregate} to a {@link Values} (currently just
   * an empty {@code Values}). */
  public static final AggregateValuesRule AGGREGATE_VALUES =
      AggregateValuesRule.Config.DEFAULT.toRule();

  /** Rule that merges a {@link Filter} onto an underlying
   * {@link org.apache.calcite.rel.logical.LogicalValues},
   * resulting in a {@code Values} with potentially fewer rows. */
  public static final ValuesReduceRule FILTER_VALUES_MERGE =
      ValuesReduceRule.Config.FILTER.toRule();

  /** Rule that merges a {@link Project} onto an underlying
   * {@link org.apache.calcite.rel.logical.LogicalValues},
   * resulting in a {@code Values} with different columns. */
  public static final ValuesReduceRule PROJECT_VALUES_MERGE =
      ValuesReduceRule.Config.PROJECT.toRule();

  /** Rule that merges a {@link Project}
   * on top of a {@link Filter} onto an underlying
   * {@link org.apache.calcite.rel.logical.LogicalValues},
   * resulting in a {@code Values} with different columns
   * and potentially fewer rows. */
  public static final ValuesReduceRule PROJECT_FILTER_VALUES_MERGE =
      ValuesReduceRule.Config.PROJECT_FILTER.toRule();

  /** Rule that reduces constants inside a {@link LogicalWindow}.
   *
   * @see #FILTER_REDUCE_EXPRESSIONS */
  public static final ReduceExpressionsRule.WindowReduceExpressionsRule
      WINDOW_REDUCE_EXPRESSIONS =
      ReduceExpressionsRule.WindowReduceExpressionsRule.WindowReduceExpressionsRuleConfig
          .DEFAULT.toRule();
}
