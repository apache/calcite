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
package org.apache.calcite.plan;

import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.interpreter.Bindables;
import org.apache.calcite.linq4j.function.Experimental;
import org.apache.calcite.plan.volcano.AbstractConverter;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.DateRangeRules;
import org.apache.calcite.rel.rules.JoinPushThroughJoinRule;
import org.apache.calcite.rel.rules.PruneEmptyRules;
import org.apache.calcite.rel.rules.materialize.MaterializedViewRules;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * A utility class for organizing built-in rules and rule related
 * methods. Currently some rule sets are package private for serving core Calcite.
 *
 * @see RelOptRule
 * @see RelOptUtil
 */
@Experimental
public class RelOptRules {

  private RelOptRules() {
  }

  /** Calc rule set; public so that {@link org.apache.calcite.tools.Programs} can
   * use it. */
  public static final ImmutableList<RelOptRule> CALC_RULES =
      ImmutableList.of(
          Bindables.FROM_NONE_RULE,
          EnumerableRules.ENUMERABLE_CALC_RULE,
          EnumerableRules.ENUMERABLE_FILTER_TO_CALC_RULE,
          EnumerableRules.ENUMERABLE_PROJECT_TO_CALC_RULE,
          CoreRules.FILTER_TO_CALC,
          CoreRules.PROJECT_TO_CALC,
          CoreRules.CALC_MERGE,

          // REVIEW jvs 9-Apr-2006: Do we still need these two?  Doesn't the
          // combination of CalcMergeRule, FilterToCalcRule, and
          // ProjectToCalcRule have the same effect?
          CoreRules.FILTER_CALC_MERGE,
          CoreRules.PROJECT_CALC_MERGE);

  static final List<RelOptRule> BASE_RULES = ImmutableList.of(
      CoreRules.AGGREGATE_STAR_TABLE,
      CoreRules.AGGREGATE_PROJECT_STAR_TABLE,
      CalciteSystemProperty.COMMUTE.value()
          ? CoreRules.JOIN_ASSOCIATE
          : CoreRules.PROJECT_MERGE,
      CoreRules.FILTER_SCAN,
      CoreRules.PROJECT_FILTER_TRANSPOSE,
      CoreRules.FILTER_PROJECT_TRANSPOSE,
      CoreRules.FILTER_INTO_JOIN,
      CoreRules.JOIN_PUSH_EXPRESSIONS,
      CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES,
      CoreRules.AGGREGATE_EXPAND_WITHIN_DISTINCT,
      CoreRules.AGGREGATE_CASE_TO_FILTER,
      CoreRules.AGGREGATE_REDUCE_FUNCTIONS,
      CoreRules.FILTER_AGGREGATE_TRANSPOSE,
      CoreRules.PROJECT_WINDOW_TRANSPOSE,
      CoreRules.MATCH,
      CoreRules.JOIN_COMMUTE,
      JoinPushThroughJoinRule.RIGHT,
      JoinPushThroughJoinRule.LEFT,
      CoreRules.SORT_PROJECT_TRANSPOSE,
      CoreRules.SORT_JOIN_TRANSPOSE,
      CoreRules.SORT_REMOVE_CONSTANT_KEYS,
      CoreRules.SORT_UNION_TRANSPOSE,
      CoreRules.EXCHANGE_REMOVE_CONSTANT_KEYS,
      CoreRules.SORT_EXCHANGE_REMOVE_CONSTANT_KEYS);

  static final List<RelOptRule> ABSTRACT_RULES = ImmutableList.of(
      CoreRules.AGGREGATE_ANY_PULL_UP_CONSTANTS,
      CoreRules.UNION_PULL_UP_CONSTANTS,
      PruneEmptyRules.UNION_INSTANCE,
      PruneEmptyRules.INTERSECT_INSTANCE,
      PruneEmptyRules.MINUS_INSTANCE,
      PruneEmptyRules.PROJECT_INSTANCE,
      PruneEmptyRules.FILTER_INSTANCE,
      PruneEmptyRules.SORT_INSTANCE,
      PruneEmptyRules.AGGREGATE_INSTANCE,
      PruneEmptyRules.JOIN_LEFT_INSTANCE,
      PruneEmptyRules.JOIN_RIGHT_INSTANCE,
      PruneEmptyRules.SORT_FETCH_ZERO_INSTANCE,
      CoreRules.UNION_MERGE,
      CoreRules.INTERSECT_MERGE,
      CoreRules.MINUS_MERGE,
      CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW,
      CoreRules.FILTER_MERGE,
      DateRangeRules.FILTER_INSTANCE,
      CoreRules.INTERSECT_TO_DISTINCT);

  static final List<RelOptRule> ABSTRACT_RELATIONAL_RULES = ImmutableList.of(
      CoreRules.FILTER_INTO_JOIN,
      CoreRules.JOIN_CONDITION_PUSH,
      AbstractConverter.ExpandConversionRule.INSTANCE,
      CoreRules.JOIN_COMMUTE,
      CoreRules.PROJECT_TO_SEMI_JOIN,
      CoreRules.JOIN_ON_UNIQUE_TO_SEMI_JOIN,
      CoreRules.JOIN_TO_SEMI_JOIN,
      CoreRules.AGGREGATE_REMOVE,
      CoreRules.UNION_TO_DISTINCT,
      CoreRules.PROJECT_REMOVE,
      CoreRules.PROJECT_AGGREGATE_MERGE,
      CoreRules.AGGREGATE_JOIN_TRANSPOSE,
      CoreRules.AGGREGATE_MERGE,
      CoreRules.AGGREGATE_PROJECT_MERGE,
      CoreRules.CALC_REMOVE,
      CoreRules.SORT_REMOVE);

  static final List<RelOptRule> CONSTANT_REDUCTION_RULES = ImmutableList.of(
      CoreRules.PROJECT_REDUCE_EXPRESSIONS,
      CoreRules.FILTER_REDUCE_EXPRESSIONS,
      CoreRules.CALC_REDUCE_EXPRESSIONS,
      CoreRules.WINDOW_REDUCE_EXPRESSIONS,
      CoreRules.JOIN_REDUCE_EXPRESSIONS,
      CoreRules.FILTER_VALUES_MERGE,
      CoreRules.PROJECT_FILTER_VALUES_MERGE,
      CoreRules.PROJECT_VALUES_MERGE,
      CoreRules.AGGREGATE_VALUES);

  public static final List<RelOptRule> MATERIALIZATION_RULES = ImmutableList.of(
      MaterializedViewRules.FILTER_SCAN,
      MaterializedViewRules.PROJECT_FILTER,
      MaterializedViewRules.FILTER,
      MaterializedViewRules.PROJECT_JOIN,
      MaterializedViewRules.JOIN,
      MaterializedViewRules.PROJECT_AGGREGATE,
      MaterializedViewRules.AGGREGATE);
}
