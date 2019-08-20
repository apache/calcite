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

package org.apache.calcite.piglet;

import org.apache.calcite.adapter.enumerable.EnumerableInterpreterRule;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.FilterMergeRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.ProjectToWindowRule;
import org.apache.calcite.rel.rules.ProjectWindowTransposeRule;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

import com.google.common.collect.ImmutableList;

import java.util.List;


/**
 * Extension of {@link VolcanoPlanner} to optimize Pig-translated logical plans
 */
public class PigRelPlanner extends VolcanoPlanner {
  // Basic transformation and implementation rules to optimize for Pig-translated logical plans
  static final List<RelOptRule> PIG_RULES =
      ImmutableList.of(
          ProjectToWindowRule.PROJECT,
          PigToSqlAggregateRule.INSTANCE,
          EnumerableRules.ENUMERABLE_VALUES_RULE,
          EnumerableRules.ENUMERABLE_JOIN_RULE,
          EnumerableRules.ENUMERABLE_CORRELATE_RULE,
          EnumerableRules.ENUMERABLE_PROJECT_RULE,
          EnumerableRules.ENUMERABLE_FILTER_RULE,
          EnumerableRules.ENUMERABLE_AGGREGATE_RULE,
          EnumerableRules.ENUMERABLE_SORT_RULE,
          EnumerableRules.ENUMERABLE_LIMIT_RULE,
          EnumerableRules.ENUMERABLE_COLLECT_RULE,
          EnumerableRules.ENUMERABLE_UNCOLLECT_RULE,
          EnumerableRules.ENUMERABLE_UNION_RULE,
          EnumerableRules.ENUMERABLE_WINDOW_RULE,
          EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE,
          EnumerableInterpreterRule.INSTANCE);

  static final List<RelOptRule> TRANSFORM_RULES =
      ImmutableList.of(
          ProjectWindowTransposeRule.INSTANCE,
          FilterMergeRule.INSTANCE,
          ProjectMergeRule.INSTANCE,
          FilterProjectTransposeRule.INSTANCE,
          EnumerableRules.ENUMERABLE_VALUES_RULE,
          EnumerableRules.ENUMERABLE_JOIN_RULE,
          EnumerableRules.ENUMERABLE_CORRELATE_RULE,
          EnumerableRules.ENUMERABLE_PROJECT_RULE,
          EnumerableRules.ENUMERABLE_FILTER_RULE,
          EnumerableRules.ENUMERABLE_AGGREGATE_RULE,
          EnumerableRules.ENUMERABLE_SORT_RULE,
          EnumerableRules.ENUMERABLE_LIMIT_RULE,
          EnumerableRules.ENUMERABLE_COLLECT_RULE,
          EnumerableRules.ENUMERABLE_UNCOLLECT_RULE,
          EnumerableRules.ENUMERABLE_UNION_RULE,
          EnumerableRules.ENUMERABLE_WINDOW_RULE,
          EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE,
          EnumerableInterpreterRule.INSTANCE);

  private PigRelPlanner(RelOptCostFactory costFactory, Context externalContext) {
    super(costFactory, externalContext);
  }

  /**
   * Creates a {@link PigRelPlanner} from a {@link RelOptPlanner} template.
   *
   * @param template RelOptPlanner template
   * @param rules RelOptRule rules to use
   */
  public static PigRelPlanner createPlanner(RelOptPlanner template, List<RelOptRule> rules) {
    PigRelPlanner planner = new PigRelPlanner(template.getCostFactory(), template.getContext());
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
    for (RelOptRule rule : rules) {
      planner.addRule(rule);
    }
    return planner;
  }

  /**
   * Creates a {@link PigRelPlanner} from a {@link RelOptPlanner} template.
   *
   * @param template RelOptPlanner template
   */
  public static PigRelPlanner createPlanner(RelOptPlanner template) {
    return createPlanner(template, PIG_RULES);
  }

  @Override public RelOptCost getCost(RelNode rel, RelMetadataQuery mq) {
    RelOptCost cost = super.getCost(rel, mq);
    if (rel instanceof Aggregate) {
      final Aggregate agg = (Aggregate) rel;
      if (agg.getAggCallList().size() == 1) {
        AggregateCall aggCall = agg.getAggCallList().get(0);
        // Make Pig aggregates 10 times more expensive to have the @PigToSqlAggregateRule applied.
        if (aggCall.getAggregation().getName().equals("COLLECT")) {
          return costFactory.makeCost(10 * cost.getRows(), 10 * cost.getCpu(), 10 * cost.getIo());
        }
      }
    }
    if (rel instanceof Project) {
      final Project proj = (Project) rel;
      for (RexNode rexNode : proj.getProjects()) {
        if (rexNode.getKind() == SqlKind.ROW) {
          // Penalize row with more operands so PigToSqlAggregateRule will be applied instead of
          // multiset projection
          final int operandCnt = ((RexCall) rexNode).operands.size();
          return costFactory.makeCost(operandCnt * cost.getRows(), operandCnt * cost.getCpu(),
              operandCnt * cost.getIo());
        }
      }
    }
    return cost;
  }
}

// End PigRelPlanner.java
