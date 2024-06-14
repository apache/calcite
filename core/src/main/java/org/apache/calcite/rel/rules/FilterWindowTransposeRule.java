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

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableList;

import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.List;

/**
 * Planner rule that pushes a {@link org.apache.calcite.rel.core.Filter}
 * past a {@link org.apache.calcite.rel.core.Window}.
 *
 * <p> If {@code Filter} condition used columns belongs {@code Window} partition keys,
 * then we could push the condition past the {@code Window}.
 *
 * <p> For example:
 *  <blockquote><pre>{@code
 *    LogicalProject(NAME=[$0], DEPTNO=[$1], EXPR$2=[$2])
 *     LogicalFilter(condition=[>($1, 0)])
 *       LogicalProject(NAME=[$1], DEPTNO=[$0], EXPR$2=[$2])
 *         LogicalWindow(window#0=[window(partition {0} aggs [COUNT()])])
 *           LogicalTableScan(table=[[CATALOG, SALES, DEPT]])
 *  }</pre></blockquote>
 *
 * <p> will convert to:
 * <blockquote><pre>{@code
 *    LogicalProject(NAME=[$1], DEPTNO=[$0], EXPR$2=[$2])
 *      LogicalWindow(window#0=[window(partition {0} aggs [COUNT()])])
 *        LogicalFilter(condition=[>($0, 0)])
 *          LogicalTableScan(table=[[CATALOG, SALES, DEPT]])
 * }</pre></blockquote>
 *
 * @see CoreRules#FILTER_PROJECT_TRANSPOSE
 * @see CoreRules#PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW
 * @see CoreRules#FILTER_WINDOW_TRANSPOSE
 */
@Value.Enclosing
public class FilterWindowTransposeRule
    extends RelRule<FilterWindowTransposeRule.Config>
    implements TransformationRule {

  protected FilterWindowTransposeRule(final Config config) {
    super(config);
  }

  @Override public void onMatch(final RelOptRuleCall call) {
    final Filter filterRel = call.rel(0);
    final Window windowRel = call.rel(1);

    // Get the window all groups
    List<Window.Group> windowGroups = windowRel.groups;

    // The window may have multi groups,now we could only
    // deal one group case,so that we could know the partition keys.
    if (windowGroups.size() != 1) {
      return;
    }

    final List<RexNode> conditions =
        RelOptUtil.conjunctions(filterRel.getCondition());
    // The conditions which could be pushed to past window
    final List<RexNode> pushedConditions = new ArrayList<>();
    final List<RexNode> remainingConditions = new ArrayList<>();
    final Window.Group group = windowGroups.get(0);
    // Get the window partition keys
    final ImmutableBitSet partitionKeys = group.keys;

    for (RexNode condition : conditions) {
      // Find the condition used columns
      ImmutableBitSet rCols = RelOptUtil.InputFinder.bits(condition);
      // If the window partition columns contains the condition used columns,
      // then we could push the condition to past window.
      if (partitionKeys.contains(rCols)) {
        pushedConditions.add(condition);
      } else {
        remainingConditions.add(condition);
      }
    }

    final RelBuilder builder = call.builder();
    // Use the pushed conditions to create a new filter above the window's input.
    RelNode rel = builder.push(windowRel.getInput()).filter(pushedConditions).build();
    if (rel == windowRel.getInput(0)) {
      return;
    }
    rel = windowRel.copy(windowRel.getTraitSet(), ImmutableList.of(rel));
    rel = builder.push(rel).filter(remainingConditions).build();
    call.transformTo(rel);
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {

    Config DEFAULT = ImmutableFilterWindowTransposeRule.Config.of()
        .withOperandSupplier(b0 ->
            b0.operand(Filter.class).oneInput(b1 ->
                b1.operand(Window.class).anyInputs()));

    @Override default FilterWindowTransposeRule toRule() {
      return new FilterWindowTransposeRule(this);
    }
  }
}
