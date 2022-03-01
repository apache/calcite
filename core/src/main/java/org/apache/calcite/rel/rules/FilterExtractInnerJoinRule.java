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
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilder;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.stream.Collectors;

/**
 * Planner rule that matches an {@link org.apache.calcite.rel.core.Aggregate}
 * on a {@link org.apache.calcite.rel.core.Join} and removes the left input
 * of the join provided that the left input is also a left join if possible.
 *
 * <p>For instance,
 *
 * <blockquote>
 * <pre>select distinct s.product_id, pc.product_id
 * from sales as s
 * left join product as p
 *   on s.product_id = p.product_id
 * left join product_class pc
 *   on s.product_id = pc.product_id</pre></blockquote>
 *
 * <p>becomes
 *
 * <blockquote>
 * <pre>select distinct s.product_id, pc.product_id
 * from sales as s
 * left join product_class pc
 *   on s.product_id = pc.product_id</pre></blockquote>
 *
 * @see CoreRules#AGGREGATE_JOIN_JOIN_REMOVE
 */
public class FilterExtractInnerJoinRule
    extends RelRule<FilterExtractInnerJoinRule.Config>
    implements TransformationRule {

  protected FilterExtractInnerJoinRule(Config config) {
    super(config);
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final Filter filter = call.rel(0);
    final Join join = call.rel(1);
    RelBuilder builder = call.builder();

    if (!isCrossJoin(join, builder)
        || isFilterWithCompositeLogicalConditions(filter.getCondition())) {
      return;
    }

    Stack<Map.Entry<RelNode, Integer>> stackForTablesWithEndIndexes = new Stack<>();
    List<RexNode> allConditions = new ArrayList<>();
    populateStackWithEndIndexesForTables(join, stackForTablesWithEndIndexes, allConditions);
    if (((RexCall) filter.getCondition()).getOperands().stream().allMatch(operand ->
        operand instanceof RexCall && ((RexCall) operand).operands.size() > 1)) {
      allConditions.addAll(((RexCall) filter.getCondition()).getOperands());
    } else {
      allConditions.add(filter.getCondition());
    }

    final RelNode modifiedJoinClauseWithWhereClause =
        moveConditionsFromWhereClauseToJoinOnClause(
            allConditions, stackForTablesWithEndIndexes, builder);

    call.transformTo(modifiedJoinClauseWithWhereClause);
  }

  private static boolean isCrossJoin(Join join, RelBuilder builder) {
    return join.getJoinType().equals(JoinRelType.INNER)
        && builder.literal(true).equals(join.getCondition());
  }

  private static boolean isFilterWithCompositeLogicalConditions(RexNode condition) {
    if (((RexCall) condition).op.kind == SqlKind.OR) {
      return true;
    }
    if (((RexCall) condition).operands.stream().allMatch(operand -> operand instanceof RexCall)) {
      return isFilterWithCompositeLogicalConditions(((RexCall) condition).operands.get(0))
          || isFilterWithCompositeLogicalConditions(((RexCall) condition).operands.get(1));
    }
    return false;
  }

  private void populateStackWithEndIndexesForTables(
      Join join, Stack<Map.Entry<RelNode, Integer>> stack, List<RexNode> joinConditions) {
    RelNode left = ((HepRelVertex) join.getLeft()).getCurrentRel();
    RelNode right = ((HepRelVertex) join.getRight()).getCurrentRel();
    int leftTableColumnSize = join.getLeft().getRowType().getFieldCount();
    int rightTableColumnSize = join.getRight().getRowType().getFieldCount();
    stack.push(Map.entry(right, leftTableColumnSize + rightTableColumnSize - 1));
    if (!(join.getCondition() instanceof RexLiteral)) {
      joinConditions.add(join.getCondition());
    }
    if (left instanceof Join) {
      populateStackWithEndIndexesForTables((Join) left, stack, joinConditions);
    } else {
      stack.push(Map.entry(left, leftTableColumnSize - 1));
    }
  }

  private RelNode moveConditionsFromWhereClauseToJoinOnClause(
      List<RexNode> allConditions, Stack<Map.Entry<RelNode, Integer>> stack, RelBuilder builder) {
    Map.Entry<RelNode, Integer> leftEntry = stack.pop();
    Map.Entry<RelNode, Integer> rightEntry;
    RelNode left = leftEntry.getKey();

    while (!stack.isEmpty()) {
      rightEntry = stack.pop();
      List<RexNode> joinConditions = getConditionsForEndIndex(allConditions, rightEntry.getValue());
      RexNode joinPredicate = builder.and(joinConditions);
      allConditions.removeAll(joinConditions);
      left = LogicalJoin.create(left, rightEntry.getKey(), ImmutableList.of(),
          joinPredicate, ImmutableSet.of(), JoinRelType.INNER);
    }
    return builder.push(left)
        .filter(builder.and(allConditions))
        .build();
  }

  private List<RexNode> getConditionsForEndIndex(List<RexNode> conditions, int endIndex) {
    return conditions.stream()
        .filter(
            condition ->
                ((RexCall) condition).operands.stream().noneMatch(
                    operand -> operand instanceof RexLiteral)
                    && isConditionPartOfCurrentJoin((RexCall) condition, endIndex)
        )
        .collect(Collectors.toList());
  }

  private boolean isOperandIndexLessThanEndIndex(RexNode operand, int endIndex) {
    if (operand instanceof RexCall) {
      return isOperandIndexLessThanEndIndex(((RexCall) operand).operands.get(0), endIndex);
    }
    if (operand instanceof RexInputRef) {
      return ((RexInputRef) operand).getIndex() <= endIndex;
    }
    return false;
  }

  private boolean isConditionPartOfCurrentJoin(RexCall condition, int endIndex) {
    if (condition instanceof RexSubQuery) {
      return false;
    }
    return isOperandIndexLessThanEndIndex(condition.operands.get(0), endIndex)
        && isOperandIndexLessThanEndIndex(condition.operands.get(1), endIndex);
  }

  /** Rule configuration. */
  public interface Config extends RelRule.Config {
    FilterExtractInnerJoinRule.Config DEFAULT = EMPTY
        .as(Config.class)
        .withOperandFor(Filter.class, Join.class);

    @Override default FilterExtractInnerJoinRule toRule() {
      return new FilterExtractInnerJoinRule(this);
    }

    /** Defines an operand tree for the given classes. */
    default FilterExtractInnerJoinRule.Config withOperandFor(Class<? extends Filter> filterClass,
        Class<? extends Join> joinClass) {
      return withOperandSupplier(b0 ->
          b0.operand(filterClass).inputs(b1 ->
              b1.operand(joinClass)
                  .predicate(join -> join.getJoinType() == JoinRelType.INNER)
                  .anyInputs())).as(FilterExtractInnerJoinRule.Config.class);
    }
  }
}
