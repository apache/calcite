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
import org.apache.calcite.util.Pair;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;
import java.util.stream.Collectors;

/**
 * Planner rule that matches an {@link org.apache.calcite.rel.core.Filter}
 * on a {@link org.apache.calcite.rel.core.Join} and removes the join
 * predicates from the filter conditions and put them on Join if possible.
 *
 * <p>For instance,
 *
 * <blockquote>
 * <pre>select e.employee_id, e.name
 * from employee as e, department as d
 * where e.department_id = d.department_id and e.salary = 500000</pre></blockquote>
 *
 * <p>becomes
 *
 * <blockquote>
 * <pre>select e.employee_id, e.name
 * from employee as e
 * INNER JOIN department as d
 * ON e.department_id = d.department_id
 * WHERE e.salary = 500000</pre></blockquote>
 *
 * @see CoreRules#FILTER_EXTRACT_INNER_JOIN_RULE
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

    Stack<Pair<RelNode, Integer>> stackForTableScanWithEndColumnIndex = new Stack<>();
    List<RexNode> allConditions = new ArrayList<>();
    populateStackWithEndIndexesForTables(join, stackForTableScanWithEndColumnIndex, allConditions);
    RexNode conditions = filter.getCondition();
    if (isConditionComposedOfMultipleConditions((RexCall) conditions)) {
      allConditions.addAll(((RexCall) conditions).getOperands());
    } else {
      allConditions.add(filter.getCondition());
    }

    final RelNode modifiedJoinClauseWithWhereClause =
        moveConditionsFromWhereClauseToJoinOnClause(
            allConditions, stackForTableScanWithEndColumnIndex, builder);

    call.transformTo(modifiedJoinClauseWithWhereClause);
  }

  private static boolean isCrossJoin(Join join, RelBuilder builder) {
    return join.getJoinType().equals(JoinRelType.INNER)
        && builder.literal(true).equals(join.getCondition());
  }

  /** This method checks whether filter conditions have both AND & OR in it.*/
  private static boolean isFilterWithCompositeLogicalConditions(RexNode condition) {
    RexCall cond = (RexCall) condition;
    if (cond.op.kind == SqlKind.OR) {
      return true;
    }
    if (cond.operands.stream().allMatch(operand -> operand instanceof RexCall)) {
      return cond.operands.stream().anyMatch(
          FilterExtractInnerJoinRule::isFilterWithCompositeLogicalConditions
      );
    }
    return false;
  }

  /** This method populates the stack, Stack< RelNode, Integer >, with
   * TableScan of a table along with its columns end index.*/
  private void populateStackWithEndIndexesForTables(
      Join join, Stack<Pair<RelNode, Integer>> stack, List<RexNode> joinConditions) {
    RelNode left = ((HepRelVertex) join.getLeft()).getCurrentRel();
    RelNode right = ((HepRelVertex) join.getRight()).getCurrentRel();
    int leftTableColumnSize = join.getLeft().getRowType().getFieldCount();
    int rightTableColumnSize = join.getRight().getRowType().getFieldCount();
    stack.push(new Pair<>(right, leftTableColumnSize + rightTableColumnSize - 1));
    if (!(join.getCondition() instanceof RexLiteral)) {
      joinConditions.add(join.getCondition());
    }
    if (left instanceof Join) {
      populateStackWithEndIndexesForTables((Join) left, stack, joinConditions);
    } else {
      stack.push(new Pair<>(left, leftTableColumnSize - 1));
    }
  }

  /** This method identifies Join Predicates from filter conditions and put them on Joins as
   * ON conditions.*/
  private RelNode moveConditionsFromWhereClauseToJoinOnClause(
      List<RexNode> allConditions, Stack<Pair<RelNode, Integer>> stack, RelBuilder builder) {
    Pair<RelNode, Integer> leftEntry = stack.pop();
    Pair<RelNode, Integer> rightEntry;
    RelNode left = leftEntry.getKey();

    while (!stack.isEmpty()) {
      rightEntry = stack.pop();
      List<RexNode> joinConditions = getConditionsForEndIndex(allConditions, rightEntry.right);
      RexNode joinPredicate = builder.and(joinConditions);
      allConditions.removeAll(joinConditions);
      left = LogicalJoin.create(left, rightEntry.left, ImmutableList.of(),
          joinPredicate, ImmutableSet.of(), JoinRelType.INNER);
    }
    return builder.push(left)
        .filter(builder.and(allConditions))
        .build();
  }

  /** Gets all the conditions that are part of the current join.*/
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

  /** Helper function for isConditionPartOfCurrentJoin method.
   * Checks index of the given operand(column) if it's less than endIndex.
   * If an operand(column) is wrapped in a function, for example TRIM(col), CAST(col) etc.,
   * we call the method recursively.*/
  private boolean isOperandIndexLessThanEndIndex(RexNode operand, int endIndex) {
    if (operand instanceof RexCall) {
      return isOperandIndexLessThanEndIndex(((RexCall) operand).operands.get(0), endIndex);
    }
    if (operand instanceof RexInputRef) {
      return ((RexInputRef) operand).getIndex() <= endIndex;
    }
    return false;
  }

  /** Checks whether the given condition is part of the current join by matching the column
   * reference with endIndex of the table on which the join is being performed.*/
  private boolean isConditionPartOfCurrentJoin(RexCall condition, int endIndex) {
    if (condition instanceof RexSubQuery) {
      return false;
    }
    return isOperandIndexLessThanEndIndex(condition.operands.get(0), endIndex)
        && isOperandIndexLessThanEndIndex(condition.operands.get(1), endIndex);
  }

  private boolean isConditionComposedOfMultipleConditions(RexCall conditions) {
    return conditions.getOperands().stream().allMatch(operand ->
        operand instanceof RexCall && ((RexCall) operand).operands.size() > 1);
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
