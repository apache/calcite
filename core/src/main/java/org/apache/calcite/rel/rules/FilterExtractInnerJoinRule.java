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

import org.apache.calcite.plan.JoinConditionTransferTrait;
import org.apache.calcite.plan.JoinConditionTransferTraitDef;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.tools.RelBuilder;

import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Collectors;

import static org.apache.calcite.sql.fun.SqlLibraryOperators.BETWEEN;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.NOT_BETWEEN;

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
@Value.Enclosing
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
    RelTraitSet filterTraitSet = filter.getTraitSet();

    if (!isCrossJoin(join, builder)) {
      return;
    }

    Stack<Triple<RelNode, Integer, JoinRelType>> stackForTableScanWithEndColumnIndex =
        new Stack<>();
    List<RexNode> allConditions = new ArrayList<>();
    populateStackWithEndIndexesForTables(join, stackForTableScanWithEndColumnIndex, allConditions);
    RexNode conditions = filter.getCondition();
    Set<CorrelationId> correlationIdSet =
        filter instanceof LogicalFilter ? filter.getVariablesSet() : null;
    if (isConditionComposedOfSingleCondition((RexCall) conditions)) {
      allConditions.add(filter.getCondition());
    } else {
      allConditions.addAll(((RexCall) conditions).getOperands());
    }

    final RelNode modifiedJoinClauseWithWhereClause =
        moveConditionsFromWhereClauseToJoinOnClause(allConditions,
            stackForTableScanWithEndColumnIndex, ((RexCall) conditions).op, builder,
        correlationIdSet, filterTraitSet);

    call.transformTo(modifiedJoinClauseWithWhereClause);
  }

  /** This method will return TRUE if it encounters at least one
   *   [INNER JOIN, LEFT JOIN, RIGHT JOIN]
   *   ON TRUE in RelNode.
   */
  private static boolean isCrossJoin(Join join, RelBuilder builder) {
    if ((join.getJoinType().equals(JoinRelType.INNER)
        || join.getJoinType().equals(JoinRelType.LEFT)
        || join.getJoinType().equals(JoinRelType.RIGHT))
        && builder.literal(true).equals(join.getCondition())) {
      return true;
    }
    if (((HepRelVertex) join.getLeft()).getCurrentRel() instanceof LogicalJoin) {
      return isCrossJoin((Join) ((HepRelVertex) join.getLeft()).getCurrentRel(), builder);
    }
    return false;
  }


  /** This method populates the stack, Stack< Triple< RelNode, Integer, JoinRelType > >, with
   * TableScan of a table along with its column's end index and JoinType.*/
  private void populateStackWithEndIndexesForTables(
      Join join, Stack<Triple<RelNode, Integer, JoinRelType>> stack, List<RexNode> joinConditions) {
    RelNode left = ((HepRelVertex) join.getLeft()).getCurrentRel();
    RelNode right = ((HepRelVertex) join.getRight()).getCurrentRel();
    int leftTableColumnSize = join.getLeft().getRowType().getFieldCount();
    int rightTableColumnSize = join.getRight().getRowType().getFieldCount();
    stack.push(
        new ImmutableTriple<>(right, leftTableColumnSize + rightTableColumnSize - 1,
        join.getJoinType()));
    if (!(join.getCondition() instanceof RexLiteral)) {
      RexNode conditions = join.getCondition();
      if (isConditionComposedOfSingleCondition((RexCall) conditions)) {
        joinConditions.add(conditions);
      } else {
        joinConditions.addAll(((RexCall) conditions).getOperands());
      }
    }
    if (left instanceof Join && !((Join) left).getJoinType().isOuterJoin()) {
      populateStackWithEndIndexesForTables((Join) left, stack, joinConditions);
    } else {
      stack.push(new ImmutableTriple<>(left, leftTableColumnSize - 1, join.getJoinType()));
    }
  }

  /** This method identifies Join Predicates from filter conditions and put them on Joins as
   * ON conditions.*/
  private RelNode moveConditionsFromWhereClauseToJoinOnClause(List<RexNode> allConditions,
      Stack<Triple<RelNode, Integer, JoinRelType>> stack, SqlOperator op, RelBuilder builder,
      Set<CorrelationId> correlationIdSet, RelTraitSet filterTraitSet) {
    RelNode left = stack.pop().getLeft();
    while (!stack.isEmpty()) {
      Triple<RelNode, Integer, JoinRelType> rightEntry = stack.pop();
      List<RexNode> joinConditions =
          getConditionsForEndIndex(allConditions, rightEntry.getMiddle(), filterTraitSet);

      RexNode joinPredicate = (op.getKind() == SqlKind.OR && !joinConditions.isEmpty())
          ? builder.or(joinConditions)
          : builder.and(joinConditions);

      allConditions.removeAll(joinConditions);
      left =
          LogicalJoin.create(left, rightEntry.getLeft(), ImmutableList.of(),
              joinPredicate, ImmutableSet.of(), rightEntry.getRight());
    }
    builder.push(left);
    RexNode remainingCondition = allConditions.isEmpty()
        ? builder.literal(true)
        : createFilterCondition(op, allConditions, builder);

    return builder
        .filter(correlationIdSet != null ? correlationIdSet : ImmutableSet.of(), remainingCondition)
        .build();
  }

  private RexNode createFilterCondition(
      SqlOperator operator, List<RexNode> remainingConditions, RelBuilder builder) {
    if (operator.kind == SqlKind.BETWEEN) {
      operator = operator.getName().equals("NOT BETWEEN") ? NOT_BETWEEN : BETWEEN;
      return builder.call(operator, remainingConditions);
    }
    return (operator.getKind() == SqlKind.OR)
        ? builder.or(remainingConditions) : builder.and(remainingConditions);
  }

  /** Gets all the conditions that are part of the current join.*/
  private List<RexNode> getConditionsForEndIndex(List<RexNode> conditions, int endIndex, RelTraitSet filterTraitSet) {
    return conditions.stream()
        .filter(
            condition ->
                !(condition instanceof RexInputRef)
                    && !(condition instanceof RexDynamicParam)
                    && ((RexCall) condition).operands.stream().noneMatch(
                      operand -> operand instanceof RexLiteral)
                    && isConditionPartOfCurrentJoin((RexCall) condition, endIndex, filterTraitSet)
        )
        .collect(Collectors.toList());
  }

  /** Helper function for isConditionPartOfCurrentJoin method.
   * Checks index of the given operand(column) if it's less than endIndex.
   * If an operand(column) is wrapped in a function, for example TRIM(col), CAST(col) etc.,
   * we call the method recursively.*/
  private boolean isOperandIndexLessThanEndIndex(RexNode operand, int endIndex, RelTraitSet filterTraitSet) {
    if (operand instanceof RexCall && !((RexCall) operand).operands.isEmpty()) {
      RexCall call = (RexCall) operand;
      boolean atLeastOneMatches = false;
      for (RexNode op : call.operands) {
        if (op instanceof RexLiteral) {
          JoinConditionTransferTrait joinConditionTransferTrait =
              filterTraitSet.getTrait(JoinConditionTransferTraitDef.instance);
          if (joinConditionTransferTrait != null && joinConditionTransferTrait.isJoinConditionMovedToFilter()) {
            atLeastOneMatches = true;
          }
          continue;
        }
        if (!isOperandIndexLessThanEndIndex(op, endIndex, filterTraitSet)) {
          return false;
        }
        atLeastOneMatches = true;
      }
      return atLeastOneMatches;
    }
    if (operand.getClass().equals(RexInputRef.class)) {
      return ((RexInputRef) operand).getIndex() <= endIndex;
    }
    return false;
  }

  /** Checks whether the given condition is part of the current join by matching the column
   * reference with endIndex of the table on which the join is being performed.*/
  private boolean isConditionPartOfCurrentJoin(RexCall condition, int endIndex,
      RelTraitSet filterTraitSet) {
    if (condition instanceof RexSubQuery) {
      return false;
    }
    return condition.operands.stream().allMatch(operand ->
        isOperandIndexLessThanEndIndex(operand, endIndex, filterTraitSet));
  }

  /** Checks whether a given condition is composed of a single condition.
   * Eg.
   * 1. In case of, =($7, $12), it will return true.
   * 2. In case of, =(lower($7), LOWER($12)), it will return true.
   * 3. In case of, =(CONCAT($1, $2), CONCAT($4, $7), it will return true.
   * 4. In case of, =(lower(TRIM($7)), LOWER(TRIM($12))), it will return true.
   * 5. In case of AND(=($7, $9), =($14, $19), it will return false.*/
  private boolean isConditionComposedOfSingleCondition(RexCall conditions) {
    return conditions.getOperands().size() <= 2
        && conditions.getOperands().stream().allMatch(
            operand -> operand instanceof RexInputRef
                || operand instanceof RexLiteral
                || operand instanceof RexDynamicParam
                || (operand instanceof RexCall
                && conditions.op.kind != SqlKind.AND && conditions.op.kind != SqlKind.OR));
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableFilterExtractInnerJoinRule.Config.of()
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
                  .predicate(join -> join.getJoinType() == JoinRelType.INNER
                      || join.getJoinType() == JoinRelType.LEFT
                      || join.getJoinType() == JoinRelType.RIGHT)
                  .anyInputs())).as(FilterExtractInnerJoinRule.Config.class);
    }
  }
}
