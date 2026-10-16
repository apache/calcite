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
import org.apache.calcite.plan.Strong;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;

import org.immutables.value.Value;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * Planner rule that converts an outer join followed by {@code IS NULL}
 * on its null-generating side to an anti join.
 *
 * <p>For example, the query
 *
 * <pre>{@code
 * SELECT e.empno, d.name
 * FROM Emp AS e
 * LEFT JOIN Dept AS d ON e.deptno = d.deptno
 * WHERE d.deptno IS NULL AND e.empno > 10
 * }</pre>
 *
 * <p>has the following plan:
 *
 * <pre>{@code
 * LogicalProject(EMPNO=[$0], NAME=[$10])
 *   LogicalFilter(condition=[AND(IS NULL($9), >($0, 10))])
 *     LogicalJoin(condition=[=($7, $9)], joinType=[left])
 *       LogicalTableScan(table=[[CATALOG, SALES, EMP]])
 *       LogicalTableScan(table=[[CATALOG, SALES, DEPT]])
 * }</pre>
 *
 * <p>The rule converts it to:
 *
 * <pre>{@code
 * LogicalProject(EMPNO=[$0], NAME=[$10])
 *   LogicalFilter(condition=[>($0, 10)])
 *     LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3],
 *         HIREDATE=[$4], SAL=[$5], COMM=[$6], DEPTNO=[$7],
 *         SLACKER=[$8], DEPTNO0=[null:INTEGER], NAME=[null:VARCHAR(10)])
 *       LogicalJoin(condition=[=($7, $9)], joinType=[anti])
 *         LogicalTableScan(table=[[CATALOG, SALES, EMP]])
 *         LogicalTableScan(table=[[CATALOG, SALES, DEPT]])
 * }</pre>
 *
 * <p>The {@code IS NULL} predicate must be a top-level conjunct over a field
 * from the null-generating input. A field that is non-nullable in that input
 * is safe. For a nullable field, the join condition must not be TRUE when its
 * value is NULL.
 */
@Value.Enclosing
public class OuterJoinToAntiJoinRule
    extends RelRule<OuterJoinToAntiJoinRule.Config>
    implements TransformationRule {

  /** Creates an OuterJoinToAntiJoinRule. */
  protected OuterJoinToAntiJoinRule(Config config) {
    super(config);
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final Filter filter = call.rel(0);
    final Join join = call.rel(1);

    // Field indexes below assume that the join has no system-field prefix.
    if (!join.getSystemFieldList().isEmpty()) {
      return;
    }
    // Rewriting may change the number and order of condition evaluations.
    if (!RexUtil.isDeterministic(filter.getCondition())
        || !RexUtil.isDeterministic(join.getCondition())) {
      return;
    }

    final boolean leftJoin = join.getJoinType() == JoinRelType.LEFT;
    // Correlated RIGHT joins are not supported because converting them requires
    // swapping the inputs and remapping correlation references.
    if (!leftJoin && !join.getVariablesSet().isEmpty()) {
      return;
    }
    // Only top-level conjuncts can independently prove that a row is unmatched.
    final List<RexNode> remainingConditions =
        new ArrayList<>(RelOptUtil.conjunctions(filter.getCondition()));
    final RexNode nullCondition =
        findSafeNullCondition(remainingConditions, join, leftJoin);
    if (nullCondition == null) {
      return;
    }
    remainingConditions.remove(nullCondition);

    final RelNode newLeft = leftJoin ? join.getLeft() : join.getRight();
    final RelNode newRight = leftJoin ? join.getRight() : join.getLeft();
    final RexNode condition = leftJoin
        ? join.getCondition()
        : JoinCommuteRule.swapJoinCond(join.getCondition(), join,
            join.getCluster().getRexBuilder());
    final RelBuilder builder = call.builder()
        .push(newLeft)
        .push(newRight)
        .join(JoinRelType.ANTI, condition, join.getVariablesSet())
        .hints(join.getHints());

    // An anti join projects only its left input. Its rows are unmatched, so every
    // field of the null-generating input is NULL. Reinsert typed NULLs to restore
    // the outer join's row type.
    final int leftCount = join.getLeft().getRowType().getFieldCount();
    final int nullOffset = leftJoin ? leftCount : 0;
    final List<RexNode> projects = new ArrayList<>(builder.fields());
    insertNulls(projects, join.getRowType(), nullOffset,
        newRight.getRowType().getFieldCount(), builder);

    builder.project(projects, join.getRowType().getFieldNames())
        .filter(filter.getVariablesSet(), remainingConditions)
        .convert(filter.getRowType(), false);
    call.transformTo(builder.build());
  }

  /** Returns an {@code IS NULL} condition on a null-generating input field
   * that is non-nullable, or for which the join condition cannot be TRUE when
   * the field is NULL; returns null if there is no such condition. */
  private static @Nullable RexNode findSafeNullCondition(
      List<RexNode> conditions, Join join, boolean leftJoin) {
    final int leftCount = join.getLeft().getRowType().getFieldCount();
    for (RexNode condition : conditions) {
      if (!(condition instanceof RexCall)
          || !condition.isA(SqlKind.IS_NULL)) {
        continue;
      }
      final RexNode operand = ((RexCall) condition).getOperands().get(0);
      if (!(operand instanceof RexInputRef)) {
        continue;
      }
      final int index = ((RexInputRef) operand).getIndex();
      final boolean inputOnLeft = index < leftCount;
      if (inputOnLeft == leftJoin) {
        continue;
      }
      final int inputIndex = inputOnLeft ? index : index - leftCount;
      final RelNode input = inputOnLeft ? join.getLeft() : join.getRight();
      final RelDataType type = input.getRowType()
          .getFieldList().get(inputIndex).getType();
      // If the input field is nullable, IS NULL may also be true for a matched
      // row. It proves the row is unmatched only if the field is non-nullable,
      // or, for a nullable field, the join condition cannot be TRUE when the
      // field is NULL.
      if (!type.isNullable()
          || Strong.isNotTrue(join.getCondition(), ImmutableBitSet.of(index))) {
        return condition;
      }
    }
    return null;
  }

  /** Inserts typed NULL expressions for fields in the original row type. */
  private static void insertNulls(List<RexNode> projects, RelDataType rowType,
      int offset, int count, RelBuilder builder) {
    final List<RexNode> nulls = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      final RelDataType type =
          rowType.getFieldList().get(offset + i).getType();
      nulls.add(builder.getRexBuilder().makeNullLiteral(type));
    }
    projects.addAll(offset, nulls);
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableOuterJoinToAntiJoinRule.Config.of()
        .withOperandFor(LogicalFilter.class, LogicalJoin.class);

    @Override default OuterJoinToAntiJoinRule toRule() {
      return new OuterJoinToAntiJoinRule(this);
    }

    /** Defines an operand tree for the given classes. */
    default Config withOperandFor(Class<? extends Filter> filterClass,
        Class<? extends Join> joinClass) {
      return withOperandSupplier(b ->
          b.operand(filterClass).oneInput(b2 ->
              b2.operand(joinClass)
                  .predicate(join -> join.getJoinType() == JoinRelType.LEFT
                      || join.getJoinType() == JoinRelType.RIGHT)
                  .anyInputs()))
          .as(Config.class);
    }
  }
}
