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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.immutables.value.Value;

import java.util.List;

/**
 * Planner rule that matches a {@link Join}
 * that join type is FULL, and convert it to
 * a LEFT JOIN and RIGHT JOIN combination
 * with a UNION ALL above them.
 *
 * <p>This rule is mainly used for SQL dialect conversion.
 * The SQL example is as follows:
 *
 * <pre>{@code
 * SELECT *
 * FROM Employees e
 * FULL JOIN Departments d ON e.id = d.id
 * }</pre>
 *
 * <p>rewritten into
 *
 * <pre>{@code
 * SELECT *
 * FROM Employees e
 * LEFT JOIN Departments d ON e.id = d.id
 * UNION ALL
 * SELECT *
 * FROM Employees e
 * RIGHT JOIN Departments d ON e.id = d.id
 * where e.id is null;
 * }</pre>
 */
@Value.Enclosing
public class FullToLeftAndRightJoinRule
    extends RelRule<FullToLeftAndRightJoinRule.Config>
    implements TransformationRule {

  /** Creates an FullToLeftAndRightJoinRule. */
  protected FullToLeftAndRightJoinRule(Config config) {
    super(config);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void onMatch(RelOptRuleCall call) {
    Join join = call.rel(0);
    RelBuilder relBuilder = call.builder();

    // Only simple join condition are supported
    // to rewrite FULL JOIN currently.
    if (!isSimpleJoinCond(join.getCondition())) {
      return;
    }

    FirstInputRefFinder finder =
        new FirstInputRefFinder(join.getCluster().getRexBuilder(), join);
    join.getCondition().accept(finder);

    final RexNode rightIsNull = finder.firstRightIsNull;
    final RexNode leftIsNull = finder.firstLeftIsNull;

    if (leftIsNull == null && rightIsNull == null) {
      // TODO: The join condition being always TRUE or FALSE
      //  could potentially be supported.
      return;
    }

    RelNode newLeft = buildJoin(relBuilder, join, JoinRelType.LEFT);
    RelNode newRight = buildJoin(relBuilder, join, JoinRelType.RIGHT);

    if (leftIsNull == null) {
      // When the condition contains only right-table columns,
      // use the first occurring right-table column to construct
      // IS NULL as the left-table filter
      assert rightIsNull != null;
      newLeft = applyFilter(relBuilder, newLeft, rightIsNull);
    } else {
      // When the condition contains left-table columns
      // (regardless of right-table columns),
      // use the first occurring left-table column to construct
      // IS NULL as the right-table filter
      assert leftIsNull != null;
      newRight = applyFilter(relBuilder, newRight, leftIsNull);
    }

    relBuilder.pushAll(ImmutableList.of(newLeft, newRight))
        .union(true);

    call.transformTo(relBuilder.build());
  }

  private RelNode buildJoin(RelBuilder relBuilder, Join join, JoinRelType joinType) {
    return relBuilder.push(join.getLeft())
        .push(join.getRight())
        .join(joinType, join.getCondition())
        .build();
  }

  private RelNode applyFilter(RelBuilder relBuilder, RelNode input, RexNode condition) {
    return relBuilder.push(input)
        .filter(condition)
        .build();
  }

  private boolean isSimpleJoinCond(RexNode cond) {
    if (!(cond instanceof RexCall)) {
      return false;
    }
    RexCall call = (RexCall) cond;
    SqlOperator operator = call.getOperator();

    switch (operator.getKind()) {
    case AND:
    case OR:
      for (RexNode operand : call.getOperands()) {
        if (!isSimpleJoinCond(operand)) {
          return false;
        }
      }
      return true;
    case EQUALS:
    case NOT_EQUALS:
    case GREATER_THAN:
    case GREATER_THAN_OR_EQUAL:
    case LESS_THAN:
    case LESS_THAN_OR_EQUAL:
      for (RexNode operand : call.getOperands()) {
        if (!(operand instanceof RexInputRef
            || operand instanceof RexLiteral)) {
          return false;
        }
      }
      return true;
    default:
      return false;
    }
  }

  /** Finds the first InputRef in condition both left and right.
   * And construct the IS NULL expression. */
  private static class FirstInputRefFinder extends RexVisitorImpl<Void> {
    private final int leftFieldCount;
    private final List<RelDataTypeField> fields;
    private final RexBuilder rexBuilder;
    public @Nullable RexNode firstLeftIsNull;
    public @Nullable RexNode firstRightIsNull;

    FirstInputRefFinder(RexBuilder rexBuilder, Join join) {
      super(true);
      this.leftFieldCount = join.getLeft().getRowType().getFieldCount();
      this.fields = join.getRowType().getFieldList();
      this.rexBuilder = rexBuilder;
    }

    @Override public Void visitInputRef(RexInputRef inputRef) {
      if (firstLeftIsNull != null && firstRightIsNull != null) {
        return null;
      }

      int index = inputRef.getIndex();
      RexNode isNullExpr =
          rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL,
              rexBuilder.makeInputRef(fields.get(index).getType(), index));

      if (index < leftFieldCount && firstLeftIsNull == null) {
        firstLeftIsNull = isNullExpr;
      } else if (index >= leftFieldCount && firstRightIsNull == null) {
        firstRightIsNull = isNullExpr;
      }

      return null;
    }
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableFullToLeftAndRightJoinRule.Config.of()
        .withOperandFor(Join.class);

    @Override default FullToLeftAndRightJoinRule toRule() {
      return new FullToLeftAndRightJoinRule(this);
    }

    /** Defines an operand tree for the given classes. */
    default Config withOperandFor(Class<? extends Join> joinClass) {
      return withOperandSupplier(b -> b.operand(joinClass)
          .predicate(join -> join.getJoinType() == JoinRelType.FULL)
          .anyInputs())
          .as(Config.class);
    }
  }
}
