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

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.InequalityOperator;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelNodes;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

/** Implementation of an inner IEJoin with two inequality predicates in
 * {@link EnumerableConvention enumerable calling convention}. */
public class EnumerableIEJoin extends Join implements EnumerableRel {
  private final ImmutableList<Condition> conditions;

  /** Creates an EnumerableIEJoin.
   *
   * <p>Use {@link #create} unless you know what you're doing. */
  protected EnumerableIEJoin(RelOptCluster cluster, RelTraitSet traitSet,
      RelNode left, RelNode right, RexNode condition) {
    super(cluster, traitSet, ImmutableList.of(), left, right, condition,
        ImmutableSet.of(), JoinRelType.INNER);
    final List<RexNode> conjunctions = RelOptUtil.conjunctions(condition);
    if (conjunctions.size() != 2) {
      throw new IllegalArgumentException(
          "condition must contain exactly two supported cross-input inequalities");
    }
    final int leftFieldCount = left.getRowType().getFieldCount();
    final Condition first =
        analyzeConjunction(conjunctions.get(0), leftFieldCount);
    final Condition second =
        analyzeConjunction(conjunctions.get(1), leftFieldCount);
    if (first == null || second == null) {
      throw new IllegalArgumentException(
          "condition must contain supported cross-input inequalities");
    }
    final ImmutableList<Condition> conditions = ImmutableList.of(first, second);
    for (Condition inequality : conditions) {
      if (!supportsKeyTypes(left, right, inequality)) {
        throw new IllegalArgumentException("unsupported IEJoin key types: left "
            + left.getRowType().getFieldList().get(inequality.leftKey).getType()
            + ", right "
            + right.getRowType().getFieldList().get(inequality.rightKey).getType());
      }
    }
    this.conditions = conditions;
  }

  /** Creates an EnumerableIEJoin. */
  public static EnumerableIEJoin create(RelNode left, RelNode right,
      RexNode condition) {
    return new EnumerableIEJoin(left.getCluster(),
        left.getCluster().traitSetOf(EnumerableConvention.INSTANCE),
        left, right, condition);
  }

  @Override public EnumerableIEJoin copy(RelTraitSet traitSet,
      RexNode condition, RelNode left, RelNode right, JoinRelType joinType,
      boolean semiJoinDone) {
    if (joinType != JoinRelType.INNER) {
      throw new IllegalArgumentException("EnumerableIEJoin only supports inner joins");
    }
    return new EnumerableIEJoin(getCluster(), traitSet, left, right,
        condition);
  }

  @Override public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    final double leftRows = mq.getRowCount(left);
    final double rightRows = mq.getRowCount(right);
    double outputRows = mq.getRowCount(this);
    if (RelNodes.COMPARATOR.compare(left, right) > 0) {
      outputRows = RelMdUtil.addEpsilon(outputRows);
    }
    final double inputRows = leftRows + rightRows;
    // IEJoin sorts the union twice, scans it once, then emits the result.
    final double cost =
        2D * Util.nLogN(inputRows) + inputRows + outputRows;
    return planner.getCostFactory().makeCost(cost, 0, 0);
  }

  @Override public Result implement(EnumerableRelImplementor implementor,
      Prefer pref) {
    final BlockBuilder builder = new BlockBuilder();
    final Result leftResult =
        implementor.visitChild(this, 0, (EnumerableRel) left, pref);
    final Expression leftExpression =
        builder.append("left", leftResult.block);
    final Result rightResult =
        implementor.visitChild(this, 1, (EnumerableRel) right, pref);
    final Expression rightExpression =
        builder.append("right", rightResult.block);
    final ParameterExpression leftParameter =
        Expressions.parameter(leftResult.physType.getJavaRowType(), "leftRow");
    final ParameterExpression rightParameter =
        Expressions.parameter(rightResult.physType.getJavaRowType(), "rightRow");
    final JavaTypeFactory typeFactory = implementor.getTypeFactory();
    final List<Expression> keySelectors = new ArrayList<>();
    final List<Expression> comparators = new ArrayList<>();

    for (Condition condition : conditions) {
      final RelDataType leftType =
          left.getRowType().getFieldList().get(condition.leftKey).getType();
      final RelDataType rightType =
          right.getRowType().getFieldList().get(condition.rightKey).getType();
      final RelDataType keyType =
          requireNonNull(typeFactory.leastRestrictive(ImmutableList.of(leftType, rightType)));
      final Type keyClass = typeFactory.getJavaClass(keyType);
      keySelectors.add(
          Expressions.lambda(
          Function1.class,
          EnumUtils.convert(
              leftResult.physType.fieldReference(
              leftParameter, condition.leftKey), keyClass), leftParameter));
      keySelectors.add(
          Expressions.lambda(
          Function1.class,
          EnumUtils.convert(
              rightResult.physType.fieldReference(
              rightParameter, condition.rightKey), keyClass), rightParameter));
      // PhysType generates comparators for row fields, so wrap the key in a
      // scalar row type.
      final RelDataType keyRowType =
          typeFactory.builder().add("key", keyType).build();
      final PhysType keyPhysType =
          PhysTypeImpl.of(typeFactory, keyRowType, JavaRowFormat.SCALAR);
      comparators.add(
          keyPhysType.generateComparator(
              RelCollations.of(
                  new RelFieldCollation(0,
                      RelFieldCollation.Direction.ASCENDING,
                      RelFieldCollation.NullDirection.LAST))));
    }

    final PhysType physType =
        PhysTypeImpl.of(typeFactory, getRowType(), pref.preferArray());
    final List<Expression> arguments = new ArrayList<>();
    arguments.add(leftExpression);
    arguments.add(rightExpression);
    arguments.addAll(keySelectors);
    arguments.addAll(comparators);
    arguments.add(Expressions.constant(conditions.get(0).operator));
    arguments.add(Expressions.constant(conditions.get(1).operator));
    arguments.add(
        EnumUtils.joinSelector(joinType, physType,
        ImmutableList.of(leftResult.physType, rightResult.physType)));

    return implementor.result(physType,
        builder.append(
            Expressions.call(BuiltInMethod.IE_JOIN.method,
            arguments)).toBlock());
  }

  static @Nullable Condition analyzeConjunction(RexNode node,
      int leftFieldCount) {
    if (!(node instanceof RexCall) || ((RexCall) node).operands.size() != 2) {
      return null;
    }
    final RexCall call = (RexCall) node;
    if (!(call.operands.get(0) instanceof RexInputRef)
        || !(call.operands.get(1) instanceof RexInputRef)) {
      return null;
    }
    final int first = ((RexInputRef) call.operands.get(0)).getIndex();
    final int second = ((RexInputRef) call.operands.get(1)).getIndex();
    final boolean firstIsLeft = first < leftFieldCount;
    final boolean secondIsLeft = second < leftFieldCount;
    if (firstIsLeft == secondIsLeft) {
      return null;
    }
    final InequalityOperator operator;
    switch (firstIsLeft ? call.getKind() : call.getKind().reverse()) {
    case LESS_THAN:
      operator = InequalityOperator.LESS_THAN;
      break;
    case LESS_THAN_OR_EQUAL:
      operator = InequalityOperator.LESS_THAN_OR_EQUAL;
      break;
    case GREATER_THAN:
      operator = InequalityOperator.GREATER_THAN;
      break;
    case GREATER_THAN_OR_EQUAL:
      operator = InequalityOperator.GREATER_THAN_OR_EQUAL;
      break;
    default:
      return null;
    }
    return firstIsLeft
        ? new Condition(first, second - leftFieldCount, operator)
        : new Condition(second, first - leftFieldCount, operator);
  }

  static boolean supportsKeyTypes(RelNode left, RelNode right,
      Condition condition) {
    final RelDataType leftType =
        left.getRowType().getFieldList().get(condition.leftKey).getType();
    final RelDataType rightType =
        right.getRowType().getFieldList().get(condition.rightKey).getType();
    final SqlTypeName typeName = leftType.getSqlTypeName();
    return SqlTypeUtil.equalSansNullability(
        left.getCluster().getTypeFactory(), leftType, rightType)
        && (SqlTypeUtil.isBoolean(leftType)
            || (SqlTypeUtil.isExactNumeric(leftType)
                && !SqlTypeName.UNSIGNED_TYPES.contains(typeName))
            || SqlTypeUtil.isCharacter(leftType)
            || SqlTypeUtil.isBinary(leftType)
            || SqlTypeUtil.isDatetime(leftType)
            || SqlTypeUtil.isInterval(leftType));
  }

  /** A normalized IEJoin condition. */
  static final class Condition {
    final int leftKey;
    final int rightKey;
    final InequalityOperator operator;

    private Condition(int leftKey, int rightKey,
        InequalityOperator operator) {
      this.leftKey = leftKey;
      this.rightKey = rightKey;
      this.operator = operator;
    }
  }
}
