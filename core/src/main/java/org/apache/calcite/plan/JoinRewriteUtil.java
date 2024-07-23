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

import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.mutable.MutableJoin;
import org.apache.calcite.rel.mutable.MutableRel;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.Pair;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Utility methods concerning {@link JoinUnifyRule}.
 */
public abstract class JoinRewriteUtil {
  //~ Methods ----------------------------------------------------------------

  /**
   * Check if filter under join can be pulled up,
   * when meeting JoinOnCalc of query unify to Join of target.
   * Working in rules: {@link JoinUnifyRule.JoinOnLeftCalcToJoinUnifyRule} <br/>
   * {@link JoinUnifyRule.JoinOnRightCalcToJoinUnifyRule} <br/>
   * {@link JoinUnifyRule.JoinOnCalcsToJoinUnifyRule} <br/>
   */
  protected static boolean canPullUpFilterUnderJoin(JoinRelType joinType,
      @Nullable RexNode leftFilterRexNode, @Nullable RexNode rightFilterRexNode) {
    if (joinType == JoinRelType.INNER) {
      return true;
    }
    if (joinType == JoinRelType.LEFT
        && (rightFilterRexNode == null || rightFilterRexNode.isAlwaysTrue())) {
      return true;
    }
    if (joinType == JoinRelType.RIGHT
        && (leftFilterRexNode == null || leftFilterRexNode.isAlwaysTrue())) {
      return true;
    }
    if (joinType == JoinRelType.FULL
        && ((rightFilterRexNode == null || rightFilterRexNode.isAlwaysTrue())
        && (leftFilterRexNode == null || leftFilterRexNode.isAlwaysTrue()))) {
      return true;
    }
    return false;
  }

  /**
   * Whether joins differ only in the join type.
   */
  public static boolean operatorsEquivalentExceptJoinType(MutableJoin join1, MutableJoin join2) {
    return join1.condition.equals(join2.condition)
        && join1.variablesSet.equals(join2.variablesSet)
        && join1.getLeft().equals(join2.getLeft())
        && join1.getRight().equals(join2.getRight())
        && join1.joinType != join2.joinType;
  }

  /**
   * Inferring not null columns in a given range before the join operator.
   *
   * <p>In the case of an outer join, the other side of the outer join produces null values,
   * and this method serves to infer which nulls in the join operator fields
   * in a given range interval are produced by the join。
   */
  public static List<Integer> inferNonNullableFieldsBeforeJoin(MutableJoin join,
      int leftIdx, int rightIdx, int inputDivision, RexBuilder rexBuilder) {
    List<Integer> columnIndexList = new ArrayList<>();
    for (RelDataTypeField relDataTypeField : join.rowType.getFieldList()) {
      int fieldIndex = relDataTypeField.getIndex();
      if (fieldIndex < leftIdx || fieldIndex >= rightIdx) {
        continue;
      }
      MutableRel child = fieldIndex < inputDivision ? join.getLeft() : join.getRight();
      int inputIndex = fieldIndex;
      if (fieldIndex >= inputDivision) {
        inputIndex = fieldIndex - inputDivision;
      }
      RelDataTypeField targetChildField = child.rowType.getFieldList().get(inputIndex);
      if (SqlTypeUtil.equalExceptNullability(rexBuilder.getTypeFactory(),
          relDataTypeField.getValue(),
          targetChildField.getValue())) {
        columnIndexList.add(fieldIndex);
      }
    }
    return columnIndexList;
  }

  /**
   * Extract all the inputs in the join Cond that are equivalent conditions
   * and the inputs to the equal conditions the inputRefs on the left and right sides respectively.
   */
  public static Pair<List<Integer>, List<Integer>> extractCondEqualColumns(
      RexNode joinCondition, int joinDivision) {
    final List<RexNode> binaryPredicates = RelOptUtil.conjunctions(joinCondition).stream()
        .filter(JoinRewriteUtil::isColumnEqualBinaryPredicate)
        .collect(Collectors.toList());
    final List<Integer> condEqualLeftColumns = new ArrayList<>();
    final List<Integer> condEqualRightColumns = new ArrayList<>();
    for (RexNode predicate : binaryPredicates) {
      RexCall rexCall = (RexCall) predicate;
      RexInputRef inputRef1 = (RexInputRef) rexCall.getOperands().get(0);
      RexInputRef inputRef2 = (RexInputRef) rexCall.getOperands().get(1);
      if (inputRef1.getIndex() < joinDivision && inputRef2.getIndex() >= joinDivision) {
        condEqualLeftColumns.add(inputRef1.getIndex());
        condEqualRightColumns.add(inputRef2.getIndex());
      } else if (inputRef1.getIndex() >= joinDivision && inputRef2.getIndex() < joinDivision) {
        condEqualLeftColumns.add(inputRef2.getIndex());
        condEqualRightColumns.add(inputRef1.getIndex());
      }
    }
    return Pair.of(condEqualLeftColumns, condEqualRightColumns);
  }

  /**
   * Whether the predicate is an equivalence condition for two RexInputRefs.
   */
  public static boolean isColumnEqualBinaryPredicate(RexNode predicate) {
    if (predicate instanceof RexCall) {
      RexCall binaryPredicate = (RexCall) predicate;
      return binaryPredicate.getOperator().equals(SqlStdOperatorTable.EQUALS)
          && binaryPredicate.getOperands().stream()
          .allMatch(rexNode -> rexNode instanceof RexInputRef);
    }
    return false;
  }

  /**
   * Returns the RexNode after aligning the rowType between the query and the target.
   *
   * <p>For mv rewriting between joins,
   * we usually need to compensate a calc to make the join operators equivalent,
   * the rewritten calc input's rowType type needs to be aligned to the target,
   * and the output's rowType type needs to be aligned to the query's rowType.
   * The purpose of this method is to compensate for delta join rewrites
   * that produce nullable misalignment.
   */
  public static RexNode adjustRewriteRowType(RexNode rexNode,
      List<RelDataType> targetDataTypeList,
      List<Integer> needCompensateNullColumn,
      RexBuilder rexBuilder) {
    if (rexNode instanceof RexInputRef) {
      int index = ((RexInputRef) rexNode).getIndex();
      RelDataType targetType = targetDataTypeList.get(index);
      if (needCompensateNullColumn.contains(index)
          && SqlTypeUtil.equalExceptNullability(
              rexBuilder.getTypeFactory(), rexNode.getType(), targetType)) {
        rexNode = rexBuilder.makeCast(rexNode.getType(), rexNode);
      }
    }
    return JoinFixNullabilityShuttle.compensateJoinNullTypeColumn(
        rexNode, rexBuilder, targetDataTypeList, needCompensateNullColumn);
  }


  public static @Nullable RexNode addJoinDerivePredicate(RexBuilder rexBuilder,
      JoinUnifyRule.JoinRewriteContext context, @Nullable RexNode rexNode, int joinDivision) {
    RexNode derivedRexNode = null;
    switch (context.getJoinRewriteType()) {
    case INNER_TO_LEFT:
    case LEFT_TO_FULL:
      derivedRexNode =
          getJoinDerivedPredicateFromCandidateColumns(
              rexBuilder, context.getTargetDataTypeList(),
              context.getCandidatePredictCompensateColumn(), context.getRightCondEqualsColumns());
      break;
    case INNER_TO_RIGHT:
    case RIGHT_TO_FULL: {
      derivedRexNode =
          getJoinDerivedPredicateFromCandidateColumns(
              rexBuilder, context.getTargetDataTypeList(),
              context.getCandidatePredictCompensateColumn(), context.getLeftCondEqualsColumns());
      break;
    }
    case INNER_TO_FULL: {
      List<Integer> leftCandidateColumns = context.getCandidatePredictCompensateColumn().stream()
          .filter(a -> a < joinDivision)
          .collect(Collectors.toList());
      List<Integer> rightCandidateColumns = context.getCandidatePredictCompensateColumn().stream()
          .filter(a -> a >= joinDivision)
          .collect(Collectors.toList());
      RexNode rexNode1 =
          getJoinDerivedPredicateFromCandidateColumns(
              rexBuilder, context.getTargetDataTypeList(),
              leftCandidateColumns, context.getLeftCondEqualsColumns());
      RexNode rexNode2 =
          getJoinDerivedPredicateFromCandidateColumns(
              rexBuilder, context.getTargetDataTypeList(),
              rightCandidateColumns, context.getRightCondEqualsColumns());
      derivedRexNode = rexBuilder.makeCall(SqlStdOperatorTable.AND, rexNode1, rexNode2);
      break;
    }
    case SAME_JOIN:
      return rexNode;
    }

    assert derivedRexNode != null;
    if (rexNode == null) {
      return derivedRexNode;
    }
    rexNode =
        rexBuilder.makeCall(SqlStdOperatorTable.AND, rexNode, derivedRexNode);
    return JoinFixNullabilityShuttle.compensateJoinNullTypeColumn(
        rexNode, rexBuilder, context.getTargetDataTypeList(),
        context.getCandidatePredictCompensateColumn());
  }

  /**
   * Generate delta join compensation predicates based on candidate fields.
   *
   * <p>In most case, the relation key is not made into a column for the final project,
   * so we avoid using associative conditional columns as compensation whenever possible.
   */
  private static RexNode getJoinDerivedPredicateFromCandidateColumns(RexBuilder rexBuilder,
      List<RelDataType> targetDataTypeList,
      List<Integer> candidatePredictCompensateColumns,
      List<Integer> condEqualsColumns) {
    // prefer non-relation keys reference columns
    List<Integer> preferredColumns = candidatePredictCompensateColumns.stream()
        .filter(column -> !condEqualsColumns.contains(column))
        .collect(Collectors.toList());
    Integer index;
    if (!preferredColumns.isEmpty()) {
      index = candidatePredictCompensateColumns.get(0);
    } else {
      index = condEqualsColumns.get(0);
    }
    return rexBuilder.makeCall(
        SqlStdOperatorTable.IS_NOT_NULL, new RexInputRef(index, targetDataTypeList.get(index)));
  }

  /**
   * A shuttle for adding cast nullable to a given field.
   */
  static class JoinFixNullabilityShuttle extends RexShuttle {
    private final List<RelDataType> targetTypeList;
    private final RexBuilder rexBuilder;
    private final List<Integer> joinCompensateNullTypeColumns;

    public static RexNode compensateJoinNullTypeColumn(RexNode rexNode, RexBuilder rexBuilder,
        List<RelDataType> typeList, List<Integer> joinCompensateNullTypeColumn) {
      return rexNode.accept(
          new JoinFixNullabilityShuttle(rexBuilder, typeList, joinCompensateNullTypeColumn));
    }

    JoinFixNullabilityShuttle(RexBuilder rexBuilder,
        List<RelDataType> targetTypeList, List<Integer> joinCompensateNullTypeColumns) {
      this.targetTypeList = targetTypeList;
      this.rexBuilder = rexBuilder;
      this.joinCompensateNullTypeColumns = joinCompensateNullTypeColumns;
    }

    @Override public RexNode visitInputRef(RexInputRef ref) {
      final RelDataType targetType = targetTypeList.get(ref.getIndex());
      final RelDataType refType = ref.getType();
      // If not in the compensation list or if the operators are already equal, then return self
      if (!joinCompensateNullTypeColumns.contains(ref.getIndex()) || refType.equals(targetType)) {
        return ref;
      }
      final RelDataType nullCompensateType =
          rexBuilder.getTypeFactory().createTypeWithNullability(refType, targetType.isNullable());
      if (nullCompensateType.equals(targetType)) {
        return new RexInputRef(ref.getIndex(), nullCompensateType);
      }
      throw new AssertionError("compensation type not supported " + ref + " " + targetType);
    }
  }
}
