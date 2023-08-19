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
package org.apache.calcite.plan.rule;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.mutable.MutableJoin;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.calcite.rel.core.JoinRelType.FULL;

import static java.util.Objects.requireNonNull;

public class JoinRexUtils
{
  private JoinRexUtils() {}
  public static RexNode addJoinDerivePredicate(RexBuilder rexBuilder, JoinContext joinContext, RexNode rexNode)
  {
    Optional<RexNode> derivedPredicateOpt = Optional.empty();
    switch (joinContext.getJoinRewriteType()) {
    case INNER_JOIN_TO_LEFT_JOIN: {
      List<RexInputRef> joinColumns = joinContext.getRightJoinColumns();
      derivedPredicateOpt = getJoinDerivedPredicate(rexBuilder, joinColumns, joinContext, false);
      break;
    }
    case INNER_JOIN_TO_RIGHT_JOIN: {
      List<RexInputRef> joinColumns = joinContext.getLeftJoinColumns();
      derivedPredicateOpt = getJoinDerivedPredicate(rexBuilder, joinColumns, joinContext, false);
      break;
    }
    case LEFT_JOIN_TO_FULL_JOIN:
    case RIGHT_JOIN_TO_FULL_JOIN:
    case INNER_JOIN_TO_FULL_JOIN: {
      List<RexInputRef> joinColumns = new ArrayList<>();
      derivedPredicateOpt = getJoinDerivedPredicate(rexBuilder, joinColumns, joinContext, true);
      break;
    }
    }
    if (!derivedPredicateOpt.isPresent()) {
      return rexNode;
    }
    rexNode = rexNode == null ? derivedPredicateOpt.get() : rexBuilder.makeCall(SqlStdOperatorTable.AND, rexNode, derivedPredicateOpt.get());
    return compensateJoinNullTypeColumn(rexNode, rexBuilder, joinContext.getTargetDataTypeList(), joinContext.getNeedCompensateNullColumn());
  }
  public static boolean canEliminateNull()
  {
    return true;
  }
  private static Optional<RexNode> getJoinDerivedPredicate(RexBuilder rexBuilder, List<RexInputRef> candidateColumns, JoinContext joinContext, boolean isNeedCompensate)
  {
    RexNode rexNode = null;
    if (isNeedCompensate || candidateColumns.isEmpty()) {
      for (RexInputRef column : joinContext.getNotNullColumns()) {
        joinContext.getNeedCompensateNullColumn().add(column.getIndex());
        RexNode rexNodeTemp = rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, column);
        if (rexNode == null) {
          rexNode = rexNodeTemp;
          continue;
        }
        rexNode = rexBuilder.makeCall(SqlStdOperatorTable.AND, rexNode, rexNodeTemp);
      }
    }
    if (rexNode == null && !candidateColumns.isEmpty()) {
      rexNode = rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, candidateColumns.get(0));
    }
    return rexNode == null ? Optional.empty() : Optional.of(rexNode);
  }
  public static boolean isJoinRewritable(JoinContext joinContext, int leftIdx)
  {
    List<RelDataTypeField> relDataTypeFieldList = joinContext.getInputDataTypeList();
    switch (joinContext.getJoinRewriteType()){
    case INNER_JOIN_TO_LEFT_JOIN :
    case INNER_JOIN_TO_RIGHT_JOIN :
      return true;
    case LEFT_JOIN_TO_FULL_JOIN :
      return hasNotNullColumn(relDataTypeFieldList.subList(0, leftIdx), joinContext, 0);
    case RIGHT_JOIN_TO_FULL_JOIN :
      return hasNotNullColumn(relDataTypeFieldList.subList(leftIdx, relDataTypeFieldList.size()), joinContext, leftIdx);
    case INNER_JOIN_TO_FULL_JOIN :
      return hasNotNullColumn(relDataTypeFieldList.subList(leftIdx, relDataTypeFieldList.size()), joinContext, leftIdx) && hasNotNullColumn(relDataTypeFieldList.subList(0, leftIdx), joinContext, 0);
    default :
      return false;
    }
  }
  public static boolean hasNotNullColumn(List<RelDataTypeField> columnList, JoinContext joinContext, int offset)
  {
    for (RelDataTypeField column : columnList) {
      if (!column.getType().isNullable()) {
        joinContext.getNotNullColumns().add(new RexInputRef(column.getIndex() + offset, column.getType()));
        return true;
      }
    }
    return false;
  }

  public static boolean isSupportedPredict(RexNode joinCondition, List<Pair<RexInputRef, RexInputRef>> joinColumnPairs, int leftInputFieldCount)
  {
    List<RexNode> conjuncts = RelOptUtil.conjunctions(joinCondition);
    List<RexNode> binaryPredicates = conjuncts.stream()
        .filter(JoinRexUtils::isColumnEqualBinaryPredicate).collect(Collectors.toList());
    if (binaryPredicates.isEmpty()) {
      return false;
    }
    for (RexNode predicate : binaryPredicates) {
      RexCall rexCall = (RexCall) predicate;
      RexInputRef inputRef1 = (RexInputRef) rexCall.getOperands().get(0);
      RexInputRef inputRef2 = (RexInputRef) rexCall.getOperands().get(1);
      if (inputRef1.getIndex() < leftInputFieldCount) {
        joinColumnPairs.add(Pair.of(inputRef1, inputRef2));
      }
      else {
        joinColumnPairs.add(Pair.of(inputRef2, inputRef1));
      }
    }
    return true;
  }
  public static boolean isColumnEqualBinaryPredicate(RexNode predicate)
  {
    if (predicate instanceof RexCall) {
      RexCall binaryPredicate = (RexCall) predicate;
      return binaryPredicate.getOperator().equals(SqlStdOperatorTable.EQUALS)
          && binaryPredicate.getOperands().stream().allMatch(rexNode -> rexNode instanceof RexInputRef);
    }
    return false;
  }
  public static JoinContext extractJoinContext(MutableJoin queryRel, MutableJoin targetRel, List<Pair<RexInputRef, RexInputRef>> joinColumnPairs, int joinDivision, RexBuilder rexBuilder)
  {
    requireNonNull(joinColumnPairs);
    JoinRewriteType joinRewriteType = createJoinReWriteType(queryRel.joinType, targetRel.joinType);
    if (joinRewriteType == null) {
      return null;
    }
    Pair<List<RexInputRef>, List<RexInputRef>> joinColumnPair = Pair.of(joinColumnPairs.stream().map(Pair::getKey).collect(Collectors.toList()), joinColumnPairs.stream().map(Pair::getValue).collect(Collectors.toList()));
    List<Integer> needCompensateNullColumn = parseNeedCompensateNullColumn(joinRewriteType, targetRel, joinDivision, rexBuilder);
    List<RelDataType> targetDataTypeList = targetRel.rowType.getFieldList().stream().map(Map.Entry::getValue).collect(Collectors.toList());
    List<RelDataTypeField> inputDataTypeList = Stream.concat(targetRel.getLeft().rowType.getFieldList().stream(), targetRel.getRight().rowType.getFieldList().stream()).collect(Collectors.toList());
    return new JoinContext(joinRewriteType, needCompensateNullColumn, joinColumnPair, targetDataTypeList, inputDataTypeList);
  }
  public static List<Integer> parseNeedCompensateNullColumn(JoinRewriteType type, MutableJoin targetRel, int joinDivision, RexBuilder rexBuilder)
  {
    List<Integer> targetRelDataTypeIndexList = targetRel.rowType.getFieldList().stream().map(RelDataTypeField::getIndex).collect(Collectors.toList());
    switch (type) {
    case INNER_JOIN_TO_LEFT_JOIN :
    case RIGHT_JOIN_TO_FULL_JOIN :
      return getNeedCompensateNullColumn(targetRel, joinDivision, targetRelDataTypeIndexList.size(), joinDivision, rexBuilder);
    case INNER_JOIN_TO_RIGHT_JOIN :
    case LEFT_JOIN_TO_FULL_JOIN :
      return getNeedCompensateNullColumn(targetRel, 0, joinDivision, joinDivision, rexBuilder);
    case INNER_JOIN_TO_FULL_JOIN :
      return getNeedCompensateNullColumn(targetRel, 0, targetRelDataTypeIndexList.size(), joinDivision, rexBuilder);
    default : return null;
    }
  }
  public static List<Integer> getNeedCompensateNullColumn(MutableJoin targetRel, int leftIdx, int rightIdx, int joinDivision, RexBuilder rexBuilder)
  {
    return targetRel.rowType.getFieldList()
        .stream()
        .filter(typeField -> typeField.getIndex() >= leftIdx && typeField.getIndex() < rightIdx)
        .filter(typeField -> {
          RelDataTypeField relDataTypeField = typeField.getIndex() < joinDivision ? targetRel.getLeft().rowType.getFieldList().get(typeField.getIndex()) : targetRel.getRight().rowType.getFieldList().get(typeField.getIndex() - joinDivision);
          return isOnlyNullableDifference(typeField.getValue(), relDataTypeField.getValue(), rexBuilder.getTypeFactory());
        }).map(RelDataTypeField::getIndex).collect(Collectors.toList());
  }
  public static boolean isOnlyNullableDifference(RelDataType queryRelDataType, RelDataType targetRelDataType, RelDataTypeFactory relDataTypeFactory)
  {
    requireNonNull(queryRelDataType);
    requireNonNull(targetRelDataType);
    if (queryRelDataType.equals(targetRelDataType)) {
      return false;
    }
    RelDataType newTargetRelDataType = relDataTypeFactory.createTypeWithNullability(targetRelDataType, queryRelDataType.isNullable());
    assert queryRelDataType.equals(newTargetRelDataType);
    return true;
  }
  public static JoinRewriteType createJoinReWriteType(JoinRelType queryType, JoinRelType targetType)
  {
    requireNonNull(queryType);
    requireNonNull(targetType);
    if (queryType.equals(targetType)) {
      return null;
    }
    switch (queryType){
    case INNER:
    {
      switch (targetType) {
      case LEFT: return JoinRewriteType.INNER_JOIN_TO_LEFT_JOIN;
      case RIGHT: return JoinRewriteType.INNER_JOIN_TO_RIGHT_JOIN;
      case FULL: return JoinRewriteType.INNER_JOIN_TO_FULL_JOIN;
      default: return null;
      }
    }
    case LEFT:
    {
      if (targetType == FULL) {
        return JoinRewriteType.LEFT_JOIN_TO_FULL_JOIN;
      }
    }
    case RIGHT:
    {
      if (targetType == FULL) {
        return JoinRewriteType.RIGHT_JOIN_TO_FULL_JOIN;
      }
    }
    default: return null;
    }
  }
  public enum JoinRewriteType{
    INNER_JOIN_TO_LEFT_JOIN,
    INNER_JOIN_TO_RIGHT_JOIN,
    INNER_JOIN_TO_FULL_JOIN,
    LEFT_JOIN_TO_FULL_JOIN,
    RIGHT_JOIN_TO_FULL_JOIN,
  }
  public static RexNode compensateJoinNullTypeColumn(RexNode rexNode, RexBuilder rexBuilder,
      List<RelDataType> typeList, List<Integer> joinCompensateNullTypeColumn)
  {
    return rexNode.accept(new JoinFixNullabilityShuttle(rexBuilder, typeList, joinCompensateNullTypeColumn));
  }
  public static class JoinFixNullabilityShuttle
      extends RexShuttle
  {
    private final List<RelDataType> typeList;
    private final RexBuilder rexBuilder;
    private final List<Integer> joinCompensateNullTypeColumn;

    public JoinFixNullabilityShuttle(RexBuilder rexBuilder,
        List<RelDataType> typeList, List<Integer> joinCompensateNullTypeColumn)
    {
      this.typeList = typeList;
      this.rexBuilder = rexBuilder;
      this.joinCompensateNullTypeColumn = joinCompensateNullTypeColumn;
    }

    @Override public RexNode visitInputRef(RexInputRef ref)
    {
      final RelDataType rightType = typeList.get(ref.getIndex());
      final RelDataType refType = ref.getType();
      if (joinCompensateNullTypeColumn != null && !joinCompensateNullTypeColumn.contains(ref.getIndex()) || refType.equals(rightType)) {
        return ref;
      }
      final RelDataType refType2 =
          rexBuilder.getTypeFactory().createTypeWithNullability(refType,
              rightType.isNullable());
      if (refType2.equals(rightType)) {
        return new RexInputRef(ref.getIndex(), refType2);
      }
      throw new AssertionError("mismatched type " + ref + " " + rightType);
    }
  }
  public static RexNode tryCastRexInputRef(RexNode rexNode, JoinContext joinContext, RexBuilder rexBuilder)
  {
    if (rexNode instanceof RexInputRef) {
      int index = ((RexInputRef) rexNode).getIndex();
      if (joinContext.getNeedCompensateNullColumn().contains(index) && isOnlyNullableDifference(rexNode.getType(), joinContext.getTargetDataTypeList().get(index), rexBuilder.getTypeFactory())) {
        rexNode = rexBuilder.makeCast(rexNode.getType(), rexNode);
      }
    }
    return compensateJoinNullTypeColumn(rexNode, rexBuilder, joinContext.getTargetDataTypeList(), joinContext.getNeedCompensateNullColumn());
  }
}
