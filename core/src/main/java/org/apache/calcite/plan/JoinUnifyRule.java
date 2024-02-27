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
import org.apache.calcite.rel.mutable.MutableCalc;
import org.apache.calcite.rel.mutable.MutableJoin;
import org.apache.calcite.rel.mutable.MutableRel;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.calcite.plan.JoinRewriteUtil.addJoinDerivePredicate;
import static org.apache.calcite.plan.JoinRewriteUtil.adjustRewriteRowType;
import static org.apache.calcite.plan.JoinRewriteUtil.canPullUpFilterUnderJoin;
import static org.apache.calcite.plan.JoinRewriteUtil.extractCondEqualColumns;
import static org.apache.calcite.plan.JoinRewriteUtil.inferNonNullableFieldsBeforeJoin;
import static org.apache.calcite.plan.JoinRewriteUtil.operatorsEquivalentExceptJoinType;
import static org.apache.calcite.plan.SubstitutionVisitor.explainCalc;
import static org.apache.calcite.plan.SubstitutionVisitor.fieldCnt;
import static org.apache.calcite.plan.SubstitutionVisitor.splitFilter;
import static org.apache.calcite.plan.SubstitutionVisitor.tryMergeParentCalcAndGenResult;
import static org.apache.calcite.rel.core.JoinRelType.FULL;
import static org.apache.calcite.rel.core.JoinRelType.INNER;
import static org.apache.calcite.rel.core.JoinRelType.LEFT;
import static org.apache.calcite.rel.core.JoinRelType.RIGHT;

import static java.util.Objects.requireNonNull;

/**
 * UnifyRule implementation of the {@link org.apache.calcite.rel.core.Join}.
 *
 * <p>Contains rules {@link JoinUnifyRule.JoinOnLeftCalcToJoinUnifyRule} <br/>
 * {@link JoinUnifyRule.JoinOnRightCalcToJoinUnifyRule} <br/>
 * {@link JoinUnifyRule.JoinOnCalcsToJoinUnifyRule} <br/>
 * {@link JoinUnifyRule.JoinExtendTrivialRule} <br/>
 */
public class JoinUnifyRule {
  private JoinUnifyRule() {
  }

  /**
   * A {@link SubstitutionVisitor.UnifyRule} that matches a {@link MutableJoin}
   * which has {@link MutableCalc} as left child to a {@link MutableJoin}.
   * We try to pull up the {@link MutableCalc} to top of {@link MutableJoin},
   * then match the {@link MutableJoin} in query to {@link MutableJoin} in target.
   */
  protected static class JoinOnLeftCalcToJoinUnifyRule
      extends SubstitutionVisitor.AbstractUnifyRule {

    public static final JoinOnLeftCalcToJoinUnifyRule INSTANCE =
        new JoinOnLeftCalcToJoinUnifyRule();

    private JoinOnLeftCalcToJoinUnifyRule() {
      super(
          operand(MutableJoin.class, operand(MutableCalc.class, query(0)), query(1)),
          operand(MutableJoin.class, target(0), target(1)), 2);
    }

    @Override protected SubstitutionVisitor.@Nullable UnifyResult apply(
        SubstitutionVisitor.UnifyRuleCall call) {
      final MutableJoin query = (MutableJoin) call.query;
      final MutableCalc qInput0 = (MutableCalc) query.getLeft();
      final MutableRel qInput1 = query.getRight();
      final Pair<RexNode, List<RexNode>> qInput0Explained = explainCalc(qInput0);
      final RexNode qInput0Cond = qInput0Explained.left;
      final List<RexNode> qInput0Projs = qInput0Explained.right;

      final MutableJoin target = (MutableJoin) call.target;
      int leftSize = fieldCnt(target.getLeft());

      final RexBuilder rexBuilder = call.getCluster().getRexBuilder();

      // Check if filter under join can be pulled up.
      if (!canPullUpFilterUnderJoin(query.joinType, qInput0Cond, null)) {
        return null;
      }
      // Try pulling up MutableCalc only when Join condition references mapping.
      final List<RexNode> identityProjects =
          rexBuilder.identityProjects(qInput1.rowType);
      if (!referenceByMapping(query.condition, qInput0Projs, identityProjects)) {
        return null;
      }

      JoinRewriteContext joinContext =
          JoinRewriteContext.of(query, target, leftSize, rexBuilder);
      if (!isSupportJoinRewrite(joinContext, leftSize)) {
        return null;
      }

      final RexNode newQueryJoinCond = new RexShuttle() {
        @Override public RexNode visitInputRef(RexInputRef inputRef) {
          final int idx = inputRef.getIndex();
          if (idx < fieldCnt(qInput0)) {
            final int newIdx = ((RexInputRef) qInput0Projs.get(idx)).getIndex();
            return new RexInputRef(newIdx, inputRef.getType());
          } else {
            int newIdx = idx - fieldCnt(qInput0) + fieldCnt(qInput0.getInput());
            return new RexInputRef(newIdx, inputRef.getType());
          }
        }
      }.apply(query.condition);

      final RexNode splitted =
          splitFilter(call.getSimplify(), newQueryJoinCond, target.condition);
      // MutableJoin matches only when the conditions are analyzed to be same.
      if (splitted != null && splitted.isAlwaysTrue()) {
        final List<RexNode> compenProjs = new ArrayList<>();
        for (int i = 0; i < fieldCnt(query); i++) {
          RexNode rexInputRef;
          if (i < fieldCnt(qInput0)) {
            rexInputRef = rexBuilder.copy(qInput0Projs.get(i));
          } else {
            final int newIdx = i - fieldCnt(qInput0) + fieldCnt(qInput0.getInput());
            rexInputRef = new RexInputRef(newIdx, query.rowType.getFieldList().get(i).getType());
          }
          compenProjs.add(
              adjustRewriteRowType(rexInputRef, joinContext.getTargetDataTypeList(),
                  joinContext.getCandidatePredictCompensateColumn(), rexBuilder));
        }

        final RexNode compenCond =
            addJoinDerivePredicate(rexBuilder, joinContext, qInput0Cond, leftSize);

        final RexProgram compenRexProgram =
            RexProgram.create(target.rowType, compenProjs, compenCond,
                query.rowType, rexBuilder);
        final MutableCalc compenCalc = MutableCalc.of(target, compenRexProgram);
        return tryMergeParentCalcAndGenResult(call, compenCalc);
      }

      return null;
    }
  }

  /**
   * A {@link SubstitutionVisitor.UnifyRule} that matches a {@link MutableJoin}
   * which has {@link MutableCalc} as right child to a {@link MutableJoin}.
   * We try to pull up the {@link MutableCalc} to top of {@link MutableJoin},
   * then match the {@link MutableJoin} in query to {@link MutableJoin} in target.
   */
  protected static class JoinOnRightCalcToJoinUnifyRule
      extends SubstitutionVisitor.AbstractUnifyRule {

    public static final JoinOnRightCalcToJoinUnifyRule INSTANCE =
        new JoinOnRightCalcToJoinUnifyRule();

    private JoinOnRightCalcToJoinUnifyRule() {
      super(
          operand(MutableJoin.class, query(0), operand(MutableCalc.class, query(1))),
          operand(MutableJoin.class, target(0), target(1)), 2);
    }

    @Override protected SubstitutionVisitor.@Nullable UnifyResult apply(
        SubstitutionVisitor.UnifyRuleCall call) {
      final MutableJoin query = (MutableJoin) call.query;
      final MutableRel qInput0 = query.getLeft();
      final MutableCalc qInput1 = (MutableCalc) query.getRight();
      final Pair<RexNode, List<RexNode>> qInput1Explained = explainCalc(qInput1);
      final RexNode qInput1Cond = qInput1Explained.left;
      final List<RexNode> qInput1Projs = qInput1Explained.right;

      final MutableJoin target = (MutableJoin) call.target;
      int leftSize = fieldCnt(target.getLeft());

      final RexBuilder rexBuilder = call.getCluster().getRexBuilder();

      // Check if filter under join can be pulled up.
      if (!canPullUpFilterUnderJoin(query.joinType, null, qInput1Cond)) {
        return null;
      }
      // Try pulling up MutableCalc only when Join condition references mapping.
      final List<RexNode> identityProjects =
          rexBuilder.identityProjects(qInput0.rowType);
      if (!referenceByMapping(query.condition, identityProjects, qInput1Projs)) {
        return null;
      }

      JoinRewriteContext joinContext =
          JoinRewriteContext.of(query, target, leftSize, rexBuilder);
      if (!isSupportJoinRewrite(joinContext, leftSize)) {
        return null;
      }

      final RexNode newQueryJoinCond = new RexShuttle() {
        @Override public RexNode visitInputRef(RexInputRef inputRef) {
          final int idx = inputRef.getIndex();
          if (idx < fieldCnt(qInput0)) {
            return inputRef;
          } else {
            final int newIdx = ((RexInputRef) qInput1Projs.get(idx - fieldCnt(qInput0)))
                .getIndex() + fieldCnt(qInput0);
            return new RexInputRef(newIdx, inputRef.getType());
          }
        }
      }.apply(query.condition);

      final RexNode splitted =
          splitFilter(call.getSimplify(), newQueryJoinCond, target.condition);
      // MutableJoin matches only when the conditions are analyzed to be same.
      if (splitted != null && splitted.isAlwaysTrue()) {
        final List<RexNode> compenProjs = new ArrayList<>();
        for (int i = 0; i < query.rowType.getFieldCount(); i++) {
          RexNode rexInputRef;
          if (i < fieldCnt(qInput0)) {
            rexInputRef =
                new RexInputRef(i, query.rowType.getFieldList().get(i).getType());
          } else {
            rexInputRef =
                RexUtil.shift(
                    qInput1Projs.get(i - fieldCnt(qInput0)), qInput0.rowType.getFieldCount());
          }
          compenProjs.add(
              adjustRewriteRowType(rexInputRef, joinContext.getTargetDataTypeList(),
                  joinContext.getCandidatePredictCompensateColumn(), rexBuilder));
        }

        final RexNode compenCond =
            addJoinDerivePredicate(rexBuilder, joinContext,
                RexUtil.shift(qInput1Cond, qInput0.rowType.getFieldCount()), leftSize);

        final RexProgram compensatingRexProgram =
            RexProgram.create(target.rowType, compenProjs, compenCond,
                query.rowType, rexBuilder);
        final MutableCalc compenCalc = MutableCalc.of(target, compensatingRexProgram);
        return tryMergeParentCalcAndGenResult(call, compenCalc);
      }
      return null;
    }
  }

  /**
   * A {@link SubstitutionVisitor.UnifyRule} that matches a {@link MutableJoin}
   * which has {@link MutableCalc} as children to a {@link MutableJoin}.
   * We try to pull up the {@link MutableCalc} to top of {@link MutableJoin},
   * then match the {@link MutableJoin} in query to {@link MutableJoin} in target.
   */
  protected static class JoinOnCalcsToJoinUnifyRule
      extends SubstitutionVisitor.AbstractUnifyRule {

    public static final JoinOnCalcsToJoinUnifyRule INSTANCE =
        new JoinOnCalcsToJoinUnifyRule();

    private JoinOnCalcsToJoinUnifyRule() {
      super(
          operand(MutableJoin.class,
              operand(MutableCalc.class, query(0)), operand(MutableCalc.class, query(1))),
          operand(MutableJoin.class, target(0), target(1)), 2);
    }

    @Override protected SubstitutionVisitor.@Nullable UnifyResult apply(
        SubstitutionVisitor.UnifyRuleCall call) {
      final MutableJoin query = (MutableJoin) call.query;
      final MutableCalc qInput0 = (MutableCalc) query.getLeft();
      final MutableCalc qInput1 = (MutableCalc) query.getRight();
      final Pair<RexNode, List<RexNode>> qInput0Explained = explainCalc(qInput0);
      final RexNode qInput0Cond = qInput0Explained.left;
      final List<RexNode> qInput0Projs = qInput0Explained.right;
      final Pair<RexNode, List<RexNode>> qInput1Explained = explainCalc(qInput1);
      final RexNode qInput1Cond = qInput1Explained.left;
      final List<RexNode> qInput1Projs = qInput1Explained.right;

      final MutableJoin target = (MutableJoin) call.target;
      int leftSize = fieldCnt(target.getLeft());

      final RexBuilder rexBuilder = call.getCluster().getRexBuilder();

      // Check if filter under join can be pulled up.
      if (!canPullUpFilterUnderJoin(query.joinType, qInput0Cond, qInput1Cond)) {
        return null;
      }
      if (!referenceByMapping(query.condition, qInput0Projs, qInput1Projs)) {
        return null;
      }

      JoinRewriteContext joinContext =
          JoinRewriteContext.of(query, target, leftSize, rexBuilder);
      if (!isSupportJoinRewrite(joinContext, leftSize)) {
        return null;
      }

      RexNode newQueryJoinCond = new RexShuttle() {
        @Override public RexNode visitInputRef(RexInputRef inputRef) {
          final int idx = inputRef.getIndex();
          if (idx < fieldCnt(qInput0)) {
            final int newIdx = ((RexInputRef) qInput0Projs.get(idx)).getIndex();
            return new RexInputRef(newIdx, inputRef.getType());
          } else {
            final int newIdx = ((RexInputRef) qInput1Projs.get(idx - fieldCnt(qInput0)))
                .getIndex() + fieldCnt(qInput0.getInput());
            return new RexInputRef(newIdx, inputRef.getType());
          }
        }
      }.apply(query.condition);
      final RexNode splitted =
          splitFilter(call.getSimplify(), newQueryJoinCond, target.condition);
      // MutableJoin matches only when the conditions are analyzed to be same.
      if (splitted != null && splitted.isAlwaysTrue()) {
        final RexNode qInput1CondShifted =
            RexUtil.shift(qInput1Cond, fieldCnt(qInput0.getInput()));

        final List<RexNode> compenProjs = new ArrayList<>();
        for (int i = 0; i < query.rowType.getFieldCount(); i++) {
          RexNode rexNode;
          if (i < fieldCnt(qInput0)) {
            rexNode = rexBuilder.copy(qInput0Projs.get(i));
          } else {
            rexNode =
                RexUtil.shift(qInput1Projs.get(i - fieldCnt(qInput0)),
                    fieldCnt(qInput0.getInput()));
          }
          compenProjs.add(
              adjustRewriteRowType(rexNode, joinContext.getTargetDataTypeList(),
                  joinContext.getCandidatePredictCompensateColumn(), rexBuilder));
        }

        final RexNode compenCond =
            addJoinDerivePredicate(
                rexBuilder, joinContext, RexUtil.composeConjunction(
                    rexBuilder, ImmutableList.of(qInput0Cond, qInput1CondShifted)), leftSize);

        final RexProgram compensatingRexProgram =
            RexProgram.create(target.rowType, compenProjs, compenCond,
                query.rowType, rexBuilder);
        final MutableCalc compensatingCalc =
            MutableCalc.of(target, compensatingRexProgram);
        return tryMergeParentCalcAndGenResult(call, compensatingCalc);
      }
      return null;
    }
  }

  /**
   * A {@link SubstitutionVisitor.UnifyRule} that matches a {@link MutableJoin}
   * which has which the query {@link MutableJoin}
   * is already equal to the target {@link MutableJoin} except joinType.
   * We try to pull up the {@link MutableCalc} to top of {@link MutableJoin},
   * then match the {@link MutableJoin} in query to {@link MutableJoin} in target.
   */
  protected static class JoinExtendTrivialRule
      extends SubstitutionVisitor.AbstractUnifyRule {
    public static final JoinExtendTrivialRule INSTANCE =
        new JoinExtendTrivialRule();

    public JoinExtendTrivialRule() {
      super(
          operand(MutableJoin.class, query(0), query(1)),
          operand(MutableJoin.class, target(0), target(1)), 2);
    }

    @Override protected SubstitutionVisitor.@Nullable UnifyRuleCall match(
        SubstitutionVisitor visitor, MutableRel query, MutableRel target) {
      return super.match(visitor, query, target);
    }

    @Override protected SubstitutionVisitor.@Nullable UnifyResult apply(
        SubstitutionVisitor.UnifyRuleCall call) {
      MutableJoin query = (MutableJoin) call.query;
      MutableJoin target = (MutableJoin) call.target;
      if (!operatorsEquivalentExceptJoinType(query, target)) {
        return null;
      }
      RexBuilder rexBuilder = call.getCluster().getRexBuilder();
      int leftSize = fieldCnt(target.getLeft());

      JoinRewriteContext joinContext =
          JoinRewriteContext.of(query, target, leftSize, rexBuilder);
      if (!isSupportJoinRewrite(joinContext, leftSize)) {
        return null;
      }

      final List<RexNode> compenProjs = new ArrayList<>();
      for (int i = 0; i < query.rowType.getFieldCount(); i++) {
        RexInputRef rexInputRef =
            rexBuilder.makeInputRef(query.rowType.getFieldList().get(i).getValue(), i);
        compenProjs.add(
            adjustRewriteRowType(rexInputRef, joinContext.getTargetDataTypeList(),
                joinContext.getCandidatePredictCompensateColumn(), rexBuilder));
      }

      final RexNode compenCond =
          addJoinDerivePredicate(rexBuilder, joinContext, null, leftSize);

      RexProgram compensatingRexProgram =
          RexProgram.create(target.rowType, compenProjs, compenCond, query.rowType, rexBuilder);

      final MutableCalc compensatingCalc =
          MutableCalc.of(target, compensatingRexProgram);
      return tryMergeParentCalcAndGenResult(call, compensatingCalc);
    }
  }

  /** Context for join rewrite. */
  protected static class JoinRewriteContext {
    private final JoinRewriteType joinRewriteType;
    private final List<Integer> candidatePredictCompensateColumns;
    private final List<RelDataType> targetDataTypeList;
    private final Pair<List<Integer>, List<Integer>> condEqualsColumns;

    public static JoinRewriteContext of(MutableJoin queryRel,
        MutableJoin targetRel,
        int inputDivision,
        RexBuilder rexBuilder) {
      JoinRewriteType joinRewriteType = JoinRewriteType.of(queryRel.joinType, targetRel.joinType);

      final List<Integer> candidatePredictCompensateColumns =
          inferNullableCompensationColumns(joinRewriteType, targetRel, inputDivision, rexBuilder);

      final List<RelDataType> targetFieldTypeList = targetRel.rowType.getFieldList().stream()
          .map(Map.Entry::getValue)
          .collect(Collectors.toList());

      final Pair<List<Integer>, List<Integer>> condEqualsColumns =
          extractCondEqualColumns(targetRel.condition, inputDivision);

      return new JoinRewriteContext(
          joinRewriteType, candidatePredictCompensateColumns,
          targetFieldTypeList, condEqualsColumns);
    }

    JoinRewriteContext(JoinRewriteType joinRewriteType,
        List<Integer> candidatePredictCompensateColumns,
        List<RelDataType> targetDataTypeList,
        Pair<List<Integer>, List<Integer>> condEqualsColumns) {
      this.joinRewriteType = joinRewriteType;
      this.candidatePredictCompensateColumns = candidatePredictCompensateColumns;
      this.targetDataTypeList = targetDataTypeList;
      this.condEqualsColumns = condEqualsColumns;
    }

    public JoinRewriteType getJoinRewriteType() {
      return joinRewriteType;
    }

    public List<Integer> getCandidatePredictCompensateColumn() {
      return candidatePredictCompensateColumns;
    }
    public List<RelDataType> getTargetDataTypeList() {
      return targetDataTypeList;
    }
    public List<Integer> getLeftCondEqualsColumns() {
      return condEqualsColumns.getKey();
    }
    public List<Integer> getRightCondEqualsColumns() {
      return condEqualsColumns.getValue();
    }
  }

  /**
   * Enumeration of join rewrite types.
   */
  protected enum JoinRewriteType {
    SAME_JOIN,
    INNER_TO_LEFT,
    INNER_TO_RIGHT,
    INNER_TO_FULL,
    LEFT_TO_FULL,
    RIGHT_TO_FULL,
    // Types of rewriting that are not currently supported or cannot be supported
    OTHER;

    public static JoinRewriteType of(JoinRelType queryType, JoinRelType targetType) {
      requireNonNull(queryType, "queryType");
      requireNonNull(targetType, "targetType");
      if (queryType == targetType) {
        return JoinRewriteType.SAME_JOIN;
      }
      if (queryType == INNER) {
        switch (targetType) {
        case LEFT:
          return INNER_TO_LEFT;
        case RIGHT:
          return INNER_TO_RIGHT;
        case FULL:
          return INNER_TO_FULL;
        default:
          return OTHER;
        }
      }
      if (queryType == LEFT && targetType == FULL) {
        return LEFT_TO_FULL;
      }
      if (queryType == RIGHT && targetType == FULL) {
        return RIGHT_TO_FULL;
      }
      return OTHER;
    }
  }

  /**
   * Returns which columns need to be compensated nullable.
   *
   * <p>The null side of the outer join of the join operator
   * affects the nullable attribute of the column,
   * so you need to record the nullable column information generated by the join in advance,
   * and compensate the column attribute after rewriting successfully.
   *
   * @param joinRewriteType Rewrite type of join
   * @param targetRel Rewritten target operator
   * @param inputDivision Boundary between left and right in join operator
   * @param rexBuilder Builder of rex
   * @return index of columns needs to be compensated for nullable
   */
  private static List<Integer> inferNullableCompensationColumns(JoinRewriteType joinRewriteType,
      MutableJoin targetRel, int inputDivision, RexBuilder rexBuilder) {
    int targetFieldSize = targetRel.rowType.getFieldList().size();
    switch (joinRewriteType) {
    case INNER_TO_LEFT:
    case RIGHT_TO_FULL:
      return inferNonNullableFieldsBeforeJoin(targetRel, inputDivision, targetFieldSize,
          inputDivision, rexBuilder);
    case INNER_TO_RIGHT:
    case LEFT_TO_FULL:
      return inferNonNullableFieldsBeforeJoin(
          targetRel, 0, inputDivision, inputDivision, rexBuilder);
    case INNER_TO_FULL:
      return inferNonNullableFieldsBeforeJoin(
          targetRel, 0, targetFieldSize, inputDivision, rexBuilder);
    default:
      return ImmutableList.of();
    }
  }

  /**
   * Returns whether rewriting can be supported based on rewrite type and candidate fields.
   *
   * <p>For example, if the query is an inner join and the target is a left join,
   * we need to ensure that there is a field in the target that is not null in the input
   * for us to compensate for the predicate to make the relNode equivalent
   */
  public static boolean isSupportJoinRewrite(JoinRewriteContext joinContext, int leftSize) {
    // Types of rewriting that are not currently supported or cannot be supported
    if (joinContext.getJoinRewriteType() == JoinRewriteType.OTHER) {
      return false;
    }
    // If not the same joinType, the candidate compensate field must not be null
    if (joinContext.getJoinRewriteType() != JoinRewriteType.SAME_JOIN
        && joinContext.getCandidatePredictCompensateColumn().isEmpty()) {
      return false;
    }
    // Inner to full requires that candidate fields exist in both tables of the join input
    if (joinContext.getJoinRewriteType() == JoinRewriteType.INNER_TO_FULL) {
      List<Integer> leftColumns = joinContext.getCandidatePredictCompensateColumn().stream()
          .filter(a -> a < leftSize)
          .collect(Collectors.toList());
      List<Integer> rightColumns = joinContext.getCandidatePredictCompensateColumn().stream()
          .filter(a -> a >= leftSize)
          .collect(Collectors.toList());
      return !leftColumns.isEmpty() && !rightColumns.isEmpty();
    }
    return true;
  }

  /** Check if join condition only references RexInputRef. */
  public static boolean referenceByMapping(
      RexNode joinCondition, List<RexNode>... projectsOfInputs) {
    List<RexNode> projects = new ArrayList<>();
    for (List<RexNode> projectsOfInput : projectsOfInputs) {
      projects.addAll(projectsOfInput);
    }

    try {
      RexVisitor rexVisitor = new RexVisitorImpl<Void>(true) {
        @Override public Void visitInputRef(RexInputRef inputRef) {
          if (!(projects.get(inputRef.getIndex()) instanceof RexInputRef)) {
            throw Util.FoundOne.NULL;
          }
          return super.visitInputRef(inputRef);
        }
      };
      joinCondition.accept(rexVisitor);
    } catch (Util.FoundOne e) {
      return false;
    }
    return true;
  }
}
