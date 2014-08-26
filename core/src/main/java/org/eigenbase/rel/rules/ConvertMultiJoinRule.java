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
package org.eigenbase.rel.rules;

import java.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.util.Pair;

/**
 * Rule to flatten a tree of {@link JoinRel}s into a single {@link MultiJoinRel}
 * with N inputs. An input is not flattened if the input is a null generating
 * input in an outer join, i.e., either input in a full outer join, the right
 * hand side of a left outer join, or the left hand side of a right outer join.
 *
 * <p>Join conditions are also pulled up from the inputs into the topmost
 * {@link MultiJoinRel},
 * unless the input corresponds to a null generating input in an outer join,
 *
 * <p>Outer join information is also stored in the {@link MultiJoinRel}. A
 * boolean flag indicates if the join is a full outer join, and in the case of
 * left and right outer joins, the join type and outer join conditions are
 * stored in arrays in the {@link MultiJoinRel}. This outer join information is
 * associated with the null generating input in the outer join. So, in the case
 * of a a left outer join between A and B, the information is associated with B,
 * not A.
 *
 * <p>Here are examples of the {@link MultiJoinRel}s constructed after this rule
 * has been applied on following join trees.
 *
 * <pre>
 * A JOIN B &rarr; MJ(A, B)
 * A JOIN B JOIN C &rarr; MJ(A, B, C)
 * A LEFTOUTER B &rarr; MJ(A, B), left outer join on input#1
 * A RIGHTOUTER B &rarr; MJ(A, B), right outer join on input#0
 * A FULLOUTER B &rarr; MJ[full](A, B)
 * A LEFTOUTER (B JOIN C) &rarr; MJ(A, MJ(B, C))), left outer join on input#1 in
 * the outermost MultiJoinRel
 * (A JOIN B) LEFTOUTER C &rarr; MJ(A, B, C), left outer join on input#2
 * A LEFTOUTER (B FULLOUTER C) &rarr; MJ(A, MJ[full](B, C)), left outer join on
 *      input#1 in the outermost MultiJoinRel
 * (A LEFTOUTER B) FULLOUTER (C RIGHTOUTER D) &rarr;
 *      MJ[full](MJ(A, B), MJ(C, D)), left outer join on input #1 in the first
 *      inner MultiJoinRel and right outer join on input#0 in the second inner
 *      MultiJoinRel
 * </pre>
 *
 * <p>The constructor is parameterized to allow any sub-class of
 * {@link JoinRelBase}, not just {@link JoinRel}.</p>
 */
public class ConvertMultiJoinRule extends RelOptRule {
  public static final ConvertMultiJoinRule INSTANCE =
      new ConvertMultiJoinRule(JoinRel.class);

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a ConvertMultiJoinRule.
   */
  public ConvertMultiJoinRule(Class<? extends JoinRelBase> clazz) {
    super(
        operand(clazz,
            operand(RelNode.class, any()),
            operand(RelNode.class, any())));
  }

  //~ Methods ----------------------------------------------------------------

  public void onMatch(RelOptRuleCall call) {
    final JoinRelBase origJoin = call.rel(0);

    final RelNode left = call.rel(1);
    final RelNode right = call.rel(2);

    // combine the children MultiJoinRel inputs into an array of inputs
    // for the new MultiJoinRel
    List<BitSet> projFieldsList = new ArrayList<BitSet>();
    List<int[]> joinFieldRefCountsList = new ArrayList<int[]>();
    List<RelNode> newInputs =
        combineInputs(
            origJoin,
            left,
            right,
            projFieldsList,
            joinFieldRefCountsList);

    // combine the outer join information from the left and right
    // inputs, and include the outer join information from the current
    // join, if it's a left/right outer join
    final List<Pair<JoinRelType, RexNode>> joinSpecs =
        new ArrayList<Pair<JoinRelType, RexNode>>();
    combineOuterJoins(
        origJoin,
        newInputs,
        left,
        right,
        joinSpecs);

    List<RexNode> newOuterJoinConds = Pair.right(joinSpecs);
    List<JoinRelType> joinTypes = Pair.left(joinSpecs);

    // pull up the join filters from the children MultiJoinRels and
    // combine them with the join filter associated with this JoinRel to
    // form the join filter for the new MultiJoinRel
    RexNode newJoinFilter = combineJoinFilters(origJoin, left, right);

    // add on the join field reference counts for the join condition
    // associated with this JoinRel
    Map<Integer, int[]> newJoinFieldRefCountsMap =
        new HashMap<Integer, int[]>();
    addOnJoinFieldRefCounts(newInputs,
        origJoin.getRowType().getFieldCount(),
        origJoin.getCondition(),
        joinFieldRefCountsList,
        newJoinFieldRefCountsMap);

    RexNode newPostJoinFilter =
        combinePostJoinFilters(origJoin, left, right);

    RelNode multiJoin =
        new MultiJoinRel(
            origJoin.getCluster(),
            newInputs,
            newJoinFilter,
            origJoin.getRowType(),
            origJoin.getJoinType() == JoinRelType.FULL,
            newOuterJoinConds,
            joinTypes,
            projFieldsList,
            newJoinFieldRefCountsMap,
            newPostJoinFilter);

    call.transformTo(multiJoin);
  }

  /**
   * Combines the inputs into a JoinRel into an array of inputs.
   *
   * @param join                   original join
   * @param left                   left input into join
   * @param right                  right input into join
   * @param projFieldsList         returns a list of the new combined projection
   *                               fields
   * @param joinFieldRefCountsList returns a list of the new combined join
   *                               field reference counts
   * @return combined left and right inputs in an array
   */
  private List<RelNode> combineInputs(
      JoinRelBase join,
      RelNode left,
      RelNode right,
      List<BitSet> projFieldsList,
      List<int[]> joinFieldRefCountsList) {
    // leave the null generating sides of an outer join intact; don't
    // pull up those children inputs into the array we're constructing
    int nInputs;
    int nInputsOnLeft;
    final MultiJoinRel leftMultiJoin;
    JoinRelType joinType = join.getJoinType();
    boolean combineLeft = canCombine(left, joinType.generatesNullsOnLeft());
    if (combineLeft) {
      leftMultiJoin = (MultiJoinRel) left;
      nInputs = left.getInputs().size();
      nInputsOnLeft = nInputs;
    } else {
      leftMultiJoin = null;
      nInputs = 1;
      nInputsOnLeft = 1;
    }
    final MultiJoinRel rightMultiJoin;
    boolean combineRight =
        canCombine(right, joinType.generatesNullsOnRight());
    if (combineRight) {
      rightMultiJoin = (MultiJoinRel) right;
      nInputs += right.getInputs().size();
    } else {
      rightMultiJoin = null;
      nInputs += 1;
    }

    List<RelNode> newInputs = new ArrayList<RelNode>();
    int i = 0;
    if (combineLeft) {
      for (; i < left.getInputs().size(); i++) {
        newInputs.add(leftMultiJoin.getInput(i));
        projFieldsList.add(leftMultiJoin.getProjFields().get(i));
        joinFieldRefCountsList.add(
            leftMultiJoin.getJoinFieldRefCountsMap().get(i));
      }
    } else {
      newInputs.add(left);
      i = 1;
      projFieldsList.add(null);
      joinFieldRefCountsList.add(
          new int[left.getRowType().getFieldCount()]);
    }
    if (combineRight) {
      for (; i < nInputs; i++) {
        newInputs.add(rightMultiJoin.getInput(i - nInputsOnLeft));
        projFieldsList.add(
            rightMultiJoin.getProjFields().get(i - nInputsOnLeft));
        joinFieldRefCountsList.add(
            rightMultiJoin.getJoinFieldRefCountsMap().get(
                i - nInputsOnLeft));
      }
    } else {
      newInputs.add(right);
      projFieldsList.add(null);
      joinFieldRefCountsList.add(
          new int[right.getRowType().getFieldCount()]);
    }

    return newInputs;
  }

  /**
   * Combines the outer join conditions and join types from the left and right
   * join inputs. If the join itself is either a left or right outer join,
   * then the join condition corresponding to the join is also set in the
   * position corresponding to the null-generating input into the join. The
   * join type is also set.
   *
   * @param joinRel        join rel
   * @param combinedInputs the combined inputs to the join
   * @param left           left child of the joinrel
   * @param right          right child of the joinrel
   * @param joinSpecs      the list where the join types and conditions will be
   *                       copied
   */
  private void combineOuterJoins(
      JoinRelBase joinRel,
      List<RelNode> combinedInputs,
      RelNode left,
      RelNode right,
      List<Pair<JoinRelType, RexNode>> joinSpecs) {
    JoinRelType joinType = joinRel.getJoinType();
    boolean leftCombined =
        canCombine(left, joinType.generatesNullsOnLeft());
    boolean rightCombined =
        canCombine(right, joinType.generatesNullsOnRight());
    switch (joinType) {
    case LEFT:
      if (leftCombined) {
        copyOuterJoinInfo(
            (MultiJoinRel) left,
            joinSpecs,
            0,
            null,
            null);
      } else {
        joinSpecs.add(Pair.of(JoinRelType.INNER, (RexNode) null));
      }
      joinSpecs.add(Pair.of(joinType, joinRel.getCondition()));
      break;
    case RIGHT:
      joinSpecs.add(Pair.of(joinType, joinRel.getCondition()));
      if (rightCombined) {
        copyOuterJoinInfo(
            (MultiJoinRel) right,
            joinSpecs,
            left.getRowType().getFieldCount(),
            right.getRowType().getFieldList(),
            joinRel.getRowType().getFieldList());
      } else {
        joinSpecs.add(Pair.of(JoinRelType.INNER, (RexNode) null));
      }
      break;
    default:
      if (leftCombined) {
        copyOuterJoinInfo(
            (MultiJoinRel) left,
            joinSpecs,
            0,
            null,
            null);
      } else {
        joinSpecs.add(Pair.of(JoinRelType.INNER, (RexNode) null));
      }
      if (rightCombined) {
        copyOuterJoinInfo(
            (MultiJoinRel) right,
            joinSpecs,
            left.getRowType().getFieldCount(),
            right.getRowType().getFieldList(),
            joinRel.getRowType().getFieldList());
      } else {
        joinSpecs.add(Pair.of(JoinRelType.INNER, (RexNode) null));
      }
    }
  }

  /**
   * Copies outer join data from a source MultiJoinRel to a new set of arrays.
   * Also adjusts the conditions to reflect the new position of an input if
   * that input ends up being shifted to the right.
   *
   * @param multiJoinRel     the source MultiJoinRel
   * @param destJoinSpecs    the list where the join types and conditions will
   *                         be copied
   * @param adjustmentAmount if &gt; 0, the amount the RexInputRefs in the join
   *                         conditions need to be adjusted by
   * @param srcFields        the source fields that the original join conditions
   *                         are referencing
   * @param destFields       the destination fields that the new join conditions
   */
  private void copyOuterJoinInfo(
      MultiJoinRel multiJoinRel,
      List<Pair<JoinRelType, RexNode>> destJoinSpecs,
      int adjustmentAmount,
      List<RelDataTypeField> srcFields,
      List<RelDataTypeField> destFields) {
    final List<Pair<JoinRelType, RexNode>> srcJoinSpecs =
        Pair.zip(
            multiJoinRel.getJoinTypes(),
            multiJoinRel.getOuterJoinConditions());

    if (adjustmentAmount == 0) {
      destJoinSpecs.addAll(srcJoinSpecs);
    } else {
      int nFields = srcFields.size();
      int[] adjustments = new int[nFields];
      for (int idx = 0; idx < nFields; idx++) {
        adjustments[idx] = adjustmentAmount;
      }
      for (Pair<JoinRelType, RexNode> src
          : srcJoinSpecs) {
        destJoinSpecs.add(
            Pair.of(
                src.left,
                src.right == null
                    ? null
                    : src.right.accept(
                        new RelOptUtil.RexInputConverter(
                            multiJoinRel.getCluster().getRexBuilder(),
                            srcFields, destFields, adjustments))));
      }
    }
  }

  /**
   * Combines the join filters from the left and right inputs (if they are
   * MultiJoinRels) with the join filter in the joinrel into a single AND'd
   * join filter, unless the inputs correspond to null generating inputs in an
   * outer join
   *
   * @param joinRel join rel
   * @param left    left child of the join
   * @param right   right child of the join
   * @return combined join filters AND-ed together
   */
  private RexNode combineJoinFilters(
      JoinRelBase joinRel,
      RelNode left,
      RelNode right) {
    RexBuilder rexBuilder = joinRel.getCluster().getRexBuilder();
    JoinRelType joinType = joinRel.getJoinType();

    // first need to adjust the RexInputs of the right child, since
    // those need to shift over to the right
    RexNode rightFilter = null;
    if (canCombine(right, joinType.generatesNullsOnRight())) {
      MultiJoinRel multiJoin = (MultiJoinRel) right;
      rightFilter =
          shiftRightFilter(
              joinRel,
              left,
              multiJoin,
              multiJoin.getJoinFilter());
    }

    // AND the join condition if this isn't a left or right outer join;
    // in those cases, the outer join condition is already tracked
    // separately
    RexNode newFilter = null;
    if ((joinType != JoinRelType.LEFT) && (joinType != JoinRelType.RIGHT)) {
      newFilter = joinRel.getCondition();
    }
    if (canCombine(left, joinType.generatesNullsOnLeft())) {
      RexNode leftFilter = ((MultiJoinRel) left).getJoinFilter();
      newFilter =
          RelOptUtil.andJoinFilters(
              rexBuilder,
              newFilter,
              leftFilter);
    }
    newFilter =
        RelOptUtil.andJoinFilters(
            rexBuilder,
            newFilter,
            rightFilter);

    return newFilter;
  }

  /**
   * @param input          input into a join
   * @param nullGenerating true if the input is null generating
   * @return true if the input can be combined into a parent MultiJoinRel
   */
  private boolean canCombine(RelNode input, boolean nullGenerating) {
    return input instanceof MultiJoinRel
        && !((MultiJoinRel) input).isFullOuterJoin()
        && !nullGenerating;
  }

  /**
   * Shifts a filter originating from the right child of the JoinRel to the
   * right, to reflect the filter now being applied on the resulting
   * MultiJoinRel.
   *
   * @param joinRel     the original JoinRel
   * @param left        the left child of the JoinRel
   * @param right       the right child of the JoinRel
   * @param rightFilter the filter originating from the right child
   * @return the adjusted right filter
   */
  private RexNode shiftRightFilter(
      JoinRelBase joinRel,
      RelNode left,
      MultiJoinRel right,
      RexNode rightFilter) {
    if (rightFilter == null) {
      return null;
    }

    int nFieldsOnLeft = left.getRowType().getFieldList().size();
    int nFieldsOnRight = right.getRowType().getFieldList().size();
    int[] adjustments = new int[nFieldsOnRight];
    for (int i = 0; i < nFieldsOnRight; i++) {
      adjustments[i] = nFieldsOnLeft;
    }
    rightFilter =
        rightFilter.accept(
            new RelOptUtil.RexInputConverter(
                joinRel.getCluster().getRexBuilder(),
                right.getRowType().getFieldList(),
                joinRel.getRowType().getFieldList(),
                adjustments));
    return rightFilter;
  }

  /**
   * Adds on to the existing join condition reference counts the references
   * from the new join condition.
   *
   * @param multiJoinInputs          inputs into the new MultiJoinRel
   * @param nTotalFields             total number of fields in the MultiJoinRel
   * @param joinCondition            the new join condition
   * @param origJoinFieldRefCounts   existing join condition reference counts
   * @param newJoinFieldRefCountsMap map containing the new join condition
   */
  private void addOnJoinFieldRefCounts(
      List<RelNode> multiJoinInputs,
      int nTotalFields,
      RexNode joinCondition,
      List<int[]> origJoinFieldRefCounts,
      Map<Integer, int[]> newJoinFieldRefCountsMap) {
    // count the input references in the join condition
    int[] joinCondRefCounts = new int[nTotalFields];
    joinCondition.accept(new InputReferenceCounter(joinCondRefCounts));

    // first, make a copy of the ref counters
    int nInputs = multiJoinInputs.size();
    int currInput = 0;
    for (int[] origRefCounts : origJoinFieldRefCounts) {
      newJoinFieldRefCountsMap.put(
          currInput,
          origRefCounts.clone());
      currInput++;
    }

    // add on to the counts for each input into the MultiJoinRel the
    // reference counts computed for the current join condition
    currInput = -1;
    int startField = 0;
    int nFields = 0;
    for (int i = 0; i < nTotalFields; i++) {
      if (joinCondRefCounts[i] == 0) {
        continue;
      }
      while (i >= (startField + nFields)) {
        startField += nFields;
        currInput++;
        assert currInput < nInputs;
        nFields =
            multiJoinInputs.get(currInput).getRowType().getFieldCount();
      }
      int[] refCounts = newJoinFieldRefCountsMap.get(currInput);
      refCounts[i - startField] += joinCondRefCounts[i];
    }
  }

  /**
   * Combines the post-join filters from the left and right inputs (if they
   * are MultiJoinRels) into a single AND'd filter.
   *
   * @param joinRel the original JoinRel
   * @param left    left child of the JoinRel
   * @param right   right child of the JoinRel
   * @return combined post-join filters AND'd together
   */
  private RexNode combinePostJoinFilters(
      JoinRelBase joinRel,
      RelNode left,
      RelNode right) {
    RexNode rightPostJoinFilter = null;
    if (right instanceof MultiJoinRel) {
      rightPostJoinFilter =
          shiftRightFilter(
              joinRel,
              left,
              (MultiJoinRel) right,
              ((MultiJoinRel) right).getPostJoinFilter());
    }

    RexNode leftPostJoinFilter = null;
    if (left instanceof MultiJoinRel) {
      leftPostJoinFilter = ((MultiJoinRel) left).getPostJoinFilter();
    }

    if ((leftPostJoinFilter == null) && (rightPostJoinFilter == null)) {
      return null;
    } else {
      return RelOptUtil.andJoinFilters(
          joinRel.getCluster().getRexBuilder(),
          leftPostJoinFilter,
          rightPostJoinFilter);
    }
  }

  //~ Inner Classes ----------------------------------------------------------

  /**
   * Visitor that keeps a reference count of the inputs used by an expression.
   */
  private class InputReferenceCounter extends RexVisitorImpl<Void> {
    private final int[] refCounts;

    public InputReferenceCounter(int[] refCounts) {
      super(true);
      this.refCounts = refCounts;
    }

    public Void visitInputRef(RexInputRef inputRef) {
      refCounts[inputRef.getIndex()]++;
      return null;
    }
  }
}

// End ConvertMultiJoinRule.java
