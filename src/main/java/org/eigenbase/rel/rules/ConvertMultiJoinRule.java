/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package org.eigenbase.rel.rules;

import java.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;


/**
 * Rule to flatten a tree of {@link JoinRel}s into a single {@link MultiJoinRel}
 * with N inputs. An input is not flattened if the input is a null generating
 * input in an outer join, i.e., either input in a full outer join, the right
 * hand side of a left outer join, or the left hand side of a right outer join.
 *
 * <p>Join conditions are also pulled up from the inputs into the topmost {@link
 * MultiJoinRel}, unless the input corresponds to a null generating input in an
 * outer join,
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
 * A JOIN B -> MJ(A, B)
 * A JOIN B JOIN C -> MJ(A, B, C)
 * A LEFTOUTER B -> MJ(A, B), left outer join on input#1
 * A RIGHTOUTER B -> MJ(A, B), right outer join on input#0
 * A FULLOUTER B -> MJ[full](A, B)
 * A LEFTOUTER (B JOIN C) -> MJ(A, MJ(B, C))), left outer join on input#1 in
 * the outermost MultiJoinRel
 * (A JOIN B) LEFTOUTER C -> MJ(A, B, C), left outer join on input#2
 * A LEFTOUTER (B FULLOUTER C) -> MJ(A, MJ[full](B, C)), left outer join on
 *      input#1 in the outermost MultiJoinRel
 * (A LEFTOUTER B) FULLOUTER (C RIGHTOUTER D) ->
 *      MJ[full](MJ(A, B), MJ(C, D)), left outer join on input #1 in the first
 *      inner MultiJoinRel and right outer join on input#0 in the second inner
 *      MultiJoinRel
 * </pre>
 */
public class ConvertMultiJoinRule
    extends RelOptRule
{
    public static final ConvertMultiJoinRule instance =
        new ConvertMultiJoinRule();

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a ConvertMultiJoinRule.
     */
    private ConvertMultiJoinRule()
    {
        super(
            some(
                JoinRel.class, any(RelNode.class), any(RelNode.class)));
    }

    //~ Methods ----------------------------------------------------------------

    public void onMatch(RelOptRuleCall call)
    {
        JoinRel origJoinRel = call.rel(0);

        RelNode left = call.rel(1);
        RelNode right = call.rel(2);

        // combine the children MultiJoinRel inputs into an array of inputs
        // for the new MultiJoinRel
        List<BitSet> projFieldsList = new ArrayList<BitSet>();
        List<int[]> joinFieldRefCountsList = new ArrayList<int[]>();
        List<RelNode> newInputs =
            combineInputs(
                origJoinRel,
                left,
                right,
                projFieldsList,
                joinFieldRefCountsList);

        // combine the outer join information from the left and right
        // inputs, and include the outer join information from the current
        // join, if it's a left/right outer join
        RexNode [] newOuterJoinConds = new RexNode[newInputs.size()];
        JoinRelType [] joinTypes = new JoinRelType[newInputs.size()];
        combineOuterJoins(
            origJoinRel,
            newInputs,
            left,
            right,
            newOuterJoinConds,
            joinTypes);

        // pull up the join filters from the children MultiJoinRels and
        // combine them with the join filter associated with this JoinRel to
        // form the join filter for the new MultiJoinRel
        RexNode newJoinFilter = combineJoinFilters(origJoinRel, left, right);

        // add on the join field reference counts for the join condition
        // associated with this JoinRel
        Map<Integer, int[]> newJoinFieldRefCountsMap =
            new HashMap<Integer, int[]>();
        addOnJoinFieldRefCounts(
            newInputs,
            origJoinRel.getRowType().getFieldCount(),
            origJoinRel.getCondition(),
            joinFieldRefCountsList,
            newJoinFieldRefCountsMap);

        RexNode newPostJoinFilter =
            combinePostJoinFilters(origJoinRel, left, right);

        RelNode multiJoin =
            new MultiJoinRel(
                origJoinRel.getCluster(),
                newInputs,
                newJoinFilter,
                origJoinRel.getRowType(),
                (origJoinRel.getJoinType() == JoinRelType.FULL),
                newOuterJoinConds,
                joinTypes,
                projFieldsList.toArray(new BitSet[projFieldsList.size()]),
                newJoinFieldRefCountsMap,
                newPostJoinFilter);

        call.transformTo(multiJoin);
    }

    /**
     * Combines the inputs into a JoinRel into an array of inputs.
     *
     * @param join original join
     * @param left left input into join
     * @param right right input into join
     * @param projFieldsList returns a list of the new combined projection
     * fields
     * @param joinFieldRefCountsList returns a list of the new combined join
     * field reference counts
     *
     * @return combined left and right inputs in an array
     */
    private List<RelNode> combineInputs(
        JoinRel join,
        RelNode left,
        RelNode right,
        List<BitSet> projFieldsList,
        List<int[]> joinFieldRefCountsList)
    {
        // leave the null generating sides of an outer join intact; don't
        // pull up those children inputs into the array we're constructing
        int nInputs;
        int nInputsOnLeft;
        MultiJoinRel leftMultiJoin = null;
        JoinRelType joinType = join.getJoinType();
        boolean combineLeft = canCombine(left, joinType.generatesNullsOnLeft());
        if (combineLeft) {
            leftMultiJoin = (MultiJoinRel) left;
            nInputs = left.getInputs().size();
            nInputsOnLeft = nInputs;
        } else {
            nInputs = 1;
            nInputsOnLeft = 1;
        }
        MultiJoinRel rightMultiJoin = null;
        boolean combineRight =
            canCombine(right, joinType.generatesNullsOnRight());
        if (combineRight) {
            rightMultiJoin = (MultiJoinRel) right;
            nInputs += right.getInputs().size();
        } else {
            nInputs += 1;
        }

        RelNode [] newInputs = new RelNode[nInputs];
        int i = 0;
        if (combineLeft) {
            for (; i < left.getInputs().size(); i++) {
                newInputs[i] = leftMultiJoin.getInput(i);
                projFieldsList.add(((MultiJoinRel) left).getProjFields()[i]);
                joinFieldRefCountsList.add(
                    ((MultiJoinRel) left).getJoinFieldRefCountsMap().get(i));
            }
        } else {
            newInputs[0] = left;
            i = 1;
            projFieldsList.add(null);
            joinFieldRefCountsList.add(
                new int[left.getRowType().getFieldCount()]);
        }
        if (combineRight) {
            for (; i < nInputs; i++) {
                newInputs[i] = rightMultiJoin.getInput(i - nInputsOnLeft);
                projFieldsList.add(
                    ((MultiJoinRel) right).getProjFields()[i - nInputsOnLeft]);
                joinFieldRefCountsList.add(
                    ((MultiJoinRel) right).getJoinFieldRefCountsMap().get(
                        i - nInputsOnLeft));
            }
        } else {
            newInputs[i] = right;
            projFieldsList.add(null);
            joinFieldRefCountsList.add(
                new int[right.getRowType().getFieldCount()]);
        }

        return Arrays.asList(newInputs);
    }

    /**
     * Combines the outer join conditions and join types from the left and right
     * join inputs. If the join itself is either a left or right outer join,
     * then the join condition corresponding to the join is also set in the
     * position corresponding to the null-generating input into the join. The
     * join type is also set.
     *
     *
     * @param joinRel join rel
     * @param combinedInputs the combined inputs to the join
     * @param left left child of the joinrel
     * @param right right child of the joinrel
     * @param combinedConds the array containing the combined join conditions
     * @param joinTypes the array containing the combined join types
     *
     * @return combined join filters AND'd together
     */
    private RexNode [] combineOuterJoins(
        JoinRel joinRel,
        List<RelNode> combinedInputs,
        RelNode left,
        RelNode right,
        RexNode [] combinedConds,
        JoinRelType [] joinTypes)
    {
        JoinRelType joinType = joinRel.getJoinType();
        int nCombinedInputs = combinedInputs.size();
        boolean leftCombined =
            canCombine(left, joinType.generatesNullsOnLeft());
        boolean rightCombined =
            canCombine(right, joinType.generatesNullsOnRight());
        if (joinType == JoinRelType.LEFT) {
            if (leftCombined) {
                copyOuterJoinInfo(
                    (MultiJoinRel) left,
                    combinedConds,
                    joinTypes,
                    0,
                    0,
                    null,
                    null);
            } else {
                joinTypes[0] = JoinRelType.INNER;
            }
            combinedConds[nCombinedInputs - 1] = joinRel.getCondition();
            joinTypes[nCombinedInputs - 1] = joinType;
        } else if (joinType == JoinRelType.RIGHT) {
            if (rightCombined) {
                copyOuterJoinInfo(
                    (MultiJoinRel) right,
                    combinedConds,
                    joinTypes,
                    1,
                    left.getRowType().getFieldCount(),
                    right.getRowType().getFields(),
                    joinRel.getRowType().getFields());
            } else {
                joinTypes[nCombinedInputs - 1] = JoinRelType.INNER;
            }
            combinedConds[0] = joinRel.getCondition();
            joinTypes[0] = joinType;
        } else {
            int nInputsLeft;
            if (leftCombined) {
                nInputsLeft = left.getInputs().size();
                copyOuterJoinInfo(
                    (MultiJoinRel) left,
                    combinedConds,
                    joinTypes,
                    0,
                    0,
                    null,
                    null);
            } else {
                nInputsLeft = 1;
                joinTypes[0] = JoinRelType.INNER;
            }
            if (rightCombined) {
                copyOuterJoinInfo(
                    (MultiJoinRel) right,
                    combinedConds,
                    joinTypes,
                    nInputsLeft,
                    left.getRowType().getFieldCount(),
                    right.getRowType().getFields(),
                    joinRel.getRowType().getFields());
            } else {
                joinTypes[nInputsLeft] = JoinRelType.INNER;
            }
        }

        return combinedConds;
    }

    /**
     * Copies outer join data from a source MultiJoinRel to a new set of arrays.
     * Also adjusts the conditions to reflect the new position of an input if
     * that input ends up being shifted to the right.
     *
     * @param multiJoinRel the source MultiJoinRel
     * @param destConds the array where the join conditions will be copied
     * @param destJoinTypes the array where the join types will be copied
     * @param destPos starting position in the array where the copying starts
     * @param adjustmentAmount if > 0, the amount the RexInputRefs in the join
     * conditions need to be adjusted by
     * @param srcFields the source fields that the original join conditions are
     * referencing
     * @param destFields the destination fields that the new join conditions
     * will be referencing
     */
    private void copyOuterJoinInfo(
        MultiJoinRel multiJoinRel,
        RexNode [] destConds,
        JoinRelType [] destJoinTypes,
        int destPos,
        int adjustmentAmount,
        RelDataTypeField [] srcFields,
        RelDataTypeField [] destFields)
    {
        RexNode [] srcConds = multiJoinRel.getOuterJoinConditions();
        JoinRelType [] srcJoinTypes = multiJoinRel.getJoinTypes();
        RexBuilder rexBuilder = multiJoinRel.getCluster().getRexBuilder();

        int len = srcConds.length;
        System.arraycopy(srcJoinTypes, 0, destJoinTypes, destPos, len);

        if (adjustmentAmount == 0) {
            System.arraycopy(srcConds, 0, destConds, 0, len);
        } else {
            int nFields = srcFields.length;
            int [] adjustments = new int[nFields];
            for (int idx = 0; idx < nFields; idx++) {
                adjustments[idx] = adjustmentAmount;
            }
            for (int i = 0; i < len; i++) {
                if (srcConds[i] != null) {
                    destConds[i + destPos] =
                        srcConds[i].accept(
                            new RelOptUtil.RexInputConverter(
                                rexBuilder,
                                srcFields,
                                destFields,
                                adjustments));
                }
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
     * @param left left child of the joinrel
     * @param right right child of the joinrel
     *
     * @return combined join filters AND'd together
     */
    private RexNode combineJoinFilters(
        JoinRel joinRel,
        RelNode left,
        RelNode right)
    {
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
     * @param input input into a join
     * @param nullGenerating true if the input is null generating
     *
     * @return true if the input can be combined into a parent MultiJoinRel
     */
    private boolean canCombine(RelNode input, boolean nullGenerating)
    {
        return ((input instanceof MultiJoinRel)
            && !((MultiJoinRel) input).isFullOuterJoin()
            && !nullGenerating);
    }

    /**
     * Shifts a filter originating from the right child of the JoinRel to the
     * right, to reflect the filter now being applied on the resulting
     * MultiJoinRel.
     *
     * @param joinRel the original JoinRel
     * @param left the left child of the JoinRel
     * @param right the right child of the JoinRel
     * @param rightFilter the filter originating from the right child
     *
     * @return the adjusted right filter
     */
    private RexNode shiftRightFilter(
        JoinRel joinRel,
        RelNode left,
        MultiJoinRel right,
        RexNode rightFilter)
    {
        if (rightFilter == null) {
            return null;
        }

        int nFieldsOnLeft = left.getRowType().getFields().length;
        int nFieldsOnRight = right.getRowType().getFields().length;
        int [] adjustments = new int[nFieldsOnRight];
        for (int i = 0; i < nFieldsOnRight; i++) {
            adjustments[i] = nFieldsOnLeft;
        }
        rightFilter =
            rightFilter.accept(
                new RelOptUtil.RexInputConverter(
                    joinRel.getCluster().getRexBuilder(),
                    right.getRowType().getFields(),
                    joinRel.getRowType().getFields(),
                    adjustments));
        return rightFilter;
    }

    /**
     * Adds on to the existing join condition reference counts the references
     * from the new join condition.
     *
     * @param multiJoinInputs inputs into the new MultiJoinRel
     * @param nTotalFields total number of fields in the MultiJoinRel
     * @param joinCondition the new join condition
     * @param origJoinFieldRefCounts existing join condition reference counts
     * @param newJoinFieldRefCountsMap map containing the new join condition
     */
    private void addOnJoinFieldRefCounts(
        List<RelNode> multiJoinInputs,
        int nTotalFields,
        RexNode joinCondition,
        List<int[]> origJoinFieldRefCounts,
        Map<Integer, int[]> newJoinFieldRefCountsMap)
    {
        // count the input references in the join condition
        int [] joinCondRefCounts = new int[nTotalFields];
        joinCondition.accept(new InputReferenceCounter(joinCondRefCounts));

        // first, make a copy of the ref counters
        int nInputs = multiJoinInputs.size();
        int currInput = 0;
        for (int [] origRefCounts : origJoinFieldRefCounts) {
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
                assert (currInput < nInputs);
                nFields =
                    multiJoinInputs.get(currInput).getRowType().getFieldCount();
            }
            int [] refCounts = newJoinFieldRefCountsMap.get(currInput);
            refCounts[i - startField] += joinCondRefCounts[i];
        }
    }

    /**
     * Combines the post-join filters from the left and right inputs (if they
     * are MultiJoinRels) into a single AND'd filter.
     *
     * @param joinRel the original JoinRel
     * @param left left child of the JoinRel
     * @param right right child of the JoinRel
     *
     * @return combined post-join filters AND'd together
     */
    private RexNode combinePostJoinFilters(
        JoinRel joinRel,
        RelNode left,
        RelNode right)
    {
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
    private class InputReferenceCounter
        extends RexVisitorImpl<Void>
    {
        private final int [] refCounts;

        public InputReferenceCounter(int [] refCounts)
        {
            super(true);
            this.refCounts = refCounts;
        }

        public Void visitInputRef(RexInputRef inputRef)
        {
            refCounts[inputRef.getIndex()]++;
            return null;
        }
    }
}

// End ConvertMultiJoinRule.java
