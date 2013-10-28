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
import org.eigenbase.util.*;

import com.google.common.collect.ImmutableList;


/**
 * <code>SwapJoinRule</code> permutes the inputs to a join. Outer joins cannot
 * be permuted.
 */
public class SwapJoinRule
    extends RelOptRule
{
    //~ Static fields/initializers ---------------------------------------------

    /**
     * The singleton
     */
    public static final SwapJoinRule instance = new SwapJoinRule();

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a SwapJoinRule.
     */
    private SwapJoinRule()
    {
        super(any(JoinRel.class));
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Returns a relational expression with the inputs switched round. Does not
     * modify <code>join</code>. Returns null if the join cannot be swapped (for
     * example, because it is an outer join).
     */
    public static RelNode swap(JoinRel join)
    {
        return swap(join, false);
    }

    /**
     * @param join join to be swapped
     * @param swapOuterJoins whether outer joins should be swapped
     *
     * @return swapped join if swapping possible; else null
     */
    public static RelNode swap(JoinRel join, boolean swapOuterJoins)
    {
        JoinRelType joinType = join.getJoinType();
        switch (joinType) {
        case LEFT:
            if (!swapOuterJoins) {
                return null;
            }
            joinType = JoinRelType.RIGHT;
            break;
        case RIGHT:
            if (!swapOuterJoins) {
                return null;
            }
            joinType = JoinRelType.LEFT;
            break;
        }
        final RexBuilder rexBuilder = join.getCluster().getRexBuilder();
        final RelDataType leftRowType = join.getLeft().getRowType();
        final RelDataType rightRowType = join.getRight().getRowType();
        final VariableReplacer variableReplacer =
            new VariableReplacer(rexBuilder, leftRowType, rightRowType);
        final RexNode oldCondition = join.getCondition().clone();
        RexNode condition = variableReplacer.go(oldCondition);

        // NOTE jvs 14-Mar-2006: We preserve attribute semiJoinDone after the
        // swap.  This way, we will generate one semijoin for the original
        // join, and one for the swapped join, and no more.  This
        // doesn't prevent us from seeing any new combinations assuming
        // that the planner tries the desired order (semijoins after swaps).

        JoinRel newJoin =
            new JoinRel(
                join.getCluster(),
                join.getRight(),
                join.getLeft(),
                condition,
                joinType,
                join.getVariablesStopped(),
                join.isSemiJoinDone(),
                join.getSystemFieldList());
        final List<RexNode> exps =
            RelOptUtil.createSwappedJoinExprs(newJoin, join, true);
        return CalcRel.createProject(
            newJoin,
            exps,
            join.getRowType().getFieldNames(),
            true);
    }

    public void onMatch(final RelOptRuleCall call)
    {
        JoinRel join = call.rel(0);

        if (!join.getSystemFieldList().isEmpty()) {
            // FIXME Enable this rule for joins with system fields
            return;
        }

        final RelNode swapped = swap(join);
        if (swapped == null) {
            return;
        }

        // The result is either a Project or, if the project is trivial, a
        // raw Join.
        final JoinRel newJoin =
            (swapped instanceof JoinRel) ? (JoinRel) swapped
            : (JoinRel) swapped.getInput(0);

        call.transformTo(swapped);

        // We have converted join='a join b' into swapped='select
        // a0,a1,a2,b0,b1 from b join a'. Now register that project='select
        // b0,b1,a0,a1,a2 from (select a0,a1,a2,b0,b1 from b join a)' is the
        // same as 'b join a'. If we didn't do this, the swap join rule
        // would fire on the new join, ad infinitum.
        final List<RexNode> exps =
            RelOptUtil.createSwappedJoinExprs(newJoin, join, false);
        RelNode project =
            CalcRel.createProject(
                swapped,
                exps,
                newJoin.getRowType().getFieldNames());

        // Make sure extra traits are carried over from the original rel
        project =
            RelOptRule.convert(
                project,
                swapped.getTraitSet());

        RelNode rel = call.getPlanner().ensureRegistered(project, newJoin);
        Util.discard(rel);
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * Walks over an expression, replacing references to fields of the left and
     * right inputs.
     *
     * <p>If the field index is less than leftFieldCount, it must be from the
     * left, and so has rightFieldCount added to it; if the field index is
     * greater than leftFieldCount, it must be from the right, so we subtract
     * leftFieldCount from it.</p>
     */
    private static class VariableReplacer
    {
        private final RexBuilder rexBuilder;
        private final List<RelDataTypeField> leftFields;
        private final List<RelDataTypeField> rightFields;

        VariableReplacer(
            RexBuilder rexBuilder,
            RelDataType leftType,
            RelDataType rightType)
        {
            this.rexBuilder = rexBuilder;
            this.leftFields = leftType.getFieldList();
            this.rightFields = rightType.getFieldList();
        }

        public RexNode go(RexNode rex)
        {
            if (rex instanceof RexCall) {
                ImmutableList.Builder<RexNode> builder =
                    ImmutableList.builder();
                final RexCall call = (RexCall) rex;
                for (RexNode operand : call.operands) {
                    builder.add(go(operand));
                }
                return call.clone(call.getType(), builder.build());
            } else if (rex instanceof RexInputRef) {
                RexInputRef var = (RexInputRef) rex;
                int index = var.getIndex();
                if (index < leftFields.size()) {
                    // Field came from left side of join. Move it to the right.
                    return rexBuilder.makeInputRef(
                        leftFields.get(index).getType(),
                        rightFields.size() + index);
                }
                index -= leftFields.size();
                if (index < rightFields.size()) {
                    // Field came from right side of join. Move it to the left.
                    return rexBuilder.makeInputRef(
                        rightFields.get(index).getType(),
                        index);
                }
                throw Util.newInternal(
                    "Bad field offset: index="
                    + var.getIndex()
                    + ", leftFieldCount=" + leftFields.size()
                    + ", rightFieldCount=" + rightFields.size());
            } else {
                return rex;
            }
        }
    }
}

// End SwapJoinRule.java
