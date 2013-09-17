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
import org.eigenbase.rex.*;
import org.eigenbase.sql.fun.*;
import org.eigenbase.util.*;


/**
 * Rule which converts a {@link JoinRel} into a {@link CorrelatorRel}, which can
 * then be implemented using nested loops.
 *
 * <p>For example,
 *
 * <blockquote><code>select * from emp join dept on emp.deptno =
 * dept.deptno</code></blockquote>
 *
 * becomes a CorrelatorRel which restarts TableAccessRel("DEPT") for each row
 * read from TableAccessRel("EMP").</p>
 *
 * <p>This rule is not applicable if for certain types of outer join. For
 * example,
 *
 * <blockquote><code>select * from emp right join dept on emp.deptno =
 * dept.deptno</code></blockquote>
 *
 * would require emitting a NULL emp row if a certain department contained no
 * employees, and CorrelatorRel cannot do that.</p>
 */
public class NestedLoopsJoinRule
    extends RelOptRule
{
    //~ Static fields/initializers ---------------------------------------------

    public static final NestedLoopsJoinRule instance =
        new NestedLoopsJoinRule();

    //~ Constructors -----------------------------------------------------------

    /**
     * Private constructor; use singleton {@link #instance}.
     */
    private NestedLoopsJoinRule()
    {
        super(any(JoinRel.class));
    }

    //~ Methods ----------------------------------------------------------------

    public boolean matches(RelOptRuleCall call)
    {
        JoinRel join = call.rel(0);
        switch (join.getJoinType()) {
        case INNER:
        case LEFT:
            return true;
        case FULL:
        case RIGHT:
            return false;
        default:
            throw Util.unexpected(join.getJoinType());
        }
    }

    public void onMatch(RelOptRuleCall call)
    {
        assert matches(call);
        final JoinRel join = call.rel(0);
        final List<Integer> leftKeys = new ArrayList<Integer>();
        final List<Integer> rightKeys = new ArrayList<Integer>();
        RelNode right = join.getRight();
        final RelNode left = join.getLeft();
        RexNode remainingCondition =
            RelOptUtil.splitJoinCondition(
                left,
                right,
                join.getCondition(),
                leftKeys,
                rightKeys);
        assert leftKeys.size() == rightKeys.size();
        final List<CorrelatorRel.Correlation> correlationList =
            new ArrayList<CorrelatorRel.Correlation>();
        if (leftKeys.size() > 0) {
            final RelOptCluster cluster = join.getCluster();
            final RexBuilder rexBuilder = cluster.getRexBuilder();
            RexNode condition = null;
            for (Pair<Integer, Integer> p : Pair.zip(leftKeys, rightKeys)) {
                final String dyn_inIdStr = cluster.getQuery().createCorrel();
                final int dyn_inId = RelOptQuery.getCorrelOrdinal(dyn_inIdStr);

                // Create correlation to say 'each row, set variable #id
                // to the value of column #leftKey'.
                correlationList.add(
                    new CorrelatorRel.Correlation(
                        dyn_inId,
                        p.left));
                condition =
                    RelOptUtil.andJoinFilters(
                        rexBuilder,
                        condition,
                        rexBuilder.makeCall(
                            SqlStdOperatorTable.equalsOperator,
                            rexBuilder.makeInputRef(
                                right.getRowType().getFieldList().get(
                                    p.right).getType(),
                                p.right),
                            rexBuilder.makeCorrel(
                                left.getRowType().getFieldList().get(
                                    p.left).getType(),
                                dyn_inIdStr)));
            }
            right =
                CalcRel.createFilter(
                    right,
                    condition);
        }
        RelNode newRel =
            new CorrelatorRel(
                join.getCluster(),
                left,
                right,
                remainingCondition,
                correlationList,
                join.getJoinType());
        call.transformTo(newRel);
    }
}

// End NestedLoopsJoinRule.java
