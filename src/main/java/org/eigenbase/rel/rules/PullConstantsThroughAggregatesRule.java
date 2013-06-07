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
import org.eigenbase.util.mapping.*;


/**
 * PullConstantsThroughAggregatesRule removes constant expressions from the
 * group list of an {@link AggregateRel}.
 *
 * <h4>Effect of the rule</h4>
 *
 * <p>Since the transformed relational expression has to match the original
 * relational expression, the constants are placed in a projection above the
 * reduced aggregate. If those constants are not used, another rule will remove
 * them from the project.
 *
 * <p>AggregateRel needs its group columns to be on the prefix of its input
 * relational expression. Therefore, if a constant is not on the trailing edge
 * of the group list, removing it will leave a hole. In this case, the rule adds
 * a project before the aggregate to reorder the columns, and permutes them back
 * afterwards.
 */
public class PullConstantsThroughAggregatesRule
    extends RelOptRule
{
    //~ Static fields/initializers ---------------------------------------------

    /**
     * The singleton.
     */
    public static final PullConstantsThroughAggregatesRule instance =
        new PullConstantsThroughAggregatesRule();

    //~ Constructors -----------------------------------------------------------

    /**
     * Private: use singleton
     */
    private PullConstantsThroughAggregatesRule()
    {
        super(
            some(
                AggregateRel.class,
                any(CalcRel.class)));
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        AggregateRel aggregate = call.rel(0);
        CalcRel child = call.rel(1);
        final RexProgram program = child.getProgram();

        final RelDataType childRowType = child.getRowType();
        final int groupCount = aggregate.getGroupSet().cardinality();
        IntList constantList = new IntList();
        Map<Integer, RexNode> constants = new HashMap<Integer, RexNode>();
        for (int i : Util.toIter(aggregate.getGroupSet())) {
            final RexLocalRef ref = program.getProjectList().get(i);
            if (program.isConstant(ref)) {
                constantList.add(i);
                constants.put(
                    i,
                    program.gatherExpr(ref));
            }
        }

        // None of the group expressions are constant. Nothing to do.
        if (constantList.size() == 0) {
            return;
        }
        final int newGroupCount = groupCount - constantList.size();
        final RelNode newAggregate;

        // If the constants are on the trailing edge of the group list, we just
        // reduce the group count.
        if (constantList.get(0) == newGroupCount) {
            // Clone aggregate calls.
            final List<AggregateCall> newAggCalls =
                new ArrayList<AggregateCall>();
            for (AggregateCall aggCall : aggregate.getAggCallList()) {
                newAggCalls.add(
                    new AggregateCall(
                        aggCall.getAggregation(),
                        aggCall.isDistinct(),
                        aggCall.getArgList(),
                        aggCall.getType(),
                        aggCall.getName()));
            }
            newAggregate =
                new AggregateRel(
                    aggregate.getCluster(),
                    child,
                    Util.bitSetBetween(0, newGroupCount), newAggCalls);
        } else {
            // Create the mapping from old field positions to new field
            // positions.
            final Permutation mapping =
                new Permutation(childRowType.getFieldCount());
            mapping.identity();

            // Ensure that the first positions in the mapping are for the new
            // group columns.
            for (
                int i = 0, groupOrdinal = 0, constOrdinal = newGroupCount;
                i < groupCount;
                ++i)
            {
                if (i >= groupCount) {
                    mapping.set(i, i);
                } else if (constants.containsKey(i)) {
                    mapping.set(i, constOrdinal++);
                } else {
                    mapping.set(i, groupOrdinal++);
                }
            }

            // Create a projection to permute fields into these positions.
            final RelNode project = createProjection(mapping, child);

            // Adjust aggregate calls for new field positions.
            final List<AggregateCall> newAggCalls =
                new ArrayList<AggregateCall>();
            for (AggregateCall aggCall : aggregate.getAggCallList()) {
                final int argCount = aggCall.getArgList().size();
                final List<Integer> args = new ArrayList<Integer>(argCount);
                for (int j = 0; j < argCount; j++) {
                    final Integer arg = aggCall.getArgList().get(j);
                    args.add(mapping.getTarget(arg));
                }
                newAggCalls.add(
                    new AggregateCall(
                        aggCall.getAggregation(),
                        aggCall.isDistinct(),
                        args,
                        aggCall.getType(),
                        aggCall.getName()));
            }

            // Aggregate on projection.
            newAggregate =
                new AggregateRel(
                    aggregate.getCluster(),
                    project,
                    Util.bitSetBetween(0, newGroupCount), newAggCalls);
        }

        // Create a projection back again.
        List<RexNode> exprList = new ArrayList<RexNode>();
        List<String> nameList = new ArrayList<String>();
        final RelDataType aggregateRowType = aggregate.getRowType();
        for (int i = 0, source = 0; i < aggregateRowType.getFieldCount(); ++i) {
            RexNode expr;
            if (i >= groupCount) {
                // Aggregate expressions' names and positions are unchanged.
                expr =
                    RelOptUtil.createInputRef(
                        newAggregate,
                        i - constantList.size());
            } else if (constantList.contains(i)) {
                // Re-generate the constant expression in the project.
                expr = constants.get(i);
            } else {
                // Project the aggregation expression, in its original
                // position.
                expr = RelOptUtil.createInputRef(newAggregate, source);
                ++source;
            }
            exprList.add(expr);
            nameList.add(aggregateRowType.getFields()[i].getName());
        }
        final RelNode inverseProject =
            CalcRel.createProject(newAggregate, exprList, nameList);

        call.transformTo(inverseProject);
    }

    /**
     * Creates a projection which permutes the fields of a given relational
     * expression.
     *
     * <p>For example, given a relational expression [A, B, C, D] and a mapping
     * [2:1, 3:0], returns a projection [$3 AS C, $2 AS B].
     *
     * @param mapping Mapping to apply to source columns
     * @param child Relational expression
     *
     * @return Relational expressions with permutation applied
     */
    private static RelNode createProjection(
        final Mapping mapping,
        RelNode child)
    {
        // Every target has precisely one source; every source has at most
        // one target.
        assert mapping.getMappingType().isA(MappingType.InverseSurjection);
        final RelDataType childRowType = child.getRowType();
        assert mapping.getSourceCount() == childRowType.getFieldCount();
        final int targetCount = mapping.getTargetCount();
        List<RexNode> exprList = new ArrayList<RexNode>(targetCount);
        List<String> nameList = new ArrayList<String>(targetCount);
        for (int target = 0; target < targetCount; ++target) {
            int source = mapping.getSource(target);
            exprList.add(RelOptUtil.createInputRef(child, source));
            nameList.add(childRowType.getFields()[source].getName());
        }
        return CalcRel.createProject(child, exprList, nameList);
    }
}

// End PullConstantsThroughAggregatesRule.java
