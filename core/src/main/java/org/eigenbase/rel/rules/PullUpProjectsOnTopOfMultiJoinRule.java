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

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;


/**
 * PullUpProjectsOnTopOfMultiJoinRule implements the rule for pulling {@link
 * ProjectRel}s that are on top of a {@link MultiJoinRel} and beneath a {@link
 * JoinRel} so the {@link ProjectRel} appears above the {@link JoinRel}. In the
 * process of doing so, also save away information about the respective fields
 * that are referenced in the expressions in the {@link ProjectRel} we're
 * pulling up, as well as the join condition, in the resultant {@link
 * MultiJoinRel}s
 *
 * <p>For example, if we have the following subselect:
 *
 * <pre>
 *      (select X.x1, Y.y1 from X, Y
 *          where X.x2 = Y.y2 and X.x3 = 1 and Y.y3 = 2)</pre>
 *
 * <p>The {@link MultiJoinRel} associated with (X, Y) associates x1 with X and
 * y1 with Y. Although x3 and y3 need to be read due to the filters, they are
 * not required after the row scan has completed and therefore are not saved.
 * The join fields, x2 and y2, are also tracked separately.
 *
 * <p>Note that by only pulling up projects that are on top of {@link
 * MultiJoinRel}s, we preserve projections on top of row scans.
 *
 * <p>See the superclass for details on restrictions regarding which {@link
 * ProjectRel}s cannot be pulled.
 */
public class PullUpProjectsOnTopOfMultiJoinRule
    extends PullUpProjectsAboveJoinRule
{
    //~ Static fields/initializers ---------------------------------------------

    public static final PullUpProjectsOnTopOfMultiJoinRule
        instanceTwoProjectChildren =
            new PullUpProjectsOnTopOfMultiJoinRule(
                some(
                    JoinRel.class,
                    some(
                        ProjectRel.class,
                        any(MultiJoinRel.class)),
                    some(
                        ProjectRel.class,
                        any(MultiJoinRel.class))),
                "PullUpProjectsOnTopOfMultiJoinRule: with two ProjectRel children");

    public static final PullUpProjectsOnTopOfMultiJoinRule
        instanceLeftProjectChild =
            new PullUpProjectsOnTopOfMultiJoinRule(
                some(
                    JoinRel.class,
                    some(
                        ProjectRel.class,
                        any(MultiJoinRel.class))),
                "PullUpProjectsOnTopOfMultiJoinRule: with ProjectRel on left");

    public static final PullUpProjectsOnTopOfMultiJoinRule
        instanceRightProjectChild =
            new PullUpProjectsOnTopOfMultiJoinRule(
                some(
                    JoinRel.class,
                    any(RelNode.class),
                some(
                    ProjectRel.class,
                    any(MultiJoinRel.class))),
                "PullUpProjectsOnTopOfMultiJoinRule: with ProjectRel on right");

    //~ Constructors -----------------------------------------------------------

    public PullUpProjectsOnTopOfMultiJoinRule(
        RelOptRuleOperand operand,
        String description)
    {
        super(operand, description);
    }

    //~ Methods ----------------------------------------------------------------

    // override PullUpProjectsAboveJoinRule
    protected boolean hasLeftChild(RelOptRuleCall call)
    {
        return (call.rels.length != 4);
    }

    // override PullUpProjectsAboveJoinRule
    protected boolean hasRightChild(RelOptRuleCall call)
    {
        return call.rels.length > 3;
    }

    // override PullUpProjectsAboveJoinRule
    protected ProjectRel getRightChild(RelOptRuleCall call)
    {
        if (call.rels.length == 4) {
            return call.rel(2);
        } else {
            return call.rel(3);
        }
    }

    // override PullUpProjectsAboveJoinRule
    protected RelNode getProjectChild(
        RelOptRuleCall call,
        ProjectRel project,
        boolean leftChild)
    {
        // locate the appropriate MultiJoinRel based on which rule was fired
        // and which projection we're dealing with
        MultiJoinRel multiJoin;
        if (leftChild) {
            multiJoin = call.rel(2);
        } else if (call.rels.length == 4) {
            multiJoin = call.rel(3);
        } else {
            multiJoin = call.rel(4);
        }

        // create a new MultiJoinRel that reflects the columns in the projection
        // above the MultiJoinRel
        return RelOptUtil.projectMultiJoin(multiJoin, project);
    }
}

// End PullUpProjectsOnTopOfMultiJoinRule.java
