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


/**
 * Collection of rules which remove sections of a query plan known never to
 * produce any rows.
 *
 * @author Julian Hyde
 * @version $Id$
 * @see EmptyRel
 */
public abstract class RemoveEmptyRule
    extends RelOptRule
{
    //~ Static fields/initializers ---------------------------------------------

    /**
     * Singleton instance of rule which removes empty children of a {@link
     * UnionRel}.
     *
     * <p>Examples:
     *
     * <ul>
     * <li>Union(Rel, Empty, Rel2) becomes Union(Rel, Rel2)
     * <li>Union(Rel, Empty, Empty) becomes Rel
     * <li>Union(Empty, Empty) becomes Empty
     * </ul>
     */
    public static final RemoveEmptyRule unionInstance =
        new RemoveEmptyRule(
            unordered(
                UnionRel.class,
                leaf(EmptyRel.class)),
            "Union")
        {
            public void onMatch(RelOptRuleCall call)
            {
                UnionRel union = (UnionRel) call.rels[0];
                final List<RelNode> childRels = call.getChildRels(union);
                final List<RelNode> newChildRels = new ArrayList<RelNode>();
                for (RelNode childRel : childRels) {
                    if (!(childRel instanceof EmptyRel)) {
                        newChildRels.add(childRel);
                    }
                }
                assert newChildRels.size() < childRels.size()
                    : "planner promised us at least one EmptyRel child";
                RelNode newRel;
                switch (newChildRels.size()) {
                case 0:
                    newRel =
                        new EmptyRel(
                            union.getCluster(),
                            union.getRowType());
                    break;
                case 1:
                    newRel =
                        RelOptUtil.createCastRel(
                            newChildRels.get(0),
                            union.getRowType(),
                            true);
                    break;
                default:
                    newRel =
                        new UnionRel(
                            union.getCluster(),
                            newChildRels,
                            union.all);
                    break;
                }
                call.transformTo(newRel);
            }
        };

    /**
     * Singleton instance of rule which converts a {@link ProjectRel} to empty
     * if its child is empty.
     *
     * <p>Examples:
     *
     * <ul>
     * <li>Project(Empty) becomes Empty
     * </ul>
     */
    public static final RemoveEmptyRule projectInstance =
        new RemoveEmptyRule(
            new RelOptRuleOperand(
                ProjectRel.class,
                (RelTrait) null,
                leaf(EmptyRel.class)),
            "Project")
        {
            public void onMatch(RelOptRuleCall call)
            {
                ProjectRel project = (ProjectRel) call.rels[0];
                call.transformTo(
                    new EmptyRel(
                        project.getCluster(),
                        project.getRowType()));
            }
        };

    /**
     * Singleton instance of rule which converts a {@link FilterRel} to empty if
     * its child is empty.
     *
     * <p>Examples:
     *
     * <ul>
     * <li>Filter(Empty) becomes Empty
     * </ul>
     */
    public static final RemoveEmptyRule filterInstance =
        new RemoveEmptyRule(
            new RelOptRuleOperand(
                FilterRel.class,
                (RelTrait) null,
                leaf(EmptyRel.class)),
            "Filter")
        {
            public void onMatch(RelOptRuleCall call)
            {
                FilterRel filter = (FilterRel) call.rels[0];
                call.transformTo(
                    new EmptyRel(
                        filter.getCluster(),
                        filter.getRowType()));
            }
        };

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a RemoveEmptyRule.
     *
     * @param operand Operand
     * @param desc Description
     */
    private RemoveEmptyRule(RelOptRuleOperand operand, String desc)
    {
        super(operand, "RemoveEmptyRule:" + desc);
    }
}

// End RemoveEmptyRule.java
