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
package org.eigenbase.relopt;

import java.util.*;
import java.util.logging.*;

import org.eigenbase.rel.*;
import org.eigenbase.trace.*;


/**
 * A <code>RelOptRuleCall</code> is an invocation of a {@link RelOptRule} with a
 * set of {@link RelNode relational expression}s as arguments.
 */
public abstract class RelOptRuleCall
{
    //~ Static fields/initializers ---------------------------------------------

    protected static final Logger tracer = EigenbaseTrace.getPlannerTracer();

    //~ Instance fields --------------------------------------------------------

    private final RelOptRuleOperand operand0;
    private final Map<RelNode, List<RelNode>> nodeChildren;
    private final RelOptRule rule;
    public final RelNode [] rels;
    private final RelOptPlanner planner;
    private final List<RelNode> parents;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a RelOptRuleCall.
     *
     * @param planner Planner
     * @param operand Root operand
     * @param rels Array of relational expressions which matched each operand
     * @param nodeChildren For each node which matched with <code>
     * matchAnyChildren</code>=true, a list of the node's children
     * @param parents list of parent RelNodes corresponding to the first
     * relational expression in the array argument, if known; otherwise, null
     */
    protected RelOptRuleCall(
        RelOptPlanner planner,
        RelOptRuleOperand operand,
        RelNode [] rels,
        Map<RelNode, List<RelNode>> nodeChildren,
        List<RelNode> parents)
    {
        this.planner = planner;
        this.operand0 = operand;
        this.nodeChildren = nodeChildren;
        this.rule = operand.getRule();
        this.rels = rels;
        this.parents = parents;
        assert (rels.length == rule.operands.length);
    }

    protected RelOptRuleCall(
        RelOptPlanner planner,
        RelOptRuleOperand operand,
        RelNode[] rels,
        Map<RelNode, List<RelNode>> nodeChildren)
    {
        this(planner, operand, rels, nodeChildren, null);
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Returns the root operand matched by this rule.
     *
     * @return root operand
     */
    public RelOptRuleOperand getOperand0()
    {
        return operand0;
    }

    /**
     * Returns the invoked planner rule.
     *
     * @return planner rule
     */
    public RelOptRule getRule()
    {
        return rule;
    }

    /**
     * Returns a list of matched relational expressions.
     *
     * @return matched relational expressions
     */
    public RelNode [] getRels()
    {
        return rels;
    }

    public <T extends RelNode> T rel(int ordinal) {
        return (T) rels[ordinal];
    }

    /**
     * Returns the children of a given relational expression node matched in a
     * rule.
     *
     * <p>If the operand which caused the match has {@link
     * RelOptRuleOperand#matchAnyChildren}=false, the children will have their
     * own operands and therefore be easily available in the array returned by
     * the {@link #getRels} method, so this method returns null.
     *
     * <p>This method is for {@link RelOptRuleOperand#matchAnyChildren}=true,
     * which is generally used when a node can have a variable number of
     * children, and hence where the matched children are not retrievable by any
     * other means.
     *
     * @param rel Relational expression
     *
     * @return Children of relational expression
     */
    public List<RelNode> getChildRels(RelNode rel)
    {
        return nodeChildren.get(rel);
    }

    /**
     * Returns the planner.
     *
     * @return planner
     */
    public RelOptPlanner getPlanner()
    {
        return planner;
    }

    /**
     * @return list of parents of the first relational expression
     */
    public List<RelNode> getParents()
    {
        return parents;
    }

    /**
     * Called by the rule whenever it finds a match. The implementation of this
     * method will guarantee that the original relational expression (e.g.,
     * <code>this.rels[0]</code>) has its traits propagated to the new
     * relational expression (<code>rel</code>) and its unregistered children.
     * Any trait not specifically set in the RelTraitSet returned by <code>
     * rel.getTraits()</code> will be copied from <code>
     * this.rels[0].getTraitSet()</code>.
     */
    public abstract void transformTo(RelNode rel);
}

// End RelOptRuleCall.java
