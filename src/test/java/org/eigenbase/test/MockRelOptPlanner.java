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
package org.eigenbase.test;

import java.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;


/**
 * MockRelOptPlanner is a mock implementation of the {@link RelOptPlanner}
 * interface.
 */
public class MockRelOptPlanner
    extends AbstractRelOptPlanner
{
    //~ Instance fields --------------------------------------------------------

    private RelNode root;

    private RelOptRule rule;

    private RelNode transformationResult;

    //~ Methods ----------------------------------------------------------------

    // implement RelOptPlanner
    public void setRoot(RelNode rel)
    {
        this.root = rel;
    }

    // implement RelOptPlanner
    public RelNode getRoot()
    {
        return root;
    }

    // implement RelOptPlanner
    public boolean addRule(RelOptRule rule)
    {
        assert this.rule == null
            : "MockRelOptPlanner only supports a single rule";
        this.rule = rule;

        return false;
    }

    // implement RelOptPlanner
    public boolean removeRule(RelOptRule rule)
    {
        return false;
    }

    // implement RelOptPlanner
    public RelNode changeTraits(RelNode rel, RelTraitSet toTraits)
    {
        return rel;
    }

    // implement RelOptPlanner
    public RelNode findBestExp()
    {
        if (rule != null) {
            matchRecursive(root, null, -1);
        }
        return root;
    }

    /**
     * Recursively matches a rule.
     *
     *
     *
     * @param rel Relational expression
     * @param parent Parent relational expression
     * @param ordinalInParent Ordinal of relational expression among its
     * siblings
     *
     * @return whether match occurred
     */
    private boolean matchRecursive(
        RelNode rel,
        RelNode parent,
        int ordinalInParent)
    {
        List<RelNode> bindings = new ArrayList<RelNode>();
        if (match(
                rule.getOperand(),
                rel,
                bindings))
        {
            MockRuleCall call =
                new MockRuleCall(
                    this,
                    rule.getOperand(),
                    bindings.toArray(new RelNode[bindings.size()]));
            if (rule.matches(call)) {
                rule.onMatch(call);
            }
        }

        if (transformationResult != null) {
            if (parent == null) {
                root = transformationResult;
            } else {
                parent.replaceInput(ordinalInParent, transformationResult);
            }
            return true;
        }

        List<? extends RelNode> children = rel.getInputs();
        for (int i = 0; i < children.size(); ++i) {
            if (matchRecursive(children.get(i), rel, i)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Matches a relational expression to a rule.
     *
     * @param operand Root operand of rule
     * @param rel Relational expression
     * @param bindings Bindings, populated on successful match
     *
     * @return whether relational expression matched rule
     */
    private boolean match(
        RelOptRuleOperand operand,
        RelNode rel,
        List<RelNode> bindings)
    {
        if (!operand.matches(rel)) {
            return false;
        }
        bindings.add(rel);
        RelOptRuleOperand [] childOperands = operand.getChildOperands();
        if (childOperands == null) {
            return true;
        }
        int n = childOperands.length;
        List<? extends RelNode> childRels = rel.getInputs();
        if (n != childRels.size()) {
            return false;
        }
        for (int i = 0; i < n; ++i) {
            if (!match(
                    childOperands[i],
                    childRels.get(i),
                    bindings))
            {
                return false;
            }
        }
        return true;
    }

    // implement RelOptPlanner
    public RelNode register(
        RelNode rel,
        RelNode equivRel)
    {
        return rel;
    }

    // implement RelOptPlanner
    public RelNode ensureRegistered(RelNode rel, RelNode equivRel)
    {
        return rel;
    }

    // implement RelOptPlanner
    public boolean isRegistered(RelNode rel)
    {
        return true;
    }

    //~ Inner Classes ----------------------------------------------------------

    private class MockRuleCall
        extends RelOptRuleCall
    {
        /**
         * Creates a MockRuleCall.
         *
         * @param planner Planner
         * @param operand Operand
         * @param rels List of matched relational expressions
         */
        MockRuleCall(
            RelOptPlanner planner,
            RelOptRuleOperand operand,
            RelNode [] rels)
        {
            super(
                planner,
                operand,
                rels,
                Collections.<RelNode, List<RelNode>>emptyMap());
        }

        // implement RelOptRuleCall
        public void transformTo(RelNode rel)
        {
            transformationResult = rel;
        }
    }
}

// End MockRelOptPlanner.java
