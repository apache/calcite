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

import org.eigenbase.rel.RelNode;
import org.eigenbase.util.*;


/**
 * A <code>RelOptRuleOperand</code> determines whether a {@link
 * org.eigenbase.relopt.RelOptRule} can be applied to a particular expression.
 *
 * <p>For example, the rule to pull a filter up from the left side of a join
 * takes operands: <code>(Join (Filter) (Any))</code>.</p>
 *
 * <p>Note that <code>children</code> means different things if it is empty or
 * it is <code>null</code>: <code>(Join (Filter <b>()</b>) (Any))</code> means
 * that, to match the rule, <code>Filter</code> must have no operands.</p>
 */
public class RelOptRuleOperand
{
    //~ Enums ------------------------------------------------------------------

    /**
     * Dummy type, containing a single value, for parameters to overloaded forms
     * of the {@link org.eigenbase.relopt.RelOptRuleOperand} constructor
     * signifying operands that will be matched by relational expressions with
     * any number of children.
     */
    enum Dummy {
        /** Signifies that operand can have any number of children. */
        ANY,

        /** Signifies that operand has no children. Therefore it matches a
         * leaf node, such as a table scan or VALUES operator.
         *
         * <p>{@code RelOptRuleOperand(Foo.class, NONE)} is equivalent to
         * {@code RelOptRuleOperand(Foo.class)} but we prefer the former because
         * it is more explicit.</p> */
        LEAF,

        SOME,

        /** Signifies that the rule matches any one of its parents' children.
         * The parent may have one or more children. */
        UNORDERED,
    }

    //~ Instance fields --------------------------------------------------------

    private RelOptRuleOperand parent;
    private RelOptRule rule;

    // REVIEW jvs 29-Aug-2004: some of these are Volcano-specific and should be
    // factored out
    public int [] solveOrder;
    public int ordinalInParent;
    public int ordinalInRule;
    private final RelTrait trait;
    private final Class<? extends RelNode> clazz;
    private final RelOptRuleOperand [] children;

    /** Whether child operands can be matched in any order. */
    public final boolean matchAnyChildren;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates an operand.
     *
     * <p>If <code>children</code> is null, the rule matches regardless of the
     * number of children.
     *
     * <p>If <code>matchAnyChild</code> is true, child operands can be matched
     * in any order. This is useful when matching a relational expression which
     * can have a variable number of children. For example, the rule to
     * eliminate empty children of a Union would have operands
     *
     * <blockquote>Operand(UnionRel, true, Operand(EmptyRel))</blockquote>
     *
     * and given the relational expressions
     *
     * <blockquote>UnionRel(FilterRel, EmptyRel, ProjectRel)</blockquote>
     *
     * would fire the rule with arguments
     *
     * <blockquote>{Union, Empty}</blockquote>
     *
     * It is up to the rule to deduce the other children, or indeed the position
     * of the matched child.</p>
     *
     * @param clazz Class of relational expression to match (must not be null)
     * @param trait Trait to match, or null to match any trait
     * @param children Child operands; or null, meaning match any number of
     * children
     */
    protected RelOptRuleOperand(
        Class<? extends RelNode> clazz,
        RelTrait trait,
        Dummy dummy,
        RelOptRuleOperand[] children)
    {
        assert clazz != null;
        switch (dummy) {
        case ANY:
            this.matchAnyChildren = false;
            assert children == null;
            break;
        case LEAF:
            this.matchAnyChildren = false;
            assert children.length == 0;
            break;
        case UNORDERED:
            this.matchAnyChildren = true;
            assert children.length == 1;
            break;
        default:
            this.matchAnyChildren = false;
            assert children.length == 1;
        }
        this.clazz = clazz;
        this.trait = trait;
        this.children = children;
        if (children != null) {
            for (RelOptRuleOperand child : this.children) {
                assert child.parent == null : "cannot re-use operands";
                child.parent = this;
            }
        }
    }

    /**
     * Creates an operand which matches a given trait and matches child operands
     * in the order they appear.
     *
     * @param clazz Class of relational expression to match (must not be null)
     * @param trait Trait to match, or null to match any trait
     * @param children Child operands; must not be null
     *
     * @deprecated Use {@link RelOptRule#some}
     */
    public RelOptRuleOperand(
        Class<? extends RelNode> clazz,
        RelTrait trait,
        RelOptRuleOperand ... children)
    {
        this(clazz, trait, Dummy.SOME, children);
    }

    /**
     * Creates an operand that matches a given trait and any number of children.
     *
     * @param clazz Class of relational expression to match (must not be null)
     * @param trait Trait to match, or null to match any trait
     * @param dummy Dummy argument to distinguish this constructor from other
     * overloaded forms; must be ANY.
     *
     * @deprecated Use {@link RelOptRule#any(Class, RelTrait)}
     */
    public RelOptRuleOperand(
        Class<? extends RelNode> clazz,
        RelTrait trait,
        Dummy dummy)
    {
        this(clazz, trait, dummy, null);
        assert dummy == Dummy.ANY;
    }

    /**
     * Creates an operand that matches child operands in the order they appear.
     *
     * <p>There must be at least one child operand. If your rule is intended
     * to match a relational expression that has no children, use
     * {@code RelOptRuleOperand(Class, NONE)}.
     *
     * @param clazz Class of relational expression to match (must not be null)
     * @param children Child operands; must not be null
     *
     * @deprecated Use {@link RelOptRule#some}
     */
    public RelOptRuleOperand(
        Class<? extends RelNode> clazz,
        RelOptRuleOperand ... children)
    {
        this(clazz, null, Dummy.SOME, children);
        assert children != null;
    }

    /**
     * Creates an operand that matches any number of children.
     *
     * <p>Example:</p>
     * <li><code>RelOptRuleOperand(X.class, ANY)</code> is equivalent to
     *     <code>RelOptRuleOperand(X.class, null)</code></li>
     * <li><code>RelOptRuleOperand(X.class, NONE)</code> is equivalent to
     *     <code>RelOptRuleOperand(X.class, new RelOptRuleOperand[0])</code>
     * </li>
     *
     * @param clazz Class of relational expression to match (must not be null)
     * @param dummy Dummy argument to distinguish this constructor from other
     * overloaded forms. Must be ANY.
     *
     * @deprecated Use {@link RelOptRule#any} or {@link RelOptRule#leaf}
     */
    public RelOptRuleOperand(
        Class<? extends RelNode> clazz,
        Dummy dummy)
    {
        this(clazz, null, dummy, null);
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Returns the parent operand.
     *
     * @return parent operand
     */
    public RelOptRuleOperand getParent()
    {
        return parent;
    }

    /**
     * Sets the parent operand.
     *
     * @param parent Parent operand
     */
    public void setParent(RelOptRuleOperand parent)
    {
        this.parent = parent;
    }

    /**
     * Returns the rule this operand belongs to.
     *
     * @return containing rule
     */
    public RelOptRule getRule()
    {
        return rule;
    }

    /**
     * Sets the rule this operand belongs to
     *
     * @param rule containing rule
     */
    public void setRule(RelOptRule rule)
    {
        this.rule = rule;
    }

    public int hashCode()
    {
        int h = clazz.hashCode();
        h = Util.hash(
            h,
            trait.hashCode());
        h = Util.hashArray(h, children);
        return h;
    }

    public boolean equals(Object obj)
    {
        if (!(obj instanceof RelOptRuleOperand)) {
            return false;
        }
        RelOptRuleOperand that = (RelOptRuleOperand) obj;

        boolean equalTraits =
            (this.trait != null) ? this.trait.equals(that.trait)
            : (that.trait == null);

        return (this.clazz == that.clazz)
            && equalTraits
            && Arrays.equals(this.children, that.children);
    }

    /**
     * @return relational expression class matched by this operand
     */
    public Class<? extends RelNode> getMatchedClass()
    {
        return clazz;
    }

    /**
     * Returns the child operands.
     *
     * @return child operands
     */
    public RelOptRuleOperand [] getChildOperands()
    {
        return children;
    }

    /**
     * Returns whether a relational expression matches this operand. It must be
     * of the right class and trait.
     */
    public boolean matches(RelNode rel)
    {
        if (!clazz.isInstance(rel)) {
            return false;
        }
        if ((trait != null) && !rel.getTraitSet().contains(trait)) {
            return false;
        }
        return true;
    }
}

// End RelOptRuleOperand.java
