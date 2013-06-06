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

import org.eigenbase.rel.*;


/**
 * A <code>RelOptRule</code> transforms an expression into another. It has a
 * list of {@link RelOptRuleOperand}s, which determine whether the rule can be
 * applied to a particular section of the tree.
 *
 * <p>The optimizer figures out which rules are applicable, then calls {@link
 * #onMatch} on each of them.</p>
 */
public abstract class RelOptRule
{
    //~ Static fields/initializers ---------------------------------------------

    /** Shorthand for {@link RelOptRuleOperand.Dummy#ANY}. */
    public static final RelOptRuleOperand.Dummy ANY =
        RelOptRuleOperand.Dummy.ANY;

    //~ Instance fields --------------------------------------------------------

    /**
     * Description of rule, must be unique within planner. Default is the name
     * of the class sans package name, but derived classes are encouraged to
     * override.
     */
    protected final String description;

    /**
     * Root of operand tree.
     */
    private final RelOptRuleOperand operand;

    /**
     * Flattened list of operands.
     */
    public RelOptRuleOperand [] operands;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a rule.
     *
     * @param operand root operand, must not be null
     */
    public RelOptRule(RelOptRuleOperand operand)
    {
        this(operand, null);
    }

    /**
     * Creates a rule with an explicit description.
     *
     * @param operand root operand, must not be null
     *
     * @param description Description, or null to guess description
     */
    public RelOptRule(RelOptRuleOperand operand, String description)
    {
        assert operand != null;
        this.operand = operand;
        if (description == null) {
            description = guessDescription(getClass().getName());
        }
        this.description = description;
        this.operands = flattenOperands(operand);
        assignSolveOrder();
    }

    //~ Methods for creating operands ------------------------------------------

    /**
     * Creates an operand that matches a relational expression that has no
     * children.
     *
     * @param clazz Class of relational expression to match (must not be null)
     * @return Operand
     */
    public static RelOptRuleOperand leaf(
        Class<? extends RelNode> clazz)
    {
        return leaf(clazz, null);
    }

    /**
     * Creates an operand that matches a relational expression that has no
     * children.
     *
     * @param clazz Class of relational expression to match (must not be null)
     * @param trait Trait to match, or null to match any trait
     * @return Operand
     */
    public static RelOptRuleOperand leaf(
        Class<? extends RelNode> clazz,
        RelTrait trait)
    {
        return new RelOptRuleOperand(
            clazz, trait, RelOptRuleOperand.Dummy.LEAF,
            new RelOptRuleOperand[0]);
    }

    /**
     * Creates an operand that matches a relational expression that has any
     * number of children.
     *
     * @param clazz Class of relational expression to match (must not be null)
     * @return Operand
     */
    public static RelOptRuleOperand any(
        Class<? extends RelNode> clazz)
    {
        return any(clazz, null);
    }

    /**
     * Creates an operand that matches a relational expression that has any
     * number of children.
     *
     * @param clazz Class of relational expression to match (must not be null)
     * @param trait Trait to match, or null to match any trait
     * @return Operand
     */
    public static RelOptRuleOperand any(
        Class<? extends RelNode> clazz,
        RelTrait trait)
    {
        return new RelOptRuleOperand(
            clazz, trait, RelOptRuleOperand.Dummy.ANY, null);
    }

    /**
     * Creates an operand which matches a given trait and matches child operands
     * in the order they appear.
     *
     * @param clazz Class of relational expression to match (must not be null)
     * @param first First child operand
     * @param rest Remaining child operands
     */
    public static RelOptRuleOperand some(
        Class<? extends RelNode> clazz,
        RelOptRuleOperand first,
        RelOptRuleOperand ... rest)
    {
        return some(clazz, null, first, rest);
    }

    /**
     * Creates an operand which matches a given trait and matches child operands
     * in the order they appear.
     *
     * @param clazz Class of relational expression to match (must not be null)
     * @param trait Trait to match, or null to match any trait
     * @param first First child operand
     * @param rest Remaining child operands
     */
    public static RelOptRuleOperand some(
        Class<? extends RelNode> clazz,
        RelTrait trait,
        RelOptRuleOperand first,
        RelOptRuleOperand ... rest)
    {
        return new RelOptRuleOperand(
            clazz, trait, RelOptRuleOperand.Dummy.SOME, array(first, rest));
    }

    /**
     * Creates an operand that matches a relational expression and its children
     * in any order.
     *
     * @param clazz Class of relational expression to match (must not be null)
     * @param first First child operand
     * @param rest Remaining child operands
     * @return Operand
     */
    public static RelOptRuleOperand unordered(
        Class<? extends RelNode> clazz,
        RelOptRuleOperand first,
        RelOptRuleOperand... rest)
    {
        return new RelOptRuleOperand(
            clazz, null, RelOptRuleOperand.Dummy.UNORDERED,
            array(first, rest));
    }

    //~ Methods ----------------------------------------------------------------

    private static RelOptRuleOperand[] array(
        RelOptRuleOperand first, RelOptRuleOperand[] rest)
    {
        assert first != null;
        for (RelOptRuleOperand operand : rest) {
            assert operand != null;
        }
        final RelOptRuleOperand[] operands =
            new RelOptRuleOperand[rest.length + 1];
        operands[0] = first;
        System.arraycopy(rest, 0, operands, 1, rest.length);
        return operands;
    }

    /**
     * Creates a flattened list of this operand and its descendants in prefix
     * order.
     *
     * @param rootOperand Root operand
     *
     * @return Flattened list of operands
     */
    private RelOptRuleOperand [] flattenOperands(
        RelOptRuleOperand rootOperand)
    {
        List<RelOptRuleOperand> operandList =
            new ArrayList<RelOptRuleOperand>();

        // Flatten the operands into a list.
        rootOperand.setRule(this);
        rootOperand.setParent(null);
        rootOperand.ordinalInParent = 0;
        rootOperand.ordinalInRule = operandList.size();
        operandList.add(rootOperand);
        flattenRecurse(operandList, rootOperand);
        return operandList.toArray(new RelOptRuleOperand[operandList.size()]);
    }

    /**
     * Adds the operand and its descendants to the list in prefix order.
     *
     * @param operandList Flattened list of operands
     * @param parentOperand Parent of this operand
     */
    private void flattenRecurse(
        List<RelOptRuleOperand> operandList,
        RelOptRuleOperand parentOperand)
    {
        if (parentOperand.getChildOperands() == null) {
            return;
        }
        int k = 0;
        for (RelOptRuleOperand operand : parentOperand.getChildOperands()) {
            operand.setRule(this);
            operand.setParent(parentOperand);
            operand.ordinalInParent = k++;
            operand.ordinalInRule = operandList.size();
            operandList.add(operand);
            flattenRecurse(operandList, operand);
        }
    }

    /**
     * Builds each operand's solve-order. Start with itself, then its parent, up
     * to the root, then the remaining operands in prefix order.
     */
    private void assignSolveOrder()
    {
        for (RelOptRuleOperand operand : operands) {
            operand.solveOrder = new int[operands.length];
            int m = 0;
            for (RelOptRuleOperand o = operand; o != null; o = o.getParent()) {
                operand.solveOrder[m++] = o.ordinalInRule;
            }
            for (int k = 0; k < operands.length; k++) {
                boolean exists = false;
                for (int n = 0; n < m; n++) {
                    if (operand.solveOrder[n] == k) {
                        exists = true;
                    }
                }
                if (!exists) {
                    operand.solveOrder[m++] = k;
                }
            }

            // Assert: operand appears once in the sort-order.
            assert m == operands.length;
        }
    }

    /**
     * Returns the root operand of this rule
     *
     * @return the root operand of this rule
     */
    public RelOptRuleOperand getOperand()
    {
        return operand;
    }

    /**
     * Returns a flattened list of operands of this rule.
     *
     * @return flattened list of operands
     */
    public List<RelOptRuleOperand> getOperands()
    {
        return Collections.unmodifiableList(Arrays.asList(operands));
    }

    public int hashCode()
    {
        // Conventionally, hashCode() and equals() should use the same
        // criteria, whereas here we only look at the description. This is
        // okay, because the planner requires all rule instances to have
        // distinct descriptions.
        return description.hashCode();
    }

    public boolean equals(Object obj)
    {
        return (obj instanceof RelOptRule)
            && equals((RelOptRule) obj);
    }

    /**
     * Returns whether this rule is equal to another rule.
     *
     * <p>The base implementation checks that the rules have the same class and
     * that the operands are equal; derived classes can override.
     */
    protected boolean equals(RelOptRule that)
    {
        // Include operands and class in the equality criteria just in case
        // they have chosen a poor description.
        return this.description.equals(that.description)
            && (this.getClass() == that.getClass())
            && this.operand.equals(that.operand);
    }

    /**
     * Returns whether this rule could possibly match the given operands.
     *
     * <p>This method is an opportunity to apply side-conditions to a rule. The
     * {@link RelOptPlanner} calls this method after matching all operands of
     * the rule, and before calling {@link #onMatch(RelOptRuleCall)}.
     *
     * <p>In implementations of {@link RelOptPlanner} which may queue up a
     * matched {@link RelOptRuleCall} for a long time before calling {@link
     * #onMatch(RelOptRuleCall)}, this method is beneficial because it allows
     * the planner to discard rules earlier in the process.
     *
     * <p>The default implementation of this method returns <code>true</code>.
     * It is acceptable for any implementation of this method to give a false
     * positives, that is, to say that the rule matches the operands but have
     * {@link #onMatch(RelOptRuleCall)} subsequently not generate any
     * successors.
     *
     * <p>The following script is useful to identify rules which commonly
     * produce no successors. You should override this method for these rules:
     *
     * <blockquote>
     * <pre><code>awk '
     * /Apply rule/ {rule=$4; ruleCount[rule]++;}
     * /generated 0 successors/ {ruleMiss[rule]++;}
     * END {
     *   printf "%-30s %s %s\n", "Rule", "Fire", "Miss";
     *   for (i in ruleCount) {
     *     printf "%-30s %5d %5d\n", i, ruleCount[i], ruleMiss[i];
     *   }
     * } ' FarragoTrace.log</code></pre>
     * </blockquote>
     *
     * @param call Rule call which has been determined to match all operands of
     * this rule
     *
     * @return whether this RelOptRule matches a given RelOptRuleCall
     */
    public boolean matches(RelOptRuleCall call)
    {
        return true;
    }

    /**
     * Receives notification about a rule match. At the time that this method is
     * called, {@link RelOptRuleCall#rels call.rels} holds the set of relational
     * expressions which match the operands to the rule; <code>
     * call.rels[0]</code> is the root expression.
     *
     * <p>Typically a rule would check that the nodes are valid matches, creates
     * a new expression, then calls back {@link RelOptRuleCall#transformTo} to
     * register the expression.</p>
     *
     * @param call Rule call
     *
     * @see #matches(RelOptRuleCall)
     */
    public abstract void onMatch(RelOptRuleCall call);

    /**
     * Returns the convention of the result of firing this rule, null if
     * not known.
     */
    public Convention getOutConvention()
    {
        return null;
    }

    /**
     * Returns the trait which will be modified as a result of firing this rule,
     * or null if the rule is not a converter rule.
     */
    public RelTrait getOutTrait()
    {
        return null;
    }

    public String toString()
    {
        return description;
    }

    /**
     * Converts a relation expression to a give set of traits, if it does not
     * already have those traits. If the conversion is not possible, returns
     * null.
     *
     * @param rel Relational expression to convert
     * @param toTraits desired traits
     *
     * @return a relational expression with the desired traits; never null
     */
    public static RelNode convert(RelNode rel, RelTraitSet toTraits)
    {
        RelOptPlanner planner = rel.getCluster().getPlanner();

        if (rel.getTraitSet().size() < toTraits.size()) {
            new RelTraitPropagationVisitor(planner, toTraits).go(rel);
        }

        RelTraitSet outTraits = rel.getTraitSet();
        for (int i = 0; i < toTraits.size(); i++) {
            RelTrait toTrait = toTraits.getTrait(i);
            if (toTrait != null) {
                outTraits = outTraits.replace(i, toTrait);
            }
        }

        if (rel.getTraitSet().matches(outTraits)) {
            return rel;
        }

        return planner.changeTraits(rel, outTraits);
    }

    /**
     * Converts a list of relational expressions.
     *
     * @param rels Relational expressions
     * @param traitSet Trait set to apply to each relational expression
     * @return List of converted relational expressions, or null if any could
     *   not be converted
     */
    public static List<RelNode> convertList(
        List<RelNode> rels,
        RelTraitSet traitSet)
    {
        final List<RelNode> list = new ArrayList<RelNode>();
        for (RelNode rel : rels) {
            list.add(convert(rel, traitSet));
        }
        return list;
    }

    /**
     * Deduces a name for a rule by taking the name of its class and returning
     * the segment after the last '.' or '$'.
     *
     * <p>Examples:
     * <ul>
     * <li>"com.foo.Bar" yields "Bar";</li>
     * <li>"com.flatten.Bar$Baz" yields "Baz";</li>
     * <li>"com.foo.Bar$1" yields "1" (which as an integer is an invalid
     *      name, and writer of the rule is encouraged to give it an
     *      explicit name).</li>
     * </ul>
     *
     * @param className Name of the rule's class
     *
     * @return Last segment of the class
     */
    static String guessDescription(String className)
    {
        String description = className;
        int punc =
            Math.max(
                className.lastIndexOf('.'),
                className.lastIndexOf('$'));
        if (punc >= 0) {
            description = className.substring(punc + 1);
        }
        if (description.matches("[0-9]+")) {
            throw new RuntimeException(
                "Derived description of rule class " + className
                + " is an integer, not valid. "
                + "Supply a description manually.");
        }
        return description;
    }
}

// End RelOptRule.java
