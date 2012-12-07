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

    /**
     * Shorthand for {@link RelOptRuleOperand.Dummy#ANY}.
     */
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

    //~ Methods ----------------------------------------------------------------

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
     * @pre matches(call)
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
     * @param rel Relexp to convert
     * @param toTraits desired traits
     *
     * @return a relational expression with the desired traits, or null if no
     * conversion is possible
     *
     * @post return == null || return.getTraits().matches(toTraits)
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
     * Creates a new RelTraitSet based on the given traits and converts the
     * relational expression to that trait set. Clones <code>baseTraits</code>
     * and merges <code>newTraits</code> with the cloned set, then converts rel
     * to that set. Normally, during a rule call, baseTraits are the traits of
     * the rel's parent and newTraits are the traits that the rule wishes to
     * guarantee.
     *
     * @param baseTraits base traits for converted rel
     * @param newTraits altered traits
     * @param rel the rel to convert
     *
     * @return converted rel or null if conversion could not be made
     */
    public static RelNode mergeTraitsAndConvert(
        RelTraitSet baseTraits,
        RelTraitSet newTraits,
        RelNode rel)
    {
        RelTraitSet traits = baseTraits.merge(newTraits);

        return convert(rel, traits);
    }

    /**
     * Creates a new RelTraitSet based on the given traits and converts the
     * relational expression to that trait set. Clones <code>baseTraits</code>
     * and merges <code>newTrait</code> with the cloned set, then converts rel
     * to that set. Normally, during a rule call, baseTraits are the traits of
     * the rel's parent and newTrait is the trait that the rule wishes to
     * guarantee.
     *
     * @param baseTraits base traits for converted rel
     * @param newTrait altered trait
     * @param rel the rel to convert
     *
     * @return converted rel or null if conversion could not be made
     */
    public static RelNode mergeTraitsAndConvert(
        RelTraitSet baseTraits,
        RelTrait newTrait,
        RelNode rel)
    {
        RelTraitSet traits = baseTraits
            .replace(
                newTrait.getTraitDef(), newTrait);

        return convert(rel, traits);
    }

    /**
     * Converts a list of relational expressions.
     *
     * @param trait Trait to apply to each relational expression
     * @param rels Relational expressions
     * @return List of converted relational expressions, or null if any could
     *   not be converted
     */
    public static List<RelNode> convert(
        RelTrait trait,
        List<RelNode> rels)
    {
        ArrayList<RelNode> list = new ArrayList<RelNode>();
        for (RelNode rel : rels) {
            RelNode convertedRel =
                mergeTraitsAndConvert(rel.getTraitSet(), trait, rel);
            if (convertedRel == null) {
                return null;
            }
            list.add(convertedRel);
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
