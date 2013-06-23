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

import java.util.logging.*;
import java.util.regex.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.trace.*;
import org.eigenbase.util.*;


/**
 * A <code>RelOptPlanner</code> is a query optimizer: it transforms a relational
 * expression into a semantically equivalent relational expression, according to
 * a given set of rules and a cost model.
 */
public interface RelOptPlanner
{
    //~ Static fields/initializers ---------------------------------------------

    public static final Logger tracer = EigenbaseTrace.getPlannerTracer();

    //~ Methods ----------------------------------------------------------------

    /**
     * Sets the root node of this query.
     *
     * @param rel Relational expression
     */
    public void setRoot(RelNode rel);

    /**
     * Returns the root node of this query.
     *
     * @return Root node
     */
    public RelNode getRoot();

    /**
     * Registers a rel trait definition. If the {@link RelTraitDef} has already
     * been registered, does nothing.
     *
     * @return whether the RelTraitDef was added, as per {@link
     * java.util.Collection#add}
     */
    public boolean addRelTraitDef(RelTraitDef relTraitDef);

    /**
     * Registers a rule. If the rule has already been registered, does nothing.
     * This method should determine if the given rule is a {@link
     * org.eigenbase.rel.convert.ConverterRule} and pass the ConverterRule to
     * all {@link #addRelTraitDef(RelTraitDef) registered} RelTraitDef
     * instances.
     *
     * @return whether the rule was added, as per {@link
     * java.util.Collection#add}
     */
    public boolean addRule(RelOptRule rule);

    /**
     * Removes a rule.
     *
     * @return true if the rule was present, as per {@link
     * java.util.Collection#remove(Object)}
     */
    boolean removeRule(RelOptRule rule);

    /**
     * Sets the exclusion filter to use for this planner. Rules which match the
     * given pattern will not be fired regardless of whether or when they are
     * added to the planner.
     *
     * @param exclusionFilter pattern to match for exclusion; null to disable
     * filtering
     */
    public void setRuleDescExclusionFilter(Pattern exclusionFilter);

    /**
     * Installs the cancellation-checking flag for this planner. The planner
     * should periodically check this flag and terminate the planning process if
     * it sees a cancellation request.
     *
     * @param cancelFlag flag which the planner should periodically check
     */
    public void setCancelFlag(CancelFlag cancelFlag);

    /**
     * Changes a relational expression to an equivalent one with a different set
     * of traits.
     *
     *
     * @param rel Relational expression, may or may not have been registered
     * @param toTraits Trait set to convert relational expression to
     *
     * @return Relational expression with desired traits. Never null, but may be
     * abstract
     *
     * @pre !rel.getTraits().equals(toTraits)
     * @post return != null
     */
    public RelNode changeTraits(RelNode rel, RelTraitSet toTraits);

    /**
     * Negotiates an appropriate planner to deal with distributed queries. The
     * idea is that the schemas decide among themselves which has the most
     * knowledge. Right now, the local planner retains control.
     */
    public RelOptPlanner chooseDelegate();

    /**
     * Finds the most efficient expression to implement this query.
     *
     * @throws CannotPlanException if cannot find a plan
     */
    public RelNode findBestExp();

    /**
     * Creates a cost object.
     */
    public RelOptCost makeCost(
        double dRows,
        double dCpu,
        double dIo);

    /**
     * Creates a cost object representing an enormous non-infinite cost.
     */
    public RelOptCost makeHugeCost();

    /**
     * Creates a cost object representing infinite cost.
     */
    public RelOptCost makeInfiniteCost();

    /**
     * Creates a cost object representing a small positive cost.
     */
    public RelOptCost makeTinyCost();

    /**
     * Creates a cost object representing zero cost.
     */
    public RelOptCost makeZeroCost();

    /**
     * Computes the cost of a RelNode. In most cases, this just dispatches to
     * {@link RelMetadataQuery#getCumulativeCost}.
     *
     * @param rel expression of interest
     *
     * @return estimated cost
     */
    public RelOptCost getCost(RelNode rel);

    /**
     * Registers a relational expression in the expression bank.
     *
     * <p>After it has been registered, you may not modify it.
     *
     * <p>The expression must not already have been registered. If you are not
     * sure whether it has been registered, call {@link
     * #ensureRegistered(RelNode,RelNode)}.
     *
     * @param rel Relational expression to register (must not already be
     * registered)
     * @param equivRel Relational expression it is equivalent to (may be null)
     *
     * @return the same expression, or an equivalent existing expression
     *
     * @pre !isRegistered(rel)
     */
    public RelNode register(
        RelNode rel,
        RelNode equivRel);

    /**
     * Registers a relational expression if it is not already registered.
     *
     * @param rel Relational expression to register
     * @param equivRel Relational expression it is equivalent to (may be null)
     *
     * @return Registered relational expression
     */
    RelNode ensureRegistered(RelNode rel, RelNode equivRel);

    /**
     * Determines whether a relational expression has been registered.
     *
     *
     * @param rel expression to test
     *
     * @return whether rel has been registered
     */
    public boolean isRegistered(RelNode rel);

    /**
     * Tells this planner that a schema exists. This is the schema's chance to
     * tell the planner about all of the special transformation rules.
     */
    public void registerSchema(RelOptSchema schema);

    /**
     * Adds a listener to this planner.
     *
     * @param newListener new listener to be notified of events
     */
    public void addListener(RelOptListener newListener);

    /**
     * Gives this planner a chance to register one or more {@link
     * RelMetadataProvider}s in the chain which will be used to answer metadata
     * queries. Planners which use their own relational expressions internally
     * to represent concepts such as equivalence classes will generally need to
     * supply corresponding metadata providers.
     *
     * @param chain receives planner's custom providers, if any
     */
    public void registerMetadataProviders(ChainedRelMetadataProvider chain);

    /**
     * Gets a timestamp for a given rel's metadata. This timestamp is used by
     * {@link CachingRelMetadataProvider} to decide whether cached metadata has
     * gone stale.
     *
     * @param rel rel of interest
     *
     * @return timestamp of last change which might affect metadata derivation
     */
    public long getRelMetadataTimestamp(RelNode rel);

    /**
     * Sets the importance of a relational expression.
     *
     * <p>An important use of this method is when a {@link RelOptRule} has
     * created a relational expression which is indisputably better than the
     * original relational expression. The rule set the original relational
     * expression's importance to zero, to reduce the search space. Pending rule
     * calls are cancelled, and future rules will not fire.
     *
     * @param rel Relational expression
     * @param importance Importance
     */
    void setImportance(RelNode rel, double importance);

    /**
     * Registers a class of RelNode. If this class of RelNode has been seen
     * before, does nothing.
     *
     * @param node Relational expression
     */
    void registerClass(RelNode node);

    /**
     * Creates an empty trait set. It contains all registered traits, and the
     * default values of any traits that have them.
     *
     * <p>The empty trait set acts as the prototype (a kind of factory) for all
     * subsequently created trait sets.</p>
     *
     * @return Empty trait set
     */
    RelTraitSet emptyTraitSet();

    /** Thrown by {@link org.eigenbase.relopt.RelOptPlanner#findBestExp()}. */
    class CannotPlanException extends RuntimeException {
        public CannotPlanException(String message) {
            super(message);
        }
    }
}

// End RelOptPlanner.java
