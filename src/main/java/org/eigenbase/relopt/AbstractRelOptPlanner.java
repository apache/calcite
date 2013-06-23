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
import java.util.regex.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.resource.*;
import org.eigenbase.util.*;


/**
 * AbstractRelOptPlanner is an abstract base for implementations of the {@link
 * RelOptPlanner} interface.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class AbstractRelOptPlanner
    implements RelOptPlanner
{
    //~ Static fields/initializers ---------------------------------------------

    /**
     * Regular expression for integer.
     */
    private static final Pattern IntegerPattern = Pattern.compile("[0-9]+");

    //~ Instance fields --------------------------------------------------------

    /**
     * Maps rule description to rule, just to ensure that rules' descriptions
     * are unique.
     */
    private final Map<String, RelOptRule> mapDescToRule;

    private MulticastRelOptListener listener;

    private Pattern ruleDescExclusionFilter;

    private CancelFlag cancelFlag;

    private final Set<Class<? extends RelNode>> classes =
        new HashSet<Class<? extends RelNode>>();

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates an AbstractRelOptPlanner.
     */
    protected AbstractRelOptPlanner()
    {
        mapDescToRule = new HashMap<String, RelOptRule>();

        // In case no one calls setCancelFlag, set up a
        // dummy here.
        cancelFlag = new CancelFlag();
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptPlanner
    public void setCancelFlag(CancelFlag cancelFlag)
    {
        this.cancelFlag = cancelFlag;
    }

    /**
     * Checks to see whether cancellation has been requested, and if so, throws
     * an exception.
     */
    public void checkCancel()
    {
        if (cancelFlag.isCancelRequested()) {
            throw EigenbaseResource.instance().PreparationAborted.ex();
        }
    }

    /**
     * Registers a rule's description.
     *
     * @param rule Rule
     */
    protected void mapRuleDescription(RelOptRule rule)
    {
        // Check that there isn't a rule with the same description,
        // also validating description string.

        final String description = rule.toString();
        assert description != null;
        assert description.indexOf("$") < 0
            : "Rule's description should not contain '$': "
            + description;
        assert !IntegerPattern.matcher(description).matches()
            : "Rule's description should not be an integer: "
            + rule.getClass().getName() + ", " + description;

        RelOptRule existingRule = mapDescToRule.put(description, rule);
        if (existingRule != null) {
            if (existingRule == rule) {
                throw new AssertionError(
                    "Rule should not already be registered");
            } else {
                // This rule has the same description as one previously
                // registered, yet it is not equal. You may need to fix the
                // rule's equals and hashCode methods.
                throw new AssertionError(
                    "Rule's description should be unique; "
                    + "existing rule=" + existingRule + "; new rule=" + rule);
            }
        }
    }

    /**
     * Removes the mapping between a rule and its description.
     *
     * @param rule Rule
     */
    protected void unmapRuleDescription(RelOptRule rule)
    {
        String description = rule.toString();
        mapDescToRule.remove(description);
    }

    /**
     * Returns the rule with a given description
     *
     * @param description Description
     *
     * @return Rule with given description, or null if not found
     */
    protected RelOptRule getRuleByDescription(String description)
    {
        return mapDescToRule.get(description);
    }

    // implement RelOptPlanner
    public void setRuleDescExclusionFilter(Pattern exclusionFilter)
    {
        ruleDescExclusionFilter = exclusionFilter;
    }

    /**
     * Determines whether a given rule is excluded by ruleDescExclusionFilter.
     *
     * @param rule rule to test
     *
     * @return true iff rule should be excluded
     */
    public boolean isRuleExcluded(RelOptRule rule)
    {
        if (ruleDescExclusionFilter == null) {
            return false;
        }
        return ruleDescExclusionFilter.matcher(rule.toString()).matches();
    }

    // implement RelOptPlanner
    public RelOptPlanner chooseDelegate()
    {
        return this;
    }

    // implement RelOptPlanner
    public void registerSchema(RelOptSchema schema)
    {
    }

    // implement RelOptPlanner
    public long getRelMetadataTimestamp(RelNode rel)
    {
        return 0;
    }

    public void setImportance(RelNode rel, double importance)
    {
    }

    public void registerClass(RelNode node) {
        final Class<? extends RelNode> clazz = node.getClass();
        if (classes.add(clazz)) {
            node.register(this);
        }
    }

    public RelTraitSet emptyTraitSet() {
        return RelTraitSet.createEmpty();
    }

    // implement RelOptPlanner
    public RelOptCost makeCost(
        double dRows,
        double dCpu,
        double dIo)
    {
        return new RelOptCostImpl(dRows);
    }

    // implement RelOptPlanner
    public RelOptCost makeHugeCost()
    {
        return new RelOptCostImpl(Double.MAX_VALUE);
    }

    // implement RelOptPlanner
    public RelOptCost makeInfiniteCost()
    {
        return new RelOptCostImpl(Double.POSITIVE_INFINITY);
    }

    // implement RelOptPlanner
    public RelOptCost makeTinyCost()
    {
        return new RelOptCostImpl(1.0);
    }

    // implement RelOptPlanner
    public RelOptCost makeZeroCost()
    {
        return new RelOptCostImpl(0.0);
    }

    // implement RelOptPlanner
    public RelOptCost getCost(RelNode rel)
    {
        return RelMetadataQuery.getCumulativeCost(rel);
    }

    // implement RelOptPlanner
    public void addListener(RelOptListener newListener)
    {
        if (listener == null) {
            listener = new MulticastRelOptListener();
        }
        listener.addListener(newListener);
    }

    // implement RelOptPlanner
    public void registerMetadataProviders(ChainedRelMetadataProvider chain)
    {
    }

    // implement RelOptPlanner
    public boolean addRelTraitDef(RelTraitDef relTraitDef)
    {
        return false;
    }

    /**
     * Fires a rule, taking care of tracing and listener notification.
     *
     * @param ruleCall description of rule call
     *
     * @pre ruleCall.getRule().matches(ruleCall)
     */
    protected void fireRule(
        RelOptRuleCall ruleCall)
    {
        checkCancel();

        assert ruleCall.getRule().matches(ruleCall);
        if (isRuleExcluded(ruleCall.getRule())) {
            if (tracer.isLoggable(Level.FINE)) {
                tracer.fine(
                    "Rule [" + ruleCall.getRule() + "] not fired"
                    + " due to exclusion filter");
            }
            return;
        }

        if (tracer.isLoggable(Level.FINE)) {
            tracer.fine(
                "Apply rule [" + ruleCall.getRule() + "] to ["
                + RelOptUtil.toString(ruleCall.getRels()) + "]");
        }

        if (listener != null) {
            RelOptListener.RuleAttemptedEvent event =
                new RelOptListener.RuleAttemptedEvent(
                    this,
                    ruleCall.getRels()[0],
                    ruleCall,
                    true);
            listener.ruleAttempted(event);
        }

        ruleCall.getRule().onMatch(ruleCall);

        if (listener != null) {
            RelOptListener.RuleAttemptedEvent event =
                new RelOptListener.RuleAttemptedEvent(
                    this,
                    ruleCall.getRels()[0],
                    ruleCall,
                    false);
            listener.ruleAttempted(event);
        }
    }

    /**
     * Takes care of tracing and listener notification when a rule's
     * transformation is applied.
     *
     * @param ruleCall description of rule call
     * @param newRel result of transformation
     * @param before true before registration of new rel; false after
     */
    protected void notifyTransformation(
        RelOptRuleCall ruleCall,
        RelNode newRel,
        boolean before)
    {
        if (before && tracer.isLoggable(Level.FINE)) {
            tracer.fine(
                "Rule " + ruleCall.getRule() + " arguments "
                + RelOptUtil.toString(ruleCall.getRels()) + " produced "
                + newRel);
        }

        if (listener != null) {
            RelOptListener.RuleProductionEvent event =
                new RelOptListener.RuleProductionEvent(
                    this,
                    newRel,
                    ruleCall,
                    before);
            listener.ruleProductionSucceeded(event);
        }
    }

    /**
     * Takes care of tracing and listener notification when a rel is chosen as
     * part of the final plan.
     *
     * @param rel chosen rel
     */
    protected void notifyChosen(RelNode rel)
    {
        if (tracer.isLoggable(Level.FINE)) {
            tracer.fine("For final plan, using " + rel);
        }

        if (listener != null) {
            RelOptListener.RelChosenEvent event =
                new RelOptListener.RelChosenEvent(
                    this,
                    rel);
            listener.relChosen(event);
        }
    }

    /**
     * Takes care of tracing and listener notification when a rel equivalence is
     * detected.
     *
     * @param rel chosen rel
     */
    protected void notifyEquivalence(
        RelNode rel,
        Object equivalenceClass,
        boolean physical)
    {
        if (listener != null) {
            RelOptListener.RelEquivalenceEvent event =
                new RelOptListener.RelEquivalenceEvent(
                    this,
                    rel,
                    equivalenceClass,
                    physical);
            listener.relEquivalenceFound(event);
        }
    }

    /**
     * Takes care of tracing and listener notification when a rel is discarded
     *
     * @param rel discarded rel
     */
    protected void notifyDiscard(
        RelNode rel)
    {
        if (listener != null) {
            RelOptListener.RelDiscardedEvent event =
                new RelOptListener.RelDiscardedEvent(
                    this,
                    rel);
            listener.relDiscarded(event);
        }
    }

    protected MulticastRelOptListener getListener()
    {
        return listener;
    }
}

// End AbstractRelOptPlanner.java
