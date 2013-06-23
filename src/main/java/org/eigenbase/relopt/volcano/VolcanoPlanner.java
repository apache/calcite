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
package org.eigenbase.relopt.volcano;

import java.io.*;

import java.util.*;
import java.util.logging.*;
import java.util.regex.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.convert.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.rel.rules.*;
import org.eigenbase.relopt.*;
import org.eigenbase.util.*;


/**
 * VolcanoPlanner optimizes queries by transforming expressions selectively
 * according to a dynamic programming algorithm.
 */
public class VolcanoPlanner
    extends AbstractRelOptPlanner
{
    //~ Static fields/initializers ---------------------------------------------

    protected static final double CostImprovement = .5;

    //~ Instance fields --------------------------------------------------------

    protected RelSubset root;

    /**
     * If true, the planner keeps applying rules as long as they continue to
     * reduce the cost. If false, the planner terminates as soon as it has found
     * any implementation, no matter how expensive. The default is false due to
     * unresolved bugs with various rules.
     */
    protected boolean ambitious = true;

    /**
     * If true, and if {@link #ambitious} is true, the planner waits a finite
     * number of iterations for the cost to improve.
     *
     * <p>The number of iterations K is equal to the number of iterations
     * required to get the first finite plan. After the first finite plan, it
     * continues to fire rules to try to improve it. The planner sets a target
     * cost of the current best cost multiplied by {@link #CostImprovement}. If
     * it does not meet that cost target within K steps, it quits, and uses the
     * current best plan. If it meets the cost, it sets a new, lower target, and
     * has another K iterations to meet it. And so forth.
     *
     * <p>If false, the planner continues to fire rules until the rule queue is
     * empty.
     */
    protected boolean impatient = false;

    /**
     * List of all operands of all rules. Any operand can be an 'entry point' to
     * a rule call, when a relexp is registered which matches the.
     */
    private final List<RelOptRuleOperand> allOperands =
        new ArrayList<RelOptRuleOperand>();

    /**
     * List of all sets. Used only for debugging.
     */
    final List<RelSet> allSets = new ArrayList<RelSet>();

    /**
     * Canonical map from {@link String digest} to the unique {@link RelNode
     * relational expression} with that digest.
     */
    private final Map<String, RelNode> mapDigestToRel =
        new HashMap<String, RelNode>();

    /**
     * Map each registered expression ({@link RelNode}) to its equivalence set
     * ({@link RelSubset}).
     *
     * <p>We use an {@link IdentityHashMap} to simplify the process of merging
     * {@link RelSet} objects. Most {@link RelNode} objects are identified by
     * their digest, which involves the set that their child relational
     * expressions belong to. If those children belong to the same set, we have
     * to be careful, otherwise it gets incestuous.</p>
     */
    private final IdentityHashMap<RelNode, RelSubset> mapRel2Subset =
        new IdentityHashMap<RelNode, RelSubset>();

    /**
     * The importance of relational expressions.
     *
     * <p>The map contains only RelNodes whose importance has been overridden
     * using {@link RelOptPlanner#setImportance(RelNode, double)}. Other
     * RelNodes are presumed to have 'normal' importance.
     *
     * <p>If a RelNode has 0 importance, all {@link RelOptRuleCall}s using it
     * are ignored, and future RelOptRuleCalls are not queued up.
     */
    final Map<RelNode, Double> relImportances = new HashMap<RelNode, Double>();

    /**
     * List of all schemas which have been registered.
     */
    private final Set<RelOptSchema> registeredSchemas =
        new HashSet<RelOptSchema>();

    /**
     * Holds rule calls waiting to be fired.
     */
    final RuleQueue ruleQueue = new RuleQueue(this);

    /**
     * Holds the currently registered RelTraitDefs.
     */
    private final List<RelTraitDef> traitDefs = new ArrayList<RelTraitDef>();

    /**
     * Set of all registered rules.
     */
    protected final Set<RelOptRule> ruleSet = new HashSet<RelOptRule>();

    private int nextSetId = 0;

    /**
     * Incremented every time a relational expression is registered or two sets
     * are merged. Tells us whether anything is going on.
     */
    private int registerCount;

    /**
     * Listener for this planner, or null if none set.
     */
    RelOptListener listener;

    /**
     * Dump of the root relational expression, as it was before any rules were
     * applied. For debugging.
     */
    private String originalRootString;

    /**
     * Whether the planner can accept new rules.
     */
    private boolean locked;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a uninitialized <code>VolcanoPlanner</code>. To fully initialize
     * it, the caller must register the desired set of relations, rules, and
     * calling conventions.
     */
    public VolcanoPlanner()
    {
    }

    //~ Methods ----------------------------------------------------------------

    protected VolcanoPlannerPhaseRuleMappingInitializer
    getPhaseRuleMappingInitializer()
    {
        return new VolcanoPlannerPhaseRuleMappingInitializer() {
            public void initialize(
                Map<VolcanoPlannerPhase, Set<String>> phaseRuleMap)
            {
                Set<String> preProcessMdrPhaseRules =
                    phaseRuleMap.get(VolcanoPlannerPhase.PRE_PROCESS_MDR);

                // Fire this rule early so we don't waste time fiddling with
                // alternate join implementations.  The MDR queries are tuned
                // to want this rule's output.
                preProcessMdrPhaseRules.add("MedMdrJoinRule");
                preProcessMdrPhaseRules.add("IterCalcRule");
                preProcessMdrPhaseRules.add("ProjectToCalcRule");
                preProcessMdrPhaseRules.add("FilterToCalcRule");
                preProcessMdrPhaseRules.add("MergeCalcRule");

                Set<String> cleanupPhaseRules =
                    phaseRuleMap.get(VolcanoPlannerPhase.CLEANUP);

                // Cleanup pass to remove trivial projects and merge calcs.
                // Helps the planner concentrate on these easy tasks.
                cleanupPhaseRules.add("RemoveTrivialProjectRule");
                cleanupPhaseRules.add("MergeCalcRule");
                cleanupPhaseRules.add("FennelCalcRule");
                cleanupPhaseRules.add("IterCalcRule");
            }
        };
    }

    // implement RelOptPlanner
    public boolean isRegistered(RelNode rel)
    {
        return mapRel2Subset.get(rel) != null;
    }

    public void setRoot(RelNode rel)
    {
        this.root = registerImpl(rel, null);
        this.originalRootString = RelOptUtil.toString(root);

        // Making a node the root changes its importance.
        this.ruleQueue.recompute(this.root);
    }

    public RelNode getRoot()
    {
        return root;
    }

    /**
     * Finds an expression's equivalence set. If the expression is not
     * registered, returns null.
     *
     * @param rel Relational expression
     * @pre rel != null
     * @return Equivalence set that expression belongs to, or null if it is not
     * registered
     */
    public RelSet getSet(RelNode rel)
    {
        assert rel != null : "pre: rel != null";
        final RelSubset subset = getSubset(rel);
        if (subset != null) {
            assert subset.set != null;
            return subset.set;
        }
        return null;
    }

    public boolean addRelTraitDef(RelTraitDef relTraitDef)
    {
        return !traitDefs.contains(relTraitDef) && traitDefs.add(relTraitDef);
    }

    @Override
    public RelTraitSet emptyTraitSet() {
        RelTraitSet traitSet = super.emptyTraitSet();
        for (RelTraitDef traitDef : traitDefs) {
            if (traitDef.multiple()) {
                // TODO: restructure RelTraitSet to allow a list of entries
                //  for any given trait
            }
        }
        return traitSet;
    }

    public boolean addRule(RelOptRule rule)
    {
        if (locked) {
            return false;
        }
        if (ruleSet.contains(rule)) {
            // Rule already exists.
            return false;
        }
        final boolean added = ruleSet.add(rule);
        assert added;

        mapRuleDescription(rule);

        // Each of this rule's operands is an 'entry point' for a rule call.
        allOperands.addAll(Arrays.asList(rule.operands));

        // If this is a converter rule, check that it operates on one of the
        // kinds of trait we are interested in, and if so, register the rule
        // with the trait.
        if (rule instanceof ConverterRule) {
            ConverterRule converterRule = (ConverterRule) rule;

            final RelTrait ruleTrait = converterRule.getInTrait();
            final RelTraitDef ruleTraitDef = ruleTrait.getTraitDef();
            if (traitDefs.contains(ruleTraitDef)) {
                ruleTraitDef.registerConverterRule(this, converterRule);
            }
        }

        return true;
    }

    public boolean removeRule(RelOptRule rule)
    {
        if (!ruleSet.remove(rule)) {
            // Rule was not present.
            return false;
        }

        // Remove description.
        unmapRuleDescription(rule);

        // Remove operands.
        for (
            Iterator<RelOptRuleOperand> operandIter = allOperands.iterator();
            operandIter.hasNext();)
        {
            RelOptRuleOperand operand = operandIter.next();
            if (operand.getRule().equals(rule)) {
                operandIter.remove();
            }
        }

        // Remove trait mappings. (In particular, entries from conversion
        // graph.)
        if (rule instanceof ConverterRule) {
            ConverterRule converterRule = (ConverterRule) rule;
            final RelTrait ruleTrait = converterRule.getInTrait();
            final RelTraitDef ruleTraitDef = ruleTrait.getTraitDef();
            if (traitDefs.contains(ruleTraitDef)) {
                ruleTraitDef.deregisterConverterRule(this, converterRule);
            }
        }
        return true;
    }

    public boolean canConvert(RelTraitSet fromTraits, RelTraitSet toTraits)
    {
        assert (fromTraits.size() >= toTraits.size());

        boolean canConvert = true;
        for (int i = 0; (i < toTraits.size()) && canConvert; i++) {
            RelTrait fromTrait = fromTraits.getTrait(i);
            RelTrait toTrait = toTraits.getTrait(i);

            assert fromTrait.getTraitDef() == toTrait.getTraitDef();
            assert traitDefs.contains(fromTrait.getTraitDef());
            assert traitDefs.contains(toTrait.getTraitDef());

            canConvert =
                fromTrait.getTraitDef().canConvert(this, fromTrait, toTrait);
        }

        return canConvert;
    }

    public RelNode changeTraits(final RelNode rel, RelTraitSet toTraits)
    {
        assert !rel.getTraitSet().equals(toTraits)
            : "pre: !rel.getTraits().equals(toTraits)";

        RelSubset rel2 = ensureRegistered(rel, null);
        if (rel2.getTraitSet().equals(toTraits)) {
            return rel2;
        }

        return rel2.set.getOrCreateSubset(rel.getCluster(), toTraits);
    }

    public RelOptPlanner chooseDelegate()
    {
        return this;
    }

    /**
     * Finds the most efficient expression to implement the query given via
     * {@link org.eigenbase.relopt.RelOptPlanner#setRoot(org.eigenbase.rel.RelNode)}.
     *
     * <p>The algorithm executes repeatedly in a series of phases. In each phase
     * the exact rules that may be fired varies. The mapping of phases to rule
     * sets is maintained in the {@link #ruleQueue}.
     *
     * <p>In each phase, the planner sets the initial importance of the existing
     * RelSubSets ({@link #setInitialImportance()}). The planner then iterates
     * over the rule matches presented by the rule queue until:
     *
     * <ol>
     * <li>The rule queue becomes empty.</li>
     * <li>For ambitious planners: No improvements to the plan have been made
     * recently (specifically within a number of iterations that is 10% of the
     * number of iterations necessary to first reach an implementable plan or 25
     * iterations whichever is larger).</li>
     * <li>For non-ambitious planners: When an implementable plan is found.</li>
     * </ol>
     *
     * <p>Furthermore, after every 10 iterations without an implementable plan,
     * RelSubSets that contain only logical RelNodes are given an importance
     * boost via {@link #injectImportanceBoost()}. Once an implementable plan is
     * found, the artificially raised importances are cleared ({@link
     * #clearImportanceBoost()}).
     *
     * @return the most efficient RelNode tree found for implementing the given
     * query
     */
    public RelNode findBestExp()
    {
        int cumulativeTicks = 0;
        for (VolcanoPlannerPhase phase : VolcanoPlannerPhase.values()) {
            setInitialImportance();

            RelOptCost targetCost = makeHugeCost();
            int tick = 0;
            int firstFiniteTick = -1;
            int splitCount = 0;
            int giveUpTick = Integer.MAX_VALUE;

            while (true) {
                ++tick;
                ++cumulativeTicks;
                if (root.bestCost.isLe(targetCost)) {
                    if (firstFiniteTick < 0) {
                        firstFiniteTick = cumulativeTicks;

                        clearImportanceBoost();
                    }
                    if (ambitious) {
                        // Choose a slightly more ambitious target cost, and
                        // try again. If it took us 1000 iterations to find our
                        // first finite plan, give ourselves another 100
                        // iterations to reduce the cost by 10%.
                        targetCost = root.bestCost.multiplyBy(0.9);
                        ++splitCount;
                        if (impatient) {
                            if (firstFiniteTick < 10) {
                                // It's possible pre-processing can create
                                // an implementable plan -- give us some time
                                // to actually optimize it.
                                giveUpTick = cumulativeTicks + 25;
                            } else {
                                giveUpTick =
                                    cumulativeTicks
                                    + Math.max(firstFiniteTick / 10, 25);
                            }
                        }
                    } else {
                        break;
                    }
                } else if (cumulativeTicks > giveUpTick) {
                    // We haven't made progress recently. Take the current best.
                    break;
                } else if (root.bestCost.isInfinite() && ((tick % 10) == 0)) {
                    injectImportanceBoost();
                }

                if (!ruleQueue.hasNextMatch(phase)) {
                    break;
                }

                if (tracer.isLoggable(Level.FINE)) {
                    tracer.fine(
                        "PLANNER = " + this
                        + "; TICK = " + cumulativeTicks + "/" + tick
                        + "; PHASE = " + phase.toString()
                        + "; COST = " + root.bestCost);
                }

                VolcanoRuleMatch match = ruleQueue.popMatch(phase);
                assert match.getRule().matches(match);
                match.onMatch();

                // The root may have been merged with another
                // subset. Find the new root subset.
                root = canonize(root);
            }

            ruleQueue.phaseCompleted(phase);
        }
        if (tracer.isLoggable(Level.FINER)) {
            StringWriter sw = new StringWriter();
            final PrintWriter pw = new PrintWriter(sw);
            dump(pw);
            pw.flush();
            tracer.finer(sw.toString());
        }
        RelNode cheapest = root.buildCheapestPlan(this);
        if (tracer.isLoggable(Level.FINE)) {
            tracer.fine(
                "Cheapest plan:\n"
                + RelOptUtil.toString(cheapest));
        }
        return cheapest;
    }

    private void setInitialImportance()
    {
        RelVisitor visitor =
            new RelVisitor() {
                int depth = 0;

                HashSet<RelSubset> visitedSubsets = new HashSet<RelSubset>();

                public void visit(
                    RelNode p,
                    int ordinal,
                    RelNode parent)
                {
                    if (p instanceof RelSubset) {
                        RelSubset subset = (RelSubset) p;

                        if (visitedSubsets.contains(subset)) {
                            return;
                        }

                        if (subset != root) {
                            Double importance = Math.pow(0.9, (double) depth);

                            ruleQueue.updateImportance(subset, importance);
                        }

                        visitedSubsets.add(subset);

                        depth++;
                        for (RelNode rel : subset.getRels()) {
                            visit(rel, -1, subset);
                        }
                        depth--;
                    } else {
                        super.visit(p, ordinal, parent);
                    }
                }
            };

        visitor.go(root);
    }

    /**
     * Finds RelSubsets in the plan that contain only rels of {@link
     * Convention#NONE} and boosts their importance by 25%.
     */
    private void injectImportanceBoost()
    {
        HashSet<RelSubset> requireBoost = new HashSet<RelSubset>();

SUBSET_LOOP:
        for (RelSubset subset : ruleQueue.subsetImportances.keySet()) {
            for (RelNode rel : subset.getRels()) {
                if (rel.getConvention() != Convention.NONE) {
                    continue SUBSET_LOOP;
                }
            }

            requireBoost.add(subset);
        }

        ruleQueue.boostImportance(requireBoost, 1.25);
    }

    /**
     * Clear all importance boosts.
     */
    private void clearImportanceBoost()
    {
        Collection<RelSubset> empty = Collections.emptySet();

        ruleQueue.boostImportance(empty, 1.0);
    }

    // implement Planner
    public RelOptCost makeCost(
        double dRows,
        double dCpu,
        double dIo)
    {
        return new VolcanoCost(dRows, dCpu, dIo);
    }

    public RelOptCost makeHugeCost()
    {
        return VolcanoCost.HUGE;
    }

    public RelOptCost makeInfiniteCost()
    {
        return VolcanoCost.INFINITY;
    }

    public RelOptCost makeTinyCost()
    {
        return VolcanoCost.TINY;
    }

    public RelOptCost makeZeroCost()
    {
        return VolcanoCost.ZERO;
    }

    public RelSubset register(
        RelNode rel,
        RelNode equivRel)
    {
        assert !isRegistered(rel) : "pre: isRegistered(rel)";
        final RelSet set;
        if (equivRel == null) {
            set = null;
        } else {
            assert RelOptUtil.equal(
                "rel rowtype",
                rel.getRowType(),
                "equivRel rowtype",
                equivRel.getRowType(),
                true);
            set = getSet(equivRel);
        }
        final RelSubset subset = registerImpl(rel, set);

        if (tracer.isLoggable(Level.FINE)) {
            validate();
        }

        return subset;
    }

    public RelSubset ensureRegistered(RelNode rel, RelNode equivRel)
    {
        final RelSubset subset = mapRel2Subset.get(rel);
        if (subset != null) {
            if (equivRel != null) {
                final RelSubset equivSubset = getSubset(equivRel);
                if (equivSubset != subset) {
                    assert subset.set != equivSubset.set
                        : "Same set, different subsets means rel and equivRel"
                        + " have different traits, and that's an error";
                    merge(subset.set, equivSubset.set);
                }
            }
            return subset;
        } else {
            return register(rel, equivRel);
        }
    }

    /**
     * Checks internal consistency.
     */
    protected void validate()
    {
        for (RelSet set : allSets) {
            if (set.equivalentSet != null) {
                throw new AssertionError(
                    "set [" + set
                    + "] has been merged: it should not be in the list");
            }
            for (RelSubset subset : set.subsets) {
                if (subset.set != set) {
                    throw new AssertionError(
                        "subset [" + subset.getDescription()
                        + "] is in wrong set [" + set + "]");
                }
                for (RelNode rel : subset.getRels()) {
                    RelOptCost relCost = getCost(rel);
                    if (relCost.isLt(subset.bestCost)) {
                        throw new AssertionError(
                            "rel [" + rel.getDescription()
                            + "] has lower cost " + relCost
                            + " than best cost " + subset.bestCost
                            + " of subset [" + subset.getDescription() + "]");
                    }
                }
            }
        }
    }

    public void registerAbstractRelationalRules()
    {
        addRule(AbstractConverter.ExpandConversionRule.instance);
        addRule(SwapJoinRule.instance);
        addRule(RemoveDistinctRule.instance);
        addRule(UnionToDistinctRule.instance);
        addRule(RemoveTrivialProjectRule.instance);
        addRule(RemoveTrivialCalcRule.instance);
        if (Bug.TodoFixed)
        addRule(RemoveSortRule.INSTANCE);

        // todo: rule which makes Project({OrdinalRef}) disappear
    }

    public void registerSchema(RelOptSchema schema)
    {
        if (registeredSchemas.add(schema)) {
            try {
                schema.registerRules(this);
            } catch (Exception e) {
                throw Util.newInternal(
                    e,
                    "Error while registering schema " + schema);
            }
        }
    }

    public RelOptCost getCost(RelNode rel)
    {
        assert rel != null : "pre-condition: rel != null";
        if (rel instanceof RelSubset) {
            return ((RelSubset) rel).bestCost;
        }
        if (rel.getTraitSet().getTrait(0) == Convention.NONE) {
            return makeInfiniteCost();
        }
        RelOptCost cost = RelMetadataQuery.getNonCumulativeCost(rel);
        if (!VolcanoCost.ZERO.isLt(cost)) {
            // cost must be positive, so nudge it
            cost = makeTinyCost();
        }
        for (RelNode input : rel.getInputs()) {
            cost = cost.plus(getCost(input));
        }
        return cost;
    }

    /**
     * Returns the subset that a relational expression belongs to.
     *
     * @param rel Relational expression
     *
     * @return Subset it belongs to, or null if it is not registered
     *
     * @pre rel != null
     */
    public RelSubset getSubset(RelNode rel)
    {
        assert rel != null : "pre: rel != null";
        if (rel instanceof RelSubset) {
            return (RelSubset) rel;
        } else {
            return mapRel2Subset.get(rel);
        }
    }

    public RelSubset getSubset(
        RelNode rel,
        RelTraitSet traits)
    {
        return getSubset(rel, traits, false);
    }

    public RelSubset getSubset(
        RelNode rel,
        RelTraitSet traits,
        boolean createIfMissing)
    {
        if ((rel instanceof RelSubset) && (rel.getTraitSet().equals(traits))) {
            return (RelSubset) rel;
        }
        RelSet set = getSet(rel);
        if (set == null) {
            return null;
        }
        if (createIfMissing) {
            return set.getOrCreateSubset(rel.getCluster(), traits);
        }
        return set.getSubset(traits);
    }

    private RelNode changeTraitsUsingConverters(
        RelNode rel,
        RelTraitSet toTraits,
        boolean allowAbstractConverters)
    {
        final RelTraitSet fromTraits = rel.getTraitSet();

        assert (fromTraits.size() >= toTraits.size());

        final boolean allowInfiniteCostConverters =
            SaffronProperties.instance().allowInfiniteCostConverters.get();

        // Naive algorithm: assumes that conversion from Tx1.Ty1 to Tx2.Ty2
        // can happen in order (e.g. the traits are completely orthogonal).
        // Also, toTraits may have fewer traits than fromTraits, excess traits
        // will be left as is.  Finally, any null entries in toTraits are
        // ignored.
        RelNode converted = rel;
        for (int i = 0; (converted != null) && (i < toTraits.size()); i++) {
            RelTrait fromTrait = fromTraits.getTrait(i);
            RelTrait toTrait = toTraits.getTrait(i);

            if (toTrait == null) {
                continue;
            }

            assert (fromTrait.getTraitDef() == toTrait.getTraitDef());

            if (fromTrait == toTrait) {
                // No need to convert, it's already correct.
                continue;
            }

            rel =
                fromTrait.getTraitDef().convert(
                    this,
                    converted,
                    toTrait,
                    allowInfiniteCostConverters);
            if (rel != null) {
                register(rel, converted);
            }

            if ((rel == null) && allowAbstractConverters) {
                RelTraitSet stepTraits =
                    converted.getTraitSet().replace(toTrait);

                rel = getSubset(converted, stepTraits);
            }

            converted = rel;
        }

        return converted;
    }

    RelNode changeTraitsUsingConverters(
        RelNode rel,
        RelTraitSet toTraits)
    {
        return changeTraitsUsingConverters(rel, toTraits, false);
    }

    void checkForSatisfiedConverters(
        RelSet set,
        RelNode rel)
    {
        int i = 0;
        while (i < set.abstractConverters.size()) {
            AbstractConverter converter = set.abstractConverters.get(i);
            RelNode converted =
                changeTraitsUsingConverters(
                    rel,
                    converter.getTraitSet());
            if (converted == null) {
                i++; // couldn't convert this; move on to the next
            } else {
                if (!isRegistered(converted)) {
                    registerImpl(converted, set);
                }
                set.abstractConverters.remove(converter); // success
            }
        }
    }

    public void setImportance(RelNode rel, double importance)
    {
        assert rel != null;
        if (importance == 0d) {
            relImportances.put(rel, importance);
        }
    }

    /**
     * Dumps the internal state of this VolcanoPlanner to a writer.
     *
     * @param pw Print writer
     *
     * @see #normalizePlan(String)
     */
    public void dump(PrintWriter pw)
    {
        pw.println("Root: " + root.getDescription());
        pw.println("Original rel:");
        pw.println(originalRootString);
        pw.println("Sets:");
        RelSet [] sets = allSets.toArray(new RelSet[allSets.size()]);
        Arrays.sort(
            sets,
            new Comparator<RelSet>() {
                public int compare(
                    RelSet o1,
                    RelSet o2)
                {
                    return o1.id - o2.id;
                }
            });
        for (RelSet set : sets) {
            pw.println(
                "Set#" + set.id
                + ", type: " + set.subsets.get(0).getRowType());
            int j = -1;
            for (RelSubset subset : set.subsets) {
                ++j;
                pw.println(
                    "\t" + subset.getDescription() + ", best="
                    + ((subset.best == null) ? "null"
                        : ("rel#" + subset.best.getId())) + ", importance="
                    + ruleQueue.getImportance(subset));
                assert (subset.set == set);
                for (int k = 0; k < j; k++) {
                    assert !set.subsets.get(k).getTraitSet().equals(
                        subset.getTraitSet());
                }
                for (RelNode rel : subset.getRels()) {
                    // "\t\trel#34:JavaProject(rel#32:JavaFilter(...), ...)"
                    pw.print("\t\t" + rel.getDescription());
                    for (RelNode input : rel.getInputs()) {
                        RelSubset inputSubset =
                            getSubset(
                                input,
                                input.getTraitSet());
                        RelSet inputSet = inputSubset.set;
                        if (input instanceof RelSubset) {
                            final Iterator<RelNode> rels =
                                inputSubset.getRels().iterator();
                            assert rels.hasNext();
                            input = rels.next();
                            assert inputSubset.getTraitSet().equals(
                                input.getTraitSet());
                            assert inputSet.rels.contains(input);
                            assert inputSet.subsets.contains(inputSubset);
                        }
                    }
                    Double importance = relImportances.get(rel);
                    if (importance != null) {
                        pw.print(", importance=" + importance);
                    }
                    pw.print(
                        ", rowcount="
                        + RelMetadataQuery.getRowCount(rel));
                    pw.println(", cumulative cost=" + getCost(rel));
                }
            }
        }
        pw.println();
    }

    /**
     * Re-computes the digest of a {@link RelNode}.
     *
     * <p>Since a relational expression's digest contains the identifiers of its
     * children, this method needs to be called when the child has been renamed,
     * for example if the child's set merges with another.
     *
     * @param rel Relational expression
     */
    void rename(RelNode rel)
    {
        final String oldDigest = rel.getDigest();
        if (fixUpInputs(rel)) {
            assert mapDigestToRel.remove(oldDigest) == rel;
            final String newDigest = rel.recomputeDigest();
            tracer.finer(
                "Rename #" + rel.getId() + " from '" + oldDigest
                + "' to '" + newDigest + "'");
            final RelNode equivRel = mapDigestToRel.put(newDigest, rel);
            if (equivRel != null) {
                assert equivRel != rel;

                // There's already an equivalent with the same name, and we
                // just knocked it out. Put it back, and forget about 'rel'.
                tracer.finer(
                    "After renaming rel#" + rel.getId()
                    + ", it is now equivalent to rel#" + equivRel.getId());
                mapDigestToRel.put(
                    equivRel.getDigest(),
                    equivRel);

                RelSubset equivRelSubset = getSubset(equivRel);
                ruleQueue.recompute(equivRelSubset, true);

                // Remove back-links from children.
                for (RelNode input : rel.getInputs()) {
                    ((RelSubset) input).set.parents.remove(rel);
                }

                // Remove rel from its subset. (This may leave the subset
                // empty, but if so, that will be dealt with when the sets
                // get merged.)
                final RelSubset subset = mapRel2Subset.put(rel, equivRelSubset);
                assert subset != null;
                //boolean existed = subset.rels.remove(rel);
                //assert existed : "rel was not known to its subset";
                boolean existed = subset.set.rels.remove(rel);
                assert existed : "rel was not known to its set";
                final RelSubset equivSubset = getSubset(equivRel);
                if (equivSubset != subset) {
                    // The equivalent relational expression is in a different
                    // subset, therefore the sets are equivalent.
                    assert equivSubset.getTraitSet().equals(
                        subset.getTraitSet());
                    assert equivSubset.set != subset.set;
                    merge(equivSubset.set, subset.set);
                }
            }
        }
    }

    /**
     * Registers a {@link RelNode}, which has already been registered, in a new
     * {@link RelSet}.
     *
     * @param set Set
     * @param rel Relational expression
     */
    void reregister(
        RelSet set,
        RelNode rel)
    {
        // Is there an equivalent relational expression? (This might have
        // just occurred because the relational expression's child was just
        // found to be equivalent to another set.)
        RelNode equivRel = mapDigestToRel.get(rel.getDigest());
        if ((equivRel != null) && (equivRel != rel)) {
            assert (equivRel.getClass() == rel.getClass());
            assert (equivRel.getTraitSet().equals(rel.getTraitSet()));

            RelSubset equivRelSubset = getSubset(equivRel);
            ruleQueue.recompute(equivRelSubset, true);
            return;
        }

        // Add the relational expression into the correct set and subset.
        RelSubset subset2 = asd(rel, set);
    }

    /**
     * If a subset has one or more equivalent subsets (owing to a set having
     * merged with another), returns the subset which is the leader of the
     * equivalence class.
     *
     * @param subset Subset
     *
     * @return Leader of subset's equivalence class
     */
    private RelSubset canonize(final RelSubset subset)
    {
        if (subset.set.equivalentSet == null) {
            return subset;
        }
        RelSet set = subset.set;
        do {
            set = set.equivalentSet;
        } while (set.equivalentSet != null);
        return set.getOrCreateSubset(
            subset.getCluster(),
            subset.getTraitSet());
    }

    /**
     * Fires all rules matched by a relational expression.
     *
     * @param rel Relational expression which has just been created (or maybe
     * from the queue)
     * @param deferred If true, each time a rule matches, just add an entry to
     * the queue.
     */
    void fireRules(
        RelNode rel,
        boolean deferred)
    {
        for (RelOptRuleOperand operand : allOperands) {
            if (operand.matches(rel)) {
                final VolcanoRuleCall ruleCall;
                if (deferred) {
                    ruleCall = new DeferringRuleCall(this, operand);
                } else {
                    ruleCall = new VolcanoRuleCall(this, operand);
                }
                ruleCall.match(rel);
            }
        }
    }

    private boolean fixUpInputs(RelNode rel) {
        List<RelNode> inputs = rel.getInputs();
        int i = -1;
        int changeCount = 0;
        for (RelNode input : inputs) {
            ++i;
            if (input instanceof RelSubset) {
                final RelSubset subset = (RelSubset) input;
                RelSubset newSubset = canonize(subset);
                if (newSubset != subset) {
                    rel.replaceInput(i, newSubset);
                    if (subset.set != newSubset.set) {
                        subset.set.parents.remove(rel);
                        newSubset.set.parents.add(rel);
                    }
                    changeCount++;
                }
            }
        }
        return changeCount > 0;
    }

    private void merge(
        RelSet set,
        RelSet set2)
    {
        assert set != set2 : "pre: set != set2";
        assert set.equivalentSet == null;

        // Find the root of set2's equivalence tree.
        if (set2.equivalentSet != null) {
            Set<RelSet> seen = new HashSet<RelSet>();
            while (true) {
                if (!seen.add(set2)) {
                    throw Util.newInternal("cycle in equivalence tree");
                }
                if (set2.equivalentSet == null) {
                    break;
                } else {
                    set2 = set2.equivalentSet;
                }
            }

            // Looks like set2 was already marked as equivalent to set. Nothing
            // to do.
            if (set2 == set) {
                return;
            }
        }

        // If necessary, swap the sets, so we're always merging the newer set
        // into the older.
        if (set.id > set2.id) {
            RelSet t = set;
            set = set2;
            set2 = t;
        }

        // Merge.
        set.mergeWith(this, set2);

        // Was the set we merged with the root? If so, the result is the new
        // root.
        if (set2 == getSet(root)) {
            root =
                set.getOrCreateSubset(
                    root.getCluster(),
                    root.getTraitSet());
        }
    }

    /**
     * Registers a new expression <code>exp</code> and queues up rule matches.
     * If <code>set</code> is not null, makes the expression part of that
     * equivalence set. If an identical expression is already registered, we
     * don't need to register this one and nor should we queue up rule matches.
     *
     * @param rel relational expression to register. Must be either a {@link
     * RelSubset}, or an unregistered {@link RelNode}
     * @param set set that rel belongs to, or <code>null</code>
     *
     * @return the equivalence-set
     *
     * @pre rel instanceof RelSubset || !isRegistered(rel)
     */
    private RelSubset registerImpl(
        RelNode rel,
        RelSet set)
    {
        assert (rel instanceof RelSubset) || !isRegistered(rel)
            : "pre: rel instanceof RelSubset || !isRegistered(rel)"
            + " : {rel=" + rel + "}";
        if (rel instanceof RelSubset) {
            return registerSubset(set, (RelSubset) rel);
        }

        if (rel.getCluster().getPlanner() != this) {
            throw Util.newInternal(
                "Relational expression " + rel
                + " belongs to a different planner than is currently being"
                + " used.");
        }

        // Now is a good time to ensure that the relational expression
        // implements the interface required by its calling convention.
        final RelTraitSet traits = rel.getTraitSet();
        final Convention convention =
            (Convention) traits.getTrait(0);
        if (!convention.getInterface().isInstance(rel)
            && !(rel instanceof ConverterRel))
        {
            throw Util.newInternal(
                "Relational expression " + rel
                + " has calling-convention " + convention
                + " but does not implement the required interface '"
                + convention.getInterface() + "' of that convention");
        }
        if (traits.size() != traitDefs.size()) {
            throw Util.newInternal(
                "Relational expression " + rel
                + " does not have the correct number of traits "
                + traits.size() + " != " + traitDefs.size());
        }

        // Ensure that its sub-expressions are registered.
        rel = rel.onRegister(this);

        // If it is equivalent to an existing expression, return the set that
        // the equivalent expression belongs to.
        String digest = rel.getDigest();
        RelNode equivExp = mapDigestToRel.get(digest);
        if (equivExp == null) {
            ;
        } else if (equivExp == rel) {
            return getSubset(rel);
        } else {
            assert (equivExp.getTraitSet().equals(traits)
                && (equivExp.getClass() == rel.getClass()));
            assert RelOptUtil.equal(
                "left", equivExp.getRowType(),
                "right", rel.getRowType(),
                true);
            RelSet equivSet = getSet(equivExp);
            if (equivSet != null) {
                if (tracer.isLoggable(Level.FINER)) {
                    tracer.finer(
                        "Register: rel#" + rel.getId()
                        + " is equivalent to " + equivExp.getDescription());
                }
                return registerSubset(
                    set,
                    getSubset(equivExp));
            }
        }

        // Converters are in the same set as their children.
        if (rel instanceof ConverterRel) {
            final RelNode input = ((ConverterRel) rel).getChild();
            final RelSet childSet = getSet(input);
            if ((set != null)
                && (set != childSet)
                && (set.equivalentSet == null))
            {
                if (tracer.isLoggable(Level.FINER)) {
                    tracer.finer(
                        "Register #" + rel.getId() + " " + digest
                        + " (and merge sets, because it is a conversion)");
                }
                merge(set, childSet);
                registerCount++;

                // During the mergers, the child set may have changed, and since
                // we're not registered yet, we won't have been informed. So
                // check whether we are now equivalent to an existing
                // expression.
                if (fixUpInputs(rel)) {
                    digest = rel.recomputeDigest();
                    RelNode equivRel = mapDigestToRel.get(digest);
                    if ((equivRel != rel) && (equivRel != null)) {
                        // make sure this bad rel didn't get into the
                        // set in any way (fixupInputs will do this but it
                        // doesn't know if it should so it does it anyway)
                        set.obliterateRelNode(rel);

                        // There is already an equivalent expression. Use that
                        // one, and forget about this one.
                        return getSubset(equivRel);
                    }
                }
            } else {
                set = childSet;
            }
        }

        // Place the expression in the appropriate equivalence set.
        if (set == null) {
            set = new RelSet(
                nextSetId++,
                Util.minus(
                    RelOptUtil.getVariablesSet(rel),
                    rel.getVariablesStopped()),
                RelOptUtil.getVariablesUsed(rel));
            this.allSets.add(set);
        }

        // Chain to find 'live' equivalent set, just in case several sets are
        // merging at the same time.
        while (set.equivalentSet != null) {
            set = set.equivalentSet;
        }
        registerCount++;
        RelSubset subset = asd(rel, set);

        final RelNode xx = mapDigestToRel.put(digest, rel);
        assert ((xx == null) || (xx == rel)) : digest;

        if (tracer.isLoggable(Level.FINER)) {
            tracer.finer(
                "Register " + rel.getDescription()
                + " in " + subset.getDescription());
        }

        // This relational expression may have been registered while we
        // recursively registered its children. If this is the case, we're done.
        if (xx != null) {
            return subset;
        }

        // Create back-links from its children, which makes children more
        // important.
        if (rel == this.root) {
            ruleQueue.subsetImportances.put(
                subset,
                1.0); // todo: remove
        }
        for (RelNode input : rel.getInputs()) {
            RelSubset childSubset = (RelSubset) input;
            childSubset.set.parents.add(rel);

            // Child subset is more important now a new parent uses it.
            ruleQueue.recompute(childSubset);
        }
        if (rel == this.root) {
            ruleQueue.subsetImportances.remove(subset);
        }

        // Remember abstract converters until they're satisfied
        if (rel instanceof AbstractConverter) {
            set.abstractConverters.add((AbstractConverter) rel);
        }

        // If this set has any unsatisfied converters, try to satisfy them.
        checkForSatisfiedConverters(set, rel);

        // Make sure this rel's subset importance is updated
        ruleQueue.recompute(subset, true);

        // Queue up all rules triggered by this relexp's creation.
        fireRules(rel, true);

        return subset;
    }

    private RelSubset asd(RelNode rel, RelSet set)
    {
        RelSubset subset = set.add(rel);
        mapRel2Subset.put(rel, subset);

        // While a tree of RelNodes is being registered, sometimes nodes' costs
        // improve and the subset doesn't hear about it. You can end up with
        // a subset with a single rel of cost 99 which thinks its best cost is
        // 100. We think this happens because the back-links to parents are
        // not established. So, give the subset another change to figure out
        // its cost.
        subset.propagateCostImprovements(this, rel, new HashSet<RelSubset>());

        return subset;
    }

    private RelSubset registerSubset(
        RelSet set,
        RelSubset subset)
    {
        if ((set != subset.set)
            && (set != null)
            && (set.equivalentSet == null)
            && (subset.set.equivalentSet == null))
        {
            tracer.finer(
                "Register #" + subset.getId() + " " + subset
                + ", and merge sets");
            merge(set, subset.set);
            registerCount++;
        }
        return subset;
    }

    // implement RelOptPlanner
    public void addListener(RelOptListener newListener)
    {
        // TODO jvs 6-Apr-2006:  new superclass AbstractRelOptPlanner
        // now defines a multicast listener; just need to hook it in
        if (listener != null) {
            throw Util.needToImplement("multiple VolcanoPlanner listeners");
        }
        listener = newListener;
    }

    // implement RelOptPlanner
    public void registerMetadataProviders(ChainedRelMetadataProvider chain)
    {
        chain.addProvider(new VolcanoRelMetadataProvider());
    }

    // implement RelOptPlanner
    public long getRelMetadataTimestamp(RelNode rel)
    {
        RelSubset subset = getSubset(rel);
        if (subset == null) {
            return 0;
        } else {
            return subset.timestamp;
        }
    }

    /**
     * Normalizes references to subsets within the string representation of a
     * plan.
     *
     * <p>This is useful when writing tests: it helps to ensure that tests don't
     * break when an extra rule is introduced that generates a new subset and
     * causes subsequent subset numbers to be off by one.
     *
     * <p>For example,
     *
     * <blockquote>
     * FennelAggRel.FENNEL_EXEC(child=Subset#17.FENNEL_EXEC,groupCount=1,
     *   EXPR$1=COUNT())<br/>
     * &nbsp;&nbsp;FennelSortRel.FENNEL_EXEC(child=Subset#2.FENNEL_EXEC,
     *   key=[0], discardDuplicates=false)<br/>
     * &nbsp;&nbsp;&nbsp;&nbsp;FennelCalcRel.FENNEL_EXEC(
     *   child=Subset#4.FENNEL_EXEC, expr#0..8={inputs}, expr#9=3456,
     *   DEPTNO=$t7, $f0=$t9)<br/>
     * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;MockTableImplRel.FENNEL_EXEC(
     *   table=[CATALOG, SALES, EMP])</blockquote>
     *
     * becomes
     *
     * <blockquote>
     * FennelAggRel.FENNEL_EXEC(child=Subset#{0}.FENNEL_EXEC, groupCount=1,
     *   EXPR$1=COUNT())<br/>
     * &nbsp;&nbsp;FennelSortRel.FENNEL_EXEC(child=Subset#{1}.FENNEL_EXEC,
     *   key=[0], discardDuplicates=false)<br/>
     * &nbsp;&nbsp;&nbsp;&nbsp;FennelCalcRel.FENNEL_EXEC(
     *   child=Subset#{2}.FENNEL_EXEC,expr#0..8={inputs},expr#9=3456,DEPTNO=$t7,
     *   $f0=$t9)<br/>
     * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;MockTableImplRel.FENNEL_EXEC(
     *   table=[CATALOG, SALES, EMP])</blockquote>
     *
     * @param plan Plan
     *
     * @return Normalized plan
     */
    public static String normalizePlan(String plan)
    {
        if (plan == null) {
            return null;
        }
        final Pattern poundDigits = Pattern.compile("Subset#[0-9]+\\.");
        int i = 0;
        while (true) {
            final Matcher matcher = poundDigits.matcher(plan);
            if (!matcher.find()) {
                return plan;
            }
            final String token = matcher.group(); // e.g. "Subset#23."
            plan = plan.replace(token, "Subset#{" + i++ + "}.");
        }
    }

    /**
     * Sets whether this planner is locked. A locked planner does not accept
     * new rules. {@link #addRule(org.eigenbase.relopt.RelOptRule)} will do
     * nothing and return false.
     *
     * @param locked Whether planner is locked
     */
    public void setLocked(boolean locked) {
        this.locked = locked;
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * A rule call which defers its actions. Whereas {@link RelOptRuleCall}
     * invokes the rule when it finds a match, a <code>DeferringRuleCall</code>
     * creates a {@link VolcanoRuleMatch} which can be invoked later.
     */
    private static class DeferringRuleCall
        extends VolcanoRuleCall
    {
        DeferringRuleCall(
            VolcanoPlanner planner,
            RelOptRuleOperand operand)
        {
            super(planner, operand);
        }

        /**
         * Rather than invoking the rule (as the base method does), creates a
         * {@link VolcanoRuleMatch} which can be invoked later.
         */
        protected void onMatch()
        {
            final VolcanoRuleMatch match =
                new VolcanoRuleMatch(
                    volcanoPlanner,
                    getOperand0(),
                    rels);
            volcanoPlanner.ruleQueue.addMatch(match);
        }
    }
}

// End VolcanoPlanner.java
