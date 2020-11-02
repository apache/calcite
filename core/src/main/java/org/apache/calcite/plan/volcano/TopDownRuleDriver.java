/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.plan.volcano;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.DeriveMode;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.PhysicalNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.trace.CalciteTrace;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Stack;
import java.util.function.Predicate;

/**
 * A rule driver that apply rules in a Top-Down manner.
 * By ensuring rule applying orders, there could be ways for
 * space pruning and rule mutual exclusivity check.
 *
 * <p>This implementation use tasks to manage rule matches.
 * A Task is a piece of work to be executed, it may apply some rules
 * or schedule other tasks</p>
 */
@SuppressWarnings("JdkObsolete")
class TopDownRuleDriver implements RuleDriver {

  private static final Logger LOGGER = CalciteTrace.getPlannerTaskTracer();

  private final VolcanoPlanner planner;

  /**
   * The rule queue designed for top-down rule applying.
   */
  private final TopDownRuleQueue ruleQueue;

  /**
   * All tasks waiting for execution.
   */
  private Stack<Task> tasks = new Stack<>(); // TODO: replace with Deque

  /**
   * A task that is currently applying and may generate new RelNode.
   * It provides a callback to schedule tasks for new RelNodes that
   * are registered during task performing.
   */
  private GeneratorTask applying = null;

  /**
   * RelNodes that are generated by passThrough or derive
   * these nodes will not takes part in another passThrough or derive.
   */
  private Set<RelNode> passThroughCache = new HashSet<>();

  //~ Constructors -----------------------------------------------------------

  TopDownRuleDriver(VolcanoPlanner planner) {
    this.planner = planner;
    ruleQueue = new TopDownRuleQueue(planner);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void drive() {
    TaskDescriptor description = new TaskDescriptor();

    // Starting from the root's OptimizeGroup task.
    tasks.push(new OptimizeGroup(planner.root, planner.infCost));

    // ensure materialized view roots get explored.
    // Note that implementation rules or enforcement rules are not applied
    // unless the mv is matched
    exploreMaterializationRoots();

    try {
      // Iterates until the root is fully optimized
      while (!tasks.isEmpty()) {
        Task task = tasks.pop();
        description.log(task);
        task.perform();
      }
    } catch (VolcanoTimeoutException ex) {
      LOGGER.warn("Volcano planning times out, cancels the subsequent optimization.");
    }
  }

  private void exploreMaterializationRoots() {
    for (RelSubset extraRoot : planner.explorationRoots) {
      RelSet rootSet = VolcanoPlanner.equivRoot(extraRoot.set);
      if (rootSet == planner.root.set) {
        continue;
      }
      for (RelNode rel : extraRoot.set.rels) {
        if (planner.isLogical(rel)) {
          tasks.push(new OptimizeMExpr(rel, extraRoot, true));
        }
      }
    }
  }

  @Override public TopDownRuleQueue getRuleQueue() {
    return ruleQueue;
  }

  @Override public void clear() {
    ruleQueue.clear();
    tasks.clear();
    passThroughCache.clear();
    applying = null;
  }

  /** Procedure. */
  private interface Procedure {
    void exec();
  }

  private void applyGenerator(GeneratorTask task, Procedure proc) {
    GeneratorTask applying = this.applying;
    this.applying = task;
    try {
      proc.exec();
    } finally {
      this.applying = applying;
    }
  }

  @Override public void onSetMerged(RelSet set) {
    // When RelSets get merged, an optimized group may get extra opportunities.
    // Clear the OPTIMISED state for the RelSubsets and all theirs ancestors
    // so that they will be optimized again
    applyGenerator(null, () -> clearProcessed(set));
  }

  private void clearProcessed(RelSet set) {
    boolean explored = set.exploringState != null;
    set.exploringState = null;

    for (RelSubset subset : set.subsets) {
      if (subset.resetTaskState() || explored) {
        Collection<RelNode> parentRels = subset.getParentRels();
        for (RelNode parentRel : parentRels) {
          clearProcessed(planner.getSet(parentRel));
        }
        if (subset == planner.root) {
          tasks.push(new OptimizeGroup(subset, planner.infCost));
        }
      }
    }
  }

  // a callback invoked when a RelNode is going to be added into a RelSubset,
  // either by Register or Reregister. The task driver should need to schedule
  // tasks for the new nodes.
  @Override public void onProduce(RelNode node, RelSubset subset) {

    // if the RelNode is added to another RelSubset, just ignore it.
    // It should be schedule in the later OptimizeGroup task
    if (applying == null || subset.set
        != VolcanoPlanner.equivRoot(applying.group().set)) {
      return;
    }

    // extra callback from each task
    if (!applying.onProduce(node)) {
      return;
    }

    if (!planner.isLogical(node)) {
      // For a physical node, schedule tasks to optimize its inputs.
      // The upper bound depends on all optimizing RelSubsets that this Rel belongs to.
      // If there are optimizing subsets that comes from the same RelSet,
      // invoke the passThrough method to generate a candidate for that Subset.
      RelSubset optimizingGroup = null;
      boolean canPassThrough = node instanceof PhysicalNode
          && !passThroughCache.contains(node);
      if (!canPassThrough && subset.taskState != null) {
        optimizingGroup = subset;
      } else {
        RelOptCost upperBound = planner.zeroCost;
        RelSet set = subset.getSet();
        List<RelSubset> subsetsToPassThrough = new ArrayList<>();
        for (RelSubset otherSubset : set.subsets) {
          if (!otherSubset.isRequired() || otherSubset != planner.root
              && otherSubset.taskState != RelSubset.OptimizeState.OPTIMIZING) {
            continue;
          }
          if (node.getTraitSet().satisfies(otherSubset.getTraitSet())) {
            if (upperBound.isLt(otherSubset.upperBound)) {
              upperBound = otherSubset.upperBound;
              optimizingGroup = otherSubset;
            }
          } else if (canPassThrough) {
            subsetsToPassThrough.add(otherSubset);
          }
        }
        for (RelSubset otherSubset : subsetsToPassThrough) {
          Task task = getOptimizeInputTask(node, otherSubset);
          if (task != null) {
            tasks.push(task);
          }
        }
      }
      if (optimizingGroup == null) {
        return;
      }
      Task task = getOptimizeInputTask(node, optimizingGroup);
      if (task != null) {
        tasks.push(task);
      }
    } else {
      boolean optimizing = subset.set.subsets.stream()
          .anyMatch(s -> s.taskState == RelSubset.OptimizeState.OPTIMIZING);
      tasks.push(
          new OptimizeMExpr(node, applying.group(),
              applying.exploring() && !optimizing));
    }
  }

  //~ Inner Classes ----------------------------------------------------------

  /**
   * Base class for planner task.
   */
  private interface Task {
    void perform();
    void describe(TaskDescriptor desc);
  }

  /**
   * A class for task logging.
   */
  private static class TaskDescriptor {
    private boolean first = true;
    private StringBuilder builder = new StringBuilder();

    void log(Task task) {
      if (!LOGGER.isDebugEnabled()) {
        return;
      }
      first = true;
      builder.setLength(0);
      builder.append("Execute task: ").append(task.getClass().getSimpleName());
      task.describe(this);
      if (!first) {
        builder.append(")");
      }

      LOGGER.info(builder.toString());
    }

    TaskDescriptor item(String name, Object value) {
      if (first) {
        first = false;
        builder.append("(");
      } else {
        builder.append(", ");
      }
      builder.append(name).append("=").append(value);
      return this;
    }
  }

  /** Task for generator. */
  private interface GeneratorTask extends Task {
    RelSubset group();
    boolean exploring();
    default boolean onProduce(RelNode node) {
      return true;
    }
  }

  /**
   * Optimize a RelSubset.
   * It schedule optimization tasks for RelNodes in the RelSet.
   */
  private class OptimizeGroup implements Task {
    private final RelSubset group;
    private RelOptCost upperBound;

    OptimizeGroup(RelSubset group, RelOptCost upperBound) {
      this.group = group;
      this.upperBound = upperBound;
    }

    @Override public void perform() {
      RelOptCost winner = group.getWinnerCost();
      if (winner != null) {
        return;
      }

      if (group.taskState != null && upperBound.isLe(group.upperBound)) {
        // either this group failed to optimize before or it is a ring
        return;
      }

      group.startOptimize(upperBound);

      // cannot decide an actual lower bound before MExpr are fully explored
      // so delay the lower bound checking

      // a gate keeper to update context
      tasks.push(new GroupOptimized(group));

      // optimize mExprs in group
      List<RelNode> physicals = new ArrayList<>();
      for (RelNode rel : group.set.rels) {
        if (planner.isLogical(rel)) {
          tasks.push(new OptimizeMExpr(rel, group, false));
        } else if (rel.isEnforcer()) {
          // Enforcers have lower priority than other physical nodes
          physicals.add(0, rel);
        } else {
          physicals.add(rel);
        }
      }

      // No need to apply O_INPUTS if the group has not been implemented yet.
      // It only happens when enabling AbstractConverter.
      if (group.getConvention() == Convention.NONE) {
        LOGGER.debug("Skip optimizing because of none convention: {}", group);
        return;
      }

      // always apply O_INPUTS first so as to get an valid upper bound
      for (RelNode rel : physicals) {
        Task task = getOptimizeInputTask(rel, group);
        if (task != null) {
          tasks.add(task);
        }
      }
    }

    @Override public void describe(TaskDescriptor desc) {
      desc.item("group", group).item("upperBound", upperBound);
    }
  }

  /**
   * Mark the RelSubset optimized.
   * When GroupOptimized returns, the group is either fully
   * optimized and has a winner or failed to optimized
   */
  private static class GroupOptimized implements Task {
    private final RelSubset group;

    GroupOptimized(RelSubset group) {
      this.group = group;
    }

    @Override public void perform() {
      group.setOptimized();
    }

    @Override public void describe(TaskDescriptor desc) {
      desc.item("group", group);
    }
  }

  /**
   * Optimize a logical node, including exploring its input and applying rules for it.
   */
  private class OptimizeMExpr implements Task {
    private final RelNode mExpr;
    private final RelSubset group;

    // when true, only apply transformation rules for mExpr
    private final boolean explore;

    OptimizeMExpr(RelNode mExpr,
        RelSubset group, boolean explore) {
      this.mExpr = mExpr;
      this.group = group;
      this.explore = explore;
    }

    @Override public void perform() {
      if (explore && group.isExplored()) {
        return;
      }
      // 1. explode input
      // 2. apply other rules
      tasks.push(new ApplyRules(mExpr, group, explore));
      for (int i = mExpr.getInputs().size() - 1; i >= 0; --i) {
        tasks.push(new ExploreInput(mExpr, i));
      }
    }

    @Override public void describe(TaskDescriptor desc) {
      desc.item("mExpr", mExpr).item("explore", explore);
    }
  }

  /**
   * Ensure that ExploreInputs are working on the correct input group.
   * Currently, a RelNode's input may change since calcite may merge RelSets.
   */
  private class EnsureGroupExplored implements Task {

    private final RelSubset input;
    private final RelNode parent;
    private final int inputOrdinal;

    EnsureGroupExplored(RelSubset input, RelNode parent, int inputOrdinal) {
      this.input = input;
      this.parent = parent;
      this.inputOrdinal = inputOrdinal;
    }

    @Override public void perform() {
      if (parent.getInput(inputOrdinal) != input) {
        tasks.push(new ExploreInput(parent, inputOrdinal));
        return;
      }
      input.setExplored();
      for (RelSubset subset : input.getSet().subsets) {
        // clear the LB cache as exploring state have changed
        input.getCluster().getMetadataQuery().clearCache(subset);
      }
    }

    @Override public void describe(TaskDescriptor desc) {
      desc.item("mExpr", parent).item("i", inputOrdinal);
    }
  }

  /**
   * Explore an input for a RelNode.
   */
  private class ExploreInput implements Task {
    private final RelSubset group;
    private final RelNode parent;
    private final int inputOrdinal;

    ExploreInput(RelNode parent, int inputOrdinal) {
      this.group = (RelSubset) parent.getInput(inputOrdinal);
      this.parent = parent;
      this.inputOrdinal = inputOrdinal;
    }

    @Override public void perform() {
      if (!group.explore()) {
        return;
      }
      tasks.push(new EnsureGroupExplored(group, parent, inputOrdinal));
      for (RelNode rel : group.set.rels) {
        if (planner.isLogical(rel)) {
          tasks.push(new OptimizeMExpr(rel, group, true));
        }
      }
    }

    @Override public void describe(TaskDescriptor desc) {
      desc.item("group", group);
    }
  }

  /**
   * Extract rule matches from rule queue and add them to task stack.
   */
  private class ApplyRules implements Task {
    private final RelNode mExpr;
    private final RelSubset group;
    private final boolean exploring;

    ApplyRules(RelNode mExpr, RelSubset group, boolean exploring) {
      this.mExpr = mExpr;
      this.group = group;
      this.exploring = exploring;
    }

    @Override public void perform() {
      Pair<RelNode, Predicate<VolcanoRuleMatch>> category =
          exploring ? Pair.of(mExpr, planner::isTransformationRule)
              : Pair.of(mExpr, m -> true);
      VolcanoRuleMatch match = ruleQueue.popMatch(category);
      while (match != null) {
        tasks.push(new ApplyRule(match, group, exploring));
        match = ruleQueue.popMatch(category);
      }
    }

    @Override public void describe(TaskDescriptor desc) {
      desc.item("mExpr", mExpr).item("exploring", exploring);
    }
  }

  /**
   * Apply a rule match.
   */
  private class ApplyRule implements GeneratorTask {
    private final VolcanoRuleMatch match;
    private final RelSubset group;
    private final boolean exploring;

    ApplyRule(VolcanoRuleMatch match, RelSubset group, boolean exploring) {
      this.match = match;
      this.group = group;
      this.exploring = exploring;
    }

    @Override public void describe(TaskDescriptor desc) {
      desc.item("match", match).item("exploring", exploring);
    }

    @Override public void perform() {
      applyGenerator(this, match::onMatch);
    }

    @Override public RelSubset group() {
      return group;
    }

    @Override public boolean exploring() {
      return exploring;
    }
  }

  // Decide how to optimize a physical node.
  private Task getOptimizeInputTask(RelNode rel, RelSubset group) {
    // If the physical does not in current optimizing RelSubset, it firstly tries to
    // convert the physical node either by converter rule or traits pass though.
    if (!rel.getTraitSet().satisfies(group.getTraitSet())) {
      RelNode passThroughRel = convert(rel, group);
      if (passThroughRel == null) {
        LOGGER.debug("Skip optimizing because of traits: {}", rel);
        return null;
      }
      final RelNode finalPassThroughRel = passThroughRel;
      applyGenerator(null, () ->
          planner.register(finalPassThroughRel, group));
      rel = passThroughRel;
    }
    boolean unProcess = false;
    for (RelNode input : rel.getInputs()) {
      RelOptCost winner = ((RelSubset) input).getWinnerCost();
      if (winner == null) {
        unProcess = true;
        break;
      }
    }
    // If the inputs are all processed, only DeriveTrait is required.
    if (!unProcess) {
      return new DeriveTrait(rel, group);
    }
    // If part of the inputs are not optimized, schedule for the node an OptimizeInput task,
    // which tried to optimize the inputs first and derive traits for further execution.
    if (rel.getInputs().size() == 1) {
      return new OptimizeInput1(rel, group);
    }
    return new OptimizeInputs(rel, group);
  }

  // Try to convert the physical node to another trait sets,
  // either by converter rule or traits pass through.
  private RelNode convert(RelNode rel, RelSubset group) {
    if (!passThroughCache.contains(rel)) {
      if (checkLowerBound(rel, group)) {
        RelNode passThrough = group.passThrough(rel);
        if (passThrough != null) {
          assert passThrough.getConvention() == rel.getConvention();
          passThroughCache.add(passThrough);
          return passThrough;
        }
      } else {
        LOGGER.debug("Skip pass though because of lower bound. LB = {}, UP = {}",
            rel, group.upperBound);
      }
    }
    VolcanoRuleMatch match = ruleQueue.popMatch(
        Pair.of(rel,
            m -> m.getRule() instanceof ConverterRule
                && m.getRule().getOutTrait().satisfies(group.getTraitSet().getConvention())));
    if (match != null) {
      tasks.add(new ApplyRule(match, group, false));
    }
    return null;
  }

  // check whether a node's lower bound is less than a RelSubset's upper bound
  private boolean checkLowerBound(RelNode rel, RelSubset group) {
    RelOptCost upperBound = group.upperBound;
    if (upperBound.isInfinite()) {
      return true;
    }
    RelOptCost lb = planner.getLowerBound(rel);
    return !upperBound.isLe(lb);
  }

  /**
   * A task that optimize input for physical nodes who has only one input.
   * This task can be replaced by OptimizeInputs but simplify lots of logic.
   */
  private class OptimizeInput1 implements Task {

    private final RelNode mExpr;
    private final RelSubset group;

    OptimizeInput1(RelNode mExpr, RelSubset group) {
      this.mExpr = mExpr;
      this.group = group;
    }

    @Override public void describe(TaskDescriptor desc) {
      desc.item("mExpr", mExpr).item("upperBound", group.upperBound);
    }

    @Override public void perform() {
      RelOptCost upperBound = group.upperBound;
      RelOptCost upperForInput = planner.upperBoundForInputs(mExpr, upperBound);
      if (upperForInput.isLe(planner.zeroCost)) {
        LOGGER.debug(
            "Skip O_INPUT because of lower bound. UB4Inputs = {}, UB = {}",
            upperForInput, upperBound);
        return;
      }

      RelSubset input = (RelSubset) mExpr.getInput(0);

      // Apply enforcing rules
      tasks.push(new DeriveTrait(mExpr, group));

      tasks.push(new CheckInput(null, mExpr, input, 0, upperForInput));
      tasks.push(new OptimizeGroup(input, upperForInput));
    }
  }

  /**
   * Optimize a physical node's inputs.
   * This task calculates a proper upper bound for the input and invoke
   * the OptimizeGroup task. Group pruning mainly happens here when
   * the upper bound for an input is less than the input's lower bound
   */
  private class OptimizeInputs implements Task {

    private final RelNode mExpr;
    private final RelSubset group;
    private final int childCount;
    private RelOptCost upperBound;
    private RelOptCost upperForInput;
    private int processingChild;

    OptimizeInputs(RelNode rel, RelSubset group) {
      this.mExpr = rel;
      this.group = group;
      this.upperBound = group.upperBound;
      this.upperForInput = planner.infCost;
      this.childCount = rel.getInputs().size();
      this.processingChild = 0;
    }

    @Override public void describe(TaskDescriptor desc) {
      desc.item("mExpr", mExpr).item("upperBound", upperBound)
          .item("processingChild", processingChild);
    }

    private List<RelOptCost> lowerBounds;
    private RelOptCost lowerBoundSum;
    @Override public void perform() {
      RelOptCost bestCost = group.bestCost;
      if (!bestCost.isInfinite()) {
        // calculate the upper bound for inputs
        if (bestCost.isLt(upperBound)) {
          upperBound = bestCost;
          upperForInput = planner.upperBoundForInputs(mExpr, upperBound);
        }

        if (lowerBoundSum == null) {
          if (upperForInput.isInfinite()) {
            upperForInput = planner.upperBoundForInputs(mExpr, upperBound);
          }
          lowerBounds = new ArrayList<>(childCount);
          for (RelNode input : mExpr.getInputs()) {
            RelOptCost lb = planner.getLowerBound(input);
            lowerBounds.add(lb);
            lowerBoundSum = lowerBoundSum == null ? lb : lowerBoundSum.plus(lb);
          }
        }
        if (upperForInput.isLt(lowerBoundSum)) {
          LOGGER.debug(
              "Skip O_INPUT because of lower bound. LB = {}, UP = {}",
              lowerBoundSum, upperForInput);
          return; // group pruned
        }
      }

      if (lowerBoundSum != null && lowerBoundSum.isInfinite()) {
        LOGGER.debug("Skip O_INPUT as one of the inputs fail to optimize");
        return;
      }

      if (processingChild == 0) {
        // derive traits after all inputs are optimized successfully
        tasks.push(new DeriveTrait(mExpr, group));
      }

      while (processingChild < childCount) {
        RelSubset input =
            (RelSubset) mExpr.getInput(processingChild);

        RelOptCost winner = input.getWinnerCost();
        if (winner != null) {
          ++ processingChild;
          continue;
        }

        RelOptCost upper = upperForInput;
        if (!upper.isInfinite()) {
          // UB(one input)
          //  = UB(current subset) - Parent's NonCumulativeCost - LB(other inputs)
          //  = UB(current subset) - Parent's NonCumulativeCost - LB(all inputs) + LB(current input)
          upper = upperForInput.minus(lowerBoundSum)
              .plus(lowerBounds.get(processingChild));
        }
        if (input.taskState != null && upper.isLe(input.upperBound)) {
          LOGGER.debug("Failed to optimize because of upper bound. LB = {}, UP = {}",
              lowerBoundSum, upperForInput);
          return;
        }

        if (processingChild != childCount - 1) {
          tasks.push(this);
        }
        tasks.push(new CheckInput(this, mExpr, input, processingChild, upper));
        tasks.push(new OptimizeGroup(input, upper));
        ++ processingChild;
        break;
      }
    }
  }

  /**
   * Ensure input is optimized correctly and modify context.
   */
  private class CheckInput implements Task {

    private final OptimizeInputs context;
    private final RelOptCost upper;
    private final RelNode parent;
    private RelSubset input;
    private final int i;

    @Override public void describe(TaskDescriptor desc) {
      desc.item("parent", parent).item("i", i);
    }

    CheckInput(OptimizeInputs context,
        RelNode parent, RelSubset input, int i, RelOptCost upper) {
      this.context = context;
      this.parent = parent;
      this.input = input;
      this.i = i;
      this.upper = upper;
    }

    @Override public void perform() {
      if (input != parent.getInput(i)) {
        // The input has chnaged. So reschedule the optimize task.
        input = (RelSubset) parent.getInput(i);
        tasks.push(this);
        tasks.push(new OptimizeGroup(input, upper));
        return;
      }

      // Optimize input completed. Update the context for other inputs
      if (context == null) {
        // If there is no other input, just return (no need to optimize other inputs)
        return;
      }

      RelOptCost winner = input.getWinnerCost();
      if (winner == null) {
        // The input fail to optimize due to group pruning
        // Then there's no need to optimize other inputs.
        context.lowerBoundSum = planner.infCost;
        return;
      }

      // Update the context.
      if (context.lowerBoundSum != null && context.lowerBoundSum != planner.infCost) {
        context.lowerBoundSum = context.
            lowerBoundSum.minus(context.lowerBounds.get(i));
        context.lowerBoundSum = context.lowerBoundSum.plus(winner);
        context.lowerBounds.set(i, winner);
      }
    }
  }

  /**
   * Derive traits for already optimized physical nodes.
   */
  private class DeriveTrait implements GeneratorTask {

    private final RelNode mExpr;
    private final RelSubset group;

    DeriveTrait(RelNode mExpr, RelSubset group) {
      this.mExpr = mExpr;
      this.group = group;
    }

    @Override public void perform() {
      List<RelNode> inputs = mExpr.getInputs();
      for (RelNode input : inputs) {
        if (((RelSubset) input).getWinnerCost() == null) {
          // fail to optimize input, then no need to deliver traits
          return;
        }
      }

      // In case some implementations use rules to convert between different physical conventions.
      // Note that this is deprecated and will be removed in the future.
      tasks.push(new ApplyRules(mExpr, group, false));

      // Derive traits from inputs
      if (!passThroughCache.contains(mExpr)) {
        applyGenerator(this, this::derive);
      }
    }

    private void derive() {
      if (!(mExpr instanceof PhysicalNode)
          || ((PhysicalNode) mExpr).getDeriveMode() == DeriveMode.PROHIBITED) {
        return;
      }

      PhysicalNode rel = (PhysicalNode) mExpr;
      DeriveMode mode = rel.getDeriveMode();
      int arity = rel.getInputs().size();
      // for OMAKASE
      List<List<RelTraitSet>> inputTraits = new ArrayList<>(arity);

      for (int i = 0; i < arity; i++) {
        int childId = i;
        if (mode == DeriveMode.RIGHT_FIRST) {
          childId = arity - i - 1;
        }

        RelSubset input = (RelSubset) rel.getInput(childId);
        List<RelTraitSet> traits = new ArrayList<>();
        inputTraits.add(traits);

        final int numSubset = input.set.subsets.size();
        for (int j = 0; j < numSubset; j++) {
          RelSubset subset = input.set.subsets.get(j);
          if (!subset.isDelivered() || subset.getTraitSet()
              .equalsSansConvention(rel.getCluster().traitSet())) {
            // Ideally we should stop deriving new relnodes when the
            // subset's traitSet equals with input traitSet, but
            // in case someone manually builds a physical relnode
            // tree, which is highly discouraged, without specifying
            // correct traitSet, e.g.
            //   EnumerableFilter  [].ANY
            //       -> EnumerableMergeJoin  [a].Hash[a]
            // We should still be able to derive the correct traitSet
            // for the dumb filter, even though the filter's traitSet
            // should be derived from the MergeJoin when it is created.
            // But if the subset's traitSet equals with the default
            // empty traitSet sans convention (the default traitSet
            // from cluster may have logical convention, NONE, which
            // is not interesting), we are safe to ignore it, because
            // a physical filter with non default traitSet, but has a
            // input with default empty traitSet, e.g.
            //   EnumerableFilter  [a].Hash[a]
            //       -> EnumerableProject  [].ANY
            // is definitely wrong, we should fail fast.
            continue;
          }

          if (mode == DeriveMode.OMAKASE) {
            traits.add(subset.getTraitSet());
          } else {
            RelNode newRel = rel.derive(subset.getTraitSet(), childId);
            if (newRel != null && !planner.isRegistered(newRel)) {
              RelNode newInput = newRel.getInput(childId);
              assert newInput instanceof RelSubset;
              if (newInput == subset) {
                // If the child subset is used to derive new traits for
                // current relnode, the subset will be marked REQUIRED
                // when registering the new derived relnode and later
                // will add enforcers between other delivered subsets.
                // e.g. a MergeJoin request both inputs hash distributed
                // by [a,b] sorted by [a,b]. If the left input R1 happens to
                // be distributed by [a], the MergeJoin can derive new
                // traits from this input and request both input to be
                // distributed by [a] sorted by [a,b]. In case there is a
                // alternative R2 with ANY distribution in the left input's
                // RelSet, we may end up with requesting hash distribution
                // [a] on alternative R2, which is unnecessary and waste,
                // because we request distribution by [a] because of R1 can
                // deliver the exact same distribution and we don't need to
                // enforce properties on other subsets that can't satisfy
                // the specific trait requirement.
                // Here we add a constraint that {@code newInput == subset},
                // because if the delivered child subset is HASH[a], but
                // we require HASH[a].SORT[a,b], we still need to enable
                // property enforcement on the required subset. Otherwise,
                // we need to restrict enforcement between HASH[a].SORT[a,b]
                // and HASH[a] only, which will make things a little bit
                // complicated. We might optimize it in the future.
                subset.disableEnforcing();
              }
              RelSubset relSubset = planner.register(newRel, rel);
              assert relSubset.set == planner.getSubset(rel).set;
            }
          }
        }

        if (mode == DeriveMode.LEFT_FIRST
            || mode == DeriveMode.RIGHT_FIRST) {
          break;
        }
      }

      if (mode == DeriveMode.OMAKASE) {
        List<RelNode> relList = rel.derive(inputTraits);
        for (RelNode relNode : relList) {
          if (!planner.isRegistered(relNode)) {
            planner.register(relNode, rel);
          }
        }
      }
    }

    @Override public void describe(TaskDescriptor desc) {
      desc.item("mExpr", mExpr).item("group", group);
    }

    @Override public RelSubset group() {
      return group;
    }

    @Override public boolean exploring() {
      return false;
    }

    @Override public boolean onProduce(RelNode node) {
      passThroughCache.add(node);
      return true;
    }
  }
}
