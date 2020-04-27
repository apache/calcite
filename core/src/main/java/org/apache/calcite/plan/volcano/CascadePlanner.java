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

import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.SubstitutionRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.Stack;

/**
 * CascadePlanner that apply memo pruning logic`
 * based on the data structure of VolcanoPlanner
 */
public class CascadePlanner extends VolcanoPlanner {

  //~ Instance fields --------------------------------------------------------

  private final RelOptCost infCost;

  /**
   * the rule queue designed for top-down rule applying
   */
  private CascadeRuleQueue cascadeRuleQueue = new CascadeRuleQueue(this);

  /**
   * all tasks waiting for execution
   */
  private Stack<Task> tasks = new Stack<>();

  /**
   * the current ApplyRule task. It provides a callback to schedule tasks for
   * new RelNodes that registered by the transformTo method call during
   * ApplyRule.perform
   */
  private ApplyRule applying = null;

  //~ Constructors -----------------------------------------------------------

  public CascadePlanner(RelOptCostFactory factory, Context context) {
    super(factory, context);
    infCost = costFactory.makeInfiniteCost();
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * check whether a rule match is a transformation rule match
   * @param match the rule match to check
   * @return true if the rule match is a transformation rule match
   */
  protected boolean isTransformationRule(VolcanoRuleMatch match) {
    // TODO: recognize rule type more precisely
    if (match.getRule() instanceof SubstitutionRule) {
      return true;
    }
    if (match.getRule() instanceof ConverterRule) {
      return false;
    }
    return match.getRule().getOperand().trait == Convention.NONE
        || match.getRule().getOperand().trait == null;
  }

  /**
   * check whether a rule match is a substitute rule match
   * @param match the rule match to check
   * @return true if the rule match is a substitute rule match
   */
  protected boolean isSubstituteRule(VolcanoRuleMatch match) {
    return match.getRule() instanceof SubstitutionRule;
  }

  /**
   * gets the lower bound cost of a relational operator
   * @param rel the rel node
   * @return the lower bound cost of the given rel. The value is ensured NOT NULL.
   */
  protected RelOptCost getLowerBound(RelNode rel) {
    RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
    RelOptCost lowerBound = mq.getLowerBoundCost(rel, this);
    if (lowerBound == null) {
      return zeroCost;
    }
    return lowerBound;
  }

  /**
   * Gets the lower bound cost of a RelOptRuleCall.
   * A match match with inputs whose Sum(LB) is higher than upper bound
   * needs not to be applied
   */
  protected RelOptCost getLowerBound(RelOptRuleCall match) {
    return getLowerBoundInternal(match, match.getOperand0());
  }

  private RelOptCost getLowerBoundInternal(
      RelOptRuleCall match, RelOptRuleOperand op) {
    List<RelOptRuleOperand> children = op.getChildOperands();
    if (children == null || children.isEmpty()) {
      return getLowerBound((RelNode) match.rel(op.ordinalInRule));
    }
    RelOptCost sum = null;
    for (RelOptRuleOperand child : children) {
      RelOptCost lb = getLowerBoundInternal(match, child);
      sum = sum == null ? lb : sum.plus(lb);
    }
    return sum;
  }

  /**
   * Gets the upper bound of its inputs.
   * Allow users to overwrite this method as some implementations may have
   * different cost model on some RelNodes, like Spool.
   */
  protected RelOptCost upperBoundForInputs(
      RelNode mExpr, RelOptCost upperBound) {
    if (!upperBound.isInfinite()) {
      RelOptCost rootCost = mExpr.computeSelfCost(CascadePlanner.this,
          mExpr.getCluster().getMetadataQuery());
      return upperBound.minus(rootCost);
    }
    return upperBound;
  }

  @Override public RelNode findBestExp() {
    registerMaterializations();
    TaskDescriptor description = new TaskDescriptor();
    tasks.push(
        new OptimizeGroup((CascadeRelSubset) root,
            costFactory.makeInfiniteCost()));
    while (!tasks.isEmpty()) {
      Task task = tasks.pop();
      description.log(task);
      task.perform();
    }
    return root.buildCheapestPlan(this);
  }

  @Override protected RelSubset addRelToSet(RelNode rel, RelSet set) {
    RelSubset relSubset = super.addRelToSet(rel, set);
    if (applying != null && set.id == applying.group.set.id) {
      applying.onProduce(rel);
    }
    return relSubset;
  }

  protected RelSet merge(RelSet set, RelSet set2) {
    ApplyRule applying = this.applying;
    this.applying = null;
    try {
      RelSet merge = super.merge(set, set2);
      RelSet canonize = canonize(merge);
      clearProcessed((CascadeRelSet) canonize);
      return canonize;
    } finally {
      this.applying = applying;
    }
  }

  private void clearProcessed(CascadeRelSet set) {
    if (set == null || set.subsets == null) {
      return;
    }
    boolean explored = set.state != CascadeRelSet.ExploreState.NEW;
    set.state = CascadeRelSet.ExploreState.NEW;

    for (RelSubset subset : set.subsets) {
      CascadeRelSubset group = (CascadeRelSubset) subset;
      if (group.resetOptimizing() || explored) {
        Collection<RelNode> parentRels = subset.getParentRels();
        for (RelNode parentRel : parentRels) {
          clearProcessed((CascadeRelSet) getSet(parentRel));
        }
      }
    }
  }

  public boolean isPruned(RelNode rel) {
    return prunedNodes.contains(rel);
  }

  @Override void fireRules(RelNode rel) {
    for (RelOptRuleOperand operand : classOperands.get(rel.getClass())) {
      if (!operand.matches(rel)) {
        continue;
      }
      VolcanoRuleCall call = new CascadeRuleCall(this, operand);
      call.match(rel);
    }
  }

  @Override void ensureRootConverters() {
    RelSet rootSet = equivRoot(this.root.set);
    if (rootSet != this.root.set) {
      this.root = rootSet.getSubset(root.getTraitSet());
    }
  }

  @Override protected RelSet newRelSet(int id,
      Set<CorrelationId> variablesPropagated,
      Set<CorrelationId> variablesUsed) {
    return new CascadeRelSet(id, variablesPropagated, variablesUsed);
  }

  /**
   * A helper class that add rule match to rule queue during onMatch
   */
  private static class CascadeRuleCall extends VolcanoRuleCall {
    CascadeRuleCall(CascadePlanner planner, RelOptRuleOperand operand) {
      super(planner, operand);
    }

    @Override protected void onMatch() {
      ((CascadePlanner) volcanoPlanner).cascadeRuleQueue.addMatch(
          new VolcanoRuleMatch(
              volcanoPlanner,
              getOperand0(),
              rels,
              nodeInputs));
    }
  }

  private RelSet canonize(RelSet set) {
    while (set.equivalentSet != null) {
      set = set.equivalentSet;
    }
    return set;
  }

  private boolean isLogical(RelNode relNode) {
    return relNode.getTraitSet().getTrait(ConventionTraitDef.INSTANCE)
        == Convention.NONE;
  }

  //~ Inner Classes ----------------------------------------------------------

  /**
   * Base class for planner task
   */
  private interface Task {
    void perform();
    void describe(TaskDescriptor desc);
  }

  /**
   * A class for task logging
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

      LOGGER.debug(builder.toString());
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

  /**
   * O_GROUP
   */
  private class OptimizeGroup implements Task {
    private final CascadeRelSubset group;
    private RelOptCost upperBound;
    private boolean acceptConverter;

    OptimizeGroup(CascadeRelSubset group, RelOptCost upperBound) {
      this(group, upperBound, true);
    }

    OptimizeGroup(CascadeRelSubset group, RelOptCost upperBound, boolean acceptConverter) {
      this.group = group;
      this.upperBound = upperBound;
      this.acceptConverter = acceptConverter;
    }

    @Override public void perform() {
      RelOptCost winner = group.getWinner();
      if (winner != null) {
        return;
      }

      if (!group.bestCost.isInfinite()
          && group.bestCost.isLt(upperBound)) {
        // this group is partial optimized, there may be other candidate in the group
        // prevOptimizeResult == null means this group is changed after previous optimizations
        upperBound = group.bestCost;
      }

      if (group.upperBound != null
          && upperBound.isLe(group.upperBound)) {
        // this group failed to optimize before or it is a ring
        return;
      }

      group.startOptimize(upperBound);

      // cannot decide an actual lower bound before MExpr are fully explored
      // so delay the lower bound checking

      // a gate keeper to update context
      tasks.add(new GroupOptimized(group));

      // optimize mExprs in group
      List<Task> physicals = new ArrayList<>();
      for (RelNode rel : group.set.rels) {
        if (isLogical(rel)) {
          tasks.add(new OptimizeMExpr(rel, group, upperBound, false));
        } else {
          if (!acceptConverter && rel instanceof AbstractConverter) {
            LOGGER.debug("Skip optimizing AC: {}", rel);
            continue;
          }
          Task task = getOptimizeInputTask(rel, group, upperBound);
          if (task != null) {
            physicals.add(task);
          }
        }
      }

      // always apply O_INPUTS first so as to get an valid upper bound
      tasks.addAll(physicals);
    }

    @Override public void describe(TaskDescriptor desc) {
      desc.item("group", group).item("upperBound", upperBound);
    }
  }

  /**
   * Mark the group optimized
   */
  private static class GroupOptimized implements Task {
    private final CascadeRelSubset group;

    GroupOptimized(CascadeRelSubset group) {
      this.group = group;
    }

    @Override public void perform() {
      group.optimized();
    }

    @Override public void describe(TaskDescriptor desc) {
      desc.item("group", group);
    }
  }

  /**
   * O_EXPR
   */
  private class OptimizeMExpr implements Task {
    private final RelNode mExpr;
    private final CascadeRelSubset group;
    private final RelOptCost upperBound;
    private final boolean explore;

    OptimizeMExpr(RelNode mExpr,
        CascadeRelSubset group, RelOptCost upperBound, boolean explore) {
      this.mExpr = mExpr;
      this.group = group;
      this.upperBound = upperBound;
      this.explore = explore;
    }

    @Override public void perform() {
      if (explore && group.getSet().state
          == CascadeRelSet.ExploreState.EXPLORED) {
        return;
      }
      // 1. explode input
      // 2. apply other rules
      tasks.push(new ApplyRules(mExpr, group, upperBound, explore));
      for (int i = 0; i < mExpr.getInputs().size(); i++) {
        CascadeRelSubset input = (CascadeRelSubset) mExpr.getInput(i);
        if (input.getSet().state != CascadeRelSet.ExploreState.NEW) {
          continue;
        }
        tasks.push(new EnsureGroupExplored(input, mExpr, i));
        tasks.push(new ExploreInput(input));
      }
    }

    @Override public void describe(TaskDescriptor desc) {
      desc.item("mExpr", mExpr).item("explore", explore);
    }
  }

  /**
   * ensure ExploreInput are working on the correct input group since calcite
   * may merge sets
   */
  private class EnsureGroupExplored implements Task {

    private CascadeRelSubset input;
    private final RelNode mExpr;
    private final int i;

    EnsureGroupExplored(CascadeRelSubset input, RelNode mExpr, int i) {
      this.input = input;
      this.mExpr = mExpr;
      this.i = i;
    }

    @Override public void perform() {
      if (mExpr.getInput(i) != input) {
        input = (CascadeRelSubset) mExpr.getInput(i);
        tasks.push(this);
        tasks.push(new ExploreInput(input));
        return;
      }
      input.getSet().state = CascadeRelSet.ExploreState.EXPLORED;
      for (RelSubset subset : input.getSet().subsets) {
        // clear the LB cache as exploring state have changed
        mExpr.getCluster().getMetadataQuery().clearCache(subset);
      }
    }

    @Override public void describe(TaskDescriptor desc) {
      desc.item("mExpr", mExpr).item("i", i);
    }
  }

  /**
   * E_GROUP
   */
  private class ExploreInput implements Task {
    private final CascadeRelSubset group;

    ExploreInput(CascadeRelSubset group) {
      this.group = group;
    }

    @Override public void perform() {
      if (group.getSet().state != CascadeRelSet.ExploreState.NEW) {
        return;
      }
      for (RelNode rel : group.set.rels) {
        if (isLogical(rel)) {
          tasks.add(
              new OptimizeMExpr(
                  rel, group, costFactory.makeInfiniteCost(), true));
        }
      }
      group.getSet().state = CascadeRelSet.ExploreState.EXPLORING;
    }

    @Override public void describe(TaskDescriptor desc) {
      desc.item("group", group);
    }
  }

  /**
   * extract rule matches from rule queue and add them to task stack
   */
  private class ApplyRules implements Task {
    private final RelNode mExpr;
    private final CascadeRelSubset group;
    private final RelOptCost upperBound;
    private final boolean exploring;

    ApplyRules(RelNode mExpr,
        CascadeRelSubset group, RelOptCost upperBound, boolean exploring) {
      this.mExpr = mExpr;
      this.group = group;
      this.upperBound = upperBound;
      this.exploring = exploring;
    }

    @Override public void perform() {
      VolcanoRuleMatch match = cascadeRuleQueue.popMatch(mExpr, exploring);
      while (match != null) {
        tasks.push(new ApplyRule(match, group, upperBound, exploring));
        match = cascadeRuleQueue.popMatch(mExpr, exploring);
      }
    }

    @Override public void describe(TaskDescriptor desc) {
      desc.item("mExpr", mExpr).item("exploring", exploring);
    }
  }

  /**
   * APPLY_RULE
   */
  private class ApplyRule implements Task {
    private final VolcanoRuleMatch match;
    private final CascadeRelSubset group;
    private final boolean exploring;
    private RelOptCost upperBound;

    ApplyRule(VolcanoRuleMatch match,
        CascadeRelSubset group, RelOptCost upperBound, boolean exploring) {
      this.match = match;
      this.group = group;
      this.upperBound = upperBound;
      this.exploring = exploring;
    }

    @Override public void describe(TaskDescriptor desc) {
      desc.item("match", match).item("exploring", exploring);
    }

    @Override public void perform() {
      if (!exploring && !isLogical(match.rel(0)) && !checkLowerBound()) {
        // for implementation and enforcing rules,
        // we can skip rule match by checking the lower bound
        // however, we cannot decide LB for logical nodes,
        // so we only check enforcing rules now
        if (!group.getTraitSet().satisfies(match.rels[0].getTraitSet())) {
          // the match cannot be discarded directly
          // because of another subset's upper bound
          cascadeRuleQueue.addMatch(match);
        }
        return;
      }
      try {
        applying = this;
        match.onMatch();
      } finally {
        applying = null;
      }
    }

    private boolean checkLowerBound() {
      RelOptCost bestCost = group.bestCost;
      if (bestCost.isInfinite() && upperBound.isInfinite()) {
        return true;
      }
      if (!bestCost.isInfinite() && bestCost.isLt(upperBound)) {
        upperBound = bestCost;
      }
      RelOptCost lb = getLowerBound(match);
      if (upperBound.isLe(lb)) {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug(
              "Skip because of lower bound. LB = {}, UP = {}",
              lb, upperBound);
        }
        return false;
      }
      return true;
    }

    /**
     * A callback scheduling tasks for new produced RelNodes
     */
    public void onProduce(RelNode node) {
      if (!isLogical(node)) {
        RelOptCost upperBound = zeroCost;
        CascadeRelSubset group = null;

        CascadeRelSubset subset = (CascadeRelSubset) getSubset(node);
        if (subset.state != CascadeRelSubset.OptimizeState.NEW) {
          group = subset;
          upperBound = subset.upperBound;
        } else {
          if (!exploring) {
            group = this.group;
            upperBound = this.upperBound;
          }
          CascadeRelSet set = subset.getSet();
          for (RelSubset relSubset : set.subsets) {
            CascadeRelSubset g = (CascadeRelSubset) relSubset;
            if (g.state == CascadeRelSubset.OptimizeState.OPTIMIZING
                && node.getTraitSet().satisfies(subset.getTraitSet())) {
              if (upperBound.isLt(g.upperBound)) {
                upperBound = g.upperBound;
                group = g;
              }
            }
          }
        }
        if (group == null) {
          return;
        }
        Task task = getOptimizeInputTask(node, group, upperBound);
        if (task != null) {
          tasks.add(task);
        }
      } else {
        tasks.add(new OptimizeMExpr(node, group, upperBound, exploring));
      }
    }
  }

  private Task getOptimizeInputTask(RelNode rel,
      CascadeRelSubset group, RelOptCost upperBound) {
    if (!rel.getTraitSet().satisfies(group.getTraitSet())) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Skip optimizing because of traits: {}", rel);
      }
      return null;
    }
    boolean unProcess = false;
    for (RelNode input : rel.getInputs()) {
      RelOptCost winner = ((CascadeRelSubset) input).getWinner();
      if (winner == null) {
        unProcess = true;
        break;
      }
    }
    if (!unProcess) {
      // all input are processed, apply enforcing rules
      return new ApplyRules(rel, group, upperBound, false);
    }
    if (rel.getInputs().size() == 1) {
      return new OptimizeInput1(rel, group, upperBound);
    }
    return new OptimizeInputs(rel, group, upperBound);
  }

  /**
   * O_INPUT when there is only one input
   */
  private class OptimizeInput1 implements Task {

    private final RelNode mExpr;
    private final CascadeRelSubset group;
    private RelOptCost upperBound;

    OptimizeInput1(RelNode mExpr,
        CascadeRelSubset group, RelOptCost upperBound) {
      this.mExpr = mExpr;
      this.group = group;
      this.upperBound = upperBound;
    }


    @Override public void describe(TaskDescriptor desc) {
      desc.item("mExpr", mExpr).item("upperBound", upperBound);
    }

    @Override public void perform() {
      if (!group.bestCost.isInfinite() && group.bestCost.isLt(upperBound)) {
        upperBound = group.bestCost;
      }
      RelOptCost upperForInput = upperBoundForInputs(mExpr, upperBound);
      if (upperForInput.isLe(zeroCost)) {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug(
              "Skip O_INPUT because of lower bound. UB4Inputs = {}, UB = {}",
              upperForInput, upperBound);
        }
        return;
      }

      CascadeRelSubset input = (CascadeRelSubset) mExpr.getInput(0);

      // Apply enforcing rules
      tasks.add(new ApplyRules(mExpr, group, upperBound, false));

      tasks.push(new CheckInput(null, mExpr, input, 0, upperForInput));
      tasks.add(
          new OptimizeGroup(input,
              upperForInput, !(mExpr instanceof AbstractConverter)));
    }
  }

  /**
   * O_INPUT
   */
  private class OptimizeInputs implements Task {

    private final RelNode mExpr;
    private final CascadeRelSubset group;
    private final int childCount;
    private RelOptCost upperBound;
    private RelOptCost upperForInput;
    private int processingChild;

    OptimizeInputs(RelNode rel,
        CascadeRelSubset group, RelOptCost upperBound) {
      this.mExpr = rel;
      this.group = group;
      this.upperBound = upperBound;
      this.upperForInput = infCost;
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
      if (!upperBound.isInfinite() || !bestCost.isInfinite()) {
        if (!bestCost.isInfinite() && bestCost.isLt(upperBound)) {
          upperBound = bestCost;
          upperForInput = upperBoundForInputs(mExpr, upperBound);
        }

        if (lowerBoundSum == null) {
          if (upperForInput.isInfinite()) {
            upperForInput = upperBoundForInputs(mExpr, upperBound);
          }
          lowerBounds = new ArrayList<>(childCount);
          for (RelNode input : mExpr.getInputs()) {
            RelOptCost lb = getLowerBound(input);
            lowerBounds.add(lb);
            lowerBoundSum = lowerBoundSum == null ? lb : lowerBoundSum.plus(lb);
          }
        }
        if (upperForInput.isLt(lowerBoundSum)) {
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                "Skip O_INPUT because of lower bound. LB = {}, UP = {}",
                lowerBoundSum, upperForInput);
          }
          return; // group pruned
        }
      }

      if (lowerBoundSum != null && lowerBoundSum.isInfinite()) {
        LOGGER.debug("Skip O_INPUT as one of the inputs fail to optimize");
        return;
      }

      if (processingChild == 0) {
        // Apply enforcing rules
        tasks.add(new ApplyRules(mExpr, group, upperBound, false));
      }

      while (processingChild < childCount) {
        CascadeRelSubset input =
            (CascadeRelSubset) mExpr.getInput(processingChild);

        RelOptCost winner = input.getWinner();
        if (winner != null) {
          ++ processingChild;
          continue;
        }

        RelOptCost upper = upperForInput;
        if (!upper.isInfinite()) {
          upper = upperForInput.minus(lowerBoundSum)
              .plus(lowerBounds.get(processingChild));
        }
        if (input.upperBound != null && upper.isLe(input.upperBound)) {
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
   * ensure input is optimized correctly and modify context
   */
  private class CheckInput implements Task {

    private final OptimizeInputs context;
    private final RelOptCost upper;
    private final RelNode parent;
    private CascadeRelSubset input;
    private final int i;

    @Override public void describe(TaskDescriptor desc) {
      desc.item("parent", parent).item("i", i);
    }

    CheckInput(OptimizeInputs context,
        RelNode parent, CascadeRelSubset input, int i, RelOptCost upper) {
      this.context = context;
      this.parent = parent;
      this.input = input;
      this.i = i;
      this.upper = upper;
    }

    @Override public void perform() {
      if (input != parent.getInput(i)) {
        input = (CascadeRelSubset) parent.getInput(i);
        tasks.push(this);
        tasks.push(
            new OptimizeGroup(input,
                upper, !(parent instanceof AbstractConverter)));
        return;
      }
      if (context == null) {
        return;
      }
      RelOptCost winner = input.getWinner();
      if (winner == null) {
        // the input fail to optimize due to group pruning
        context.lowerBoundSum = infCost;
        return;
      }
      if (context.lowerBoundSum != null && context.lowerBoundSum != infCost) {
        context.lowerBoundSum = context.
            lowerBoundSum.minus(context.lowerBounds.get(i));
        context.lowerBoundSum = context.lowerBoundSum.plus(winner);
        context.lowerBounds.set(i, winner);
      }
    }
  }
}
