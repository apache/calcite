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
package org.apache.calcite.plan;

import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.util.CancelFlag;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.trace.CalciteTrace;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.initialization.qual.UnknownInitialization;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Pure;
import org.slf4j.Logger;

import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

import static org.apache.calcite.util.Static.RESOURCE;

import static java.util.Objects.requireNonNull;

/**
 * Abstract base for implementations of the {@link RelOptPlanner} interface.
 */
public abstract class AbstractRelOptPlanner implements RelOptPlanner {
  //~ Static fields/initializers ---------------------------------------------

  /** Logger for rule attempts information. */
  private static final Logger RULE_ATTEMPTS_LOGGER = CalciteTrace.getRuleAttemptsTracer();

  //~ Instance fields --------------------------------------------------------

  /**
   * Maps rule description to rule, just to ensure that rules' descriptions
   * are unique.
   */
  protected final Map<String, RelOptRule> mapDescToRule = new LinkedHashMap<>();

  protected final RelOptCostFactory costFactory;

  private @MonotonicNonNull MulticastRelOptListener listener;

  private @MonotonicNonNull RuleAttemptsListener ruleAttemptsListener;

  private @Nullable Pattern ruleDescExclusionFilter;

  protected final AtomicBoolean cancelFlag;

  private final Set<Class<? extends RelNode>> classes = new HashSet<>();

  private final Set<Convention> conventions = new HashSet<>();

  /** External context. Never null. */
  protected final Context context;

  private @Nullable RexExecutor executor;

  private @Nullable RelDecorrelator decorrelator;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates an AbstractRelOptPlanner.
   */
  protected AbstractRelOptPlanner(RelOptCostFactory costFactory,
      @Nullable Context context) {
    this.costFactory = requireNonNull(costFactory, "costFactory");
    if (context == null) {
      context = Contexts.empty();
    }
    this.context = context;

    this.cancelFlag =
        context.maybeUnwrap(CancelFlag.class)
            .map(flag -> flag.atomicBoolean)
            .orElseGet(AtomicBoolean::new);

    // Add abstract RelNode classes. No RelNodes will ever be registered with
    // these types, but some operands may use them.
    classes.add(RelNode.class);
    classes.add(RelSubset.class);

    if (RULE_ATTEMPTS_LOGGER.isDebugEnabled()) {
      this.ruleAttemptsListener = new RuleAttemptsListener();
      addListener(this.ruleAttemptsListener);
    }
    addListener(new RuleEventLogger());
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void clear() {}

  @Override public Context getContext() {
    return context;
  }

  @Override public RelOptCostFactory getCostFactory() {
    return costFactory;
  }

  @SuppressWarnings("deprecation")
  @Override public void setCancelFlag(CancelFlag cancelFlag) {
    // ignored
  }

  /**
   * Checks to see whether cancellation has been requested, and if so, throws
   * an exception.
   */
  public void checkCancel() {
    if (cancelFlag.get()) {
      throw RESOURCE.preparationAborted().ex();
    }
  }

  @Override public List<RelOptRule> getRules() {
    return ImmutableList.copyOf(mapDescToRule.values());
  }

  @Override public boolean addRule(RelOptRule rule) {
    // Check that there isn't a rule with the same description
    final String description = requireNonNull(rule.toString());

    RelOptRule existingRule = mapDescToRule.put(description, rule);
    if (existingRule != null) {
      if (existingRule.equals(rule)) {
        return false;
      } else {
        // This rule has the same description as one previously
        // registered, yet it is not equal. You may need to fix the
        // rule's equals and hashCode methods.
        throw new AssertionError("Rule's description should be unique; "
            + "existing rule=" + existingRule + "; new rule=" + rule);
      }
    }
    return true;
  }

  @Override public boolean removeRule(RelOptRule rule) {
    String description = rule.toString();
    RelOptRule removed = mapDescToRule.remove(description);
    return removed != null;
  }

  /**
   * Returns the rule with a given description.
   *
   * @param description Description
   * @return Rule with given description, or null if not found
   */
  protected @Nullable RelOptRule getRuleByDescription(String description) {
    return mapDescToRule.get(description);
  }

  @Override public void setRuleDescExclusionFilter(@Nullable Pattern exclusionFilter) {
    ruleDescExclusionFilter = exclusionFilter;
  }

  /**
   * Determines whether a given rule is excluded by ruleDescExclusionFilter.
   *
   * @param rule rule to test
   * @return true iff rule should be excluded
   */
  public boolean isRuleExcluded(RelOptRule rule) {
    return ruleDescExclusionFilter != null
        && ruleDescExclusionFilter.matcher(rule.toString()).matches();
  }

  @Override public RelOptPlanner chooseDelegate() {
    return this;
  }

  @Override public void addMaterialization(RelOptMaterialization materialization) {
    // ignore - this planner does not support materializations
  }

  @Override public List<RelOptMaterialization> getMaterializations() {
    return ImmutableList.of();
  }

  @Override public void addLattice(RelOptLattice lattice) {
    // ignore - this planner does not support lattices
  }

  @Override public @Nullable RelOptLattice getLattice(RelOptTable table) {
    // this planner does not support lattices
    return null;
  }

  @Override public void registerSchema(RelOptSchema schema) {
  }

  @Deprecated // to be removed before 2.0
  @Override public long getRelMetadataTimestamp(RelNode rel) {
    return 0;
  }

  @Override public void prune(RelNode rel) {
  }

  @Override public void registerClass(RelNode node) {
    final Class<? extends RelNode> clazz = node.getClass();
    if (classes.add(clazz)) {
      onNewClass(node);
    }
    Convention convention = node.getConvention();
    if (convention != null && conventions.add(convention)) {
      convention.register(this);
    }
  }

  /** Called when a new class of {@link RelNode} is seen. */
  protected void onNewClass(RelNode node) {
    node.register(this);
  }

  @Override public RelTraitSet emptyTraitSet() {
    return RelTraitSet.createEmpty();
  }

  @Override public @Nullable RelOptCost getCost(RelNode rel, RelMetadataQuery mq) {
    return mq.getCumulativeCost(rel);
  }

  @Deprecated // to be removed before 2.0
  @Override public @Nullable RelOptCost getCost(RelNode rel) {
    final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
    return getCost(rel, mq);
  }

  @Override public void addListener(
      @UnknownInitialization AbstractRelOptPlanner this,
      RelOptListener newListener) {
    if (listener == null) {
      listener = new MulticastRelOptListener();
    }
    listener.addListener(newListener);
  }

  @Deprecated // to be removed before 2.0
  @Override public void registerMetadataProviders(List<RelMetadataProvider> list) {
  }

  @Override public boolean addRelTraitDef(RelTraitDef relTraitDef) {
    return false;
  }

  @Override public void clearRelTraitDefs() {}

  @Override public List<RelTraitDef> getRelTraitDefs() {
    return ImmutableList.of();
  }

  @Override public void setExecutor(@Nullable RexExecutor executor) {
    this.executor = executor;
  }

  @Override public @Nullable RexExecutor getExecutor() {
    return executor;
  }

  @Override public void setDecorrelator(@Nullable RelDecorrelator decorrelator) {
    this.decorrelator = decorrelator;
  }

  @Override public RelDecorrelator getDecorrelator() {
    if (decorrelator == null) {
      throw new IllegalStateException("RelDecorrelator has not been set");
    }
    return decorrelator;
  }

  @Override public void onCopy(RelNode rel, RelNode newRel) {
    // do nothing
  }

  protected void dumpRuleAttemptsInfo() {
    if (this.ruleAttemptsListener != null) {
      RULE_ATTEMPTS_LOGGER.debug("Rule Attempts Info for " + this.getClass().getSimpleName());
      RULE_ATTEMPTS_LOGGER.debug(this.ruleAttemptsListener.dump());
    }
  }

  /**
   * Fires a rule, taking care of tracing and listener notification.
   *
   * @param ruleCall description of rule call
   */
  protected void fireRule(
      RelOptRuleCall ruleCall) {
    checkCancel();

    assert ruleCall.getRule().matches(ruleCall);
    if (isRuleExcluded(ruleCall.getRule())) {
      LOGGER.debug("call#{}: Rule [{}] not fired due to exclusion filter",
          ruleCall.id, ruleCall.getRule());
      return;
    }

    if (ruleCall.isRuleExcluded()) {
      LOGGER.debug("call#{}: Rule [{}] not fired due to exclusion hint",
          ruleCall.id, ruleCall.getRule());
      return;
    }

    if (listener != null) {
      RelOptListener.RuleAttemptedEvent event =
          new RelOptListener.RuleAttemptedEvent(
              this,
              ruleCall.rel(0),
              ruleCall,
              true);
      listener.ruleAttempted(event);
    }

    ruleCall.getRule().onMatch(ruleCall);

    if (listener != null) {
      RelOptListener.RuleAttemptedEvent event =
          new RelOptListener.RuleAttemptedEvent(
              this,
              ruleCall.rel(0),
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
   * @param newRel   result of transformation
   * @param before   true before registration of new rel; false after
   */
  protected void notifyTransformation(
      RelOptRuleCall ruleCall,
      RelNode newRel,
      boolean before) {
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
  protected void notifyChosen(RelNode rel) {
    LOGGER.debug("For final plan, using {}", rel);

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
      boolean physical) {
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
   * Takes care of tracing and listener notification when a rel is discarded.
   *
   * @param rel Discarded rel
   */
  protected void notifyDiscard(RelNode rel) {
    if (listener != null) {
      RelOptListener.RelDiscardedEvent event =
          new RelOptListener.RelDiscardedEvent(
              this,
              rel);
      listener.relDiscarded(event);
    }
  }

  @Pure
  public @Nullable RelOptListener getListener() {
    return listener;
  }

  /** Returns sub-classes of relational expression. */
  public Iterable<Class<? extends RelNode>> subClasses(
      final Class<? extends RelNode> clazz) {
    return Util.filter(classes, c -> {
      // RelSubset must be exact type, not subclass
      if (c == RelSubset.class) {
        return c == clazz;
      }
      return clazz.isAssignableFrom(c);
    });
  }

  /** Listener for counting the attempts of each rule. Only enabled under DEBUG level.*/
  private static class RuleAttemptsListener implements RelOptListener {
    private long beforeTimestamp;
    private final Map<String, Pair<Long, Long>> ruleAttempts;

    RuleAttemptsListener() {
      ruleAttempts = new HashMap<>();
    }

    @Override public void relEquivalenceFound(RelEquivalenceEvent event) {
    }

    @Override public void ruleAttempted(RuleAttemptedEvent event) {
      if (event.isBefore()) {
        this.beforeTimestamp = System.nanoTime();
      } else {
        long elapsed = (System.nanoTime() - this.beforeTimestamp) / 1000;
        String rule = event.getRuleCall().getRule().toString();
        ruleAttempts.compute(rule, (k, p) ->
            p == null
                ? Pair.of(1L,  elapsed)
                : Pair.of(p.left + 1, p.right + elapsed));
      }
    }

    @Override public void ruleProductionSucceeded(RuleProductionEvent event) {
    }

    @Override public void relDiscarded(RelDiscardedEvent event) {
    }

    @Override public void relChosen(RelChosenEvent event) {
    }

    public String dump() {
      // Sort rules by number of attempts descending, then by rule elapsed time descending,
      // then by rule name ascending.
      List<Map.Entry<String, Pair<Long, Long>>> list =
          new ArrayList<>(this.ruleAttempts.entrySet());
      list.sort((left, right) -> {
        int res = right.getValue().left.compareTo(left.getValue().left);
        if (res == 0) {
          res = right.getValue().right.compareTo(left.getValue().right);
        }
        if (res == 0) {
          res = left.getKey().compareTo(right.getKey());
        }
        return res;
      });

      // Print out rule attempts and time
      StringBuilder sb = new StringBuilder();
      sb.append(String
          .format(Locale.ROOT, "%n%-60s%20s%20s%n", "Rules", "Attempts", "Time (us)"));
      NumberFormat usFormat = NumberFormat.getNumberInstance(Locale.US);
      long totalAttempts = 0;
      long totalTime = 0;
      for (Map.Entry<String, Pair<Long, Long>> entry : list) {
        sb.append(
            String.format(Locale.ROOT, "%-60s%20s%20s%n",
                entry.getKey(),
                usFormat.format(entry.getValue().left),
                usFormat.format(entry.getValue().right)));
        totalAttempts += entry.getValue().left;
        totalTime += entry.getValue().right;
      }
      sb.append(
          String.format(Locale.ROOT, "%-60s%20s%20s%n",
              "* Total",
              usFormat.format(totalAttempts),
              usFormat.format(totalTime)));

      return sb.toString();
    }
  }
}
