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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.util.CancelFlag;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.trace.CalciteTrace;

import com.google.common.collect.ImmutableList;

import org.slf4j.Logger;

import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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

  private MulticastRelOptListener listener;

  private RuleAttemptsListener ruleAttemptsListener;

  private Pattern ruleDescExclusionFilter;

  protected final AtomicBoolean cancelFlag;

  private final Set<Class<? extends RelNode>> classes = new HashSet<>();

  private final Set<Convention> conventions = new HashSet<>();

  /** External context. Never null. */
  protected final Context context;

  private RexExecutor executor;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates an AbstractRelOptPlanner.
   */
  protected AbstractRelOptPlanner(RelOptCostFactory costFactory,
      Context context) {
    assert costFactory != null;
    this.costFactory = costFactory;
    if (context == null) {
      context = Contexts.empty();
    }
    this.context = context;

    final CancelFlag cancelFlag = context.unwrap(CancelFlag.class);
    this.cancelFlag = cancelFlag != null ? cancelFlag.atomicBoolean
        : new AtomicBoolean();

    // Add abstract RelNode classes. No RelNodes will ever be registered with
    // these types, but some operands may use them.
    classes.add(RelNode.class);
    classes.add(RelSubset.class);

    if (RULE_ATTEMPTS_LOGGER.isDebugEnabled()) {
      this.ruleAttemptsListener = new RuleAttemptsListener();
      addListener(this.ruleAttemptsListener);
    }
  }

  //~ Methods ----------------------------------------------------------------
  public void clear() {}

  public Context getContext() {
    return context;
  }

  public RelOptCostFactory getCostFactory() {
    return costFactory;
  }

  @SuppressWarnings("deprecation")
  public void setCancelFlag(CancelFlag cancelFlag) {
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

  public List<RelOptRule> getRules() {
    return ImmutableList.copyOf(mapDescToRule.values());
  }

  public boolean addRule(RelOptRule rule) {
    // Check that there isn't a rule with the same description
    final String description = rule.toString();
    assert description != null;

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

  public boolean removeRule(RelOptRule rule) {
    String description = rule.toString();
    RelOptRule removed = mapDescToRule.remove(description);
    return removed != null;
  }

  /**
   * Returns the rule with a given description
   *
   * @param description Description
   * @return Rule with given description, or null if not found
   */
  protected RelOptRule getRuleByDescription(String description) {
    return mapDescToRule.get(description);
  }

  public void setRuleDescExclusionFilter(Pattern exclusionFilter) {
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

  public RelOptPlanner chooseDelegate() {
    return this;
  }

  public void addMaterialization(RelOptMaterialization materialization) {
    // ignore - this planner does not support materializations
  }

  public List<RelOptMaterialization> getMaterializations() {
    return ImmutableList.of();
  }

  public void addLattice(RelOptLattice lattice) {
    // ignore - this planner does not support lattices
  }

  public RelOptLattice getLattice(RelOptTable table) {
    // this planner does not support lattices
    return null;
  }

  public void registerSchema(RelOptSchema schema) {
  }

  public long getRelMetadataTimestamp(RelNode rel) {
    return 0;
  }

  @Deprecated // to be removed before 1.24
  public void setImportance(RelNode rel, double importance) {
  }

  @Override public void prune(RelNode rel) {
  }

  public void registerClass(RelNode node) {
    final Class<? extends RelNode> clazz = node.getClass();
    if (classes.add(clazz)) {
      onNewClass(node);
    }
    if (conventions.add(node.getConvention())) {
      node.getConvention().register(this);
    }
  }

  /** Called when a new class of {@link RelNode} is seen. */
  protected void onNewClass(RelNode node) {
    node.register(this);
  }

  public RelTraitSet emptyTraitSet() {
    return RelTraitSet.createEmpty();
  }

  public RelOptCost getCost(RelNode rel, RelMetadataQuery mq) {
    return mq.getCumulativeCost(rel);
  }

  @SuppressWarnings("deprecation")
  public RelOptCost getCost(RelNode rel) {
    final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
    return getCost(rel, mq);
  }

  public void addListener(RelOptListener newListener) {
    if (listener == null) {
      listener = new MulticastRelOptListener();
    }
    listener.addListener(newListener);
  }

  public void registerMetadataProviders(List<RelMetadataProvider> list) {
  }

  public boolean addRelTraitDef(RelTraitDef relTraitDef) {
    return false;
  }

  public void clearRelTraitDefs() {}

  public List<RelTraitDef> getRelTraitDefs() {
    return ImmutableList.of();
  }

  public void setExecutor(RexExecutor executor) {
    this.executor = executor;
  }

  public RexExecutor getExecutor() {
    return executor;
  }

  public void onCopy(RelNode rel, RelNode newRel) {
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

    if (LOGGER.isDebugEnabled()) {
      // Leave this wrapped in a conditional to prevent unnecessarily calling Arrays.toString(...)
      LOGGER.debug("call#{}: Apply rule [{}] to {}",
          ruleCall.id, ruleCall.getRule(), Arrays.toString(ruleCall.rels));
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
    if (before && LOGGER.isDebugEnabled()) {
      LOGGER.debug("call#{}: Rule {} arguments {} produced {}",
          ruleCall.id, ruleCall.getRule(), Arrays.toString(ruleCall.rels), newRel);
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
   * Takes care of tracing and listener notification when a rel is discarded
   *
   * @param rel discarded rel
   */
  protected void notifyDiscard(
      RelNode rel) {
    if (listener != null) {
      RelOptListener.RelDiscardedEvent event =
          new RelOptListener.RelDiscardedEvent(
              this,
              rel);
      listener.relDiscarded(event);
    }
  }

  public RelOptListener getListener() {
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

  /** Computes the key for relational expression digest cache. */
  protected static Pair<String, List<RelDataType>> key(RelNode rel) {
    return key(rel.getDigest(), rel.getRowType());
  }

  /** Computes the key for relational expression digest cache. */
  protected static Pair<String, List<RelDataType>> key(String digest, RelDataType relType) {
    final List<RelDataType> v = relType.isStruct()
        ? Pair.right(relType.getFieldList())
        : Collections.singletonList(relType);
    return Pair.of(digest, v);
  }

  /** Listener for counting the attempts of each rule. Only enabled under DEBUG level.*/
  private class RuleAttemptsListener implements RelOptListener {
    private long beforeTimestamp;
    private Map<String, Pair<Long, Long>> ruleAttempts;

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
        if (ruleAttempts.containsKey(rule)) {
          Pair<Long, Long> p = ruleAttempts.get(rule);
          ruleAttempts.put(rule, Pair.of(p.left + 1, p.right + elapsed));
        } else {
          ruleAttempts.put(rule, Pair.of(1L,  elapsed));
        }
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
      Collections.sort(list,
          (left, right) -> {
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
      for (Map.Entry<String, Pair<Long, Long>> entry : list) {
        sb.append(
            String.format(Locale.ROOT, "%-60s%20s%20s%n",
                entry.getKey(),
                usFormat.format(entry.getValue().left),
                usFormat.format(entry.getValue().right)));
      }
      return sb.toString();
    }
  }
}
