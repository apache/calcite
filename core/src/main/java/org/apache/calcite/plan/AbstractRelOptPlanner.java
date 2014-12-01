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
import org.apache.calcite.util.CancelFlag;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.regex.Pattern;

import static org.apache.calcite.util.Static.RESOURCE;


/**
 * Abstract base for implementations of the {@link RelOptPlanner} interface.
 */
public abstract class AbstractRelOptPlanner implements RelOptPlanner {
  //~ Static fields/initializers ---------------------------------------------

  /** Regular expression for integer. */
  private static final Pattern INTEGER_PATTERN = Pattern.compile("[0-9]+");

  //~ Instance fields --------------------------------------------------------

  /**
   * Maps rule description to rule, just to ensure that rules' descriptions
   * are unique.
   */
  private final Map<String, RelOptRule> mapDescToRule =
      new HashMap<String, RelOptRule>();

  protected final RelOptCostFactory costFactory;

  private MulticastRelOptListener listener;

  private Pattern ruleDescExclusionFilter;

  private CancelFlag cancelFlag;

  @SuppressWarnings("unchecked")
  private final Set<Class<? extends RelNode>> classes =
      new HashSet<Class<? extends RelNode>>();

  private final Set<RelTrait> traits = new HashSet<RelTrait>();

  /** External context. Never null. */
  protected final Context context;

  private Executor executor;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates an AbstractRelOptPlanner.
   */
  protected AbstractRelOptPlanner(RelOptCostFactory costFactory, //
      Context context) {
    assert costFactory != null;
    this.costFactory = costFactory;
    if (context == null) {
      context = Contexts.empty();
    }
    this.context = context;

    // In case no one calls setCancelFlag, set up a
    // dummy here.
    cancelFlag = new CancelFlag();

    // Add abstract RelNode classes. No RelNodes will ever be registered with
    // these types, but some operands may use them.
    classes.add(RelNode.class);
    classes.add(RelSubset.class);
  }

  //~ Methods ----------------------------------------------------------------

  public void clear() {}

  public Context getContext() {
    return context;
  }

  public RelOptCostFactory getCostFactory() {
    return costFactory;
  }

  // implement RelOptPlanner
  public void setCancelFlag(CancelFlag cancelFlag) {
    this.cancelFlag = cancelFlag;
  }

  /**
   * Checks to see whether cancellation has been requested, and if so, throws
   * an exception.
   */
  public void checkCancel() {
    if (cancelFlag.isCancelRequested()) {
      throw RESOURCE.preparationAborted().ex();
    }
  }

  /**
   * Registers a rule's description.
   *
   * @param rule Rule
   */
  protected void mapRuleDescription(RelOptRule rule) {
    // Check that there isn't a rule with the same description,
    // also validating description string.

    final String description = rule.toString();
    assert description != null;
    assert !description.contains("$")
        : "Rule's description should not contain '$': "
        + description;
    assert !INTEGER_PATTERN.matcher(description).matches()
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
        throw new AssertionError("Rule's description should be unique; "
            + "existing rule=" + existingRule + "; new rule=" + rule);
      }
    }
  }

  /**
   * Removes the mapping between a rule and its description.
   *
   * @param rule Rule
   */
  protected void unmapRuleDescription(RelOptRule rule) {
    String description = rule.toString();
    mapDescToRule.remove(description);
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

  // implement RelOptPlanner
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

  // implement RelOptPlanner
  public RelOptPlanner chooseDelegate() {
    return this;
  }

  public void addMaterialization(RelOptMaterialization materialization) {
    // ignore - this planner does not support materializations
  }

  public void addLattice(RelOptLattice lattice) {
    // ignore - this planner does not support lattices
  }

  public RelOptLattice getLattice(RelOptTable table) {
    // this planner does not support lattices
    return null;
  }

  // implement RelOptPlanner
  public void registerSchema(RelOptSchema schema) {
  }

  // implement RelOptPlanner
  public long getRelMetadataTimestamp(RelNode rel) {
    return 0;
  }

  public void setImportance(RelNode rel, double importance) {
  }

  public void registerClass(RelNode node) {
    final Class<? extends RelNode> clazz = node.getClass();
    if (classes.add(clazz)) {
      onNewClass(node);
    }
    for (RelTrait trait : node.getTraitSet()) {
      if (traits.add(trait)) {
        trait.register(this);
      }
    }
  }

  /** Called when a new class of {@link RelNode} is seen. */
  protected void onNewClass(RelNode node) {
    node.register(this);
  }

  public RelTraitSet emptyTraitSet() {
    return RelTraitSet.createEmpty();
  }

  // implement RelOptPlanner
  public RelOptCost getCost(RelNode rel) {
    return RelMetadataQuery.getCumulativeCost(rel);
  }

  // implement RelOptPlanner
  public void addListener(RelOptListener newListener) {
    if (listener == null) {
      listener = new MulticastRelOptListener();
    }
    listener.addListener(newListener);
  }

  // implement RelOptPlanner
  public void registerMetadataProviders(List<RelMetadataProvider> list) {
  }

  // implement RelOptPlanner
  public boolean addRelTraitDef(RelTraitDef relTraitDef) {
    return false;
  }

  // implement RelOptPlanner
  public void clearRelTraitDefs() {}

  public List<RelTraitDef> getRelTraitDefs() {
    return Collections.emptyList();
  }

  public void setExecutor(Executor executor) {
    this.executor = executor;
  }

  public Executor getExecutor() {
    return executor;
  }

  public void onCopy(RelNode rel, RelNode newRel) {
    // do nothing
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
      if (LOGGER.isLoggable(Level.FINE)) {
        LOGGER.fine("call#" + ruleCall.id
            + ": Rule [" + ruleCall.getRule() + "] not fired"
            + " due to exclusion filter");
      }
      return;
    }

    if (LOGGER.isLoggable(Level.FINE)) {
      LOGGER.fine(
          "call#" + ruleCall.id
          + ": Apply rule [" + ruleCall.getRule() + "] to "
          + Arrays.toString(ruleCall.rels));
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
    if (before && LOGGER.isLoggable(Level.FINE)) {
      LOGGER.fine("call#" + ruleCall.id
          + ": Rule " + ruleCall.getRule() + " arguments "
          + Arrays.toString(ruleCall.rels) + " produced "
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
  protected void notifyChosen(RelNode rel) {
    if (LOGGER.isLoggable(Level.FINE)) {
      LOGGER.fine("For final plan, using " + rel);
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

  protected MulticastRelOptListener getListener() {
    return listener;
  }

  /** Returns sub-classes of relational expression. */
  public Iterable<Class<? extends RelNode>> subClasses(
      final Class<? extends RelNode> clazz) {
    return Iterables.filter(classes,
        new Predicate<Class<? extends RelNode>>() {
          public boolean apply(Class<? extends RelNode> input) {
            return clazz.isAssignableFrom(input);
          }
        });
  }
}

// End AbstractRelOptPlanner.java
