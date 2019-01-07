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

import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Util;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A match of a rule to a particular set of target relational expressions,
 * frozen in time.
 */
class VolcanoRuleMatch extends VolcanoRuleCall {
  //~ Instance fields --------------------------------------------------------

  private final RelSet targetSet;
  private RelSubset targetSubset;
  private double cachedImportance = Double.NaN;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a <code>VolcanoRuleMatch</code>.
   *
   * @param operand0 Primary operand
   * @param rels     List of targets; copied by the constructor, so the client
   *                 can modify it later
   * @param nodeInputs Map from relational expressions to their inputs
   */
  VolcanoRuleMatch(VolcanoPlanner volcanoPlanner, RelOptRuleOperand operand0,
      RelNode[] rels, Map<RelNode, List<RelNode>> nodeInputs) {
    super(volcanoPlanner, operand0, rels.clone(), nodeInputs);
    assert allNotNull(rels, Litmus.THROW);

    // Try to deduce which subset the result will belong to. Assume --
    // for now -- that the set is the same as the root relexp.
    targetSet = volcanoPlanner.getSet(rels[0]);
    assert targetSet != null : rels[0].toString() + " isn't in a set";
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Clears the cached importance value of this rule match. The importance
   * will be re-calculated next time {@link #getImportance()} is called.
   */
  void clearCachedImportance() {
    cachedImportance = Double.NaN;
  }

  /**
   * Returns the importance of this rule.
   *
   * <p>Calls {@link #computeImportance()} the first time, thereafter uses a
   * cached value until {@link #clearCachedImportance()} is called.
   *
   * @return importance of this rule; a value between 0 and 1
   */
  double getImportance() {
    if (Double.isNaN(cachedImportance)) {
      cachedImportance = computeImportance();
    }

    return cachedImportance;
  }

  /**
   * Computes the importance of this rule match.
   *
   * @return importance of this rule match
   */
  double computeImportance() {
    assert rels[0] != null;
    RelSubset subset = volcanoPlanner.getSubset(rels[0]);
    double importance = 0;
    if (subset != null) {
      importance = volcanoPlanner.ruleQueue.getImportance(subset);
    }
    final RelSubset targetSubset = guessSubset();
    if ((targetSubset != null) && (targetSubset != subset)) {
      // If this rule will generate a member of an equivalence class
      // which is more important, use that importance.
      final double targetImportance =
          volcanoPlanner.ruleQueue.getImportance(targetSubset);
      if (targetImportance > importance) {
        importance = targetImportance;

        // If the equivalence class is cheaper than the target, bump up
        // the importance of the rule. A converter is an easy way to
        // make the plan cheaper, so we'd hate to miss this opportunity.
        //
        // REVIEW: jhyde, 2007/12/21: This rule seems to make sense, but
        // is disabled until it has been proven.
        //
        // CHECKSTYLE: IGNORE 3
        if ((subset != null)
            && subset.bestCost.isLt(targetSubset.bestCost)
            && false) {
          importance *=
              targetSubset.bestCost.divideBy(subset.bestCost);
          importance = Math.min(importance, 0.99);
        }
      }
    }

    return importance;
  }

  @Override public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    VolcanoRuleMatch that = (VolcanoRuleMatch) o;
    if (rels.length != that.rels.length) {
      return false;
    }
    for (int i = 0; i < rels.length; i++) {
      if (rels[i].getId() != that.rels[i].getId()) {
        return false;
      }
    }

    return Objects.equals(getRule().toString(), that.getRule().toString());
  }

  @Override public int hashCode() {
    int result = getRule().toString().hashCode();
    for (int i = 0; i < rels.length; i++) {
      result = result * 31 + Integer.hashCode(rels[i].getId());
    }
    return result;
  }

  public String toString() {
    return Util.toString(
        Arrays.asList(rels),
        "rule [" + getRule() + "] rels [",
        ", ",
        "]");
  }

  /**
   * Returns a guess as to which subset (that is equivalence class of
   * relational expressions combined with a set of physical traits) the result
   * of this rule will belong to.
   *
   * @return expected subset, or null if we cannot guess
   */
  private RelSubset guessSubset() {
    if (targetSubset != null) {
      return targetSubset;
    }
    final RelTrait targetTrait = getRule().getOutTrait();
    if ((targetSet != null) && (targetTrait != null)) {
      final RelTraitSet targetTraitSet =
          rels[0].getTraitSet().replace(targetTrait);

      // Find the subset in the target set which matches the expected
      // set of traits. It may not exist yet.
      targetSubset = targetSet.getSubset(targetTraitSet);
      return targetSubset;
    }

    // The target subset doesn't exist yet.
    return null;
  }

  /** Returns whether all elements of a given array are not-null;
   * fails if any are null. */
  private static <E> boolean allNotNull(E[] es, Litmus litmus) {
    for (E e : es) {
      if (e == null) {
        return litmus.fail("was null", (Object) es);
      }
    }
    return litmus.succeed();
  }

}

// End VolcanoRuleMatch.java
