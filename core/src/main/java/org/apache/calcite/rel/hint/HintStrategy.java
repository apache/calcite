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
package org.apache.calcite.rel.hint;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.convert.ConverterRule;

import com.google.common.collect.ImmutableSet;

import org.checkerframework.checker.nullness.qual.Nullable;

import static java.util.Objects.requireNonNull;

/**
 * Represents a hint strategy entry of {@link HintStrategyTable}.
 *
 * <p>A {@code HintStrategy} defines:
 *
 * <ul>
 *   <li>{@link HintPredicate}: tests whether a hint should apply to
 *   a relational expression;</li>
 *   <li>{@link HintOptionChecker}: validates the hint options;</li>
 *   <li>{@code excludedRules}: rules to exclude when a relational expression
 *   is going to apply a planner rule;</li>
 *   <li>{@code converterRules}: fallback rules to apply when there are
 *   no proper implementations after excluding the {@code excludedRules}.</li>
 * </ul>
 *
 * <p>The {@link HintPredicate} is required, all the other items are optional.
 *
 * <p>{@link HintStrategy} is immutable.
 */
public class HintStrategy {
  //~ Instance fields --------------------------------------------------------

  public final HintPredicate predicate;
  public final @Nullable HintOptionChecker hintOptionChecker;
  public final ImmutableSet<RelOptRule> excludedRules;
  public final ImmutableSet<ConverterRule> converterRules;

  //~ Constructors -----------------------------------------------------------

  private HintStrategy(
      HintPredicate predicate,
      @Nullable HintOptionChecker hintOptionChecker,
      ImmutableSet<RelOptRule> excludedRules,
      ImmutableSet<ConverterRule> converterRules) {
    this.predicate = predicate;
    this.hintOptionChecker = hintOptionChecker;
    this.excludedRules = excludedRules;
    this.converterRules = converterRules;
  }

  /**
   * Returns a {@link HintStrategy} builder with given hint predicate.
   *
   * @param hintPredicate hint predicate
   * @return {@link Builder} instance
   */
  public static Builder builder(HintPredicate hintPredicate) {
    return new Builder(hintPredicate);
  }

  //~ Inner Class ------------------------------------------------------------

  /** Builder for {@link HintStrategy}. */
  public static class Builder {
    private final HintPredicate predicate;
    private @Nullable HintOptionChecker optionChecker;
    private ImmutableSet<RelOptRule> excludedRules;
    private ImmutableSet<ConverterRule> converterRules;

    private Builder(HintPredicate predicate) {
      this.predicate = requireNonNull(predicate, "predicate");
      this.excludedRules = ImmutableSet.of();
      this.converterRules = ImmutableSet.of();
    }

    /** Registers a hint option checker to validate the hint options. */
    public Builder optionChecker(HintOptionChecker optionChecker) {
      this.optionChecker = requireNonNull(optionChecker, "optionChecker");
      return this;
    }

    /**
     * Registers an array of rules to exclude during the
     * {@link org.apache.calcite.plan.RelOptPlanner} planning.
     *
     * <p>The desired converter rules work together with the excluded rules.
     * We have no validation here but they expect to have the same
     * function(semantic equivalent).
     *
     * <p>A rule fire cancels if:
     *
     * <ol>
     *   <li>The registered {@link #excludedRules} contains the rule</li>
     *   <li>And the desired converter rules conversion is not possible
     *   for the rule matched root node</li>
     * </ol>
     *
     * @param rules excluded rules
     */
    public Builder excludedRules(RelOptRule... rules) {
      this.excludedRules = ImmutableSet.copyOf(rules);
      return this;
    }

    /**
     * Registers an array of desired converter rules during the
     * {@link org.apache.calcite.plan.RelOptPlanner} planning.
     *
     * <p>The desired converter rules work together with the excluded rules.
     * We have no validation here but they expect to have the same
     * function(semantic equivalent).
     *
     * <p>A rule fire cancels if:
     *
     * <ol>
     *   <li>The registered {@link #excludedRules} contains the rule</li>
     *   <li>And the desired converter rules conversion is not possible
     *   for the rule matched root node</li>
     * </ol>
     *
     * <p>If no converter rules are specified, we assume the conversion is possible.
     *
     * @param rules desired converter rules
     */
    public Builder converterRules(ConverterRule... rules) {
      this.converterRules = ImmutableSet.copyOf(rules);
      return this;
    }

    public HintStrategy build() {
      return new HintStrategy(predicate, optionChecker, excludedRules, converterRules);
    }
  }
}
