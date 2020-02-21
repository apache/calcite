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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.trace.CalciteTrace;

import com.google.common.collect.ImmutableSet;

import org.slf4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * A collections of {@link HintStrategy}s.
 *
 * <p>Every supported hint should register a {@link HintPredicate}
 * into this collection. For example, {@link HintPredicates#JOIN} implies that this hint
 * would be propagated and attached to the {@link org.apache.calcite.rel.core.Join}
 * relational expressions.
 *
 * <p>The matching for hint name is case in-sensitive.
 *
 * @see HintPredicate
 */
public class HintStrategyTable {
  //~ Static fields/initializers ---------------------------------------------

  /** Empty strategies. */
  // Need to replace the EMPTY with DEFAULT if we have any hint implementations.
  public static final HintStrategyTable EMPTY = new HintStrategyTable(
      Collections.emptyMap(), HintErrorLogger.INSTANCE);

  //~ Instance fields --------------------------------------------------------

  /** Mapping from hint name to {@link HintStrategy}. */
  private final Map<Key, HintStrategy> strategies;

  /** Handler for the hint error. */
  private final Litmus errorHandler;

  private HintStrategyTable(Map<Key, HintStrategy> strategies, Litmus litmus) {
    this.strategies = strategies;
    this.errorHandler = litmus;
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Applies this {@link HintStrategyTable} hint strategies to the given relational
   * expression and the {@code hints}.
   *
   * @param hints Hints that may attach to the {@code rel}
   * @param rel   Relational expression
   * @return A hint list that can be attached to the {@code rel}
   */
  public List<RelHint> apply(List<RelHint> hints, RelNode rel) {
    return hints.stream()
        .filter(relHint -> canApply(relHint, rel))
        .collect(Collectors.toList());
  }

  private boolean canApply(RelHint hint, RelNode rel) {
    final Key key = Key.of(hint.hintName);
    assert this.strategies.containsKey(key);
    return this.strategies.get(key).predicate.apply(hint, rel);
  }

  /**
   * Checks if the given hint is valid.
   *
   * @param hint The hint
   */
  public boolean validateHint(RelHint hint) {
    final Key key = Key.of(hint.hintName);
    boolean hintExists = this.errorHandler.check(
        this.strategies.containsKey(key),
        "Hint: {} should be registered in the {}",
        hint.hintName,
        this.getClass().getSimpleName());
    if (!hintExists) {
      return false;
    }
    final HintStrategy strategy = strategies.get(key);
    if (strategy.hintOptionChecker != null) {
      return strategy.hintOptionChecker.checkOptions(hint, this.errorHandler);
    }
    return true;
  }

  /** Returns whether the {@code hintable} has hints that imply
   * the given {@code rule} should be excluded. */
  public boolean isRuleExcluded(Hintable hintable, RelOptRule rule) {
    final List<RelHint> hints = hintable.getHints();
    if (hints.size() == 0) {
      return false;
    }

    for (RelHint hint : hints) {
      final Key key = Key.of(hint.hintName);
      assert this.strategies.containsKey(key);
      final HintStrategy strategy = strategies.get(key);
      if (strategy.excludedRules.contains(rule)) {
        return isDesiredConversionPossible(strategy.converterRules, hintable);
      }
    }

    return false;
  }

  /** Returns whether the {@code hintable} has hints that imply
   * the given {@code hintable} can make conversion successfully. */
  private static boolean isDesiredConversionPossible(
      Set<ConverterRule> converterRules,
      Hintable hintable) {
    // If no converter rules are specified, we assume the conversion is possible.
    return converterRules.size() == 0
        || converterRules.stream()
            .anyMatch(converterRule -> converterRule.convert((RelNode) hintable) != null);
  }

  /**
   * Returns a {@code HintStrategyTable} builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Returns a {@link HintStrategy} builder with given hint predicate.
   *
   * @param hintPredicate hint predicate
   * @return {@link StrategyBuilder} instance
   */
  public static StrategyBuilder strategyBuilder(HintPredicate hintPredicate) {
    return new StrategyBuilder(hintPredicate);
  }

  //~ Inner Class ------------------------------------------------------------

  /**
   * Key used to keep the strategies which ignores the case sensitivity.
   */
  private static class Key {
    private String name;
    private Key(String name) {
      this.name = name;
    }

    static Key of(String name) {
      return new Key(name.toLowerCase(Locale.ROOT));
    }

    @Override public boolean equals(Object obj) {
      if (!(obj instanceof Key)) {
        return false;
      }
      return Objects.equals(this.name, ((Key) obj).name);
    }

    @Override public int hashCode() {
      return this.name.hashCode();
    }
  }

  /**
   * Builder for {@code HintStrategyTable}.
   */
  public static class Builder {
    private Map<Key, HintStrategy> strategies;
    private Litmus errorHandler;

    private Builder() {
      this.strategies = new HashMap<>();
      this.errorHandler = HintErrorLogger.INSTANCE;
    }

    public Builder hintStrategy(String hintName, HintPredicate strategy) {
      this.strategies.put(Key.of(hintName),
          strategyBuilder(Objects.requireNonNull(strategy)).build());
      return this;
    }

    public Builder hintStrategy(String hintName, HintStrategy entry) {
      this.strategies.put(Key.of(hintName), Objects.requireNonNull(entry));
      return this;
    }

    /**
     * Sets an error handler to customize the hints error handling.
     *
     * <p>The default behavior is to log warnings.
     *
     * @param errorHandler The handler
     */
    public Builder errorHandler(Litmus errorHandler) {
      this.errorHandler = errorHandler;
      return this;
    }

    public HintStrategyTable build() {
      return new HintStrategyTable(
          this.strategies,
          this.errorHandler);
    }
  }

  /**
   * Represents a hint strategy entry of this {@link HintStrategyTable}.
   *
   * <p>A {@code HintStrategy} includes:
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
   */
  public static class HintStrategy {
    public final HintPredicate predicate;
    public final HintOptionChecker hintOptionChecker;
    public final ImmutableSet<RelOptRule> excludedRules;
    public final ImmutableSet<ConverterRule> converterRules;

    public HintStrategy(
        HintPredicate predicate,
        HintOptionChecker hintOptionChecker,
        ImmutableSet<RelOptRule> excludedRules,
        ImmutableSet<ConverterRule> converterRules) {
      this.predicate = predicate;
      this.hintOptionChecker = hintOptionChecker;
      this.excludedRules = excludedRules;
      this.converterRules = converterRules;
    }
  }

  /** Builder for {@link HintStrategy}. */
  public static class StrategyBuilder {
    private final HintPredicate predicate;
    @Nullable
    private HintOptionChecker optionChecker;
    private ImmutableSet<RelOptRule> excludedRules;
    private ImmutableSet<ConverterRule> converterRules;

    private StrategyBuilder(HintPredicate predicate) {
      this.predicate = Objects.requireNonNull(predicate);
      this.excludedRules = ImmutableSet.of();
      this.converterRules = ImmutableSet.of();
    }

    /** Registers a hint option checker to validate the hint options. */
    public StrategyBuilder optionChecker(HintOptionChecker optionChecker) {
      this.optionChecker = Objects.requireNonNull(optionChecker);
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
     *   <li>The desired converter rules conversion is not possible for the rule
     *   matched root node</li>
     * </ol>
     *
     * @param rules excluded rules
     */
    public StrategyBuilder excludedRules(RelOptRule... rules) {
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
     *   <li>The desired converter rules conversion is not possible for the rule
     *   matched root node</li>
     * </ol>
     *
     * <p>If no converter rules are specified, we assume the conversion is possible.
     *
     * @param rules desired converter rules
     */
    public StrategyBuilder converterRules(ConverterRule... rules) {
      this.converterRules = ImmutableSet.copyOf(rules);
      return this;
    }

    public HintStrategy build() {
      return new HintStrategy(predicate, optionChecker, excludedRules, converterRules);
    }
  }

  /** Implementation of {@link org.apache.calcite.util.Litmus} that returns
   * a status code, it logs warnings for fail check and does not throw. */
  public static class HintErrorLogger implements Litmus {
    private static final Logger LOGGER = CalciteTrace.PARSER_LOGGER;

    public static final HintErrorLogger INSTANCE = new HintErrorLogger();

    public boolean fail(String message, Object... args) {
      LOGGER.warn(message, args);
      return false;
    }

    public boolean succeed() {
      return true;
    }

    public boolean check(boolean condition, String message, Object... args) {
      if (condition) {
        return succeed();
      } else {
        return fail(message, args);
      }
    }
  }
}
