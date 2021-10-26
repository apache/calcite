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

import com.google.common.collect.ImmutableMap;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * A collection of {@link HintStrategy}s.
 *
 * <p>Every hint must register a {@link HintStrategy} into the collection.
 * With a hint strategies mapping, the hint strategy table is used as a tool
 * to decide i) if the given hint was registered; ii) which hints are suitable for the rel with
 * a given hints collection; iii) if the hint options are valid.
 *
 * <p>The hint strategy table is immutable. To create one, use
 * {@link #builder()}.
 *
 * <p>Match of hint name is case insensitive.
 *
 * @see HintPredicate
 */
public class HintStrategyTable {
  //~ Static fields/initializers ---------------------------------------------

  /** Empty strategies. */
  public static final HintStrategyTable EMPTY =
      new HintStrategyTable(ImmutableMap.of(), HintErrorLogger.INSTANCE);

  //~ Instance fields --------------------------------------------------------

  /** Mapping from hint name to {@link HintStrategy}. */
  private final Map<Key, HintStrategy> strategies;

  /** Handler for the hint error. */
  private final Litmus errorHandler;

  private HintStrategyTable(Map<Key, HintStrategy> strategies, Litmus litmus) {
    this.strategies = ImmutableMap.copyOf(strategies);
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
    assert this.strategies.containsKey(key) : "hint " + hint.hintName + " must be present";
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
    if (strategy != null && strategy.hintOptionChecker != null) {
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
      assert this.strategies.containsKey(key) : "hint " + hint.hintName + " must be present";
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

  //~ Inner Class ------------------------------------------------------------

  /**
   * Key used to keep the strategies which ignores the case sensitivity.
   */
  private static class Key {
    private final String name;

    private Key(String name) {
      this.name = name;
    }

    static Key of(String name) {
      return new Key(name.toLowerCase(Locale.ROOT));
    }

    @Override public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Key key = (Key) o;
      return name.equals(key.name);
    }

    @Override public int hashCode() {
      return this.name.hashCode();
    }
  }

  /**
   * Builder for {@code HintStrategyTable}.
   */
  public static class Builder {
    private final Map<Key, HintStrategy> strategies = new HashMap<>();
    private Litmus errorHandler = HintErrorLogger.INSTANCE;

    public Builder hintStrategy(String hintName, HintPredicate hintPredicate) {
      this.strategies.put(Key.of(hintName),
          HintStrategy.builder(requireNonNull(hintPredicate, "hintPredicate")).build());
      return this;
    }

    public Builder hintStrategy(String hintName, HintStrategy hintStrategy) {
      this.strategies.put(Key.of(hintName), requireNonNull(hintStrategy, "hintStrategy"));
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

  /** Implementation of {@link org.apache.calcite.util.Litmus} that returns
   * a status code, it logs warnings for fail check and does not throw. */
  public static class HintErrorLogger implements Litmus {
    private static final Logger LOGGER = CalciteTrace.PARSER_LOGGER;

    public static final HintErrorLogger INSTANCE = new HintErrorLogger();

    @Override public boolean fail(@Nullable String message, @Nullable Object... args) {
      LOGGER.warn(requireNonNull(message, "message"), args);
      return false;
    }
  }
}
