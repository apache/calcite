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

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.trace.CalciteTrace;

import com.google.common.collect.ImmutableMap;

import org.slf4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * {@code HintStrategy} collection indicates which kind of
 * {@link org.apache.calcite.rel.RelNode} a hint can apply to.
 *
 * <p>Typically, every supported hint should register a {@code HintStrategy}
 * into this collection. For example, {@link HintStrategies#JOIN} implies that this hint
 * would be propagated and attached to the {@link org.apache.calcite.rel.core.Join}
 * relational expressions.
 *
 * <p>A {@code HintStrategy} can be used independently or cascaded with other strategies
 * with method {@link HintStrategies#and}.
 *
 * <p>The matching for hint name is case in-sensitive.
 *
 * @see HintStrategy
 */
public class HintStrategyTable {
  //~ Static fields/initializers ---------------------------------------------

  /** Empty strategies. */
  // Need to replace the EMPTY with DEFAULT if we have any hint implementations.
  public static final HintStrategyTable EMPTY = new HintStrategyTable(
      Collections.emptyMap(), Collections.emptyMap(), HintErrorLogger.INSTANCE);

  //~ Instance fields --------------------------------------------------------

  /** Mapping from hint name to strategy. */
  private final Map<Key, HintStrategy> hintStrategyMap;

  /** Mapping from hint name to option checker. */
  private final Map<Key, HintOptionChecker> hintOptionCheckerMap;

  /** Handler for the hint error. */
  private final Litmus errorHandler;

  private HintStrategyTable(Map<Key, HintStrategy> strategies,
      Map<Key, HintOptionChecker> optionCheckers,
      Litmus litmus) {
    this.hintStrategyMap = ImmutableMap.copyOf(strategies);
    this.hintOptionCheckerMap = ImmutableMap.copyOf(optionCheckers);
    this.errorHandler = litmus;
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Apply this {@link HintStrategyTable} to the given relational
   * expression for the {@code hints}.
   *
   * @param hints Hints that may attach to the {@code rel}
   * @param rel   Relational expression
   * @return A hints list that can be attached to the {@code rel}
   */
  public List<RelHint> apply(List<RelHint> hints, RelNode rel) {
    return hints.stream()
        .filter(relHint -> supportsRel(relHint, rel))
        .collect(Collectors.toList());
  }

  /**
   * Checks if the given hint is valid.
   *
   * @param hint The hint
   */
  public boolean validateHint(RelHint hint) {
    final Key key = Key.of(hint.hintName);
    boolean hintExists = this.errorHandler.check(
        this.hintStrategyMap.containsKey(key),
        "Hint: {} should be registered in the {}",
        hint.hintName,
        this.getClass().getSimpleName());
    if (!hintExists) {
      return false;
    }
    if (this.hintOptionCheckerMap.containsKey(key)) {
      return this.hintOptionCheckerMap.get(key).checkOptions(hint, this.errorHandler);
    }
    return true;
  }

  private boolean supportsRel(RelHint hint, RelNode rel) {
    final Key key = Key.of(hint.hintName);
    assert this.hintStrategyMap.containsKey(key);
    return this.hintStrategyMap.get(key).supportsRel(hint, rel);
  }

  /**
   * @return A strategies builder
   */
  public static Builder builder() {
    return new Builder();
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
    private Map<Key, HintStrategy> hintStrategyMap;
    private Map<Key, HintOptionChecker> hintOptionCheckerMap;
    private Litmus errorHandler;

    private Builder() {
      this.hintStrategyMap = new HashMap<>();
      this.hintOptionCheckerMap = new HashMap<>();
      this.errorHandler = HintErrorLogger.INSTANCE;
    }

    public Builder addHintStrategy(String hintName, HintStrategy strategy) {
      this.hintStrategyMap.put(Key.of(hintName), Objects.requireNonNull(strategy));
      return this;
    }

    public Builder addHintStrategy(
        String hintName,
        HintStrategy strategy,
        HintOptionChecker optionChecker) {
      this.hintStrategyMap.put(Key.of(hintName), Objects.requireNonNull(strategy));
      this.hintOptionCheckerMap.put(Key.of(hintName), Objects.requireNonNull(optionChecker));
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
          this.hintStrategyMap,
          this.hintOptionCheckerMap,
          this.errorHandler);
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
