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

import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * {@code HintStrategy} collection indicating which kind of
 * {@link org.apache.calcite.rel.RelNode} a hint can apply to.
 *
 * <p>Typically, every supported hints should register a {@code HintStrategy}
 * in this collection. For example, {@link HintStrategies#JOIN} implies that this hint
 * would be propagated and applied to the {@link org.apache.calcite.rel.core.Join}
 * relational expressions.
 *
 * <p>A {@code HintStrategy} can be used independently or cascaded with other strategies
 * with method {@link HintStrategies#cascade}.
 *
 * <p>The matching for hint name is case in-sensitive.
 *
 * @see HintStrategy
 */
public class HintStrategyTable {
  //~ Static fields/initializers ---------------------------------------------

  /** Empty strategies. */
  // Need to replace the EMPTY with DEFAULT if we have any hint implementations.
  public static final HintStrategyTable EMPTY = new HintStrategyTable(new HashMap<>());

  //~ Instance fields --------------------------------------------------------

  /** Mapping from hint name to strategy. */
  private final Map<Key, HintStrategy> hintStrategyMap;

  private HintStrategyTable(Map<Key, HintStrategy> strategies) {
    this.hintStrategyMap = ImmutableMap.copyOf(strategies);
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
   * Check if the give hint name is valid.
   *
   * @param name The hint name
   */
  public void validateHint(String name) {
    if (!this.hintStrategyMap.containsKey(Key.of(name))) {
      throw new RuntimeException("Hint: " + name + " should be registered in the HintStrategies.");
    }
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

  //~ Inner Class ------------------------------------------------------------

  /**
   * Builder for {@code HintStrategyTable}.
   */
  public static class Builder {
    private Map<Key, HintStrategy> hintStrategyMap;

    public Builder() {
      this.hintStrategyMap = new HashMap<>();
    }

    public Builder addHintStrategy(String hintName, HintStrategy strategy) {
      this.hintStrategyMap.put(Key.of(hintName), strategy);
      return this;
    }

    public HintStrategyTable build() {
      return new HintStrategyTable(this.hintStrategyMap);
    }
  }
}
