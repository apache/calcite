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

import com.google.common.collect.ImmutableList;

/**
 * A {@link HintStrategy} to combine multiple hint strategies into one.
 *
 * <p>The composition can be {@code AND} or {@code OR}.
 */
public class CompositeHintStrategy implements HintStrategy {
  //~ Enums ------------------------------------------------------------------

  /** How hint strategies are composed. */
  public enum Composition {
    AND, OR
  }

  //~ Instance fields --------------------------------------------------------

  private ImmutableList<HintStrategy> strategies;
  private Composition composition;

  /**
   * Creates a {@link CompositeHintStrategy} with a {@link Composition}
   * and an array of hint strategies.
   *
   * <p>Make this constructor package-protected intentionally.
   * Use utility methods in {@link HintStrategies}
   * to create a {@link CompositeHintStrategy}.</p>
   */
  CompositeHintStrategy(Composition composition, HintStrategy... strategies) {
    assert strategies != null;
    assert strategies.length > 1;
    for (HintStrategy strategy : strategies) {
      assert strategy != null;
    }
    this.strategies = ImmutableList.copyOf(strategies);
    this.composition = composition;
  }

  //~ Methods ----------------------------------------------------------------

  @Override public boolean canApply(RelHint hint, RelNode rel) {
    return canApply(composition, hint, rel);
  }

  private boolean canApply(Composition composition, RelHint hint, RelNode rel) {
    switch (composition) {
    case AND:
      for (HintStrategy hintStrategy: strategies) {
        if (!hintStrategy.canApply(hint, rel)) {
          return false;
        }
      }
      return true;
    case OR:
    default:
      for (HintStrategy hintStrategy: strategies) {
        if (hintStrategy.canApply(hint, rel)) {
          return true;
        }
      }
      return false;
    }
  }
}
