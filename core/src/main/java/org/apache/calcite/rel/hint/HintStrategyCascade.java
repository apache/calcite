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
 * Strategy to decide if a hint should apply to a relational expression
 * by using a series of {@link HintStrategy} rules in a given order.
 * If a rule fails, returns false early, else return true.
 */
public class HintStrategyCascade implements HintStrategy {
  //~ Instance fields --------------------------------------------------------

  private ImmutableList<HintStrategy> strategies;

  /**
   * Creates a HintStrategyCascade from an array of hint strategies.
   *
   * <p>Make this constructor package-protected intentionally.
   * Use {@link HintStrategies#cascade}.</p>
   */
  HintStrategyCascade(HintStrategy... strategies) {
    assert strategies != null;
    assert strategies.length > 1;
    for (HintStrategy strategy : strategies) {
      assert strategy != null;
    }
    this.strategies = ImmutableList.copyOf(strategies);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public boolean supportsRel(RelHint hint, RelNode rel) {
    for (HintStrategy strategy : strategies) {
      if (!strategy.supportsRel(hint, rel)) {
        return false;
      }
    }
    return true;
  }
}

// End HintStrategyCascade.java
