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

import static com.google.common.base.Preconditions.checkArgument;

/**
 * A {@link HintPredicate} to combine multiple hint predicates into one.
 *
 * <p>The composition can be {@code AND} or {@code OR}.
 */
public class CompositeHintPredicate implements HintPredicate {
  //~ Enums ------------------------------------------------------------------

  /** How hint predicates are composed. */
  public enum Composition {
    AND, OR
  }

  //~ Instance fields --------------------------------------------------------

  private final ImmutableList<HintPredicate> predicates;
  private final Composition composition;

  /**
   * Creates a {@link CompositeHintPredicate} with a {@link Composition}
   * and an array of hint predicates.
   *
   * <p>Make this constructor package-protected intentionally.
   * Use utility methods in {@link HintPredicates}
   * to create a {@link CompositeHintPredicate}.
   */
  CompositeHintPredicate(Composition composition, HintPredicate... predicates) {
    this.predicates = ImmutableList.copyOf(predicates);
    checkArgument(this.predicates.size() > 1);
    this.composition = composition;
  }

  //~ Methods ----------------------------------------------------------------

  @Override public boolean apply(RelHint hint, RelNode rel) {
    return apply(composition, hint, rel);
  }

  private boolean apply(Composition composition, RelHint hint, RelNode rel) {
    switch (composition) {
    case AND:
      for (HintPredicate predicate : predicates) {
        if (!predicate.apply(hint, rel)) {
          return false;
        }
      }
      return true;
    case OR:
    default:
      for (HintPredicate predicate : predicates) {
        if (predicate.apply(hint, rel)) {
          return true;
        }
      }
      return false;
    }
  }
}
