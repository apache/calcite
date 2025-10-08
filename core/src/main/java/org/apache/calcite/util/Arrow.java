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
package org.apache.calcite.util;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Represents one functional dependency (Arrow) between two sets of columns,
 * where each column is identified by its ordinal index.
 *
 * <p>{@link Arrow} models the functional dependency such that the values of the
 * {@link #determinants} columns uniquely determine the values of the
 * {@link #dependents} columns. In other words, if two rows have the same values
 * for all determinant columns, they must also have the same values for all
 * dependent columns. Both {@link #determinants} and {@link #dependents} are
 * ImmutableBitSet column ordinals.
 *
 * <p>This structure supports arbitrary cardinality for both determinant and
 * dependent column sets, allowing the representation relationships:
 * <ul>
 *   <li>One-to-one: {@code {0} → {1}}
 *   <li>One-to-many: {@code {0} → {1, 2}}
 *   <li>Many-to-one: {@code {0, 1} → {2}}
 *   <li>Many-to-many: {@code {0, 1} → {2, 3}}
 * </ul>
 *
 * <p>Example:
 *
 * <blockquote>
 * <pre>
 * Table schema: [emp_id, name, dept, salary]  // ordinals: 0, 1, 2, 3
 * Arrow: {0} → {1, 2}
 * Functional dependency: emp_id → {name, dept}
 * </pre>
 * </blockquote>
 *
 * <p>This indicates that the employee ID uniquely determines both the name
 * and department attributes.
 */
public class Arrow {
  // The set of column ordinals that are the determinants in the functional dependency.
  private final ImmutableBitSet determinants;

  // The set of column ordinals that are determined by the determinants.
  private final ImmutableBitSet dependents;

  private Arrow(ImmutableBitSet determinants, ImmutableBitSet dependents) {
    this.determinants = requireNonNull(determinants, "determinants must not be null");
    this.dependents = requireNonNull(dependents, "dependents must not be null");
  }

  /**
   * Create Arrow from determinant set to dependent set.
   */
  public static Arrow of(ImmutableBitSet determinants, ImmutableBitSet dependents) {
    return new Arrow(determinants, dependents);
  }

  /**
   * Create Arrow from single determinant to single dependent.
   */
  public static Arrow of(int determinant, int dependent) {
    return Arrow.of(ImmutableBitSet.of(determinant), ImmutableBitSet.of(dependent));
  }

  public ImmutableBitSet getDeterminants() {
    return determinants;
  }

  public ImmutableBitSet getDependents() {
    return dependents;
  }

  /**
   * Returns true if this Arrow is trivial (dependents ⊆ determinants).
   */
  public boolean isTrivial() {
    return determinants.contains(dependents);
  }

  @Override public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Arrow)) {
      return false;
    }
    Arrow that = (Arrow) o;
    return determinants.equals(that.determinants)
        && dependents.equals(that.dependents);
  }

  @Override public int hashCode() {
    return Objects.hash(determinants, dependents);
  }

  @Override public String toString() {
    return determinants + " -> " + dependents;
  }
}
