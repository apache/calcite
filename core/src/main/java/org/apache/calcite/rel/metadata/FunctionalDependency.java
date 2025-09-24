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
package org.apache.calcite.rel.metadata;

import org.apache.calcite.util.ImmutableBitSet;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Functional dependency: X (column indexes) determines Y.
 *
 * <p>X, Y are sets of column indexes.
 *
 * <p>If t1[X]=t2[X], then t1[Y]=t2[Y] for any tuples t1, t2.
 */
public class FunctionalDependency {
  private final ImmutableBitSet determinants;
  private final ImmutableBitSet dependents;

  private FunctionalDependency(ImmutableBitSet determinants, ImmutableBitSet dependents) {
    this.determinants = requireNonNull(determinants, "determinants");
    this.dependents = requireNonNull(dependents, "dependents");
  }

  /**
   * Create FD from column indices.
   */
  public static FunctionalDependency of(int[] determinantColumns, int[] dependentColumns) {
    return new FunctionalDependency(
        ImmutableBitSet.of(determinantColumns),
        ImmutableBitSet.of(dependentColumns));
  }

  /**
   * Create FD from single determinant to single dependent.
   */
  public static FunctionalDependency of(int determinant, int dependent) {
    return new FunctionalDependency(
        ImmutableBitSet.of(determinant),
        ImmutableBitSet.of(dependent));
  }

  /**
   * Create FD from determinant set to dependent set.
   */
  public static FunctionalDependency of(ImmutableBitSet determinants, ImmutableBitSet dependents) {
    return new FunctionalDependency(determinants, dependents);
  }

  public ImmutableBitSet getDeterminants() {
    return determinants;
  }

  public ImmutableBitSet getDependents() {
    return dependents;
  }

  /**
   * Returns true if this FD is trivial (dependents ⊆ determinants).
   */
  public boolean isTrivial() {
    return determinants.contains(dependents);
  }

  /**
   * Split this FD into multiple FDs, each with a single dependent column.
   */
  public Set<FunctionalDependency> split() {
    Set<FunctionalDependency> result = new HashSet<>();
    for (int dependent : dependents) {
      result.add(new FunctionalDependency(determinants, ImmutableBitSet.of(dependent)));
    }
    return result;
  }

  @Override public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof FunctionalDependency)) {
      return false;
    }
    FunctionalDependency that = (FunctionalDependency) o;
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
