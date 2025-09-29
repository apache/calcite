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

import com.google.common.collect.ImmutableSet;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Objects;
import java.util.Set;

/**
 * Represents a single functional dependency (FD) between two sets of attributes.
 */
public class FunctionalDependency {
  private final ImmutableSet<Integer> determinants;
  private final ImmutableSet<Integer> dependents;

  private FunctionalDependency(Set<Integer> determinants, Set<Integer> dependents) {
    this.determinants = ImmutableSet.copyOf(determinants);
    this.dependents = ImmutableSet.copyOf(dependents);
  }

  /**
   * Create FD from determinant set to dependent set.
   */
  public static FunctionalDependency of(Set<Integer> determinants, Set<Integer> dependents) {
    return new FunctionalDependency(determinants, dependents);
  }

  /**
   * Create FD from single determinant to single dependent.
   */
  public static FunctionalDependency of(int determinant, int dependent) {
    return FunctionalDependency.of(ImmutableSet.of(determinant), ImmutableSet.of(dependent));
  }

  public Set<Integer> getDeterminants() {
    return determinants;
  }

  public Set<Integer> getDependents() {
    return dependents;
  }

  /**
   * Returns true if this FD is trivial (dependents ⊆ determinants).
   */
  public boolean isTrivial() {
    return determinants.containsAll(dependents);
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
