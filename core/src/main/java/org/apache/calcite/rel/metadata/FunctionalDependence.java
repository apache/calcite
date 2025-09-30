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
 * Represents a functional dependency between two sets of columns (by their indices)
 * in a relational schema.
 *
 * <p>A functional dependency specifies that the values of the {@code determinants} uniquely
 * determine the values of the {@code dependents}。In other words, for any two rows in the
 * relation, if the values of the determinant columns are equal, then the values of the
 * dependent columns must also be equal.
 *
 * <p>Notation: {@code determinants} &rarr; {@code dependents}
 *
 * <p>Example:
 * Given a table with columns [emp_id, name, dept]
 * If determinants = {0} (emp_id) and dependents = {1, 2} (name, dept),
 * this means: emp_id &rarr; {name, dept}
 * That is, each emp_id uniquely determines both name and dept.
 *
 * <p>Both determinants and dependents are sets of column indices, allowing this class to represent
 * one-to-one, one-to-many, many-to-one, and many-to-many relationships.
 */
public class FunctionalDependence {
  // The set of column indices that are the determinants in the functional dependency.
  private final ImmutableSet<Integer> determinants;

  // The set of column indices that are functionally determined by the {@link #determinants}.
  private final ImmutableSet<Integer> dependents;

  private FunctionalDependence(Set<Integer> determinants, Set<Integer> dependents) {
    this.determinants = ImmutableSet.copyOf(determinants);
    this.dependents = ImmutableSet.copyOf(dependents);
  }

  /**
   * Create FD from determinant set to dependent set.
   */
  public static FunctionalDependence of(Set<Integer> determinants, Set<Integer> dependents) {
    return new FunctionalDependence(determinants, dependents);
  }

  /**
   * Create FD from single determinant to single dependent.
   */
  public static FunctionalDependence of(int determinant, int dependent) {
    return FunctionalDependence.of(ImmutableSet.of(determinant), ImmutableSet.of(dependent));
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
    if (!(o instanceof FunctionalDependence)) {
      return false;
    }
    FunctionalDependence that = (FunctionalDependence) o;
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
