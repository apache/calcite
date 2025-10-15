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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayDeque;
import java.util.BitSet;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Represents a set of functional dependencies. Each functional dependency is an {@link Arrow}.
 *
 * <p>An {@link ArrowSet} models a set of functional dependencies that may hold in a relation.
 * This class provides implementations for several core algorithms in functional dependency theory,
 * such as closure computation and candidate key discovery.
 * For theory background, see:
 * <a href="https://en.wikipedia.org/wiki/Functional_dependency">
 * Functional dependency (Wikipedia)</a>
 *
 * @see Arrow
 * @see ImmutableBitSet
 */
public class ArrowSet {
  public static final ArrowSet EMPTY = new ArrowSet(ImmutableSet.of());

  // All arrows in this ArrowSet.
  private ImmutableList<Arrow> arrowSet;

  // Maps each determinant set to the dependent set it functionally determines (for fast lookup).
  private final ImmutableMap<ImmutableBitSet, ImmutableBitSet> determinantsToDependentsMap;

  // Maps each column ordinal to the determinant sets (keys of determinantsToDependentsMap).
  private final ImmutableMap<Integer, ImmutableSet<Arrow>> ordinalToArrows;

  public ArrowSet(Set<Arrow> arrows) {
    Set<Arrow> minimalArrows = computeMinimalDependencySet(arrows);
    arrowSet = ImmutableList.copyOf(minimalArrows);
    Map<ImmutableBitSet, ImmutableBitSet> detToDep = new HashMap<>();
    Map<Integer, Set<Arrow>> ordToArrows = new HashMap<>();
    for (Arrow arrow : minimalArrows) {
      ImmutableBitSet determinants = arrow.getDeterminants();
      ImmutableBitSet dependents = arrow.getDependents();
      detToDep.merge(determinants, dependents, ImmutableBitSet::union);
      for (int det : determinants) {
        ordToArrows.computeIfAbsent(det, k -> new HashSet<>()).add(arrow);
      }
    }
    determinantsToDependentsMap = ImmutableMap.copyOf(detToDep);
    ImmutableMap.Builder<Integer, ImmutableSet<Arrow>> builder = ImmutableMap.builder();
    for (Map.Entry<Integer, Set<Arrow>> entry : ordToArrows.entrySet()) {
      builder.put(entry.getKey(), ImmutableSet.copyOf(entry.getValue()));
    }
    ordinalToArrows = builder.build();
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Computes the closure of a given set of column ordinals with respect to this ArrowSet.
   *
   * <p>The closure is the maximal set of attributes such that X → X⁺ can be inferred
   * through transitive application of
   * <a href="https://en.wikipedia.org/wiki/Armstrong%27s_axioms">Armstrong's axioms</a>
   *
   * <p>Example:
   * <blockquote>
   * <pre>
   * // Given functional dependencies:
   * // {0} → {1}
   * // {1} → {2}
   * // dependents(ImmutableBitSet.of(0)) = {0, 1, 2}
   * </pre>
   * </blockquote>
   *
   * <p>Time complexity: O(m + n), m = arrow count, n = ordinal count.
   * For interactive use, keep n below a few hundred for performance.
   *
   * @param ordinals the set of column ordinals whose closure is to be computed
   * @return an immutable set of column ordinals that can be determined from the input
   */
  public ImmutableBitSet dependents(ImmutableBitSet ordinals) {
    if (ordinals.isEmpty()) {
      return ImmutableBitSet.of();
    }

    BitSet closureSet = new BitSet();
    Queue<Integer> queue = new ArrayDeque<>();
    for (int attr : ordinals) {
      closureSet.set(attr);
      queue.add(attr);
    }

    Map<Arrow, Integer> missingCount = new HashMap<>();
    for (Arrow arrow : arrowSet) {
      missingCount.put(arrow, arrow.getDeterminants().cardinality());
    }

    while (!queue.isEmpty()) {
      int attr =
          requireNonNull(queue.poll(), "Queue returned null while computing dependents");
      Set<Arrow> arrows = ordinalToArrows.get(attr);
      if (arrows == null) {
        continue;
      }
      for (Arrow arrow : arrows) {
        int count =
            requireNonNull(missingCount.get(arrow),
                "missingCount returned null for Arrow " + arrow);
        if (count > 0) {
          count--;
          missingCount.put(arrow, count);
          if (count == 0) {
            for (int dep : arrow.getDependents()) {
              if (!closureSet.get(dep)) {
                closureSet.set(dep);
                queue.add(dep);
              }
            }
          }
        }
      }
    }

    return ImmutableBitSet.of(closureSet.stream().toArray());
  }

  /**
   * Finds all minimal determinant sets for a given set of column ordinals based on this ArrowSet.
   *
   * <p>Example:
   * <pre>{@code
   * // Given functional dependencies:
   * // {0} → {1}
   * // {1} → {2}
   * // {2} → {3}
   * // The ordinals is {0, 1, 2, 3}:
   * // determinants(ImmutableBitSet.of(0, 1, 2, 3)) returns [{0}]
   * }
   * </pre>
   *
   * @param ordinals a set of attribute ordinals for which to find determinant sets
   * @return the determinant sets, each represented as an ImmutableBitSet
   */
  public Set<ImmutableBitSet> determinants(ImmutableBitSet ordinals) {
    if (arrowSet.isEmpty()) {
      return ImmutableSet.of(ordinals);
    }

    ImmutableBitSet nonDependentOrdinals = findNonDependentAttributes(ordinals);
    if (dependents(nonDependentOrdinals).contains(ordinals)) {
      return ImmutableSet.of(nonDependentOrdinals);
    }

    Set<ImmutableBitSet> keys = new HashSet<>();
    int minSize = Integer.MAX_VALUE;
    PriorityQueue<ImmutableBitSet> queue =
        new PriorityQueue<>(Comparator.comparingInt(ImmutableBitSet::cardinality));
    Set<ImmutableBitSet> visited = new HashSet<>();
    queue.add(nonDependentOrdinals);

    while (!queue.isEmpty()) {
      ImmutableBitSet ords = requireNonNull(queue.poll(), "queue.poll() returned null");
      if (visited.contains(ords)) {
        continue;
      }
      visited.add(ords);
      if (ords.cardinality() > minSize) {
        continue;
      }
      boolean covered = false;
      for (ImmutableBitSet key : keys) {
        if (ords.contains(key)) {
          covered = true;
          break;
        }
      }
      if (covered) {
        continue;
      }
      ImmutableBitSet closure = dependents(ords);
      if (closure.contains(ordinals)) {
        keys.add(ords);
        minSize = ords.cardinality();
        continue;
      }
      // Try adding more attributes from ordinals
      for (int attr : ordinals) {
        if (!ords.get(attr)) {
          ImmutableBitSet next = ords.union(ImmutableBitSet.of(attr));
          if (!visited.contains(next)) {
            queue.add(next);
          }
        }
      }
    }
    return keys.isEmpty() ? ImmutableSet.of(ordinals) : keys;
  }

  /**
   * Find ordinals in the given set that never appear as dependents in any functional dependency.
   * These are the "source" ordinals that cannot be derived from others.
   */
  private ImmutableBitSet findNonDependentAttributes(ImmutableBitSet ordinals) {
    ImmutableBitSet dependentsAttrs = determinantsToDependentsMap.values().stream()
        .reduce(ImmutableBitSet.of(), ImmutableBitSet::union);
    return ordinals.except(dependentsAttrs);
  }

  /**
   * Returns a new ArrowSet that is the union of this and another ArrowSet.
   */
  public ArrowSet union(ArrowSet other) {
    Set<Arrow> unionSet = new HashSet<>();
    unionSet.addAll(this.getArrows());
    unionSet.addAll(other.getArrows());
    return new ArrowSet(unionSet);
  }

  /**
   * Returns all arrows (functional dependencies) in this ArrowSet.
   */
  public ImmutableList<Arrow> getArrows() {
    return arrowSet;
  }

  @Override public ArrowSet clone() {
    return new ArrowSet(new HashSet<>(this.arrowSet));
  }

  public boolean equalTo(ArrowSet other) {
    for (Arrow arrow : arrowSet) {
      if (!other.implies(arrow.getDeterminants(), arrow.getDependents())) {
        return false;
      }
    }
    return true;
  }

  @Override public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("ArrowSet{");
    boolean first = true;
    for (Arrow arrow : getArrows()) {
      if (!first) {
        sb.append(", ");
      }
      sb.append(arrow);
      first = false;
    }
    sb.append('}');
    return sb.toString();
  }

  /**
   * Returns true if, from this ArrowSet, one can deduce that {@code determinants}
   * determine {@code dependents}. That is,
   * if {@code dependents} ⊆ closure({@code determinants}).
   */
  public boolean implies(ImmutableBitSet determinants, ImmutableBitSet dependents) {
    ImmutableBitSet dets = determinantsToDependentsMap.get(determinants);
    if (dets != null && dets.contains(dependents)) {
      return true;
    }
    return dependents(determinants).contains(dependents);
  }

  /**
   * Builder for ArrowSet.
   */
  public static class Builder {
    private final Set<Arrow> arrowSet = new HashSet<>();

    /**
     * Add an Arrow from determinant set to dependent set.
     */
    public Builder addArrow(ImmutableBitSet lhs, ImmutableBitSet rhs) {
      arrowSet.add(Arrow.of(lhs, rhs));
      return this;
    }

    /**
     * Add an Arrow from a single determinant to a single dependent.
     */
    public Builder addArrow(int lhs, int rhs) {
      arrowSet.add(Arrow.of(lhs, rhs));
      return this;
    }

    public Builder addBidirectionalArrow(int lhs, int rhs) {
      addArrow(lhs, rhs);
      addArrow(rhs, lhs);
      return this;
    }

    public Builder addBidirectionalArrow(ImmutableBitSet lhs, ImmutableBitSet rhs) {
      addArrow(lhs, rhs);
      addArrow(rhs, lhs);
      return this;
    }

    public Builder addArrowSet(ArrowSet set) {
      for (Arrow arrow : set.getArrows()) {
        addArrow(arrow.getDeterminants(), arrow.getDependents());
      }
      return this;
    }

    /**
     * Build the ArrowSet instance and compute the functional dependency graph.
     */
    public ArrowSet build() {
      return new ArrowSet(arrowSet);
    }
  }

  /**
   * Computes a minimal ArrowSet by removing obvious redundant Arrow.
   *
   * <p>This method removes three obvious types of redundancy:
   * <ul>
   *   <li>Right-side consolidation: If {0} → {1} and {0} → {2}, merge to {0} → {1, 2}</li>
   *   <li>Left-side redundancy: If {0} → {1} exists, then {0, 2} → {1} is redundant</li>
   *   <li>Trivial dependencies: Remove dependents that are already in determinants,
   *   If {0, 1, 2} → {0, 1, 3, 4}, simplify to {0, 1, 2} → {3, 4}</li>
   * </ul>
   */
  private static Set<Arrow> computeMinimalDependencySet(Set<Arrow> arrows) {
    if (arrows.isEmpty()) {
      return new HashSet<>();
    }

    // right-side consolidation and remove trivial dependencies
    Map<ImmutableBitSet, ImmutableBitSet> consolidated = new HashMap<>();
    for (Arrow arrow : arrows) {
      ImmutableBitSet determinants = arrow.getDeterminants();
      ImmutableBitSet dependents = arrow.getDependents();

      ImmutableBitSet nonTrivialDependents = dependents.except(determinants);
      if (nonTrivialDependents.isEmpty()) {
        continue;
      }

      consolidated.merge(determinants, nonTrivialDependents, ImmutableBitSet::union);
    }

    // left-side redundancy
    // If {0} → N exists, remove any {0, 1, ...} → N' where N' ⊆ N
    Set<ImmutableBitSet> toRemove = new HashSet<>();
    for (Map.Entry<ImmutableBitSet, ImmutableBitSet> entry : consolidated.entrySet()) {
      ImmutableBitSet determinants = entry.getKey();
      ImmutableBitSet dependents = entry.getValue();

      for (Map.Entry<ImmutableBitSet, ImmutableBitSet> other : consolidated.entrySet()) {
        if (entry.equals(other)) {
          continue;
        }
        ImmutableBitSet otherDeterminants = other.getKey();
        ImmutableBitSet otherDependents = other.getValue();

        // If otherDeterminants is a proper subset of determinants
        // and otherDependents contains all of dependents, then this entry is redundant
        if (determinants.contains(otherDeterminants)
            && determinants.cardinality() > otherDeterminants.cardinality()
            && otherDependents.contains(dependents)) {
          toRemove.add(determinants);
          break;
        }
      }
    }

    Set<Arrow> minimal = new HashSet<>();
    for (Map.Entry<ImmutableBitSet, ImmutableBitSet> entry : consolidated.entrySet()) {
      if (!toRemove.contains(entry.getKey())) {
        minimal.add(Arrow.of(entry.getKey(), entry.getValue()));
      }
    }

    return minimal;
  }
}
