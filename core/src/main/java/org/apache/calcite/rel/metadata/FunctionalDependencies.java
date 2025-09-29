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

import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * Models a set of functional dependencies and provides methods for closure computation,
 * candidate key search, and related reasoning.
 */
public class FunctionalDependencies {
  // Maximum number of attributes supported in closure computation
  private static final int MAX_CLOSURE_ATTRS = 1000000;

  // determinant set -> dependents
  private final Map<Set<Integer>, Set<Integer>> dependencyGraph = new HashMap<>();
  // attribute -> determinant sets containing this attribute
  private final Map<Integer, Set<Set<Integer>>> reverseIndex = new HashMap<>();

  public FunctionalDependencies(Set<FunctionalDependency> fds) {
    for (FunctionalDependency fd : fds) {
      Set<Integer> determinants = new HashSet<>(fd.getDeterminants());
      Set<Integer> dependents = new HashSet<>(fd.getDependents());
      // Build dependency graph (merge dependents for same determinants)
      dependencyGraph.merge(determinants, dependents, (existing, newDeps) -> {
        existing.addAll(newDeps);
        return existing;
      });
      // Build reverse index
      for (int attr : determinants) {
        reverseIndex.computeIfAbsent(attr, k -> new HashSet<>()).add(determinants);
      }
    }
  }

  public Set<Set<Integer>> getDeterminants(int attr) {
    return reverseIndex.getOrDefault(attr, ImmutableSet.of());
  }

  public Set<Integer> getDependents(Set<Integer> determinants) {
    return dependencyGraph.getOrDefault(determinants, ImmutableSet.of());
  }

  //~ Methods ----------------------------------------------------------------

  public Set<Integer> computeClosure(Set<Integer> attributes) {
    if (attributes.size() > MAX_CLOSURE_ATTRS) {
      throw new IllegalArgumentException(
          "closure only supports up to " + MAX_CLOSURE_ATTRS
              + " attributes, but got " + attributes.size());
    }

    if (attributes.isEmpty()) {
      return ImmutableSet.of();
    }

    Set<Integer> closure = new HashSet<>(attributes);
    Set<Integer> processed = new HashSet<>(attributes);
    Queue<Integer> queue = new ArrayDeque<>(attributes);

    while (!queue.isEmpty()) {
      Integer currentAttr =
          requireNonNull(queue.poll(), "Queue returned null while computing closure");

      // Find all determinant sets related to the current attribute
      Set<Set<Integer>> relatedDeterminants = getDeterminants(currentAttr);

      for (Set<Integer> determinants : relatedDeterminants) {
        // Check if closure contains all determinants
        if (closure.containsAll(determinants)) {
          // Get dependents and find new attributes
          Set<Integer> dependents = getDependents(determinants);
          Set<Integer> newAttributes = dependents.stream()
              .filter(attr -> !closure.contains(attr))
              .collect(Collectors.toSet());

          // Add new attributes to closure and activate propagation
          if (!newAttributes.isEmpty()) {
            closure.addAll(newAttributes);
            newAttributes.stream()
                .filter(attr -> !processed.contains(attr))
                .forEach(attr -> {
                  queue.add(attr);
                  processed.add(attr);
                });
          }
        }
      }
    }

    return closure;
  }

  /**
   * Find candidate keys within the given attribute set.
   *
   * @param attributes the set of attributes to search for candidate keys within
   * @param onlyMinimalKeys if true, only return minimal candidate keys (shortest length);
   *                        if false, return all minimal candidate keys
   * @return a set of attribute subsets that can determine all given attributes
   */
  public Set<Set<Integer>> findCandidateKeys(Set<Integer> attributes, boolean onlyMinimalKeys) {
    if (dependencyGraph.isEmpty()) {
      return ImmutableSet.of(attributes);
    }

    Set<Integer> nonDependentAttrs = findNonDependentAttributes(attributes);
    if (computeClosure(nonDependentAttrs).containsAll(attributes)) {
      return ImmutableSet.of(nonDependentAttrs);
    }

    Set<Set<Integer>> result = new HashSet<>();
    int minKeySize = Integer.MAX_VALUE;
    PriorityQueue<Set<Integer>> queue = new PriorityQueue<>(Comparator.comparingInt(Set::size));
    Set<Set<Integer>> visited = new HashSet<>();
    queue.add(nonDependentAttrs);
    while (!queue.isEmpty()) {
      Set<Integer> cand = requireNonNull(queue.poll(), "queue.poll() returned null");
      if (visited.contains(cand)) {
        continue;
      }
      visited.add(cand);
      if (onlyMinimalKeys && cand.size() > minKeySize) {
        break;
      }
      boolean covered = false;
      for (Set<Integer> key : result) {
        if (cand.containsAll(key)) {
          covered = true;
          break;
        }
      }
      if (covered) {
        continue;
      }
      Set<Integer> candClosure = computeClosure(cand);
      if (candClosure.containsAll(attributes)) {
        result.add(cand);
        if (onlyMinimalKeys) {
          minKeySize = cand.size();
        }
        continue;
      }
      Set<Integer> remain = new HashSet<>(attributes);
      remain.removeAll(cand);
      for (int attr : remain) {
        if (candClosure.contains(attr)) {
          continue;
        }
        Set<Integer> next = new HashSet<>(cand);
        next.add(attr);
        if (!visited.contains(next)) {
          queue.add(next);
        }
      }
    }
    return result.isEmpty() ? ImmutableSet.of(attributes) : result;
  }

  /**
   * Find attributes in the given set that never appear as dependents in any FD.
   * These are the "source" attributes that cannot be derived from others.
   */
  private Set<Integer> findNonDependentAttributes(Set<Integer> attributes) {
    // Collect all attributes that appear as dependents in any FD
    Set<Integer> dependentsAttrs = dependencyGraph.values().stream()
        .flatMap(Set::stream)
        .collect(Collectors.toSet());
    Set<Integer> result = new HashSet<>(attributes);
    result.removeAll(dependentsAttrs);
    return result;
  }

  /**
   * Returns a new FunctionalDependencies that is the union of this and another FD set.
   */
  public FunctionalDependencies union(FunctionalDependencies other) {
    Set<FunctionalDependency> unionSet = new HashSet<>();
    unionSet.addAll(this.getAllFDs());
    unionSet.addAll(other.getAllFDs());
    return new FunctionalDependencies(unionSet);
  }

  public Set<FunctionalDependency> getAllFDs() {
    Set<FunctionalDependency> result = new HashSet<>();
    for (Map.Entry<Set<Integer>, Set<Integer>> entry : dependencyGraph.entrySet()) {
      result.add(FunctionalDependency.of(entry.getKey(), entry.getValue()));
    }
    return result;
  }

  public boolean equalTo(FunctionalDependencies other) {
    for (Map.Entry<Set<Integer>, Set<Integer>> entry : dependencyGraph.entrySet()) {
      if (!other.implies(entry.getKey(), entry.getValue())) {
        return false;
      }
    }
    for (Map.Entry<Set<Integer>, Set<Integer>> entry : other.dependencyGraph.entrySet()) {
      if (!implies(entry.getKey(), entry.getValue())) {
        return false;
      }
    }
    return true;
  }

  @Override public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("FunctionalDependencies{");
    boolean first = true;
    for (FunctionalDependency fd : getAllFDs()) {
      if (!first) {
        sb.append(", ");
      }
      sb.append(fd);
      first = false;
    }
    sb.append('}');
    return sb.toString();
  }

  /**
   * Returns true if this FD set implies that determinants determinate dependents.
   * That is, if dependents ⊆ closure(determinants).
   */
  public boolean implies(Set<Integer> determinants, Set<Integer> dependents) {
    Set<Integer> dets = dependencyGraph.get(determinants);
    if (dets != null && dets.containsAll(dependents)) {
      return true;
    }
    return computeClosure(determinants).containsAll(dependents);
  }

  /**
   * Builder for FunctionalDependencies.
   * Supports fluent FD addition and builds the dependency graph.
   */
  public static class Builder {
    private final Set<FunctionalDependency> fdSet = new HashSet<>();

    /**
     * Add a functional dependency from determinant set to dependent set.
     */
    public Builder addFD(Set<Integer> determinants, Set<Integer> dependents) {
      fdSet.add(FunctionalDependency.of(determinants, dependents));
      return this;
    }

    /**
     * Add a functional dependency from a single determinant to a single dependent.
     */
    public Builder addFD(int determinant, int dependent) {
      fdSet.add(FunctionalDependency.of(determinant, dependent));
      return this;
    }

    /**
     * Add a functional dependency from a single determinant to multiple dependents.
     */
    public Builder addFD(int determinant, Set<Integer> dependents) {
      fdSet.add(FunctionalDependency.of(ImmutableSet.of(determinant), dependents));
      return this;
    }

    /**
     * Add a functional dependency from multiple determinants to a single dependent.
     */
    public Builder addFD(Set<Integer> determinants, int dependent) {
      fdSet.add(FunctionalDependency.of(determinants, ImmutableSet.of(dependent)));
      return this;
    }

    /**
     * Build the FunctionalDependencies instance and compute the dependency graph.
     */
    public FunctionalDependencies build() {
      return new FunctionalDependencies(fdSet);
    }
  }
}
