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

import com.google.common.collect.ImmutableSet;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * A set of functional dependencies with closure and minimal cover operations.
 * This class implements standard algorithms for functional dependency reasoning.
 */
public class FunctionalDependencySet {
  // Maximum number of transitive closure iterations to prevent infinite loops
  public static final int MAX_TRANSITIVE_CLOSURE_LOOPS = 10;
  // Maximum number of attributes supported in closure computation
  private static final int MAX_CLOSURE_ATTRS = 10000;

  private final Set<FunctionalDependency> fdSet = new HashSet<>();

  public FunctionalDependencySet() {}

  public FunctionalDependencySet(Set<FunctionalDependency> fds) {
    this.fdSet.addAll(fds);
  }

  public void addFD(FunctionalDependency fd) {
    if (!fd.isTrivial()) {
      fdSet.add(fd);
    }
  }

  public void addFD(ImmutableBitSet determinants, ImmutableBitSet dependents) {
    addFD(FunctionalDependency.of(determinants, dependents));
  }

  public void addBidirectionalFD(ImmutableBitSet determinants, ImmutableBitSet dependents) {
    addFD(determinants, dependents);
    addFD(dependents, determinants);
  }

  public void addFD(int determinant, int dependent) {
    addFD(ImmutableBitSet.of(determinant), ImmutableBitSet.of(dependent));
  }

  public void addBidirectionalFD(int determinant, int dependent) {
    addFD(ImmutableBitSet.of(determinant), ImmutableBitSet.of(dependent));
    addFD(ImmutableBitSet.of(dependent), ImmutableBitSet.of(determinant));
  }

  public void removeFD(FunctionalDependency fd) {
    fdSet.remove(fd);
  }

  public Set<FunctionalDependency> getFDs() {
    return Collections.unmodifiableSet(fdSet);
  }

  /**
   * Returns an ImmutableBitSet containing all attribute indexes that appear in any FD in the set.
   */
  public static ImmutableBitSet allAttributesFromFDs(FunctionalDependencySet fds) {
    ImmutableBitSet.Builder builder = ImmutableBitSet.builder();
    Set<FunctionalDependency> fdSet = fds.getFDs();
    for (FunctionalDependency fd : fdSet) {
      builder.addAll(fd.getDeterminants());
      builder.addAll(fd.getDependents());
    }
    return builder.build();
  }

  /**
   * Computes the closure of a set of attributes under this functional dependency set.
   * The closure of X, denoted X+, is the set of all attributes that can be functionally
   * determined by X using the functional dependencies in this set and
   * <a href="https://en.wikipedia.org/wiki/Armstrong%27s_axioms">Armstrong's axioms</a>
   *
   * @param attributes the input attribute set
   * @return the closure of the input attributes
   */
  public ImmutableBitSet closure(ImmutableBitSet attributes) {
    if (attributes.isEmpty()) {
      return ImmutableBitSet.of();
    }

    // For large attribute sets, skip detailed closure computation to avoid performance issues
    // The result may be an over-approximation
    if (attributes.cardinality() > MAX_CLOSURE_ATTRS) {
      return ImmutableBitSet.of(attributes);
    }

    Set<Integer> closureSet = new HashSet<>();
    Queue<Integer> queue = new ArrayDeque<>();
    for (int attr : attributes) {
      closureSet.add(attr);
      queue.add(attr);
    }

    Map<FunctionalDependency, Integer> fdMissingCount = new HashMap<>();
    Map<Integer, List<FunctionalDependency>> attrToFDs = new HashMap<>();
    for (FunctionalDependency fd : fdSet) {
      fdMissingCount.put(fd, fd.getDeterminants().cardinality());
      for (int det : fd.getDeterminants()) {
        attrToFDs.computeIfAbsent(det, k -> new ArrayList<>()).add(fd);
      }
    }

    while (!queue.isEmpty()) {
      Integer attr =
          requireNonNull(queue.poll(), "Queue returned null while computing closure");
      List<FunctionalDependency> fds = attrToFDs.get(attr);
      if (fds == null) {
        continue;
      }
      for (FunctionalDependency fd : fds) {
        int missing =
            requireNonNull(fdMissingCount.get(fd), "fdMissingCount returned null for FD " + fd);
        missing = missing - 1;
        fdMissingCount.put(fd, missing);
        if (missing == 0) {
          for (int dep : fd.getDependents()) {
            if (closureSet.add(dep)) {
              queue.add(dep);
            }
          }
        }
      }
    }

    return ImmutableBitSet.of(closureSet);
  }

  /**
   * Check if X determined Y is implied by this FD set.
   */
  public boolean implies(ImmutableBitSet determinants, ImmutableBitSet dependents) {
    // Check if there is a direct FD match
    for (FunctionalDependency fd : fdSet) {
      if (fd.getDeterminants().equals(determinants)
          && fd.getDependents().contains(dependents)) {
        return true;
      }
    }
    return closure(determinants).contains(dependents);
  }

  /**
   * Check if a single column is functionally determined by another column.
   */
  public boolean determines(int determinant, int dependent) {
    // Check if there is a direct FD match
    ImmutableBitSet detSet = ImmutableBitSet.of(determinant);
    for (FunctionalDependency fd : fdSet) {
      if (fd.getDeterminants().equals(detSet)
          && fd.getDependents().get(dependent)) {
        return true;
      }
    }
    return closure(detSet).get(dependent);
  }

  /**
   * Compute the minimal cover of this functional dependency set.
   * Returns an equivalent set with minimal dependencies.
   */
  public FunctionalDependencySet minimalCover() {
    // Split multi-attribute right sides into single attributes
    Set<FunctionalDependency> splitFDs = new HashSet<>();
    for (FunctionalDependency fd : fdSet) {
      splitFDs.addAll(fd.split());
    }
    splitFDs.removeIf(FunctionalDependency::isTrivial);

    // Remove redundant attributes from left sides
    Set<FunctionalDependency> reducedFDs = new HashSet<>();
    for (FunctionalDependency fd : splitFDs) {
      if (fd.getDeterminants().cardinality() <= 1) {
        reducedFDs.add(fd);
      } else {
        FunctionalDependencySet tempSet = new FunctionalDependencySet(splitFDs);
        tempSet.removeFD(fd);
        reducedFDs.add(reduceLeft(fd, tempSet));
      }
    }

    // Remove redundant functional dependencies
    reducedFDs.removeIf(fd -> {
      FunctionalDependencySet remainingFDs = new FunctionalDependencySet(reducedFDs);
      remainingFDs.removeFD(fd);
      return remainingFDs.implies(fd.getDeterminants(), fd.getDependents());
    });

    return new FunctionalDependencySet(reducedFDs);
  }

  /**
   * Reduce left side by removing redundant columns from determinants.
   */
  private static FunctionalDependency reduceLeft(FunctionalDependency fd,
      FunctionalDependencySet fdSet) {
    ImmutableBitSet determinants = fd.getDeterminants();
    ImmutableBitSet dependents = fd.getDependents();

    // Try removing each attribute to find minimal determinant set
    for (int attr : fd.getDeterminants()) {
      ImmutableBitSet reduced = determinants.clear(attr);
      if (fdSet.closure(reduced).contains(dependents)) {
        determinants = reduced;
      }
    }
    return FunctionalDependency.of(determinants, dependents);
  }

  /**
   * Check if this FD set is equivalent to another FD set.
   * Two FD sets are equivalent if they have the same closure for any attribute set.
   */
  public boolean equalTo(FunctionalDependencySet other) {
    for (FunctionalDependency fd : fdSet) {
      if (!other.implies(fd.getDeterminants(), fd.getDependents())) {
        return false;
      }
    }

    for (FunctionalDependency fd : other.fdSet) {
      if (!implies(fd.getDeterminants(), fd.getDependents())) {
        return false;
      }
    }

    return true;
  }

  /**
   * Find candidate keys within the given attribute set.
   *
   * @param attributes the set of attributes to search for candidate keys within
   * @param onlyMinimalKeys if true, only return minimal candidate keys (shortest length);
   *                        if false, return all minimal candidate keys
   * @return a set of attribute subsets that can determine all given attributes
   */
  public Set<ImmutableBitSet> findCandidateKeys(
      ImmutableBitSet attributes, boolean onlyMinimalKeys) {
    // Branch and bound algorithm for minimal candidate key search
    if (fdSet.isEmpty()) {
      return ImmutableSet.of(attributes);
    }

    // Attributes that are not dependents of any FD
    ImmutableBitSet essentialAttrs = findEssentialAttributes(attributes);
    if (closure(essentialAttrs).contains(attributes)) {
      return ImmutableSet.of(essentialAttrs);
    }

    Set<ImmutableBitSet> result = new HashSet<>();
    int minKeySize = Integer.MAX_VALUE;
    PriorityQueue<ImmutableBitSet> queue =
        new PriorityQueue<>(Comparator.comparingInt(ImmutableBitSet::cardinality));
    Set<ImmutableBitSet> visited = new HashSet<>();
    queue.add(essentialAttrs);

    while (!queue.isEmpty()) {
      ImmutableBitSet cand =
          requireNonNull(queue.poll(), "Queue returned null while searching candidate keys");
      if (visited.contains(cand)) {
        continue;
      }
      visited.add(cand);
      if (onlyMinimalKeys && cand.cardinality() > minKeySize) {
        break;
      }

      // If a candidate key is a superset of key, It is a redundant set
      boolean covered = false;
      for (ImmutableBitSet key : result) {
        if (cand.contains(key)) {
          covered = true;
          break;
        }
      }

      if (covered) {
        continue;
      }

      ImmutableBitSet candClosure = closure(cand);
      if (candClosure.contains(attributes)) {
        result.add(cand);
        if (onlyMinimalKeys) {
          minKeySize = cand.cardinality();
        }
        continue;
      }

      // Expand: only add attributes not in the closure
      ImmutableBitSet remain = attributes.except(cand);
      for (int attr : remain) {
        if (candClosure.get(attr)) {
          continue;
        }
        ImmutableBitSet next = cand.set(attr);
        if (!visited.contains(next)) {
          queue.add(next);
        }
      }
    }
    return result.isEmpty() ? ImmutableSet.of(attributes) : result;
  }

  private ImmutableBitSet findEssentialAttributes(ImmutableBitSet attributes) {
    // Find attributes that do not appear on the right side of any FD (essential for key)
    ImmutableBitSet.Builder dependentsBuilder = ImmutableBitSet.builder();
    for (FunctionalDependency fd : fdSet) {
      dependentsBuilder.addAll(fd.getDependents());
    }
    ImmutableBitSet dependentsAttrs = dependentsBuilder.build();
    return attributes.except(dependentsAttrs);
  }

  /**
   * Computes the transitive closure of this FD set by repeatedly applying transitivity.
   * For all {@code X->Y}, {@code Y->Z}, add {@code X->Z} until no new FDs are produced.
   */
  public FunctionalDependencySet computeTransitiveClosure() {
    boolean changed = true;
    int loops = 0;
    while (changed && loops++ < MAX_TRANSITIVE_CLOSURE_LOOPS) {
      changed = false;
      List<FunctionalDependency> currentFDs = new ArrayList<>(fdSet);
      for (FunctionalDependency fd1 : currentFDs) {
        for (FunctionalDependency fd2 : currentFDs) {
          // Only apply transitivity when fd1's dependents completely contain fd2's determinants
          if (fd1.getDependents().contains(fd2.getDeterminants())) {
            ImmutableBitSet newDeterminants = fd1.getDeterminants();
            ImmutableBitSet newDependents = fd2.getDependents();
            FunctionalDependency newFd = FunctionalDependency.of(newDeterminants, newDependents);
            if (!fdSet.contains(newFd)) {
              addFD(newFd);
              changed = true;
            }
          }
        }
      }
    }
    return this;
  }

  /**
   * Check if the given attribute set is a superkey.
   */
  public boolean isSuperkey(ImmutableBitSet attrs, ImmutableBitSet allAttributes) {
    return closure(attrs).equals(allAttributes);
  }

  /**
   * Check if the given attribute set is a key (minimal superkey).
   */
  public boolean isKey(ImmutableBitSet attrs, ImmutableBitSet allAttributes) {
    if (!isSuperkey(attrs, allAttributes)) {
      return false;
    }
    // Check minimality
    for (int attr : attrs) {
      if (isSuperkey(attrs.clear(attr), allAttributes)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Union with another FD set.
   */
  public FunctionalDependencySet union(FunctionalDependencySet other) {
    FunctionalDependencySet result = new FunctionalDependencySet(this.fdSet);
    for (FunctionalDependency fd : other.fdSet) {
      result.addFD(fd);
    }
    return result;
  }

  /**
   * Get all functional dependencies in this set.
   */
  public Set<FunctionalDependency> getFunctionalDependencies() {
    return Collections.unmodifiableSet(fdSet);
  }

  @Override public String toString() {
    return fdSet.toString();
  }
}
