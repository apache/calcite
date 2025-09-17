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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * A set of functional dependencies with closure and minimal cover operations.
 * This class implements standard algorithms for functional dependency reasoning.
 */
public class FunctionalDependencySet {
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
    addFD(new FunctionalDependency(determinants, dependents));
  }

  public void addFD(int determinant, int dependent) {
    addFD(ImmutableBitSet.of(determinant), ImmutableBitSet.of(dependent));
  }

  public void removeFD(FunctionalDependency fd) {
    fdSet.remove(fd);
  }

  public Set<FunctionalDependency> getFDs() {
    return Collections.unmodifiableSet(fdSet);
  }

  public boolean isEmpty() {
    return fdSet.isEmpty();
  }

  public int size() {
    return fdSet.size();
  }

  /**
   * Returns an ImmutableBitSet containing all attribute indexes that appear in any FD in the set.
   */
  public static ImmutableBitSet allAttributesFromFds(FunctionalDependencySet fds) {
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
   * determined by X using the functional dependencies in this set and Armstrong's axioms.
   *
   * @param attrs the input attribute set
   * @return the closure of the input attributes
   */
  public ImmutableBitSet closure(ImmutableBitSet attrs) {
    ImmutableBitSet closure = attrs;
    boolean changed;
    do {
      changed = false;
      for (FunctionalDependency fd : fdSet) {
        if (closure.contains(fd.getDeterminants())) {
          ImmutableBitSet newClosure = closure.union(fd.getDependents());
          if (!newClosure.equals(closure)) {
            closure = newClosure;
            changed = true;
          }
        }
      }
    } while (changed);
    return closure;
  }

  /**
   * Check if X determined Y is implied by this FD set.
   */
  public boolean implies(ImmutableBitSet determinants, ImmutableBitSet dependents) {
    return closure(determinants).contains(dependents);
  }

  /**
   * Check if a single column is functionally determined by another column.
   */
  public boolean determines(int determinant, int dependent) {
    return closure(ImmutableBitSet.of(determinant)).get(dependent);
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
      FunctionalDependencySet tempSet = new FunctionalDependencySet(splitFDs);
      tempSet.removeFD(fd);
      reducedFDs.add(reduceLeft(fd, tempSet));
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
    return new FunctionalDependency(determinants, dependents);
  }

  /**
   * Check if this FD set is equivalent to another FD set.
   * Two FD sets are equivalent if they have the same closure for any attribute set.
   */
  public boolean equalTo(FunctionalDependencySet other) {
    // Check if every FD in this set is implied by the other set
    for (FunctionalDependency fd : fdSet) {
      if (!other.implies(fd.getDeterminants(), fd.getDependents())) {
        return false;
      }
    }
    // Check if every FD in the other set is implied by this set
    for (FunctionalDependency fd : other.fdSet) {
      if (!implies(fd.getDeterminants(), fd.getDependents())) {
        return false;
      }
    }
    return true;
  }

  /**
   * Find all candidate keys within the given attribute set.
   * A candidate key is a minimal subset of the given attributes such that
   * its closure contains all the given attributes.
   *
   * @param attributes the set of attributes to search for candidate keys within
   * @return a set of minimal attribute subsets that can determine all given attributes
   */
  public Set<ImmutableBitSet> findKeys(ImmutableBitSet attributes) {
    if (fdSet.isEmpty()) {
      return ImmutableSet.of(attributes);
    }

    Set<ImmutableBitSet> level = new HashSet<>();
    for (int attr : attributes) {
      level.add(ImmutableBitSet.of(attr));
    }

    while (!level.isEmpty()) {
      Set<ImmutableBitSet> found = new HashSet<>();
      for (ImmutableBitSet cand : level) {
        if (closure(cand).contains(attributes)) {
          found.add(cand);
        }
      }
      if (!found.isEmpty()) {
        // Keys at this level are minimal and then return
        return found;
      }

      Set<ImmutableBitSet> next = new HashSet<>();
      for (ImmutableBitSet cand : level) {
        for (int attr : attributes) {
          if (!cand.get(attr)) {
            next.add(cand.set(attr));
          }
        }
      }

      level = next;
    }

    return ImmutableSet.of(attributes);
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
