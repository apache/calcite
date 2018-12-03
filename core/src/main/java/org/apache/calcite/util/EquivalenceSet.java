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

import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.TreeMultimap;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.SortedSet;

/** Set of elements organized into equivalence classes.
 *
 * <p>Elements are equivalent by the rules of a mathematical equivalence
 * relation:
 *
 * <dl>
 *   <dt>Reflexive
 *   <dd>Every element {@code e} is equivalent to itself
 *   <dt>Symmetric
 *   <dd>If {@code e} is equivalent to {@code f},
 *     then {@code f} is equivalent to {@code e}
 *   <dt>Transitive
 *   <dd>If {@code e} is equivalent to {@code f},
 *     and {@code f} is equivalent to {@code g},
 *     then {@code e} is equivalent to {@code g}
 * </dl>
 *
 * <p>For any given pair of elements, answers in O(log N) (two hash-table
 * lookups) whether they are equivalent to each other.
 *
 * @param <E> Element type
 */
public class EquivalenceSet<E extends Comparable<E>> {
  private final Map<E, E> parents = new HashMap<>();

  /** Adds an element, and returns the element (which is its own parent).
   * If already present, returns the element's parent. */
  public E add(E e) {
    final E parent = parents.get(Objects.requireNonNull(e));
    if (parent == null) {
      // Element is new. Add it to the map, as its own parent.
      parents.put(e, e);
      return e;
    } else {
      return parent;
    }
  }

  /** Marks two elements as equivalent.
   * They may or may not be registered, and they may or may not be equal. */
  public E equiv(E e, E f) {
    final E eParent = add(e);
    if (!eParent.equals(e)) {
      assert parents.get(eParent).equals(eParent);
      final E root = equiv(eParent, f);
      parents.put(e, root);
      return root;
    }
    final E fParent = add(f);
    if (!fParent.equals(f)) {
      assert parents.get(fParent).equals(fParent);
      final E root = equiv(e, fParent);
      parents.put(f, root);
      return root;
    }
    final int c = e.compareTo(f);
    if (c == 0) {
      return e;
    }
    if (c < 0) {
      // e is a better (lower) parent of f
      parents.put(f, e);
      return e;
    } else {
      // f is a better (lower) parent of e
      parents.put(e, f);
      return f;
    }
  }

  /** Returns whether two elements are in the same equivalence class.
   * Returns false if either or both of the elements are not registered. */
  public boolean areEquivalent(E e, E f) {
    final E eParent = parents.get(e);
    final E fParent = parents.get(f);
    return Objects.equals(eParent, fParent);
  }

  /** Returns a map of the canonical element in each equivalence class to the
   * set of elements in that class. The keys are sorted in natural order, as
   * are the elements within each key. */
  public SortedMap<E, SortedSet<E>> map() {
    final TreeMultimap<E, E> multimap = TreeMultimap.create();
    for (Map.Entry<E, E> entry : parents.entrySet()) {
      multimap.put(entry.getValue(), entry.getKey());
    }
    // Create an immutable copy. Keys and values remain in sorted order.
    final ImmutableSortedMap.Builder<E, SortedSet<E>> builder =
        ImmutableSortedMap.naturalOrder();
    for (Map.Entry<E, Collection<E>> entry : multimap.asMap().entrySet()) {
      builder.put(entry.getKey(), ImmutableSortedSet.copyOf(entry.getValue()));
    }
    return builder.build();
  }

  /** Removes all elements in this equivalence set. */
  public void clear() {
    parents.clear();
  }

  /** Returns the number of elements in this equivalence set. */
  public int size() {
    return parents.size();
  }

  /** Returns the number of equivalence classes in this equivalence set. */
  public int classCount() {
    return new HashSet<>(parents.values()).size();
  }
}

// End EquivalenceSet.java
