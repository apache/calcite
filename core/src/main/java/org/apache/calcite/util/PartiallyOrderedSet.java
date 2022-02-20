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

import org.apache.calcite.config.CalciteSystemProperty;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.AbstractSet;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.apache.calcite.linq4j.Nullness.castNonNull;

import static java.util.Objects.requireNonNull;

/**
 * Partially-ordered set.
 *
 * <p>When you create a partially-ordered set ('poset' for short) you must
 * provide an {@link Ordering} that determines the order relation. The
 * ordering must be:</p>
 *
 * <ul>
 *     <li>reflexive: e.lte(e) returns true;</li>
 *     <li>anti-symmetric: if e.lte(f) returns true,
 *     then f.lte(e) returns false only if e = f;</li>
 *     <li>transitive: if e.lte(f) returns true and
 *     f.lte(g) returns true, then e.lte(g) must return true.</li>
 * </ul>
 *
 * <p>Note that not all pairs of elements are related. It is OK if e.lte(f)
 * returns false and f.lte(e) returns false also.</p>
 *
 * <p>In addition to the usual set methods, there are methods to determine the
 * immediate parents and children of an element in the set, and method to find
 * all elements which have no parents or no children (i.e. "root" and "leaf"
 * elements).</p>
 *
 * <p>A lattice is a special kind of poset where there is a unique top and
 * bottom element. You can use a PartiallyOrderedSet for a lattice also. It may
 * be helpful to add the top and bottom elements to the poset on
 * construction.</p>
 *
 * @param <E> Element type
 */
public class PartiallyOrderedSet<E> extends AbstractSet<E> {
  /** Ordering that orders bit sets by inclusion.
   *
   * <p>For example, the children of 14 (1110) are 12 (1100), 10 (1010) and
   * 6 (0110).
   */
  public static final Ordering<ImmutableBitSet> BIT_SET_INCLUSION_ORDERING =
      ImmutableBitSet::contains;

  private final Map<E, Node<E>> map;
  private final @Nullable Function<E, Iterable<E>> parentFunction;
  @SuppressWarnings("unused")
  private final @Nullable Function<E, Iterable<E>> childFunction;
  private final Ordering<E> ordering;

  /**
   * Synthetic node to hold all nodes that have no parents. It does not appear
   * in the set.
   */
  private final Node<E> topNode;
  private final Node<E> bottomNode;

  /**
   * Creates a partially-ordered set.
   *
   * @param ordering Ordering relation
   */
  public PartiallyOrderedSet(Ordering<E> ordering) {
    this(ordering, new HashMap<>(), null, null);
  }

  /**
   * Creates a partially-ordered set with a parent-generating function.
   *
   * @param ordering Ordering relation
   * @param parentFunction Function to compute parents of a node; may be null
   */
  public PartiallyOrderedSet(Ordering<E> ordering,
      Function<E, Iterable<E>> childFunction,
      Function<E, Iterable<E>> parentFunction) {
    this(ordering, new HashMap<>(), childFunction, parentFunction);
  }

  @SuppressWarnings({"Guava", "UnnecessaryMethodReference"})
  @Deprecated // to be removed before 2.0
  public PartiallyOrderedSet(Ordering<E> ordering,
      com.google.common.base.Function<E, Iterable<E>> childFunction,
      com.google.common.base.Function<E, Iterable<E>> parentFunction) {
    //noinspection FunctionalExpressionCanBeFolded
    this(ordering, (Function<E, Iterable<E>>) childFunction::apply,
        parentFunction::apply);
  }

  /**
   * Creates a partially-ordered set, and populates it with a given
   * collection.
   *
   * @param ordering Ordering relation
   * @param collection Initial contents of partially-ordered set
   */
  @SuppressWarnings("method.invocation.invalid")
  public PartiallyOrderedSet(Ordering<E> ordering, Collection<E> collection) {
    this(ordering, new HashMap<>(collection.size() * 3 / 2), null, null);
    addAll(collection);
  }

  /**
   * Internal constructor.
   *
   * @param ordering Ordering relation
   * @param map Map from values to nodes
   * @param parentFunction Function to compute parents of a node; may be null
   */
  private PartiallyOrderedSet(Ordering<E> ordering, Map<E, Node<E>> map,
      @Nullable Function<E, Iterable<E>> childFunction,
      @Nullable Function<E, Iterable<E>> parentFunction) {
    this.ordering = ordering;
    this.map = map;
    this.childFunction = childFunction;
    this.parentFunction = parentFunction;
    this.topNode = new TopBottomNode<>(true);
    this.bottomNode = new TopBottomNode<>(false);
    this.topNode.childList.add(bottomNode);
    this.bottomNode.parentList.add(topNode);
  }

  @SuppressWarnings("NullableProblems")
  @Override public Iterator<E> iterator() {
    final Iterator<E> iterator = map.keySet().iterator();
    return new Iterator<E>() {
      @Nullable E previous;

      @Override public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override public E next() {
        return previous = iterator.next();
      }

      @Override public void remove() {
        if (!PartiallyOrderedSet.this.remove(previous)) {
          // Object was not present.
          // Maybe they have never called 'next'?
          // Maybe they called 'remove' twice?
          // Either way, something is screwy.
          throw new IllegalStateException();
        }
      }
    };
  }

  @Override public int size() {
    return map.size();
  }

  @Override public boolean contains(@Nullable Object o) {
    //noinspection SuspiciousMethodCalls
    return map.containsKey(o);
  }

  @Override public boolean remove(@Nullable Object o) {
    final Node<E> node = map.remove(o);
    if (node == null) {
      return false;
    }
    for (int i = 0; i < node.parentList.size(); i++) {
      Node<E> parent = node.parentList.get(i);
      for (Node<E> child : node.childList) {
        if (parent.e == null && child.e == null) {
          parent.childList.remove(node);
          continue;
        }
        replace(parent.childList, node, child);
      }
    }
    for (int i = 0; i < node.childList.size(); i++) {
      Node<E> child = node.childList.get(i);
      for (Node<E> parent : node.parentList) {
        if (child.e == null && parent.e == null) {
          child.parentList.remove(node);
          continue;
        }
        replace(child.parentList, node, parent);
      }
    }
    return true;
  }

  /**
   * Adds an element to this lattice.
   */
  @Override public boolean add(E e) {
    assert e != null;
    assert !CalciteSystemProperty.DEBUG.value() || isValid(true);
    Node<E> node = map.get(e);
    if (node != null) {
      // already present
      return false;
    }
    Set<Node<E>> parents = findParents(e);
    Set<Node<E>> children = findChildren(e);

    node = new Node<>(e);

    for (Node<E> parent : parents) {
      node.parentList.add(parent);
      int n = 0;
      for (int i = 0; i < parent.childList.size(); i++) {
        Node<E> child = parent.childList.get(i);
        if (child.e == null || ordering.lessThan(child.e, e)) {
          if (parent.childList.contains(node)) {
            parent.childList.remove(i);
            --i;
          } else {
            parent.childList.set(i, node);
          }
          replace(child.parentList, parent, node);
          if (!node.childList.contains(child)) {
            node.childList.add(child);
          }
          ++n;
        }
      }
      if (n == 0) {
        parent.childList.add(node);
      }
    }

    // Nodes reachable from parents.
    final Set<Node<E>> childSet = new HashSet<>(node.childList);
    for (Node<E> child : children) {
      if (!isDescendantOfAny(child, childSet)) {
        node.childList.add(child);
        if (!child.parentList.contains(node)) {
          child.parentList.add(node);
        }
      }
    }

    map.put(node.e, node);
    assert !CalciteSystemProperty.DEBUG.value() || isValid(true);
    return true;
  }

  /**
   * Returns whether node's value is a descendant of any of the values in
   * nodeSet.
   *
   * @param node Node
   * @param nodeSet Suspected ancestors
   * @return Whether node is a descendant of any of the nodes
   */
  private boolean isDescendantOfAny(
      Node<E> node,
      Set<Node<E>> nodeSet) {
    final Deque<Node<E>> deque = new ArrayDeque<>();
    final Set<Node<E>> seen = new HashSet<>();
    deque.add(node);
    while (!deque.isEmpty()) {
      final Node<E> node1 = deque.pop();
      if (nodeSet.contains(node1)) {
        return true;
      }
      for (Node<E> parent : node1.parentList) {
        if (seen.add(parent)) {
          deque.add(parent);
        }
      }
    }
    return false;
  }

  private Set<Node<E>> findChildren(E e) {
    final Deque<Node<E>> descendants = new ArrayDeque<>();
    descendants.add(bottomNode);
    return findParentsChildren(e, descendants, false);
  }

  private Set<Node<E>> findParents(E e) {
    final Deque<Node<E>> ancestors = new ArrayDeque<>();
    ancestors.add(topNode);
    return findParentsChildren(e, ancestors, true);
  }

  private Set<Node<E>> findParentsChildren(
      E e,
      Deque<Node<E>> ancestors,
      boolean up) {
    final Set<Node<E>> parents = new HashSet<>();
    while (!ancestors.isEmpty()) {
      final Node<E> ancestor = ancestors.pop();
      assert ancestor.e == null
          || (up
          ? !ordering.lessThan(ancestor.e, e)
          : !ordering.lessThan(e, ancestor.e));
      assert ancestor.e != e; // e not in tree yet
      // Example: e = "a", ancestor = "abc".
      // If there is at least one child c of ancestor such that e <= c
      // (e.g. "ab", "ac") examine those children. If there are no
      // such children, ancestor becomes a parent
      int found = 0;
      for (Node<E> child : up ? ancestor.childList : ancestor.parentList) {
        if (child.e == null) {
          continue; // child is the bottom node
        }
        if (up
            ? ordering.lessThan(e, child.e)
            : ordering.lessThan(child.e, e)) {
          ancestors.add(child);
          ++found;
        } else if (up
            ? !ordering.lessThan(child.e, e)
            : !ordering.lessThan(e, child.e)) {
          // e is not less than child (and therefore some descendant
          // of child might be less than e). Recurse into its
          // children.
          ancestors.add(child);
        }
      }
      if (found == 0) {
        // None of the descendants of the node are greater than e.
        // Therefore node is a parent, provided that e is definitely
        // less than node
        if (ancestor.e == null
            || (up
            ? ordering.lessThan(e, ancestor.e)
            : ordering.lessThan(ancestor.e, e))) {
          parents.add(ancestor);
        }
      }
    }
    return parents;
  }

  private static <T> void replace(List<T> list, T remove, T add) {
    if (list.contains(add)) {
      list.remove(remove);
    } else {
      final int index = list.indexOf(remove);
      if (index >= 0) {
        list.set(index, add);
      } else {
        list.add(add);
      }
    }
  }

  /**
   * Checks internal consistency of this lattice.
   *
   * @param fail Whether to throw an assertion error
   * @return Whether valid
   */
  @SuppressWarnings({"ConstantConditions" })
  public boolean isValid(boolean fail) {
    // Top has no parents.
    // Bottom has no children.
    // Each node except top has at least one parent.
    // Each node except bottom has at least one child.
    // Every node's children list it as a parent.
    // Every node's parents list it as a child.
    for (Node<E> node : map.values()) {
      if ((node == topNode)
          != node.parentList.isEmpty()) {
        assert !fail
            : "only top node should have no parents " + node
            + ", parents " + node.parentList;
        return false;
      }
      if ((node == bottomNode)
          != node.childList.isEmpty()) {
        assert !fail
            : "only bottom node should have no children " + node
            + ", children " + node.childList;
        return false;
      }
      for (int i = 0; i < node.childList.size(); i++) {
        Node<E> child = node.childList.get(i);
        if (!child.parentList.contains(node)) {
          assert !fail
              : "child " + child + " of " + node
              + " does not know its parent";
          return false;
        }
        if (child.e != null && !ordering.lessThan(child.e, node.e)) {
          assert !fail
              : "child " + child.e + " not less than parent "
              + node.e;
          return false;
        }
        for (int i2 = 0; i2 < node.childList.size(); i2++) {
          Node<E> child2 = node.childList.get(i2);
          if (child == child2
              && i != i2) {
            assert !fail
                : "duplicate child " + child + " of parent " + node;
            return false;
          }
          if (child.e != null
              && child2.e != null
              && child != child2
              && ordering.lessThan(child.e, child2.e)) {
            assert !fail
                : "relation between children " + child.e
                + " and " + child2.e + " of node " + node.e;
            return false;
          }
        }
      }
      for (Node<E> parent : node.parentList) {
        if (!parent.childList.contains(node)) {
          assert !fail;
          return false;
        }
      }
    }
    final Map<Node, Integer> distanceToRoot = new HashMap<>();
    distanceRecurse(distanceToRoot, topNode, 0);
    for (Node<E> node : map.values()) {
      if (!distanceToRoot.containsKey(node)) {
        assert !fail : "node " + node + " is not reachable";
        return false;
      }
    }

    // For each pair of elements, ensure that elements are related if and
    // only if they are in the ancestors or descendants lists.
    final Map<Node<E>, Set<E>> nodeAncestors = new HashMap<>();
    final Map<Node<E>, Set<E>> nodeDescendants = new HashMap<>();
    for (Node<E> node : map.values()) {
      nodeAncestors.put(node, new HashSet<>(getAncestors(node.e)));
      nodeDescendants.put(node, new HashSet<>(getDescendants(node.e)));
    }
    for (Node<E> node1 : map.values()) {
      for (Node<E> node2 : map.values()) {
        final boolean lt12 = ordering.lessThan(node1.e, node2.e);
        final boolean lt21 = ordering.lessThan(node2.e, node1.e);
        if (node1 == node2) {
          if (!(lt12 && lt21)) {
            assert !fail
                : "self should be less than self: " + node1;
          }
        }
        if (lt12 && lt21) {
          if (!(node1 == node2)) {
            assert !fail
                : "node " + node1.e + " and node " + node2.e
                + " are less than each other but are not the same"
                + " value";
            return false;
          }
        }
        if (lt12 && !lt21) {
          if (!get(nodeAncestors, node1, "nodeAncestors").contains(node2.e)) {
            assert !fail
                : node1.e + " is less than " + node2.e + " but "
                + node2.e + " is not in the ancestor set of "
                + node1.e;
            return false;
          }
          if (!get(nodeDescendants, node2, "nodeDescendants").contains(node1.e)) {
            assert !fail
                : node1.e + " is less than " + node2.e + " but "
                + node1.e + " is not in the descendant set of "
                + node2.e;
            return false;
          }
        }
        if (lt21 && !lt12) {
          if (!get(nodeAncestors, node2, "nodeAncestors").contains(node1.e)) {
            assert !fail
                : node2.e + " is less than " + node1.e + " but "
                + node1.e + " is not in the ancestor set of "
                + node2.e;
            return false;
          }
          if (!get(nodeDescendants, node1, "nodeDescendants").contains(node2.e)) {
            assert !fail
                : node2.e + " is less than " + node1.e + " but "
                + node2.e + " is not in the descendant set of "
                + node1.e;
            return false;
          }
        }
      }
    }
    return true;
  }

  private static <E> Set<E> get(Map<Node<E>, Set<E>> map, Node<E> node, String label) {
    return requireNonNull(map.get(node),
        () -> label + " for node " + node);
  }

  private void distanceRecurse(
      Map<Node, Integer> distanceToRoot,
      Node<E> node,
      int distance) {
    final Integer best = distanceToRoot.get(node);
    if (best == null || distance < best) {
      distanceToRoot.put(node, distance);
    }
    if (best != null) {
      return;
    }
    for (Node<E> child : node.childList) {
      distanceRecurse(
          distanceToRoot,
          child,
          distance + 1);
    }
  }

  public void out(StringBuilder buf) {
    buf.append("PartiallyOrderedSet size: ");
    buf.append(size());
    buf.append(" elements: {\n");

    // breadth-first search, to iterate over every element once, printing
    // those nearest the top element first
    final Set<E> seen = new HashSet<>();
    final Deque<E> unseen = new ArrayDeque<>(getNonChildren());
    while (!unseen.isEmpty()) {
      E e = unseen.pop();
      buf.append("  ");
      buf.append(e);
      buf.append(" parents: ");
      final List<E> parents = getParents(e);
      buf.append(parents);
      buf.append(" children: ");
      final List<E> children = getChildren(e);
      buf.append(children);
      buf.append("\n");

      if (children != null) {
        for (E child : children) {
          if (seen.add(child)) {
            unseen.add(child);
          }
        }
      }
    }
    buf.append("}");
  }

  /**
   * Returns the values in this partially-ordered set that are less-than
   * a given value and there are no intervening values.
   *
   * <p>If the value is not in this set, returns null.
   *
   * @see #getDescendants
   *
   * @param e Value
   * @return List of values in this set that are directly less than the given
   *   value
   */
  public @Nullable List<E> getChildren(E e) {
    return getChildren(e, false);
  }

  /**
   * Returns the values in this partially-ordered set that are less-than
   * a given value and there are no intervening values.
   *
   * <p>If the value is not in this set, returns null if {@code hypothetical}
   * is false.
   *
   * @see #getDescendants
   *
   * @param e Value
   * @param hypothetical Whether to generate a list if value is not in the set
   * @return List of values in this set that are directly less than the given
   *   value
   */
  public @Nullable List<E> getChildren(E e, boolean hypothetical) {
    final Node<E> node = map.get(e);
    if (node == null) {
      if (hypothetical) {
        return strip(findChildren(e));
      } else {
        return null;
      }
    } else {
      return strip(node.childList);
    }
  }

  /**
   * Returns the values in this partially-ordered set that are greater-than
   * a given value and there are no intervening values.
   *
   * <p>If the value is not in this set, returns null.
   *
   * @see #getAncestors
   *
   * @param e Value
   * @return List of values in this set that are directly greater than the
   *   given value
   */
  public @Nullable List<E> getParents(E e) {
    return getParents(e, false);
  }

  /**
   * Returns the values in this partially-ordered set that are greater-than
   * a given value and there are no intervening values.
   *
   * <p>If the value is not in this set, returns {@code null} if
   * {@code hypothetical} is false.
   *
   * @see #getAncestors
   *
   * @param e Value
   * @param hypothetical Whether to generate a list if value is not in the set
   * @return List of values in this set that are directly greater than the
   *   given value
   */
  public @Nullable List<E> getParents(E e, boolean hypothetical) {
    final Node<E> node = map.get(e);
    if (node == null) {
      if (hypothetical) {
        if (parentFunction != null) {
          final ImmutableList.Builder<E> list = new ImmutableList.Builder<>();
          closure(parentFunction, e, list, new HashSet<>());
          return list.build();
        } else {
          return ImmutableList.copyOf(strip(findParents(e)));
        }
      } else {
        return null;
      }
    } else {
      return ImmutableList.copyOf(strip(node.parentList));
    }
  }

  private void closure(Function<E, Iterable<E>> generator, E e, ImmutableList.Builder<E> list,
      Set<E> set) {
    for (E p : requireNonNull(generator.apply(e))) {
      if (set.add(e)) {
        if (map.containsKey(p)) {
          list.add(p);
        } else {
          closure(generator, p, list, set);
        }
      }
    }
  }

  public List<E> getNonChildren() {
    return strip(topNode.childList);
  }

  public List<E> getNonParents() {
    return strip(bottomNode.parentList);
  }

  @Override public void clear() {
    map.clear();
    assert topNode.parentList.isEmpty();
    topNode.childList.clear();
    topNode.childList.add(bottomNode);
    assert bottomNode.childList.isEmpty();
    bottomNode.parentList.clear();
    bottomNode.parentList.add(topNode);
  }

  /**
   * Returns a list of values in the set that are less-than a given value.
   * The list is in topological order but order is otherwise
   * non-deterministic.
   *
   * @param e Value
   * @return Values less than given value
   */
  public List<E> getDescendants(E e) {
    return descendants(e, true);
  }

  /** Returns a list, backed by a list of
   * {@link org.apache.calcite.util.PartiallyOrderedSet.Node}s, that strips
   * away the node and returns the element inside.
   *
   * @param <E> Element type
   */
  public static <E> List<E> strip(List<Node<E>> list) {
    if (list.size() == 1
        && list.get(0).e == null) {
      // If parent list contains top element, a node whose element is null,
      // officially there are no parents.
      // Similarly child list and bottom element.
      return ImmutableList.of();
    }
    return Util.transform(list, node -> node.e);
  }

  /** Converts an iterable of nodes into the list of the elements inside.
   * If there is one node whose element is null, it represents a list
   * containing either the top or bottom element, so we return the empty list.
   *
   * @param <E> Element type
   */
  private static <E> ImmutableList<E> strip(Iterable<Node<E>> iterable) {
    final Iterator<Node<E>> iterator = iterable.iterator();
    if (!iterator.hasNext()) {
      return ImmutableList.of();
    }
    Node<E> node = iterator.next();
    if (!iterator.hasNext()) {
      if (node.e == null) {
        return ImmutableList.of();
      } else {
        return ImmutableList.of(node.e);
      }
    }
    final ImmutableList.Builder<E> builder = ImmutableList.builder();
    for (;;) {
      builder.add(node.e);
      if (!iterator.hasNext()) {
        return builder.build();
      }
      node = iterator.next();
    }
  }

  /**
   * Returns a list of values in the set that are less-than a given value.
   * The list is in topological order but order is otherwise
   * non-deterministic.
   *
   * @param e Value
   * @return Values less than given value
   */
  public List<E> getAncestors(E e) {
    return descendants(e, false);
  }

  private List<E> descendants(E e, boolean up) {
    Node<E> node = map.get(e);
    final Collection<Node<E>> c;
    if (node == null) {
      c = up ? findChildren(e) : findParents(e);
    } else {
      c = up ? node.childList : node.parentList;
    }
    if (c.size() == 1 && c.iterator().next().e == null) {
      return Collections.emptyList();
    }
    final Deque<Node<E>> deque = new ArrayDeque<>(c);

    final Set<Node<E>> seen = new HashSet<>();
    final ImmutableList.Builder<E> list = new ImmutableList.Builder<>();
    while (!deque.isEmpty()) {
      Node<E> node1 = deque.pop();
      list.add(node1.e);
      for (Node<E> child : up ? node1.childList : node1.parentList) {
        if (child.e == null) {
          // Node is top or bottom.
          break;
        }
        if (seen.add(child)) {
          deque.add(child);
        }
      }
    }
    return list.build();
  }

  /**
   * Holds a value, its parent nodes, and child nodes.
   *
   * <p>We deliberately do not override {@link #hashCode} or
   * {@link #equals(Object)}. A canonizing map ensures that within a
   * given PartiallyOrderedSet, two nodes are identical if and only if they
   * contain the same value.</p>
   *
   * @param <E> Element type
   */
  private static class Node<E> {
    final List<Node<E>> parentList = new ArrayList<>();
    final List<Node<E>> childList = new ArrayList<>();
    final E e;

    Node(E e) {
      this.e = e;
    }

    @Override public String toString() {
      return String.valueOf(e);
    }
  }

  /**
   * Subclass of Node for top/bottom nodes. Improves readability when
   * debugging.
   *
   * @param <E> Element type
   */
  private static class TopBottomNode<E> extends Node<E> {
    private final String description;

    TopBottomNode(boolean top) {
      super(castNonNull(null));
      this.description = top ? "top" : "bottom";
    }

    @Override public String toString() {
      return description;
    }
  }

  /**
   * Ordering relation.
   *
   * <p>To obey the constraints of the partially-ordered set, the function
   * must be consistent with the reflexive, anti-symmetric, and transitive
   * properties required by a partially ordered set.</p>
   *
   * <p>For instance, if {@code ordering(foo, foo)} returned false for any
   * not-null value of foo, it would violate the reflexive property.</p>
   *
   * <p>If an ordering violates any of these required properties, the behavior
   * of a {@link PartiallyOrderedSet} is unspecified. (But mayhem is
   * likely.)</p>
   *
   * @param <E> Element type
   */
  public interface Ordering<E> {
    /**
     * Returns whether element e1 is &le; e2 according to
     * the relation that defines a partially-ordered set.
     *
     * @param e1 Element 1
     * @param e2 Element 2
     * @return Whether element 1 is &le; element 2
     */
    boolean lessThan(E e1, E e2);
  }
}
