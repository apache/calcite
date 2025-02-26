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

import org.apache.calcite.rel.metadata.NullSentinel;

import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.apache.calcite.linq4j.Nullness.castNonNull;

import static java.util.Objects.requireNonNull;

/**
 * An immutable set that may contain null values.
 *
 * <p>If the set cannot contain null values, use {@link ImmutableSet}.
 *
 * <p>We do not yet support sorted sets.
 *
 * @param <E> Element type
 */
public class ImmutableNullableSet<E> extends AbstractSet<E> {
  @SuppressWarnings("rawtypes")
  private static final Set SINGLETON_NULL =
      new ImmutableNullableSet(ImmutableSet.of(NullSentinel.INSTANCE));

  private static final Set<Integer> SINGLETON = Collections.singleton(0);

  private final ImmutableSet<Object> elements;

  private ImmutableNullableSet(ImmutableSet<Object> elements) {
    this.elements = requireNonNull(elements, "elements");
  }

  @Override public Iterator<E> iterator() {
    return Util.transform(elements.iterator(), e ->
        e == NullSentinel.INSTANCE ? castNonNull(null) : (E) e);
  }

  @Override public int size() {
    return elements.size();
  }

  @Override public boolean contains(@Nullable Object o) {
    return elements.contains(o == null ? NullSentinel.INSTANCE : o);
  }

  @Override public boolean remove(@Nullable Object o) {
    throw new UnsupportedOperationException();
  }

  @Override public boolean removeAll(Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  /**
   * Returns an immutable set containing the given elements.
   *
   * <p>Behavior is as {@link ImmutableSet#copyOf(Iterable)}
   * except that this set allows nulls.
   */
  @SuppressWarnings({"unchecked", "StaticPseudoFunctionalStyleMethod"})
  public static <E> Set<E> copyOf(Iterable<? extends E> elements) {
    if (elements instanceof ImmutableNullableSet
        || elements instanceof ImmutableSet
        || elements == Collections.emptySet()
        || elements == Collections.emptySortedSet()
        || elements == SINGLETON_NULL
        || elements.getClass() == SINGLETON.getClass()) {
      return (Set<E>) elements;
    }
    final ImmutableSet<Object> set;
    if (elements instanceof Collection) {
      final Collection<E> collection = (Collection<E>) elements;
      switch (collection.size()) {
      case 0:
        return ImmutableSet.of();
      case 1:
        E element = Iterables.getOnlyElement(collection);
        return element == null ? SINGLETON_NULL : ImmutableSet.of(element);
      default:
        set =
            ImmutableSet.copyOf(
                Collections2.transform(collection, e ->
                    e == null ? NullSentinel.INSTANCE : e));
      }
    } else {
      set =
          ImmutableSet.copyOf(
              Util.transform(elements, e ->
                  e == null ? NullSentinel.INSTANCE : e));
    }
    if (set.contains(NullSentinel.INSTANCE)) {
      return new ImmutableNullableSet<>(set);
    } else {
      return (Set<E>) set;
    }
  }

  /**
   * Returns an immutable set containing the given elements.
   *
   * <p>Behavior as
   * {@link ImmutableSet#copyOf(Object[])}
   * except that this set allows nulls.
   */
  public static <E> Set<E> copyOf(E[] elements) {
    return copyOf(elements, true);
  }

  private static <E> Set<E> copyOf(E[] elements, boolean needCopy) {
    // If there are no nulls, ImmutableSet is better.
    if (!containsNull(elements)) {
      return ImmutableSet.copyOf(elements);
    }

    final @Nullable Object[] objects =
        needCopy ? Arrays.copyOf(elements, elements.length, Object[].class)
            : elements;
    for (int i = 0; i < objects.length; i++) {
      if (objects[i] == null) {
        objects[i] = NullSentinel.INSTANCE;
      }
    }
    @SuppressWarnings({"nullness", "NullableProblems"})
    @NonNull Object[] nonNullObjects = objects;
    return new ImmutableNullableSet<E>(ImmutableSet.copyOf(nonNullObjects));
  }

  private static <E> boolean containsNull(E[] elements) {
    for (E element : elements) {
      if (element == null) {
        return true;
      }
    }
    return false;
  }

  /** Creates an immutable set of 1 element. */
  public static <E> Set<E> of(E e1) {
    //noinspection unchecked
    return e1 == null ? (Set<E>) SINGLETON_NULL : ImmutableSet.of(e1);
  }

  /** Creates an immutable set of 2 elements. */
  @SuppressWarnings("unchecked")
  public static <E> Set<E> of(E e1, E e2) {
    return copyOf((E []) new Object[] {e1, e2}, false);
  }

  /** Creates an immutable set of 3 elements. */
  @SuppressWarnings("unchecked")
  public static <E> Set<E> of(E e1, E e2, E e3) {
    return copyOf((E []) new Object[] {e1, e2, e3}, false);
  }

  /** Creates an immutable set of 4 elements. */
  @SuppressWarnings("unchecked")
  public static <E> Set<E> of(E e1, E e2, E e3, E e4) {
    return copyOf((E []) new Object[] {e1, e2, e3, e4}, false);
  }

  /** Creates an immutable set of 5 or more elements. */
  @SuppressWarnings("unchecked")
  public static <E> Set<E> of(E e1, E e2, E e3, E e4, E e5, E... others) {
    E[] elements = (E[]) new Object[5 + others.length];
    elements[0] = e1;
    elements[1] = e2;
    elements[2] = e3;
    elements[3] = e4;
    elements[4] = e5;
    System.arraycopy(others, 0, elements, 5, others.length);
    return copyOf(elements, false);
  }

  /**
   * Returns a new builder. The generated builder is equivalent to the builder
   * created by the {@link Builder} constructor.
   */
  public static <E> Builder<E> builder() {
    return new Builder<>();
  }

  /**
   * A builder for creating immutable nullable set instances.
   *
   * @param <E> element type
   */
  public static final class Builder<E> {
    private final List<E> contents = new ArrayList<>();

    /**
     * Creates a new builder. The returned builder is equivalent to the builder
     * generated by
     * {@link ImmutableNullableSet#builder}.
     */
    public Builder() {}

    /**
     * Adds {@code element} to the {@code ImmutableNullableSet}.
     *
     * @param element the element to add
     * @return this {@code Builder} object
     */
    public Builder<E> add(E element) {
      contents.add(element);
      return this;
    }

    /**
     * Adds each element of {@code elements} to the
     * {@code ImmutableNullableSet}.
     *
     * @param elements the {@code Iterable} to add to the
     *     {@code ImmutableNullableSet}
     * @return this {@code Builder} object
     * @throws NullPointerException if {@code elements} is null
     */
    public Builder<E> addAll(Iterable<? extends E> elements) {
      Iterables.addAll(contents, elements);
      return this;
    }

    /**
     * Adds each element of {@code elements} to the
     * {@code ImmutableNullableSet}.
     *
     * @param elements the elements to add to the {@code ImmutableNullableSet}
     * @return this {@code Builder} object
     * @throws NullPointerException if {@code elements} is null
     */
    public Builder<E> add(E... elements) {
      for (E element : elements) {
        add(element);
      }
      return this;
    }

    /**
     * Adds each element of {@code elements} to the
     * {@code ImmutableNullableSet}.
     *
     * @param elements the elements to add to the {@code ImmutableNullableSet}
     * @return this {@code Builder} object
     * @throws NullPointerException if {@code elements} is null
     */
    public Builder<E> addAll(Iterator<? extends E> elements) {
      Iterators.addAll(contents, elements);
      return this;
    }

    /**
     * Returns a newly-created {@code ImmutableNullableSet} based on the
     * contents of the {@code Builder}.
     */
    public Set<E> build() {
      return copyOf(contents);
    }
  }
}
