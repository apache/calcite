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
package org.apache.calcite.runtime;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;

import static com.google.common.base.Preconditions.checkArgument;

import static org.apache.calcite.linq4j.Nullness.castNonNullList;

/** A list of pairs, stored as a quotient list.
 *
 * @param <T> First type
 * @param <U> Second type
 */
public interface PairList<T, U> extends List<Map.Entry<T, U>> {
  /** Creates an empty PairList. */
  static <T, U> PairList<T, U> of() {
    return new PairLists.MutablePairList<>(new ArrayList<>());
  }

  /** Creates a singleton PairList. */
  @SuppressWarnings("RedundantCast")
  static <T, U> PairList<T, U> of(T t, U u) {
    final List<@Nullable Object> list = new ArrayList<>();
    list.add((Object) t);
    list.add((Object) u);
    return new PairLists.MutablePairList<>(list);
  }

  /** Creates a PairList with one or more entries. */
  static <T, U> PairList<T, U> copyOf(T t, U u, Object... rest) {
    checkArgument(rest.length % 2 == 0, "even number");
    final List<@Nullable Object> list = Lists.asList(t, u, rest);
    return new PairLists.MutablePairList<>(new ArrayList<>(list));
  }

  /** Creates an empty PairList with a specified initial capacity. */
  static <T, U> PairList<T, U> withCapacity(int initialCapacity) {
    return backedBy(new ArrayList<>(initialCapacity));
  }

  /** Creates a PairList backed by a given list.
   *
   * <p>Changes to the backing list will be reflected in the PairList.
   * If the backing list is immutable, this PairList will be also. */
  static <T, U> PairList<T, U> backedBy(List<@Nullable Object> list) {
    return new PairLists.MutablePairList<>(list);
  }

  /** Creates a PairList from a Map. */
  @SuppressWarnings("RedundantCast")
  static <T, U> PairList<T, U> of(Map<T, U> map) {
    final List<@Nullable Object> list = new ArrayList<>(map.size() * 2);
    map.forEach((t, u) -> {
      list.add((Object) t);
      list.add((Object) u);
    });
    return new PairLists.MutablePairList<>(list);
  }

  /** Creates a Builder. */
  static <T, U> Builder<T, U> builder() {
    return new Builder<>();
  }

  /** Adds a pair to this list. */
  default void add(T t, U u) {
    throw new UnsupportedOperationException("add");
  }

  /** Adds a pair to this list at a given position. */
  default void add(int index, T t, U u) {
    throw new UnsupportedOperationException("add");
  }

  /** Adds to this list the contents of another PairList.
   *
   * <p>Equivalent to {@link #addAll(Collection)}, but more efficient. */
  default boolean addAll(PairList<T, U> list2) {
    throw new UnsupportedOperationException("addAll");
  }

  /** Adds to this list, at a given index, the contents of another PairList.
   *
   * <p>Equivalent to {@link #addAll(int, Collection)}, but more efficient. */
  default boolean addAll(int index, PairList<T, U> list2) {
    throw new UnsupportedOperationException("addAll");
  }

  /** Sets the entry at position {@code index} to the pair {@code (t, u)}. */
  default Map.Entry<T, U> set(int index, T t, U u) {
    throw new UnsupportedOperationException("set");
  }

  @Override default Map.Entry<T, U> remove(int index) {
    throw new UnsupportedOperationException("remove");
  }

  /** Returns the left part of the {@code index}th pair. */
  T left(int index);

  /** Returns the right part of the {@code index}th pair. */
  U right(int index);

  /** Returns an unmodifiable list view consisting of the left entry of each
   * pair. */
  List<T> leftList();

  /** Returns an unmodifiable list view consisting of the right entry of each
   * pair. */
  List<U> rightList();

  /** Calls a BiConsumer with each pair in this list. */
  void forEach(BiConsumer<T, U> consumer);

  /** Calls a BiConsumer with each pair in this list. */
  void forEachIndexed(IndexedBiConsumer<T, U> consumer);

  /** Creates an {@link ImmutableMap} whose entries are the pairs in this list.
   * Throws if keys are not unique. */
  default ImmutableMap<T, U> toImmutableMap() {
    final ImmutableMap.Builder<T, U> b = ImmutableMap.builder();
    forEach((t, u) -> b.put(t, u));
    return b.build();
  }

  /** Returns an ImmutablePairList whose contents are the same as this
   * PairList. */
  ImmutablePairList<T, U> immutable();

  /** Applies a mapping function to each element of this list. */
  <R> List<R> transform(BiFunction<T, U, R> function);

  /** Applies a mapping function to each element of this list. */
  <R> ImmutableList<R> transform2(BiFunction<T, U, R> function);

  @Override PairList<T, U> subList(int fromIndex, int toIndex);

  /** Returns whether the predicate is true for at least one pair
   * in this list. */
  boolean anyMatch(BiPredicate<T, U> predicate);

  /** Returns whether the predicate is true for all pairs
   * in this list. */
  boolean allMatch(BiPredicate<T, U> predicate);

  /** Returns whether the predicate is true for no pairs
   * in this list. */
  boolean noMatch(BiPredicate<T, U> predicate);

  /** Reverses the contents of this PairList.
   * Throws if this PairList is immutable.
   *
   * @see #reversed() */
  default void reverse() {
    throw new UnsupportedOperationException("reverse");
  }

  /** Returns an immutable copy of this ImmutablePairList with the elements
   * reversed.
   *
   * <p>Throws {@link NullPointerException} if any keys or values are null. */
  ImmutablePairList<T, U> reversed();

  /** Action to be taken each step of an indexed iteration over a PairList.
   *
   * @param <T> First type
   * @param <U> Second type
   *
   * @see PairList#forEachIndexed(IndexedBiConsumer)
   */
  interface IndexedBiConsumer<T, U> {
    /**
     * Performs this operation on the given arguments.
     *
     * @param index Index
     * @param t First input argument
     * @param u Second input argument
     */
    void accept(int index, T t, U u);
  }

  /** Builds a PairList.
   *
   * @param <T> First type
   * @param <U> Second type */
  class Builder<T, U> {
    final List<@Nullable Object> list = new ArrayList<>();

    /** Adds a pair to the list under construction. */
    @SuppressWarnings("RedundantCast")
    public Builder<T, U> add(T t, U u) {
      list.add((Object) t);
      list.add((Object) u);
      return this;
    }

    /** Builds the PairList. */
    public PairList<T, U> build() {
      return new PairLists.MutablePairList<>(list);
    }

    /** Builds an ImmutablePairList. */
    public ImmutablePairList<T, U> buildImmutable() {
      return PairLists.immutableBackedBy(castNonNullList(list));
    }
  }

  /** For code invoked via Janino, which cannot call static methods
   * in interfaces. */
  @Deprecated
  class Helper {
    private Helper() {}

    public static <T, U> PairList<T, U> copyOf(T t, U u, Object... rest) {
      return PairList.copyOf(t, u, rest);
    }
  }
}
