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

import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import static java.util.Objects.requireNonNull;

/** A list of pairs, stored as a quotient list.
 *
 * @param <T> First type
 * @param <U> Second type
 */
public class PairList<T, U> extends AbstractList<Map.Entry<T, U>> {
  final List<@Nullable Object> list;

  private PairList(List<@Nullable Object> list) {
    this.list = list;
  }

  /** Creates an empty PairList. */
  public static <T, U> PairList<T, U> of() {
    return new PairList<>(new ArrayList<>());
  }

  /** Creates a PairList backed by a given list.
   *
   * <p>Changes to the backing list will be reflected in the PairList.
   * If the backing list is immutable, this PairList will be also. */
  public static <T, U> PairList<T, U> backedBy(List<@Nullable Object> list) {
    return new PairList<>(list);
  }

  /** Creates a PairList from a Map. */
  @SuppressWarnings("RedundantCast")
  public static <T, U> PairList<T, U> of(Map<T, U> map) {
    final List<@Nullable Object> list = new ArrayList<>(map.size() * 2);
    map.forEach((t, u) -> {
      list.add((Object) t);
      list.add((Object) u);
    });
    return new PairList<>(list);
  }

  @SuppressWarnings("unchecked")
  @Override public Map.Entry<T, U> get(int index) {
    int x = index * 2;
    return Pair.of((T) list.get(x), (U) list.get(x + 1));
  }

  @Override public int size() {
    return list.size() / 2;
  }

  @SuppressWarnings("RedundantCast")
  @Override public boolean add(Map.Entry<T, U> entry) {
    list.add((Object) entry.getKey());
    list.add((Object) entry.getValue());
    return true;
  }

  @SuppressWarnings("RedundantCast")
  @Override public void add(int index, Map.Entry<T, U> entry) {
    int x = index * 2;
    list.add(x, (Object) entry.getKey());
    list.add(x + 1, (Object) entry.getValue());
  }

  /** Adds a pair to this list. */
  @SuppressWarnings("RedundantCast")
  public void add(T t, U u) {
    list.add((Object) t);
    list.add((Object) u);
  }

  @SuppressWarnings("unchecked")
  @Override public Map.Entry<T, U> remove(int index) {
    final int x = index * 2;
    T t = (T) list.remove(x);
    U u = (U) list.remove(x);
    return Pair.of(t, u);
  }

  /** Returns an unmodifiable list view consisting of the left entry of each
   * pair. */
  @SuppressWarnings("unchecked")
  public List<T> leftList() {
    return Util.quotientList((List<T>) list, 2, 0);
  }

  /** Returns an unmodifiable list view consisting of the right entry of each
   * pair. */
  @SuppressWarnings("unchecked")
  public List<U> rightList() {
    return Util.quotientList((List<U>) list, 2, 1);
  }

  /** Calls a BiConsumer with each pair in this list. */
  @SuppressWarnings("unchecked")
  public void forEach(BiConsumer<T, U> consumer) {
    requireNonNull(consumer, "consumer");
    for (int i = 0; i < list.size();) {
      T t = (T) list.get(i++);
      U u = (U) list.get(i++);
      consumer.accept(t, u);
    }
  }

  /** Calls a BiConsumer with each pair in this list. */
  @SuppressWarnings("unchecked")
  public void forEachIndexed(IndexedBiConsumer<T, U> consumer) {
    requireNonNull(consumer, "consumer");
    for (int i = 0, j = 0; i < list.size();) {
      T t = (T) list.get(i++);
      U u = (U) list.get(i++);
      consumer.accept(j++, t, u);
    }
  }

  /** Creates an {@link ImmutableMap} whose entries are the pairs in this list.
   * Throws if keys are not unique. */
  public ImmutableMap<T, U> toImmutableMap() {
    final ImmutableMap.Builder<T, U> b = ImmutableMap.builder();
    forEach((t, u) -> b.put(t, u));
    return b.build();
  }

  /** Returns an immutable PairList whose contents are the same as this
   * PairList. */
  public PairList<T, U> immutable() {
    final List<@Nullable Object> immutableList = ImmutableList.copyOf(list);
    return backedBy(immutableList);
  }

  /** Action to be taken each step of an indexed iteration over a PairList.
   *
   * @param <T> First type
   * @param <U> Second type
   *
   * @see PairList#forEachIndexed(IndexedBiConsumer)
   */
  public interface IndexedBiConsumer<T, U> {
    /**
     * Performs this operation on the given arguments.
     *
     * @param index Index
     * @param t First input argument
     * @param u Second input argument
     */
    void accept(int index, T t, U u);
  }
}
