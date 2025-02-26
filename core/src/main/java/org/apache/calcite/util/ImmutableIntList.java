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

import org.apache.calcite.linq4j.function.Functions;
import org.apache.calcite.runtime.FlatLists;
import org.apache.calcite.util.mapping.Mappings;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.UnmodifiableListIterator;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.function.Consumer;
import java.util.function.IntConsumer;

import static org.apache.calcite.linq4j.Nullness.castNonNull;

import static java.util.Objects.requireNonNull;

/**
 * An immutable list of {@link Integer} values backed by an array of
 * {@code int}s.
 */
public class ImmutableIntList extends FlatLists.AbstractFlatList<Integer> {
  private final int[] ints;

  private static final Object[] EMPTY_ARRAY = new Object[0];

  private static final ImmutableIntList EMPTY = new EmptyImmutableIntList();

  // Does not copy array. Must remain private.
  private ImmutableIntList(int... ints) {
    this.ints = ints;
  }

  /**
   * Returns an empty ImmutableIntList.
   */
  public static ImmutableIntList of() {
    return EMPTY;
  }

  /**
   * Creates an ImmutableIntList from an array of {@code int}.
   */
  public static ImmutableIntList of(int... ints) {
    if (ints.length == 0) {
      return EMPTY;
    }
    return new ImmutableIntList(ints.clone());
  }

  /** Same as {@link #of(int...)}, but less ambiguous for code generators
   * and compilers. */
  public static ImmutableIntList copyOf(int... ints) {
    return of(ints);
  }

  /**
   * Creates an ImmutableIntList from an array of {@code Number}.
   */
  public static ImmutableIntList copyOf(Number... numbers) {
    if (numbers.length == 0) {
      return EMPTY;
    }
    final int[] ints = new int[numbers.length];
    for (int i = 0; i < ints.length; i++) {
      ints[i] = numbers[i].intValue();
    }
    return new ImmutableIntList(ints);
  }

  /**
   * Creates an ImmutableIntList from an iterable of {@link Number}.
   */
  public static ImmutableIntList copyOf(Iterable<? extends Number> list) {
    if (list instanceof ImmutableIntList) {
      return (ImmutableIntList) list;
    }
    @SuppressWarnings("unchecked")
    final Collection<? extends Number> collection =
        list instanceof Collection
            ? (Collection<? extends Number>) list
            : Lists.newArrayList(list);
    return copyFromCollection(collection);
  }

  /**
   * Creates an ImmutableIntList from an iterator of {@link Number}.
   */
  public static ImmutableIntList copyOf(Iterator<? extends Number> list) {
    return copyFromCollection(Lists.newArrayList(list));
  }

  private static ImmutableIntList copyFromCollection(
      Collection<? extends Number> list) {
    if (list.isEmpty()) {
      return EMPTY;
    }
    final int[] ints = new int[list.size()];
    int i = 0;
    for (Number number : list) {
      ints[i++] = number.intValue();
    }
    return new ImmutableIntList(ints);
  }

  @Override public int hashCode() {
    return Arrays.hashCode(ints);
  }

  @SuppressWarnings("contracts.conditional.postcondition.not.satisfied")
  @Override public boolean equals(@Nullable Object obj) {
    return ((this == obj)
        || (obj instanceof ImmutableIntList))
        ? Arrays.equals(ints, ((ImmutableIntList) obj).ints)
        : ((obj instanceof List)
            && obj.equals(this));
  }

  @Override public String toString() {
    return Arrays.toString(ints);
  }

  @Override public boolean isEmpty() {
    return ints.length == 0;
  }

  @Override public int size() {
    return ints.length;
  }

  @Override public void forEach(Consumer<? super Integer> action) {
    requireNonNull(action, "action");
    for (int i : ints) {
      action.accept(i);
    }
  }

  /** As {@link #forEach(Consumer)} but on primitive {@code int} values. */
  public void forEachInt(IntConsumer action) {
    requireNonNull(action, "action");
    for (int i : ints) {
      action.accept(i);
    }
  }

  @Override public Object[] toArray() {
    final Object[] objects = new Object[ints.length];
    for (int i = 0; i < objects.length; i++) {
      objects[i] = ints[i];
    }
    return objects;
  }

  @Override public <T> @Nullable T[] toArray(T @Nullable [] a) {
    final int size = ints.length;
    if (castNonNull(a).length < size) {
      // Make a new array of a's runtime type, but my contents:
      a = a.getClass() == Object[].class
          ? (T[]) new Object[size]
          : (T[]) Array.newInstance(
              requireNonNull(a.getClass().getComponentType()), size);
    }
    if ((Class) a.getClass() == Integer[].class) {
      final Integer[] integers = (Integer[]) a;
      for (int i = 0; i < size; i++) {
        integers[i] = ints[i];
      }
    } else {
      System.arraycopy(toArray(), 0, a, 0, size);
    }
    if (a.length > size) {
      a[size] = castNonNull(null);
    }
    return a;
  }

  /** Returns an array of {@code int}s with the same contents as this list. */
  public int[] toIntArray() {
    return ints.clone();
  }

  /** Returns a List of {@code Integer}. */
  public List<Integer> toIntegerList() {
    ArrayList<Integer> arrayList = new ArrayList<>(size());
    for (int i : ints) {
      arrayList.add(i);
    }
    return arrayList;
  }

  @Override public Integer get(int index) {
    return ints[index];
  }

  public int getInt(int index) {
    return ints[index];
  }

  @Override public Iterator<Integer> iterator() {
    return listIterator();
  }

  @Override public ListIterator<Integer> listIterator() {
    return listIterator(0);
  }

  @Override public ListIterator<Integer> listIterator(int index) {
    return new AbstractIndexedListIterator<Integer>(size(), index) {
      @Override protected Integer get(int index) {
        return ImmutableIntList.this.get(index);
      }
    };
  }

  @Override public int indexOf(@Nullable Object o) {
    if (o instanceof Integer) {
      return indexOf((int) (Integer) o);
    }
    return -1;
  }

  public int indexOf(int seek) {
    for (int i = 0; i < ints.length; i++) {
      if (ints[i] == seek) {
        return i;
      }
    }
    return -1;
  }

  @Override public int lastIndexOf(@Nullable Object o) {
    if (o instanceof Integer) {
      return lastIndexOf((int) (Integer) o);
    }
    return -1;
  }

  public int lastIndexOf(int seek) {
    for (int i = ints.length - 1; i >= 0; --i) {
      if (ints[i] == seek) {
        return i;
      }
    }
    return -1;
  }

  @Override public ImmutableIntList append(Integer e) {
    return append((int) e);
  }

  /** Returns a copy of this list with one element added. */
  public ImmutableIntList append(int element) {
    if (ints.length == 0) {
      return of(element);
    }
    final int[] newInts = Arrays.copyOf(this.ints, ints.length + 1);
    newInts[ints.length] = element;
    return new ImmutableIntList(newInts);
  }

  /** Returns a list that contains the values lower to upper - 1.
   *
   * <p>For example, {@code range(1, 3)} contains [1, 2]. */
  public static List<Integer> range(final int lower, final int upper) {
    return Functions.generate(upper - lower, index -> lower + index);
  }

  /** Returns the identity list [0, ..., count - 1].
   *
   * @see Mappings#isIdentity(List, int)
   */
  public static ImmutableIntList identity(int count) {
    if (count == 0) {
      return EMPTY;
    }
    final int[] integers = new int[count];
    for (int i = 0; i < integers.length; i++) {
      integers[i] = i;
    }
    return new ImmutableIntList(integers);
  }

  /** Returns a copy of this list with all of the given integers added. */
  public ImmutableIntList appendAll(Iterable<Integer> list) {
    if (list instanceof Collection && ((Collection) list).isEmpty()) {
      return this;
    }
    return ImmutableIntList.copyOf(Iterables.concat(this, list));
  }

  /**
   * Increments {@code offset} to each element of the list and
   * returns a new int list.
   */
  public ImmutableIntList incr(int offset) {
    final int[] integers = new int[ints.length];
    for (int i = 0; i < ints.length; i++) {
      integers[i] = ints[i] + offset;
    }
    return new ImmutableIntList(integers);
  }

  /** Special sub-class of {@link ImmutableIntList} that is always
   * empty and has only one instance. */
  private static class EmptyImmutableIntList extends ImmutableIntList {
    @Override public Object[] toArray() {
      return EMPTY_ARRAY;
    }

    @Override public <T> @Nullable T[] toArray(T @Nullable [] a) {
      if (castNonNull(a).length > 0) {
        a[0] = castNonNull(null);
      }
      return a;
    }

    @Override public Iterator<Integer> iterator() {
      return Collections.<Integer>emptyList().iterator();
    }

    @Override public ListIterator<Integer> listIterator() {
      return Collections.<Integer>emptyList().listIterator();
    }
  }

  /** Extension to {@link com.google.common.collect.UnmodifiableListIterator}
   * that operates by index.
   *
   * @param <E> element type */
  private abstract static class AbstractIndexedListIterator<E>
      extends UnmodifiableListIterator<E> {
    private final int size;
    private int position;

    protected abstract E get(int index);

    protected AbstractIndexedListIterator(int size, int position) {
      Preconditions.checkPositionIndex(position, size);
      this.size = size;
      this.position = position;
    }

    @Override public final boolean hasNext() {
      return position < size;
    }

    @Override public final E next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      return get(position++);
    }

    @Override public final int nextIndex() {
      return position;
    }

    @Override public final boolean hasPrevious() {
      return position > 0;
    }

    @Override public final E previous() {
      if (!hasPrevious()) {
        throw new NoSuchElementException();
      }
      return get(--position);
    }

    @Override public final int previousIndex() {
      return position - 1;
    }
  }
}
