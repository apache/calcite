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

import org.apache.calcite.util.ImmutableNullableList;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.RandomAccess;

/**
 * Space-efficient, comparable, immutable lists.
 */
public class FlatLists {
  private FlatLists() {
  }

  public static final ComparableEmptyList COMPARABLE_EMPTY_LIST =
      new ComparableEmptyList();

  /** Creates a flat list with 0 elements. */
  public static <T> ComparableList<T> of() {
    //noinspection unchecked
    return COMPARABLE_EMPTY_LIST;
  }

  /** Creates a flat list with 1 element. */
  public static <T> List<T> of(T t0) {
    return new Flat1List<>(t0);
  }

  /** Creates a flat list with 2 elements. */
  public static <T> List<T> of(T t0, T t1) {
    return new Flat2List<>(t0, t1);
  }

  /** Creates a flat list with 3 elements. */
  public static <T> List<T> of(T t0, T t1, T t2) {
    return new Flat3List<>(t0, t1, t2);
  }

  /** Creates a flat list with 4 elements. */
  public static <T> List<T> of(T t0, T t1, T t2, T t3) {
    return new Flat4List<>(t0, t1, t2, t3);
  }

  /** Creates a flat list with 6 elements. */
  public static <T> List<T> of(T t0, T t1, T t2, T t3, T t4) {
    return new Flat5List<>(t0, t1, t2, t3, t4);
  }

  /** Creates a flat list with 6 elements. */
  public static <T> List<T> of(T t0, T t1, T t2, T t3, T t4, T t5) {
    return new Flat6List<>(t0, t1, t2, t3, t4, t5);
  }

  /**
   * Creates a memory-, CPU- and cache-efficient immutable list.
   *
   * @param t Array of members of list
   * @param <T> Element type
   * @return List containing the given members
   */
  public static <T extends Comparable> List<T> of(T... t) {
    return flatList_(t, false);
  }

  /**
   * Creates a memory-, CPU- and cache-efficient immutable list,
   * always copying the contents.
   *
   * @param t Array of members of list
   * @param <T> Element type
   * @return List containing the given members
   */
  @Deprecated // to be removed before 2.0
  public static <T> List<T> copy(T... t) {
    return flatListNotComparable(t);
  }

  /**
   * Creates a memory-, CPU- and cache-efficient comparable immutable list,
   * always copying the contents.
   *
   * <p>The elements are comparable, and so is the returned list.
   * Elements may be null.
   *
   * @param t Array of members of list
   * @param <T> Element type
   * @return List containing the given members
   */
  public static <T extends Comparable> List<T> copyOf(T... t) {
    return flatList_(t, true);
  }

  /**
   * Creates a memory-, CPU- and cache-efficient immutable list,
   * always copying the contents.
   *
   * <p>The elements need not be comparable,
   * and the returned list may not implement {@link Comparable}.
   * Elements may be null.
   *
   * @param t Array of members of list
   * @param <T> Element type
   * @return List containing the given members
   */
  public static <T> List<T> copyOf(T... t) {
    return flatListNotComparable(t);
  }

  /**
   * Creates a memory-, CPU- and cache-efficient comparable immutable list,
   * optionally copying the list.
   *
   * @param copy Whether to always copy the list
   * @param t Array of members of list
   * @return List containing the given members
   */
  private static <T extends Comparable> ComparableList<T> flatList_(
      T[] t, boolean copy) {
    switch (t.length) {
    case 0:
      //noinspection unchecked
      return COMPARABLE_EMPTY_LIST;
    case 1:
      return new Flat1List<>(t[0]);
    case 2:
      return new Flat2List<>(t[0], t[1]);
    case 3:
      return new Flat3List<>(t[0], t[1], t[2]);
    case 4:
      return new Flat4List<>(t[0], t[1], t[2], t[3]);
    case 5:
      return new Flat5List<>(t[0], t[1], t[2], t[3], t[4]);
    case 6:
      return new Flat6List<>(t[0], t[1], t[2], t[3], t[4], t[5]);
    default:
      // REVIEW: AbstractList contains a modCount field; we could
      //   write our own implementation and reduce creation overhead a
      //   bit.
      if (copy) {
        return new ComparableListImpl<>(Arrays.asList(t.clone()));
      } else {
        return new ComparableListImpl<>(Arrays.asList(t));
      }
    }
  }

  /**
   * Creates a memory-, CPU- and cache-efficient immutable list,
   * always copying the list.
   *
   * @param t Array of members of list
   * @return List containing the given members
   */
  private static <T> List<T> flatListNotComparable(T[] t) {
    switch (t.length) {
    case 0:
      //noinspection unchecked
      return COMPARABLE_EMPTY_LIST;
    case 1:
      return new Flat1List<>(t[0]);
    case 2:
      return new Flat2List<>(t[0], t[1]);
    case 3:
      return new Flat3List<>(t[0], t[1], t[2]);
    case 4:
      return new Flat4List<>(t[0], t[1], t[2], t[3]);
    case 5:
      return new Flat5List<>(t[0], t[1], t[2], t[3], t[4]);
    case 6:
      return new Flat6List<>(t[0], t[1], t[2], t[3], t[4], t[5]);
    default:
      return ImmutableNullableList.copyOf(t);
    }
  }

  /**
   * Creates a memory-, CPU- and cache-efficient immutable list from an
   * existing list. The list is always copied.
   *
   * @param t Array of members of list
   * @param <T> Element type
   * @return List containing the given members
   */
  public static <T> List<T> of(List<T> t) {
    return of_(t);
  }

  public static <T extends Comparable> ComparableList<T> ofComparable(
      List<T> t) {
    return of_(t);
  }

  private static <T> ComparableList<T> of_(List<T> t) {
    switch (t.size()) {
    case 0:
      //noinspection unchecked
      return COMPARABLE_EMPTY_LIST;
    case 1:
      return new Flat1List<>(t.get(0));
    case 2:
      return new Flat2List<>(t.get(0), t.get(1));
    case 3:
      return new Flat3List<>(t.get(0), t.get(1), t.get(2));
    case 4:
      return new Flat4List<>(t.get(0), t.get(1), t.get(2), t.get(3));
    case 5:
      return new Flat5List<>(t.get(0), t.get(1), t.get(2), t.get(3), t.get(4));
    case 6:
      return new Flat6List<>(t.get(0), t.get(1), t.get(2), t.get(3), t.get(4),
          t.get(5));
    default:
      // REVIEW: AbstractList contains a modCount field; we could
      //   write our own implementation and reduce creation overhead a
      //   bit.
      //noinspection unchecked
      return new ComparableListImpl(Arrays.asList(t.toArray()));
    }
  }

  /** Returns a list that consists of a given list plus an element. */
  public static <E> List<E> append(List<E> list, E e) {
    if (list instanceof AbstractFlatList) {
      //noinspection unchecked
      return ((AbstractFlatList) list).append(e);
    }
    final List<E> newList = new ArrayList<>(list);
    newList.add(e);
    return FlatLists.of(newList);
  }

  /** Returns a list that consists of a given list plus an element, guaranteed
   * to be an {@link ImmutableList}. */
  public static <E> ImmutableList<E> append(ImmutableList<E> list, E e) {
    return ImmutableList.<E>builder().addAll(list).add(e).build();
  }

  /** Returns a map that consists of a given map plus an (key, value),
   * guaranteed to be an {@link ImmutableMap}. */
  public static <K, V> ImmutableMap<K, V> append(Map<K, V> map, K k, V v) {
    final ImmutableMap.Builder<K, V> builder = ImmutableMap.builder();
    builder.put(k, v);
    map.forEach((k2, v2) -> {
      if (!k.equals(k2)) {
        builder.put(k2, v2);
      }
    });
    return builder.build();
  }

  /** Base class for flat lists.
   *
   * @param <T> element type */
  public abstract static class AbstractFlatList<T>
      extends AbstractImmutableList<T> implements RandomAccess {
    protected final List<T> toList() {
      //noinspection unchecked
      return Arrays.asList((T[]) toArray());
    }

    /** Returns a list that consists of a this list's elements plus a given
     * element. */
    public abstract List<T> append(T e);
  }

  /**
   * List that stores its one elements in the one members of the class.
   * Unlike {@link java.util.ArrayList} or
   * {@link java.util.Arrays#asList(Object[])} there is
   * no array, only one piece of memory allocated, therefore is very compact
   * and cache and CPU efficient.
   *
   * <p>The list is read-only and cannot be modified or re-sized.
   * The element may be null.
   *
   * <p>The list is created via {@link FlatLists#of}.
   *
   * @param <T> Element type
   */
  protected static class Flat1List<T>
      extends AbstractFlatList<T>
      implements ComparableList<T> {
    private final T t0;

    Flat1List(T t0) {
      this.t0 = t0;
    }

    public String toString() {
      return "[" + t0 + "]";
    }

    public T get(int index) {
      switch (index) {
      case 0:
        return t0;
      default:
        throw new IndexOutOfBoundsException("index " + index);
      }
    }

    public int size() {
      return 1;
    }

    public Iterator<T> iterator() {
      return Collections.singletonList(t0).iterator();
    }

    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o instanceof Flat1List) {
        Flat1List that = (Flat1List) o;
        return Objects.equals(this.t0, that.t0);
      }
      return o instanceof List
          && ((List) o).size() == 1
          && Objects.equals(t0, ((List) o).get(0));
    }

    public int hashCode() {
      int h = 1;
      h = h * 31 + Utilities.hash(t0);
      return h;
    }

    public int indexOf(Object o) {
      if (o == null) {
        if (t0 == null) {
          return 0;
        }
      } else {
        if (o.equals(t0)) {
          return 0;
        }
      }
      return -1;
    }

    public int lastIndexOf(Object o) {
      if (o == null) {
        if (t0 == null) {
          return 0;
        }
      } else {
        if (o.equals(t0)) {
          return 0;
        }
      }
      return -1;
    }

    @SuppressWarnings({"unchecked" })
    public <T2> T2[] toArray(T2[] a) {
      if (a.length < 1) {
        // Make a new array of a's runtime type, but my contents:
        return (T2[]) Arrays.copyOf(toArray(), 1, a.getClass());
      }
      a[0] = (T2) t0;
      return a;
    }

    public Object[] toArray() {
      return new Object[] {t0};
    }

    public int compareTo(List o) {
      return ComparableListImpl.compare((List) this, o);
    }

    public List<T> append(T e) {
      return new Flat2List<>(t0, e);
    }
  }

  /**
   * List that stores its two elements in the two members of the class.
   * Unlike {@link java.util.ArrayList} or
   * {@link java.util.Arrays#asList(Object[])} there is
   * no array, only one piece of memory allocated, therefore is very compact
   * and cache and CPU efficient.
   *
   * <p>The list is read-only and cannot be modified or re-sized.
   * The elements may be null.
   *
   * <p>The list is created via {@link FlatLists#of}.
   *
   * @param <T> Element type
   */
  protected static class Flat2List<T>
      extends AbstractFlatList<T>
      implements ComparableList<T> {
    private final T t0;
    private final T t1;

    Flat2List(T t0, T t1) {
      this.t0 = t0;
      this.t1 = t1;
    }

    public String toString() {
      return "[" + t0 + ", " + t1 + "]";
    }

    public T get(int index) {
      switch (index) {
      case 0:
        return t0;
      case 1:
        return t1;
      default:
        throw new IndexOutOfBoundsException("index " + index);
      }
    }

    public int size() {
      return 2;
    }

    public Iterator<T> iterator() {
      return Arrays.asList(t0, t1).iterator();
    }

    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o instanceof Flat2List) {
        Flat2List that = (Flat2List) o;
        return Objects.equals(this.t0, that.t0)
            && Objects.equals(this.t1, that.t1);
      }
      if (o instanceof List) {
        List lo = (List) o;
        return lo.size() == 2 && o.equals(this);
      }
      return false;
    }

    public int hashCode() {
      int h = 1;
      h = h * 31 + Utilities.hash(t0);
      h = h * 31 + Utilities.hash(t1);
      return h;
    }

    public int indexOf(Object o) {
      if (o == null) {
        if (t0 == null) {
          return 0;
        }
        if (t1 == null) {
          return 1;
        }
      } else {
        if (o.equals(t0)) {
          return 0;
        }
        if (o.equals(t1)) {
          return 1;
        }
      }
      return -1;
    }

    public int lastIndexOf(Object o) {
      if (o == null) {
        if (t1 == null) {
          return 1;
        }
        if (t0 == null) {
          return 0;
        }
      } else {
        if (o.equals(t1)) {
          return 1;
        }
        if (o.equals(t0)) {
          return 0;
        }
      }
      return -1;
    }

    @SuppressWarnings({"unchecked" })
    public <T2> T2[] toArray(T2[] a) {
      if (a.length < 2) {
        // Make a new array of a's runtime type, but my contents:
        return (T2[]) Arrays.copyOf(toArray(), 2, a.getClass());
      }
      a[0] = (T2) t0;
      a[1] = (T2) t1;
      return a;
    }

    public Object[] toArray() {
      return new Object[] {t0, t1};
    }

    public int compareTo(List o) {
      return ComparableListImpl.compare((List) this, o);
    }

    public List<T> append(T e) {
      return new Flat3List<>(t0, t1, e);
    }
  }

  /**
   * List that stores its three elements in the three members of the class.
   * Unlike {@link java.util.ArrayList} or
   * {@link java.util.Arrays#asList(Object[])} there is
   * no array, only one piece of memory allocated, therefore is very compact
   * and cache and CPU efficient.
   *
   * <p>The list is read-only, cannot be modified or re-sized.
   * The elements may be null.
   *
   * <p>The list is created via {@link FlatLists#of(java.util.List)}.
   *
   * @param <T> Element type
   */
  protected static class Flat3List<T>
      extends AbstractFlatList<T>
      implements ComparableList<T> {
    private final T t0;
    private final T t1;
    private final T t2;

    Flat3List(T t0, T t1, T t2) {
      this.t0 = t0;
      this.t1 = t1;
      this.t2 = t2;
    }

    public String toString() {
      return "[" + t0 + ", " + t1 + ", " + t2 + "]";
    }

    public T get(int index) {
      switch (index) {
      case 0:
        return t0;
      case 1:
        return t1;
      case 2:
        return t2;
      default:
        throw new IndexOutOfBoundsException("index " + index);
      }
    }

    public int size() {
      return 3;
    }

    public Iterator<T> iterator() {
      return Arrays.asList(t0, t1, t2).iterator();
    }

    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o instanceof Flat3List) {
        Flat3List that = (Flat3List) o;
        return Objects.equals(this.t0, that.t0)
            && Objects.equals(this.t1, that.t1)
            && Objects.equals(this.t2, that.t2);
      }
      return o instanceof List
          && ((List) o).size() == 3
          && Arrays.asList(t0, t1, t2).equals(o);
    }

    public int hashCode() {
      int h = 1;
      h = h * 31 + Utilities.hash(t0);
      h = h * 31 + Utilities.hash(t1);
      h = h * 31 + Utilities.hash(t2);
      return h;
    }

    public int indexOf(Object o) {
      if (o == null) {
        if (t0 == null) {
          return 0;
        }
        if (t1 == null) {
          return 1;
        }
        if (t2 == null) {
          return 2;
        }
      } else {
        if (o.equals(t0)) {
          return 0;
        }
        if (o.equals(t1)) {
          return 1;
        }
        if (o.equals(t2)) {
          return 2;
        }
      }
      return -1;
    }

    public int lastIndexOf(Object o) {
      if (o == null) {
        if (t2 == null) {
          return 2;
        }
        if (t1 == null) {
          return 1;
        }
        if (t0 == null) {
          return 0;
        }
      } else {
        if (o.equals(t2)) {
          return 2;
        }
        if (o.equals(t1)) {
          return 1;
        }
        if (o.equals(t0)) {
          return 0;
        }
      }
      return -1;
    }

    @SuppressWarnings({"unchecked" })
    public <T2> T2[] toArray(T2[] a) {
      if (a.length < 3) {
        // Make a new array of a's runtime type, but my contents:
        return (T2[]) Arrays.copyOf(toArray(), 3, a.getClass());
      }
      a[0] = (T2) t0;
      a[1] = (T2) t1;
      a[2] = (T2) t2;
      return a;
    }

    public Object[] toArray() {
      return new Object[] {t0, t1, t2};
    }

    public int compareTo(List o) {
      return ComparableListImpl.compare((List) this, o);
    }

    public List<T> append(T e) {
      return new Flat4List<>(t0, t1, t2, e);
    }
  }

  /**
   * List that stores its four elements in the four members of the class.
   * Unlike {@link java.util.ArrayList} or
   * {@link java.util.Arrays#asList(Object[])} there is
   * no array, only one piece of memory allocated, therefore is very compact
   * and cache and CPU efficient.
   *
   * <p>The list is read-only, cannot be modified or re-sized.
   * The elements may be null.
   *
   * <p>The list is created via {@link FlatLists#of(java.util.List)}.
   *
   * @param <T> Element type
   */
  protected static class Flat4List<T>
      extends AbstractFlatList<T>
      implements ComparableList<T> {
    private final T t0;
    private final T t1;
    private final T t2;
    private final T t3;

    Flat4List(T t0, T t1, T t2, T t3) {
      this.t0 = t0;
      this.t1 = t1;
      this.t2 = t2;
      this.t3 = t3;
    }

    public String toString() {
      return "[" + t0 + ", " + t1 + ", " + t2 + ", " + t3 + "]";
    }

    public T get(int index) {
      switch (index) {
      case 0:
        return t0;
      case 1:
        return t1;
      case 2:
        return t2;
      case 3:
        return t3;
      default:
        throw new IndexOutOfBoundsException("index " + index);
      }
    }

    public int size() {
      return 4;
    }

    public Iterator<T> iterator() {
      return Arrays.asList(t0, t1, t2, t3).iterator();
    }

    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o instanceof Flat4List) {
        Flat4List that = (Flat4List) o;
        return Objects.equals(this.t0, that.t0)
            && Objects.equals(this.t1, that.t1)
            && Objects.equals(this.t2, that.t2)
            && Objects.equals(this.t3, that.t3);
      }
      return o instanceof List
          && ((List) o).size() == 4
          && Arrays.asList(t0, t1, t2, t3).equals(o);
    }

    public int hashCode() {
      int h = 1;
      h = h * 31 + Utilities.hash(t0);
      h = h * 31 + Utilities.hash(t1);
      h = h * 31 + Utilities.hash(t2);
      h = h * 31 + Utilities.hash(t3);
      return h;
    }

    public int indexOf(Object o) {
      if (o == null) {
        if (t0 == null) {
          return 0;
        }
        if (t1 == null) {
          return 1;
        }
        if (t2 == null) {
          return 2;
        }
        if (t3 == null) {
          return 3;
        }
      } else {
        if (o.equals(t0)) {
          return 0;
        }
        if (o.equals(t1)) {
          return 1;
        }
        if (o.equals(t2)) {
          return 2;
        }
        if (o.equals(t3)) {
          return 3;
        }
      }
      return -1;
    }

    public int lastIndexOf(Object o) {
      if (o == null) {
        if (t3 == null) {
          return 3;
        }
        if (t2 == null) {
          return 2;
        }
        if (t1 == null) {
          return 1;
        }
        if (t0 == null) {
          return 0;
        }
      } else {
        if (o.equals(t3)) {
          return 3;
        }
        if (o.equals(t2)) {
          return 2;
        }
        if (o.equals(t1)) {
          return 1;
        }
        if (o.equals(t0)) {
          return 0;
        }
      }
      return -1;
    }

    @SuppressWarnings({"unchecked" })
    public <T2> T2[] toArray(T2[] a) {
      if (a.length < 4) {
        // Make a new array of a's runtime type, but my contents:
        return (T2[]) Arrays.copyOf(toArray(), 4, a.getClass());
      }
      a[0] = (T2) t0;
      a[1] = (T2) t1;
      a[2] = (T2) t2;
      a[3] = (T2) t3;
      return a;
    }

    public Object[] toArray() {
      return new Object[] {t0, t1, t2, t3};
    }

    public int compareTo(List o) {
      return ComparableListImpl.compare((List) this, o);
    }

    public List<T> append(T e) {
      return new Flat5List<>(t0, t1, t2, t3, e);
    }
  }

  /**
   * List that stores its five elements in the five members of the class.
   * Unlike {@link java.util.ArrayList} or
   * {@link java.util.Arrays#asList(Object[])} there is
   * no array, only one piece of memory allocated, therefore is very compact
   * and cache and CPU efficient.
   *
   * <p>The list is read-only, cannot be modified or re-sized.
   * The elements may be null.
   *
   * <p>The list is created via {@link FlatLists#of(java.util.List)}.
   *
   * @param <T> Element type
   */
  protected static class Flat5List<T>
      extends AbstractFlatList<T>
      implements ComparableList<T> {
    private final T t0;
    private final T t1;
    private final T t2;
    private final T t3;
    private final T t4;

    Flat5List(T t0, T t1, T t2, T t3, T t4) {
      this.t0 = t0;
      this.t1 = t1;
      this.t2 = t2;
      this.t3 = t3;
      this.t4 = t4;
    }

    public String toString() {
      return "[" + t0 + ", " + t1 + ", " + t2 + ", " + t3 + ", " + t4 + "]";
    }

    public T get(int index) {
      switch (index) {
      case 0:
        return t0;
      case 1:
        return t1;
      case 2:
        return t2;
      case 3:
        return t3;
      case 4:
        return t4;
      default:
        throw new IndexOutOfBoundsException("index " + index);
      }
    }

    public int size() {
      return 5;
    }

    public Iterator<T> iterator() {
      return Arrays.asList(t0, t1, t2, t3, t4).iterator();
    }

    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o instanceof Flat5List) {
        Flat5List that = (Flat5List) o;
        return Objects.equals(this.t0, that.t0)
            && Objects.equals(this.t1, that.t1)
            && Objects.equals(this.t2, that.t2)
            && Objects.equals(this.t3, that.t3)
            && Objects.equals(this.t4, that.t4);
      }
      return o instanceof List
          && ((List) o).size() == 5
          && Arrays.asList(t0, t1, t2, t3, t4).equals(o);
    }

    public int hashCode() {
      int h = 1;
      h = h * 31 + Utilities.hash(t0);
      h = h * 31 + Utilities.hash(t1);
      h = h * 31 + Utilities.hash(t2);
      h = h * 31 + Utilities.hash(t3);
      h = h * 31 + Utilities.hash(t4);
      return h;
    }

    public int indexOf(Object o) {
      if (o == null) {
        if (t0 == null) {
          return 0;
        }
        if (t1 == null) {
          return 1;
        }
        if (t2 == null) {
          return 2;
        }
        if (t3 == null) {
          return 3;
        }
        if (t4 == null) {
          return 4;
        }
      } else {
        if (o.equals(t0)) {
          return 0;
        }
        if (o.equals(t1)) {
          return 1;
        }
        if (o.equals(t2)) {
          return 2;
        }
        if (o.equals(t3)) {
          return 3;
        }
        if (o.equals(t4)) {
          return 4;
        }
      }
      return -1;
    }

    public int lastIndexOf(Object o) {
      if (o == null) {
        if (t4 == null) {
          return 4;
        }
        if (t3 == null) {
          return 3;
        }
        if (t2 == null) {
          return 2;
        }
        if (t1 == null) {
          return 1;
        }
        if (t0 == null) {
          return 0;
        }
      } else {
        if (o.equals(t4)) {
          return 4;
        }
        if (o.equals(t3)) {
          return 3;
        }
        if (o.equals(t2)) {
          return 2;
        }
        if (o.equals(t1)) {
          return 1;
        }
        if (o.equals(t0)) {
          return 0;
        }
      }
      return -1;
    }

    @SuppressWarnings({"unchecked" })
    public <T2> T2[] toArray(T2[] a) {
      if (a.length < 5) {
        // Make a new array of a's runtime type, but my contents:
        return (T2[]) Arrays.copyOf(toArray(), 5, a.getClass());
      }
      a[0] = (T2) t0;
      a[1] = (T2) t1;
      a[2] = (T2) t2;
      a[3] = (T2) t3;
      a[4] = (T2) t4;
      return a;
    }

    public Object[] toArray() {
      return new Object[] {t0, t1, t2, t3, t4};
    }

    public int compareTo(List o) {
      return ComparableListImpl.compare((List) this, o);
    }

    public List<T> append(T e) {
      return new Flat6List<>(t0, t1, t2, t3, t4, e);
    }
  }

  /**
   * List that stores its six elements in the six members of the class.
   * Unlike {@link java.util.ArrayList} or
   * {@link java.util.Arrays#asList(Object[])} there is
   * no array, only one piece of memory allocated, therefore is very compact
   * and cache and CPU efficient.
   *
   * <p>The list is read-only, cannot be modified or re-sized.
   * The elements may be null.
   *
   * <p>The list is created via {@link FlatLists#of(java.util.List)}.
   *
   * @param <T> Element type
   */
  protected static class Flat6List<T>
      extends AbstractFlatList<T>
      implements ComparableList<T> {
    private final T t0;
    private final T t1;
    private final T t2;
    private final T t3;
    private final T t4;
    private final T t5;

    Flat6List(T t0, T t1, T t2, T t3, T t4, T t5) {
      this.t0 = t0;
      this.t1 = t1;
      this.t2 = t2;
      this.t3 = t3;
      this.t4 = t4;
      this.t5 = t5;
    }

    public String toString() {
      return "[" + t0 + ", " + t1 + ", " + t2 + ", " + t3 + ", " + t4
          + ", " + t5 + "]";
    }

    public T get(int index) {
      switch (index) {
      case 0:
        return t0;
      case 1:
        return t1;
      case 2:
        return t2;
      case 3:
        return t3;
      case 4:
        return t4;
      case 5:
        return t5;
      default:
        throw new IndexOutOfBoundsException("index " + index);
      }
    }

    public int size() {
      return 6;
    }

    public Iterator<T> iterator() {
      return Arrays.asList(t0, t1, t2, t3, t4, t5).iterator();
    }

    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o instanceof Flat6List) {
        Flat6List that = (Flat6List) o;
        return Objects.equals(this.t0, that.t0)
            && Objects.equals(this.t1, that.t1)
            && Objects.equals(this.t2, that.t2)
            && Objects.equals(this.t3, that.t3)
            && Objects.equals(this.t4, that.t4)
            && Objects.equals(this.t5, that.t5);
      }
      return o instanceof List
          && ((List) o).size() == 6
          && Arrays.asList(t0, t1, t2, t3, t4, t5).equals(o);
    }

    public int hashCode() {
      int h = 1;
      h = h * 31 + Utilities.hash(t0);
      h = h * 31 + Utilities.hash(t1);
      h = h * 31 + Utilities.hash(t2);
      h = h * 31 + Utilities.hash(t3);
      h = h * 31 + Utilities.hash(t4);
      h = h * 31 + Utilities.hash(t5);
      return h;
    }

    public int indexOf(Object o) {
      if (o == null) {
        if (t0 == null) {
          return 0;
        }
        if (t1 == null) {
          return 1;
        }
        if (t2 == null) {
          return 2;
        }
        if (t3 == null) {
          return 3;
        }
        if (t4 == null) {
          return 4;
        }
        if (t5 == null) {
          return 5;
        }
      } else {
        if (o.equals(t0)) {
          return 0;
        }
        if (o.equals(t1)) {
          return 1;
        }
        if (o.equals(t2)) {
          return 2;
        }
        if (o.equals(t3)) {
          return 3;
        }
        if (o.equals(t4)) {
          return 4;
        }
        if (o.equals(t5)) {
          return 5;
        }
      }
      return -1;
    }

    public int lastIndexOf(Object o) {
      if (o == null) {
        if (t5 == null) {
          return 5;
        }
        if (t4 == null) {
          return 4;
        }
        if (t3 == null) {
          return 3;
        }
        if (t2 == null) {
          return 2;
        }
        if (t1 == null) {
          return 1;
        }
        if (t0 == null) {
          return 0;
        }
      } else {
        if (o.equals(t5)) {
          return 5;
        }
        if (o.equals(t4)) {
          return 4;
        }
        if (o.equals(t3)) {
          return 3;
        }
        if (o.equals(t2)) {
          return 2;
        }
        if (o.equals(t1)) {
          return 1;
        }
        if (o.equals(t0)) {
          return 0;
        }
      }
      return -1;
    }

    @SuppressWarnings({"unchecked" })
    public <T2> T2[] toArray(T2[] a) {
      if (a.length < 6) {
        // Make a new array of a's runtime type, but my contents:
        return (T2[]) Arrays.copyOf(toArray(), 6, a.getClass());
      }
      a[0] = (T2) t0;
      a[1] = (T2) t1;
      a[2] = (T2) t2;
      a[3] = (T2) t3;
      a[4] = (T2) t4;
      a[5] = (T2) t5;
      return a;
    }

    public Object[] toArray() {
      return new Object[] {t0, t1, t2, t3, t4, t5};
    }

    public int compareTo(List o) {
      return ComparableListImpl.compare((List) this, o);
    }

    public List<T> append(T e) {
      return ImmutableList.of(t0, t1, t2, t3, t5, e);
    }
  }

  /** Empty list that implements the {@link Comparable} interface.
   *
   * @param <T> element type */
  private static class ComparableEmptyList<T>
      extends AbstractList<T>
      implements ComparableList<T> {
    private ComparableEmptyList() {
    }

    public T get(int index) {
      throw new IndexOutOfBoundsException();
    }

    public int hashCode() {
      return 1; // same as Collections.emptyList()
    }

    public boolean equals(Object o) {
      return o == this
          || o instanceof List && ((List) o).isEmpty();
    }

    public int size() {
      return 0;
    }

    public int compareTo(List o) {
      return ComparableListImpl.compare((List) this, o);
    }
  }

  /** List that is also comparable.
   *
   * <p>You can create an instance whose type
   * parameter {@code T} does not extend {@link Comparable}, but you will get a
   * {@link ClassCastException} at runtime when you call
   * {@link #compareTo(Object)} if the elements of the list do not implement
   * {@code Comparable}.
   *
   * @param <T> element type
   */
  public interface ComparableList<T> extends List<T>, Comparable<List> {
  }

  /** Wrapper around a list that makes it implement the {@link Comparable}
   * interface using lexical ordering. The elements must be comparable.
   *
   * @param <T> element type */
  static class ComparableListImpl<T extends Comparable<T>>
      extends AbstractList<T>
      implements ComparableList<T> {
    private final List<T> list;

    protected ComparableListImpl(List<T> list) {
      this.list = list;
    }

    public T get(int index) {
      return list.get(index);
    }

    public int size() {
      return list.size();
    }

    public int compareTo(List o) {
      return compare(list, o);
    }

    static <T extends Comparable<T>> int compare(List<T> list0, List<T> list1) {
      final int size0 = list0.size();
      final int size1 = list1.size();
      if (size1 == size0) {
        return compare(list0, list1, size0);
      }
      final int c = compare(list0, list1, Math.min(size0, size1));
      if (c != 0) {
        return c;
      }
      return size0 - size1;
    }

    static <T extends Comparable<T>> int compare(List<T> list0, List<T> list1,
        int size) {
      for (int i = 0; i < size; i++) {
        Comparable o0 = list0.get(i);
        Comparable o1 = list1.get(i);
        int c = compare(o0, o1);
        if (c != 0) {
          return c;
        }
      }
      return 0;
    }

    static <T extends Comparable<T>> int compare(T a, T b) {
      if (a == b) {
        return 0;
      }
      if (a == null) {
        return -1;
      }
      if (b == null) {
        return 1;
      }
      return a.compareTo(b);
    }
  }

}

// End FlatLists.java
