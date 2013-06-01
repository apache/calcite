/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package net.hydromatic.optiq.runtime;

import java.util.*;

/**
 * Space-efficient, comparable, immutable lists.
 */
public class FlatLists {
  public static final ComparableEmptyList COMPARABLE_EMPTY_LIST =
      new ComparableEmptyList();

  /** Creates a flat list with 2 elements. */
  public static <T> List of(T t0, T t1) {
    return new Flat2List<T>(t0, t1);
  }

  /** Creates a flat list with 3 elements. */
  public static <T> List of(T t0, T t1, T t2) {
    return new Flat3List<T>(t0, t1, t2);
  }

  /**
   * Creates a memory-, CPU- and cache-efficient immutable list.
   *
   * @param t Array of members of list
   * @param <T> Element type
   * @return List containing the given members
   */
  public static <T> List<T> of(T... t) {
    return _flatList(t, false);
  }

  /**
   * Creates a memory-, CPU- and cache-efficient immutable list,
   * always copying the contents.
   *
   * @param t Array of members of list
   * @param <T> Element type
   * @return List containing the given members
   */
  public static <T> List<T> copy(T... t) {
    return _flatList(t, true);
  }

  /**
   * Creates a memory-, CPU- and cache-efficient immutable list, optionally
   * copying the list.
   *
   * @param copy Whether to always copy the list
   * @param t Array of members of list
   * @return List containing the given members
   */
  private static <T> List<T> _flatList(T[] t, boolean copy) {
    switch (t.length) {
    case 0:
      return COMPARABLE_EMPTY_LIST;
    case 1:
      return Collections.singletonList(t[0]);
    case 2:
      return new Flat2List<T>(t[0], t[1]);
    case 3:
      return new Flat3List<T>(t[0], t[1], t[2]);
    default:
      // REVIEW: AbstractList contains a modCount field; we could
      //   write our own implementation and reduce creation overhead a
      //   bit.
      if (copy) {
        return new ComparableList(Arrays.asList(t.clone()));
      } else {
        return new ComparableList(Arrays.asList(t));
      }
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
    switch (t.size()) {
    case 0:
      return COMPARABLE_EMPTY_LIST;
    case 1:
      return Collections.singletonList(t.get(0));
    case 2:
      return new Flat2List<T>(t.get(0), t.get(1));
    case 3:
      return new Flat3List<T>(t.get(0), t.get(1), t.get(2));
    default:
      // REVIEW: AbstractList contains a modCount field; we could
      //   write our own implementation and reduce creation overhead a
      //   bit.
      //noinspection unchecked
      return new ComparableList(Arrays.asList(t.toArray()));
    }
  }

  public static abstract class AbstractFlatList<T>
      implements List<T>, RandomAccess {
    protected final List<T> asArrayList() {
      //noinspection unchecked
      return Arrays.asList((T[]) toArray());
    }

    public Iterator<T> iterator() {
      return asArrayList().iterator();
    }

    public ListIterator<T> listIterator() {
      return asArrayList().listIterator();
    }

    public boolean isEmpty() {
      return false;
    }

    public boolean add(T t) {
      throw new UnsupportedOperationException();
    }

    public boolean addAll(Collection<? extends T> c) {
      throw new UnsupportedOperationException();
    }

    public boolean addAll(int index, Collection<? extends T> c) {
      throw new UnsupportedOperationException();
    }

    public boolean removeAll(Collection<?> c) {
      throw new UnsupportedOperationException();
    }

    public boolean retainAll(Collection<?> c) {
      throw new UnsupportedOperationException();
    }

    public void clear() {
      throw new UnsupportedOperationException();
    }

    public T set(int index, T element) {
      throw new UnsupportedOperationException();
    }

    public void add(int index, T element) {
      throw new UnsupportedOperationException();
    }

    public T remove(int index) {
      throw new UnsupportedOperationException();
    }

    public ListIterator<T> listIterator(int index) {
      return asArrayList().listIterator(index);
    }

    public List<T> subList(int fromIndex, int toIndex) {
      return asArrayList().subList(fromIndex, toIndex);
    }

    public boolean contains(Object o) {
      return indexOf(o) >= 0;
    }

    public boolean containsAll(Collection<?> c) {
      for (Object o : c) {
        if (!contains(o)) {
          return false;
        }
      }
      return true;
    }

    public boolean remove(Object o) {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * List that stores its two elements in the two members of the class.
   * Unlike {@link java.util.ArrayList} or
   * {@link java.util.Arrays#asList(Object[])} there is
   * no array, only one piece of memory allocated, therefore is very compact
   * and cache and CPU efficient.
   *
   * <p>The list is read-only, cannot be modified or resized, and neither
   * of the elements can be null.
   *
   * <p>The list is created via {@link FlatLists#of}.
   *
   * @param <T>
   */
  protected static class Flat2List<T>
      extends AbstractFlatList<T>
      implements Comparable<T> {
    private final T t0;
    private final T t1;

    Flat2List(T t0, T t1) {
      this.t0 = t0;
      this.t1 = t1;
      assert t0 != null;
      assert t1 != null;
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
        return Utilities.equal(this.t0, that.t0)
            && Utilities.equal(this.t1, that.t1);
      }
      return Arrays.asList(t0, t1).equals(o);
    }

    public int hashCode() {
      int h = 1;
      h = h * 31 + t0.hashCode();
      h = h * 31 + t1.hashCode();
      return h;
    }

    public int indexOf(Object o) {
      if (t0.equals(o)) {
        return 0;
      }
      if (t1.equals(o)) {
        return 1;
      }
      return -1;
    }

    public int lastIndexOf(Object o) {
      if (t1.equals(o)) {
        return 1;
      }
      if (t0.equals(o)) {
        return 0;
      }
      return -1;
    }

    @SuppressWarnings({"unchecked"})
    public <T2> T2[] toArray(T2[] a) {
      a[0] = (T2) t0;
      a[1] = (T2) t1;
      return a;
    }

    public Object[] toArray() {
      return new Object[] {t0, t1};
    }

    public int compareTo(T o) {
      //noinspection unchecked
      Flat2List<T> that = (Flat2List<T>) o;
      int c = Utilities.compare((Comparable) t0, (Comparable) that.t0);
      if (c != 0) {
        return c;
      }
      return Utilities.compare((Comparable) t1, (Comparable) that.t1);
    }
  }

  /**
   * List that stores its three elements in the three members of the class.
   * Unlike {@link java.util.ArrayList} or
   * {@link java.util.Arrays#asList(Object[])} there is
   * no array, only one piece of memory allocated, therefore is very compact
   * and cache and CPU efficient.
   *
   * <p>The list is read-only, cannot be modified or resized, and none
   * of the elements can be null.
   *
   * <p>The list is created via {@link FlatLists#of(java.util.List)}.
   *
   * @param <T>
   */
  protected static class Flat3List<T>
      extends AbstractFlatList<T>
      implements Comparable<T> {
    private final T t0;
    private final T t1;
    private final T t2;

    Flat3List(T t0, T t1, T t2) {
      this.t0 = t0;
      this.t1 = t1;
      this.t2 = t2;
      assert t0 != null;
      assert t1 != null;
      assert t2 != null;
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
        return Utilities.equal(this.t0, that.t0)
            && Utilities.equal(this.t1, that.t1)
            && Utilities.equal(this.t2, that.t2);
      }
      return o.equals(this);
    }

    public int hashCode() {
      int h = 1;
      h = h * 31 + t0.hashCode();
      h = h * 31 + t1.hashCode();
      h = h * 31 + t2.hashCode();
      return h;
    }

    public int indexOf(Object o) {
      if (t0.equals(o)) {
        return 0;
      }
      if (t1.equals(o)) {
        return 1;
      }
      if (t2.equals(o)) {
        return 2;
      }
      return -1;
    }

    public int lastIndexOf(Object o) {
      if (t2.equals(o)) {
        return 2;
      }
      if (t1.equals(o)) {
        return 1;
      }
      if (t0.equals(o)) {
        return 0;
      }
      return -1;
    }

    @SuppressWarnings({"unchecked"})
    public <T2> T2[] toArray(T2[] a) {
      a[0] = (T2) t0;
      a[1] = (T2) t1;
      a[2] = (T2) t2;
      return a;
    }

    public Object[] toArray() {
      return new Object[] {t0, t1, t2};
    }

    public int compareTo(T o) {
      //noinspection unchecked
      Flat3List<T> that = (Flat3List<T>) o;
      int c = Utilities.compare((Comparable) t0, (Comparable) that.t0);
      if (c != 0) {
        return c;
      }
      c = Utilities.compare((Comparable) t1, (Comparable) that.t1);
      if (c != 0) {
        return c;
      }
      return Utilities.compare((Comparable) t2, (Comparable) that.t2);
    }
  }

  private static class ComparableEmptyList
      extends AbstractList
      implements Comparable<List> {
    private ComparableEmptyList() {
    }

    public Object get(int index) {
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
      if (o == this) {
        return 0;
      }
      return o.size() == 0 ? 0 : -1;
    }
  }

  static class ComparableList<T extends Comparable<T>>
      extends AbstractList<T>
      implements Comparable<List<T>> {
    private final List<T> list;

    protected ComparableList(List<T> list) {
      this.list = list;
    }

    public T get(int index) {
      return list.get(index);
    }

    public int size() {
      return list.size();
    }

    public int compareTo(List<T> o) {
      return compare(list, o);
    }

    static <T extends Comparable<T>>
    int compare(List<T> list0, List<T> list1) {
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

    static <T extends Comparable>
    int compare(List<T> list0, List<T> list1, int size) {
      for (int i = 0; i < size; i++) {
        Comparable o0 = list0.get(i);
        Comparable o1 = list1.get(i);
        int c = o0.compareTo(o1);
        if (c != 0) {
          return c;
        }
      }
      return 0;
    }
  }
}

// End FlatLists.java

