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

import java.io.Serializable;
import java.util.AbstractList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nonnull;

/**
 * Pair of objects.
 *
 * <p>Because a pair implements {@link #equals(Object)}, {@link #hashCode()} and
 * {@link #compareTo(Pair)}, it can be used in any kind of
 * {@link java.util.Collection}.
 *
 * @param <T1> Left-hand type
 * @param <T2> Right-hand type
 */
public class Pair<T1, T2>
    implements Comparable<Pair<T1, T2>>, Map.Entry<T1, T2>, Serializable {
  //~ Instance fields --------------------------------------------------------

  public final T1 left;
  public final T2 right;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a Pair.
   *
   * @param left  left value
   * @param right right value
   */
  public Pair(T1 left, T2 right) {
    this.left = left;
    this.right = right;
  }

  /**
   * Creates a Pair of appropriate type.
   *
   * <p>This is a shorthand that allows you to omit implicit types. For
   * example, you can write:
   * <blockquote>return Pair.of(s, n);</blockquote>
   * instead of
   * <blockquote>return new Pair&lt;String, Integer&gt;(s, n);</blockquote>
   *
   * @param left  left value
   * @param right right value
   * @return A Pair
   */
  public static <T1, T2> Pair<T1, T2> of(T1 left, T2 right) {
    return new Pair<>(left, right);
  }

  /** Creates a {@code Pair} from a {@link java.util.Map.Entry}. */
  public static <K, V> Pair<K, V> of(Map.Entry<K, V> entry) {
    return of(entry.getKey(), entry.getValue());
  }

  //~ Methods ----------------------------------------------------------------

  public boolean equals(Object obj) {
    return this == obj
        || (obj instanceof Pair)
        && Objects.equals(this.left, ((Pair) obj).left)
        && Objects.equals(this.right, ((Pair) obj).right);
  }

  /** {@inheritDoc}
   *
   * <p>Computes hash code consistent with
   * {@link java.util.Map.Entry#hashCode()}. */
  @Override public int hashCode() {
    int keyHash = left == null ? 0 : left.hashCode();
    int valueHash = right == null ? 0 : right.hashCode();
    return keyHash ^ valueHash;
  }

  public int compareTo(@Nonnull Pair<T1, T2> that) {
    //noinspection unchecked
    int c = compare((Comparable) this.left, (Comparable) that.left);
    if (c == 0) {
      //noinspection unchecked
      c = compare((Comparable) this.right, (Comparable) that.right);
    }
    return c;
  }

  public String toString() {
    return "<" + left + ", " + right + ">";
  }

  public T1 getKey() {
    return left;
  }

  public T2 getValue() {
    return right;
  }

  public T2 setValue(T2 value) {
    throw new UnsupportedOperationException();
  }

  /**
   * Compares a pair of comparable values of the same type. Null collates
   * less than everything else, but equal to itself.
   *
   * @param c1 First value
   * @param c2 Second value
   * @return a negative integer, zero, or a positive integer if c1
   * is less than, equal to, or greater than c2.
   */
  private static <C extends Comparable<C>> int compare(C c1, C c2) {
    if (c1 == null) {
      if (c2 == null) {
        return 0;
      } else {
        return -1;
      }
    } else if (c2 == null) {
      return 1;
    } else {
      return c1.compareTo(c2);
    }
  }

  /**
   * Converts a collection of Pairs into a Map.
   *
   * <p>This is an obvious thing to do because Pair is similar in structure to
   * {@link java.util.Map.Entry}.
   *
   * <p>The map contains a copy of the collection of Pairs; if you change the
   * collection, the map does not change.
   *
   * @param pairs Collection of Pair objects
   * @return map with the same contents as the collection
   */
  public static <K, V> Map<K, V> toMap(Iterable<Pair<K, V>> pairs) {
    final Map<K, V> map = new HashMap<>();
    for (Pair<K, V> pair : pairs) {
      map.put(pair.left, pair.right);
    }
    return map;
  }

  /**
   * Converts two lists into a list of {@link Pair}s,
   * whose length is the lesser of the lengths of the
   * source lists.
   *
   * @param ks Left list
   * @param vs Right list
   * @return List of pairs
   * @see org.apache.calcite.linq4j.Ord#zip(java.util.List)
   */
  public static <K, V> List<Pair<K, V>> zip(List<K> ks, List<V> vs) {
    return zip(ks, vs, false);
  }

  /**
   * Converts two lists into a list of {@link Pair}s.
   *
   * <p>The length of the combined list is the lesser of the lengths of the
   * source lists. But typically the source lists will be the same length.</p>
   *
   * @param ks     Left list
   * @param vs     Right list
   * @param strict Whether to fail if lists have different size
   * @return List of pairs
   * @see org.apache.calcite.linq4j.Ord#zip(java.util.List)
   */
  public static <K, V> List<Pair<K, V>> zip(
      final List<K> ks,
      final List<V> vs,
      boolean strict) {
    final int size;
    if (strict) {
      if (ks.size() != vs.size()) {
        throw new AssertionError();
      }
      size = ks.size();
    } else {
      size = Math.min(ks.size(), vs.size());
    }
    return new ZipList<>(ks, vs, size);
  }

  /**
   * Converts two iterables into an iterable of {@link Pair}s.
   *
   * <p>The resulting iterator ends whenever the first of the input iterators
   * ends. But typically the source iterators will be the same length.</p>
   *
   * @param ks Left iterable
   * @param vs Right iterable
   * @return Iterable over pairs
   */
  public static <K, V> Iterable<Pair<K, V>> zip(
      final Iterable<? extends K> ks,
      final Iterable<? extends V> vs) {
    return () -> {
      final Iterator<? extends K> kIterator = ks.iterator();
      final Iterator<? extends V> vIterator = vs.iterator();

      return new ZipIterator<>(kIterator, vIterator);
    };
  }

  /**
   * Converts two arrays into a list of {@link Pair}s.
   *
   * <p>The length of the combined list is the lesser of the lengths of the
   * source arrays. But typically the source arrays will be the same
   * length.</p>
   *
   * @param ks Left array
   * @param vs Right array
   * @return List of pairs
   */
  public static <K, V> List<Pair<K, V>> zip(
      final K[] ks,
      final V[] vs) {
    return new AbstractList<Pair<K, V>>() {
      public Pair<K, V> get(int index) {
        return Pair.of(ks[index], vs[index]);
      }

      public int size() {
        return Math.min(ks.length, vs.length);
      }
    };
  }

  /** Returns a mutable list of pairs backed by a pair of mutable lists.
   *
   * <p>Modifications to this list are reflected in the backing lists, and vice
   * versa.
   *
   * @param <K> Key (left) value type
   * @param <V> Value (right) value type */
  public static <K, V> List<Pair<K, V>> zipMutable(
      final List<K> ks,
      final List<V> vs) {
    return new MutableZipList<>(ks, vs);
  }

  /**
   * Returns an iterable over the left slice of an iterable.
   *
   * @param iterable Iterable over pairs
   * @param <L>      Left type
   * @param <R>      Right type
   * @return Iterable over the left elements
   */
  public static <L, R> Iterable<L> left(
      final Iterable<? extends Map.Entry<L, R>> iterable) {
    return () -> new LeftIterator<>(iterable.iterator());
  }

  /**
   * Returns an iterable over the right slice of an iterable.
   *
   * @param iterable Iterable over pairs
   * @param <L>      right type
   * @param <R>      Right type
   * @return Iterable over the right elements
   */
  public static <L, R> Iterable<R> right(
      final Iterable<? extends Map.Entry<L, R>> iterable) {
    return () -> new RightIterator<>(iterable.iterator());
  }

  public static <K, V> List<K> left(
      final List<? extends Map.Entry<K, V>> pairs) {
    return new AbstractList<K>() {
      public K get(int index) {
        return pairs.get(index).getKey();
      }

      public int size() {
        return pairs.size();
      }
    };
  }

  public static <K, V> List<V> right(
      final List<? extends Map.Entry<K, V>> pairs) {
    return new AbstractList<V>() {
      public V get(int index) {
        return pairs.get(index).getValue();
      }

      public int size() {
        return pairs.size();
      }
    };
  }

  /**
   * Returns an iterator that iterates over (i, i + 1) pairs in an iterable.
   *
   * <p>For example, {@code adjacents([3, 5, 7])} returns [(3, 5), (5, 7)].</p>
   *
   * @param iterable Source collection
   * @param <T> Element type
   * @return Iterable over adjacent element pairs
   */
  public static <T> Iterable<Pair<T, T>> adjacents(final Iterable<T> iterable) {
    return () -> {
      final Iterator<T> iterator = iterable.iterator();
      if (!iterator.hasNext()) {
        return Collections.emptyIterator();
      }
      return new AdjacentIterator<>(iterator);
    };
  }

  /**
   * Returns an iterator that iterates over (0, i) pairs in an iterable for
   * i &gt; 0.
   *
   * <p>For example, {@code firstAnd([3, 5, 7])} returns [(3, 5), (3, 7)].</p>
   *
   * @param iterable Source collection
   * @param <T> Element type
   * @return Iterable over pairs of the first element and all other elements
   */
  public static <T> Iterable<Pair<T, T>> firstAnd(final Iterable<T> iterable) {
    return () -> {
      final Iterator<T> iterator = iterable.iterator();
      if (!iterator.hasNext()) {
        return Collections.emptyIterator();
      }
      final T first = iterator.next();
      return new FirstAndIterator<>(iterator, first);
    };
  }

  /** Iterator that returns the left field of each pair.
   *
   * @param <L> Left-hand type
   * @param <R> Right-hand type */
  private static class LeftIterator<L, R> implements Iterator<L> {
    private final Iterator<? extends Map.Entry<L, R>> iterator;

    LeftIterator(Iterator<? extends Map.Entry<L, R>> iterator) {
      this.iterator = Objects.requireNonNull(iterator);
    }

    public boolean hasNext() {
      return iterator.hasNext();
    }

    public L next() {
      return iterator.next().getKey();
    }

    public void remove() {
      iterator.remove();
    }
  }

  /** Iterator that returns the right field of each pair.
   *
   * @param <L> Left-hand type
   * @param <R> Right-hand type */
  private static class RightIterator<L, R> implements Iterator<R> {
    private final Iterator<? extends Map.Entry<L, R>> iterator;

    RightIterator(Iterator<? extends Map.Entry<L, R>> iterator) {
      this.iterator = Objects.requireNonNull(iterator);
    }

    public boolean hasNext() {
      return iterator.hasNext();
    }

    public R next() {
      return iterator.next().getValue();
    }

    public void remove() {
      iterator.remove();
    }
  }

  /** Iterator that returns the first element of a collection paired with every
   * other element.
   *
   * @param <E> Element type */
  private static class FirstAndIterator<E> implements Iterator<Pair<E, E>> {
    private final Iterator<E> iterator;
    private final E first;

    FirstAndIterator(Iterator<E> iterator, E first) {
      this.iterator = Objects.requireNonNull(iterator);
      this.first = first;
    }

    public boolean hasNext() {
      return iterator.hasNext();
    }

    public Pair<E, E> next() {
      return of(first, iterator.next());
    }

    public void remove() {
      throw new UnsupportedOperationException("remove");
    }
  }

  /** Iterator that pairs elements from two iterators.
   *
   * @param <L> Left-hand type
   * @param <R> Right-hand type */
  private static class ZipIterator<L, R> implements Iterator<Pair<L, R>> {
    private final Iterator<? extends L> leftIterator;
    private final Iterator<? extends R> rightIterator;

    ZipIterator(Iterator<? extends L> leftIterator,
        Iterator<? extends R> rightIterator) {
      this.leftIterator = Objects.requireNonNull(leftIterator);
      this.rightIterator = Objects.requireNonNull(rightIterator);
    }

    public boolean hasNext() {
      return leftIterator.hasNext() && rightIterator.hasNext();
    }

    public Pair<L, R> next() {
      return Pair.of(leftIterator.next(), rightIterator.next());
    }

    public void remove() {
      leftIterator.remove();
      rightIterator.remove();
    }
  }

  /** Iterator that returns consecutive pairs of elements from an underlying
   * iterator.
   *
   * @param <E> Element type */
  private static class AdjacentIterator<E> implements Iterator<Pair<E, E>> {
    private final E first;
    private final Iterator<E> iterator;
    E previous;

    AdjacentIterator(Iterator<E> iterator) {
      this.iterator = Objects.requireNonNull(iterator);
      this.first = iterator.next();
      previous = first;
    }

    public boolean hasNext() {
      return iterator.hasNext();
    }

    public Pair<E, E> next() {
      final E current = iterator.next();
      final Pair<E, E> pair = of(previous, current);
      previous = current;
      return pair;
    }

    public void remove() {
      throw new UnsupportedOperationException("remove");
    }
  }

  /** Unmodifiable list of pairs, backed by a pair of lists.
   *
   * <p>Though it is unmodifiable, it is mutable: if the contents of one
   * of the backing lists changes, the contents of this list will appear to
   * change. The length, however, is fixed on creation.
   *
   * @param <K> Left-hand type
   * @param <V> Right-hand type
   *
   * @see MutableZipList */
  private static class ZipList<K, V> extends AbstractList<Pair<K, V>> {
    private final List<K> ks;
    private final List<V> vs;
    private final int size;

    ZipList(List<K> ks, List<V> vs, int size) {
      this.ks = ks;
      this.vs = vs;
      this.size = size;
    }

    public Pair<K, V> get(int index) {
      return Pair.of(ks.get(index), vs.get(index));
    }

    public int size() {
      return size;
    }
  }

  /** A mutable list of pairs backed by a pair of mutable lists.
   *
   * <p>Modifications to this list are reflected in the backing lists, and vice
   * versa.
   *
   * @param <K> Key (left) value type
   * @param <V> Value (right) value type */
  private static class MutableZipList<K, V> extends AbstractList<Pair<K, V>> {
    private final List<K> ks;
    private final List<V> vs;

    MutableZipList(List<K> ks, List<V> vs) {
      this.ks = Objects.requireNonNull(ks);
      this.vs = Objects.requireNonNull(vs);
    }

    @Override public Pair<K, V> get(int index) {
      return Pair.of(ks.get(index), vs.get(index));
    }

    @Override public int size() {
      return Math.min(ks.size(), vs.size());
    }

    @Override public void add(int index, Pair<K, V> pair) {
      ks.add(index, pair.left);
      vs.add(index, pair.right);
    }

    @Override public Pair<K, V> remove(int index) {
      final K bufferedRow = ks.remove(index);
      final V stateSet = vs.remove(index);
      return Pair.of(bufferedRow, stateSet);
    }

    @Override public Pair<K, V> set(int index, Pair<K, V> pair) {
      final Pair<K, V> previous = get(index);
      ks.set(index, pair.left);
      vs.set(index, pair.right);
      return previous;
    }
  }
}

// End Pair.java
