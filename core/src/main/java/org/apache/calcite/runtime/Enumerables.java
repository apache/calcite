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

import org.apache.calcite.interpreter.Row;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.function.EqualityComparer;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.function.Function2;
import org.apache.calcite.linq4j.function.Predicate1;
import org.apache.calcite.linq4j.function.Predicate2;
import org.apache.calcite.util.Bug;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Set;

/**
 * Utilities for processing {@link org.apache.calcite.linq4j.Enumerable}
 * collections.
 *
 * <p>This class is a place to put things not yet added to linq4j.
 * Methods are subject to removal without notice.</p>
 */
public class Enumerables {
  private static final Function1<?, ?> SLICE =
      new Function1<Object[], Object>() {
        public Object apply(Object[] a0) {
          return a0[0];
        }
      };

  private static final Function1<Object[], Row> ARRAY_TO_ROW =
      new Function1<Object[], Row>() {
        public Row apply(Object[] a0) {
          return Row.asCopy(a0);
        }
      };

  private Enumerables() {}

  /** Converts an enumerable over singleton arrays into the enumerable of their
   * first elements. */
  public static <E> Enumerable<E> slice0(Enumerable<E[]> enumerable) {
    //noinspection unchecked
    return enumerable.select((Function1<E[], E>) SLICE);
  }

   /**
   * Returns elements of {@code outer} for which there is a member of
   * {@code inner} with a matching key.
   */
  public static <TSource, TInner, TKey> Enumerable<TSource> semiJoin(
      final Enumerable<TSource> outer, final Enumerable<TInner> inner,
      final Function1<TSource, TKey> outerKeySelector,
      final Function1<TInner, TKey> innerKeySelector) {
    Bug.upgrade("move into linq4j");
    return semiJoin(outer, inner, outerKeySelector, innerKeySelector, null);
  }

  /**
   * Returns elements of {@code outer} for which there is a member of
   * {@code inner} with a matching key. A specified
   * {@code EqualityComparer<TSource>} is used to compare keys.
   */
  public static <TSource, TInner, TKey> Enumerable<TSource> semiJoin(
      final Enumerable<TSource> outer, final Enumerable<TInner> inner,
      final Function1<TSource, TKey> outerKeySelector,
      final Function1<TInner, TKey> innerKeySelector,
      final EqualityComparer<TKey> comparer) {
    return new AbstractEnumerable<TSource>() {
      public Enumerator<TSource> enumerator() {
        final Enumerable<TKey> innerLookup =
            comparer == null
                ? inner.select(innerKeySelector).distinct()
                : inner.select(innerKeySelector).distinct(comparer);

        return Enumerables.where(outer.enumerator(),
            new Predicate1<TSource>() {
              public boolean apply(TSource v0) {
                final TKey key = outerKeySelector.apply(v0);
                return innerLookup.contains(key);
              }
            });
      }
    };
  }

  /**
   * Correlates the elements of two sequences based on a predicate.
   */
  public static <TSource, TInner, TResult> Enumerable<TResult> thetaJoin(
      final Enumerable<TSource> outer, final Enumerable<TInner> inner,
      final Predicate2<TSource, TInner> predicate,
      Function2<TSource, TInner, TResult> resultSelector,
      final boolean generateNullsOnLeft,
      final boolean generateNullsOnRight) {
    // Building the result as a list is easy but hogs memory. We should iterate.
    final List<TResult> result = Lists.newArrayList();
    final Enumerator<TSource> lefts = outer.enumerator();
    final List<TInner> rightList = inner.toList();
    final Set<TInner> rightUnmatched;
    if (generateNullsOnLeft) {
      rightUnmatched = Sets.newIdentityHashSet();
      rightUnmatched.addAll(rightList);
    } else {
      rightUnmatched = null;
    }
    while (lefts.moveNext()) {
      int leftMatchCount = 0;
      final TSource left = lefts.current();
      final Enumerator<TInner> rights = Linq4j.iterableEnumerator(rightList);
      while (rights.moveNext()) {
        TInner right = rights.current();
        if (predicate.apply(left, right)) {
          ++leftMatchCount;
          if (rightUnmatched != null) {
            rightUnmatched.remove(right);
          }
          result.add(resultSelector.apply(left, right));
        }
      }
      if (generateNullsOnRight && leftMatchCount == 0) {
        result.add(resultSelector.apply(left, null));
      }
    }
    if (rightUnmatched != null) {
      final Enumerator<TInner> rights =
          Linq4j.iterableEnumerator(rightUnmatched);
      while (rights.moveNext()) {
        TInner right = rights.current();
        result.add(resultSelector.apply(null, right));
      }
    }
    return Linq4j.asEnumerable(result);
  }

  /**
   * Filters a sequence of values based on a
   * predicate.
   */
  public static <TSource> Enumerable<TSource> where(
      final Enumerable<TSource> source, final Predicate1<TSource> predicate) {
    assert predicate != null;
    return new AbstractEnumerable<TSource>() {
      public Enumerator<TSource> enumerator() {
        final Enumerator<TSource> enumerator = source.enumerator();
        return Enumerables.where(enumerator, predicate);
      }
    };
  }

  private static <TSource> Enumerator<TSource> where(
      final Enumerator<TSource> enumerator,
      final Predicate1<TSource> predicate) {
    return new Enumerator<TSource>() {
      public TSource current() {
        return enumerator.current();
      }

      public boolean moveNext() {
        while (enumerator.moveNext()) {
          if (predicate.apply(enumerator.current())) {
            return true;
          }
        }
        return false;
      }

      public void reset() {
        enumerator.reset();
      }

      public void close() {
        enumerator.close();
      }
    };
  }

  /** Converts an {@link Enumerable} over object arrays into an
   * {@link Enumerable} over {@link Row} objects. */
  public static Enumerable<Row> toRow(final Enumerable<Object[]> enumerator) {
    return enumerator.select(ARRAY_TO_ROW);
  }

  /** Joins two inputs that are sorted on the key. */
  public static <TSource, TInner, TKey extends Comparable<TKey>, TResult>
  Enumerable<TResult> mergeJoin(final Enumerable<TSource> outer,
      final Enumerable<TInner> inner,
      final Function1<TSource, TKey> outerKeySelector,
      final Function1<TInner, TKey> innerKeySelector,
      final Function2<TSource, TInner, TResult> resultSelector,
      boolean generateNullsOnLeft,
      boolean generateNullsOnRight) {
    assert !generateNullsOnLeft : "not implemented";
    assert !generateNullsOnRight : "not implemented";
    return new AbstractEnumerable<TResult>() {
      public Enumerator<TResult> enumerator() {
        return new Enumerator<TResult>() {
          final Enumerator<TSource> leftEnumerator = outer.enumerator();
          final Enumerator<TInner> rightEnumerator = inner.enumerator();
          final List<TSource> lefts = Lists.newArrayList();
          final List<TInner> rights = Lists.newArrayList();
          boolean done;
          Enumerator<List<Object>> cartesians;

          {
            start();
          }

          private void start() {
            if (!leftEnumerator.moveNext()
                || !rightEnumerator.moveNext()
                || !advance()) {
              done = true;
              cartesians = Linq4j.emptyEnumerator();
            }
          }

          /** Moves to the next key that is present in both sides. Populates
           * lefts and rights with the rows. Restarts the cross-join
           * enumerator. */
          private boolean advance() {
            TSource left = leftEnumerator.current();
            TKey leftKey = outerKeySelector.apply(left);
            TInner right = rightEnumerator.current();
            TKey rightKey = innerKeySelector.apply(right);
            for (;;) {
              int c = leftKey.compareTo(rightKey);
              if (c == 0) {
                break;
              }
              if (c < 0) {
                if (!leftEnumerator.moveNext()) {
                  done = true;
                  return false;
                }
                left = leftEnumerator.current();
                leftKey = outerKeySelector.apply(left);
              } else {
                if (!rightEnumerator.moveNext()) {
                  done = true;
                  return false;
                }
                right = rightEnumerator.current();
                rightKey = innerKeySelector.apply(right);
              }
            }
            lefts.clear();
            lefts.add(left);
            for (;;) {
              if (!leftEnumerator.moveNext()) {
                done = true;
                break;
              }
              left = leftEnumerator.current();
              TKey leftKey2 = outerKeySelector.apply(left);
              int c = leftKey.compareTo(leftKey2);
              if (c != 0) {
                assert c < 0 : "not sorted";
                break;
              }
              lefts.add(left);
            }
            rights.clear();
            rights.add(right);
            for (;;) {
              if (!rightEnumerator.moveNext()) {
                done = true;
                break;
              }
              right = rightEnumerator.current();
              TKey rightKey2 = innerKeySelector.apply(right);
              int c = rightKey.compareTo(rightKey2);
              if (c != 0) {
                assert c < 0 : "not sorted";
                break;
              }
              rights.add(right);
            }
            cartesians = Linq4j.product(
                ImmutableList.of(Linq4j.<Object>enumerator(lefts),
                    Linq4j.<Object>enumerator(rights)));
            return true;
          }

          public TResult current() {
            final List<Object> list = cartesians.current();
            @SuppressWarnings("unchecked") final TSource left =
                (TSource) list.get(0);
            @SuppressWarnings("unchecked") final TInner right =
                (TInner) list.get(1);
            return resultSelector.apply(left, right);
          }

          public boolean moveNext() {
            for (;;) {
              if (cartesians.moveNext()) {
                return true;
              }
              if (done) {
                return false;
              }
              if (!advance()) {
                return false;
              }
            }
          }

          public void reset() {
            done = false;
            leftEnumerator.reset();
            rightEnumerator.reset();
            start();
          }

          public void close() {
          }
        };
      }
    };
  }

}

// End Enumerables.java
