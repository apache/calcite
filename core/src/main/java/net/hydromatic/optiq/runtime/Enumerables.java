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
package net.hydromatic.optiq.runtime;

import net.hydromatic.linq4j.AbstractEnumerable;
import net.hydromatic.linq4j.Enumerable;
import net.hydromatic.linq4j.Enumerator;
import net.hydromatic.linq4j.function.EqualityComparer;
import net.hydromatic.linq4j.function.Function1;
import net.hydromatic.linq4j.function.Predicate1;

import net.hydromatic.optiq.impl.interpreter.Row;

import org.eigenbase.util.Bug;

/**
 * Utilities for processing {@link net.hydromatic.linq4j.Enumerable}
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
}

// End Enumerables.java
