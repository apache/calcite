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
package org.apache.calcite.linq4j;

import org.apache.calcite.linq4j.function.BigDecimalFunction1;
import org.apache.calcite.linq4j.function.DoubleFunction1;
import org.apache.calcite.linq4j.function.EqualityComparer;
import org.apache.calcite.linq4j.function.FloatFunction1;
import org.apache.calcite.linq4j.function.Function0;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.function.Function2;
import org.apache.calcite.linq4j.function.Functions;
import org.apache.calcite.linq4j.function.IntegerFunction1;
import org.apache.calcite.linq4j.function.LongFunction1;
import org.apache.calcite.linq4j.function.NullableBigDecimalFunction1;
import org.apache.calcite.linq4j.function.NullableDoubleFunction1;
import org.apache.calcite.linq4j.function.NullableFloatFunction1;
import org.apache.calcite.linq4j.function.NullableIntegerFunction1;
import org.apache.calcite.linq4j.function.NullableLongFunction1;
import org.apache.calcite.linq4j.function.Predicate1;
import org.apache.calcite.linq4j.function.Predicate2;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import java.math.BigDecimal;
import java.util.AbstractList;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.RandomAccess;
import java.util.Set;
import java.util.TreeMap;

import static org.apache.calcite.linq4j.Linq4j.CollectionEnumerable;
import static org.apache.calcite.linq4j.Linq4j.ListEnumerable;
import static org.apache.calcite.linq4j.function.Functions.adapt;

/**
 * Default implementations of methods in the {@link Enumerable} interface.
 */
public abstract class EnumerableDefaults {

  /**
   * Applies an accumulator function over a sequence.
   */
  public static <TSource> TSource aggregate(Enumerable<TSource> source,
      Function2<TSource, TSource, TSource> func) {
    TSource result = null;
    try (Enumerator<TSource> os = source.enumerator()) {
      while (os.moveNext()) {
        TSource o = os.current();
        result = func.apply(result, o);
      }
      return result;
    }
  }

  /**
   * Applies an accumulator function over a
   * sequence. The specified seed value is used as the initial
   * accumulator value.
   */
  public static <TSource, TAccumulate> TAccumulate aggregate(
      Enumerable<TSource> source, TAccumulate seed,
      Function2<TAccumulate, TSource, TAccumulate> func) {
    TAccumulate result = seed;
    try (Enumerator<TSource> os = source.enumerator()) {
      while (os.moveNext()) {
        TSource o = os.current();
        result = func.apply(result, o);
      }
      return result;
    }
  }

  /**
   * Applies an accumulator function over a
   * sequence. The specified seed value is used as the initial
   * accumulator value, and the specified function is used to select
   * the result value.
   */
  public static <TSource, TAccumulate, TResult> TResult aggregate(
      Enumerable<TSource> source, TAccumulate seed,
      Function2<TAccumulate, TSource, TAccumulate> func,
      Function1<TAccumulate, TResult> selector) {
    TAccumulate accumulate = seed;
    try (Enumerator<TSource> os = source.enumerator()) {
      while (os.moveNext()) {
        TSource o = os.current();
        accumulate = func.apply(accumulate, o);
      }
      return selector.apply(accumulate);
    }
  }

  /**
   * Determines whether all elements of a sequence
   * satisfy a condition.
   */
  public static <TSource> boolean all(Enumerable<TSource> enumerable,
      Predicate1<TSource> predicate) {
    try (Enumerator<TSource> os = enumerable.enumerator()) {
      while (os.moveNext()) {
        TSource o = os.current();
        if (!predicate.apply(o)) {
          return false;
        }
      }
      return true;
    }
  }

  /**
   * Determines whether a sequence contains any
   * elements.
   */
  public static boolean any(Enumerable enumerable) {
    return enumerable.enumerator().moveNext();
  }

  /**
   * Determines whether any element of a sequence
   * satisfies a condition.
   */
  public static <TSource> boolean any(Enumerable<TSource> enumerable,
      Predicate1<TSource> predicate) {
    try (Enumerator<TSource> os = enumerable.enumerator()) {
      while (os.moveNext()) {
        TSource o = os.current();
        if (predicate.apply(o)) {
          return true;
        }
      }
      return false;
    }
  }

  /**
   * Returns the input typed as {@code Enumerable<TSource>}.
   *
   * <p>This method has no effect other than to change the compile-time type of
   * source from a type that implements {@code Enumerable<TSource>} to
   * {@code Enumerable<TSource>} itself.
   *
   * <p>{@code AsEnumerable<TSource>(Enumerable<TSource>)} can be used to choose
   * between query implementations when a sequence implements
   * {@code Enumerable<TSource>} but also has a different set of public query
   * methods available. For example, given a generic class {@code Table} that
   * implements {@code Enumerable<TSource>} and has its own methods such as
   * {@code where}, {@code select}, and {@code selectMany}, a call to
   * {@code where} would invoke the public {@code where} method of
   * {@code Table}. A {@code Table} type that represents a database table could
   * have a {@code where} method that takes the predicate argument as an
   * expression tree and converts the tree to SQL for remote execution. If
   * remote execution is not desired, for example because the predicate invokes
   * a local method, the {@code asEnumerable<TSource>} method can be used to
   * hide the custom methods and instead make the standard query operators
   * available.
   */
  public static <TSource> Enumerable<TSource> asEnumerable(
      Enumerable<TSource> enumerable) {
    return enumerable;
  }

  /**
   * Converts an Enumerable to an IQueryable.
   *
   * <p>Analogous to the LINQ's Enumerable.AsQueryable extension method.</p>
   *
   * @param enumerable Enumerable
   * @param <TSource> Element type
   *
   * @return A queryable
   */
  public static <TSource> Queryable<TSource> asQueryable(
      Enumerable<TSource> enumerable) {
    throw Extensions.todo();
  }

  /**
   * Computes the average of a sequence of Decimal
   * values that are obtained by invoking a transform function on
   * each element of the input sequence.
   */
  public static <TSource> BigDecimal average(Enumerable<TSource> source,
      BigDecimalFunction1<TSource> selector) {
    return sum(source, selector).divide(BigDecimal.valueOf(longCount(source)));
  }

  /**
   * Computes the average of a sequence of nullable
   * Decimal values that are obtained by invoking a transform
   * function on each element of the input sequence.
   */
  public static <TSource> BigDecimal average(Enumerable<TSource> source,
      NullableBigDecimalFunction1<TSource> selector) {
    return sum(source, selector).divide(BigDecimal.valueOf(longCount(source)));
  }

  /**
   * Computes the average of a sequence of Double
   * values that are obtained by invoking a transform function on
   * each element of the input sequence.
   */
  public static <TSource> double average(Enumerable<TSource> source,
      DoubleFunction1<TSource> selector) {
    return sum(source, selector) / longCount(source);
  }

  /**
   * Computes the average of a sequence of nullable
   * Double values that are obtained by invoking a transform
   * function on each element of the input sequence.
   */
  public static <TSource> Double average(Enumerable<TSource> source,
      NullableDoubleFunction1<TSource> selector) {
    return sum(source, selector) / longCount(source);
  }

  /**
   * Computes the average of a sequence of int values
   * that are obtained by invoking a transform function on each
   * element of the input sequence.
   */
  public static <TSource> int average(Enumerable<TSource> source,
      IntegerFunction1<TSource> selector) {
    return sum(source, selector) / count(source);
  }

  /**
   * Computes the average of a sequence of nullable
   * int values that are obtained by invoking a transform function
   * on each element of the input sequence.
   */
  public static <TSource> Integer average(Enumerable<TSource> source,
      NullableIntegerFunction1<TSource> selector) {
    return sum(source, selector) / count(source);
  }

  /**
   * Computes the average of a sequence of long values
   * that are obtained by invoking a transform function on each
   * element of the input sequence.
   */
  public static <TSource> long average(Enumerable<TSource> source,
      LongFunction1<TSource> selector) {
    return sum(source, selector) / longCount(source);
  }

  /**
   * Computes the average of a sequence of nullable
   * long values that are obtained by invoking a transform function
   * on each element of the input sequence.
   */
  public static <TSource> Long average(Enumerable<TSource> source,
      NullableLongFunction1<TSource> selector) {
    return sum(source, selector) / longCount(source);
  }

  /**
   * Computes the average of a sequence of Float
   * values that are obtained by invoking a transform function on
   * each element of the input sequence.
   */
  public static <TSource> float average(Enumerable<TSource> source,
      FloatFunction1<TSource> selector) {
    return sum(source, selector) / longCount(source);
  }

  /**
   * Computes the average of a sequence of nullable
   * Float values that are obtained by invoking a transform
   * function on each element of the input sequence.
   */
  public static <TSource> Float average(Enumerable<TSource> source,
      NullableFloatFunction1<TSource> selector) {
    return sum(source, selector) / longCount(source);
  }

  /**
   * <p>Analogous to LINQ's Enumerable.Cast extension method.</p>
   *
   * @param clazz Target type
   * @param <T2> Target type
   *
   * @return Collection of T2
   */
  public static <TSource, T2> Enumerable<T2> cast(
      final Enumerable<TSource> source, final Class<T2> clazz) {
    return new AbstractEnumerable<T2>() {
      public Enumerator<T2> enumerator() {
        return new CastingEnumerator<>(source.enumerator(), clazz);
      }
    };
  }

  /**
   * Concatenates two sequences.
   */
  public static <TSource> Enumerable<TSource> concat(
      Enumerable<TSource> enumerable0, Enumerable<TSource> enumerable1) {
    //noinspection unchecked
    return Linq4j.concat(
        Arrays.asList(enumerable0, enumerable1));
  }

  /**
   * Determines whether a sequence contains a specified
   * element by using the default equality comparer.
   */
  public static <TSource> boolean contains(Enumerable<TSource> enumerable,
      TSource element) {
    // Implementations of Enumerable backed by a Collection call
    // Collection.contains, which may be more efficient, not this method.
    try (Enumerator<TSource> os = enumerable.enumerator()) {
      while (os.moveNext()) {
        TSource o = os.current();
        if (o.equals(element)) {
          return true;
        }
      }
      return false;
    }
  }

  /**
   * Determines whether a sequence contains a specified
   * element by using a specified {@code EqualityComparer<TSource>}.
   */
  public static <TSource> boolean contains(Enumerable<TSource> enumerable,
      TSource element, EqualityComparer<TSource> comparer) {
    for (TSource o : enumerable) {
      if (comparer.equal(o, element)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns the number of elements in a
   * sequence.
   */
  public static <TSource> int count(Enumerable<TSource> enumerable) {
    return (int) longCount(enumerable, Functions.truePredicate1());
  }

  /**
   * Returns a number that represents how many elements
   * in the specified sequence satisfy a condition.
   */
  public static <TSource> int count(Enumerable<TSource> enumerable,
      Predicate1<TSource> predicate) {
    return (int) longCount(enumerable, predicate);
  }

  /**
   * Returns the elements of the specified sequence or
   * the type parameter's default value in a singleton collection if
   * the sequence is empty.
   */
  public static <TSource> Enumerable<TSource> defaultIfEmpty(
      Enumerable<TSource> enumerable) {
    return defaultIfEmpty(enumerable, null);
  }

  /**
   * Returns the elements of the specified sequence or
   * the specified value in a singleton collection if the sequence
   * is empty.
   */
  public static <TSource> Enumerable<TSource> defaultIfEmpty(
      Enumerable<TSource> enumerable,
      TSource value) {
    try (Enumerator<TSource> os = enumerable.enumerator()) {
      if (os.moveNext()) {
        return Linq4j.asEnumerable(() -> new Iterator<TSource>() {

          private boolean nonFirst;

          private Iterator<TSource> rest;

          public boolean hasNext() {
            return !nonFirst || rest.hasNext();
          }

          public TSource next() {
            if (nonFirst) {
              return rest.next();
            } else {
              final TSource first = os.current();
              nonFirst = true;
              rest = Linq4j.enumeratorIterator(os);
              return first;
            }
          }

          public void remove() {
            throw new UnsupportedOperationException("remove");
          }
        });
      } else {
        return Linq4j.singletonEnumerable(value);
      }
    }
  }

  /**
   * Returns distinct elements from a sequence by using
   * the default {@link EqualityComparer} to compare values.
   */
  public static <TSource> Enumerable<TSource> distinct(
      Enumerable<TSource> enumerable) {
    final Enumerator<TSource> os = enumerable.enumerator();
    final Set<TSource> set = new HashSet<>();
    while (os.moveNext()) {
      set.add(os.current());
    }
    os.close();
    return Linq4j.asEnumerable(set);
  }

  /**
   * Returns distinct elements from a sequence by using
   * a specified {@link EqualityComparer} to compare values.
   */
  public static <TSource> Enumerable<TSource> distinct(
      Enumerable<TSource> enumerable, EqualityComparer<TSource> comparer) {
    if (comparer == Functions.identityComparer()) {
      return distinct(enumerable);
    }
    final Set<Wrapped<TSource>> set = new HashSet<>();
    Function1<TSource, Wrapped<TSource>> wrapper = wrapperFor(comparer);
    Function1<Wrapped<TSource>, TSource> unwrapper = unwrapper();
    enumerable.select(wrapper).into(set);
    return Linq4j.asEnumerable(set).select(unwrapper);
  }

  /**
   * Returns the element at a specified index in a
   * sequence.
   */
  public static <TSource> TSource elementAt(Enumerable<TSource> enumerable,
      int index) {
    final ListEnumerable<TSource> list = enumerable instanceof ListEnumerable
        ? ((ListEnumerable<TSource>) enumerable)
        : null;
    if (list != null) {
      return list.toList().get(index);
    }
    if (index < 0) {
      throw new IndexOutOfBoundsException();
    }
    try (Enumerator<TSource> os = enumerable.enumerator()) {
      while (true) {
        if (!os.moveNext()) {
          throw new IndexOutOfBoundsException();
        }
        if (index == 0) {
          return os.current();
        }
        index--;
      }
    }
  }

  /**
   * Returns the element at a specified index in a
   * sequence or a default value if the index is out of
   * range.
   */
  public static <TSource> TSource elementAtOrDefault(
      Enumerable<TSource> enumerable, int index) {
    final ListEnumerable<TSource> list = enumerable instanceof ListEnumerable
        ? ((ListEnumerable<TSource>) enumerable)
        : null;
    if (index >= 0) {
      if (list != null) {
        final List<TSource> rawList = list.toList();
        if (index < rawList.size()) {
          return rawList.get(index);
        }
      } else {
        try (Enumerator<TSource> os = enumerable.enumerator()) {
          while (true) {
            if (!os.moveNext()) {
              break;
            }
            if (index == 0) {
              return os.current();
            }
            index--;
          }
        }
      }
    }
    return null;
  }

  /**
   * Produces the set difference of two sequences by
   * using the default equality comparer to compare values. (Defined
   * by Enumerable.)
   */
  public static <TSource> Enumerable<TSource> except(
      Enumerable<TSource> source0, Enumerable<TSource> source1) {
    Set<TSource> set = new HashSet<>();
    source0.into(set);
    try (Enumerator<TSource> os = source1.enumerator()) {
      while (os.moveNext()) {
        TSource o = os.current();
        set.remove(o);
      }
      return Linq4j.asEnumerable(set);
    }
  }

  /**
   * Produces the set difference of two sequences by
   * using the specified {@code EqualityComparer<TSource>} to compare
   * values.
   */
  public static <TSource> Enumerable<TSource> except(
      Enumerable<TSource> source0, Enumerable<TSource> source1,
      EqualityComparer<TSource> comparer) {
    if (comparer == Functions.identityComparer()) {
      return except(source0, source1);
    }
    Set<Wrapped<TSource>> set = new HashSet<>();
    Function1<TSource, Wrapped<TSource>> wrapper = wrapperFor(comparer);
    source0.select(wrapper).into(set);
    try (Enumerator<Wrapped<TSource>> os =
             source1.select(wrapper).enumerator()) {
      while (os.moveNext()) {
        Wrapped<TSource> o = os.current();
        set.remove(o);
      }
    }
    Function1<Wrapped<TSource>, TSource> unwrapper = unwrapper();
    return Linq4j.asEnumerable(set).select(unwrapper);
  }

  /**
   * Returns the first element of a sequence. (Defined
   * by Enumerable.)
   */
  public static <TSource> TSource first(Enumerable<TSource> enumerable) {
    try (Enumerator<TSource> os = enumerable.enumerator()) {
      if (os.moveNext()) {
        return os.current();
      }
      throw new NoSuchElementException();
    }
  }

  /**
   * Returns the first element in a sequence that
   * satisfies a specified condition.
   */
  public static <TSource> TSource first(Enumerable<TSource> enumerable,
      Predicate1<TSource> predicate) {
    for (TSource o : enumerable) {
      if (predicate.apply(o)) {
        return o;
      }
    }
    throw new NoSuchElementException();
  }

  /**
   * Returns the first element of a sequence, or a
   * default value if the sequence contains no elements.
   */
  public static <TSource> TSource firstOrDefault(
        Enumerable<TSource> enumerable) {
    try (Enumerator<TSource> os = enumerable.enumerator()) {
      if (os.moveNext()) {
        return os.current();
      }
      return null;
    }
  }

  /**
   * Returns the first element of the sequence that
   * satisfies a condition or a default value if no such element is
   * found.
   */
  public static <TSource> TSource firstOrDefault(Enumerable<TSource> enumerable,
      Predicate1<TSource> predicate) {
    for (TSource o : enumerable) {
      if (predicate.apply(o)) {
        return o;
      }
    }
    return null;
  }

  /**
   * Groups the elements of a sequence according to a
   * specified key selector function.
   */
  public static <TSource, TKey> Enumerable<Grouping<TKey, TSource>> groupBy(
      final Enumerable<TSource> enumerable,
      final Function1<TSource, TKey> keySelector) {
    return enumerable.toLookup(keySelector);
  }

  /**
   * Groups the elements of a sequence according to a
   * specified key selector function and compares the keys by using
   * a specified comparer.
   */
  public static <TSource, TKey> Enumerable<Grouping<TKey, TSource>> groupBy(
      Enumerable<TSource> enumerable, Function1<TSource, TKey> keySelector,
      EqualityComparer<TKey> comparer) {
    return enumerable.toLookup(keySelector, comparer);
  }

  /**
   * Groups the elements of a sequence according to a
   * specified key selector function and projects the elements for
   * each group by using a specified function.
   */
  public static <TSource, TKey, TElement> Enumerable<Grouping<TKey, TElement>> groupBy(
      Enumerable<TSource> enumerable, Function1<TSource, TKey> keySelector,
      Function1<TSource, TElement> elementSelector) {
    return enumerable.toLookup(keySelector, elementSelector);
  }

  /**
   * Groups the elements of a sequence according to a
   * key selector function. The keys are compared by using a
   * comparer and each group's elements are projected by using a
   * specified function.
   */
  public static <TSource, TKey, TElement> Enumerable<Grouping<TKey, TElement>> groupBy(
      Enumerable<TSource> enumerable, Function1<TSource, TKey> keySelector,
      Function1<TSource, TElement> elementSelector,
      EqualityComparer<TKey> comparer) {
    return enumerable.toLookup(keySelector, elementSelector, comparer);
  }

  /**
   * Groups the elements of a sequence according to a
   * specified key selector function and creates a result value from
   * each group and its key.
   */
  public static <TSource, TKey, TResult> Enumerable<TResult> groupBy(
      Enumerable<TSource> enumerable, Function1<TSource, TKey> keySelector,
      final Function2<TKey, Enumerable<TSource>, TResult> resultSelector) {
    return enumerable.toLookup(keySelector)
        .select(group -> resultSelector.apply(group.getKey(), group));
  }

  /**
   * Groups the elements of a sequence according to a
   * specified key selector function and creates a result value from
   * each group and its key. The keys are compared by using a
   * specified comparer.
   */
  public static <TSource, TKey, TResult> Enumerable<TResult> groupBy(
      Enumerable<TSource> enumerable, Function1<TSource, TKey> keySelector,
      final Function2<TKey, Enumerable<TSource>, TResult> resultSelector,
      EqualityComparer<TKey> comparer) {
    return enumerable.toLookup(keySelector, comparer)
        .select(group -> resultSelector.apply(group.getKey(), group));
  }

  /**
   * Groups the elements of a sequence according to a
   * specified key selector function and creates a result value from
   * each group and its key. The elements of each group are
   * projected by using a specified function.
   */
  public static <TSource, TKey, TElement, TResult> Enumerable<TResult> groupBy(
      Enumerable<TSource> enumerable, Function1<TSource, TKey> keySelector,
      Function1<TSource, TElement> elementSelector,
      final Function2<TKey, Enumerable<TElement>, TResult> resultSelector) {
    return enumerable.toLookup(keySelector, elementSelector)
        .select(group -> resultSelector.apply(group.getKey(), group));
  }

  /**
   * Groups the elements of a sequence according to a
   * specified key selector function and creates a result value from
   * each group and its key. Key values are compared by using a
   * specified comparer, and the elements of each group are
   * projected by using a specified function.
   */
  public static <TSource, TKey, TElement, TResult> Enumerable<TResult> groupBy(
      Enumerable<TSource> enumerable, Function1<TSource, TKey> keySelector,
      Function1<TSource, TElement> elementSelector,
      final Function2<TKey, Enumerable<TElement>, TResult> resultSelector,
      EqualityComparer<TKey> comparer) {
    return enumerable.toLookup(keySelector, elementSelector, comparer)
        .select(group -> resultSelector.apply(group.getKey(), group));
  }

  /**
   * Groups the elements of a sequence according to a
   * specified key selector function, initializing an accumulator for each
   * group and adding to it each time an element with the same key is seen.
   * Creates a result value from each accumulator and its key using a
   * specified function.
   */
  public static <TSource, TKey, TAccumulate, TResult> Enumerable<TResult> groupBy(
      Enumerable<TSource> enumerable, Function1<TSource, TKey> keySelector,
      Function0<TAccumulate> accumulatorInitializer,
      Function2<TAccumulate, TSource, TAccumulate> accumulatorAdder,
      final Function2<TKey, TAccumulate, TResult> resultSelector) {
    return groupBy_(new HashMap<>(), enumerable, keySelector,
        accumulatorInitializer, accumulatorAdder, resultSelector);
  }

  /**
   * Groups the elements of a sequence according to a list of
   * specified key selector functions, initializing an accumulator for each
   * group and adding to it each time an element with the same key is seen.
   * Creates a result value from each accumulator and its key using a
   * specified function.
   *
   * <p>This method exists to support SQL {@code GROUPING SETS}.
   * It does not correspond to any method in {@link Enumerable}.
   */
  public static <TSource, TKey, TAccumulate, TResult> Enumerable<TResult> groupByMultiple(
      Enumerable<TSource> enumerable, List<Function1<TSource, TKey>> keySelectors,
      Function0<TAccumulate> accumulatorInitializer,
      Function2<TAccumulate, TSource, TAccumulate> accumulatorAdder,
      final Function2<TKey, TAccumulate, TResult> resultSelector) {
    return groupByMultiple_(
        new HashMap<>(),
        enumerable,
        keySelectors,
        accumulatorInitializer,
        accumulatorAdder,
        resultSelector);
  }

  /**
   * Groups the elements of a sequence according to a
   * specified key selector function, initializing an accumulator for each
   * group and adding to it each time an element with the same key is seen.
   * Creates a result value from each accumulator and its key using a
   * specified function. Key values are compared by using a
   * specified comparer.
   */
  public static <TSource, TKey, TAccumulate, TResult> Enumerable<TResult> groupBy(
      Enumerable<TSource> enumerable, Function1<TSource, TKey> keySelector,
      Function0<TAccumulate> accumulatorInitializer,
      Function2<TAccumulate, TSource, TAccumulate> accumulatorAdder,
      Function2<TKey, TAccumulate, TResult> resultSelector,
      EqualityComparer<TKey> comparer) {
    return groupBy_(
        new WrapMap<>(
            // Java 8 cannot infer return type with HashMap::new is used
            () -> new HashMap<Wrapped<TKey>, TAccumulate>(),
          comparer),
        enumerable,
        keySelector,
        accumulatorInitializer,
        accumulatorAdder,
        resultSelector);
  }

  private static <TSource, TKey, TAccumulate, TResult> Enumerable<TResult> groupBy_(
      final Map<TKey, TAccumulate> map, Enumerable<TSource> enumerable,
      Function1<TSource, TKey> keySelector,
      Function0<TAccumulate> accumulatorInitializer,
      Function2<TAccumulate, TSource, TAccumulate> accumulatorAdder,
      final Function2<TKey, TAccumulate, TResult> resultSelector) {
    try (Enumerator<TSource> os = enumerable.enumerator()) {
      while (os.moveNext()) {
        TSource o = os.current();
        TKey key = keySelector.apply(o);
        TAccumulate accumulator = map.get(key);
        if (accumulator == null) {
          accumulator = accumulatorInitializer.apply();
          accumulator = accumulatorAdder.apply(accumulator, o);
          map.put(key, accumulator);
        } else {
          TAccumulate accumulator0 = accumulator;
          accumulator = accumulatorAdder.apply(accumulator, o);
          if (accumulator != accumulator0) {
            map.put(key, accumulator);
          }
        }
      }
    }
    return new LookupResultEnumerable<>(map, resultSelector);
  }

  private static <TSource, TKey, TAccumulate, TResult> Enumerable<TResult> groupByMultiple_(
      final Map<TKey, TAccumulate> map, Enumerable<TSource> enumerable,
      List<Function1<TSource, TKey>> keySelectors,
      Function0<TAccumulate> accumulatorInitializer,
      Function2<TAccumulate, TSource, TAccumulate> accumulatorAdder,
      final Function2<TKey, TAccumulate, TResult> resultSelector) {
    try (Enumerator<TSource> os = enumerable.enumerator()) {
      while (os.moveNext()) {
        for (Function1<TSource, TKey> keySelector : keySelectors) {
          TSource o = os.current();
          TKey key = keySelector.apply(o);
          TAccumulate accumulator = map.get(key);
          if (accumulator == null) {
            accumulator = accumulatorInitializer.apply();
            accumulator = accumulatorAdder.apply(accumulator, o);
            map.put(key, accumulator);
          } else {
            TAccumulate accumulator0 = accumulator;
            accumulator = accumulatorAdder.apply(accumulator, o);
            if (accumulator != accumulator0) {
              map.put(key, accumulator);
            }
          }
        }
      }
    }
    return new LookupResultEnumerable<>(map, resultSelector);
  }

  private static <TSource, TKey, TResult> Enumerable<TResult> groupBy_(
      final Set<TKey> map, Enumerable<TSource> enumerable,
      Function1<TSource, TKey> keySelector,
      final Function1<TKey, TResult> resultSelector) {
    try (Enumerator<TSource> os = enumerable.enumerator()) {
      while (os.moveNext()) {
        TSource o = os.current();
        TKey key = keySelector.apply(o);
        map.add(key);
      }
    }
    return Linq4j.asEnumerable(map).select(resultSelector);
  }

  /**
   * Correlates the elements of two sequences based on
   * equality of keys and groups the results. The default equality
   * comparer is used to compare keys.
   */
  public static <TSource, TInner, TKey, TResult> Enumerable<TResult> groupJoin(
      final Enumerable<TSource> outer, final Enumerable<TInner> inner,
      final Function1<TSource, TKey> outerKeySelector,
      final Function1<TInner, TKey> innerKeySelector,
      final Function2<TSource, Enumerable<TInner>, TResult> resultSelector) {
    return new AbstractEnumerable<TResult>() {
      final Map<TKey, TSource> outerMap = outer.toMap(outerKeySelector);
      final Lookup<TKey, TInner> innerLookup = inner.toLookup(innerKeySelector);
      final Enumerator<Map.Entry<TKey, TSource>> entries =
          Linq4j.enumerator(outerMap.entrySet());

      public Enumerator<TResult> enumerator() {
        return new Enumerator<TResult>() {
          public TResult current() {
            final Map.Entry<TKey, TSource> entry = entries.current();
            final Enumerable<TInner> inners = innerLookup.get(entry.getKey());
            return resultSelector.apply(entry.getValue(),
                inners == null ? Linq4j.emptyEnumerable() : inners);
          }

          public boolean moveNext() {
            return entries.moveNext();
          }

          public void reset() {
            entries.reset();
          }

          public void close() {
          }
        };
      }
    };
  }

  /**
   * Correlates the elements of two sequences based on
   * key equality and groups the results. A specified
   * {@code EqualityComparer<TSource>} is used to compare keys.
   */
  public static <TSource, TInner, TKey, TResult> Enumerable<TResult> groupJoin(
      final Enumerable<TSource> outer, final Enumerable<TInner> inner,
      final Function1<TSource, TKey> outerKeySelector,
      final Function1<TInner, TKey> innerKeySelector,
      final Function2<TSource, Enumerable<TInner>, TResult> resultSelector,
      final EqualityComparer<TKey> comparer) {
    return new AbstractEnumerable<TResult>() {
      final Map<TKey, TSource> outerMap = outer.toMap(outerKeySelector, comparer);
      final Lookup<TKey, TInner> innerLookup = inner.toLookup(innerKeySelector, comparer);
      final Enumerator<Map.Entry<TKey, TSource>> entries =
          Linq4j.enumerator(outerMap.entrySet());

      public Enumerator<TResult> enumerator() {
        return new Enumerator<TResult>() {
          public TResult current() {
            final Map.Entry<TKey, TSource> entry = entries.current();
            final Enumerable<TInner> inners = innerLookup.get(entry.getKey());
            return resultSelector.apply(entry.getValue(),
                inners == null ? Linq4j.emptyEnumerable() : inners);
          }

          public boolean moveNext() {
            return entries.moveNext();
          }

          public void reset() {
            entries.reset();
          }

          public void close() {
          }
        };
      }
    };
  }

  /**
   * Produces the set intersection of two sequences by
   * using the default equality comparer to compare values. (Defined
   * by Enumerable.)
   */
  public static <TSource> Enumerable<TSource> intersect(
      Enumerable<TSource> source0, Enumerable<TSource> source1) {
    Set<TSource> set0 = new HashSet<>();
    source0.into(set0);
    Set<TSource> set1 = new HashSet<>();
    try (Enumerator<TSource> os = source1.enumerator()) {
      while (os.moveNext()) {
        TSource o = os.current();
        if (set0.contains(o)) {
          set1.add(o);
        }
      }
    }
    return Linq4j.asEnumerable(set1);
  }

  /**
   * Produces the set intersection of two sequences by
   * using the specified {@code EqualityComparer<TSource>} to compare
   * values.
   */
  public static <TSource> Enumerable<TSource> intersect(
      Enumerable<TSource> source0, Enumerable<TSource> source1,
      EqualityComparer<TSource> comparer) {
    if (comparer == Functions.identityComparer()) {
      return intersect(source0, source1);
    }
    Set<Wrapped<TSource>> set0 = new HashSet<>();
    Function1<TSource, Wrapped<TSource>> wrapper = wrapperFor(comparer);
    source0.select(wrapper).into(set0);
    Set<Wrapped<TSource>> set1 = new HashSet<>();
    try (Enumerator<Wrapped<TSource>> os = source1.select(wrapper).enumerator()) {
      while (os.moveNext()) {
        Wrapped<TSource> o = os.current();
        if (set0.contains(o)) {
          set1.add(o);
        }
      }
    }
    Function1<Wrapped<TSource>, TSource> unwrapper = unwrapper();
    return Linq4j.asEnumerable(set1).select(unwrapper);
  }

  /**
   * Correlates the elements of two sequences based on
   * matching keys. The default equality comparer is used to compare
   * keys.
   */
  public static <TSource, TInner, TKey, TResult> Enumerable<TResult> hashJoin(
      final Enumerable<TSource> outer, final Enumerable<TInner> inner,
      final Function1<TSource, TKey> outerKeySelector,
      final Function1<TInner, TKey> innerKeySelector,
      final Function2<TSource, TInner, TResult> resultSelector) {
    return hashJoin(
        outer,
        inner,
        outerKeySelector,
        innerKeySelector,
        resultSelector,
        null,
        false,
        false);
  }

  /**
   * Correlates the elements of two sequences based on
   * matching keys. A specified {@code EqualityComparer<TSource>} is used to
   * compare keys.
   */
  public static <TSource, TInner, TKey, TResult> Enumerable<TResult> hashJoin(
      Enumerable<TSource> outer, Enumerable<TInner> inner,
      Function1<TSource, TKey> outerKeySelector,
      Function1<TInner, TKey> innerKeySelector,
      Function2<TSource, TInner, TResult> resultSelector,
      EqualityComparer<TKey> comparer) {
    return hashJoin(
        outer,
        inner,
        outerKeySelector,
        innerKeySelector,
        resultSelector,
        comparer,
        false,
        false);
  }

  /**
   * Correlates the elements of two sequences based on
   * matching keys. A specified {@code EqualityComparer<TSource>} is used to
   * compare keys.
   */
  public static <TSource, TInner, TKey, TResult> Enumerable<TResult> hashJoin(
      Enumerable<TSource> outer, Enumerable<TInner> inner,
      Function1<TSource, TKey> outerKeySelector,
      Function1<TInner, TKey> innerKeySelector,
      Function2<TSource, TInner, TResult> resultSelector,
      EqualityComparer<TKey> comparer, boolean generateNullsOnLeft,
      boolean generateNullsOnRight) {
    return hashEquiJoin_(
        outer,
        inner,
        outerKeySelector,
        innerKeySelector,
        resultSelector,
        comparer,
        generateNullsOnLeft,
        generateNullsOnRight);
  }

  /**
   * Correlates the elements of two sequences based on
   * matching keys. A specified {@code EqualityComparer<TSource>} is used to
   * compare keys.A predicate is used to filter the join result per-row.
   */
  public static <TSource, TInner, TKey, TResult> Enumerable<TResult> hashJoin(
      Enumerable<TSource> outer, Enumerable<TInner> inner,
      Function1<TSource, TKey> outerKeySelector,
      Function1<TInner, TKey> innerKeySelector,
      Function2<TSource, TInner, TResult> resultSelector,
      EqualityComparer<TKey> comparer, boolean generateNullsOnLeft,
      boolean generateNullsOnRight,
      Predicate2<TSource, TInner> predicate) {
    if (predicate == null) {
      return hashEquiJoin_(
          outer,
          inner,
          outerKeySelector,
          innerKeySelector,
          resultSelector,
          comparer,
          generateNullsOnLeft,
          generateNullsOnRight);
    } else {
      return hashJoinWithPredicate_(
          outer,
          inner,
          outerKeySelector,
          innerKeySelector,
          resultSelector,
          comparer,
          generateNullsOnLeft,
          generateNullsOnRight, predicate);
    }
  }

  /** Implementation of join that builds the right input and probes with the
   * left. */
  private static <TSource, TInner, TKey, TResult> Enumerable<TResult> hashEquiJoin_(
      final Enumerable<TSource> outer, final Enumerable<TInner> inner,
      final Function1<TSource, TKey> outerKeySelector,
      final Function1<TInner, TKey> innerKeySelector,
      final Function2<TSource, TInner, TResult> resultSelector,
      final EqualityComparer<TKey> comparer, final boolean generateNullsOnLeft,
      final boolean generateNullsOnRight) {
    return new AbstractEnumerable<TResult>() {
      public Enumerator<TResult> enumerator() {
        final Lookup<TKey, TInner> innerLookup =
            comparer == null
                ? inner.toLookup(innerKeySelector)
                : inner.toLookup(innerKeySelector, comparer);

        return new Enumerator<TResult>() {
          Enumerator<TSource> outers = outer.enumerator();
          Enumerator<TInner> inners = Linq4j.emptyEnumerator();
          Set<TKey> unmatchedKeys =
              generateNullsOnLeft
                  ? new HashSet<>(innerLookup.keySet())
                  : null;

          public TResult current() {
            return resultSelector.apply(outers.current(), inners.current());
          }

          public boolean moveNext() {
            for (;;) {
              if (inners.moveNext()) {
                return true;
              }
              if (!outers.moveNext()) {
                if (unmatchedKeys != null) {
                  // We've seen everything else. If we are doing a RIGHT or FULL
                  // join (leftNull = true) there are any keys which right but
                  // not the left.
                  List<TInner> list = new ArrayList<>();
                  for (TKey key : unmatchedKeys) {
                    for (TInner tInner : innerLookup.get(key)) {
                      list.add(tInner);
                    }
                  }
                  inners = Linq4j.enumerator(list);
                  outers.close();
                  outers = Linq4j.singletonNullEnumerator();
                  outers.moveNext();
                  unmatchedKeys = null; // don't do the 'leftovers' again
                  continue;
                }
                return false;
              }
              final TSource outer = outers.current();
              final Enumerable<TInner> innerEnumerable;
              if (outer == null) {
                innerEnumerable = null;
              } else {
                final TKey outerKey = outerKeySelector.apply(outer);
                if (outerKey == null) {
                  innerEnumerable = null;
                } else {
                  if (unmatchedKeys != null) {
                    unmatchedKeys.remove(outerKey);
                  }
                  innerEnumerable = innerLookup.get(outerKey);
                }
              }
              if (innerEnumerable == null
                  || !innerEnumerable.any()) {
                if (generateNullsOnRight) {
                  inners = Linq4j.singletonNullEnumerator();
                } else {
                  inners = Linq4j.emptyEnumerator();
                }
              } else {
                inners = innerEnumerable.enumerator();
              }
            }
          }

          public void reset() {
            outers.reset();
          }

          public void close() {
            outers.close();
          }
        };
      }
    };
  }

  /** Implementation of join that builds the right input and probes with the
   * left */
  private static <TSource, TInner, TKey, TResult> Enumerable<TResult> hashJoinWithPredicate_(
      final Enumerable<TSource> outer, final Enumerable<TInner> inner,
      final Function1<TSource, TKey> outerKeySelector,
      final Function1<TInner, TKey> innerKeySelector,
      final Function2<TSource, TInner, TResult> resultSelector,
      final EqualityComparer<TKey> comparer, final boolean generateNullsOnLeft,
      final boolean generateNullsOnRight, final Predicate2<TSource, TInner> predicate) {

    return new AbstractEnumerable<TResult>() {
      public Enumerator<TResult> enumerator() {
        /**
         * the innerToLookUp will refer the inner , if current join
         * is a right join, we should figure out the right list first, if
         * not, then keep the original inner here.
         */
        final Enumerable<TInner> innerToLookUp = generateNullsOnLeft
            ? Linq4j.asEnumerable(inner.toList())
            : inner;

        final Lookup<TKey, TInner> innerLookup =
            comparer == null
                ? innerToLookUp.toLookup(innerKeySelector)
                : innerToLookUp
                    .toLookup(innerKeySelector, comparer);

        return new Enumerator<TResult>() {
          Enumerator<TSource> outers = outer.enumerator();
          Enumerator<TInner> inners = Linq4j.emptyEnumerator();
          List<TInner> innersUnmatched =
              generateNullsOnLeft
                  ? new ArrayList<>(innerToLookUp.toList())
                  : null;

          public TResult current() {
            return resultSelector.apply(outers.current(), inners.current());
          }

          public boolean moveNext() {
            for (;;) {
              if (inners.moveNext()) {
                return true;
              }
              if (!outers.moveNext()) {
                if (innersUnmatched != null) {
                  inners = Linq4j.enumerator(innersUnmatched);
                  outers.close();
                  outers = Linq4j.singletonNullEnumerator();
                  outers.moveNext();
                  innersUnmatched = null; // don't do the 'leftovers' again
                  continue;
                }
                return false;
              }
              final TSource outer = outers.current();
              Enumerable<TInner> innerEnumerable;
              if (outer == null) {
                innerEnumerable = null;
              } else {
                final TKey outerKey = outerKeySelector.apply(outer);
                if (outerKey == null) {
                  innerEnumerable = null;
                } else {
                  innerEnumerable = innerLookup.get(outerKey);
                  //apply predicate to filter per-row
                  if (innerEnumerable != null) {
                    final List<TInner> matchedInners = new ArrayList<>();
                    try (Enumerator<TInner> innerEnumerator =
                        innerEnumerable.enumerator()) {
                      while (innerEnumerator.moveNext()) {
                        final TInner inner = innerEnumerator.current();
                        if (predicate.apply(outer, inner)) {
                          matchedInners.add(inner);
                        }
                      }
                    }
                    innerEnumerable = Linq4j.asEnumerable(matchedInners);
                    if (innersUnmatched != null) {
                      innersUnmatched.removeAll(matchedInners);
                    }
                  }
                }
              }
              if (innerEnumerable == null
                  || !innerEnumerable.any()) {
                if (generateNullsOnRight) {
                  inners = Linq4j.singletonNullEnumerator();
                } else {
                  inners = Linq4j.emptyEnumerator();
                }
              } else {
                inners = innerEnumerable.enumerator();
              }
            }
          }

          public void reset() {
            outers.reset();
          }

          public void close() {
            outers.close();
          }
        };
      }
    };
  }

  /**
   * For each row of the {@code outer} enumerable returns the correlated rows
   * from the {@code inner} enumerable.
   */
  public static <TSource, TInner, TResult> Enumerable<TResult> correlateJoin(
      final JoinType joinType, final Enumerable<TSource> outer,
      final Function1<TSource, Enumerable<TInner>> inner,
      final Function2<TSource, TInner, TResult> resultSelector) {
    if (joinType == JoinType.RIGHT || joinType == JoinType.FULL) {
      throw new IllegalArgumentException("JoinType " + joinType + " is not valid for correlation");
    }

    return new AbstractEnumerable<TResult>() {
      public Enumerator<TResult> enumerator() {
        return new Enumerator<TResult>() {
          private Enumerator<TSource> outerEnumerator = outer.enumerator();
          private Enumerator<TInner> innerEnumerator;
          TSource outerValue;
          TInner innerValue;
          int state = 0; // 0 -- moving outer, 1 moving inner;

          public TResult current() {
            return resultSelector.apply(outerValue, innerValue);
          }

          public boolean moveNext() {
            while (true) {
              switch (state) {
              case 0:
                // move outer
                if (!outerEnumerator.moveNext()) {
                  return false;
                }
                outerValue = outerEnumerator.current();
                // initial move inner
                Enumerable<TInner> innerEnumerable = inner.apply(outerValue);
                if (innerEnumerable == null) {
                  innerEnumerable = Linq4j.emptyEnumerable();
                }
                if (innerEnumerator != null) {
                  innerEnumerator.close();
                }
                innerEnumerator = innerEnumerable.enumerator();
                if (innerEnumerator.moveNext()) {
                  switch (joinType) {
                  case ANTI:
                    // For anti-join need to try next outer row
                    // Current does not match
                    continue;
                  case SEMI:
                    return true; // current row matches
                  }
                  // INNER and LEFT just return result
                  innerValue = innerEnumerator.current();
                  state = 1; // iterate over inner results
                  return true;
                }
                // No match detected
                innerValue = null;
                switch (joinType) {
                case LEFT:
                case ANTI:
                  return true;
                }
                // For INNER and LEFT need to find another outer row
                continue;
              case 1:
                // subsequent move inner
                if (innerEnumerator.moveNext()) {
                  innerValue = innerEnumerator.current();
                  return true;
                }
                state = 0;
                // continue loop, move outer
              }
            }
          }

          public void reset() {
            state = 0;
            outerEnumerator.reset();
            closeInner();
          }

          public void close() {
            outerEnumerator.close();
            closeInner();
            outerValue = null;
          }

          private void closeInner() {
            innerValue = null;
            if (innerEnumerator != null) {
              innerEnumerator.close();
              innerEnumerator = null;
            }
          }
        };
      }
    };
  }

  /**
   * Returns the last element of a sequence. (Defined
   * by Enumerable.)
   */
  public static <TSource> TSource last(Enumerable<TSource> enumerable) {
    final ListEnumerable<TSource> list = enumerable instanceof ListEnumerable
        ? ((ListEnumerable<TSource>) enumerable)
        : null;
    if (list != null) {
      final List<TSource> rawList = list.toList();
      final int count = rawList.size();
      if (count > 0) {
        return rawList.get(count - 1);
      }
    } else {
      try (Enumerator<TSource> os = enumerable.enumerator()) {
        if (os.moveNext()) {
          TSource result;
          do {
            result = os.current();
          } while (os.moveNext());
          return result;
        }
      }
    }
    throw new NoSuchElementException();
  }

  /**
   * <p>Fetches blocks of size {@code batchSize} from {@code outer},
   * storing each block into a list ({@code outerValues}).
   * For each block, it uses the {@code inner} function to
   * obtain an enumerable with the correlated rows from the right (inner) input.</p>
   *
   * <p>Each result present in the {@code innerEnumerator} has matched at least one
   * value from the block {@code outerValues}.
   * At this point a mini nested loop is performed between the outer values
   * and inner values using the {@code predicate} to find out the actual matching join results.</p>
   *
   * <p>In order to optimize this mini nested loop, during the first iteration
   * (the first value from {@code outerValues}) we use the {@code innerEnumerator}
   * to compare it to inner rows, and at the same time we fill a list ({@code innerValues})
   * with said {@code innerEnumerator} rows. In the subsequent iterations
   * (2nd, 3rd, etc. value from {@code outerValues}) the list {@code innerValues} is used,
   * since it contains all the {@code innerEnumerator} values,
   * which were stored in the first iteration.</p>
   */
  public static <TSource, TInner, TResult> Enumerable<TResult> correlateBatchJoin(
      final JoinType joinType,
      final Enumerable<TSource> outer,
      final Function1<List<TSource>, Enumerable<TInner>> inner,
      final Function2<TSource, TInner, TResult> resultSelector,
      final Predicate2<TSource, TInner> predicate,
      final int batchSize) {
    return new AbstractEnumerable<TResult>() {
      @Override public Enumerator<TResult> enumerator() {
        return new Enumerator<TResult>() {
          Enumerator<TSource> outerEnumerator = outer.enumerator();
          List<TSource> outerValues = new ArrayList<>(batchSize);
          List<TInner> innerValues = new ArrayList<>();
          TSource outerValue;
          TInner innerValue;
          Enumerable<TInner> innerEnumerable;
          Enumerator<TInner> innerEnumerator;
          boolean innerEnumHasNext = false;
          boolean atLeastOneResult = false;
          int i = -1; // outer position
          int j = -1; // inner position

          @Override public TResult current() {
            return resultSelector.apply(outerValue, innerValue);
          }

          @Override public boolean moveNext() {
            while (true) {
              // Fetch a new batch
              if (i == outerValues.size() || i == -1) {
                i = 0;
                j = 0;
                outerValues.clear();
                innerValues.clear();
                while (outerValues.size() < batchSize && outerEnumerator.moveNext()) {
                  TSource tSource = outerEnumerator.current();
                  outerValues.add(tSource);
                }
                if (outerValues.isEmpty()) {
                  return false;
                }
                innerEnumerable = inner.apply(new AbstractList<TSource>() {
                  // If the last batch isn't complete fill it with the first value
                  // No harm since it's a disjunction
                  @Override public TSource get(final int index) {
                    return index < outerValues.size() ? outerValues.get(index) : outerValues.get(0);
                  }
                  @Override public int size() {
                    return batchSize;
                  }
                });
                if (innerEnumerable == null) {
                  innerEnumerable = Linq4j.emptyEnumerable();
                }
                innerEnumerator = innerEnumerable.enumerator();
                innerEnumHasNext = innerEnumerator.moveNext();

                // If no inner values skip the whole batch
                // in case of SEMI and INNER join
                if (!innerEnumHasNext
                    && (joinType == JoinType.SEMI || joinType == JoinType.INNER)) {
                  i = outerValues.size();
                  continue;
                }
              }
              if (innerHasNext()) {
                outerValue = outerValues.get(i); // get current outer value
                nextInnerValue();
                // Compare current block row to current inner value
                if (predicate.apply(outerValue, innerValue)) {
                  atLeastOneResult = true;
                  // Skip the rest of inner values in case of
                  // ANTI and SEMI when a match is found
                  if (joinType == JoinType.ANTI || joinType == JoinType.SEMI) {
                    // Two ways of skipping inner values,
                    // enumerator way and ArrayList way
                    if (i == 0) {
                      while (innerEnumHasNext) {
                        innerValues.add(innerEnumerator.current());
                        innerEnumHasNext = innerEnumerator.moveNext();
                      }
                    } else {
                      j = innerValues.size();
                    }
                    if (joinType == JoinType.ANTI) {
                      continue;
                    }
                  }
                  return true;
                }
              } else { // End of inner
                if (!atLeastOneResult
                    && (joinType == JoinType.LEFT
                    || joinType == JoinType.ANTI)) {
                  outerValue = outerValues.get(i); // get current outer value
                  innerValue = null;
                  nextOuterValue();
                  return true;
                }
                nextOuterValue();
              }
            }
          }

          public void nextOuterValue() {
            i++; // next outerValue
            j = 0; // rewind innerValues
            atLeastOneResult = false;
          }

          private void nextInnerValue() {
            if (i == 0) {
              innerValue = innerEnumerator.current();
              innerValues.add(innerValue);
              innerEnumHasNext = innerEnumerator.moveNext(); // next enumerator inner value
            } else {
              innerValue = innerValues.get(j++); // next ArrayList inner value
            }
          }

          private boolean innerHasNext() {
            return i == 0 ? innerEnumHasNext : j < innerValues.size();
          }

          @Override public void reset() {
            outerEnumerator.reset();
            innerValue = null;
            outerValue = null;
            outerValues.clear();
            innerValues.clear();
            atLeastOneResult = false;
            i = -1;
          }

          @Override public void close() {
            outerEnumerator.close();
            if (innerEnumerator != null) {
              innerEnumerator.close();
            }
            outerValue = null;
            innerValue = null;
          }
        };
      }
    };
  }

  /**
   * Returns elements of {@code outer} for which there is a member of
   * {@code inner} with a matching key.
   */
  public static <TSource, TInner, TKey> Enumerable<TSource> semiJoin(
      final Enumerable<TSource> outer, final Enumerable<TInner> inner,
      final Function1<TSource, TKey> outerKeySelector,
      final Function1<TInner, TKey> innerKeySelector) {
    return semiEquiJoin_(outer, inner, outerKeySelector, innerKeySelector, null,
        false);
  }

  public static <TSource, TInner, TKey> Enumerable<TSource> semiJoin(
      final Enumerable<TSource> outer, final Enumerable<TInner> inner,
      final Function1<TSource, TKey> outerKeySelector,
      final Function1<TInner, TKey> innerKeySelector,
      final EqualityComparer<TKey> comparer) {
    return semiEquiJoin_(outer, inner, outerKeySelector, innerKeySelector, comparer,
        false);
  }

  public static <TSource, TInner, TKey> Enumerable<TSource> semiJoin(
      final Enumerable<TSource> outer, final Enumerable<TInner> inner,
      final Function1<TSource, TKey> outerKeySelector,
      final Function1<TInner, TKey> innerKeySelector,
      final EqualityComparer<TKey> comparer,
      final Predicate2<TSource, TInner> nonEquiPredicate) {
    return semiJoin(outer, inner, outerKeySelector,
        innerKeySelector, comparer,
        false, nonEquiPredicate);
  }

  /**
   * Returns elements of {@code outer} for which there is NOT a member of
   * {@code inner} with a matching key.
   */
  public static <TSource, TInner, TKey> Enumerable<TSource> antiJoin(
      final Enumerable<TSource> outer, final Enumerable<TInner> inner,
      final Function1<TSource, TKey> outerKeySelector,
      final Function1<TInner, TKey> innerKeySelector) {
    return semiEquiJoin_(outer, inner, outerKeySelector, innerKeySelector, null,
        true);
  }

  public static <TSource, TInner, TKey> Enumerable<TSource> antiJoin(
      final Enumerable<TSource> outer, final Enumerable<TInner> inner,
      final Function1<TSource, TKey> outerKeySelector,
      final Function1<TInner, TKey> innerKeySelector,
      final EqualityComparer<TKey> comparer) {
    return semiEquiJoin_(outer, inner, outerKeySelector, innerKeySelector, comparer,
        true);
  }

  public static <TSource, TInner, TKey> Enumerable<TSource> antiJoin(
      final Enumerable<TSource> outer, final Enumerable<TInner> inner,
      final Function1<TSource, TKey> outerKeySelector,
      final Function1<TInner, TKey> innerKeySelector,
      final EqualityComparer<TKey> comparer,
      final Predicate2<TSource, TInner> nonEquiPredicate) {
    return semiJoin(outer, inner, outerKeySelector,
        innerKeySelector, comparer,
        true, nonEquiPredicate);
  }

  /**
   * Returns elements of {@code outer} for which there is (semi-join) / is not (anti-semi-join)
   * a member of {@code inner} with a matching key. A specified
   * {@code EqualityComparer<TSource>} is used to compare keys.
   * A predicate is used to filter the join result per-row.
   */
  public static <TSource, TInner, TKey> Enumerable<TSource> semiJoin(
      final Enumerable<TSource> outer, final Enumerable<TInner> inner,
      final Function1<TSource, TKey> outerKeySelector,
      final Function1<TInner, TKey> innerKeySelector,
      final EqualityComparer<TKey> comparer,
      final boolean anti,
      final Predicate2<TSource, TInner> nonEquiPredicate) {
    if (nonEquiPredicate == null) {
      return semiEquiJoin_(outer, inner, outerKeySelector, innerKeySelector,
          comparer,
          anti);
    } else {
      return semiJoinWithPredicate_(outer, inner, outerKeySelector,
          innerKeySelector,
          comparer,
          anti, nonEquiPredicate);
    }
  }

  private static <TSource, TInner, TKey> Enumerable<TSource> semiJoinWithPredicate_(
      final Enumerable<TSource> outer, final Enumerable<TInner> inner,
      final Function1<TSource, TKey> outerKeySelector,
      final Function1<TInner, TKey> innerKeySelector,
      final EqualityComparer<TKey> comparer,
      final boolean anti,
      final Predicate2<TSource, TInner> nonEquiPredicate) {

    return new AbstractEnumerable<TSource>() {
      public Enumerator<TSource> enumerator() {
        // CALCITE-2909 Delay the computation of the innerLookup until the
        // moment when we are sure
        // that it will be really needed, i.e. when the first outer
        // enumerator item is processed
        final Supplier<Lookup<TKey, TInner>> innerLookup = Suppliers.memoize(
            () ->
                comparer == null
                    ? inner.toLookup(innerKeySelector)
                    : inner.toLookup(innerKeySelector, comparer));

        final Predicate1<TSource> predicate = v0 -> {
          TKey key = outerKeySelector.apply(v0);
          if (!innerLookup.get().containsKey(key)) {
            return anti;
          }
          Enumerable<TInner> innersOfKey = innerLookup.get().get(key);
          try (Enumerator<TInner> os = innersOfKey.enumerator()) {
            while (os.moveNext()) {
              TInner v1 = os.current();
              if (nonEquiPredicate.apply(v0, v1)) {
                return !anti;
              }
            }
            return anti;
          }
        };
        return EnumerableDefaults.where(outer.enumerator(), predicate);
      }
    };
  }

  /**
   * Returns elements of {@code outer} for which there is (semi-join) / is not (anti-semi-join)
   * a member of {@code inner} with a matching key. A specified
   * {@code EqualityComparer<TSource>} is used to compare keys.
   */
  private static <TSource, TInner, TKey> Enumerable<TSource> semiEquiJoin_(
      final Enumerable<TSource> outer, final Enumerable<TInner> inner,
      final Function1<TSource, TKey> outerKeySelector,
      final Function1<TInner, TKey> innerKeySelector,
      final EqualityComparer<TKey> comparer,
      final boolean anti) {
    return new AbstractEnumerable<TSource>() {
      public Enumerator<TSource> enumerator() {
        // CALCITE-2909 Delay the computation of the innerLookup until the moment when we are sure
        // that it will be really needed, i.e. when the first outer enumerator item is processed
        final Supplier<Enumerable<TKey>> innerLookup = Suppliers.memoize(() ->
            comparer == null
                ? inner.select(innerKeySelector).distinct()
                : inner.select(innerKeySelector).distinct(comparer));

        final Predicate1<TSource> predicate = anti
            ? v0 -> !innerLookup.get().contains(outerKeySelector.apply(v0))
            : v0 -> innerLookup.get().contains(outerKeySelector.apply(v0));

        return EnumerableDefaults.where(outer.enumerator(), predicate);
      }
    };
  }

  /**
   * Correlates the elements of two sequences based on a predicate.
   */
  public static <TSource, TInner, TResult> Enumerable<TResult> nestedLoopJoin(
      final Enumerable<TSource> outer, final Enumerable<TInner> inner,
      final Predicate2<TSource, TInner> predicate,
      Function2<TSource, TInner, TResult> resultSelector,
      final JoinType joinType) {
    // Building the result as a list is easy but hogs memory. We should iterate.
    final boolean generateNullsOnLeft = joinType.generatesNullsOnLeft();
    final boolean generateNullsOnRight = joinType.generatesNullsOnRight();
    final List<TResult> result = new ArrayList<>();
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
          if (joinType == JoinType.ANTI) {
            break;
          } else {
            if (rightUnmatched != null) {
              rightUnmatched.remove(right);
            }
            result.add(resultSelector.apply(left, right));
          }
        }
      }
      if (leftMatchCount == 0 && (generateNullsOnRight || joinType == JoinType.ANTI)) {
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

  /** Joins two inputs that are sorted on the key. */
  public static <TSource, TInner, TKey extends Comparable<TKey>, TResult> Enumerable<TResult>
      mergeJoin(final Enumerable<TSource> outer,
      final Enumerable<TInner> inner,
      final Function1<TSource, TKey> outerKeySelector,
      final Function1<TInner, TKey> innerKeySelector,
      final Function2<TSource, TInner, TResult> resultSelector,
      boolean generateNullsOnLeft,
      boolean generateNullsOnRight) {
    if (generateNullsOnLeft) {
      throw new UnsupportedOperationException(
        "not implemented, mergeJoin with generateNullsOnLeft");
    }
    if (generateNullsOnRight) {
      throw new UnsupportedOperationException(
        "not implemented, mergeJoin with generateNullsOnRight");
    }
    return new AbstractEnumerable<TResult>() {
      public Enumerator<TResult> enumerator() {
        return new MergeJoinEnumerator<>(outer.enumerator(),
            inner.enumerator(), outerKeySelector, innerKeySelector,
            resultSelector);
      }
    };
  }

  /**
   * Returns the last element of a sequence that
   * satisfies a specified condition.
   */
  public static <TSource> TSource last(Enumerable<TSource> enumerable,
      Predicate1<TSource> predicate) {
    final ListEnumerable<TSource> list = enumerable instanceof ListEnumerable
        ? ((ListEnumerable<TSource>) enumerable)
        : null;
    if (list != null) {
      final List<TSource> rawList = list.toList();
      final int count = rawList.size();
      for (int i = count - 1; i >= 0; --i) {
        TSource result = rawList.get(i);
        if (predicate.apply(result)) {
          return result;
        }
      }
    } else {
      try (Enumerator<TSource> os = enumerable.enumerator()) {
        while (os.moveNext()) {
          TSource result = os.current();
          if (predicate.apply(result)) {
            while (os.moveNext()) {
              TSource element = os.current();
              if (predicate.apply(element)) {
                result = element;
              }
            }
            return result;
          }
        }
      }
    }
    throw new NoSuchElementException();
  }

  /**
   * Returns the last element of a sequence, or a
   * default value if the sequence contains no elements.
   */
  public static <TSource> TSource lastOrDefault(
      Enumerable<TSource> enumerable) {
    final ListEnumerable<TSource> list = enumerable instanceof ListEnumerable
        ? ((ListEnumerable<TSource>) enumerable)
        : null;
    if (list != null) {
      final List<TSource> rawList = list.toList();
      final int count = rawList.size();
      if (count > 0) {
        return rawList.get(count - 1);
      }
    } else {
      try (Enumerator<TSource> os = enumerable.enumerator()) {
        if (os.moveNext()) {
          TSource result;
          do {
            result = os.current();
          } while (os.moveNext());
          return result;
        }
      }
    }
    return null;
  }

  /**
   * Returns the last element of a sequence that
   * satisfies a condition or a default value if no such element is
   * found.
   */
  public static <TSource> TSource lastOrDefault(Enumerable<TSource> enumerable,
      Predicate1<TSource> predicate) {
    final ListEnumerable<TSource> list = enumerable instanceof ListEnumerable
        ? ((ListEnumerable<TSource>) enumerable)
        : null;
    if (list != null) {
      final List<TSource> rawList = list.toList();
      final int count = rawList.size();
      for (int i = count - 1; i >= 0; --i) {
        TSource result = rawList.get(i);
        if (predicate.apply(result)) {
          return result;
        }
      }
    } else {
      try (Enumerator<TSource> os = enumerable.enumerator()) {
        while (os.moveNext()) {
          TSource result = os.current();
          if (predicate.apply(result)) {
            while (os.moveNext()) {
              TSource element = os.current();
              if (predicate.apply(element)) {
                result = element;
              }
            }
            return result;
          }
        }
      }
    }
    return null;
  }

  /**
   * Returns an long that represents the total number
   * of elements in a sequence.
   */
  public static <TSource> long longCount(Enumerable<TSource> source) {
    return longCount(source, Functions.truePredicate1());
  }

  /**
   * Returns an long that represents how many elements
   * in a sequence satisfy a condition.
   */
  public static <TSource> long longCount(Enumerable<TSource> enumerable,
      Predicate1<TSource> predicate) {
    // Shortcut if this is a collection and the predicate is always true.
    if (predicate == Predicate1.TRUE && enumerable instanceof Collection) {
      return ((Collection) enumerable).size();
    }
    int n = 0;
    try (Enumerator<TSource> os = enumerable.enumerator()) {
      while (os.moveNext()) {
        TSource o = os.current();
        if (predicate.apply(o)) {
          ++n;
        }
      }
    }
    return n;
  }

  /**
   * Returns the maximum value in a generic
   * sequence.
   */
  public static <TSource extends Comparable<TSource>> TSource max(
      Enumerable<TSource> source) {
    Function2<TSource, TSource, TSource> max = maxFunction();
    return aggregate(source, null, max);
  }

  /**
   * Invokes a transform function on each element of a
   * sequence and returns the maximum Decimal value.
   */
  public static <TSource> BigDecimal max(Enumerable<TSource> source,
      BigDecimalFunction1<TSource> selector) {
    Function2<BigDecimal, BigDecimal, BigDecimal> max = maxFunction();
    return aggregate(source.select(selector), null, max);
  }

  /**
   * Invokes a transform function on each element of a
   * sequence and returns the maximum nullable Decimal
   * value.
   */
  public static <TSource> BigDecimal max(Enumerable<TSource> source,
      NullableBigDecimalFunction1<TSource> selector) {
    Function2<BigDecimal, BigDecimal, BigDecimal> max = maxFunction();
    return aggregate(source.select(selector), null, max);
  }

  /**
   * Invokes a transform function on each element of a
   * sequence and returns the maximum Double value.
   */
  public static <TSource> double max(Enumerable<TSource> source,
      DoubleFunction1<TSource> selector) {
    return aggregate(source.select(adapt(selector)), null,
        Extensions.DOUBLE_MAX);
  }

  /**
   * Invokes a transform function on each element of a
   * sequence and returns the maximum nullable Double
   * value.
   */
  public static <TSource> Double max(Enumerable<TSource> source,
      NullableDoubleFunction1<TSource> selector) {
    return aggregate(source.select(selector), null, Extensions.DOUBLE_MAX);
  }

  /**
   * Invokes a transform function on each element of a
   * sequence and returns the maximum int value.
   */
  public static <TSource> int max(Enumerable<TSource> source,
      IntegerFunction1<TSource> selector) {
    return aggregate(source.select(adapt(selector)), null,
        Extensions.INTEGER_MAX);
  }

  /**
   * Invokes a transform function on each element of a
   * sequence and returns the maximum nullable int value. (Defined
   * by Enumerable.)
   */
  public static <TSource> Integer max(Enumerable<TSource> source,
      NullableIntegerFunction1<TSource> selector) {
    return aggregate(source.select(selector), null, Extensions.INTEGER_MAX);
  }

  /**
   * Invokes a transform function on each element of a
   * sequence and returns the maximum long value.
   */
  public static <TSource> long max(Enumerable<TSource> source,
      LongFunction1<TSource> selector) {
    return aggregate(source.select(adapt(selector)), null, Extensions.LONG_MAX);
  }

  /**
   * Invokes a transform function on each element of a
   * sequence and returns the maximum nullable long value. (Defined
   * by Enumerable.)
   */
  public static <TSource> Long max(Enumerable<TSource> source,
      NullableLongFunction1<TSource> selector) {
    return aggregate(source.select(selector), null, Extensions.LONG_MAX);
  }

  /**
   * Invokes a transform function on each element of a
   * sequence and returns the maximum Float value.
   */
  public static <TSource> float max(Enumerable<TSource> source,
      FloatFunction1<TSource> selector) {
    return aggregate(source.select(adapt(selector)), null,
        Extensions.FLOAT_MAX);
  }

  /**
   * Invokes a transform function on each element of a
   * sequence and returns the maximum nullable Float
   * value.
   */
  public static <TSource> Float max(Enumerable<TSource> source,
      NullableFloatFunction1<TSource> selector) {
    return aggregate(source.select(selector), null, Extensions.FLOAT_MAX);
  }

  /**
   * Invokes a transform function on each element of a
   * generic sequence and returns the maximum resulting
   * value.
   */
  public static <TSource, TResult extends Comparable<TResult>> TResult max(
      Enumerable<TSource> source, Function1<TSource, TResult> selector) {
    Function2<TResult, TResult, TResult> max = maxFunction();
    return aggregate(source.select(selector), null, max);
  }

  /**
   * Returns the minimum value in a generic
   * sequence.
   */
  public static <TSource extends Comparable<TSource>> TSource min(
      Enumerable<TSource> source) {
    Function2<TSource, TSource, TSource> min = minFunction();
    return aggregate(source, null, min);
  }

  @SuppressWarnings("unchecked")
  private static <TSource extends Comparable<TSource>> Function2<TSource, TSource, TSource>
      minFunction() {
    return (Function2<TSource, TSource, TSource>) (Function2) Extensions.COMPARABLE_MIN;
  }

  @SuppressWarnings("unchecked")
  private static <TSource extends Comparable<TSource>> Function2<TSource, TSource, TSource>
      maxFunction() {
    return (Function2<TSource, TSource, TSource>) (Function2) Extensions.COMPARABLE_MAX;
  }

  /**
   * Invokes a transform function on each element of a
   * sequence and returns the minimum Decimal value.
   */
  public static <TSource> BigDecimal min(Enumerable<TSource> source,
      BigDecimalFunction1<TSource> selector) {
    Function2<BigDecimal, BigDecimal, BigDecimal> min = minFunction();
    return aggregate(source.select(selector), null, min);
  }

  /**
   * Invokes a transform function on each element of a
   * sequence and returns the minimum nullable Decimal
   * value.
   */
  public static <TSource> BigDecimal min(Enumerable<TSource> source,
      NullableBigDecimalFunction1<TSource> selector) {
    Function2<BigDecimal, BigDecimal, BigDecimal> min = minFunction();
    return aggregate(source.select(selector), null, min);
  }

  /**
   * Invokes a transform function on each element of a
   * sequence and returns the minimum Double value.
   */
  public static <TSource> double min(Enumerable<TSource> source,
      DoubleFunction1<TSource> selector) {
    return aggregate(source.select(adapt(selector)), null,
        Extensions.DOUBLE_MIN);
  }

  /**
   * Invokes a transform function on each element of a
   * sequence and returns the minimum nullable Double
   * value.
   */
  public static <TSource> Double min(Enumerable<TSource> source,
      NullableDoubleFunction1<TSource> selector) {
    return aggregate(source.select(selector), null, Extensions.DOUBLE_MIN);
  }

  /**
   * Invokes a transform function on each element of a
   * sequence and returns the minimum int value.
   */
  public static <TSource> int min(Enumerable<TSource> source,
      IntegerFunction1<TSource> selector) {
    return aggregate(source.select(adapt(selector)), null,
        Extensions.INTEGER_MIN);
  }

  /**
   * Invokes a transform function on each element of a
   * sequence and returns the minimum nullable int value. (Defined
   * by Enumerable.)
   */
  public static <TSource> Integer min(Enumerable<TSource> source,
      NullableIntegerFunction1<TSource> selector) {
    return aggregate(source.select(selector), null, Extensions.INTEGER_MIN);
  }

  /**
   * Invokes a transform function on each element of a
   * sequence and returns the minimum long value.
   */
  public static <TSource> long min(Enumerable<TSource> source,
      LongFunction1<TSource> selector) {
    return aggregate(source.select(adapt(selector)), null, Extensions.LONG_MIN);
  }

  /**
   * Invokes a transform function on each element of a
   * sequence and returns the minimum nullable long value. (Defined
   * by Enumerable.)
   */
  public static <TSource> Long min(Enumerable<TSource> source,
      NullableLongFunction1<TSource> selector) {
    return aggregate(source.select(selector), null, Extensions.LONG_MIN);
  }

  /**
   * Invokes a transform function on each element of a
   * sequence and returns the minimum Float value.
   */
  public static <TSource> float min(Enumerable<TSource> source,
      FloatFunction1<TSource> selector) {
    return aggregate(source.select(adapt(selector)), null,
        Extensions.FLOAT_MIN);
  }

  /**
   * Invokes a transform function on each element of a
   * sequence and returns the minimum nullable Float
   * value.
   */
  public static <TSource> Float min(Enumerable<TSource> source,
      NullableFloatFunction1<TSource> selector) {
    return aggregate(source.select(selector), null, Extensions.FLOAT_MIN);
  }

  /**
   * Invokes a transform function on each element of a
   * generic sequence and returns the minimum resulting
   * value.
   */
  public static <TSource, TResult extends Comparable<TResult>> TResult min(
      Enumerable<TSource> source, Function1<TSource, TResult> selector) {
    Function2<TResult, TResult, TResult> min = minFunction();
    return aggregate(source.select(selector), null, min);
  }

  /**
   * Filters the elements of an Enumerable based on a
   * specified type.
   *
   * <p>Analogous to LINQ's Enumerable.OfType extension method.</p>
   *
   * @param clazz Target type
   * @param <TResult> Target type
   *
   * @return Collection of T2
   */
  public static <TSource, TResult> Enumerable<TResult> ofType(
      Enumerable<TSource> enumerable, Class<TResult> clazz) {
    //noinspection unchecked
    return (Enumerable) where(enumerable,
        Functions.ofTypePredicate(clazz));
  }

  /**
   * Sorts the elements of a sequence in ascending
   * order according to a key.
   */
  public static <TSource, TKey extends Comparable> Enumerable<TSource> orderBy(
      Enumerable<TSource> source, Function1<TSource, TKey> keySelector) {
    return orderBy(source, keySelector, null);
  }

  /**
   * Sorts the elements of a sequence in ascending
   * order by using a specified comparer.
   */
  public static <TSource, TKey> Enumerable<TSource> orderBy(
      Enumerable<TSource> source, Function1<TSource, TKey> keySelector,
      Comparator<TKey> comparator) {
    // NOTE: TreeMap allows null comparator. But the caller of this method
    // must supply a comparator if the key does not extend Comparable.
    // Otherwise there will be a ClassCastException while retrieving.
    final Map<TKey, List<TSource>> map = new TreeMap<>(comparator);
    LookupImpl<TKey, TSource> lookup = toLookup_(map, source, keySelector,
        Functions.identitySelector());
    return lookup.valuesEnumerable();
  }

  /**
   * Sorts the elements of a sequence in descending
   * order according to a key.
   */
  public static <TSource, TKey extends Comparable> Enumerable<TSource> orderByDescending(
      Enumerable<TSource> source, Function1<TSource, TKey> keySelector) {
    return orderBy(source, keySelector, Collections.reverseOrder());
  }

  /**
   * Sorts the elements of a sequence in descending
   * order by using a specified comparer.
   */
  public static <TSource, TKey> Enumerable<TSource> orderByDescending(
      Enumerable<TSource> source, Function1<TSource, TKey> keySelector,
      Comparator<TKey> comparator) {
    return orderBy(source, keySelector, Collections.reverseOrder(comparator));
  }

  /**
   * Inverts the order of the elements in a
   * sequence.
   */
  public static <TSource> Enumerable<TSource> reverse(
      Enumerable<TSource> source) {
    final List<TSource> list = toList(source);
    final int n = list.size();
    return Linq4j.asEnumerable(
        new AbstractList<TSource>() {
          public TSource get(int index) {
            return list.get(n - 1 - index);
          }

          public int size() {
            return n;
          }
        });
  }

  /**
   * Projects each element of a sequence into a new form.
   */
  public static <TSource, TResult> Enumerable<TResult> select(
      final Enumerable<TSource> source,
      final Function1<TSource, TResult> selector) {
    if (selector == Functions.identitySelector()) {
      //noinspection unchecked
      return (Enumerable<TResult>) source;
    }
    return new AbstractEnumerable<TResult>() {
      public Enumerator<TResult> enumerator() {
        return new Enumerator<TResult>() {
          final Enumerator<TSource> enumerator = source.enumerator();

          public TResult current() {
            return selector.apply(enumerator.current());
          }

          public boolean moveNext() {
            return enumerator.moveNext();
          }

          public void reset() {
            enumerator.reset();
          }

          public void close() {
            enumerator.close();
          }
        };
      }
    };
  }

  /**
   * Projects each element of a sequence into a new
   * form by incorporating the element's index.
   */
  public static <TSource, TResult> Enumerable<TResult> select(
      final Enumerable<TSource> source,
      final Function2<TSource, Integer, TResult> selector) {
    return new AbstractEnumerable<TResult>() {
      public Enumerator<TResult> enumerator() {
        return new Enumerator<TResult>() {
          final Enumerator<TSource> enumerator = source.enumerator();
          int n = -1;

          public TResult current() {
            return selector.apply(enumerator.current(), n);
          }

          public boolean moveNext() {
            if (enumerator.moveNext()) {
              ++n;
              return true;
            } else {
              return false;
            }
          }

          public void reset() {
            enumerator.reset();
          }

          public void close() {
            enumerator.close();
          }
        };
      }
    };
  }

  /**
   * Projects each element of a sequence to an
   * {@code Enumerable<TSource>} and flattens the resulting sequences into one
   * sequence.
   */
  public static <TSource, TResult> Enumerable<TResult> selectMany(
      final Enumerable<TSource> source,
      final Function1<TSource, Enumerable<TResult>> selector) {
    return new AbstractEnumerable<TResult>() {
      public Enumerator<TResult> enumerator() {
        return new Enumerator<TResult>() {
          Enumerator<TSource> sourceEnumerator = source.enumerator();
          Enumerator<TResult> resultEnumerator = Linq4j.emptyEnumerator();

          public TResult current() {
            return resultEnumerator.current();
          }

          public boolean moveNext() {
            for (;;) {
              if (resultEnumerator.moveNext()) {
                return true;
              }
              if (!sourceEnumerator.moveNext()) {
                return false;
              }
              resultEnumerator = selector.apply(sourceEnumerator.current())
                  .enumerator();
            }
          }

          public void reset() {
            sourceEnumerator.reset();
            resultEnumerator = Linq4j.emptyEnumerator();
          }

          public void close() {
            sourceEnumerator.close();
            resultEnumerator.close();
          }
        };
      }
    };
  }

  /**
   * Projects each element of a sequence to an
   * {@code Enumerable<TSource>}, and flattens the resulting sequences into one
   * sequence. The index of each source element is used in the
   * projected form of that element.
   */
  public static <TSource, TResult> Enumerable<TResult> selectMany(
      final Enumerable<TSource> source,
      final Function2<TSource, Integer, Enumerable<TResult>> selector) {
    return new AbstractEnumerable<TResult>() {
      public Enumerator<TResult> enumerator() {
        return new Enumerator<TResult>() {
          int index = -1;
          Enumerator<TSource> sourceEnumerator = source.enumerator();
          Enumerator<TResult> resultEnumerator = Linq4j.emptyEnumerator();

          public TResult current() {
            return resultEnumerator.current();
          }

          public boolean moveNext() {
            for (;;) {
              if (resultEnumerator.moveNext()) {
                return true;
              }
              if (!sourceEnumerator.moveNext()) {
                return false;
              }
              index += 1;
              resultEnumerator = selector.apply(sourceEnumerator.current(), index)
                  .enumerator();
            }
          }

          public void reset() {
            sourceEnumerator.reset();
            resultEnumerator = Linq4j.emptyEnumerator();
          }

          public void close() {
            sourceEnumerator.close();
            resultEnumerator.close();
          }
        };
      }
    };
  }

  /**
   * Projects each element of a sequence to an
   * {@code Enumerable<TSource>}, flattens the resulting sequences into one
   * sequence, and invokes a result selector function on each
   * element therein. The index of each source element is used in
   * the intermediate projected form of that element.
   */
  public static <TSource, TCollection, TResult> Enumerable<TResult> selectMany(
      final Enumerable<TSource> source,
      final Function2<TSource, Integer, Enumerable<TCollection>> collectionSelector,
      final Function2<TSource, TCollection, TResult> resultSelector) {
    return new AbstractEnumerable<TResult>() {
      public Enumerator<TResult> enumerator() {
        return new Enumerator<TResult>() {
          int index = -1;
          Enumerator<TSource> sourceEnumerator = source.enumerator();
          Enumerator<TCollection> collectionEnumerator = Linq4j.emptyEnumerator();
          Enumerator<TResult> resultEnumerator = Linq4j.emptyEnumerator();

          public TResult current() {
            return resultEnumerator.current();
          }

          public boolean moveNext() {
            for (;;) {
              if (resultEnumerator.moveNext()) {
                return true;
              }
              if (!sourceEnumerator.moveNext()) {
                return false;
              }
              index += 1;
              final TSource sourceElement = sourceEnumerator.current();
              collectionEnumerator = collectionSelector.apply(sourceElement, index)
                  .enumerator();
              resultEnumerator =
                  new TransformedEnumerator<TCollection, TResult>(collectionEnumerator) {
                    protected TResult transform(TCollection collectionElement) {
                      return resultSelector.apply(sourceElement, collectionElement);
                    }
                  };
            }
          }

          public void reset() {
            sourceEnumerator.reset();
            resultEnumerator = Linq4j.emptyEnumerator();
          }

          public void close() {
            sourceEnumerator.close();
            resultEnumerator.close();
          }
        };
      }
    };
  }

  /**
   * Projects each element of a sequence to an
   * {@code Enumerable<TSource>}, flattens the resulting sequences into one
   * sequence, and invokes a result selector function on each
   * element therein.
   */
  public static <TSource, TCollection, TResult> Enumerable<TResult> selectMany(
      final Enumerable<TSource> source,
      final Function1<TSource, Enumerable<TCollection>> collectionSelector,
      final Function2<TSource, TCollection, TResult> resultSelector) {
    return new AbstractEnumerable<TResult>() {
      public Enumerator<TResult> enumerator() {
        return new Enumerator<TResult>() {
          Enumerator<TSource> sourceEnumerator = source.enumerator();
          Enumerator<TCollection> collectionEnumerator = Linq4j.emptyEnumerator();
          Enumerator<TResult> resultEnumerator = Linq4j.emptyEnumerator();

          public TResult current() {
            return resultEnumerator.current();
          }

          public boolean moveNext() {
            boolean incremented = false;
            for (;;) {
              if (resultEnumerator.moveNext()) {
                return true;
              }
              if (!sourceEnumerator.moveNext()) {
                return false;
              }
              final TSource sourceElement = sourceEnumerator.current();
              collectionEnumerator = collectionSelector.apply(sourceElement)
                  .enumerator();
              resultEnumerator =
                  new TransformedEnumerator<TCollection, TResult>(collectionEnumerator) {
                    protected TResult transform(TCollection collectionElement) {
                      return resultSelector.apply(sourceElement, collectionElement);
                    }
                  };
            }
          }

          public void reset() {
            sourceEnumerator.reset();
            resultEnumerator = Linq4j.emptyEnumerator();
          }

          public void close() {
            sourceEnumerator.close();
            resultEnumerator.close();
          }
        };
      }
    };
  }

  /**
   * Determines whether two sequences are equal by
   * comparing the elements by using the default equality comparer
   * for their type.
   */
  public static <TSource> boolean sequenceEqual(Enumerable<TSource> first,
      Enumerable<TSource> second) {
    return sequenceEqual(first, second, null);
  }

  /**
   * Determines whether two sequences are equal by
   * comparing their elements by using a specified
   * {@code EqualityComparer<TSource>}.
   */
  public static <TSource> boolean sequenceEqual(Enumerable<TSource> first,
      Enumerable<TSource> second, EqualityComparer<TSource> comparer) {
    Objects.requireNonNull(first);
    Objects.requireNonNull(second);
    if (comparer == null) {
      comparer = new EqualityComparer<TSource>() {
        public boolean equal(TSource v1, TSource v2) {
          return Objects.equals(v1, v2);
        }
        public int hashCode(TSource tSource) {
          return Objects.hashCode(tSource);
        }
      };
    }

    final CollectionEnumerable<TSource> firstCollection = first instanceof CollectionEnumerable
        ? ((CollectionEnumerable<TSource>) first)
        : null;
    if (firstCollection != null) {
      final CollectionEnumerable<TSource> secondCollection = second instanceof CollectionEnumerable
          ? ((CollectionEnumerable<TSource>) second)
          : null;
      if (secondCollection != null) {
        if (firstCollection.getCollection().size() != secondCollection.getCollection().size()) {
          return false;
        }
      }
    }

    try (Enumerator<TSource> os1 = first.enumerator();
         Enumerator<TSource> os2 = second.enumerator()) {
      while (os1.moveNext()) {
        if (!(os2.moveNext() && comparer.equal(os1.current(), os2.current()))) {
          return false;
        }
      }
      return !os2.moveNext();
    }
  }

  /**
   * Returns the only element of a sequence, and throws
   * an exception if there is not exactly one element in the
   * sequence.
   */
  public static <TSource> TSource single(Enumerable<TSource> source) {
    TSource toRet = null;
    try (Enumerator<TSource> os = source.enumerator()) {
      if (os.moveNext()) {
        toRet = os.current();

        if (os.moveNext()) {
          throw new IllegalStateException();
        }
      }
      if (toRet != null) {
        return toRet;
      }
      throw new IllegalStateException();
    }
  }

  /**
   * Returns the only element of a sequence that
   * satisfies a specified condition, and throws an exception if
   * more than one such element exists.
   */
  public static <TSource> TSource single(Enumerable<TSource> source,
      Predicate1<TSource> predicate) {
    TSource toRet = null;
    try (Enumerator<TSource> os = source.enumerator()) {
      while (os.moveNext()) {
        if (predicate.apply(os.current())) {
          if (toRet == null) {
            toRet = os.current();
          } else {
            throw new IllegalStateException();
          }
        }
      }
      if (toRet != null) {
        return toRet;
      }
      throw new IllegalStateException();
    }
  }

  /**
   * Returns the only element of a sequence, or a
   * default value if the sequence is empty; this method throws an
   * exception if there is more than one element in the
   * sequence.
   */
  public static <TSource> TSource singleOrDefault(Enumerable<TSource> source) {
    TSource toRet = null;
    try (Enumerator<TSource> os = source.enumerator()) {
      if (os.moveNext()) {
        toRet = os.current();
      }

      if (os.moveNext()) {
        return null;
      }

      return toRet;
    }
  }

  /**
   * Returns the only element of a sequence that
   * satisfies a specified condition or a default value if no such
   * element exists; this method throws an exception if more than
   * one element satisfies the condition.
   */
  public static <TSource> TSource singleOrDefault(Enumerable<TSource> source,
      Predicate1<TSource> predicate) {
    TSource toRet = null;
    for (TSource s : source) {
      if (predicate.apply(s)) {
        if (toRet != null) {
          return null;
        } else {
          toRet = s;
        }
      }
    }
    return toRet;
  }

  /**
   * Bypasses a specified number of elements in a
   * sequence and then returns the remaining elements.
   */
  public static <TSource> Enumerable<TSource> skip(Enumerable<TSource> source,
      final int count) {
    return skipWhile(source, (v1, v2) -> {
      // Count is 1-based
      return v2 < count;
    });
  }

  /**
   * Bypasses elements in a sequence as long as a
   * specified condition is true and then returns the remaining
   * elements.
   */
  public static <TSource> Enumerable<TSource> skipWhile(
      Enumerable<TSource> source, Predicate1<TSource> predicate) {
    return skipWhile(source,
        Functions.toPredicate2(predicate));
  }

  /**
   * Bypasses elements in a sequence as long as a
   * specified condition is true and then returns the remaining
   * elements. The element's index is used in the logic of the
   * predicate function.
   */
  public static <TSource> Enumerable<TSource> skipWhile(
      final Enumerable<TSource> source,
      final Predicate2<TSource, Integer> predicate) {
    return new AbstractEnumerable<TSource>() {
      public Enumerator<TSource> enumerator() {
        return new SkipWhileEnumerator<>(source.enumerator(), predicate);
      }
    };
  }

  /**
   * Computes the sum of the sequence of Decimal values
   * that are obtained by invoking a transform function on each
   * element of the input sequence.
   */
  public static <TSource> BigDecimal sum(Enumerable<TSource> source,
      BigDecimalFunction1<TSource> selector) {
    return aggregate(source.select(selector), BigDecimal.ZERO,
        Extensions.BIG_DECIMAL_SUM);
  }

  /**
   * Computes the sum of the sequence of nullable
   * Decimal values that are obtained by invoking a transform
   * function on each element of the input sequence.
   */
  public static <TSource> BigDecimal sum(Enumerable<TSource> source,
      NullableBigDecimalFunction1<TSource> selector) {
    return aggregate(source.select(selector), BigDecimal.ZERO,
        Extensions.BIG_DECIMAL_SUM);
  }

  /**
   * Computes the sum of the sequence of Double values
   * that are obtained by invoking a transform function on each
   * element of the input sequence.
   */
  public static <TSource> double sum(Enumerable<TSource> source,
      DoubleFunction1<TSource> selector) {
    return aggregate(source.select(adapt(selector)), 0d, Extensions.DOUBLE_SUM);
  }

  /**
   * Computes the sum of the sequence of nullable
   * Double values that are obtained by invoking a transform
   * function on each element of the input sequence.
   */
  public static <TSource> Double sum(Enumerable<TSource> source,
      NullableDoubleFunction1<TSource> selector) {
    return aggregate(source.select(selector), 0d, Extensions.DOUBLE_SUM);
  }

  /**
   * Computes the sum of the sequence of int values
   * that are obtained by invoking a transform function on each
   * element of the input sequence.
   */
  public static <TSource> int sum(Enumerable<TSource> source,
      IntegerFunction1<TSource> selector) {
    return aggregate(source.select(adapt(selector)), 0, Extensions.INTEGER_SUM);
  }

  /**
   * Computes the sum of the sequence of nullable int
   * values that are obtained by invoking a transform function on
   * each element of the input sequence.
   */
  public static <TSource> Integer sum(Enumerable<TSource> source,
      NullableIntegerFunction1<TSource> selector) {
    return aggregate(source.select(selector), 0, Extensions.INTEGER_SUM);
  }

  /**
   * Computes the sum of the sequence of long values
   * that are obtained by invoking a transform function on each
   * element of the input sequence.
   */
  public static <TSource> long sum(Enumerable<TSource> source,
      LongFunction1<TSource> selector) {
    return aggregate(source.select(adapt(selector)), 0L, Extensions.LONG_SUM);
  }

  /**
   * Computes the sum of the sequence of nullable long
   * values that are obtained by invoking a transform function on
   * each element of the input sequence.
   */
  public static <TSource> Long sum(Enumerable<TSource> source,
      NullableLongFunction1<TSource> selector) {
    return aggregate(source.select(selector), 0L, Extensions.LONG_SUM);
  }

  /**
   * Computes the sum of the sequence of Float values
   * that are obtained by invoking a transform function on each
   * element of the input sequence.
   */
  public static <TSource> float sum(Enumerable<TSource> source,
      FloatFunction1<TSource> selector) {
    return aggregate(source.select(adapt(selector)), 0F, Extensions.FLOAT_SUM);
  }

  /**
   * Computes the sum of the sequence of nullable
   * Float values that are obtained by invoking a transform
   * function on each element of the input sequence.
   */
  public static <TSource> Float sum(Enumerable<TSource> source,
      NullableFloatFunction1<TSource> selector) {
    return aggregate(source.select(selector), 0F, Extensions.FLOAT_SUM);
  }

  /**
   * Returns a specified number of contiguous elements
   * from the start of a sequence.
   */
  public static <TSource> Enumerable<TSource> take(Enumerable<TSource> source,
      final int count) {
    return takeWhile(
        source, (v1, v2) -> {
          // Count is 1-based
          return v2 < count;
        });
  }

  /**
   * Returns a specified number of contiguous elements
   * from the start of a sequence.
   */
  public static <TSource> Enumerable<TSource> take(Enumerable<TSource> source,
      final long count) {
    return takeWhileLong(
        source, (v1, v2) -> {
          // Count is 1-based
          return v2 < count;
        });
  }

  /**
   * Returns elements from a sequence as long as a
   * specified condition is true.
   */
  public static <TSource> Enumerable<TSource> takeWhile(
      Enumerable<TSource> source, final Predicate1<TSource> predicate) {
    return takeWhile(source,
        Functions.toPredicate2(predicate));
  }

  /**
   * Returns elements from a sequence as long as a
   * specified condition is true. The element's index is used in the
   * logic of the predicate function.
   */
  public static <TSource> Enumerable<TSource> takeWhile(
      final Enumerable<TSource> source,
      final Predicate2<TSource, Integer> predicate) {
    return new AbstractEnumerable<TSource>() {
      public Enumerator<TSource> enumerator() {
        return new TakeWhileEnumerator<>(source.enumerator(), predicate);
      }
    };
  }

  /**
   * Returns elements from a sequence as long as a
   * specified condition is true. The element's index is used in the
   * logic of the predicate function.
   */
  public static <TSource> Enumerable<TSource> takeWhileLong(
      final Enumerable<TSource> source,
      final Predicate2<TSource, Long> predicate) {
    return new AbstractEnumerable<TSource>() {
      public Enumerator<TSource> enumerator() {
        return new TakeWhileLongEnumerator<>(source.enumerator(), predicate);
      }
    };
  }

  /**
   * Performs a subsequent ordering of the elements in a sequence according
   * to a key.
   */
  public static <TSource, TKey> OrderedEnumerable<TSource> createOrderedEnumerable(
      OrderedEnumerable<TSource> source, Function1<TSource, TKey> keySelector,
      Comparator<TKey> comparator, boolean descending) {
    throw Extensions.todo();
  }

  /**
   * Performs a subsequent ordering of the elements in a sequence in
   * ascending order according to a key.
   */
  public static <TSource, TKey extends Comparable<TKey>> OrderedEnumerable<TSource> thenBy(
      OrderedEnumerable<TSource> source, Function1<TSource, TKey> keySelector) {
    return createOrderedEnumerable(source, keySelector,
        Extensions.comparableComparator(), false);
  }

  /**
   * Performs a subsequent ordering of the elements in a sequence in
   * ascending order according to a key, using a specified comparator.
   */
  public static <TSource, TKey> OrderedEnumerable<TSource> thenBy(
      OrderedEnumerable<TSource> source, Function1<TSource, TKey> keySelector,
      Comparator<TKey> comparator) {
    return createOrderedEnumerable(source, keySelector, comparator, false);
  }

  /**
   * Performs a subsequent ordering of the elements in a sequence in
   * descending order according to a key.
   */
  public static <TSource, TKey extends Comparable<TKey>>
      OrderedEnumerable<TSource> thenByDescending(
      OrderedEnumerable<TSource> source, Function1<TSource, TKey> keySelector) {
    return createOrderedEnumerable(source, keySelector,
        Extensions.comparableComparator(), true);
  }

  /**
   * Performs a subsequent ordering of the elements in a sequence in
   * descending order according to a key, using a specified comparator.
   */
  public static <TSource, TKey> OrderedEnumerable<TSource> thenByDescending(
      OrderedEnumerable<TSource> source, Function1<TSource, TKey> keySelector,
      Comparator<TKey> comparator) {
    return createOrderedEnumerable(source, keySelector, comparator, true);
  }

  /**
   * Creates a Map&lt;TKey, TValue&gt; from an
   * Enumerable&lt;TSource&gt; according to a specified key selector
   * function.
   *
   * <p>NOTE: Called {@code toDictionary} in LINQ.NET.</p>
   */
  public static <TSource, TKey> Map<TKey, TSource> toMap(
      Enumerable<TSource> source, Function1<TSource, TKey> keySelector) {
    return toMap(source, keySelector, Functions.identitySelector());
  }

  /**
   * Creates a {@code Map<TKey, TValue>} from an
   * {@code Enumerable<TSource>} according to a specified key selector function
   * and key comparer.
   */
  public static <TSource, TKey> Map<TKey, TSource> toMap(
      Enumerable<TSource> source, Function1<TSource, TKey> keySelector,
      EqualityComparer<TKey> comparer) {
    return toMap(source, keySelector, Functions.identitySelector(), comparer);
  }

  /**
   * Creates a {@code Map<TKey, TValue>} from an
   * {@code Enumerable<TSource>} according to specified key selector and element
   * selector functions.
   */
  public static <TSource, TKey, TElement> Map<TKey, TElement> toMap(
      Enumerable<TSource> source, Function1<TSource, TKey> keySelector,
      Function1<TSource, TElement> elementSelector) {
    // Use LinkedHashMap because groupJoin requires order of keys to be
    // preserved.
    final Map<TKey, TElement> map = new LinkedHashMap<>();
    try (Enumerator<TSource> os = source.enumerator()) {
      while (os.moveNext()) {
        TSource o = os.current();
        map.put(keySelector.apply(o), elementSelector.apply(o));
      }
    }
    return map;
  }

  /**
   * Creates a {@code Map<TKey, TValue>} from an
   * {@code Enumerable<TSource>} according to a specified key selector function,
   * a comparer, and an element selector function.
   */
  public static <TSource, TKey, TElement> Map<TKey, TElement> toMap(
      Enumerable<TSource> source, Function1<TSource, TKey> keySelector,
      Function1<TSource, TElement> elementSelector,
      EqualityComparer<TKey> comparer) {
    // Use LinkedHashMap because groupJoin requires order of keys to be
    // preserved.
    final Map<TKey, TElement> map = new WrapMap<>(
        // Java 8 cannot infer return type with LinkedHashMap::new is used
        () -> new LinkedHashMap<Wrapped<TKey>, TElement>(), comparer);
    try (Enumerator<TSource> os = source.enumerator()) {
      while (os.moveNext()) {
        TSource o = os.current();
        map.put(keySelector.apply(o), elementSelector.apply(o));
      }
    }
    return map;
  }

  /**
   * Creates a {@code List<TSource>} from an {@code Enumerable<TSource>}.
   */
  @SuppressWarnings("unchecked")
  public static <TSource> List<TSource> toList(Enumerable<TSource> source) {
    if (source instanceof List && source instanceof RandomAccess) {
      return (List<TSource>) source;
    } else {
      return source.into(
          source instanceof Collection
              ? new ArrayList<>(((Collection) source).size())
              : new ArrayList<>());
    }
  }

  /**
   * Creates a Lookup&lt;TKey, TElement&gt; from an
   * Enumerable&lt;TSource&gt; according to a specified key selector
   * function.
   */
  public static <TSource, TKey> Lookup<TKey, TSource> toLookup(
      Enumerable<TSource> source, Function1<TSource, TKey> keySelector) {
    return toLookup(source, keySelector, Functions.identitySelector());
  }

  /**
   * Creates a {@code Lookup<TKey, TElement>} from an
   * {@code Enumerable<TSource>} according to a specified key selector function
   * and key comparer.
   */
  public static <TSource, TKey> Lookup<TKey, TSource> toLookup(
      Enumerable<TSource> source, Function1<TSource, TKey> keySelector,
      EqualityComparer<TKey> comparer) {
    return toLookup(
        source, keySelector, Functions.identitySelector(), comparer);
  }

  /**
   * Creates a {@code Lookup<TKey, TElement>} from an
   * {@code Enumerable<TSource>} according to specified key selector and element
   * selector functions.
   */
  public static <TSource, TKey, TElement> Lookup<TKey, TElement> toLookup(
      Enumerable<TSource> source, Function1<TSource, TKey> keySelector,
      Function1<TSource, TElement> elementSelector) {
    final Map<TKey, List<TElement>> map = new HashMap<>();
    return toLookup_(map, source, keySelector, elementSelector);
  }

  static <TSource, TKey, TElement> LookupImpl<TKey, TElement> toLookup_(
      Map<TKey, List<TElement>> map, Enumerable<TSource> source,
      Function1<TSource, TKey> keySelector,
      Function1<TSource, TElement> elementSelector) {
    try (Enumerator<TSource> os = source.enumerator()) {
      while (os.moveNext()) {
        TSource o = os.current();
        final TKey key = keySelector.apply(o);
        List<TElement> list = map.get(key);
        if (list == null) {
          // for first entry, use a singleton list to save space
          list = Collections.singletonList(elementSelector.apply(o));
        } else {
          if (list.size() == 1) {
            // when we go from 1 to 2 elements, switch to array list
            TElement element = list.get(0);
            list = new ArrayList<>();
            list.add(element);
          }
          list.add(elementSelector.apply(o));
        }
        map.put(key, list);
      }
    }
    return new LookupImpl<>(map);
  }

  /**
   * Creates a {@code Lookup<TKey, TElement>} from an
   * {@code Enumerable<TSource>} according to a specified key selector function,
   * a comparer and an element selector function.
   */
  public static <TSource, TKey, TElement> Lookup<TKey, TElement> toLookup(
      Enumerable<TSource> source, Function1<TSource, TKey> keySelector,
      Function1<TSource, TElement> elementSelector,
      EqualityComparer<TKey> comparer) {
    return toLookup_(
        new WrapMap<>(
            // Java 8 cannot infer return type with HashMap::new is used
            () -> new HashMap<Wrapped<TKey>, List<TElement>>(),
          comparer),
        source,
        keySelector,
        elementSelector);
  }

  /**
   * Produces the set union of two sequences by using
   * the default equality comparer.
   */
  public static <TSource> Enumerable<TSource> union(Enumerable<TSource> source0,
      Enumerable<TSource> source1) {
    Set<TSource> set = new HashSet<>();
    source0.into(set);
    source1.into(set);
    return Linq4j.asEnumerable(set);
  }

  /**
   * Produces the set union of two sequences by using a
   * specified EqualityComparer&lt;TSource&gt;.
   */
  public static <TSource> Enumerable<TSource> union(Enumerable<TSource> source0,
      Enumerable<TSource> source1, final EqualityComparer<TSource> comparer) {
    if (comparer == Functions.identityComparer()) {
      return union(source0, source1);
    }
    Set<Wrapped<TSource>> set = new HashSet<>();
    Function1<TSource, Wrapped<TSource>> wrapper = wrapperFor(comparer);
    Function1<Wrapped<TSource>, TSource> unwrapper = unwrapper();
    source0.select(wrapper).into(set);
    source1.select(wrapper).into(set);
    return Linq4j.asEnumerable(set).select(unwrapper);
  }

  private static <TSource> Function1<Wrapped<TSource>, TSource> unwrapper() {
    return a0 -> a0.element;
  }

  private static <TSource> Function1<TSource, Wrapped<TSource>> wrapperFor(
      final EqualityComparer<TSource> comparer) {
    return a0 -> Wrapped.upAs(comparer, a0);
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
        return EnumerableDefaults.where(enumerator, predicate);
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

  /**
   * Filters a sequence of values based on a
   * predicate. Each element's index is used in the logic of the
   * predicate function.
   */
  public static <TSource> Enumerable<TSource> where(
      final Enumerable<TSource> source,
      final Predicate2<TSource, Integer> predicate) {
    return new AbstractEnumerable<TSource>() {
      public Enumerator<TSource> enumerator() {
        return new Enumerator<TSource>() {
          final Enumerator<TSource> enumerator = source.enumerator();
          int n = -1;

          public TSource current() {
            return enumerator.current();
          }

          public boolean moveNext() {
            while (enumerator.moveNext()) {
              ++n;
              if (predicate.apply(enumerator.current(), n)) {
                return true;
              }
            }
            return false;
          }

          public void reset() {
            enumerator.reset();
            n = -1;
          }

          public void close() {
            enumerator.close();
          }
        };
      }
    };
  }

  /**
   * Applies a specified function to the corresponding
   * elements of two sequences, producing a sequence of the
   * results.
   */
  public static <T0, T1, TResult> Enumerable<TResult> zip(
      final Enumerable<T0> first, final Enumerable<T1> second,
      final Function2<T0, T1, TResult> resultSelector) {
    return new AbstractEnumerable<TResult>() {
      public Enumerator<TResult> enumerator() {
        return new Enumerator<TResult>() {
          final Enumerator<T0> e1 = first.enumerator();
          final Enumerator<T1> e2 = second.enumerator();

          public TResult current() {
            return resultSelector.apply(e1.current(), e2.current());
          }
          public boolean moveNext() {
            return e1.moveNext() && e2.moveNext();
          }
          public void reset() {
            e1.reset();
            e2.reset();
          }
          public void close() {
            e1.close();
            e2.close();
          }
        };
      }
    };
  }

  public static <T> OrderedQueryable<T> asOrderedQueryable(
      Enumerable<T> source) {
    //noinspection unchecked
    return source instanceof OrderedQueryable
        ? ((OrderedQueryable<T>) source)
        : new EnumerableOrderedQueryable<>(
            source, (Class) Object.class, null, null);
  }

  /** Default implementation of {@link ExtendedEnumerable#into(Collection)}. */
  public static <T, C extends Collection<? super T>> C into(
      Enumerable<T> source, C sink) {
    try (Enumerator<T> enumerator = source.enumerator()) {
      while (enumerator.moveNext()) {
        T t = enumerator.current();
        sink.add(t);
      }
    }
    return sink;
  }

  /** Default implementation of {@link ExtendedEnumerable#removeAll(Collection)}. */
  public static <T, C extends Collection<? super T>> C remove(
      Enumerable<T> source, C sink) {
    List<T> tempList = new ArrayList<>();
    source.into(tempList);
    sink.removeAll(tempList);
    return sink;
  }

  /** Enumerable that implements take-while.
   *
   * @param <TSource> element type */
  static class TakeWhileEnumerator<TSource> implements Enumerator<TSource> {
    private final Enumerator<TSource> enumerator;
    private final Predicate2<TSource, Integer> predicate;

    boolean done = false;
    int n = -1;

    TakeWhileEnumerator(Enumerator<TSource> enumerator,
        Predicate2<TSource, Integer> predicate) {
      this.enumerator = enumerator;
      this.predicate = predicate;
    }

    public TSource current() {
      return enumerator.current();
    }

    public boolean moveNext() {
      if (!done) {
        if (enumerator.moveNext()
            && predicate.apply(enumerator.current(), ++n)) {
          return true;
        } else {
          done = true;
        }
      }
      return false;
    }

    public void reset() {
      enumerator.reset();
      done = false;
      n = -1;
    }

    public void close() {
      enumerator.close();
    }
  }

  /** Enumerable that implements take-while.
   *
   * @param <TSource> element type */
  static class TakeWhileLongEnumerator<TSource> implements Enumerator<TSource> {
    private final Enumerator<TSource> enumerator;
    private final Predicate2<TSource, Long> predicate;

    boolean done = false;
    long n = -1;

    TakeWhileLongEnumerator(Enumerator<TSource> enumerator,
        Predicate2<TSource, Long> predicate) {
      this.enumerator = enumerator;
      this.predicate = predicate;
    }

    public TSource current() {
      return enumerator.current();
    }

    public boolean moveNext() {
      if (!done) {
        if (enumerator.moveNext()
            && predicate.apply(enumerator.current(), ++n)) {
          return true;
        } else {
          done = true;
        }
      }
      return false;
    }

    public void reset() {
      enumerator.reset();
      done = false;
      n = -1;
    }

    public void close() {
      enumerator.close();
    }
  }

  /** Enumerator that implements skip-while.
   *
   * @param <TSource> element type */
  static class SkipWhileEnumerator<TSource> implements Enumerator<TSource> {
    private final Enumerator<TSource> enumerator;
    private final Predicate2<TSource, Integer> predicate;

    boolean started = false;
    int n = -1;

    SkipWhileEnumerator(Enumerator<TSource> enumerator,
        Predicate2<TSource, Integer> predicate) {
      this.enumerator = enumerator;
      this.predicate = predicate;
    }

    public TSource current() {
      return enumerator.current();
    }

    public boolean moveNext() {
      for (;;) {
        if (!enumerator.moveNext()) {
          return false;
        }
        if (started) {
          return true;
        }
        if (!predicate.apply(enumerator.current(), ++n)) {
          started = true;
          return true;
        }
      }
    }

    public void reset() {
      enumerator.reset();
      started = false;
      n = -1;
    }

    public void close() {
      enumerator.close();
    }
  }

  /** Enumerator that casts each value.
   *
   * @param <T> element type */
  static class CastingEnumerator<T> implements Enumerator<T> {
    private final Enumerator<?> enumerator;
    private final Class<T> clazz;

    CastingEnumerator(Enumerator<?> enumerator, Class<T> clazz) {
      this.enumerator = enumerator;
      this.clazz = clazz;
    }

    public T current() {
      return clazz.cast(enumerator.current());
    }

    public boolean moveNext() {
      return enumerator.moveNext();
    }

    public void reset() {
      enumerator.reset();
    }

    public void close() {
      enumerator.close();
    }
  }

  /** Value wrapped with a comparer.
   *
   * @param <T> element type */
  private static class Wrapped<T> {
    private final EqualityComparer<T> comparer;
    private final T element;

    private Wrapped(EqualityComparer<T> comparer, T element) {
      this.comparer = comparer;
      this.element = element;
    }

    static <T> Wrapped<T> upAs(EqualityComparer<T> comparer, T element) {
      return new Wrapped<>(comparer, element);
    }

    @Override public int hashCode() {
      return comparer.hashCode(element);
    }

    @Override public boolean equals(Object obj) {
      //noinspection unchecked
      return obj == this || obj instanceof Wrapped && comparer.equal(element,
          ((Wrapped<T>) obj).element);
    }

    public T unwrap() {
      return element;
    }
  }

  /** Map that wraps each value.
   *
   * @param <K> key type
   * @param <V> value type */
  private static class WrapMap<K, V> extends AbstractMap<K, V> {
    private final Map<Wrapped<K>, V> map;
    private final EqualityComparer<K> comparer;

    protected WrapMap(Function0<Map<Wrapped<K>, V>> mapProvider, EqualityComparer<K> comparer) {
      this.map = mapProvider.apply();
      this.comparer = comparer;
    }

    @Override public Set<Entry<K, V>> entrySet() {
      return new AbstractSet<Entry<K, V>>() {
        @Override public Iterator<Entry<K, V>> iterator() {
          final Iterator<Entry<Wrapped<K>, V>> iterator =
              map.entrySet().iterator();

          return new Iterator<Entry<K, V>>() {
            public boolean hasNext() {
              return iterator.hasNext();
            }

            public Entry<K, V> next() {
              Entry<Wrapped<K>, V> next = iterator.next();
              return new SimpleEntry<>(next.getKey().element, next.getValue());
            }

            public void remove() {
              iterator.remove();
            }
          };
        }

        @Override public int size() {
          return map.size();
        }
      };
    }

    @Override public boolean containsKey(Object key) {
      return map.containsKey(wrap((K) key));
    }

    private Wrapped<K> wrap(K key) {
      return Wrapped.upAs(comparer, key);
    }

    @Override public V get(Object key) {
      return map.get(wrap((K) key));
    }

    @Override public V put(K key, V value) {
      return map.put(wrap(key), value);
    }

    @Override public V remove(Object key) {
      return map.remove(wrap((K) key));
    }

    @Override public void clear() {
      map.clear();
    }

    @Override public Collection<V> values() {
      return map.values();
    }
  }

  /** Reads a populated map, applying a selector function.
   *
   * @param <TResult> result type
   * @param <TKey> key type
   * @param <TAccumulate> accumulator type */
  private static class LookupResultEnumerable<TResult, TKey, TAccumulate>
      extends AbstractEnumerable2<TResult> {
    private final Map<TKey, TAccumulate> map;
    private final Function2<TKey, TAccumulate, TResult> resultSelector;

    LookupResultEnumerable(Map<TKey, TAccumulate> map,
        Function2<TKey, TAccumulate, TResult> resultSelector) {
      this.map = map;
      this.resultSelector = resultSelector;
    }

    public Iterator<TResult> iterator() {
      final Iterator<Map.Entry<TKey, TAccumulate>> iterator =
          map.entrySet().iterator();
      return new Iterator<TResult>() {
        public boolean hasNext() {
          return iterator.hasNext();
        }

        public TResult next() {
          final Map.Entry<TKey, TAccumulate> entry = iterator.next();
          return resultSelector.apply(entry.getKey(), entry.getValue());
        }

        public void remove() {
          throw new UnsupportedOperationException();
        }
      };
    }
  }

  /** Enumerator that performs a merge join on its sorted inputs.
   *
   * @param <TResult> result type
   * @param <TSource> left input record type
   * @param <TKey> key type
   * @param <TInner> right input record type */
  private static class MergeJoinEnumerator<TResult, TSource, TInner, TKey extends Comparable<TKey>>
      implements Enumerator<TResult> {
    final List<TSource> lefts = new ArrayList<>();
    final List<TInner> rights = new ArrayList<>();
    private final Enumerator<TSource> leftEnumerator;
    private final Enumerator<TInner> rightEnumerator;
    private final Function1<TSource, TKey> outerKeySelector;
    private final Function1<TInner, TKey> innerKeySelector;
    private final Function2<TSource, TInner, TResult> resultSelector;
    boolean done;
    Enumerator<List<Object>> cartesians;

    MergeJoinEnumerator(Enumerator<TSource> leftEnumerator,
        Enumerator<TInner> rightEnumerator,
        Function1<TSource, TKey> outerKeySelector,
        Function1<TInner, TKey> innerKeySelector,
        Function2<TSource, TInner, TResult> resultSelector) {
      this.leftEnumerator = leftEnumerator;
      this.rightEnumerator = rightEnumerator;
      this.outerKeySelector = outerKeySelector;
      this.innerKeySelector = innerKeySelector;
      this.resultSelector = resultSelector;
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
          if (c > 0) {
            throw new IllegalStateException(
              "mergeJoin assumes inputs sorted in ascending order, "
                 + "however " + leftKey + " is greater than " + leftKey2);
          }
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
          if (c > 0) {
            throw new IllegalStateException(
              "mergeJoin assumes input sorted in ascending order, "
                 + "however " + rightKey + " is greater than " + rightKey2);
          }
          break;
        }
        rights.add(right);
      }
      cartesians = Linq4j.product(
          ImmutableList.of(Linq4j.enumerator(lefts),
              Linq4j.enumerator(rights)));
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
  }

  private static final Object DUMMY = new Object();

  /**
   * Repeat Union All enumerable: it will evaluate the seed enumerable once, and then
   * it will start to evaluate the iteration enumerable over and over until either it returns
   * no results, or an optional maximum numbers of iterations is reached
   * @param seed seed enumerable
   * @param iteration iteration enumerable
   * @param iterationLimit maximum numbers of repetitions for the iteration enumerable
   *                       (negative value means no limit)
   * @param <TSource> record type
   */
  @SuppressWarnings("unchecked")
  public static <TSource> Enumerable<TSource> repeatUnionAll(
          Enumerable<TSource> seed,
          Enumerable<TSource> iteration,
          int iterationLimit) {
    return new AbstractEnumerable<TSource>() {
      @Override public Enumerator<TSource> enumerator() {
        return new Enumerator<TSource>() {
          private TSource current = (TSource) DUMMY;
          private boolean seedProcessed = false;
          private int currentIteration = 0;
          private final Enumerator<TSource> seedEnumerator = seed.enumerator();
          private Enumerator<TSource> iterativeEnumerator = null;

          @Override public TSource current() {
            if (this.current == DUMMY) {
              throw new NoSuchElementException();
            }
            return this.current;
          }

          @Override public boolean moveNext() {
            // if we are not done with the seed moveNext on it
            if (!this.seedProcessed) {
              if (this.seedEnumerator.moveNext()) {
                this.current = this.seedEnumerator.current();
                return true;
              } else {
                this.seedProcessed = true;
              }
            }

            // if we are done with the seed, moveNext on the iterative part
            while (true) {
              if (iterationLimit >= 0 && this.currentIteration == iterationLimit) {
                // max number of iterations reached, we are done
                this.current = (TSource) DUMMY;
                return false;
              }

              if (this.iterativeEnumerator == null) {
                this.iterativeEnumerator = iteration.enumerator();
              }

              if (this.iterativeEnumerator.moveNext()) {
                this.current = this.iterativeEnumerator.current();
                return true;
              }

              if (this.current == DUMMY) {
                // current iteration did not return any value, we are done
                return false;
              }

              // current iteration level (which returned some values) is finished, go to next one
              this.current = (TSource) DUMMY;
              this.iterativeEnumerator.close();
              this.iterativeEnumerator = null;
              this.currentIteration++;
            }
          }

          @Override public void reset() {
            this.seedEnumerator.reset();
            if (this.iterativeEnumerator != null) {
              this.iterativeEnumerator.close();
              this.iterativeEnumerator = null;
            }
            this.currentIteration = 0;
          }

          @Override public void close() {
            this.seedEnumerator.close();
            if (this.iterativeEnumerator != null) {
              this.iterativeEnumerator.close();
            }
          }
        };
      }
    };
  }

  /**
   * Lazy read and lazy write spool that stores data into a collection
   */
  @SuppressWarnings("unchecked")
  public static <TSource> Enumerable<TSource> lazyCollectionSpool(
      Collection<TSource> outputCollection,
      Enumerable<TSource> input) {

    return new AbstractEnumerable<TSource>() {
      @Override public Enumerator<TSource> enumerator() {
        return new Enumerator<TSource>() {
          private TSource current = (TSource) DUMMY;
          private final Enumerator<TSource> inputEnumerator = input.enumerator();
          private final Collection<TSource> collection = outputCollection;
          private final Collection<TSource> tempCollection = new ArrayList<>();

          @Override public TSource current() {
            if (this.current == DUMMY) {
              throw new NoSuchElementException();
            }
            return this.current;
          }

          @Override public boolean moveNext() {
            if (this.inputEnumerator.moveNext()) {
              this.current = this.inputEnumerator.current();
              this.tempCollection.add(this.current);
              return true;
            }
            this.flush();
            return false;
          }

          private void flush() {
            this.collection.clear();
            this.collection.addAll(this.tempCollection);
            this.tempCollection.clear();
          }

          @Override public void reset() {
            this.inputEnumerator.reset();
            this.collection.clear();
            this.tempCollection.clear();
          }

          @Override public void close() {
            this.inputEnumerator.close();
          }
        };
      }
    };
  }
}

// End EnumerableDefaults.java
