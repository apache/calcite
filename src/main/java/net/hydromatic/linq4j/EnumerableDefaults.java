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
package net.hydromatic.linq4j;

import net.hydromatic.linq4j.function.*;

import java.math.BigDecimal;
import java.util.*;

import static net.hydromatic.linq4j.function.Functions.adapt;

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
    final Enumerator<TSource> os = source.enumerator();
    try {
      while (os.moveNext()) {
        TSource o = os.current();
        result = func.apply(result, o);
      }
      return result;
    } finally {
      os.close();
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
    final Enumerator<TSource> os = source.enumerator();
    try {
      while (os.moveNext()) {
        TSource o = os.current();
        result = func.apply(result, o);
      }
      return result;
    } finally {
      os.close();
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
    final Enumerator<TSource> os = source.enumerator();
    try {
      while (os.moveNext()) {
        TSource o = os.current();
        accumulate = func.apply(accumulate, o);
      }
      return selector.apply(accumulate);
    } finally {
      os.close();
    }
  }

  /**
   * Determines whether all elements of a sequence
   * satisfy a condition.
   */
  public static <TSource> boolean all(Enumerable<?> enumerable,
      Predicate1<TSource> predicate) {
    for (Object o : enumerable) {
      if (!predicate.apply((TSource) o)) {
        return false;
      }
    }
    return true;
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
    final Enumerator<TSource> os = enumerable.enumerator();
    try {
      while (os.moveNext()) {
        TSource o = os.current();
        if (predicate.apply(o)) {
          return true;
        }
      }
      return false;
    } finally {
      os.close();
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
        return new CastingEnumerator<T2>(source.enumerator(), clazz);
      }
    };
  }

  /**
   * Concatenates two sequences.
   */
  public static <TSource> Enumerable<TSource> concat(
      Enumerable<TSource> enumerable0, Enumerable<TSource> enumerable1) {
    //noinspection unchecked
    return Linq4j.concat(Arrays.<Enumerable<TSource>>asList(enumerable0,
        enumerable1));
  }

  /**
   * Determines whether a sequence contains a specified
   * element by using the default equality comparer.
   */
  public static <TSource> boolean contains(Enumerable<TSource> enumerable,
      TSource element) {
    // Implementations of Enumerable backed by a Collection call
    // Collection.contains, which may be more efficient, not this method.
    final Enumerator<TSource> os = enumerable.enumerator();
    try {
      while (os.moveNext()) {
        TSource o = os.current();
        if (o.equals(element)) {
          return true;
        }
      }
      return false;
    } finally {
      os.close();
    }
  }

  /**
   * Determines whether a sequence contains a specified
   * element by using a specified {@code EqualityComparer<TSource>}.
   */
  public static <TSource> boolean contains(Enumerable<TSource> enumerable,
      TSource element, EqualityComparer comparer) {
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
    return (int) longCount(enumerable, Functions.<TSource>truePredicate1());
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
    throw Extensions.todo();
  }

  /**
   * Returns the elements of the specified sequence or
   * the specified value in a singleton collection if the sequence
   * is empty.
   */
  public static <TSource> TSource defaultIfEmpty(Enumerable<TSource> enumerable,
      TSource value) {
    throw Extensions.todo();
  }

  /**
   * Returns distinct elements from a sequence by using
   * the default {@link EqualityComparer} to compare values.
   */
  public static <TSource> Enumerable<TSource> distinct(
      Enumerable<TSource> enumerable) {
    final Enumerator<TSource> os = enumerable.enumerator();
    final Set<TSource> set = new HashSet<TSource>();
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
    final Set<Wrapped<TSource>> set = new HashSet<Wrapped<TSource>>();
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
    throw Extensions.todo();
  }

  /**
   * Returns the element at a specified index in a
   * sequence or a default value if the index is out of
   * range.
   */
  public static <TSource> TSource elementAtOrDefault(
      Enumerable<TSource> enumerable, int index) {
    throw Extensions.todo();
  }

  /**
   * Produces the set difference of two sequences by
   * using the default equality comparer to compare values. (Defined
   * by Enumerable.)
   */
  public static <TSource> Enumerable<TSource> except(
      Enumerable<TSource> source0, Enumerable<TSource> source1) {
    Set<TSource> set = new HashSet<TSource>();
    source0.into(set);
    final Enumerator<TSource> os = source1.enumerator();
    try {
      while (os.moveNext()) {
        TSource o = os.current();
        set.remove(o);
      }
      return Linq4j.asEnumerable(set);
    } finally {
      os.close();
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
    Set<Wrapped<TSource>> set = new HashSet<Wrapped<TSource>>();
    Function1<TSource, Wrapped<TSource>> wrapper = wrapperFor(comparer);
    source0.select(wrapper).into(set);
    final Enumerator<Wrapped<TSource>> os =
        source1.select(wrapper).enumerator();
    try {
      while (os.moveNext()) {
        Wrapped<TSource> o = os.current();
        set.remove(o);
      }
    } finally {
      os.close();
    }
    Function1<Wrapped<TSource>, TSource> unwrapper = unwrapper();
    return Linq4j.asEnumerable(set).select(unwrapper);
  }

  /**
   * Returns the first element of a sequence. (Defined
   * by Enumerable.)
   */
  public static <TSource> TSource first(Enumerable<TSource> enumerable) {
    final Enumerator<TSource> os = enumerable.enumerator();
    try {
      if (os.moveNext()) {
        return os.current();
      }
      throw new NoSuchElementException();
    } finally {
      os.close();
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
    return null;
  }

  /**
   * Returns the first element of a sequence, or a
   * default value if the sequence contains no elements.
   */
  public static <TSource> TSource firstOrDefault(
      Enumerable<TSource> enumerable) {
    throw Extensions.todo();
  }

  /**
   * Returns the first element of the sequence that
   * satisfies a condition or a default value if no such element is
   * found.
   */
  public static <TSource> TSource firstOrDefault(Enumerable<TSource> enumerable,
      Predicate1<TSource> predicate) {
    throw Extensions.todo();
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
  public static <TSource, TKey, TElement> Enumerable<Grouping<TKey, TElement>>
  groupBy(Enumerable<TSource> enumerable, Function1<TSource, TKey> keySelector,
      Function1<TSource, TElement> elementSelector) {
    throw Extensions.todo();
  }

  /**
   * Groups the elements of a sequence according to a
   * specified key selector function and creates a result value from
   * each group and its key.
   */
  public static <TSource, TKey, TResult> Enumerable<Grouping<TKey, TResult>>
  groupBy(Enumerable<TSource> enumerable, Function1<TSource, TKey> keySelector,
      Function2<TKey, Enumerable<TSource>, TResult> elementSelector) {
    throw Extensions.todo();
  }

  /**
   * Groups the elements of a sequence according to a
   * key selector function. The keys are compared by using a
   * comparer and each group's elements are projected by using a
   * specified function.
   */
  public static <TSource, TKey, TElement> Enumerable<Grouping<TKey, TElement>>
  groupBy(Enumerable<TSource> enumerable, Function1<TSource, TKey> keySelector,
      Function1<TSource, TElement> elementSelector,
      EqualityComparer<TKey> comparer) {
    throw Extensions.todo();
  }

  /**
   * Groups the elements of a sequence according to a
   * specified key selector function and creates a result value from
   * each group and its key. The keys are compared by using a
   * specified comparer.
   */
  public static <TSource, TKey, TResult> Enumerable<TResult> groupBy(
      Enumerable<TSource> enumerable, Function1<TSource, TKey> keySelector,
      Function2<TKey, Enumerable<TSource>, TResult> elementSelector,
      EqualityComparer comparer) {
    throw Extensions.todo();
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
      Function2<TKey, Enumerable<TElement>, TResult> resultSelector) {
    throw Extensions.todo();
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
      Function2<TKey, Enumerable<TElement>, TResult> resultSelector,
      EqualityComparer<TKey> comparer) {
    throw Extensions.todo();
  }

  /**
   * Groups the elements of a sequence according to a
   * specified key selector function, initializing an accumulator for each
   * group and adding to it each time an element with the same key is seen.
   * Creates a result value from each accumulator and its key using a
   * specified function.
   */
  public static <TSource, TKey, TAccumulate, TResult> Enumerable<TResult>
  groupBy(Enumerable<TSource> enumerable, Function1<TSource, TKey> keySelector,
      Function0<TAccumulate> accumulatorInitializer,
      Function2<TAccumulate, TSource, TAccumulate> accumulatorAdder,
      final Function2<TKey, TAccumulate, TResult> resultSelector) {
    return groupBy_(new HashMap<TKey, TAccumulate>(), enumerable, keySelector,
        accumulatorInitializer, accumulatorAdder, resultSelector);
  }

  /**
   * Groups the elements of a sequence according to a
   * specified key selector function, initializing an accumulator for each
   * group and adding to it each time an element with the same key is seen.
   * Creates a result value from each accumulator and its key using a
   * specified function. Key values are compared by using a
   * specified comparer.
   */
  public static <TSource, TKey, TAccumulate, TResult> Enumerable<TResult>
  groupBy(Enumerable<TSource> enumerable, Function1<TSource, TKey> keySelector,
      Function0<TAccumulate> accumulatorInitializer,
      Function2<TAccumulate, TSource, TAccumulate> accumulatorAdder,
      Function2<TKey, TAccumulate, TResult> resultSelector,
      EqualityComparer<TKey> comparer) {
    return groupBy_(new WrapMap<TKey, TAccumulate>(comparer), enumerable,
        keySelector, accumulatorInitializer, accumulatorAdder, resultSelector);
  }

  private static <TSource, TKey, TAccumulate, TResult> Enumerable<TResult>
  groupBy_(final Map<TKey, TAccumulate> map, Enumerable<TSource> enumerable,
      Function1<TSource, TKey> keySelector,
      Function0<TAccumulate> accumulatorInitializer,
      Function2<TAccumulate, TSource, TAccumulate> accumulatorAdder,
      final Function2<TKey, TAccumulate, TResult> resultSelector) {
    final Enumerator<TSource> os = enumerable.enumerator();
    try {
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
    } finally {
      os.close();
    }
    return new AbstractEnumerable2<TResult>() {
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
    };
  }

  private static <TSource, TKey, TResult> Enumerable<TResult>
  groupBy_(final Set<TKey> map, Enumerable<TSource> enumerable,
      Function1<TSource, TKey> keySelector,
      final Function1<TKey, TResult> resultSelector) {
    final Enumerator<TSource> os = enumerable.enumerator();
    try {
      while (os.moveNext()) {
        TSource o = os.current();
        TKey key = keySelector.apply(o);
        map.add(key);
      }
    } finally {
      os.close();
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
                inners == null ? Linq4j.<TInner>emptyEnumerable() : inners);
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
      Enumerable<TSource> outer, Enumerable<TInner> inner,
      Function1<TSource, TKey> outerKeySelector,
      Function1<TInner, TKey> innerKeySelector,
      Function2<TSource, Enumerable<TInner>, TResult> resultSelector,
      EqualityComparer<TKey> comparer) {
    throw Extensions.todo();
  }

  /**
   * Produces the set intersection of two sequences by
   * using the default equality comparer to compare values. (Defined
   * by Enumerable.)
   */
  public static <TSource> Enumerable<TSource> intersect(
      Enumerable<TSource> source0, Enumerable<TSource> source1) {
    Set<TSource> set0 = new HashSet<TSource>();
    source0.into(set0);
    Set<TSource> set1 = new HashSet<TSource>();
    final Enumerator<TSource> os = source1.enumerator();
    try {
      while (os.moveNext()) {
        TSource o = os.current();
        if (set0.contains(o)) {
          set1.add(o);
        }
      }
    } finally {
      os.close();
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
    Set<Wrapped<TSource>> set0 = new HashSet<Wrapped<TSource>>();
    Function1<TSource, Wrapped<TSource>> wrapper = wrapperFor(comparer);
    source0.select(wrapper).into(set0);
    Set<Wrapped<TSource>> set1 = new HashSet<Wrapped<TSource>>();
    final Enumerator<Wrapped<TSource>> os =
        source1.select(wrapper).enumerator();
    try {
      while (os.moveNext()) {
        Wrapped<TSource> o = os.current();
        if (set0.contains(o)) {
          set1.add(o);
        }
      }
    } finally {
      os.close();
    }
    Function1<Wrapped<TSource>, TSource> unwrapper = unwrapper();
    return Linq4j.asEnumerable(set1).select(unwrapper);
  }

  /**
   * Correlates the elements of two sequences based on
   * matching keys. The default equality comparer is used to compare
   * keys.
   */
  public static <TSource, TInner, TKey, TResult> Enumerable<TResult> join(
      final Enumerable<TSource> outer, final Enumerable<TInner> inner,
      final Function1<TSource, TKey> outerKeySelector,
      final Function1<TInner, TKey> innerKeySelector,
      final Function2<TSource, TInner, TResult> resultSelector) {
    return join(outer, inner, outerKeySelector, innerKeySelector,
        resultSelector, null, false, false);
  }

  /**
   * Correlates the elements of two sequences based on
   * matching keys. A specified {@code EqualityComparer<TSource>} is used to
   * compare keys.
   */
  public static <TSource, TInner, TKey, TResult> Enumerable<TResult> join(
      Enumerable<TSource> outer, Enumerable<TInner> inner,
      Function1<TSource, TKey> outerKeySelector,
      Function1<TInner, TKey> innerKeySelector,
      Function2<TSource, TInner, TResult> resultSelector,
      EqualityComparer<TKey> comparer) {
    return join(outer, inner, outerKeySelector, innerKeySelector,
        resultSelector, comparer, false, false);
  }

  /**
   * Correlates the elements of two sequences based on
   * matching keys. A specified {@code EqualityComparer<TSource>} is used to
   * compare keys.
   */
  public static <TSource, TInner, TKey, TResult> Enumerable<TResult> join(
      Enumerable<TSource> outer, Enumerable<TInner> inner,
      Function1<TSource, TKey> outerKeySelector,
      Function1<TInner, TKey> innerKeySelector,
      Function2<TSource, TInner, TResult> resultSelector,
      EqualityComparer<TKey> comparer, boolean generateNullsOnLeft,
      boolean generateNullsOnRight) {
    return join_(outer, inner, outerKeySelector, innerKeySelector,
        resultSelector, comparer, generateNullsOnLeft, generateNullsOnRight);
  }

  /** Implementation of join that builds the right input and probes with the
   * left. */
  private static <TSource, TInner, TKey, TResult> Enumerable<TResult> join_(
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
                  ? new HashSet<TKey>(innerLookup.keySet())
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
                  List<TInner> list = new ArrayList<TInner>();
                  for (TKey key : unmatchedKeys) {
                    for (TInner tInner : innerLookup.get(key)) {
                      list.add(tInner);
                    }
                  }
                  list = new ArrayList<TInner>();
                  for (TKey key : unmatchedKeys) {
                    for (TInner inner : innerLookup.get(key)) {
                      list.add(inner);
                    }
                  }
                  inners = Linq4j.enumerator(list);
                  outers = Linq4j.singletonNullEnumerator();
                  unmatchedKeys = null; // don't do the 'leftovers' again
                  continue;
                }
                return false;
              }
              final TSource outer = outers.current();
              if (outer == null) {
                continue;
              }
              final TKey outerKey = outerKeySelector.apply(outer);
              if (unmatchedKeys != null) {
                unmatchedKeys.remove(outerKey);
              }
              final Enumerable<TInner> innerEnumerable =
                  innerLookup.get(outerKey);
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
   * Returns the last element of a sequence. (Defined
   * by Enumerable.)
   */
  public static <TSource> TSource last(Enumerable<TSource> enumerable) {
    throw Extensions.todo();
  }

  /**
   * Returns the last element of a sequence that
   * satisfies a specified condition.
   */
  public static <TSource> TSource last(Enumerable<TSource> enumerable,
      Predicate1<TSource> predicate) {
    throw Extensions.todo();
  }

  /**
   * Returns the last element of a sequence, or a
   * default value if the sequence contains no elements.
   */
  public static <TSource> TSource lastOrDefault(
      Enumerable<TSource> enumerable) {
    throw Extensions.todo();
  }

  /**
   * Returns the last element of a sequence that
   * satisfies a condition or a default value if no such element is
   * found.
   */
  public static <TSource> TSource lastOrDefault(Enumerable<TSource> enumerable,
      Predicate1<TSource> predicate) {
    throw Extensions.todo();
  }

  /**
   * Returns an long that represents the total number
   * of elements in a sequence.
   */
  public static <TSource> long longCount(Enumerable<TSource> source) {
    return longCount(source, Functions.<TSource>truePredicate1());
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
    final Enumerator<TSource> os = enumerable.enumerator();
    try {
      while (os.moveNext()) {
        TSource o = os.current();
        if (predicate.apply(o)) {
          ++n;
        }
      }
    } finally {
      os.close();
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
  private static <TSource extends Comparable<TSource>>
  Function2<TSource, TSource, TSource> minFunction() {
    return (Function2<TSource, TSource, TSource>) Extensions.COMPARABLE_MIN;
  }

  @SuppressWarnings("unchecked")
  private static <TSource extends Comparable<TSource>>
  Function2<TSource, TSource, TSource> maxFunction() {
    return (Function2<TSource, TSource, TSource>) Extensions.COMPARABLE_MAX;
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
        Functions.<TSource, TResult>ofTypePredicate(clazz));
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
    final Map<TKey, List<TSource>> map = new TreeMap<TKey, List<TSource>>(
        comparator);
    LookupImpl<TKey, TSource> lookup = toLookup_(map, source, keySelector,
        Functions.<TSource>identitySelector());
    return lookup.valuesEnumerable();
  }

  /**
   * Sorts the elements of a sequence in descending
   * order according to a key.
   */
  public static <TSource, TKey extends Comparable> Enumerable<TSource>
  orderByDescending(
      Enumerable<TSource> source, Function1<TSource, TKey> keySelector) {
    return orderBy(source, keySelector, Collections.<TKey>reverseOrder());
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
    return Linq4j.asEnumerable(new AbstractList<TSource>() {
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
      Enumerable<TSource> source,
      Function2<TSource, Integer, Enumerable<TResult>> selector) {
    throw Extensions.todo();
  }

  /**
   * Projects each element of a sequence to an
   * {@code Enumerable<TSource>}, flattens the resulting sequences into one
   * sequence, and invokes a result selector function on each
   * element therein. The index of each source element is used in
   * the intermediate projected form of that element.
   */
  public static <TSource, TCollection, TResult> Enumerable<TResult> selectMany(
      Enumerable<TSource> source,
      Function2<TSource, Integer, Enumerable<TCollection>> collectionSelector,
      Function2<TSource, TCollection, TResult> resultSelector) {
    throw Extensions.todo();
  }

  /**
   * Projects each element of a sequence to an
   * {@code Enumerable<TSource>}, flattens the resulting sequences into one
   * sequence, and invokes a result selector function on each
   * element therein.
   */
  public static <TSource, TCollection, TResult> Enumerable<TResult> selectMany(
      Enumerable<TSource> source,
      Function1<TSource, Enumerable<TCollection>> collectionSelector,
      Function2<TSource, TCollection, TResult> resultSelector) {
    throw Extensions.todo();
  }

  /**
   * Determines whether two sequences are equal by
   * comparing the elements by using the default equality comparer
   * for their type.
   */
  public static <TSource> boolean sequenceEqual(Enumerable<TSource> enumerable0,
      Enumerable<TSource> enumerable1) {
    throw Extensions.todo();
  }

  /**
   * Determines whether two sequences are equal by
   * comparing their elements by using a specified
   * {@code EqualityComparer<TSource>}.
   */
  public static <TSource> boolean sequenceEqual(Enumerable<TSource> enumerable0,
      Enumerable<TSource> enumerable1, EqualityComparer<TSource> comparer) {
    throw Extensions.todo();
  }

  /**
   * Returns the only element of a sequence, and throws
   * an exception if there is not exactly one element in the
   * sequence.
   */
  public static <TSource> TSource single(Enumerable<TSource> source) {
    throw Extensions.todo();
  }

  /**
   * Returns the only element of a sequence that
   * satisfies a specified condition, and throws an exception if
   * more than one such element exists.
   */
  public static <TSource> TSource single(Enumerable<TSource> source,
      Predicate1<TSource> predicate) {
    throw Extensions.todo();
  }

  /**
   * Returns the only element of a sequence, or a
   * default value if the sequence is empty; this method throws an
   * exception if there is more than one element in the
   * sequence.
   */
  public static <TSource> TSource singleOrDefault(Enumerable<TSource> source) {
    throw Extensions.todo();
  }

  /**
   * Returns the only element of a sequence that
   * satisfies a specified condition or a default value if no such
   * element exists; this method throws an exception if more than
   * one element satisfies the condition.
   */
  public static <TSource> TSource singleOrDefault(Enumerable<TSource> source,
      Predicate1<TSource> predicate) {
    throw Extensions.todo();
  }

  /**
   * Bypasses a specified number of elements in a
   * sequence and then returns the remaining elements.
   */
  public static <TSource> Enumerable<TSource> skip(Enumerable<TSource> source,
      final int count) {
    return skipWhile(source, new Predicate2<TSource, Integer>() {
      public boolean apply(TSource v1, Integer v2) {
        // Count is 1-based
        return v2 < count;
      }
    });
  }

  /**
   * Bypasses elements in a sequence as long as a
   * specified condition is true and then returns the remaining
   * elements.
   */
  public static <TSource> Enumerable<TSource> skipWhile(
      Enumerable<TSource> source, Predicate1<TSource> predicate) {
    return skipWhile(source, Functions.<TSource, Integer>toPredicate2(
        predicate));
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
        return new SkipWhileEnumerator<TSource>(source.enumerator(), predicate);
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
    return takeWhile(source, new Predicate2<TSource, Integer>() {
      public boolean apply(TSource v1, Integer v2) {
        // Count is 1-based
        return v2 < count;
      }
    });
  }

  /**
   * Returns elements from a sequence as long as a
   * specified condition is true.
   */
  public static <TSource> Enumerable<TSource> takeWhile(
      Enumerable<TSource> source, final Predicate1<TSource> predicate) {
    return takeWhile(source, Functions.<TSource, Integer>toPredicate2(
        predicate));
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
        return new TakeWhileEnumerator<TSource>(source.enumerator(), predicate);
      }
    };
  }

  /**
   * Performs a subsequent ordering of the elements in a sequence according
   * to a key.
   */
  public static <TSource, TKey> OrderedEnumerable<TSource>
  createOrderedEnumerable(
      OrderedEnumerable<TSource> source, Function1<TSource, TKey> keySelector,
      Comparator<TKey> comparator, boolean descending) {
    throw Extensions.todo();
  }

  /**
   * Performs a subsequent ordering of the elements in a sequence in
   * ascending order according to a key.
   */
  public static <TSource, TKey extends Comparable<TKey>>
  OrderedEnumerable<TSource> thenBy(
      OrderedEnumerable<TSource> source, Function1<TSource, TKey> keySelector) {
    return createOrderedEnumerable(source, keySelector,
        Extensions.<TKey>comparableComparator(), false);
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
        Extensions.<TKey>comparableComparator(), true);
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
    return toMap(source, keySelector, Functions.<TSource>identitySelector());
  }

  /**
   * Creates a {@code Map<TKey, TValue>} from an
   * {@code Enumerable<TSource>} according to a specified key selector function
   * and key comparer.
   */
  public static <TSource, TKey> Map<TKey, TSource> toMap(
      Enumerable<TSource> source, Function1<TSource, TKey> keySelector,
      EqualityComparer<TKey> comparer) {
    throw Extensions.todo();
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
    final Map<TKey, TElement> map = new LinkedHashMap<TKey, TElement>();
    final Enumerator<TSource> os = source.enumerator();
    try {
      while (os.moveNext()) {
        TSource o = os.current();
        map.put(keySelector.apply(o), elementSelector.apply(o));
      }
    } finally {
      os.close();
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
    throw Extensions.todo();
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
              ? new ArrayList<TSource>(((Collection) source).size())
              : new ArrayList<TSource>());
    }
  }

  /**
   * Creates a Lookup&lt;TKey, TElement&gt; from an
   * Enumerable&lt;TSource&gt; according to a specified key selector
   * function.
   */
  public static <TSource, TKey> Lookup<TKey, TSource> toLookup(
      Enumerable<TSource> source, Function1<TSource, TKey> keySelector) {
    return toLookup(source, keySelector, Functions.<TSource>identitySelector());
  }

  /**
   * Creates a {@code Lookup<TKey, TElement>} from an
   * {@code Enumerable<TSource>} according to a specified key selector function
   * and key comparer.
   */
  public static <TSource, TKey> Lookup<TKey, TSource> toLookup(
      Enumerable<TSource> source, Function1<TSource, TKey> keySelector,
      EqualityComparer<TKey> comparer) {
    return toLookup(source, keySelector, Functions.<TSource>identitySelector(),
        comparer);
  }

  /**
   * Creates a {@code Lookup<TKey, TElement>} from an
   * {@code Enumerable<TSource>} according to specified key selector and element
   * selector functions.
   */
  public static <TSource, TKey, TElement> Lookup<TKey, TElement> toLookup(
      Enumerable<TSource> source, Function1<TSource, TKey> keySelector,
      Function1<TSource, TElement> elementSelector) {
    final Map<TKey, List<TElement>> map = new HashMap<TKey, List<TElement>>();
    return toLookup_(map, source, keySelector, elementSelector);
  }

  static <TSource, TKey, TElement> LookupImpl<TKey, TElement> toLookup_(
      Map<TKey, List<TElement>> map, Enumerable<TSource> source,
      Function1<TSource, TKey> keySelector,
      Function1<TSource, TElement> elementSelector) {
    final Enumerator<TSource> os = source.enumerator();
    try {
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
            list = new ArrayList<TElement>();
            list.add(element);
          }
          list.add(elementSelector.apply(o));
        }
        map.put(key, list);
      }
    } finally {
      os.close();
    }
    return new LookupImpl<TKey, TElement>(map);
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
    return toLookup_(new WrapMap<TKey, List<TElement>>(comparer), source,
        keySelector, elementSelector);
  }

  /**
   * Produces the set union of two sequences by using
   * the default equality comparer.
   */
  public static <TSource> Enumerable<TSource> union(Enumerable<TSource> source0,
      Enumerable<TSource> source1) {
    Set<TSource> set = new HashSet<TSource>();
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
    Set<Wrapped<TSource>> set = new HashSet<Wrapped<TSource>>();
    Function1<TSource, Wrapped<TSource>> wrapper = wrapperFor(comparer);
    Function1<Wrapped<TSource>, TSource> unwrapper = unwrapper();
    source0.select(wrapper).into(set);
    source1.select(wrapper).into(set);
    return Linq4j.asEnumerable(set).select(unwrapper);
  }

  private static <TSource> Function1<Wrapped<TSource>, TSource> unwrapper() {
    return new Function1<Wrapped<TSource>, TSource>() {
      public TSource apply(Wrapped<TSource> a0) {
        return a0.element;
      }
    };
  }

  private static <TSource> Function1<TSource, Wrapped<TSource>> wrapperFor(
      final EqualityComparer<TSource> comparer) {
    return new Function1<TSource, Wrapped<TSource>>() {
      public Wrapped<TSource> apply(TSource a0) {
        return Wrapped.upAs(comparer, a0);
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
      Enumerable<T0> source0, Enumerable<T1> source1,
      Function2<T0, T1, TResult> resultSelector) {
    throw Extensions.todo();
  }

  public static <T> OrderedQueryable<T> asOrderedQueryable(
      Enumerable<T> source) {
    //noinspection unchecked
    return source instanceof OrderedQueryable
        ? ((OrderedQueryable<T>) source)
        : new EnumerableOrderedQueryable<T>(
            source, (Class) Object.class, null, null);
  }

  public static <T, C extends Collection<? super T>> C into(
      Enumerable<T> source, C sink) {
    final Enumerator<T> enumerator = source.enumerator();
    try {
      while (enumerator.moveNext()) {
        T t = enumerator.current();
        sink.add(t);
      }
    } finally {
      enumerator.close();
    }
    return sink;
  }

  static class TakeWhileEnumerator<TSource> implements Enumerator<TSource> {
    private final Enumerator<TSource> enumerator;
    private final Predicate2<TSource, Integer> predicate;

    boolean done = false;
    int n = -1;

    public TakeWhileEnumerator(Enumerator<TSource> enumerator,
        Predicate2<TSource, Integer> predicate) {
      this.enumerator = enumerator;
      this.predicate = predicate;
    }

    public TSource current() {
      return enumerator.current();
    }

    public boolean moveNext() {
      if (!done) {
        if (enumerator.moveNext() && predicate.apply(enumerator.current(),
            ++n)) {
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

  static class SkipWhileEnumerator<TSource> implements Enumerator<TSource> {
    private final Enumerator<TSource> enumerator;
    private final Predicate2<TSource, Integer> predicate;

    boolean started = false;
    int n = -1;

    public SkipWhileEnumerator(Enumerator<TSource> enumerator,
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

  static class CastingEnumerator<T> implements Enumerator<T> {
    private final Enumerator<?> enumerator;
    private final Class<T> clazz;

    public CastingEnumerator(Enumerator<?> enumerator, Class<T> clazz) {
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

  private static class Wrapped<T> {
    private final EqualityComparer<T> comparer;
    private final T element;

    private Wrapped(EqualityComparer<T> comparer, T element) {
      this.comparer = comparer;
      this.element = element;
    }

    static <T> Wrapped<T> upAs(EqualityComparer<T> comparer, T element) {
      return new Wrapped<T>(comparer, element);
    }

    @Override
    public int hashCode() {
      return comparer.hashCode(element);
    }

    @Override
    public boolean equals(Object obj) {
      //noinspection unchecked
      return obj == this || obj instanceof Wrapped && comparer.equal(element,
          ((Wrapped<T>) obj).element);
    }

    public T unwrap() {
      return element;
    }
  }

  private static class WrapMap<K, V> extends AbstractMap<K, V> {
    private final Map<Wrapped<K>, V> map = new HashMap<Wrapped<K>, V>();
    private final EqualityComparer<K> comparer;

    protected WrapMap(EqualityComparer<K> comparer) {
      this.comparer = comparer;
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
      return new AbstractSet<Entry<K, V>>() {
        @Override
        public Iterator<Entry<K, V>> iterator() {
          final Iterator<Entry<Wrapped<K>, V>> iterator =
              map.entrySet().iterator();

          return new Iterator<Entry<K, V>>() {
            public boolean hasNext() {
              return iterator.hasNext();
            }

            public Entry<K, V> next() {
              Entry<Wrapped<K>, V> next = iterator.next();
              return new SimpleEntry<K, V>(next.getKey().element,
                  next.getValue());
            }

            public void remove() {
              iterator.remove();
            }
          };
        }

        @Override
        public int size() {
          return map.size();
        }
      };
    }

    @Override
    public boolean containsKey(Object key) {
      return map.containsKey(wrap((K) key));
    }

    private Wrapped<K> wrap(K key) {
      return Wrapped.upAs(comparer, key);
    }

    @Override
    public V get(Object key) {
      return map.get(wrap((K) key));
    }

    @Override
    public V put(K key, V value) {
      return map.put(wrap(key), value);
    }

    @Override
    public V remove(Object key) {
      return map.remove(wrap((K) key));
    }

    @Override
    public void clear() {
      map.clear();
    }

    @Override
    public Collection<V> values() {
      return map.values();
    }
  }
}

// End EnumerableDefaults.java
