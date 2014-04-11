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

import net.hydromatic.linq4j.expressions.Expression;
import net.hydromatic.linq4j.expressions.Expressions;
import net.hydromatic.linq4j.expressions.FunctionExpression;
import net.hydromatic.linq4j.function.*;

import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.*;

/**
 * Default implementations for methods in the {@link Queryable} interface.
 */
public abstract class QueryableDefaults {

  /**
   * Applies an accumulator function over a
   * sequence.
   */
  public static <T> T aggregate(Queryable<T> queryable,
      FunctionExpression<Function2<T, T, T>> func) {
    throw Extensions.todo();
  }

  /**
   * Applies an accumulator function over a
   * sequence. The specified seed value is used as the initial
   * accumulator value.
   */
  public static <T, TAccumulate> TAccumulate aggregate(Queryable<T> queryable,
      TAccumulate seed,
      FunctionExpression<Function2<TAccumulate, T, TAccumulate>> func) {
    throw Extensions.todo();
  }

  /**
   * Applies an accumulator function over a
   * sequence. The specified seed value is used as the initial
   * accumulator value, and the specified function is used to select
   * the result value.
   */
  public static <T, TAccumulate, TResult> TResult aggregate(
      Queryable<T> queryable, TAccumulate seed,
      FunctionExpression<Function2<TAccumulate, T, TAccumulate>> func,
      FunctionExpression<Function1<TAccumulate, TResult>> selector) {
    throw Extensions.todo();
  }

  /**
   * Determines whether all the elements of a sequence
   * satisfy a condition.
   */
  public static <T> boolean all(Queryable<T> queryable,
      FunctionExpression<Predicate1<T>> predicate) {
    throw Extensions.todo();
  }

  /**
   * Determines whether a sequence contains any
   * elements.
   */
  public static <T> void any(Queryable<T> queryable) {
    throw Extensions.todo();
  }

  /**
   * Determines whether any element of a sequence
   * satisfies a condition.
   */
  public static <T> boolean any(Queryable<T> queryable,
      FunctionExpression<Predicate1<T>> predicate) {
    throw Extensions.todo();
  }

  /**
   * Converts a generic {@code Enumerable<T>} to a generic
   * {@code IQueryable<T>}.
   */
  public static <T> Queryable<T> asQueryable(Queryable<T> queryable) {
    throw Extensions.todo();
  }

  /**
   * Computes the average of a sequence of Decimal
   * values that is obtained by invoking a projection function on
   * each element of the input sequence.
   */
  public static <T> BigDecimal averageBigDecimal(Queryable<T> queryable,
      FunctionExpression<BigDecimalFunction1<T>> selector) {
    throw Extensions.todo();
  }

  /**
   * Computes the average of a sequence of nullable
   * Decimal values that is obtained by invoking a projection
   * function on each element of the input sequence.
   */
  public static <T> BigDecimal averageNullableBigDecimal(Queryable<T> queryable,
      FunctionExpression<NullableBigDecimalFunction1<T>> selector) {
    throw Extensions.todo();
  }

  /**
   * Computes the average of a sequence of Double
   * values that is obtained by invoking a projection function on
   * each element of the input sequence.
   */
  public static <T> double averageDouble(Queryable<T> queryable,
      FunctionExpression<DoubleFunction1<T>> selector) {
    throw Extensions.todo();
  }

  /**
   * Computes the average of a sequence of nullable
   * Double values that is obtained by invoking a projection
   * function on each element of the input sequence.
   */
  public static <T> Double averageNullableDouble(Queryable<T> queryable,
      FunctionExpression<NullableDoubleFunction1<T>> selector) {
    throw Extensions.todo();
  }

  /**
   * Computes the average of a sequence of int values
   * that is obtained by invoking a projection function on each
   * element of the input sequence.
   */
  public static <T> int averageInteger(Queryable<T> queryable,
      FunctionExpression<IntegerFunction1<T>> selector) {
    throw Extensions.todo();
  }

  /**
   * Computes the average of a sequence of nullable
   * int values that is obtained by invoking a projection function
   * on each element of the input sequence.
   */
  public static <T> Integer averageNullableInteger(Queryable<T> queryable,
      FunctionExpression<NullableIntegerFunction1<T>> selector) {
    throw Extensions.todo();
  }

  /**
   * Computes the average of a sequence of Float
   * values that is obtained by invoking a projection function on
   * each element of the input sequence.
   */
  public static <T> float averageFloat(Queryable<T> queryable,
      FunctionExpression<FloatFunction1<T>> selector) {
    throw Extensions.todo();
  }

  /**
   * Computes the average of a sequence of nullable
   * Float values that is obtained by invoking a projection
   * function on each element of the input sequence.
   */
  public static <T> Float averageNullableFloat(Queryable<T> queryable,
      FunctionExpression<NullableFloatFunction1<T>> selector) {
    throw Extensions.todo();
  }

  /**
   * Computes the average of a sequence of long values
   * that is obtained by invoking a projection function on each
   * element of the input sequence.
   */
  public static <T> long averageLong(Queryable<T> queryable,
      FunctionExpression<LongFunction1<T>> selector) {
    throw Extensions.todo();
  }

  /**
   * Computes the average of a sequence of nullable
   * long values that is obtained by invoking a projection function
   * on each element of the input sequence.
   */
  public static <T> Long averageNullableLong(Queryable<T> queryable,
      FunctionExpression<NullableLongFunction1<T>> selector) {
    throw Extensions.todo();
  }

  /**
   * <p>Analogous to LINQ's Enumerable.Cast extension method.</p>
   *
   * @param clazz Target type
   * @param <T2> Target type
   *
   * @return Collection of T2
   */
  public static <T, T2> Queryable<T2> cast(final Queryable<T> source,
      final Class<T2> clazz) {
    return new BaseQueryable<T2>(source.getProvider(), clazz,
        source.getExpression()) {
      public Enumerator<T2> enumerator() {
        return new EnumerableDefaults.CastingEnumerator<T2>(source.enumerator(),
            clazz);
      }
    };
  }

  /**
   * Concatenates two sequences.
   */
  public static <T> Queryable<T> concat(Queryable<T> queryable0,
      Enumerable<T> source2) {
    throw Extensions.todo();
  }

  /**
   * Determines whether a sequence contains a specified
   * element by using the default equality comparer.
   */
  public static <T> boolean contains(Queryable<T> queryable, T element) {
    throw Extensions.todo();
  }

  /**
   * Determines whether a sequence contains a specified
   * element by using a specified {@code EqualityComparer<T>}.
   */
  public static <T> boolean contains(Queryable<T> queryable, T element,
      EqualityComparer comparer) {
    throw Extensions.todo();
  }

  /**
   * Returns the number of elements in a
   * sequence.
   */
  public static <T> int count(Queryable<T> queryable) {
    throw Extensions.todo();
  }

  /**
   * Returns the number of elements in the specified
   * sequence that satisfies a condition.
   */
  public static <T> int count(Queryable<T> queryable,
      FunctionExpression<Predicate1<T>> func) {
    throw Extensions.todo();
  }

  /**
   * Returns the elements of the specified sequence or
   * the type parameter's default value in a singleton collection if
   * the sequence is empty.
   */
  public static <T> Queryable<T> defaultIfEmpty(Queryable<T> queryable) {
    throw Extensions.todo();
  }

  /**
   * Returns the elements of the specified sequence or
   * the specified value in a singleton collection if the sequence
   * is empty.
   */
  public static <T> T defaultIfEmpty(Queryable<T> queryable, T value) {
    throw Extensions.todo();
  }

  /**
   * Returns distinct elements from a sequence by using
   * the default equality comparer to compare values.
   */
  public static <T> Queryable<T> distinct(Queryable<T> queryable) {
    throw Extensions.todo();
  }

  /**
   * Returns distinct elements from a sequence by using
   * a specified {@code EqualityComparer<T>} to compare values.
   */
  public static <T> Queryable<T> distinct(Queryable<T> queryable,
      EqualityComparer comparer) {
    throw Extensions.todo();
  }

  /**
   * Returns the element at a specified index in a
   * sequence.
   */
  public static <T> T elementAt(Queryable<T> queryable, int index) {
    throw Extensions.todo();
  }

  /**
   * Returns the element at a specified index in a
   * sequence or a default value if the index is out of
   * range.
   */
  public static <T> T elementAtOrDefault(Queryable<T> queryable, int index) {
    throw Extensions.todo();
  }

  /**
   * Produces the set difference of two sequences by
   * using the default equality comparer to compare values. (Defined
   * by Queryable.)
   */
  public static <T> Queryable<T> except(Queryable<T> queryable,
      Enumerable<T> enumerable) {
    throw Extensions.todo();
  }

  /**
   * Produces the set difference of two sequences by
   * using the specified {@code EqualityComparer<T>} to compare
   * values.
   */
  public static <T> Queryable<T> except(Queryable<T> queryable,
      Enumerable<T> enumerable, EqualityComparer<T> comparer) {
    throw Extensions.todo();
  }

  /**
   * Returns the first element of a sequence. (Defined
   * by Queryable.)
   */
  public static <T> T first(Queryable<T> queryable) {
    throw Extensions.todo();
  }

  /**
   * Returns the first element of a sequence that
   * satisfies a specified condition.
   */
  public static <T> T first(Queryable<T> queryable,
      FunctionExpression<Predicate1<T>> predicate) {
    throw Extensions.todo();
  }

  /**
   * Returns the first element of a sequence, or a
   * default value if the sequence contains no elements.
   */
  public static <T> T firstOrDefault(Queryable<T> queryable) {
    throw Extensions.todo();
  }

  /**
   * Returns the first element of a sequence that
   * satisfies a specified condition or a default value if no such
   * element is found.
   */
  public static <T> T firstOrDefault(Queryable<T> queryable,
      FunctionExpression<Predicate1<T>> predicate) {
    throw Extensions.todo();
  }

  /**
   * Groups the elements of a sequence according to a
   * specified key selector function.
   */
  public static <T, TKey> Queryable<Grouping<TKey, T>> groupBy(
      Queryable<T> queryable,
      FunctionExpression<Function1<T, TKey>> keySelector) {
    throw Extensions.todo();
  }

  /**
   * Groups the elements of a sequence according to a
   * specified key selector function and compares the keys by using
   * a specified comparer.
   */
  public static <T, TKey> Queryable<Grouping<TKey, T>> groupBy(
      Queryable<T> queryable,
      FunctionExpression<Function1<T, TKey>> keySelector,
      EqualityComparer comparer) {
    throw Extensions.todo();
  }

  /**
   * Groups the elements of a sequence according to a
   * specified key selector function and projects the elements for
   * each group by using a specified function.
   */
  public static <T, TKey, TElement> Queryable<Grouping<TKey, TElement>> groupBy(
      Queryable<T> queryable,
      FunctionExpression<Function1<T, TKey>> keySelector,
      FunctionExpression<Function1<T, TElement>> elementSelector) {
    throw Extensions.todo();
  }

  /**
   * Groups the elements of a sequence according to a
   * specified key selector function and creates a result value from
   * each group and its key.
   *
   * <p>NOTE: Renamed from {@code groupBy} to distinguish from
   * {@link #groupBy(net.hydromatic.linq4j.Queryable, net.hydromatic.linq4j.expressions.FunctionExpression, net.hydromatic.linq4j.expressions.FunctionExpression)},
   * which has the same erasure.</p>
   */
  public static <T, TKey, TResult> Queryable<Grouping<TKey, TResult>> groupByK(
      Queryable<T> queryable,
      FunctionExpression<Function1<T, TKey>> keySelector,
      FunctionExpression<Function2<TKey, Enumerable<T>, TResult>>
        elementSelector) {
    throw Extensions.todo();
  }

  /**
   * Groups the elements of a sequence and projects the
   * elements for each group by using a specified function. Key
   * values are compared by using a specified comparer.
   */
  public static <T, TKey, TElement> Queryable<Grouping<TKey, TElement>> groupBy(
      Queryable<T> queryable,
      FunctionExpression<Function1<T, TKey>> keySelector,
      FunctionExpression<Function1<T, TElement>> elementSelector,
      EqualityComparer comparer) {
    throw Extensions.todo();
  }

  /**
   * Groups the elements of a sequence according to a
   * specified key selector function and creates a result value from
   * each group and its key. Keys are compared by using a specified
   * comparer.
   *
   * <p>NOTE: Renamed from {@code groupBy} to distinguish from
   * {@link #groupBy(net.hydromatic.linq4j.Queryable, net.hydromatic.linq4j.expressions.FunctionExpression, net.hydromatic.linq4j.expressions.FunctionExpression, net.hydromatic.linq4j.function.EqualityComparer)},
   * which has the same erasure.</p>
   */
  public static <T, TKey, TResult> Queryable<TResult> groupByK(
      Queryable<T> queryable,
      FunctionExpression<Function1<T, TKey>> keySelector,
      FunctionExpression<Function2<TKey, Enumerable<T>, TResult>>
        elementSelector,
      EqualityComparer comparer) {
    throw Extensions.todo();
  }

  /**
   * Groups the elements of a sequence according to a
   * specified key selector function and creates a result value from
   * each group and its key. The elements of each group are
   * projected by using a specified function.
   */
  public static <T, TKey, TElement, TResult> Queryable<TResult> groupBy(
      Queryable<T> queryable,
      FunctionExpression<Function1<T, TKey>> keySelector,
      FunctionExpression<Function1<T, TElement>> elementSelector,
      FunctionExpression<Function2<TKey, Enumerable<TElement>, TResult>>
        resultSelector) {
    throw Extensions.todo();
  }

  /**
   * Groups the elements of a sequence according to a
   * specified key selector function and creates a result value from
   * each group and its key. Keys are compared by using a specified
   * comparer and the elements of each group are projected by using
   * a specified function.
   */
  public static <T, TKey, TElement, TResult> Queryable<TResult> groupBy(
      Queryable<T> queryable,
      FunctionExpression<Function1<T, TKey>> keySelector,
      FunctionExpression<Function1<T, TElement>> elementSelector,
      FunctionExpression<Function2<TKey, Enumerable<TElement>, TResult>>
        resultSelector,
      EqualityComparer<TKey> comparer) {
    throw Extensions.todo();
  }

  /**
   * Correlates the elements of two sequences based on
   * key equality and groups the results. The default equality
   * comparer is used to compare keys.
   */
  public static <TOuter, TInner, TKey, TResult> Queryable<TResult> groupJoin(
      Queryable<TOuter> outer, Enumerable<TInner> inner,
      FunctionExpression<Function1<TOuter, TKey>> outerKeySelector,
      FunctionExpression<Function1<TInner, TKey>> innerKeySelector,
      FunctionExpression<Function2<TOuter, Enumerable<TInner>, TResult>>
        resultSelector) {
    throw Extensions.todo();
  }

  /**
   * Correlates the elements of two sequences based on
   * key equality and groups the results. A specified
   * {@code EqualityComparer<T>} is used to compare keys.
   */
  public static <TOuter, TInner, TKey, TResult> Enumerable<TResult> groupJoin(
      Queryable<TOuter> outer, Enumerable<TInner> inner,
      FunctionExpression<Function1<TOuter, TKey>> outerKeySelector,
      FunctionExpression<Function1<TInner, TKey>> innerKeySelector,
      FunctionExpression<Function2<TOuter, Enumerable<TInner>, TResult>>
        resultSelector,
      EqualityComparer<TKey> comparer) {
    throw Extensions.todo();
  }

  /**
   * Produces the set intersection of two sequences by
   * using the default equality comparer to compare values. (Defined
   * by Queryable.)
   */
  public static <T> Queryable<T> intersect(Queryable<T> queryable,
      Enumerable<T> enumerable) {
    throw Extensions.todo();
  }

  /**
   * Produces the set intersection of two sequences by
   * using the specified {@code EqualityComparer<T>} to compare
   * values.
   */
  public static <T> Queryable<T> intersect(Queryable<T> queryable,
      Enumerable<T> enumerable, EqualityComparer<T> comparer) {
    throw Extensions.todo();
  }

  /**
   * Correlates the elements of two sequences based on
   * matching keys. The default equality comparer is used to compare
   * keys.
   */
  public static <TOuter, TInner, TKey, TResult> Queryable<TResult> join(
      Queryable<TOuter> outer, Enumerable<TInner> inner,
      FunctionExpression<Function1<TOuter, TKey>> outerKeySelector,
      FunctionExpression<Function1<TInner, TKey>> innerKeySelector,
      FunctionExpression<Function2<TOuter, TInner, TResult>> resultSelector) {
    throw Extensions.todo();
  }

  /**
   * Correlates the elements of two sequences based on
   * matching keys. A specified {@code EqualityComparer<T>} is used to
   * compare keys.
   */
  public static <TOuter, TInner, TKey, TResult> Queryable<TResult> join(
      Queryable<TOuter> outer, Enumerable<TInner> inner,
      FunctionExpression<Function1<TOuter, TKey>> outerKeySelector,
      FunctionExpression<Function1<TInner, TKey>> innerKeySelector,
      FunctionExpression<Function2<TOuter, TInner, TResult>> resultSelector,
      EqualityComparer<TKey> comparer) {
    throw Extensions.todo();
  }

  /**
   * Returns the last element in a sequence. (Defined
   * by Queryable.)
   */
  public static <T> T last(Queryable<T> queryable) {
    throw Extensions.todo();
  }

  /**
   * Returns the last element of a sequence that
   * satisfies a specified condition.
   */
  public static <T> T last(Queryable<T> queryable,
      FunctionExpression<Predicate1<T>> predicate) {
    throw Extensions.todo();
  }

  /**
   * Returns the last element in a sequence, or a
   * default value if the sequence contains no elements.
   */
  public static <T> T lastOrDefault(Queryable<T> queryable) {
    throw Extensions.todo();
  }

  /**
   * Returns the last element of a sequence that
   * satisfies a condition or a default value if no such element is
   * found.
   */
  public static <T> T lastOrDefault(Queryable<T> queryable,
      FunctionExpression<Predicate1<T>> predicate) {
    throw Extensions.todo();
  }

  /**
   * Returns an long that represents the total number
   * of elements in a sequence.
   */
  public static <T> long longCount(Queryable<T> xable) {
    throw Extensions.todo();
  }

  /**
   * Returns an long that represents the number of
   * elements in a sequence that satisfy a condition.
   */
  public static <T> long longCount(Queryable<T> queryable,
      FunctionExpression<Predicate1<T>> predicate) {
    throw Extensions.todo();
  }

  /**
   * Returns the maximum value in a generic
   * {@code IQueryable<T>}.
   */
  public static <T> T max(Queryable<T> queryable) {
    throw Extensions.todo();
  }

  /**
   * Invokes a projection function on each element of a
   * generic {@code IQueryable<T>} and returns the maximum resulting
   * value.
   */
  public static <T, TResult> TResult max(Queryable<T> queryable,
      FunctionExpression<Function1<T, TResult>> selector) {
    throw Extensions.todo();
  }

  /**
   * Returns the minimum value in a generic
   * {@code IQueryable<T>}.
   */
  public static <T> T min(Queryable<T> queryable) {
    throw Extensions.todo();
  }

  /**
   * Invokes a projection function on each element of a
   * generic {@code IQueryable<T>} and returns the minimum resulting
   * value.
   */
  public static <T, TResult> TResult min(Queryable<T> queryable,
      FunctionExpression<Function1<T, TResult>> selector) {
    throw Extensions.todo();
  }

  /**
   * Filters the elements of an IQueryable based on a
   * specified type.
   *
   * <p>This method generates a
   * {@link net.hydromatic.linq4j.expressions.MethodCallExpression} that
   * represents calling {@code ofType} itself as a constructed generic method.
   * It then passes the {@code MethodCallExpression} to the
   * {@link net.hydromatic.linq4j.QueryProvider#createQuery createQuery} method of the
   * {@link net.hydromatic.linq4j.QueryProvider} represented by the Provider property of the source
   * parameter.</p>
   *
   * <p>The query behavior that occurs as a result of executing an expression
   * tree that represents calling OfType depends on the implementation of the
   * type of the source parameter. The expected behavior is that it filters
   * out any elements in source that are not of type TResult.
   *
   * <p>NOTE: clazz parameter not present in C# LINQ; necessary because of
   * Java type erasure.</p>
   */
  public static <TResult> Queryable<TResult> ofType(Queryable<?> queryable,
      Class<TResult> clazz) {
    throw Extensions.todo();
  }

  /**
   * Sorts the elements of a sequence in ascending
   * order according to a key.
   *
   * @see #thenBy
   */
  public static <T, TKey extends Comparable> OrderedQueryable<T> orderBy(
      Queryable<T> source, FunctionExpression<Function1<T, TKey>> keySelector) {
    throw Extensions.todo();
  }

  /**
   * Sorts the elements of a sequence in ascending
   * order by using a specified comparer.
   */
  public static <T, TKey> OrderedQueryable<T> orderBy(Queryable<T> source,
      FunctionExpression<Function1<T, TKey>> keySelector,
      Comparator<TKey> comparator) {
    throw Extensions.todo();
  }

  /**
   * Sorts the elements of a sequence in descending
   * order according to a key.
   */
  public static <T, TKey extends Comparable> OrderedQueryable<T>
  orderByDescending(Queryable<T> source,
      FunctionExpression<Function1<T, TKey>> keySelector) {
    throw Extensions.todo();
  }

  /**
   * Sorts the elements of a sequence in descending
   * order by using a specified comparer.
   */
  public static <T, TKey> OrderedQueryable<T> orderByDescending(
      Queryable<T> source, FunctionExpression<Function1<T, TKey>> keySelector,
      Comparator<TKey> comparator) {
    throw Extensions.todo();
  }

  /**
   * Inverts the order of the elements in a
   * sequence.
   */
  public static <T> Queryable<T> reverse(Queryable<T> source) {
    throw Extensions.todo();
  }

  /**
   * Projects each element of a sequence into a new form.
   */
  public static <T, TResult> Queryable<TResult> select(Queryable<T> source,
      FunctionExpression<Function1<T, TResult>> selector) {
    return source.getProvider().createQuery(
        Expressions.call(source.getExpression(), "select", selector),
        functionResultType(selector));
  }

  private static <P0, R> Type functionResultType(
      FunctionExpression<Function1<P0, R>> selector) {
    return selector.body.getType();
  }

  /**
   * Projects each element of a sequence into a new
   * form by incorporating the element's index.
   *
   * <p>NOTE: Renamed from {@code select} because had same erasure as
   * {@link #select(net.hydromatic.linq4j.Queryable, net.hydromatic.linq4j.expressions.FunctionExpression)}.</p>
   */
  public static <T, TResult> Queryable<TResult> selectN(Queryable<T> source,
      FunctionExpression<Function2<T, Integer, TResult>> selector) {
    throw Extensions.todo();
  }

  /**
   * Projects each element of a sequence to an
   * {@code Enumerable<T>} and combines the resulting sequences into one
   * sequence.
   */
  public static <T, TResult> Queryable<TResult> selectMany(Queryable<T> source,
      FunctionExpression<Function1<T, Enumerable<TResult>>> selector) {
    throw Extensions.todo();
  }

  /**
   * Projects each element of a sequence to an
   * {@code Enumerable<T>} and combines the resulting sequences into one
   * sequence. The index of each source element is used in the
   * projected form of that element.
   *
   * <p>NOTE: Renamed from {@code selectMany} because had same erasure as
   * {@link #selectMany(net.hydromatic.linq4j.Queryable, net.hydromatic.linq4j.expressions.FunctionExpression)}</p>
   */
  public static <T, TResult> Queryable<TResult> selectManyN(Queryable<T> source,
      FunctionExpression<Function2<T, Integer, Enumerable<TResult>>> selector) {
    throw Extensions.todo();
  }

  /**
   * Projects each element of a sequence to an
   * {@code Enumerable<T>} that incorporates the index of the source
   * element that produced it. A result selector function is invoked
   * on each element of each intermediate sequence, and the
   * resulting values are combined into a single, one-dimensional
   * sequence and returned.
   */
  public static <T, TCollection, TResult> Queryable<TResult> selectMany(
      Queryable<T> source,
      FunctionExpression<Function2<T, Integer, Enumerable<TCollection>>>
        collectionSelector,
      FunctionExpression<Function2<T, TCollection, TResult>> resultSelector) {
    throw Extensions.todo();
  }

  /**
   * Projects each element of a sequence to an
   * {@code Enumerable<T>} and invokes a result selector function on each
   * element therein. The resulting values from each intermediate
   * sequence are combined into a single, one-dimensional sequence
   * and returned.
   *
   * <p>NOTE: Renamed from {@code selectMany} because had same erasure as
   * {@link #selectMany(net.hydromatic.linq4j.Queryable, net.hydromatic.linq4j.expressions.FunctionExpression, net.hydromatic.linq4j.expressions.FunctionExpression)}</p>
   */
  public static <T, TCollection, TResult> Queryable<TResult> selectManyN(
      Queryable<T> source,
      FunctionExpression<Function1<T, Enumerable<TCollection>>>
        collectionSelector,
      FunctionExpression<Function2<T, TCollection, TResult>> resultSelector) {
    throw Extensions.todo();
  }

  /**
   * Determines whether two sequences are equal by
   * using the default equality comparer to compare
   * elements.
   */
  public static <T> boolean sequenceEqual(Queryable<T> queryable,
      Enumerable<T> enumerable) {
    throw Extensions.todo();
  }

  /**
   * Determines whether two sequences are equal by
   * using a specified {@code EqualityComparer<T>} to compare
   * elements.
   */
  public static <T> boolean sequenceEqual(Queryable<T> queryable,
      Enumerable<T> enumerable, EqualityComparer<T> comparer) {
    throw Extensions.todo();
  }

  /**
   * Returns the only element of a sequence, and throws
   * an exception if there is not exactly one element in the
   * sequence.
   */
  public static <T> T single(Queryable<T> source) {
    throw Extensions.todo();
  }

  /**
   * Returns the only element of a sequence that
   * satisfies a specified condition, and throws an exception if
   * more than one such element exists.
   */
  public static <T> T single(Queryable<T> source,
      FunctionExpression<Predicate1<T>> predicate) {
    throw Extensions.todo();
  }

  /**
   * Returns the only element of a sequence, or a
   * default value if the sequence is empty; this method throws an
   * exception if there is more than one element in the
   * sequence.
   */
  public static <T> T singleOrDefault(Queryable<T> source) {
    throw Extensions.todo();
  }

  /**
   * Returns the only element of a sequence that
   * satisfies a specified condition or a default value if no such
   * element exists; this method throws an exception if more than
   * one element satisfies the condition.
   */
  public static <T> T singleOrDefault(Queryable<T> source,
      FunctionExpression<Predicate1<T>> predicate) {
    throw Extensions.todo();
  }

  /**
   * Bypasses a specified number of elements in a
   * sequence and then returns the remaining elements.
   */
  public static <T> Queryable<T> skip(Queryable<T> source, int count) {
    return EnumerableDefaults.skip(source.asEnumerable(), count).asQueryable();
  }

  /**
   * Bypasses elements in a sequence as long as a
   * specified condition is true and then returns the remaining
   * elements.
   */
  public static <T> Queryable<T> skipWhile(Queryable<T> source,
      FunctionExpression<Predicate1<T>> predicate) {
    return skipWhileN(source, Expressions.lambda(
        Functions.<T, Integer>toPredicate2(predicate.getFunction())));
  }

  /**
   * Bypasses elements in a sequence as long as a
   * specified condition is true and then returns the remaining
   * elements. The element's index is used in the logic of the
   * predicate function.
   */
  public static <T> Queryable<T> skipWhileN(final Queryable<T> source,
      final FunctionExpression<Predicate2<T, Integer>> predicate) {
    return new BaseQueryable<T>(source.getProvider(), source.getElementType(),
        source.getExpression()) {
      public Enumerator<T> enumerator() {
        return new EnumerableDefaults.SkipWhileEnumerator<T>(
            source.enumerator(), predicate.getFunction());
      }
    };
  }

  /**
   * Computes the sum of the sequence of Decimal values
   * that is obtained by invoking a projection function on each
   * element of the input sequence.
   */
  public static <T> BigDecimal sumBigDecimal(Queryable<T> sources,
      FunctionExpression<BigDecimalFunction1<T>> selector) {
    throw Extensions.todo();
  }

  /**
   * Computes the sum of the sequence of nullable
   * Decimal values that is obtained by invoking a projection
   * function on each element of the input sequence.
   */
  public static <T> BigDecimal sumNullableBigDecimal(Queryable<T> source,
      FunctionExpression<NullableBigDecimalFunction1<T>> selector) {
    throw Extensions.todo();
  }

  /**
   * Computes the sum of the sequence of Double values
   * that is obtained by invoking a projection function on each
   * element of the input sequence.
   */
  public static <T> double sumDouble(Queryable<T> source,
      FunctionExpression<DoubleFunction1<T>> selector) {
    throw Extensions.todo();
  }

  /**
   * Computes the sum of the sequence of nullable
   * Double values that is obtained by invoking a projection
   * function on each element of the input sequence.
   */
  public static <T> Double sumNullableDouble(Queryable<T> source,
      FunctionExpression<NullableDoubleFunction1<T>> selector) {
    throw Extensions.todo();
  }

  /**
   * Computes the sum of the sequence of int values
   * that is obtained by invoking a projection function on each
   * element of the input sequence.
   */
  public static <T> int sumInteger(Queryable<T> source,
      FunctionExpression<IntegerFunction1<T>> selector) {
    throw Extensions.todo();
  }

  /**
   * Computes the sum of the sequence of nullable int
   * values that is obtained by invoking a projection function on
   * each element of the input sequence.
   */
  public static <T> Integer sumNullableInteger(Queryable<T> source,
      FunctionExpression<NullableIntegerFunction1<T>> selector) {
    throw Extensions.todo();
  }

  /**
   * Computes the sum of the sequence of long values
   * that is obtained by invoking a projection function on each
   * element of the input sequence.
   */
  public static <T> long sumLong(Queryable<T> source,
      FunctionExpression<LongFunction1<T>> selector) {
    throw Extensions.todo();
  }

  /**
   * Computes the sum of the sequence of nullable long
   * values that is obtained by invoking a projection function on
   * each element of the input sequence.
   */
  public static <T> Long sumNullableLong(Queryable<T> source,
      FunctionExpression<NullableLongFunction1<T>> selector) {
    throw Extensions.todo();
  }

  /**
   * Computes the sum of the sequence of Float values
   * that is obtained by invoking a projection function on each
   * element of the input sequence.
   */
  public static <T> float sumFloat(Queryable<T> source,
      FunctionExpression<FloatFunction1<T>> selector) {
    throw Extensions.todo();
  }

  /**
   * Computes the sum of the sequence of nullable
   * Float values that is obtained by invoking a projection
   * function on each element of the input sequence.
   */
  public static <T> Float sumNullableFloat(Queryable<T> source,
      FunctionExpression<NullableFloatFunction1<T>> selector) {
    throw Extensions.todo();
  }

  /**
   * Returns a specified number of contiguous elements
   * from the start of a sequence.
   */
  public static <T> Queryable<T> take(Queryable<T> source, int count) {
    return EnumerableDefaults.take(source.asEnumerable(), count).asQueryable();
  }

  /**
   * Returns elements from a sequence as long as a
   * specified condition is true.
   */
  public static <T> Queryable<T> takeWhile(Queryable<T> source,
      FunctionExpression<Predicate1<T>> predicate) {
    return takeWhileN(source, Expressions.lambda(
        Functions.<T, Integer>toPredicate2(predicate.getFunction())));
  }

  /**
   * Returns elements from a sequence as long as a
   * specified condition is true. The element's index is used in the
   * logic of the predicate function.
   */
  public static <T> Queryable<T> takeWhileN(final Queryable<T> source,
      final FunctionExpression<Predicate2<T, Integer>> predicate) {
    return new BaseQueryable<T>(source.getProvider(), source.getElementType(),
        source.getExpression()) {
      public Enumerator<T> enumerator() {
        return new EnumerableDefaults.TakeWhileEnumerator<T>(
            source.enumerator(), predicate.getFunction());
      }
    };
  }

  /**
   * Performs a subsequent ordering of the elements in a sequence in
   * ascending order according to a key.
   */
  public static <T, TKey extends Comparable<TKey>> OrderedQueryable<T> thenBy(
      OrderedQueryable<T> source,
      FunctionExpression<Function1<T, TKey>> keySelector) {
    throw Extensions.todo();
  }

  /**
   * Performs a subsequent ordering of the elements in a sequence in
   * ascending order according to a key, using a specified comparator.
   */
  public static <T, TKey> OrderedQueryable<T> thenBy(OrderedQueryable<T> source,
      FunctionExpression<Function1<T, TKey>> keySelector,
      Comparator<TKey> comparator) {
    throw Extensions.todo();
  }

  /**
   * Performs a subsequent ordering of the elements in a sequence in
   * descending order according to a key.
   */
  public static <T, TKey extends Comparable<TKey>> OrderedQueryable<T>
  thenByDescending(OrderedQueryable<T> source,
      FunctionExpression<Function1<T, TKey>> keySelector) {
    throw Extensions.todo();
  }

  /**
   * Performs a subsequent ordering of the elements in a sequence in
   * dscending order according to a key, using a specified comparator.
   */
  public static <T, TKey> OrderedQueryable<T> thenByDescending(
      OrderedQueryable<T> source,
      FunctionExpression<Function1<T, TKey>> keySelector,
      Comparator<TKey> comparator) {
    throw Extensions.todo();
  }

  /**
   * Produces the set union of two sequences by using
   * the default equality comparer.
   */
  public static <T> Queryable<T> union(Queryable<T> source0,
      Enumerable<T> source1) {
    throw Extensions.todo();
  }

  /**
   * Produces the set union of two sequences by using a
   * specified {@code EqualityComparer<T>}.
   */
  public static <T> Queryable<T> union(Queryable<T> source0,
      Enumerable<T> source1, EqualityComparer<T> comparer) {
    throw Extensions.todo();
  }

  /**
   * Filters a sequence of values based on a
   * predicate.
   */
  public static <T> Queryable<T> where(final Queryable<T> source,
      final FunctionExpression<Predicate1<T>> predicate) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.where(source, predicate);
      }
    };
  }

  /**
   * Filters a sequence of values based on a
   * predicate. Each element's index is used in the logic of the
   * predicate function.
   */
  public static <T> Queryable<T> whereN(Queryable<T> source,
      FunctionExpression<Predicate2<T, Integer>> predicate) {
    throw Extensions.todo();
  }

  /**
   * Merges two sequences by using the specified
   * predicate function.
   */
  public static <T0, T1, TResult> Queryable<TResult> zip(Queryable<T0> source0,
      Enumerable<T1> source1,
      FunctionExpression<Function2<T0, T1, TResult>> resultSelector) {
    throw Extensions.todo();
  }

  public interface Replayable<T> extends Queryable<T> {
    void replay(QueryableFactory<T> factory);
  }

  public abstract static class ReplayableQueryable<T>
      extends DefaultQueryable<T> implements Replayable<T> {
    public void replay(QueryableFactory<T> factory) {
    }

    public Iterator<T> iterator() {
      return Linq4j.enumeratorIterator(enumerator());
    }

    public Enumerator<T> enumerator() {
      return getProvider().executeQuery(this);
    }

    /**
     * Convenience method, for {@link QueryableRecorder} methods that
     * return a scalar value such as {@code boolean} or
     * {@link BigDecimal}.
     */
    @SuppressWarnings("unchecked")
    <U> U castSingle() {
      return ((Queryable<U>) (Queryable) this).single();
    }

    /**
     * Convenience method, for {@link QueryableRecorder} methods that
     * return a Queryable of a different element type than the source.
     */
    @SuppressWarnings("unchecked")
    public <U> Queryable<U> castQueryable() {
      return (Queryable<U>) (Queryable) this;
    }
  }

  public abstract static class NonLeafReplayableQueryable<T>
      extends ReplayableQueryable<T> {
    private final Queryable<T> original;

    protected NonLeafReplayableQueryable(Queryable<T> original) {
      this.original = original;
    }

    public Type getElementType() {
      return original.getElementType();
    }

    public Expression getExpression() {
      return original.getExpression();
    }

    public QueryProvider getProvider() {
      return original.getProvider();
    }
  }
}

// End QueryableDefaults.java
