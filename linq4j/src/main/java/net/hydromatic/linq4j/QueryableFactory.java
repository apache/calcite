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

import net.hydromatic.linq4j.expressions.FunctionExpression;
import net.hydromatic.linq4j.function.*;

import java.math.BigDecimal;
import java.util.Comparator;

/**
 * Factory for building {@link Queryable} objects.
 *
 * @param <T> Element type
 */
public interface QueryableFactory<T> {

  /**
   * Applies an accumulator function over a sequence.
   */
  T aggregate(Queryable<T> source,
      FunctionExpression<Function2<T, T, T>> selector);

  /**
   * Applies an accumulator function over a
   * sequence. The specified seed value is used as the initial
   * accumulator value.
   */
  <TAccumulate> TAccumulate aggregate(Queryable<T> source, TAccumulate seed,
      FunctionExpression<Function2<TAccumulate, T, TAccumulate>> selector);

  /**
   * Applies an accumulator function over a
   * sequence. The specified seed value is used as the initial
   * accumulator value, and the specified function is used to select
   * the result value.
   */
  <TAccumulate, TResult> TResult aggregate(Queryable<T> source,
      TAccumulate seed,
      FunctionExpression<Function2<TAccumulate, T, TAccumulate>> func,
      FunctionExpression<Function1<TAccumulate, TResult>> selector);

  /**
   * Determines whether all the elements of a sequence
   * satisfy a condition.
   */
  boolean all(Queryable<T> source, FunctionExpression<Predicate1<T>> predicate);

  /**
   * Determines whether a sequence contains any
   * elements.
   */
  boolean any(Queryable<T> source);

  /**
   * Determines whether any element of a sequence
   * satisfies a condition.
   */
  boolean any(Queryable<T> source, FunctionExpression<Predicate1<T>> predicate);

  /**
   * Computes the average of a sequence of Decimal
   * values that is obtained by invoking a projection function on
   * each element of the input sequence.
   */
  BigDecimal averageBigDecimal(Queryable<T> source,
      FunctionExpression<BigDecimalFunction1<T>> selector);

  /**
   * Computes the average of a sequence of nullable
   * Decimal values that is obtained by invoking a projection
   * function on each element of the input sequence.
   */
  BigDecimal averageNullableBigDecimal(Queryable<T> source,
      FunctionExpression<NullableBigDecimalFunction1<T>> selector);

  /**
   * Computes the average of a sequence of Double
   * values that is obtained by invoking a projection function on
   * each element of the input sequence.
   */
  double averageDouble(Queryable<T> source,
      FunctionExpression<DoubleFunction1<T>> selector);

  /**
   * Computes the average of a sequence of nullable
   * Double values that is obtained by invoking a projection
   * function on each element of the input sequence.
   */
  Double averageNullableDouble(Queryable<T> source,
      FunctionExpression<NullableDoubleFunction1<T>> selector);

  /**
   * Computes the average of a sequence of int values
   * that is obtained by invoking a projection function on each
   * element of the input sequence.
   */
  int averageInteger(Queryable<T> source,
      FunctionExpression<IntegerFunction1<T>> selector);

  /**
   * Computes the average of a sequence of nullable
   * int values that is obtained by invoking a projection function
   * on each element of the input sequence.
   */
  Integer averageNullableInteger(Queryable<T> source,
      FunctionExpression<NullableIntegerFunction1<T>> selector);

  /**
   * Computes the average of a sequence of Float
   * values that is obtained by invoking a projection function on
   * each element of the input sequence.
   */
  float averageFloat(Queryable<T> source,
      FunctionExpression<FloatFunction1<T>> selector);

  /**
   * Computes the average of a sequence of nullable
   * Float values that is obtained by invoking a projection
   * function on each element of the input sequence.
   */
  Float averageNullableFloat(Queryable<T> source,
      FunctionExpression<NullableFloatFunction1<T>> selector);

  /**
   * Computes the average of a sequence of long values
   * that is obtained by invoking a projection function on each
   * element of the input sequence.
   */
  long averageLong(Queryable<T> source,
      FunctionExpression<LongFunction1<T>> selector);

  /**
   * Computes the average of a sequence of nullable
   * long values that is obtained by invoking a projection function
   * on each element of the input sequence.
   */
  Long averageNullableLong(Queryable<T> source,
      FunctionExpression<NullableLongFunction1<T>> selector);

  /**
   * Concatenates two sequences.
   */
  Queryable<T> concat(Queryable<T> source, Enumerable<T> source2);

  /**
   * Determines whether a sequence contains a specified
   * element by using the default equality comparer.
   */
  boolean contains(Queryable<T> source, T element);

  /**
   * Determines whether a sequence contains a specified
   * element by using a specified {@code EqualityComparer<T>}.
   */
  boolean contains(Queryable<T> source, T element,
      EqualityComparer<T> comparer);

  /**
   * Returns the number of elements in a
   * sequence.
   */
  int count(Queryable<T> source);

  /**
   * Returns the number of elements in the specified
   * sequence that satisfies a condition.
   */
  int count(Queryable<T> source, FunctionExpression<Predicate1<T>> predicate);

  /**
   * Returns the elements of the specified sequence or
   * the type parameter's default value in a singleton collection if
   * the sequence is empty.
   */
  Queryable<T> defaultIfEmpty(Queryable<T> source);

  /**
   * Returns the elements of the specified sequence or
   * the specified value in a singleton collection if the sequence
   * is empty.
   */
  Queryable<T> defaultIfEmpty(Queryable<T> source, T value);

  /**
   * Returns distinct elements from a sequence by using
   * the default equality comparer to compare values.
   */
  Queryable<T> distinct(Queryable<T> source);

  /**
   * Returns distinct elements from a sequence by using
   * a specified {@code EqualityComparer<T>} to compare values.
   */
  Queryable<T> distinct(Queryable<T> source, EqualityComparer<T> comparer);

  /**
   * Returns the element at a specified index in a
   * sequence.
   */
  T elementAt(Queryable<T> source, int index);

  /**
   * Returns the element at a specified index in a
   * sequence or a default value if the index is out of
   * range.
   */
  T elementAtOrDefault(Queryable<T> source, int index);

  /**
   * Produces the set difference of two sequences by
   * using the default equality comparer to compare values. (Defined
   * by Queryable.)
   */
  Queryable<T> except(Queryable<T> source, Enumerable<T> enumerable);

  /**
   * Produces the set difference of two sequences by
   * using the specified {@code EqualityComparer<T>} to compare
   * values.
   */
  Queryable<T> except(Queryable<T> source, Enumerable<T> enumerable,
      EqualityComparer<T> comparer);

  /**
   * Returns the first element of a sequence. (Defined
   * by Queryable.)
   */
  T first(Queryable<T> source);

  /**
   * Returns the first element of a sequence that
   * satisfies a specified condition.
   */
  T first(Queryable<T> source, FunctionExpression<Predicate1<T>> predicate);

  /**
   * Returns the first element of a sequence, or a
   * default value if the sequence contains no elements.
   */
  T firstOrDefault(Queryable<T> source);

  /**
   * Returns the first element of a sequence that
   * satisfies a specified condition or a default value if no such
   * element is found.
   */
  T firstOrDefault(Queryable<T> source,
      FunctionExpression<Predicate1<T>> predicate);

  /**
   * Groups the elements of a sequence according to a
   * specified key selector function.
   */
  <TKey> Queryable<Grouping<TKey, T>> groupBy(Queryable<T> source,
      FunctionExpression<Function1<T, TKey>> keySelector);

  /**
   * Groups the elements of a sequence according to a
   * specified key selector function and compares the keys by using
   * a specified comparer.
   */
  <TKey> Queryable<Grouping<TKey, T>> groupBy(Queryable<T> source,
      FunctionExpression<Function1<T, TKey>> keySelector,
      EqualityComparer<TKey> comparer);

  /**
   * Groups the elements of a sequence according to a
   * specified key selector function and projects the elements for
   * each group by using a specified function.
   */
  <TKey, TElement> Queryable<Grouping<TKey, TElement>> groupBy(
      Queryable<T> source, FunctionExpression<Function1<T, TKey>> keySelector,
      FunctionExpression<Function1<T, TElement>> elementSelector);

  /**
   * Groups the elements of a sequence according to a
   * specified key selector function and creates a result value from
   * each group and its key.
   */
  <TKey, TResult> Queryable<Grouping<TKey, TResult>> groupByK(
      Queryable<T> source, FunctionExpression<Function1<T, TKey>> keySelector,
      FunctionExpression<Function2<TKey, Enumerable<T>, TResult>>
        elementSelector);

  /**
   * Groups the elements of a sequence and projects the
   * elements for each group by using a specified function. Key
   * values are compared by using a specified comparer.
   */
  <TKey, TElement> Queryable<Grouping<TKey, TElement>> groupBy(
      Queryable<T> source, FunctionExpression<Function1<T, TKey>> keySelector,
      FunctionExpression<Function1<T, TElement>> elementSelector,
      EqualityComparer<TKey> comparer);

  /**
   * Groups the elements of a sequence according to a
   * specified key selector function and creates a result value from
   * each group and its key. Keys are compared by using a specified
   * comparer.
   */
  <TKey, TResult> Queryable<TResult> groupByK(Queryable<T> source,
      FunctionExpression<Function1<T, TKey>> keySelector,
      FunctionExpression<Function2<TKey, Enumerable<T>, TResult>>
        elementSelector,
      EqualityComparer<TKey> comparer);

  /**
   * Groups the elements of a sequence according to a
   * specified key selector function and creates a result value from
   * each group and its key. The elements of each group are
   * projected by using a specified function.
   */
  <TKey, TElement, TResult> Queryable<TResult> groupBy(Queryable<T> source,
      FunctionExpression<Function1<T, TKey>> keySelector,
      FunctionExpression<Function1<T, TElement>> elementSelector,
      FunctionExpression<Function2<TKey, Enumerable<TElement>, TResult>>
        resultSelector);

  /**
   * Groups the elements of a sequence according to a
   * specified key selector function and creates a result value from
   * each group and its key. Keys are compared by using a specified
   * comparer and the elements of each group are projected by using
   * a specified function.
   */
  <TKey, TElement, TResult> Queryable<TResult> groupBy(Queryable<T> source,
      FunctionExpression<Function1<T, TKey>> keySelector,
      FunctionExpression<Function1<T, TElement>> elementSelector,
      FunctionExpression<Function2<TKey, Enumerable<TElement>, TResult>>
        resultSelector,
      EqualityComparer<TKey> comparer);

  /**
   * Correlates the elements of two sequences based on
   * key equality and groups the results. The default equality
   * comparer is used to compare keys.
   */
  <TInner, TKey, TResult> Queryable<TResult> groupJoin(Queryable<T> source,
      Enumerable<TInner> inner,
      FunctionExpression<Function1<T, TKey>> outerKeySelector,
      FunctionExpression<Function1<TInner, TKey>> innerKeySelector,
      FunctionExpression<Function2<T, Enumerable<TInner>, TResult>>
        resultSelector);

  /**
   * Correlates the elements of two sequences based on
   * key equality and groups the results. A specified
   * EqualityComparer is used to compare keys.
   */
  <TInner, TKey, TResult> Queryable<TResult> groupJoin(Queryable<T> source,
      Enumerable<TInner> inner,
      FunctionExpression<Function1<T, TKey>> outerKeySelector,
      FunctionExpression<Function1<TInner, TKey>> innerKeySelector,
      FunctionExpression<Function2<T, Enumerable<TInner>, TResult>>
        resultSelector,
      EqualityComparer<TKey> comparer);

  /**
   * Produces the set intersection of two sequences by
   * using the default equality comparer to compare values. (Defined
   * by Queryable.)
   */
  Queryable<T> intersect(Queryable<T> source, Enumerable<T> enumerable);

  /**
   * Produces the set intersection of two sequences by
   * using the specified EqualityComparer to compare
   * values.
   */
  Queryable<T> intersect(Queryable<T> source, Enumerable<T> enumerable,
      EqualityComparer<T> comparer);

  /**
   * Correlates the elements of two sequences based on
   * matching keys. The default equality comparer is used to compare
   * keys.
   */
  <TInner, TKey, TResult> Queryable<TResult> join(Queryable<T> source,
      Enumerable<TInner> inner,
      FunctionExpression<Function1<T, TKey>> outerKeySelector,
      FunctionExpression<Function1<TInner, TKey>> innerKeySelector,
      FunctionExpression<Function2<T, TInner, TResult>> resultSelector);

  /**
   * Correlates the elements of two sequences based on
   * matching keys. A specified EqualityComparer is used to
   * compare keys.
   */
  <TInner, TKey, TResult> Queryable<TResult> join(Queryable<T> source,
      Enumerable<TInner> inner,
      FunctionExpression<Function1<T, TKey>> outerKeySelector,
      FunctionExpression<Function1<TInner, TKey>> innerKeySelector,
      FunctionExpression<Function2<T, TInner, TResult>> resultSelector,
      EqualityComparer<TKey> comparer);

  /**
   * Returns the last element in a sequence. (Defined
   * by Queryable.)
   */
  T last(Queryable<T> source);

  /**
   * Returns the last element of a sequence that
   * satisfies a specified condition.
   */
  T last(Queryable<T> source, FunctionExpression<Predicate1<T>> predicate);

  /**
   * Returns the last element in a sequence, or a
   * default value if the sequence contains no elements.
   */
  T lastOrDefault(Queryable<T> source);

  /**
   * Returns the last element of a sequence that
   * satisfies a condition or a default value if no such element is
   * found.
   */
  T lastOrDefault(Queryable<T> source,
      FunctionExpression<Predicate1<T>> predicate);

  /**
   * Returns an long that represents the total number
   * of elements in a sequence.
   */
  long longCount(Queryable<T> source);

  /**
   * Returns an long that represents the number of
   * elements in a sequence that satisfy a condition.
   */
  long longCount(Queryable<T> source,
      FunctionExpression<Predicate1<T>> predicate);

  /**
   * Returns the maximum value in a generic
   * {@code IQueryable<T>}.
   */
  T max(Queryable<T> source);

  /**
   * Invokes a projection function on each element of a
   * generic {@code IQueryable<T>} and returns the maximum resulting
   * value.
   */
  <TResult extends Comparable<TResult>> TResult max(Queryable<T> source,
      FunctionExpression<Function1<T, TResult>> selector);

  /**
   * Returns the minimum value in a generic
   * {@code IQueryable<T>}.
   */
  T min(Queryable<T> source);

  /**
   * Invokes a projection function on each element of a
   * generic {@code IQueryable<T>} and returns the minimum resulting
   * value.
   */
  <TResult extends Comparable<TResult>> TResult min(Queryable<T> source,
      FunctionExpression<Function1<T, TResult>> selector);

  /**
   * Filters the elements of an IQueryable based on a
   * specified type.
   */
  <TResult> Queryable<TResult> ofType(Queryable<T> source,
      Class<TResult> clazz);

  <T2> Queryable<T2> cast(Queryable<T> source, Class<T2> clazz);

  /**
   * Sorts the elements of a sequence in ascending
   * order according to a key.
   */
  <TKey extends Comparable> OrderedQueryable<T> orderBy(Queryable<T> source,
      FunctionExpression<Function1<T, TKey>> keySelector);

  /**
   * Sorts the elements of a sequence in ascending
   * order by using a specified comparer.
   */
  <TKey> OrderedQueryable<T> orderBy(Queryable<T> source,
      FunctionExpression<Function1<T, TKey>> keySelector,
      Comparator<TKey> comparator);

  /**
   * Sorts the elements of a sequence in descending
   * order according to a key.
   */
  <TKey extends Comparable> OrderedQueryable<T> orderByDescending(
      Queryable<T> source, FunctionExpression<Function1<T, TKey>> keySelector);

  /**
   * Sorts the elements of a sequence in descending
   * order by using a specified comparer.
   */
  <TKey> OrderedQueryable<T> orderByDescending(Queryable<T> source,
      FunctionExpression<Function1<T, TKey>> keySelector,
      Comparator<TKey> comparator);

  /**
   * Inverts the order of the elements in a sequence.
   */
  Queryable<T> reverse(Queryable<T> source);

  /**
   * Projects each element of a sequence into a new form.
   */
  <TResult> Queryable<TResult> select(Queryable<T> source,
      FunctionExpression<Function1<T, TResult>> selector);

  /**
   * Projects each element of a sequence into a new
   * form by incorporating the element's index.
   */
  <TResult> Queryable<TResult> selectN(Queryable<T> source,
      FunctionExpression<Function2<T, Integer, TResult>> selector);


  /**
   * Projects each element of a sequence to an
   * {@code Enumerable<T>} and combines the resulting sequences into one
   * sequence.
   */
  <TResult> Queryable<TResult> selectMany(Queryable<T> source,
      FunctionExpression<Function1<T, Enumerable<TResult>>> selector);

  /**
   * Projects each element of a sequence to an
   * {@code Enumerable<T>} and combines the resulting sequences into one
   * sequence. The index of each source element is used in the
   * projected form of that element.
   */
  <TResult> Queryable<TResult> selectManyN(Queryable<T> source,
      FunctionExpression<Function2<T, Integer, Enumerable<TResult>>> selector);

  /**
   * Projects each element of a sequence to an
   * {@code Enumerable<T>} that incorporates the index of the source
   * element that produced it. A result selector function is invoked
   * on each element of each intermediate sequence, and the
   * resulting values are combined into a single, one-dimensional
   * sequence and returned.
   */
  <TCollection, TResult> Queryable<TResult> selectMany(Queryable<T> source,
      FunctionExpression<Function2<T, Integer, Enumerable<TCollection>>>
        collectionSelector,
      FunctionExpression<Function2<T, TCollection, TResult>> resultSelector);

  /**
   * Projects each element of a sequence to an
   * {@code Enumerable<T>} and invokes a result selector function on each
   * element therein. The resulting values from each intermediate
   * sequence are combined into a single, one-dimensional sequence
   * and returned.
   */
  <TCollection, TResult> Queryable<TResult> selectManyN(Queryable<T> source,
      FunctionExpression<Function1<T, Enumerable<TCollection>>>
        collectionSelector,
      FunctionExpression<Function2<T, TCollection, TResult>> resultSelector);

  /**
   * Determines whether two sequences are equal by
   * using the default equality comparer to compare
   * elements.
   */
  boolean sequenceEqual(Queryable<T> source, Enumerable<T> enumerable);

  /**
   * Determines whether two sequences are equal by
   * using a specified EqualityComparer to compare
   * elements.
   */
  boolean sequenceEqual(Queryable<T> source, Enumerable<T> enumerable,
      EqualityComparer<T> comparer);

  /**
   * Returns the only element of a sequence, and throws
   * an exception if there is not exactly one element in the
   * sequence.
   */
  T single(Queryable<T> source);

  /**
   * Returns the only element of a sequence that
   * satisfies a specified condition, and throws an exception if
   * more than one such element exists.
   */
  T single(Queryable<T> source, FunctionExpression<Predicate1<T>> predicate);

  /**
   * Returns the only element of a sequence, or a
   * default value if the sequence is empty; this method throws an
   * exception if there is more than one element in the
   * sequence.
   */
  T singleOrDefault(Queryable<T> source);

  /**
   * Returns the only element of a sequence that
   * satisfies a specified condition or a default value if no such
   * element exists; this method throws an exception if more than
   * one element satisfies the condition.
   */
  T singleOrDefault(Queryable<T> source,
      FunctionExpression<Predicate1<T>> predicate);

  /**
   * Bypasses a specified number of elements in a
   * sequence and then returns the remaining elements.
   */
  Queryable<T> skip(Queryable<T> source, int count);

  /**
   * Bypasses elements in a sequence as long as a
   * specified condition is true and then returns the remaining
   * elements.
   */
  Queryable<T> skipWhile(Queryable<T> source,
      FunctionExpression<Predicate1<T>> predicate);

  /**
   * Bypasses elements in a sequence as long as a
   * specified condition is true and then returns the remaining
   * elements. The element's index is used in the logic of the
   * predicate function.
   */
  Queryable<T> skipWhileN(Queryable<T> source,
      FunctionExpression<Predicate2<T, Integer>> predicate);

  /**
   * Computes the sum of the sequence of Decimal values
   * that is obtained by invoking a projection function on each
   * element of the input sequence.
   */
  BigDecimal sumBigDecimal(Queryable<T> source,
      FunctionExpression<BigDecimalFunction1<T>> selector);

  /**
   * Computes the sum of the sequence of nullable
   * Decimal values that is obtained by invoking a projection
   * function on each element of the input sequence.
   */
  BigDecimal sumNullableBigDecimal(Queryable<T> source,
      FunctionExpression<NullableBigDecimalFunction1<T>> selector);

  /**
   * Computes the sum of the sequence of Double values
   * that is obtained by invoking a projection function on each
   * element of the input sequence.
   */
  double sumDouble(Queryable<T> source,
      FunctionExpression<DoubleFunction1<T>> selector);

  /**
   * Computes the sum of the sequence of nullable
   * Double values that is obtained by invoking a projection
   * function on each element of the input sequence.
   */
  Double sumNullableDouble(Queryable<T> source,
      FunctionExpression<NullableDoubleFunction1<T>> selector);

  /**
   * Computes the sum of the sequence of int values
   * that is obtained by invoking a projection function on each
   * element of the input sequence.
   */
  int sumInteger(Queryable<T> source,
      FunctionExpression<IntegerFunction1<T>> selector);

  /**
   * Computes the sum of the sequence of nullable int
   * values that is obtained by invoking a projection function on
   * each element of the input sequence.
   */
  Integer sumNullableInteger(Queryable<T> source,
      FunctionExpression<NullableIntegerFunction1<T>> selector);

  /**
   * Computes the sum of the sequence of long values
   * that is obtained by invoking a projection function on each
   * element of the input sequence.
   */
  long sumLong(Queryable<T> source,
      FunctionExpression<LongFunction1<T>> selector);

  /**
   * Computes the sum of the sequence of nullable long
   * values that is obtained by invoking a projection function on
   * each element of the input sequence.
   */
  Long sumNullableLong(Queryable<T> source,
      FunctionExpression<NullableLongFunction1<T>> selector);

  /**
   * Computes the sum of the sequence of Float values
   * that is obtained by invoking a projection function on each
   * element of the input sequence.
   */
  float sumFloat(Queryable<T> source,
      FunctionExpression<FloatFunction1<T>> selector);

  /**
   * Computes the sum of the sequence of nullable
   * Float values that is obtained by invoking a projection
   * function on each element of the input sequence.
   */
  Float sumNullableFloat(Queryable<T> source,
      FunctionExpression<NullableFloatFunction1<T>> selector);

  /**
   * Returns a specified number of contiguous elements
   * from the start of a sequence.
   */
  Queryable<T> take(Queryable<T> source, int count);

  /**
   * Returns elements from a sequence as long as a
   * specified condition is true.
   */
  Queryable<T> takeWhile(Queryable<T> source,
      FunctionExpression<Predicate1<T>> predicate);

  /**
   * Returns elements from a sequence as long as a
   * specified condition is true. The element's index is used in the
   * logic of the predicate function.
   */
  Queryable<T> takeWhileN(Queryable<T> source,
      FunctionExpression<Predicate2<T, Integer>> predicate);

  <TKey extends Comparable<TKey>> OrderedQueryable<T> thenBy(
      OrderedQueryable<T> source,
      FunctionExpression<Function1<T, TKey>> keySelector);

  <TKey> OrderedQueryable<T> thenBy(OrderedQueryable<T> source,
      FunctionExpression<Function1<T, TKey>> keySelector,
      Comparator<TKey> comparator);

  <TKey extends Comparable<TKey>> OrderedQueryable<T> thenByDescending(
      OrderedQueryable<T> source,
      FunctionExpression<Function1<T, TKey>> keySelector);

  <TKey> OrderedQueryable<T> thenByDescending(OrderedQueryable<T> source,
      FunctionExpression<Function1<T, TKey>> keySelector,
      Comparator<TKey> comparator);

  /**
   * Produces the set union of two sequences by using
   * the default equality comparer.
   */
  Queryable<T> union(Queryable<T> source, Enumerable<T> source1);

  /**
   * Produces the set union of two sequences by using a
   * specified {@code EqualityComparer<T>}.
   */
  Queryable<T> union(Queryable<T> source, Enumerable<T> source1,
      EqualityComparer<T> comparer);

  /**
   * Filters a sequence of values based on a
   * predicate.
   */
  Queryable<T> where(Queryable<T> source,
      FunctionExpression<? extends Predicate1<T>> predicate);

  /**
   * Filters a sequence of values based on a
   * predicate. Each element's index is used in the logic of the
   * predicate function.
   */
  Queryable<T> whereN(Queryable<T> source,
      FunctionExpression<? extends Predicate2<T, Integer>> predicate);

  /**
   * Merges two sequences by using the specified
   * predicate function.
   */
  <T1, TResult> Queryable<TResult> zip(Queryable<T> source,
      Enumerable<T1> source1,
      FunctionExpression<Function2<T, T1, TResult>> resultSelector);
}

// End QueryableFactory.java
