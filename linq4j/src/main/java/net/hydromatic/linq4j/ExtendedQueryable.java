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
 * Extension methods in Queryable.
 *
 * @param <TSource> Element type
 */
interface ExtendedQueryable<TSource> extends ExtendedEnumerable<TSource> {

  /**
   * Applies an accumulator function over a sequence.
   */
  TSource aggregate(
      FunctionExpression<Function2<TSource, TSource, TSource>> selector);

  /**
   * Applies an accumulator function over a
   * sequence. The specified seed value is used as the initial
   * accumulator value.
   */
  <TAccumulate> TAccumulate aggregate(TAccumulate seed,
      FunctionExpression<Function2<TAccumulate, TSource, TAccumulate>>
        selector);

  /**
   * Applies an accumulator function over a
   * sequence. The specified seed value is used as the initial
   * accumulator value, and the specified function is used to select
   * the result value.
   */
  <TAccumulate, TResult> TResult aggregate(TAccumulate seed,
      FunctionExpression<Function2<TAccumulate, TSource, TAccumulate>> func,
      FunctionExpression<Function1<TAccumulate, TResult>> selector);

  /**
   * Determines whether all the elements of a sequence
   * satisfy a condition.
   */
  boolean all(FunctionExpression<Predicate1<TSource>> predicate);

  /**
   * Determines whether any element of a sequence
   * satisfies a condition.
   */
  boolean any(FunctionExpression<Predicate1<TSource>> predicate);

  /**
   * Computes the average of a sequence of Decimal
   * values that is obtained by invoking a projection function on
   * each element of the input sequence.
   */
  BigDecimal averageBigDecimal(
      FunctionExpression<BigDecimalFunction1<TSource>> selector);

  /**
   * Computes the average of a sequence of nullable
   * Decimal values that is obtained by invoking a projection
   * function on each element of the input sequence.
   */
  BigDecimal averageNullableBigDecimal(
      FunctionExpression<NullableBigDecimalFunction1<TSource>> selector);

  /**
   * Computes the average of a sequence of Double
   * values that is obtained by invoking a projection function on
   * each element of the input sequence.
   */
  double averageDouble(FunctionExpression<DoubleFunction1<TSource>> selector);

  /**
   * Computes the average of a sequence of nullable
   * Double values that is obtained by invoking a projection
   * function on each element of the input sequence.
   */
  Double averageNullableDouble(
      FunctionExpression<NullableDoubleFunction1<TSource>> selector);

  /**
   * Computes the average of a sequence of int values
   * that is obtained by invoking a projection function on each
   * element of the input sequence.
   */
  int averageInteger(FunctionExpression<IntegerFunction1<TSource>> selector);

  /**
   * Computes the average of a sequence of nullable
   * int values that is obtained by invoking a projection function
   * on each element of the input sequence.
   */
  Integer averageNullableInteger(
      FunctionExpression<NullableIntegerFunction1<TSource>> selector);

  /**
   * Computes the average of a sequence of Float
   * values that is obtained by invoking a projection function on
   * each element of the input sequence.
   */
  float averageFloat(FunctionExpression<FloatFunction1<TSource>> selector);

  /**
   * Computes the average of a sequence of nullable
   * Float values that is obtained by invoking a projection
   * function on each element of the input sequence.
   */
  Float averageNullableFloat(
      FunctionExpression<NullableFloatFunction1<TSource>> selector);

  /**
   * Computes the average of a sequence of long values
   * that is obtained by invoking a projection function on each
   * element of the input sequence.
   */
  long averageLong(FunctionExpression<LongFunction1<TSource>> selector);

  /**
   * Computes the average of a sequence of nullable
   * long values that is obtained by invoking a projection function
   * on each element of the input sequence.
   */
  Long averageNullableLong(
      FunctionExpression<NullableLongFunction1<TSource>> selector);

  /**
   * Concatenates two sequences.
   */
  Queryable<TSource> concat(Enumerable<TSource> source2);

  /**
   * Returns the number of elements in the specified
   * sequence that satisfies a condition.
   */
  int count(FunctionExpression<Predicate1<TSource>> predicate);

  /**
   * Returns the elements of the specified sequence or
   * the type parameter's default value in a singleton collection if
   * the sequence is empty.
   */
  Queryable<TSource> defaultIfEmpty();

  /**
   * Returns distinct elements from a sequence by using
   * the default equality comparer to compare values.
   */
  Queryable<TSource> distinct();

  /**
   * Returns distinct elements from a sequence by using
   * a specified EqualityComparer&lt;TSource&gt; to compare values.
   */
  Queryable<TSource> distinct(EqualityComparer<TSource> comparer);

  /**
   * Produces the set difference of two sequences by
   * using the default equality comparer to compare values. (Defined
   * by Queryable.)
   */
  Queryable<TSource> except(Enumerable<TSource> enumerable);

  /**
   * Produces the set difference of two sequences by
   * using the specified {@code EqualityComparer<TSource>} to compare
   * values.
   */
  Queryable<TSource> except(Enumerable<TSource> enumerable,
      EqualityComparer<TSource> comparer);

  /**
   * Returns the first element of a sequence that
   * satisfies a specified condition.
   */
  TSource first(FunctionExpression<Predicate1<TSource>> predicate);

  /**
   * Returns the first element of a sequence that
   * satisfies a specified condition or a default value if no such
   * element is found.
   */
  TSource firstOrDefault(FunctionExpression<Predicate1<TSource>> predicate);

  /**
   * Groups the elements of a sequence according to a
   * specified key selector function.
   */
  <TKey> Queryable<Grouping<TKey, TSource>> groupBy(
      FunctionExpression<Function1<TSource, TKey>> keySelector);

  /**
   * Groups the elements of a sequence according to a
   * specified key selector function and compares the keys by using
   * a specified comparer.
   */
  <TKey> Queryable<Grouping<TKey, TSource>> groupBy(
      FunctionExpression<Function1<TSource, TKey>> keySelector,
      EqualityComparer<TKey> comparer);

  /**
   * Groups the elements of a sequence according to a
   * specified key selector function and projects the elements for
   * each group by using a specified function.
   */
  <TKey, TElement> Queryable<Grouping<TKey, TElement>> groupBy(
      FunctionExpression<Function1<TSource, TKey>> keySelector,
      FunctionExpression<Function1<TSource, TElement>> elementSelector);

  /**
   * Groups the elements of a sequence according to a
   * specified key selector function and creates a result value from
   * each group and its key.
   *
   * <p>NOTE: Renamed from {@code groupBy} to distinguish from
   * {@link #groupBy(net.hydromatic.linq4j.expressions.FunctionExpression, net.hydromatic.linq4j.expressions.FunctionExpression)},
   * which has the same erasure.</p>
   */
  <TKey, TResult> Queryable<Grouping<TKey, TResult>> groupByK(
      FunctionExpression<Function1<TSource, TKey>> keySelector,
      FunctionExpression<Function2<TKey, Enumerable<TSource>, TResult>>
        elementSelector);

  /**
   * Groups the elements of a sequence and projects the
   * elements for each group by using a specified function. Key
   * values are compared by using a specified comparer.
   */
  <TKey, TElement> Queryable<Grouping<TKey, TElement>> groupBy(
      FunctionExpression<Function1<TSource, TKey>> keySelector,
      FunctionExpression<Function1<TSource, TElement>> elementSelector,
      EqualityComparer<TKey> comparer);

  /**
   * Groups the elements of a sequence according to a
   * specified key selector function and creates a result value from
   * each group and its key. Keys are compared by using a specified
   * comparer.
   */
  <TKey, TResult> Queryable<TResult> groupByK(
      FunctionExpression<Function1<TSource, TKey>> keySelector,
      FunctionExpression<Function2<TKey, Enumerable<TSource>, TResult>>
        elementSelector,
      EqualityComparer<TKey> comparer);

  /**
   * Groups the elements of a sequence according to a
   * specified key selector function and creates a result value from
   * each group and its key. The elements of each group are
   * projected by using a specified function.
   */
  <TKey, TElement, TResult> Queryable<TResult> groupBy(
      FunctionExpression<Function1<TSource, TKey>> keySelector,
      FunctionExpression<Function1<TSource, TElement>> elementSelector,
      FunctionExpression<Function2<TKey, Enumerable<TElement>, TResult>>
        resultSelector);

  /**
   * Groups the elements of a sequence according to a
   * specified key selector function and creates a result value from
   * each group and its key. Keys are compared by using a specified
   * comparer and the elements of each group are projected by using
   * a specified function.
   */
  <TKey, TElement, TResult> Queryable<TResult> groupBy(
      FunctionExpression<Function1<TSource, TKey>> keySelector,
      FunctionExpression<Function1<TSource, TElement>> elementSelector,
      FunctionExpression<Function2<TKey, Enumerable<TElement>, TResult>>
        resultSelector,
      EqualityComparer<TKey> comparer);

  /**
   * Correlates the elements of two sequences based on
   * key equality and groups the results. The default equality
   * comparer is used to compare keys.
   */
  <TInner, TKey, TResult> Queryable<TResult> groupJoin(Enumerable<TInner> inner,
      FunctionExpression<Function1<TSource, TKey>> outerKeySelector,
      FunctionExpression<Function1<TInner, TKey>> innerKeySelector,
      FunctionExpression<Function2<TSource, Enumerable<TInner>, TResult>>
        resultSelector);

  /**
   * Correlates the elements of two sequences based on
   * key equality and groups the results. A specified
   * {@code EqualityComparer<TSource>} is used to compare keys.
   */
  <TInner, TKey, TResult> Queryable<TResult> groupJoin(Enumerable<TInner> inner,
      FunctionExpression<Function1<TSource, TKey>> outerKeySelector,
      FunctionExpression<Function1<TInner, TKey>> innerKeySelector,
      FunctionExpression<Function2<TSource, Enumerable<TInner>, TResult>>
        resultSelector,
      EqualityComparer<TKey> comparer);

  /**
   * Produces the set intersection of two sequences by
   * using the default equality comparer to compare values. (Defined
   * by Queryable.)
   */
  Queryable<TSource> intersect(Enumerable<TSource> enumerable);

  /**
   * Produces the set intersection of two sequences by
   * using the specified {@code EqualityComparer<TSource>} to compare
   * values.
   */
  Queryable<TSource> intersect(Enumerable<TSource> enumerable,
      EqualityComparer<TSource> comparer);

  /**
   * Correlates the elements of two sequences based on
   * matching keys. The default equality comparer is used to compare
   * keys.
   */
  <TInner, TKey, TResult> Queryable<TResult> join(Enumerable<TInner> inner,
      FunctionExpression<Function1<TSource, TKey>> outerKeySelector,
      FunctionExpression<Function1<TInner, TKey>> innerKeySelector,
      FunctionExpression<Function2<TSource, TInner, TResult>> resultSelector);

  /**
   * Correlates the elements of two sequences based on
   * matching keys. A specified {@code EqualityComparer<TSource>} is used to
   * compare keys.
   */
  <TInner, TKey, TResult> Queryable<TResult> join(Enumerable<TInner> inner,
      FunctionExpression<Function1<TSource, TKey>> outerKeySelector,
      FunctionExpression<Function1<TInner, TKey>> innerKeySelector,
      FunctionExpression<Function2<TSource, TInner, TResult>> resultSelector,
      EqualityComparer<TKey> comparer);

  /**
   * Returns the last element of a sequence that
   * satisfies a specified condition.
   */
  TSource last(FunctionExpression<Predicate1<TSource>> predicate);

  /**
   * Returns the last element of a sequence that
   * satisfies a condition or a default value if no such element is
   * found.
   */
  TSource lastOrDefault(FunctionExpression<Predicate1<TSource>> predicate);

  /**
   * Returns an long that represents the number of
   * elements in a sequence that satisfy a condition.
   */
  long longCount(FunctionExpression<Predicate1<TSource>> predicate);

  /**
   * Invokes a projection function on each element of a
   * generic {@code IQueryable<TSource>} and returns the maximum resulting
   * value.
   */
  <TResult extends Comparable<TResult>> TResult max(
      FunctionExpression<Function1<TSource, TResult>> selector);

  /**
   * Invokes a projection function on each element of a
   * generic {@code IQueryable<TSource>} and returns the minimum resulting
   * value.
   */
  <TResult extends Comparable<TResult>> TResult min(
      FunctionExpression<Function1<TSource, TResult>> selector);

  /**
   * Filters the elements of an IQueryable based on a
   * specified type.
   *
   * <p>The OfType method generates a
   * {@link net.hydromatic.linq4j.expressions.MethodCallExpression} that represents
   * calling OfType itself as a constructed generic method. It then passes the
   * MethodCallExpression to the CreateQuery(Expression) method of the
   * {@link QueryProvider} represented by the Provider property of the source
   * parameter.
   *
   * <p>The query behavior that occurs as a result of executing an expression
   * tree that represents calling OfType depends on the implementation of the
   * type of the source parameter. The expected behavior is that it filters
   * out any elements in source that are not of type TResult.
   *
   * <p>NOTE: clazz parameter not present in C# LINQ; necessary because of
   * Java type erasure.</p>
   */
  <TResult> Queryable<TResult> ofType(Class<TResult> clazz);

  <T2> Queryable<T2> cast(Class<T2> clazz);

  /**
   * Sorts the elements of a sequence in ascending
   * order according to a key.
   */
  <TKey extends Comparable> OrderedQueryable<TSource> orderBy(
      FunctionExpression<Function1<TSource, TKey>> keySelector);

  /**
   * Sorts the elements of a sequence in ascending
   * order by using a specified comparer.
   */
  <TKey> OrderedQueryable<TSource> orderBy(
      FunctionExpression<Function1<TSource, TKey>> keySelector,
      Comparator<TKey> comparator);

  /**
   * Sorts the elements of a sequence in descending
   * order according to a key.
   */
  <TKey extends Comparable> OrderedQueryable<TSource> orderByDescending(
      FunctionExpression<Function1<TSource, TKey>> keySelector);

  /**
   * Sorts the elements of a sequence in descending
   * order by using a specified comparer.
   */
  <TKey> OrderedQueryable<TSource> orderByDescending(
      FunctionExpression<Function1<TSource, TKey>> keySelector,
      Comparator<TKey> comparator);

  /**
   * Inverts the order of the elements in a sequence.
   */
  Queryable<TSource> reverse();


  /**
   * Projects each element of a sequence into a new form.
   */
  <TResult> Queryable<TResult> select(
      FunctionExpression<Function1<TSource, TResult>> selector);

  /**
   * Projects each element of a sequence into a new
   * form by incorporating the element's index.
   *
   * <p>NOTE: Renamed from {@code select} because had same erasure as
   * {@link #select(net.hydromatic.linq4j.expressions.FunctionExpression)}.</p>
   */
  <TResult> Queryable<TResult> selectN(
      FunctionExpression<Function2<TSource, Integer, TResult>> selector);


  /**
   * Projects each element of a sequence to an
   * {@code Enumerable<TSource>} and combines the resulting sequences into one
   * sequence.
   */
  <TResult> Queryable<TResult> selectMany(
      FunctionExpression<Function1<TSource, Enumerable<TResult>>> selector);

  /**
   * Projects each element of a sequence to an
   * {@code Enumerable<TSource>} and combines the resulting sequences into one
   * sequence. The index of each source element is used in the
   * projected form of that element.
   *
   * <p>NOTE: Renamed from {@code selectMany} because had same erasure as
   * {@link #selectMany(net.hydromatic.linq4j.expressions.FunctionExpression)}</p>
   */
  <TResult> Queryable<TResult> selectManyN(
      FunctionExpression<Function2<TSource, Integer, Enumerable<TResult>>>
        selector);

  /**
   * Projects each element of a sequence to an
   * {@code Enumerable<TSource>} that incorporates the index of the source
   * element that produced it. A result selector function is invoked
   * on each element of each intermediate sequence, and the
   * resulting values are combined into a single, one-dimensional
   * sequence and returned.
   */
  <TCollection, TResult> Queryable<TResult> selectMany(
      FunctionExpression<Function2<TSource, Integer, Enumerable<TCollection>>>
        collectionSelector,
      FunctionExpression<Function2<TSource, TCollection, TResult>>
        resultSelector);

  /**
   * Projects each element of a sequence to an
   * {@code Enumerable<TSource>} and invokes a result selector function on each
   * element therein. The resulting values from each intermediate
   * sequence are combined into a single, one-dimensional sequence
   * and returned.
   *
   * <p>NOTE: Renamed from {@code selectMany} because had same erasure as
   * {@link #selectMany(net.hydromatic.linq4j.expressions.FunctionExpression, net.hydromatic.linq4j.expressions.FunctionExpression)}</p>
   */
  <TCollection, TResult> Queryable<TResult> selectManyN(
      FunctionExpression<Function1<TSource, Enumerable<TCollection>>>
        collectionSelector,
      FunctionExpression<Function2<TSource, TCollection, TResult>>
        resultSelector);

  /**
   * Returns the only element of a sequence that
   * satisfies a specified condition, and throws an exception if
   * more than one such element exists.
   */
  TSource single(FunctionExpression<Predicate1<TSource>> predicate);

  /**
   * Returns the only element of a sequence, or a
   * default value if the sequence is empty; this method throws an
   * exception if there is more than one element in the
   * sequence.
   */
  TSource singleOrDefault();

  /**
   * Returns the only element of a sequence that
   * satisfies a specified condition or a default value if no such
   * element exists; this method throws an exception if more than
   * one element satisfies the condition.
   */
  TSource singleOrDefault(FunctionExpression<Predicate1<TSource>> predicate);

  /**
   * Bypasses a specified number of elements in a
   * sequence and then returns the remaining elements.
   */
  Queryable<TSource> skip(int count);

  /**
   * Bypasses elements in a sequence as long as a
   * specified condition is true and then returns the remaining
   * elements.
   */
  Queryable<TSource> skipWhile(
      FunctionExpression<Predicate1<TSource>> predicate);

  /**
   * Bypasses elements in a sequence as long as a
   * specified condition is true and then returns the remaining
   * elements. The element's index is used in the logic of the
   * predicate function.
   */
  Queryable<TSource> skipWhileN(
      FunctionExpression<Predicate2<TSource, Integer>> predicate);

  /**
   * Computes the sum of the sequence of Decimal values
   * that is obtained by invoking a projection function on each
   * element of the input sequence.
   */
  BigDecimal sumBigDecimal(
      FunctionExpression<BigDecimalFunction1<TSource>> selector);

  /**
   * Computes the sum of the sequence of nullable
   * Decimal values that is obtained by invoking a projection
   * function on each element of the input sequence.
   */
  BigDecimal sumNullableBigDecimal(
      FunctionExpression<NullableBigDecimalFunction1<TSource>> selector);

  /**
   * Computes the sum of the sequence of Double values
   * that is obtained by invoking a projection function on each
   * element of the input sequence.
   */
  double sumDouble(FunctionExpression<DoubleFunction1<TSource>> selector);

  /**
   * Computes the sum of the sequence of nullable
   * Double values that is obtained by invoking a projection
   * function on each element of the input sequence.
   */
  Double sumNullableDouble(
      FunctionExpression<NullableDoubleFunction1<TSource>> selector);

  /**
   * Computes the sum of the sequence of int values
   * that is obtained by invoking a projection function on each
   * element of the input sequence.
   */
  int sumInteger(FunctionExpression<IntegerFunction1<TSource>> selector);

  /**
   * Computes the sum of the sequence of nullable int
   * values that is obtained by invoking a projection function on
   * each element of the input sequence.
   */
  Integer sumNullableInteger(
      FunctionExpression<NullableIntegerFunction1<TSource>> selector);

  /**
   * Computes the sum of the sequence of long values
   * that is obtained by invoking a projection function on each
   * element of the input sequence.
   */
  long sumLong(FunctionExpression<LongFunction1<TSource>> selector);

  /**
   * Computes the sum of the sequence of nullable long
   * values that is obtained by invoking a projection function on
   * each element of the input sequence.
   */
  Long sumNullableLong(
      FunctionExpression<NullableLongFunction1<TSource>> selector);

  /**
   * Computes the sum of the sequence of Float values
   * that is obtained by invoking a projection function on each
   * element of the input sequence.
   */
  float sumFloat(FunctionExpression<FloatFunction1<TSource>> selector);

  /**
   * Computes the sum of the sequence of nullable
   * Float values that is obtained by invoking a projection
   * function on each element of the input sequence.
   */
  Float sumNullableFloat(
      FunctionExpression<NullableFloatFunction1<TSource>> selector);

  /**
   * Returns a specified number of contiguous elements
   * from the start of a sequence.
   */
  Queryable<TSource> take(int count);

  /**
   * Returns elements from a sequence as long as a
   * specified condition is true.
   */
  Queryable<TSource> takeWhile(
      FunctionExpression<Predicate1<TSource>> predicate);

  /**
   * Returns elements from a sequence as long as a
   * specified condition is true. The element's index is used in the
   * logic of the predicate function.
   */
  Queryable<TSource> takeWhileN(
      FunctionExpression<Predicate2<TSource, Integer>> predicate);

  /**
   * Produces the set union of two sequences by using
   * the default equality comparer.
   */
  Queryable<TSource> union(Enumerable<TSource> source1);

  /**
   * Produces the set union of two sequences by using a
   * specified {@code EqualityComparer<TSource>}.
   */
  Queryable<TSource> union(Enumerable<TSource> source1,
      EqualityComparer<TSource> comparer);

  /**
   * Filters a sequence of values based on a
   * predicate.
   */
  Queryable<TSource> where(
      FunctionExpression<? extends Predicate1<TSource>> predicate);

  /**
   * Filters a sequence of values based on a
   * predicate. Each element's index is used in the logic of the
   * predicate function.
   */
  Queryable<TSource> whereN(
      FunctionExpression<? extends Predicate2<TSource, Integer>> predicate);

  /**
   * Merges two sequences by using the specified
   * predicate function.
   */
  <T1, TResult> Queryable<TResult> zip(Enumerable<T1> source1,
      FunctionExpression<Function2<TSource, T1, TResult>> resultSelector);
}

// End ExtendedQueryable.java
