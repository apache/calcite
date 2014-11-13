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

/**
 * Extension methods in {@link Enumerable}.
 *
 * @param <TSource> Element type
 */
public interface ExtendedEnumerable<TSource> {

  /**
   * Performs an operation for each member of this enumeration.
   *
   * <p>Returns the value returned by the function for the last element in
   * this enumeration, or null if this enumeration is empty.</p>
   *
   * @param func Operation
   * @param <R> Return type
   */
  <R> R foreach(Function1<TSource, R> func);

  /**
   * Applies an accumulator function over a
   * sequence.
   */
  TSource aggregate(Function2<TSource, TSource, TSource> func);

  /**
   * Applies an accumulator function over a
   * sequence. The specified seed value is used as the initial
   * accumulator value.
   */
  <TAccumulate> TAccumulate aggregate(TAccumulate seed,
      Function2<TAccumulate, TSource, TAccumulate> func);

  /**
   * Applies an accumulator function over a
   * sequence. The specified seed value is used as the initial
   * accumulator value, and the specified function is used to select
   * the result value.
   */
  <TAccumulate, TResult> TResult aggregate(TAccumulate seed,
      Function2<TAccumulate, TSource, TAccumulate> func,
      Function1<TAccumulate, TResult> selector);

  /**
   * Determines whether all elements of a sequence
   * satisfy a condition.
   */
  boolean all(Predicate1<TSource> predicate);

  /**
   * Determines whether a sequence contains any
   * elements. (Defined by Enumerable.)
   */
  boolean any();

  /**
   * Determines whether any element of a sequence
   * satisfies a condition.
   */
  boolean any(Predicate1<TSource> predicate);

  /**
   * Returns the input typed as {@code Enumerable<TSource>}.
   *
   * <p>This method has no effect
   * other than to change the compile-time type of source from a type that
   * implements {@code Enumerable<TSource>} to {@code Enumerable<TSource>}
   * itself.
   *
   * <p>{@code asEnumerable<TSource>(Enumerable<TSource>)} can be used to choose
   * between query implementations when a sequence implements

   * {@code Enumerable<TSource>} but also has a different set of public query
   * methods available. For example, given a generic class Table that implements
   * {@code Enumerable<TSource>} and has its own methods such as {@code where},
   * {@code select}, and {@code selectMany}, a call to {@code where} would
   * invoke the public {@code where} method of {@code Table}. A {@code Table}
   * type that represents a database table could have a {@code where} method
   * that takes the predicate argument as an expression tree and converts the
   * tree to SQL for remote execution. If remote execution is not desired, for
   * example because the predicate invokes a local method, the
   * {@code asEnumerable<TSource>} method can be used to hide the custom methods
   * and instead make the standard query operators available.
   */
  Enumerable<TSource> asEnumerable();

  /**
   * Converts an Enumerable to a {@link Queryable}.
   *
   * <p>If the type of source implements {@code Queryable}, this method
   * returns it directly. Otherwise, it returns a {@code Queryable} that
   * executes queries by calling the equivalent query operator methods in
   * {@code Enumerable} instead of those in {@code Queryable}.</p>
   *
   * <p>Analogous to the LINQ's Enumerable.AsQueryable extension method.</p>
   *
   * @return A queryable
   */
  Queryable<TSource> asQueryable();

  /**
   * Computes the average of a sequence of Decimal
   * values that are obtained by invoking a transform function on
   * each element of the input sequence.
   */
  BigDecimal average(BigDecimalFunction1<TSource> selector);

  /**
   * Computes the average of a sequence of nullable
   * Decimal values that are obtained by invoking a transform
   * function on each element of the input sequence.
   */
  BigDecimal average(NullableBigDecimalFunction1<TSource> selector);

  /**
   * Computes the average of a sequence of Double
   * values that are obtained by invoking a transform function on
   * each element of the input sequence.
   */
  double average(DoubleFunction1<TSource> selector);

  /**
   * Computes the average of a sequence of nullable
   * Double values that are obtained by invoking a transform
   * function on each element of the input sequence.
   */
  Double average(NullableDoubleFunction1<TSource> selector);

  /**
   * Computes the average of a sequence of int values
   * that are obtained by invoking a transform function on each
   * element of the input sequence.
   */
  int average(IntegerFunction1<TSource> selector);

  /**
   * Computes the average of a sequence of nullable
   * int values that are obtained by invoking a transform function
   * on each element of the input sequence.
   */
  Integer average(NullableIntegerFunction1<TSource> selector);

  /**
   * Computes the average of a sequence of long values
   * that are obtained by invoking a transform function on each
   * element of the input sequence.
   */
  long average(LongFunction1<TSource> selector);

  /**
   * Computes the average of a sequence of nullable
   * long values that are obtained by invoking a transform function
   * on each element of the input sequence.
   */
  Long average(NullableLongFunction1<TSource> selector);

  /**
   * Computes the average of a sequence of Float
   * values that are obtained by invoking a transform function on
   * each element of the input sequence.
   */
  float average(FloatFunction1<TSource> selector);

  /**
   * Computes the average of a sequence of nullable
   * Float values that are obtained by invoking a transform
   * function on each element of the input sequence.
   */
  Float average(NullableFloatFunction1<TSource> selector);

  /**
   * Converts the elements of this Enumerable to the specified type.
   *
   * <p>This method is implemented by using deferred execution. The immediate
   * return value is an object that stores all the information that is
   * required to perform the action. The query represented by this method is
   * not executed until the object is enumerated either by calling its
   * {@link Enumerable#enumerator} method directly or by using
   * {@code for (... in ...)}.
   *
   * <p>If an element cannot be cast to type TResult, the
   * {@link Enumerator#current()} method will throw a
   * {@link ClassCastException} a exception when the element it accessed. To
   * obtain only those elements that can be cast to type TResult, use the
   * {@link #ofType(Class)} method instead.
   *
   * @see EnumerableDefaults#cast
   * @see #ofType(Class)
   */
  <T2> Enumerable<T2> cast(Class<T2> clazz);

  /**
   * Concatenates two sequences.
   */
  Enumerable<TSource> concat(Enumerable<TSource> enumerable1);

  /**
   * Determines whether a sequence contains a specified
   * element by using the default equality comparer.
   */
  boolean contains(TSource element);

  /**
   * Determines whether a sequence contains a specified
   * element by using a specified {@code EqualityComparer<TSource>}.
   */
  boolean contains(TSource element, EqualityComparer comparer);

  /**
   * Returns the number of elements in a
   * sequence.
   */
  int count();

  /**
   * Returns a number that represents how many elements
   * in the specified sequence satisfy a condition.
   */
  int count(Predicate1<TSource> predicate);

  /**
   * Returns the elements of the specified sequence or
   * the type parameter's default value in a singleton collection if
   * the sequence is empty.
   */
  Enumerable<TSource> defaultIfEmpty();

  /**
   * Returns the elements of the specified sequence or
   * the specified value in a singleton collection if the sequence
   * is empty.
   */
  TSource defaultIfEmpty(TSource value);

  /**
   * Returns distinct elements from a sequence by using
   * the default equality comparer to compare values.
   */
  Enumerable<TSource> distinct();

  /**
   * Returns distinct elements from a sequence by using
   * a specified {@code EqualityComparer<TSource>} to compare values.
   */
  Enumerable<TSource> distinct(EqualityComparer<TSource> comparer);

  /**
   * Returns the element at a specified index in a
   * sequence.
   */
  TSource elementAt(int index);

  /**
   * Returns the element at a specified index in a
   * sequence or a default value if the index is out of
   * range.
   */
  TSource elementAtOrDefault(int index);

  /**
   * Produces the set difference of two sequences by
   * using the default equality comparer to compare values. (Defined
   * by Enumerable.)
   */
  Enumerable<TSource> except(Enumerable<TSource> enumerable1);

  /**
   * Produces the set difference of two sequences by
   * using the specified {@code EqualityComparer<TSource>} to compare
   * values.
   */
  Enumerable<TSource> except(Enumerable<TSource> enumerable1,
      EqualityComparer<TSource> comparer);

  /**
   * Returns the first element of a sequence. (Defined
   * by Enumerable.)
   */
  TSource first();

  /**
   * Returns the first element in a sequence that
   * satisfies a specified condition.
   */
  TSource first(Predicate1<TSource> predicate);

  /**
   * Returns the first element of a sequence, or a
   * default value if the sequence contains no elements.
   */
  TSource firstOrDefault();

  /**
   * Returns the first element of the sequence that
   * satisfies a condition or a default value if no such element is
   * found.
   */
  TSource firstOrDefault(Predicate1<TSource> predicate);

  /**
   * Groups the elements of a sequence according to a
   * specified key selector function.
   */
  <TKey> Enumerable<Grouping<TKey, TSource>> groupBy(
      Function1<TSource, TKey> keySelector);

  /**
   * Groups the elements of a sequence according to a
   * specified key selector function and compares the keys by using
   * a specified comparer.
   */
  <TKey> Enumerable<Grouping<TKey, TSource>> groupBy(
      Function1<TSource, TKey> keySelector, EqualityComparer<TKey> comparer);

  /**
   * Groups the elements of a sequence according to a
   * specified key selector function and projects the elements for
   * each group by using a specified function.
   */
  <TKey, TElement> Enumerable<Grouping<TKey, TElement>> groupBy(
      Function1<TSource, TKey> keySelector,
      Function1<TSource, TElement> elementSelector);

  /**
   * Groups the elements of a sequence according to a
   * specified key selector function and creates a result value from
   * each group and its key.
   */
  <TKey, TResult> Enumerable<Grouping<TKey, TResult>> groupBy(
      Function1<TSource, TKey> keySelector,
      Function2<TKey, Enumerable<TSource>, TResult> elementSelector);

  /**
   * Groups the elements of a sequence according to a
   * key selector function. The keys are compared by using a
   * comparer and each group's elements are projected by using a
   * specified function.
   */
  <TKey, TElement> Enumerable<Grouping<TKey, TElement>> groupBy(
      Function1<TSource, TKey> keySelector,
      Function1<TSource, TElement> elementSelector, EqualityComparer comparer);

  /**
   * Groups the elements of a sequence according to a
   * specified key selector function and creates a result value from
   * each group and its key. The keys are compared by using a
   * specified comparer.
   */
  <TKey, TResult> Enumerable<TResult> groupBy(
      Function1<TSource, TKey> keySelector,
      Function2<TKey, Enumerable<TSource>, TResult> elementSelector,
      EqualityComparer comparer);

  /**
   * Groups the elements of a sequence according to a
   * specified key selector function and creates a result value from
   * each group and its key. The elements of each group are
   * projected by using a specified function.
   */
  <TKey, TElement, TResult> Enumerable<TResult> groupBy(
      Function1<TSource, TKey> keySelector,
      Function1<TSource, TElement> elementSelector,
      Function2<TKey, Enumerable<TElement>, TResult> resultSelector);

  /**
   * Groups the elements of a sequence according to a
   * specified key selector function and creates a result value from
   * each group and its key. Key values are compared by using a
   * specified comparer, and the elements of each group are
   * projected by using a specified function.
   */
  <TKey, TElement, TResult> Enumerable<TResult> groupBy(
      Function1<TSource, TKey> keySelector,
      Function1<TSource, TElement> elementSelector,
      Function2<TKey, Enumerable<TElement>, TResult> resultSelector,
      EqualityComparer<TKey> comparer);

  /**
   * Groups the elements of a sequence according to a
   * specified key selector function, initializing an accumulator for each
   * group and adding to it each time an element with the same key is seen.
   * Creates a result value from each accumulator and its key using a
   * specified function.
   */
  <TKey, TAccumulate, TResult> Enumerable<TResult> groupBy(
      Function1<TSource, TKey> keySelector,
      Function0<TAccumulate> accumulatorInitializer,
      Function2<TAccumulate, TSource, TAccumulate> accumulatorAdder,
      Function2<TKey, TAccumulate, TResult> resultSelector);

  /**
   * Groups the elements of a sequence according to a
   * specified key selector function, initializing an accumulator for each
   * group and adding to it each time an element with the same key is seen.
   * Creates a result value from each accumulator and its key using a
   * specified function. Key values are compared by using a
   * specified comparer.
   */
  <TKey, TAccumulate, TResult> Enumerable<TResult> groupBy(
      Function1<TSource, TKey> keySelector,
      Function0<TAccumulate> accumulatorInitializer,
      Function2<TAccumulate, TSource, TAccumulate> accumulatorAdder,
      Function2<TKey, TAccumulate, TResult> resultSelector,
      EqualityComparer<TKey> comparer);

  /**
   * Correlates the elements of two sequences based on
   * equality of keys and groups the results. The default equality
   * comparer is used to compare keys.
   */
  <TInner, TKey, TResult> Enumerable<TResult> groupJoin(
      Enumerable<TInner> inner, Function1<TSource, TKey> outerKeySelector,
      Function1<TInner, TKey> innerKeySelector,
      Function2<TSource, Enumerable<TInner>, TResult> resultSelector);

  /**
   * Correlates the elements of two sequences based on
   * key equality and groups the results. A specified
   * {@code EqualityComparer<TSource>} is used to compare keys.
   */
  <TInner, TKey, TResult> Enumerable<TResult> groupJoin(
      Enumerable<TInner> inner, Function1<TSource, TKey> outerKeySelector,
      Function1<TInner, TKey> innerKeySelector,
      Function2<TSource, Enumerable<TInner>, TResult> resultSelector,
      EqualityComparer<TKey> comparer);

  /**
   * Produces the set intersection of two sequences by
   * using the default equality comparer to compare values. (Defined
   * by Enumerable.)
   */
  Enumerable<TSource> intersect(Enumerable<TSource> enumerable1);

  /**
   * Produces the set intersection of two sequences by
   * using the specified {@code EqualityComparer<TSource>} to compare
   * values.
   */
  Enumerable<TSource> intersect(Enumerable<TSource> enumerable1,
      EqualityComparer<TSource> comparer);

  /**
   * Copies the contents of the sequence into a collection.
   */
  <C extends Collection<? super TSource>> C into(C sink);

  /**
   * Correlates the elements of two sequences based on
   * matching keys. The default equality comparer is used to compare
   * keys.
   */
  <TInner, TKey, TResult> Enumerable<TResult> join(Enumerable<TInner> inner,
      Function1<TSource, TKey> outerKeySelector,
      Function1<TInner, TKey> innerKeySelector,
      Function2<TSource, TInner, TResult> resultSelector);

  /**
   * Correlates the elements of two sequences based on
   * matching keys. A specified {@code EqualityComparer<TSource>} is used to
   * compare keys.
   */
  <TInner, TKey, TResult> Enumerable<TResult> join(Enumerable<TInner> inner,
      Function1<TSource, TKey> outerKeySelector,
      Function1<TInner, TKey> innerKeySelector,
      Function2<TSource, TInner, TResult> resultSelector,
      EqualityComparer<TKey> comparer);

  /**
   * Correlates the elements of two sequences based on matching keys, with
   * optional outer join semantics. A specified
   * {@code EqualityComparer<TSource>} is used to compare keys.
   *
   * <p>A left join generates nulls on right, and vice versa:</p>
   *
   * <table>
   *   <caption>Join types</caption>
   *   <tr>
   *     <td>Join type</td>
   *     <td>generateNullsOnLeft</td>
   *     <td>generateNullsOnRight</td>
   *   </tr>
   *   <tr><td>INNER</td><td>false</td><td>false</td></tr>
   *   <tr><td>LEFT</td><td>false</td><td>true</td></tr>
   *   <tr><td>RIGHT</td><td>true</td><td>false</td></tr>
   *   <tr><td>FULL</td><td>true</td><td>true</td></tr>
   * </table>
   */
  <TInner, TKey, TResult> Enumerable<TResult> join(Enumerable<TInner> inner,
      Function1<TSource, TKey> outerKeySelector,
      Function1<TInner, TKey> innerKeySelector,
      Function2<TSource, TInner, TResult> resultSelector,
      EqualityComparer<TKey> comparer,
      boolean generateNullsOnLeft, boolean generateNullsOnRight);

  /**
   * Returns the last element of a sequence. (Defined
   * by Enumerable.)
   */
  TSource last();

  /**
   * Returns the last element of a sequence that
   * satisfies a specified condition.
   */
  TSource last(Predicate1<TSource> predicate);

  /**
   * Returns the last element of a sequence, or a
   * default value if the sequence contains no elements.
   */
  TSource lastOrDefault();

  /**
   * Returns the last element of a sequence that
   * satisfies a condition or a default value if no such element is
   * found.
   */
  TSource lastOrDefault(Predicate1<TSource> predicate);

  /**
   * Returns an long that represents the total number
   * of elements in a sequence.
   */
  long longCount();

  /**
   * Returns an long that represents how many elements
   * in a sequence satisfy a condition.
   */
  long longCount(Predicate1<TSource> predicate);

  /**
   * Returns the maximum value in a generic
   * sequence.
   */
  TSource max();

  /**
   * Invokes a transform function on each element of a
   * sequence and returns the maximum Decimal value.
   */
  BigDecimal max(BigDecimalFunction1<TSource> selector);

  /**
   * Invokes a transform function on each element of a
   * sequence and returns the maximum nullable Decimal
   * value.
   */
  BigDecimal max(NullableBigDecimalFunction1<TSource> selector);

  /**
   * Invokes a transform function on each element of a
   * sequence and returns the maximum Double value.
   */
  double max(DoubleFunction1<TSource> selector);

  /**
   * Invokes a transform function on each element of a
   * sequence and returns the maximum nullable Double
   * value.
   */
  Double max(NullableDoubleFunction1<TSource> selector);

  /**
   * Invokes a transform function on each element of a
   * sequence and returns the maximum int value.
   */
  int max(IntegerFunction1<TSource> selector);

  /**
   * Invokes a transform function on each element of a
   * sequence and returns the maximum nullable int value. (Defined
   * by Enumerable.)
   */
  Integer max(NullableIntegerFunction1<TSource> selector);

  /**
   * Invokes a transform function on each element of a
   * sequence and returns the maximum long value.
   */
  long max(LongFunction1<TSource> selector);

  /**
   * Invokes a transform function on each element of a
   * sequence and returns the maximum nullable long value. (Defined
   * by Enumerable.)
   */
  Long max(NullableLongFunction1<TSource> selector);

  /**
   * Invokes a transform function on each element of a
   * sequence and returns the maximum Float value.
   */
  float max(FloatFunction1<TSource> selector);

  /**
   * Invokes a transform function on each element of a
   * sequence and returns the maximum nullable Float
   * value.
   */
  Float max(NullableFloatFunction1<TSource> selector);

  /**
   * Invokes a transform function on each element of a
   * generic sequence and returns the maximum resulting
   * value.
   */
  <TResult extends Comparable<TResult>> TResult max(
      Function1<TSource, TResult> selector);

  /**
   * Returns the minimum value in a generic
   * sequence.
   */
  TSource min();

  /**
   * Invokes a transform function on each element of a
   * sequence and returns the minimum Decimal value.
   */
  BigDecimal min(BigDecimalFunction1<TSource> selector);

  /**
   * Invokes a transform function on each element of a
   * sequence and returns the minimum nullable Decimal
   * value.
   */
  BigDecimal min(NullableBigDecimalFunction1<TSource> selector);

  /**
   * Invokes a transform function on each element of a
   * sequence and returns the minimum Double value.
   */
  double min(DoubleFunction1<TSource> selector);

  /**
   * Invokes a transform function on each element of a
   * sequence and returns the minimum nullable Double
   * value.
   */
  Double min(NullableDoubleFunction1<TSource> selector);

  /**
   * Invokes a transform function on each element of a
   * sequence and returns the minimum int value.
   */
  int min(IntegerFunction1<TSource> selector);

  /**
   * Invokes a transform function on each element of a
   * sequence and returns the minimum nullable int value. (Defined
   * by Enumerable.)
   */
  Integer min(NullableIntegerFunction1<TSource> selector);

  /**
   * Invokes a transform function on each element of a
   * sequence and returns the minimum long value.
   */
  long min(LongFunction1<TSource> selector);

  /**
   * Invokes a transform function on each element of a
   * sequence and returns the minimum nullable long value. (Defined
   * by Enumerable.)
   */
  Long min(NullableLongFunction1<TSource> selector);

  /**
   * Invokes a transform function on each element of a
   * sequence and returns the minimum Float value.
   */
  float min(FloatFunction1<TSource> selector);

  /**
   * Invokes a transform function on each element of a
   * sequence and returns the minimum nullable Float
   * value.
   */
  Float min(NullableFloatFunction1<TSource> selector);

  /**
   * Invokes a transform function on each element of a
   * generic sequence and returns the minimum resulting
   * value.
   */
  <TResult extends Comparable<TResult>> TResult min(
      Function1<TSource, TResult> selector);

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
  <TResult> Enumerable<TResult> ofType(Class<TResult> clazz);

  /**
   * Sorts the elements of a sequence in ascending
   * order according to a key.
   */
  <TKey extends Comparable> Enumerable<TSource> orderBy(
      Function1<TSource, TKey> keySelector);

  /**
   * Sorts the elements of a sequence in ascending
   * order by using a specified comparer.
   */
  <TKey> Enumerable<TSource> orderBy(Function1<TSource, TKey> keySelector,
      Comparator<TKey> comparator);

  /**
   * Sorts the elements of a sequence in descending
   * order according to a key.
   */
  <TKey extends Comparable> Enumerable<TSource> orderByDescending(
      Function1<TSource, TKey> keySelector);

  /**
   * Sorts the elements of a sequence in descending
   * order by using a specified comparer.
   */
  <TKey> Enumerable<TSource> orderByDescending(
      Function1<TSource, TKey> keySelector, Comparator<TKey> comparator);

  /**
   * Inverts the order of the elements in a
   * sequence.
   */
  Enumerable<TSource> reverse();

  /**
   * Projects each element of a sequence into a new
   * form.
   */
  <TResult> Enumerable<TResult> select(Function1<TSource, TResult> selector);

  /**
   * Projects each element of a sequence into a new
   * form by incorporating the element's index.
   */
  <TResult> Enumerable<TResult> select(
      Function2<TSource, Integer, TResult> selector);

  /**
   * Projects each element of a sequence to an
   * {@code Enumerable<TSource>} and flattens the resulting sequences into one
   * sequence.
   */
  <TResult> Enumerable<TResult> selectMany(
      Function1<TSource, Enumerable<TResult>> selector);

  /**
   * Projects each element of a sequence to an
   * {@code Enumerable<TSource>}, and flattens the resulting sequences into one
   * sequence. The index of each source element is used in the
   * projected form of that element.
   */
  <TResult> Enumerable<TResult> selectMany(
      Function2<TSource, Integer, Enumerable<TResult>> selector);

  /**
   * Projects each element of a sequence to an
   * {@code Enumerable<TSource>}, flattens the resulting sequences into one
   * sequence, and invokes a result selector function on each
   * element therein. The index of each source element is used in
   * the intermediate projected form of that element.
   */
  <TCollection, TResult> Enumerable<TResult> selectMany(
      Function2<TSource, Integer, Enumerable<TCollection>> collectionSelector,
      Function2<TSource, TCollection, TResult> resultSelector);

  /**
   * Projects each element of a sequence to an
   * {@code Enumerable<TSource>}, flattens the resulting sequences into one
   * sequence, and invokes a result selector function on each
   * element therein.
   */
  <TCollection, TResult> Enumerable<TResult> selectMany(
      Function1<TSource, Enumerable<TCollection>> collectionSelector,
      Function2<TSource, TCollection, TResult> resultSelector);

  /**
   * Determines whether two sequences are equal by
   * comparing the elements by using the default equality comparer
   * for their type.
   */
  boolean sequenceEqual(Enumerable<TSource> enumerable1);

  /**
   * Determines whether two sequences are equal by
   * comparing their elements by using a specified
   * {@code EqualityComparer<TSource>}.
   */
  boolean sequenceEqual(Enumerable<TSource> enumerable1,
      EqualityComparer<TSource> comparer);

  /**
   * Returns the only element of a sequence, and throws
   * an exception if there is not exactly one element in the
   * sequence.
   */
  TSource single();

  /**
   * Returns the only element of a sequence that
   * satisfies a specified condition, and throws an exception if
   * more than one such element exists.
   */
  TSource single(Predicate1<TSource> predicate);

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
  TSource singleOrDefault(Predicate1<TSource> predicate);

  /**
   * Bypasses a specified number of elements in a
   * sequence and then returns the remaining elements.
   */
  Enumerable<TSource> skip(int count);

  /**
   * Bypasses elements in a sequence as long as a
   * specified condition is true and then returns the remaining
   * elements.
   */
  Enumerable<TSource> skipWhile(Predicate1<TSource> predicate);

  /**
   * Bypasses elements in a sequence as long as a
   * specified condition is true and then returns the remaining
   * elements. The element's index is used in the logic of the
   * predicate function.
   */
  Enumerable<TSource> skipWhile(Predicate2<TSource, Integer> predicate);

  /**
   * Computes the sum of the sequence of Decimal values
   * that are obtained by invoking a transform function on each
   * element of the input sequence.
   */
  BigDecimal sum(BigDecimalFunction1<TSource> selector);

  /**
   * Computes the sum of the sequence of nullable
   * Decimal values that are obtained by invoking a transform
   * function on each element of the input sequence.
   */
  BigDecimal sum(NullableBigDecimalFunction1<TSource> selector);

  /**
   * Computes the sum of the sequence of Double values
   * that are obtained by invoking a transform function on each
   * element of the input sequence.
   */
  double sum(DoubleFunction1<TSource> selector);

  /**
   * Computes the sum of the sequence of nullable
   * Double values that are obtained by invoking a transform
   * function on each element of the input sequence.
   */
  Double sum(NullableDoubleFunction1<TSource> selector);

  /**
   * Computes the sum of the sequence of int values
   * that are obtained by invoking a transform function on each
   * element of the input sequence.
   */
  int sum(IntegerFunction1<TSource> selector);

  /**
   * Computes the sum of the sequence of nullable int
   * values that are obtained by invoking a transform function on
   * each element of the input sequence.
   */
  Integer sum(NullableIntegerFunction1<TSource> selector);

  /**
   * Computes the sum of the sequence of long values
   * that are obtained by invoking a transform function on each
   * element of the input sequence.
   */
  long sum(LongFunction1<TSource> selector);

  /**
   * Computes the sum of the sequence of nullable long
   * values that are obtained by invoking a transform function on
   * each element of the input sequence.
   */
  Long sum(NullableLongFunction1<TSource> selector);

  /**
   * Computes the sum of the sequence of Float values
   * that are obtained by invoking a transform function on each
   * element of the input sequence.
   */
  float sum(FloatFunction1<TSource> selector);

  /**
   * Computes the sum of the sequence of nullable
   * Float values that are obtained by invoking a transform
   * function on each element of the input sequence.
   */
  Float sum(NullableFloatFunction1<TSource> selector);

  /**
   * Returns a specified number of contiguous elements
   * from the start of a sequence.
   */
  Enumerable<TSource> take(int count);

  /**
   * Returns elements from a sequence as long as a
   * specified condition is true.
   */
  Enumerable<TSource> takeWhile(Predicate1<TSource> predicate);

  /**
   * Returns elements from a sequence as long as a
   * specified condition is true. The element's index is used in the
   * logic of the predicate function.
   */
  Enumerable<TSource> takeWhile(Predicate2<TSource, Integer> predicate);

  /**
   * Creates a {@code Map<TKey, TValue>} from an
   * {@code Enumerable<TSource>} according to a specified key selector
   * function.
   *
   * <p>NOTE: Called {@code toDictionary} in LINQ.NET.</p>
   */
  <TKey> Map<TKey, TSource> toMap(Function1<TSource, TKey> keySelector);

  /**
   * Creates a {@code Map<TKey, TValue>} from an
   * {@code Enumerable<TSource>} according to a specified key selector function
   * and key comparer.
   */
  <TKey> Map<TKey, TSource> toMap(Function1<TSource, TKey> keySelector,
      EqualityComparer<TKey> comparer);

  /**
   * Creates a {@code Map<TKey, TValue>} from an
   * {@code Enumerable<TSource>} according to specified key selector and element
   * selector functions.
   */
  <TKey, TElement> Map<TKey, TElement> toMap(
      Function1<TSource, TKey> keySelector,
      Function1<TSource, TElement> elementSelector);

  /**
   * Creates a {@code Map<TKey, TValue>} from an
   * {@code Enumerable<TSource>} according to a specified key selector function,
   * a comparer, and an element selector function.
   */
  <TKey, TElement> Map<TKey, TElement> toMap(
      Function1<TSource, TKey> keySelector,
      Function1<TSource, TElement> elementSelector,
      EqualityComparer<TKey> comparer);

  /**
   * Creates a {@code List<TSource>} from an {@code Enumerable<TSource>}.
   */
  List<TSource> toList();

  /**
   * Creates a {@code Lookup<TKey, TElement>} from an
   * {@code Enumerable<TSource>} according to a specified key selector
   * function.
   */
  <TKey> Lookup<TKey, TSource> toLookup(Function1<TSource, TKey> keySelector);

  /**
   * Creates a {@code Lookup<TKey, TElement>} from an
   * {@code Enumerable<TSource>} according to a specified key selector function
   * and key comparer.
   */
  <TKey> Lookup<TKey, TSource> toLookup(Function1<TSource, TKey> keySelector,
      EqualityComparer<TKey> comparer);

  /**
   * Creates a {@code Lookup<TKey, TElement>} from an
   * {@code Enumerable<TSource>} according to specified key selector and element
   * selector functions.
   */
  <TKey, TElement> Lookup<TKey, TElement> toLookup(
      Function1<TSource, TKey> keySelector,
      Function1<TSource, TElement> elementSelector);

  /**
   * Creates a {@code Lookup<TKey, TElement>} from an
   * {@code Enumerable<TSource>} according to a specified key selector function,
   * a comparer and an element selector function.
   */
  <TKey, TElement> Lookup<TKey, TElement> toLookup(
      Function1<TSource, TKey> keySelector,
      Function1<TSource, TElement> elementSelector,
      EqualityComparer<TKey> comparer);

  /**
   * Produces the set union of two sequences by using
   * the default equality comparer.
   */
  Enumerable<TSource> union(Enumerable<TSource> source1);

  /**
   * Produces the set union of two sequences by using a
   * specified {@code EqualityComparer<TSource>}.
   */
  Enumerable<TSource> union(Enumerable<TSource> source1,
      EqualityComparer<TSource> comparer);

  /**
   * Filters a sequence of values based on a
   * predicate.
   */
  Enumerable<TSource> where(Predicate1<TSource> predicate);

  /**
   * Filters a sequence of values based on a
   * predicate. Each element's index is used in the logic of the
   * predicate function.
   */
  Enumerable<TSource> where(Predicate2<TSource, Integer> predicate);

  /**
   * Applies a specified function to the corresponding
   * elements of two sequences, producing a sequence of the
   * results.
   */
  <T1, TResult> Enumerable<TResult> zip(Enumerable<T1> source1,
      Function2<TSource, T1, TResult> resultSelector);
}

// End ExtendedEnumerable.java
