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
 * Implementation of the {@link net.hydromatic.linq4j.Enumerable} interface that
 * implements the extension methods by calling into the {@link Extensions}
 * class.
 *
 * <p>The are two abstract methods:
 * {@link #enumerator()} and {@link #iterator()}.
 * The derived class can implement each separately, or implement one in terms of
 * the other.</p>
 *
 * @param <T> Element type
 */
public abstract class DefaultEnumerable<T> implements OrderedEnumerable<T> {

  /**
   * Derived classes might wish to override this method to return the "outer"
   * enumerable.
   */
  protected Enumerable<T> getThis() {
    return this;
  }

  /**
   * Derived classes might wish to override this method to return the "outer"
   * ordered-enumerable.
   */
  protected OrderedEnumerable<T> getThisOrdered() {
    return this;
  }

  public <R> R foreach(Function1<T, R> func) {
    R result = null;
    final Enumerator<T> enumerator = enumerator();
    try {
      while (enumerator.moveNext()) {
        T t = enumerator.current();
        result = func.apply(t);
      }
      return result;
    } finally {
      enumerator.close();
    }
  }

  public Queryable<T> asQueryable() {
    return Extensions.asQueryable(this);
  }

  // NOTE: Not part of the Queryable interface.
  protected OrderedQueryable<T> asOrderedQueryable() {
    return EnumerableDefaults.asOrderedQueryable(this);
  }

  public T aggregate(Function2<T, T, T> func) {
    return EnumerableDefaults.aggregate(getThis(), func);
  }

  public <TAccumulate> TAccumulate aggregate(TAccumulate seed,
      Function2<TAccumulate, T, TAccumulate> func) {
    return EnumerableDefaults.aggregate(getThis(), seed, func);
  }

  public <TAccumulate, TResult> TResult aggregate(TAccumulate seed,
      Function2<TAccumulate, T, TAccumulate> func,
      Function1<TAccumulate, TResult> selector) {
    return EnumerableDefaults.aggregate(getThis(), seed, func, selector);
  }

  public boolean all(Predicate1<T> predicate) {
    return EnumerableDefaults.all(getThis(), predicate);
  }

  public boolean any() {
    return EnumerableDefaults.any(getThis());
  }

  public boolean any(Predicate1<T> predicate) {
    return EnumerableDefaults.any(getThis(), predicate);
  }

  public Enumerable<T> asEnumerable() {
    return EnumerableDefaults.asEnumerable(getThis());
  }

  public BigDecimal average(BigDecimalFunction1<T> selector) {
    return EnumerableDefaults.average(getThis(), selector);
  }

  public BigDecimal average(NullableBigDecimalFunction1<T> selector) {
    return EnumerableDefaults.average(getThis(), selector);
  }

  public double average(DoubleFunction1<T> selector) {
    return EnumerableDefaults.average(getThis(), selector);
  }

  public Double average(NullableDoubleFunction1<T> selector) {
    return EnumerableDefaults.average(getThis(), selector);
  }

  public int average(IntegerFunction1<T> selector) {
    return EnumerableDefaults.average(getThis(), selector);
  }

  public Integer average(NullableIntegerFunction1<T> selector) {
    return EnumerableDefaults.average(getThis(), selector);
  }

  public long average(LongFunction1<T> selector) {
    return EnumerableDefaults.average(getThis(), selector);
  }

  public Long average(NullableLongFunction1<T> selector) {
    return EnumerableDefaults.average(getThis(), selector);
  }

  public float average(FloatFunction1<T> selector) {
    return EnumerableDefaults.average(getThis(), selector);
  }

  public Float average(NullableFloatFunction1<T> selector) {
    return EnumerableDefaults.average(getThis(), selector);
  }

  public <T2> Enumerable<T2> cast(Class<T2> clazz) {
    return EnumerableDefaults.cast(getThis(), clazz);
  }

  public Enumerable<T> concat(Enumerable<T> enumerable1) {
    return EnumerableDefaults.concat(getThis(), enumerable1);
  }

  public boolean contains(T element) {
    return EnumerableDefaults.contains(getThis(), element);
  }

  public boolean contains(T element, EqualityComparer comparer) {
    return EnumerableDefaults.contains(getThis(), element, comparer);
  }

  public int count() {
    return EnumerableDefaults.count(getThis());
  }

  public int count(Predicate1<T> predicate) {
    return EnumerableDefaults.count(getThis(), predicate);
  }

  public <TKey> OrderedEnumerable<T> createOrderedEnumerable(
      Function1<T, TKey> keySelector, Comparator<TKey> comparator,
      boolean descending) {
    return EnumerableDefaults.createOrderedEnumerable(getThisOrdered(),
        keySelector, comparator, descending);
  }

  public Enumerable<T> defaultIfEmpty() {
    return EnumerableDefaults.defaultIfEmpty(getThis());
  }

  public T defaultIfEmpty(T value) {
    return EnumerableDefaults.defaultIfEmpty(getThis(), value);
  }

  public Enumerable<T> distinct() {
    return EnumerableDefaults.distinct(getThis());
  }

  public Enumerable<T> distinct(EqualityComparer<T> comparer) {
    return EnumerableDefaults.distinct(getThis(), comparer);
  }

  public T elementAt(int index) {
    return EnumerableDefaults.elementAt(getThis(), index);
  }

  public T elementAtOrDefault(int index) {
    return EnumerableDefaults.elementAtOrDefault(getThis(), index);
  }

  public Enumerable<T> except(Enumerable<T> enumerable1) {
    return EnumerableDefaults.except(getThis(), enumerable1);
  }

  public Enumerable<T> except(Enumerable<T> enumerable1,
      EqualityComparer<T> comparer) {
    return EnumerableDefaults.except(getThis(), enumerable1, comparer);
  }

  public T first() {
    return EnumerableDefaults.first(getThis());
  }

  public T first(Predicate1<T> predicate) {
    return EnumerableDefaults.first(getThis(), predicate);
  }

  public T firstOrDefault() {
    return EnumerableDefaults.firstOrDefault(getThis());
  }

  public T firstOrDefault(Predicate1<T> predicate) {
    return EnumerableDefaults.firstOrDefault(getThis(), predicate);
  }

  public <TKey> Enumerable<Grouping<TKey, T>> groupBy(
      Function1<T, TKey> keySelector) {
    return EnumerableDefaults.groupBy(getThis(), keySelector);
  }

  public <TKey> Enumerable<Grouping<TKey, T>> groupBy(
      Function1<T, TKey> keySelector, EqualityComparer<TKey> comparer) {
    return EnumerableDefaults.groupBy(getThis(), keySelector, comparer);
  }

  public <TKey, TElement> Enumerable<Grouping<TKey, TElement>> groupBy(
      Function1<T, TKey> keySelector, Function1<T, TElement> elementSelector) {
    return EnumerableDefaults.groupBy(getThis(), keySelector, elementSelector);
  }

  public <TKey, TResult> Enumerable<Grouping<TKey, TResult>> groupBy(
      Function1<T, TKey> keySelector,
      Function2<TKey, Enumerable<T>, TResult> elementSelector) {
    return EnumerableDefaults.groupBy(getThis(), keySelector, elementSelector);
  }

  public <TKey, TElement> Enumerable<Grouping<TKey, TElement>> groupBy(
      Function1<T, TKey> keySelector, Function1<T, TElement> elementSelector,
      EqualityComparer comparer) {
    return EnumerableDefaults.groupBy(getThis(), keySelector, elementSelector);
  }

  public <TKey, TResult> Enumerable<TResult> groupBy(
      Function1<T, TKey> keySelector,
      Function2<TKey, Enumerable<T>, TResult> elementSelector,
      EqualityComparer comparer) {
    return EnumerableDefaults.groupBy(getThis(), keySelector, elementSelector,
        comparer);
  }

  public <TKey, TElement, TResult> Enumerable<TResult> groupBy(
      Function1<T, TKey> keySelector, Function1<T, TElement> elementSelector,
      Function2<TKey, Enumerable<TElement>, TResult> resultSelector) {
    return EnumerableDefaults.groupBy(getThis(), keySelector, elementSelector,
        resultSelector);
  }

  public <TKey, TElement, TResult> Enumerable<TResult> groupBy(
      Function1<T, TKey> keySelector, Function1<T, TElement> elementSelector,
      Function2<TKey, Enumerable<TElement>, TResult> resultSelector,
      EqualityComparer<TKey> comparer) {
    return EnumerableDefaults.groupBy(getThis(), keySelector, elementSelector,
        resultSelector, comparer);
  }

  public <TKey, TAccumulate, TResult> Enumerable<TResult> groupBy(
      Function1<T, TKey> keySelector,
      Function0<TAccumulate> accumulatorInitializer,
      Function2<TAccumulate, T, TAccumulate> accumulatorAdder,
      Function2<TKey, TAccumulate, TResult> resultSelector) {
    return EnumerableDefaults.groupBy(getThis(), keySelector,
        accumulatorInitializer, accumulatorAdder, resultSelector);
  }

  public <TKey, TAccumulate, TResult> Enumerable<TResult> groupBy(
      Function1<T, TKey> keySelector,
      Function0<TAccumulate> accumulatorInitializer,
      Function2<TAccumulate, T, TAccumulate> accumulatorAdder,
      Function2<TKey, TAccumulate, TResult> resultSelector,
      EqualityComparer<TKey> comparer) {
    return EnumerableDefaults.groupBy(getThis(), keySelector,
        accumulatorInitializer, accumulatorAdder, resultSelector, comparer);
  }

  public <TInner, TKey, TResult> Enumerable<TResult> groupJoin(
      Enumerable<TInner> inner, Function1<T, TKey> outerKeySelector,
      Function1<TInner, TKey> innerKeySelector,
      Function2<T, Enumerable<TInner>, TResult> resultSelector) {
    return EnumerableDefaults.groupJoin(getThis(), inner, outerKeySelector,
        innerKeySelector, resultSelector);
  }

  public <TInner, TKey, TResult> Enumerable<TResult> groupJoin(
      Enumerable<TInner> inner, Function1<T, TKey> outerKeySelector,
      Function1<TInner, TKey> innerKeySelector,
      Function2<T, Enumerable<TInner>, TResult> resultSelector,
      EqualityComparer<TKey> comparer) {
    return EnumerableDefaults.groupJoin(getThis(), inner, outerKeySelector,
        innerKeySelector, resultSelector, comparer);
  }

  public Enumerable<T> intersect(Enumerable<T> enumerable1) {
    return EnumerableDefaults.intersect(getThis(), enumerable1);
  }

  public Enumerable<T> intersect(Enumerable<T> enumerable1,
      EqualityComparer<T> comparer) {
    return EnumerableDefaults.intersect(getThis(), enumerable1, comparer);
  }

  public <C extends Collection<? super T>> C into(C sink) {
    return EnumerableDefaults.into(getThis(), sink);
  }

  public <TInner, TKey, TResult> Enumerable<TResult> join(
      Enumerable<TInner> inner, Function1<T, TKey> outerKeySelector,
      Function1<TInner, TKey> innerKeySelector,
      Function2<T, TInner, TResult> resultSelector) {
    return EnumerableDefaults.join(getThis(), inner, outerKeySelector,
        innerKeySelector, resultSelector);
  }

  public <TInner, TKey, TResult> Enumerable<TResult> join(
      Enumerable<TInner> inner, Function1<T, TKey> outerKeySelector,
      Function1<TInner, TKey> innerKeySelector,
      Function2<T, TInner, TResult> resultSelector,
      EqualityComparer<TKey> comparer) {
    return EnumerableDefaults.join(getThis(), inner, outerKeySelector,
        innerKeySelector, resultSelector, comparer);
  }

  public <TInner, TKey, TResult> Enumerable<TResult> join(
      Enumerable<TInner> inner, Function1<T, TKey> outerKeySelector,
      Function1<TInner, TKey> innerKeySelector,
      Function2<T, TInner, TResult> resultSelector,
      EqualityComparer<TKey> comparer,
      boolean generateNullsOnLeft, boolean generateNullsOnRight) {
    return EnumerableDefaults.join(getThis(), inner, outerKeySelector,
        innerKeySelector, resultSelector, comparer, generateNullsOnLeft,
        generateNullsOnRight);
  }

  public T last() {
    return EnumerableDefaults.last(getThis());
  }

  public T last(Predicate1<T> predicate) {
    return EnumerableDefaults.last(getThis(), predicate);
  }

  public T lastOrDefault() {
    return EnumerableDefaults.lastOrDefault(getThis());
  }

  public T lastOrDefault(Predicate1<T> predicate) {
    return EnumerableDefaults.lastOrDefault(getThis(), predicate);
  }

  public long longCount() {
    return EnumerableDefaults.longCount(getThis());
  }

  public long longCount(Predicate1<T> predicate) {
    return EnumerableDefaults.longCount(getThis(), predicate);
  }

  public T max() {
    return (T) EnumerableDefaults.max((Enumerable) getThis());
  }

  public BigDecimal max(BigDecimalFunction1<T> selector) {
    return EnumerableDefaults.max(getThis(), selector);
  }

  public BigDecimal max(NullableBigDecimalFunction1<T> selector) {
    return EnumerableDefaults.max(getThis(), selector);
  }

  public double max(DoubleFunction1<T> selector) {
    return EnumerableDefaults.max(getThis(), selector);
  }

  public Double max(NullableDoubleFunction1<T> selector) {
    return EnumerableDefaults.max(getThis(), selector);
  }

  public int max(IntegerFunction1<T> selector) {
    return EnumerableDefaults.max(getThis(), selector);
  }

  public Integer max(NullableIntegerFunction1<T> selector) {
    return EnumerableDefaults.max(getThis(), selector);
  }

  public long max(LongFunction1<T> selector) {
    return EnumerableDefaults.max(getThis(), selector);
  }

  public Long max(NullableLongFunction1<T> selector) {
    return EnumerableDefaults.max(getThis(), selector);
  }

  public float max(FloatFunction1<T> selector) {
    return EnumerableDefaults.max(getThis(), selector);
  }

  public Float max(NullableFloatFunction1<T> selector) {
    return EnumerableDefaults.max(getThis(), selector);
  }

  public <TResult extends Comparable<TResult>> TResult max(
      Function1<T, TResult> selector) {
    return EnumerableDefaults.max(getThis(), selector);
  }

  public T min() {
    return (T) EnumerableDefaults.min((Enumerable) getThis());
  }

  public BigDecimal min(BigDecimalFunction1<T> selector) {
    return EnumerableDefaults.min(getThis(), selector);
  }

  public BigDecimal min(NullableBigDecimalFunction1<T> selector) {
    return EnumerableDefaults.min(getThis(), selector);
  }

  public double min(DoubleFunction1<T> selector) {
    return EnumerableDefaults.min(getThis(), selector);
  }

  public Double min(NullableDoubleFunction1<T> selector) {
    return EnumerableDefaults.min(getThis(), selector);
  }

  public int min(IntegerFunction1<T> selector) {
    return EnumerableDefaults.min(getThis(), selector);
  }

  public Integer min(NullableIntegerFunction1<T> selector) {
    return EnumerableDefaults.min(getThis(), selector);
  }

  public long min(LongFunction1<T> selector) {
    return EnumerableDefaults.min(getThis(), selector);
  }

  public Long min(NullableLongFunction1<T> selector) {
    return EnumerableDefaults.min(getThis(), selector);
  }

  public float min(FloatFunction1<T> selector) {
    return EnumerableDefaults.min(getThis(), selector);
  }

  public Float min(NullableFloatFunction1<T> selector) {
    return EnumerableDefaults.min(getThis(), selector);
  }

  public <TResult extends Comparable<TResult>> TResult min(
      Function1<T, TResult> selector) {
    return EnumerableDefaults.min(getThis(), selector);
  }

  public <TResult> Enumerable<TResult> ofType(Class<TResult> clazz) {
    return EnumerableDefaults.ofType(getThis(), clazz);
  }

  public <TKey extends Comparable> Enumerable<T> orderBy(
      Function1<T, TKey> keySelector) {
    return EnumerableDefaults.orderBy(getThis(), keySelector);
  }

  public <TKey> Enumerable<T> orderBy(Function1<T, TKey> keySelector,
      Comparator<TKey> comparator) {
    return EnumerableDefaults.orderBy(getThis(), keySelector, comparator);
  }

  public <TKey extends Comparable> Enumerable<T> orderByDescending(
      Function1<T, TKey> keySelector) {
    return EnumerableDefaults.orderByDescending(getThis(), keySelector);
  }

  public <TKey> Enumerable<T> orderByDescending(Function1<T, TKey> keySelector,
      Comparator<TKey> comparator) {
    return EnumerableDefaults.orderByDescending(getThis(), keySelector,
        comparator);
  }

  public Enumerable<T> reverse() {
    return EnumerableDefaults.reverse(getThis());
  }

  public <TResult> Enumerable<TResult> select(Function1<T, TResult> selector) {
    return EnumerableDefaults.select(getThis(), selector);
  }

  public <TResult> Enumerable<TResult> select(
      Function2<T, Integer, TResult> selector) {
    return EnumerableDefaults.select(getThis(), selector);
  }

  public <TResult> Enumerable<TResult> selectMany(
      Function1<T, Enumerable<TResult>> selector) {
    return EnumerableDefaults.selectMany(getThis(), selector);
  }

  public <TResult> Enumerable<TResult> selectMany(
      Function2<T, Integer, Enumerable<TResult>> selector) {
    return EnumerableDefaults.selectMany(getThis(), selector);
  }

  public <TCollection, TResult> Enumerable<TResult> selectMany(
      Function2<T, Integer, Enumerable<TCollection>> collectionSelector,
      Function2<T, TCollection, TResult> resultSelector) {
    return EnumerableDefaults.selectMany(getThis(), collectionSelector,
        resultSelector);
  }

  public <TCollection, TResult> Enumerable<TResult> selectMany(
      Function1<T, Enumerable<TCollection>> collectionSelector,
      Function2<T, TCollection, TResult> resultSelector) {
    return EnumerableDefaults.selectMany(getThis(), collectionSelector,
        resultSelector);
  }

  public boolean sequenceEqual(Enumerable<T> enumerable1) {
    return EnumerableDefaults.sequenceEqual(getThis(), enumerable1);
  }

  public boolean sequenceEqual(Enumerable<T> enumerable1,
      EqualityComparer<T> comparer) {
    return EnumerableDefaults.sequenceEqual(getThis(), enumerable1, comparer);
  }

  public T single() {
    return EnumerableDefaults.single(getThis());
  }

  public T single(Predicate1<T> predicate) {
    return EnumerableDefaults.single(getThis(), predicate);
  }

  public T singleOrDefault() {
    return EnumerableDefaults.singleOrDefault(getThis());
  }

  public T singleOrDefault(Predicate1<T> predicate) {
    return EnumerableDefaults.singleOrDefault(getThis(), predicate);
  }

  public Enumerable<T> skip(int count) {
    return EnumerableDefaults.skip(getThis(), count);
  }

  public Enumerable<T> skipWhile(Predicate1<T> predicate) {
    return EnumerableDefaults.skipWhile(getThis(), predicate);
  }

  public Enumerable<T> skipWhile(Predicate2<T, Integer> predicate) {
    return EnumerableDefaults.skipWhile(getThis(), predicate);
  }

  public BigDecimal sum(BigDecimalFunction1<T> selector) {
    return EnumerableDefaults.sum(getThis(), selector);
  }

  public BigDecimal sum(NullableBigDecimalFunction1<T> selector) {
    return EnumerableDefaults.sum(getThis(), selector);
  }

  public double sum(DoubleFunction1<T> selector) {
    return EnumerableDefaults.sum(getThis(), selector);
  }

  public Double sum(NullableDoubleFunction1<T> selector) {
    return EnumerableDefaults.sum(getThis(), selector);
  }

  public int sum(IntegerFunction1<T> selector) {
    return EnumerableDefaults.sum(getThis(), selector);
  }

  public Integer sum(NullableIntegerFunction1<T> selector) {
    return EnumerableDefaults.sum(getThis(), selector);
  }

  public long sum(LongFunction1<T> selector) {
    return EnumerableDefaults.sum(getThis(), selector);
  }

  public Long sum(NullableLongFunction1<T> selector) {
    return EnumerableDefaults.sum(getThis(), selector);
  }

  public float sum(FloatFunction1<T> selector) {
    return EnumerableDefaults.sum(getThis(), selector);
  }

  public Float sum(NullableFloatFunction1<T> selector) {
    return EnumerableDefaults.sum(getThis(), selector);
  }

  public Enumerable<T> take(int count) {
    return EnumerableDefaults.take(getThis(), count);
  }

  public Enumerable<T> takeWhile(Predicate1<T> predicate) {
    return EnumerableDefaults.takeWhile(getThis(), predicate);
  }

  public Enumerable<T> takeWhile(Predicate2<T, Integer> predicate) {
    return EnumerableDefaults.takeWhile(getThis(), predicate);
  }

  public <TKey extends Comparable<TKey>> OrderedEnumerable<T> thenBy(
      Function1<T, TKey> keySelector) {
    return EnumerableDefaults.thenBy(getThisOrdered(), keySelector);
  }

  public <TKey> OrderedEnumerable<T> thenBy(Function1<T, TKey> keySelector,
      Comparator<TKey> comparator) {
    return EnumerableDefaults.thenByDescending(getThisOrdered(), keySelector,
        comparator);
  }

  public <TKey extends Comparable<TKey>> OrderedEnumerable<T> thenByDescending(
      Function1<T, TKey> keySelector) {
    return EnumerableDefaults.thenByDescending(getThisOrdered(), keySelector);
  }

  public <TKey> OrderedEnumerable<T> thenByDescending(
      Function1<T, TKey> keySelector, Comparator<TKey> comparator) {
    return EnumerableDefaults.thenBy(getThisOrdered(), keySelector, comparator);
  }

  public <TKey> Map<TKey, T> toMap(Function1<T, TKey> keySelector) {
    return EnumerableDefaults.toMap(getThis(), keySelector);
  }

  public <TKey> Map<TKey, T> toMap(Function1<T, TKey> keySelector,
      EqualityComparer<TKey> comparer) {
    return EnumerableDefaults.toMap(getThis(), keySelector, comparer);
  }

  public <TKey, TElement> Map<TKey, TElement> toMap(
      Function1<T, TKey> keySelector, Function1<T, TElement> elementSelector) {
    return EnumerableDefaults.toMap(getThis(), keySelector, elementSelector);
  }

  public <TKey, TElement> Map<TKey, TElement> toMap(
      Function1<T, TKey> keySelector, Function1<T, TElement> elementSelector,
      EqualityComparer<TKey> comparer) {
    return EnumerableDefaults.toMap(getThis(), keySelector, elementSelector,
        comparer);
  }

  public List<T> toList() {
    return EnumerableDefaults.toList(getThis());
  }

  public <TKey> Lookup<TKey, T> toLookup(Function1<T, TKey> keySelector) {
    return EnumerableDefaults.toLookup(getThis(), keySelector);
  }

  public <TKey> Lookup<TKey, T> toLookup(Function1<T, TKey> keySelector,
      EqualityComparer<TKey> comparer) {
    return EnumerableDefaults.toLookup(getThis(), keySelector, comparer);
  }

  public <TKey, TElement> Lookup<TKey, TElement> toLookup(
      Function1<T, TKey> keySelector, Function1<T, TElement> elementSelector) {
    return EnumerableDefaults.toLookup(getThis(), keySelector, elementSelector);
  }

  public <TKey, TElement> Lookup<TKey, TElement> toLookup(
      Function1<T, TKey> keySelector, Function1<T, TElement> elementSelector,
      EqualityComparer<TKey> comparer) {
    return EnumerableDefaults.toLookup(getThis(), keySelector, elementSelector,
        comparer);
  }

  public Enumerable<T> union(Enumerable<T> source1) {
    return EnumerableDefaults.union(getThis(), source1);
  }

  public Enumerable<T> union(Enumerable<T> source1,
      EqualityComparer<T> comparer) {
    return EnumerableDefaults.union(getThis(), source1, comparer);
  }

  public Enumerable<T> where(Predicate1<T> predicate) {
    return EnumerableDefaults.where(getThis(), predicate);
  }

  public Enumerable<T> where(Predicate2<T, Integer> predicate) {
    return EnumerableDefaults.where(getThis(), predicate);
  }

  public <T1, TResult> Enumerable<TResult> zip(Enumerable<T1> source1,
      Function2<T, T1, TResult> resultSelector) {
    return EnumerableDefaults.zip(getThis(), source1, resultSelector);
  }
}

// End DefaultEnumerable.java
