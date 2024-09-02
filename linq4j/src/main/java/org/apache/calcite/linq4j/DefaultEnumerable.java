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
import org.apache.calcite.linq4j.function.IntegerFunction1;
import org.apache.calcite.linq4j.function.LongFunction1;
import org.apache.calcite.linq4j.function.NullableBigDecimalFunction1;
import org.apache.calcite.linq4j.function.NullableDoubleFunction1;
import org.apache.calcite.linq4j.function.NullableFloatFunction1;
import org.apache.calcite.linq4j.function.NullableIntegerFunction1;
import org.apache.calcite.linq4j.function.NullableLongFunction1;
import org.apache.calcite.linq4j.function.Predicate1;
import org.apache.calcite.linq4j.function.Predicate2;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.PolyNull;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Implementation of the {@link org.apache.calcite.linq4j.Enumerable} interface
 * that implements the extension methods by calling into the {@link Extensions}
 * class.
 *
 * <p>The are two abstract methods:
 * {@link #enumerator()} and {@link #iterator()}.
 * The derived class can implement each separately, or implement one in terms of
 * the other.
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

  @Override public <R> @Nullable R foreach(Function1<T, R> func) {
    R result = null;
    try (Enumerator<T> enumerator = enumerator()) {
      while (enumerator.moveNext()) {
        T t = enumerator.current();
        result = func.apply(t);
      }
      return result;
    }
  }

  @Override public Queryable<T> asQueryable() {
    return Extensions.asQueryable(this);
  }

  // NOTE: Not part of the Queryable interface.
  protected OrderedQueryable<T> asOrderedQueryable() {
    return EnumerableDefaults.asOrderedQueryable(this);
  }

  @Override public @Nullable T aggregate(Function2<@Nullable T, T, T> func) {
    return EnumerableDefaults.aggregate(getThis(), func);
  }

  @Override public <TAccumulate> @PolyNull TAccumulate aggregate(@PolyNull TAccumulate seed,
      Function2<@PolyNull TAccumulate, T, @PolyNull TAccumulate> func) {
    return EnumerableDefaults.aggregate(getThis(), seed, func);
  }

  @Override public <TAccumulate, TResult> TResult aggregate(TAccumulate seed,
      Function2<TAccumulate, T, TAccumulate> func,
      Function1<TAccumulate, TResult> selector) {
    return EnumerableDefaults.aggregate(getThis(), seed, func, selector);
  }

  @Override public boolean all(Predicate1<T> predicate) {
    return EnumerableDefaults.all(getThis(), predicate);
  }

  @Override public boolean any() {
    return EnumerableDefaults.any(getThis());
  }

  @Override public boolean any(Predicate1<T> predicate) {
    return EnumerableDefaults.any(getThis(), predicate);
  }

  @Override public Enumerable<T> asEnumerable() {
    return EnumerableDefaults.asEnumerable(getThis());
  }

  @Override public BigDecimal average(BigDecimalFunction1<T> selector) {
    return EnumerableDefaults.average(getThis(), selector);
  }

  @Override public BigDecimal average(NullableBigDecimalFunction1<T> selector) {
    return EnumerableDefaults.average(getThis(), selector);
  }

  @Override public double average(DoubleFunction1<T> selector) {
    return EnumerableDefaults.average(getThis(), selector);
  }

  @Override public Double average(NullableDoubleFunction1<T> selector) {
    return EnumerableDefaults.average(getThis(), selector);
  }

  @Override public int average(IntegerFunction1<T> selector) {
    return EnumerableDefaults.average(getThis(), selector);
  }

  @Override public Integer average(NullableIntegerFunction1<T> selector) {
    return EnumerableDefaults.average(getThis(), selector);
  }

  @Override public long average(LongFunction1<T> selector) {
    return EnumerableDefaults.average(getThis(), selector);
  }

  @Override public Long average(NullableLongFunction1<T> selector) {
    return EnumerableDefaults.average(getThis(), selector);
  }

  @Override public float average(FloatFunction1<T> selector) {
    return EnumerableDefaults.average(getThis(), selector);
  }

  @Override public Float average(NullableFloatFunction1<T> selector) {
    return EnumerableDefaults.average(getThis(), selector);
  }

  @Override public <T2> Enumerable<T2> cast(Class<T2> clazz) {
    return EnumerableDefaults.cast(getThis(), clazz);
  }

  @Override public Enumerable<T> concat(Enumerable<T> enumerable1) {
    return EnumerableDefaults.concat(getThis(), enumerable1);
  }

  @Override public boolean contains(T element) {
    return EnumerableDefaults.contains(getThis(), element);
  }

  @Override public boolean contains(T element, EqualityComparer<T> comparer) {
    return EnumerableDefaults.contains(getThis(), element, comparer);
  }

  @Override public int count() {
    return EnumerableDefaults.count(getThis());
  }

  @Override public int count(Predicate1<T> predicate) {
    return EnumerableDefaults.count(getThis(), predicate);
  }

  @Override public <TKey> OrderedEnumerable<T> createOrderedEnumerable(
      Function1<T, TKey> keySelector, Comparator<TKey> comparator,
      boolean descending) {
    return EnumerableDefaults.createOrderedEnumerable(getThisOrdered(),
        keySelector, comparator, descending);
  }

  @Override public Enumerable<@Nullable T> defaultIfEmpty() {
    return EnumerableDefaults.defaultIfEmpty(getThis());
  }

  @Override public Enumerable<@PolyNull T> defaultIfEmpty(@PolyNull T value) {
    return EnumerableDefaults.defaultIfEmpty(getThis(), value);
  }

  @Override public Enumerable<T> distinct() {
    return EnumerableDefaults.distinct(getThis());
  }

  @Override public Enumerable<T> distinct(EqualityComparer<T> comparer) {
    return EnumerableDefaults.distinct(getThis(), comparer);
  }

  @Override public T elementAt(int index) {
    return EnumerableDefaults.elementAt(getThis(), index);
  }

  @Override public @Nullable T elementAtOrDefault(int index) {
    return EnumerableDefaults.elementAtOrDefault(getThis(), index);
  }

  @Override public Enumerable<T> except(Enumerable<T> enumerable1) {
    return except(enumerable1, false);
  }

  @Override public Enumerable<T> except(Enumerable<T> enumerable1, boolean all) {
    return EnumerableDefaults.except(getThis(), enumerable1, all);
  }

  @Override public Enumerable<T> except(Enumerable<T> enumerable1,
      EqualityComparer<T> comparer) {
    return except(enumerable1, comparer, false);
  }

  @Override public Enumerable<T> except(Enumerable<T> enumerable1,
      EqualityComparer<T> comparer, boolean all) {
    return EnumerableDefaults.except(getThis(), enumerable1, comparer, all);
  }

  @Override public T first() {
    return EnumerableDefaults.first(getThis());
  }

  @Override public T first(Predicate1<T> predicate) {
    return EnumerableDefaults.first(getThis(), predicate);
  }

  @Override public @Nullable T firstOrDefault() {
    return EnumerableDefaults.firstOrDefault(getThis());
  }

  @Override public @Nullable T firstOrDefault(Predicate1<T> predicate) {
    return EnumerableDefaults.firstOrDefault(getThis(), predicate);
  }

  @Override public <TKey> Enumerable<Grouping<TKey, T>> groupBy(
      Function1<T, TKey> keySelector) {
    return EnumerableDefaults.groupBy(getThis(), keySelector);
  }

  @Override public <TKey> Enumerable<Grouping<TKey, T>> groupBy(
      Function1<T, TKey> keySelector, EqualityComparer<TKey> comparer) {
    return EnumerableDefaults.groupBy(getThis(), keySelector, comparer);
  }

  @Override public <TKey, TElement> Enumerable<Grouping<TKey, TElement>> groupBy(
      Function1<T, TKey> keySelector, Function1<T, TElement> elementSelector) {
    return EnumerableDefaults.groupBy(getThis(), keySelector, elementSelector);
  }

  @Override public <TKey, TElement> Enumerable<Grouping<TKey, TElement>> groupBy(
      Function1<T, TKey> keySelector, Function1<T, TElement> elementSelector,
      EqualityComparer<TKey> comparer) {
    return EnumerableDefaults.groupBy(getThis(), keySelector, elementSelector,
        comparer);
  }

  @Override public <TKey, TResult> Enumerable<TResult> groupBy(
      Function1<T, TKey> keySelector,
      Function2<TKey, Enumerable<T>, TResult> elementSelector,
      EqualityComparer<TKey> comparer) {
    return EnumerableDefaults.groupBy(getThis(), keySelector, elementSelector,
        comparer);
  }

  @Override public <TKey, TResult> Enumerable<TResult> groupBy(
      Function1<T, TKey> keySelector,
      Function2<TKey, Enumerable<T>, TResult> resultSelector) {
    return EnumerableDefaults.groupBy(getThis(), keySelector, resultSelector);
  }

  @Override public <TKey, TElement, TResult> Enumerable<TResult> groupBy(
      Function1<T, TKey> keySelector, Function1<T, TElement> elementSelector,
      Function2<TKey, Enumerable<TElement>, TResult> resultSelector) {
    return EnumerableDefaults.groupBy(getThis(), keySelector, elementSelector,
        resultSelector);
  }

  @Override public <TKey, TElement, TResult> Enumerable<TResult> groupBy(
      Function1<T, TKey> keySelector, Function1<T, TElement> elementSelector,
      Function2<TKey, Enumerable<TElement>, TResult> resultSelector,
      EqualityComparer<TKey> comparer) {
    return EnumerableDefaults.groupBy(getThis(), keySelector, elementSelector,
        resultSelector, comparer);
  }

  @Override public <TKey, TAccumulate, TResult> Enumerable<TResult> groupBy(
      Function1<T, TKey> keySelector,
      Function0<TAccumulate> accumulatorInitializer,
      Function2<TAccumulate, T, TAccumulate> accumulatorAdder,
      Function2<TKey, TAccumulate, TResult> resultSelector) {
    return EnumerableDefaults.groupBy(getThis(), keySelector,
        accumulatorInitializer, accumulatorAdder, resultSelector);
  }

  @Override public <TKey, TAccumulate, TResult> Enumerable<TResult> groupBy(
      Function1<T, TKey> keySelector,
      Function0<TAccumulate> accumulatorInitializer,
      Function2<TAccumulate, T, TAccumulate> accumulatorAdder,
      Function2<TKey, TAccumulate, TResult> resultSelector,
      EqualityComparer<TKey> comparer) {
    return EnumerableDefaults.groupBy(getThis(), keySelector,
        accumulatorInitializer, accumulatorAdder, resultSelector, comparer);
  }

  @Override public <TKey, TAccumulate, TResult> Enumerable<TResult> sortedGroupBy(
      Function1<T, TKey> keySelector,
      Function0<TAccumulate> accumulatorInitializer,
      Function2<TAccumulate, T, TAccumulate> accumulatorAdder,
      Function2<TKey, TAccumulate, TResult> resultSelector,
      Comparator<TKey> comparator) {
    return EnumerableDefaults.sortedGroupBy(
        getThis(), keySelector, accumulatorInitializer,
        accumulatorAdder, resultSelector, comparator);
  }

  @Override public <TInner, TKey, TResult> Enumerable<TResult> groupJoin(
      Enumerable<TInner> inner, Function1<T, TKey> outerKeySelector,
      Function1<TInner, TKey> innerKeySelector,
      Function2<T, Enumerable<TInner>, TResult> resultSelector) {
    return EnumerableDefaults.groupJoin(getThis(), inner, outerKeySelector,
        innerKeySelector, resultSelector);
  }

  @Override public <TInner, TKey, TResult> Enumerable<TResult> groupJoin(
      Enumerable<TInner> inner, Function1<T, TKey> outerKeySelector,
      Function1<TInner, TKey> innerKeySelector,
      Function2<T, Enumerable<TInner>, TResult> resultSelector,
      EqualityComparer<TKey> comparer) {
    return EnumerableDefaults.groupJoin(getThis(), inner, outerKeySelector,
        innerKeySelector, resultSelector, comparer);
  }

  @Override public Enumerable<T> intersect(Enumerable<T> enumerable1) {
    return intersect(enumerable1, false);
  }

  @Override public Enumerable<T> intersect(Enumerable<T> enumerable1, boolean all) {
    return EnumerableDefaults.intersect(getThis(), enumerable1, all);
  }

  @Override public Enumerable<T> intersect(Enumerable<T> enumerable1,
      EqualityComparer<T> comparer) {
    return intersect(enumerable1, comparer, false);
  }

  @Override public Enumerable<T> intersect(Enumerable<T> enumerable1,
      EqualityComparer<T> comparer, boolean all) {
    return EnumerableDefaults.intersect(getThis(), enumerable1, comparer, all);
  }

  @Override public <C extends Collection<? super T>> C into(C sink) {
    return EnumerableDefaults.into(getThis(), sink);
  }

  @Override public <C extends Collection<? super T>> C removeAll(C sink) {
    return EnumerableDefaults.remove(getThis(), sink);
  }

  @Override public <TInner, TKey, TResult> Enumerable<TResult> hashJoin(
      Enumerable<TInner> inner, Function1<T, TKey> outerKeySelector,
      Function1<TInner, TKey> innerKeySelector,
      Function2<T, TInner, TResult> resultSelector) {
    return EnumerableDefaults.hashJoin(getThis(), inner, outerKeySelector,
        innerKeySelector, resultSelector);
  }

  @Override public <TInner, TKey, TResult> Enumerable<TResult> hashJoin(
      Enumerable<TInner> inner, Function1<T, TKey> outerKeySelector,
      Function1<TInner, TKey> innerKeySelector,
      Function2<T, TInner, TResult> resultSelector,
      EqualityComparer<TKey> comparer) {
    return EnumerableDefaults.hashJoin(getThis(), inner, outerKeySelector,
        innerKeySelector, resultSelector, comparer);
  }

  @Override public <TInner, TKey, TResult> Enumerable<TResult> asofJoin(
      Enumerable<TInner> inner,
      Function1<T, TKey> outerKeySelector,
      Function1<TInner, TKey> innerKeySelector,
      Function2<T, @Nullable TInner, TResult> resultSelector,
      Predicate2<T, TInner> matchComparator,
      Comparator<TInner> timestampComparator,
      boolean generateNullsOnRight) {
    return EnumerableDefaults.asofJoin(getThis(), inner, outerKeySelector,
        innerKeySelector, resultSelector, matchComparator,
        timestampComparator, generateNullsOnRight);
  }

  @Override public <TInner, TKey, TResult> Enumerable<TResult> hashJoin(
      Enumerable<TInner> inner, Function1<T, TKey> outerKeySelector,
      Function1<TInner, TKey> innerKeySelector,
      Function2<T, TInner, TResult> resultSelector,
      @Nullable EqualityComparer<TKey> comparer,
      boolean generateNullsOnLeft, boolean generateNullsOnRight) {
    return EnumerableDefaults.hashJoin(getThis(), inner, outerKeySelector,
        innerKeySelector, resultSelector, comparer, generateNullsOnLeft,
        generateNullsOnRight);
  }

  @Override public <TInner, TKey, TResult> Enumerable<TResult> hashJoin(
      Enumerable<TInner> inner, Function1<T, TKey> outerKeySelector,
      Function1<TInner, TKey> innerKeySelector,
      Function2<T, TInner, TResult> resultSelector,
      EqualityComparer<TKey> comparer,
      boolean generateNullsOnLeft, boolean generateNullsOnRight,
      @Nullable Predicate2<T, TInner> predicate) {
    return EnumerableDefaults.hashJoin(getThis(), inner, outerKeySelector,
        innerKeySelector, resultSelector, comparer, generateNullsOnLeft,
        generateNullsOnRight, predicate);
  }

  @Override public <TInner, TResult> Enumerable<TResult> correlateJoin(
      JoinType joinType, Function1<T, Enumerable<TInner>> inner,
      Function2<T, TInner, TResult> resultSelector) {
    return EnumerableDefaults.correlateJoin(joinType, getThis(), inner,
        resultSelector);
  }

  @Override public T last() {
    return EnumerableDefaults.last(getThis());
  }

  @Override public T last(Predicate1<T> predicate) {
    return EnumerableDefaults.last(getThis(), predicate);
  }

  @Override public @Nullable T lastOrDefault() {
    return EnumerableDefaults.lastOrDefault(getThis());
  }

  @Override public @Nullable T lastOrDefault(Predicate1<T> predicate) {
    return EnumerableDefaults.lastOrDefault(getThis(), predicate);
  }

  @Override public long longCount() {
    return EnumerableDefaults.longCount(getThis());
  }

  @Override public long longCount(Predicate1<T> predicate) {
    return EnumerableDefaults.longCount(getThis(), predicate);
  }

  @SuppressWarnings("unchecked")
  @Override public @Nullable T max() {
    return (@Nullable T) EnumerableDefaults.max((Enumerable) getThis());
  }

  @Override public @Nullable BigDecimal max(BigDecimalFunction1<T> selector) {
    return EnumerableDefaults.max(getThis(), selector);
  }

  @Override public @Nullable BigDecimal max(NullableBigDecimalFunction1<T> selector) {
    return EnumerableDefaults.max(getThis(), selector);
  }

  @Override public double max(DoubleFunction1<T> selector) {
    return EnumerableDefaults.max(getThis(), selector);
  }

  @Override public @Nullable Double max(NullableDoubleFunction1<T> selector) {
    return EnumerableDefaults.max(getThis(), selector);
  }

  @Override public int max(IntegerFunction1<T> selector) {
    return EnumerableDefaults.max(getThis(), selector);
  }

  @Override public @Nullable Integer max(NullableIntegerFunction1<T> selector) {
    return EnumerableDefaults.max(getThis(), selector);
  }

  @Override public long max(LongFunction1<T> selector) {
    return EnumerableDefaults.max(getThis(), selector);
  }

  @Override public @Nullable Long max(NullableLongFunction1<T> selector) {
    return EnumerableDefaults.max(getThis(), selector);
  }

  @Override public float max(FloatFunction1<T> selector) {
    return EnumerableDefaults.max(getThis(), selector);
  }

  @Override public @Nullable Float max(NullableFloatFunction1<T> selector) {
    return EnumerableDefaults.max(getThis(), selector);
  }

  @Override public <TResult extends Comparable<TResult>> @Nullable TResult max(
      Function1<T, TResult> selector) {
    return EnumerableDefaults.max(getThis(), selector);
  }

  @SuppressWarnings("unchecked")
  @Override public @Nullable T min() {
    return (@Nullable T) EnumerableDefaults.min((Enumerable) getThis());
  }

  @Override public @Nullable BigDecimal min(BigDecimalFunction1<T> selector) {
    return EnumerableDefaults.min(getThis(), selector);
  }

  @Override public @Nullable BigDecimal min(NullableBigDecimalFunction1<T> selector) {
    return EnumerableDefaults.min(getThis(), selector);
  }

  @Override public double min(DoubleFunction1<T> selector) {
    return EnumerableDefaults.min(getThis(), selector);
  }

  @Override public @Nullable Double min(NullableDoubleFunction1<T> selector) {
    return EnumerableDefaults.min(getThis(), selector);
  }

  @Override public int min(IntegerFunction1<T> selector) {
    return EnumerableDefaults.min(getThis(), selector);
  }

  @Override public @Nullable Integer min(NullableIntegerFunction1<T> selector) {
    return EnumerableDefaults.min(getThis(), selector);
  }

  @Override public long min(LongFunction1<T> selector) {
    return EnumerableDefaults.min(getThis(), selector);
  }

  @Override public @Nullable Long min(NullableLongFunction1<T> selector) {
    return EnumerableDefaults.min(getThis(), selector);
  }

  @Override public float min(FloatFunction1<T> selector) {
    return EnumerableDefaults.min(getThis(), selector);
  }

  @Override public @Nullable Float min(NullableFloatFunction1<T> selector) {
    return EnumerableDefaults.min(getThis(), selector);
  }

  @Override public <TResult extends Comparable<TResult>> @Nullable TResult min(
      Function1<T, TResult> selector) {
    return EnumerableDefaults.min(getThis(), selector);
  }

  @Override public <TResult> Enumerable<TResult> ofType(Class<TResult> clazz) {
    return EnumerableDefaults.ofType(getThis(), clazz);
  }

  @Override public <TKey extends Comparable> Enumerable<T> orderBy(
      Function1<T, TKey> keySelector) {
    return EnumerableDefaults.orderBy(getThis(), keySelector);
  }

  @Override public <TKey> Enumerable<T> orderBy(Function1<T, TKey> keySelector,
      Comparator<TKey> comparator) {
    return EnumerableDefaults.orderBy(getThis(), keySelector, comparator);
  }

  @Override public <TKey extends Comparable> Enumerable<T> orderByDescending(
      Function1<T, TKey> keySelector) {
    return EnumerableDefaults.orderByDescending(getThis(), keySelector);
  }

  @Override public <TKey> Enumerable<T> orderByDescending(Function1<T, TKey> keySelector,
      Comparator<TKey> comparator) {
    return EnumerableDefaults.orderByDescending(getThis(), keySelector,
        comparator);
  }

  @Override public Enumerable<T> reverse() {
    return EnumerableDefaults.reverse(getThis());
  }

  @Override public <TResult> Enumerable<TResult> select(Function1<T, TResult> selector) {
    return EnumerableDefaults.select(getThis(), selector);
  }

  @Override public <TResult> Enumerable<TResult> select(
      Function2<T, Integer, TResult> selector) {
    return EnumerableDefaults.select(getThis(), selector);
  }

  @Override public <TResult> Enumerable<TResult> selectMany(
      Function1<T, Enumerable<TResult>> selector) {
    return EnumerableDefaults.selectMany(getThis(), selector);
  }

  @Override public <TResult> Enumerable<TResult> selectMany(
      Function2<T, Integer, Enumerable<TResult>> selector) {
    return EnumerableDefaults.selectMany(getThis(), selector);
  }

  @Override public <TCollection, TResult> Enumerable<TResult> selectMany(
      Function2<T, Integer, Enumerable<TCollection>> collectionSelector,
      Function2<T, TCollection, TResult> resultSelector) {
    return EnumerableDefaults.selectMany(getThis(), collectionSelector,
        resultSelector);
  }

  @Override public <TCollection, TResult> Enumerable<TResult> selectMany(
      Function1<T, Enumerable<TCollection>> collectionSelector,
      Function2<T, TCollection, TResult> resultSelector) {
    return EnumerableDefaults.selectMany(getThis(), collectionSelector,
        resultSelector);
  }

  @Override public boolean sequenceEqual(Enumerable<T> enumerable1) {
    return EnumerableDefaults.sequenceEqual(getThis(), enumerable1);
  }

  @Override public boolean sequenceEqual(Enumerable<T> enumerable1,
      EqualityComparer<T> comparer) {
    return EnumerableDefaults.sequenceEqual(getThis(), enumerable1, comparer);
  }

  @Override public T single() {
    return EnumerableDefaults.single(getThis());
  }

  @Override public T single(Predicate1<T> predicate) {
    return EnumerableDefaults.single(getThis(), predicate);
  }

  @Override public @Nullable T singleOrDefault() {
    return EnumerableDefaults.singleOrDefault(getThis());
  }

  @Override public @Nullable T singleOrDefault(Predicate1<T> predicate) {
    return EnumerableDefaults.singleOrDefault(getThis(), predicate);
  }

  @Override public Enumerable<T> skip(int count) {
    return EnumerableDefaults.skip(getThis(), count);
  }

  @Override public Enumerable<T> skipWhile(Predicate1<T> predicate) {
    return EnumerableDefaults.skipWhile(getThis(), predicate);
  }

  @Override public Enumerable<T> skipWhile(Predicate2<T, Integer> predicate) {
    return EnumerableDefaults.skipWhile(getThis(), predicate);
  }

  @Override public BigDecimal sum(BigDecimalFunction1<T> selector) {
    return EnumerableDefaults.sum(getThis(), selector);
  }

  @Override public BigDecimal sum(NullableBigDecimalFunction1<T> selector) {
    return EnumerableDefaults.sum(getThis(), selector);
  }

  @Override public double sum(DoubleFunction1<T> selector) {
    return EnumerableDefaults.sum(getThis(), selector);
  }

  @Override public Double sum(NullableDoubleFunction1<T> selector) {
    return EnumerableDefaults.sum(getThis(), selector);
  }

  @Override public int sum(IntegerFunction1<T> selector) {
    return EnumerableDefaults.sum(getThis(), selector);
  }

  @Override public Integer sum(NullableIntegerFunction1<T> selector) {
    return EnumerableDefaults.sum(getThis(), selector);
  }

  @Override public long sum(LongFunction1<T> selector) {
    return EnumerableDefaults.sum(getThis(), selector);
  }

  @Override public Long sum(NullableLongFunction1<T> selector) {
    return EnumerableDefaults.sum(getThis(), selector);
  }

  @Override public float sum(FloatFunction1<T> selector) {
    return EnumerableDefaults.sum(getThis(), selector);
  }

  @Override public Float sum(NullableFloatFunction1<T> selector) {
    return EnumerableDefaults.sum(getThis(), selector);
  }

  @Override public Enumerable<T> take(int count) {
    return EnumerableDefaults.take(getThis(), count);
  }

  @Override public Enumerable<T> takeWhile(Predicate1<T> predicate) {
    return EnumerableDefaults.takeWhile(getThis(), predicate);
  }

  @Override public Enumerable<T> takeWhile(Predicate2<T, Integer> predicate) {
    return EnumerableDefaults.takeWhile(getThis(), predicate);
  }

  @Override public <TKey extends Comparable<TKey>> OrderedEnumerable<T> thenBy(
      Function1<T, TKey> keySelector) {
    return EnumerableDefaults.thenBy(getThisOrdered(), keySelector);
  }

  @Override public <TKey> OrderedEnumerable<T> thenBy(Function1<T, TKey> keySelector,
      Comparator<TKey> comparator) {
    return EnumerableDefaults.thenByDescending(getThisOrdered(), keySelector,
        comparator);
  }

  @Override public <TKey extends Comparable<TKey>> OrderedEnumerable<T> thenByDescending(
      Function1<T, TKey> keySelector) {
    return EnumerableDefaults.thenByDescending(getThisOrdered(), keySelector);
  }

  @Override public <TKey> OrderedEnumerable<T> thenByDescending(
      Function1<T, TKey> keySelector, Comparator<TKey> comparator) {
    return EnumerableDefaults.thenBy(getThisOrdered(), keySelector, comparator);
  }

  @Override public <TKey> Map<TKey, T> toMap(Function1<T, TKey> keySelector) {
    return EnumerableDefaults.toMap(getThis(), keySelector);
  }

  @Override public <TKey> Map<TKey, T> toMap(Function1<T, TKey> keySelector,
      EqualityComparer<TKey> comparer) {
    return EnumerableDefaults.toMap(getThis(), keySelector, comparer);
  }

  @Override public <TKey, TElement> Map<TKey, TElement> toMap(
      Function1<T, TKey> keySelector, Function1<T, TElement> elementSelector) {
    return EnumerableDefaults.toMap(getThis(), keySelector, elementSelector);
  }

  @Override public <TKey, TElement> Map<TKey, TElement> toMap(
      Function1<T, TKey> keySelector, Function1<T, TElement> elementSelector,
      EqualityComparer<TKey> comparer) {
    return EnumerableDefaults.toMap(getThis(), keySelector, elementSelector,
        comparer);
  }

  @Override public List<T> toList() {
    return EnumerableDefaults.toList(getThis());
  }

  @Override public <TKey> Lookup<TKey, T> toLookup(Function1<T, TKey> keySelector) {
    return EnumerableDefaults.toLookup(getThis(), keySelector);
  }

  @Override public <TKey> Lookup<TKey, T> toLookup(Function1<T, TKey> keySelector,
      EqualityComparer<TKey> comparer) {
    return EnumerableDefaults.toLookup(getThis(), keySelector, comparer);
  }

  @Override public <TKey, TElement> Lookup<TKey, TElement> toLookup(
      Function1<T, TKey> keySelector, Function1<T, TElement> elementSelector) {
    return EnumerableDefaults.toLookup(getThis(), keySelector, elementSelector);
  }

  @Override public <TKey, TElement> Lookup<TKey, TElement> toLookup(
      Function1<T, TKey> keySelector, Function1<T, TElement> elementSelector,
      EqualityComparer<TKey> comparer) {
    return EnumerableDefaults.toLookup(getThis(), keySelector, elementSelector,
        comparer);
  }

  @Override public Enumerable<T> union(Enumerable<T> source1) {
    return EnumerableDefaults.union(getThis(), source1);
  }

  @Override public Enumerable<T> union(Enumerable<T> source1,
      EqualityComparer<T> comparer) {
    return EnumerableDefaults.union(getThis(), source1, comparer);
  }

  @Override public Enumerable<T> where(Predicate1<T> predicate) {
    return EnumerableDefaults.where(getThis(), predicate);
  }

  @Override public Enumerable<T> where(Predicate2<T, Integer> predicate) {
    return EnumerableDefaults.where(getThis(), predicate);
  }

  @Override public <T1, TResult> Enumerable<TResult> zip(Enumerable<T1> source1,
      Function2<T, T1, TResult> resultSelector) {
    return EnumerableDefaults.zip(getThis(), source1, resultSelector);
  }
}
