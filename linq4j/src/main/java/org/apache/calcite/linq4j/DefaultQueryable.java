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
import org.apache.calcite.linq4j.tree.FunctionExpression;

import java.math.BigDecimal;
import java.util.Comparator;

/**
 * Implementation of the {@link Queryable} interface that
 * implements the extension methods by calling into the {@link Extensions}
 * class.
 *
 * @param <T> Element type
 */
abstract class DefaultQueryable<T> extends DefaultEnumerable<T>
    implements Queryable<T>, OrderedQueryable<T> {
  private final QueryableFactory<T> factory;

  /**
   * Creates a DefaultQueryable using a factory that records events.
   */
  protected DefaultQueryable() {
    this(QueryableRecorder.instance());
  }

  /**
   * Creates a DefaultQueryable using a particular factory.
   */
  protected DefaultQueryable(QueryableFactory<T> factory) {
    this.factory = factory;
  }

  // override return type
  @Override protected Queryable<T> getThis() {
    return this;
  }

  protected OrderedQueryable<T> getThisOrderedQueryable() {
    return this;
  }

  @Override public Enumerable<T> asEnumerable() {
    return new AbstractEnumerable<T>() {
      @Override public Enumerator<T> enumerator() {
        return DefaultQueryable.this.enumerator();
      }
    };
  }

  // Disambiguate

  @Override public Queryable<T> union(Enumerable<T> source1) {
    return factory.union(getThis(), source1);
  }

  @Override public Queryable<T> union(Enumerable<T> source1,
      EqualityComparer<T> comparer) {
    return factory.union(getThis(), source1, comparer);
  }

  @Override public Queryable<T> intersect(Enumerable<T> source1) {
    return intersect(source1, false);
  }

  @Override public Queryable<T> intersect(Enumerable<T> source1, boolean all) {
    return factory.intersect(getThis(), source1, all);
  }

  @Override public Queryable<T> intersect(Enumerable<T> source1,
      EqualityComparer<T> comparer) {
    return intersect(source1, comparer, false);
  }

  @Override public Queryable<T> intersect(Enumerable<T> source1,
      EqualityComparer<T> comparer, boolean all) {
    return factory.intersect(getThis(), source1, comparer, all);
  }

  @Override public Queryable<T> except(Enumerable<T> enumerable1,
      EqualityComparer<T> comparer) {
    return except(enumerable1, comparer, false);
  }
  @Override public Queryable<T> except(Enumerable<T> enumerable1,
      EqualityComparer<T> comparer, boolean all) {
    return factory.except(getThis(), enumerable1, comparer, all);
  }

  @Override public Queryable<T> except(Enumerable<T> enumerable1) {
    return except(enumerable1, false);
  }

  @Override public Queryable<T> except(Enumerable<T> enumerable1, boolean all) {
    return factory.except(getThis(), enumerable1, all);
  }

  @Override public Queryable<T> take(int count) {
    return factory.take(getThis(), count);
  }

  @Override public Queryable<T> skip(int count) {
    return factory.skip(getThis(), count);
  }

  @Override public Queryable<T> reverse() {
    return factory.reverse(getThis());
  }

  @Override public Queryable<T> distinct() {
    return factory.distinct(getThis());
  }

  @Override public Queryable<T> distinct(EqualityComparer<T> comparer) {
    return factory.distinct(getThis(), comparer);
  }

  @Override public <TResult> Queryable<TResult> ofType(Class<TResult> clazz) {
    return factory.ofType(getThis(), clazz);
  }

  @Override public Queryable<T> defaultIfEmpty() {
    return factory.defaultIfEmpty(getThis());
  }

  @Override public Queryable<T> asQueryable() {
    return this;
  }

  @Override public <T2> Queryable<T2> cast(Class<T2> clazz) {
    return factory.cast(getThis(), clazz);
  }

  // End disambiguate

  @Override public T aggregate(FunctionExpression<Function2<T, T, T>> selector) {
    return factory.aggregate(getThis(), selector);
  }

  @Override public <TAccumulate> TAccumulate aggregate(TAccumulate seed,
      FunctionExpression<Function2<TAccumulate, T, TAccumulate>> selector) {
    return factory.aggregate(getThis(), seed, selector);
  }

  @Override public <TAccumulate, TResult> TResult aggregate(TAccumulate seed,
      FunctionExpression<Function2<TAccumulate, T, TAccumulate>> func,
      FunctionExpression<Function1<TAccumulate, TResult>> selector) {
    return factory.aggregate(getThis(), seed, func, selector);
  }

  @Override public boolean all(FunctionExpression<Predicate1<T>> predicate) {
    return factory.all(getThis(), predicate);
  }

  @Override public boolean any(FunctionExpression<Predicate1<T>> predicate) {
    return factory.any(getThis(), predicate);
  }

  @Override public BigDecimal averageBigDecimal(
      FunctionExpression<BigDecimalFunction1<T>> selector) {
    return factory.averageBigDecimal(getThis(), selector);
  }

  @Override public BigDecimal averageNullableBigDecimal(
      FunctionExpression<NullableBigDecimalFunction1<T>> selector) {
    return factory.averageNullableBigDecimal(getThis(), selector);
  }

  @Override public double averageDouble(FunctionExpression<DoubleFunction1<T>> selector) {
    return factory.averageDouble(getThis(), selector);
  }

  @Override public Double averageNullableDouble(
      FunctionExpression<NullableDoubleFunction1<T>> selector) {
    return factory.averageNullableDouble(getThis(), selector);
  }

  @Override public int averageInteger(FunctionExpression<IntegerFunction1<T>> selector) {
    return factory.averageInteger(getThis(), selector);
  }

  @Override public Integer averageNullableInteger(
      FunctionExpression<NullableIntegerFunction1<T>> selector) {
    return factory.averageNullableInteger(getThis(), selector);
  }

  @Override public float averageFloat(FunctionExpression<FloatFunction1<T>> selector) {
    return factory.averageFloat(getThis(), selector);
  }

  @Override public Float averageNullableFloat(
      FunctionExpression<NullableFloatFunction1<T>> selector) {
    return factory.averageNullableFloat(getThis(), selector);
  }

  @Override public long averageLong(FunctionExpression<LongFunction1<T>> selector) {
    return factory.averageLong(getThis(), selector);
  }

  @Override public Long averageNullableLong(
      FunctionExpression<NullableLongFunction1<T>> selector) {
    return factory.averageNullableLong(getThis(), selector);
  }

  @Override public Queryable<T> concat(Enumerable<T> source2) {
    return factory.concat(getThis(), source2);
  }

  @Override public int count(FunctionExpression<Predicate1<T>> func) {
    return factory.count(getThis(), func);
  }

  @Override public T first(FunctionExpression<Predicate1<T>> predicate) {
    return factory.first(getThis(), predicate);
  }

  @Override public T firstOrDefault(FunctionExpression<Predicate1<T>> predicate) {
    return factory.firstOrDefault(getThis(), predicate);
  }

  @Override public <TKey> Queryable<Grouping<TKey, T>> groupBy(
      FunctionExpression<Function1<T, TKey>> keySelector) {
    return factory.groupBy(getThis(), keySelector);
  }

  @Override public <TKey> Queryable<Grouping<TKey, T>> groupBy(
      FunctionExpression<Function1<T, TKey>> keySelector,
      EqualityComparer<TKey> comparer) {
    return factory.groupBy(getThis(), keySelector, comparer);
  }

  @Override public <TKey, TElement> Queryable<Grouping<TKey, TElement>> groupBy(
      FunctionExpression<Function1<T, TKey>> keySelector,
      FunctionExpression<Function1<T, TElement>> elementSelector) {
    return factory.groupBy(getThis(), keySelector, elementSelector);
  }

  @Override public <TKey, TElement> Queryable<Grouping<TKey, TElement>> groupBy(
      FunctionExpression<Function1<T, TKey>> keySelector,
      FunctionExpression<Function1<T, TElement>> elementSelector,
      EqualityComparer<TKey> comparer) {
    return factory.groupBy(getThis(), keySelector, elementSelector, comparer);
  }

  @Override public <TKey, TResult> Queryable<TResult> groupByK(
      FunctionExpression<Function1<T, TKey>> keySelector,
      FunctionExpression<Function2<TKey, Enumerable<T>, TResult>> resultSelector) {
    return factory.groupByK(getThis(), keySelector, resultSelector);
  }

  @Override public <TKey, TResult> Queryable<TResult> groupByK(
      FunctionExpression<Function1<T, TKey>> keySelector,
      FunctionExpression<Function2<TKey, Enumerable<T>, TResult>> resultSelector,
      EqualityComparer<TKey> comparer) {
    return factory.groupByK(getThis(), keySelector, resultSelector, comparer);
  }

  @Override public <TKey, TElement, TResult> Queryable<TResult> groupBy(
      FunctionExpression<Function1<T, TKey>> keySelector,
      FunctionExpression<Function1<T, TElement>> elementSelector,
      FunctionExpression<Function2<TKey, Enumerable<TElement>, TResult>> resultSelector) {
    return factory.groupBy(getThis(), keySelector, elementSelector,
        resultSelector);
  }

  @Override public <TKey, TElement, TResult> Queryable<TResult> groupBy(
      FunctionExpression<Function1<T, TKey>> keySelector,
      FunctionExpression<Function1<T, TElement>> elementSelector,
      FunctionExpression<Function2<TKey, Enumerable<TElement>, TResult>> resultSelector,
      EqualityComparer<TKey> comparer) {
    return factory.groupBy(getThis(), keySelector, elementSelector,
        resultSelector, comparer);
  }

  @Override public <TInner, TKey, TResult> Queryable<TResult> groupJoin(
      Enumerable<TInner> inner,
      FunctionExpression<Function1<T, TKey>> outerKeySelector,
      FunctionExpression<Function1<TInner, TKey>> innerKeySelector,
      FunctionExpression<Function2<T, Enumerable<TInner>, TResult>> resultSelector) {
    return factory.groupJoin(getThis(), inner, outerKeySelector,
        innerKeySelector, resultSelector);
  }

  @Override public <TInner, TKey, TResult> Queryable<TResult> groupJoin(
      Enumerable<TInner> inner,
      FunctionExpression<Function1<T, TKey>> outerKeySelector,
      FunctionExpression<Function1<TInner, TKey>> innerKeySelector,
      FunctionExpression<Function2<T, Enumerable<TInner>, TResult>> resultSelector,
      EqualityComparer<TKey> comparer) {
    return factory.groupJoin(getThis(), inner, outerKeySelector,
        innerKeySelector, resultSelector, comparer);
  }

  @Override public <TInner, TKey, TResult> Queryable<TResult> join(
      Enumerable<TInner> inner,
      FunctionExpression<Function1<T, TKey>> outerKeySelector,
      FunctionExpression<Function1<TInner, TKey>> innerKeySelector,
      FunctionExpression<Function2<T, TInner, TResult>> resultSelector) {
    return factory.join(getThis(), inner, outerKeySelector, innerKeySelector,
        resultSelector);
  }

  @Override public <TInner, TKey, TResult> Queryable<TResult> join(
      Enumerable<TInner> inner,
      FunctionExpression<Function1<T, TKey>> outerKeySelector,
      FunctionExpression<Function1<TInner, TKey>> innerKeySelector,
      FunctionExpression<Function2<T, TInner, TResult>> resultSelector,
      EqualityComparer<TKey> comparer) {
    return factory.join(getThis(), inner, outerKeySelector, innerKeySelector,
        resultSelector, comparer);
  }

  @Override public T last(FunctionExpression<Predicate1<T>> predicate) {
    return factory.last(getThis(), predicate);
  }

  @Override public T lastOrDefault(FunctionExpression<Predicate1<T>> predicate) {
    return factory.lastOrDefault(getThis(), predicate);
  }

  @Override public long longCount(FunctionExpression<Predicate1<T>> predicate) {
    return factory.longCount(getThis(), predicate);
  }

  @Override public <TResult extends Comparable<TResult>> TResult max(
      FunctionExpression<Function1<T, TResult>> selector) {
    return factory.max(getThis(), selector);
  }

  @Override public <TResult extends Comparable<TResult>> TResult min(
      FunctionExpression<Function1<T, TResult>> selector) {
    return factory.min(getThis(), selector);
  }

  @Override public <TKey extends Comparable> OrderedQueryable<T> orderBy(
      FunctionExpression<Function1<T, TKey>> keySelector) {
    return factory.orderBy(getThis(), keySelector);
  }

  @Override public <TKey> OrderedQueryable<T> orderBy(
      FunctionExpression<Function1<T, TKey>> keySelector,
      Comparator<TKey> comparator) {
    return factory.orderBy(getThis(), keySelector, comparator);
  }

  @Override public <TKey extends Comparable> OrderedQueryable<T> orderByDescending(
      FunctionExpression<Function1<T, TKey>> keySelector) {
    return factory.orderByDescending(getThis(), keySelector);
  }

  @Override public <TKey> OrderedQueryable<T> orderByDescending(
      FunctionExpression<Function1<T, TKey>> keySelector,
      Comparator<TKey> comparator) {
    return factory.orderByDescending(getThis(), keySelector, comparator);
  }

  @Override public <TResult> Queryable<TResult> select(
      FunctionExpression<Function1<T, TResult>> selector) {
    return factory.select(getThis(), selector);
  }

  @Override public <TResult> Queryable<TResult> selectN(
      FunctionExpression<Function2<T, Integer, TResult>> selector) {
    return factory.selectN(getThis(), selector);
  }

  @Override public <TResult> Queryable<TResult> selectMany(
      FunctionExpression<Function1<T, Enumerable<TResult>>> selector) {
    return factory.selectMany(getThis(), selector);
  }

  @Override public <TResult> Queryable<TResult> selectManyN(
      FunctionExpression<Function2<T, Integer, Enumerable<TResult>>> selector) {
    return factory.selectManyN(getThis(), selector);
  }

  @Override public <TCollection, TResult> Queryable<TResult> selectMany(
      FunctionExpression<Function2<T, Integer, Enumerable<TCollection>>>
        collectionSelector,
      FunctionExpression<Function2<T, TCollection, TResult>> resultSelector) {
    return factory.selectMany(getThis(), collectionSelector, resultSelector);
  }

  @Override public <TCollection, TResult> Queryable<TResult> selectManyN(
      FunctionExpression<Function1<T, Enumerable<TCollection>>>
        collectionSelector,
      FunctionExpression<Function2<T, TCollection, TResult>> resultSelector) {
    return factory.selectManyN(getThis(), collectionSelector, resultSelector);
  }

  @Override public T single(FunctionExpression<Predicate1<T>> predicate) {
    return factory.single(getThis(), predicate);
  }

  @Override public T singleOrDefault(FunctionExpression<Predicate1<T>> predicate) {
    return factory.singleOrDefault(getThis(), predicate);
  }

  @Override public Queryable<T> skipWhile(FunctionExpression<Predicate1<T>> predicate) {
    return factory.skipWhile(getThis(), predicate);
  }

  @Override public Queryable<T> skipWhileN(
      FunctionExpression<Predicate2<T, Integer>> predicate) {
    return factory.skipWhileN(getThis(), predicate);
  }

  @Override public BigDecimal sumBigDecimal(
      FunctionExpression<BigDecimalFunction1<T>> selector) {
    return factory.sumBigDecimal(getThis(), selector);
  }

  @Override public BigDecimal sumNullableBigDecimal(
      FunctionExpression<NullableBigDecimalFunction1<T>> selector) {
    return factory.sumNullableBigDecimal(getThis(), selector);
  }

  @Override public double sumDouble(FunctionExpression<DoubleFunction1<T>> selector) {
    return factory.sumDouble(getThis(), selector);
  }

  @Override public Double sumNullableDouble(
      FunctionExpression<NullableDoubleFunction1<T>> selector) {
    return factory.sumNullableDouble(getThis(), selector);
  }

  @Override public int sumInteger(FunctionExpression<IntegerFunction1<T>> selector) {
    return factory.sumInteger(getThis(), selector);
  }

  @Override public Integer sumNullableInteger(
      FunctionExpression<NullableIntegerFunction1<T>> selector) {
    return factory.sumNullableInteger(getThis(), selector);
  }

  @Override public long sumLong(FunctionExpression<LongFunction1<T>> selector) {
    return factory.sumLong(getThis(), selector);
  }

  @Override public Long sumNullableLong(
      FunctionExpression<NullableLongFunction1<T>> selector) {
    return factory.sumNullableLong(getThis(), selector);
  }

  @Override public float sumFloat(FunctionExpression<FloatFunction1<T>> selector) {
    return factory.sumFloat(getThis(), selector);
  }

  @Override public Float sumNullableFloat(
      FunctionExpression<NullableFloatFunction1<T>> selector) {
    return factory.sumNullableFloat(getThis(), selector);
  }

  @Override public Queryable<T> takeWhile(FunctionExpression<Predicate1<T>> predicate) {
    return factory.takeWhile(getThis(), predicate);
  }

  @Override public Queryable<T> takeWhileN(
      FunctionExpression<Predicate2<T, Integer>> predicate) {
    return factory.takeWhileN(getThis(), predicate);
  }

  @Override public <TKey extends Comparable<TKey>> OrderedQueryable<T> thenBy(
      FunctionExpression<Function1<T, TKey>> keySelector) {
    return factory.thenBy(getThisOrderedQueryable(), keySelector);
  }

  @Override public <TKey> OrderedQueryable<T> thenBy(
      FunctionExpression<Function1<T, TKey>> keySelector,
      Comparator<TKey> comparator) {
    return factory.thenByDescending(getThisOrderedQueryable(), keySelector,
        comparator);
  }

  @Override public <TKey extends Comparable<TKey>> OrderedQueryable<T> thenByDescending(
      FunctionExpression<Function1<T, TKey>> keySelector) {
    return factory.thenByDescending(getThisOrderedQueryable(), keySelector);
  }

  @Override public <TKey> OrderedQueryable<T> thenByDescending(
      FunctionExpression<Function1<T, TKey>> keySelector,
      Comparator<TKey> comparator) {
    return factory.thenBy(getThisOrderedQueryable(), keySelector, comparator);
  }

  @Override public Queryable<T> where(
      FunctionExpression<? extends Predicate1<T>> predicate) {
    return factory.where(getThis(), predicate);
  }

  @Override public Queryable<T> whereN(
      FunctionExpression<? extends Predicate2<T, Integer>> predicate) {
    return factory.whereN(getThis(), predicate);
  }

  @Override public <T1, TResult> Queryable<TResult> zip(Enumerable<T1> source1,
      FunctionExpression<Function2<T, T1, TResult>> resultSelector) {
    return factory.zip(getThis(), source1, resultSelector);
  }
}
