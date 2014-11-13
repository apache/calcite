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
    this(QueryableRecorder.<T>instance());
  }

  /**
   * Creates a DefaultQueryable using a particular factory.
   */
  protected DefaultQueryable(QueryableFactory<T> factory) {
    this.factory = factory;
  }

  // override return type
  @Override
  protected Queryable<T> getThis() {
    return this;
  }

  protected OrderedQueryable<T> getThisOrderedQueryable() {
    return this;
  }

  @Override
  public Enumerable<T> asEnumerable() {
    return new AbstractEnumerable<T>() {
      public Enumerator<T> enumerator() {
        return DefaultQueryable.this.enumerator();
      }
    };
  }

  // Disambiguate

  @Override
  public Queryable<T> union(Enumerable<T> source1) {
    return factory.union(getThis(), source1);
  }

  @Override
  public Queryable<T> union(Enumerable<T> source1,
      EqualityComparer<T> comparer) {
    return factory.union(getThis(), source1, comparer);
  }

  @Override
  public Queryable<T> intersect(Enumerable<T> source1) {
    return factory.intersect(getThis(), source1);
  }

  @Override
  public Queryable<T> intersect(Enumerable<T> source1,
      EqualityComparer<T> comparer) {
    return factory.intersect(getThis(), source1, comparer);
  }

  @Override
  public Queryable<T> except(Enumerable<T> enumerable1,
      EqualityComparer<T> comparer) {
    return factory.except(getThis(), enumerable1, comparer);
  }

  @Override
  public Queryable<T> except(Enumerable<T> enumerable1) {
    return factory.except(getThis(), enumerable1);
  }

  @Override
  public Queryable<T> take(int count) {
    return factory.take(getThis(), count);
  }

  @Override
  public Queryable<T> skip(int count) {
    return factory.skip(getThis(), count);
  }

  @Override
  public Queryable<T> reverse() {
    return factory.reverse(getThis());
  }

  @Override
  public Queryable<T> distinct() {
    return factory.distinct(getThis());
  }

  @Override
  public Queryable<T> distinct(EqualityComparer<T> comparer) {
    return factory.distinct(getThis(), comparer);
  }

  @Override
  public <TResult> Queryable<TResult> ofType(Class<TResult> clazz) {
    return factory.ofType(getThis(), clazz);
  }

  @Override
  public Queryable<T> defaultIfEmpty() {
    return factory.defaultIfEmpty(getThis());
  }

  @Override
  public Queryable<T> asQueryable() {
    return this;
  }

  @Override
  public <T2> Queryable<T2> cast(Class<T2> clazz) {
    return factory.cast(getThis(), clazz);
  }

  // End disambiguate

  public T aggregate(FunctionExpression<Function2<T, T, T>> selector) {
    return factory.aggregate(getThis(), selector);
  }

  public <TAccumulate> TAccumulate aggregate(TAccumulate seed,
      FunctionExpression<Function2<TAccumulate, T, TAccumulate>> selector) {
    return factory.aggregate(getThis(), seed, selector);
  }

  public <TAccumulate, TResult> TResult aggregate(TAccumulate seed,
      FunctionExpression<Function2<TAccumulate, T, TAccumulate>> func,
      FunctionExpression<Function1<TAccumulate, TResult>> selector) {
    return factory.aggregate(getThis(), seed, func, selector);
  }

  public boolean all(FunctionExpression<Predicate1<T>> predicate) {
    return factory.all(getThis(), predicate);
  }

  public boolean any(FunctionExpression<Predicate1<T>> predicate) {
    return factory.any(getThis(), predicate);
  }

  public BigDecimal averageBigDecimal(
      FunctionExpression<BigDecimalFunction1<T>> selector) {
    return factory.averageBigDecimal(getThis(), selector);
  }

  public BigDecimal averageNullableBigDecimal(
      FunctionExpression<NullableBigDecimalFunction1<T>> selector) {
    return factory.averageNullableBigDecimal(getThis(), selector);
  }

  public double averageDouble(FunctionExpression<DoubleFunction1<T>> selector) {
    return factory.averageDouble(getThis(), selector);
  }

  public Double averageNullableDouble(
      FunctionExpression<NullableDoubleFunction1<T>> selector) {
    return factory.averageNullableDouble(getThis(), selector);
  }

  public int averageInteger(FunctionExpression<IntegerFunction1<T>> selector) {
    return factory.averageInteger(getThis(), selector);
  }

  public Integer averageNullableInteger(
      FunctionExpression<NullableIntegerFunction1<T>> selector) {
    return factory.averageNullableInteger(getThis(), selector);
  }

  public float averageFloat(FunctionExpression<FloatFunction1<T>> selector) {
    return factory.averageFloat(getThis(), selector);
  }

  public Float averageNullableFloat(
      FunctionExpression<NullableFloatFunction1<T>> selector) {
    return factory.averageNullableFloat(getThis(), selector);
  }

  public long averageLong(FunctionExpression<LongFunction1<T>> selector) {
    return factory.averageLong(getThis(), selector);
  }

  public Long averageNullableLong(
      FunctionExpression<NullableLongFunction1<T>> selector) {
    return factory.averageNullableLong(getThis(), selector);
  }

  public Queryable<T> concat(Enumerable<T> source2) {
    return factory.concat(getThis(), source2);
  }

  public int count(FunctionExpression<Predicate1<T>> func) {
    return factory.count(getThis(), func);
  }

  public T first(FunctionExpression<Predicate1<T>> predicate) {
    return factory.first(getThis(), predicate);
  }

  public T firstOrDefault(FunctionExpression<Predicate1<T>> predicate) {
    return factory.firstOrDefault(getThis(), predicate);
  }

  public <TKey> Queryable<Grouping<TKey, T>> groupBy(
      FunctionExpression<Function1<T, TKey>> keySelector) {
    return factory.groupBy(getThis(), keySelector);
  }

  public <TKey> Queryable<Grouping<TKey, T>> groupBy(
      FunctionExpression<Function1<T, TKey>> keySelector,
      EqualityComparer<TKey> comparer) {
    return factory.groupBy(getThis(), keySelector, comparer);
  }

  public <TKey, TElement> Queryable<Grouping<TKey, TElement>> groupBy(
      FunctionExpression<Function1<T, TKey>> keySelector,
      FunctionExpression<Function1<T, TElement>> elementSelector) {
    return factory.groupBy(getThis(), keySelector, elementSelector);
  }

  public <TKey, TResult> Queryable<Grouping<TKey, TResult>> groupByK(
      FunctionExpression<Function1<T, TKey>> keySelector,
      FunctionExpression<Function2<TKey, Enumerable<T>, TResult>>
          elementSelector) {
    return factory.groupByK(getThis(), keySelector, elementSelector);
  }

  public <TKey, TElement> Queryable<Grouping<TKey, TElement>> groupBy(
      FunctionExpression<Function1<T, TKey>> keySelector,
      FunctionExpression<Function1<T, TElement>> elementSelector,
      EqualityComparer<TKey> comparer) {
    return factory.groupBy(getThis(), keySelector, elementSelector, comparer);
  }

  public <TKey, TResult> Queryable<TResult> groupByK(
      FunctionExpression<Function1<T, TKey>> keySelector,
      FunctionExpression<Function2<TKey, Enumerable<T>, TResult>>
        elementSelector,
      EqualityComparer<TKey> comparer) {
    return factory.groupByK(getThis(), keySelector, elementSelector, comparer);
  }

  public <TKey, TElement, TResult> Queryable<TResult> groupBy(
      FunctionExpression<Function1<T, TKey>> keySelector,
      FunctionExpression<Function1<T, TElement>> elementSelector,
      FunctionExpression<Function2<TKey, Enumerable<TElement>, TResult>>
        resultSelector) {
    return factory.groupBy(getThis(), keySelector, elementSelector,
        resultSelector);
  }

  public <TKey, TElement, TResult> Queryable<TResult> groupBy(
      FunctionExpression<Function1<T, TKey>> keySelector,
      FunctionExpression<Function1<T, TElement>> elementSelector,
      FunctionExpression<Function2<TKey, Enumerable<TElement>, TResult>>
        resultSelector,
      EqualityComparer<TKey> comparer) {
    return factory.groupBy(getThis(), keySelector, elementSelector,
        resultSelector, comparer);
  }

  public <TInner, TKey, TResult> Queryable<TResult> groupJoin(
      Enumerable<TInner> inner,
      FunctionExpression<Function1<T, TKey>> outerKeySelector,
      FunctionExpression<Function1<TInner, TKey>> innerKeySelector,
      FunctionExpression<Function2<T, Enumerable<TInner>, TResult>>
        resultSelector) {
    return factory.groupJoin(getThis(), inner, outerKeySelector,
        innerKeySelector, resultSelector);
  }

  public <TInner, TKey, TResult> Queryable<TResult> groupJoin(
      Enumerable<TInner> inner,
      FunctionExpression<Function1<T, TKey>> outerKeySelector,
      FunctionExpression<Function1<TInner, TKey>> innerKeySelector,
      FunctionExpression<Function2<T, Enumerable<TInner>, TResult>>
        resultSelector,
      EqualityComparer<TKey> comparer) {
    return factory.groupJoin(getThis(), inner, outerKeySelector,
        innerKeySelector, resultSelector, comparer);
  }

  public <TInner, TKey, TResult> Queryable<TResult> join(
      Enumerable<TInner> inner,
      FunctionExpression<Function1<T, TKey>> outerKeySelector,
      FunctionExpression<Function1<TInner, TKey>> innerKeySelector,
      FunctionExpression<Function2<T, TInner, TResult>> resultSelector) {
    return factory.join(getThis(), inner, outerKeySelector, innerKeySelector,
        resultSelector);
  }

  public <TInner, TKey, TResult> Queryable<TResult> join(
      Enumerable<TInner> inner,
      FunctionExpression<Function1<T, TKey>> outerKeySelector,
      FunctionExpression<Function1<TInner, TKey>> innerKeySelector,
      FunctionExpression<Function2<T, TInner, TResult>> resultSelector,
      EqualityComparer<TKey> comparer) {
    return factory.join(getThis(), inner, outerKeySelector, innerKeySelector,
        resultSelector, comparer);
  }

  public T last(FunctionExpression<Predicate1<T>> predicate) {
    return factory.last(getThis(), predicate);
  }

  public T lastOrDefault(FunctionExpression<Predicate1<T>> predicate) {
    return factory.lastOrDefault(getThis(), predicate);
  }

  public long longCount(FunctionExpression<Predicate1<T>> predicate) {
    return factory.longCount(getThis(), predicate);
  }

  public <TResult extends Comparable<TResult>> TResult max(
      FunctionExpression<Function1<T, TResult>> selector) {
    return factory.max(getThis(), selector);
  }

  public <TResult extends Comparable<TResult>> TResult min(
      FunctionExpression<Function1<T, TResult>> selector) {
    return factory.min(getThis(), selector);
  }

  public <TKey extends Comparable> OrderedQueryable<T> orderBy(
      FunctionExpression<Function1<T, TKey>> keySelector) {
    return factory.orderBy(getThis(), keySelector);
  }

  public <TKey> OrderedQueryable<T> orderBy(
      FunctionExpression<Function1<T, TKey>> keySelector,
      Comparator<TKey> comparator) {
    return factory.orderBy(getThis(), keySelector, comparator);
  }

  public <TKey extends Comparable> OrderedQueryable<T> orderByDescending(
      FunctionExpression<Function1<T, TKey>> keySelector) {
    return factory.orderByDescending(getThis(), keySelector);
  }

  public <TKey> OrderedQueryable<T> orderByDescending(
      FunctionExpression<Function1<T, TKey>> keySelector,
      Comparator<TKey> comparator) {
    return factory.orderByDescending(getThis(), keySelector, comparator);
  }

  public <TResult> Queryable<TResult> select(
      FunctionExpression<Function1<T, TResult>> selector) {
    return factory.select(getThis(), selector);
  }

  public <TResult> Queryable<TResult> selectN(
      FunctionExpression<Function2<T, Integer, TResult>> selector) {
    return factory.selectN(getThis(), selector);
  }

  public <TResult> Queryable<TResult> selectMany(
      FunctionExpression<Function1<T, Enumerable<TResult>>> selector) {
    return factory.selectMany(getThis(), selector);
  }

  public <TResult> Queryable<TResult> selectManyN(
      FunctionExpression<Function2<T, Integer, Enumerable<TResult>>> selector) {
    return factory.selectManyN(getThis(), selector);
  }

  public <TCollection, TResult> Queryable<TResult> selectMany(
      FunctionExpression<Function2<T, Integer, Enumerable<TCollection>>>
        collectionSelector,
      FunctionExpression<Function2<T, TCollection, TResult>> resultSelector) {
    return factory.selectMany(getThis(), collectionSelector, resultSelector);
  }

  public <TCollection, TResult> Queryable<TResult> selectManyN(
      FunctionExpression<Function1<T, Enumerable<TCollection>>>
        collectionSelector,
      FunctionExpression<Function2<T, TCollection, TResult>> resultSelector) {
    return factory.selectManyN(getThis(), collectionSelector, resultSelector);
  }

  public T single(FunctionExpression<Predicate1<T>> predicate) {
    return factory.single(getThis(), predicate);
  }

  public T singleOrDefault(FunctionExpression<Predicate1<T>> predicate) {
    return factory.singleOrDefault(getThis(), predicate);
  }

  public Queryable<T> skipWhile(FunctionExpression<Predicate1<T>> predicate) {
    return factory.skipWhile(getThis(), predicate);
  }

  public Queryable<T> skipWhileN(
      FunctionExpression<Predicate2<T, Integer>> predicate) {
    return factory.skipWhileN(getThis(), predicate);
  }

  public BigDecimal sumBigDecimal(
      FunctionExpression<BigDecimalFunction1<T>> selector) {
    return factory.sumBigDecimal(getThis(), selector);
  }

  public BigDecimal sumNullableBigDecimal(
      FunctionExpression<NullableBigDecimalFunction1<T>> selector) {
    return factory.sumNullableBigDecimal(getThis(), selector);
  }

  public double sumDouble(FunctionExpression<DoubleFunction1<T>> selector) {
    return factory.sumDouble(getThis(), selector);
  }

  public Double sumNullableDouble(
      FunctionExpression<NullableDoubleFunction1<T>> selector) {
    return factory.sumNullableDouble(getThis(), selector);
  }

  public int sumInteger(FunctionExpression<IntegerFunction1<T>> selector) {
    return factory.sumInteger(getThis(), selector);
  }

  public Integer sumNullableInteger(
      FunctionExpression<NullableIntegerFunction1<T>> selector) {
    return factory.sumNullableInteger(getThis(), selector);
  }

  public long sumLong(FunctionExpression<LongFunction1<T>> selector) {
    return factory.sumLong(getThis(), selector);
  }

  public Long sumNullableLong(
      FunctionExpression<NullableLongFunction1<T>> selector) {
    return factory.sumNullableLong(getThis(), selector);
  }

  public float sumFloat(FunctionExpression<FloatFunction1<T>> selector) {
    return factory.sumFloat(getThis(), selector);
  }

  public Float sumNullableFloat(
      FunctionExpression<NullableFloatFunction1<T>> selector) {
    return factory.sumNullableFloat(getThis(), selector);
  }

  public Queryable<T> takeWhile(FunctionExpression<Predicate1<T>> predicate) {
    return factory.takeWhile(getThis(), predicate);
  }

  public Queryable<T> takeWhileN(
      FunctionExpression<Predicate2<T, Integer>> predicate) {
    return factory.takeWhileN(getThis(), predicate);
  }

  public <TKey extends Comparable<TKey>> OrderedQueryable<T> thenBy(
      FunctionExpression<Function1<T, TKey>> keySelector) {
    return factory.thenBy(getThisOrderedQueryable(), keySelector);
  }

  public <TKey> OrderedQueryable<T> thenBy(
      FunctionExpression<Function1<T, TKey>> keySelector,
      Comparator<TKey> comparator) {
    return factory.thenByDescending(getThisOrderedQueryable(), keySelector,
        comparator);
  }

  public <TKey extends Comparable<TKey>> OrderedQueryable<T> thenByDescending(
      FunctionExpression<Function1<T, TKey>> keySelector) {
    return factory.thenByDescending(getThisOrderedQueryable(), keySelector);
  }

  public <TKey> OrderedQueryable<T> thenByDescending(
      FunctionExpression<Function1<T, TKey>> keySelector,
      Comparator<TKey> comparator) {
    return factory.thenBy(getThisOrderedQueryable(), keySelector, comparator);
  }

  public Queryable<T> where(
      FunctionExpression<? extends Predicate1<T>> predicate) {
    return factory.where(getThis(), predicate);
  }

  public Queryable<T> whereN(
      FunctionExpression<? extends Predicate2<T, Integer>> predicate) {
    return factory.whereN(getThis(), predicate);
  }

  public <T1, TResult> Queryable<TResult> zip(Enumerable<T1> source1,
      FunctionExpression<Function2<T, T1, TResult>> resultSelector) {
    return factory.zip(getThis(), source1, resultSelector);
  }
}

// End DefaultQueryable.java
