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
import net.hydromatic.linq4j.expressions.FunctionExpression;
import net.hydromatic.linq4j.function.*;

import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.Comparator;
import java.util.Iterator;

/**
 * Implementation of {@link Queryable} by a {@link Enumerable}.
 *
 * @param <T> Element type
 */
class EnumerableQueryable<T> extends DefaultEnumerable<T>
    implements Queryable<T> {
  private final QueryProvider provider;
  private final Class<T> elementType;
  private final Enumerable<T> enumerable;
  private final Expression expression;

  EnumerableQueryable(QueryProvider provider, Class<T> elementType,
      Expression expression, Enumerable<T> enumerable) {
    this.enumerable = enumerable;
    this.elementType = elementType;
    this.provider = provider;
    this.expression = expression;
  }

  @Override
  protected Enumerable<T> getThis() {
    return enumerable;
  }

  /**
   * Returns the target queryable. Usually this.
   *
   * @return Target queryable
   */
  protected Queryable<T> queryable() {
    return this;
  }

  public Iterator<T> iterator() {
    return Linq4j.enumeratorIterator(enumerator());
  }

  public Enumerator<T> enumerator() {
    return enumerable.enumerator();
  }

  // Disambiguate

  public Queryable<T> union(Enumerable<T> source1) {
    return EnumerableDefaults.union(getThis(), source1).asQueryable();
  }

  public Queryable<T> union(Enumerable<T> source1,
      EqualityComparer<T> comparer) {
    return EnumerableDefaults.union(getThis(), source1, comparer).asQueryable();
  }

  @Override
  public Queryable<T> intersect(Enumerable<T> source1) {
    return EnumerableDefaults.intersect(getThis(), source1).asQueryable();
  }

  @Override
  public Queryable<T> intersect(Enumerable<T> source1,
      EqualityComparer<T> comparer) {
    return EnumerableDefaults.intersect(getThis(), source1, comparer)
        .asQueryable();
  }

  @Override
  public Queryable<T> except(Enumerable<T> enumerable1,
      EqualityComparer<T> comparer) {
    return EnumerableDefaults.except(getThis(), enumerable1, comparer)
        .asQueryable();
  }

  @Override
  public Queryable<T> except(Enumerable<T> enumerable1) {
    return EnumerableDefaults.except(getThis(), enumerable1).asQueryable();
  }

  public Queryable<T> take(int count) {
    return EnumerableDefaults.take(getThis(), count).asQueryable();
  }

  public Queryable<T> skip(int count) {
    return EnumerableDefaults.skip(getThis(), count).asQueryable();
  }

  public Queryable<T> reverse() {
    return EnumerableDefaults.reverse(getThis()).asQueryable();
  }

  @Override
  public Queryable<T> distinct() {
    return EnumerableDefaults.distinct(getThis()).asQueryable();
  }

  @Override
  public Queryable<T> distinct(EqualityComparer<T> comparer) {
    return EnumerableDefaults.distinct(getThis(), comparer).asQueryable();
  }

  @Override
  public <TResult> Queryable<TResult> ofType(Class<TResult> clazz) {
    return EnumerableDefaults.ofType(getThis(), clazz).asQueryable();
  }

  @Override
  public Queryable<T> defaultIfEmpty() {
    return EnumerableDefaults.defaultIfEmpty(getThis()).asQueryable();
  }

  public <T2> Queryable<T2> cast(Class<T2> clazz) {
    return EnumerableDefaults.cast(getThis(), clazz).asQueryable();
  }

  // Queryable methods

  public Type getElementType() {
    return elementType;
  }

  public Expression getExpression() {
    return expression;
  }

  public QueryProvider getProvider() {
    return provider;
  }

  // .............

  public T aggregate(FunctionExpression<Function2<T, T, T>> selector) {
    return EnumerableDefaults.aggregate(getThis(), selector.getFunction());
  }

  public <TAccumulate> TAccumulate aggregate(TAccumulate seed,
      FunctionExpression<Function2<TAccumulate, T, TAccumulate>> selector) {
    return EnumerableDefaults.aggregate(getThis(), seed,
        selector.getFunction());
  }

  public <TAccumulate, TResult> TResult aggregate(TAccumulate seed,
      FunctionExpression<Function2<TAccumulate, T, TAccumulate>> func,
      FunctionExpression<Function1<TAccumulate, TResult>> selector) {
    return EnumerableDefaults.aggregate(getThis(), seed, func.getFunction(),
        selector.getFunction());
  }

  public boolean all(FunctionExpression<Predicate1<T>> predicate) {
    return EnumerableDefaults.all(getThis(), predicate.getFunction());
  }

  public boolean any(FunctionExpression<Predicate1<T>> predicate) {
    return EnumerableDefaults.any(getThis(), predicate.getFunction());
  }

  public BigDecimal averageBigDecimal(
      FunctionExpression<BigDecimalFunction1<T>> selector) {
    return EnumerableDefaults.average(getThis(), selector.getFunction());
  }

  public BigDecimal averageNullableBigDecimal(
      FunctionExpression<NullableBigDecimalFunction1<T>> selector) {
    return EnumerableDefaults.average(getThis(), selector.getFunction());
  }

  public double averageDouble(FunctionExpression<DoubleFunction1<T>> selector) {
    return EnumerableDefaults.average(getThis(), selector.getFunction());
  }

  public Double averageNullableDouble(
      FunctionExpression<NullableDoubleFunction1<T>> selector) {
    return EnumerableDefaults.average(getThis(), selector.getFunction());
  }

  public int averageInteger(FunctionExpression<IntegerFunction1<T>> selector) {
    return EnumerableDefaults.average(getThis(), selector.getFunction());
  }

  public Integer averageNullableInteger(
      FunctionExpression<NullableIntegerFunction1<T>> selector) {
    return EnumerableDefaults.average(getThis(), selector.getFunction());
  }

  public float averageFloat(FunctionExpression<FloatFunction1<T>> selector) {
    return EnumerableDefaults.average(getThis(), selector.getFunction());
  }

  public Float averageNullableFloat(
      FunctionExpression<NullableFloatFunction1<T>> selector) {
    return EnumerableDefaults.average(getThis(), selector.getFunction());
  }

  public long averageLong(FunctionExpression<LongFunction1<T>> selector) {
    return EnumerableDefaults.average(getThis(), selector.getFunction());
  }

  public Long averageNullableLong(
      FunctionExpression<NullableLongFunction1<T>> selector) {
    return EnumerableDefaults.average(getThis(), selector.getFunction());
  }

  public Queryable<T> concat(Enumerable<T> source2) {
    return EnumerableDefaults.concat(getThis(), source2).asQueryable();
  }

  public int count(FunctionExpression<Predicate1<T>> predicate) {
    return EnumerableDefaults.count(getThis(), predicate.getFunction());
  }

  public T first(FunctionExpression<Predicate1<T>> predicate) {
    return EnumerableDefaults.first(getThis(), predicate.getFunction());
  }

  public T firstOrDefault(FunctionExpression<Predicate1<T>> predicate) {
    return EnumerableDefaults.firstOrDefault(getThis(),
        predicate.getFunction());
  }

  public <TKey> Queryable<Grouping<TKey, T>> groupBy(
      FunctionExpression<Function1<T, TKey>> keySelector) {
    return EnumerableDefaults.groupBy(getThis(), keySelector.getFunction())
        .asQueryable();
  }

  public <TKey> Queryable<Grouping<TKey, T>> groupBy(
      FunctionExpression<Function1<T, TKey>> keySelector,
      EqualityComparer<TKey> comparer) {
    return EnumerableDefaults.groupBy(getThis(), keySelector.getFunction(),
        comparer).asQueryable();
  }

  public <TKey, TElement> Queryable<Grouping<TKey, TElement>> groupBy(
      FunctionExpression<Function1<T, TKey>> keySelector,
      FunctionExpression<Function1<T, TElement>> elementSelector) {
    return EnumerableDefaults.groupBy(getThis(), keySelector.getFunction(),
        elementSelector.getFunction()).asQueryable();
  }

  public <TKey, TResult> Queryable<Grouping<TKey, TResult>> groupByK(
      FunctionExpression<Function1<T, TKey>> keySelector,
      FunctionExpression<Function2<TKey, Enumerable<T>, TResult>>
        elementSelector) {
    return EnumerableDefaults.groupBy(getThis(), keySelector.getFunction(),
        elementSelector.getFunction()).asQueryable();
  }

  public <TKey, TElement> Queryable<Grouping<TKey, TElement>> groupBy(
      FunctionExpression<Function1<T, TKey>> keySelector,
      FunctionExpression<Function1<T, TElement>> elementSelector,
      EqualityComparer<TKey> comparer) {
    return EnumerableDefaults.groupBy(getThis(), keySelector.getFunction(),
        elementSelector.getFunction(), comparer).asQueryable();
  }

  public <TKey, TResult> Queryable<TResult> groupByK(
      FunctionExpression<Function1<T, TKey>> keySelector,
      FunctionExpression<Function2<TKey, Enumerable<T>, TResult>>
        elementSelector,
      EqualityComparer<TKey> comparer) {
    return EnumerableDefaults.groupBy(getThis(), keySelector.getFunction(),
        elementSelector.getFunction(), comparer).asQueryable();
  }

  public <TKey, TElement, TResult> Queryable<TResult> groupBy(
      FunctionExpression<Function1<T, TKey>> keySelector,
      FunctionExpression<Function1<T, TElement>> elementSelector,
      FunctionExpression<Function2<TKey, Enumerable<TElement>, TResult>>
        resultSelector) {
    return EnumerableDefaults.groupBy(getThis(), keySelector.getFunction(),
        elementSelector.getFunction(), resultSelector.getFunction())
        .asQueryable();
  }

  public <TKey, TElement, TResult> Queryable<TResult> groupBy(
      FunctionExpression<Function1<T, TKey>> keySelector,
      FunctionExpression<Function1<T, TElement>> elementSelector,
      FunctionExpression<Function2<TKey, Enumerable<TElement>, TResult>>
        resultSelector,
      EqualityComparer<TKey> comparer) {
    return EnumerableDefaults.groupBy(getThis(), keySelector.getFunction(),
        elementSelector.getFunction(), resultSelector.getFunction(), comparer)
        .asQueryable();
  }

  public <TInner, TKey, TResult> Queryable<TResult> groupJoin(
      Enumerable<TInner> inner,
      FunctionExpression<Function1<T, TKey>> outerKeySelector,
      FunctionExpression<Function1<TInner, TKey>> innerKeySelector,
      FunctionExpression<Function2<T, Enumerable<TInner>, TResult>>
        resultSelector) {
    return EnumerableDefaults.groupJoin(getThis(), inner,
        outerKeySelector.getFunction(), innerKeySelector.getFunction(),
        resultSelector.getFunction()).asQueryable();
  }

  public <TInner, TKey, TResult> Queryable<TResult> groupJoin(
      Enumerable<TInner> inner,
      FunctionExpression<Function1<T, TKey>> outerKeySelector,
      FunctionExpression<Function1<TInner, TKey>> innerKeySelector,
      FunctionExpression<Function2<T, Enumerable<TInner>, TResult>>
        resultSelector,
      EqualityComparer<TKey> comparer) {
    return EnumerableDefaults.groupJoin(getThis(), inner,
        outerKeySelector.getFunction(), innerKeySelector.getFunction(),
        resultSelector.getFunction(), comparer).asQueryable();
  }

  public <TInner, TKey, TResult> Queryable<TResult> join(
      Enumerable<TInner> inner,
      FunctionExpression<Function1<T, TKey>> outerKeySelector,
      FunctionExpression<Function1<TInner, TKey>> innerKeySelector,
      FunctionExpression<Function2<T, TInner, TResult>> resultSelector) {
    return EnumerableDefaults.join(getThis(), inner,
        outerKeySelector.getFunction(), innerKeySelector.getFunction(),
        resultSelector.getFunction()).asQueryable();
  }

  public <TInner, TKey, TResult> Queryable<TResult> join(
      Enumerable<TInner> inner,
      FunctionExpression<Function1<T, TKey>> outerKeySelector,
      FunctionExpression<Function1<TInner, TKey>> innerKeySelector,
      FunctionExpression<Function2<T, TInner, TResult>> resultSelector,
      EqualityComparer<TKey> comparer) {
    return EnumerableDefaults.join(getThis(), inner,
        outerKeySelector.getFunction(), innerKeySelector.getFunction(),
        resultSelector.getFunction(), comparer).asQueryable();
  }

  public T last(FunctionExpression<Predicate1<T>> predicate) {
    return EnumerableDefaults.last(getThis(), predicate.getFunction());
  }

  public T lastOrDefault(FunctionExpression<Predicate1<T>> predicate) {
    return EnumerableDefaults.lastOrDefault(getThis(), predicate.getFunction());
  }

  public long longCount(FunctionExpression<Predicate1<T>> predicate) {
    return EnumerableDefaults.longCount(getThis(), predicate.getFunction());
  }

  public <TResult extends Comparable<TResult>> TResult max(
      FunctionExpression<Function1<T, TResult>> selector) {
    return EnumerableDefaults.max(getThis(), selector.getFunction());
  }

  public <TResult extends Comparable<TResult>> TResult min(
      FunctionExpression<Function1<T, TResult>> selector) {
    return EnumerableDefaults.min(getThis(), selector.getFunction());
  }

  public <TKey extends Comparable> OrderedQueryable<T> orderBy(
      FunctionExpression<Function1<T, TKey>> keySelector) {
    return EnumerableDefaults.asOrderedQueryable(EnumerableDefaults.orderBy(
        getThis(), keySelector.getFunction()));
  }

  public <TKey> OrderedQueryable<T> orderBy(
      FunctionExpression<Function1<T, TKey>> keySelector,
      Comparator<TKey> comparator) {
    return EnumerableDefaults.asOrderedQueryable(EnumerableDefaults.orderBy(
        getThis(), keySelector.getFunction(), comparator));
  }

  public <TKey extends Comparable> OrderedQueryable<T> orderByDescending(
      FunctionExpression<Function1<T, TKey>> keySelector) {
    return EnumerableDefaults.asOrderedQueryable(
        EnumerableDefaults.orderByDescending(getThis(),
            keySelector.getFunction()));
  }

  public <TKey> OrderedQueryable<T> orderByDescending(
      FunctionExpression<Function1<T, TKey>> keySelector,
      Comparator<TKey> comparator) {
    return EnumerableDefaults.asOrderedQueryable(
        EnumerableDefaults.orderByDescending(getThis(),
            keySelector.getFunction(), comparator));
  }

  public <TResult> Queryable<TResult> select(
      FunctionExpression<Function1<T, TResult>> selector) {
    return EnumerableDefaults.select(getThis(), selector.getFunction())
        .asQueryable();
  }

  public <TResult> Queryable<TResult> selectN(
      FunctionExpression<Function2<T, Integer, TResult>> selector) {
    return EnumerableDefaults.select(getThis(), selector.getFunction())
        .asQueryable();
  }

  public <TResult> Queryable<TResult> selectMany(
      FunctionExpression<Function1<T, Enumerable<TResult>>> selector) {
    return EnumerableDefaults.selectMany(getThis(), selector.getFunction())
        .asQueryable();
  }

  public <TResult> Queryable<TResult> selectManyN(
      FunctionExpression<Function2<T, Integer, Enumerable<TResult>>> selector) {
    return EnumerableDefaults.selectMany(getThis(), selector.getFunction())
        .asQueryable();
  }

  public <TCollection, TResult> Queryable<TResult> selectMany(
      FunctionExpression<Function2<T, Integer, Enumerable<TCollection>>>
        collectionSelector,
      FunctionExpression<Function2<T, TCollection, TResult>> resultSelector) {
    return EnumerableDefaults.selectMany(getThis(),
        collectionSelector.getFunction(), resultSelector.getFunction())
        .asQueryable();
  }

  public <TCollection, TResult> Queryable<TResult> selectManyN(
      FunctionExpression<Function1<T, Enumerable<TCollection>>>
        collectionSelector,
      FunctionExpression<Function2<T, TCollection, TResult>> resultSelector) {
    return EnumerableDefaults.selectMany(getThis(),
        collectionSelector.getFunction(), resultSelector.getFunction())
        .asQueryable();
  }

  public T single(FunctionExpression<Predicate1<T>> predicate) {
    return EnumerableDefaults.single(getThis(), predicate.getFunction());
  }

  public T singleOrDefault(FunctionExpression<Predicate1<T>> predicate) {
    return EnumerableDefaults.singleOrDefault(getThis(),
        predicate.getFunction());
  }

  public Queryable<T> skipWhile(FunctionExpression<Predicate1<T>> predicate) {
    return EnumerableDefaults.skipWhile(getThis(), predicate.getFunction())
        .asQueryable();
  }

  public Queryable<T> skipWhileN(
      FunctionExpression<Predicate2<T, Integer>> predicate) {
    return EnumerableDefaults.skipWhile(getThis(), predicate.getFunction())
        .asQueryable();
  }

  public BigDecimal sumBigDecimal(
      FunctionExpression<BigDecimalFunction1<T>> selector) {
    return EnumerableDefaults.sum(getThis(), selector.getFunction());
  }

  public BigDecimal sumNullableBigDecimal(
      FunctionExpression<NullableBigDecimalFunction1<T>> selector) {
    return EnumerableDefaults.sum(getThis(), selector.getFunction());
  }

  public double sumDouble(FunctionExpression<DoubleFunction1<T>> selector) {
    return EnumerableDefaults.sum(getThis(), selector.getFunction());
  }

  public Double sumNullableDouble(
      FunctionExpression<NullableDoubleFunction1<T>> selector) {
    return EnumerableDefaults.sum(getThis(), selector.getFunction());
  }

  public int sumInteger(FunctionExpression<IntegerFunction1<T>> selector) {
    return EnumerableDefaults.sum(getThis(), selector.getFunction());
  }

  public Integer sumNullableInteger(
      FunctionExpression<NullableIntegerFunction1<T>> selector) {
    return EnumerableDefaults.sum(getThis(), selector.getFunction());
  }

  public long sumLong(FunctionExpression<LongFunction1<T>> selector) {
    return EnumerableDefaults.sum(getThis(), selector.getFunction());
  }

  public Long sumNullableLong(
      FunctionExpression<NullableLongFunction1<T>> selector) {
    return EnumerableDefaults.sum(getThis(), selector.getFunction());
  }

  public float sumFloat(FunctionExpression<FloatFunction1<T>> selector) {
    return EnumerableDefaults.sum(getThis(), selector.getFunction());
  }

  public Float sumNullableFloat(
      FunctionExpression<NullableFloatFunction1<T>> selector) {
    return EnumerableDefaults.sum(getThis(), selector.getFunction());
  }

  public Queryable<T> takeWhile(FunctionExpression<Predicate1<T>> predicate) {
    return EnumerableDefaults.takeWhile(getThis(), predicate.getFunction())
        .asQueryable();
  }

  public Queryable<T> takeWhileN(
      FunctionExpression<Predicate2<T, Integer>> predicate) {
    return EnumerableDefaults.takeWhile(getThis(), predicate.getFunction())
        .asQueryable();
  }

  public Queryable<T> where(
      FunctionExpression<? extends Predicate1<T>> predicate) {
    return EnumerableDefaults.where(getThis(), predicate.getFunction())
        .asQueryable();
  }

  public Queryable<T> whereN(
      FunctionExpression<? extends Predicate2<T, Integer>> predicate) {
    return EnumerableDefaults.where(getThis(), predicate.getFunction())
        .asQueryable();
  }

  public <T1, TResult> Queryable<TResult> zip(Enumerable<T1> source1,
      FunctionExpression<Function2<T, T1, TResult>> resultSelector) {
    return EnumerableDefaults.zip(getThis(), source1,
        resultSelector.getFunction()).asQueryable();
  }

  public T aggregate(Function2<T, T, T> func) {
    return EnumerableDefaults.aggregate(getThis(), func);
  }


  public <TAccumulate, TResult> TResult aggregate(TAccumulate seed,
      Function2<TAccumulate, T, TAccumulate> func,
      Function1<TAccumulate, TResult> selector) {
    return EnumerableDefaults.aggregate(getThis(), seed, func, selector);
  }
}

// End EnumerableQueryable.java
