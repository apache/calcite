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
 */
abstract class DefaultQueryable<T>
    extends DefaultEnumerable<T>
    implements Queryable<T>
{
    protected Queryable<T> getThis() {
        return this;
    }

    // Disambiguate

    public Queryable<T> union(Enumerable<T> source1) {
        return Extensions.union(getThis(), source1);
    }

    public Queryable<T> union(
        Enumerable<T> source1, EqualityComparer<T> comparer)
    {
        return Extensions.union(getThis(), source1, comparer);
    }

    public Queryable<T> intersect(Enumerable<T> source1) {
        return Extensions.intersect(getThis(), source1);
    }

    public Queryable<T> intersect(
        Enumerable<T> source1, EqualityComparer<T> comparer)
    {
        return Extensions.intersect(getThis(), source1, comparer);
    }

    @Override
    public Queryable<T> except(
        Enumerable<T> enumerable1, EqualityComparer comparer)
    {
        return Extensions.except(getThis(), enumerable1, comparer);
    }

    @Override
    public Queryable<T> except(Enumerable<T> enumerable1) {
        return Extensions.except(getThis(), enumerable1);
    }

    public Queryable<T> take(int count) {
        return Extensions.take(getThis(), count);
    }

    public Queryable<T> skip(int count) {
        return Extensions.skip(getThis(), count);
    }

    public Queryable<T> reverse() {
        return Extensions.reverse(getThis());
    }

    @Override
    public Queryable<T> distinct() {
        return Extensions.distinct(getThis());
    }

    @Override
    public Queryable<T> distinct(EqualityComparer comparer) {
        return Extensions.distinct(getThis(), comparer);
    }

    @Override
    public <TResult> Queryable<TResult> ofType(Class<TResult> clazz) {
        return Extensions.ofType(getThis(), clazz);
    }

    @Override
    public Queryable<T> defaultIfEmpty() {
        return Extensions.defaultIfEmpty(getThis());
    }

    @Override
    public Queryable<T> asQueryable() {
        return Extensions.asQueryable(getThis());
    }

    public <T2> Enumerable<T2> cast(Class<T2> clazz) {
        return Extensions.cast(getThis(), clazz);
    }

    // End disambiguate

    public T aggregate(FunctionExpression<Function2<T, T, T>> selector) {
        return Extensions.aggregate(getThis(), selector);
    }

    public <TAccumulate> T aggregate(
        TAccumulate seed,
        FunctionExpression<Function2<TAccumulate, T, TAccumulate>> selector)
    {
        return Extensions.aggregate(getThis(), seed, selector);
    }

    public <TAccumulate, TResult> TResult aggregate(
        TAccumulate seed,
        FunctionExpression<Function2<TAccumulate, T, TAccumulate>> func,
        FunctionExpression<Function1<TAccumulate, TResult>> selector)
    {
        return Extensions.aggregate(getThis(), seed, func, selector);
    }

    public boolean all(FunctionExpression<Predicate1<T>> predicate) {
        return Extensions.all(getThis(), predicate);
    }

    public boolean any(FunctionExpression<Predicate1<T>> predicate) {
        return Extensions.any(getThis(), predicate);
    }

    public BigDecimal averageBigDecimal(
        FunctionExpression<BigDecimalFunction1<T>> selector)
    {
        return Extensions.averageBigDecimal(getThis(), selector);
    }

    public BigDecimal averageNullableBigDecimal(
        FunctionExpression<NullableBigDecimalFunction1<T>> selector)
    {
        return Extensions.averageNullableBigDecimal(getThis(), selector);
    }

    public double averageDouble(
        FunctionExpression<DoubleFunction1<T>> selector)
    {
        return Extensions.averageDouble(getThis(), selector);
    }

    public Double averageNullableDouble(
        FunctionExpression<NullableDoubleFunction1<T>> selector)
    {
        return Extensions.averageNullableDouble(getThis(), selector);
    }

    public int averageInteger(
        FunctionExpression<IntegerFunction1<T>> selector)
    {
        return Extensions.averageInteger(getThis(), selector);
    }

    public Integer averageNullableInteger(
        FunctionExpression<NullableIntegerFunction1<T>> selector)
    {
        return Extensions.averageNullableInteger(getThis(), selector);
    }

    public float averageFloat(
        FunctionExpression<FloatFunction1<T>> selector)
    {
        return Extensions.averageFloat(getThis(), selector);
    }

    public Float averageNullableFloat(
        FunctionExpression<NullableFloatFunction1<T>> selector)
    {
        return Extensions.averageNullableFloat(getThis(), selector);
    }

    public long averageLong(FunctionExpression<LongFunction1<T>> selector) {
        return Extensions.averageLong(getThis(), selector);
    }

    public Long averageNullableLong(
        FunctionExpression<NullableLongFunction1<T>> selector)
    {
        return Extensions.averageNullableLong(getThis(), selector);
    }

    public Queryable<T> concat(Queryable<T> queryable1) {
        return Extensions.concat(getThis(), queryable1);
    }

    public int count(FunctionExpression<Predicate1<T>> func) {
        return Extensions.count(getThis(), func);
    }

    public T first(FunctionExpression<Predicate1<T>> predicate) {
        return Extensions.first(getThis(), predicate);
    }

    public T firstOrDefault(FunctionExpression<Predicate1<T>> predicate) {
        return Extensions.firstOrDefault(getThis(), predicate);
    }

    public <TKey> Queryable<Grouping<TKey, T>> groupBy(
        FunctionExpression<Function1<T, TKey>> keySelector)
    {
        return Extensions.groupBy(getThis(), keySelector);
    }

    public <TKey> Queryable<Grouping<TKey, T>> groupBy(
        FunctionExpression<Function1<T, TKey>> keySelector,
        EqualityComparer comparer)
    {
        return Extensions.groupBy(getThis(), keySelector, comparer);
    }

    public <TKey, TElement> Queryable<Grouping<TKey, TElement>> groupBy(
        FunctionExpression<Function1<T, TKey>> keySelector,
        FunctionExpression<Function1<T, TElement>> elementSelector)
    {
        return Extensions.groupBy(getThis(), keySelector, elementSelector);
    }

    public <TKey, TResult> Queryable<Grouping<TKey, TResult>> groupByK(
        FunctionExpression<Function1<T, TKey>> keySelector,
        FunctionExpression<Function2<TKey, Enumerable<T>, TResult>>
            elementSelector)
    {
        return Extensions.groupByK(getThis(), keySelector, elementSelector);
    }

    public <TKey, TElement> Queryable<TElement> groupBy(
        FunctionExpression<Function1<T, TKey>> keySelector,
        FunctionExpression<Function1<T, TElement>> elementSelector,
        EqualityComparer comparer)
    {
        return Extensions.groupBy(
            getThis(), keySelector, elementSelector, comparer);
    }

    public <TKey, TResult> Queryable<TResult> groupByK(
        FunctionExpression<Function1<T, TKey>> keySelector,
        FunctionExpression<Function2<TKey, Enumerable<T>, TResult>>
            elementSelector,
        EqualityComparer comparer)
    {
        return Extensions.groupByK(
            getThis(), keySelector, elementSelector, comparer);
    }

    public <TKey, TElement, TResult> Queryable<TResult> groupBy(
        FunctionExpression<Function1<T, TKey>> keySelector,
        FunctionExpression<Function1<T, TElement>> elementSelector,
        FunctionExpression<Function2<TKey, Enumerable<TElement>, TResult>>
            resultSelector)
    {
        return Extensions.groupBy(
            getThis(), keySelector, elementSelector, resultSelector);
    }

    public <TKey, TElement, TResult> Queryable<TResult> groupBy(
        FunctionExpression<Function1<T, TKey>> keySelector,
        FunctionExpression<Function1<T, TElement>> elementSelector,
        FunctionExpression<Function2<TKey, Enumerable<TElement>, TResult>>
            resultSelector,
        EqualityComparer<TKey> comparer)
    {
        return Extensions.groupBy(
            getThis(), keySelector, elementSelector, resultSelector, comparer);
    }

    public <TInner, TKey, TResult> Queryable<TResult> groupJoin(
        Enumerable<TInner> inner,
        FunctionExpression<Function1<T, TKey>> outerKeySelector,
        FunctionExpression<Function1<TInner, TKey>> innerKeySelector,
        FunctionExpression<Function2<T, Enumerable<TInner>, TResult>>
            resultSelector)
    {
        return Extensions.groupJoin(
            getThis(), inner, outerKeySelector, innerKeySelector,
            resultSelector);
    }

    public <TInner, TKey, TResult> Enumerable<TResult> groupJoin(
        Enumerable<TInner> inner,
        FunctionExpression<Function1<T, TKey>> outerKeySelector,
        FunctionExpression<Function1<TInner, TKey>> innerKeySelector,
        FunctionExpression<Function2<T, Enumerable<TInner>, TResult>>
            resultSelector,
        EqualityComparer<TKey> comparer)
    {
        return Extensions.groupJoin(
            getThis(), inner, outerKeySelector, innerKeySelector,
            resultSelector, comparer);
    }

    public <TInner, TKey, TResult> Queryable<TResult> join(
        Enumerable<TInner> inner,
        FunctionExpression<Function1<T, TKey>> outerKeySelector,
        FunctionExpression<Function1<TInner, TKey>> innerKeySelector,
        FunctionExpression<Function2<T, TInner, TResult>> resultSelector)
    {
        return Extensions.join(
            getThis(), inner, outerKeySelector, innerKeySelector,
            resultSelector);
    }

    public <TInner, TKey, TResult> Queryable<TResult> join(
        Enumerable<TInner> inner,
        FunctionExpression<Function1<T, TKey>> outerKeySelector,
        FunctionExpression<Function1<TInner, TKey>> innerKeySelector,
        FunctionExpression<Function2<T, TInner, TResult>> resultSelector,
        EqualityComparer<TKey> comparer)
    {
        return Extensions.join(
            getThis(), inner, outerKeySelector, innerKeySelector,
            resultSelector, comparer);
    }

    public Queryable<T> last(FunctionExpression<Predicate1<T>> predicate) {
        return Extensions.last(getThis(), predicate);
    }

    public T lastOrDefault(FunctionExpression<Predicate1<T>> predicate) {
        return Extensions.lastOrDefault(getThis(), predicate);
    }

    public long longCount(FunctionExpression<Predicate1<T>> predicate) {
        return Extensions.longCount(getThis(), predicate);
    }

    public <TResult> TResult max(
        FunctionExpression<Function1<T, TResult>> selector)
    {
        return Extensions.max(getThis(), selector);
    }

    public <TResult> TResult min(
        FunctionExpression<Function1<T, TResult>> selector)
    {
        return Extensions.min(getThis(), selector);
    }

    public <TKey extends Comparable> Queryable<T> orderBy(
        FunctionExpression<Function1<T, TKey>> keySelector)
    {
        return Extensions.orderBy(getThis(), keySelector);
    }

    public <TKey> Queryable<T> orderBy(
        FunctionExpression<Function1<T, TKey>> keySelector,
        Comparator<TKey> comparator)
    {
        return Extensions.orderBy(getThis(), keySelector, comparator);
    }

    public <TKey extends Comparable> Queryable<T> orderByDescending(
        FunctionExpression<Function1<T, TKey>> keySelector)
    {
        return Extensions.orderByDescending(getThis(), keySelector);
    }

    public <TKey> Queryable<T> orderByDescending(
        FunctionExpression<Function1<T, TKey>> keySelector,
        Comparator<TKey> comparator)
    {
        return Extensions.orderByDescending(getThis(), keySelector, comparator);
    }

    public <TResult> Queryable<TResult> select(
        FunctionExpression<Function1<T, TResult>> selector)
    {
        return Extensions.select(getThis(), selector);
    }

    public <TResult> Queryable<TResult> selectN(
        FunctionExpression<Function2<T, Integer, TResult>> selector)
    {
        return Extensions.selectN(getThis(), selector);
    }

    public <TResult> Queryable<TResult> selectMany(
        FunctionExpression<Function1<T, Enumerable<TResult>>> selector)
    {
        return Extensions.selectMany(getThis(), selector);
    }

    public <TResult> Queryable<TResult> selectManyN(
        FunctionExpression<Function2<T, Integer, Enumerable<TResult>>> selector)
    {
        return Extensions.selectManyN(getThis(), selector);
    }

    public <TCollection, TResult> Queryable<TResult> selectMany(
        FunctionExpression<Function2<T, Integer, Enumerable<TCollection>>>
            collectionSelector,
        FunctionExpression<Function2<T, TCollection, TResult>> resultSelector)
    {
        return Extensions.selectMany(
            getThis(), collectionSelector, resultSelector);
    }

    public <TCollection, TResult> Queryable<TResult> selectManyN(
        FunctionExpression<Function1<T, Enumerable<TCollection>>>
            collectionSelector,
        FunctionExpression<Function2<T, TCollection, TResult>> resultSelector)
    {
        return Extensions.selectManyN(
            getThis(), collectionSelector, resultSelector);
    }

    public T single(FunctionExpression<Predicate1<T>> predicate) {
        return Extensions.single(getThis(), predicate);
    }

    public T singleOrDefault(FunctionExpression<Predicate1<T>> predicate) {
        return Extensions.singleOrDefault(getThis(), predicate);
    }

    public Queryable<T> skipWhile(FunctionExpression<Predicate1<T>> predicate) {
        return Extensions.skipWhile(getThis(), predicate);
    }

    public Queryable<T> skipWhileN(
        FunctionExpression<Function2<T, Integer, Boolean>> predicate)
    {
        return Extensions.skipWhileN(getThis(), predicate);
    }

    public BigDecimal sumBigDecimal(
        FunctionExpression<BigDecimalFunction1<T>> selector)
    {
        return Extensions.sumBigDecimal(getThis(), selector);
    }

    public BigDecimal sumNullableBigDecimal(
        FunctionExpression<NullableBigDecimalFunction1<T>> selector)
    {
        return Extensions.sumNullableBigDecimal(getThis(), selector);
    }

    public double sumDouble(FunctionExpression<DoubleFunction1<T>> selector) {
        return Extensions.sumDouble(getThis(), selector);
    }

    public Double sumNullableDouble(
        FunctionExpression<NullableDoubleFunction1<T>> selector)
    {
        return Extensions.sumNullableDouble(getThis(), selector);
    }

    public int sumInteger(FunctionExpression<IntegerFunction1<T>> selector) {
        return Extensions.sumInteger(getThis(), selector);
    }

    public Integer sumNullableInteger(
        FunctionExpression<NullableIntegerFunction1<T>> selector)
    {
        return Extensions.sumNullableInteger(getThis(), selector);
    }

    public long sumLong(FunctionExpression<LongFunction1<T>> selector) {
        return Extensions.sumLong(getThis(), selector);
    }

    public Long sumNullableLong(
        FunctionExpression<NullableLongFunction1<T>> selector)
    {
        return Extensions.sumNullableLong(getThis(), selector);
    }

    public float sumFloat(FunctionExpression<FloatFunction1<T>> selector) {
        return Extensions.sumFloat(getThis(), selector);
    }

    public Float sumNullableFloat(
        FunctionExpression<NullableFloatFunction1<T>> selector)
    {
        return Extensions.sumNullableFloat(getThis(), selector);
    }

    public Queryable<T> takeWhile(FunctionExpression<Predicate1<T>> predicate) {
        return Extensions.takeWhile(getThis(), predicate);
    }

    public Queryable<T> takeWhileN(
        FunctionExpression<Function2<T, Integer, Boolean>> predicate)
    {
        return Extensions.takeWhileN(getThis(), predicate);
    }

    public Queryable<T> where(FunctionExpression<Predicate1<T>> predicate) {
        return Extensions.where(getThis(), predicate);
    }

    public Queryable<T> whereN(
        FunctionExpression<Function2<T, Integer, Boolean>> predicate)
    {
        return Extensions.whereN(getThis(), predicate);
    }

    public <T1, TResult> Queryable<TResult> zip(
        Enumerable<T1> source1,
        FunctionExpression<Function2<T, T1, TResult>> resultSelector)
    {
        return Extensions.zip(getThis(), source1, resultSelector);
    }

    public T aggregate(Function2<T, T, T> func) {
        return Extensions.aggregate(getThis(), func);
    }

    public <TAccumulate> T aggregate(
        TAccumulate seed, Function2<TAccumulate, T, TAccumulate> func)
    {
        return Extensions.aggregate(getThis(), seed, func);
    }

    public <TAccumulate, TResult> TResult aggregate(
        TAccumulate seed,
        Function2<TAccumulate, T, TAccumulate> func,
        Function1<TAccumulate, TResult> selector)
    {
        return Extensions.aggregate(getThis(), seed, func, selector);
    }

}

// End DefaultQueryable.java
