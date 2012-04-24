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

import java.math.BigDecimal;
import java.util.Comparator;
import java.util.Iterator;

/**
 * Implementation of {@link Queryable} by a {@link Enumerable}.
 */
class EnumerableQueryable<T>
    extends DefaultEnumerable<T>
    implements Queryable<T>
{
    private final Enumerable<T> enumerable;
    private final Class<T> rowType;
    private final QueryProvider provider;
    private final Expression expression;

    EnumerableQueryable(
        Enumerable<T> enumerable,
        Class<T> rowType,
        QueryProvider provider,
        Expression expression)
    {
        this.enumerable = enumerable;
        this.rowType = rowType;
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
        return Extensions.union(getThis(), source1).asQueryable();
    }

    public Queryable<T> union(
        Enumerable<T> source1, EqualityComparer<T> comparer)
    {
        return Extensions.union(getThis(), source1, comparer).asQueryable();
    }

    @Override
    public Queryable<T> intersect(Enumerable<T> source1) {
        return Extensions.intersect(getThis(), source1).asQueryable();
    }

    @Override
    public Queryable<T> intersect(
        Enumerable<T> source1, EqualityComparer<T> comparer)
    {
        return Extensions.intersect(getThis(), source1, comparer).asQueryable();
    }

    @Override
    public Queryable<T> except(
        Enumerable<T> enumerable1, EqualityComparer comparer)
    {
        return Extensions.except(getThis(), enumerable1, comparer)
            .asQueryable();
    }

    @Override
    public Queryable<T> except(Enumerable<T> enumerable1) {
        return Extensions.except(getThis(), enumerable1).asQueryable();
    }

    public Queryable<T> take(int count) {
        return Extensions.take(getThis(), count).asQueryable();
    }

    public Queryable<T> skip(int count) {
        return Extensions.skip(getThis(), count).asQueryable();
    }

    public Queryable<T> reverse() {
        return Extensions.reverse(getThis()).asQueryable();
    }

    @Override
    public Queryable<T> distinct() {
        return Extensions.distinct(getThis()).asQueryable();
    }

    @Override
    public Queryable<T> distinct(EqualityComparer comparer) {
        return Extensions.distinct(getThis(), comparer).asQueryable();
    }

    @Override
    public <TResult> Queryable<TResult> ofType(Class<TResult> clazz) {
        return Extensions.ofType(getThis(), clazz).asQueryable();
    }

    @Override
    public Queryable<T> defaultIfEmpty() {
        return Extensions.defaultIfEmpty(getThis()).asQueryable();
    }

    public <T2> Queryable<T2> cast(Class<T2> clazz) {
        return Extensions.cast(getThis(), clazz).asQueryable();
    }

    // Queryable methods

    public Class<T> getElementType() {
        return rowType;
    }

    public Expression getExpression() {
        return expression;
    }

    public QueryProvider getProvider() {
        return provider;
    }

    // .............

    public T aggregate(FunctionExpression<Function2<T, T, T>> selector) {
        return Extensions.aggregate(getThis(), selector.getFunction());
    }

    public <TAccumulate> T aggregate(
        TAccumulate seed,
        FunctionExpression<Function2<TAccumulate, T, TAccumulate>> selector)
    {
        return Extensions.aggregate(getThis(), seed, selector.getFunction());
    }

    public <TAccumulate, TResult> TResult aggregate(
        TAccumulate seed,
        FunctionExpression<Function2<TAccumulate, T, TAccumulate>> func,
        FunctionExpression<Function1<TAccumulate, TResult>> selector)
    {
        return Extensions.aggregate(
            getThis(), seed, func.getFunction(), selector.getFunction());
    }

    public boolean all(FunctionExpression<Predicate1<T>> predicate) {
        return Extensions.all(getThis(), predicate.getFunction());
    }

    public boolean any(FunctionExpression<Predicate1<T>> predicate) {
        return Extensions.any(getThis(), predicate.getFunction());
    }

    public BigDecimal averageBigDecimal(
        FunctionExpression<BigDecimalFunction1<T>> selector)
    {
        return Extensions.average(getThis(), selector.getFunction());
    }

    public BigDecimal averageNullableBigDecimal(
        FunctionExpression<NullableBigDecimalFunction1<T>> selector)
    {
        return Extensions.average(getThis(), selector.getFunction());
    }

    public double averageDouble(
        FunctionExpression<DoubleFunction1<T>> selector)
    {
        return Extensions.average(getThis(), selector.getFunction());
    }

    public Double averageNullableDouble(
        FunctionExpression<NullableDoubleFunction1<T>> selector)
    {
        return Extensions.average(getThis(), selector.getFunction());
    }

    public int averageInteger(
        FunctionExpression<IntegerFunction1<T>> selector)
    {
        return Extensions.average(getThis(), selector.getFunction());
    }

    public Integer averageNullableInteger(
        FunctionExpression<NullableIntegerFunction1<T>> selector)
    {
        return Extensions.average(getThis(), selector.getFunction());
    }

    public float averageFloat(
        FunctionExpression<FloatFunction1<T>> selector)
    {
        return Extensions.average(getThis(), selector.getFunction());
    }

    public Float averageNullableFloat(
        FunctionExpression<NullableFloatFunction1<T>> selector)
    {
        return Extensions.average(getThis(), selector.getFunction());
    }

    public long averageLong(FunctionExpression<LongFunction1<T>> selector) {
        return Extensions.average(getThis(), selector.getFunction());
    }

    public Long averageNullableLong(
        FunctionExpression<NullableLongFunction1<T>> selector)
    {
        return Extensions.average(getThis(), selector.getFunction());
    }

    public Queryable<T> concat(Enumerable<T> source2) {
        return Extensions.concat(getThis(), source2).asQueryable();
    }

    public int count(FunctionExpression<Predicate1<T>> predicate) {
        return Extensions.count(getThis(), predicate.getFunction());
    }

    public T first(FunctionExpression<Predicate1<T>> predicate) {
        return Extensions.first(getThis(), predicate.getFunction());
    }

    public T firstOrDefault(FunctionExpression<Predicate1<T>> predicate) {
        return Extensions.firstOrDefault(getThis(), predicate.getFunction());
    }

    public <TKey> Queryable<Grouping<TKey, T>> groupBy(
        FunctionExpression<Function1<T, TKey>> keySelector)
    {
        return Extensions.groupBy(getThis(), keySelector.getFunction())
            .asQueryable();
    }

    public <TKey> Queryable<Grouping<TKey, T>> groupBy(
        FunctionExpression<Function1<T, TKey>> keySelector,
        EqualityComparer comparer)
    {
        return Extensions
            .groupBy(
                getThis(), keySelector.getFunction(), comparer)
            .asQueryable();
    }

    public <TKey, TElement> Queryable<Grouping<TKey, TElement>> groupBy(
        FunctionExpression<Function1<T, TKey>> keySelector,
        FunctionExpression<Function1<T, TElement>> elementSelector)
    {
        return Extensions.groupBy(
            getThis(), keySelector.getFunction(), elementSelector.getFunction())
            .asQueryable();
    }

    public <TKey, TResult> Queryable<Grouping<TKey, TResult>> groupByK(
        FunctionExpression<Function1<T, TKey>> keySelector,
        FunctionExpression<Function2<TKey, Enumerable<T>, TResult>>
            elementSelector)
    {
        return Extensions.groupBy(
            getThis(), keySelector.getFunction(), elementSelector.getFunction())
            .asQueryable();
    }

    public <TKey, TElement> Queryable<Grouping<TKey, TElement>> groupBy(
        FunctionExpression<Function1<T, TKey>> keySelector,
        FunctionExpression<Function1<T, TElement>> elementSelector,
        EqualityComparer comparer)
    {
        return Extensions.groupBy(
            getThis(),
            keySelector.getFunction(),
            elementSelector.getFunction(),
            comparer).asQueryable();
    }

    public <TKey, TResult> Queryable<TResult> groupByK(
        FunctionExpression<Function1<T, TKey>> keySelector,
        FunctionExpression<Function2<TKey, Enumerable<T>, TResult>>
            elementSelector,
        EqualityComparer comparer)
    {
        return Extensions.groupBy(
            getThis(),
            keySelector.getFunction(),
            elementSelector.getFunction(),
            comparer).asQueryable();
    }

    public <TKey, TElement, TResult> Queryable<TResult> groupBy(
        FunctionExpression<Function1<T, TKey>> keySelector,
        FunctionExpression<Function1<T, TElement>> elementSelector,
        FunctionExpression<Function2<TKey, Enumerable<TElement>, TResult>>
            resultSelector)
    {
        return Extensions.groupBy(
            getThis(),
            keySelector.getFunction(),
            elementSelector.getFunction(),
            resultSelector.getFunction()).asQueryable();
    }

    public <TKey, TElement, TResult> Queryable<TResult> groupBy(
        FunctionExpression<Function1<T, TKey>> keySelector,
        FunctionExpression<Function1<T, TElement>> elementSelector,
        FunctionExpression<Function2<TKey, Enumerable<TElement>, TResult>>
            resultSelector,
        EqualityComparer<TKey> comparer)
    {
        return Extensions.groupBy(
            getThis(),
            keySelector.getFunction(),
            elementSelector.getFunction(),
            resultSelector.getFunction(),
            comparer).asQueryable();
    }

    public <TInner, TKey, TResult> Queryable<TResult> groupJoin(
        Enumerable<TInner> inner,
        FunctionExpression<Function1<T, TKey>> outerKeySelector,
        FunctionExpression<Function1<TInner, TKey>> innerKeySelector,
        FunctionExpression<Function2<T, Enumerable<TInner>, TResult>>
            resultSelector)
    {
        return Extensions.groupJoin(
            getThis(),
            inner,
            outerKeySelector.getFunction(),
            innerKeySelector.getFunction(),
            resultSelector.getFunction()).asQueryable();
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
            getThis(),
            inner,
            outerKeySelector.getFunction(),
            innerKeySelector.getFunction(),
            resultSelector.getFunction(),
            comparer);
    }

    public <TInner, TKey, TResult> Queryable<TResult> join(
        Enumerable<TInner> inner,
        FunctionExpression<Function1<T, TKey>> outerKeySelector,
        FunctionExpression<Function1<TInner, TKey>> innerKeySelector,
        FunctionExpression<Function2<T, TInner, TResult>> resultSelector)
    {
        return Extensions.join(
            getThis(),
            inner,
            outerKeySelector.getFunction(),
            innerKeySelector.getFunction(),
            resultSelector.getFunction()).asQueryable();
    }

    public <TInner, TKey, TResult> Queryable<TResult> join(
        Enumerable<TInner> inner,
        FunctionExpression<Function1<T, TKey>> outerKeySelector,
        FunctionExpression<Function1<TInner, TKey>> innerKeySelector,
        FunctionExpression<Function2<T, TInner, TResult>> resultSelector,
        EqualityComparer<TKey> comparer)
    {
        return Extensions.join(
            getThis(),
            inner,
            outerKeySelector.getFunction(),
            innerKeySelector.getFunction(),
            resultSelector.getFunction(),
            comparer).asQueryable();
    }

    public T last(FunctionExpression<Predicate1<T>> predicate) {
        return Extensions.last(getThis(), predicate.getFunction());
    }

    public T lastOrDefault(FunctionExpression<Predicate1<T>> predicate) {
        return Extensions.lastOrDefault(getThis(), predicate.getFunction());
    }

    public long longCount(FunctionExpression<Predicate1<T>> predicate) {
        return Extensions.longCount(getThis(), predicate.getFunction());
    }

    public <TResult> TResult max(
        FunctionExpression<Function1<T, TResult>> selector)
    {
        return Extensions.max(getThis(), selector.getFunction());
    }

    public <TResult> TResult min(
        FunctionExpression<Function1<T, TResult>> selector)
    {
        return Extensions.min(getThis(), selector.getFunction());
    }

    public <TKey extends Comparable> Queryable<T> orderBy(
        FunctionExpression<Function1<T, TKey>> keySelector)
    {
        return Extensions.orderBy(getThis(), keySelector.getFunction())
            .asQueryable();
    }

    public <TKey> Queryable<T> orderBy(
        FunctionExpression<Function1<T, TKey>> keySelector,
        Comparator<TKey> comparator)
    {
        return Extensions
            .orderBy(getThis(), keySelector.getFunction(), comparator)
            .asQueryable();
    }

    public <TKey extends Comparable> Queryable<T> orderByDescending(
        FunctionExpression<Function1<T, TKey>> keySelector)
    {
        return Extensions
            .orderByDescending(getThis(), keySelector.getFunction())
            .asQueryable();
    }

    public <TKey> Queryable<T> orderByDescending(
        FunctionExpression<Function1<T, TKey>> keySelector,
        Comparator<TKey> comparator)
    {
        return Extensions
            .orderByDescending(getThis(), keySelector.getFunction(), comparator)
            .asQueryable();
    }

    public <TResult> Queryable<TResult> select(
        FunctionExpression<Function1<T, TResult>> selector)
    {
        return Extensions
            .select(getThis(), selector.getFunction())
            .asQueryable();
    }

    public <TResult> Queryable<TResult> selectN(
        FunctionExpression<Function2<T, Integer, TResult>> selector)
    {
        return Extensions
            .select(getThis(), selector.getFunction())
            .asQueryable();
    }

    public <TResult> Queryable<TResult> selectMany(
        FunctionExpression<Function1<T, Enumerable<TResult>>> selector)
    {
        return Extensions
            .selectMany(getThis(), selector.getFunction())
            .asQueryable();
    }

    public <TResult> Queryable<TResult> selectManyN(
        FunctionExpression<Function2<T, Integer, Enumerable<TResult>>> selector)
    {
        return Extensions
            .selectMany(getThis(), selector.getFunction())
            .asQueryable();
    }

    public <TCollection, TResult> Queryable<TResult> selectMany(
        FunctionExpression<Function2<T, Integer, Enumerable<TCollection>>>
            collectionSelector,
        FunctionExpression<Function2<T, TCollection, TResult>> resultSelector)
    {
        return Extensions.selectMany(
            getThis(),
            collectionSelector.getFunction(),
            resultSelector.getFunction()).asQueryable();
    }

    public <TCollection, TResult> Queryable<TResult> selectManyN(
        FunctionExpression<Function1<T, Enumerable<TCollection>>>
            collectionSelector,
        FunctionExpression<Function2<T, TCollection, TResult>> resultSelector)
    {
        return Extensions.selectMany(
            getThis(),
            collectionSelector.getFunction(),
            resultSelector.getFunction()).asQueryable();
    }

    public T single(FunctionExpression<Predicate1<T>> predicate) {
        return Extensions.single(getThis(), predicate.getFunction());
    }

    public T singleOrDefault(FunctionExpression<Predicate1<T>> predicate) {
        return Extensions.singleOrDefault(getThis(), predicate.getFunction());
    }

    public Queryable<T> skipWhile(FunctionExpression<Predicate1<T>> predicate) {
        return Extensions.skipWhile(getThis(), predicate.getFunction())
            .asQueryable();
    }

    public Queryable<T> skipWhileN(
        FunctionExpression<Function2<T, Integer, Boolean>> predicate)
    {
        return Extensions.skipWhile(getThis(), predicate.getFunction())
            .asQueryable();
    }

    public BigDecimal sumBigDecimal(
        FunctionExpression<BigDecimalFunction1<T>> selector)
    {
        return Extensions.sum(getThis(), selector.getFunction());
    }

    public BigDecimal sumNullableBigDecimal(
        FunctionExpression<NullableBigDecimalFunction1<T>> selector)
    {
        return Extensions.sum(getThis(), selector.getFunction());
    }

    public double sumDouble(FunctionExpression<DoubleFunction1<T>> selector) {
        return Extensions.sum(getThis(), selector.getFunction());
    }

    public Double sumNullableDouble(
        FunctionExpression<NullableDoubleFunction1<T>> selector)
    {
        return Extensions.sum(getThis(), selector.getFunction());
    }

    public int sumInteger(FunctionExpression<IntegerFunction1<T>> selector) {
        return Extensions.sum(getThis(), selector.getFunction());
    }

    public Integer sumNullableInteger(
        FunctionExpression<NullableIntegerFunction1<T>> selector)
    {
        return Extensions.sum(getThis(), selector.getFunction());
    }

    public long sumLong(FunctionExpression<LongFunction1<T>> selector) {
        return Extensions.sum(getThis(), selector.getFunction());
    }

    public Long sumNullableLong(
        FunctionExpression<NullableLongFunction1<T>> selector)
    {
        return Extensions.sum(getThis(), selector.getFunction());
    }

    public float sumFloat(FunctionExpression<FloatFunction1<T>> selector) {
        return Extensions.sum(getThis(), selector.getFunction());
    }

    public Float sumNullableFloat(
        FunctionExpression<NullableFloatFunction1<T>> selector)
    {
        return Extensions.sum(getThis(), selector.getFunction());
    }

    public Queryable<T> takeWhile(FunctionExpression<Predicate1<T>> predicate) {
        return Extensions.takeWhile(getThis(), predicate.getFunction())
            .asQueryable();
    }

    public Queryable<T> takeWhileN(
        FunctionExpression<Function2<T, Integer, Boolean>> predicate)
    {
        return Extensions.takeWhile(getThis(), predicate.getFunction())
            .asQueryable();
    }

    public Queryable<T> where(FunctionExpression<Predicate1<T>> predicate) {
        return Extensions.where(getThis(), predicate.getFunction())
            .asQueryable();
    }

    public Queryable<T> whereN(
        FunctionExpression<Predicate2<T, Integer>> predicate)
    {
        return Extensions.where(getThis(), predicate.getFunction())
            .asQueryable();
    }

    public <T1, TResult> Queryable<TResult> zip(
        Enumerable<T1> source1,
        FunctionExpression<Function2<T, T1, TResult>> resultSelector)
    {
        return Extensions.zip(getThis(), source1, resultSelector.getFunction())
            .asQueryable();
    }

    public T aggregate(Function2<T, T, T> func) {
        return Extensions.aggregate(getThis(), func);
    }


    public <TAccumulate, TResult> TResult aggregate(
        TAccumulate seed,
        Function2<TAccumulate, T, TAccumulate> func,
        Function1<TAccumulate, TResult> selector)
    {
        return Extensions.aggregate(getThis(), seed, func, selector);
    }
}

// End EnumerableQueryable.java
