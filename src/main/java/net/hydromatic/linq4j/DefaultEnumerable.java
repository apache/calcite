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
 */
abstract class DefaultEnumerable<T> implements Enumerable<T> {

    /**
     * Derived classes might wish to override this method to return the "outer"
     * enumerable.
     */
    protected Enumerable<T> getThis() {
        return this;
    }

    public Queryable<T> asQueryable() {
        return Extensions.asQueryable(getThis());
    }

    public <T2> Enumerable<T2> cast(Class<T2> clazz) {
        return Extensions.cast(getThis(), clazz);
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

    public boolean all(Predicate1<T> predicate) {
        return Extensions.all(getThis(), predicate);
    }

    public boolean any() {
        return Extensions.any(getThis());
    }

    public boolean any(Predicate1<T> predicate) {
        return Extensions.any(getThis(), predicate);
    }

    public Enumerable<T> asEnumerable() {
        return Extensions.asEnumerable(getThis());
    }

    public BigDecimal average(BigDecimalFunction1<T> selector) {
        return Extensions.averageDecimal(getThis(), selector);
    }

    public BigDecimal average(NullableBigDecimalFunction1<T> selector) {
        return Extensions.average(getThis(), selector);
    }

    public double average(DoubleFunction1<T> selector) {
        return Extensions.average(getThis(), selector);
    }

    public Double average(NullableDoubleFunction1<T> selector) {
        return Extensions.average(getThis(), selector);
    }

    public int average(IntegerFunction1<T> selector) {
        return Extensions.average(getThis(), selector);
    }

    public Integer average(NullableIntegerFunction1<T> selector) {
        return Extensions.average(getThis(), selector);
    }

    public long average(LongFunction1<T> selector) {
        return Extensions.average(getThis(), selector);
    }

    public Long average(NullableLongFunction1<T> selector) {
        return Extensions.average(getThis(), selector);
    }

    public float average(FloatFunction1<T> selector) {
        return Extensions.average(getThis(), selector);
    }

    public Float average(NullableFloatFunction1<T> selector) {
        return Extensions.average(getThis(), selector);
    }

    public <TResult> Enumerable<TResult> cast() {
        return Extensions.cast(getThis());
    }

    public Enumerable<T> concat(Enumerable<T> enumerable1) {
        return Extensions.concat(getThis(), enumerable1);
    }

    public boolean contains(T element) {
        return Extensions.contains(getThis(), element);
    }

    public boolean contains(T element, EqualityComparer comparer) {
        return Extensions.contains(getThis(), element, comparer);
    }

    public int count() {
        return Extensions.count(getThis());
    }

    public int count(Predicate1<T> predicate) {
        return Extensions.count(getThis(), predicate);
    }

    public Enumerable<T> defaultIfEmpty() {
        return Extensions.defaultIfEmpty(getThis());
    }

    public T defaultIfEmpty(T value) {
        return Extensions.defaultIfEmpty(getThis(), value);
    }

    public Enumerable<T> distinct() {
        return Extensions.distinct(getThis());
    }

    public Enumerable<T> distinct(
        EqualityComparer comparer)
    {
        return Extensions.distinct(getThis(), comparer);
    }

    public T elementAt(int index) {
        return Extensions.elementAt(getThis(), index);
    }

    public T elementAtOrDefault(int index) {
        return Extensions.elementAtOrDefault(getThis(), index);
    }

    public Enumerable<T> except(Enumerable<T> enumerable1) {
        return Extensions.except(getThis(), enumerable1);
    }

    public Enumerable<T> except(
        Enumerable<T> enumerable1, EqualityComparer comparer)
    {
        return Extensions.except(getThis(), enumerable1, comparer);
    }

    public T first() {
        return Extensions.first(getThis());
    }

    public T first(Predicate1<T> predicate) {
        return Extensions.first(getThis(), predicate);
    }

    public T firstOrDefault() {
        return Extensions.firstOrDefault(getThis());
    }

    public T firstOrDefault(Predicate1<T> predicate) {
        return Extensions.firstOrDefault(getThis(), predicate);
    }

    public <TKey> Enumerable<Grouping<TKey, T>> groupBy(
        Function1<T, TKey> keySelector)
    {
        return Extensions.groupBy(getThis(), keySelector);
    }

    public <TKey> Enumerable<Grouping<TKey, T>> groupBy(
        Function1<T, TKey> keySelector,
        EqualityComparer comparer)
    {
        return Extensions.groupBy(getThis(), keySelector, comparer);
    }

    public <TKey, TElement> Enumerable<Grouping<TKey, TElement>> groupBy(
        Function1<T, TKey> keySelector,
        Function1<T, TElement> elementSelector)
    {
        return Extensions.groupBy(getThis(), keySelector, elementSelector);
    }

    public <TKey, TResult> Enumerable<Grouping<TKey, TResult>> groupBy(
        Function1<T, TKey> keySelector,
        Function2<TKey, Enumerable<T>, TResult> elementSelector)
    {
        return Extensions.groupBy(getThis(), keySelector, elementSelector);
    }

    public <TKey, TElement> Enumerable<Grouping<TKey, TElement>> groupBy(
        Function1<T, TKey> keySelector,
        Function1<T, TElement> elementSelector,
        EqualityComparer comparer)
    {
        return Extensions.groupBy(getThis(), keySelector, elementSelector);
    }

    public <TKey, TResult> Enumerable<TResult> groupBy(
        Function1<T, TKey> keySelector,
        Function2<TKey, Enumerable<T>, TResult> elementSelector,
        EqualityComparer comparer)
    {
        return Extensions.groupBy(
            getThis(),
            keySelector,
            elementSelector,
            comparer);
    }

    public <TKey, TElement, TResult> Enumerable<TResult> groupBy(
        Function1<T, TKey> keySelector,
        Function1<T, TElement> elementSelector,
        Function2<TKey, Enumerable<TElement>, TResult> resultSelector)
    {
        return Extensions.groupBy(getThis(), keySelector, elementSelector, resultSelector);
    }

    public <TKey, TElement, TResult> Enumerable<TResult> groupBy(
        Function1<T, TKey> keySelector,
        Function1<T, TElement> elementSelector,
        Function2<TKey, Enumerable<TElement>, TResult> resultSelector,
        EqualityComparer<TKey> comparer)
    {
        return Extensions.groupBy(
            getThis(),
            keySelector,
            elementSelector,
            resultSelector,
            comparer);
    }

    public <TInner, TKey, TResult> Enumerable<TResult> groupJoin(
        Enumerable<TInner> inner,
        Function1<T, TKey> outerKeySelector,
        Function1<TInner, TKey> innerKeySelector,
        Function2<T, Enumerable<TInner>, TResult> resultSelector)
    {
        return Extensions.groupJoin(getThis(), inner, outerKeySelector, innerKeySelector, resultSelector);
    }

    public <TInner, TKey, TResult> Enumerable<TResult> groupJoin(
        Enumerable<TInner> inner,
        Function1<T, TKey> outerKeySelector,
        Function1<TInner, TKey> innerKeySelector,
        Function2<T, Enumerable<TInner>, TResult> resultSelector,
        EqualityComparer<TKey> comparer)
    {
        return Extensions.groupJoin(
            getThis(),
            inner,
            outerKeySelector,
            innerKeySelector,
            resultSelector,
            comparer);
    }

    public Enumerable<T> intersect(Enumerable<T> enumerable1) {
        return Extensions.intersect(getThis(), enumerable1);
    }

    public Enumerable<T> intersect(
        Enumerable<T> enumerable1,
        EqualityComparer<T> comparer)
    {
        return Extensions.intersect(getThis(), enumerable1, comparer);
    }

    public <TInner, TKey, TResult> Enumerable<TResult> join(
        Enumerable<TInner> inner,
        Function1<T, TKey> outerKeySelector,
        Function1<TInner, TKey> innerKeySelector,
        Function2<T, TInner, TResult> resultSelector)
    {
        return Extensions.join(getThis(), inner, outerKeySelector, innerKeySelector, resultSelector);
    }

    public <TInner, TKey, TResult> Enumerable<TResult> join(
        Enumerable<TInner> inner,
        Function1<T, TKey> outerKeySelector,
        Function1<TInner, TKey> innerKeySelector,
        Function2<T, TInner, TResult> resultSelector,
        EqualityComparer<TKey> comparer)
    {
        return Extensions.join(
            getThis(),
            inner,
            outerKeySelector,
            innerKeySelector,
            resultSelector,
            comparer);
    }

    public T last() {
        return Extensions.last(getThis());
    }

    public T last(Predicate1<T> predicate) {
        return Extensions.last(getThis(), predicate);
    }

    public T lastOrDefault() {
        return Extensions.lastOrDefault(getThis());
    }

    public T lastOrDefault(Predicate1<T> predicate) {
        return Extensions.lastOrDefault(getThis(), predicate);
    }

    public long longCount() {
        return Extensions.longCount(getThis());
    }

    public long longCount(Predicate1<T> predicate) {
        return Extensions.longCount(getThis(), predicate);
    }

    public T max() {
        return Extensions.max(getThis());
    }

    public BigDecimal max(BigDecimalFunction1<T> selector) {
        return Extensions.max(getThis(), selector);
    }

    public BigDecimal max(NullableBigDecimalFunction1<T> selector) {
        return Extensions.max(getThis(), selector);
    }

    public double max(DoubleFunction1<T> selector) {
        return Extensions.max(getThis(), selector);
    }

    public Double max(NullableDoubleFunction1<T> selector) {
        return Extensions.max(getThis(), selector);
    }

    public int max(IntegerFunction1<T> selector) {
        return Extensions.max(getThis(), selector);
    }

    public Integer max(NullableIntegerFunction1<T> selector) {
        return Extensions.max(getThis(), selector);
    }

    public long max(LongFunction1<T> selector) {
        return Extensions.max(getThis(), selector);
    }

    public Long max(NullableLongFunction1<T> selector) {
        return Extensions.max(getThis(), selector);
    }

    public float max(FloatFunction1<T> selector) {
        return Extensions.max(getThis(), selector);
    }

    public Float max(NullableFloatFunction1<T> selector) {
        return Extensions.max(getThis(), selector);
    }

    public <TResult> TResult max(Function1<T, TResult> selector) {
        return Extensions.max(getThis(), selector);
    }

    public T min() {
        return Extensions.min(getThis());
    }

    public BigDecimal min(BigDecimalFunction1<T> selector) {
        return Extensions.min(getThis(), selector);
    }

    public BigDecimal min(NullableBigDecimalFunction1<T> selector) {
        return Extensions.min(getThis(), selector);
    }

    public double min(DoubleFunction1<T> selector) {
        return Extensions.min(getThis(), selector);
    }

    public Double min(NullableDoubleFunction1<T> selector) {
        return Extensions.min(getThis(), selector);
    }

    public int min(IntegerFunction1<T> selector) {
        return Extensions.min(getThis(), selector);
    }

    public Integer min(NullableIntegerFunction1<T> selector) {
        return Extensions.min(getThis(), selector);
    }

    public long min(LongFunction1<T> selector) {
        return Extensions.min(getThis(), selector);
    }

    public Long min(NullableLongFunction1<T> selector) {
        return Extensions.min(getThis(), selector);
    }

    public float min(FloatFunction1<T> selector) {
        return Extensions.min(getThis(), selector);
    }

    public Float min(NullableFloatFunction1<T> selector) {
        return Extensions.min(getThis(), selector);
    }

    public <TResult> TResult min(Function1<T, TResult> selector) {
        return Extensions.min(getThis(), selector);
    }

    public <TResult> Enumerable<TResult> ofType(Class<TResult> clazz) {
        return Extensions.ofType(getThis(), clazz);
    }

    public <TKey extends Comparable> Enumerable<T> orderBy(Function1<T, TKey> keySelector) {
        return Extensions.orderBy(getThis(), keySelector);
    }

    public <TKey> Enumerable<T> orderBy(
        Function1<T, TKey> keySelector, Comparator<TKey> comparator)
    {
        return Extensions.orderBy(getThis(), keySelector, comparator);
    }

    public <TKey extends Comparable> Enumerable<T> orderByDescending(
        Function1<T, TKey> keySelector)
    {
        return Extensions.orderByDescending(getThis(), keySelector);
    }

    public <TKey> Enumerable<T> orderByDescending(
        Function1<T, TKey> keySelector, Comparator<TKey> comparator)
    {
        return Extensions.orderByDescending(getThis(), keySelector, comparator);
    }

    public Enumerable<T> reverse() {
        return Extensions.reverse(getThis());
    }

    public <TResult> Enumerable<TResult> select(Function1<T, TResult> selector) {
        return Extensions.select(getThis(), selector);
    }

    public <TResult> Enumerable<TResult> select(
        Function2<T, Integer, TResult> selector)
    {
        return Extensions.select(getThis(), selector);
    }

    public <TResult> Enumerable<TResult> selectMany(Function1<T, Enumerable<TResult>> selector) {
        return Extensions.selectMany(getThis(), selector);
    }

    public <TResult> Enumerable<TResult> selectMany(
        Function2<T, Integer, Enumerable<TResult>> selector)
    {
        return Extensions.selectMany(getThis(), selector);
    }

    public <TCollection, TResult> Enumerable<TResult> selectMany(
        Function2<T, Integer, Enumerable<TCollection>> collectionSelector,
        Function2<T, TCollection, TResult> resultSelector)
    {
        return Extensions.selectMany(getThis(), collectionSelector, resultSelector);
    }

    public <TCollection, TResult> Enumerable<TResult> selectMany(
        Function1<T, Enumerable<TCollection>> collectionSelector,
        Function2<T, TCollection, TResult> resultSelector)
    {
        return Extensions.selectMany(
            getThis(),
            collectionSelector,
            resultSelector);
    }

    public boolean sequenceEqual(Enumerable<T> enumerable1) {
        return Extensions.sequenceEqual(getThis(), enumerable1);
    }

    public boolean sequenceEqual(
        Enumerable<T> enumerable1,
        EqualityComparer<T> comparer)
    {
        return Extensions.sequenceEqual(getThis(), enumerable1, comparer);
    }

    public T single() {
        return Extensions.single(getThis());
    }

    public T single(Predicate1<T> predicate) {
        return Extensions.single(getThis(), predicate);
    }

    public T singleOrDefault() {
        return Extensions.singleOrDefault(getThis());
    }

    public T singleOrDefault(Predicate1<T> predicate) {
        return Extensions.singleOrDefault(getThis(), predicate);
    }

    public Enumerable<T> skip(int count) {
        return Extensions.skip(getThis(), count);
    }

    public Enumerable<T> skipWhile(Predicate1<T> predicate) {
        return Extensions.skipWhile(getThis(), predicate);
    }

    public Enumerable<T> skipWhile(Function2<T, Integer, Boolean> predicate) {
        return Extensions.skipWhile(getThis(), predicate);
    }

    public BigDecimal sum(Function1<T, BigDecimal> selector) {
        return Extensions.sum(getThis(), selector);
    }

    public BigDecimal sum(NullableBigDecimalFunction1<T> selector) {
        return Extensions.sum(getThis(), selector);
    }

    public double sum(DoubleFunction1<T> selector) {
        return Extensions.sum(getThis(), selector);
    }

    public Double sum(NullableDoubleFunction1<T> selector) {
        return Extensions.sum(getThis(), selector);
    }

    public int sum(IntegerFunction1<T> selector) {
        return Extensions.sum(getThis(), selector);
    }

    public Integer sum(NullableIntegerFunction1<T> selector) {
        return Extensions.sum(getThis(), selector);
    }

    public long sum(LongFunction1<T> selector) {
        return Extensions.sum(getThis(), selector);
    }

    public Long sum(NullableLongFunction1<T> selector) {
        return Extensions.sum(getThis(), selector);
    }

    public float sum(FloatFunction1<T> selector) {
        return Extensions.sum(getThis(), selector);
    }

    public Float sum(NullableFloatFunction1<T> selector) {
        return Extensions.sum(getThis(), selector);
    }

    public Enumerable<T> take(int count) {
        return Extensions.take(getThis(), count);
    }

    public Enumerable<T> takeWhile(Predicate1<T> predicate) {
        return Extensions.takeWhile(getThis(), predicate);
    }

    public Enumerable<T> takeWhile(
        Function2<T, Integer, Boolean> predicate)
    {
        return Extensions.takeWhile(getThis(), predicate);
    }

    public <TKey> Map<TKey, T> toMap(Function1<T, TKey> keySelector) {
        return Extensions.toMap(getThis(), keySelector);
    }

    public <TKey> Map<TKey, T> toMap(
        Function1<T, TKey> keySelector,
        EqualityComparer<TKey> comparer)
    {
        return Extensions.toMap(getThis(), keySelector, comparer);
    }

    public <TKey, TElement> Map<TKey, TElement> toMap(
        Function1<T, TKey> keySelector,
        Function1<T, TElement> elementSelector)
    {
        return Extensions.toMap(getThis(), keySelector, elementSelector);
    }

    public <TKey, TElement> Map<TKey, TElement> toMap(
        Function1<T, TKey> keySelector,
        Function1<T, TElement> elementSelector,
        EqualityComparer<TKey> comparer)
    {
        return Extensions.toMap(
            getThis(),
            keySelector,
            elementSelector,
            comparer);
    }

    public List<T> toList() {
        return Extensions.toList(getThis());
    }

    public <TKey> MultiMap<T, TKey> toLookup(Function1<T, TKey> keySelector) {
        return Extensions.toLookup(getThis(), keySelector);
    }

    public <TKey> MultiMap<T, TKey> toLookup(
        Function1<T, TKey> keySelector,
        EqualityComparer<TKey> comparer)
    {
        return Extensions.toLookup(getThis(), keySelector, comparer);
    }

    public <TKey, TElement> MultiMap<T, TElement> toLookup(
        Function1<T, TKey> keySelector,
        Function1<T, TElement> elementSelector)
    {
        return Extensions.toLookup(getThis(), keySelector, elementSelector);
    }

    public <TKey, TElement> MultiMap<T, TElement> toLookup(
        Function1<T, TKey> keySelector,
        Function1<T, TElement> elementSelector,
        EqualityComparer<TKey> comparer)
    {
        return Extensions.toLookup(
            getThis(),
            keySelector,
            elementSelector,
            comparer);
    }

    public Enumerable<T> union(Enumerable<T> source1) {
        return Extensions.union(getThis(), source1);
    }

    public Enumerable<T> union(
        Enumerable<T> source1, EqualityComparer<T> comparer)
    {
        return Extensions.union(getThis(), source1, comparer);
    }

    public Enumerable<T> where(Predicate1<T> predicate) {
        return Extensions.where(getThis(), predicate);
    }

    public Enumerable<T> where(Function2<T, Integer, Boolean> predicate) {
        return Extensions.where(getThis(), predicate);
    }

    public <T1, TResult> Enumerable<TResult> zip(
        Enumerable<T1> source1, Function2<T, T1, TResult> resultSelector)
    {
        return Extensions.zip(getThis(), source1, resultSelector);
    }
}

// End AbstractEnumerable.java
