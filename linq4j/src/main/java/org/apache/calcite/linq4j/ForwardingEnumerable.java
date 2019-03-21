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

import java.math.BigDecimal;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Abstract implementation of the {@link Enumerable} interface that forwards all methods
 * to another {@link Enumerable}, accessible via the abstract method {@link #delegate()}
 * (to be overridden by the derived classes).
 *
 * @param <E> Element type
 */
abstract class ForwardingEnumerable<E> implements Enumerable<E> {

  /**
   * @return the {@link Enumerable} where all methods will be forwarded to
   */
  protected abstract Enumerable<E> delegate();

  @Override public Enumerator<E> enumerator() {
    return delegate().enumerator();
  }

  @Override public Iterator<E> iterator() {
    return delegate().iterator();
  }

  @Override public <R> R foreach(Function1<E, R> func) {
    return delegate().foreach(func);
  }

  @Override public E aggregate(Function2<E, E, E> func) {
    return delegate().aggregate(func);
  }

  @Override public <TAccumulate> TAccumulate aggregate(
          TAccumulate seed,
          Function2<TAccumulate, E, TAccumulate> func) {
    return delegate().aggregate(seed, func);
  }

  @Override public <TAccumulate, TResult> TResult aggregate(
          TAccumulate seed,
          Function2<TAccumulate, E, TAccumulate> func,
          Function1<TAccumulate, TResult> selector) {
    return delegate().aggregate(seed, func, selector);
  }

  @Override public boolean all(Predicate1<E> predicate) {
    return delegate().all(predicate);
  }

  @Override public boolean any() {
    return delegate().any();
  }

  @Override public boolean any(Predicate1<E> predicate) {
    return delegate().any(predicate);
  }

  @Override public Enumerable<E> asEnumerable() {
    return delegate().asEnumerable();
  }

  @Override public Queryable<E> asQueryable() {
    return delegate().asQueryable();
  }

  @Override public BigDecimal average(BigDecimalFunction1<E> selector) {
    return delegate().average(selector);
  }

  @Override public BigDecimal average(NullableBigDecimalFunction1<E> selector) {
    return delegate().average(selector);
  }

  @Override public double average(DoubleFunction1<E> selector) {
    return delegate().average(selector);
  }

  @Override public Double average(NullableDoubleFunction1<E> selector) {
    return delegate().average(selector);
  }

  @Override public int average(IntegerFunction1<E> selector) {
    return delegate().average(selector);
  }

  @Override public Integer average(NullableIntegerFunction1<E> selector) {
    return delegate().average(selector);
  }

  @Override public long average(LongFunction1<E> selector) {
    return delegate().average(selector);
  }

  @Override public Long average(NullableLongFunction1<E> selector) {
    return delegate().average(selector);
  }

  @Override public float average(FloatFunction1<E> selector) {
    return delegate().average(selector);
  }

  @Override public Float average(NullableFloatFunction1<E> selector) {
    return delegate().average(selector);
  }

  @Override public <T2> Enumerable<T2> cast(Class<T2> clazz) {
    return delegate().cast(clazz);
  }

  @Override public Enumerable<E> concat(Enumerable<E> enumerable1) {
    return delegate().concat(enumerable1);
  }

  @Override public boolean contains(E element) {
    return delegate().contains(element);
  }

  @Override public boolean contains(E element, EqualityComparer<E> comparer) {
    return delegate().contains(element, comparer);
  }

  @Override public int count() {
    return delegate().count();
  }

  @Override public int count(Predicate1<E> predicate) {
    return delegate().count(predicate);
  }

  @Override public Enumerable<E> defaultIfEmpty() {
    return delegate().defaultIfEmpty();
  }

  @Override public Enumerable<E> defaultIfEmpty(E value) {
    return delegate().defaultIfEmpty(value);
  }

  @Override public Enumerable<E> distinct() {
    return delegate().distinct();
  }

  @Override public Enumerable<E> distinct(EqualityComparer<E> comparer) {
    return delegate().distinct(comparer);
  }

  @Override public E elementAt(int index) {
    return delegate().elementAt(index);
  }

  @Override public E elementAtOrDefault(int index) {
    return delegate().elementAtOrDefault(index);
  }

  @Override public Enumerable<E> except(Enumerable<E> enumerable1) {
    return delegate().except(enumerable1);
  }

  @Override public Enumerable<E> except(Enumerable<E> enumerable1, EqualityComparer<E> comparer) {
    return delegate().except(enumerable1, comparer);
  }

  @Override public E first() {
    return delegate().first();
  }

  @Override public E first(Predicate1<E> predicate) {
    return delegate().first(predicate);
  }

  @Override public E firstOrDefault() {
    return delegate().firstOrDefault();
  }

  @Override public E firstOrDefault(Predicate1<E> predicate) {
    return delegate().firstOrDefault(predicate);
  }

  @Override public <TKey> Enumerable<Grouping<TKey, E>> groupBy(Function1<E, TKey> keySelector) {
    return delegate().groupBy(keySelector);
  }

  @Override public <TKey> Enumerable<Grouping<TKey, E>> groupBy(
          Function1<E, TKey> keySelector,
          EqualityComparer<TKey> comparer) {
    return delegate().groupBy(keySelector, comparer);
  }

  @Override public <TKey, TElement> Enumerable<Grouping<TKey, TElement>> groupBy(
          Function1<E, TKey> keySelector,
          Function1<E, TElement> elementSelector) {
    return delegate().groupBy(keySelector, elementSelector);
  }

  @Override public <TKey, TElement> Enumerable<Grouping<TKey, TElement>> groupBy(
          Function1<E, TKey> keySelector,
          Function1<E, TElement> elementSelector,
          EqualityComparer<TKey> comparer) {
    return delegate().groupBy(keySelector, elementSelector, comparer);
  }

  @Override public <TKey, TResult> Enumerable<TResult> groupBy(
          Function1<E, TKey> keySelector,
          Function2<TKey, Enumerable<E>, TResult> resultSelector) {
    return delegate().groupBy(keySelector, resultSelector);
  }

  @Override public <TKey, TResult> Enumerable<TResult> groupBy(
          Function1<E, TKey> keySelector,
          Function2<TKey, Enumerable<E>, TResult> resultSelector,
          EqualityComparer<TKey> comparer) {
    return delegate().groupBy(keySelector, resultSelector, comparer);
  }

  @Override public <TKey, TElement, TResult> Enumerable<TResult> groupBy(
          Function1<E, TKey> keySelector,
          Function1<E, TElement> elementSelector,
          Function2<TKey, Enumerable<TElement>, TResult> resultSelector) {
    return delegate().groupBy(keySelector, elementSelector, resultSelector);
  }

  @Override public <TKey, TElement, TResult> Enumerable<TResult> groupBy(
          Function1<E, TKey> keySelector,
          Function1<E, TElement> elementSelector,
          Function2<TKey, Enumerable<TElement>, TResult> resultSelector,
          EqualityComparer<TKey> comparer) {
    return delegate().groupBy(keySelector, elementSelector, resultSelector, comparer);
  }

  @Override public <TKey, TAccumulate, TResult> Enumerable<TResult> groupBy(
          Function1<E, TKey> keySelector,
          Function0<TAccumulate> accumulatorInitializer,
          Function2<TAccumulate, E, TAccumulate> accumulatorAdder,
          Function2<TKey, TAccumulate, TResult> resultSelector) {
    return delegate().groupBy(keySelector, accumulatorInitializer,
            accumulatorAdder, resultSelector);
  }

  @Override public <TKey, TAccumulate, TResult> Enumerable<TResult> groupBy(
          Function1<E, TKey> keySelector,
          Function0<TAccumulate> accumulatorInitializer,
          Function2<TAccumulate, E, TAccumulate> accumulatorAdder,
          Function2<TKey, TAccumulate, TResult> resultSelector,
          EqualityComparer<TKey> comparer) {
    return delegate().groupBy(keySelector, accumulatorInitializer, accumulatorAdder,
            resultSelector, comparer);
  }

  @Override public <TInner, TKey, TResult> Enumerable<TResult> groupJoin(
          Enumerable<TInner> inner,
          Function1<E, TKey> outerKeySelector,
          Function1<TInner, TKey> innerKeySelector,
          Function2<E, Enumerable<TInner>, TResult> resultSelector) {
    return delegate().groupJoin(inner, outerKeySelector, innerKeySelector, resultSelector);
  }

  @Override public <TInner, TKey, TResult> Enumerable<TResult> groupJoin(
          Enumerable<TInner> inner,
          Function1<E, TKey> outerKeySelector,
          Function1<TInner, TKey> innerKeySelector,
          Function2<E, Enumerable<TInner>, TResult> resultSelector,
          EqualityComparer<TKey> comparer) {
    return delegate().groupJoin(inner, outerKeySelector, innerKeySelector,
            resultSelector, comparer);
  }

  @Override public Enumerable<E> intersect(Enumerable<E> intersect) {
    return delegate().intersect(intersect);
  }

  @Override public Enumerable<E> intersect(
          Enumerable<E> enumerable1,
          EqualityComparer<E> comparer) {
    return delegate().intersect(enumerable1, comparer);
  }

  @Override public <C extends Collection<? super E>> C into(C sink) {
    return delegate().into(sink);
  }

  @Override public <C extends Collection<? super E>> C removeAll(C sink) {
    return delegate().removeAll(sink);
  }

  @Override public <TInner, TKey, TResult> Enumerable<TResult> join(
          Enumerable<TInner> inner,
          Function1<E, TKey> outerKeySelector,
          Function1<TInner, TKey> innerKeySelector,
          Function2<E, TInner, TResult> resultSelector) {
    return delegate().join(inner, outerKeySelector, innerKeySelector, resultSelector);
  }

  @Override public <TInner, TKey, TResult> Enumerable<TResult> join(
          Enumerable<TInner> inner,
          Function1<E, TKey> outerKeySelector,
          Function1<TInner, TKey> innerKeySelector,
          Function2<E, TInner, TResult> resultSelector,
          EqualityComparer<TKey> comparer) {
    return delegate().join(inner, outerKeySelector,
            innerKeySelector, resultSelector, comparer);
  }

  @Override public <TInner, TKey, TResult> Enumerable<TResult> join(
          Enumerable<TInner> inner,
          Function1<E, TKey> outerKeySelector,
          Function1<TInner, TKey> innerKeySelector,
          Function2<E, TInner, TResult> resultSelector,
          EqualityComparer<TKey> comparer,
          boolean generateNullsOnLeft,
          boolean generateNullsOnRight) {
    return delegate().join(inner, outerKeySelector, innerKeySelector, resultSelector,
            comparer, generateNullsOnLeft, generateNullsOnRight);
  }

  @Override public <TInner, TResult> Enumerable<TResult> correlateJoin(
          CorrelateJoinType joinType,
          Function1<E, Enumerable<TInner>> inner,
          Function2<E, TInner, TResult> resultSelector) {
    return delegate().correlateJoin(joinType, inner, resultSelector);
  }

  @Override public E last() {
    return delegate().last();
  }

  @Override public E last(Predicate1<E> predicate) {
    return delegate().last(predicate);
  }

  @Override public E lastOrDefault() {
    return delegate().lastOrDefault();
  }

  @Override public E lastOrDefault(Predicate1<E> predicate) {
    return delegate().lastOrDefault(predicate);
  }

  @Override public long longCount() {
    return delegate().longCount();
  }

  @Override public long longCount(Predicate1<E> predicate) {
    return delegate().longCount(predicate);
  }

  @Override public E max() {
    return delegate().max();
  }

  @Override public BigDecimal max(BigDecimalFunction1<E> selector) {
    return delegate().max(selector);
  }

  @Override public BigDecimal max(NullableBigDecimalFunction1<E> selector) {
    return delegate().max(selector);
  }

  @Override public double max(DoubleFunction1<E> selector) {
    return delegate().max(selector);
  }

  @Override public Double max(NullableDoubleFunction1<E> selector) {
    return delegate().max(selector);
  }

  @Override public int max(IntegerFunction1<E> selector) {
    return delegate().max(selector);
  }

  @Override public Integer max(NullableIntegerFunction1<E> selector) {
    return delegate().max(selector);
  }

  @Override public long max(LongFunction1<E> selector) {
    return delegate().max(selector);
  }

  @Override public Long max(NullableLongFunction1<E> selector) {
    return delegate().max(selector);
  }

  @Override public float max(FloatFunction1<E> selector) {
    return delegate().max(selector);
  }

  @Override public Float max(NullableFloatFunction1<E> selector) {
    return delegate().max(selector);
  }

  @Override public <TResult extends Comparable<TResult>> TResult max(
          Function1<E, TResult> selector) {
    return delegate().max(selector);
  }

  @Override public E min() {
    return delegate().min();
  }

  @Override public BigDecimal min(BigDecimalFunction1<E> selector) {
    return delegate().min(selector);
  }

  @Override public BigDecimal min(NullableBigDecimalFunction1<E> selector) {
    return delegate().min(selector);
  }

  @Override public double min(DoubleFunction1<E> selector) {
    return delegate().min(selector);
  }

  @Override public Double min(NullableDoubleFunction1<E> selector) {
    return delegate().min(selector);
  }

  @Override public int min(IntegerFunction1<E> selector) {
    return delegate().min(selector);
  }

  @Override public Integer min(NullableIntegerFunction1<E> selector) {
    return delegate().min(selector);
  }

  @Override public long min(LongFunction1<E> selector) {
    return delegate().min(selector);
  }

  @Override public Long min(NullableLongFunction1<E> selector) {
    return delegate().min(selector);
  }

  @Override public float min(FloatFunction1<E> selector) {
    return delegate().min(selector);
  }

  @Override public Float min(NullableFloatFunction1<E> selector) {
    return delegate().min(selector);
  }

  @Override public <TResult extends Comparable<TResult>> TResult min(
          Function1<E, TResult> selector) {
    return delegate().min(selector);
  }

  @Override public <TResult> Enumerable<TResult> ofType(Class<TResult> clazz) {
    return delegate().ofType(clazz);
  }

  @Override public <TKey extends Comparable> Enumerable<E> orderBy(
          Function1<E, TKey> keySelector) {
    return delegate().orderBy(keySelector);
  }

  @Override public <TKey> Enumerable<E> orderBy(
          Function1<E, TKey> keySelector,
          Comparator<TKey> comparator) {
    return delegate().orderBy(keySelector, comparator);
  }

  @Override public <TKey extends Comparable> Enumerable<E> orderByDescending(
          Function1<E, TKey> keySelector) {
    return delegate().orderByDescending(keySelector);
  }

  @Override public <TKey> Enumerable<E> orderByDescending(
          Function1<E, TKey> keySelector,
          Comparator<TKey> comparator) {
    return delegate().orderByDescending(keySelector, comparator);
  }

  @Override public Enumerable<E> reverse() {
    return delegate().reverse();
  }

  @Override public <TResult> Enumerable<TResult> select(Function1<E, TResult> selector) {
    return delegate().select(selector);
  }

  @Override public <TResult> Enumerable<TResult> select(Function2<E, Integer, TResult> selector) {
    return delegate().select(selector);
  }

  @Override public <TResult> Enumerable<TResult> selectMany(
          Function1<E, Enumerable<TResult>> selector) {
    return delegate().selectMany(selector);
  }

  @Override public <TResult> Enumerable<TResult> selectMany(
          Function2<E, Integer, Enumerable<TResult>> selector) {
    return delegate().selectMany(selector);
  }

  @Override public <TCollection, TResult> Enumerable<TResult> selectMany(
          Function2<E, Integer, Enumerable<TCollection>> collectionSelector,
          Function2<E, TCollection, TResult> resultSelector) {
    return delegate().selectMany(collectionSelector, resultSelector);
  }

  @Override public <TCollection, TResult> Enumerable<TResult> selectMany(
          Function1<E, Enumerable<TCollection>> collectionSelector,
          Function2<E, TCollection, TResult> resultSelector) {
    return delegate().selectMany(collectionSelector, resultSelector);
  }

  @Override public boolean sequenceEqual(Enumerable<E> enumerable1) {
    return delegate().sequenceEqual(enumerable1);
  }

  @Override public boolean sequenceEqual(Enumerable<E> enumerable1, EqualityComparer<E> comparer) {
    return delegate().sequenceEqual(enumerable1, comparer);
  }

  @Override public E single() {
    return delegate().single();
  }

  @Override public E single(Predicate1<E> predicate) {
    return delegate().single(predicate);
  }

  @Override public E singleOrDefault() {
    return delegate().singleOrDefault();
  }

  @Override public E singleOrDefault(Predicate1<E> predicate) {
    return delegate().singleOrDefault(predicate);
  }

  @Override public Enumerable<E> skip(int count) {
    return delegate().skip(count);
  }

  @Override public Enumerable<E> skipWhile(Predicate1<E> predicate) {
    return delegate().skipWhile(predicate);
  }

  @Override public Enumerable<E> skipWhile(Predicate2<E, Integer> predicate) {
    return delegate().skipWhile(predicate);
  }

  @Override public BigDecimal sum(BigDecimalFunction1<E> selector) {
    return delegate().sum(selector);
  }

  @Override public BigDecimal sum(NullableBigDecimalFunction1<E> selector) {
    return delegate().sum(selector);
  }

  @Override public double sum(DoubleFunction1<E> selector) {
    return delegate().sum(selector);
  }

  @Override public Double sum(NullableDoubleFunction1<E> selector) {
    return delegate().sum(selector);
  }

  @Override public int sum(IntegerFunction1<E> selector) {
    return delegate().sum(selector);
  }

  @Override public Integer sum(NullableIntegerFunction1<E> selector) {
    return delegate().sum(selector);
  }

  @Override public long sum(LongFunction1<E> selector) {
    return delegate().sum(selector);
  }

  @Override public Long sum(NullableLongFunction1<E> selector) {
    return delegate().sum(selector);
  }

  @Override public float sum(FloatFunction1<E> selector) {
    return delegate().sum(selector);
  }

  @Override public Float sum(NullableFloatFunction1<E> selector) {
    return delegate().sum(selector);
  }

  @Override public Enumerable<E> take(int count) {
    return delegate().take(count);
  }

  @Override public Enumerable<E> takeWhile(Predicate1<E> predicate) {
    return delegate().takeWhile(predicate);
  }

  @Override public Enumerable<E> takeWhile(Predicate2<E, Integer> predicate) {
    return delegate().takeWhile(predicate);
  }

  @Override public <TKey> Map<TKey, E> toMap(Function1<E, TKey> keySelector) {
    return delegate().toMap(keySelector);
  }

  @Override public <TKey> Map<TKey, E> toMap(
          Function1<E, TKey> keySelector,
          EqualityComparer<TKey> comparer) {
    return delegate().toMap(keySelector, comparer);
  }

  @Override public <TKey, TElement> Map<TKey, TElement> toMap(
          Function1<E, TKey> keySelector,
          Function1<E, TElement> elementSelector) {
    return delegate().toMap(keySelector, elementSelector);
  }

  @Override public <TKey, TElement> Map<TKey, TElement> toMap(
          Function1<E, TKey> keySelector,
          Function1<E, TElement> elementSelector,
          EqualityComparer<TKey> comparer) {
    return delegate().toMap(keySelector, elementSelector, comparer);
  }

  @Override public List<E> toList() {
    return delegate().toList();
  }

  @Override public <TKey> Lookup<TKey, E> toLookup(Function1<E, TKey> keySelector) {
    return delegate().toLookup(keySelector);
  }

  @Override public <TKey> Lookup<TKey, E> toLookup(
          Function1<E, TKey> keySelector,
          EqualityComparer<TKey> comparer) {
    return delegate().toLookup(keySelector, comparer);
  }

  @Override public <TKey, TElement> Lookup<TKey, TElement> toLookup(
          Function1<E, TKey> keySelector,
          Function1<E, TElement> elementSelector) {
    return delegate().toLookup(keySelector, elementSelector);
  }

  @Override public <TKey, TElement> Lookup<TKey, TElement> toLookup(
          Function1<E, TKey> keySelector,
          Function1<E, TElement> elementSelector,
          EqualityComparer<TKey> comparer) {
    return delegate().toLookup(keySelector, elementSelector, comparer);
  }

  @Override public Enumerable<E> union(Enumerable<E> source1) {
    return delegate().union(source1);
  }

  @Override public Enumerable<E> union(Enumerable<E> source1, EqualityComparer<E> comparer) {
    return delegate().union(source1, comparer);
  }

  @Override public Enumerable<E> where(Predicate1<E> predicate) {
    return delegate().where(predicate);
  }

  @Override public Enumerable<E> where(Predicate2<E, Integer> predicate) {
    return delegate().where(predicate);
  }

  @Override public <T1, TResult> Enumerable<TResult> zip(
          Enumerable<T1> source1,
          Function2<E, T1, TResult> resultSelector) {
    return delegate().zip(source1, resultSelector);
  }
}

// End ForwardingEnumerable.java
