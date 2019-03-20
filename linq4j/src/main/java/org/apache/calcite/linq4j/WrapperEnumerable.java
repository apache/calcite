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
 * Abstract implementation of the {@link Enumerable} interface that
 * forwards all methods to another wrappee {@link Enumerable}.
 *
 * <p>There is one abstract method: {@link #getEnumerable()}, to be implemented by
 * the derived class, that will be used to obtained the wrappee {@link Enumerable}.</p>
 *
 * @param <E> Element type
 */
public abstract class WrapperEnumerable<E> implements Enumerable<E> {

  /**
   * @return the wrapee {@link Enumerable} where all methods will be forwarded to
   */
  protected abstract Enumerable<E> getEnumerable();

  @Override public Enumerator<E> enumerator() {
    return getEnumerable().enumerator();
  }

  @Override public Iterator<E> iterator() {
    return getEnumerable().iterator();
  }

  @Override public <R> R foreach(Function1<E, R> func) {
    return getEnumerable().foreach(func);
  }

  @Override public E aggregate(Function2<E, E, E> func) {
    return getEnumerable().aggregate(func);
  }

  @Override public <TAccumulate> TAccumulate aggregate(
          TAccumulate seed,
          Function2<TAccumulate, E, TAccumulate> func) {
    return getEnumerable().aggregate(seed, func);
  }

  @Override public <TAccumulate, TResult> TResult aggregate(
          TAccumulate seed,
          Function2<TAccumulate, E, TAccumulate> func,
          Function1<TAccumulate, TResult> selector) {
    return getEnumerable().aggregate(seed, func, selector);
  }

  @Override public boolean all(Predicate1<E> predicate) {
    return getEnumerable().all(predicate);
  }

  @Override public boolean any() {
    return getEnumerable().any();
  }

  @Override public boolean any(Predicate1<E> predicate) {
    return getEnumerable().any(predicate);
  }

  @Override public Enumerable<E> asEnumerable() {
    return getEnumerable().asEnumerable();
  }

  @Override public Queryable<E> asQueryable() {
    return getEnumerable().asQueryable();
  }

  @Override public BigDecimal average(BigDecimalFunction1<E> selector) {
    return getEnumerable().average(selector);
  }

  @Override public BigDecimal average(NullableBigDecimalFunction1<E> selector) {
    return getEnumerable().average(selector);
  }

  @Override public double average(DoubleFunction1<E> selector) {
    return getEnumerable().average(selector);
  }

  @Override public Double average(NullableDoubleFunction1<E> selector) {
    return getEnumerable().average(selector);
  }

  @Override public int average(IntegerFunction1<E> selector) {
    return getEnumerable().average(selector);
  }

  @Override public Integer average(NullableIntegerFunction1<E> selector) {
    return getEnumerable().average(selector);
  }

  @Override public long average(LongFunction1<E> selector) {
    return getEnumerable().average(selector);
  }

  @Override public Long average(NullableLongFunction1<E> selector) {
    return getEnumerable().average(selector);
  }

  @Override public float average(FloatFunction1<E> selector) {
    return getEnumerable().average(selector);
  }

  @Override public Float average(NullableFloatFunction1<E> selector) {
    return getEnumerable().average(selector);
  }

  @Override public <T2> Enumerable<T2> cast(Class<T2> clazz) {
    return getEnumerable().cast(clazz);
  }

  @Override public Enumerable<E> concat(Enumerable<E> enumerable1) {
    return getEnumerable().concat(enumerable1);
  }

  @Override public boolean contains(E element) {
    return getEnumerable().contains(element);
  }

  @Override public boolean contains(E element, EqualityComparer<E> comparer) {
    return getEnumerable().contains(element, comparer);
  }

  @Override public int count() {
    return getEnumerable().count();
  }

  @Override public int count(Predicate1<E> predicate) {
    return getEnumerable().count(predicate);
  }

  @Override public Enumerable<E> defaultIfEmpty() {
    return getEnumerable().defaultIfEmpty();
  }

  @Override public Enumerable<E> defaultIfEmpty(E value) {
    return getEnumerable().defaultIfEmpty(value);
  }

  @Override public Enumerable<E> distinct() {
    return getEnumerable().distinct();
  }

  @Override public Enumerable<E> distinct(EqualityComparer<E> comparer) {
    return getEnumerable().distinct(comparer);
  }

  @Override public E elementAt(int index) {
    return getEnumerable().elementAt(index);
  }

  @Override public E elementAtOrDefault(int index) {
    return getEnumerable().elementAtOrDefault(index);
  }

  @Override public Enumerable<E> except(Enumerable<E> enumerable1) {
    return getEnumerable().except(enumerable1);
  }

  @Override public Enumerable<E> except(Enumerable<E> enumerable1, EqualityComparer<E> comparer) {
    return getEnumerable().except(enumerable1, comparer);
  }

  @Override public E first() {
    return getEnumerable().first();
  }

  @Override public E first(Predicate1<E> predicate) {
    return getEnumerable().first(predicate);
  }

  @Override public E firstOrDefault() {
    return getEnumerable().firstOrDefault();
  }

  @Override public E firstOrDefault(Predicate1<E> predicate) {
    return getEnumerable().firstOrDefault(predicate);
  }

  @Override public <TKey> Enumerable<Grouping<TKey, E>> groupBy(Function1<E, TKey> keySelector) {
    return getEnumerable().groupBy(keySelector);
  }

  @Override public <TKey> Enumerable<Grouping<TKey, E>> groupBy(
          Function1<E, TKey> keySelector,
          EqualityComparer<TKey> comparer) {
    return getEnumerable().groupBy(keySelector, comparer);
  }

  @Override public <TKey, TElement> Enumerable<Grouping<TKey, TElement>> groupBy(
          Function1<E, TKey> keySelector,
          Function1<E, TElement> elementSelector) {
    return getEnumerable().groupBy(keySelector, elementSelector);
  }

  @Override public <TKey, TElement> Enumerable<Grouping<TKey, TElement>> groupBy(
          Function1<E, TKey> keySelector,
          Function1<E, TElement> elementSelector,
          EqualityComparer<TKey> comparer) {
    return getEnumerable().groupBy(keySelector, elementSelector, comparer);
  }

  @Override public <TKey, TResult> Enumerable<TResult> groupBy(
          Function1<E, TKey> keySelector,
          Function2<TKey, Enumerable<E>, TResult> resultSelector) {
    return getEnumerable().groupBy(keySelector, resultSelector);
  }

  @Override public <TKey, TResult> Enumerable<TResult> groupBy(
          Function1<E, TKey> keySelector,
          Function2<TKey, Enumerable<E>, TResult> resultSelector,
          EqualityComparer<TKey> comparer) {
    return getEnumerable().groupBy(keySelector, resultSelector, comparer);
  }

  @Override public <TKey, TElement, TResult> Enumerable<TResult> groupBy(
          Function1<E, TKey> keySelector,
          Function1<E, TElement> elementSelector,
          Function2<TKey, Enumerable<TElement>, TResult> resultSelector) {
    return getEnumerable().groupBy(keySelector, elementSelector, resultSelector);
  }

  @Override public <TKey, TElement, TResult> Enumerable<TResult> groupBy(
          Function1<E, TKey> keySelector,
          Function1<E, TElement> elementSelector,
          Function2<TKey, Enumerable<TElement>, TResult> resultSelector,
          EqualityComparer<TKey> comparer) {
    return getEnumerable().groupBy(keySelector, elementSelector, resultSelector, comparer);
  }

  @Override public <TKey, TAccumulate, TResult> Enumerable<TResult> groupBy(
          Function1<E, TKey> keySelector,
          Function0<TAccumulate> accumulatorInitializer,
          Function2<TAccumulate, E, TAccumulate> accumulatorAdder,
          Function2<TKey, TAccumulate, TResult> resultSelector) {
    return getEnumerable().groupBy(keySelector, accumulatorInitializer,
            accumulatorAdder, resultSelector);
  }

  @Override public <TKey, TAccumulate, TResult> Enumerable<TResult> groupBy(
          Function1<E, TKey> keySelector,
          Function0<TAccumulate> accumulatorInitializer,
          Function2<TAccumulate, E, TAccumulate> accumulatorAdder,
          Function2<TKey, TAccumulate, TResult> resultSelector,
          EqualityComparer<TKey> comparer) {
    return getEnumerable().groupBy(keySelector, accumulatorInitializer, accumulatorAdder,
            resultSelector, comparer);
  }

  @Override public <TInner, TKey, TResult> Enumerable<TResult> groupJoin(
          Enumerable<TInner> inner,
          Function1<E, TKey> outerKeySelector,
          Function1<TInner, TKey> innerKeySelector,
          Function2<E, Enumerable<TInner>, TResult> resultSelector) {
    return getEnumerable().groupJoin(inner, outerKeySelector, innerKeySelector, resultSelector);
  }

  @Override public <TInner, TKey, TResult> Enumerable<TResult> groupJoin(
          Enumerable<TInner> inner,
          Function1<E, TKey> outerKeySelector,
          Function1<TInner, TKey> innerKeySelector,
          Function2<E, Enumerable<TInner>, TResult> resultSelector,
          EqualityComparer<TKey> comparer) {
    return getEnumerable().groupJoin(inner, outerKeySelector, innerKeySelector,
            resultSelector, comparer);
  }

  @Override public Enumerable<E> intersect(Enumerable<E> intersect) {
    return getEnumerable().intersect(intersect);
  }

  @Override public Enumerable<E> intersect(
          Enumerable<E> enumerable1,
          EqualityComparer<E> comparer) {
    return getEnumerable().intersect(enumerable1, comparer);
  }

  @Override public <C extends Collection<? super E>> C into(C sink) {
    return getEnumerable().into(sink);
  }

  @Override public <C extends Collection<? super E>> C removeAll(C sink) {
    return getEnumerable().removeAll(sink);
  }

  @Override public <TInner, TKey, TResult> Enumerable<TResult> join(
          Enumerable<TInner> inner,
          Function1<E, TKey> outerKeySelector,
          Function1<TInner, TKey> innerKeySelector,
          Function2<E, TInner, TResult> resultSelector) {
    return getEnumerable().join(inner, outerKeySelector, innerKeySelector, resultSelector);
  }

  @Override public <TInner, TKey, TResult> Enumerable<TResult> join(
          Enumerable<TInner> inner,
          Function1<E, TKey> outerKeySelector,
          Function1<TInner, TKey> innerKeySelector,
          Function2<E, TInner, TResult> resultSelector,
          EqualityComparer<TKey> comparer) {
    return getEnumerable().join(inner, outerKeySelector,
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
    return getEnumerable().join(inner, outerKeySelector, innerKeySelector, resultSelector,
            comparer, generateNullsOnLeft, generateNullsOnRight);
  }

  @Override public <TInner, TResult> Enumerable<TResult> correlateJoin(
          CorrelateJoinType joinType,
          Function1<E, Enumerable<TInner>> inner,
          Function2<E, TInner, TResult> resultSelector) {
    return getEnumerable().correlateJoin(joinType, inner, resultSelector);
  }

  @Override public E last() {
    return getEnumerable().last();
  }

  @Override public E last(Predicate1<E> predicate) {
    return getEnumerable().last(predicate);
  }

  @Override public E lastOrDefault() {
    return getEnumerable().lastOrDefault();
  }

  @Override public E lastOrDefault(Predicate1<E> predicate) {
    return getEnumerable().lastOrDefault(predicate);
  }

  @Override public long longCount() {
    return getEnumerable().longCount();
  }

  @Override public long longCount(Predicate1<E> predicate) {
    return getEnumerable().longCount(predicate);
  }

  @Override public E max() {
    return getEnumerable().max();
  }

  @Override public BigDecimal max(BigDecimalFunction1<E> selector) {
    return getEnumerable().max(selector);
  }

  @Override public BigDecimal max(NullableBigDecimalFunction1<E> selector) {
    return getEnumerable().max(selector);
  }

  @Override public double max(DoubleFunction1<E> selector) {
    return getEnumerable().max(selector);
  }

  @Override public Double max(NullableDoubleFunction1<E> selector) {
    return getEnumerable().max(selector);
  }

  @Override public int max(IntegerFunction1<E> selector) {
    return getEnumerable().max(selector);
  }

  @Override public Integer max(NullableIntegerFunction1<E> selector) {
    return getEnumerable().max(selector);
  }

  @Override public long max(LongFunction1<E> selector) {
    return getEnumerable().max(selector);
  }

  @Override public Long max(NullableLongFunction1<E> selector) {
    return getEnumerable().max(selector);
  }

  @Override public float max(FloatFunction1<E> selector) {
    return getEnumerable().max(selector);
  }

  @Override public Float max(NullableFloatFunction1<E> selector) {
    return getEnumerable().max(selector);
  }

  @Override public <TResult extends Comparable<TResult>> TResult max(
          Function1<E, TResult> selector) {
    return getEnumerable().max(selector);
  }

  @Override public E min() {
    return getEnumerable().min();
  }

  @Override public BigDecimal min(BigDecimalFunction1<E> selector) {
    return getEnumerable().min(selector);
  }

  @Override public BigDecimal min(NullableBigDecimalFunction1<E> selector) {
    return getEnumerable().min(selector);
  }

  @Override public double min(DoubleFunction1<E> selector) {
    return getEnumerable().min(selector);
  }

  @Override public Double min(NullableDoubleFunction1<E> selector) {
    return getEnumerable().min(selector);
  }

  @Override public int min(IntegerFunction1<E> selector) {
    return getEnumerable().min(selector);
  }

  @Override public Integer min(NullableIntegerFunction1<E> selector) {
    return getEnumerable().min(selector);
  }

  @Override public long min(LongFunction1<E> selector) {
    return getEnumerable().min(selector);
  }

  @Override public Long min(NullableLongFunction1<E> selector) {
    return getEnumerable().min(selector);
  }

  @Override public float min(FloatFunction1<E> selector) {
    return getEnumerable().min(selector);
  }

  @Override public Float min(NullableFloatFunction1<E> selector) {
    return getEnumerable().min(selector);
  }

  @Override public <TResult extends Comparable<TResult>> TResult min(
          Function1<E, TResult> selector) {
    return getEnumerable().min(selector);
  }

  @Override public <TResult> Enumerable<TResult> ofType(Class<TResult> clazz) {
    return getEnumerable().ofType(clazz);
  }

  @Override public <TKey extends Comparable> Enumerable<E> orderBy(
          Function1<E, TKey> keySelector) {
    return getEnumerable().orderBy(keySelector);
  }

  @Override public <TKey> Enumerable<E> orderBy(
          Function1<E, TKey> keySelector,
          Comparator<TKey> comparator) {
    return getEnumerable().orderBy(keySelector, comparator);
  }

  @Override public <TKey extends Comparable> Enumerable<E> orderByDescending(
          Function1<E, TKey> keySelector) {
    return getEnumerable().orderByDescending(keySelector);
  }

  @Override public <TKey> Enumerable<E> orderByDescending(
          Function1<E, TKey> keySelector,
          Comparator<TKey> comparator) {
    return getEnumerable().orderByDescending(keySelector, comparator);
  }

  @Override public Enumerable<E> reverse() {
    return getEnumerable().reverse();
  }

  @Override public <TResult> Enumerable<TResult> select(Function1<E, TResult> selector) {
    return getEnumerable().select(selector);
  }

  @Override public <TResult> Enumerable<TResult> select(Function2<E, Integer, TResult> selector) {
    return getEnumerable().select(selector);
  }

  @Override public <TResult> Enumerable<TResult> selectMany(
          Function1<E, Enumerable<TResult>> selector) {
    return getEnumerable().selectMany(selector);
  }

  @Override public <TResult> Enumerable<TResult> selectMany(
          Function2<E, Integer, Enumerable<TResult>> selector) {
    return getEnumerable().selectMany(selector);
  }

  @Override public <TCollection, TResult> Enumerable<TResult> selectMany(
          Function2<E, Integer, Enumerable<TCollection>> collectionSelector,
          Function2<E, TCollection, TResult> resultSelector) {
    return getEnumerable().selectMany(collectionSelector, resultSelector);
  }

  @Override public <TCollection, TResult> Enumerable<TResult> selectMany(
          Function1<E, Enumerable<TCollection>> collectionSelector,
          Function2<E, TCollection, TResult> resultSelector) {
    return getEnumerable().selectMany(collectionSelector, resultSelector);
  }

  @Override public boolean sequenceEqual(Enumerable<E> enumerable1) {
    return getEnumerable().sequenceEqual(enumerable1);
  }

  @Override public boolean sequenceEqual(Enumerable<E> enumerable1, EqualityComparer<E> comparer) {
    return getEnumerable().sequenceEqual(enumerable1, comparer);
  }

  @Override public E single() {
    return getEnumerable().single();
  }

  @Override public E single(Predicate1<E> predicate) {
    return getEnumerable().single(predicate);
  }

  @Override public E singleOrDefault() {
    return getEnumerable().singleOrDefault();
  }

  @Override public E singleOrDefault(Predicate1<E> predicate) {
    return getEnumerable().singleOrDefault(predicate);
  }

  @Override public Enumerable<E> skip(int count) {
    return getEnumerable().skip(count);
  }

  @Override public Enumerable<E> skipWhile(Predicate1<E> predicate) {
    return getEnumerable().skipWhile(predicate);
  }

  @Override public Enumerable<E> skipWhile(Predicate2<E, Integer> predicate) {
    return getEnumerable().skipWhile(predicate);
  }

  @Override public BigDecimal sum(BigDecimalFunction1<E> selector) {
    return getEnumerable().sum(selector);
  }

  @Override public BigDecimal sum(NullableBigDecimalFunction1<E> selector) {
    return getEnumerable().sum(selector);
  }

  @Override public double sum(DoubleFunction1<E> selector) {
    return getEnumerable().sum(selector);
  }

  @Override public Double sum(NullableDoubleFunction1<E> selector) {
    return getEnumerable().sum(selector);
  }

  @Override public int sum(IntegerFunction1<E> selector) {
    return getEnumerable().sum(selector);
  }

  @Override public Integer sum(NullableIntegerFunction1<E> selector) {
    return getEnumerable().sum(selector);
  }

  @Override public long sum(LongFunction1<E> selector) {
    return getEnumerable().sum(selector);
  }

  @Override public Long sum(NullableLongFunction1<E> selector) {
    return getEnumerable().sum(selector);
  }

  @Override public float sum(FloatFunction1<E> selector) {
    return getEnumerable().sum(selector);
  }

  @Override public Float sum(NullableFloatFunction1<E> selector) {
    return getEnumerable().sum(selector);
  }

  @Override public Enumerable<E> take(int count) {
    return getEnumerable().take(count);
  }

  @Override public Enumerable<E> takeWhile(Predicate1<E> predicate) {
    return getEnumerable().takeWhile(predicate);
  }

  @Override public Enumerable<E> takeWhile(Predicate2<E, Integer> predicate) {
    return getEnumerable().takeWhile(predicate);
  }

  @Override public <TKey> Map<TKey, E> toMap(Function1<E, TKey> keySelector) {
    return getEnumerable().toMap(keySelector);
  }

  @Override public <TKey> Map<TKey, E> toMap(
          Function1<E, TKey> keySelector,
          EqualityComparer<TKey> comparer) {
    return getEnumerable().toMap(keySelector, comparer);
  }

  @Override public <TKey, TElement> Map<TKey, TElement> toMap(
          Function1<E, TKey> keySelector,
          Function1<E, TElement> elementSelector) {
    return getEnumerable().toMap(keySelector, elementSelector);
  }

  @Override public <TKey, TElement> Map<TKey, TElement> toMap(
          Function1<E, TKey> keySelector,
          Function1<E, TElement> elementSelector,
          EqualityComparer<TKey> comparer) {
    return getEnumerable().toMap(keySelector, elementSelector, comparer);
  }

  @Override public List<E> toList() {
    return getEnumerable().toList();
  }

  @Override public <TKey> Lookup<TKey, E> toLookup(Function1<E, TKey> keySelector) {
    return getEnumerable().toLookup(keySelector);
  }

  @Override public <TKey> Lookup<TKey, E> toLookup(
          Function1<E, TKey> keySelector,
          EqualityComparer<TKey> comparer) {
    return getEnumerable().toLookup(keySelector, comparer);
  }

  @Override public <TKey, TElement> Lookup<TKey, TElement> toLookup(
          Function1<E, TKey> keySelector,
          Function1<E, TElement> elementSelector) {
    return getEnumerable().toLookup(keySelector, elementSelector);
  }

  @Override public <TKey, TElement> Lookup<TKey, TElement> toLookup(
          Function1<E, TKey> keySelector,
          Function1<E, TElement> elementSelector,
          EqualityComparer<TKey> comparer) {
    return getEnumerable().toLookup(keySelector, elementSelector, comparer);
  }

  @Override public Enumerable<E> union(Enumerable<E> source1) {
    return getEnumerable().union(source1);
  }

  @Override public Enumerable<E> union(Enumerable<E> source1, EqualityComparer<E> comparer) {
    return getEnumerable().union(source1, comparer);
  }

  @Override public Enumerable<E> where(Predicate1<E> predicate) {
    return getEnumerable().where(predicate);
  }

  @Override public Enumerable<E> where(Predicate2<E, Integer> predicate) {
    return getEnumerable().where(predicate);
  }

  @Override public <T1, TResult> Enumerable<TResult> zip(
          Enumerable<T1> source1,
          Function2<E, T1, TResult> resultSelector) {
    return getEnumerable().zip(source1, resultSelector);
  }
}

// End WrapperEnumerable.java
