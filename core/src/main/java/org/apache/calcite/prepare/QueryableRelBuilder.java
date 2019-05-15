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
package org.apache.calcite.prepare;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Grouping;
import org.apache.calcite.linq4j.OrderedQueryable;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.QueryableDefaults;
import org.apache.calcite.linq4j.QueryableFactory;
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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTableQueryable;

import java.math.BigDecimal;
import java.util.Comparator;
import java.util.List;

/**
 * Implementation of {@link QueryableFactory}
 * that builds a tree of {@link RelNode} planner nodes. Used by
 * {@link LixToRelTranslator}.
 *
 * <p>Each of the methods that implements a {@code Replayer} method creates
 * a tree of {@code RelNode}s equivalent to the arguments, and calls
 * {@link #setRel} to assign the root of that tree to the {@link #rel} member
 * variable.</p>
 *
 * <p>To comply with the {@link org.apache.calcite.linq4j.QueryableFactory}
 * interface, which is after all a factory, each method returns a dummy result
 * such as {@code null} or {@code 0}.
 * The caller will not use the result.
 * The real effect of the method is to
 * call {@link #setRel} with a {@code RelNode}.</p>
 *
 * <p>NOTE: Many methods currently throw {@link UnsupportedOperationException}.
 * These method need to be implemented.</p>
 *
 * @param <T> Element type
 */
class QueryableRelBuilder<T> implements QueryableFactory<T> {
  private final LixToRelTranslator translator;
  private RelNode rel;

  QueryableRelBuilder(LixToRelTranslator translator) {
    this.translator = translator;
  }

  RelNode toRel(Queryable<T> queryable) {
    if (queryable instanceof QueryableDefaults.Replayable) {
      //noinspection unchecked
      ((QueryableDefaults.Replayable) queryable).replay(this);
      return rel;
    }
    if (queryable instanceof AbstractTableQueryable) {
      final AbstractTableQueryable tableQueryable =
          (AbstractTableQueryable) queryable;
      final QueryableTable table = tableQueryable.table;
      final CalciteSchema.TableEntry tableEntry =
          CalciteSchema.from(tableQueryable.schema)
              .add(tableQueryable.tableName, tableQueryable.table);
      final RelOptTableImpl relOptTable =
          RelOptTableImpl.create(null, table.getRowType(translator.typeFactory),
              tableEntry, null);
      if (table instanceof TranslatableTable) {
        return ((TranslatableTable) table).toRel(translator.toRelContext(),
            relOptTable);
      } else {
        return LogicalTableScan.create(translator.cluster, relOptTable);
      }
    }
    return translator.translate(queryable.getExpression());
  }

  /** Sets the output of this event. */
  private void setRel(RelNode rel) {
    this.rel = rel;
  }

  // ~ Methods from QueryableFactory -----------------------------------------

  public <TAccumulate, TResult> TResult aggregate(
      Queryable<T> source,
      TAccumulate seed,
      FunctionExpression<Function2<TAccumulate, T, TAccumulate>> func,
      FunctionExpression<Function1<TAccumulate, TResult>> selector) {
    throw new UnsupportedOperationException();
  }

  public T aggregate(
      Queryable<T> source,
      FunctionExpression<Function2<T, T, T>> selector) {
    throw new UnsupportedOperationException();
  }

  public <TAccumulate> TAccumulate aggregate(
      Queryable<T> source,
      TAccumulate seed,
      FunctionExpression<Function2<TAccumulate, T, TAccumulate>> selector) {
    throw new UnsupportedOperationException();
  }

  public boolean all(
      Queryable<T> source,
      FunctionExpression<Predicate1<T>> predicate) {
    throw new UnsupportedOperationException();
  }

  public boolean any(Queryable<T> source) {
    throw new UnsupportedOperationException();
  }

  public boolean any(
      Queryable<T> source,
      FunctionExpression<Predicate1<T>> predicate) {
    throw new UnsupportedOperationException();
  }

  public BigDecimal averageBigDecimal(
      Queryable<T> source,
      FunctionExpression<BigDecimalFunction1<T>> selector) {
    throw new UnsupportedOperationException();
  }

  public BigDecimal averageNullableBigDecimal(
      Queryable<T> source,
      FunctionExpression<NullableBigDecimalFunction1<T>> selector) {
    throw new UnsupportedOperationException();
  }

  public double averageDouble(
      Queryable<T> source,
      FunctionExpression<DoubleFunction1<T>> selector) {
    throw new UnsupportedOperationException();
  }

  public Double averageNullableDouble(
      Queryable<T> source,
      FunctionExpression<NullableDoubleFunction1<T>> selector) {
    throw new UnsupportedOperationException();
  }

  public int averageInteger(
      Queryable<T> source,
      FunctionExpression<IntegerFunction1<T>> selector) {
    throw new UnsupportedOperationException();
  }

  public Integer averageNullableInteger(
      Queryable<T> source,
      FunctionExpression<NullableIntegerFunction1<T>> selector) {
    throw new UnsupportedOperationException();
  }

  public float averageFloat(
      Queryable<T> source,
      FunctionExpression<FloatFunction1<T>> selector) {
    throw new UnsupportedOperationException();
  }

  public Float averageNullableFloat(
      Queryable<T> source,
      FunctionExpression<NullableFloatFunction1<T>> selector) {
    throw new UnsupportedOperationException();
  }

  public long averageLong(
      Queryable<T> source,
      FunctionExpression<LongFunction1<T>> selector) {
    throw new UnsupportedOperationException();
  }

  public Long averageNullableLong(
      Queryable<T> source,
      FunctionExpression<NullableLongFunction1<T>> selector) {
    throw new UnsupportedOperationException();
  }

  public Queryable<T> concat(
      Queryable<T> source, Enumerable<T> source2) {
    throw new UnsupportedOperationException();
  }

  public boolean contains(
      Queryable<T> source, T element) {
    throw new UnsupportedOperationException();
  }

  public boolean contains(
      Queryable<T> source, T element, EqualityComparer<T> comparer) {
    throw new UnsupportedOperationException();
  }

  public int count(Queryable<T> source) {
    throw new UnsupportedOperationException();
  }

  public int count(
      Queryable<T> source,
      FunctionExpression<Predicate1<T>> predicate) {
    throw new UnsupportedOperationException();
  }

  public Queryable<T> defaultIfEmpty(Queryable<T> source) {
    throw new UnsupportedOperationException();
  }

  public Queryable<T> defaultIfEmpty(Queryable<T> source, T value) {
    throw new UnsupportedOperationException();
  }

  public Queryable<T> distinct(
      Queryable<T> source) {
    throw new UnsupportedOperationException();
  }

  public Queryable<T> distinct(
      Queryable<T> source, EqualityComparer<T> comparer) {
    throw new UnsupportedOperationException();
  }

  public T elementAt(Queryable<T> source, int index) {
    throw new UnsupportedOperationException();
  }

  public T elementAtOrDefault(Queryable<T> source, int index) {
    throw new UnsupportedOperationException();
  }

  public Queryable<T> except(
      Queryable<T> source, Enumerable<T> enumerable) {
    throw new UnsupportedOperationException();
  }

  public Queryable<T> except(
      Queryable<T> source,
      Enumerable<T> enumerable,
      EqualityComparer<T> tEqualityComparer) {
    throw new UnsupportedOperationException();
  }

  public T first(Queryable<T> source) {
    throw new UnsupportedOperationException();
  }

  public T first(
      Queryable<T> source,
      FunctionExpression<Predicate1<T>> predicate) {
    throw new UnsupportedOperationException();
  }

  public T firstOrDefault(
      Queryable<T> source) {
    throw new UnsupportedOperationException();
  }

  public T firstOrDefault(
      Queryable<T> source,
      FunctionExpression<Predicate1<T>> predicate) {
    throw new UnsupportedOperationException();
  }

  public <TKey> Queryable<Grouping<TKey, T>> groupBy(
      Queryable<T> source,
      FunctionExpression<Function1<T, TKey>> keySelector) {
    throw new UnsupportedOperationException();
  }

  public <TKey> Queryable<Grouping<TKey, T>> groupBy(
      Queryable<T> source,
      FunctionExpression<Function1<T, TKey>> keySelector,
      EqualityComparer<TKey> comparer) {
    throw new UnsupportedOperationException();
  }

  public <TKey, TElement> Queryable<Grouping<TKey, TElement>> groupBy(
      Queryable<T> source,
      FunctionExpression<Function1<T, TKey>> keySelector,
      FunctionExpression<Function1<T, TElement>> elementSelector) {
    throw new UnsupportedOperationException();
  }

  public <TKey, TResult> Queryable<TResult> groupByK(
      Queryable<T> source,
      FunctionExpression<Function1<T, TKey>> keySelector,
      FunctionExpression<Function2<TKey, Enumerable<T>, TResult>>
          resultSelector) {
    throw new UnsupportedOperationException();
  }

  public <TKey, TElement> Queryable<Grouping<TKey, TElement>> groupBy(
      Queryable<T> source,
      FunctionExpression<Function1<T, TKey>> keySelector,
      FunctionExpression<Function1<T, TElement>> elementSelector,
      EqualityComparer<TKey> comparer) {
    throw new UnsupportedOperationException();
  }

  public <TKey, TResult> Queryable<TResult> groupByK(
      Queryable<T> source,
      FunctionExpression<Function1<T, TKey>> keySelector,
      FunctionExpression<Function2<TKey, Enumerable<T>, TResult>>
          elementSelector,
      EqualityComparer<TKey> comparer) {
    throw new UnsupportedOperationException();
  }

  public <TKey, TElement, TResult> Queryable<TResult> groupBy(
      Queryable<T> source,
      FunctionExpression<Function1<T, TKey>> keySelector,
      FunctionExpression<Function1<T, TElement>> elementSelector,
      FunctionExpression<Function2<TKey, Enumerable<TElement>, TResult>>
          resultSelector) {
    throw new UnsupportedOperationException();
  }

  public <TKey, TElement, TResult> Queryable<TResult> groupBy(
      Queryable<T> source,
      FunctionExpression<Function1<T, TKey>> keySelector,
      FunctionExpression<Function1<T, TElement>> elementSelector,
      FunctionExpression<Function2<TKey, Enumerable<TElement>, TResult>>
          resultSelector,
      EqualityComparer<TKey> comparer) {
    throw new UnsupportedOperationException();
  }

  public <TInner, TKey, TResult> Queryable<TResult> groupJoin(
      Queryable<T> source,
      Enumerable<TInner> inner,
      FunctionExpression<Function1<T, TKey>> outerKeySelector,
      FunctionExpression<Function1<TInner, TKey>> innerKeySelector,
      FunctionExpression<Function2<T, Enumerable<TInner>, TResult>>
          resultSelector) {
    throw new UnsupportedOperationException();
  }

  public <TInner, TKey, TResult> Queryable<TResult> groupJoin(
      Queryable<T> source,
      Enumerable<TInner> inner,
      FunctionExpression<Function1<T, TKey>> outerKeySelector,
      FunctionExpression<Function1<TInner, TKey>> innerKeySelector,
      FunctionExpression<Function2<T, Enumerable<TInner>, TResult>>
          resultSelector,
      EqualityComparer<TKey> comparer) {
    throw new UnsupportedOperationException();
  }

  public Queryable<T> intersect(
      Queryable<T> source, Enumerable<T> enumerable) {
    throw new UnsupportedOperationException();
  }

  public Queryable<T> intersect(
      Queryable<T> source,
      Enumerable<T> enumerable,
      EqualityComparer<T> tEqualityComparer) {
    throw new UnsupportedOperationException();
  }

  public <TInner, TKey, TResult> Queryable<TResult> join(
      Queryable<T> source,
      Enumerable<TInner> inner,
      FunctionExpression<Function1<T, TKey>> outerKeySelector,
      FunctionExpression<Function1<TInner, TKey>> innerKeySelector,
      FunctionExpression<Function2<T, TInner, TResult>> resultSelector) {
    throw new UnsupportedOperationException();
  }

  public <TInner, TKey, TResult> Queryable<TResult> join(
      Queryable<T> source,
      Enumerable<TInner> inner,
      FunctionExpression<Function1<T, TKey>> outerKeySelector,
      FunctionExpression<Function1<TInner, TKey>> innerKeySelector,
      FunctionExpression<Function2<T, TInner, TResult>> resultSelector,
      EqualityComparer<TKey> comparer) {
    throw new UnsupportedOperationException();
  }

  public T last(Queryable<T> source) {
    throw new UnsupportedOperationException();
  }

  public T last(
      Queryable<T> source,
      FunctionExpression<Predicate1<T>> predicate) {
    throw new UnsupportedOperationException();
  }

  public T lastOrDefault(
      Queryable<T> source) {
    throw new UnsupportedOperationException();
  }

  public T lastOrDefault(
      Queryable<T> source,
      FunctionExpression<Predicate1<T>> predicate) {
    throw new UnsupportedOperationException();
  }

  public long longCount(Queryable<T> source) {
    throw new UnsupportedOperationException();
  }

  public long longCount(
      Queryable<T> source,
      FunctionExpression<Predicate1<T>> predicate) {
    throw new UnsupportedOperationException();
  }

  public T max(Queryable<T> source) {
    throw new UnsupportedOperationException();
  }

  public <TResult extends Comparable<TResult>> TResult max(
      Queryable<T> source,
      FunctionExpression<Function1<T, TResult>> selector) {
    throw new UnsupportedOperationException();
  }

  public T min(Queryable<T> source) {
    throw new UnsupportedOperationException();
  }

  public <TResult extends Comparable<TResult>> TResult min(
      Queryable<T> source,
      FunctionExpression<Function1<T, TResult>> selector) {
    throw new UnsupportedOperationException();
  }

  public <TResult> Queryable<TResult> ofType(
      Queryable<T> source, Class<TResult> clazz) {
    throw new UnsupportedOperationException();
  }

  public <T2> Queryable<T2> cast(
      Queryable<T> source,
      Class<T2> clazz) {
    throw new UnsupportedOperationException();
  }

  public <TKey extends Comparable> OrderedQueryable<T> orderBy(
      Queryable<T> source,
      FunctionExpression<Function1<T, TKey>> keySelector) {
    throw new UnsupportedOperationException();
  }

  public <TKey> OrderedQueryable<T> orderBy(
      Queryable<T> source,
      FunctionExpression<Function1<T, TKey>> keySelector,
      Comparator<TKey> comparator) {
    throw new UnsupportedOperationException();
  }

  public <TKey extends Comparable> OrderedQueryable<T> orderByDescending(
      Queryable<T> source,
      FunctionExpression<Function1<T, TKey>> keySelector) {
    throw new UnsupportedOperationException();
  }

  public <TKey> OrderedQueryable<T> orderByDescending(
      Queryable<T> source,
      FunctionExpression<Function1<T, TKey>> keySelector,
      Comparator<TKey> comparator) {
    throw new UnsupportedOperationException();
  }

  public Queryable<T> reverse(
      Queryable<T> source) {
    throw new UnsupportedOperationException();
  }

  public <TResult> Queryable<TResult> select(
      Queryable<T> source,
      FunctionExpression<Function1<T, TResult>> selector) {
    RelNode child = toRel(source);
    List<RexNode> nodes = translator.toRexList(selector, child);
    setRel(
        LogicalProject.create(child, nodes, (List<String>)  null));
    return null;
  }

  public <TResult> Queryable<TResult> selectN(
      Queryable<T> source,
      FunctionExpression<Function2<T, Integer, TResult>> selector) {
    throw new UnsupportedOperationException();
  }

  public <TResult> Queryable<TResult> selectMany(
      Queryable<T> source,
      FunctionExpression<Function1<T, Enumerable<TResult>>> selector) {
    throw new UnsupportedOperationException();
  }

  public <TResult> Queryable<TResult> selectManyN(
      Queryable<T> source,
      FunctionExpression<Function2<T, Integer, Enumerable<TResult>>> selector) {
    throw new UnsupportedOperationException();
  }

  public <TCollection, TResult> Queryable<TResult> selectMany(
      Queryable<T> source,
      FunctionExpression<Function2<T, Integer, Enumerable<TCollection>>>
          collectionSelector,
      FunctionExpression<Function2<T, TCollection, TResult>> resultSelector) {
    throw new UnsupportedOperationException();
  }

  public <TCollection, TResult> Queryable<TResult> selectManyN(
      Queryable<T> source,
      FunctionExpression<Function1<T, Enumerable<TCollection>>>
          collectionSelector,
      FunctionExpression<Function2<T, TCollection, TResult>> resultSelector) {
    throw new UnsupportedOperationException();
  }

  public boolean sequenceEqual(
      Queryable<T> source, Enumerable<T> enumerable) {
    throw new UnsupportedOperationException();
  }

  public boolean sequenceEqual(
      Queryable<T> source,
      Enumerable<T> enumerable,
      EqualityComparer<T> tEqualityComparer) {
    throw new UnsupportedOperationException();
  }

  public T single(Queryable<T> source) {
    throw new UnsupportedOperationException();
  }

  public T single(
      Queryable<T> source,
      FunctionExpression<Predicate1<T>> predicate) {
    throw new UnsupportedOperationException();
  }

  public T singleOrDefault(Queryable<T> source) {
    throw new UnsupportedOperationException();
  }

  public T singleOrDefault(
      Queryable<T> source,
      FunctionExpression<Predicate1<T>> predicate) {
    throw new UnsupportedOperationException();
  }

  public Queryable<T> skip(
      Queryable<T> source, int count) {
    throw new UnsupportedOperationException();
  }

  public Queryable<T> skipWhile(
      Queryable<T> source,
      FunctionExpression<Predicate1<T>> predicate) {
    throw new UnsupportedOperationException();
  }

  public Queryable<T> skipWhileN(
      Queryable<T> source,
      FunctionExpression<Predicate2<T, Integer>> predicate) {
    throw new UnsupportedOperationException();
  }

  public BigDecimal sumBigDecimal(
      Queryable<T> source,
      FunctionExpression<BigDecimalFunction1<T>> selector) {
    throw new UnsupportedOperationException();
  }

  public BigDecimal sumNullableBigDecimal(
      Queryable<T> source,
      FunctionExpression<NullableBigDecimalFunction1<T>> selector) {
    throw new UnsupportedOperationException();
  }

  public double sumDouble(
      Queryable<T> source,
      FunctionExpression<DoubleFunction1<T>> selector) {
    throw new UnsupportedOperationException();
  }

  public Double sumNullableDouble(
      Queryable<T> source,
      FunctionExpression<NullableDoubleFunction1<T>> selector) {
    throw new UnsupportedOperationException();
  }

  public int sumInteger(
      Queryable<T> source,
      FunctionExpression<IntegerFunction1<T>> selector) {
    throw new UnsupportedOperationException();
  }

  public Integer sumNullableInteger(
      Queryable<T> source,
      FunctionExpression<NullableIntegerFunction1<T>> selector) {
    throw new UnsupportedOperationException();
  }

  public long sumLong(
      Queryable<T> source,
      FunctionExpression<LongFunction1<T>> selector) {
    throw new UnsupportedOperationException();
  }

  public Long sumNullableLong(
      Queryable<T> source,
      FunctionExpression<NullableLongFunction1<T>> selector) {
    throw new UnsupportedOperationException();
  }

  public float sumFloat(
      Queryable<T> source,
      FunctionExpression<FloatFunction1<T>> selector) {
    throw new UnsupportedOperationException();
  }

  public Float sumNullableFloat(
      Queryable<T> source,
      FunctionExpression<NullableFloatFunction1<T>> selector) {
    throw new UnsupportedOperationException();
  }

  public Queryable<T> take(
      Queryable<T> source, int count) {
    throw new UnsupportedOperationException();
  }

  public Queryable<T> takeWhile(
      Queryable<T> source,
      FunctionExpression<Predicate1<T>> predicate) {
    throw new UnsupportedOperationException();
  }

  public Queryable<T> takeWhileN(
      Queryable<T> source,
      FunctionExpression<Predicate2<T, Integer>> predicate) {
    throw new UnsupportedOperationException();
  }

  public <TKey extends Comparable<TKey>> OrderedQueryable<T> thenBy(
      OrderedQueryable<T> source,
      FunctionExpression<Function1<T, TKey>> keySelector) {
    throw new UnsupportedOperationException();
  }

  public <TKey> OrderedQueryable<T> thenBy(
      OrderedQueryable<T> source,
      FunctionExpression<Function1<T, TKey>> keySelector,
      Comparator<TKey> comparator) {
    throw new UnsupportedOperationException();
  }

  public <TKey extends Comparable<TKey>> OrderedQueryable<T> thenByDescending(
      OrderedQueryable<T> source,
      FunctionExpression<Function1<T, TKey>> keySelector) {
    throw new UnsupportedOperationException();
  }

  public <TKey> OrderedQueryable<T> thenByDescending(
      OrderedQueryable<T> source,
      FunctionExpression<Function1<T, TKey>> keySelector,
      Comparator<TKey> comparator) {
    throw new UnsupportedOperationException();
  }

  public Queryable<T> union(
      Queryable<T> source, Enumerable<T> source1) {
    throw new UnsupportedOperationException();
  }

  public Queryable<T> union(
      Queryable<T> source,
      Enumerable<T> source1,
      EqualityComparer<T> tEqualityComparer) {
    throw new UnsupportedOperationException();
  }

  public Queryable<T> where(
      Queryable<T> source,
      FunctionExpression<? extends Predicate1<T>> predicate) {
    RelNode child = toRel(source);
    RexNode node = translator.toRex(predicate, child);
    setRel(LogicalFilter.create(child, node));
    return source;
  }

  public Queryable<T> whereN(
      Queryable<T> source,
      FunctionExpression<? extends Predicate2<T, Integer>> predicate) {
    throw new UnsupportedOperationException();
  }

  public <T1, TResult> Queryable<TResult> zip(
      Queryable<T> source,
      Enumerable<T1> source1,
      FunctionExpression<Function2<T, T1, TResult>> resultSelector) {
    throw new UnsupportedOperationException();
  }
}

// End QueryableRelBuilder.java
