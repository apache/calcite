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
import net.hydromatic.linq4j.expressions.Expressions;
import net.hydromatic.linq4j.expressions.FunctionExpression;
import net.hydromatic.linq4j.function.*;

import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Default implementations for methods in the {@link Queryable} interface.
 *
 * @author jhyde
 */
public abstract class QueryableDefaults {

    /** Applies an accumulator function over a
     * sequence. */
    public static <TSource> TSource aggregate(
        Queryable<TSource> queryable,
        FunctionExpression<Function2<TSource, TSource, TSource>> func)
    {
        throw Extensions.todo();
    }

    /** Applies an accumulator function over a
     * sequence. The specified seed value is used as the initial
     * accumulator value. */
    public static <TSource, TAccumulate> TAccumulate aggregate(
        Queryable<TSource> queryable,
        TAccumulate seed,
        FunctionExpression<Function2<TAccumulate, TSource, TAccumulate>> func)
    {
        throw Extensions.todo();
    }

    /** Applies an accumulator function over a
     * sequence. The specified seed value is used as the initial
     * accumulator value, and the specified function is used to select
     * the result value. */
    public static <TSource, TAccumulate, TResult> TResult aggregate(
        Queryable<TSource> queryable,
        TAccumulate seed,
        FunctionExpression<Function2<TAccumulate, TSource, TAccumulate>> func,
        FunctionExpression<Function1<TAccumulate, TResult>> selector)
    {
        throw Extensions.todo();
    }

    /** Determines whether all the elements of a sequence
     * satisfy a condition. */
    public static <TSource> boolean all(
        Queryable<TSource> queryable,
        FunctionExpression<Predicate1<TSource>> predicate)
    {
        throw Extensions.todo();
    }

    /** Determines whether a sequence contains any
     * elements. */
    public static <TSource> void any(
        Queryable<TSource> queryable)
    {
        throw Extensions.todo();
    }

    /** Determines whether any element of a sequence
     * satisfies a condition. */
    public static <TSource> boolean any(
        Queryable<TSource> queryable,
        FunctionExpression<Predicate1<TSource>> predicate)
    {
        throw Extensions.todo();
    }

    /** Converts a generic Enumerable<TSource> to a generic
     * IQueryable<TSource>. */
    public static <TSource> Queryable<TSource> asQueryable(
        Queryable<TSource> queryable)
    {
        throw Extensions.todo();
    }

    /** Computes the average of a sequence of Decimal
     * values that is obtained by invoking a projection function on
     * each element of the input sequence. */
    public static <TSource> BigDecimal averageBigDecimal(
        Queryable<TSource> queryable,
        FunctionExpression<BigDecimalFunction1<TSource>> selector)
    {
        throw Extensions.todo();
    }

    /** Computes the average of a sequence of nullable
     * Decimal values that is obtained by invoking a projection
     * function on each element of the input sequence. */
    public static <TSource> BigDecimal averageNullableBigDecimal(
        Queryable<TSource> queryable,
        FunctionExpression<NullableBigDecimalFunction1<TSource>> selector)
    {
        throw Extensions.todo();
    }

    /** Computes the average of a sequence of Double
     * values that is obtained by invoking a projection function on
     * each element of the input sequence. */
    public static <TSource> double averageDouble(
        Queryable<TSource> queryable,
        FunctionExpression<DoubleFunction1<TSource>> selector)
    {
        throw Extensions.todo();
    }

    /** Computes the average of a sequence of nullable
     * Double values that is obtained by invoking a projection
     * function on each element of the input sequence. */
    public static <TSource> Double averageNullableDouble(
        Queryable<TSource> queryable,
        FunctionExpression<NullableDoubleFunction1<TSource>> selector)
    {
        throw Extensions.todo();
    }

    /** Computes the average of a sequence of int values
     * that is obtained by invoking a projection function on each
     * element of the input sequence. */
    public static <TSource> int averageInteger(
        Queryable<TSource> queryable,
        FunctionExpression<IntegerFunction1<TSource>> selector)
    {
        throw Extensions.todo();
    }

    /** Computes the average of a sequence of nullable
     * int values that is obtained by invoking a projection function
     * on each element of the input sequence. */
    public static <TSource> Integer averageNullableInteger(
        Queryable<TSource> queryable,
        FunctionExpression<NullableIntegerFunction1<TSource>> selector)
    {
        throw Extensions.todo();
    }

    /** Computes the average of a sequence of Float
     * values that is obtained by invoking a projection function on
     * each element of the input sequence. */
    public static <TSource> float averageFloat(
        Queryable<TSource> queryable,
        FunctionExpression<FloatFunction1<TSource>> selector)
    {
        throw Extensions.todo();
    }

    /** Computes the average of a sequence of nullable
     * Float values that is obtained by invoking a projection
     * function on each element of the input sequence. */
    public static <TSource> Float averageNullableFloat(
        Queryable<TSource> queryable,
        FunctionExpression<NullableFloatFunction1<TSource>> selector)
    {
        throw Extensions.todo();
    }

    /** Computes the average of a sequence of long values
     * that is obtained by invoking a projection function on each
     * element of the input sequence. */
    public static <TSource> long averageLong(
        Queryable<TSource> queryable,
        FunctionExpression<LongFunction1<TSource>> selector)
    {
        throw Extensions.todo();
    }

    /** Computes the average of a sequence of nullable
     * long values that is obtained by invoking a projection function
     * on each element of the input sequence. */
    public static <TSource> Long averageNullableLong(
        Queryable<TSource> queryable,
        FunctionExpression<NullableLongFunction1<TSource>> selector)
    {
        throw Extensions.todo();
    }

    /**
     * <p>Analogous to LINQ's Enumerable.Cast extension method.</p>
     *
     * @param clazz Target type
     * @param <T2> Target type
     * @return Collection of T2
     */
    public static <TSource, T2> Queryable<T2> cast(
        final Queryable<TSource> source,
        final Class<T2> clazz)
    {
        return new BaseQueryable<T2>(
            source.getProvider(), clazz, source.getExpression())
        {
            public Enumerator<T2> enumerator() {
                return new EnumerableDefaults.CastingEnumerator<T2>(
                    source.enumerator(), clazz);
            }
        };
    }

    /** Concatenates two sequences. */
    public static <TSource> Queryable<TSource> concat(
        Queryable<TSource> queryable0, Enumerable<TSource> source2)
    {
        throw Extensions.todo();
    }

    /** Determines whether a sequence contains a specified
     * element by using the default equality comparer. */
    public static <TSource> boolean contains(
        Queryable<TSource> queryable, TSource element)
    {
        throw Extensions.todo();
    }

    /** Determines whether a sequence contains a specified
     * element by using a specified EqualityComparer<TSource>. */
    public static <TSource> boolean contains(
        Queryable<TSource> queryable,
        TSource element,
        EqualityComparer comparer)
    {
        throw Extensions.todo();
    }

    /** Returns the number of elements in a
     * sequence. */
    public static <TSource> int count(
        Queryable<TSource> queryable)
    {
        throw Extensions.todo();
    }

    /** Returns the number of elements in the specified
     * sequence that satisfies a condition. */
    public static <TSource> int count(
        Queryable<TSource> queryable,
        FunctionExpression<Predicate1<TSource>> func)
    {
        throw Extensions.todo();
    }

    /** Returns the elements of the specified sequence or
     * the type parameter's default value in a singleton collection if
     * the sequence is empty. */
    public static <TSource> Queryable<TSource> defaultIfEmpty(
        Queryable<TSource> queryable)
    {
        throw Extensions.todo();
    }

    /** Returns the elements of the specified sequence or
     * the specified value in a singleton collection if the sequence
     * is empty. */
    public static <TSource> TSource defaultIfEmpty(
        Queryable<TSource> queryable, TSource value)
    {
        throw Extensions.todo();
    }

    /** Returns distinct elements from a sequence by using
     * the default equality comparer to compare values. */
    public static <TSource> Queryable<TSource> distinct(
        Queryable<TSource> queryable)
    {
        throw Extensions.todo();
    }

    /** Returns distinct elements from a sequence by using
     * a specified EqualityComparer<TSource> to compare values. */
    public static <TSource> Queryable<TSource> distinct(
        Queryable<TSource> queryable, EqualityComparer comparer)
    {
        throw Extensions.todo();
    }

    /** Returns the element at a specified index in a
     * sequence. */
    public static <TSource> TSource elementAt(
        Queryable<TSource> queryable, int index)
    {
        throw Extensions.todo();
    }

    /** Returns the element at a specified index in a
     * sequence or a default value if the index is out of
     * range. */
    public static <TSource> TSource elementAtOrDefault(
        Queryable<TSource> queryable, int index)
    {
        throw Extensions.todo();
    }

    /** Produces the set difference of two sequences by
     * using the default equality comparer to compare values. (Defined
     * by Queryable.) */
    public static <TSource> Queryable<TSource> except(
        Queryable<TSource> queryable, Enumerable<TSource> enumerable)
    {
        throw Extensions.todo();
    }

    /** Produces the set difference of two sequences by
     * using the specified EqualityComparer<TSource> to compare
     * values. */
    public static <TSource> Queryable<TSource> except(
        Queryable<TSource> queryable,
        Enumerable<TSource> enumerable,
        EqualityComparer<TSource> comparer)
    {
        throw Extensions.todo();
    }

    /** Returns the first element of a sequence. (Defined
     * by Queryable.) */
    public static <TSource> TSource first(
        Queryable<TSource> queryable)
    {
        throw Extensions.todo();
    }

    /** Returns the first element of a sequence that
     * satisfies a specified condition. */
    public static <TSource> TSource first(
        Queryable<TSource> queryable,
        FunctionExpression<Predicate1<TSource>> predicate)
    {
        throw Extensions.todo();
    }

    /** Returns the first element of a sequence, or a
     * default value if the sequence contains no elements. */
    public static <TSource> TSource firstOrDefault(
        Queryable<TSource> queryable)
    {
        throw Extensions.todo();
    }

    /** Returns the first element of a sequence that
     * satisfies a specified condition or a default value if no such
     * element is found. */
    public static <TSource> TSource firstOrDefault(
        Queryable<TSource> queryable,
        FunctionExpression<Predicate1<TSource>> predicate)
    {
        throw Extensions.todo();
    }

    /** Groups the elements of a sequence according to a
     * specified key selector function. */
    public static <TSource, TKey> Queryable<Grouping<TKey, TSource>> groupBy(
        Queryable<TSource> queryable,
        FunctionExpression<Function1<TSource, TKey>> keySelector)
    {
        throw Extensions.todo();
    }

    /** Groups the elements of a sequence according to a
     * specified key selector function and compares the keys by using
     * a specified comparer. */
    public static <TSource, TKey> Queryable<Grouping<TKey, TSource>> groupBy(
        Queryable<TSource> queryable,
        FunctionExpression<Function1<TSource, TKey>> keySelector,
        EqualityComparer comparer)
    {
        throw Extensions.todo();
    }

    /** Groups the elements of a sequence according to a
     * specified key selector function and projects the elements for
     * each group by using a specified function. */
    public static <TSource, TKey, TElement>
    Queryable<Grouping<TKey, TElement>> groupBy(
        Queryable<TSource> queryable,
        FunctionExpression<Function1<TSource, TKey>> keySelector,
        FunctionExpression<Function1<TSource, TElement>> elementSelector)
    {
        throw Extensions.todo();
    }

    /** Groups the elements of a sequence according to a
     * specified key selector function and creates a result value from
     * each group and its key.
     *
     * <p>NOTE: Renamed from {@code groupBy} to distinguish from
     * {@link #groupBy(net.hydromatic.linq4j.Queryable, net.hydromatic.linq4j.expressions.FunctionExpression, net.hydromatic.linq4j.expressions.FunctionExpression)},
     * which has the same erasure.</p>
     */
    public static <TSource, TKey, TResult>
    Queryable<Grouping<TKey, TResult>> groupByK(
        Queryable<TSource> queryable,
        FunctionExpression<Function1<TSource, TKey>> keySelector,
        FunctionExpression<Function2<TKey, Enumerable<TSource>, TResult>>
            elementSelector)
    {
        throw Extensions.todo();
    }

    /** Groups the elements of a sequence and projects the
     * elements for each group by using a specified function. Key
     * values are compared by using a specified comparer.
     */
    public static <TSource, TKey, TElement>
    Queryable<Grouping<TKey, TElement>> groupBy(
        Queryable<TSource> queryable,
        FunctionExpression<Function1<TSource, TKey>> keySelector,
        FunctionExpression<Function1<TSource, TElement>> elementSelector,
        EqualityComparer comparer)
    {
        throw Extensions.todo();
    }

    /** Groups the elements of a sequence according to a
     * specified key selector function and creates a result value from
     * each group and its key. Keys are compared by using a specified
     * comparer.
     *
     * <p>NOTE: Renamed from {@code groupBy} to distinguish from
     * {@link #groupBy(net.hydromatic.linq4j.Queryable, net.hydromatic.linq4j.expressions.FunctionExpression, net.hydromatic.linq4j.expressions.FunctionExpression, net.hydromatic.linq4j.function.EqualityComparer)},
     * which has the same erasure.</p>
     * */
    public static <TSource, TKey, TResult> Queryable<TResult> groupByK(
        Queryable<TSource> queryable,
        FunctionExpression<Function1<TSource, TKey>> keySelector,
        FunctionExpression<Function2<TKey, Enumerable<TSource>,
        TResult>> elementSelector,
        EqualityComparer comparer)
    {
        throw Extensions.todo();
    }

    /** Groups the elements of a sequence according to a
     * specified key selector function and creates a result value from
     * each group and its key. The elements of each group are
     * projected by using a specified function. */
    public static <TSource, TKey, TElement, TResult> Queryable<TResult> groupBy(
        Queryable<TSource> queryable,
        FunctionExpression<Function1<TSource, TKey>> keySelector,
        FunctionExpression<Function1<TSource, TElement>> elementSelector,
        FunctionExpression<Function2<TKey, Enumerable<TElement>, TResult>>
            resultSelector)
    {
        throw Extensions.todo();
    }

    /** Groups the elements of a sequence according to a
     * specified key selector function and creates a result value from
     * each group and its key. Keys are compared by using a specified
     * comparer and the elements of each group are projected by using
     * a specified function. */
    public static <TSource, TKey, TElement, TResult> Queryable<TResult> groupBy(
        Queryable<TSource> queryable,
        FunctionExpression<Function1<TSource, TKey>> keySelector,
        FunctionExpression<Function1<TSource, TElement>> elementSelector,
        FunctionExpression<Function2<TKey, Enumerable<TElement>, TResult>>
            resultSelector,
        EqualityComparer<TKey> comparer)
    {
        throw Extensions.todo();
    }

    /** Correlates the elements of two sequences based on
     * key equality and groups the results. The default equality
     * comparer is used to compare keys. */
    public static <TOuter, TInner, TKey, TResult> Queryable<TResult> groupJoin(
        Queryable<TOuter> outer,
        Enumerable<TInner> inner,
        FunctionExpression<Function1<TOuter, TKey>> outerKeySelector,
        FunctionExpression<Function1<TInner, TKey>> innerKeySelector,
        FunctionExpression<Function2<TOuter, Enumerable<TInner>, TResult>>
            resultSelector)
    {
        throw Extensions.todo();
    }

    /** Correlates the elements of two sequences based on
     * key equality and groups the results. A specified
     * EqualityComparer<TSource> is used to compare keys. */
    public static <TOuter, TInner, TKey, TResult> Enumerable<TResult> groupJoin(
        Queryable<TOuter> outer,
        Enumerable<TInner> inner,
        FunctionExpression<Function1<TOuter, TKey>> outerKeySelector,
        FunctionExpression<Function1<TInner, TKey>> innerKeySelector,
        FunctionExpression<Function2<TOuter, Enumerable<TInner>, TResult>>
            resultSelector,
        EqualityComparer<TKey> comparer)
    {
        throw Extensions.todo();
    }

    /** Produces the set intersection of two sequences by
     * using the default equality comparer to compare values. (Defined
     * by Queryable.) */
    public static <TSource> Queryable<TSource> intersect(
        Queryable<TSource> queryable, Enumerable<TSource> enumerable)
    {
        throw Extensions.todo();
    }

    /** Produces the set intersection of two sequences by
     * using the specified EqualityComparer<TSource> to compare
     * values. */
    public static <TSource> Queryable<TSource> intersect(
        Queryable<TSource> queryable,
        Enumerable<TSource> enumerable,
        EqualityComparer<TSource> comparer)
    {
        throw Extensions.todo();
    }

    /** Correlates the elements of two sequences based on
     * matching keys. The default equality comparer is used to compare
     * keys. */
    public static <TOuter, TInner, TKey, TResult> Queryable<TResult> join(
        Queryable<TOuter> outer,
        Enumerable<TInner> inner,
        FunctionExpression<Function1<TOuter, TKey>> outerKeySelector,
        FunctionExpression<Function1<TInner, TKey>> innerKeySelector,
        FunctionExpression<Function2<TOuter, TInner, TResult>> resultSelector)
    {
        throw Extensions.todo();
    }

    /** Correlates the elements of two sequences based on
     * matching keys. A specified EqualityComparer<TSource> is used to
     * compare keys. */
    public static <TOuter, TInner, TKey, TResult> Queryable<TResult> join(
        Queryable<TOuter> outer,
        Enumerable<TInner> inner,
        FunctionExpression<Function1<TOuter, TKey>> outerKeySelector,
        FunctionExpression<Function1<TInner, TKey>> innerKeySelector,
        FunctionExpression<Function2<TOuter, TInner, TResult>> resultSelector,
        EqualityComparer<TKey> comparer)
    {
        throw Extensions.todo();
    }

    /** Returns the last element in a sequence. (Defined
     * by Queryable.) */
    public static <TSource> TSource last(Queryable<TSource> queryable) {
        throw Extensions.todo();
    }

    /** Returns the last element of a sequence that
     * satisfies a specified condition. */
    public static <TSource> TSource last(
        Queryable<TSource> queryable,
        FunctionExpression<Predicate1<TSource>> predicate)
    {
        throw Extensions.todo();
    }

    /** Returns the last element in a sequence, or a
     * default value if the sequence contains no elements. */
    public static <TSource> TSource lastOrDefault(Queryable<TSource> queryable)
    {
        throw Extensions.todo();
    }

    /** Returns the last element of a sequence that
     * satisfies a condition or a default value if no such element is
     * found. */
    public static <TSource> TSource lastOrDefault(
        Queryable<TSource> queryable,
        FunctionExpression<Predicate1<TSource>> predicate)
    {
        throw Extensions.todo();
    }

    /** Returns an long that represents the total number
     * of elements in a sequence. */
    public static <TSource> long longCount(Queryable<TSource> xable) {
        throw Extensions.todo();
    }

    /** Returns an long that represents the number of
     * elements in a sequence that satisfy a condition. */
    public static <TSource> long longCount(
        Queryable<TSource> queryable,
        FunctionExpression<Predicate1<TSource>> predicate)
    {
        throw Extensions.todo();
    }

    /** Returns the maximum value in a generic
     * IQueryable<TSource>. */
    public static <TSource> TSource max(Queryable<TSource> queryable) {
        throw Extensions.todo();
    }

    /** Invokes a projection function on each element of a
     * generic IQueryable<TSource> and returns the maximum resulting
     * value. */
    public static <TSource, TResult> TResult max(
        Queryable<TSource> queryable,
        FunctionExpression<Function1<TSource, TResult>> selector)
    {
        throw Extensions.todo();
    }

    /** Returns the minimum value in a generic
     * IQueryable<TSource>. */
    public static <TSource> TSource min(
        Queryable<TSource> queryable)
    {
        throw Extensions.todo();
    }

    /** Invokes a projection function on each element of a
     * generic IQueryable<TSource> and returns the minimum resulting
     * value. */
    public static <TSource, TResult> TResult min(
        Queryable<TSource> queryable,
        FunctionExpression<Function1<TSource, TResult>> selector)
    {
        throw Extensions.todo();
    }

    /** Filters the elements of an IQueryable based on a
     * specified type.
     *
     * <p>This method generates a
     * {@link net.hydromatic.linq4j.expressions.MethodCallExpression} that
     * represents calling {@code ofType} itself as a constructed generic method.
     * It then passes the {@code MethodCallExpression} to the
     * {@link net.hydromatic.linq4j.QueryProvider#createQuery createQuery} method of the
     * {@link net.hydromatic.linq4j.QueryProvider} represented by the Provider property of the source
     * parameter.</p>
     *
     * <p>The query behavior that occurs as a result of executing an expression
     * tree that represents calling OfType depends on the implementation of the
     * type of the source parameter. The expected behavior is that it filters
     * out any elements in source that are not of type TResult.
     *
     * <p>NOTE: clazz parameter not present in C# LINQ; necessary because of
     * Java type erasure.</p>
     */
    public static <TResult> Queryable<TResult> ofType(
        Queryable<?> queryable, Class<TResult> clazz)
    {
        throw Extensions.todo();
    }

    /** Sorts the elements of a sequence in ascending
     * order according to a key.
     *
     * @see #thenBy */
    public static <TSource, TKey extends Comparable>
    OrderedQueryable<TSource> orderBy(
        Queryable<TSource> source,
        FunctionExpression<Function1<TSource, TKey>> keySelector)
    {
        throw Extensions.todo();
    }

    /** Sorts the elements of a sequence in ascending
     * order by using a specified comparer. */
    public static <TSource, TKey> OrderedQueryable<TSource> orderBy(
        Queryable<TSource> source,
        FunctionExpression<Function1<TSource, TKey>> keySelector,
        Comparator<TKey> comparator)
    {
        throw Extensions.todo();
    }

    /** Sorts the elements of a sequence in descending
     * order according to a key. */
    public static <TSource, TKey extends Comparable>
    OrderedQueryable<TSource> orderByDescending(
        Queryable<TSource> source,
        FunctionExpression<Function1<TSource, TKey>> keySelector)
    {
        throw Extensions.todo();
    }

    /** Sorts the elements of a sequence in descending
     * order by using a specified comparer. */
    public static <TSource, TKey> OrderedQueryable<TSource> orderByDescending(
        Queryable<TSource> source,
        FunctionExpression<Function1<TSource, TKey>> keySelector,
        Comparator<TKey> comparator)
    {
        throw Extensions.todo();
    }

    /** Inverts the order of the elements in a
     * sequence. */
    public static <TSource> Queryable<TSource> reverse(
        Queryable<TSource> source)
    {
        throw Extensions.todo();
    }

    /** Projects each element of a sequence into a new form. */
    public static <TSource, TResult> Queryable<TResult> select(
        Queryable<TSource> source,
        FunctionExpression<Function1<TSource, TResult>> selector)
    {
        return source.getProvider().createQuery(
            Expressions.call(source.getExpression(), "select", selector),
            functionResultType(selector));
    }

    private static <P0, R> Type functionResultType(
        FunctionExpression<Function1<P0, R>> selector)
    {
        return selector.body.getType();
    }

    /** Projects each element of a sequence into a new
     * form by incorporating the element's index.
     *
     * <p>NOTE: Renamed from {@code select} because had same erasure as
     * {@link #select(net.hydromatic.linq4j.Queryable, net.hydromatic.linq4j.expressions.FunctionExpression)}.</p>
     */
    public static <TSource, TResult> Queryable<TResult> selectN(
        Queryable<TSource> source,
        FunctionExpression<Function2<TSource, Integer, TResult>> selector)
    {
        throw Extensions.todo();
    }

    /** Projects each element of a sequence to an
     * Enumerable<TSource> and combines the resulting sequences into one
     * sequence. */
    public static <TSource, TResult> Queryable<TResult> selectMany(
        Queryable<TSource> source,
        FunctionExpression<Function1<TSource, Enumerable<TResult>>> selector)
    {
        throw Extensions.todo();
    }

    /** Projects each element of a sequence to an
     * Enumerable<TSource> and combines the resulting sequences into one
     * sequence. The index of each source element is used in the
     * projected form of that element.
     *
     * <p>NOTE: Renamed from {@code selectMany} because had same erasure as
     * {@link #selectMany(net.hydromatic.linq4j.Queryable, net.hydromatic.linq4j.expressions.FunctionExpression)}</p>
     */
    public static <TSource, TResult> Queryable<TResult> selectManyN(
        Queryable<TSource> source,
        FunctionExpression<Function2<TSource, Integer, Enumerable<TResult>>>
            selector)
    {
        throw Extensions.todo();
    }

    /** Projects each element of a sequence to an
     * Enumerable<TSource> that incorporates the index of the source
     * element that produced it. A result selector function is invoked
     * on each element of each intermediate sequence, and the
     * resulting values are combined into a single, one-dimensional
     * sequence and returned. */
    public static <TSource, TCollection, TResult> Queryable<TResult> selectMany(
        Queryable<TSource> source,
        FunctionExpression<Function2<TSource, Integer, Enumerable<TCollection>>>
            collectionSelector,
        FunctionExpression<Function2<TSource, TCollection, TResult>>
            resultSelector)
    {
        throw Extensions.todo();
    }

    /** Projects each element of a sequence to an
     * Enumerable<TSource> and invokes a result selector function on each
     * element therein. The resulting values from each intermediate
     * sequence are combined into a single, one-dimensional sequence
     * and returned.
     *
     * <p>NOTE: Renamed from {@code selectMany} because had same erasure as
     * {@link #selectMany(net.hydromatic.linq4j.Queryable, net.hydromatic.linq4j.expressions.FunctionExpression, net.hydromatic.linq4j.expressions.FunctionExpression)}</p>
     * */
    public static <TSource, TCollection, TResult>
    Queryable<TResult> selectManyN(
        Queryable<TSource> source,
        FunctionExpression<Function1<TSource, Enumerable<TCollection>>>
            collectionSelector,
        FunctionExpression<Function2<TSource, TCollection, TResult>>
            resultSelector)
    {
        throw Extensions.todo();
    }

    /** Determines whether two sequences are equal by
     * using the default equality comparer to compare
     * elements. */
    public static <TSource> boolean sequenceEqual(
        Queryable<TSource> queryable, Enumerable<TSource> enumerable)
    {
        throw Extensions.todo();
    }

    /** Determines whether two sequences are equal by
     * using a specified EqualityComparer<TSource> to compare
     * elements. */
    public static <TSource> boolean sequenceEqual(
        Queryable<TSource> queryable,
        Enumerable<TSource> enumerable,
        EqualityComparer<TSource> comparer)
    {
        throw Extensions.todo();
    }

    /** Returns the only element of a sequence, and throws
     * an exception if there is not exactly one element in the
     * sequence. */
    public static <TSource> TSource single(
        Queryable<TSource> source)
    {
        throw Extensions.todo();
    }

    /** Returns the only element of a sequence that
     * satisfies a specified condition, and throws an exception if
     * more than one such element exists. */
    public static <TSource> TSource single(
        Queryable<TSource> source,
        FunctionExpression<Predicate1<TSource>> predicate)
    {
        throw Extensions.todo();
    }

    /** Returns the only element of a sequence, or a
     * default value if the sequence is empty; this method throws an
     * exception if there is more than one element in the
     * sequence. */
    public static <TSource> TSource singleOrDefault(
        Queryable<TSource> source)
    {
        throw Extensions.todo();
    }

    /** Returns the only element of a sequence that
     * satisfies a specified condition or a default value if no such
     * element exists; this method throws an exception if more than
     * one element satisfies the condition. */
    public static <TSource> TSource singleOrDefault(
        Queryable<TSource> source,
        FunctionExpression<Predicate1<TSource>> predicate)
    {
        throw Extensions.todo();
    }

    /** Bypasses a specified number of elements in a
     * sequence and then returns the remaining elements. */
    public static <TSource> Queryable<TSource> skip(
        Queryable<TSource> source, int count)
    {
        return EnumerableDefaults.skip(source.asEnumerable(), count)
            .asQueryable();
    }

    /** Bypasses elements in a sequence as long as a
     * specified condition is true and then returns the remaining
     * elements. */
    public static <TSource> Queryable<TSource> skipWhile(
        Queryable<TSource> source,
        FunctionExpression<Predicate1<TSource>> predicate)
    {
        return skipWhileN(
            source,
            Expressions.lambda(
                Functions.<TSource, Integer>toPredicate2(
                    predicate.getFunction())));
    }

    /** Bypasses elements in a sequence as long as a
     * specified condition is true and then returns the remaining
     * elements. The element's index is used in the logic of the
     * predicate function. */
    public static <TSource> Queryable<TSource> skipWhileN(
        final Queryable<TSource> source,
        final FunctionExpression<Predicate2<TSource, Integer>> predicate)
    {
        return new BaseQueryable<TSource>(
            source.getProvider(),
            source.getElementType(),
            source.getExpression())
        {
            public Enumerator<TSource> enumerator() {
                return new EnumerableDefaults.SkipWhileEnumerator<TSource>(
                    source.enumerator(), predicate.getFunction());
            }
        };
    }

    /** Computes the sum of the sequence of Decimal values
     * that is obtained by invoking a projection function on each
     * element of the input sequence. */
    public static <TSource> BigDecimal sumBigDecimal(
        Queryable<TSource> sources,
        FunctionExpression<BigDecimalFunction1<TSource>> selector)
    {
        throw Extensions.todo();
    }

    /** Computes the sum of the sequence of nullable
     * Decimal values that is obtained by invoking a projection
     * function on each element of the input sequence. */
    public static <TSource> BigDecimal sumNullableBigDecimal(
        Queryable<TSource> source,
        FunctionExpression<NullableBigDecimalFunction1<TSource>> selector)
    {
        throw Extensions.todo();
    }

    /** Computes the sum of the sequence of Double values
     * that is obtained by invoking a projection function on each
     * element of the input sequence. */
    public static <TSource> double sumDouble(
        Queryable<TSource> source,
        FunctionExpression<DoubleFunction1<TSource>> selector)
    {
        throw Extensions.todo();
    }

    /** Computes the sum of the sequence of nullable
     * Double values that is obtained by invoking a projection
     * function on each element of the input sequence. */
    public static <TSource> Double sumNullableDouble(
        Queryable<TSource> source,
        FunctionExpression<NullableDoubleFunction1<TSource>> selector)
    {
        throw Extensions.todo();
    }

    /** Computes the sum of the sequence of int values
     * that is obtained by invoking a projection function on each
     * element of the input sequence. */
    public static <TSource> int sumInteger(
        Queryable<TSource> source, FunctionExpression<IntegerFunction1<TSource>>
            selector)
    {
        throw Extensions.todo();
    }

    /** Computes the sum of the sequence of nullable int
     * values that is obtained by invoking a projection function on
     * each element of the input sequence. */
    public static <TSource> Integer sumNullableInteger(
        Queryable<TSource> source,
        FunctionExpression<NullableIntegerFunction1<TSource>> selector)
    {
        throw Extensions.todo();
    }

    /** Computes the sum of the sequence of long values
     * that is obtained by invoking a projection function on each
     * element of the input sequence. */
    public static <TSource> long sumLong(
        Queryable<TSource> source,
        FunctionExpression<LongFunction1<TSource>> selector)
    {
        throw Extensions.todo();
    }

    /** Computes the sum of the sequence of nullable long
     * values that is obtained by invoking a projection function on
     * each element of the input sequence. */
    public static <TSource> Long sumNullableLong(
        Queryable<TSource> source,
        FunctionExpression<NullableLongFunction1<TSource>> selector)
    {
        throw Extensions.todo();
    }

    /** Computes the sum of the sequence of Float values
     * that is obtained by invoking a projection function on each
     * element of the input sequence. */
    public static <TSource> float sumFloat(
        Queryable<TSource> source,
        FunctionExpression<FloatFunction1<TSource>> selector)
    {
        throw Extensions.todo();
    }

    /** Computes the sum of the sequence of nullable
     * Float values that is obtained by invoking a projection
     * function on each element of the input sequence. */
    public static <TSource> Float sumNullableFloat(
        Queryable<TSource> source,
        FunctionExpression<NullableFloatFunction1<TSource>> selector)
    {
        throw Extensions.todo();
    }

    /** Returns a specified number of contiguous elements
     * from the start of a sequence. */
    public static <TSource> Queryable<TSource> take(
        Queryable<TSource> source, int count)
    {
        return EnumerableDefaults.take(source.asEnumerable(), count)
            .asQueryable();
    }

    /** Returns elements from a sequence as long as a
     * specified condition is true. */
    public static <TSource> Queryable<TSource> takeWhile(
        Queryable<TSource> source,
        FunctionExpression<Predicate1<TSource>> predicate)
    {
        return takeWhileN(
            source,
            Expressions.lambda(
                Functions.<TSource, Integer>toPredicate2(
                    predicate.getFunction())));
    }

    /** Returns elements from a sequence as long as a
     * specified condition is true. The element's index is used in the
     * logic of the predicate function. */
    public static <TSource> Queryable<TSource> takeWhileN(
        final Queryable<TSource> source,
        final FunctionExpression<Predicate2<TSource, Integer>> predicate)
    {
        return new BaseQueryable<TSource>(
            source.getProvider(),
            source.getElementType(),
            source.getExpression())
        {
            public Enumerator<TSource> enumerator() {
                return new EnumerableDefaults.TakeWhileEnumerator<TSource>(
                    source.enumerator(), predicate.getFunction());
            }
        };
    }

    /** Performs a subsequent ordering of the elements in a sequence in
     * ascending order according to a key. */
    public static <TSource, TKey extends Comparable<TKey>>
    OrderedQueryable<TSource> thenBy(
        OrderedQueryable<TSource> source,
        FunctionExpression<Function1<TSource, TKey>> keySelector)
    {
        throw Extensions.todo();
    }

    /** Performs a subsequent ordering of the elements in a sequence in
     * ascending order according to a key, using a specified comparator. */
    public static <TSource, TKey> OrderedQueryable<TSource> thenBy(
        OrderedQueryable<TSource> source,
        FunctionExpression<Function1<TSource, TKey>> keySelector,
        Comparator<TKey> comparator)
    {
        throw Extensions.todo();
    }

    /** Performs a subsequent ordering of the elements in a sequence in
     * descending order according to a key. */
    public static <TSource, TKey extends Comparable<TKey>>
    OrderedQueryable<TSource> thenByDescending(
        OrderedQueryable<TSource> source,
        FunctionExpression<Function1<TSource, TKey>> keySelector)
    {
        throw Extensions.todo();
    }

    /** Performs a subsequent ordering of the elements in a sequence in
     * dscending order according to a key, using a specified comparator. */
    public static <TSource, TKey> OrderedQueryable<TSource> thenByDescending(
        OrderedQueryable<TSource> source,
        FunctionExpression<Function1<TSource, TKey>> keySelector,
        Comparator<TKey> comparator)
    {
        throw Extensions.todo();
    }

    /** Produces the set union of two sequences by using
     * the default equality comparer. */
    public static <TSource> Queryable<TSource> union(
        Queryable<TSource> source0, Enumerable<TSource> source1)
    {
        throw Extensions.todo();
    }

    /** Produces the set union of two sequences by using a
     * specified EqualityComparer<TSource>. */
    public static <TSource> Queryable<TSource> union(
        Queryable<TSource> source0,
        Enumerable<TSource> source1,
        EqualityComparer<TSource> comparer)
    {
        throw Extensions.todo();
    }

    /** Filters a sequence of values based on a
     * predicate. */
    public static <TSource> Queryable<TSource> where(
        Queryable<TSource> source,
        FunctionExpression<Predicate1<TSource>> predicate)
    {
        return new OpQueryable<TSource>(
            source.getProvider(),
            source.getElementType(),
            Expressions.call(source.getExpression(), "where", predicate),
            OpType.WHERE,
            source);
    }

    /** Filters a sequence of values based on a
     * predicate. Each element's index is used in the logic of the
     * predicate function. */
    public static <TSource> Queryable<TSource> whereN(
        Queryable<TSource> source,
        FunctionExpression<Predicate2<TSource, Integer>> predicate)
    {
        throw Extensions.todo();
    }

    /** Merges two sequences by using the specified
     * predicate function. */
    public static <T0, T1, TResult> Queryable<TResult> zip(
        Queryable<T0> source0,
        Enumerable<T1> source1,
        FunctionExpression<Function2<T0, T1, TResult>> resultSelector)
    {
        throw Extensions.todo();
    }

    public static class OpQueryable<T> extends BaseQueryable<T> {
        public final OpType opType;
        public final List<Object> args;

        public OpQueryable(
            QueryProvider provider,
            Type elementType,
            Expression expression,
            OpType opType,
            Object... args)
        {
            super(provider, elementType, expression);
            this.opType = opType;
            this.args =
                args == null || args.length == 0
                    ? Collections.emptyList()
                    : Arrays.asList(args);
        }
    }
}

// End QueryableDefaults.java
