package net.hydromatic.linq4j;

import net.hydromatic.linq4j.expressions.Expressions;
import net.hydromatic.linq4j.expressions.FunctionExpression;
import net.hydromatic.linq4j.function.*;

import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.Comparator;

import static net.hydromatic.linq4j.QueryableDefaults.NonLeafReplayableQueryable;

/**
 * Implementation of {@link QueryableFactory} that records each event
 * and returns an object that can replay them.
 *
 * @author jhyde
 */
public class QueryableRecorder<T> implements QueryableFactory<T> {
    private static final QueryableRecorder INSTANCE =
        new QueryableRecorder();

    @SuppressWarnings("unchecked")
    public static <T> QueryableRecorder<T> instance() {
        return INSTANCE;
    }

    public T aggregate(
        final Queryable<T> source,
        final FunctionExpression<Function2<T, T, T>> func)
    {
        return new QueryableDefaults.NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.aggregate(source, func);
            }
        }.single();
    }

    public <TAccumulate> TAccumulate aggregate(
        final Queryable<T> source,
        final TAccumulate seed,
        final FunctionExpression<Function2<TAccumulate, T, TAccumulate>> func)
    {
        final Queryable<TAccumulate> source1 = (Queryable) source;
        return new QueryableDefaults.NonLeafReplayableQueryable<TAccumulate>(source1) {
            public void replay(QueryableFactory<TAccumulate> factory) {
                QueryableFactory<T> factory1 = (QueryableFactory) factory;
                factory1.aggregate(source, seed, func);
            }
        }.single();
    }

    public <TAccumulate, TResult> TResult aggregate(
        final Queryable<T> source,
        final TAccumulate seed,
        final FunctionExpression<Function2<TAccumulate, T, TAccumulate>> func,
        final FunctionExpression<Function1<TAccumulate, TResult>> selector)
    {
        final Queryable<TResult> source1 = (Queryable) source;
        return new NonLeafReplayableQueryable<TResult>(source1) {
            public void replay(QueryableFactory<TResult> factory) {
                QueryableFactory<TResult> factory1 = (QueryableFactory) factory;
                factory1.aggregate(source, seed, func, selector);
            }
        }.single();
    }

    public boolean all(
        Queryable<T> source,
        FunctionExpression<Predicate1<T>> predicate)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.all(source, predicate);
            }
        }.single();
    }

    public boolean any(Queryable<T> source) {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.any(source);
            }
        }.single();
    }

    /** Determines whether any element of a sequence
     * satisfies a condition. */
    public boolean any(
        Queryable<T> queryable,
        FunctionExpression<Predicate1<T>> predicate)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.any(source, predicate);
            }
        }.single();
    }

    /** Computes the average of a sequence of Decimal
     * values that is obtained by invoking a projection function on
     * each element of the input sequence. */
    public BigDecimal averageBigDecimal(
        Queryable<T> queryable,
        FunctionExpression<BigDecimalFunction1<T>> selector)
    {
        throw Extensions.todo();
    }

    /** Computes the average of a sequence of nullable
     * Decimal values that is obtained by invoking a projection
     * function on each element of the input sequence. */
    public BigDecimal averageNullableBigDecimal(
        Queryable<T> queryable,
        FunctionExpression<NullableBigDecimalFunction1<T>> selector)
    {
        throw Extensions.todo();
    }

    /** Computes the average of a sequence of Double
     * values that is obtained by invoking a projection function on
     * each element of the input sequence. */
    public double averageDouble(
        Queryable<T> queryable,
        FunctionExpression<DoubleFunction1<T>> selector)
    {
        throw Extensions.todo();
    }

    /** Computes the average of a sequence of nullable
     * Double values that is obtained by invoking a projection
     * function on each element of the input sequence. */
    public Double averageNullableDouble(
        Queryable<T> queryable,
        FunctionExpression<NullableDoubleFunction1<T>> selector)
    {
        throw Extensions.todo();
    }

    /** Computes the average of a sequence of int values
     * that is obtained by invoking a projection function on each
     * element of the input sequence. */
    public int averageInteger(
        Queryable<T> queryable,
        FunctionExpression<IntegerFunction1<T>> selector)
    {
        throw Extensions.todo();
    }

    /** Computes the average of a sequence of nullable
     * int values that is obtained by invoking a projection function
     * on each element of the input sequence. */
    public Integer averageNullableInteger(
        Queryable<T> queryable,
        FunctionExpression<NullableIntegerFunction1<T>> selector)
    {
        throw Extensions.todo();
    }

    /** Computes the average of a sequence of Float
     * values that is obtained by invoking a projection function on
     * each element of the input sequence. */
    public float averageFloat(
        Queryable<T> queryable,
        FunctionExpression<FloatFunction1<T>> selector)
    {
        throw Extensions.todo();
    }

    /** Computes the average of a sequence of nullable
     * Float values that is obtained by invoking a projection
     * function on each element of the input sequence. */
    public Float averageNullableFloat(
        Queryable<T> queryable,
        FunctionExpression<NullableFloatFunction1<T>> selector)
    {
        throw Extensions.todo();
    }

    /** Computes the average of a sequence of long values
     * that is obtained by invoking a projection function on each
     * element of the input sequence. */
    public long averageLong(
        Queryable<T> queryable,
        FunctionExpression<LongFunction1<T>> selector)
    {
        throw Extensions.todo();
    }

    /** Computes the average of a sequence of nullable
     * long values that is obtained by invoking a projection function
     * on each element of the input sequence. */
    public Long averageNullableLong(
        Queryable<T> queryable,
        FunctionExpression<NullableLongFunction1<T>> selector)
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
    public <T2> Queryable<T2> cast(
        final Queryable<T> source,
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
    public Queryable<T> concat(
        Queryable<T> queryable0, Enumerable<T> source2)
    {
        throw Extensions.todo();
    }

    /** Determines whether a sequence contains a specified
     * element by using the default equality comparer. */
    public boolean contains(
        Queryable<T> queryable, T element)
    {
        throw Extensions.todo();
    }

    /** Determines whether a sequence contains a specified
     * element by using a specified EqualityComparer<T>. */
    public boolean contains(
        Queryable<T> queryable,
        T element,
        EqualityComparer comparer)
    {
        throw Extensions.todo();
    }

    /** Returns the number of elements in a
     * sequence. */
    public int count(
        Queryable<T> queryable)
    {
        throw Extensions.todo();
    }

    /** Returns the number of elements in the specified
     * sequence that satisfies a condition. */
    public int count(
        Queryable<T> queryable,
        FunctionExpression<Predicate1<T>> func)
    {
        throw Extensions.todo();
    }

    /** Returns the elements of the specified sequence or
     * the type parameter's default value in a singleton collection if
     * the sequence is empty. */
    public Queryable<T> defaultIfEmpty(
        Queryable<T> queryable)
    {
        throw Extensions.todo();
    }

    /** Returns the elements of the specified sequence or
     * the specified value in a singleton collection if the sequence
     * is empty. */
    public T defaultIfEmpty(
        Queryable<T> queryable, T value)
    {
        throw Extensions.todo();
    }

    /** Returns distinct elements from a sequence by using
     * the default equality comparer to compare values. */
    public Queryable<T> distinct(
        Queryable<T> queryable)
    {
        throw Extensions.todo();
    }

    /** Returns distinct elements from a sequence by using
     * a specified EqualityComparer<T> to compare values. */
    public Queryable<T> distinct(
        Queryable<T> queryable, EqualityComparer comparer)
    {
        throw Extensions.todo();
    }

    /** Returns the element at a specified index in a
     * sequence. */
    public T elementAt(
        Queryable<T> queryable, int index)
    {
        throw Extensions.todo();
    }

    /** Returns the element at a specified index in a
     * sequence or a default value if the index is out of
     * range. */
    public T elementAtOrDefault(
        Queryable<T> queryable, int index)
    {
        throw Extensions.todo();
    }

    /** Produces the set difference of two sequences by
     * using the default equality comparer to compare values. (Defined
     * by Queryable.) */
    public Queryable<T> except(
        Queryable<T> queryable, Enumerable<T> enumerable)
    {
        throw Extensions.todo();
    }

    /** Produces the set difference of two sequences by
     * using the specified EqualityComparer<T> to compare
     * values. */
    public Queryable<T> except(
        Queryable<T> queryable,
        Enumerable<T> enumerable,
        EqualityComparer<T> comparer)
    {
        throw Extensions.todo();
    }

    /** Returns the first element of a sequence. (Defined
     * by Queryable.) */
    public T first(
        Queryable<T> queryable)
    {
        throw Extensions.todo();
    }

    /** Returns the first element of a sequence that
     * satisfies a specified condition. */
    public T first(
        Queryable<T> queryable,
        FunctionExpression<Predicate1<T>> predicate)
    {
        throw Extensions.todo();
    }

    /** Returns the first element of a sequence, or a
     * default value if the sequence contains no elements. */
    public T firstOrDefault(
        Queryable<T> queryable)
    {
        throw Extensions.todo();
    }

    /** Returns the first element of a sequence that
     * satisfies a specified condition or a default value if no such
     * element is found. */
    public T firstOrDefault(
        Queryable<T> queryable,
        FunctionExpression<Predicate1<T>> predicate)
    {
        throw Extensions.todo();
    }

    /** Groups the elements of a sequence according to a
     * specified key selector function. */
    public <TKey> Queryable<Grouping<TKey, T>> groupBy(
        Queryable<T> queryable,
        FunctionExpression<Function1<T, TKey>> keySelector)
    {
        throw Extensions.todo();
    }

    /** Groups the elements of a sequence according to a
     * specified key selector function and compares the keys by using
     * a specified comparer. */
    public <TKey> Queryable<Grouping<TKey, T>> groupBy(
        Queryable<T> queryable,
        FunctionExpression<Function1<T, TKey>> keySelector,
        EqualityComparer comparer)
    {
        throw Extensions.todo();
    }

    /** Groups the elements of a sequence according to a
     * specified key selector function and projects the elements for
     * each group by using a specified function. */
    public <TKey, TElement>
    Queryable<Grouping<TKey, TElement>> groupBy(
        Queryable<T> queryable,
        FunctionExpression<Function1<T, TKey>> keySelector,
        FunctionExpression<Function1<T, TElement>> elementSelector)
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
    public <TKey, TResult>
    Queryable<Grouping<TKey, TResult>> groupByK(
        Queryable<T> queryable,
        FunctionExpression<Function1<T, TKey>> keySelector,
        FunctionExpression<Function2<TKey, Enumerable<T>, TResult>>
            elementSelector)
    {
        throw Extensions.todo();
    }

    /** Groups the elements of a sequence and projects the
     * elements for each group by using a specified function. Key
     * values are compared by using a specified comparer.
     */
    public <TKey, TElement>
    Queryable<Grouping<TKey, TElement>> groupBy(
        Queryable<T> queryable,
        FunctionExpression<Function1<T, TKey>> keySelector,
        FunctionExpression<Function1<T, TElement>> elementSelector,
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
    public <TKey, TResult> Queryable<TResult> groupByK(
        Queryable<T> queryable,
        FunctionExpression<Function1<T, TKey>> keySelector,
        FunctionExpression<Function2<TKey, Enumerable<T>,
            TResult>> elementSelector,
        EqualityComparer comparer)
    {
        throw Extensions.todo();
    }

    /** Groups the elements of a sequence according to a
     * specified key selector function and creates a result value from
     * each group and its key. The elements of each group are
     * projected by using a specified function. */
    public <TKey, TElement, TResult> Queryable<TResult> groupBy(
        Queryable<T> queryable,
        FunctionExpression<Function1<T, TKey>> keySelector,
        FunctionExpression<Function1<T, TElement>> elementSelector,
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
    public <TKey, TElement, TResult> Queryable<TResult> groupBy(
        Queryable<T> queryable,
        FunctionExpression<Function1<T, TKey>> keySelector,
        FunctionExpression<Function1<T, TElement>> elementSelector,
        FunctionExpression<Function2<TKey, Enumerable<TElement>, TResult>>
            resultSelector,
        EqualityComparer<TKey> comparer)
    {
        throw Extensions.todo();
    }

    public <TInner, TKey, TResult> Queryable<TResult> groupJoin(
        Queryable<T> source,
        Enumerable<TInner> inner,
        FunctionExpression<Function1<T, TKey>> outerKeySelector,
        FunctionExpression<Function1<TInner, TKey>> innerKeySelector,
        FunctionExpression<Function2<T, Enumerable<TInner>, TResult>>
            resultSelector)
    {
        throw Extensions.todo();
    }

    public <TInner, TKey, TResult> Enumerable<TResult> groupJoin(
        Queryable<T> source,
        Enumerable<TInner> inner,
        FunctionExpression<Function1<T, TKey>> outerKeySelector,
        FunctionExpression<Function1<TInner, TKey>> innerKeySelector,
        FunctionExpression<Function2<T, Enumerable<TInner>, TResult>>
            resultSelector,
        EqualityComparer<TKey> comparer)
    {
        throw Extensions.todo();
    }

    /** Produces the set intersection of two sequences by
     * using the default equality comparer to compare values. (Defined
     * by Queryable.) */
    public Queryable<T> intersect(
        Queryable<T> queryable, Enumerable<T> enumerable)
    {
        throw Extensions.todo();
    }

    /** Produces the set intersection of two sequences by
     * using the specified EqualityComparer<T> to compare
     * values. */
    public Queryable<T> intersect(
        Queryable<T> queryable,
        Enumerable<T> enumerable,
        EqualityComparer<T> comparer)
    {
        throw Extensions.todo();
    }

    public <TInner, TKey, TResult> Queryable<TResult> join(
        Queryable<T> source,
        Enumerable<TInner> inner,
        FunctionExpression<Function1<T, TKey>> outerKeySelector,
        FunctionExpression<Function1<TInner, TKey>> innerKeySelector,
        FunctionExpression<Function2<T, TInner, TResult>> resultSelector)
    {
        throw Extensions.todo();
    }

    public <TInner, TKey, TResult> Queryable<TResult> join(
        Queryable<T> source,
        Enumerable<TInner> inner,
        FunctionExpression<Function1<T, TKey>> outerKeySelector,
        FunctionExpression<Function1<TInner, TKey>> innerKeySelector,
        FunctionExpression<Function2<T, TInner, TResult>> resultSelector,
        EqualityComparer<TKey> comparer)
    {
        throw Extensions.todo();
    }

    /** Returns the last element in a sequence. (Defined
     * by Queryable.) */
    public T last(Queryable<T> queryable) {
        throw Extensions.todo();
    }

    /** Returns the last element of a sequence that
     * satisfies a specified condition. */
    public T last(
        Queryable<T> queryable,
        FunctionExpression<Predicate1<T>> predicate)
    {
        throw Extensions.todo();
    }

    /** Returns the last element in a sequence, or a
     * default value if the sequence contains no elements. */
    public T lastOrDefault(Queryable<T> queryable)
    {
        throw Extensions.todo();
    }

    /** Returns the last element of a sequence that
     * satisfies a condition or a default value if no such element is
     * found. */
    public T lastOrDefault(
        Queryable<T> queryable,
        FunctionExpression<Predicate1<T>> predicate)
    {
        throw Extensions.todo();
    }

    /** Returns an long that represents the total number
     * of elements in a sequence. */
    public long longCount(Queryable<T> xable) {
        throw Extensions.todo();
    }

    /** Returns an long that represents the number of
     * elements in a sequence that satisfy a condition. */
    public long longCount(
        Queryable<T> queryable,
        FunctionExpression<Predicate1<T>> predicate)
    {
        throw Extensions.todo();
    }

    /** Returns the maximum value in a generic
     * IQueryable<T>. */
    public T max(Queryable<T> queryable) {
        throw Extensions.todo();
    }

    public <TResult extends Comparable<TResult>> TResult max(
        Queryable<T> source,
        FunctionExpression<Function1<T, TResult>> selector)
    {
        throw Extensions.todo();
    }

    /** Returns the minimum value in a generic
     * IQueryable<T>. */
    public T min(Queryable<T> queryable) {
        throw Extensions.todo();
    }

    public <TResult extends Comparable<TResult>> TResult min(
        Queryable<T> queryable,
        FunctionExpression<Function1<T, TResult>> selector)
    {
        throw Extensions.todo();
    }

    public <TResult> Queryable<TResult> ofType(
        Queryable<T> source,
        Class<TResult> clazz)
    {
        throw Extensions.todo();
    }

    public <TKey extends Comparable>
    OrderedQueryable<T> orderBy(
        Queryable<T> source,
        FunctionExpression<Function1<T, TKey>> keySelector)
    {
        throw Extensions.todo();
    }

    /** Sorts the elements of a sequence in ascending
     * order by using a specified comparer. */
    public <TKey> OrderedQueryable<T> orderBy(
        Queryable<T> source,
        FunctionExpression<Function1<T, TKey>> keySelector,
        Comparator<TKey> comparator)
    {
        throw Extensions.todo();
    }

    /** Sorts the elements of a sequence in descending
     * order according to a key. */
    public <TKey extends Comparable>
    OrderedQueryable<T> orderByDescending(
        Queryable<T> source,
        FunctionExpression<Function1<T, TKey>> keySelector)
    {
        throw Extensions.todo();
    }

    /** Sorts the elements of a sequence in descending
     * order by using a specified comparer. */
    public <TKey> OrderedQueryable<T> orderByDescending(
        Queryable<T> source,
        FunctionExpression<Function1<T, TKey>> keySelector,
        Comparator<TKey> comparator)
    {
        throw Extensions.todo();
    }

    /** Inverts the order of the elements in a
     * sequence. */
    public Queryable<T> reverse(
        Queryable<T> source)
    {
        throw Extensions.todo();
    }

    /** Projects each element of a sequence into a new form. */
    public <TResult> Queryable<TResult> select(
        Queryable<T> source,
        FunctionExpression<Function1<T, TResult>> selector)
    {
        return source.getProvider().createQuery(
            Expressions.call(source.getExpression(), "select", selector),
            functionResultType(selector));
    }

    private <P0, R> Type functionResultType(
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
    public <TResult> Queryable<TResult> selectN(
        Queryable<T> source,
        FunctionExpression<Function2<T, Integer, TResult>> selector)
    {
        throw Extensions.todo();
    }

    /** Projects each element of a sequence to an
     * Enumerable<T> and combines the resulting sequences into one
     * sequence. */
    public <TResult> Queryable<TResult> selectMany(
        Queryable<T> source,
        FunctionExpression<Function1<T, Enumerable<TResult>>> selector)
    {
        throw Extensions.todo();
    }

    /** Projects each element of a sequence to an
     * Enumerable<T> and combines the resulting sequences into one
     * sequence. The index of each source element is used in the
     * projected form of that element.
     *
     * <p>NOTE: Renamed from {@code selectMany} because had same erasure as
     * {@link #selectMany(net.hydromatic.linq4j.Queryable, net.hydromatic.linq4j.expressions.FunctionExpression)}</p>
     */
    public <TResult> Queryable<TResult> selectManyN(
        Queryable<T> source,
        FunctionExpression<Function2<T, Integer, Enumerable<TResult>>>
            selector)
    {
        throw Extensions.todo();
    }

    /** Projects each element of a sequence to an
     * Enumerable<T> that incorporates the index of the source
     * element that produced it. A result selector function is invoked
     * on each element of each intermediate sequence, and the
     * resulting values are combined into a single, one-dimensional
     * sequence and returned. */
    public <TCollection, TResult> Queryable<TResult> selectMany(
        Queryable<T> source,
        FunctionExpression<Function2<T, Integer, Enumerable<TCollection>>>
            collectionSelector,
        FunctionExpression<Function2<T, TCollection, TResult>>
            resultSelector)
    {
        throw Extensions.todo();
    }

    /** Projects each element of a sequence to an
     * Enumerable<T> and invokes a result selector function on each
     * element therein. The resulting values from each intermediate
     * sequence are combined into a single, one-dimensional sequence
     * and returned.
     *
     * <p>NOTE: Renamed from {@code selectMany} because had same erasure as
     * {@link #selectMany(net.hydromatic.linq4j.Queryable, net.hydromatic.linq4j.expressions.FunctionExpression, net.hydromatic.linq4j.expressions.FunctionExpression)}</p>
     * */
    public <TCollection, TResult>
    Queryable<TResult> selectManyN(
        Queryable<T> source,
        FunctionExpression<Function1<T, Enumerable<TCollection>>>
            collectionSelector,
        FunctionExpression<Function2<T, TCollection, TResult>>
            resultSelector)
    {
        throw Extensions.todo();
    }

    /** Determines whether two sequences are equal by
     * using the default equality comparer to compare
     * elements. */
    public boolean sequenceEqual(
        Queryable<T> queryable, Enumerable<T> enumerable)
    {
        throw Extensions.todo();
    }

    /** Determines whether two sequences are equal by
     * using a specified EqualityComparer<T> to compare
     * elements. */
    public boolean sequenceEqual(
        Queryable<T> queryable,
        Enumerable<T> enumerable,
        EqualityComparer<T> comparer)
    {
        throw Extensions.todo();
    }

    /** Returns the only element of a sequence, and throws
     * an exception if there is not exactly one element in the
     * sequence. */
    public T single(
        Queryable<T> source)
    {
        throw Extensions.todo();
    }

    /** Returns the only element of a sequence that
     * satisfies a specified condition, and throws an exception if
     * more than one such element exists. */
    public T single(
        Queryable<T> source,
        FunctionExpression<Predicate1<T>> predicate)
    {
        throw Extensions.todo();
    }

    /** Returns the only element of a sequence, or a
     * default value if the sequence is empty; this method throws an
     * exception if there is more than one element in the
     * sequence. */
    public T singleOrDefault(
        Queryable<T> source)
    {
        throw Extensions.todo();
    }

    /** Returns the only element of a sequence that
     * satisfies a specified condition or a default value if no such
     * element exists; this method throws an exception if more than
     * one element satisfies the condition. */
    public T singleOrDefault(
        Queryable<T> source,
        FunctionExpression<Predicate1<T>> predicate)
    {
        throw Extensions.todo();
    }

    /** Bypasses a specified number of elements in a
     * sequence and then returns the remaining elements. */
    public Queryable<T> skip(
        Queryable<T> source, int count)
    {
        return EnumerableDefaults.skip(source.asEnumerable(), count)
            .asQueryable();
    }

    /** Bypasses elements in a sequence as long as a
     * specified condition is true and then returns the remaining
     * elements. */
    public Queryable<T> skipWhile(
        Queryable<T> source,
        FunctionExpression<Predicate1<T>> predicate)
    {
        return skipWhileN(
            source,
            Expressions.lambda(
                Functions.<T, Integer>toPredicate2(
                    predicate.getFunction())));
    }

    /** Bypasses elements in a sequence as long as a
     * specified condition is true and then returns the remaining
     * elements. The element's index is used in the logic of the
     * predicate function. */
    public Queryable<T> skipWhileN(
        final Queryable<T> source,
        final FunctionExpression<Predicate2<T, Integer>> predicate)
    {
        return new BaseQueryable<T>(
            source.getProvider(),
            source.getElementType(),
            source.getExpression())
        {
            public Enumerator<T> enumerator() {
                return new EnumerableDefaults.SkipWhileEnumerator<T>(
                    source.enumerator(), predicate.getFunction());
            }
        };
    }

    /** Computes the sum of the sequence of Decimal values
     * that is obtained by invoking a projection function on each
     * element of the input sequence. */
    public BigDecimal sumBigDecimal(
        Queryable<T> sources,
        FunctionExpression<BigDecimalFunction1<T>> selector)
    {
        throw Extensions.todo();
    }

    /** Computes the sum of the sequence of nullable
     * Decimal values that is obtained by invoking a projection
     * function on each element of the input sequence. */
    public BigDecimal sumNullableBigDecimal(
        Queryable<T> source,
        FunctionExpression<NullableBigDecimalFunction1<T>> selector)
    {
        throw Extensions.todo();
    }

    /** Computes the sum of the sequence of Double values
     * that is obtained by invoking a projection function on each
     * element of the input sequence. */
    public double sumDouble(
        Queryable<T> source,
        FunctionExpression<DoubleFunction1<T>> selector)
    {
        throw Extensions.todo();
    }

    /** Computes the sum of the sequence of nullable
     * Double values that is obtained by invoking a projection
     * function on each element of the input sequence. */
    public Double sumNullableDouble(
        Queryable<T> source,
        FunctionExpression<NullableDoubleFunction1<T>> selector)
    {
        throw Extensions.todo();
    }

    /** Computes the sum of the sequence of int values
     * that is obtained by invoking a projection function on each
     * element of the input sequence. */
    public int sumInteger(
        Queryable<T> source, FunctionExpression<IntegerFunction1<T>>
        selector)
    {
        throw Extensions.todo();
    }

    /** Computes the sum of the sequence of nullable int
     * values that is obtained by invoking a projection function on
     * each element of the input sequence. */
    public Integer sumNullableInteger(
        Queryable<T> source,
        FunctionExpression<NullableIntegerFunction1<T>> selector)
    {
        throw Extensions.todo();
    }

    /** Computes the sum of the sequence of long values
     * that is obtained by invoking a projection function on each
     * element of the input sequence. */
    public long sumLong(
        Queryable<T> source,
        FunctionExpression<LongFunction1<T>> selector)
    {
        throw Extensions.todo();
    }

    /** Computes the sum of the sequence of nullable long
     * values that is obtained by invoking a projection function on
     * each element of the input sequence. */
    public Long sumNullableLong(
        Queryable<T> source,
        FunctionExpression<NullableLongFunction1<T>> selector)
    {
        throw Extensions.todo();
    }

    /** Computes the sum of the sequence of Float values
     * that is obtained by invoking a projection function on each
     * element of the input sequence. */
    public float sumFloat(
        Queryable<T> source,
        FunctionExpression<FloatFunction1<T>> selector)
    {
        throw Extensions.todo();
    }

    /** Computes the sum of the sequence of nullable
     * Float values that is obtained by invoking a projection
     * function on each element of the input sequence. */
    public Float sumNullableFloat(
        Queryable<T> source,
        FunctionExpression<NullableFloatFunction1<T>> selector)
    {
        throw Extensions.todo();
    }

    /** Returns a specified number of contiguous elements
     * from the start of a sequence. */
    public Queryable<T> take(
        Queryable<T> source, int count)
    {
        return EnumerableDefaults.take(source.asEnumerable(), count)
            .asQueryable();
    }

    /** Returns elements from a sequence as long as a
     * specified condition is true. */
    public Queryable<T> takeWhile(
        Queryable<T> source,
        FunctionExpression<Predicate1<T>> predicate)
    {
        return takeWhileN(
            source,
            Expressions.lambda(
                Functions.<T, Integer>toPredicate2(
                    predicate.getFunction())));
    }

    /** Returns elements from a sequence as long as a
     * specified condition is true. The element's index is used in the
     * logic of the predicate function. */
    public Queryable<T> takeWhileN(
        final Queryable<T> source,
        final FunctionExpression<Predicate2<T, Integer>> predicate)
    {
        return new BaseQueryable<T>(
            source.getProvider(),
            source.getElementType(),
            source.getExpression())
        {
            public Enumerator<T> enumerator() {
                return new EnumerableDefaults.TakeWhileEnumerator<T>(
                    source.enumerator(), predicate.getFunction());
            }
        };
    }

    /** Performs a subsequent ordering of the elements in a sequence in
     * ascending order according to a key. */
    public <TKey extends Comparable<TKey>>
    OrderedQueryable<T> thenBy(
        OrderedQueryable<T> source,
        FunctionExpression<Function1<T, TKey>> keySelector)
    {
        throw Extensions.todo();
    }

    /** Performs a subsequent ordering of the elements in a sequence in
     * ascending order according to a key, using a specified comparator. */
    public <TKey> OrderedQueryable<T> thenBy(
        OrderedQueryable<T> source,
        FunctionExpression<Function1<T, TKey>> keySelector,
        Comparator<TKey> comparator)
    {
        throw Extensions.todo();
    }

    /** Performs a subsequent ordering of the elements in a sequence in
     * descending order according to a key. */
    public <TKey extends Comparable<TKey>>
    OrderedQueryable<T> thenByDescending(
        OrderedQueryable<T> source,
        FunctionExpression<Function1<T, TKey>> keySelector)
    {
        throw Extensions.todo();
    }

    /** Performs a subsequent ordering of the elements in a sequence in
     * dscending order according to a key, using a specified comparator. */
    public <TKey> OrderedQueryable<T> thenByDescending(
        OrderedQueryable<T> source,
        FunctionExpression<Function1<T, TKey>> keySelector,
        Comparator<TKey> comparator)
    {
        throw Extensions.todo();
    }

    /** Produces the set union of two sequences by using
     * the default equality comparer. */
    public Queryable<T> union(
        Queryable<T> source0, Enumerable<T> source1)
    {
        throw Extensions.todo();
    }

    /** Produces the set union of two sequences by using a
     * specified EqualityComparer<T>. */
    public Queryable<T> union(
        Queryable<T> source0,
        Enumerable<T> source1,
        EqualityComparer<T> comparer)
    {
        throw Extensions.todo();
    }

    public Queryable<T> where(
        final Queryable<T> source,
        final FunctionExpression<? extends Predicate1<T>> predicate)
    {
        return new QueryableDefaults.NonLeafReplayableQueryable<T>(source) {
            public Queryable<T> replay(QueryableFactory<T> factory) {
                return factory.where(source, predicate);
            }
        };
    }

    public Queryable<T> whereN(
        Queryable<T> source,
        FunctionExpression<? extends Predicate2<T, Integer>> predicate)
    {
        throw Extensions.todo();
    }

    public <T1, TResult> Queryable<TResult> zip(
        Queryable<T> source,
        Enumerable<T1> source1,
        FunctionExpression<Function2<T, T1, TResult>> resultSelector)
    {
        throw Extensions.todo();
    }
}

// End QueryableRecorder.java
