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
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.aggregate(source, seed, func);
            }
        }.castSingle();
    }

    public <TAccumulate, TResult> TResult aggregate(
        final Queryable<T> source,
        final TAccumulate seed,
        final FunctionExpression<Function2<TAccumulate, T, TAccumulate>> func,
        final FunctionExpression<Function1<TAccumulate, TResult>> selector)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.aggregate(source, seed, func, selector);
            }
        }.castSingle();
    }

    public boolean all(
        final Queryable<T> source,
        final FunctionExpression<Predicate1<T>> predicate)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.all(source, predicate);
            }
        }.castSingle();
    }

    public boolean any(final Queryable<T> source) {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.any(source);
            }
        }.castSingle();
    }

    public boolean any(
        final Queryable<T> source,
        final FunctionExpression<Predicate1<T>> predicate)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.any(source, predicate);
            }
        }.castSingle();
    }

    public BigDecimal averageBigDecimal(
        final Queryable<T> source,
        final FunctionExpression<BigDecimalFunction1<T>> selector)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.averageBigDecimal(source, selector);
            }
        }.castSingle();
    }

    public BigDecimal averageNullableBigDecimal(
        final Queryable<T> source,
        final FunctionExpression<NullableBigDecimalFunction1<T>> selector)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.averageNullableBigDecimal(source, selector);
            }
        }.castSingle();
    }

    public double averageDouble(
        final Queryable<T> source,
        final FunctionExpression<DoubleFunction1<T>> selector)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.averageDouble(source, selector);
            }
        }.castSingle();
    }

    public Double averageNullableDouble(
        final Queryable<T> source,
        final FunctionExpression<NullableDoubleFunction1<T>> selector)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.averageNullableDouble(source, selector);
            }
        }.castSingle();
    }

    public int averageInteger(
        final Queryable<T> source,
        final FunctionExpression<IntegerFunction1<T>> selector)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.averageInteger(source, selector);
            }
        }.castSingle();
    }

    public Integer averageNullableInteger(
        final Queryable<T> source,
        final FunctionExpression<NullableIntegerFunction1<T>> selector)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.averageNullableInteger(source, selector);
            }
        }.castSingle();
    }

    public float averageFloat(
        final Queryable<T> source,
        final FunctionExpression<FloatFunction1<T>> selector)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.averageFloat(source, selector);
            }
        }.castSingle();
    }

    public Float averageNullableFloat(
        final Queryable<T> source,
        final FunctionExpression<NullableFloatFunction1<T>> selector)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.averageNullableFloat(source, selector);
            }
        }.castSingle();
    }

    /** Computes the average of a sequence of long values
     * that is obtained by invoking a projection function on each
     * element of the input sequence. */
    public long averageLong(
        final Queryable<T> source,
        final FunctionExpression<LongFunction1<T>> selector)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.averageLong(source, selector);
            }
        }.castSingle();
    }

    /** Computes the average of a sequence of nullable
     * long values that is obtained by invoking a projection function
     * on each element of the input sequence. */
    public Long averageNullableLong(
        final Queryable<T> source,
        final FunctionExpression<NullableLongFunction1<T>> selector)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.averageNullableLong(source, selector);
            }
        }.castSingle();
    }

    public <T2> Queryable<T2> cast(
        final Queryable<T> source,
        final Class<T2> clazz)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.cast(source, clazz);
            }
        }.castQueryable();
    }

    /** Concatenates two sequences. */
    public Queryable<T> concat(
        final Queryable<T> source,
        final Enumerable<T> source2)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.concat(source, source2);
            }
        };
    }

    public boolean contains(
        final Queryable<T> source,
        final T element)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.contains(source, element);
            }
        }.castSingle();
    }

    public boolean contains(
        final Queryable<T> source,
        final T element,
        final EqualityComparer<T> comparer)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.contains(source, element, comparer);
            }
        }.castSingle();
    }

    public int count(
        final Queryable<T> source)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.count(source);
            }
        }.castSingle();
    }

    public int count(
        final Queryable<T> source,
        final FunctionExpression<Predicate1<T>> func)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.count(source, func);
            }
        }.castSingle();
    }

    public Queryable<T> defaultIfEmpty(
        final Queryable<T> source)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.defaultIfEmpty(source);
            }
        };
    }

    public Queryable<T> defaultIfEmpty(
        final Queryable<T> source,
        final T value)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.defaultIfEmpty(source, value);
            }
        };
    }

    public Queryable<T> distinct(
        final Queryable<T> source)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.distinct(source);
            }
        };
    }

    public Queryable<T> distinct(
        final Queryable<T> source,
        final EqualityComparer<T> comparer)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.distinct(source, comparer);
            }
        };
    }

    public T elementAt(
        final Queryable<T> source,
        final int index)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.elementAt(source, index);
            }
        }.castSingle();
    }

    public T elementAtOrDefault(
        final Queryable<T> source,
        final int index)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.elementAtOrDefault(source, index);
            }
        }.castSingle();
    }

    public Queryable<T> except(
        final Queryable<T> source,
        final Enumerable<T> enumerable)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.except(source, enumerable);
            }
        };
    }

    public Queryable<T> except(
        final Queryable<T> source,
        final Enumerable<T> enumerable,
        final EqualityComparer<T> comparer)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.except(source, enumerable, comparer);
            }
        };
    }

    public T first(
        final Queryable<T> source)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.first(source);
            }
        }.single();
    }

    public T first(
        final Queryable<T> source,
        final FunctionExpression<Predicate1<T>> predicate)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.first(source, predicate);
            }
        }.single();
    }

    public T firstOrDefault(
        final Queryable<T> source)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.firstOrDefault(source);
            }
        }.single();
    }

    public T firstOrDefault(
        final Queryable<T> source,
        final FunctionExpression<Predicate1<T>> predicate)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.firstOrDefault(source, predicate);
            }
        }.single();
    }

    public <TKey> Queryable<Grouping<TKey, T>> groupBy(
        final Queryable<T> source,
        final FunctionExpression<Function1<T, TKey>> keySelector)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.groupBy(source, keySelector);
            }
        }.castQueryable();
    }

    public <TKey> Queryable<Grouping<TKey, T>> groupBy(
        final Queryable<T> source,
        final FunctionExpression<Function1<T, TKey>> keySelector,
        final EqualityComparer<T> comparer)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.groupBy(source, keySelector, comparer);
            }
        }.castQueryable();
    }

    public <TKey, TElement>
    Queryable<Grouping<TKey, TElement>> groupBy(
        final Queryable<T> source,
        final FunctionExpression<Function1<T, TKey>> keySelector,
        final FunctionExpression<Function1<T, TElement>> elementSelector)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.groupBy(source, keySelector, elementSelector);
            }
        }.castQueryable();
    }

    public <TKey, TResult>
    Queryable<Grouping<TKey, TResult>> groupByK(
        final Queryable<T> source,
        final FunctionExpression<Function1<T, TKey>> keySelector,
        final FunctionExpression<Function2<TKey, Enumerable<T>, TResult>>
            elementSelector)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.groupByK(source, keySelector, elementSelector);
            }
        }.castQueryable();
    }

    public <TKey, TElement>
    Queryable<Grouping<TKey, TElement>> groupBy(
        final Queryable<T> source,
        final FunctionExpression<Function1<T, TKey>> keySelector,
        final FunctionExpression<Function1<T, TElement>> elementSelector,
        final EqualityComparer<T> comparer)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.groupBy(source, keySelector, elementSelector, comparer);
            }
        }.castQueryable();
    }

    public <TKey, TResult> Queryable<TResult> groupByK(
        final Queryable<T> source,
        final FunctionExpression<Function1<T, TKey>> keySelector,
        final FunctionExpression<Function2<TKey, Enumerable<T>,
            TResult>> elementSelector,
        final EqualityComparer<TKey> comparer)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.groupByK(
                    source, keySelector, elementSelector, comparer);
            }
        }.castQueryable();
    }

    public <TKey, TElement, TResult> Queryable<TResult> groupBy(
        final Queryable<T> source,
        final FunctionExpression<Function1<T, TKey>> keySelector,
        final FunctionExpression<Function1<T, TElement>> elementSelector,
        final FunctionExpression<Function2<TKey, Enumerable<TElement>, TResult>>
            resultSelector)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.groupBy(
                    source, keySelector, elementSelector, resultSelector);
            }
        }.castQueryable();
    }

    public <TKey, TElement, TResult> Queryable<TResult> groupBy(
        final Queryable<T> source,
        final FunctionExpression<Function1<T, TKey>> keySelector,
        final FunctionExpression<Function1<T, TElement>> elementSelector,
        final FunctionExpression<Function2<TKey, Enumerable<TElement>, TResult>>
            resultSelector,
        final EqualityComparer<TKey> comparer)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.groupBy(
                    source, keySelector, elementSelector, resultSelector,
                    comparer);
            }
        }.castQueryable();
    }

    public <TInner, TKey, TResult> Queryable<TResult> groupJoin(
        final Queryable<T> source,
        final Enumerable<TInner> inner,
        final FunctionExpression<Function1<T, TKey>> outerKeySelector,
        final FunctionExpression<Function1<TInner, TKey>> innerKeySelector,
        final FunctionExpression<Function2<T, Enumerable<TInner>, TResult>>
            resultSelector)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.groupJoin(
                    source, inner, outerKeySelector, innerKeySelector,
                    resultSelector);
            }
        }.castQueryable();
    }

    public <TInner, TKey, TResult> Queryable<TResult> groupJoin(
        final Queryable<T> source,
        final Enumerable<TInner> inner,
        final FunctionExpression<Function1<T, TKey>> outerKeySelector,
        final FunctionExpression<Function1<TInner, TKey>> innerKeySelector,
        final FunctionExpression<Function2<T, Enumerable<TInner>, TResult>>
            resultSelector,
        final EqualityComparer<TKey> comparer)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.groupJoin(
                    source, inner, outerKeySelector, innerKeySelector,
                    resultSelector, comparer);
            }
        }.castQueryable();
    }

    public Queryable<T> intersect(
        final Queryable<T> source,
        final Enumerable<T> enumerable)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.intersect(source, enumerable);
            }
        };
    }

    public Queryable<T> intersect(
        final Queryable<T> source,
        final Enumerable<T> enumerable,
        final EqualityComparer<T> comparer)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.intersect(source, enumerable, comparer);
            }
        };
    }

    public <TInner, TKey, TResult> Queryable<TResult> join(
        final Queryable<T> source,
        final Enumerable<TInner> inner,
        final FunctionExpression<Function1<T, TKey>> outerKeySelector,
        final FunctionExpression<Function1<TInner, TKey>> innerKeySelector,
        final FunctionExpression<Function2<T, TInner, TResult>> resultSelector)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.join(
                    source, inner, outerKeySelector, innerKeySelector,
                    resultSelector);
            }
        }.castQueryable();
    }

    public <TInner, TKey, TResult> Queryable<TResult> join(
        final Queryable<T> source,
        final Enumerable<TInner> inner,
        final FunctionExpression<Function1<T, TKey>> outerKeySelector,
        final FunctionExpression<Function1<TInner, TKey>> innerKeySelector,
        final FunctionExpression<Function2<T, TInner, TResult>> resultSelector,
        final EqualityComparer<TKey> comparer)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.join(
                    source, inner, outerKeySelector, innerKeySelector,
                    resultSelector, comparer);
            }
        }.castQueryable();
    }

    public T last(
        final Queryable<T> source)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.last(source);
            }
        }.single();
    }

    public T last(
        final Queryable<T> source,
        final FunctionExpression<Predicate1<T>> predicate)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.last(source, predicate);
            }
        }.single();
    }

    public T lastOrDefault(
        final Queryable<T> source)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.lastOrDefault(source);
            }
        }.single();
    }

    public T lastOrDefault(
        final Queryable<T> source,
        final FunctionExpression<Predicate1<T>> predicate)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.lastOrDefault(source, predicate);
            }
        }.single();
    }

    public long longCount(
        final Queryable<T> source)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.longCount(source);
            }
        }.castSingle();
    }

    public long longCount(
        final Queryable<T> source,
        final FunctionExpression<Predicate1<T>> predicate)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.longCount(source, predicate);
            }
        }.longCount();
    }

    public T max(
        final Queryable<T> source)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.max(source);
            }
        }.castSingle();
    }

    public <TResult extends Comparable<TResult>> TResult max(
        final Queryable<T> source,
        final FunctionExpression<Function1<T, TResult>> selector)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.max(source, selector);
            }
        }.castSingle();
    }

    public T min(
        final Queryable<T> source)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.min(source);
            }
        }.castSingle();
    }

    public <TResult extends Comparable<TResult>> TResult min(
        final Queryable<T> source,
        final FunctionExpression<Function1<T, TResult>> selector)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.min(source, selector);
            }
        }.castSingle();
    }

    public <TResult> Queryable<TResult> ofType(
        final Queryable<T> source,
        final Class<TResult> clazz)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.ofType(source, clazz);
            }
        }.castQueryable();
    }

    public <TKey extends Comparable>
    OrderedQueryable<T> orderBy(
        final Queryable<T> source,
        final FunctionExpression<Function1<T, TKey>> keySelector)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.xxx(source, selector);
            }
        }.castSingle();
    }

    /** Sorts the elements of a sequence in ascending
     * order by using a specified comparer. */
    public <TKey> OrderedQueryable<T> orderBy(
        final Queryable<T> source,
        final FunctionExpression<Function1<T, TKey>> keySelector,
        Comparator<TKey> comparator)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.xxx(source, selector);
            }
        }.castSingle();
    }

    /** Sorts the elements of a sequence in descending
     * order according to a key. */
    public <TKey extends Comparable>
    OrderedQueryable<T> orderByDescending(
        final Queryable<T> source,
        final FunctionExpression<Function1<T, TKey>> keySelector)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.xxx(source, selector);
            }
        }.castSingle();
    }

    public <TKey> OrderedQueryable<T> orderByDescending(
        final Queryable<T> source,
        final FunctionExpression<Function1<T, TKey>> keySelector,
        Comparator<TKey> comparator)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.xxx(source, selector);
            }
        }.castSingle();
    }

    /** Inverts the order of the elements in a
     * sequence. */
    public Queryable<T> reverse(
        final Queryable<T> source)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.xxx(source, selector);
            }
        }.castSingle();
    }

    /** Projects each element of a sequence into a new form. */
    public <TResult> Queryable<TResult> select(
        final Queryable<T> source,
        final FunctionExpression<Function1<T, TResult>> selector)
    {
        return source.getProvider().createQuery(
            Expressions.call(source.getExpression(), "select", selector),
            functionResultType(selector));
    }

    private <P0, R> Type functionResultType(
        final FunctionExpression<Function1<P0, R>> selector)
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
        final Queryable<T> source,
        final FunctionExpression<Function2<T, Integer, TResult>> selector)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.xxx(source, selector);
            }
        }.castSingle();
    }

    /** Projects each element of a sequence to an
     * Enumerable<T> and combines the resulting sequences into one
     * sequence. */
    public <TResult> Queryable<TResult> selectMany(
        final Queryable<T> source,
        final FunctionExpression<Function1<T, Enumerable<TResult>>> selector)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.xxx(source, selector);
            }
        }.castSingle();
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
        final Queryable<T> source,
        final FunctionExpression<Function2<T, Integer, Enumerable<TResult>>>
            selector)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.xxx(source, selector);
            }
        }.castSingle();
    }

    /** Projects each element of a sequence to an
     * Enumerable<T> that incorporates the index of the source
     * element that produced it. A result selector function is invoked
     * on each element of each intermediate sequence, and the
     * resulting values are combined into a single, one-dimensional
     * sequence and returned. */
    public <TCollection, TResult> Queryable<TResult> selectMany(
        final Queryable<T> source,
        final FunctionExpression<Function2<T, Integer, Enumerable<TCollection>>>
            collectionSelector,
        final FunctionExpression<Function2<T, TCollection, TResult>>
            resultSelector)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.xxx(source, selector);
            }
        }.castSingle();
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
        final Queryable<T> source,
        final FunctionExpression<Function1<T, Enumerable<TCollection>>>
            collectionSelector,
        final FunctionExpression<Function2<T, TCollection, TResult>>
            resultSelector)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.xxx(source, selector);
            }
        }.castSingle();
    }

    public boolean sequenceEqual(
        final Queryable<T> source,
        final Enumerable<T> enumerable)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.xxx(source, selector);
            }
        }.castSingle();
    }

    /** Determines whether two sequences are equal by
     * using a specified EqualityComparer<T> to compare
     * elements. */
    public boolean sequenceEqual(
        final Queryable<T> source,
        Enumerable<T> enumerable,
        EqualityComparer<T> comparer)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.xxx(source, selector);
            }
        }.castSingle();
    }

    /** Returns the only element of a sequence, and throws
     * an exception if there is not exactly one element in the
     * sequence. */
    public T single(
        final Queryable<T> source)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.xxx(source, selector);
            }
        }.castSingle();
    }

    /** Returns the only element of a sequence that
     * satisfies a specified condition, and throws an exception if
     * more than one such element exists. */
    public T single(
        final Queryable<T> source,
        final FunctionExpression<Predicate1<T>> predicate)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.xxx(source, selector);
            }
        }.castSingle();
    }

    /** Returns the only element of a sequence, or a
     * default value if the sequence is empty; this method throws an
     * exception if there is more than one element in the
     * sequence. */
    public T singleOrDefault(
        final Queryable<T> source)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.xxx(source, selector);
            }
        }.castSingle();
    }

    /** Returns the only element of a sequence that
     * satisfies a specified condition or a default value if no such
     * element exists; this method throws an exception if more than
     * one element satisfies the condition. */
    public T singleOrDefault(
        final Queryable<T> source,
        final FunctionExpression<Predicate1<T>> predicate)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.xxx(source, selector);
            }
        }.castSingle();
    }

    /** Bypasses a specified number of elements in a
     * sequence and then returns the remaining elements. */
    public Queryable<T> skip(
        final Queryable<T> source, int count)
    {
        return EnumerableDefaults.skip(source.asEnumerable(), count)
            .asQueryable();
    }

    /** Bypasses elements in a sequence as long as a
     * specified condition is true and then returns the remaining
     * elements. */
    public Queryable<T> skipWhile(
        final Queryable<T> source,
        final FunctionExpression<Predicate1<T>> predicate)
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
        final Queryable<T> sources,
        final FunctionExpression<BigDecimalFunction1<T>> selector)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.xxx(source, selector);
            }
        }.castSingle();
    }

    /** Computes the sum of the sequence of nullable
     * Decimal values that is obtained by invoking a projection
     * function on each element of the input sequence. */
    public BigDecimal sumNullableBigDecimal(
        final Queryable<T> source,
        final FunctionExpression<NullableBigDecimalFunction1<T>> selector)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.xxx(source, selector);
            }
        }.castSingle();
    }

    /** Computes the sum of the sequence of Double values
     * that is obtained by invoking a projection function on each
     * element of the input sequence. */
    public double sumDouble(
        final Queryable<T> source,
        final FunctionExpression<DoubleFunction1<T>> selector)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.xxx(source, selector);
            }
        }.castSingle();
    }

    /** Computes the sum of the sequence of nullable
     * Double values that is obtained by invoking a projection
     * function on each element of the input sequence. */
    public Double sumNullableDouble(
        final Queryable<T> source,
        final FunctionExpression<NullableDoubleFunction1<T>> selector)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.xxx(source, selector);
            }
        }.castSingle();
    }

    /** Computes the sum of the sequence of int values
     * that is obtained by invoking a projection function on each
     * element of the input sequence. */
    public int sumInteger(
        final Queryable<T> source, FunctionExpression<IntegerFunction1<T>>
        selector)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.xxx(source, selector);
            }
        }.castSingle();
    }

    /** Computes the sum of the sequence of nullable int
     * values that is obtained by invoking a projection function on
     * each element of the input sequence. */
    public Integer sumNullableInteger(
        final Queryable<T> source,
        final FunctionExpression<NullableIntegerFunction1<T>> selector)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.xxx(source, selector);
            }
        }.castSingle();
    }

    /** Computes the sum of the sequence of long values
     * that is obtained by invoking a projection function on each
     * element of the input sequence. */
    public long sumLong(
        final Queryable<T> source,
        final FunctionExpression<LongFunction1<T>> selector)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.xxx(source, selector);
            }
        }.castSingle();
    }

    /** Computes the sum of the sequence of nullable long
     * values that is obtained by invoking a projection function on
     * each element of the input sequence. */
    public Long sumNullableLong(
        final Queryable<T> source,
        final FunctionExpression<NullableLongFunction1<T>> selector)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.xxx(source, selector);
            }
        }.castSingle();
    }

    /** Computes the sum of the sequence of Float values
     * that is obtained by invoking a projection function on each
     * element of the input sequence. */
    public float sumFloat(
        final Queryable<T> source,
        final FunctionExpression<FloatFunction1<T>> selector)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.xxx(source, selector);
            }
        }.castSingle();
    }

    /** Computes the sum of the sequence of nullable
     * Float values that is obtained by invoking a projection
     * function on each element of the input sequence. */
    public Float sumNullableFloat(
        final Queryable<T> source,
        final FunctionExpression<NullableFloatFunction1<T>> selector)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.xxx(source, selector);
            }
        }.castSingle();
    }

    /** Returns a specified number of contiguous elements
     * from the start of a sequence. */
    public Queryable<T> take(
        final Queryable<T> source, int count)
    {
        return EnumerableDefaults.take(source.asEnumerable(), count)
            .asQueryable();
    }

    /** Returns elements from a sequence as long as a
     * specified condition is true. */
    public Queryable<T> takeWhile(
        final Queryable<T> source,
        final FunctionExpression<Predicate1<T>> predicate)
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
        final FunctionExpression<Function1<T, TKey>> keySelector)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.xxx(source, selector);
            }
        }.castSingle();
    }

    /** Performs a subsequent ordering of the elements in a sequence in
     * ascending order according to a key, using a specified comparator. */
    public <TKey> OrderedQueryable<T> thenBy(
        OrderedQueryable<T> source,
        final FunctionExpression<Function1<T, TKey>> keySelector,
        Comparator<TKey> comparator)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.xxx(source, selector);
            }
        }.castSingle();
    }

    /** Performs a subsequent ordering of the elements in a sequence in
     * descending order according to a key. */
    public <TKey extends Comparable<TKey>>
    OrderedQueryable<T> thenByDescending(
        OrderedQueryable<T> source,
        final FunctionExpression<Function1<T, TKey>> keySelector)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.xxx(source, selector);
            }
        }.castSingle();
    }

    /** Performs a subsequent ordering of the elements in a sequence in
     * dscending order according to a key, using a specified comparator. */
    public <TKey> OrderedQueryable<T> thenByDescending(
        OrderedQueryable<T> source,
        final FunctionExpression<Function1<T, TKey>> keySelector,
        Comparator<TKey> comparator)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.xxx(source, selector);
            }
        }.castSingle();
    }

    public Queryable<T> union(
        final Queryable<T> source0,
        final Enumerable<T> source1)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.xxx(source, selector);
            }
        }.castSingle();
    }

    /** Produces the set union of two sequences by using a
     * specified EqualityComparer<T>. */
    public Queryable<T> union(
        final Queryable<T> source0,
        Enumerable<T> source1,
        EqualityComparer<T> comparer)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.xxx(source, selector);
            }
        }.castSingle();
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
        final Queryable<T> source,
        final FunctionExpression<? extends Predicate2<T, Integer>> predicate)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.xxx(source, selector);
            }
        }.castSingle();
    }

    public <T1, TResult> Queryable<TResult> zip(
        final Queryable<T> source,
        Enumerable<T1> source1,
        final FunctionExpression<Function2<T, T1, TResult>> resultSelector)
    {
        return new NonLeafReplayableQueryable<T>(source) {
            public void replay(QueryableFactory<T> factory) {
                factory.xxx(source, selector);
            }
        }.castSingle();
    }
}

// End QueryableRecorder.java
