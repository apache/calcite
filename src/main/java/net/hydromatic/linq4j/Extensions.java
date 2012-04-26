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
import java.util.*;

/**
 * Contains what, in LINQ.NET, would be extension methods.
 *
 * <h3>Notes on mapping from LINQ.NET to Java</h3>
 *
 * <p>We have preserved most of the API. But we've changed a few things, so that
 * the API is more typical Java API:</p>
 *
 * <ul>
 *
 * <li>Java method names start with a lower-case letter.</li>
 *
 * <li>A few methods became keywords when their first letter was converted
 *   to lower case; hence
 *   {@link net.hydromatic.linq4j.expressions.Expressions#break_}</li>
 *
 * <li>We created a Java interface {@link Enumerable}, similar to LINQ.NET's
 *         IEnumerable. IEnumerable is built into C#, and that gives it
 *         advantages: the standard collections implement it, and you can use
 *         any IEnumerable in a foreach loop. We made the Java
 *         {@code Enumerable} extend {@link Iterable},
 *         so that it can be used in for-each loops. But the standard
 *         collections still don't implement it. A few methods that take an
 *         IEnumerable in LINQ.NET take an Iterable in LINQ4J.</li>
 *
 * <li>LINQ.NET's Dictionary interface maps to Map in Java;
 *     hence, the LINQ.NET {@code ToDictionary} methods become
 *     {@code toMap}.</li>
 *
 * <li>LINQ.NET's decimal type changes to BigDecimal. (A little bit unnatural,
 * since decimal is primitive and BigDecimal is not.)</li>
 *
 * <li>There is no Nullable in Java. Therefore we distinguish between methods
 * that return, say, Long (which may be null) and long. See for example
 * {@link NullableLongFunction1} and {@link LongFunction1}, and the
 * variants of {@link Enumerable#sum} that call them.
 *
 * <li>Java erases type parameters from argument types before resolving
 * overloading. Therefore similar methods have the same erasure. Methods
 * {@link #averageDouble(Queryable, net.hydromatic.linq4j.expressions.FunctionExpression)},
 * {@link #averageInteger(Queryable, net.hydromatic.linq4j.expressions.FunctionExpression)},
 * {@link #groupByK(Queryable, net.hydromatic.linq4j.expressions.FunctionExpression, net.hydromatic.linq4j.expressions.FunctionExpression)},
 * {@link #groupByK(Queryable, net.hydromatic.linq4j.expressions.FunctionExpression, net.hydromatic.linq4j.expressions.FunctionExpression, net.hydromatic.linq4j.function.EqualityComparer)},
 * {@link #selectN(Queryable, net.hydromatic.linq4j.expressions.FunctionExpression)},
 * {@link #selectManyN(Queryable, net.hydromatic.linq4j.expressions.FunctionExpression)},
 * {@link #selectManyN(Queryable, net.hydromatic.linq4j.expressions.FunctionExpression, net.hydromatic.linq4j.expressions.FunctionExpression)},
 * {@link #skipWhileN(Queryable, net.hydromatic.linq4j.expressions.FunctionExpression)},
 * {@link #sumBigDecimal(Queryable, net.hydromatic.linq4j.expressions.FunctionExpression)},
 * {@link #sumNullableBigDecimal(Queryable, net.hydromatic.linq4j.expressions.FunctionExpression)},
 * {@link #whereN}
 * have been renamed from {@code average}, {@code groupBy}, {@code max},
 * {@code min}, {@code select}, {@code selectMany}, {@code skipWhile} and
 * {@code where} to prevent ambiguity.</li>
 *
 * <li>.NET allows <i>extension methods</i> &mdash; static methods that then
 * become, via compiler magic, a method of any object whose type is the
 * same as the first parameter of the extension method. In LINQ.NET, the
 * {@code IQueryable} and {@code IEnumerable} interfaces have many such methods.
 * In Java, those methods need to be explicitly added to the interface, and will
 * need to be implemented by every class that implements that interface.
 * We can help by implementing the methods as static methods, and by
 * providing an abstract base class that implements the extension methods
 * in the interface. Hence {@link AbstractEnumerable} and
 * {@link AbstractQueryable} call methods in {@link Extensions}.</li>
 * </ul>
 *
 * <li>.NET Func becomes {@link net.hydromatic.linq4j.function.Function0}, {@link net.hydromatic.linq4j.function.Function1}, {@link net.hydromatic.linq4j.function.Function2}, depending
 * on the number of arguments to the function, because Java types cannot be
 * overloaded based on the number of type parameters.</li>
 *
 * <li>Types map as follows:
 * {@code Int32} => {@code int} or {@link Integer},
 * {@code Int64} => {@code long} or {@link Long},
 * {@code bool} => {@code boolean} or {@link Boolean},
 * {@code Dictionary} => {@link Map},
 * {@code Lookup} => {@link Map} whose value type is an {@link Iterable},
 * </li>
 *
 * <li>Function types that accept primitive types in LINQ.NET have become
 * boxed types in LINQ4J. For example, a predicate function
 * {@code Func&lt;T, bool&gt;} becomes {@code Func1&lt;T, Boolean&gt;}.
 * It would be wrong to infer that the function is allowed to return null.</li>
 */
public class Extensions {

    // flags a piece of code we're yet to implement
    public static RuntimeException todo() {
        return new RuntimeException();
    }

    /** Applies an accumulator function over a
     * sequence. */
    public static <TSource> TSource aggregate(
        Enumerable<?> enumerable, Function2<TSource, TSource, TSource> func)
    {
        throw Extensions.todo();
    }

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
    public static <TSource, TAccumulate> TSource aggregate(
        Enumerable<?> enumerable,
        TAccumulate seed,
        Function2<TAccumulate, TSource, TAccumulate> func)
    {
        throw Extensions.todo();
    }

    /** Applies an accumulator function over a
     * sequence. The specified seed value is used as the initial
     * accumulator value. */
    public static <TSource, TAccumulate> TSource aggregate(
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
        Enumerable<TSource> enumerable,
        TAccumulate seed,
        Function2<TAccumulate, TSource, TAccumulate> func,
        Function1<TAccumulate, TResult> selector)
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

    /** Determines whether all elements of a sequence
     * satisfy a condition. */
    public static <TSource> boolean all(
        Enumerable<?> enumerable, Predicate1<TSource> predicate)
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
    public static boolean any(Enumerable enumerable) {
        return enumerable.enumerator().moveNext();
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
        Enumerable<TSource> enumerable, Predicate1<TSource> predicate)
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

    /** Returns the input typed as Enumerable<TSource>.
     *
     * <p>The AsEnumerable<TSource>(Enumerable<TSource>) method has no effect
     * other than to change the compile-time type of source from a type that
     * implements Enumerable<TSource> to Enumerable<TSource> itself.
     *
     * <p>AsEnumerable<TSource>(Enumerable<TSource>) can be used to choose
     * between query implementations when a sequence implements
     * Enumerable<TSource> but also has a different set of public query methods
     * available. For example, given a generic class Table that implements
     * Enumerable<TSource> and has its own methods such as Where, Select, and
     * SelectMany, a call to Where would invoke the public Where method of
     * Table. A Table type that represents a database table could have a Where
     * method that takes the predicate argument as an expression tree and
     * converts the tree to SQL for remote execution. If remote execution is not
     * desired, for example because the predicate invokes a local method, the
     * AsEnumerable<TSource> method can be used to hide the custom methods and
     * instead make the standard query operators available.
     */
    public static <TSource> Enumerable<TSource> asEnumerable(
        Enumerable<TSource> enumerable)
    {
        return enumerable;
    }

    /**
     * Converts an Enumerable to an IQueryable.
     *
     * <p>Analogous to the LINQ's Enumerable.AsQueryable extension method.</p>
     *
     * @param enumerable Enumerable
     * @param <TSource> Element type
     * @return A queryable
     */
    public static <TSource> Queryable<TSource> asQueryable(
        Enumerable<TSource> enumerable)
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
     * values that are obtained by invoking a transform function on
     * each element of the input sequence. */
    public static <TSource> BigDecimal average(
        Enumerable<TSource> enumerable,
        Function1<TSource, BigDecimal> selector)
    {
        throw Extensions.todo();
    }

    /** Computes the average of a sequence of nullable
     * Decimal values that are obtained by invoking a transform
     * function on each element of the input sequence. */
    public static <TSource> BigDecimal average(
        Enumerable<TSource> enumerable,
        NullableBigDecimalFunction1<TSource> selector)
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
     * values that are obtained by invoking a transform function on
     * each element of the input sequence. */
    public static <TSource> double average(
        Enumerable<TSource> enumerable, DoubleFunction1<TSource> selector)
    {
        throw Extensions.todo();
    }

    /** Computes the average of a sequence of nullable
     * Double values that are obtained by invoking a transform
     * function on each element of the input sequence. */
    public static <TSource> Double average(
        Enumerable<TSource> enumerable,
        NullableDoubleFunction1<TSource> selector)
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
     * that are obtained by invoking a transform function on each
     * element of the input sequence. */
    public static <TSource> int average(
        Enumerable<TSource> enumerable, IntegerFunction1<TSource> selector)
    {
        throw Extensions.todo();
    }

    /** Computes the average of a sequence of nullable
     * int values that are obtained by invoking a transform function
     * on each element of the input sequence. */
    public static <TSource> Integer average(
        Enumerable<TSource> enumerable,
        NullableIntegerFunction1<TSource> selector)
    {
        throw Extensions.todo();
    }

    /** Computes the average of a sequence of long values
     * that are obtained by invoking a transform function on each
     * element of the input sequence. */
    public static <TSource> long average(
        Enumerable<TSource> enumerable, LongFunction1<TSource> selector)
    {
        throw Extensions.todo();
    }

    /** Computes the average of a sequence of nullable
     * long values that are obtained by invoking a transform function
     * on each element of the input sequence. */
    public static <TSource> Long average(
        Enumerable<TSource> enumerable, NullableLongFunction1<TSource> selector)
    {
        throw Extensions.todo();
    }

    /** Computes the average of a sequence of Float
     * values that are obtained by invoking a transform function on
     * each element of the input sequence. */
    public static <TSource> float average(
        Enumerable<TSource> enumerable, FloatFunction1<TSource> selector)
    {
        throw Extensions.todo();
    }

    /** Computes the average of a sequence of nullable
     * Float values that are obtained by invoking a transform
     * function on each element of the input sequence. */
    public static <TSource> Float average(
        Enumerable<TSource> enumerable,
        NullableFloatFunction1<TSource> selector)
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
    public static <TSource, T2> Enumerable<T2> cast(
        final Enumerable<TSource> enumerable,
        final Class<T2> clazz)
    {
        return new AbstractEnumerable<T2>() {
            public Enumerator<T2> enumerator() {
                final Enumerator<TSource> enumerator = enumerable.enumerator();
                return new Enumerator<T2>() {
                    public T2 current() {
                        return clazz.cast(enumerator.current());
                    }

                    public boolean moveNext() {
                        return enumerator.moveNext();
                    }

                    public void reset() {
                        enumerator.reset();
                    }
                };
            }
        };
    }

    /** Concatenates two sequences. */
    public static <TSource> Enumerable<TSource> concat(
        Enumerable<TSource> enumerable0,
        Enumerable<TSource> enumerable1)
    {
        //noinspection unchecked
        return Linq4j.concat(
            Arrays.<Enumerable<TSource>>asList(enumerable0, enumerable1));
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
        Enumerable<TSource> enumerable,
        TSource element)
    {
        // Implementations of Enumerable backed by a Collection call
        // Collection.contains, which may be more efficient, not this method.
        for (TSource o : enumerable) {
            if (o.equals(element)) {
                return true;
            }
        }
        return false;
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
        Enumerable<TSource> enumerable,
        TSource element,
        EqualityComparer comparer)
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
    public static <TSource> int count(Enumerable<TSource> enumerable) {
        return (int) longCount(enumerable, Functions.<TSource>truePredicate1());
    }

    /** Returns the number of elements in a
     * sequence. */
    public static <TSource> int count(
        Queryable<TSource> queryable)
    {
        throw Extensions.todo();
    }

    /** Returns a number that represents how many elements
     * in the specified sequence satisfy a condition. */
    public static <TSource> int count(
        Enumerable<TSource> enumerable,
        Predicate1<TSource> predicate)
    {
        return (int) longCount(enumerable, predicate);
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
    public static <TSource> Enumerable<TSource> defaultIfEmpty(
        Enumerable<TSource> enumerable)
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
        Enumerable<TSource> enumerable, TSource value)
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
    public static <TSource> Enumerable<TSource> distinct(
        Enumerable<TSource> enumerable)
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
    public static <TSource> Enumerable<TSource> distinct(
        Enumerable<TSource> enumerable, EqualityComparer comparer)
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
        Enumerable<TSource> enumerable, int index)
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
        Enumerable<TSource> enumerable, int index)
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
     * by Enumerable.) */
    public static <TSource> Enumerable<TSource> except(
        Enumerable<TSource> enumerable0, Enumerable<TSource> enumerable1)
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
    public static <TSource> Enumerable<TSource> except(
        Enumerable<TSource> enumerable0,
        Enumerable<TSource> enumerable1,
        EqualityComparer comparer)
    {
        throw Extensions.todo();
    }

    /** Produces the set difference of two sequences by
     * using the specified EqualityComparer<TSource> to compare
     * values. */
    public static <TSource> Queryable<TSource> except(
        Queryable<TSource> queryable,
        Enumerable<TSource> enumerable,
        EqualityComparer comparer)
    {
        throw Extensions.todo();
    }

    /** Returns the first element of a sequence. (Defined
     * by Enumerable.) */
    public static <TSource> TSource first(
        Enumerable<TSource> enumerable)
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

    /** Returns the first element in a sequence that
     * satisfies a specified condition. */
    public static <TSource> TSource first(
        Enumerable<TSource> enumerable, Predicate1<TSource> predicate)
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
        Enumerable<TSource> enumerable)
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

    /** Returns the first element of the sequence that
     * satisfies a condition or a default value if no such element is
     * found. */
    public static <TSource> TSource firstOrDefault(
        Enumerable<TSource> enumerable, Predicate1<TSource> predicate)
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
    public static <TSource, TKey> Enumerable<Grouping<TKey, TSource>> groupBy(
        Enumerable<TSource> enumerable, Function1<TSource, TKey> keySelector)
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
    public static <TSource, TKey> Enumerable<Grouping<TKey, TSource>> groupBy(
        Enumerable<TSource> enumerable,
        Function1<TSource, TKey> keySelector,
        EqualityComparer comparer)
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
    Enumerable<Grouping<TKey, TElement>> groupBy(
        Enumerable<TSource> enumerable,
        Function1<TSource, TKey> keySelector,
        Function1<TSource, TElement> elementSelector)
    {
        throw Extensions.todo();
    }

    /** Groups the elements of a sequence according to a
     * specified key selector function and creates a result value from
     * each group and its key. */
    public static <TSource, TKey, TResult>
    Enumerable<Grouping<TKey, TResult>> groupBy(
        Enumerable<TSource> queryable,
        Function1<TSource, TKey> keySelector,
        Function2<TKey, Enumerable<TSource>, TResult> elementSelector)
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
     * {@link #groupBy(Queryable, net.hydromatic.linq4j.expressions.FunctionExpression, net.hydromatic.linq4j.expressions.FunctionExpression)},
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

    /** Groups the elements of a sequence according to a
     * key selector function. The keys are compared by using a
     * comparer and each group's elements are projected by using a
     * specified function. */
    public static <TSource, TKey, TElement>
    Enumerable<Grouping<TKey, TElement>> groupBy(
        Enumerable<TSource> enumerable,
        Function1<TSource, TKey> keySelector,
        Function1<TSource, TElement> elementSelector,
        EqualityComparer comparer)
    {
        throw Extensions.todo();
    }

    /** Groups the elements of a sequence according to a
     * specified key selector function and creates a result value from
     * each group and its key. The keys are compared by using a
     * specified comparer. */
    public static <TSource, TKey, TResult> Enumerable<TResult> groupBy(
        Enumerable<TSource> enumerable,
        Function1<TSource, TKey> keySelector,
        Function2<TKey, Enumerable<TSource>, TResult> elementSelector,
        EqualityComparer comparer)
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
     * {@link #groupBy(Queryable, net.hydromatic.linq4j.expressions.FunctionExpression, net.hydromatic.linq4j.expressions.FunctionExpression, EqualityComparer)},
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
    public static <TSource, TKey, TElement, TResult>
    Enumerable<TResult> groupBy(
        Enumerable<TSource> enumerable,
        Function1<TSource, TKey> keySelector,
        Function1<TSource, TElement> elementSelector,
        Function2<TKey, Enumerable<TElement>, TResult> resultSelector)
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
     * each group and its key. Key values are compared by using a
     * specified comparer, and the elements of each group are
     * projected by using a specified function. */
    public static <TSource, TKey, TElement, TResult>
    Enumerable<TResult> groupBy(
        Enumerable<TSource> enumerable,
        Function1<TSource, TKey> keySelector,
        Function1<TSource, TElement> elementSelector,
        Function2<TKey, Enumerable<TElement>, TResult> resultSelector,
        EqualityComparer<TKey> comparer)
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
     * equality of keys and groups the results. The default equality
     * comparer is used to compare keys. */
    public static <TSource, TInner, TKey, TResult>
    Enumerable<TResult> groupJoin(
        final Enumerable<TSource> outer,
        final Enumerable<TInner> inner,
        final Function1<TSource, TKey> outerKeySelector,
        final Function1<TInner, TKey> innerKeySelector,
        final Function2<TSource, Enumerable<TInner>, TResult> resultSelector)
    {
        return new AbstractEnumerable<TResult>() {
            final Map<TKey, TSource> outerMap = outer.toMap(outerKeySelector);
            final Lookup<TKey, TInner> innerLookup =
                inner.toLookup(innerKeySelector);
            final Enumerator<Map.Entry<TKey, TSource>> entries =
                Linq4j.enumerator(outerMap.entrySet());
            public Enumerator<TResult> enumerator() {
                return new Enumerator<TResult>() {
                    public TResult current() {
                        final Map.Entry<TKey, TSource> entry =
                            entries.current();
                        final Enumerable<TInner> inners =
                            innerLookup.get(entry.getKey());
                        return resultSelector.apply(
                            entry.getValue(),
                            inners == null
                                ? Linq4j.<TInner>emptyEnumerable()
                                : inners);
                    }

                    public boolean moveNext() {
                        return entries.moveNext();
                    }

                    public void reset() {
                        entries.reset();
                    }
                };
            }
        };
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
    public static <TSource, TInner, TKey, TResult>
    Enumerable<TResult> groupJoin(
        Enumerable<TSource> outer,
        Enumerable<TInner> inner,
        Function1<TSource, TKey> outerKeySelector,
        Function1<TInner, TKey> innerKeySelector,
        Function2<TSource, Enumerable<TInner>, TResult> resultSelector,
        EqualityComparer<TKey> comparer)
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
     * by Enumerable.) */
    public static <TSource> Enumerable<TSource> intersect(
        Enumerable<TSource> enumerable0, Enumerable<TSource> enumerable1)
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
    public static <TSource> Enumerable<TSource> intersect(
        Enumerable<TSource> enumerable0,
        Enumerable<TSource> enumerable1,
        EqualityComparer<TSource> comparer)
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
    public static <TSource, TInner, TKey, TResult> Enumerable<TResult> join(
        final Enumerable<TSource> outer,
        final Enumerable<TInner> inner,
        final Function1<TSource, TKey> outerKeySelector,
        final Function1<TInner, TKey> innerKeySelector,
        final Function2<TSource, TInner, TResult> resultSelector)
    {
        return new AbstractEnumerable<TResult>() {
            final Lookup<TKey, TSource> outerMap =
                outer.toLookup(outerKeySelector);
            final Lookup<TKey, TInner> innerLookup =
                inner.toLookup(innerKeySelector);
            final Enumerator<Map.Entry<TKey, Enumerable<TSource>>> entries =
                Linq4j.enumerator(outerMap.entrySet());
            public Enumerator<TResult> enumerator() {
                return new Enumerator<TResult>() {
                    Enumerator<List<Object>> productEnumerator =
                        Linq4j.emptyEnumerator();

                    public TResult current() {
                        return resultSelector.apply(
                            (TSource) productEnumerator.current().get(0),
                            (TInner) productEnumerator.current().get(1));
                    }

                    public boolean moveNext() {
                        for (;;) {
                            if (productEnumerator.moveNext()) {
                                return true;
                            }
                            if (!entries.moveNext()) {
                                return false;
                            }
                            final Map.Entry<TKey, Enumerable<TSource>> outer =
                                entries.current();
                            final Enumerable<TSource> outerEnumerable =
                                outer.getValue();
                            final Enumerable<TInner> innerEnumerable =
                                innerLookup.get(outer.getKey());
                            if (innerEnumerable == null
                                || !innerEnumerable.any()
                                || !outerEnumerable.any())
                            {
                                productEnumerator = Linq4j.emptyEnumerator();
                            } else {
                                productEnumerator =
                                    Linq4j.product(
                                        Arrays.asList(
                                            (Enumerator<Object>)
                                            (Enumerator)
                                                outerEnumerable.enumerator(),
                                            (Enumerator<Object>)
                                                (Enumerator)
                                                innerEnumerable.enumerator()));
                            }
                        }
                    }

                    public void reset() {
                        entries.reset();
                    }
                };
            }
        };
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
    public static <TSource, TInner, TKey, TResult> Enumerable<TResult> join(
        Enumerable<TSource> outer,
        Enumerable<TInner> inner,
        Function1<TSource, TKey> outerKeySelector,
        Function1<TInner, TKey> innerKeySelector,
        Function2<TSource, TInner, TResult> resultSelector,
        EqualityComparer<TKey> comparer)
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

    /** Returns the last element of a sequence. (Defined
     * by Enumerable.) */
    public static <TSource> TSource last(Enumerable<TSource> enumerable) {
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
        Enumerable<TSource> enumerable,
        Predicate1<TSource> predicate)
    {
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

    /** Returns the last element of a sequence, or a
     * default value if the sequence contains no elements. */
    public static <TSource> TSource lastOrDefault(
        Enumerable<TSource> enumerable)
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
        Enumerable<TSource> enumerable,
        Predicate1<TSource> predicate)
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
    public static <TSource> long longCount(Enumerable<TSource> source)  {
        return longCount(source, Functions.<TSource>truePredicate1());
    }

    /** Returns an long that represents the total number
     * of elements in a sequence. */
    public static <TSource> long longCount(Queryable<TSource> xable) {
        throw Extensions.todo();
    }

    /** Returns an long that represents how many elements
     * in a sequence satisfy a condition. */
    public static <TSource> long longCount(
        Enumerable<TSource> enumerable,
        Predicate1<TSource> predicate)
    {
        // Shortcut if this is a collection and the predicate is always true.
        if (predicate == Predicate1.TRUE
            && enumerable instanceof Collection)
        {
            return ((Collection) enumerable).size();
        }
        int n = 0;
        for (TSource o : enumerable) {
            if (predicate.apply(o)) {
                ++n;
            }
        }
        return n;
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
     * sequence. */
    public static <TSource> TSource max(Enumerable<TSource> enumerable) {
        throw Extensions.todo();
    }

    /** Returns the maximum value in a generic
     * IQueryable<TSource>. */
    public static <TSource> TSource max(Queryable<TSource> queryable) {
        throw Extensions.todo();
    }

    /** Invokes a transform function on each element of a
     * sequence and returns the maximum Decimal value. */
    public static <TSource> BigDecimal max(
        Enumerable<TSource> enumerable, BigDecimalFunction1<TSource> selector)
    {
        throw Extensions.todo();
    }

    /** Invokes a transform function on each element of a
     * sequence and returns the maximum nullable Decimal
     * value. */
    public static <TSource> BigDecimal max(
        Enumerable<TSource> enumerable,
        NullableBigDecimalFunction1<TSource> selector)
    {
        throw Extensions.todo();
    }

    /** Invokes a transform function on each element of a
     * sequence and returns the maximum Double value. */
    public static <TSource> double max(
        Enumerable<TSource> enumerable, DoubleFunction1<TSource> selector)
    {
        throw Extensions.todo();
    }

    /** Invokes a transform function on each element of a
     * sequence and returns the maximum nullable Double
     * value. */
    public static <TSource> Double max(
        Enumerable<TSource> enumerable,
        NullableDoubleFunction1<TSource> selector)
    {
        throw Extensions.todo();
    }

    /** Invokes a transform function on each element of a
     * sequence and returns the maximum int value. */
    public static <TSource> int max(
        Enumerable<TSource> enumerable, IntegerFunction1<TSource> selector)
    {
        throw Extensions.todo();
    }

    /** Invokes a transform function on each element of a
     * sequence and returns the maximum nullable int value. (Defined
     * by Enumerable.) */
    public static <TSource> Integer max(
        Enumerable<TSource> enumerable,
        NullableIntegerFunction1<TSource> selector)
    {
        throw Extensions.todo();
    }

    /** Invokes a transform function on each element of a
     * sequence and returns the maximum long value. */
    public static <TSource> long max(
        Enumerable<TSource> enumerable, LongFunction1<TSource> selector)
    {
        throw Extensions.todo();
    }

    /** Invokes a transform function on each element of a
     * sequence and returns the maximum nullable long value. (Defined
     * by Enumerable.) */
    public static <TSource> Long max(
        Enumerable<TSource> enumerable, NullableLongFunction1<TSource> selector)
    {
        throw Extensions.todo();
    }

    /** Invokes a transform function on each element of a
     * sequence and returns the maximum Float value. */
    public static <TSource> float max(
        Enumerable<TSource> enumerable, FloatFunction1<TSource> selector)
    {
        throw Extensions.todo();
    }

    /** Invokes a transform function on each element of a
     * sequence and returns the maximum nullable Float
     * value. */
    public static <TSource> Float max(
        Enumerable<TSource> enumerable,
        NullableFloatFunction1<TSource> selector)
    {
        throw Extensions.todo();
    }

    /** Invokes a transform function on each element of a
     * generic sequence and returns the maximum resulting
     * value. */
    public static <TSource, TResult> TResult max(
        Enumerable<TSource> enumerable, Function1<TSource, TResult> selector)
    {
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
     * sequence. */
    public static <TSource> TSource min(
        Enumerable<TSource> enumerable)
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

    /** Invokes a transform function on each element of a
     * sequence and returns the minimum Decimal value. */
    public static <TSource> BigDecimal min(
        Enumerable<TSource> enumerable, BigDecimalFunction1<TSource> selector)
    {
        throw Extensions.todo();
    }

    /** Invokes a transform function on each element of a
     * sequence and returns the minimum nullable Decimal
     * value. */
    public static <TSource> BigDecimal min(
        Enumerable<TSource> enumerable,
        NullableBigDecimalFunction1<TSource> selector)
    {
        throw Extensions.todo();
    }

    /** Invokes a transform function on each element of a
     * sequence and returns the minimum Double value. */
    public static <TSource> double min(
        Enumerable<TSource> enumerable, DoubleFunction1<TSource> selector)
    {
        throw Extensions.todo();
    }

    /** Invokes a transform function on each element of a
     * sequence and returns the minimum nullable Double
     * value. */
    public static <TSource> Double min(
        Enumerable<TSource> enumerable,
        NullableDoubleFunction1<TSource> selector)
    {
        throw Extensions.todo();
    }

    /** Invokes a transform function on each element of a
     * sequence and returns the minimum int value. */
    public static <TSource> int min(
        Enumerable<TSource> enumerable, IntegerFunction1<TSource> selector)
    {
        throw Extensions.todo();
    }

    /** Invokes a transform function on each element of a
     * sequence and returns the minimum nullable int value. (Defined
     * by Enumerable.) */
    public static <TSource> Integer min(
        Enumerable<TSource> enumerable,
        NullableIntegerFunction1<TSource> selector)
    {
        throw Extensions.todo();
    }

    /** Invokes a transform function on each element of a
     * sequence and returns the minimum long value. */
    public static <TSource> long min(
        Enumerable<TSource> enumerable, LongFunction1<TSource> selector)
    {
        throw Extensions.todo();
    }

    /** Invokes a transform function on each element of a
     * sequence and returns the minimum nullable long value. (Defined
     * by Enumerable.) */
    public static <TSource> Long min(
        Enumerable<TSource> enumerable, NullableLongFunction1<TSource> selector)
    {
        throw Extensions.todo();
    }

    /** Invokes a transform function on each element of a
     * sequence and returns the minimum Float value. */
    public static <TSource> float min(
        Enumerable<TSource> enumerable, FloatFunction1<TSource> selector)
    {
        throw Extensions.todo();
    }

    /** Invokes a transform function on each element of a
     * sequence and returns the minimum nullable Float
     * value. */
    public static <TSource> Float min(
        Enumerable<TSource> enumerable,
        NullableFloatFunction1<TSource> selector)
    {
        throw Extensions.todo();
    }

    /** Invokes a transform function on each element of a
     * generic sequence and returns the minimum resulting
     * value. */
    public static <TSource, TResult> TResult min(
        Enumerable<TSource> enumerable, Function1<TSource, TResult> selector)
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
     * <p>The OfType method generates a
     * {@link net.hydromatic.linq4j.expressions.MethodCallExpression} that represents
     * calling OfType itself as a constructed generic method. It then passes the
     * MethodCallExpression to the CreateQuery(Expression) method of the
     * {@link QueryProvider} represented by the Provider property of the source
     * parameter.
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

    /**
     * Filters the elements of an Enumerable based on a
     * specified type.
     *
     * <p>Analogous to LINQ's Enumerable.OfType extension method.</p>
     *
     * @param clazz Target type
     * @param <TResult> Target type
     * @return Collection of T2
     */
    public static <TSource, TResult> Enumerable<TResult> ofType(
        Enumerable<TSource> enumerable,
        Class<TResult> clazz)
    {
        //noinspection unchecked
        return (Enumerable) where(
            enumerable,
            Functions.<TSource, TResult>ofTypePredicate(clazz));
    }

    /** Sorts the elements of a sequence in ascending
     * order according to a key. */
    public static <TSource, TKey extends Comparable>
    Enumerable<TSource> orderBy(
        Enumerable<TSource> source, Function1<TSource, TKey> keySelector)
    {
        throw Extensions.todo();
    }

    /** Sorts the elements of a sequence in ascending
     * order according to a key. */
    public static <TSource, TKey extends Comparable> Queryable<TSource> orderBy(
        Queryable<TSource> source,
        FunctionExpression<Function1<TSource, TKey>> keySelector)
    {
        throw Extensions.todo();
    }

    /** Sorts the elements of a sequence in ascending
     * order by using a specified comparer. */
    public static <TSource, TKey> Enumerable<TSource> orderBy(
        Enumerable<TSource> source,
        Function1<TSource, TKey> keySelector,
        Comparator<TKey> comparator)
    {
        throw Extensions.todo();
    }

    /** Sorts the elements of a sequence in ascending
     * order by using a specified comparer. */
    public static <TSource, TKey> Queryable<TSource> orderBy(
        Queryable<TSource> source,
        FunctionExpression<Function1<TSource, TKey>> keySelector,
        Comparator<TKey> comparator)
    {
        throw Extensions.todo();
    }

    /** Sorts the elements of a sequence in descending
     * order according to a key. */
    public static <TSource, TKey extends Comparable>
    Enumerable<TSource> orderByDescending(
        Enumerable<TSource> source, Function1<TSource, TKey> keySelector)
    {
        throw Extensions.todo();
    }

    /** Sorts the elements of a sequence in descending
     * order according to a key. */
    public static <TSource, TKey extends Comparable>
    Queryable<TSource> orderByDescending(
        Queryable<TSource> source,
        FunctionExpression<Function1<TSource, TKey>> keySelector)
    {
        throw Extensions.todo();
    }

    /** Sorts the elements of a sequence in descending
     * order by using a specified comparer. */
    public static <TSource, TKey> Enumerable<TSource> orderByDescending(
        Enumerable<TSource> source,
        Function1<TSource, TKey> keySelector,
        Comparator<TKey> comparator)
    {
        throw Extensions.todo();
    }

    /** Sorts the elements of a sequence in descending
     * order by using a specified comparer. */
    public static <TSource, TKey> Queryable<TSource> orderByDescending(
        Queryable<TSource> source,
        FunctionExpression<Function1<TSource, TKey>> keySelector,
        Comparator<TKey> comparator)
    {
        throw Extensions.todo();
    }

    /** Inverts the order of the elements in a
     * sequence. */
    public static <TSource> Enumerable<TSource> reverse(
        Enumerable<TSource> source)
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

    /** Projects each element of a sequence into a new
     * form. */
    public static <TSource, TResult> Enumerable<TResult> select(
        final Enumerable<TSource> source,
        final Function1<TSource, TResult> selector)
    {
        return new AbstractEnumerable<TResult>() {
            public Enumerator<TResult> enumerator() {
                return new Enumerator<TResult>() {
                    final Enumerator<TSource> enumerator = source.enumerator();
                    public TResult current() {
                        return selector.apply(enumerator.current());
                    }

                    public boolean moveNext() {
                        return enumerator.moveNext();
                    }

                    public void reset() {
                        enumerator.reset();
                    }
                };
            }
        };
    }

    /** Projects each element of a sequence into a new
     * form by incorporating the element's index. */
    public static <TSource, TResult> Enumerable<TResult> select(
        final Enumerable<TSource> source,
        final Function2<TSource, Integer, TResult> selector)
    {
        return new AbstractEnumerable<TResult>() {
            public Enumerator<TResult> enumerator() {
                return new Enumerator<TResult>() {
                    final Enumerator<TSource> enumerator = source.enumerator();
                    int n = -1;

                    public TResult current() {
                        return selector.apply(enumerator.current(), n);
                    }

                    public boolean moveNext() {
                        if (enumerator.moveNext()) {
                            ++n;
                            return true;
                        } else {
                            return false;
                        }
                    }

                    public void reset() {
                        enumerator.reset();
                    }
                };
            }
        };
    }

    /** Projects each element of a sequence into a new form. */
    public static <TSource, TResult> Queryable<TResult> select(
        Queryable<TSource> source,
        FunctionExpression<Function1<TSource, TResult>> selector)
    {
        throw Extensions.todo();
    }

    /** Projects each element of a sequence into a new
     * form by incorporating the element's index.
     *
     * <p>NOTE: Renamed from {@code select} because had same erasure as
     * {@link #select(Queryable, net.hydromatic.linq4j.expressions.FunctionExpression)}.</p>
     */
    public static <TSource, TResult> Queryable<TResult> selectN(
        Queryable<TSource> source,
        FunctionExpression<Function2<TSource, Integer, TResult>> selector)
    {
        throw Extensions.todo();
    }

    /** Projects each element of a sequence to an
     * Enumerable<TSource> and flattens the resulting sequences into one
     * sequence. */
    public static <TSource, TResult> Enumerable<TResult> selectMany(
        final Enumerable<TSource> source,
        final Function1<TSource, Enumerable<TResult>> selector)
    {
        return new AbstractEnumerable<TResult>() {
            public Enumerator<TResult> enumerator() {
                return new Enumerator<TResult>() {
                    Enumerator<TSource> sourceEnumerator = source.enumerator();
                    Enumerator<TResult> resultEnumerator =
                        Linq4j.emptyEnumerator();

                    public TResult current() {
                        return resultEnumerator.current();
                    }

                    public boolean moveNext() {
                        for (;;) {
                            if (resultEnumerator.moveNext()) {
                                return true;
                            }
                            if (!sourceEnumerator.moveNext()) {
                                return false;
                            }
                            resultEnumerator =
                                selector.apply(sourceEnumerator.current())
                                    .enumerator();
                        }
                    }

                    public void reset() {
                        sourceEnumerator.reset();
                        resultEnumerator = Linq4j.emptyEnumerator();
                    }
                };
            }
        };
    }

    /** Projects each element of a sequence to an
     * Enumerable<TSource>, and flattens the resulting sequences into one
     * sequence. The index of each source element is used in the
     * projected form of that element. */
    public static <TSource, TResult> Enumerable<TResult> selectMany(
        Enumerable<TSource> source,
        Function2<TSource, Integer, Enumerable<TResult>> selector)
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
     * {@link #selectMany(Queryable, net.hydromatic.linq4j.expressions.FunctionExpression)}</p>
     */
    public static <TSource, TResult> Queryable<TResult> selectManyN(
        Queryable<TSource> source,
        FunctionExpression<Function2<TSource, Integer, Enumerable<TResult>>>
            selector)
    {
        throw Extensions.todo();
    }

    /** Projects each element of a sequence to an
     * Enumerable<TSource>, flattens the resulting sequences into one
     * sequence, and invokes a result selector function on each
     * element therein. The index of each source element is used in
     * the intermediate projected form of that element. */
    public static <TSource, TCollection, TResult>
    Enumerable<TResult> selectMany(
        Enumerable<TSource> source,
        Function2<TSource, Integer, Enumerable<TCollection>> collectionSelector,
        Function2<TSource, TCollection, TResult> resultSelector)
    {
        throw Extensions.todo();
    }

    /** Projects each element of a sequence to an
     * Enumerable<TSource>, flattens the resulting sequences into one
     * sequence, and invokes a result selector function on each
     * element therein. */
    public static <TSource, TCollection, TResult>
    Enumerable<TResult> selectMany(
        Enumerable<TSource> source,
        Function1<TSource, Enumerable<TCollection>> collectionSelector,
        Function2<TSource, TCollection, TResult> resultSelector)
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
     * {@link #selectMany(Queryable, net.hydromatic.linq4j.expressions.FunctionExpression, net.hydromatic.linq4j.expressions.FunctionExpression)}</p>
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
     * comparing the elements by using the default equality comparer
     * for their type. */
    public static <TSource> boolean sequenceEqual(
        Enumerable<TSource> enumerable0, Enumerable<TSource> enumerable1)
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
     * comparing their elements by using a specified
     * EqualityComparer<TSource>. */
    public static <TSource> boolean sequenceEqual(
        Enumerable<TSource> enumerable0,
        Enumerable<TSource> enumerable1,
        EqualityComparer<TSource> comparer)
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
        Enumerable<TSource> source)
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
        Enumerable<TSource> source, Predicate1<TSource> predicate)
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
        Enumerable<TSource> source)
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
        Enumerable<TSource> source, Predicate1<TSource> predicate)
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
    public static <TSource> Enumerable<TSource> skip(
        Enumerable<TSource> source, int count)
    {
        throw Extensions.todo();
    }

    /** Bypasses a specified number of elements in a
     * sequence and then returns the remaining elements. */
    public static <TSource> Queryable<TSource> skip(
        Queryable<TSource> source, int count)
    {
        throw Extensions.todo();
    }

    /** Bypasses elements in a sequence as long as a
     * specified condition is true and then returns the remaining
     * elements. */
    public static <TSource> Enumerable<TSource> skipWhile(
        Enumerable<TSource> source, Predicate1<TSource> predicate)
    {
        throw Extensions.todo();
    }

    /** Bypasses elements in a sequence as long as a
     * specified condition is true and then returns the remaining
     * elements. */
    public static <TSource> Queryable<TSource> skipWhile(
        Queryable<TSource> source,
        FunctionExpression<Predicate1<TSource>> predicate)
    {
        throw Extensions.todo();
    }

    /** Bypasses elements in a sequence as long as a
     * specified condition is true and then returns the remaining
     * elements. The element's index is used in the logic of the
     * predicate function. */
    public static <TSource> Enumerable<TSource> skipWhile(
        Enumerable<TSource> source,
        Function2<TSource, Integer, Boolean> predicate)
    {
        throw Extensions.todo();
    }

    /** Bypasses elements in a sequence as long as a
     * specified condition is true and then returns the remaining
     * elements. The element's index is used in the logic of the
     * predicate function. */
    public static <TSource> Queryable<TSource> skipWhileN(
        Queryable<TSource> source,
        FunctionExpression<Function2<TSource, Integer, Boolean>> predicate)
    {
        throw Extensions.todo();
    }

    /** Computes the sum of the sequence of Decimal values
     * that are obtained by invoking a transform function on each
     * element of the input sequence. */
    public static <TSource> BigDecimal sum(
        Enumerable<TSource> source, Function1<TSource, BigDecimal> selector)
    {
        throw Extensions.todo();
    }

    /** Computes the sum of the sequence of nullable
     * Decimal values that are obtained by invoking a transform
     * function on each element of the input sequence. */
    public static <TSource> BigDecimal sum(
        Enumerable<TSource> source,
        NullableBigDecimalFunction1<TSource> selector)
    {
        throw Extensions.todo();
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
     * that are obtained by invoking a transform function on each
     * element of the input sequence. */
    public static <TSource> double sum(
        Enumerable<TSource> source, DoubleFunction1<TSource> selector)
    {
        throw Extensions.todo();
    }

    /** Computes the sum of the sequence of nullable
     * Double values that are obtained by invoking a transform
     * function on each element of the input sequence. */
    public static <TSource> Double sum(
        Enumerable<TSource> source, NullableDoubleFunction1<TSource> selector)
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
     * that are obtained by invoking a transform function on each
     * element of the input sequence. */
    public static <TSource> int sum(
        Enumerable<TSource> source, IntegerFunction1<TSource> selector)
    {
        throw Extensions.todo();
    }

    /** Computes the sum of the sequence of nullable int
     * values that are obtained by invoking a transform function on
     * each element of the input sequence. */
    public static <TSource> Integer sum(
        Enumerable<TSource> sources, NullableIntegerFunction1<TSource> selector)
    {
        throw Extensions.todo();
    }

    /** Computes the sum of the sequence of long values
     * that are obtained by invoking a transform function on each
     * element of the input sequence. */
    public static <TSource> long sum(
        Enumerable<TSource> source, LongFunction1<TSource> selector)
    {
        throw Extensions.todo();
    }

    /** Computes the sum of the sequence of nullable long
     * values that are obtained by invoking a transform function on
     * each element of the input sequence. */
    public static <TSource> Long sum(
        Enumerable<TSource> source, NullableLongFunction1<TSource> selector)
    {
        throw Extensions.todo();
    }

    /** Computes the sum of the sequence of Float values
     * that are obtained by invoking a transform function on each
     * element of the input sequence. */
    public static <TSource> float sum(
        Enumerable<TSource> source, FloatFunction1<TSource> selector)
    {
        throw Extensions.todo();
    }

    /** Computes the sum of the sequence of nullable
     * Float values that are obtained by invoking a transform
     * function on each element of the input sequence. */
    public static <TSource> Float sum(
        Enumerable<TSource> source, NullableFloatFunction1<TSource> selector)
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
    public static <TSource> Enumerable<TSource> take(Enumerable<TSource> source, final int count) {
        return takeWhile(source, new Function2<TSource, Integer, Boolean>() {
            public Boolean apply(TSource v1, Integer v2) {
                // Count is 1-based
                return v2 < count;
            }
        });
    }

    /** Returns a specified number of contiguous elements
     * from the start of a sequence. */
    public static <TSource> Queryable<TSource> take(
        Queryable<TSource> source, int count)
    {
        return take(source.asEnumerable(), count).asQueryable();
    }

    /** Returns elements from a sequence as long as a
     * specified condition is true. */
    public static <TSource> Enumerable<TSource> takeWhile(
        Enumerable<TSource> source, final Predicate1<TSource> predicate)
    {
        return takeWhile(source, new Function2<TSource, Integer, Boolean>() {
            public Boolean apply(TSource v1, Integer v2) {
                return predicate.apply(v1);
            }
        });
    }

    /** Returns elements from a sequence as long as a
     * specified condition is true. */
    public static <TSource> Queryable<TSource> takeWhile(
        Queryable<TSource> source,
        FunctionExpression<Predicate1<TSource>> predicate)
    {
        final Predicate1<TSource> p1 = predicate.getFunction();
        Function2<TSource, Integer, Boolean> f2 = new Function2<TSource, Integer, Boolean>() {
            public Boolean apply(TSource v1, Integer v2) {
                return p1.apply(v1);
            }
        };
        return takeWhileN(source, new FunctionExpression<Function2<TSource, Integer, Boolean>>(f2));
    }

    /** Returns elements from a sequence as long as a
     * specified condition is true. The element's index is used in the
     * logic of the predicate function. */
    public static <TSource> Enumerable<TSource> takeWhile(
        Enumerable<TSource> source,
        Function2<TSource, Integer, Boolean> predicate)
    {
        return takeWhileN(source.asQueryable(), new FunctionExpression<Function2<TSource, Integer, Boolean>>(predicate));
    }

    /** Returns elements from a sequence as long as a
     * specified condition is true. The element's index is used in the
     * logic of the predicate function. */
    public static <TSource> Queryable<TSource> takeWhileN(
        final Queryable<TSource> source,
        FunctionExpression<Function2<TSource, Integer, Boolean>> predicate)
    {
        final Function2<TSource, Integer, Boolean> function = predicate.getFunction();
        return new AbstractQueryable<TSource>() {
            public Class<TSource> getElementType() {
                return source.getElementType();
            }

            public Expression getExpression() {
                return source.getExpression();
            }

            public QueryProvider getProvider() {
                return source.getProvider();
            }

            public Iterator<TSource> iterator() {
                return Linq4j.enumeratorIterator(enumerator());
            }

            public Enumerator<TSource> enumerator() {
                return new Enumerator<TSource>() {
                    Enumerator<TSource> enumerator = source.enumerator();
                    int i = -1;
                    public TSource current() {
                        return enumerator.current();
                    }

                    public boolean moveNext() {
                        while(enumerator.moveNext()) {
                            if (function.apply(enumerator.current(), ++i)) {
                                return true;
                            }
                        }
                        return false;
                    }

                    public void reset() {
                        enumerator.reset();
                        i = -1;
                    }
                };
            }
        };
    }

    /** Creates a Map&lt;TKey, TValue&gt; from an
     * Enumerable&lt;TSource&gt; according to a specified key selector
     * function.
     *
     * <p>NOTE: Called {@code toDictionary} in LINQ.NET.</p>
     * */
    public static <TSource, TKey> Map<TKey, TSource> toMap(
        Enumerable<TSource> source,
        Function1<TSource, TKey> keySelector)
    {
        return toMap(
            source, keySelector, Functions.<TSource>identitySelector());
    }

    /** Creates a Dictionary<TKey, TValue> from an
     * Enumerable<TSource> according to a specified key selector function
     * and key comparer. */
    public static <TSource, TKey> Map<TKey, TSource> toMap(
        Enumerable<TSource> source,
        Function1<TSource, TKey> keySelector,
        EqualityComparer<TKey> comparer)
    {
        throw Extensions.todo();
    }

    /** Creates a Dictionary<TKey, TValue> from an
     * Enumerable<TSource> according to specified key selector and element
     * selector functions. */
    public static <TSource, TKey, TElement> Map<TKey, TElement> toMap(
        Enumerable<TSource> source,
        Function1<TSource, TKey> keySelector,
        Function1<TSource, TElement> elementSelector)
    {
        // Use LinkedHashMap because groupJoin requires order of keys to be
        // preserved.
        final Map<TKey, TElement> map = new LinkedHashMap<TKey, TElement>();
        for (TSource o : source) {
            map.put(keySelector.apply(o), elementSelector.apply(o));
        }
        return map;
    }

    /** Creates a Dictionary<TKey, TValue> from an
     * Enumerable<TSource> according to a specified key selector function,
     * a comparer, and an element selector function. */
    public static <TSource, TKey, TElement> Map<TKey, TElement> toMap(
        Enumerable<TSource> source,
        Function1<TSource, TKey> keySelector,
        Function1<TSource, TElement> elementSelector,
        EqualityComparer<TKey> comparer)
    {
        throw Extensions.todo();
    }

    /** Creates a List<TSource> from an Enumerable<TSource>. */
    public static <TSource> List<TSource> toList(Enumerable<TSource> source) {
        final ArrayList<TSource> list = new ArrayList<TSource>();
        for (TSource element : source) {
            list.add(element);
        }
        return list;
    }

    /** Creates a Lookup&lt;TKey, TElement&gt; from an
     * Enumerable&lt;TSource&gt; according to a specified key selector
     * function. */
    public static <TSource, TKey> Lookup<TKey, TSource> toLookup(
        Enumerable<TSource> source,
        Function1<TSource, TKey> keySelector)
    {
        return toLookup(
            source, keySelector, Functions.<TSource>identitySelector());
    }

    /** Creates a Lookup<TKey, TElement> from an
     * Enumerable<TSource> according to a specified key selector function
     * and key comparer. */
    public static <TSource, TKey> Lookup<TKey, TSource> toLookup(
        Enumerable<TSource> source,
        Function1<TSource, TKey> keySelector,
        EqualityComparer<TKey> comparer)
    {
        throw Extensions.todo();
    }

    /** Creates a Lookup<TKey, TElement> from an
     * Enumerable<TSource> according to specified key selector and element
     * selector functions. */
    public static <TSource, TKey, TElement> Lookup<TKey, TElement> toLookup(
        Enumerable<TSource> source,
        Function1<TSource, TKey> keySelector,
        Function1<TSource, TElement> elementSelector)
    {
        final Map<TKey, List<TElement>> map =
            new HashMap<TKey, List<TElement>>();
        for (TSource o : source) {
            final TKey key = keySelector.apply(o);
            List<TElement> list = map.get(key);
            if (list == null) {
                list = new ArrayList<TElement>();
                map.put(key, list);
            }
            list.add(elementSelector.apply(o));
        }
        return new LookupImpl<TKey, TElement>(map);
    }

    /** Creates a Lookup<TKey, TElement> from an
     * Enumerable<TSource> according to a specified key selector function,
     * a comparer and an element selector function. */
    public static <TSource, TKey, TElement> Lookup<TKey, TElement> toLookup(
        Enumerable<TSource> source,
        Function1<TSource, TKey> keySelector,
        Function1<TSource, TElement> elementSelector,
        EqualityComparer<TKey> comparer)
    {
        throw Extensions.todo();
    }

    /** Produces the set union of two sequences by using
     * the default equality comparer. */
    public static <TSource> Enumerable<TSource> union(
        Enumerable<TSource> source0, Enumerable<TSource> source1)
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
    public static <TSource> Enumerable<TSource> union(
        Enumerable<TSource> source0,
        Enumerable<TSource> source1,
        EqualityComparer<TSource> comparer)
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
    public static <TSource> Enumerable<TSource> where(
        final Enumerable<TSource> source,
        final Predicate1<TSource> predicate)
    {
        assert predicate != null;
        return new AbstractEnumerable<TSource>() {
            public Enumerator<TSource> enumerator() {
                final Enumerator<TSource> enumerator = source.enumerator();
                return new Enumerator<TSource>() {
                    public TSource current() {
                        return enumerator.current();
                    }

                    public boolean moveNext() {
                        while (enumerator.moveNext()) {
                            if (predicate.apply(enumerator.current())) {
                                return true;
                            }
                        }
                        return false;
                    }

                    public void reset() {
                        enumerator.reset();
                    }
                };
            }
        };
    }

    /** Filters a sequence of values based on a
     * predicate. */
    public static <TSource> Queryable<TSource> where(
        Queryable<TSource> source,
        FunctionExpression<Predicate1<TSource>> predicate)
    {
        throw Extensions.todo();
    }

    /** Filters a sequence of values based on a
     * predicate. Each element's index is used in the logic of the
     * predicate function. */
    public static <TSource> Enumerable<TSource> where(
        final Enumerable<TSource> source,
        final Predicate2<TSource, Integer> predicate)
    {
        return new AbstractEnumerable<TSource>() {
            public Enumerator<TSource> enumerator() {
                return new Enumerator<TSource>() {
                    final Enumerator<TSource> enumerator = source.enumerator();
                    int n = -1;

                    public TSource current() {
                        return enumerator.current();
                    }

                    public boolean moveNext() {
                        while (enumerator.moveNext()) {
                            ++n;
                            if (predicate.apply(enumerator.current(), n)) {
                                return true;
                            }
                        }
                        return false;
                    }

                    public void reset() {
                        enumerator.reset();
                        n = -1;
                    }
                };
            }
        };
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

    /** Applies a specified function to the corresponding
     * elements of two sequences, producing a sequence of the
     * results. */
    public static <T0, T1, TResult> Enumerable<TResult> zip(
        Enumerable<T0> source0,
        Enumerable<T1> source1,
        Function2<T0, T1, TResult> resultSelector)
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

}

// End Extensions.java
