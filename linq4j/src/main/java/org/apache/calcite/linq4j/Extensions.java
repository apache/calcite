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

import org.apache.calcite.linq4j.function.Function2;

import java.math.BigDecimal;
import java.util.Comparator;
import java.util.Map;

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
 * to lower case; hence
 * {@link org.apache.calcite.linq4j.tree.Expressions#break_}</li>
 *
 * <li>We created a Java interface {@link Enumerable}, similar to LINQ.NET's
 * IEnumerable. IEnumerable is built into C#, and that gives it
 * advantages: the standard collections implement it, and you can use
 * any IEnumerable in a foreach loop. We made the Java
 * {@code Enumerable} extend {@link Iterable},
 * so that it can be used in for-each loops. But the standard
 * collections still don't implement it. A few methods that take an
 * IEnumerable in LINQ.NET take an Iterable in LINQ4J.</li>
 *
 * <li>LINQ.NET's Dictionary interface maps to Map in Java;
 * hence, the LINQ.NET {@code ToDictionary} methods become
 * {@code toMap}.</li>
 *
 * <li>LINQ.NET's decimal type changes to BigDecimal. (A little bit unnatural,
 * since decimal is primitive and BigDecimal is not.)</li>
 *
 * <li>There is no Nullable in Java. Therefore we distinguish between methods
 * that return, say, Long (which may be null) and long. See for example
 * {@link org.apache.calcite.linq4j.function.NullableLongFunction1} and
 * {@link org.apache.calcite.linq4j.function.LongFunction1}, and the
 * variants of {@link Enumerable#sum} that call them.
 *
 * <li>Java erases type parameters from argument types before resolving
 * overloading. Therefore similar methods have the same erasure. Methods
 * {@link ExtendedQueryable#averageDouble averageDouble},
 * {@link ExtendedQueryable#averageInteger averageInteger},
 * {@link ExtendedQueryable#groupByK groupByK},
 * {@link ExtendedQueryable#selectN selectN},
 * {@link ExtendedQueryable#selectManyN selectManyN},
 * {@link ExtendedQueryable#skipWhileN skipWhileN},
 * {@link ExtendedQueryable#sumBigDecimal sumBigDecimal},
 * {@link ExtendedQueryable#sumNullableBigDecimal sumNullableBigDecimal},
 * {@link ExtendedQueryable#whereN whereN}
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
 *
 * <li>.NET Func becomes {@link org.apache.calcite.linq4j.function.Function0},
 * {@link org.apache.calcite.linq4j.function.Function1},
 * {@link org.apache.calcite.linq4j.function.Function2}, depending
 * on the number of arguments to the function, because Java types cannot be
 * overloaded based on the number of type parameters.</li>
 *
 * <li>Types map as follows:
 * {@code Int32} &rarr; {@code int} or {@link Integer},
 * {@code Int64} &rarr; {@code long} or {@link Long},
 * {@code bool} &rarr; {@code boolean} or {@link Boolean},
 * {@code Dictionary} &rarr; {@link Map},
 * {@code Lookup} &rarr; {@link Map} whose value type is an {@link Iterable},
 * </li>
 *
 * <li>Function types that accept primitive types in LINQ.NET have become
 * boxed types in LINQ4J. For example, a predicate function
 * {@code Func&lt;T, bool&gt;} becomes {@code Func1&lt;T, Boolean&gt;}.
 * It would be wrong to infer that the function is allowed to return null.</li>
 *
 * </ul>
 */
public abstract class Extensions {
  private Extensions() {}

  static final Function2<BigDecimal, BigDecimal, BigDecimal> BIG_DECIMAL_SUM =
      BigDecimal::add;

  static final Function2<Float, Float, Float> FLOAT_SUM =
      (v1, v2) -> v1 + v2;

  static final Function2<Double, Double, Double> DOUBLE_SUM =
      (v1, v2) -> v1 + v2;

  static final Function2<Integer, Integer, Integer> INTEGER_SUM =
      (v1, v2) -> v1 + v2;

  static final Function2<Long, Long, Long> LONG_SUM =
      (v1, v2) -> v1 + v2;

  @SuppressWarnings("unchecked")
  static final Function2<Comparable, Comparable, Comparable> COMPARABLE_MIN =
      (v1, v2) -> v1 == null || v1.compareTo(v2) > 0 ? v2 : v1;

  @SuppressWarnings("unchecked")
  static final Function2<Comparable, Comparable, Comparable> COMPARABLE_MAX =
      (v1, v2) -> v1 == null || v1.compareTo(v2) < 0 ? v2 : v1;

  static final Function2<Float, Float, Float> FLOAT_MIN =
      (v1, v2) -> v1 == null || v1.compareTo(v2) > 0 ? v2 : v1;

  static final Function2<Float, Float, Float> FLOAT_MAX =
      (v1, v2) -> v1 == null || v1.compareTo(v2) < 0 ? v2 : v1;

  static final Function2<Double, Double, Double> DOUBLE_MIN =
      (v1, v2) -> v1 == null || v1.compareTo(v2) > 0 ? v2 : v1;

  static final Function2<Double, Double, Double> DOUBLE_MAX =
      (v1, v2) -> v1 == null || v1.compareTo(v2) < 0 ? v2 : v1;

  static final Function2<Integer, Integer, Integer> INTEGER_MIN =
      (v1, v2) -> v1 == null || v1.compareTo(v2) > 0 ? v2 : v1;

  static final Function2<Integer, Integer, Integer> INTEGER_MAX =
      (v1, v2) -> v1 == null || v1.compareTo(v2) < 0 ? v2 : v1;

  static final Function2<Long, Long, Long> LONG_MIN =
      (v1, v2) -> v1 == null || v1.compareTo(v2) > 0 ? v2 : v1;

  static final Function2<Long, Long, Long> LONG_MAX =
      (v1, v2) -> v1 == null || v1.compareTo(v2) < 0 ? v2 : v1;

  // flags a piece of code we're yet to implement
  public static RuntimeException todo() {
    return new RuntimeException();
  }

  public static <T> Queryable<T> asQueryable(DefaultEnumerable<T> source) {
    //noinspection unchecked
    return source instanceof Queryable
        ? ((Queryable<T>) source)
        : new EnumerableQueryable<T>(
            Linq4j.DEFAULT_PROVIDER, (Class) Object.class, null, source);
  }

  static <T extends Comparable<T>> Comparator<T> comparableComparator() {
    return Comparable::compareTo;
  }
}

// End Extensions.java
