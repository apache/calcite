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
package net.hydromatic.linq4j.function;

/**
 * Utilities relating to functions.
 */
public class Functions {
    /**
     * A predicate with one parameter that always returns {@code true}.
     *
     * @param <T> First parameter type
     * @return Predicate that always returns true
     */
    public static <T> Predicate1<T> truePredicate1() {
        //noinspection unchecked
        return (Predicate1<T>) Predicate1.TRUE;
    }

    /**
     * A predicate with one parameter that always returns {@code true}.
     *
     * @param <T> First parameter type
     * @return Predicate that always returns true
     */
    public static <T> Predicate1<T> falsePredicate1() {
        //noinspection unchecked
        return (Predicate1<T>) Predicate1.FALSE;
    }

    /**
     * A predicate with two parameters that always returns {@code true}.
     *
     * @param <T1> First parameter type
     * @param <T2> Second parameter type
     * @return Predicate that always returns true
     */
    public static <T1, T2> Predicate2<T1, T2> truePredicate2() {
        //noinspection unchecked
        return (Predicate2<T1, T2>) Predicate2.TRUE;
    }

    /**
     * A predicate with two parameters that always returns {@code false}.
     *
     * @param <T1> First parameter type
     * @param <T2> Second parameter type
     * @return Predicate that always returns false
     */
    public static <T1, T2> Predicate2<T1, T2> falsePredicate2() {
        //noinspection unchecked
        return (Predicate2<T1, T2>) Predicate2.FALSE;
    }

    public static <TSource> Function1<TSource, TSource> identitySelector() {
        //noinspection unchecked
        return (Function1) Function1.IDENTITY;
    }

    /**
     * Creates a predicate that returns whether an object is an instance of a
     * particular type or is null.
     *
     * @param clazz Desired type
     * @param <T> Type of objects to test
     * @param <T2> Desired type
     * @return Predicate that tests for desired type
     */
    public static <T, T2> Predicate1<T> ofTypePredicate(
        final Class<T2> clazz)
    {
        return new Predicate1<T>() {
            public boolean apply(T v1) {
                return v1 == null || clazz.isInstance(v1);
            }
        };
    }

    public static <T1, T2> Predicate2<T1, T2> toPredicate2(
        final Predicate1<T1> p1)
    {
        return new Predicate2<T1, T2>() {
            public boolean apply(T1 v1, T2 v2) {
                return p1.apply(v1);
            }
        };
    }

    /**
     * Converts a 2-parameter function to a predicate.
     */
    public static <T1, T2> Predicate2<T1, T2> toPredicate(
        final Function2<T1, T2, Boolean> function)
    {
        return new Predicate2<T1, T2>() {
            public boolean apply(T1 v1, T2 v2) {
                return function.apply(v1, v2);
            }
        };
    }

    /**
     * Converts a 1-parameter function to a predicate.
     */
    private static <T> Predicate1<T> toPredicate(
        final Function1<T, Boolean> function)
    {
        return new Predicate1<T>() {
            public boolean apply(T v1) {
                return function.apply(v1);
            }
        };
    }
}

// End Functions.java
