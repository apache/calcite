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

import java.math.BigDecimal;
import java.util.*;

/**
 * Utilities relating to functions.
 */
public class Functions {
    public static final Map<Class<? extends Function>, Class>
        FUNCTION_RESULT_TYPES =
        Collections.<Class<? extends Function>, Class>unmodifiableMap(
            map(
                Function1.class, Object.class,
                BigDecimalFunction1.class, BigDecimal.class,
                DoubleFunction1.class, Double.TYPE,
                FloatFunction1.class, Float.TYPE,
                IntegerFunction1.class, Integer.TYPE,
                LongFunction1.class, Long.TYPE,
                NullableBigDecimalFunction1.class, BigDecimal.class,
                NullableDoubleFunction1.class, Double.class,
                NullableFloatFunction1.class, Float.class,
                NullableIntegerFunction1.class, Integer.class,
                NullableLongFunction1.class, Long.class));

    private static final Map<Class, Class<? extends Function>>
        FUNCTION1_CLASSES =
        Collections.unmodifiableMap(
            new HashMap<Class, Class<? extends Function>>(
                inverse(FUNCTION_RESULT_TYPES)));

    private static final EqualityComparer<Object> IDENTITY_COMPARER =
        new IdentityEqualityComparer();

    private static final EqualityComparer<Object[]> ARRAY_COMPARER =
        new ArrayEqualityComparer();

    @SuppressWarnings("unchecked")
    private static <K, V> Map<K, V> map(K k, V v, Object... rest) {
        final Map<K, V> map = new HashMap<K, V>();
        map.put(k, v);
        for (int i = 0; i < rest.length; i++) {
            map.put((K) rest[i++], (V) rest[i++]);
        }
        return map;
    }

    private static <K, V> Map<V, K> inverse(Map<K, V> map) {
        HashMap<V, K> inverseMap = new HashMap<V, K>();
        for (Map.Entry<K, V> entry : map.entrySet()) {
            inverseMap.put(entry.getValue(), entry.getKey());
        }
        return inverseMap;
    }

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

    /**
     * Returns the appropriate interface for a lambda function with
     * 1 argument and the given return type.
     *
     * <p>For example:</p>
     * functionClass(Integer.TYPE) returns IntegerFunction1.class
     * functionClass(String.class) returns Function1.class
     *
     * @param aClass Return type
     * @return Function class
     */
    public static Class<? extends Function> functionClass(Class aClass) {
        Class<? extends Function> c = FUNCTION1_CLASSES.get(aClass);
        if (c != null) {
            return c;
        }
        return Function1.class;
    }

    /** Adapts an {@link IntegerFunction1} (that returns an {@code int}) to
     * an {@link Function1} returning an {@link Integer}. */
    public static <T1> Function1<T1, Integer> adapt(
        final IntegerFunction1<T1> f)
    {
        return new Function1<T1, Integer>() {
            public Integer apply(T1 a0) {
                return f.apply(a0);
            }
        };
    }

    /** Adapts a {@link DoubleFunction1} (that returns a {@code double}) to
     * an {@link Function1} returning a {@link Double}. */
    public static <T1> Function1<T1, Double> adapt(final DoubleFunction1<T1> f)
    {
        return new Function1<T1, Double>() {
            public Double apply(T1 a0) {
                return f.apply(a0);
            }
        };
    }

    /** Adapts a {@link LongFunction1} (that returns a {@code long}) to
     * an {@link Function1} returning a {@link Long}. */
    public static <T1> Function1<T1, Long> adapt(final LongFunction1<T1> f) {
        return new Function1<T1, Long>() {
            public Long apply(T1 a0) {
                return f.apply(a0);
            }
        };
    }

    /** Adapts a {@link FloatFunction1} (that returns a {@code float}) to
     * an {@link Function1} returning a {@link Float}. */
    public static <T1> Function1<T1, Float> adapt(final FloatFunction1<T1> f) {
        return new Function1<T1, Float>() {
            public Float apply(T1 a0) {
                return f.apply(a0);
            }
        };
    }

    /** Returns an {@link EqualityComparer} that uses object identity and hash
     * code. */
    @SuppressWarnings("unchecked")
    public static <T> EqualityComparer<T> identityComparer() {
        return (EqualityComparer) IDENTITY_COMPARER;
    }

    /** Returns an {@link EqualityComparer} that works on arrays of objects. */
    @SuppressWarnings("unchecked")
    public static <T> EqualityComparer<T[]> arrayComparer() {
        return (EqualityComparer) ARRAY_COMPARER;
    }

    private static class ArrayEqualityComparer
        implements EqualityComparer<Object[]>
    {
        public boolean equal(Object[] v1, Object[] v2) {
            return Arrays.equals(v1, v2);
        }

        public int hashCode(Object[] t) {
            return Arrays.hashCode(t);
        }
    }

    private static class IdentityEqualityComparer
        implements EqualityComparer<Object>
    {
        public boolean equal(Object v1, Object v2) {
            return Objects.equals(v1, v2);
        }

        public int hashCode(Object t) {
            return t == null ? 0x789d : t.hashCode();
        }
    }
}

// End Functions.java
