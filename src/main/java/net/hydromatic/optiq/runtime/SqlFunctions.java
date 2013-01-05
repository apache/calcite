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
package net.hydromatic.optiq.runtime;

import java.math.BigDecimal;

/**
 * Helper methods to implement SQL functions in generated code.
 *
 * @author jhyde
 */
@SuppressWarnings("UnnecessaryUnboxing")
public class SqlFunctions {
    /** SQL SUBSTRING(string FROM ... FOR ...) function. */
    public static String substring(String s, int from, int for_) {
        return s.substring(from - 1, Math.min(from - 1 + for_, s.length()));
    }

    /** SQL SUBSTRING(string FROM ... FOR ...) function; nullable arguments. */
    public static String substring(String s, Integer from, Integer for_) {
        if (s == null || from == null || for_ == null) {
            return null;
        }
        return substring(s, from.intValue(), for_.intValue());
    }

    /** SQL UPPER(string) function. */
    public static String upper(String s) {
        if (s == null) {
            return null;
        }
        return s.toUpperCase();
    }

    /** SQL LOWER(string) function. */
    public static String lower(String s) {
        if (s == null) {
            return null;
        }
        return s.toLowerCase();
    }

    // AND

    /** SQL AND operator. */
    public static boolean and(boolean b0, boolean b1) {
        return b0 && b1;
    }

    /** SQL AND operator; left side may be null. */
    public static Boolean and(Boolean b0, boolean b1) {
        return b0 == null ? null : (b0 && b1);
    }

    /** SQL AND operator; right side may be null. */
    public static Boolean and(boolean b0, Boolean b1) {
        return b1 == null ? null : (b0 && b1);
    }

    /** SQL AND operator; either side may be null. */
    public static Boolean and(Boolean b0, Boolean b1) {
        return (b0 == null || b1 == null) ? null : (b0 && b1);
    }

    // OR

    /** SQL OR operator. */
    public static boolean or(boolean b0, boolean b1) {
        return b0 || b1;
    }

    /** SQL OR operator; left side may be null. */
    public static Boolean or(Boolean b0, boolean b1) {
        return b0 == null ? null : (b0 || b1);
    }

    /** SQL OR operator; right side may be null. */
    public static Boolean or(boolean b0, Boolean b1) {
        return b1 == null ? null : (b0 || b1);
    }

    /** SQL OR operator; either side may be null. */
    public static Boolean or(Boolean b0, Boolean b1) {
        return (b0 == null || b1 == null) ? null : (b0 || b1);
    }

    // NOT

    /** SQL NOT operator. */
    public static boolean not(boolean b) {
        return !b;
    }

    /** SQL OR operator; operand may be null. */
    public static Boolean not(Boolean b) {
        return b == null ? null : !b;
    }

    // =

    /** SQL = operator applied to int values. */
    public static boolean eq(int b0, int b1) {
        return b0 == b1;
    }

    /** SQL == operator applied to int values; left side may be null. */
    public static Boolean eq(Integer b0, int b1) {
        return b0 == null ? null : (b0 == b1);
    }

    /** SQL = operator applied to int values; right side may be null. */
    public static Boolean eq(int b0, Integer b1) {
        return b1 == null ? null : (b0 == b1);
    }

    /** SQL = operator applied to int values; either side may be null. */
    public static Boolean eq(Integer b0, Integer b1) {
        return (b0 == null || b1 == null)
            ? null
            : (b0.intValue() == b1.intValue());
    }

    /** SQL = operator applied to short values. */
    public static boolean eq(short b0, short b1) {
        return b0 == b1;
    }

    /** SQL == operator applied to short values; left side may be null. */
    public static Boolean eq(Short b0, short b1) {
        return b0 == null ? null : (b0 == b1);
    }

    /** SQL = operator applied to short values; right side may be null. */
    public static Boolean eq(short b0, Short b1) {
        return b1 == null ? null : (b0 == b1);
    }

    /** SQL = operator applied to short values; either side may be null. */
    public static Boolean eq(Short b0, Short b1) {
        return (b0 == null || b1 == null)
            ? null
            : (b0.shortValue() == b1.shortValue());
    }

    /** SQL = operator applied to Object values (including String; either
     * side may be null). */
    public static Boolean eq(Object b0, Object b1) {
        return (b0 == null || b1 == null) ? null : b0.equals(b1);
    }

    // <>

    /** SQL &lt;&gt; operator applied to int values. */
    public static boolean ne(int b0, int b1) {
        return b0 != b1;
    }

    /** SQL &lt;&gt; operator applied to int values; left side may be null. */
    public static Boolean ne(Integer b0, int b1) {
        return b0 == null ? null : (b0 != b1);
    }

    /** SQL &lt;&gt; operator applied to int values (right side may be null). */
    public static Boolean ne(int b0, Integer b1) {
        return b1 == null ? null : (b0 != b1);
    }

    /** SQL &lt;&gt; operator applied to int values; either side may be null. */
    public static Boolean ne(Integer b0, Integer b1) {
        return (b0 == null || b1 == null)
            ? null
            : (b0.intValue() != b1.intValue());
    }

    /** SQL &lt;&gt; operator applied to Object values (including String; either
     * side may be null). */
    public static Boolean ne(Object b0, Object b1) {
        return (b0 == null || b1 == null) ? null : !b0.equals(b1);
    }

    // <

    /** SQL &lt; operator applied to int values. */
    public static boolean lt(int b0, int b1) {
        return b0 < b1;
    }

    /** SQL &lt; operator applied to int values; left side may be null. */
    public static Boolean lt(Integer b0, int b1) {
        return b0 == null ? null : (b0 < b1);
    }

    /** SQL &lt; operator applied to int values (right side may be null). */
    public static Boolean lt(int b0, Integer b1) {
        return b1 == null ? null : (b0 < b1);
    }

    /** SQL &lt; operator applied to nullable int values. */
    public static Boolean lt(Integer b0, Integer b1) {
        return (b0 == null || b1 == null) ? null : (b0 < b1);
    }

    /** SQL &lt; operator applied to nullable long and int values. */
    public static Boolean lt(Long b0, Integer b1) {
        return (b0 == null || b1 == null)
            ? null
            : (b0.longValue() < b1.longValue());
    }

    /** SQL &lt; operator applied to nullable int and long values. */
    public static Boolean lt(Integer b0, Long b1) {
        return (b0 == null || b1 == null)
            ? null
            : (b0.longValue() < b1.longValue());
    }

    /** SQL &lt; operator applied to String values. */
    public static Boolean lt(String b0, String b1) {
        return (b0 == null || b1 == null) ? null : (b0.compareTo(b1) < 0);
    }

    // <=

    /** SQL &le; operator applied to int values. */
    public static boolean le(int b0, int b1) {
        return b0 <= b1;
    }

    /** SQL &le; operator applied to int values; left side may be null. */
    public static Boolean le(Integer b0, int b1) {
        return b0 == null ? null : (b0 <= b1);
    }

    /** SQL &le; operator applied to int values (right side may be null). */
    public static Boolean le(int b0, Integer b1) {
        return b1 == null ? null : (b0 <= b1);
    }

    /** SQL &le; operator applied to nullable int values. */
    public static Boolean le(Integer b0, Integer b1) {
        return (b0 == null || b1 == null) ? null : (b0 <= b1);
    }

    /** SQL &le; operator applied to nullable long and int values. */
    public static Boolean le(Long b0, Integer b1) {
        return (b0 == null || b1 == null)
            ? null
            : (b0.longValue() <= b1.longValue());
    }

    /** SQL &le; operator applied to nullable int and long values. */
    public static Boolean le(Integer b0, Long b1) {
        return (b0 == null || b1 == null)
            ? null
            : (b0.longValue() <= b1.longValue());
    }

    /** SQL &le; operator applied to String values. */
    public static Boolean le(String b0, String b1) {
        return (b0 == null || b1 == null) ? null : (b0.compareTo(b1) <= 0);
    }

    // >

    /** SQL &gt; operator applied to long values. */
    public static boolean gt(long b0, long b1) {
        return b0 > b1;
    }

    /** SQL &gt; operator applied to int values. */
    public static boolean gt(int b0, int b1) {
        return b0 > b1;
    }

    /** SQL &gt; operator applied to int values; left side may be null. */
    public static Boolean gt(Integer b0, int b1) {
        return b0 == null ? null : (b0 > b1);
    }

    /** SQL &gt; operator applied to int values (right side may be null). */
    public static Boolean gt(int b0, Integer b1) {
        return b1 == null ? null : (b0 > b1);
    }

    /** SQL &gt; operator applied to nullable int values. */
    public static Boolean gt(Integer b0, Integer b1) {
        return (b0 == null || b1 == null) ? null : (b0 > b1);
    }

    /** SQL &gt; operator applied to nullable long and int values. */
    public static Boolean gt(Long b0, Integer b1) {
        return (b0 == null || b1 == null)
            ? null
            : (b0.longValue() > b1.longValue());
    }

    /** SQL &gt; operator applied to nullable int and long values. */
    public static Boolean gt(Integer b0, Long b1) {
        return (b0 == null || b1 == null)
            ? null
            : (b0.longValue() > b1.longValue());
    }

    /** SQL &gt; operator applied to String values. */
    public static Boolean gt(String b0, String b1) {
        return (b0 == null || b1 == null) ? null : (b0.compareTo(b1) > 0);
    }

    // >=

    /** SQL &ge; operator applied to int values. */
    public static boolean ge(int b0, int b1) {
        return b0 >= b1;
    }

    /** SQL &ge; operator applied to int values; left side may be null. */
    public static Boolean ge(Integer b0, int b1) {
        return b0 == null ? null : (b0 >= b1);
    }

    /** SQL &ge; operator applied to int values; right side may be null. */
    public static Boolean ge(int b0, Integer b1) {
        return b1 == null ? null : (b0 >= b1);
    }

    /** SQL &ge; operator applied to nullable int values. */
    public static Boolean ge(Integer b0, Integer b1) {
        return (b0 == null || b1 == null) ? null : (b0 >= b1);
    }

    /** SQL &ge; operator applied to nullable long and int values. */
    public static Boolean ge(Long b0, Integer b1) {
        return (b0 == null || b1 == null)
            ? null
            : (b0.longValue() >= b1.longValue());
    }

    /** SQL &ge; operator applied to nullable int and long values. */
    public static Boolean ge(Integer b0, Long b1) {
        return (b0 == null || b1 == null)
            ? null
            : (b0.longValue() >= b1.longValue());
    }

    /** SQL &ge; operator applied to String values. */
    public static Boolean ge(String b0, String b1) {
        return (b0 == null || b1 == null) ? null : (b0.compareTo(b1) >= 0);
    }

    // +

    /** SQL <code>+</code> operator applied to int values. */
    public static int plus(int b0, int b1) {
        return b0 + b1;
    }

    /** SQL <code>+</code> operator applied to int values; left side may be
     * null. */
    public static Integer plus(Integer b0, int b1) {
        return b0 == null ? null : (b0 + b1);
    }

    /** SQL <code>+</code> operator applied to int values; right side may be
     * null. */
    public static Integer plus(int b0, Integer b1) {
        return b1 == null ? null : (b0 + b1);
    }

    /** SQL <code>+</code> operator applied to nullable int values. */
    public static Integer plus(Integer b0, Integer b1) {
        return (b0 == null || b1 == null) ? null : (b0 + b1);
    }

    /** SQL <code>+</code> operator applied to nullable long and int values. */
    public static Long plus(Long b0, Integer b1) {
        return (b0 == null || b1 == null)
            ? null
            : (b0.longValue() + b1.longValue());
    }

    /** SQL <code>+</code> operator applied to nullable int and long values. */
    public static Long plus(Integer b0, Long b1) {
        return (b0 == null || b1 == null)
            ? null
            : (b0.longValue() + b1.longValue());
    }

    /** SQL <code>+</code> operator applied to BigDecimal values. */
    public static BigDecimal plus(BigDecimal b0, BigDecimal b1) {
        return (b0 == null || b1 == null) ? null : b0.add(b1);
    }

    // -

    /** SQL <code>-</code> operator applied to int values. */
    public static int minus(int b0, int b1) {
        return b0 - b1;
    }

    /** SQL <code>-</code> operator applied to int values; left side may be
     * null. */
    public static Integer minus(Integer b0, int b1) {
        return b0 == null ? null : (b0 - b1);
    }

    /** SQL <code>-</code> operator applied to int values; right side may be
     * null. */
    public static Integer minus(int b0, Integer b1) {
        return b1 == null ? null : (b0 - b1);
    }

    /** SQL <code>-</code> operator applied to nullable int values. */
    public static Integer minus(Integer b0, Integer b1) {
        return (b0 == null || b1 == null) ? null : (b0 - b1);
    }

    /** SQL <code>-</code> operator applied to nullable long and int values. */
    public static Long minus(Long b0, Integer b1) {
        return (b0 == null || b1 == null)
            ? null
            : (b0.longValue() - b1.longValue());
    }

    /** SQL <code>-</code> operator applied to nullable int and long values. */
    public static Long minus(Integer b0, Long b1) {
        return (b0 == null || b1 == null)
            ? null
            : (b0.longValue() - b1.longValue());
    }

    /** SQL <code>-</code> operator applied to BigDecimal values. */
    public static BigDecimal minus(BigDecimal b0, BigDecimal b1) {
        return (b0 == null || b1 == null) ? null : b0.subtract(b1);
    }

    // /

    /** SQL <code>/</code> operator applied to int values. */
    public static int divide(int b0, int b1) {
        return b0 / b1;
    }

    /** SQL <code>/</code> operator applied to int values; left side may be
     * null. */
    public static Integer divide(Integer b0, int b1) {
        return b0 == null ? null : (b0 / b1);
    }

    /** SQL <code>/</code> operator applied to int values; right side may be
     * null. */
    public static Integer divide(int b0, Integer b1) {
        return b1 == null ? null : (b0 / b1);
    }

    /** SQL <code>/</code> operator applied to nullable int values. */
    public static Integer divide(Integer b0, Integer b1) {
        return (b0 == null || b1 == null) ? null : (b0 / b1);
    }

    /** SQL <code>/</code> operator applied to nullable long and int values. */
    public static Long divide(Long b0, Integer b1) {
        return (b0 == null || b1 == null)
            ? null
            : (b0.longValue() / b1.longValue());
    }

    /** SQL <code>/</code> operator applied to nullable int and long values. */
    public static Long divide(Integer b0, Long b1) {
        return (b0 == null || b1 == null)
            ? null
            : (b0.longValue() / b1.longValue());
    }

    /** SQL <code>/</code> operator applied to BigDecimal values. */
    public static BigDecimal divide(BigDecimal b0, BigDecimal b1) {
        return (b0 == null || b1 == null) ? null : b0.divide(b1);
    }

    // *

    /** SQL <code>*</code> operator applied to int values. */
    public static int multiply(int b0, int b1) {
        return b0 * b1;
    }

    /** SQL <code>*</code> operator applied to int values; left side may be
     * null. */
    public static Integer multiply(Integer b0, int b1) {
        return b0 == null ? null : (b0 * b1);
    }

    /** SQL <code>*</code> operator applied to int values; right side may be
     * null. */
    public static Integer multiply(int b0, Integer b1) {
        return b1 == null ? null : (b0 * b1);
    }

    /** SQL <code>*</code> operator applied to nullable int values. */
    public static Integer multiply(Integer b0, Integer b1) {
        return (b0 == null || b1 == null) ? null : (b0 * b1);
    }

    /** SQL <code>*</code> operator applied to nullable long and int values. */
    public static Long multiply(Long b0, Integer b1) {
        return (b0 == null || b1 == null)
            ? null
            : (b0.longValue() * b1.longValue());
    }

    /** SQL <code>*</code> operator applied to nullable int and long values. */
    public static Long multiply(Integer b0, Long b1) {
        return (b0 == null || b1 == null)
            ? null
            : (b0.longValue() * b1.longValue());
    }

    /** SQL <code>*</code> operator applied to BigDecimal values. */
    public static BigDecimal multiply(BigDecimal b0, BigDecimal b1) {
        return (b0 == null || b1 == null) ? null : b0.multiply(b1);
    }

    // Helpers

    /** Helper for implementing MIN. Somewhat similar to LEAST operator. */
    public static <T extends Comparable<T>> T lesser(T b0, T b1) {
        return b0 == null || b0.compareTo(b1) > 0 ? b1 : b0;
    }

    public static int lesser(int b0, int b1) {
        return b0 > b1 ? b1 : b0;
    }

    /** Helper for implementing MAX. Somewhat similar to GREATEST operator. */
    public static <T extends Comparable<T>> T greater(T b0, T b1) {
        return b0 == null || b0.compareTo(b1) < 0 ? b1 : b0;
    }
}

// End SqlFunctions.java
