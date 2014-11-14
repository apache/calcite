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
package net.hydromatic.optiq.runtime;

import net.hydromatic.avatica.ByteString;

import net.hydromatic.linq4j.Enumerable;
import net.hydromatic.linq4j.Linq4j;
import net.hydromatic.linq4j.expressions.Primitive;
import net.hydromatic.linq4j.function.Deterministic;
import net.hydromatic.linq4j.function.Function1;
import net.hydromatic.linq4j.function.NonDeterministic;

import net.hydromatic.optiq.DataContext;

import org.eigenbase.util14.DateTimeUtil;

import java.math.*;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Helper methods to implement SQL functions in generated code.
 *
 * <p>Not present: and, or, not (builtin operators are better, because they
 * use lazy evaluation. Implementations do not check for null values; the
 * calling code must do that.</p>
 *
 * <p>Many of the functions do not check for null values. This is intentional.
 * If null arguments are possible, the code-generation framework checks for
 * nulls before calling the functions.</p>
 */
@SuppressWarnings("UnnecessaryUnboxing")
@Deterministic
public class SqlFunctions {
  private static final DecimalFormat DOUBLE_FORMAT =
      new DecimalFormat("0.0E0");

  /** The julian date of the epoch, 1970-01-01. */
  public static final int EPOCH_JULIAN = 2440588;

  private static final TimeZone LOCAL_TZ = TimeZone.getDefault();

  private static final Function1<List<Object>, Enumerable<Object>>
  LIST_AS_ENUMERABLE =
      new Function1<List<Object>, Enumerable<Object>>() {
        public Enumerable<Object> apply(List<Object> list) {
          return Linq4j.asEnumerable(list);
        }
      };

  private SqlFunctions() {
  }

  /** SQL SUBSTRING(string FROM ... FOR ...) function. */
  public static String substring(String s, int from, int for_) {
    return s.substring(from - 1, Math.min(from - 1 + for_, s.length()));
  }

  /** SQL SUBSTRING(string FROM ...) function. */
  public static String substring(String s, int from) {
    return s.substring(from - 1);
  }

  /** SQL UPPER(string) function. */
  public static String upper(String s) {
    return s.toUpperCase();
  }

  /** SQL LOWER(string) function. */
  public static String lower(String s) {
    return s.toLowerCase();
  }

  /** SQL INITCAP(string) function. */
  public static String initcap(String s) {
    // Assumes Alpha as [A-Za-z0-9]
    // white space is treated as everything else.
    final int len = s.length();
    boolean start = true;
    final StringBuilder newS = new StringBuilder();

    for (int i = 0; i < len; i++) {
      char curCh = s.charAt(i);
      final int c = (int) curCh;
      if (start) {  // curCh is whitespace or first character of word.
        if (c > 47 && c < 58) { // 0-9
          start = false;
        } else if (c > 64 && c < 91) {  // A-Z
          start = false;
        } else if (c > 96 && c < 123) {  // a-z
          start = false;
          curCh = (char) (c - 32); // Uppercase this character
        }
        // else {} whitespace
      } else {  // Inside of a word or white space after end of word.
        if (c > 47 && c < 58) { // 0-9
          // noop
        } else if (c > 64 && c < 91) {  // A-Z
          curCh = (char) (c + 32); // Lowercase this character
        } else if (c > 96 && c < 123) {  // a-z
          // noop
        } else { // whitespace
          start = true;
        }
      }
      newS.append(curCh);
    } // for each character in s
    return newS.toString();
  }

  /** SQL CHARACTER_LENGTH(string) function. */
  public static int charLength(String s) {
    return s.length();
  }

  /** SQL {@code string || string} operator. */
  public static String concat(String s0, String s1) {
    return s0 + s1;
  }

  /** SQL {@code binary || binary} operator. */
  public static ByteString concat(ByteString s0, ByteString s1) {
    return s0.concat(s1);
  }

  /** SQL {@code RTRIM} function applied to string. */
  public static String rtrim(String s) {
    return trim_(s, false, true, ' ');
  }

  /** SQL {@code LTRIM} function. */
  public static String ltrim(String s) {
    return trim_(s, true, false, ' ');
  }

  /** SQL {@code TRIM(... seek FROM s)} function. */
  public static String trim(boolean leading, boolean trailing, String seek,
      String s) {
    return trim_(s, leading, trailing, seek.charAt(0));
  }

  /** SQL {@code TRIM} function. */
  private static String trim_(String s, boolean left, boolean right, char c) {
    int j = s.length();
    if (right) {
      for (;;) {
        if (j == 0) {
          return "";
        }
        if (s.charAt(j - 1) != c) {
          break;
        }
        --j;
      }
    }
    int i = 0;
    if (left) {
      for (;;) {
        if (i == j) {
          return "";
        }
        if (s.charAt(i) != c) {
          break;
        }
        ++i;
      }
    }
    return s.substring(i, j);
  }

  /** SQL {@code TRIM} function applied to binary string. */
  public static ByteString trim(ByteString s) {
    return trim_(s, true, true);
  }

  /** Helper for CAST. */
  public static ByteString rtrim(ByteString s) {
    return trim_(s, false, true);
  }

  /** SQL {@code TRIM} function applied to binary string. */
  private static ByteString trim_(ByteString s, boolean left, boolean right) {
    int j = s.length();
    if (right) {
      for (;;) {
        if (j == 0) {
          return ByteString.EMPTY;
        }
        if (s.byteAt(j - 1) != 0) {
          break;
        }
        --j;
      }
    }
    int i = 0;
    if (left) {
      for (;;) {
        if (i == j) {
          return ByteString.EMPTY;
        }
        if (s.byteAt(i) != 0) {
          break;
        }
        ++i;
      }
    }
    return s.substring(i, j);
  }

  /** SQL {@code OVERLAY} function. */
  public static String overlay(String s, String r, int start) {
    if (s == null || r == null) {
      return null;
    }
    return s.substring(0, start - 1)
        + r
        + s.substring(start - 1 + r.length());
  }

  /** SQL {@code OVERLAY} function. */
  public static String overlay(String s, String r, int start, int length) {
    if (s == null || r == null) {
      return null;
    }
    return s.substring(0, start - 1)
        + r
        + s.substring(start - 1 + length);
  }

  /** SQL {@code OVERLAY} function applied to binary strings. */
  public static ByteString overlay(ByteString s, ByteString r, int start) {
    if (s == null || r == null) {
      return null;
    }
    return s.substring(0, start - 1)
           .concat(r)
           .concat(s.substring(start - 1 + r.length()));
  }

  /** SQL {@code OVERLAY} function applied to binary strings. */
  public static ByteString overlay(ByteString s, ByteString r, int start,
      int length) {
    if (s == null || r == null) {
      return null;
    }
    return s.substring(0, start - 1)
           .concat(r)
           .concat(s.substring(start - 1 + length));
  }

  /** SQL {@code LIKE} function. */
  public static boolean like(String s, String pattern) {
    final String regex = Like.sqlToRegexLike(pattern, null);
    return Pattern.matches(regex, s);
  }

  /** SQL {@code LIKE} function with escape. */
  public static boolean like(String s, String pattern, String escape) {
    final String regex = Like.sqlToRegexLike(pattern, escape);
    return Pattern.matches(regex, s);
  }

  /** SQL {@code SIMILAR} function. */
  public static boolean similar(String s, String pattern) {
    final String regex = Like.sqlToRegexSimilar(pattern, null);
    return Pattern.matches(regex, s);
  }

  /** SQL {@code SIMILAR} function with escape. */
  public static boolean similar(String s, String pattern, String escape) {
    final String regex = Like.sqlToRegexSimilar(pattern, escape);
    return Pattern.matches(regex, s);
  }

  // =

  /** SQL = operator applied to Object values (including String; neither
   * side may be null). */
  public static boolean eq(Object b0, Object b1) {
    return b0.equals(b1);
  }

  /** SQL = operator applied to BigDecimal values (neither may be null). */
  public static boolean eq(BigDecimal b0, BigDecimal b1) {
    return b0.stripTrailingZeros().equals(b1.stripTrailingZeros());
  }

  // <>

  /** SQL &lt;&gt; operator applied to Object values (including String;
   * neither side may be null). */
  public static boolean ne(Object b0, Object b1) {
    return !b0.equals(b1);
  }

  /** SQL &lt;&gt; operator applied to BigDecimal values. */
  public static boolean ne(BigDecimal b0, BigDecimal b1) {
    return b0.compareTo(b1) != 0;
  }

  // <

  /** SQL &lt; operator applied to boolean values. */
  public static boolean lt(boolean b0, boolean b1) {
    return compare(b0, b1) < 0;
  }

  /** SQL &lt; operator applied to String values. */
  public static boolean lt(String b0, String b1) {
    return b0.compareTo(b1) < 0;
  }

  /** SQL &lt; operator applied to ByteString values. */
  public static boolean lt(ByteString b0, ByteString b1) {
    return b0.compareTo(b1) < 0;
  }

  /** SQL &lt; operator applied to BigDecimal values. */
  public static boolean lt(BigDecimal b0, BigDecimal b1) {
    return b0.compareTo(b1) < 0;
  }

  // <=

  /** SQL &le; operator applied to boolean values. */
  public static boolean le(boolean b0, boolean b1) {
    return compare(b0, b1) <= 0;
  }

  /** SQL &le; operator applied to String values. */
  public static boolean le(String b0, String b1) {
    return b0.compareTo(b1) <= 0;
  }

  /** SQL &le; operator applied to ByteString values. */
  public static boolean le(ByteString b0, ByteString b1) {
    return b0.compareTo(b1) <= 0;
  }

  /** SQL &le; operator applied to BigDecimal values. */
  public static boolean le(BigDecimal b0, BigDecimal b1) {
    return b0.compareTo(b1) <= 0;
  }

  // >

  /** SQL &gt; operator applied to boolean values. */
  public static boolean gt(boolean b0, boolean b1) {
    return compare(b0, b1) > 0;
  }

  /** SQL &gt; operator applied to String values. */
  public static boolean gt(String b0, String b1) {
    return b0.compareTo(b1) > 0;
  }

  /** SQL &gt; operator applied to ByteString values. */
  public static boolean gt(ByteString b0, ByteString b1) {
    return b0.compareTo(b1) > 0;
  }

  /** SQL &gt; operator applied to BigDecimal values. */
  public static boolean gt(BigDecimal b0, BigDecimal b1) {
    return b0.compareTo(b1) > 0;
  }

  // >=

  /** SQL &ge; operator applied to boolean values. */
  public static boolean ge(boolean b0, boolean b1) {
    return compare(b0, b1) >= 0;
  }

  /** SQL &ge; operator applied to String values. */
  public static boolean ge(String b0, String b1) {
    return b0.compareTo(b1) >= 0;
  }

  /** SQL &ge; operator applied to ByteString values. */
  public static boolean ge(ByteString b0, ByteString b1) {
    return b0.compareTo(b1) >= 0;
  }

  /** SQL &ge; operator applied to BigDecimal values. */
  public static boolean ge(BigDecimal b0, BigDecimal b1) {
    return b0.compareTo(b1) >= 0;
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

  // EXP

  /** SQL <code>EXP</code> operator applied to double values. */
  public static double exp(double b0) {
    return Math.exp(b0);
  }

  public static double exp(long b0) {
    return Math.exp(b0);
  }

  // POWER

  /** SQL <code>POWER</code> operator applied to double values. */
  public static double power(double b0, double b1) {
    return Math.pow(b0, b1);
  }

  public static double power(long b0, long b1) {
    return Math.pow(b0, b1);
  }

  public static double power(long b0, BigDecimal b1) {
    return Math.pow(b0, b1.doubleValue());
  }

  // LN

  /** SQL {@code LN(number)} function applied to double values. */
  public static double ln(double d) {
    return Math.log(d);
  }

  /** SQL {@code LN(number)} function applied to long values. */
  public static double ln(long b0) {
    return Math.log(b0);
  }

  /** SQL {@code LN(number)} function applied to BigDecimal values. */
  public static double ln(BigDecimal d) {
    return Math.log(d.doubleValue());
  }

  // LOG10

  /** SQL <code>LOG10(numeric)</code> operator applied to double values. */
  public static double log10(double b0) {
    return Math.log10(b0);
  }

  /** SQL {@code LOG10(number)} function applied to long values. */
  public static double log10(long b0) {
    return Math.log10(b0);
  }

  /** SQL {@code LOG10(number)} function applied to BigDecimal values. */
  public static double log10(BigDecimal d) {
    return Math.log10(d.doubleValue());
  }

  // MOD

  /** SQL <code>MOD</code> operator applied to byte values. */
  public static byte mod(byte b0, byte b1) {
    return (byte) (b0 % b1);
  }

  /** SQL <code>MOD</code> operator applied to short values. */
  public static short mod(short b0, short b1) {
    return (short) (b0 % b1);
  }

  /** SQL <code>MOD</code> operator applied to int values. */
  public static int mod(int b0, int b1) {
    return b0 % b1;
  }

  /** SQL <code>MOD</code> operator applied to long values. */
  public static long mod(long b0, long b1) {
    return b0 % b1;
  }

  // temporary
  public static BigDecimal mod(BigDecimal b0, int b1) {
    return mod(b0, BigDecimal.valueOf(b1));
  }

  // temporary
  public static int mod(int b0, BigDecimal b1) {
    return mod(b0, b1.intValue());
  }

  public static BigDecimal mod(BigDecimal b0, BigDecimal b1) {
    final BigDecimal[] bigDecimals = b0.divideAndRemainder(b1);
    return bigDecimals[1];
  }

  // ABS

  /** SQL <code>ABS</code> operator applied to byte values. */
  public static byte abs(byte b0) {
    return (byte) Math.abs(b0);
  }

  /** SQL <code>ABS</code> operator applied to short values. */
  public static short abs(short b0) {
    return (short) Math.abs(b0);
  }

  /** SQL <code>ABS</code> operator applied to int values. */
  public static int abs(int b0) {
    return Math.abs(b0);
  }

  /** SQL <code>ABS</code> operator applied to long values. */
  public static long abs(long b0) {
    return Math.abs(b0);
  }

  /** SQL <code>ABS</code> operator applied to float values. */
  public static float abs(float b0) {
    return Math.abs(b0);
  }

  /** SQL <code>ABS</code> operator applied to double values. */
  public static double abs(double b0) {
    return Math.abs(b0);
  }

  /** SQL <code>ABS</code> operator applied to BigDecimal values. */
  public static BigDecimal abs(BigDecimal b0) {
    return b0.abs();
  }

  // Helpers

  /** Helper for implementing MIN. Somewhat similar to LEAST operator. */
  public static <T extends Comparable<T>> T lesser(T b0, T b1) {
    return b0 == null || b0.compareTo(b1) > 0 ? b1 : b0;
  }

  /** LEAST operator. */
  public static <T extends Comparable<T>> T least(T b0, T b1) {
    return b0 == null || b1 != null && b0.compareTo(b1) > 0 ? b1 : b0;
  }

  public static boolean greater(boolean b0, boolean b1) {
    return b0 || b1;
  }

  public static boolean lesser(boolean b0, boolean b1) {
    return b0 && b1;
  }

  public static byte greater(byte b0, byte b1) {
    return b0 > b1 ? b0 : b1;
  }

  public static byte lesser(byte b0, byte b1) {
    return b0 > b1 ? b1 : b0;
  }

  public static char greater(char b0, char b1) {
    return b0 > b1 ? b0 : b1;
  }

  public static char lesser(char b0, char b1) {
    return b0 > b1 ? b1 : b0;
  }

  public static short greater(short b0, short b1) {
    return b0 > b1 ? b0 : b1;
  }

  public static short lesser(short b0, short b1) {
    return b0 > b1 ? b1 : b0;
  }

  public static int greater(int b0, int b1) {
    return b0 > b1 ? b0 : b1;
  }

  public static int lesser(int b0, int b1) {
    return b0 > b1 ? b1 : b0;
  }

  public static long greater(long b0, long b1) {
    return b0 > b1 ? b0 : b1;
  }

  public static long lesser(long b0, long b1) {
    return b0 > b1 ? b1 : b0;
  }

  public static float greater(float b0, float b1) {
    return b0 > b1 ? b0 : b1;
  }

  public static float lesser(float b0, float b1) {
    return b0 > b1 ? b1 : b0;
  }

  public static double greater(double b0, double b1) {
    return b0 > b1 ? b0 : b1;
  }

  public static double lesser(double b0, double b1) {
    return b0 > b1 ? b1 : b0;
  }

  /** Helper for implementing MAX. Somewhat similar to GREATEST operator. */
  public static <T extends Comparable<T>> T greater(T b0, T b1) {
    return b0 == null || b0.compareTo(b1) < 0 ? b1 : b0;
  }

  /** GREATEST operator. */
  public static <T extends Comparable<T>> T greatest(T b0, T b1) {
    return b0 == null || b1 != null && b0.compareTo(b1) < 0 ? b1 : b0;
  }

  /** Boolean comparison. */
  public static int compare(boolean x, boolean y) {
    return x == y ? 0 : x ? 1 : -1;
  }

  /** CAST(FLOAT AS VARCHAR). */
  public static String toString(float x) {
    if (x == 0) {
      return "0E0";
    }
    BigDecimal bigDecimal =
        new BigDecimal(x, MathContext.DECIMAL32).stripTrailingZeros();
    final String s = bigDecimal.toString();
    return s.replaceAll("0*E", "E").replace("E+", "E");
  }

  /** CAST(DOUBLE AS VARCHAR). */
  public static String toString(double x) {
    if (x == 0) {
      return "0E0";
    }
    BigDecimal bigDecimal =
        new BigDecimal(x, MathContext.DECIMAL64).stripTrailingZeros();
    final String s = bigDecimal.toString();
    return s.replaceAll("0*E", "E").replace("E+", "E");
  }

  /** CAST(DECIMAL AS VARCHAR). */
  public static String toString(BigDecimal x) {
    final String s = x.toString();
    if (s.startsWith("0")) {
      // we want ".1" not "0.1"
      return s.substring(1);
    } else if (s.startsWith("-0")) {
      // we want "-.1" not "-0.1"
      return "-" + s.substring(2);
    } else {
      return s;
    }
  }

  /** CAST(BOOLEAN AS VARCHAR). */
  public static String toString(boolean x) {
    // Boolean.toString returns lower case -- no good.
    return x ? "TRUE" : "FALSE";
  }

  @NonDeterministic
  private static Object cannotConvert(Object o, Class toType) {
    throw new RuntimeException("Cannot convert " + o + " to " + toType);
  }

  /** CAST(VARCHAR AS BOOLEAN). */
  public static boolean toBoolean(String s) {
    s = trim_(s, true, true, ' ');
    if (s.equalsIgnoreCase("TRUE")) {
      return true;
    } else if (s.equalsIgnoreCase("FALSE")) {
      return false;
    } else {
      throw new RuntimeException("Invalid character for cast");
    }
  }

  public static boolean toBoolean(Number number) {
    return !number.equals(0);
  }

  public static boolean toBoolean(Object o) {
    return o instanceof Boolean ? (Boolean) o
        : o instanceof Number ? toBoolean((Number) o)
        : o instanceof String ? toBoolean((String) o)
        : (Boolean) cannotConvert(o, boolean.class);
  }

  // Don't need parseByte etc. - Byte.parseByte is sufficient.

  public static byte toByte(Object o) {
    return o instanceof Byte ? (Byte) o
        : o instanceof Number ? toByte((Number) o)
        : Byte.parseByte(o.toString());
  }

  public static byte toByte(Number number) {
    return number.byteValue();
  }

  public static char toChar(String s) {
    return s.charAt(0);
  }

  public static Character toCharBoxed(String s) {
    return s.charAt(0);
  }

  public static short toShort(String s) {
    return Short.parseShort(s.trim());
  }

  public static short toShort(Number number) {
    return number.shortValue();
  }

  public static short toShort(Object o) {
    return o instanceof Short ? (Short) o
        : o instanceof Number ? toShort((Number) o)
        : o instanceof String ? toShort((String) o)
        : (Short) cannotConvert(o, short.class);
  }

  public static int toInt(java.util.Date v) {
    return toInt(v, LOCAL_TZ);
  }

  public static int toInt(java.util.Date v, TimeZone timeZone) {
    return (int) (toLong(v, timeZone)  / DateTimeUtil.MILLIS_PER_DAY);
  }

  public static Integer toIntOptional(java.util.Date v) {
    return v == null ? null : toInt(v);
  }

  public static Integer toIntOptional(java.util.Date v, TimeZone timeZone) {
    return v == null
        ? null
        : toInt(v, timeZone);
  }

  public static long toLong(Date v) {
    return toLong(v, LOCAL_TZ);
  }

  public static int toInt(java.sql.Time v) {
    return (int) (toLong(v) % DateTimeUtil.MILLIS_PER_DAY);
  }

  public static Integer toIntOptional(java.sql.Time v) {
    return v == null ? null : toInt(v);
  }

  public static int toInt(String s) {
    return Integer.parseInt(s.trim());
  }

  public static int toInt(Number number) {
    return number.intValue();
  }

  public static int toInt(Object o) {
    return o instanceof Integer ? (Integer) o
        : o instanceof Number ? toInt((Number) o)
        : o instanceof String ? toInt((String) o)
        : (Integer) cannotConvert(o, int.class);
  }

  public static long toLong(Timestamp v) {
    return toLong(v, LOCAL_TZ);
  }

  // mainly intended for java.sql.Timestamp but works for other dates also
  public static long toLong(java.util.Date v, TimeZone timeZone) {
    final long time = v.getTime();
    return time + timeZone.getOffset(time);
  }

  // mainly intended for java.sql.Timestamp but works for other dates also
  public static Long toLongOptional(java.util.Date v) {
    return v == null ? null : toLong(v, LOCAL_TZ);
  }

  public static Long toLongOptional(Timestamp v, TimeZone timeZone) {
    if (v == null) {
      return null;
    }
    return toLong(v, LOCAL_TZ);
  }

  public static long toLong(String s) {
    if (s.startsWith("199") && s.contains(":")) {
      return Timestamp.valueOf(s).getTime();
    }
    return Long.parseLong(s.trim());
  }

  public static long toLong(Number number) {
    return number.longValue();
  }

  public static long toLong(Object o) {
    return o instanceof Long ? (Long) o
        : o instanceof Number ? toLong((Number) o)
        : o instanceof String ? toLong((String) o)
        : (Long) cannotConvert(o, long.class);
  }

  public static float toFloat(String s) {
    return Float.parseFloat(s.trim());
  }

  public static float toFloat(Number number) {
    return number.floatValue();
  }

  public static float toFloat(Object o) {
    return o instanceof Float ? (Float) o
        : o instanceof Number ? toFloat((Number) o)
            : o instanceof String ? toFloat((String) o)
                : (Float) cannotConvert(o, float.class);
  }

  public static double toDouble(String s) {
    return Double.parseDouble(s.trim());
  }

  public static double toDouble(Number number) {
    return number.doubleValue();
  }

  public static double toDouble(Object o) {
    return o instanceof Double ? (Double) o
        : o instanceof Number ? toDouble((Number) o)
        : o instanceof String ? toDouble((String) o)
        : (Double) cannotConvert(o, double.class);
  }

  public static BigDecimal toBigDecimal(String s) {
    return new BigDecimal(s.trim());
  }

  public static BigDecimal toBigDecimal(Number number) {
    // There are some values of "long" that cannot be represented as "double".
    // Not so "int". If it isn't a long, go straight to double.
    return number instanceof BigDecimal ? (BigDecimal) number
        : number instanceof BigInteger ? new BigDecimal((BigInteger) number)
        : number instanceof Long ? new BigDecimal(number.longValue())
        : new BigDecimal(number.doubleValue());
  }

  public static BigDecimal toBigDecimal(Object o) {
    return o instanceof Number ? toBigDecimal((Number) o)
        : toBigDecimal(o.toString());
  }

  // Don't need shortValueOf etc. - Short.valueOf is sufficient.

  /** Helper for CAST(... AS VARCHAR(maxLength)). */
  public static String truncate(String s, int maxLength) {
    return s == null ? null
        : s.length() > maxLength ? s.substring(0, maxLength)
        : s;
  }

  /** Helper for CAST(... AS VARBINARY(maxLength)). */
  public static ByteString truncate(ByteString s, int maxLength) {
    return s == null ? null
        : s.length() > maxLength ? s.substring(0, maxLength)
        : s;
  }

  /** SQL {@code POSITION(seek IN string)} function. */
  public static int position(String seek, String s) {
    return s.indexOf(seek) + 1;
  }

  /** SQL {@code POSITION(seek IN string)} function. */
  public static int position(ByteString seek, ByteString s) {
    return s.indexOf(seek) + 1;
  }

  /** Cheap, unsafe, long power. power(2, 3) returns 8. */
  public static long powerX(long a, long b) {
    long x = 1;
    while (b > 0) {
      x *= a;
      --b;
    }
    return x;
  }

  /** Helper for rounding. Truncate(12345, 1000) returns 12000. */
  public static long round(long v, long x) {
    return truncate(v + x / 2, x);
  }

  /** Helper for rounding. Truncate(12345, 1000) returns 12000. */
  public static long truncate(long v, long x) {
    long remainder = v % x;
    if (remainder < 0) {
      remainder += x;
    }
    return v - remainder;
  }

  /** Helper for rounding. Truncate(12345, 1000) returns 12000. */
  public static int round(int v, int x) {
    return truncate(v + x / 2, x);
  }

  /** Helper for rounding. Truncate(12345, 1000) returns 12000. */
  public static int truncate(int v, int x) {
    int remainder = v % x;
    if (remainder < 0) {
      remainder += x;
    }
    return v - remainder;
  }

  /** Helper for CAST({timestamp} AS VARCHAR(n)). */
  public static String unixTimestampToString(long timestamp) {
    final StringBuilder buf = new StringBuilder(17);
    int date = (int) (timestamp / DateTimeUtil.MILLIS_PER_DAY);
    int time = (int) (timestamp % DateTimeUtil.MILLIS_PER_DAY);
    if (time < 0) {
      --date;
      time += DateTimeUtil.MILLIS_PER_DAY;
    }
    unixDateToString(buf, date);
    buf.append(' ');
    unixTimeToString(buf, time);
    return buf.toString();
  }

  /** Helper for CAST({timestamp} AS VARCHAR(n)). */
  public static String unixTimeToString(int time) {
    final StringBuilder buf = new StringBuilder(8);
    unixTimeToString(buf, time);
    return buf.toString();
  }

  private static void unixTimeToString(StringBuilder buf, int time) {
    int h = time / 3600000;
    int time2 = time % 3600000;
    int m = time2 / 60000;
    int time3 = time2 % 60000;
    int s = time3 / 1000;
    int ms = time3 % 1000;
    int2(buf, h);
    buf.append(':');
    int2(buf, m);
    buf.append(':');
    int2(buf, s);
  }

  /** SQL {@code CURRENT_TIMESTAMP} function. */
  @NonDeterministic
  public static long currentTimestamp(DataContext root) {
    // Cast required for JDK 1.6.
    return (Long) DataContext.Variable.CURRENT_TIMESTAMP.get(root);
  }

  /** SQL {@code CURRENT_TIME} function. */
  @NonDeterministic
  public static int currentTime(DataContext root) {
    int time = (int) (currentTimestamp(root) % DateTimeUtil.MILLIS_PER_DAY);
    if (time < 0) {
      time += DateTimeUtil.MILLIS_PER_DAY;
    }
    return time;
  }

  /** SQL {@code CURRENT_DATE} function. */
  @NonDeterministic
  public static int currentDate(DataContext root) {
    final long timestamp = currentTimestamp(root);
    int date = (int) (timestamp / DateTimeUtil.MILLIS_PER_DAY);
    final int time = (int) (timestamp % DateTimeUtil.MILLIS_PER_DAY);
    if (time < 0) {
      --date;
    }
    return date;
  }

  /** SQL {@code LOCAL_TIMESTAMP} function. */
  @NonDeterministic
  public static long localTimestamp(DataContext root) {
    // Cast required for JDK 1.6.
    return (Long) DataContext.Variable.LOCAL_TIMESTAMP.get(root);
  }

  /** SQL {@code LOCAL_TIME} function. */
  @NonDeterministic
  public static int localTime(DataContext root) {
    return (int) (localTimestamp(root) % DateTimeUtil.MILLIS_PER_DAY);
  }

  private static void int2(StringBuilder buf, int i) {
    buf.append((char) ('0' + (i / 10) % 10));
    buf.append((char) ('0' + i % 10));
  }

  private static void int4(StringBuilder buf, int i) {
    buf.append((char) ('0' + (i / 1000) % 10));
    buf.append((char) ('0' + (i / 100) % 10));
    buf.append((char) ('0' + (i / 10) % 10));
    buf.append((char) ('0' + i % 10));
  }

  public static int dateStringToUnixDate(String s) {
    int hyphen1 = s.indexOf('-');
    int y;
    int m;
    int d;
    if (hyphen1 < 0) {
      y = Integer.parseInt(s.trim());
      m = 1;
      d = 1;
    } else {
      y = Integer.parseInt(s.substring(0, hyphen1).trim());
      final int hyphen2 = s.indexOf('-', hyphen1 + 1);
      if (hyphen2 < 0) {
        m = Integer.parseInt(s.substring(hyphen1 + 1).trim());
        d = 1;
      } else {
        m = Integer.parseInt(s.substring(hyphen1 + 1, hyphen2).trim());
        d = Integer.parseInt(s.substring(hyphen2 + 1).trim());
      }
    }
    return ymdToUnixDate(y, m, d);
  }

  public static int timeStringToUnixDate(String v) {
    return timeStringToUnixDate(v, 0);
  }

  public static int timeStringToUnixDate(String v, int start) {
    final int colon1 = v.indexOf(':', start);
    int hour;
    int minute;
    int second;
    int milli;
    if (colon1 < 0) {
      hour = Integer.parseInt(v.trim());
      minute = 1;
      second = 1;
      milli = 0;
    } else {
      hour = Integer.parseInt(v.substring(start, colon1).trim());
      final int colon2 = v.indexOf(':', colon1 + 1);
      if (colon2 < 0) {
        minute = Integer.parseInt(v.substring(colon1 + 1).trim());
        second = 1;
        milli = 0;
      } else {
        minute = Integer.parseInt(v.substring(colon1 + 1, colon2).trim());
        int dot = v.indexOf('.', colon2);
        if (dot < 0) {
          second = Integer.parseInt(v.substring(colon2 + 1).trim());
          milli = 0;
        } else {
          second = Integer.parseInt(v.substring(colon2 + 1, dot).trim());
          milli = Integer.parseInt(v.substring(dot + 1).trim());
        }
      }
    }
    return hour * (int) DateTimeUtil.MILLIS_PER_HOUR
        + minute * (int) DateTimeUtil.MILLIS_PER_MINUTE
        + second * (int) DateTimeUtil.MILLIS_PER_SECOND
        + milli;
  }

  public static long timestampStringToUnixDate(String s) {
    final long d;
    final long t;
    s = s.trim();
    int space = s.indexOf(' ');
    if (space >= 0) {
      d = dateStringToUnixDate(s.substring(0, space));
      t = timeStringToUnixDate(s, space + 1);
    } else {
      d = dateStringToUnixDate(s);
      t = 0;
    }
    return d * DateTimeUtil.MILLIS_PER_DAY + t;
  }

  /** Helper for CAST({date} AS VARCHAR(n)). */
  public static String unixDateToString(int date) {
    final StringBuilder buf = new StringBuilder(10);
    unixDateToString(buf, date);
    return buf.toString();
  }

  private static void unixDateToString(StringBuilder buf, int date) {
    julianToString(buf, date + EPOCH_JULIAN);
  }

  private static void julianToString(StringBuilder buf, int julian) {
    // this shifts the epoch back to astronomical year -4800 instead of the
    // start of the Christian era in year AD 1 of the proleptic Gregorian
    // calendar.
    int j = julian + 32044;
    int g = j / 146097;
    int dg = j % 146097;
    int c = (dg / 36524 + 1) * 3 / 4;
    int dc = dg - c * 36524;
    int b = dc / 1461;
    int db = dc % 1461;
    int a = (db / 365 + 1) * 3 / 4;
    int da = db - a * 365;

    // integer number of full years elapsed since March 1, 4801 BC
    int y = g * 400 + c * 100 + b * 4 + a;
    // integer number of full months elapsed since the last March 1
    int m = (da * 5 + 308) / 153 - 2;
    // number of days elapsed since day 1 of the month
    int d = da - (m + 4) * 153 / 5 + 122;
    int year = y - 4800 + (m + 2) / 12;
    int month = (m + 2) % 12 + 1;
    int day = d + 1;
    int4(buf, year);
    buf.append('-');
    int2(buf, month);
    buf.append('-');
    int2(buf, day);
  }

  public static long unixDateExtract(TimeUnitRange range, long date) {
    return julianExtract(range, (int) date + EPOCH_JULIAN);
  }

  private static int julianExtract(TimeUnitRange range, int julian) {
    // this shifts the epoch back to astronomical year -4800 instead of the
    // start of the Christian era in year AD 1 of the proleptic Gregorian
    // calendar.
    int j = julian + 32044;
    int g = j / 146097;
    int dg = j % 146097;
    int c = (dg / 36524 + 1) * 3 / 4;
    int dc = dg - c * 36524;
    int b = dc / 1461;
    int db = dc % 1461;
    int a = (db / 365 + 1) * 3 / 4;
    int da = db - a * 365;

    // integer number of full years elapsed since March 1, 4801 BC
    int y = g * 400 + c * 100 + b * 4 + a;
    // integer number of full months elapsed since the last March 1
    int m = (da * 5 + 308) / 153 - 2;
    // number of days elapsed since day 1 of the month
    int d = da - (m + 4) * 153 / 5 + 122;
    int year = y - 4800 + (m + 2) / 12;
    int month = (m + 2) % 12 + 1;
    int day = d + 1;
    switch (range) {
    case YEAR:
      return year;
    case MONTH:
      return month;
    case DAY:
      return day;
    default:
      throw new AssertionError(range);
    }
  }

  public static int ymdToUnixDate(int year, int month, int day) {
    final int julian = ymdToJulian(year, month, day);
    return julian - EPOCH_JULIAN;
  }

  public static int ymdToJulian(int year, int month, int day) {
    int a = (14 - month) / 12;
    int y = year + 4800 - a;
    int m = month + 12 * a - 3;
    int j = day + (153 * m + 2) / 5
        + 365 * y
        + y / 4
        - y / 100
        + y / 400
        - 32045;
    if (j < 2299161) {
      j = day + (153 * m + 2) / 5 + 365 * y + y / 4 - 32083;
    }
    return j;
  }

  public static String intervalYearMonthToString(int v, TimeUnitRange range) {
    final StringBuilder buf = new StringBuilder();
    if (v >= 0) {
      buf.append('+');
    } else {
      buf.append('-');
      v = -v;
    }
    final int y;
    final int m;
    switch (range) {
    case YEAR:
      v = roundUp(v, 12);
      y = v / 12;
      buf.append(y);
      break;
    case YEAR_TO_MONTH:
      y = v / 12;
      buf.append(y);
      buf.append('-');
      m = v % 12;
      number(buf, m, 2);
      break;
    case MONTH:
      m = v;
      buf.append(m);
      break;
    default:
      throw new AssertionError(range);
    }
    return buf.toString();
  }

  private static StringBuilder number(StringBuilder buf, int v, int n) {
    for (int k = digitCount(v); k < n; k++) {
      buf.append('0');
    }
    return buf.append(v);
  }

  public static int digitCount(int v) {
    for (int n = 1;; n++) {
      v /= 10;
      if (v == 0) {
        return n;
      }
    }
  }

  public static String intervalDayTimeToString(long v, TimeUnitRange range,
      int scale) {
    final StringBuilder buf = new StringBuilder();
    if (v >= 0) {
      buf.append('+');
    } else {
      buf.append('-');
      v = -v;
    }
    final long ms;
    final long s;
    final long m;
    final long h;
    final long d;
    switch (range) {
    case DAY_TO_SECOND:
      v = roundUp(v, powerX(10, 3 - scale));
      ms = v % 1000;
      v /= 1000;
      s = v % 60;
      v /= 60;
      m = v % 60;
      v /= 60;
      h = v % 24;
      v /= 24;
      d = v;
      buf.append((int) d);
      buf.append(' ');
      number(buf, (int) h, 2);
      buf.append(':');
      number(buf, (int) m, 2);
      buf.append(':');
      number(buf, (int) s, 2);
      fraction(buf, scale, ms);
      break;
    case DAY_TO_MINUTE:
      v = roundUp(v, 1000 * 60);
      v /= 1000;
      v /= 60;
      m = v % 60;
      v /= 60;
      h = v % 24;
      v /= 24;
      d = v;
      buf.append((int) d);
      buf.append(' ');
      number(buf, (int) h, 2);
      buf.append(':');
      number(buf, (int) m, 2);
      break;
    case DAY_TO_HOUR:
      v = roundUp(v, 1000 * 60 * 60);
      v /= 1000;
      v /= 60;
      v /= 60;
      h = v % 24;
      v /= 24;
      d = v;
      buf.append((int) d);
      buf.append(' ');
      number(buf, (int) h, 2);
      break;
    case DAY:
      v = roundUp(v, 1000 * 60 * 60 * 24);
      d = v / (1000 * 60 * 60 * 24);
      buf.append((int) d);
      break;
    case HOUR:
      v = roundUp(v, 1000 * 60 * 60);
      v /= 1000;
      v /= 60;
      v /= 60;
      h = v;
      buf.append((int) h);
      break;
    case HOUR_TO_MINUTE:
      v = roundUp(v, 1000 * 60);
      v /= 1000;
      v /= 60;
      m = v % 60;
      v /= 60;
      h = v;
      buf.append((int) h);
      buf.append(':');
      number(buf, (int) m, 2);
      break;
    case HOUR_TO_SECOND:
      v = roundUp(v, powerX(10, 3 - scale));
      ms = v % 1000;
      v /= 1000;
      s = v % 60;
      v /= 60;
      m = v % 60;
      v /= 60;
      h = v;
      buf.append((int) h);
      buf.append(':');
      number(buf, (int) m, 2);
      buf.append(':');
      number(buf, (int) s, 2);
      fraction(buf, scale, ms);
      break;
    case MINUTE_TO_SECOND:
      v = roundUp(v, powerX(10, 3 - scale));
      ms = v % 1000;
      v /= 1000;
      s = v % 60;
      v /= 60;
      m = v;
      buf.append((int) m);
      buf.append(':');
      number(buf, (int) s, 2);
      fraction(buf, scale, ms);
      break;
    case MINUTE:
      v = roundUp(v, 1000 * 60);
      v /= 1000;
      v /= 60;
      m = v;
      buf.append((int) m);
      break;
    case SECOND:
      v = roundUp(v, powerX(10, 3 - scale));
      ms = v % 1000;
      v /= 1000;
      s = v;
      buf.append((int) s);
      fraction(buf, scale, ms);
      break;
    default:
      throw new AssertionError(range);
    }
    return buf.toString();
  }

  /**
   * Rounds a dividend to the nearest divisor.
   * For example roundUp(31, 10) yields 30; roundUp(37, 10) yields 40.
   * @param dividend Number to be divided
   * @param divisor Number to divide by
   * @return Rounded dividend
   */
  private static long roundUp(long dividend, long divisor) {
    long remainder = dividend % divisor;
    dividend -= remainder;
    if (remainder * 2 > divisor) {
      dividend += divisor;
    }
    return dividend;
  }

  private static int roundUp(int dividend, int divisor) {
    int remainder = dividend % divisor;
    dividend -= remainder;
    if (remainder * 2 > divisor) {
      dividend += divisor;
    }
    return dividend;
  }

  private static void fraction(StringBuilder buf, int scale, long ms) {
    if (scale > 0) {
      buf.append('.');
      long v1 = scale == 3 ? ms
          : scale == 2 ? ms / 10
          : scale == 1 ? ms / 100
            : 0;
      number(buf, (int) v1, scale);
    }
  }

  /** Helper for "array element reference". Caller has already ensured that
   * array and index are not null. Index is 1-based, per SQL. */
  public static Object arrayItem(List list, int item) {
    if (item < 1 || item > list.size()) {
      return null;
    }
    return list.get(item - 1);
  }

  /** Helper for "map element reference". Caller has already ensured that
   * array and index are not null. Index is 1-based, per SQL. */
  public static Object mapItem(Map map, Object item) {
    return map.get(item);
  }

  /** Implements the {@code [ ... ]} operator on an object whose type is not
   * known until runtime.
   */
  public static Object item(Object object, Object index) {
    if (object instanceof Map) {
      return ((Map) object).get(index);
    }
    if (object instanceof List && index instanceof Number) {
      List list = (List) object;
      return list.get(((Number) index).intValue());
    }
    return null;
  }

  /** NULL &rarr; FALSE, FALSE &rarr; FALSE, TRUE &rarr; TRUE. */
  public static boolean isTrue(Boolean b) {
    return b != null && b;
  }

  /** NULL &rarr; TRUE, FALSE &rarr; FALSE, TRUE &rarr; TRUE. */
  public static boolean isNotFalse(Boolean b) {
    return b == null || b;
  }

  /** NULL &rarr; NULL, FALSE &rarr; TRUE, TRUE &rarr; FALSE. */
  public static Boolean not(Boolean b) {
    return (b == null) ? null : !b;
  }

  /** Converts a JDBC array to a list. */
  public static List arrayToList(final java.sql.Array a) {
    if (a == null) {
      return null;
    }
    try {
      return Primitive.asList(a.getArray());
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  /** Support the SLICE function. */
  public static List slice(List list) {
    return list;
  }

  /** Support the ELEMENT function. */
  public static Object element(List list) {
    switch (list.size()) {
    case 0:
      return null;
    case 1:
      return list.get(0);
    default:
      throw new RuntimeException("more than one value");
    }
  }

  /** Returns a lambda that converts a list to an enumerable. */
  public static <E> Function1<List<E>, Enumerable<E>> listToEnumerable() {
    //noinspection unchecked
    return (Function1<List<E>, Enumerable<E>>) (Function1) LIST_AS_ENUMERABLE;
  }

  /** A range of time units. The first is more significant than the
   * other (e.g. year-to-day) or the same as the other
   * (e.g. month). */
  public enum TimeUnitRange {
    YEAR,
    YEAR_TO_MONTH,
    MONTH,
    DAY,
    DAY_TO_HOUR,
    DAY_TO_MINUTE,
    DAY_TO_SECOND,
    HOUR,
    HOUR_TO_MINUTE,
    HOUR_TO_SECOND,
    MINUTE,
    MINUTE_TO_SECOND,
    SECOND;

    /** Whether this is in the YEAR-TO-MONTH family of intervals. */
    public boolean monthly() {
      return ordinal() <= MONTH.ordinal();
    }
  }
}

// End SqlFunctions.java
