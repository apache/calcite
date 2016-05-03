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
package org.apache.calcite.runtime;

import org.apache.calcite.DataContext;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.function.Deterministic;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.function.NonDeterministic;
import org.apache.calcite.linq4j.tree.Primitive;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicLong;
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

  private static final TimeZone LOCAL_TZ = TimeZone.getDefault();

  private static final Function1<List<Object>, Enumerable<Object>>
  LIST_AS_ENUMERABLE =
      new Function1<List<Object>, Enumerable<Object>>() {
        public Enumerable<Object> apply(List<Object> list) {
          return Linq4j.asEnumerable(list);
        }
      };

  /** Holds, for each thread, a map from sequence name to sequence current
   * value.
   *
   * <p>This is a straw man of an implementation whose main goal is to prove
   * that sequences can be parsed, validated and planned. A real application
   * will want persistent values for sequences, shared among threads. */
  private static final ThreadLocal<Map<String, AtomicLong>> THREAD_SEQUENCES =
      new ThreadLocal<Map<String, AtomicLong>>() {
        @Override protected Map<String, AtomicLong> initialValue() {
          return new HashMap<String, AtomicLong>();
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
    return (b0 == null || b1 == null)
        ? null
        : b0.divide(b1, MathContext.DECIMAL64);
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

  // FLOOR

  public static double floor(double b0) {
    return Math.floor(b0);
  }

  public static float floor(float b0) {
    return (float) Math.floor(b0);
  }

  public static BigDecimal floor(BigDecimal b0) {
    return b0.setScale(0, BigDecimal.ROUND_FLOOR);
  }

  /** SQL <code>FLOOR</code> operator applied to byte values. */
  public static byte floor(byte b0, byte b1) {
    return (byte) floor((int) b0, (int) b1);
  }

  /** SQL <code>FLOOR</code> operator applied to short values. */
  public static short floor(short b0, short b1) {
    return (short) floor((int) b0, (int) b1);
  }

  /** SQL <code>FLOOR</code> operator applied to int values. */
  public static int floor(int b0, int b1) {
    int r = b0 % b1;
    if (r < 0) {
      r += b1;
    }
    return b0 - r;
  }

  /** SQL <code>FLOOR</code> operator applied to long values. */
  public static long floor(long b0, long b1) {
    long r = b0 % b1;
    if (r < 0) {
      r += b1;
    }
    return b0 - r;
  }

  // temporary
  public static BigDecimal floor(BigDecimal b0, int b1) {
    return floor(b0, BigDecimal.valueOf(b1));
  }

  // temporary
  public static int floor(int b0, BigDecimal b1) {
    return floor(b0, b1.intValue());
  }

  public static BigDecimal floor(BigDecimal b0, BigDecimal b1) {
    final BigDecimal[] bigDecimals = b0.divideAndRemainder(b1);
    BigDecimal r = bigDecimals[1];
    if (r.signum() < 0) {
      r = r.add(b1);
    }
    return b0.subtract(r);
  }

  // CEIL

  public static double ceil(double b0) {
    return Math.ceil(b0);
  }

  public static float ceil(float b0) {
    return (float) Math.ceil(b0);
  }

  public static BigDecimal ceil(BigDecimal b0) {
    return b0.setScale(0, BigDecimal.ROUND_CEILING);
  }

  /** SQL <code>CEIL</code> operator applied to byte values. */
  public static byte ceil(byte b0, byte b1) {
    return floor((byte) (b0 + b1 - 1), b1);
  }

  /** SQL <code>CEIL</code> operator applied to short values. */
  public static short ceil(short b0, short b1) {
    return floor((short) (b0 + b1 - 1), b1);
  }

  /** SQL <code>CEIL</code> operator applied to int values. */
  public static int ceil(int b0, int b1) {
    int r = b0 % b1;
    if (r > 0) {
      r -= b1;
    }
    return b0 - r;
  }

  /** SQL <code>CEIL</code> operator applied to long values. */
  public static long ceil(long b0, long b1) {
    return floor(b0 + b1 - 1, b1);
  }

  // temporary
  public static BigDecimal ceil(BigDecimal b0, int b1) {
    return ceil(b0, BigDecimal.valueOf(b1));
  }

  // temporary
  public static int ceil(int b0, BigDecimal b1) {
    return ceil(b0, b1.intValue());
  }

  public static BigDecimal ceil(BigDecimal b0, BigDecimal b1) {
    final BigDecimal[] bigDecimals = b0.divideAndRemainder(b1);
    BigDecimal r = bigDecimals[1];
    if (r.signum() > 0) {
      r = r.subtract(b1);
    }
    return b0.subtract(r);
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

  /** Converts the Java type used for UDF parameters of SQL DATE type
   * ({@link java.sql.Date}) to internal representation (int).
   *
   * <p>Converse of {@link #internalToDate(int)}. */
  public static int toInt(java.util.Date v) {
    return toInt(v, LOCAL_TZ);
  }

  public static int toInt(java.util.Date v, TimeZone timeZone) {
    return (int) (toLong(v, timeZone)  / DateTimeUtils.MILLIS_PER_DAY);
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

  /** Converts the Java type used for UDF parameters of SQL TIME type
   * ({@link java.sql.Time}) to internal representation (int).
   *
   * <p>Converse of {@link #internalToTime(int)}. */
  public static int toInt(java.sql.Time v) {
    return (int) (toLong(v) % DateTimeUtils.MILLIS_PER_DAY);
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

  /** Converts the Java type used for UDF parameters of SQL TIMESTAMP type
   * ({@link java.sql.Timestamp}) to internal representation (long).
   *
   * <p>Converse of {@link #internalToTimestamp(long)}. */
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

  /** Converts the internal representation of a SQL DATE (int) to the Java
   * type used for UDF parameters ({@link java.sql.Date}). */
  public static java.sql.Date internalToDate(int v) {
    final long t = v * DateTimeUtils.MILLIS_PER_DAY;
    return new java.sql.Date(t - LOCAL_TZ.getOffset(t));
  }

  /** As {@link #internalToDate(int)} but allows nulls. */
  public static java.sql.Date internalToDate(Integer v) {
    return v == null ? null : internalToDate(v.intValue());
  }

  /** Converts the internal representation of a SQL TIME (int) to the Java
   * type used for UDF parameters ({@link java.sql.Time}). */
  public static java.sql.Time internalToTime(int v) {
    return new java.sql.Time(v - LOCAL_TZ.getOffset(v));
  }

  public static java.sql.Time internalToTime(Integer v) {
    return v == null ? null : internalToTime(v.intValue());
  }

  /** Converts the internal representation of a SQL TIMESTAMP (long) to the Java
   * type used for UDF parameters ({@link java.sql.Timestamp}). */
  public static java.sql.Timestamp internalToTimestamp(long v) {
    return new java.sql.Timestamp(v - LOCAL_TZ.getOffset(v));
  }

  public static java.sql.Timestamp internalToTimestamp(Long v) {
    return v == null ? null : internalToTimestamp(v.longValue());
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

  /** SQL {@code CURRENT_TIMESTAMP} function. */
  @NonDeterministic
  public static long currentTimestamp(DataContext root) {
    // Cast required for JDK 1.6.
    return (Long) DataContext.Variable.CURRENT_TIMESTAMP.get(root);
  }

  /** SQL {@code CURRENT_TIME} function. */
  @NonDeterministic
  public static int currentTime(DataContext root) {
    int time = (int) (currentTimestamp(root) % DateTimeUtils.MILLIS_PER_DAY);
    if (time < 0) {
      time += DateTimeUtils.MILLIS_PER_DAY;
    }
    return time;
  }

  /** SQL {@code CURRENT_DATE} function. */
  @NonDeterministic
  public static int currentDate(DataContext root) {
    final long timestamp = currentTimestamp(root);
    int date = (int) (timestamp / DateTimeUtils.MILLIS_PER_DAY);
    final int time = (int) (timestamp % DateTimeUtils.MILLIS_PER_DAY);
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
    return (int) (localTimestamp(root) % DateTimeUtils.MILLIS_PER_DAY);
  }

  /** SQL TRANSLATE(string, search_chars, replacement_chars) function. */
  public static String translate3(String s, String search, String replacement) {
    return org.apache.commons.lang3.StringUtils.replaceChars(s, search, replacement);
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

  /** NULL &rarr; FALSE, FALSE &rarr; TRUE, TRUE &rarr; FALSE. */
  public static boolean isFalse(Boolean b) {
    return b != null && !b;
  }

  /** NULL &rarr; TRUE, FALSE &rarr; TRUE, TRUE &rarr; FALSE. */
  public static boolean isNotTrue(Boolean b) {
    return b == null || !b;
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

  /** Support the {@code CURRENT VALUE OF sequence} operator. */
  @NonDeterministic
  public static long sequenceCurrentValue(String key) {
    return getAtomicLong(key).get();
  }

  /** Support the {@code NEXT VALUE OF sequence} operator. */
  @NonDeterministic
  public static long sequenceNextValue(String key) {
    return getAtomicLong(key).incrementAndGet();
  }

  private static AtomicLong getAtomicLong(String key) {
    final Map<String, AtomicLong> map = THREAD_SEQUENCES.get();
    AtomicLong atomic = map.get(key);
    if (atomic == null) {
      atomic = new AtomicLong();
      map.put(key, atomic);
    }
    return atomic;
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

  public static Object[] array(Object... args) {
    return args;
  }
}

// End SqlFunctions.java
