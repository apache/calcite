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
import org.apache.calcite.avatica.util.Spaces;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.interpreter.Row;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.CartesianProductEnumerator;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.function.Deterministic;
import org.apache.calcite.linq4j.function.Experimental;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.function.NonDeterministic;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.runtime.FlatLists.ComparableList;
import org.apache.calcite.util.Bug;
import org.apache.calcite.util.NumberUtil;
import org.apache.calcite.util.TimeWithTimeZoneString;
import org.apache.calcite.util.TimestampWithTimeZoneString;
import org.apache.calcite.util.Unsafe;
import org.apache.calcite.util.Util;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.codec.language.Soundex;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.math.RoundingMode;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;

import static org.apache.calcite.util.Static.RESOURCE;

import static java.nio.charset.StandardCharsets.UTF_8;

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
      NumberUtil.decimalFormat("0.0E0");

  private static final TimeZone LOCAL_TZ = TimeZone.getDefault();

  private static final DateTimeFormatter ROOT_DAY_FORMAT =
      DateTimeFormatter.ofPattern("EEEE", Locale.ROOT);

  private static final DateTimeFormatter ROOT_MONTH_FORMAT =
      DateTimeFormatter.ofPattern("MMMM", Locale.ROOT);

  private static final Soundex SOUNDEX = new Soundex();

  private static final int SOUNDEX_LENGTH = 4;

  private static final Pattern FROM_BASE64_REGEXP = Pattern.compile("[\\t\\n\\r\\s]");

  private static final Function1<List<Object>, Enumerable<Object>> LIST_AS_ENUMERABLE =
      Linq4j::asEnumerable;

  // It's important to have XDigit before Digit to match XDigit first
  // (i.e. see the posixRegex method)
  private static final String[] POSIX_CHARACTER_CLASSES = new String[] { "Lower", "Upper", "ASCII",
      "Alpha", "XDigit", "Digit", "Alnum", "Punct", "Graph", "Print", "Blank", "Cntrl", "Space" };

  private static final Function1<Object[], Enumerable<Object[]>> ARRAY_CARTESIAN_PRODUCT =
      lists -> {
        final List<Enumerator<Object>> enumerators = new ArrayList<>();
        for (Object list : lists) {
          enumerators.add(Linq4j.enumerator((List) list));
        }
        final Enumerator<List<Object>> product = Linq4j.product(enumerators);
        return new AbstractEnumerable<Object[]>() {
          public Enumerator<Object[]> enumerator() {
            return Linq4j.transform(product, List::toArray);
          }
        };
      };

  /** Holds, for each thread, a map from sequence name to sequence current
   * value.
   *
   * <p>This is a straw man of an implementation whose main goal is to prove
   * that sequences can be parsed, validated and planned. A real application
   * will want persistent values for sequences, shared among threads. */
  private static final ThreadLocal<Map<String, AtomicLong>> THREAD_SEQUENCES =
      ThreadLocal.withInitial(HashMap::new);

  private SqlFunctions() {
  }

  /** SQL TO_BASE64(string) function. */
  public static String toBase64(String string) {
    return toBase64_(string.getBytes(UTF_8));
  }

  /** SQL TO_BASE64(string) function for binary string. */
  public static String toBase64(ByteString string) {
    return toBase64_(string.getBytes());
  }

  private static String toBase64_(byte[] bytes) {
    String base64 = Base64.getEncoder().encodeToString(bytes);
    StringBuilder str = new StringBuilder(base64.length() + base64.length() / 76);
    Splitter.fixedLength(76).split(base64).iterator().forEachRemaining(s -> {
      str.append(s);
      str.append("\n");
    });
    return str.substring(0, str.length() - 1);
  }

  /** SQL FROM_BASE64(string) function. */
  public static ByteString fromBase64(String base64) {
    try {
      base64 = FROM_BASE64_REGEXP.matcher(base64).replaceAll("");
      return new ByteString(Base64.getDecoder().decode(base64));
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  /** SQL MD5(string) function. */
  public static @Nonnull String md5(@Nonnull String string)  {
    return DigestUtils.md5Hex(string.getBytes(UTF_8));
  }

  /** SQL MD5(string) function for binary string. */
  public static @Nonnull String md5(@Nonnull ByteString string)  {
    return DigestUtils.md5Hex(string.getBytes());
  }

  /** SQL SHA1(string) function. */
  public static @Nonnull String sha1(@Nonnull String string)  {
    return DigestUtils.sha1Hex(string.getBytes(UTF_8));
  }

  /** SQL SHA1(string) function for binary string. */
  public static @Nonnull String sha1(@Nonnull ByteString string)  {
    return DigestUtils.sha1Hex(string.getBytes());
  }

  /** SQL {@code REGEXP_REPLACE} function with 3 arguments. */
  public static String regexpReplace(String s, String regex,
      String replacement) {
    return regexpReplace(s, regex, replacement, 1, 0, null);
  }

  /** SQL {@code REGEXP_REPLACE} function with 4 arguments. */
  public static String regexpReplace(String s, String regex, String replacement,
      int pos) {
    return regexpReplace(s, regex, replacement, pos, 0, null);
  }

  /** SQL {@code REGEXP_REPLACE} function with 5 arguments. */
  public static String regexpReplace(String s, String regex, String replacement,
      int pos, int occurrence) {
    return regexpReplace(s, regex, replacement, pos, occurrence, null);
  }

  /** SQL {@code REGEXP_REPLACE} function with 6 arguments. */
  public static String regexpReplace(String s, String regex, String replacement,
      int pos, int occurrence, String matchType) {
    if (pos < 1 || pos > s.length()) {
      throw RESOURCE.invalidInputForRegexpReplace(Integer.toString(pos)).ex();
    }

    final int flags = makeRegexpFlags(matchType);
    final Pattern pattern = Pattern.compile(regex, flags);

    return Unsafe.regexpReplace(s, pattern, replacement, pos, occurrence);
  }

  private static int makeRegexpFlags(String stringFlags) {
    int flags = 0;
    if (stringFlags != null) {
      for (int i = 0; i < stringFlags.length(); ++i) {
        switch (stringFlags.charAt(i)) {
        case 'i':
          flags |= Pattern.CASE_INSENSITIVE;
          break;
        case 'c':
          flags &= ~Pattern.CASE_INSENSITIVE;
          break;
        case 'n':
          flags |= Pattern.DOTALL;
          break;
        case 'm':
          flags |= Pattern.MULTILINE;
          break;
        default:
          throw RESOURCE.invalidInputForRegexpReplace(stringFlags).ex();
        }
      }
    }
    return flags;
  }

  /** SQL SUBSTRING(string FROM ... FOR ...) function. */
  public static String substring(String c, int s, int l) {
    int lc = c.length();
    if (s < 0) {
      s += lc + 1;
    }
    int e = s + l;
    if (e < s) {
      throw RESOURCE.illegalNegativeSubstringLength().ex();
    }
    if (s > lc || e < 1) {
      return "";
    }
    int s1 = Math.max(s, 1);
    int e1 = Math.min(e, lc + 1);
    return c.substring(s1 - 1, e1 - 1);
  }

  /** SQL SUBSTRING(string FROM ...) function. */
  public static String substring(String c, int s) {
    return substring(c, s, c.length() + 1);
  }

  /** SQL SUBSTRING(binary FROM ... FOR ...) function. */
  public static ByteString substring(ByteString c, int s, int l) {
    int lc = c.length();
    if (s < 0) {
      s += lc + 1;
    }
    int e = s + l;
    if (e < s) {
      throw RESOURCE.illegalNegativeSubstringLength().ex();
    }
    if (s > lc || e < 1) {
      return ByteString.EMPTY;
    }
    int s1 = Math.max(s, 1);
    int e1 = Math.min(e, lc + 1);
    return c.substring(s1 - 1, e1 - 1);
  }

  /** SQL SUBSTRING(binary FROM ...) function. */
  public static ByteString substring(ByteString c, int s) {
    return substring(c, s, c.length() + 1);
  }

  /** SQL UPPER(string) function. */
  public static String upper(String s) {
    return s.toUpperCase(Locale.ROOT);
  }

  /** SQL LOWER(string) function. */
  public static String lower(String s) {
    return s.toLowerCase(Locale.ROOT);
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

  /** SQL REVERSE(string) function. */
  public static String reverse(String s) {
    final StringBuilder buf = new StringBuilder(s);
    return buf.reverse().toString();
  }

  /** SQL ASCII(string) function. */
  public static int ascii(String s) {
    return s.isEmpty()
        ? 0 : s.codePointAt(0);
  }

  /** SQL REPEAT(string, int) function. */
  public static String repeat(String s, int n) {
    if (n < 1) {
      return "";
    }
    return Strings.repeat(s, n);
  }

  /** SQL SPACE(int) function. */
  public static String space(int n) {
    return repeat(" ", n);
  }

  /** SQL SOUNDEX(string) function. */
  public static String soundex(String s) {
    return SOUNDEX.soundex(s);
  }

  /** SQL DIFFERENCE(string, string) function. */
  public static int difference(String s0, String s1) {
    String result0 = soundex(s0);
    String result1 = soundex(s1);
    for (int i = 0; i < SOUNDEX_LENGTH; i++) {
      if (result0.charAt(i) != result1.charAt(i)) {
        return i;
      }
    }
    return SOUNDEX_LENGTH;
  }

  /** SQL LEFT(string, integer) function. */
  public static @Nonnull String left(@Nonnull String s, int n) {
    if (n <= 0) {
      return "";
    }
    int len = s.length();
    if (n >= len) {
      return s;
    }
    return s.substring(0, n);
  }

  /** SQL LEFT(ByteString, integer) function. */
  public static @Nonnull ByteString left(@Nonnull ByteString s, int n) {
    if (n <= 0) {
      return ByteString.EMPTY;
    }
    int len = s.length();
    if (n >= len) {
      return s;
    }
    return s.substring(0, n);
  }

  /** SQL RIGHT(string, integer) function. */
  public static @Nonnull String right(@Nonnull String s, int n) {
    if (n <= 0) {
      return "";
    }
    int len = s.length();
    if (n >= len) {
      return s;
    }
    return s.substring(len - n);
  }

  /** SQL RIGHT(ByteString, integer) function. */
  public static @Nonnull ByteString right(@Nonnull ByteString s, int n) {
    if (n <= 0) {
      return ByteString.EMPTY;
    }
    final int len = s.length();
    if (n >= len) {
      return s;
    }
    return s.substring(len - n);
  }

  /** SQL CHR(long) function. */
  public static String chr(long n) {
    return String.valueOf(Character.toChars((int) n));
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
    return trim(false, true, " ", s);
  }

  /** SQL {@code LTRIM} function. */
  public static String ltrim(String s) {
    return trim(true, false, " ", s);
  }

  /** SQL {@code TRIM(... seek FROM s)} function. */
  public static String trim(boolean left, boolean right, String seek,
      String s) {
    return trim(left, right, seek, s, true);
  }

  public static String trim(boolean left, boolean right, String seek,
      String s, boolean strict) {
    if (strict && seek.length() != 1) {
      throw RESOURCE.trimError().ex();
    }
    int j = s.length();
    if (right) {
      for (;;) {
        if (j == 0) {
          return "";
        }
        if (seek.indexOf(s.charAt(j - 1)) < 0) {
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
        if (seek.indexOf(s.charAt(i)) < 0) {
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
    return s.substring(0, start - 1)
        + r
        + s.substring(start - 1 + r.length());
  }

  /** SQL {@code OVERLAY} function. */
  public static String overlay(String s, String r, int start, int length) {
    return s.substring(0, start - 1)
        + r
        + s.substring(start - 1 + length);
  }

  /** SQL {@code OVERLAY} function applied to binary strings. */
  public static ByteString overlay(ByteString s, ByteString r, int start) {
    return s.substring(0, start - 1)
        .concat(r)
        .concat(s.substring(start - 1 + r.length()));
  }

  /** SQL {@code OVERLAY} function applied to binary strings. */
  public static ByteString overlay(ByteString s, ByteString r, int start,
      int length) {
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

  public static boolean posixRegex(String s, String regex, Boolean caseSensitive) {
    // Replace existing character classes with java equivalent ones
    String originalRegex = regex;
    String[] existingExpressions = Arrays.stream(POSIX_CHARACTER_CLASSES)
        .filter(v -> originalRegex.contains(v.toLowerCase(Locale.ROOT))).toArray(String[]::new);
    for (String v : existingExpressions) {
      regex = regex.replaceAll(v.toLowerCase(Locale.ROOT), "\\\\p{" + v + "}");
    }

    int flags = caseSensitive ? 0 : Pattern.CASE_INSENSITIVE;
    return Pattern.compile(regex, flags).matcher(s).find();
  }

  // =

  /** SQL <code>=</code> operator applied to BigDecimal values (neither may be
   * null). */
  public static boolean eq(BigDecimal b0, BigDecimal b1) {
    return b0.stripTrailingZeros().equals(b1.stripTrailingZeros());
  }

  /** SQL <code>=</code> operator applied to Object values (including String;
   * neither side may be null). */
  public static boolean eq(Object b0, Object b1) {
    return b0.equals(b1);
  }

  /** SQL <code>=</code> operator applied to Object values (at least one operand
   * has ANY type; neither may be null). */
  public static boolean eqAny(Object b0, Object b1) {
    if (b0.getClass().equals(b1.getClass())) {
      // The result of SqlFunctions.eq(BigDecimal, BigDecimal) makes more sense
      // than BigDecimal.equals(BigDecimal). So if both of types are BigDecimal,
      // we just use SqlFunctions.eq(BigDecimal, BigDecimal).
      if (BigDecimal.class.isInstance(b0)) {
        return eq((BigDecimal) b0, (BigDecimal) b1);
      } else {
        return b0.equals(b1);
      }
    } else if (allAssignable(Number.class, b0, b1)) {
      return eq(toBigDecimal((Number) b0), toBigDecimal((Number) b1));
    }
    // We shouldn't rely on implementation even though overridden equals can
    // handle other types which may create worse result: for example,
    // a.equals(b) != b.equals(a)
    return false;
  }

  /** Returns whether two objects can both be assigned to a given class. */
  private static boolean allAssignable(Class clazz, Object o0, Object o1) {
    return clazz.isInstance(o0) && clazz.isInstance(o1);
  }

  // <>

  /** SQL <code>&lt;gt;</code> operator applied to BigDecimal values. */
  public static boolean ne(BigDecimal b0, BigDecimal b1) {
    return b0.compareTo(b1) != 0;
  }

  /** SQL <code>&lt;gt;</code> operator applied to Object values (including
   * String; neither side may be null). */
  public static boolean ne(Object b0, Object b1) {
    return !eq(b0, b1);
  }

  /** SQL <code>&lt;gt;</code> operator applied to Object values (at least one
   *  operand has ANY type, including String; neither may be null). */
  public static boolean neAny(Object b0, Object b1) {
    return !eqAny(b0, b1);
  }

  // <

  /** SQL <code>&lt;</code> operator applied to boolean values. */
  public static boolean lt(boolean b0, boolean b1) {
    return compare(b0, b1) < 0;
  }

  /** SQL <code>&lt;</code> operator applied to String values. */
  public static boolean lt(String b0, String b1) {
    return b0.compareTo(b1) < 0;
  }

  /** SQL <code>&lt;</code> operator applied to ByteString values. */
  public static boolean lt(ByteString b0, ByteString b1) {
    return b0.compareTo(b1) < 0;
  }

  /** SQL <code>&lt;</code> operator applied to BigDecimal values. */
  public static boolean lt(BigDecimal b0, BigDecimal b1) {
    return b0.compareTo(b1) < 0;
  }

  /** SQL <code>&lt;</code> operator applied to Object values. */
  public static boolean ltAny(Object b0, Object b1) {
    if (b0.getClass().equals(b1.getClass())
        && b0 instanceof Comparable) {
      //noinspection unchecked
      return ((Comparable) b0).compareTo(b1) < 0;
    } else if (allAssignable(Number.class, b0, b1)) {
      return lt(toBigDecimal((Number) b0), toBigDecimal((Number) b1));
    }

    throw notComparable("<", b0, b1);
  }

  // <=

  /** SQL <code>&le;</code> operator applied to boolean values. */
  public static boolean le(boolean b0, boolean b1) {
    return compare(b0, b1) <= 0;
  }

  /** SQL <code>&le;</code> operator applied to String values. */
  public static boolean le(String b0, String b1) {
    return b0.compareTo(b1) <= 0;
  }

  /** SQL <code>&le;</code> operator applied to ByteString values. */
  public static boolean le(ByteString b0, ByteString b1) {
    return b0.compareTo(b1) <= 0;
  }

  /** SQL <code>&le;</code> operator applied to BigDecimal values. */
  public static boolean le(BigDecimal b0, BigDecimal b1) {
    return b0.compareTo(b1) <= 0;
  }

  /** SQL <code>&le;</code> operator applied to Object values (at least one
   * operand has ANY type; neither may be null). */
  public static boolean leAny(Object b0, Object b1) {
    if (b0.getClass().equals(b1.getClass())
        && b0 instanceof Comparable) {
      //noinspection unchecked
      return ((Comparable) b0).compareTo(b1) <= 0;
    } else if (allAssignable(Number.class, b0, b1)) {
      return le(toBigDecimal((Number) b0), toBigDecimal((Number) b1));
    }

    throw notComparable("<=", b0, b1);
  }

  // >

  /** SQL <code>&gt;</code> operator applied to boolean values. */
  public static boolean gt(boolean b0, boolean b1) {
    return compare(b0, b1) > 0;
  }

  /** SQL <code>&gt;</code> operator applied to String values. */
  public static boolean gt(String b0, String b1) {
    return b0.compareTo(b1) > 0;
  }

  /** SQL <code>&gt;</code> operator applied to ByteString values. */
  public static boolean gt(ByteString b0, ByteString b1) {
    return b0.compareTo(b1) > 0;
  }

  /** SQL <code>&gt;</code> operator applied to BigDecimal values. */
  public static boolean gt(BigDecimal b0, BigDecimal b1) {
    return b0.compareTo(b1) > 0;
  }

  /** SQL <code>&gt;</code> operator applied to Object values (at least one
   * operand has ANY type; neither may be null). */
  public static boolean gtAny(Object b0, Object b1) {
    if (b0.getClass().equals(b1.getClass())
        && b0 instanceof Comparable) {
      //noinspection unchecked
      return ((Comparable) b0).compareTo(b1) > 0;
    } else if (allAssignable(Number.class, b0, b1)) {
      return gt(toBigDecimal((Number) b0), toBigDecimal((Number) b1));
    }

    throw notComparable(">", b0, b1);
  }

  // >=

  /** SQL <code>&ge;</code> operator applied to boolean values. */
  public static boolean ge(boolean b0, boolean b1) {
    return compare(b0, b1) >= 0;
  }

  /** SQL <code>&ge;</code> operator applied to String values. */
  public static boolean ge(String b0, String b1) {
    return b0.compareTo(b1) >= 0;
  }

  /** SQL <code>&ge;</code> operator applied to ByteString values. */
  public static boolean ge(ByteString b0, ByteString b1) {
    return b0.compareTo(b1) >= 0;
  }

  /** SQL <code>&ge;</code> operator applied to BigDecimal values. */
  public static boolean ge(BigDecimal b0, BigDecimal b1) {
    return b0.compareTo(b1) >= 0;
  }

  /** SQL <code>&ge;</code> operator applied to Object values (at least one
   * operand has ANY type; neither may be null). */
  public static boolean geAny(Object b0, Object b1) {
    if (b0.getClass().equals(b1.getClass())
        && b0 instanceof Comparable) {
      //noinspection unchecked
      return ((Comparable) b0).compareTo(b1) >= 0;
    } else if (allAssignable(Number.class, b0, b1)) {
      return ge(toBigDecimal((Number) b0), toBigDecimal((Number) b1));
    }

    throw notComparable(">=", b0, b1);
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

  /** SQL <code>+</code> operator applied to Object values (at least one operand
   * has ANY type; either may be null). */
  public static Object plusAny(Object b0, Object b1) {
    if (b0 == null || b1 == null) {
      return null;
    }

    if (allAssignable(Number.class, b0, b1)) {
      return plus(toBigDecimal((Number) b0), toBigDecimal((Number) b1));
    }

    throw notArithmetic("+", b0, b1);
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

  /** SQL <code>-</code> operator applied to Object values (at least one operand
   * has ANY type; either may be null). */
  public static Object minusAny(Object b0, Object b1) {
    if (b0 == null || b1 == null) {
      return null;
    }

    if (allAssignable(Number.class, b0, b1)) {
      return minus(toBigDecimal((Number) b0), toBigDecimal((Number) b1));
    }

    throw notArithmetic("-", b0, b1);
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

  /** SQL <code>/</code> operator applied to Object values (at least one operand
   * has ANY type; either may be null). */
  public static Object divideAny(Object b0, Object b1) {
    if (b0 == null || b1 == null) {
      return null;
    }

    if (allAssignable(Number.class, b0, b1)) {
      return divide(toBigDecimal((Number) b0), toBigDecimal((Number) b1));
    }

    throw notArithmetic("/", b0, b1);
  }

  public static int divide(int b0, BigDecimal b1) {
    return BigDecimal.valueOf(b0)
        .divide(b1, RoundingMode.HALF_DOWN).intValue();
  }

  public static long divide(long b0, BigDecimal b1) {
    return BigDecimal.valueOf(b0)
        .divide(b1, RoundingMode.HALF_DOWN).longValue();
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

  /** SQL <code>*</code> operator applied to Object values (at least one operand
   * has ANY type; either may be null). */
  public static Object multiplyAny(Object b0, Object b1) {
    if (b0 == null || b1 == null) {
      return null;
    }

    if (allAssignable(Number.class, b0, b1)) {
      return multiply(toBigDecimal((Number) b0), toBigDecimal((Number) b1));
    }

    throw notArithmetic("*", b0, b1);
  }

  private static RuntimeException notArithmetic(String op, Object b0,
      Object b1) {
    return RESOURCE.invalidTypesForArithmetic(b0.getClass().toString(),
        op, b1.getClass().toString()).ex();
  }

  private static RuntimeException notComparable(String op, Object b0,
      Object b1) {
    return RESOURCE.invalidTypesForComparison(b0.getClass().toString(),
        op, b1.getClass().toString()).ex();
  }

  // &

  /** Helper function for implementing <code>BIT_AND</code> */
  public static long bitAnd(long b0, long b1) {
    return b0 & b1;
  }

  // |

  /** Helper function for implementing <code>BIT_OR</code> */
  public static long bitOr(long b0, long b1) {
    return b0 | b1;
  }

  // EXP

  /** SQL <code>EXP</code> operator applied to double values. */
  public static double exp(double b0) {
    return Math.exp(b0);
  }

  public static double exp(BigDecimal b0) {
    return Math.exp(b0.doubleValue());
  }

  // POWER

  /** SQL <code>POWER</code> operator applied to double values. */
  public static double power(double b0, double b1) {
    return Math.pow(b0, b1);
  }

  public static double power(double b0, BigDecimal b1) {
    return Math.pow(b0, b1.doubleValue());
  }

  public static double power(BigDecimal b0, double b1) {
    return Math.pow(b0.doubleValue(), b1);
  }

  public static double power(BigDecimal b0, BigDecimal b1) {
    return Math.pow(b0.doubleValue(), b1.doubleValue());
  }

  // LN

  /** SQL {@code LN(number)} function applied to double values. */
  public static double ln(double d) {
    return Math.log(d);
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
    return b0.setScale(0, RoundingMode.FLOOR);
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
    return b0.setScale(0, RoundingMode.CEILING);
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

  // ACOS
  /** SQL <code>ACOS</code> operator applied to BigDecimal values. */
  public static double acos(BigDecimal b0) {
    return Math.acos(b0.doubleValue());
  }

  /** SQL <code>ACOS</code> operator applied to double values. */
  public static double acos(double b0) {
    return Math.acos(b0);
  }

  // ASIN
  /** SQL <code>ASIN</code> operator applied to BigDecimal values. */
  public static double asin(BigDecimal b0) {
    return Math.asin(b0.doubleValue());
  }

  /** SQL <code>ASIN</code> operator applied to double values. */
  public static double asin(double b0) {
    return Math.asin(b0);
  }

  // ATAN
  /** SQL <code>ATAN</code> operator applied to BigDecimal values. */
  public static double atan(BigDecimal b0) {
    return Math.atan(b0.doubleValue());
  }

  /** SQL <code>ATAN</code> operator applied to double values. */
  public static double atan(double b0) {
    return Math.atan(b0);
  }

  // ATAN2
  /** SQL <code>ATAN2</code> operator applied to double/BigDecimal values. */
  public static double atan2(double b0, BigDecimal b1) {
    return Math.atan2(b0, b1.doubleValue());
  }

  /** SQL <code>ATAN2</code> operator applied to BigDecimal/double values. */
  public static double atan2(BigDecimal b0, double b1) {
    return Math.atan2(b0.doubleValue(), b1);
  }

  /** SQL <code>ATAN2</code> operator applied to BigDecimal values. */
  public static double atan2(BigDecimal b0, BigDecimal b1) {
    return Math.atan2(b0.doubleValue(), b1.doubleValue());
  }

  /** SQL <code>ATAN2</code> operator applied to double values. */
  public static double atan2(double b0, double b1) {
    return Math.atan2(b0, b1);
  }

  // COS
  /** SQL <code>COS</code> operator applied to BigDecimal values. */
  public static double cos(BigDecimal b0) {
    return Math.cos(b0.doubleValue());
  }

  /** SQL <code>COS</code> operator applied to double values. */
  public static double cos(double b0) {
    return Math.cos(b0);
  }

  // COT
  /** SQL <code>COT</code> operator applied to BigDecimal values. */
  public static double cot(BigDecimal b0) {
    return 1.0d / Math.tan(b0.doubleValue());
  }

  /** SQL <code>COT</code> operator applied to double values. */
  public static double cot(double b0) {
    return 1.0d / Math.tan(b0);
  }

  // DEGREES
  /** SQL <code>DEGREES</code> operator applied to BigDecimal values. */
  public static double degrees(BigDecimal b0) {
    return Math.toDegrees(b0.doubleValue());
  }

  /** SQL <code>DEGREES</code> operator applied to double values. */
  public static double degrees(double b0) {
    return Math.toDegrees(b0);
  }

  // RADIANS
  /** SQL <code>RADIANS</code> operator applied to BigDecimal values. */
  public static double radians(BigDecimal b0) {
    return Math.toRadians(b0.doubleValue());
  }

  /** SQL <code>RADIANS</code> operator applied to double values. */
  public static double radians(double b0) {
    return Math.toRadians(b0);
  }

  // SQL ROUND
  /** SQL <code>ROUND</code> operator applied to int values. */
  public static int sround(int b0) {
    return sround(b0, 0);
  }

  /** SQL <code>ROUND</code> operator applied to int values. */
  public static int sround(int b0, int b1) {
    return sround(BigDecimal.valueOf(b0), b1).intValue();
  }

  /** SQL <code>ROUND</code> operator applied to long values. */
  public static long sround(long b0) {
    return sround(b0, 0);
  }

  /** SQL <code>ROUND</code> operator applied to long values. */
  public static long sround(long b0, int b1) {
    return sround(BigDecimal.valueOf(b0), b1).longValue();
  }

  /** SQL <code>ROUND</code> operator applied to BigDecimal values. */
  public static BigDecimal sround(BigDecimal b0) {
    return sround(b0, 0);
  }

  /** SQL <code>ROUND</code> operator applied to BigDecimal values. */
  public static BigDecimal sround(BigDecimal b0, int b1) {
    return b0.movePointRight(b1)
        .setScale(0, RoundingMode.HALF_UP).movePointLeft(b1);
  }

  /** SQL <code>ROUND</code> operator applied to double values. */
  public static double sround(double b0) {
    return sround(b0, 0);
  }

  /** SQL <code>ROUND</code> operator applied to double values. */
  public static double sround(double b0, int b1) {
    return sround(BigDecimal.valueOf(b0), b1).doubleValue();
  }

  // SQL TRUNCATE
  /** SQL <code>TRUNCATE</code> operator applied to int values. */
  public static int struncate(int b0) {
    return struncate(b0, 0);
  }

  public static int struncate(int b0, int b1) {
    return struncate(BigDecimal.valueOf(b0), b1).intValue();
  }

  /** SQL <code>TRUNCATE</code> operator applied to long values. */
  public static long struncate(long b0) {
    return struncate(b0, 0);
  }

  public static long struncate(long b0, int b1) {
    return struncate(BigDecimal.valueOf(b0), b1).longValue();
  }

  /** SQL <code>TRUNCATE</code> operator applied to BigDecimal values. */
  public static BigDecimal struncate(BigDecimal b0) {
    return struncate(b0, 0);
  }

  public static BigDecimal struncate(BigDecimal b0, int b1) {
    return b0.movePointRight(b1)
        .setScale(0, RoundingMode.DOWN).movePointLeft(b1);
  }

  /** SQL <code>TRUNCATE</code> operator applied to double values. */
  public static double struncate(double b0) {
    return struncate(b0, 0);
  }

  public static double struncate(double b0, int b1) {
    return struncate(BigDecimal.valueOf(b0), b1).doubleValue();
  }

  // SIGN
  /** SQL <code>SIGN</code> operator applied to int values. */
  public static int sign(int b0) {
    return Integer.signum(b0);
  }

  /** SQL <code>SIGN</code> operator applied to long values. */
  public static long sign(long b0) {
    return Long.signum(b0);
  }

  /** SQL <code>SIGN</code> operator applied to BigDecimal values. */
  public static BigDecimal sign(BigDecimal b0) {
    return BigDecimal.valueOf(b0.signum());
  }

  /** SQL <code>SIGN</code> operator applied to double values. */
  public static double sign(double b0) {
    return Math.signum(b0);
  }

  // SIN
  /** SQL <code>SIN</code> operator applied to BigDecimal values. */
  public static double sin(BigDecimal b0) {
    return Math.sin(b0.doubleValue());
  }

  /** SQL <code>SIN</code> operator applied to double values. */
  public static double sin(double b0) {
    return Math.sin(b0);
  }

  // TAN
  /** SQL <code>TAN</code> operator applied to BigDecimal values. */
  public static double tan(BigDecimal b0) {
    return Math.tan(b0.doubleValue());
  }

  /** SQL <code>TAN</code> operator applied to double values. */
  public static double tan(double b0) {
    return Math.tan(b0);
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
    throw RESOURCE.cannotConvert(o.toString(), toType.toString()).ex();
  }

  /** CAST(VARCHAR AS BOOLEAN). */
  public static boolean toBoolean(String s) {
    s = trim(true, true, " ", s);
    if (s.equalsIgnoreCase("TRUE")) {
      return true;
    } else if (s.equalsIgnoreCase("FALSE")) {
      return false;
    } else {
      throw RESOURCE.invalidCharacterForCast(s).ex();
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
        : o instanceof java.util.Date ? toInt((java.util.Date) o)
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

  public static Integer toTimeWithLocalTimeZone(String v) {
    return v == null ? null : new TimeWithTimeZoneString(v)
        .withTimeZone(DateTimeUtils.UTC_ZONE)
        .getLocalTimeString()
        .getMillisOfDay();
  }

  public static Integer toTimeWithLocalTimeZone(String v, TimeZone timeZone) {
    return v == null ? null : new TimeWithTimeZoneString(v + " " + timeZone.getID())
        .withTimeZone(DateTimeUtils.UTC_ZONE)
        .getLocalTimeString()
        .getMillisOfDay();
  }

  public static int timeWithLocalTimeZoneToTime(int v, TimeZone timeZone) {
    return TimeWithTimeZoneString.fromMillisOfDay(v)
        .withTimeZone(timeZone)
        .getLocalTimeString()
        .getMillisOfDay();
  }

  public static long timeWithLocalTimeZoneToTimestamp(String date, int v, TimeZone timeZone) {
    final TimeWithTimeZoneString tTZ = TimeWithTimeZoneString.fromMillisOfDay(v)
        .withTimeZone(DateTimeUtils.UTC_ZONE);
    return new TimestampWithTimeZoneString(date + " " + tTZ.toString())
        .withTimeZone(timeZone)
        .getLocalTimestampString()
        .getMillisSinceEpoch();
  }

  public static long timeWithLocalTimeZoneToTimestampWithLocalTimeZone(String date, int v) {
    final TimeWithTimeZoneString tTZ = TimeWithTimeZoneString.fromMillisOfDay(v)
        .withTimeZone(DateTimeUtils.UTC_ZONE);
    return new TimestampWithTimeZoneString(date + " " + tTZ.toString())
        .getLocalTimestampString()
        .getMillisSinceEpoch();
  }

  public static String timeWithLocalTimeZoneToString(int v, TimeZone timeZone) {
    return TimeWithTimeZoneString.fromMillisOfDay(v)
        .withTimeZone(timeZone)
        .toString();
  }

  /** Converts the internal representation of a SQL TIMESTAMP (long) to the Java
   * type used for UDF parameters ({@link java.sql.Timestamp}). */
  public static java.sql.Timestamp internalToTimestamp(long v) {
    return new java.sql.Timestamp(v - LOCAL_TZ.getOffset(v));
  }

  public static java.sql.Timestamp internalToTimestamp(Long v) {
    return v == null ? null : internalToTimestamp(v.longValue());
  }

  public static int timestampWithLocalTimeZoneToDate(long v, TimeZone timeZone) {
    return TimestampWithTimeZoneString.fromMillisSinceEpoch(v)
        .withTimeZone(timeZone)
        .getLocalDateString()
        .getDaysSinceEpoch();
  }

  public static int timestampWithLocalTimeZoneToTime(long v, TimeZone timeZone) {
    return TimestampWithTimeZoneString.fromMillisSinceEpoch(v)
        .withTimeZone(timeZone)
        .getLocalTimeString()
        .getMillisOfDay();
  }

  public static long timestampWithLocalTimeZoneToTimestamp(long v, TimeZone timeZone) {
    return TimestampWithTimeZoneString.fromMillisSinceEpoch(v)
        .withTimeZone(timeZone)
        .getLocalTimestampString()
        .getMillisSinceEpoch();
  }

  public static String timestampWithLocalTimeZoneToString(long v, TimeZone timeZone) {
    return TimestampWithTimeZoneString.fromMillisSinceEpoch(v)
        .withTimeZone(timeZone)
        .toString();
  }

  public static int timestampWithLocalTimeZoneToTimeWithLocalTimeZone(long v) {
    return TimestampWithTimeZoneString.fromMillisSinceEpoch(v)
        .getLocalTimeString()
        .getMillisOfDay();
  }

  public static Long toTimestampWithLocalTimeZone(String v) {
    return v == null ? null : new TimestampWithTimeZoneString(v)
        .withTimeZone(DateTimeUtils.UTC_ZONE)
        .getLocalTimestampString()
        .getMillisSinceEpoch();
  }

  public static Long toTimestampWithLocalTimeZone(String v, TimeZone timeZone) {
    return v == null ? null : new TimestampWithTimeZoneString(v + " " + timeZone.getID())
        .withTimeZone(DateTimeUtils.UTC_ZONE)
        .getLocalTimestampString()
        .getMillisSinceEpoch();
  }

  // Don't need shortValueOf etc. - Short.valueOf is sufficient.

  /** Helper for CAST(... AS VARCHAR(maxLength)). */
  public static String truncate(String s, int maxLength) {
    if (s == null) {
      return null;
    } else if (s.length() > maxLength) {
      return s.substring(0, maxLength);
    } else {
      return s;
    }
  }

  /** Helper for CAST(... AS CHAR(maxLength)). */
  public static String truncateOrPad(String s, int maxLength) {
    if (s == null) {
      return null;
    } else {
      final int length = s.length();
      if (length > maxLength) {
        return s.substring(0, maxLength);
      } else {
        return length < maxLength ? Spaces.padRight(s, maxLength) : s;
      }
    }
  }

  /** Helper for CAST(... AS VARBINARY(maxLength)). */
  public static ByteString truncate(ByteString s, int maxLength) {
    if (s == null) {
      return null;
    } else if (s.length() > maxLength) {
      return s.substring(0, maxLength);
    } else {
      return s;
    }
  }

  /** Helper for CAST(... AS BINARY(maxLength)). */
  public static ByteString truncateOrPad(ByteString s, int maxLength) {
    if (s == null) {
      return null;
    } else {
      final int length = s.length();
      if (length > maxLength) {
        return s.substring(0, maxLength);
      } else if (length < maxLength) {
        return s.concat(new ByteString(new byte[maxLength - length]));
      } else {
        return s;
      }
    }
  }

  /** SQL {@code POSITION(seek IN string)} function. */
  public static int position(String seek, String s) {
    return s.indexOf(seek) + 1;
  }

  /** SQL {@code POSITION(seek IN string)} function for byte strings. */
  public static int position(ByteString seek, ByteString s) {
    return s.indexOf(seek) + 1;
  }

  /** SQL {@code POSITION(seek IN string FROM integer)} function. */
  public static int position(String seek, String s, int from) {
    final int from0 = from - 1; // 0-based
    if (from0 > s.length() || from0 < 0) {
      return 0;
    }

    return s.indexOf(seek, from0) + 1;
  }

  /** SQL {@code POSITION(seek IN string FROM integer)} function for byte
   * strings. */
  public static int position(ByteString seek, ByteString s, int from) {
    final int from0 = from - 1;
    if (from0 > s.length() || from0 < 0) {
      return 0;
    }

    // ByteString doesn't have indexOf(ByteString, int) until avatica-1.9
    // (see [CALCITE-1423]), so apply substring and find from there.
    Bug.upgrade("in avatica-1.9, use ByteString.substring(ByteString, int)");
    final int p = s.substring(from0).indexOf(seek);
    if (p < 0) {
      return 0;
    }
    return p + from;
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

  /**
   * SQL {@code LAST_DAY} function.
   *
   * @param date days since epoch
   * @return days of the last day of the month since epoch
   */
  public static int lastDay(int date) {
    int y0 = (int) DateTimeUtils.unixDateExtract(TimeUnitRange.YEAR, date);
    int m0 = (int) DateTimeUtils.unixDateExtract(TimeUnitRange.MONTH, date);
    int last = lastDay(y0, m0);
    return DateTimeUtils.ymdToUnixDate(y0, m0, last);
  }

  /**
   * SQL {@code LAST_DAY} function.
   *
   * @param timestamp milliseconds from epoch
   * @return milliseconds of the last day of the month since epoch
   */
  public static int lastDay(long timestamp) {
    int date = (int) (timestamp / DateTimeUtils.MILLIS_PER_DAY);
    int y0 = (int) DateTimeUtils.unixDateExtract(TimeUnitRange.YEAR, date);
    int m0 = (int) DateTimeUtils.unixDateExtract(TimeUnitRange.MONTH, date);
    int last = lastDay(y0, m0);
    return DateTimeUtils.ymdToUnixDate(y0, m0, last);
  }

  /**
   * SQL {@code DAYNAME} function, applied to a TIMESTAMP argument.
   *
   * @param timestamp Milliseconds from epoch
   * @param locale Locale
   * @return Name of the weekday in the given locale
   */
  public static String dayNameWithTimestamp(long timestamp, Locale locale) {
    return timeStampToLocalDate(timestamp)
        .format(ROOT_DAY_FORMAT.withLocale(locale));
  }

  /**
   * SQL {@code DAYNAME} function, applied to a DATE argument.
   *
   * @param date Days since epoch
   * @param locale Locale
   * @return Name of the weekday in the given locale
   */
  public static String dayNameWithDate(int date, Locale locale) {
    return dateToLocalDate(date)
        .format(ROOT_DAY_FORMAT.withLocale(locale));
  }

  /**
   * SQL {@code MONTHNAME} function, applied to a TIMESTAMP argument.
   *
   * @param timestamp Milliseconds from epoch
   * @param locale Locale
   * @return Name of the month in the given locale
   */
  public static String monthNameWithTimestamp(long timestamp, Locale locale) {
    return timeStampToLocalDate(timestamp)
        .format(ROOT_MONTH_FORMAT.withLocale(locale));
  }

  /**
   * SQL {@code MONTHNAME} function, applied to a DATE argument.
   *
   * @param date Days from epoch
   * @param locale Locale
   * @return Name of the month in the given locale
   */
  public static String monthNameWithDate(int date, Locale locale) {
    return dateToLocalDate(date)
        .format(ROOT_MONTH_FORMAT.withLocale(locale));
  }

  /**
   * Converts a date (days since epoch) to a {@link LocalDate}.
   *
   * @param date days since epoch
   * @return localDate
   */
  private static LocalDate dateToLocalDate(int date) {
    int y0 = (int) DateTimeUtils.unixDateExtract(TimeUnitRange.YEAR, date);
    int m0 = (int) DateTimeUtils.unixDateExtract(TimeUnitRange.MONTH, date);
    int d0 = (int) DateTimeUtils.unixDateExtract(TimeUnitRange.DAY, date);
    return LocalDate.of(y0, m0, d0);
  }

  /**
   * Converts a timestamp (milliseconds since epoch) to a {@link LocalDate}.
   *
   * @param timestamp milliseconds from epoch
   * @return localDate
   */
  private static LocalDate timeStampToLocalDate(long timestamp) {
    int date = (int) (timestamp / DateTimeUtils.MILLIS_PER_DAY);
    return dateToLocalDate(date);
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

  @NonDeterministic
  public static TimeZone timeZone(DataContext root) {
    return (TimeZone) DataContext.Variable.TIME_ZONE.get(root);
  }

  /** SQL {@code USER} function. */
  @Deterministic
  public static String user(DataContext root) {
    return Objects.requireNonNull(DataContext.Variable.USER.get(root));
  }

  /** SQL {@code SYSTEM_USER} function. */
  @Deterministic
  public static String systemUser(DataContext root) {
    return Objects.requireNonNull(DataContext.Variable.SYSTEM_USER.get(root));
  }

  @NonDeterministic
  public static Locale locale(DataContext root) {
    return (Locale) DataContext.Variable.LOCALE.get(root);
  }

  /** SQL {@code TRANSLATE(string, search_chars, replacement_chars)}
   * function. */
  public static String translate3(String s, String search, String replacement) {
    return org.apache.commons.lang3.StringUtils.replaceChars(s, search, replacement);
  }

  /** SQL {@code REPLACE(string, search, replacement)} function. */
  public static String replace(String s, String search, String replacement) {
    return s.replace(search, replacement);
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
      return mapItem((Map) object, index);
    }
    if (object instanceof List && index instanceof Number) {
      return arrayItem((List) object, ((Number) index).intValue());
    }
    return null;
  }

  /** As {@link #arrayItem} method, but allows array to be nullable. */
  public static Object arrayItemOptional(List list, int item) {
    if (list == null) {
      return null;
    }
    return arrayItem(list, item);
  }

  /** As {@link #mapItem} method, but allows map to be nullable. */
  public static Object mapItemOptional(Map map, Object item) {
    if (map == null) {
      return null;
    }
    return mapItem(map, item);
  }

  /** As {@link #item} method, but allows object to be nullable. */
  public static Object itemOptional(Object object, Object index) {
    if (object == null) {
      return null;
    }
    return item(object, index);
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
      throw Util.toUnchecked(e);
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
    List result = new ArrayList(list.size());
    for (Object e : list) {
      result.add(structAccess(e, 0, null));
    }
    return result;
  }

  /** Support the ELEMENT function. */
  public static Object element(List list) {
    switch (list.size()) {
    case 0:
      return null;
    case 1:
      return list.get(0);
    default:
      throw RESOURCE.moreThanOneValueInList(list.toString()).ex();
    }
  }

  /** Support the MEMBER OF function. */
  public static boolean memberOf(Object object, Collection collection) {
    return collection.contains(object);
  }

  /** Support the MULTISET INTERSECT DISTINCT function. */
  public static <E> Collection<E> multisetIntersectDistinct(Collection<E> c1,
      Collection<E> c2) {
    final Set<E> result = new HashSet<>(c1);
    result.retainAll(c2);
    return new ArrayList<>(result);
  }

  /** Support the MULTISET INTERSECT ALL function. */
  public static <E> Collection<E> multisetIntersectAll(Collection<E> c1,
      Collection<E> c2) {
    final List<E> result = new ArrayList<>(c1.size());
    final List<E> c2Copy = new ArrayList<>(c2);
    for (E e : c1) {
      if (c2Copy.remove(e)) {
        result.add(e);
      }
    }
    return result;
  }

  /** Support the MULTISET EXCEPT ALL function. */
  public static <E> Collection<E> multisetExceptAll(Collection<E> c1,
      Collection<E> c2) {
    final List<E> result = new LinkedList<>(c1);
    for (E e : c2) {
      result.remove(e);
    }
    return result;
  }

  /** Support the MULTISET EXCEPT DISTINCT function. */
  public static <E> Collection<E> multisetExceptDistinct(Collection<E> c1,
      Collection<E> c2) {
    final Set<E> result = new HashSet<>(c1);
    result.removeAll(c2);
    return new ArrayList<>(result);
  }

  /** Support the IS A SET function. */
  public static boolean isASet(Collection collection) {
    if (collection instanceof Set) {
      return true;
    }
    // capacity calculation is in the same way like for new HashSet(Collection)
    // however return immediately in case of duplicates
    Set set = new HashSet(Math.max((int) (collection.size() / .75f) + 1, 16));
    for (Object e : collection) {
      if (!set.add(e)) {
        return false;
      }
    }
    return true;
  }

  /** Support the SUBMULTISET OF function. */
  public static boolean submultisetOf(Collection possibleSubMultiset,
      Collection multiset) {
    if (possibleSubMultiset.size() > multiset.size()) {
      return false;
    }
    Collection multisetLocal = new LinkedList(multiset);
    for (Object e : possibleSubMultiset) {
      if (!multisetLocal.remove(e)) {
        return false;
      }
    }
    return true;
  }

  /** Support the MULTISET UNION function. */
  public static Collection multisetUnionDistinct(Collection collection1,
      Collection collection2) {
    // capacity calculation is in the same way like for new HashSet(Collection)
    Set resultCollection =
        new HashSet(Math.max((int) ((collection1.size() + collection2.size()) / .75f) + 1, 16));
    resultCollection.addAll(collection1);
    resultCollection.addAll(collection2);
    return new ArrayList(resultCollection);
  }

  /** Support the MULTISET UNION ALL function. */
  public static Collection multisetUnionAll(Collection collection1,
      Collection collection2) {
    List resultCollection = new ArrayList(collection1.size() + collection2.size());
    resultCollection.addAll(collection1);
    resultCollection.addAll(collection2);
    return resultCollection;
  }

  public static Function1<Object, Enumerable<ComparableList<Comparable>>> flatProduct(
      final int[] fieldCounts, final boolean withOrdinality,
      final FlatProductInputType[] inputTypes) {
    if (fieldCounts.length == 1) {
      if (!withOrdinality && inputTypes[0] == FlatProductInputType.SCALAR) {
        //noinspection unchecked
        return (Function1) LIST_AS_ENUMERABLE;
      } else {
        return row -> p2(new Object[] { row }, fieldCounts, withOrdinality,
            inputTypes);
      }
    }
    return lists -> p2((Object[]) lists, fieldCounts, withOrdinality,
        inputTypes);
  }

  private static Enumerable<FlatLists.ComparableList<Comparable>> p2(
      Object[] lists, int[] fieldCounts, boolean withOrdinality,
      FlatProductInputType[] inputTypes) {
    final List<Enumerator<List<Comparable>>> enumerators = new ArrayList<>();
    int totalFieldCount = 0;
    for (int i = 0; i < lists.length; i++) {
      int fieldCount = fieldCounts[i];
      FlatProductInputType inputType = inputTypes[i];
      Object inputObject = lists[i];
      switch (inputType) {
      case SCALAR:
        @SuppressWarnings("unchecked") List<Comparable> list =
            (List<Comparable>) inputObject;
        enumerators.add(
            Linq4j.transform(
                Linq4j.enumerator(list), FlatLists::of));
        break;
      case LIST:
        @SuppressWarnings("unchecked") List<List<Comparable>> listList =
            (List<List<Comparable>>) inputObject;
        enumerators.add(Linq4j.enumerator(listList));
        break;
      case MAP:
        @SuppressWarnings("unchecked") Map<Comparable, Comparable> map =
            (Map<Comparable, Comparable>) inputObject;
        Enumerator<Entry<Comparable, Comparable>> enumerator =
            Linq4j.enumerator(map.entrySet());

        Enumerator<List<Comparable>> transformed = Linq4j.transform(enumerator,
            e -> FlatLists.of(e.getKey(), e.getValue()));
        enumerators.add(transformed);
        break;
      default:
        break;
      }
      if (fieldCount < 0) {
        ++totalFieldCount;
      } else {
        totalFieldCount += fieldCount;
      }
    }
    if (withOrdinality) {
      ++totalFieldCount;
    }
    return product(enumerators, totalFieldCount, withOrdinality);
  }

  public static Object[] array(Object... args) {
    return args;
  }

  /** Similar to {@link Linq4j#product(Iterable)} but each resulting list
   * implements {@link FlatLists.ComparableList}. */
  public static <E extends Comparable> Enumerable<FlatLists.ComparableList<E>> product(
      final List<Enumerator<List<E>>> enumerators, final int fieldCount,
      final boolean withOrdinality) {
    return new AbstractEnumerable<FlatLists.ComparableList<E>>() {
      public Enumerator<FlatLists.ComparableList<E>> enumerator() {
        return new ProductComparableListEnumerator<>(enumerators, fieldCount,
            withOrdinality);
      }
    };
  }

  /** Adds a given number of months to a timestamp, represented as the number
   * of milliseconds since the epoch. */
  public static long addMonths(long timestamp, int m) {
    final long millis =
        DateTimeUtils.floorMod(timestamp, DateTimeUtils.MILLIS_PER_DAY);
    timestamp -= millis;
    final long x =
        addMonths((int) (timestamp / DateTimeUtils.MILLIS_PER_DAY), m);
    return x * DateTimeUtils.MILLIS_PER_DAY + millis;
  }

  /** Adds a given number of months to a date, represented as the number of
   * days since the epoch. */
  public static int addMonths(int date, int m) {
    int y0 = (int) DateTimeUtils.unixDateExtract(TimeUnitRange.YEAR, date);
    int m0 = (int) DateTimeUtils.unixDateExtract(TimeUnitRange.MONTH, date);
    int d0 = (int) DateTimeUtils.unixDateExtract(TimeUnitRange.DAY, date);
    int y = m / 12;
    y0 += y;
    m0 += m - y * 12;
    int last = lastDay(y0, m0);
    if (d0 > last) {
      d0 = last;
    }
    return DateTimeUtils.ymdToUnixDate(y0, m0, d0);
  }

  private static int lastDay(int y, int m) {
    switch (m) {
    case 2:
      return y % 4 == 0
          && (y % 100 != 0
          || y % 400 == 0)
          ? 29 : 28;
    case 4:
    case 6:
    case 9:
    case 11:
      return 30;
    default:
      return 31;
    }
  }

  /** Finds the number of months between two dates, each represented as the
   * number of days since the epoch. */
  public static int subtractMonths(int date0, int date1) {
    if (date0 < date1) {
      return -subtractMonths(date1, date0);
    }
    // Start with an estimate.
    // Since no month has more than 31 days, the estimate is <= the true value.
    int m = (date0 - date1) / 31;
    for (;;) {
      int date2 = addMonths(date1, m);
      if (date2 >= date0) {
        return m;
      }
      int date3 = addMonths(date1, m + 1);
      if (date3 > date0) {
        return m;
      }
      ++m;
    }
  }

  public static int subtractMonths(long t0, long t1) {
    final long millis0 =
        DateTimeUtils.floorMod(t0, DateTimeUtils.MILLIS_PER_DAY);
    final int d0 = (int) DateTimeUtils.floorDiv(t0 - millis0,
        DateTimeUtils.MILLIS_PER_DAY);
    final long millis1 =
        DateTimeUtils.floorMod(t1, DateTimeUtils.MILLIS_PER_DAY);
    final int d1 = (int) DateTimeUtils.floorDiv(t1 - millis1,
        DateTimeUtils.MILLIS_PER_DAY);
    int x = subtractMonths(d0, d1);
    final long d2 = addMonths(d1, x);
    if (d2 == d0 && millis0 < millis1) {
      --x;
    }
    return x;
  }

  /**
   * Implements the {@code .} (field access) operator on an object
   * whose type is not known until runtime.
   *
   * <p>A struct object can be represented in various ways by the
   * runtime and depends on the
   * {@link org.apache.calcite.adapter.enumerable.JavaRowFormat}.
   */
  @Experimental
  public static Object structAccess(Object structObject, int index, String fieldName) {
    if (structObject == null) {
      return null;
    }

    if (structObject instanceof Object[]) {
      return ((Object[]) structObject)[index];
    } else if (structObject instanceof List) {
      return ((List) structObject).get(index);
    } else if (structObject instanceof Row) {
      return ((Row) structObject).getObject(index);
    } else {
      Class<?> beanClass = structObject.getClass();
      try {
        Field structField = beanClass.getDeclaredField(fieldName);
        return structField.get(structObject);
      } catch (NoSuchFieldException | IllegalAccessException ex) {
        throw RESOURCE.failedToAccessField(fieldName, beanClass.getName()).ex(ex);
      }
    }
  }

  /** Enumerates over the cartesian product of the given lists, returning
   * a comparable list for each row.
   *
   * @param <E> element type */
  private static class ProductComparableListEnumerator<E extends Comparable>
      extends CartesianProductEnumerator<List<E>, FlatLists.ComparableList<E>> {
    final E[] flatElements;
    final List<E> list;
    private final boolean withOrdinality;
    private int ordinality;

    ProductComparableListEnumerator(List<Enumerator<List<E>>> enumerators,
        int fieldCount, boolean withOrdinality) {
      super(enumerators);
      this.withOrdinality = withOrdinality;
      flatElements = (E[]) new Comparable[fieldCount];
      list = Arrays.asList(flatElements);
    }

    public FlatLists.ComparableList<E> current() {
      int i = 0;
      for (Object element : (Object[]) elements) {
        Object[] a;
        if (element.getClass().isArray()) {
          a = (Object[]) element;
        } else {
          final List list2 = (List) element;
          a = list2.toArray();
        }
        System.arraycopy(a, 0, flatElements, i, a.length);
        i += a.length;
      }
      if (withOrdinality) {
        flatElements[i] = (E) Integer.valueOf(++ordinality); // 1-based
      }
      return FlatLists.ofComparable(list);
    }
  }

  /** Type of argument passed into {@link #flatProduct}. */
  public enum FlatProductInputType {
    SCALAR, LIST, MAP
  }

}

// End SqlFunctions.java
