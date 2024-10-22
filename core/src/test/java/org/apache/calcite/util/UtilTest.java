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
package org.apache.calcite.util;
import org.apache.calcite.avatica.AvaticaUtils;
import org.apache.calcite.avatica.util.Spaces;
import org.apache.calcite.examples.RelBuilderExample;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.function.Functions;
import org.apache.calcite.linq4j.function.Parameter;
import org.apache.calcite.runtime.ConsList;
import org.apache.calcite.runtime.FlatLists;
import org.apache.calcite.runtime.Resources;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.runtime.Utilities;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.fun.SqlLibrary;
import org.apache.calcite.sql.util.IdPair;
import org.apache.calcite.sql.util.SqlBuilder;
import org.apache.calcite.sql.util.SqlString;
import org.apache.calcite.test.DiffTestCase;
import org.apache.calcite.test.Matchers;
import org.apache.calcite.test.Unsafe;
import org.apache.calcite.testlib.annotations.LocaleEnUs;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.hamcrest.Description;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.hamcrest.StringDescription;
import org.hamcrest.TypeSafeMatcher;
import org.junit.jupiter.api.Test;

import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.lang.management.MemoryType;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.MessageFormat;
import java.time.DayOfWeek;
import java.time.temporal.TemporalAccessor;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Properties;
import java.util.Random;
import java.util.RandomAccess;
import java.util.Set;
import java.util.SortedSet;
import java.util.TimeZone;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.ObjIntConsumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import static org.apache.calcite.test.Matchers.isLinux;
import static org.apache.calcite.test.Matchers.isListOf;
import static org.apache.calcite.util.ReflectUtil.isStatic;
import static org.apache.calcite.util.TestUtil.assertThatScientific;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.isA;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.hasToString;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Unit test for {@link Util} and other classes in this package.
 */
@LocaleEnUs
class UtilTest {
  @Test void testPrintEquals() {
    assertPrintEquals("\"x\"", "x", true);
  }

  @Test void testPrintEquals2() {
    assertPrintEquals("\"x\"", "x", false);
  }

  @Test void testPrintEquals3() {
    assertPrintEquals("null", null, true);
  }

  @Test void testPrintEquals4() {
    assertPrintEquals("", null, false);
  }

  @Test void testPrintEquals5() {
    assertPrintEquals("\"\\\\\\\"\\r\\n\"", "\\\"\r\n", true);
  }

  @Test void testScientificNotation() {
    BigDecimal bd;

    bd = new BigDecimal("0.0");
    TestUtil.assertEqualsVerbose(
        "0E0",
        Util.toScientificNotation(bd));
    bd = new BigDecimal("0.001234");
    TestUtil.assertEqualsVerbose(
        "1.234E-3",
        Util.toScientificNotation(bd));
    bd = new BigDecimal("0.001");
    TestUtil.assertEqualsVerbose(
        "1E-3",
        Util.toScientificNotation(bd));
    bd = new BigDecimal("-0.001");
    TestUtil.assertEqualsVerbose(
        "-1E-3",
        Util.toScientificNotation(bd));
    bd = new BigDecimal("1");
    TestUtil.assertEqualsVerbose(
        "1E0",
        Util.toScientificNotation(bd));
    bd = new BigDecimal("-1");
    TestUtil.assertEqualsVerbose(
        "-1E0",
        Util.toScientificNotation(bd));
    bd = new BigDecimal("1.0");
    TestUtil.assertEqualsVerbose(
        "1.0E0",
        Util.toScientificNotation(bd));
    bd = new BigDecimal("12345");
    TestUtil.assertEqualsVerbose(
        "1.2345E4",
        Util.toScientificNotation(bd));
    bd = new BigDecimal("12345.00");
    TestUtil.assertEqualsVerbose(
        "1.234500E4",
        Util.toScientificNotation(bd));
    bd = new BigDecimal("12345.001");
    TestUtil.assertEqualsVerbose(
        "1.2345001E4",
        Util.toScientificNotation(bd));

    // test truncate
    bd = new BigDecimal("1.23456789012345678901");
    TestUtil.assertEqualsVerbose(
        "1.2345678901234567890E0",
        Util.toScientificNotation(bd));
    bd = new BigDecimal("-1.23456789012345678901");
    TestUtil.assertEqualsVerbose(
        "-1.2345678901234567890E0",
        Util.toScientificNotation(bd));
  }

  @Test void testDoubleScientificNotation() {
    assertThatScientific("0.001234", is("0.001234E0"));
    assertThatScientific("0.001", is("0.001E0"));
    assertThatScientific("-0.001", is("-0.001E0"));
    assertThatScientific("1", is("1.0E0"));
    assertThatScientific("-1", is("-1.0E0"));
    assertThatScientific("1.0", is("1.0E0"));
    assertThatScientific("12345", is("12345.0E0"));
    assertThatScientific("12345.00", is("12345.0E0"));
    assertThatScientific("12345.001", is("12345.001E0"));

    // test truncate
    assertThatScientific("1.23456789012345678901", is("1.2345678901234567E0"));
    assertThatScientific("-1.23456789012345678901", is("-1.2345678901234567E0"));

    // special values
    assertThatScientific("Infinity", is("Infinity"));
    assertThatScientific("-Infinity", is("-Infinity"));
    assertThatScientific("NaN", is("NaN"));
    assertThatScientific("-0.0", is("-0.0E0"));
  }

  @Test void testToJavaId() throws UnsupportedEncodingException {
    assertThat(Util.toJavaId("foo", 0), is("ID$0$foo"));
    assertThat(Util.toJavaId("foo bar", 0), is("ID$0$foo_20_bar"));
    assertThat(Util.toJavaId("foo_bar", 0), is("ID$0$foo__bar"));
    assertThat(Util.toJavaId("0bar", 100), is("ID$100$_30_bar"));
    assertThat(Util.toJavaId("foo0bar", 0), is("ID$0$foo0bar"));
    assertThat(Util.toJavaId("it's a bird, it's a plane!", 0),
        is("ID$0$it_27_s_20_a_20_bird_2c__20_it_27_s_20_a_20_plane_21_"));

    // Try some funny non-ASCII charsets
    assertThat(
        Util.toJavaId("\u00f6\u00cb\u00c4\u00ca\u00ae\u00c1\u00f9\u00cb", 0),
        is("ID$0$_f6__cb__c4__ca__ae__c1__f9__cb_"));
    assertThat(Util.toJavaId("\uf6cb\uc4ca\uaec1\uf9cb", 0),
        is("ID$0$_f6cb__c4ca__aec1__f9cb_"));
    byte[] bytes1 = {3, 12, 54, 23, 33, 23, 45, 21, 127, -34, -92, -113};
    assertThat(
        Util.toJavaId(new String(bytes1, "EUC-JP"), 0), // CHECKSTYLE: IGNORE 0
        is("ID$0$_3__c_6_17__21__17__2d__15__7f__6cd9__fffd_"));
    byte[] bytes2 = {
        64, 32, 43, -45, -23, 0, 43, 54, 119, -32, -56, -34
    };
    assertThat(
        Util.toJavaId(new String(bytes1, "UTF-16"), 0), // CHECKSTYLE: IGNORE 0
        is("ID$0$_30c__3617__2117__2d15__7fde__a48f_"));
  }

  private void assertPrintEquals(
      String expect,
      String in,
      boolean nullMeansNull) {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    Util.printJavaString(pw, in, nullMeansNull);
    pw.flush();
    String out = sw.toString();
    assertThat(out, is(expect));
  }

  /**
   * Unit-test for {@link Util#tokenize(String, String)}.
   */
  @Test void testTokenize() {
    final List<String> list = new ArrayList<>();
    for (String s : Util.tokenize("abc,de,f", ",")) {
      list.add(s);
    }
    assertThat(list, hasSize(3));
    assertThat(list, hasToString("[abc, de, f]"));
  }

  /**
   * Unit-test for {@link BitString}.
   */
  @Test void testBitString() {
    // Powers of two, minimal length.
    final BitString b0 = new BitString("", 0);
    final BitString b1 = new BitString("1", 1);
    final BitString b2 = new BitString("10", 2);
    final BitString b4 = new BitString("100", 3);
    final BitString b8 = new BitString("1000", 4);
    final BitString b16 = new BitString("10000", 5);
    final BitString b32 = new BitString("100000", 6);
    final BitString b64 = new BitString("1000000", 7);
    final BitString b128 = new BitString("10000000", 8);
    final BitString b256 = new BitString("100000000", 9);

    // other strings
    final BitString b0x1 = new BitString("", 1);
    final BitString b0x12 = new BitString("", 12);

    // conversion to hex strings
    assertThat(b0.toHexString(), is(""));
    assertThat(b1.toHexString(), is("1"));
    assertThat(b2.toHexString(), is("2"));
    assertThat(b4.toHexString(), is("4"));
    assertThat(b8.toHexString(), is("8"));
    assertThat(b16.toHexString(), is("10"));
    assertThat(b32.toHexString(), is("20"));
    assertThat(b64.toHexString(), is("40"));
    assertThat(b128.toHexString(), is("80"));
    assertThat(b256.toHexString(), is("100"));
    assertThat(b0x1.toHexString(), is("0"));
    assertThat(b0x12.toHexString(), is("000"));

    // to byte array
    assertByteArray("01", "1", 1);
    assertByteArray("01", "1", 5);
    assertByteArray("01", "1", 8);
    assertByteArray("00, 01", "1", 9);
    assertByteArray("", "", 0);
    assertByteArray("00", "0", 1);
    assertByteArray("00", "0000", 2); // bit count less than string
    assertByteArray("00", "000", 5); // bit count larger than string
    assertByteArray("00", "0", 8); // precisely 1 byte
    assertByteArray("00, 00", "00", 9); // just over 1 byte

    // from hex string
    assertReversible("");
    assertReversible("1");
    assertReversible("10");
    assertReversible("100");
    assertReversible("1000");
    assertReversible("10000");
    assertReversible("100000");
    assertReversible("1000000");
    assertReversible("10000000");
    assertReversible("100000000");
    assertReversible("01");
    assertReversible("001010");
    assertReversible("000000000100");

    // from bytes
    final byte[] b255 = {(byte) 0xFF};
    assertThat(BitString.createFromBytes(b255),
        hasToString("11111111"));
    final byte[] b11 = {(byte) 0x0B};
    assertThat(BitString.createFromBytes(b11),
        hasToString("00001011"));
    final byte[] b011 = {(byte) 0x00, 0x0B};
    assertThat(BitString.createFromBytes(b011),
        hasToString("0000000000001011"));
  }

  private static void assertReversible(String s) {
    final BitString bitString = BitString.createFromBitString(s);
    assertThat(bitString.toBitString(), is(s));
    assertThat(BitString.createFromHexString(s).toHexString(), is(s));

    final BitString bitString8 =
        BitString.createFromBytes(bitString.getAsByteArray());
    assertThat(bitString8.getAsByteArray(), is(bitString.getAsByteArray()));
  }

  private void assertByteArray(
      String expected,
      String bits,
      int bitCount) {
    byte[] bytes = BitString.toByteArrayFromBitString(bits, bitCount);
    final String s = toString(bytes);
    assertThat(s, is(expected));
  }

  /**
   * Converts a byte array to a hex string like "AB, CD".
   */
  private String toString(byte[] bytes) {
    StringBuilder buf = new StringBuilder();
    for (int i = 0; i < bytes.length; i++) {
      byte b = bytes[i];
      if (i > 0) {
        buf.append(", ");
      }
      String s = Integer.toString(b, 16);
      buf.append((b < 16) ? ("0" + s) : s);
    }
    return buf.toString();
  }

  /**
   * Tests {@link org.apache.calcite.util.CastingList} and {@link Util#cast}.
   */
  @Test void testCastingList() {
    final List<Number> numberList = new ArrayList<>();
    numberList.add(1);
    numberList.add(null);
    numberList.add(2);
    List<Integer> integerList = Util.cast(numberList, Integer.class);
    assertThat(integerList, hasSize(3));
    assertThat(integerList.get(2), is(Integer.valueOf(2)));

    // Nulls are OK.
    assertNull(integerList.get(1));

    // Can update the underlying list.
    integerList.set(1, 345);
    assertThat(integerList.get(1), is(Integer.valueOf(345)));
    integerList.set(1, null);
    assertNull(integerList.get(1));

    // Can add a member of the wrong type to the underlying list.
    numberList.add(3.1415D);
    assertThat(integerList, hasSize(4));

    // Access a member which is of the wrong type.
    try {
      integerList.get(3);
      fail("expected exception");
    } catch (ClassCastException e) {
      // ok
    }
  }

  @Test void testCons() {
    final List<String> abc0 = Arrays.asList("a", "b", "c");

    final List<String> abc = ConsList.of("a", ImmutableList.of("b", "c"));
    assertThat(abc, hasSize(3));
    assertThat(abc, is(abc0));

    final List<String> bc = Lists.newArrayList("b", "c");
    final List<String> abc2 = ConsList.of("a", bc);
    assertThat(abc2, hasSize(3));
    assertThat(abc2, is(abc0));
    bc.set(0, "z");
    assertThat(abc2, is(abc0));

    final List<String> bc3 = ConsList.of("b", Collections.singletonList("c"));
    final List<String> abc3 = ConsList.of("a", bc3);
    assertThat(abc3, hasSize(3));
    assertThat(abc3, is(abc0));
    assertThat(abc3.indexOf("b"), is(1));
    assertThat(abc3.indexOf("z"), is(-1));
    assertThat(abc3.lastIndexOf("b"), is(1));
    assertThat(abc3.lastIndexOf("z"), is(-1));
    assertThat(abc3.hashCode(), is(abc0.hashCode()));

    assertThat(abc3.get(0), is("a"));
    assertThat(abc3.get(1), is("b"));
    assertThat(abc3.get(2), is("c"));
    try {
      final String z = abc3.get(3);
      fail("expected error, got " + z);
    } catch (IndexOutOfBoundsException e) {
      // ok
    }
    try {
      final String z = abc3.get(-3);
      fail("expected error, got " + z);
    } catch (IndexOutOfBoundsException e) {
      // ok
    }
    try {
      final String z = abc3.get(30);
      fail("expected error, got " + z);
    } catch (IndexOutOfBoundsException e) {
      // ok
    }

    final List<String> a = ConsList.of("a", ImmutableList.of());
    assertThat(a, hasSize(1));
    assertThat(a, isListOf("a"));
  }

  @Test void testConsPerformance() {
    final int n = 2000000;
    final int start = 10;
    List<Integer> list = makeConsList(start, n + start);
    assertThat(list, hasSize(n));
    assertThat(list, hasToString(startsWith("[10, 11, 12, ")));
    assertThat(list.contains(n / 2 + start), is(true));
    assertThat(list.contains(n * 2 + start), is(false));
    assertThat(list.indexOf(n / 2 + start), is(n / 2));
    assertThat(list.containsAll(Arrays.asList(n - 1, n - 10, n / 2, start)),
        is(true));
    long total = 0;
    for (Integer i : list) {
      total += i - start;
    }
    assertThat(total, is((long) n * (n - 1) / 2));

    final Object[] objects = list.toArray();
    assertThat(objects, arrayWithSize(n));
    final Integer[] integers = new Integer[n - 1];
    assertThat(integers, arrayWithSize(n - 1));
    final Integer[] integers2 = list.toArray(integers);
    assertThat(integers2, arrayWithSize(n));
    assertThat(integers2[0], is(start));
    assertThat(integers2[integers2.length - 1], is(n + start - 1));
    final Integer[] integers3 = list.toArray(integers2);
    assertThat(integers2, sameInstance(integers3));
    final Integer[] integers4 = new Integer[n + 1];
    final Integer[] integers5 = list.toArray(integers4);
    assertThat(integers5, sameInstance(integers4));
    assertThat(integers5, arrayWithSize(n + 1));
    assertThat(integers5[0], is(start));
    assertThat(integers5[n - 1], is(n + start - 1));
    assertThat(integers5[n], nullValue());

    assertThat(list.hashCode(), is(Arrays.hashCode(integers3)));
    assertThat(list, isListOf(integers3));
    assertThat(list, is(list));
    assertThat(Arrays.asList(integers3), is(list));
  }

  private List<Integer> makeConsList(int start, int end) {
    List<Integer> list = null;
    for (int i = end - 1; i >= start; i--) {
      if (i == end - 1) {
        list = Collections.singletonList(i);
      } else {
        list = ConsList.of(i, list);
      }
    }
    return list;
  }

  @Test void testIterableProperties() {
    Properties properties = new Properties();
    properties.put("foo", "george");
    properties.put("bar", "ringo");
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, String> entry : Util.toMap(properties).entrySet()) {
      sb.append(entry.getKey()).append("=").append(entry.getValue());
      sb.append(";");
    }
    assertThat(sb, hasToString("bar=ringo;foo=george;"));

    assertThat(Util.toMap(properties).entrySet(), hasSize(2));

    properties.put("nonString", 34);
    try {
      for (Map.Entry<String, String> e : Util.toMap(properties).entrySet()) {
        String s = e.getValue();
        Util.discard(s);
      }
      fail("expected exception");
    } catch (ClassCastException e) {
      // ok
    }
  }

  /** Tests {@link Util#printList(StringBuilder, int, ObjIntConsumer)}. */
  @Test void testPrintList() {
    final StringBuilder sb = new StringBuilder();
    Util.printList(sb, 0, (sb2, i) -> sb2.append(i * 2 + 1));
    assertThat(sb, hasToString("[]"));
    sb.setLength(0);

    Util.printList(sb, 1, (sb2, i) -> sb2.append(i * 2 + 1));
    assertThat(sb, hasToString("[1]"));
    sb.setLength(0);

    Util.printList(sb, 3, (sb2, i) -> sb2.append(i * 2 + 1));
    assertThat(sb, hasToString("[1, 3, 5]"));
    sb.setLength(0);
  }

  /** Tests {@link Util#printIterable(StringBuilder, Iterable)}. */
  @Test void testPrintIterable() {
    final StringBuilder sb = new StringBuilder();
    final Set<String> beatles =
        new LinkedHashSet<>(Arrays.asList("John", "Paul", "George", "Ringo"));
    Util.printIterable(sb, beatles);
    assertThat(sb, hasToString("[John, Paul, George, Ringo]"));
    sb.setLength(0);

    Util.printIterable(sb, ImmutableSet.of("abc"));
    assertThat(sb, hasToString("[abc]"));
    sb.setLength(0);

    Util.printIterable(sb, ImmutableList.of());
    assertThat(sb, hasToString("[]"));
    sb.setLength(0);
  }

  /**
   * Tests the difference engine, {@link DiffTestCase#diff}.
   */
  @Test void testDiffLines() {
    String[] before = {
        "Get a dose of her in jackboots and kilt",
        "She's killer-diller when she's dressed to the hilt",
        "She's the kind of a girl that makes The News of The World",
        "Yes you could say she was attractively built.",
        "Yeah yeah yeah."
    };
    String[] after = {
        "Get a dose of her in jackboots and kilt",
        "(they call her \"Polythene Pam\")",
        "She's killer-diller when she's dressed to the hilt",
        "She's the kind of a girl that makes The Sunday Times",
        "seem more interesting.",
        "Yes you could say she was attractively built."
    };
    String diff =
        DiffTestCase.diffLines(
            Arrays.asList(before),
            Arrays.asList(after));
    assertThat(Util.toLinux(diff),
        equalTo("1a2\n"
            + "> (they call her \"Polythene Pam\")\n"
            + "3c4,5\n"
            + "< She's the kind of a girl that makes The News of The World\n"
            + "---\n"
            + "> She's the kind of a girl that makes The Sunday Times\n"
            + "> seem more interesting.\n"
            + "5d6\n"
            + "< Yeah yeah yeah.\n"));
  }

  /**
   * Tests the {@link Util#toPosix(TimeZone, boolean)} method.
   */
  @Test void testPosixTimeZone() {
    // Pacific Standard Time. Effective 2007, the local time changes from
    // PST to PDT at 02:00 LST to 03:00 LDT on the second Sunday in March
    // and returns at 02:00 LDT to 01:00 LST on the first Sunday in
    // November.
    assertThat(Util.toPosix(TimeZone.getTimeZone("PST"), false),
        anyOf(is("PST-8PDT,M3.2.0,M11.1.0"),
            is("GMT-08:00-8GMT-07:00,M3.2.0,M11.1.0")));

    assertThat(Util.toPosix(TimeZone.getTimeZone("PST"), true),
        anyOf(is("PST-8PDT1,M3.2.0/2,M11.1.0/2"),
            is("GMT-08:00-8GMT-07:001,M3.2.0/2,M11.1.0/2")));

    // Tokyo has +ve offset, no DST
    assertThat(
        Util.toPosix(TimeZone.getTimeZone("Asia/Tokyo"), true),
        anyOf(
            // Before JDK 23
            is("JST9"),
            // JDK 23 and later
            is("GMT+09:009")));

    // Sydney, Australia lies ten hours east of GMT and makes a one-hour
    // shift forward during daylight savings. Being located in the southern
    // hemisphere, daylight savings begins on the last Sunday in October at
    // 2am and ends on the last Sunday in March at 3am.
    // (Uses STANDARD_TIME time-transition mode.)

    // Because australia changed their daylight savings rules, some JVMs
    // have a different (older and incorrect) timezone settings for
    // Australia.  So we test for the older one first then do the
    // correct assert based upon what the toPosix method returns
    assertThat(Util.toPosix(TimeZone.getTimeZone("Australia/Sydney"), true),
        anyOf(
            // very old JVMs without the fix
            is("EST10EST1,M10.5.0/2,M3.5.0/3"),
            // old JVMs without the fix
            is("EST10EST1,M10.1.0/2,M4.1.0/3"),
            // newer JVMs with the fix
            is("AEST10AEDT1,M10.1.0/2,M4.1.0/3"),
            // JDK 23 and later
            is("GMT+10:0010GMT+11:001,M10.1.0/2,M4.1.0/3")));

    // Paris, France. (Uses UTC_TIME time-transition mode.)
    assertThat(Util.toPosix(TimeZone.getTimeZone("Europe/Paris"), true),
        anyOf(is("CET1CEST1,M3.5.0/2,M10.5.0/3"),
            is("GMT+01:001GMT+02:001,M3.5.0/2,M10.5.0/3")));

    assertThat(Util.toPosix(TimeZone.getTimeZone("UTC"), true),
        is("UTC0"));
  }

  /**
   * Tests the methods {@link Util#enumConstants(Class)} and
   * {@link Util#enumVal(Class, String)}.
   */
  @Test void testEnumConstants() {
    final Map<String, MemoryType> memoryTypeMap =
        Util.enumConstants(MemoryType.class);
    assertThat(memoryTypeMap, aMapWithSize(2));
    assertThat(memoryTypeMap.get("HEAP"), is(MemoryType.HEAP));
    assertThat(memoryTypeMap.get("NON_HEAP"), is(MemoryType.NON_HEAP));
    try {
      memoryTypeMap.put("FOO", null);
      fail("expected exception");
    } catch (UnsupportedOperationException e) {
      // expected: map is immutable
    }

    assertThat(Util.enumVal(MemoryType.class, "HEAP").name(), is("HEAP"));
    assertNull(Util.enumVal(MemoryType.class, "heap"));
    assertNull(Util.enumVal(MemoryType.class, "nonexistent"));
  }

  /**
   * Tests SQL builders.
   */
  @Test void testSqlBuilder() {
    final SqlBuilder buf = new SqlBuilder(CalciteSqlDialect.DEFAULT);
    assertThat(buf.length(), is(0));
    buf.append("select ");
    assertThat(buf.getSql(), is("select "));

    buf.identifier("x");
    assertThat(buf.getSql(), is("select \"x\""));

    buf.append(", ");
    buf.identifier("y", "a b");
    assertThat(buf.getSql(), is("select \"x\", \"y\".\"a b\""));

    final SqlString sqlString = buf.toSqlString();
    assertThat(sqlString.getDialect(), is(CalciteSqlDialect.DEFAULT));
    assertThat(sqlString.getSql(), is(buf.getSql()));

    assertThat(buf.getSql(), not(emptyString()));
    assertThat(sqlString.getSql(), is(buf.getSqlAndClear()));
    assertThat(buf.length(), is(0));

    buf.clear();
    assertThat(buf.length(), is(0));

    buf.literal("can't get no satisfaction");
    assertThat(buf.getSqlAndClear(), is("'can''t get no satisfaction'"));

    buf.literal(new Timestamp(0));
    assertThat(buf.getSqlAndClear(), is("TIMESTAMP '1970-01-01 00:00:00'"));

    buf.literal(new Timestamp(1261053296000L));
    assertThat(buf.getSqlAndClear(), is("TIMESTAMP '2009-12-17 12:34:56'"));

    buf.append("select ");
    buf.literal(new Timestamp(1261053296000L));
    assertThat(buf.getSqlAndClear(),
        is("select TIMESTAMP '2009-12-17 12:34:56'"));

    buf.clear();
    assertThat(buf.length(), is(0));

    buf.append("hello world");
    assertThat(buf.indexOf("l"), is(2));
    assertThat(buf.indexOf("z"), is(-1));
    assertThat(buf.indexOf("l", 5), is(9));
  }

  /**
   * Unit test for {@link org.apache.calcite.util.CompositeList}.
   */
  @Test void testCompositeList() {
    // Made up of zero lists
    //noinspection unchecked
    List<String> list = CompositeList.of(new List[0]);
    assertThat(list, hasSize(0));
    assertTrue(list.isEmpty());
    try {
      final String s = list.get(0);
      fail("expected error, got " + s);
    } catch (IndexOutOfBoundsException e) {
      // ok
    }
    assertFalse(list.listIterator().hasNext());

    List<String> listEmpty = Collections.emptyList();
    List<String> listAbc = Arrays.asList("a", "b", "c");
    List<String> listEmpty2 = new ArrayList<>();

    // Made up of three lists, two of which are empty
    list = CompositeList.of(listEmpty, listAbc, listEmpty2);
    assertThat(list, hasSize(3));
    assertFalse(list.isEmpty());
    assertThat(list.get(0), is("a"));
    assertThat(list.get(2), is("c"));
    try {
      final String s = list.get(3);
      fail("expected error, got " + s);
    } catch (IndexOutOfBoundsException e) {
      // ok
    }
    try {
      final String s = list.set(0, "z");
      fail("expected error, got " + s);
    } catch (UnsupportedOperationException e) {
      // ok
    }

    // Iterator
    final Iterator<String> iterator = list.iterator();
    assertTrue(iterator.hasNext());
    assertThat(iterator.next(), is("a"));
    assertThat(iterator.next(), is("b"));
    assertTrue(iterator.hasNext());
    try {
      iterator.remove();
      fail("expected error");
    } catch (UnsupportedOperationException e) {
      // ok
    }
    assertThat(iterator.next(), is("c"));
    assertFalse(iterator.hasNext());

    // Extend one of the backing lists, and list grows.
    listEmpty2.add("zz");
    assertThat(list, hasSize(4));
    assertThat(list.get(3), is("zz"));

    // Syntactic sugar 'of' method
    String ss = "";
    for (String s : CompositeList.of(list, list)) {
      ss += s;
    }
    assertThat(ss, is("abczzabczz"));
  }

  /**
   * Unit test for {@link Template}.
   */
  @Test void testTemplate() {
    // Regular java message format.
    assertThat(
        new MessageFormat("Hello, {0}, what a nice {1}.", Locale.ROOT)
            .format(new Object[]{"world", "day"}),
        is("Hello, world, what a nice day."));

    // Our extended message format. First, just strings.
    final HashMap<Object, Object> map = new HashMap<>();
    map.put("person", "world");
    map.put("time", "day");
    assertThat(
        Template.formatByName("Hello, {person}, what a nice {time}.", map),
        is("Hello, world, what a nice day."));

    // String and an integer.
    final Template template =
        Template.of("Happy {age,number,#.00}th birthday, {person}!");
    map.clear();
    map.put("person", "Ringo");
    map.put("age", 64.5);
    assertThat(template.format(map), is("Happy 64.50th birthday, Ringo!"));

    // Missing parameters evaluate to null.
    map.remove("person");
    assertThat(template.format(map), is("Happy 64.50th birthday, null!"));

    // Specify parameter by Integer ordinal.
    map.clear();
    map.put(1, "Ringo");
    map.put("0", 64.5);
    assertThat(template.format(map), is("Happy 64.50th birthday, Ringo!"));

    // Too many parameters supplied.
    map.put("lastName", "Starr");
    map.put("homeTown", "Liverpool");
    assertThat(template.format(map), is("Happy 64.50th birthday, Ringo!"));

    // Get parameter names. In order of appearance.
    assertThat(template.getParameterNames(),
        isListOf("age", "person"));

    // No parameters; doubled single quotes; quoted braces.
    final Template template2 =
        Template.of("Don''t expand 'this {brace}'.");
    assertThat(template2.getParameterNames(), empty());
    assertThat(template2.format(Collections.emptyMap()),
        is("Don't expand this {brace}."));

    // Empty template.
    assertThat(Template.formatByName("", map), is(""));
  }

  /**
   * Unit test for {@link Util#parseLocale(String)} method.
   */
  @Test void testParseLocale() {
    Locale[] locales = {
        Locale.CANADA,
        Locale.CANADA_FRENCH,
        Locale.getDefault(),
        Locale.US,
        Locale.TRADITIONAL_CHINESE,
        Locale.ROOT,
    };
    for (Locale locale : locales) {
      assertThat(Util.parseLocale(locale.toString()), is(locale));
    }
    // Example locale names in Locale.toString() javadoc.
    String[] localeNames = {
        "en", "de_DE", "gb_GB", "en_US_WINDOWS", "de__POSIX", "fr__MACOS"
    };
    for (String localeName : localeNames) {
      assertThat(Util.parseLocale(localeName), hasToString(localeName));
    }
  }

  @Test void testSqlLibrary() {
    final SqlLibrary a = SqlLibrary.ALL;
    final SqlLibrary c = SqlLibrary.CALCITE;
    final SqlLibrary o = SqlLibrary.ORACLE;
    final SqlLibrary sp = SqlLibrary.SPATIAL;
    final SqlLibrary st = SqlLibrary.STANDARD;

    assertThat(SqlLibrary.parse("oracle"),
        is(ImmutableList.of(o)));
    assertThat(SqlLibrary.parse("oracle,calcite"),
        is(ImmutableList.of(o, c)));
    assertThat(SqlLibrary.parse("oracle,calcite,oracle"),
        is(ImmutableList.of(o, c, o)));
    assertThat(SqlLibrary.parse("oracle,CALCITE,oracle"),
        is(ImmutableList.of(o, c, o)));
    assertThat(SqlLibrary.parse(""),
        is(ImmutableList.of()));
    assertThrows(IllegalArgumentException.class,
        () -> SqlLibrary.parse("oracle,calcite,foo,oracle"));
    assertThrows(IllegalArgumentException.class,
        () -> SqlLibrary.parse("oracle,calcite,,oracle"));

    assertThat(SqlLibrary.expand(ImmutableList.of(a)),
        hasToString("[ALL, BIG_QUERY, CALCITE, HIVE, MSSQL, MYSQL, ORACLE, "
            + "POSTGRESQL, REDSHIFT, SNOWFLAKE, SPARK]"));
    assertThat(SqlLibrary.expand(ImmutableList.of(a, c)),
        hasToString("[ALL, BIG_QUERY, CALCITE, HIVE, MSSQL, MYSQL, ORACLE, "
            + "POSTGRESQL, REDSHIFT, SNOWFLAKE, SPARK]"));
    assertThat(SqlLibrary.expand(ImmutableList.of(c, a)),
        hasToString("[CALCITE, ALL, BIG_QUERY, HIVE, MSSQL, MYSQL, ORACLE, "
            + "POSTGRESQL, REDSHIFT, SNOWFLAKE, SPARK]"));
    assertThat(SqlLibrary.expand(ImmutableList.of(c, o, a)),
        hasToString("[CALCITE, ORACLE, ALL, BIG_QUERY, HIVE, MSSQL, MYSQL, "
            + "POSTGRESQL, REDSHIFT, SNOWFLAKE, SPARK]"));
    assertThat(SqlLibrary.expand(ImmutableList.of(o, c, o)),
        hasToString("[ORACLE, CALCITE]"));

    assertThat("all + spatial + standard covers everything",
        ImmutableSet.copyOf(SqlLibrary.expand(ImmutableList.of(a, sp, st))),
        is(ImmutableSet.copyOf(SqlLibrary.values())));

    assertThat(SqlLibrary.expandUp(ImmutableList.of(c)),
        hasToString("[ALL, CALCITE]"));
    assertThat(SqlLibrary.expandUp(ImmutableList.of(c, o)),
        hasToString("[ALL, CALCITE, ORACLE]"));
    assertThat(SqlLibrary.expandUp(ImmutableList.of(st, c, o)),
        hasToString("[STANDARD, ALL, CALCITE, ORACLE]"));
    assertThat(SqlLibrary.expandUp(ImmutableList.of(st, sp)),
        hasToString("[STANDARD, SPATIAL]"));
    assertThat(SqlLibrary.expandUp(ImmutableList.of(st, a, sp)),
        hasToString("[STANDARD, ALL, SPATIAL]"));
    assertThat(SqlLibrary.expandUp(ImmutableList.of(a)),
        hasToString("[ALL]"));
  }

  @Test void testSpaces() {
    assertThat(Spaces.of(0), is(""));
    assertThat(Spaces.of(1), is(" "));
    assertThat(Spaces.of(1), is(" "));
    assertThat(Spaces.of(9), is("         "));
    assertThat(Spaces.of(5), is("     "));
    assertThat(Spaces.of(1000).length(), is(1000));
  }

  @Test void testSpaceString() {
    assertThat(Spaces.sequence(0), hasToString(""));
    assertThat(Spaces.sequence(1), hasToString(" "));
    assertThat(Spaces.sequence(9), hasToString("         "));
    assertThat(Spaces.sequence(5), hasToString("     "));
    String s =
        new StringBuilder().append("xx").append(Spaces.MAX, 0, 100)
            .toString();
    assertThat(s.length(), equalTo(102));

    // this would blow memory if the string were materialized... check that it
    // is not
    assertThat(Spaces.sequence(1000000000).length(), equalTo(1000000000));

    final StringWriter sw = new StringWriter();
    Spaces.append(sw, 4);
    assertThat(sw, hasToString("    "));

    final StringBuilder buf = new StringBuilder();
    Spaces.append(buf, 4);
    assertThat(buf, hasToString("    "));

    assertThat(Spaces.padLeft("xy", 5), equalTo("   xy"));
    assertThat(Spaces.padLeft("abcde", 5), equalTo("abcde"));
    assertThat(Spaces.padLeft("abcdef", 5), equalTo("abcdef"));

    assertThat(Spaces.padRight("xy", 5), equalTo("xy   "));
    assertThat(Spaces.padRight("abcde", 5), equalTo("abcde"));
    assertThat(Spaces.padRight("abcdef", 5), equalTo("abcdef"));
  }

  /**
   * Unit test for {@link Pair#zip(java.util.List, java.util.List)}.
   */
  @Test void testPairZip() {
    List<String> strings = Arrays.asList("paul", "george", "john", "ringo");
    List<Integer> integers = Arrays.asList(1942, 1943, 1940);
    List<Pair<String, Integer>> zip = Pair.zip(strings, integers);
    assertThat(zip, hasSize(3));
    assertThat(zip.get(0).left + ":" + zip.get(0).right, is("paul:1942"));
    assertThat(zip.get(2).left, is("john"));
    int x = 0;
    for (Pair<String, Integer> pair : zip) {
      x += pair.right;
    }
    assertThat(x, is(5825));
  }

  /**
   * Unit test for {@link Pair#forEach(Iterable, Iterable, BiConsumer)}.
   */
  @Test void testPairForEach() {
    List<String> strings = Arrays.asList("paul", "george", "john", "ringo");
    List<Integer> integers = Arrays.asList(1942, 1943, 1940);

    final List<Pair<String, Integer>> pairs =
        Pair.zip(strings, integers, false);
    final Map<String, Integer> map = ImmutableMap.copyOf(pairs);

    // shorter list on the right
    final AtomicInteger size = new AtomicInteger();
    Pair.forEach(strings, integers, (s, i) -> size.incrementAndGet());
    assertThat(size.get(), is(3));

    // shorter list on the left
    size.set(0);
    Pair.forEach(integers, strings, (i, s) -> size.incrementAndGet());
    assertThat(size.get(), is(3));

    // same on left and right
    size.set(0);
    Pair.forEach(strings, strings, (s1, s2) -> size.incrementAndGet());
    assertThat(size.get(), is(4));

    // same, using a list of pairs
    size.set(0);
    Pair.forEach(pairs, (s, i) -> size.incrementAndGet());
    assertThat(size.get(), is(3));

    // same, using a map
    size.set(0);
    map.forEach((s, i) -> size.incrementAndGet());
    assertThat(size.get(), is(3));

    // empty on left
    size.set(0);
    Pair.forEach(strings, ImmutableList.of(), (s, i) -> size.incrementAndGet());
    assertThat(size.get(), is(0));

    // empty on right
    size.set(0);
    Pair.forEach(strings, ImmutableList.of(), (s, i) -> size.incrementAndGet());
    assertThat(size.get(), is(0));

    // empty on right
    size.set(0);
    Pair.forEach(ImmutableList.<String>of(), integers,
        (s, i) -> size.incrementAndGet());
    assertThat(size.get(), is(0));

    // both empty
    size.set(0);
    Pair.forEach(ImmutableList.<String>of(), ImmutableList.<Integer>of(),
        (s, i) -> size.incrementAndGet());
    assertThat(size.get(), is(0));

    // empty list of pairs
    size.set(0);
    Pair.forEach(Util.first(pairs, 0), (s, i) -> size.incrementAndGet());
    assertThat(size.get(), is(0));

    // empty map
    size.set(0);
    ImmutableMap.of().forEach((s, i) -> size.incrementAndGet());
    assertThat(size.get(), is(0));

    // build a string
    final StringBuilder b = new StringBuilder();
    Pair.forEach(strings, integers,
        (s, i) -> b.append(s).append(":").append(i).append(";"));
    final String expected = "paul:1942;george:1943;john:1940;";
    assertThat(b, hasToString(expected));

    // same, using list of pairs
    b.setLength(0);
    Pair.forEach(pairs,
        (s, i) -> b.append(s).append(":").append(i).append(";"));
    assertThat(b, hasToString(expected));

    // same, using map
    b.setLength(0);
    map.forEach((s, i) -> b.append(s).append(":").append(i).append(";"));
    assertThat(b, hasToString(expected));
  }

  /**
   * Unit test for {@link Pair#adjacents(Iterable)}.
   */
  @Test void testPairAdjacents() {
    List<String> strings = Arrays.asList("a", "b", "c");
    List<String> result = new ArrayList<>();
    for (Pair<String, String> pair : Pair.adjacents(strings)) {
      result.add(pair.toString());
    }
    assertThat(result, hasToString("[<a, b>, <b, c>]"));

    // empty source yields empty result
    assertThat(Pair.adjacents(ImmutableList.of()).iterator().hasNext(),
        is(false));

    // source with 1 element yields empty result
    assertThat(Pair.adjacents(ImmutableList.of("a")).iterator().hasNext(),
        is(false));

    // source with 100 elements yields result with 99 elements;
    // null elements are ok
    assertThat(Iterables.size(Pair.adjacents(Collections.nCopies(100, null))),
        equalTo(99));
  }

  /**
   * Unit test for {@link Pair#firstAnd(Iterable)}.
   */
  @Test void testPairFirstAnd() {
    List<String> strings = Arrays.asList("a", "b", "c");
    List<String> result = new ArrayList<>();
    for (Pair<String, String> pair : Pair.firstAnd(strings)) {
      result.add(pair.toString());
    }
    assertThat(result, hasToString("[<a, b>, <a, c>]"));

    // empty source yields empty result
    assertThat(Pair.firstAnd(ImmutableList.of()).iterator().hasNext(),
        is(false));

    // source with 1 element yields empty result
    assertThat(Pair.firstAnd(ImmutableList.of("a")).iterator().hasNext(),
        is(false));

    // source with 100 elements yields result with 99 elements;
    // null elements are ok
    assertThat(Iterables.size(Pair.firstAnd(Collections.nCopies(100, null))),
        equalTo(99));
  }

  /**
   * Unit test for {@link Util#quotientList(java.util.List, int, int)}
   * and {@link Util#pairs(List)}.
   */
  @Test void testQuotientList() {
    List<String> beatles = Arrays.asList("john", "paul", "george", "ringo");
    final List<String> list0 = Util.quotientList(beatles, 3, 0);
    assertThat(list0, hasSize(2));
    assertThat(list0.get(0), is("john"));
    assertThat(list0.get(1), is("ringo"));

    final List<String> list1 = Util.quotientList(beatles, 3, 1);
    assertThat(list1, hasSize(1));
    assertThat(list1.get(0), is("paul"));

    final List<String> list2 = Util.quotientList(beatles, 3, 2);
    assertThat(list2, hasSize(1));
    assertThat(list2.get(0), is("george"));

    try {
      final List<String> listBad = Util.quotientList(beatles, 3, 4);
      fail("Expected error, got " + listBad);
    } catch (IllegalArgumentException e) {
      // ok
    }
    try {
      final List<String> listBad = Util.quotientList(beatles, 3, 3);
      fail("Expected error, got " + listBad);
    } catch (IllegalArgumentException e) {
      // ok
    }
    try {
      final List<String> listBad = Util.quotientList(beatles, 0, 0);
      fail("Expected error, got " + listBad);
    } catch (IllegalArgumentException e) {
      // ok
    }

    // empty
    final List<String> empty = Collections.emptyList();
    final List<String> list3 = Util.quotientList(empty, 7, 2);
    assertThat(list3, hasSize(0));

    // shorter than n
    final List<String> list4 = Util.quotientList(beatles, 10, 0);
    assertThat(list4, hasSize(1));
    assertThat(list4.get(0), is("john"));

    final List<String> list5 = Util.quotientList(beatles, 10, 5);
    assertThat(list5, hasSize(0));

    final List<Pair<String, String>> list6 = Util.pairs(beatles);
    assertThat(list6, hasSize(2));
    assertThat(list6.get(0).left, is("john"));
    assertThat(list6.get(0).right, is("paul"));
    assertThat(list6.get(1).left, is("george"));
    assertThat(list6.get(1).right, is("ringo"));

    final List<Pair<String, String>> list7 = Util.pairs(empty);
    assertThat(list7, hasSize(0));
  }

  @Test void testImmutableIntList() {
    final BiConsumer<ImmutableIntList, List<Integer>> c2 = (intList, list) -> {
      assertThat(list, hasSize(intList.size()));
      assertThat(list, is(intList));
      assertThat(list, hasToString(intList.toString()));
      assertThat(list.hashCode(), is(intList.hashCode()));
    };

    final Consumer<ImmutableIntList> c = list -> {
      final List<Integer> arrayList = new ArrayList<>(list);
      c2.accept(list, arrayList);

      final List<Integer> arrayList2 = new ArrayList<>();
      //noinspection CollectionAddAllCanBeReplacedWithConstructor
      arrayList2.addAll(list);
      c2.accept(list, arrayList2);

      final List<Integer> arrayList3 = new ArrayList<>();
      //noinspection UseBulkOperation
      list.forEach(arrayList3::add);
      c2.accept(list, arrayList3);

      final List<Integer> arrayList4 = new ArrayList<>();
      list.forEachInt(arrayList4::add);
      c2.accept(list, arrayList4);
    };

    final ImmutableIntList list = ImmutableIntList.of();
    assertThat(list, hasSize(0));
    assertThat(Collections.<Integer>emptyList(), is(list));
    assertThat(list, hasToString("[]"));
    assertThat(BitSets.of(list), equalTo(new BitSet()));

    final ImmutableIntList list2 = ImmutableIntList.of(1, 3, 5);
    assertThat(list2, hasSize(3));
    assertThat(list2, hasToString("[1, 3, 5]"));
    assertThat(Arrays.asList(1, 3, 5).hashCode(), is(list2.hashCode()));

    Integer[] integers = list2.toArray(new Integer[3]);
    assertThat((int) integers[0], is(1));
    assertThat((int) integers[1], is(3));
    assertThat((int) integers[2], is(5));

    //noinspection EqualsWithItself
    assertThat(list.equals(list), is(true));
    assertThat(list.equals(list2), is(false));
    assertThat(list2.equals(list), is(false));
    //noinspection EqualsWithItself
    assertThat(list2.equals(list2), is(true));

    assertThat(list2.appendAll(Collections.emptyList()), sameInstance(list2));
    assertThat(list2.appendAll(list), sameInstance(list2));
    //noinspection CollectionAddedToSelf
    assertThat(list2.appendAll(list2), isListOf(1, 3, 5, 1, 3, 5));
    assertThat(
        Arrays.toString(ImmutableIntList.of(1).toArray(new Integer[]{5, 6, 7})),
        is("[1, null, 7]"));

    c.accept(list);
    c.accept(list2);
    c.accept(ImmutableIntList.of(-2, 10, 1, -2));
  }

  /** Unit test for {@link IdPair}. */
  @Test void testIdPair() {
    final IdPair<Integer, Integer> p0OneTwo = IdPair.of(1, 2);
    final IdPair<Integer, Integer> p1OneTwo = IdPair.of(1, 2);
    final IdPair<Integer, Integer> p1TwoOne = IdPair.of(2, 1);
    assertThat(p0OneTwo, is(p0OneTwo));
    assertThat(p1OneTwo, is(p0OneTwo));
    assertThat(p1OneTwo.hashCode(), is(p0OneTwo.hashCode()));
    assertNotEquals(p0OneTwo, p1TwoOne);

    final String s0 = "xy";

    // p0s0One and p1s0One are different objects but are equal because their
    // contents are the same objects
    final IdPair<String, Integer> p0s0One = IdPair.of(s0, 1);
    final IdPair<String, Integer> p1s0One = IdPair.of(s0, 1);
    assertNotSame(p0s0One, p1s0One); // different objects, but are equal
    assertThat(p0s0One, is(p0s0One));
    assertThat(p1s0One, is(p0s0One));
    assertThat(p0s0One, is(p1s0One));
    assertThat(p1s0One.hashCode(), is(p0s0One.hashCode()));

    // A copy of "s0" that is equal but not the same object
    final String s1 = s0.toUpperCase(Locale.ROOT).toLowerCase(Locale.ROOT);
    assertThat(s1, is(s0));
    assertNotSame(s0, s1);

    // p0s1One is not equal to p0s0One because s1 is not the same object as s0
    final IdPair<String, Integer> p0s1One = IdPair.of(s1, 1);
    assertNotEquals(p0s0One, p0s1One);
    assertThat(p0s1One.hashCode(), is(p0s1One.hashCode()));

    final Set<IdPair<?, ?>> set =
        ImmutableSet.of(p0OneTwo, p1OneTwo, p1TwoOne,
            p0s0One, p1s0One, p0s1One);
    assertThat(set, hasSize(4));
    final String[] expected = {"1=2", "2=1", "xy=1", "xy=1"};
    assertThat(set.stream().map(IdPair::toString).sorted().toArray(),
        is(expected));
  }

  /**
   * Unit test for {@link IntegerIntervalSet}.
   */
  @Test void testIntegerIntervalSet() {
    checkIntegerIntervalSet("1,5", 1, 5);

    // empty
    checkIntegerIntervalSet("");

    // empty due to exclusions
    checkIntegerIntervalSet("2,4,-1-5");

    // open range
    checkIntegerIntervalSet("1-6,-3-5,4,9", 1, 2, 4, 6, 9);

    // repeats
    checkIntegerIntervalSet("1,3,1,2-4,-2,-4", 1, 3);
  }

  @SuppressWarnings({"UseBulkOperation",
      "CollectionAddAllCanBeReplacedWithConstructor"})
  private void checkIntegerIntervalSet(String s, int... ints) {
    final List<Integer> intList = Ints.asList(ints);
    final Set<Integer> intSet = new LinkedHashSet<>(intList);

    final Set<Integer> set = IntegerIntervalSet.of(s);
    assertThat(set, hasSize(ints.length));
    assertThat(set, is(intSet));

    List<Integer> list = new ArrayList<>(set);
    assertThat(list, is(intList));

    List<Integer> list2 = new ArrayList<>();
    for (Integer i : set) {
      list2.add(i);
    }
    assertThat(list2, is(intList));

    List<Integer> list3 = new ArrayList<>();
    list3.addAll(intSet);
    assertThat(list3, is(intList));
  }

  /**
   * Tests that flat lists behave like regular lists in terms of equals
   * and hashCode.
   */
  @Test void testFlatList() {
    final List<String> emp = FlatLists.of();
    final List<String> emp0 = Collections.emptyList();
    assertThat(emp0, is(emp));
    assertThat(emp0.hashCode(), is(emp.hashCode()));

    final List<String> ab = FlatLists.of("A", "B");
    final List<String> ab0 = Arrays.asList("A", "B");
    assertThat(ab0, is(ab));
    assertThat(ab0.hashCode(), is(ab.hashCode()));

    final List<String> abc = FlatLists.of("A", "B", "C");
    final List<String> abc0 = Arrays.asList("A", "B", "C");
    assertThat(abc0, is(abc));
    assertThat(abc0.hashCode(), is(abc.hashCode()));

    final List<Object> abc1 = FlatLists.of((Object) "A", "B", "C");
    assertThat(abc0, is(abc1));
    assertThat(abc0, is(abc));
    assertThat(abc0.hashCode(), is(abc1.hashCode()));

    final List<String> an = FlatLists.of("A", null);
    final List<String> an0 = Arrays.asList("A", null);
    assertThat(an0, is(an));
    assertThat(an0.hashCode(), is(an.hashCode()));

    final List<String> anb = FlatLists.of("A", null, "B");
    final List<String> anb0 = Arrays.asList("A", null, "B");
    assertThat(anb0, is(anb));
    assertThat(anb0.hashCode(), is(anb.hashCode()));
    assertThat(anb + ".indexOf(null)", anb.indexOf(null), is(1));
    assertThat(anb + ".lastIndexOf(null)", anb.lastIndexOf(null), is(1));
    assertThat(anb + ".indexOf(B)", anb.indexOf("B"), is(2));
    assertThat(anb + ".lastIndexOf(A)", anb.lastIndexOf("A"), is(0));
    assertThat(anb + ".indexOf(Z)", anb.indexOf("Z"), is(-1));
    assertThat(anb + ".lastIndexOf(Z)", anb.lastIndexOf("Z"), is(-1));

    // Comparisons
    assertThat(emp, instanceOf(Comparable.class));
    assertThat(ab, instanceOf(Comparable.class));
    @SuppressWarnings("unchecked")
    final Comparable<List> cemp = (Comparable) emp;
    @SuppressWarnings("unchecked")
    final Comparable<List> cab = (Comparable) ab;
    assertThat(cemp.compareTo(emp), is(0));
    assertThat(cemp.compareTo(ab) < 0, is(true));
    assertThat(cab.compareTo(ab), is(0));
    assertThat(cab.compareTo(emp) > 0, is(true));
    assertThat(cab.compareTo(anb) > 0, is(true));
  }

  @Test void testFlatList2() {
    checkFlatList(0);
    checkFlatList(1);
    checkFlatList(2);
    checkFlatList(3);
    checkFlatList(4);
    checkFlatList(5);
    checkFlatList(6);
    checkFlatList(7);
  }

  private void checkFlatList(int n) {
    final List<String> emp;
    final List<Object> emp1;
    final List<String> eNull;
    switch (n) {
    case 0:
      emp = FlatLists.of();
      emp1 = FlatLists.<Object>copyOf();
      eNull = null;
      break;
    case 1:
      emp = FlatLists.of("A");
      emp1 = FlatLists.copyOf((Object) "A");
      eNull = null;
      break;
    case 2:
      emp = FlatLists.of("A", "B");
      emp1 = FlatLists.of((Object) "A", "B");
      eNull = FlatLists.of("A", null);
      break;
    case 3:
      emp = FlatLists.of("A", "B", "C");
      emp1 = FlatLists.copyOf((Object) "A", "B", "C");
      eNull = FlatLists.of("A", null, "C");
      break;
    case 4:
      emp = FlatLists.of("A", "B", "C", "D");
      emp1 = FlatLists.copyOf((Object) "A", "B", "C", "D");
      eNull = FlatLists.of("A", null, "C", "D");
      break;
    case 5:
      emp = FlatLists.of("A", "B", "C", "D", "E");
      emp1 = FlatLists.copyOf((Object) "A", "B", "C", "D", "E");
      eNull = FlatLists.of("A", null, "C", "D", "E");
      break;
    case 6:
      emp = FlatLists.of("A", "B", "C", "D", "E", "F");
      emp1 = FlatLists.copyOf((Object) "A", "B", "C", "D", "E", "F");
      eNull = FlatLists.of("A", null, "C", "D", "E", "F");
      break;
    case 7:
      emp = FlatLists.of("A", "B", "C", "D", "E", "F", "G");
      emp1 = FlatLists.copyOf((Object) "A", "B", "C", "D", "E", "F", "G");
      eNull = FlatLists.of("A", null, "C", "D", "E", "F", "G");
      break;
    default:
      throw new AssertionError(n);
    }
    final List<String> emp0 =
        Arrays.asList("A", "B", "C", "D", "E", "F", "G").subList(0, n);
    final List<String> eNull0 =
        Arrays.asList("A", null, "C", "D", "E", "F", "G").subList(0, n);
    assertThat(emp0, is(emp));
    assertThat(emp1, is(emp));
    assertThat(emp1, is(emp0));
    assertThat(emp0, is(emp1));
    assertThat(emp0.hashCode(), is(emp.hashCode()));
    assertThat(emp1.hashCode(), is(emp.hashCode()));

    assertThat(emp, hasSize(n));
    if (eNull != null) {
      assertThat(eNull, hasSize(n));
    }

    final List<String> an = FlatLists.of("A", null);
    final List<String> an0 = Arrays.asList("A", null);
    assertThat(an0, is(an));
    assertThat(an0.hashCode(), is(an.hashCode()));

    if (eNull != null) {
      assertThat(eNull0, is(eNull));
      assertThat(eNull0.hashCode(), is(eNull.hashCode()));
    }

    assertThat(emp, hasToString(emp1.toString()));
    if (eNull != null) {
      assertThat(eNull.toString().length(), is(emp1.toString().length() + 3));
    }

    // Comparisons
    assertThat(emp, instanceOf(Comparable.class));
    if (n < 7) {
      assertThat(emp1, instanceOf(Comparable.class));
    }
    if (eNull != null) {
      assertThat(eNull, instanceOf(Comparable.class));
    }
    @SuppressWarnings("unchecked")
    final Comparable<List> cemp = (Comparable) emp;
    assertThat(cemp.compareTo(emp), is(0));
    if (eNull != null) {
      assertThat(cemp.compareTo(eNull) < 0, is(false));
    }
  }

  private <E> List<E> l1(E e) {
    return Collections.singletonList(e);
  }

  private <E> List<E> l2(E e0, E e1) {
    return Arrays.asList(e0, e1);
  }

  private <E> List<E> l3(E e0, E e1, E e2) {
    return Arrays.asList(e0, e1, e2);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2287">[CALCITE-2287]
   * FlatList.equals throws StackOverflowError</a>. */
  @Test void testFlat34Equals() {
    List f3list = FlatLists.of(1, 2, 3);
    List f4list = FlatLists.of(1, 2, 3, 4);
    assertThat(f3list.equals(f4list), is(false));
  }

  @SuppressWarnings("unchecked")
  @Test void testFlatListN() {
    List<List<Object>> list = new ArrayList<>();
    list.add(FlatLists.of());
    list.add(FlatLists.<Object>copyOf());
    list.add(FlatLists.of("A"));
    list.add(FlatLists.copyOf((Object) "A"));
    list.add(FlatLists.of("A", "B"));
    list.add(FlatLists.of((Object) "A", "B"));
    list.add(Lists.newArrayList(Util.last(list)));
    list.add(FlatLists.of("A", null));
    list.add(Lists.newArrayList(Util.last(list)));
    list.add(FlatLists.of("A", "B", "C"));
    list.add(Lists.newArrayList(Util.last(list)));
    list.add(FlatLists.copyOf((Object) "A", "B", "C"));
    list.add(FlatLists.of("A", null, "C"));
    list.add(FlatLists.of("A", "B", "C", "D"));
    list.add(Lists.newArrayList(Util.last(list)));
    list.add(FlatLists.copyOf((Object) "A", "B", "C", "D"));
    list.add(FlatLists.of("A", null, "C", "D"));
    list.add(Lists.newArrayList(Util.last(list)));
    list.add(FlatLists.of("A", "B", "C", "D", "E"));
    list.add(Lists.newArrayList(Util.last(list)));
    list.add(FlatLists.copyOf((Object) "A", "B", "C", "D", "E"));
    list.add(FlatLists.of("A", null, "C", "D", "E"));
    list.add(FlatLists.of("A", "B", "C", "D", "E", "F"));
    list.add(FlatLists.copyOf((Object) "A", "B", "C", "D", "E", "F"));
    list.add(FlatLists.of("A", null, "C", "D", "E", "F"));
    list.add((List)
        FlatLists.of((Comparable) "A", "B", "C", "D", "E", "F", "G"));
    list.add(FlatLists.copyOf((Object) "A", "B", "C", "D", "E", "F", "G"));
    list.add(Lists.newArrayList(Util.last(list)));
    list.add((List)
        FlatLists.of((Comparable) "A", null, "C", "D", "E", "F", "G"));
    list.add(Lists.newArrayList(Util.last(list)));
    for (int i = 0; i < list.size(); i++) {
      final List<Object> outer = list.get(i);
      for (List<Object> inner : list) {
        if (inner.toString().equals("[A, B, C,D]")) {
          System.out.println(1);
        }
        boolean strEq = outer.toString().equals(inner.toString());
        assertThat(outer + "=" + inner, outer.equals(inner), is(strEq));
      }
    }
  }

  @Test void testFlatListProduct() {
    final List<Enumerator<List<String>>> list = new ArrayList<>();
    list.add(Linq4j.enumerator(l2(l1("a"), l1("b"))));
    list.add(Linq4j.enumerator(l3(l2("x", "p"), l2("y", "q"), l2("z", "r"))));
    final Enumerable<FlatLists.ComparableList<String>> product =
        SqlFunctions.product(list, 3, false);
    int n = 0;
    FlatLists.ComparableList<String> previous = FlatLists.of();
    for (FlatLists.ComparableList<String> strings : product) {
      if (n++ == 1) {
        assertThat(strings, hasSize(3));
        assertThat(strings.get(0), is("a"));
        assertThat(strings.get(1), is("y"));
        assertThat(strings.get(2), is("q"));
      }
      if (previous != null) {
        assertTrue(previous.compareTo(strings) < 0);
      }
      previous = strings;
    }
    assertThat(n, is(6));
  }

  /**
   * Unit test for {@link AvaticaUtils#toCamelCase(String)}.
   */
  @Test void testToCamelCase() {
    assertThat(AvaticaUtils.toCamelCase("MY_JDBC_DRIVER"), is("myJdbcDriver"));
    assertThat(AvaticaUtils.toCamelCase("a_JDBC__DRIVER"), is("aJdbcDriver"));
    assertThat(AvaticaUtils.toCamelCase("my_jdbc_driver"), is("myJdbcDriver"));
    assertThat(AvaticaUtils.toCamelCase("ab_cdEf_g_Hij"), is("abCdefGHij"));
    assertThat(AvaticaUtils.toCamelCase("_JDBC_DRIVER"), is("JdbcDriver"));
    assertThat(AvaticaUtils.toCamelCase("_"), is(""));
    assertThat(AvaticaUtils.toCamelCase(""), is(""));
  }

  /** Unit test for {@link AvaticaUtils#camelToUpper(String)}. */
  @Test void testCamelToUpper() {
    assertThat(AvaticaUtils.camelToUpper("myJdbcDriver"), is("MY_JDBC_DRIVER"));
    assertThat(AvaticaUtils.camelToUpper("myJDBCDriver"), is("MY_J_D_B_C_DRIVER"));
    assertThat(AvaticaUtils.camelToUpper("abCdefGHij"), is("AB_CDEF_G_HIJ"));
    assertThat(AvaticaUtils.camelToUpper("JdbcDriver"), is("_JDBC_DRIVER"));
    assertThat(AvaticaUtils.camelToUpper(""), is(""));
  }

  /**
   * Unit test for {@link Util#isDistinct(java.util.List)}.
   */
  @Test void testDistinct() {
    assertTrue(Util.isDistinct(Collections.emptyList()));
    assertTrue(Util.isDistinct(Arrays.asList("a")));
    assertTrue(Util.isDistinct(Arrays.asList("a", "b", "c")));
    assertFalse(Util.isDistinct(Arrays.asList("a", "b", "a")));
    assertTrue(Util.isDistinct(Arrays.asList("a", "b", null)));
    assertFalse(Util.isDistinct(Arrays.asList("a", null, "b", null)));
  }

  /** Unit test for
   * {@link Util#intersects(java.util.Collection, java.util.Collection)}. */
  @Test void testIntersects() {
    final List<String> empty = Collections.emptyList();
    final List<String> listA = Collections.singletonList("a");
    final List<String> listC = Collections.singletonList("c");
    final List<String> listD = Collections.singletonList("d");
    final List<String> listAbc = Arrays.asList("a", "b", "c");
    assertThat(Util.intersects(empty, listA), is(false));
    assertThat(Util.intersects(empty, empty), is(false));
    assertThat(Util.intersects(listA, listAbc), is(true));
    assertThat(Util.intersects(listAbc, listAbc), is(true));
    assertThat(Util.intersects(listAbc, listC), is(true));
    assertThat(Util.intersects(listAbc, listD), is(false));
    assertThat(Util.intersects(listC, listD), is(false));
  }

  /**
   * Unit test for {@link org.apache.calcite.util.JsonBuilder}.
   */
  @Test void testJsonBuilder() {
    JsonBuilder builder = new JsonBuilder();
    Map<String, Object> map = builder.map();
    map.put("foo", 1);
    map.put("baz", true);
    map.put("bar", "can't");
    final String tricky = "string with doublequote\", singlequote', "
        + "backslash\\, percent20%20, plus+, ampersand&, linefeed\n"
        + ", carriage return\r, lfcr\n"
        + "\r.";
    map.put("tricky", tricky);
    List<Object> list = builder.list();
    map.put("list", list);
    list.add(2);
    list.add(3);
    list.add(builder.list());
    list.add(builder.map());
    list.add(tricky);
    list.add(null);
    map.put("nullValue", null);
    final String expected = "{\n"
        + "  \"foo\": 1,\n"
        + "  \"baz\": true,\n"
        + "  \"bar\": \"can't\",\n"
        + "  \"tricky\": \"string with doublequote\\\", singlequote', "
        + "backslash\\\\, percent20%20, plus+, ampersand&, linefeed\\n"
        + ", carriage return\\r, lfcr\\n\\r.\",\n"
        + "  \"list\": [\n"
        + "    2,\n"
        + "    3,\n"
        + "    [],\n"
        + "    {},\n"
        + "    \"string with doublequote\\\", singlequote', backslash\\\\, "
        + "percent20%20, plus+, ampersand&, linefeed\\n"
        + ", carriage return\\r, lfcr\\n\\r.\",\n"
        + "    null\n"
        + "  ],\n"
        + "  \"nullValue\": null\n"
        + "}";
    assertThat(builder.toJsonString(map), is(expected));
  }

  @Test void testCompositeMap() {
    String[] beatles = {"john", "paul", "george", "ringo"};
    Map<String, Integer> beatleMap = new LinkedHashMap<String, Integer>();
    for (String beatle : beatles) {
      beatleMap.put(beatle, beatle.length());
    }

    CompositeMap<String, Integer> map = CompositeMap.of(beatleMap);
    checkCompositeMap(beatles, map);

    map = CompositeMap.of(beatleMap, Collections.emptyMap());
    checkCompositeMap(beatles, map);

    map = CompositeMap.of(Collections.emptyMap(), beatleMap);
    checkCompositeMap(beatles, map);

    map = CompositeMap.of(beatleMap, beatleMap);
    checkCompositeMap(beatles, map);

    final Map<String, Integer> founderMap =
        new LinkedHashMap<String, Integer>();
    founderMap.put("ben", 1706);
    founderMap.put("george", 1732);
    founderMap.put("thomas", 1743);

    map = CompositeMap.of(beatleMap, founderMap);
    assertThat(map.isEmpty(), equalTo(false));
    assertThat(map, aMapWithSize(6));
    assertThat(map.keySet(), hasSize(6));
    assertThat(map.entrySet(), hasSize(6));
    assertThat(map.values(), hasSize(6));
    assertThat(map.containsKey("john"), equalTo(true));
    assertThat(map.containsKey("george"), equalTo(true));
    assertThat(map.containsKey("ben"), equalTo(true));
    assertThat(map.containsKey("andrew"), equalTo(false));
    assertThat(map.get("ben"), equalTo(1706));
    assertThat(map.get("george"), equalTo(6)); // use value from first map
    assertThat(map.values().contains(1743), equalTo(true));
    assertThat(map.values().contains(1732), equalTo(false)); // masked
    assertThat(map.values().contains(1999), equalTo(false));
  }

  private void checkCompositeMap(String[] beatles, Map<String, Integer> map) {
    assertThat(map, aMapWithSize(4));
    assertThat(map.isEmpty(), is(false));
    assertThat(map.keySet(),
        equalTo(new HashSet<>(Arrays.asList(beatles))));
    assertThat(
        ImmutableMultiset.copyOf(map.values()),
        equalTo(ImmutableMultiset.copyOf(Arrays.asList(4, 4, 6, 5))));
  }

  /** Tests {@link Util#commaList(java.util.List)}. */
  @Test void testCommaList() {
    try {
      String s = Util.commaList(null);
      fail("expected NPE, got " + s);
    } catch (NullPointerException e) {
      // ok
    }
    assertThat(Util.commaList(ImmutableList.of()), equalTo(""));
    assertThat(Util.commaList(ImmutableList.of(1)), equalTo("1"));
    assertThat(Util.commaList(ImmutableList.of(2, 3)), equalTo("2, 3"));
    assertThat(Util.commaList(Arrays.asList(2, null, 3)),
        equalTo("2, null, 3"));
  }

  /** Unit test for {@link Util#firstDuplicate(java.util.List)}. */
  @Test void testFirstDuplicate() {
    assertThat(Util.firstDuplicate(ImmutableList.of()), equalTo(-1));
    assertThat(Util.firstDuplicate(ImmutableList.of(5)), equalTo(-1));
    assertThat(Util.firstDuplicate(ImmutableList.of(5, 6)), equalTo(-1));
    assertThat(Util.firstDuplicate(ImmutableList.of(5, 6, 5)), equalTo(2));
    assertThat(Util.firstDuplicate(ImmutableList.of(5, 5, 6)), equalTo(1));
    assertThat(Util.firstDuplicate(ImmutableList.of(5, 5, 6, 5)), equalTo(1));
    // list longer than 15, the threshold where we move to set-based algorithm
    assertThat(
        Util.firstDuplicate(
            ImmutableList.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14,
                15, 16, 17, 3, 19, 3, 21)),
        equalTo(18));
  }

  /** Benchmark for {@link Util#isDistinct}. Has determined that map-based
   * implementation is better than nested loops implementation if list is larger
   * than about 15. */
  @Test void testIsDistinctBenchmark() {
    // Run a much quicker form of the test during regular testing.
    final int limit = Benchmark.enabled() ? 1000000 : 10;
    final int zMax = 100;
    for (int i = 0; i < 30; i++) {
      final int size = i;
      new Benchmark("isDistinct " + i + " (set)", statistician -> {
        final Random random = new Random(0);
        final List<List<Integer>> lists = new ArrayList<List<Integer>>();
        for (int z = 0; z < zMax; z++) {
          final List<Integer> list = new ArrayList<Integer>();
          for (int k = 0; k < size; k++) {
            list.add(random.nextInt(size * size));
          }
          lists.add(list);
        }
        long nanos = System.nanoTime();
        int n = 0;
        for (int j = 0; j < limit; j++) {
          n += Util.firstDuplicate(lists.get(j % zMax));
        }
        statistician.record(nanos);
        Util.discard(n);
        return null;
      },
          5).run();
    }
  }

  /** Unit test for {@link Util#distinctList(List)}
   * and {@link Util#distinctList(Iterable)}. */
  @Test void testDistinctList() {
    assertThat(Util.distinctList(Arrays.asList(1, 2)), isListOf(1, 2));
    assertThat(Util.distinctList(Arrays.asList(1, 2, 1)),
        isListOf(1, 2));
    try {
      List<Object> o = Util.distinctList(null);
      fail("expected exception, got " + o);
    } catch (NullPointerException ignore) {
    }
    final List<Integer> empty = ImmutableList.of();
    assertThat(Util.distinctList(empty), sameInstance(empty));
    final Iterable<Integer> emptyIterable = empty;
    assertThat(Util.distinctList(emptyIterable), sameInstance(emptyIterable));
    final List<Integer> empty2 = ImmutableList.of();
    assertThat(Util.distinctList(empty2), sameInstance(empty2));
    final List<String> abc = ImmutableList.of("a", "b", "c");
    assertThat(Util.distinctList(abc), sameInstance(abc));
    final List<String> a = ImmutableList.of("a");
    assertThat(Util.distinctList(a), sameInstance(a));
    final List<String> cbca = ImmutableList.of("c", "b", "c", "a");
    assertThat(Util.distinctList(cbca), not(sameInstance(cbca)));
    assertThat(Util.distinctList(cbca), isListOf("c", "b", "a"));
    final Collection<String> cbcaC = new LinkedHashSet<>(cbca);
    assertThat(Util.distinctList(cbcaC), not(sameInstance(cbca)));
    assertThat(Util.distinctList(cbcaC), isListOf("c", "b", "a"));
    final List<String> a2 = ImmutableList.of("a", "a");
    assertThat(Util.distinctList(a2), is(a));
    // Short list that is not distinct
    final List<String> aab = ImmutableList.of("a", "a", "b");
    final List<String> ab = Arrays.asList("a", "b");
    assertThat(Util.distinctList(aab), is(ab));
    // Short list that is not distinct and has later duplicate element
    final List<String> aaba = ImmutableList.of("a", "a", "b", "a");
    assertThat(Util.distinctList(aaba), is(ab));
    final List<String> a1m = Collections.nCopies(1_000_000, "a");
    assertThat(Util.distinctList(a1m), is(a));
  }

  @Test void testIsDefinitelyDistinctAndNonNull() {
    final BiConsumer<List<String>, Boolean> f = (list, b) -> {
      assertThat(Util.isDefinitelyDistinctAndNonNull(list), is(b));
    };

    f.accept(ImmutableList.of(), true);
    f.accept(Collections.singletonList(null), false);
    f.accept(list("a", "b", "a"), false);
    f.accept(list("a", "b", "c"), true);
    f.accept(list(null, "b", "c"), false);
    f.accept(list(null, "b", null), false);
    f.accept(list(null, null), false);
    final IntFunction<String> stringFn =
        i -> String.valueOf((char) ('a' + i));
    // {"a", "b", "c", "d", "e", "f"} is distinct
    final List<String> alpha6 = Functions.generate(6, stringFn);
    f.accept(alpha6, true);
    // {"a", ... "n"} is distinct
    final List<String> alpha14 =
        new ArrayList<>(Functions.generate(14, stringFn));
    f.accept(alpha14, true);
    alpha14.set(10, "a");
    f.accept(alpha14, false);
    alpha14.set(10, null);
    f.accept(alpha14, false);
    alpha14.set(10, "z");
    f.accept(alpha14, true);
    // {"a", ... "t"} is distinct but has length above the threshold,
    // so gives a false negative
    f.accept(Functions.generate(20, stringFn), false);
  }

  /** Unit test for {@link Utilities#hashCode(double)}. */
  @Test void testHash() {
    checkHash(0d);
    checkHash(1d);
    checkHash(-2.5d);
    checkHash(10d / 3d);
    checkHash(Double.NEGATIVE_INFINITY);
    checkHash(Double.POSITIVE_INFINITY);
    checkHash(Double.MAX_VALUE);
    checkHash(Double.MIN_VALUE);
  }

  @SuppressWarnings("deprecation")
  public void checkHash(double v) {
    assertThat(Double.valueOf(v).hashCode(), is(Utilities.hashCode(v)));
    final long long_ = (long) v;
    assertThat(Long.valueOf(long_).hashCode(), is(Utilities.hashCode(long_)));
    final float float_ = (float) v;
    assertThat(Float.valueOf(float_).hashCode(),
        is(Utilities.hashCode(float_)));
    final boolean boolean_ = v != 0;
    assertThat(Boolean.valueOf(boolean_).hashCode(),
        is(Utilities.hashCode(boolean_)));
  }

  /** Unit test for {@link Util#startsWith}. */
  @Test void testStartsWithList() {
    assertThat(Util.startsWith(list("x"), list()), is(true));
    assertThat(Util.startsWith(list("x"), list("x")), is(true));
    assertThat(Util.startsWith(list("x"), list("y")), is(false));
    assertThat(Util.startsWith(list("x"), list("x", "y")), is(false));
    assertThat(Util.startsWith(list("x", "y"), list("x")), is(true));
    assertThat(Util.startsWith(list(), list()), is(true));
    assertThat(Util.startsWith(list(), list("x")), is(false));
  }

  public List<String> list(String... xs) {
    return Arrays.asList(xs);
  }

  @Test void testResources() {
    Resources.validate(Static.RESOURCE);
    checkResourceMethodNames(Static.RESOURCE);
  }

  private void checkResourceMethodNames(Object resource) {
    for (Method method : resource.getClass().getMethods()) {
      if (!isStatic(method)
          && !method.getName().matches("^[a-z][A-Za-z0-9_]*$")) {
        fail("resource method name must be camel case: " + method.getName());
      }
    }
  }

  /** Tests that sorted sets behave the way we expect. */
  @Test void testSortedSet() {
    final TreeSet<String> treeSet = new TreeSet<String>();
    Collections.addAll(treeSet, "foo", "bar", "fOo", "FOO", "pug");
    assertThat(treeSet, hasSize(5));

    final TreeSet<String> treeSet2 =
        new TreeSet<String>(String.CASE_INSENSITIVE_ORDER);
    treeSet2.addAll(treeSet);
    assertThat(treeSet2, hasSize(3));

    final Comparator<String> comparator = (o1, o2) -> {
      String u1 = o1.toUpperCase(Locale.ROOT);
      String u2 = o2.toUpperCase(Locale.ROOT);
      int c = u1.compareTo(u2);
      if (c == 0) {
        c = o1.compareTo(o2);
      }
      return c;
    };
    final TreeSet<String> treeSet3 = new TreeSet<String>(comparator);
    treeSet3.addAll(treeSet);
    assertThat(treeSet3, hasSize(5));

    assertThat(checkNav(treeSet3, "foo"), hasSize(3));
    assertThat(checkNav(treeSet3, "FOO"), hasSize(3));
    assertThat(checkNav(treeSet3, "FoO"), hasSize(3));
    assertThat(checkNav(treeSet3, "BAR"), hasSize(1));
  }

  private NavigableSet<String> checkNav(NavigableSet<String> set, String s) {
    // Note this does not support some unicode characters
    // however it is fine for testing purposes
    return set.subSet(s.toUpperCase(Locale.ROOT), true,
        s.toLowerCase(Locale.ROOT), true);
  }

  /** Test for {@link org.apache.calcite.util.ImmutableNullableList}. */
  @Test void testImmutableNullableList() {
    final List<String> arrayList = Arrays.asList("a", null, "c");
    final List<String> list = ImmutableNullableList.copyOf(arrayList);
    assertThat(list, hasSize(arrayList.size()));
    assertThat(list, equalTo(arrayList));
    assertThat(list.hashCode(), equalTo(arrayList.hashCode()));
    assertThat(list, hasToString(arrayList.toString()));
    String z = "";
    for (String s : list) {
      z += s;
    }
    assertThat(z, equalTo("anullc"));

    // changes to array list do not affect copy
    arrayList.set(0, "z");
    assertThat(arrayList.get(0), equalTo("z"));
    assertThat(list.get(0), equalTo("a"));

    try {
      boolean b = list.add("z");
      fail("expected error, got " + b);
    } catch (UnsupportedOperationException e) {
      // ok
    }
    try {
      String b = list.set(1, "z");
      fail("expected error, got " + b);
    } catch (UnsupportedOperationException e) {
      // ok
    }

    // empty list uses ImmutableList
    assertThat(ImmutableNullableList.copyOf(Collections.emptyList()),
        isA((Class) ImmutableList.class));

    // list with no nulls uses ImmutableList
    final List<String> abcList = Arrays.asList("a", "b", "c");
    assertThat(ImmutableNullableList.copyOf(abcList),
        isA((Class) ImmutableList.class));

    // list with no nulls uses ImmutableList
    final Iterable<String> abc = abcList::iterator;
    assertThat(ImmutableNullableList.copyOf(abc),
        isA((Class) ImmutableList.class));
    assertThat(ImmutableNullableList.copyOf(abc), equalTo(abcList));

    // list with no nulls uses ImmutableList
    final List<String> ab0cList = Arrays.asList("a", "b", null, "c");
    final Iterable<String> ab0c = ab0cList::iterator;
    assertThat(ImmutableNullableList.copyOf(ab0c),
        not(isA((Class) ImmutableList.class)));
    assertThat(ImmutableNullableList.copyOf(ab0c), equalTo(ab0cList));
  }

  /** Test for {@link org.apache.calcite.util.UnmodifiableArrayList}. */
  @Test void testUnmodifiableArrayList() {
    final String[] strings = {"a", null, "c"};
    final List<String> arrayList = Arrays.asList(strings);
    final List<String> list = UnmodifiableArrayList.of(strings);
    assertThat(list, hasSize(arrayList.size()));
    assertThat(list, equalTo(arrayList));
    assertThat(list.hashCode(), equalTo(arrayList.hashCode()));
    assertThat(list, hasToString(arrayList.toString()));
    String z = "";
    for (String s : list) {
      z += s;
    }
    assertThat(z, equalTo("anullc"));

    // changes to array list do affect copy
    arrayList.set(0, "z");
    assertThat(arrayList.get(0), equalTo("z"));
    assertThat(list.get(0), equalTo("z"));

    try {
      boolean b = list.add("z");
      fail("expected error, got " + b);
    } catch (UnsupportedOperationException e) {
      // ok
    }
    try {
      String b = list.set(1, "z");
      fail("expected error, got " + b);
    } catch (UnsupportedOperationException e) {
      // ok
    }
  }

  /** Test for {@link org.apache.calcite.util.ImmutableNullableList.Builder}. */
  @Test void testImmutableNullableListBuilder() {
    final ImmutableNullableList.Builder<String> builder =
        ImmutableNullableList.builder();
    builder.add("a")
        .add((String) null)
        .add("c");
    final List<String> arrayList = Arrays.asList("a", null, "c");
    final List<String> list = builder.build();
    assertThat(arrayList.equals(list), is(true));
  }

  /** Test for {@link org.apache.calcite.util.ImmutableNullableSet}. */
  @Test void testImmutableNullableSet() {
    final List<String> arrayList = Arrays.asList("a", null, "c", "a");
    final Set<String> set = ImmutableNullableSet.copyOf(arrayList);
    final Set<String> set2 = new LinkedHashSet<>(arrayList);
    assertThat(set, hasSize(set2.size()));
    assertThat(set, equalTo(set2));
    assertThat(set.hashCode(), equalTo(set2.hashCode()));
    assertThat(set, hasToString(set2.toString()));
    StringBuilder z = new StringBuilder();
    for (String s : set) {
      z.append(s);
    }
    assertThat(z, hasToString("anullc"));

    // changes to array list do not affect copy
    arrayList.set(0, "z");
    assertThat(arrayList.get(0), equalTo("z"));
    assertThat(set.iterator().next(), equalTo("a"));

    try {
      boolean b = set.add("z");
      fail("expected error, got " + b);
    } catch (UnsupportedOperationException e) {
      // ok
    }
    try {
      boolean b = set.remove("z");
      fail("expected error, got " + b);
    } catch (UnsupportedOperationException e) {
      // ok
    }

    // Collections.emptySet() is unchanged
    assertThat(ImmutableNullableSet.copyOf(Collections.emptySet()),
        sameInstance(Collections.emptySet()));

    // any other empty set becomes ImmutableSet
    assertThat(ImmutableNullableSet.copyOf(ImmutableSet.of()),
        sameInstance(ImmutableSet.of()));

    assertThat(ImmutableNullableSet.copyOf(new HashSet<>()),
        sameInstance(ImmutableSet.of()));

    // singleton set is unchanged
    final Set<String> justA = Collections.singleton("a");
    assertThat(ImmutableNullableSet.copyOf(justA),
        sameInstance(justA));

    final Set<String> justNull = Collections.singleton(null);
    assertThat(ImmutableNullableSet.copyOf(justNull),
        sameInstance(justNull));

    // set with no nulls uses ImmutableSet
    final List<String> abcList = Arrays.asList("a", "b", "c");
    final Set<String> abcSet = new LinkedHashSet<>(abcList);
    assertThat(ImmutableNullableSet.copyOf(abcList),
        isA((Class) ImmutableSet.class));

    // set with no nulls uses ImmutableSet
    assertThat(ImmutableNullableSet.copyOf(abcList),
        isA((Class) ImmutableSet.class));
    assertThat(ImmutableNullableSet.copyOf(abcList), equalTo(abcSet));

    assertThat(ImmutableNullableSet.copyOf(abcSet),
        isA((Class) ImmutableSet.class));
    assertThat(ImmutableNullableSet.copyOf(abcSet), equalTo(abcSet));

    // set with no nulls uses ImmutableSet
    final List<String> ab0cList = Arrays.asList("a", "b", null, "c");
    final Set<String> ab0cSet = new LinkedHashSet<>(ab0cList);
    assertThat(ImmutableNullableSet.copyOf(ab0cList),
        not(isA((Class) ImmutableSet.class)));
    assertThat(ImmutableNullableSet.copyOf(ab0cList), equalTo(ab0cSet));

    assertThat(ImmutableNullableSet.copyOf(ab0cSet),
        not(isA((Class) ImmutableSet.class)));
    assertThat(ImmutableNullableSet.copyOf(ab0cSet), equalTo(ab0cSet));
  }

  /** Tests {@link ReflectUtil#mightBeAssignableFrom(Class, Class)}. */
  @Test void testMightBeAssignableFrom() {
    final Object myMap = new HashMap<String, Integer>() {
      @Override public Set<Entry<String, Integer>> entrySet() {
        throw new UnsupportedOperationException();
      }
      @Override public Integer put(String key, Integer value) {
        throw new UnsupportedOperationException();
      }
      @Override public int size() {
        throw new UnsupportedOperationException();
      }
    };
    final Class<?> myMapClass = myMap.getClass();

    // Categories:
    //   String - final class
    //   int - primitive
    //   Map - interface
    //   HashMap - non-final class
    //   myMapClass - anonymous (therefore final) class that extends HashMap
    //   StringBuilder - final class
    //   DayOfWeek - enum

    // What can be assigned to an Object parameter? Anything except a primitive.
    checkAssignable(Object.class, Object.class, true);
    checkAssignable(Object.class, String.class, true);
    checkAssignable(Object.class, DayOfWeek.class, true);
    checkAssignable(Object.class, int.class, false);
    checkAssignable(Object.class, Map.class, true);
    checkAssignable(Object.class, HashMap.class, true);
    checkAssignable(Object.class, myMapClass, true);

    // What can be assigned to an String parameter? String is a final class, so
    // only itself, super-classes and super-interfaces.
    checkAssignable(String.class, Object.class, true);
    checkAssignable(String.class, String.class, true);
    checkAssignable(String.class, DayOfWeek.class, false);
    checkAssignable(String.class, int.class, false);
    checkAssignable(String.class, Integer.class, false);
    checkAssignable(String.class, Map.class, false);
    checkAssignable(String.class, HashMap.class, false);
    checkAssignable(String.class, myMapClass, false);
    checkAssignable(String.class, Serializable.class, true);
    checkAssignable(String.class, Throwable.class, false);

    // What can be assigned to an int parameter? int is primitive, so only int.
    checkAssignable(int.class, Object.class, false);
    checkAssignable(int.class, String.class, false);
    checkAssignable(int.class, DayOfWeek.class, false);
    checkAssignable(int.class, int.class, true);
    checkAssignable(int.class, Map.class, false);
    checkAssignable(int.class, HashMap.class, false);
    checkAssignable(int.class, myMapClass, false);
    checkAssignable(int.class, Serializable.class, false);

    // What can be assigned to an Integer parameter? Integer is a final class.
    checkAssignable(Integer.class, Object.class, true);
    checkAssignable(Integer.class, String.class, false);
    checkAssignable(Integer.class, DayOfWeek.class, false);
    checkAssignable(Integer.class, Integer.class, true);
    checkAssignable(Integer.class, int.class, false);
    checkAssignable(Integer.class, Map.class, false);
    checkAssignable(Integer.class, HashMap.class, false);
    checkAssignable(Integer.class, myMapClass, false);
    checkAssignable(Integer.class, Serializable.class, true);
    checkAssignable(Integer.class, Number.class, true);

    // What can be assigned to an HashMap parameter? HashMap is a non-final
    // class.
    checkAssignable(HashMap.class, Object.class, true);
    checkAssignable(HashMap.class, String.class, false);
    checkAssignable(HashMap.class, DayOfWeek.class, false);
    checkAssignable(HashMap.class, int.class, false);
    checkAssignable(HashMap.class, Map.class, true);
    checkAssignable(HashMap.class, HashMap.class, true);
    checkAssignable(HashMap.class, myMapClass, true);
    checkAssignable(HashMap.class, Serializable.class, true);
    checkAssignable(HashMap.class, Number.class, true);

    // What can be assigned to a Map parameter? Map is an interface, so
    // anything except primitives and final classes that do not implement Map.
    checkAssignable(Map.class, Object.class, true);
    checkAssignable(Map.class, String.class, false);
    checkAssignable(Map.class, DayOfWeek.class, false);
    checkAssignable(Map.class, int.class, false);
    checkAssignable(Map.class, Map.class, true);
    checkAssignable(Map.class, HashMap.class, true);
    checkAssignable(Map.class, myMapClass, true);
    checkAssignable(Map.class, NavigableMap.class, true);
    checkAssignable(Map.class, Serializable.class, true);
    checkAssignable(Map.class, StringBuilder.class, false);

    // What can be assigned to an Enum parameter? An Enum is very similar to a
    // final class.
    checkAssignable(DayOfWeek.class, Object.class, true);
    checkAssignable(DayOfWeek.class, String.class, false);
    checkAssignable(DayOfWeek.class, DayOfWeek.class, true);
    checkAssignable(DayOfWeek.class, int.class, false);
    checkAssignable(DayOfWeek.class, Map.class, false);
    checkAssignable(DayOfWeek.class, HashMap.class, false);
    checkAssignable(DayOfWeek.class, myMapClass, false);
    checkAssignable(DayOfWeek.class, Serializable.class, true);
    checkAssignable(DayOfWeek.class, Enum.class, true);
    checkAssignable(DayOfWeek.class, TemporalAccessor.class, true);
    checkAssignable(DayOfWeek.class, StringBuilder.class, false);
  }

  private void checkAssignable(Class<?> target, Class<?> source, boolean b) {
    assertThat(ReflectUtil.mightBeAssignableFrom(target, source), is(b));
  }

  @Test void testHuman() {
    assertThat(Util.human(0D), equalTo("0"));
    assertThat(Util.human(1D), equalTo("1"));
    assertThat(Util.human(19D), equalTo("19"));
    assertThat(Util.human(198D), equalTo("198"));
    assertThat(Util.human(1000D), equalTo("1.00K"));
    assertThat(Util.human(1002D), equalTo("1.00K"));
    assertThat(Util.human(1009D), equalTo("1.01K"));
    assertThat(Util.human(1234D), equalTo("1.23K"));
    assertThat(Util.human(1987D), equalTo("1.99K"));
    assertThat(Util.human(1999D), equalTo("2.00K"));
    assertThat(Util.human(86837.2D), equalTo("86.8K"));
    assertThat(Util.human(868372.8D), equalTo("868K"));
    assertThat(Util.human(1009000D), equalTo("1.01M"));
    assertThat(Util.human(1999999D), equalTo("2.00M"));
    assertThat(Util.human(1009000000D), equalTo("1.01G"));
    assertThat(Util.human(1999999000D), equalTo("2.00G"));

    assertThat(Util.human(-1D), equalTo("-1"));
    assertThat(Util.human(-19D), equalTo("-19"));
    assertThat(Util.human(-198D), equalTo("-198"));
    assertThat(Util.human(-1999999000D), equalTo("-2.00G"));

    // not ideal - should use m (milli) and u (micro)
    assertThat(Util.human(0.18D), equalTo("0.18"));
    assertThat(Util.human(0.018D), equalTo("0.018"));
    assertThat(Util.human(0.0018D), equalTo("0.0018"));
    assertThat(Util.human(0.00018D), equalTo("1.8E-4"));
    assertThat(Util.human(0.000018D), equalTo("1.8E-5"));
    assertThat(Util.human(0.0000018D), equalTo("1.8E-6"));

    // bad - should round to 3 digits
    assertThat(Util.human(0.181111D), equalTo("0.181111"));
    assertThat(Util.human(0.0181111D), equalTo("0.0181111"));
    assertThat(Util.human(0.00181111D), equalTo("0.00181111"));
    assertThat(Util.human(0.000181111D), equalTo("1.81111E-4"));
    assertThat(Util.human(0.0000181111D), equalTo("1.81111E-5"));
    assertThat(Util.human(0.00000181111D), equalTo("1.81111E-6"));

  }

  /** Tests {@link Util#immutableCopy(Iterable)}. */
  @Test void testImmutableCopy() {
    final List<Integer> list3 = Arrays.asList(1, 2, 3);
    final List<Integer> immutableList3 = ImmutableList.copyOf(list3);
    final List<Integer> list0 = Arrays.asList();
    final List<Integer> immutableList0 = ImmutableList.copyOf(list0);
    final List<Integer> list1 = Arrays.asList(1);
    final List<Integer> immutableList1 = ImmutableList.copyOf(list1);

    final List<List<Integer>> list301 = Arrays.asList(list3, list0, list1);
    final List<List<Integer>> immutableList301 = Util.immutableCopy(list301);
    assertThat(immutableList301, hasSize(3));
    assertThat(immutableList301, is(list301));
    assertThat(immutableList301, not(sameInstance(list301)));
    for (List<Integer> list : immutableList301) {
      assertThat(list, isA((Class) ImmutableList.class));
    }

    // if you copy the copy, you get the same instance
    final List<List<Integer>> immutableList301b =
        Util.immutableCopy(immutableList301);
    assertThat(immutableList301b, sameInstance(immutableList301));
    assertThat(immutableList301b, not(sameInstance(list301)));

    // if the elements of the list are immutable lists, they are not copied
    final List<List<Integer>> list301c =
        Arrays.asList(immutableList3, immutableList0, immutableList1);
    final List<List<Integer>> list301d = Util.immutableCopy(list301c);
    assertThat(list301d, hasSize(3));
    assertThat(list301d, is(list301));
    assertThat(list301d, not(sameInstance(list301)));
    assertThat(list301d.get(0), sameInstance(immutableList3));
    assertThat(list301d.get(1), sameInstance(immutableList0));
    assertThat(list301d.get(2), sameInstance(immutableList1));
  }

  @Test void testAsIndexView() {
    final List<String> values  = Lists.newArrayList("abCde", "X", "y");
    final Map<String, String> map =
        Util.asIndexMapJ(values, input -> input.toUpperCase(Locale.ROOT));
    assertThat(map, aMapWithSize(values.size()));
    assertThat(map.get("X"), equalTo("X"));
    assertThat(map.get("Y"), equalTo("y"));
    assertThat(map.get("y"), is((String) null));
    assertThat(map.get("ABCDE"), equalTo("abCde"));

    // If you change the values collection, the map changes.
    values.remove(1);
    assertThat(map, aMapWithSize(values.size()));
    assertThat(map.get("X"), is((String) null));
    assertThat(map.get("Y"), equalTo("y"));
  }

  @Test void testRelBuilderExample() {
    new RelBuilderExample(false).runAllExamples();
  }

  @Test void testOrdReverse() {
    checkOrdReverse(Ord.reverse(Arrays.asList("a", "b", "c")));
    checkOrdReverse(Ord.reverse("a", "b", "c"));
    assertThat(Ord.reverse(ImmutableList.<String>of()).iterator().hasNext(),
        is(false));
    assertThat(Ord.reverse().iterator().hasNext(), is(false));
  }

  private void checkOrdReverse(Iterable<Ord<String>> reverse1) {
    final Iterator<Ord<String>> reverse = reverse1.iterator();
    assertThat(reverse.hasNext(), is(true));
    assertThat(reverse.next().i, is(2));
    assertThat(reverse.hasNext(), is(true));
    assertThat(reverse.next().e, is("b"));
    assertThat(reverse.hasNext(), is(true));
    assertThat(reverse.next().e, is("a"));
    assertThat(reverse.hasNext(), is(false));
  }

  /** Tests {@link Ord#forEach(Iterable, ObjIntConsumer)}. */
  @Test void testOrdForEach() {
    final String[] strings = {"ab", "", "cde"};
    final StringBuilder b = new StringBuilder();
    final String expected = "0:ab;1:;2:cde;";

    Ord.forEach(strings,
        (e, i) -> b.append(i).append(":").append(e).append(";"));
    assertThat(b, hasToString(expected));
    b.setLength(0);

    final List<String> list = Arrays.asList(strings);
    Ord.forEach(list, (e, i) -> b.append(i).append(":").append(e).append(";"));
    assertThat(b, hasToString(expected));
  }

  /** Tests {@link org.apache.calcite.util.ReflectUtil#getParameterName}. */
  @Test void testParameterName() throws NoSuchMethodException {
    final Method method = UtilTest.class.getMethod("foo", int.class, int.class);
    assertThat(ReflectUtil.getParameterName(method, 0), is("arg0"));
    assertThat(ReflectUtil.getParameterName(method, 1), is("j"));
  }

  /** Dummy method for {@link #testParameterName()} to inspect. */
  public static void foo(int i, @Parameter(name = "j") int j) {
  }

  @Test void testListToString() {
    checkListToString("x");
    checkListToString("");
    checkListToString();
    checkListToString("ab", "c", "");
    checkListToString("ab", "c", "", "de");
    checkListToString("ab", "c.");
    checkListToString("ab", "c.d");
    checkListToString("ab", ".d");
    checkListToString(".ab", "d");
    checkListToString(".a", "d");
    checkListToString("a.", "d");
  }

  private void checkListToString(String... strings) {
    final List<String> list = ImmutableList.copyOf(strings);
    final String asString = Util.listToString(list);
    assertThat(Util.stringToList(asString), is(list));
  }

  /** Tests {@link org.apache.calcite.util.TryThreadLocal}.
   *
   * <p>TryThreadLocal was introduced to fix
   * <a href="https://issues.apache.org/jira/browse/CALCITE-915">[CALCITE-915]
   * Tests do not unset ThreadLocal values on exit</a>. */
  @Test void testTryThreadLocal() {
    final TryThreadLocal<String> local0 = TryThreadLocal.ofNonNull("foo");
    assertThat(local0.get(), is("foo"));
    TryThreadLocal.Memo memo0 = local0.push("bar");
    assertThat(local0.get(), is("bar"));
    local0.set("baz");
    assertThat(local0.get(), is("baz"));
    memo0.close();
    assertThat(local0.get(), is("foo"));
    try {
      local0.set(null);
      fail("expected err");
    } catch (NullPointerException e) {
      // ok
    }

    final TryThreadLocal<String> local1 = TryThreadLocal.of("foo");
    assertThat(local1.get(), is("foo"));
    TryThreadLocal.Memo memo1 = local1.push("bar");
    assertThat(local1.get(), is("bar"));
    local1.set("baz");
    assertThat(local1.get(), is("baz"));
    memo1.close();
    assertThat(local1.get(), is("foo"));
    local1.set(null); // null values are allowed

    final TryThreadLocal<@Nullable String> local2 =
        TryThreadLocal.of(null);
    assertThat(local2.get(), nullValue());
    TryThreadLocal.Memo memo2 = local2.push("a");
    assertThat(local2.get(), is("a"));
    local2.set("b");
    assertThat(local2.get(), is("b"));
    TryThreadLocal.Memo memo2B = local2.push(null);
    assertThat(local2.get(), nullValue());
    memo2B.close();
    assertThat(local2.get(), is("b"));
    memo2.close();
    assertThat(local2.get(), nullValue());

    local2.set("x");
    try (TryThreadLocal.Memo ignore = local2.push("y")) {
      assertThat(local2.get(), is("y"));
      local2.set("z");
    }
    assertThat(local2.get(), is("x"));

    final Supplier<@NonNull String> stringSupplier =
        new Supplier<String>() {
      final Random random = new Random();

      @Override public String get() {
        return "s" + random.nextInt(15);
      }
    };
    final TryThreadLocal<String> local3 =
        TryThreadLocal.withInitial(stringSupplier);
    assertThat(local3.get(), startsWith("s"));
    TryThreadLocal.Memo memo3 = local3.push("bar");
    assertThat(local3.get(), is("bar"));
    local3.set("baz");
    assertThat(local3.get(), is("baz"));
    memo3.close();
    assertThat(local3.get(), startsWith("s"));
    try {
      local3.set(null);
      fail("expected err");
    } catch (NullPointerException e) {
      assertThat(e, notNullValue());
    }

    @SuppressWarnings("DataFlowIssue")
    final Supplier<@NonNull String> nullSupplier = () -> null;
    final TryThreadLocal<String> local4 =
        TryThreadLocal.withInitial(nullSupplier);
    local4.set("abc");
    assertThat(local4.get(), is("abc"));
    local4.remove();
    try {
      final String s = local4.get();
      fail("expected error, got " + s);
    } catch (NullPointerException e) {
      assertThat(e, notNullValue());
    }
  }

  /** Tests
   * {@link org.apache.calcite.util.TryThreadLocal#letIn(Object, Runnable)}
   * and
   * {@link org.apache.calcite.util.TryThreadLocal#letIn(Object, java.util.function.Supplier)}. */
  @Test void testTryThreadLocalLetIn() {
    final TryThreadLocal<Integer> local = TryThreadLocal.of(2);
    String s3 = local.letIn(3, () -> "the value is " + local.get());
    assertThat(s3, is("the value is 3"));
    assertThat(local.get(), is(2));

    String s2 = local.letIn(2, () -> "the value is " + local.get());
    assertThat(s2, is("the value is 2"));
    assertThat(local.get(), is(2));

    final StringBuilder sb = new StringBuilder();
    local.letIn(4, () -> sb.append("the value is ").append(local.get()));
    assertThat(sb, hasToString("the value is 4"));
    assertThat(local.get(), is(2));

    // even when the Runnable throws, the value is restored
    local.set(10);
    sb.setLength(0);
    try {
      local.letIn(5, () -> {
        sb.append("the value is ").append(local.get());
        throw new IllegalArgumentException("oops");
      });
      fail("expected exception");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), is("oops"));
    }
    assertThat(sb, hasToString("the value is 5"));
    assertThat(local.get(), is(10));
    local.remove();
    assertThat(local.get(), is(2));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1264">[CALCITE-1264]
   * Litmus argument interpolation</a>. */
  @Test void testLitmus() {
    boolean b = checkLitmus(2, Litmus.THROW);
    assertThat(b, is(true));
    b = checkLitmus(2, Litmus.IGNORE);
    assertThat(b, is(true));
    try {
      b = checkLitmus(-1, Litmus.THROW);
      fail("expected fail, got " + b);
    } catch (AssertionError e) {
      assertThat(e.getMessage(), is("-1 is less than 0"));
    }
    b = checkLitmus(-1, Litmus.IGNORE);
    assertThat(b, is(false));
  }

  private boolean checkLitmus(int i, Litmus litmus) {
    if (i < 0) {
      return litmus.fail("{} is less than {}", i, 0);
    } else {
      return litmus.succeed();
    }
  }

  /** Unit test for {@link org.apache.calcite.util.NameSet}. */
  @Test void testNameSet() {
    final NameSet names = new NameSet();
    assertThat(names.contains("foo", true), is(false));
    assertThat(names.contains("foo", false), is(false));
    names.add("baz");
    assertThat(names.contains("foo", true), is(false));
    assertThat(names.contains("foo", false), is(false));
    assertThat(names.contains("baz", true), is(true));
    assertThat(names.contains("baz", false), is(true));
    assertThat(names.contains("BAZ", true), is(false));
    assertThat(names.contains("BAZ", false), is(true));
    assertThat(names.contains("bAz", false), is(true));
    assertThat(names.range("baz", true), hasSize(1));
    assertThat(names.range("baz", false), hasSize(1));
    assertThat(names.range("BAZ", true), hasSize(0));
    assertThat(names.range("BaZ", true), hasSize(0));
    assertThat(names.range("BaZ", false), hasSize(1));
    assertThat(names.range("BAZ", false), hasSize(1));

    assertThat(names.contains("bAzinga", false), is(false));
    assertThat(names.range("bAzinga", true), hasSize(0));
    assertThat(names.range("bAzinga", false), hasSize(0));

    assertThat(names.contains("zoo", true), is(false));
    assertThat(names.contains("zoo", false), is(false));
    assertThat(names.range("zoo", true), hasSize(0));

    assertThat(Iterables.size(names.iterable()), is(1));
    names.add("Baz");
    names.add("Abcde");
    names.add("WOMBAT");
    names.add("Zymurgy");
    assertThat(names, hasToString("[Abcde, Baz, baz, WOMBAT, Zymurgy]"));
    assertThat(Iterables.size(names.iterable()), is(5));
    assertThat(names.range("baz", false), hasSize(2));
    assertThat(names.range("baz", true), hasSize(1));
    assertThat(names.range("BAZ", true), hasSize(0));
    assertThat(names.range("Baz", true), hasSize(1));
    assertThat(names.contains("baz", true), is(true));
    assertThat(names.contains("baz", false), is(true));
    assertThat(names.contains("BAZ", true), is(false));
    assertThat(names.contains("BAZ", false), is(true));
    assertThat(names.contains("abcde", true), is(false));
    assertThat(names.contains("abcde", false), is(true));
    assertThat(names.contains("ABCDE", true), is(false));
    assertThat(names.contains("ABCDE", false), is(true));
    assertThat(names.contains("wombat", true), is(false));
    assertThat(names.contains("wombat", false), is(true));
    assertThat(names.contains("womBat", true), is(false));
    assertThat(names.contains("womBat", false), is(true));
    assertThat(names.contains("WOMBAT", true), is(true));
    assertThat(names.contains("WOMBAT", false), is(true));
    assertThat(names.contains("zyMurgy", true), is(false));
    assertThat(names.contains("zyMurgy", false), is(true));

    // [CALCITE-2481] NameSet assumes lowercase characters have greater codes
    // which does not hold for certain characters
    checkCase0("a");
    checkCase0("\u00b5"); // "µ"
  }

  private void checkCase0(String s) {
    checkCase1(s);
    checkCase1(s.toUpperCase(Locale.ROOT));
    checkCase1(s.toLowerCase(Locale.ROOT));
    checkCase1("a" + s + "z");
  }

  private void checkCase1(String s) {
    final NameSet set = new NameSet();
    set.add(s);
    checkNameSet(s, set);

    set.add("");
    checkNameSet(s, set);

    set.add("zzz");
    checkNameSet(s, set);

    final NameMap<Integer> map = new NameMap<>();
    map.put(s, 1);
    checkNameMap(s, map);

    map.put("", 11);
    checkNameMap(s, map);

    map.put("zzz", 21);
    checkNameMap(s, map);

    final NameMultimap<Integer> multimap = new NameMultimap<>();
    multimap.put(s, 1);
    checkNameMultimap(s, multimap);

    multimap.put("", 11);
    checkNameMultimap(s, multimap);

    multimap.put("zzz", 21);
    checkNameMultimap(s, multimap);
  }

  private void checkNameSet(String s, NameSet set) {
    final String upper = s.toUpperCase(Locale.ROOT);
    final String lower = s.toLowerCase(Locale.ROOT);
    final boolean isUpper = upper.equals(s);
    final boolean isLower = lower.equals(s);
    assertThat(set.contains(s, true), is(true));
    assertThat(set.contains(s, false), is(true));
    assertThat(set.contains(upper, false), is(true));
    assertThat(set.contains(upper, true), is(isUpper));
    assertThat(set.contains(lower, false), is(true));
    assertThat(set.contains(lower, true), is(isLower));

    // Create a copy of NameSet, to avoid polluting further tests
    final NameSet set2 = new NameSet();
    for (String name : set.iterable()) {
      set2.add(name);
    }
    set2.add(upper);
    set2.add(lower);
    final Collection<String> rangeInsensitive = set2.range(s, false);
    assertThat(rangeInsensitive.contains(s), is(true));
    assertThat(rangeInsensitive.contains(upper), is(true));
    assertThat(rangeInsensitive.contains(lower), is(true));
    final Collection<String> rangeSensitive = set2.range(s, true);
    assertThat(rangeSensitive.contains(s), is(true));
    assertThat(rangeSensitive.contains(upper), is(isUpper));
    assertThat(rangeSensitive.contains(lower), is(isLower));
  }

  private void checkNameMap(String s, NameMap<Integer> map) {
    final String upper = s.toUpperCase(Locale.ROOT);
    final String lower = s.toLowerCase(Locale.ROOT);
    boolean isUpper = upper.equals(s);
    boolean isLower = lower.equals(s);
    assertThat(map.containsKey(s, true), is(true));
    assertThat(map.containsKey(s, false), is(true));
    assertThat(map.containsKey(upper, false), is(true));
    assertThat(map.containsKey(upper, true), is(isUpper));
    assertThat(map.containsKey(lower, false), is(true));
    assertThat(map.containsKey(lower, true), is(isLower));

    // Create a copy of NameMap, to avoid polluting further tests
    final NameMap<Integer> map2 = new NameMap<>();
    for (Map.Entry<String, Integer> entry : map.map().entrySet()) {
      map2.put(entry.getKey(), entry.getValue());
    }
    map2.put(upper, 2);
    map2.put(lower, 3);
    final NavigableMap<String, Integer> rangeInsensitive =
        map2.range(s, false);
    assertThat(rangeInsensitive.containsKey(s), is(true));
    assertThat(rangeInsensitive.containsKey(upper), is(true));
    assertThat(rangeInsensitive.containsKey(lower), is(true));
    final NavigableMap<String, Integer> rangeSensitive = map2.range(s, true);
    assertThat(rangeSensitive.containsKey(s), is(true));
    assertThat(rangeSensitive.containsKey(upper), is(isUpper));
    assertThat(rangeSensitive.containsKey(lower), is(isLower));
  }

  private void checkNameMultimap(String s, NameMultimap<Integer> map) {
    final String upper = s.toUpperCase(Locale.ROOT);
    final String lower = s.toLowerCase(Locale.ROOT);
    boolean isUpper = upper.equals(s);
    boolean isLower = lower.equals(s);
    assertThat(map.containsKey(s, true), is(true));
    assertThat(map.containsKey(s, false), is(true));
    assertThat(map.containsKey(upper, false), is(true));
    assertThat(map.containsKey(upper, true), is(isUpper));
    assertThat(map.containsKey(lower, false), is(true));
    assertThat(map.containsKey(lower, true), is(isLower));

    // Create a copy of NameMultimap, to avoid polluting further tests
    final NameMap<Integer> map2 = new NameMap<>();
    for (Map.Entry<String, List<Integer>> entry : map.map().entrySet()) {
      for (Integer integer : entry.getValue()) {
        map2.put(entry.getKey(), integer);
      }
    }
    map2.put(upper, 2);
    map2.put(lower, 3);
    final NavigableMap<String, Integer> rangeInsensitive =
        map2.range(s, false);
    assertThat(rangeInsensitive.containsKey(s), is(true));
    assertThat(rangeInsensitive.containsKey(upper), is(true));
    assertThat(rangeInsensitive.containsKey(lower), is(true));
    final NavigableMap<String, Integer> rangeSensitive = map2.range(s, true);
    assertThat(rangeSensitive.containsKey(s), is(true));
    assertThat(rangeSensitive.containsKey(upper), is(isUpper));
    assertThat(rangeSensitive.containsKey(lower), is(isLower));
  }

  /** Unit test for {@link org.apache.calcite.util.NameMap}. */
  @Test void testNameMap() {
    final NameMap<Integer> map = new NameMap<>();
    assertThat(map.containsKey("foo", true), is(false));
    assertThat(map.containsKey("foo", false), is(false));
    map.put("baz", 0);
    assertThat(map.containsKey("foo", true), is(false));
    assertThat(map.containsKey("foo", false), is(false));
    assertThat(map.containsKey("baz", true), is(true));
    assertThat(map.containsKey("baz", false), is(true));
    assertThat(map.containsKey("BAZ", true), is(false));
    assertThat(map.containsKey("BAZ", false), is(true));
    assertThat(map.containsKey("bAz", false), is(true));
    assertThat(map.range("baz", true), aMapWithSize(1));
    assertThat(map.range("baz", false), aMapWithSize(1));
    assertThat(map.range("BAZ", true), aMapWithSize(0));
    assertThat(map.range("BaZ", true), aMapWithSize(0));
    assertThat(map.range("BaZ", false), aMapWithSize(1));
    assertThat(map.range("BAZ", false), aMapWithSize(1));

    assertThat(map.containsKey("bAzinga", false), is(false));
    assertThat(map.range("bAzinga", true), aMapWithSize(0));
    assertThat(map.range("bAzinga", false), aMapWithSize(0));

    assertThat(map.containsKey("zoo", true), is(false));
    assertThat(map.containsKey("zoo", false), is(false));
    assertThat(map.range("zoo", true), aMapWithSize(0));

    assertThat(map.map(), aMapWithSize(1));
    map.put("Baz", 1);
    map.put("Abcde", 2);
    map.put("WOMBAT", 4);
    map.put("Zymurgy", 3);
    assertThat(map,
        hasToString("{Abcde=2, Baz=1, baz=0, WOMBAT=4, Zymurgy=3}"));
    assertThat(map.map(), aMapWithSize(5));
    assertThat(map.map().entrySet(), hasSize(5));
    assertThat(map.map().keySet(), hasSize(5));
    assertThat(map.range("baz", false), aMapWithSize(2));
    assertThat(map.range("baz", true), aMapWithSize(1));
    assertThat(map.range("BAZ", true), aMapWithSize(0));
    assertThat(map.range("Baz", true), aMapWithSize(1));
    assertThat(map.containsKey("baz", true), is(true));
    assertThat(map.containsKey("baz", false), is(true));
    assertThat(map.containsKey("BAZ", true), is(false));
    assertThat(map.containsKey("BAZ", false), is(true));
    assertThat(map.containsKey("abcde", true), is(false));
    assertThat(map.containsKey("abcde", false), is(true));
    assertThat(map.containsKey("ABCDE", true), is(false));
    assertThat(map.containsKey("ABCDE", false), is(true));
    assertThat(map.containsKey("wombat", true), is(false));
    assertThat(map.containsKey("wombat", false), is(true));
    assertThat(map.containsKey("womBat", false), is(true));
    assertThat(map.containsKey("zyMurgy", true), is(false));
    assertThat(map.containsKey("zyMurgy", false), is(true));
  }

  /** Unit test for {@link org.apache.calcite.util.NameMultimap}. */
  @Test void testNameMultimap() {
    final NameMultimap<Integer> map = new NameMultimap<>();
    assertThat(map.containsKey("foo", true), is(false));
    assertThat(map.containsKey("foo", false), is(false));
    map.put("baz", 0);
    map.put("baz", 0);
    map.put("BAz", 0);
    assertThat(map.containsKey("foo", true), is(false));
    assertThat(map.containsKey("foo", false), is(false));
    assertThat(map.containsKey("baz", true), is(true));
    assertThat(map.containsKey("baz", false), is(true));
    assertThat(map.containsKey("BAZ", true), is(false));
    assertThat(map.containsKey("BAZ", false), is(true));
    assertThat(map.containsKey("bAz", false), is(true));
    assertThat(map.range("baz", true), hasSize(2));
    assertThat(map.range("baz", false), hasSize(3));
    assertThat(map.range("BAZ", true), hasSize(0));
    assertThat(map.range("BaZ", true), hasSize(0));
    assertThat(map.range("BaZ", false), hasSize(3));
    assertThat(map.range("BAZ", false), hasSize(3));

    assertThat(map.containsKey("bAzinga", false), is(false));
    assertThat(map.range("bAzinga", true), hasSize(0));
    assertThat(map.range("bAzinga", false), hasSize(0));

    assertThat(map.containsKey("zoo", true), is(false));
    assertThat(map.containsKey("zoo", false), is(false));
    assertThat(map.range("zoo", true), hasSize(0));

    assertThat(map.map(), aMapWithSize(2));
    map.put("Baz", 1);
    map.put("Abcde", 2);
    map.put("WOMBAT", 4);
    map.put("Zymurgy", 3);
    final String expected = "{Abcde=[2], BAz=[0], Baz=[1], baz=[0, 0],"
        + " WOMBAT=[4], Zymurgy=[3]}";
    assertThat(map, hasToString(expected));
    assertThat(map.map(), aMapWithSize(6));
    assertThat(map.map().entrySet(), hasSize(6));
    assertThat(map.map().keySet(), hasSize(6));
    assertThat(map.range("baz", false), hasSize(4));
    assertThat(map.range("baz", true), hasSize(2));
    assertThat(map.range("BAZ", true), hasSize(0));
    assertThat(map.range("Baz", true), hasSize(1));
    assertThat(map.containsKey("baz", true), is(true));
    assertThat(map.containsKey("baz", false), is(true));
    assertThat(map.containsKey("BAZ", true), is(false));
    assertThat(map.containsKey("BAZ", false), is(true));
    assertThat(map.containsKey("abcde", true), is(false));
    assertThat(map.containsKey("abcde", false), is(true));
    assertThat(map.containsKey("ABCDE", true), is(false));
    assertThat(map.containsKey("ABCDE", false), is(true));
    assertThat(map.containsKey("wombat", true), is(false));
    assertThat(map.containsKey("wombat", false), is(true));
    assertThat(map.containsKey("womBat", false), is(true));
    assertThat(map.containsKey("zyMurgy", true), is(false));
    assertThat(map.containsKey("zyMurgy", false), is(true));
  }

  /** Test {@link MonotonicSupplier}. */
  @Test void testMonotonicSupplier() {
    final MonotonicSupplier<String> monotonicSupplier =
        new MonotonicSupplier<>();

    // Cannot 'get' before 'accept'
    try {
      final String s = monotonicSupplier.get();
      fail("expected error, got " + s);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), is("accept has not been called"));
    }

    // Does not accept null
    try {
      //noinspection ConstantConditions
      monotonicSupplier.accept(null);
      fail("expected error");
    } catch (NullPointerException e) {
      assertThat(e.getMessage(), is("element must not be null"));
    }

    monotonicSupplier.accept("hello");

    // After 'accept' we can call 'get' multiple times
    final String s = monotonicSupplier.get();
    assertThat(s, is("hello"));

    final String s2 = monotonicSupplier.get();
    assertThat(s2, is("hello"));

    // Does not 'accept' twice
    try {
      monotonicSupplier.accept("goodbye");
      fail("expected error");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), is("accept has been called already"));
    }

    final String s3 = monotonicSupplier.get();
    assertThat(s3, is("hello"));
  }

  @Test void testNlsStringClone() {
    final NlsString s = new NlsString("foo", "LATIN1", SqlCollation.IMPLICIT);
    assertThat(s, hasToString("_LATIN1'foo'"));
    final Object s2 = s.clone();
    assertThat(s2, instanceOf(NlsString.class));
    assertThat(s2, not(sameInstance((Object) s)));
    assertThat(s2, hasToString(s.toString()));
  }

  @Test void testXmlOutput() {
    final StringWriter w = new StringWriter();
    final XmlOutput o = new XmlOutput(w);
    o.beginBeginTag("root");
    o.attribute("a1", "v1");
    o.attribute("a2", null);
    o.endBeginTag("root");
    o.beginTag("someText", null);
    o.content("line 1 followed by empty line\n"
        + "\n"
        + "line 3 with windows line ending\r\n"
        + "line 4 with no ending");
    o.endTag("someText");
    o.endTag("root");
    final String s = w.toString();
    final String expected = ""
        + "<root a1=\"v1\">\n"
        + "\t<someText>\n"
        + "\t\t\tline 1 followed by empty line\n"
        + "\t\t\t\n"
        + "\t\t\tline 3 with windows line ending\n"
        + "\t\t\tline 4 with no ending\n"
        + "\t</someText>\n"
        + "</root>\n";
    assertThat(Util.toLinux(s), is(expected));
  }

  /** Unit test for {@link Matchers#compose}. */
  @Test void testComposeMatcher() {
    final Function<String, String> toUpper = s -> s.toUpperCase(Locale.ROOT);
    assertThat(Unsafe.matches(Matchers.compose(is("A"), toUpper), "a"), is(true));
    assertThat(Unsafe.matches(Matchers.compose(is("A"), toUpper), "A"), is(true));
    assertThat(Unsafe.matches(Matchers.compose(is("a"), toUpper), "A"), is(false));
    assertThat(describe(Matchers.compose(is("a"), toUpper)), is("is \"a\""));
    assertThat(mismatchDescription(Matchers.compose(is("a"), toUpper), "A"),
        is("was \"A\""));
  }

  /** Unit test for {@link Matchers#isLinux}. */
  @Test void testIsLinux() {
    assertThat("xy", isLinux("xy"));
    assertThat("x\ny", isLinux("x\ny"));
    assertThat("x\r\ny", isLinux("x\ny"));
    assertThat(Unsafe.matches(isLinux("x"), "x"), is(true));
    assertThat(Unsafe.matches(isLinux("X"), "x"), is(false));
    assertThat(mismatchDescription(isLinux("X"), "x"), is("was \"x\""));
    assertThat(describe(isLinux("X")), is("is \"X\""));
    assertThat(Unsafe.matches(isLinux("x\ny"), "x\ny"), is(true));
    assertThat(Unsafe.matches(isLinux("x\ny"), "x\r\ny"), is(true));
    // "\n\r" is not a valid windows line ending
    assertThat(Unsafe.matches(isLinux("x\ny"), "x\n\ry"), is(false));
    assertThat(Unsafe.matches(isLinux("x\ny"), "x\n\ryz"), is(false));
    // left-hand side must be linux or will never match
    assertThat(Unsafe.matches(isLinux("x\r\ny"), "x\r\ny"), is(false));
    assertThat(Unsafe.matches(isLinux("x\r\ny"), "x\ny"), is(false));
  }

  /** Tests {@link Util#andThen(UnaryOperator, UnaryOperator)}. */
  @Test void testAndThen() {
    final UnaryOperator<Integer> inc = x -> x + 1;
    final UnaryOperator<Integer> triple = x -> x * 3;
    final UnaryOperator<Integer> tripleInc = Util.andThen(triple, inc);
    final UnaryOperator<Integer> incTriple = Util.andThen(inc, triple);
    final Function<Integer, Integer> incTripleFn = inc.andThen(triple);
    assertThat(tripleInc.apply(2), is(7));
    assertThat(incTriple.apply(2), is(9));
    assertThat(incTripleFn.apply(2), is(9));
  }

  /** Tests {@link Util#transform(List, java.util.function.Function)}
   * and {@link Util#transformIndexed(List, BiFunction)}. */
  @Test void testTransform() {
    final List<String> beatles =
        Arrays.asList("John", "Paul", "George", "Ringo");
    final List<String> empty = Collections.emptyList();
    assertThat(Util.transform(beatles, s -> s.toUpperCase(Locale.ROOT)),
        isListOf("JOHN", "PAUL", "GEORGE", "RINGO"));
    assertThat(Util.transform(empty, s -> s.toUpperCase(Locale.ROOT)), is(empty));
    assertThat(Util.transform(beatles, String::length),
        isListOf(4, 4, 6, 5));
    assertThat(Util.transform(beatles, String::length),
        instanceOf(RandomAccess.class));
    final List<String> beatles2 = new LinkedList<>(beatles);
    assertThat(Util.transform(beatles2, String::length),
        not(instanceOf(RandomAccess.class)));

    assertThat(Util.transformIndexed(beatles, (s, i) -> i + ": " + s),
        allOf(isListOf("0: John", "1: Paul", "2: George", "3: Ringo"),
            instanceOf(RandomAccess.class)));
    assertThat(Util.transformIndexed(beatles2, (s, i) -> i + ": " + s),
        allOf(isListOf("0: John", "1: Paul", "2: George", "3: Ringo"),
            not(instanceOf(RandomAccess.class))));
  }

  /** Tests {@link Util#filter(Iterable, java.util.function.Predicate)}. */
  @Test void testFilter() {
    final List<String> beatles =
        Arrays.asList("John", "Paul", "George", "Ringo");
    final List<String> empty = Collections.emptyList();
    final List<String> nullBeatles =
        Arrays.asList("John", "Paul", null, "Ringo");
    assertThat(Util.filter(beatles, s -> s.length() == 4),
        isIterable(Arrays.asList("John", "Paul")));
    assertThat(Util.filter(empty, s -> s.length() == 4), isIterable(empty));
    assertThat(Util.filter(empty, s -> false), isIterable(empty));
    assertThat(Util.filter(empty, s -> true), isIterable(empty));
    assertThat(Util.filter(beatles, s -> false), isIterable(empty));
    assertThat(Util.filter(beatles, s -> true), isIterable(beatles));
    assertThat(Util.filter(nullBeatles, s -> false), isIterable(empty));
    assertThat(Util.filter(nullBeatles, s -> true), isIterable(nullBeatles));
    assertThat(Util.filter(nullBeatles, Objects::isNull),
        isIterable(Collections.singletonList(null)));
    assertThat(Util.filter(nullBeatles, Objects::nonNull),
        isIterable(Arrays.asList("John", "Paul", "Ringo")));
  }

  /** Tests {@link Util#moveToHead(List, Predicate)}. */
  @Test void testMoveToHead() {
    final List<Integer> primes = ImmutableList.of(2, 3, 5, 7);
    final List<Integer> evenInMiddle = ImmutableList.of(1, 2, 3);
    final List<Integer> evenAtEnd = ImmutableList.of(1, 3, 8);
    final List<Integer> empty = ImmutableList.of();
    final List<Integer> evens = ImmutableList.of(0, 2, 4);
    final List<Integer> odds = ImmutableList.of(1, 3, 5);
    final Predicate<Integer> isEven = i -> i % 2 == 0;
    assertThat(Util.moveToHead(primes, isEven), hasToString("[2, 3, 5, 7]"));
    assertThat(Util.moveToHead(primes, isEven), sameInstance(primes));
    assertThat(Util.moveToHead(evenInMiddle, isEven),
        hasToString("[2, 1, 3]"));
    assertThat(Util.moveToHead(evenAtEnd, isEven), hasToString("[8, 1, 3]"));
    assertThat(Util.moveToHead(empty, isEven), hasToString("[]"));
    assertThat(Util.moveToHead(empty, isEven), sameInstance(empty));
    assertThat(Util.moveToHead(evens, isEven), hasToString("[0, 2, 4]"));
    assertThat(Util.moveToHead(evens, isEven), sameInstance(evens));
    assertThat(Util.moveToHead(odds, isEven), hasToString("[1, 3, 5]"));
    assertThat(Util.moveToHead(odds, isEven), sameInstance(odds));
  }

  /** Tests {@link Util#select(List, List)}. */
  @Test void testSelect() {
    final List<String> beatles =
        Arrays.asList("John", "Paul", "George", "Ringo");
    final List<String> nullBeatles =
        Arrays.asList("John", "Paul", null, "Ringo");

    final List<Integer> emptyOrdinals = Collections.emptyList();
    assertThat(Util.select(beatles, emptyOrdinals).isEmpty(), is(true));
    assertThat(Util.select(beatles, emptyOrdinals), hasToString("[]"));

    final List<Integer> ordinal0 = Collections.singletonList(0);
    assertThat(Util.select(beatles, ordinal0).isEmpty(), is(false));
    assertThat(Util.select(beatles, ordinal0), hasToString("[John]"));

    final List<Integer> ordinal20 = Arrays.asList(2, 0);
    assertThat(Util.select(beatles, ordinal20).isEmpty(), is(false));
    assertThat(Util.select(beatles, ordinal20),
        hasToString("[George, John]"));

    final List<Integer> ordinal232 = Arrays.asList(2, 3, 2);
    assertThat(Util.select(beatles, ordinal232).isEmpty(), is(false));
    assertThat(Util.select(beatles, ordinal232),
        hasToString("[George, Ringo, George]"));
    assertThat(Util.select(beatles, ordinal232),
        isIterable(Arrays.asList("George", "Ringo", "George")));

    assertThat(Util.select(nullBeatles, ordinal232).isEmpty(), is(false));
    assertThat(Util.select(nullBeatles, ordinal232),
        hasToString("[null, Ringo, null]"));
    assertThat(Util.select(nullBeatles, ordinal232),
        isIterable(Arrays.asList(null, "Ringo", null)));
  }

  /** Returns a Matcher that checks {@link EquivalenceSet#size()}. */
  private static <E extends Comparable<E>> Matcher<EquivalenceSet<E>>
      isEquivalenceSetWithSize(int n) {
    return isEquivalenceSetWithSize(is(n));
  }

  /** Returns a Matcher that checks {@link EquivalenceSet#size()}
   * against a Matcher. */
  private static <E extends Comparable<E>> Matcher<EquivalenceSet<E>>
      isEquivalenceSetWithSize(Matcher<Integer> matcher) {
    return new FeatureMatcher<EquivalenceSet<E>, Integer>(matcher,
        "EquivalenceSet", "size") {
      @Override protected Integer featureValueOf(EquivalenceSet<E> actual) {
        return actual.size();
      }
    };
  }

  @Test void testEquivalenceSet() {
    final EquivalenceSet<String> c = new EquivalenceSet<>();
    assertThat(c, isEquivalenceSetWithSize(0));
    assertThat(c.classCount(), is(0));
    c.add("abc");
    assertThat(c, isEquivalenceSetWithSize(1));
    assertThat(c.classCount(), is(1));
    c.add("Abc");
    assertThat(c, isEquivalenceSetWithSize(2));
    assertThat(c.classCount(), is(2));
    assertThat(c.areEquivalent("abc", "Abc"), is(false));
    assertThat(c.areEquivalent("abc", "abc"), is(true));
    assertThat(c.areEquivalent("abc", "ABC"), is(false));
    c.equiv("abc", "ABC");
    assertThat(c, isEquivalenceSetWithSize(3));
    assertThat(c.classCount(), is(2));
    assertThat(c.areEquivalent("abc", "ABC"), is(true));
    assertThat(c.areEquivalent("ABC", "abc"), is(true));
    assertThat(c.areEquivalent("abc", "abc"), is(true));
    assertThat(c.areEquivalent("abc", "Abc"), is(false));
    c.equiv("Abc", "ABC");
    assertThat(c, isEquivalenceSetWithSize(3));
    assertThat(c.classCount(), is(1));
    assertThat(c.areEquivalent("abc", "Abc"), is(true));

    c.add("de");
    c.equiv("fg", "fG");
    assertThat(c, isEquivalenceSetWithSize(6));
    assertThat(c.classCount(), is(3));
    final NavigableMap<String, SortedSet<String>> map = c.map();
    assertThat(map,
        hasToString("{ABC=[ABC, Abc, abc], de=[de], fG=[fG, fg]}"));

    c.clear();
    assertThat(c, isEquivalenceSetWithSize(0));
    assertThat(c.classCount(), is(0));
  }

  @Test void testBlackHoleMap() {
    final Map<Integer, Integer> map = BlackholeMap.of();

    for (int i = 0; i < 100; i++) {
      assertThat(map.put(i, i * i), is(nullValue()));
      assertThat(map, aMapWithSize(0));
      assertThat(map.entrySet().add(new SimpleEntry<>(i, i * i)), is(true));
      assertThat(map.entrySet(), hasSize(0));
      assertThat(map.keySet(), hasSize(0));
      assertThat(map.values(), hasSize(0));
      assertThat(map.entrySet().iterator().hasNext(), is(false));
      try {
        map.entrySet().iterator().next();
        fail();
      } catch (NoSuchElementException e) {
        // Success
      }
    }
  }

  private static <E> Matcher<Iterable<E>> isIterable(final Iterable<E> iterable) {
    final List<E> list = toList(iterable);
    return new TypeSafeMatcher<Iterable<E>>() {
      protected boolean matchesSafely(Iterable<E> iterable) {
        return list.equals(toList(iterable));
      }

      public void describeTo(Description description) {
        description.appendText("is iterable ").appendValue(list);
      }
    };
  }

  private static <E> List<E> toList(Iterable<E> iterable) {
    final List<E> list = new ArrayList<>();
    for (E e : iterable) {
      list.add(e);
    }
    return list;
  }

  static String mismatchDescription(Matcher m, Object item) {
    final StringDescription d = new StringDescription();
    m.describeMismatch(item, d);
    return d.toString();
  }

  static String describe(Matcher m) {
    final StringDescription d = new StringDescription();
    m.describeTo(d);
    return d.toString();
  }
}
