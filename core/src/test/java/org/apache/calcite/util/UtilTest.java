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
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.runtime.FlatLists;
import org.apache.calcite.runtime.Resources;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.util.SqlBuilder;
import org.apache.calcite.sql.util.SqlString;
import org.apache.calcite.test.DiffTestCase;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import org.junit.BeforeClass;
import org.junit.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.lang.management.MemoryType;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TimeZone;
import java.util.TreeSet;
import javax.annotation.Nullable;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.isA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit test for {@link Util} and other classes in this package.
 */
public class UtilTest {
  //~ Constructors -----------------------------------------------------------

  public UtilTest() {
  }

  //~ Methods ----------------------------------------------------------------

  @BeforeClass public static void setUSLocale() {
    // This ensures numbers in exceptions are printed as in asserts.
    // For example, 1,000 vs 1 000
    Locale.setDefault(Locale.US);
  }

  @Test public void testPrintEquals() {
    assertPrintEquals("\"x\"", "x", true);
  }

  @Test public void testPrintEquals2() {
    assertPrintEquals("\"x\"", "x", false);
  }

  @Test public void testPrintEquals3() {
    assertPrintEquals("null", null, true);
  }

  @Test public void testPrintEquals4() {
    assertPrintEquals("", null, false);
  }

  @Test public void testPrintEquals5() {
    assertPrintEquals("\"\\\\\\\"\\r\\n\"", "\\\"\r\n", true);
  }

  @Test public void testScientificNotation() {
    BigDecimal bd;

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

  @Test public void testToJavaId() throws UnsupportedEncodingException {
    assertEquals(
        "ID$0$foo",
        Util.toJavaId("foo", 0));
    assertEquals(
        "ID$0$foo_20_bar",
        Util.toJavaId("foo bar", 0));
    assertEquals(
        "ID$0$foo__bar",
        Util.toJavaId("foo_bar", 0));
    assertEquals(
        "ID$100$_30_bar",
        Util.toJavaId("0bar", 100));
    assertEquals(
        "ID$0$foo0bar",
        Util.toJavaId("foo0bar", 0));
    assertEquals(
        "ID$0$it_27_s_20_a_20_bird_2c__20_it_27_s_20_a_20_plane_21_",
        Util.toJavaId("it's a bird, it's a plane!", 0));

    // Try some funny non-ASCII charsets
    assertEquals(
        "ID$0$_f6__cb__c4__ca__ae__c1__f9__cb_",
        Util.toJavaId(
            "\u00f6\u00cb\u00c4\u00ca\u00ae\u00c1\u00f9\u00cb",
            0));
    assertEquals(
        "ID$0$_f6cb__c4ca__aec1__f9cb_",
        Util.toJavaId("\uf6cb\uc4ca\uaec1\uf9cb", 0));
    byte[] bytes1 = {3, 12, 54, 23, 33, 23, 45, 21, 127, -34, -92, -113};
    assertEquals(
        "ID$0$_3__c_6_17__21__17__2d__15__7f__6cd9__fffd_",
        Util.toJavaId(
            new String(bytes1, "EUC-JP"),
            0));
    byte[] bytes2 = {
      64, 32, 43, -45, -23, 0, 43, 54, 119, -32, -56, -34
    };
    assertEquals(
        "ID$0$_30c__3617__2117__2d15__7fde__a48f_",
        Util.toJavaId(
            new String(bytes1, "UTF-16"),
            0));
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
    assertEquals(expect, out);
  }

  /**
   * Unit-test for {@link BitString}.
   */
  @Test public void testBitString() {
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
    assertEquals(
        "",
        b0.toHexString());
    assertEquals(
        "1",
        b1.toHexString());
    assertEquals(
        "2",
        b2.toHexString());
    assertEquals(
        "4",
        b4.toHexString());
    assertEquals(
        "8",
        b8.toHexString());
    assertEquals(
        "10",
        b16.toHexString());
    assertEquals(
        "20",
        b32.toHexString());
    assertEquals(
        "40",
        b64.toHexString());
    assertEquals(
        "80",
        b128.toHexString());
    assertEquals(
        "100",
        b256.toHexString());
    assertEquals(
        "0", b0x1.toHexString());
    assertEquals(
        "000", b0x12.toHexString());

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
  }

  private static void assertReversible(String s) {
    assertEquals(
        s,
        BitString.createFromBitString(s).toBitString(),
        s);
    assertEquals(
        s,
        BitString.createFromHexString(s).toHexString());
  }

  private void assertByteArray(
      String expected,
      String bits,
      int bitCount) {
    byte[] bytes = BitString.toByteArrayFromBitString(bits, bitCount);
    final String s = toString(bytes);
    assertEquals(expected, s);
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
  @Test public void testCastingList() {
    final List<Number> numberList = new ArrayList<Number>();
    numberList.add(new Integer(1));
    numberList.add(null);
    numberList.add(new Integer(2));
    List<Integer> integerList = Util.cast(numberList, Integer.class);
    assertEquals(3, integerList.size());
    assertEquals(new Integer(2), integerList.get(2));

    // Nulls are OK.
    assertNull(integerList.get(1));

    // Can update the underlying list.
    integerList.set(1, 345);
    assertEquals(new Integer(345), integerList.get(1));
    integerList.set(1, null);
    assertNull(integerList.get(1));

    // Can add a member of the wrong type to the underlying list.
    numberList.add(new Double(3.1415));
    assertEquals(4, integerList.size());

    // Access a member which is of the wrong type.
    try {
      integerList.get(3);
      fail("expected exception");
    } catch (ClassCastException e) {
      // ok
    }
  }

  @Test public void testIterableProperties() {
    Properties properties = new Properties();
    properties.put("foo", "george");
    properties.put("bar", "ringo");
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, String> entry : Util.toMap(properties).entrySet()) {
      sb.append(entry.getKey()).append("=").append(entry.getValue());
      sb.append(";");
    }
    assertEquals("bar=ringo;foo=george;", sb.toString());

    assertEquals(2, Util.toMap(properties).entrySet().size());

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

  /**
   * Tests the difference engine, {@link DiffTestCase#diff}.
   */
  @Test public void testDiffLines() {
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
  @Test public void testPosixTimeZone() {
    // NOTE jvs 31-July-2007:  First two tests are disabled since
    // not everyone may have patched their system yet for recent
    // DST change.

    // Pacific Standard Time. Effective 2007, the local time changes from
    // PST to PDT at 02:00 LST to 03:00 LDT on the second Sunday in March
    // and returns at 02:00 LDT to 01:00 LST on the first Sunday in
    // November.
    if (false) {
      assertEquals(
          "PST-8PDT,M3.2.0,M11.1.0",
          Util.toPosix(TimeZone.getTimeZone("PST"), false));

      assertEquals(
          "PST-8PDT1,M3.2.0/2,M11.1.0/2",
          Util.toPosix(TimeZone.getTimeZone("PST"), true));
    }

    // Tokyo has +ve offset, no DST
    assertEquals(
        "JST9",
        Util.toPosix(TimeZone.getTimeZone("Asia/Tokyo"), true));

    // Sydney, Australia lies ten hours east of GMT and makes a one hour
    // shift forward during daylight savings. Being located in the southern
    // hemisphere, daylight savings begins on the last Sunday in October at
    // 2am and ends on the last Sunday in March at 3am.
    // (Uses STANDARD_TIME time-transition mode.)

    // Because australia changed their daylight savings rules, some JVMs
    // have a different (older and incorrect) timezone settings for
    // Australia.  So we test for the older one first then do the
    // correct assert based upon what the toPosix method returns
    String posixTime =
        Util.toPosix(TimeZone.getTimeZone("Australia/Sydney"), true);

    if (posixTime.equals("EST10EST1,M10.5.0/2,M3.5.0/3")) {
      // very old JVMs without the fix
      assertEquals("EST10EST1,M10.5.0/2,M3.5.0/3", posixTime);
    } else if (posixTime.equals("EST10EST1,M10.1.0/2,M4.1.0/3")) {
      // old JVMs without the fix
      assertEquals("EST10EST1,M10.1.0/2,M4.1.0/3", posixTime);
    } else {
      // newer JVMs with the fix
      assertEquals("AEST10AEDT1,M10.1.0/2,M4.1.0/3", posixTime);
    }

    // Paris, France. (Uses UTC_TIME time-transition mode.)
    assertEquals(
        "CET1CEST1,M3.5.0/2,M10.5.0/3",
        Util.toPosix(TimeZone.getTimeZone("Europe/Paris"), true));

    assertEquals(
        "UTC0",
        Util.toPosix(TimeZone.getTimeZone("UTC"), true));
  }

  /**
   * Tests the methods {@link Util#enumConstants(Class)} and
   * {@link Util#enumVal(Class, String)}.
   */
  @Test public void testEnumConstants() {
    final Map<String, MemoryType> memoryTypeMap =
        Util.enumConstants(MemoryType.class);
    assertEquals(2, memoryTypeMap.size());
    assertEquals(MemoryType.HEAP, memoryTypeMap.get("HEAP"));
    assertEquals(MemoryType.NON_HEAP, memoryTypeMap.get("NON_HEAP"));
    try {
      memoryTypeMap.put("FOO", null);
      fail("expected exception");
    } catch (UnsupportedOperationException e) {
      // expected: map is immutable
    }

    assertEquals("HEAP", Util.enumVal(MemoryType.class, "HEAP").name());
    assertNull(Util.enumVal(MemoryType.class, "heap"));
    assertNull(Util.enumVal(MemoryType.class, "nonexistent"));
  }

  /**
   * Tests SQL builders.
   */
  @Test public void testSqlBuilder() {
    final SqlBuilder buf = new SqlBuilder(SqlDialect.CALCITE);
    assertEquals(0, buf.length());
    buf.append("select ");
    assertEquals("select ", buf.getSql());

    buf.identifier("x");
    assertEquals("select \"x\"", buf.getSql());

    buf.append(", ");
    buf.identifier("y", "a b");
    assertEquals("select \"x\", \"y\".\"a b\"", buf.getSql());

    final SqlString sqlString = buf.toSqlString();
    assertEquals(SqlDialect.CALCITE, sqlString.getDialect());
    assertEquals(buf.getSql(), sqlString.getSql());

    assertTrue(buf.getSql().length() > 0);
    assertEquals(buf.getSqlAndClear(), sqlString.getSql());
    assertEquals(0, buf.length());

    buf.clear();
    assertEquals(0, buf.length());

    buf.literal("can't get no satisfaction");
    assertEquals("'can''t get no satisfaction'", buf.getSqlAndClear());

    buf.literal(new Timestamp(0));
    assertEquals("TIMESTAMP '1970-01-01 00:00:00'", buf.getSqlAndClear());

    buf.clear();
    assertEquals(0, buf.length());

    buf.append("hello world");
    assertEquals(2, buf.indexOf("l"));
    assertEquals(-1, buf.indexOf("z"));
    assertEquals(9, buf.indexOf("l", 5));
  }

  /**
   * Unit test for {@link org.apache.calcite.util.CompositeList}.
   */
  @Test public void testCompositeList() {
    // Made up of zero lists
    //noinspection unchecked
    List<String> list = CompositeList.of(new List[0]);
    assertEquals(0, list.size());
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
    List<String> listEmpty2 = new ArrayList<String>();

    // Made up of three lists, two of which are empty
    list = CompositeList.of(listEmpty, listAbc, listEmpty2);
    assertEquals(3, list.size());
    assertFalse(list.isEmpty());
    assertEquals("a", list.get(0));
    assertEquals("c", list.get(2));
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
    assertEquals("a", iterator.next());
    assertEquals("b", iterator.next());
    assertTrue(iterator.hasNext());
    try {
      iterator.remove();
      fail("expected error");
    } catch (UnsupportedOperationException e) {
      // ok
    }
    assertEquals("c", iterator.next());
    assertFalse(iterator.hasNext());

    // Extend one of the backing lists, and list grows.
    listEmpty2.add("zz");
    assertEquals(4, list.size());
    assertEquals("zz", list.get(3));

    // Syntactic sugar 'of' method
    String ss = "";
    for (String s : CompositeList.of(list, list)) {
      ss += s;
    }
    assertEquals("abczzabczz", ss);
  }

  /**
   * Unit test for {@link Template}.
   */
  @Test public void testTemplate() {
    // Regular java message format.
    assertEquals(
        "Hello, world, what a nice day.",
        MessageFormat.format(
            "Hello, {0}, what a nice {1}.", "world", "day"));

    // Our extended message format. First, just strings.
    final HashMap<Object, Object> map = new HashMap<Object, Object>();
    map.put("person", "world");
    map.put("time", "day");
    assertEquals(
        "Hello, world, what a nice day.",
        Template.formatByName(
            "Hello, {person}, what a nice {time}.", map));

    // String and an integer.
    final Template template =
        Template.of("Happy {age,number,#.00}th birthday, {person}!");
    map.clear();
    map.put("person", "Ringo");
    map.put("age", 64.5);
    assertEquals(
        "Happy 64.50th birthday, Ringo!", template.format(map));

    // Missing parameters evaluate to null.
    map.remove("person");
    assertEquals(
        "Happy 64.50th birthday, null!",
        template.format(map));

    // Specify parameter by Integer ordinal.
    map.clear();
    map.put(1, "Ringo");
    map.put("0", 64.5);
    assertEquals(
        "Happy 64.50th birthday, Ringo!",
        template.format(map));

    // Too many parameters supplied.
    map.put("lastName", "Starr");
    map.put("homeTown", "Liverpool");
    assertEquals(
        "Happy 64.50th birthday, Ringo!",
        template.format(map));

    // Get parameter names. In order of appearance.
    assertEquals(
        Arrays.asList("age", "person"),
        template.getParameterNames());

    // No parameters; doubled single quotes; quoted braces.
    final Template template2 =
        Template.of("Don''t expand 'this {brace}'.");
    assertEquals(
        Collections.<String>emptyList(), template2.getParameterNames());
    assertEquals(
        "Don't expand this {brace}.",
        template2.format(Collections.<Object, Object>emptyMap()));

    // Empty template.
    assertEquals("", Template.formatByName("", map));
  }

  /**
   * Unit test for {@link Util#parseLocale(String)} method.
   */
  @Test public void testParseLocale() {
    Locale[] locales = {
      Locale.CANADA,
      Locale.CANADA_FRENCH,
      Locale.getDefault(),
      Locale.US,
      Locale.TRADITIONAL_CHINESE,
    };
    for (Locale locale : locales) {
      assertEquals(locale, Util.parseLocale(locale.toString()));
    }
    // Example locale names in Locale.toString() javadoc.
    String[] localeNames = {
      "en", "de_DE", "_GB", "en_US_WIN", "de__POSIX", "fr__MAC"
    };
    for (String localeName : localeNames) {
      assertEquals(localeName, Util.parseLocale(localeName).toString());
    }
  }

  @Test public void testSpaces() {
    assertEquals("", Spaces.of(0));
    assertEquals(" ", Spaces.of(1));
    assertEquals(" ", Spaces.of(1));
    assertEquals("         ", Spaces.of(9));
    assertEquals("     ", Spaces.of(5));
    assertEquals(1000, Spaces.of(1000).length());
  }

  @Test public void testSpaceString() {
    assertThat(Spaces.sequence(0).toString(), equalTo(""));
    assertThat(Spaces.sequence(1).toString(), equalTo(" "));
    assertThat(Spaces.sequence(9).toString(), equalTo("         "));
    assertThat(Spaces.sequence(5).toString(), equalTo("     "));
    String s =
        new StringBuilder().append("xx").append(Spaces.MAX, 0, 100)
            .toString();
    assertThat(s.length(), equalTo(102));

    // this would blow memory if the string were materialized... check that it
    // is not
    assertThat(Spaces.sequence(1000000000).length(), equalTo(1000000000));

    final StringWriter sw = new StringWriter();
    Spaces.append(sw, 4);
    assertThat(sw.toString(), equalTo("    "));

    final StringBuilder buf = new StringBuilder();
    Spaces.append(buf, 4);
    assertThat(buf.toString(), equalTo("    "));

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
  @Test public void testPairZip() {
    List<String> strings = Arrays.asList("paul", "george", "john", "ringo");
    List<Integer> integers = Arrays.asList(1942, 1943, 1940);
    List<Pair<String, Integer>> zip = Pair.zip(strings, integers);
    assertEquals(3, zip.size());
    assertEquals("paul:1942", zip.get(0).left + ":" + zip.get(0).right);
    assertEquals("john", zip.get(2).left);
    int x = 0;
    for (Pair<String, Integer> pair : zip) {
      x += pair.right;
    }
    assertEquals(5825, x);
  }

  /**
   * Unit test for {@link Pair#adjacents(Iterable)}.
   */
  @Test public void testPairAdjacents() {
    List<String> strings = Arrays.asList("a", "b", "c");
    List<String> result = new ArrayList<String>();
    for (Pair<String, String> pair : Pair.adjacents(strings)) {
      result.add(pair.toString());
    }
    assertThat(result.toString(), equalTo("[<a, b>, <b, c>]"));

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
  @Test public void testPairFirstAnd() {
    List<String> strings = Arrays.asList("a", "b", "c");
    List<String> result = new ArrayList<String>();
    for (Pair<String, String> pair : Pair.firstAnd(strings)) {
      result.add(pair.toString());
    }
    assertThat(result.toString(), equalTo("[<a, b>, <a, c>]"));

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
   * Unit test for {@link Util#quotientList(java.util.List, int, int)}.
   */
  @Test public void testQuotientList() {
    List<String> beatles = Arrays.asList("john", "paul", "george", "ringo");
    final List list0 = Util.quotientList(beatles, 3, 0);
    assertEquals(2, list0.size());
    assertEquals("john", list0.get(0));
    assertEquals("ringo", list0.get(1));

    final List list1 = Util.quotientList(beatles, 3, 1);
    assertEquals(1, list1.size());
    assertEquals("paul", list1.get(0));

    final List list2 = Util.quotientList(beatles, 3, 2);
    assertEquals(1, list2.size());
    assertEquals("george", list2.get(0));

    try {
      final List listBad = Util.quotientList(beatles, 3, 4);
      fail("Expected error, got " + listBad);
    } catch (IllegalArgumentException e) {
      // ok
    }
    try {
      final List listBad = Util.quotientList(beatles, 3, 3);
      fail("Expected error, got " + listBad);
    } catch (IllegalArgumentException e) {
      // ok
    }
    try {
      final List listBad = Util.quotientList(beatles, 0, 0);
      fail("Expected error, got " + listBad);
    } catch (IllegalArgumentException e) {
      // ok
    }

    // empty
    final List<String> empty = Collections.emptyList();
    final List<String> list3 = Util.quotientList(empty, 7, 2);
    assertEquals(0, list3.size());

    // shorter than n
    final List list4 = Util.quotientList(beatles, 10, 0);
    assertEquals(1, list4.size());
    assertEquals("john", list4.get(0));

    final List list5 = Util.quotientList(beatles, 10, 5);
    assertEquals(0, list5.size());
  }

  @Test public void testImmutableIntList() {
    final ImmutableIntList list = ImmutableIntList.of();
    assertEquals(0, list.size());
    assertEquals(list, Collections.<Integer>emptyList());
    assertThat(list.toString(), equalTo("[]"));
    assertThat(BitSets.of(list), equalTo(new BitSet()));

    final ImmutableIntList list2 = ImmutableIntList.of(1, 3, 5);
    assertEquals(3, list2.size());
    assertEquals("[1, 3, 5]", list2.toString());
    assertEquals(list2.hashCode(), Arrays.asList(1, 3, 5).hashCode());

    Integer[] integers = list2.toArray(new Integer[3]);
    assertEquals(1, (int) integers[0]);
    assertEquals(3, (int) integers[1]);
    assertEquals(5, (int) integers[2]);

    //noinspection EqualsWithItself
    assertThat(list.equals(list), is(true));
    assertThat(list.equals(list2), is(false));
    assertThat(list2.equals(list), is(false));
    //noinspection EqualsWithItself
    assertThat(list2.equals(list2), is(true));
  }

  /**
   * Unit test for {@link IntegerIntervalSet}.
   */
  @Test public void testIntegerIntervalSet() {
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

  private List<Integer> checkIntegerIntervalSet(String s, int... ints) {
    List<Integer> list = new ArrayList<Integer>();
    final Set<Integer> set = IntegerIntervalSet.of(s);
    assertEquals(set.size(), ints.length);
    for (Integer integer : set) {
      list.add(integer);
    }
    assertEquals(new HashSet<Integer>(IntList.asList(ints)), set);
    return list;
  }

  /**
   * Tests that flat lists behave like regular lists in terms of equals
   * and hashCode.
   */
  @Test public void testFlatList() {
    final List<String> emp = FlatLists.of();
    final List<String> emp0 = Collections.emptyList();
    assertEquals(emp, emp0);
    assertEquals(emp.hashCode(), emp0.hashCode());

    final List<String> ab = FlatLists.of("A", "B");
    final List<String> ab0 = Arrays.asList("A", "B");
    assertEquals(ab, ab0);
    assertEquals(ab.hashCode(), ab0.hashCode());

    final List<String> an = FlatLists.of("A", null);
    final List<String> an0 = Arrays.asList("A", null);
    assertEquals(an, an0);
    assertEquals(an.hashCode(), an0.hashCode());

    final List<String> anb = FlatLists.of("A", null, "B");
    final List<String> anb0 = Arrays.asList("A", null, "B");
    assertEquals(anb, anb0);
    assertEquals(anb.hashCode(), anb0.hashCode());
  }


  /**
   * Unit test for {@link AvaticaUtils#toCamelCase(String)}.
   */
  @Test public void testToCamelCase() {
    assertEquals("myJdbcDriver", AvaticaUtils.toCamelCase("MY_JDBC_DRIVER"));
    assertEquals("myJdbcDriver", AvaticaUtils.toCamelCase("MY_JDBC__DRIVER"));
    assertEquals("myJdbcDriver", AvaticaUtils.toCamelCase("my_jdbc_driver"));
    assertEquals("abCdefGHij", AvaticaUtils.toCamelCase("ab_cdEf_g_Hij"));
    assertEquals("JdbcDriver", AvaticaUtils.toCamelCase("_JDBC_DRIVER"));
    assertEquals("", AvaticaUtils.toCamelCase("_"));
    assertEquals("", AvaticaUtils.toCamelCase(""));
  }

  /** Unit test for {@link AvaticaUtils#camelToUpper(String)}. */
  @Test public void testCamelToUpper() {
    assertEquals("MY_JDBC_DRIVER", AvaticaUtils.camelToUpper("myJdbcDriver"));
    assertEquals("MY_J_D_B_C_DRIVER",
        AvaticaUtils.camelToUpper("myJDBCDriver"));
    assertEquals("AB_CDEF_G_HIJ", AvaticaUtils.camelToUpper("abCdefGHij"));
    assertEquals("_JDBC_DRIVER", AvaticaUtils.camelToUpper("JdbcDriver"));
    assertEquals("", AvaticaUtils.camelToUpper(""));
  }

  /**
   * Unit test for {@link Util#isDistinct(java.util.List)}.
   */
  @Test public void testDistinct() {
    assertTrue(Util.isDistinct(Collections.emptyList()));
    assertTrue(Util.isDistinct(Arrays.asList("a")));
    assertTrue(Util.isDistinct(Arrays.asList("a", "b", "c")));
    assertFalse(Util.isDistinct(Arrays.asList("a", "b", "a")));
    assertTrue(Util.isDistinct(Arrays.asList("a", "b", null)));
    assertFalse(Util.isDistinct(Arrays.asList("a", null, "b", null)));
  }

  /**
   * Unit test for {@link org.apache.calcite.util.JsonBuilder}.
   */
  @Test public void testJsonBuilder() {
    JsonBuilder builder = new JsonBuilder();
    Map<String, Object> map = builder.map();
    map.put("foo", 1);
    map.put("baz", true);
    map.put("bar", "can't");
    List<Object> list = builder.list();
    map.put("list", list);
    list.add(2);
    list.add(3);
    list.add(builder.list());
    list.add(builder.map());
    list.add(null);
    map.put("nullValue", null);
    assertEquals(
        "{\n"
            + "  foo: 1,\n"
            + "  baz: true,\n"
            + "  bar: \"can't\",\n"
            + "  list: [\n"
            + "    2,\n"
            + "    3,\n"
            + "    [],\n"
            + "    {},\n"
            + "    null\n"
            + "  ],\n"
            + "  nullValue: null\n"
            + "}",
        builder.toJsonString(map));
  }

  @Test public void testCompositeMap() {
    String[] beatles = {"john", "paul", "george", "ringo"};
    Map<String, Integer> beatleMap = new LinkedHashMap<String, Integer>();
    for (String beatle : beatles) {
      beatleMap.put(beatle, beatle.length());
    }

    CompositeMap<String, Integer> map = CompositeMap.of(beatleMap);
    checkCompositeMap(beatles, map);

    map = CompositeMap.of(
        beatleMap, Collections.<String, Integer>emptyMap());
    checkCompositeMap(beatles, map);

    map = CompositeMap.of(
        Collections.<String, Integer>emptyMap(), beatleMap);
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
    assertThat(map.size(), equalTo(6));
    assertThat(map.keySet().size(), equalTo(6));
    assertThat(map.entrySet().size(), equalTo(6));
    assertThat(map.values().size(), equalTo(6));
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
    assertThat(4, equalTo(map.size()));
    assertThat(false, equalTo(map.isEmpty()));
    assertThat(
        map.keySet(),
        equalTo((Set<String>) new HashSet<String>(Arrays.asList(beatles))));
    assertThat(
        ImmutableMultiset.copyOf(map.values()),
        equalTo(ImmutableMultiset.copyOf(Arrays.asList(4, 4, 6, 5))));
  }

  /** Tests {@link Util#commaList(java.util.List)}. */
  @Test public void testCommaList() {
    try {
      String s = Util.commaList(null);
      fail("expected NPE, got " + s);
    } catch (NullPointerException e) {
      // ok
    }
    assertThat(Util.commaList(ImmutableList.<Object>of()), equalTo(""));
    assertThat(Util.commaList(ImmutableList.of(1)), equalTo("1"));
    assertThat(Util.commaList(ImmutableList.of(2, 3)), equalTo("2, 3"));
    assertThat(Util.commaList(Arrays.asList(2, null, 3)),
        equalTo("2, null, 3"));
  }

  /** Unit test for {@link Util#firstDuplicate(java.util.List)}. */
  @Test public void testFirstDuplicate() {
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
  @Test public void testIsDistinctBenchmark() {
    // Run a much quicker form of the test during regular testing.
    final int limit = Benchmark.enabled() ? 1000000 : 10;
    final int zMax = 100;
    for (int i = 0; i < 30; i++) {
      final int size = i;
      new Benchmark("isDistinct " + i + " (set)",
          new Function1<Benchmark.Statistician, Void>() {
            public Void apply(Benchmark.Statistician statistician) {
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
            }
          },
          5).run();
    }
  }

  /** Unit test for {@link Util#hashCode(double)}. */
  @Test public void testHash() {
    checkHash(0d);
    checkHash(1d);
    checkHash(-2.5d);
    checkHash(10d / 3d);
    checkHash(Double.NEGATIVE_INFINITY);
    checkHash(Double.POSITIVE_INFINITY);
    checkHash(Double.MAX_VALUE);
    checkHash(Double.MIN_VALUE);
  }

  public void checkHash(double v) {
    assertThat(new Double(v).hashCode(), equalTo(Util.hashCode(v)));
  }

  /** Unit test for {@link Util#startsWith}. */
  @Test public void testStartsWithList() {
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

  @Test public void testResources() {
    Resources.validate(Static.RESOURCE);
  }

  /** Tests that sorted sets behave the way we expect. */
  @Test public void testSortedSet() {
    final TreeSet<String> treeSet = new TreeSet<String>();
    Collections.addAll(treeSet, "foo", "bar", "fOo", "FOO", "pug");
    assertThat(treeSet.size(), equalTo(5));

    final TreeSet<String> treeSet2 =
        new TreeSet<String>(String.CASE_INSENSITIVE_ORDER);
    treeSet2.addAll(treeSet);
    assertThat(treeSet2.size(), equalTo(3));

    final Comparator<String> comparator = new Comparator<String>() {
      public int compare(String o1, String o2) {
        String u1 = o1.toUpperCase();
        String u2 = o2.toUpperCase();
        int c = u1.compareTo(u2);
        if (c == 0) {
          c = o1.compareTo(o2);
        }
        return c;
      }
    };
    final TreeSet<String> treeSet3 = new TreeSet<String>(comparator);
    treeSet3.addAll(treeSet);
    assertThat(treeSet3.size(), equalTo(5));

    assertThat(checkNav(treeSet3, "foo").size(), equalTo(3));
    assertThat(checkNav(treeSet3, "FOO").size(), equalTo(3));
    assertThat(checkNav(treeSet3, "FoO").size(), equalTo(3));
    assertThat(checkNav(treeSet3, "BAR").size(), equalTo(1));

    final ImmutableSortedSet<String> treeSet4 =
        ImmutableSortedSet.copyOf(comparator, treeSet);
    final NavigableSet<String> navigableSet4 =
        Compatible.INSTANCE.navigableSet(treeSet4);
    assertThat(treeSet4.size(), equalTo(5));
    assertThat(navigableSet4.size(), equalTo(5));
    assertThat(navigableSet4, equalTo((SortedSet<String>) treeSet4));
    assertThat(checkNav(navigableSet4, "foo").size(), equalTo(3));
    assertThat(checkNav(navigableSet4, "FOO").size(), equalTo(3));
    assertThat(checkNav(navigableSet4, "FoO").size(), equalTo(3));
    assertThat(checkNav(navigableSet4, "BAR").size(), equalTo(1));
  }

  private NavigableSet<String> checkNav(NavigableSet<String> set, String s) {
    return set.subSet(s.toUpperCase(), true, s.toLowerCase(), true);
  }

  /** Test for {@link org.apache.calcite.util.ImmutableNullableList}. */
  @Test public void testImmutableNullableList() {
    final List<String> arrayList = Arrays.asList("a", null, "c");
    final List<String> list = ImmutableNullableList.copyOf(arrayList);
    assertThat(list.size(), equalTo(arrayList.size()));
    assertThat(list, equalTo(arrayList));
    assertThat(list.hashCode(), equalTo(arrayList.hashCode()));
    assertThat(list.toString(), equalTo(arrayList.toString()));
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
    assertThat(ImmutableNullableList.copyOf(Arrays.asList("a", "b", "c")),
        isA((Class) ImmutableList.class));
  }

  /** Test for {@link org.apache.calcite.util.UnmodifiableArrayList}. */
  @Test public void testUnmodifiableArrayList() {
    final String[] strings = {"a", null, "c"};
    final List<String> arrayList = Arrays.asList(strings);
    final List<String> list = UnmodifiableArrayList.of(strings);
    assertThat(list.size(), equalTo(arrayList.size()));
    assertThat(list, equalTo(arrayList));
    assertThat(list.hashCode(), equalTo(arrayList.hashCode()));
    assertThat(list.toString(), equalTo(arrayList.toString()));
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
  @Test public void testImmutableNullableListBuilder() {
    final ImmutableNullableList.Builder<String> builder =
        ImmutableNullableList.builder();
    builder.add("a")
        .add((String) null)
        .add("c");
    final List<String> arrayList = Arrays.asList("a", null, "c");
    final List<String> list = builder.build();
    assertThat(arrayList.equals(list), is(true));
  }

  @Test public void testHuman() {
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

  @Test public void testAsIndexView() {
    final List<String> values  = Lists.newArrayList("abCde", "X", "y");
    final Map<String, String> map = Util.asIndexMap(values,
        new Function<String, String>() {
          public String apply(@Nullable String input) {
            return input.toUpperCase();
          }
        });
    assertThat(map.size(), equalTo(values.size()));
    assertThat(map.get("X"), equalTo("X"));
    assertThat(map.get("Y"), equalTo("y"));
    assertThat(map.get("y"), is((String) null));
    assertThat(map.get("ABCDE"), equalTo("abCde"));

    // If you change the values collection, the map changes.
    values.remove(1);
    assertThat(map.size(), equalTo(values.size()));
    assertThat(map.get("X"), is((String) null));
    assertThat(map.get("Y"), equalTo("y"));
  }

  @Test public void testRelBuilderExample() {
    new RelBuilderExample(false).runAllExamples();
  }
}

// End UtilTest.java
