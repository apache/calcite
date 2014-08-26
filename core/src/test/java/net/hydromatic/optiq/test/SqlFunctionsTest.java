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
package net.hydromatic.optiq.test;

import net.hydromatic.avatica.ByteString;

import net.hydromatic.optiq.runtime.*;

import org.junit.Test;

import java.util.*;

import static net.hydromatic.optiq.runtime.SqlFunctions.*;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

/**
 * Unit test for the methods in {@link SqlFunctions} that implement SQL
 * functions.
 */
public class SqlFunctionsTest {
  @Test public void testCharLength() {
    assertEquals(3, charLength("xyz"));
  }

  @Test public void testConcat() {
    assertEquals("a bcd", concat("a b", "cd"));
    // The code generator will ensure that nulls are never passed in. If we
    // pass in null, it is treated like the string "null", as the following
    // tests show. Not the desired behavior for SQL.
    assertEquals("anull", concat("a", null));
    assertEquals("nullnull", concat((String) null, null));
    assertEquals("nullb", concat(null, "b"));
  }

  @Test public void testLower() {
    assertEquals("a bcd", lower("A bCd"));
  }

  @Test public void testUpper() {
    assertEquals("A BCD", upper("A bCd"));
  }

  @Test public void testInitcap() {
    assertEquals("Aa", initcap("aA"));
    assertEquals("Zz", initcap("zz"));
    assertEquals("Az", initcap("AZ"));
    assertEquals("Try A Little  ", initcap("tRy a littlE  "));
    assertEquals("Won'T It?No", initcap("won't it?no"));
    assertEquals("1a", initcap("1A"));
    assertEquals(" B0123b", initcap(" b0123B"));
  }

  @Test public void testLesser() {
    assertEquals("a", lesser("a", "bc"));
    assertEquals("ac", lesser("bc", "ac"));
    try {
      Object o = lesser("a", null);
      fail("Expected NPE, got " + o);
    } catch (NullPointerException e) {
      // ok
    }
    assertEquals("a", lesser(null, "a"));
    assertNull(lesser((String) null, null));
  }

  @Test public void testGreater() {
    assertEquals("bc", greater("a", "bc"));
    assertEquals("bc", greater("bc", "ac"));
    try {
      Object o = greater("a", null);
      fail("Expected NPE, got " + o);
    } catch (NullPointerException e) {
      // ok
    }
    assertEquals("a", greater(null, "a"));
    assertNull(greater((String) null, null));
  }

  /** Test for {@link SqlFunctions#rtrim}. */
  @Test public void testRtrim() {
    assertEquals("", rtrim(""));
    assertEquals("", rtrim("    "));
    assertEquals("   x", rtrim("   x  "));
    assertEquals("   x", rtrim("   x "));
    assertEquals("   x y", rtrim("   x y "));
    assertEquals("   x", rtrim("   x"));
    assertEquals("x", rtrim("x"));
  }

  /** Test for {@link SqlFunctions#ltrim}. */
  @Test public void testLtrim() {
    assertEquals("", ltrim(""));
    assertEquals("", ltrim("    "));
    assertEquals("x  ", ltrim("   x  "));
    assertEquals("x ", ltrim("   x "));
    assertEquals("x y ", ltrim("x y "));
    assertEquals("x", ltrim("   x"));
    assertEquals("x", ltrim("x"));
  }

  /** Test for {@link SqlFunctions#trim}. */
  @Test public void testTrim() {
    assertEquals("", trimSpacesBoth(""));
    assertEquals("", trimSpacesBoth("    "));
    assertEquals("x", trimSpacesBoth("   x  "));
    assertEquals("x", trimSpacesBoth("   x "));
    assertEquals("x y", trimSpacesBoth("   x y "));
    assertEquals("x", trimSpacesBoth("   x"));
    assertEquals("x", trimSpacesBoth("x"));
  }

  static String trimSpacesBoth(String s) {
    return trim(true, true, " ", s);
  }

  @Test public void testUnixDateToString() {
    // Verify these using the "date" command. E.g.
    // $ date -u --date="@$(expr 10957 \* 86400)"
    // Sat Jan  1 00:00:00 UTC 2000
    assertEquals("2000-01-01", unixDateToString(10957));

    assertEquals("1970-01-01", unixDateToString(0));
    assertEquals("1970-01-02", unixDateToString(1));
    assertEquals("1971-01-01", unixDateToString(365));
    assertEquals("1972-01-01", unixDateToString(730));
    assertEquals("1972-02-28", unixDateToString(788));
    assertEquals("1972-02-29", unixDateToString(789));
    assertEquals("1972-03-01", unixDateToString(790));

    assertEquals("1969-01-01", unixDateToString(-365));
    assertEquals("2000-01-01", unixDateToString(10957));
    assertEquals("2000-02-28", unixDateToString(11015));
    assertEquals("2000-02-29", unixDateToString(11016));
    assertEquals("2000-03-01", unixDateToString(11017));
    assertEquals("1945-02-24", unixDateToString(-9077));
  }

  @Test public void testYmdToUnixDate() {
    assertEquals(0, ymdToUnixDate(1970, 1, 1));
    assertEquals(365, ymdToUnixDate(1971, 1, 1));
    assertEquals(-365, ymdToUnixDate(1969, 1, 1));
    assertEquals(11017, ymdToUnixDate(2000, 3, 1));
    assertEquals(-9077, ymdToUnixDate(1945, 2, 24));
  }

  @Test public void testDateToString() {
    checkDateString("1970-01-01", 0);
    checkDateString("1971-02-03", 0 + 365 + 31 + 2);
  }

  private void checkDateString(String s, int d) {
    assertThat(unixDateToString(d), equalTo(s));
    assertThat(dateStringToUnixDate(s), equalTo(d));
  }

  @Test public void testTimeToString() {
    checkTimeString("00:00:00", 0);
    checkTimeString("23:59:59", 86400000 - 1000);
  }

  private void checkTimeString(String s, int d) {
    assertThat(unixTimeToString(d), equalTo(s));
    assertThat(timeStringToUnixDate(s), equalTo(d));
  }

  @Test public void testTimestampToString() {
    // ISO format would be "1970-01-01T00:00:00" but SQL format is different
    checkTimestampString("1970-01-01 00:00:00", 0L);
    checkTimestampString("1970-02-01 23:59:59", 86400000L * 32L - 1000L);
  }

  private void checkTimestampString(String s, long d) {
    assertThat(unixTimestampToString(d), equalTo(s));
    assertThat(timestampStringToUnixDate(s), equalTo(d));
  }

  @Test public void testIntervalYearMonthToString() {
    TimeUnitRange range = TimeUnitRange.YEAR_TO_MONTH;
    assertEquals("+0-00", intervalYearMonthToString(0, range));
    assertEquals("+1-00", intervalYearMonthToString(12, range));
    assertEquals("+1-01", intervalYearMonthToString(13, range));
    assertEquals("-1-01", intervalYearMonthToString(-13, range));
  }

  @Test public void testIntervalDayTimeToString() {
    assertEquals("+0", intervalYearMonthToString(0, TimeUnitRange.YEAR));
    assertEquals("+0-00",
        intervalYearMonthToString(0, TimeUnitRange.YEAR_TO_MONTH));
    assertEquals("+0", intervalYearMonthToString(0, TimeUnitRange.MONTH));
    assertEquals("+0", intervalDayTimeToString(0, TimeUnitRange.DAY, 0));
    assertEquals("+0 00",
        intervalDayTimeToString(0, TimeUnitRange.DAY_TO_HOUR, 0));
    assertEquals("+0 00:00",
        intervalDayTimeToString(0, TimeUnitRange.DAY_TO_MINUTE, 0));
    assertEquals("+0 00:00:00",
        intervalDayTimeToString(0, TimeUnitRange.DAY_TO_SECOND, 0));
    assertEquals("+0", intervalDayTimeToString(0, TimeUnitRange.HOUR, 0));
    assertEquals("+0:00",
        intervalDayTimeToString(0, TimeUnitRange.HOUR_TO_MINUTE, 0));
    assertEquals("+0:00:00",
        intervalDayTimeToString(0, TimeUnitRange.HOUR_TO_SECOND, 0));
    assertEquals("+0",
        intervalDayTimeToString(0, TimeUnitRange.MINUTE, 0));
    assertEquals("+0:00",
        intervalDayTimeToString(0, TimeUnitRange.MINUTE_TO_SECOND, 0));
    assertEquals("+0",
        intervalDayTimeToString(0, TimeUnitRange.SECOND, 0));
  }

  @Test public void testYmdToJulian() {
    // All checked using http://aa.usno.navy.mil/data/docs/JulianDate.php.
    // We round up - if JulianDate.php gives 2451544.5, we use 2451545.
    assertThat(ymdToJulian(2014, 4, 3), equalTo(2456751));
    assertThat(ymdToJulian(2000, 1, 1), equalTo(2451545));
    assertThat(ymdToJulian(1970, 1, 1), equalTo(2440588));
    assertThat(ymdToJulian(1970, 1, 1), equalTo(EPOCH_JULIAN));
    assertThat(ymdToJulian(1901, 1, 1), equalTo(2415386));
    assertThat(ymdToJulian(1900, 10, 17), equalTo(2415310));
    assertThat(ymdToJulian(1900, 3, 1), equalTo(2415080));
    assertThat(ymdToJulian(1900, 2, 28), equalTo(2415079));
    assertThat(ymdToJulian(1900, 2, 1), equalTo(2415052));
    assertThat(ymdToJulian(1900, 1, 1), equalTo(2415021));
    assertThat(ymdToJulian(1777, 7, 4), equalTo(2370281));
  }

  @Test public void testExtract() {
    assertThat(unixDateExtract(TimeUnitRange.YEAR, 0), equalTo(1970L));
    assertThat(unixDateExtract(TimeUnitRange.YEAR, -1), equalTo(1969L));
    assertThat(unixDateExtract(TimeUnitRange.YEAR, 364), equalTo(1970L));
    assertThat(unixDateExtract(TimeUnitRange.YEAR, 365), equalTo(1971L));

    assertThat(unixDateExtract(TimeUnitRange.MONTH, 0), equalTo(1L));
    assertThat(unixDateExtract(TimeUnitRange.MONTH, -1), equalTo(12L));
    assertThat(unixDateExtract(TimeUnitRange.MONTH, 364), equalTo(12L));
    assertThat(unixDateExtract(TimeUnitRange.MONTH, 365), equalTo(1L));

    thereAndBack(2000, 1, 1);
    thereAndBack(2000, 2, 28);
    thereAndBack(2000, 2, 29); // does day
    thereAndBack(2000, 3, 1);
    thereAndBack(1964, 1, 1);
    thereAndBack(1964, 2, 28);
    thereAndBack(1964, 2, 29); // leap day
    thereAndBack(1964, 3, 1);
    thereAndBack(1864, 1, 1);
    thereAndBack(1864, 2, 28);
    thereAndBack(1864, 2, 29); // leap day
    thereAndBack(1864, 3, 1);
    thereAndBack(1900, 1, 1);
    thereAndBack(1900, 2, 28);
    thereAndBack(1900, 3, 1);
    thereAndBack(2004, 2, 28);
    thereAndBack(2004, 2, 29); // leap day
    thereAndBack(2004, 3, 1);
  }

  private void thereAndBack(int year, int month, int day) {
    final int unixDate = ymdToUnixDate(year, month, day);
    assertThat(unixDateExtract(TimeUnitRange.YEAR, unixDate),
        equalTo((long) year));
    assertThat(unixDateExtract(TimeUnitRange.MONTH, unixDate),
        equalTo((long) month));
    assertThat(unixDateExtract(TimeUnitRange.DAY, unixDate),
        equalTo((long) day));
  }

  /** Unit test for
   * {@link Utilities#compare(java.util.List, java.util.List)}. */
  @Test public void testCompare() {
    final List<String> ac = Arrays.asList("a", "c");
    final List<String> abc = Arrays.asList("a", "b", "c");
    final List<String> a = Arrays.asList("a");
    final List<String> empty = Collections.emptyList();
    assertEquals(0, Utilities.compare(ac, ac));
    assertEquals(0, Utilities.compare(ac, new ArrayList<String>(ac)));
    assertEquals(-1, Utilities.compare(a, ac));
    assertEquals(-1, Utilities.compare(empty, ac));
    assertEquals(1, Utilities.compare(ac, a));
    assertEquals(1, Utilities.compare(ac, abc));
    assertEquals(1, Utilities.compare(ac, empty));
    assertEquals(0, Utilities.compare(empty, empty));
  }

  @Test public void testTruncateLong() {
    assertEquals(12000L, SqlFunctions.truncate(12345L, 1000L));
    assertEquals(12000L, SqlFunctions.truncate(12000L, 1000L));
    assertEquals(12000L, SqlFunctions.truncate(12001L, 1000L));
    assertEquals(11000L, SqlFunctions.truncate(11999L, 1000L));

    assertEquals(-13000L, SqlFunctions.truncate(-12345L, 1000L));
    assertEquals(-12000L, SqlFunctions.truncate(-12000L, 1000L));
    assertEquals(-13000L, SqlFunctions.truncate(-12001L, 1000L));
    assertEquals(-12000L, SqlFunctions.truncate(-11999L, 1000L));
  }

  @Test public void testTruncateInt() {
    assertEquals(12000, SqlFunctions.truncate(12345, 1000));
    assertEquals(12000, SqlFunctions.truncate(12000, 1000));
    assertEquals(12000, SqlFunctions.truncate(12001, 1000));
    assertEquals(11000, SqlFunctions.truncate(11999, 1000));

    assertEquals(-13000, SqlFunctions.truncate(-12345, 1000));
    assertEquals(-12000, SqlFunctions.truncate(-12000, 1000));
    assertEquals(-13000, SqlFunctions.truncate(-12001, 1000));
    assertEquals(-12000, SqlFunctions.truncate(-11999, 1000));

    assertEquals(12000, SqlFunctions.round(12345, 1000));
    assertEquals(13000, SqlFunctions.round(12845, 1000));
    assertEquals(-12000, SqlFunctions.round(-12345, 1000));
    assertEquals(-13000, SqlFunctions.round(-12845, 1000));
  }

  @Test public void testByteString() {
    final byte[] bytes = {(byte) 0xAB, (byte) 0xFF};
    final ByteString byteString = new ByteString(bytes);
    assertEquals(2, byteString.length());
    assertEquals("abff", byteString.toString());
    assertEquals("abff", byteString.toString(16));
    assertEquals("1010101111111111", byteString.toString(2));

    final ByteString emptyByteString = new ByteString(new byte[0]);
    assertEquals(0, emptyByteString.length());
    assertEquals("", emptyByteString.toString());
    assertEquals("", emptyByteString.toString(16));
    assertEquals("", emptyByteString.toString(2));

    assertEquals(emptyByteString, ByteString.EMPTY);

    assertEquals("ff", byteString.substring(1, 2).toString());
    assertEquals("abff", byteString.substring(0, 2).toString());
    assertEquals("", byteString.substring(2, 2).toString());

    // Add empty string, get original string back
    assertSame(byteString.concat(emptyByteString), byteString);
    final ByteString byteString1 = new ByteString(new byte[]{(byte) 12});
    assertEquals("abff0c", byteString.concat(byteString1).toString());

    final byte[] bytes3 = {(byte) 0xFF};
    final ByteString byteString3 = new ByteString(bytes3);

    assertEquals(0, byteString.indexOf(emptyByteString));
    assertEquals(-1, byteString.indexOf(byteString1));
    assertEquals(1, byteString.indexOf(byteString3));
    assertEquals(-1, byteString3.indexOf(byteString));
  }

  @Test public void testEasyLog10() {
    assertEquals(1, SqlFunctions.digitCount(0));
    assertEquals(1, SqlFunctions.digitCount(1));
    assertEquals(1, SqlFunctions.digitCount(9));
    assertEquals(2, SqlFunctions.digitCount(10));
    assertEquals(2, SqlFunctions.digitCount(11));
    assertEquals(2, SqlFunctions.digitCount(99));
    assertEquals(3, SqlFunctions.digitCount(100));
  }
}

// End SqlFunctionsTest.java
