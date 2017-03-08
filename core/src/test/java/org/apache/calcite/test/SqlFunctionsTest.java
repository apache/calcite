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
package org.apache.calcite.test;

import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.runtime.Utilities;

import org.junit.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.calcite.avatica.util.DateTimeUtils.EPOCH_JULIAN;
import static org.apache.calcite.avatica.util.DateTimeUtils.dateStringToUnixDate;
import static org.apache.calcite.avatica.util.DateTimeUtils.digitCount;
import static org.apache.calcite.avatica.util.DateTimeUtils.floorDiv;
import static org.apache.calcite.avatica.util.DateTimeUtils.floorMod;
import static org.apache.calcite.avatica.util.DateTimeUtils.intervalDayTimeToString;
import static org.apache.calcite.avatica.util.DateTimeUtils.intervalYearMonthToString;
import static org.apache.calcite.avatica.util.DateTimeUtils.timeStringToUnixDate;
import static org.apache.calcite.avatica.util.DateTimeUtils.timestampStringToUnixDate;
import static org.apache.calcite.avatica.util.DateTimeUtils.unixDateExtract;
import static org.apache.calcite.avatica.util.DateTimeUtils.unixDateToString;
import static org.apache.calcite.avatica.util.DateTimeUtils.unixTimeToString;
import static org.apache.calcite.avatica.util.DateTimeUtils.unixTimestamp;
import static org.apache.calcite.avatica.util.DateTimeUtils.unixTimestampToString;
import static org.apache.calcite.avatica.util.DateTimeUtils.ymdToJulian;
import static org.apache.calcite.avatica.util.DateTimeUtils.ymdToUnixDate;
import static org.apache.calcite.runtime.SqlFunctions.addMonths;
import static org.apache.calcite.runtime.SqlFunctions.charLength;
import static org.apache.calcite.runtime.SqlFunctions.concat;
import static org.apache.calcite.runtime.SqlFunctions.greater;
import static org.apache.calcite.runtime.SqlFunctions.initcap;
import static org.apache.calcite.runtime.SqlFunctions.lesser;
import static org.apache.calcite.runtime.SqlFunctions.lower;
import static org.apache.calcite.runtime.SqlFunctions.ltrim;
import static org.apache.calcite.runtime.SqlFunctions.rtrim;
import static org.apache.calcite.runtime.SqlFunctions.subtractMonths;
import static org.apache.calcite.runtime.SqlFunctions.trim;
import static org.apache.calcite.runtime.SqlFunctions.upper;

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

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
    assertEquals("a bcd iijk", lower("A bCd Iijk"));
  }

  @Test public void testUpper() {
    assertEquals("A BCD IIJK", upper("A bCd iIjk"));
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
    assertEquals("1900-01-01", unixDateToString(-25567));
    assertEquals("1900-02-28", unixDateToString(-25509));
    assertEquals("1900-03-01", unixDateToString(-25508));
    assertEquals("1945-02-24", unixDateToString(-9077));
  }

  @Test public void testYmdToUnixDate() {
    assertEquals(0, ymdToUnixDate(1970, 1, 1));
    assertEquals(365, ymdToUnixDate(1971, 1, 1));
    assertEquals(-365, ymdToUnixDate(1969, 1, 1));
    assertEquals(11015, ymdToUnixDate(2000, 2, 28));
    assertEquals(11016, ymdToUnixDate(2000, 2, 29));
    assertEquals(11017, ymdToUnixDate(2000, 3, 1));
    assertEquals(-9077, ymdToUnixDate(1945, 2, 24));
    assertEquals(-25509, ymdToUnixDate(1900, 2, 28));
    assertEquals(-25508, ymdToUnixDate(1900, 3, 1));
  }

  @Test public void testDateToString() {
    checkDateString("1970-01-01", 0);
    //noinspection PointlessArithmeticExpression
    checkDateString("1971-02-03", 0 + 365 + 31 + (3 - 1));
    //noinspection PointlessArithmeticExpression
    checkDateString("1971-02-28", 0 + 365 + 31 + (28 - 1));
    //noinspection PointlessArithmeticExpression
    checkDateString("1971-03-01", 0 + 365 + 31 + 28 + (1 - 1));
    //noinspection PointlessArithmeticExpression
    checkDateString("1972-02-28", 0 + 365 * 2 + 31 + (28 - 1));
    //noinspection PointlessArithmeticExpression
    checkDateString("1972-02-29", 0 + 365 * 2 + 31 + (29 - 1));
    //noinspection PointlessArithmeticExpression
    checkDateString("1972-03-01", 0 + 365 * 2 + 31 + 29 + (1 - 1));
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

    // 2000 is a leap year
    assertThat(ymdToJulian(2000, 1, 1), equalTo(2451545));
    assertThat(ymdToJulian(2000, 2, 28), equalTo(2451603));
    assertThat(ymdToJulian(2000, 2, 29), equalTo(2451604));
    assertThat(ymdToJulian(2000, 3, 1), equalTo(2451605));

    assertThat(ymdToJulian(1970, 1, 1), equalTo(2440588));
    assertThat(ymdToJulian(1970, 1, 1), equalTo(EPOCH_JULIAN));
    assertThat(ymdToJulian(1901, 1, 1), equalTo(2415386));

    // 1900 is not a leap year
    assertThat(ymdToJulian(1900, 10, 17), equalTo(2415310));
    assertThat(ymdToJulian(1900, 3, 1), equalTo(2415080));
    assertThat(ymdToJulian(1900, 2, 28), equalTo(2415079));
    assertThat(ymdToJulian(1900, 2, 1), equalTo(2415052));
    assertThat(ymdToJulian(1900, 1, 1), equalTo(2415021));

    assertThat(ymdToJulian(1777, 7, 4), equalTo(2370281));

    // 2016 is a leap year
    assertThat(ymdToJulian(2016, 2, 28), equalTo(2457447));
    assertThat(ymdToJulian(2016, 2, 29), equalTo(2457448));
    assertThat(ymdToJulian(2016, 3, 1), equalTo(2457449));
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

    thereAndBack(1900, 1, 1);
    thereAndBack(1900, 2, 28); // no leap day
    thereAndBack(1900, 3, 1);
    thereAndBack(1901, 1, 1);
    thereAndBack(1901, 2, 28); // no leap day
    thereAndBack(1901, 3, 1);
    thereAndBack(2000, 1, 1);
    thereAndBack(2000, 2, 28);
    thereAndBack(2000, 2, 29); // leap day
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
    thereAndBack(2005, 2, 28); // no leap day
    thereAndBack(2005, 3, 1);
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

  @Test public void testAddMonths() {
    checkAddMonths(2016, 1, 1, 2016, 2, 1, 1);
    checkAddMonths(2016, 1, 1, 2017, 1, 1, 12);
    checkAddMonths(2016, 1, 1, 2017, 2, 1, 13);
    checkAddMonths(2016, 1, 1, 2015, 1, 1, -12);
    checkAddMonths(2016, 1, 1, 2018, 10, 1, 33);
    checkAddMonths(2016, 1, 31, 2016, 5, 1, 3); // roll up
    checkAddMonths(2016, 4, 30, 2016, 7, 30, 3); // roll up
    checkAddMonths(2016, 1, 31, 2016, 3, 1, 1);
    checkAddMonths(2016, 3, 31, 2016, 3, 1, -1);
    checkAddMonths(2016, 3, 31, 2116, 3, 31, 1200);
    checkAddMonths(2016, 2, 28, 2116, 2, 28, 1200);
  }

  private void checkAddMonths(int y0, int m0, int d0, int y1, int m1, int d1,
      int months) {
    final int date0 = ymdToUnixDate(y0, m0, d0);
    final long date = addMonths(date0, months);
    final int date1 = ymdToUnixDate(y1, m1, d1);
    assertThat((int) date, is(date1));

    assertThat(subtractMonths(date1, date0),
        anyOf(is(months), is(months + 1)));
    assertThat(subtractMonths(date1 + 1, date0),
        anyOf(is(months), is(months + 1)));
    assertThat(subtractMonths(date1, date0 + 1),
        anyOf(is(months), is(months - 1)));
    assertThat(subtractMonths(d2ts(date1, 1), d2ts(date0, 0)),
        anyOf(is(months), is(months + 1)));
    assertThat(subtractMonths(d2ts(date1, 0), d2ts(date0, 1)),
        anyOf(is(months - 1), is(months), is(months + 1)));
  }

  /** Converts a date (days since epoch) and milliseconds (since midnight)
   * into a timestamp (milliseconds since epoch). */
  private long d2ts(int date, int millis) {
    return date * DateTimeUtils.MILLIS_PER_DAY + millis;
  }

  @Test public void testUnixTimestamp() {
    assertThat(unixTimestamp(1970, 1, 1, 0, 0, 0), is(0L));
    final long day = 86400000L;
    assertThat(unixTimestamp(1970, 1, 2, 0, 0, 0), is(day));
    assertThat(unixTimestamp(1970, 1, 1, 23, 59, 59), is(86399000L));

    // 1900 is not a leap year
    final long y1900 = -2203977600000L;
    assertThat(unixTimestamp(1900, 2, 28, 0, 0, 0), is(y1900));
    assertThat(unixTimestamp(1900, 3, 1, 0, 0, 0), is(y1900 + day));

    // 2000 is a leap year
    final long y2k = 951696000000L;
    assertThat(unixTimestamp(2000, 2, 28, 0, 0, 0), is(y2k));
    assertThat(unixTimestamp(2000, 2, 29, 0, 0, 0), is(y2k + day));
    assertThat(unixTimestamp(2000, 3, 1, 0, 0, 0), is(y2k + day + day));

    // 2016 is a leap year
    final long y2016 = 1456617600000L;
    assertThat(unixTimestamp(2016, 2, 28, 0, 0, 0), is(y2016));
    assertThat(unixTimestamp(2016, 2, 29, 0, 0, 0), is(y2016 + day));
    assertThat(unixTimestamp(2016, 3, 1, 0, 0, 0), is(y2016 + day + day));
  }

  @Test public void testFloor() {
    checkFloor(0, 10, 0);
    checkFloor(27, 10, 20);
    checkFloor(30, 10, 30);
    checkFloor(-30, 10, -30);
    checkFloor(-27, 10, -30);
  }

  private void checkFloor(int x, int y, int result) {
    assertThat(SqlFunctions.floor(x, y), is(result));
    assertThat(SqlFunctions.floor((long) x, (long) y), is((long) result));
    assertThat(SqlFunctions.floor((short) x, (short) y), is((short) result));
    assertThat(SqlFunctions.floor((byte) x, (byte) y), is((byte) result));
    assertThat(
        SqlFunctions.floor(BigDecimal.valueOf(x), BigDecimal.valueOf(y)),
        is(BigDecimal.valueOf(result)));
  }

  @Test public void testCeil() {
    checkCeil(0, 10, 0);
    checkCeil(27, 10, 30);
    checkCeil(30, 10, 30);
    checkCeil(-30, 10, -30);
    checkCeil(-27, 10, -20);
    checkCeil(-27, 1, -27);
  }

  private void checkCeil(int x, int y, int result) {
    assertThat(SqlFunctions.ceil(x, y), is(result));
    assertThat(SqlFunctions.ceil((long) x, (long) y), is((long) result));
    assertThat(SqlFunctions.ceil((short) x, (short) y), is((short) result));
    assertThat(SqlFunctions.ceil((byte) x, (byte) y), is((byte) result));
    assertThat(
        SqlFunctions.ceil(BigDecimal.valueOf(x), BigDecimal.valueOf(y)),
        is(BigDecimal.valueOf(result)));
  }

  /** Unit test for
   * {@link Utilities#compare(java.util.List, java.util.List)}. */
  @Test public void testCompare() {
    final List<String> ac = Arrays.asList("a", "c");
    final List<String> abc = Arrays.asList("a", "b", "c");
    final List<String> a = Collections.singletonList("a");
    final List<String> empty = Collections.emptyList();
    assertEquals(0, Utilities.compare(ac, ac));
    assertEquals(0, Utilities.compare(ac, new ArrayList<>(ac)));
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

  @Test public void testSTruncateDouble() {
    assertEquals(12.345d, SqlFunctions.struncate(12.345d, 3), 0.001);
    assertEquals(12.340d, SqlFunctions.struncate(12.345d, 2), 0.001);
    assertEquals(12.300d, SqlFunctions.struncate(12.345d, 1), 0.001);
    assertEquals(12.000d, SqlFunctions.struncate(12.999d, 0), 0.001);

    assertEquals(-12.345d, SqlFunctions.struncate(-12.345d, 3), 0.001);
    assertEquals(-12.340d, SqlFunctions.struncate(-12.345d, 2), 0.001);
    assertEquals(-12.300d, SqlFunctions.struncate(-12.345d, 1), 0.001);
    assertEquals(-12.000d, SqlFunctions.struncate(-12.999d, 0), 0.001);

    assertEquals(12000d, SqlFunctions.struncate(12345d, -3), 0.001);
    assertEquals(12000d, SqlFunctions.struncate(12000d, -3), 0.001);
    assertEquals(12000d, SqlFunctions.struncate(12001d, -3), 0.001);
    assertEquals(10000d, SqlFunctions.struncate(12000d, -4), 0.001);
    assertEquals(0d, SqlFunctions.struncate(12000d, -5), 0.001);
    assertEquals(11000d, SqlFunctions.struncate(11999d, -3), 0.001);

    assertEquals(-12000d, SqlFunctions.struncate(-12345d, -3), 0.001);
    assertEquals(-12000d, SqlFunctions.struncate(-12000d, -3), 0.001);
    assertEquals(-11000d, SqlFunctions.struncate(-11999d, -3), 0.001);
    assertEquals(-10000d, SqlFunctions.struncate(-12000d, -4), 0.001);
    assertEquals(0d, SqlFunctions.struncate(-12000d, -5), 0.001);
  }

  @Test public void testSTruncateLong() {
    assertEquals(12000d, SqlFunctions.struncate(12345L, -3), 0.001);
    assertEquals(12000d, SqlFunctions.struncate(12000L, -3), 0.001);
    assertEquals(12000d, SqlFunctions.struncate(12001L, -3), 0.001);
    assertEquals(10000d, SqlFunctions.struncate(12000L, -4), 0.001);
    assertEquals(0d, SqlFunctions.struncate(12000L, -5), 0.001);
    assertEquals(11000d, SqlFunctions.struncate(11999L, -3), 0.001);

    assertEquals(-12000d, SqlFunctions.struncate(-12345L, -3), 0.001);
    assertEquals(-12000d, SqlFunctions.struncate(-12000L, -3), 0.001);
    assertEquals(-11000d, SqlFunctions.struncate(-11999L, -3), 0.001);
    assertEquals(-10000d, SqlFunctions.struncate(-12000L, -4), 0.001);
    assertEquals(0d, SqlFunctions.struncate(-12000L, -5), 0.001);
  }

  @Test public void testSTruncateInt() {
    assertEquals(12000d, SqlFunctions.struncate(12345, -3), 0.001);
    assertEquals(12000d, SqlFunctions.struncate(12000, -3), 0.001);
    assertEquals(12000d, SqlFunctions.struncate(12001, -3), 0.001);
    assertEquals(10000d, SqlFunctions.struncate(12000, -4), 0.001);
    assertEquals(0d, SqlFunctions.struncate(12000, -5), 0.001);
    assertEquals(11000d, SqlFunctions.struncate(11999, -3), 0.001);

    assertEquals(-12000d, SqlFunctions.struncate(-12345, -3), 0.001);
    assertEquals(-12000d, SqlFunctions.struncate(-12000, -3), 0.001);
    assertEquals(-11000d, SqlFunctions.struncate(-11999, -3), 0.001);
    assertEquals(-10000d, SqlFunctions.struncate(-12000, -4), 0.001);
    assertEquals(0d, SqlFunctions.struncate(-12000, -5), 0.001);
  }

  @Test public void testSRoundDouble() {
    assertEquals(12.345d, SqlFunctions.sround(12.345d, 3), 0.001);
    assertEquals(12.350d, SqlFunctions.sround(12.345d, 2), 0.001);
    assertEquals(12.300d, SqlFunctions.sround(12.345d, 1), 0.001);
    assertEquals(13.000d, SqlFunctions.sround(12.999d, 2), 0.001);
    assertEquals(13.000d, SqlFunctions.sround(12.999d, 1), 0.001);
    assertEquals(13.000d, SqlFunctions.sround(12.999d, 0), 0.001);

    assertEquals(-12.345d, SqlFunctions.sround(-12.345d, 3), 0.001);
    assertEquals(-12.350d, SqlFunctions.sround(-12.345d, 2), 0.001);
    assertEquals(-12.300d, SqlFunctions.sround(-12.345d, 1), 0.001);
    assertEquals(-13.000d, SqlFunctions.sround(-12.999d, 2), 0.001);
    assertEquals(-13.000d, SqlFunctions.sround(-12.999d, 1), 0.001);
    assertEquals(-13.000d, SqlFunctions.sround(-12.999d, 0), 0.001);

    assertEquals(12350d, SqlFunctions.sround(12345d, -1), 0.001);
    assertEquals(12300d, SqlFunctions.sround(12345d, -2), 0.001);
    assertEquals(12000d, SqlFunctions.sround(12345d, -3), 0.001);
    assertEquals(12000d, SqlFunctions.sround(12000d, -3), 0.001);
    assertEquals(12000d, SqlFunctions.sround(12001d, -3), 0.001);
    assertEquals(10000d, SqlFunctions.sround(12000d, -4), 0.001);
    assertEquals(0d, SqlFunctions.sround(12000d, -5), 0.001);
    assertEquals(12000d, SqlFunctions.sround(11999d, -3), 0.001);

    assertEquals(-12350d, SqlFunctions.sround(-12345d, -1), 0.001);
    assertEquals(-12300d, SqlFunctions.sround(-12345d, -2), 0.001);
    assertEquals(-12000d, SqlFunctions.sround(-12345d, -3), 0.001);
    assertEquals(-12000d, SqlFunctions.sround(-12000d, -3), 0.001);
    assertEquals(-12000d, SqlFunctions.sround(-11999d, -3), 0.001);
    assertEquals(-10000d, SqlFunctions.sround(-12000d, -4), 0.001);
    assertEquals(0d, SqlFunctions.sround(-12000d, -5), 0.001);
  }

  @Test public void testSRoundLong() {
    assertEquals(12350d, SqlFunctions.sround(12345L, -1), 0.001);
    assertEquals(12300d, SqlFunctions.sround(12345L, -2), 0.001);
    assertEquals(12000d, SqlFunctions.sround(12345L, -3), 0.001);
    assertEquals(12000d, SqlFunctions.sround(12000L, -3), 0.001);
    assertEquals(12000d, SqlFunctions.sround(12001L, -3), 0.001);
    assertEquals(10000d, SqlFunctions.sround(12000L, -4), 0.001);
    assertEquals(0d, SqlFunctions.sround(12000L, -5), 0.001);
    assertEquals(12000d, SqlFunctions.sround(11999L, -3), 0.001);

    assertEquals(-12350d, SqlFunctions.sround(-12345L, -1), 0.001);
    assertEquals(-12300d, SqlFunctions.sround(-12345L, -2), 0.001);
    assertEquals(-12000d, SqlFunctions.sround(-12345L, -3), 0.001);
    assertEquals(-12000d, SqlFunctions.sround(-12000L, -3), 0.001);
    assertEquals(-12000d, SqlFunctions.sround(-11999L, -3), 0.001);
    assertEquals(-10000d, SqlFunctions.sround(-12000L, -4), 0.001);
    assertEquals(0d, SqlFunctions.sround(-12000L, -5), 0.001);
  }

  @Test public void testSRoundInt() {
    assertEquals(12350d, SqlFunctions.sround(12345, -1), 0.001);
    assertEquals(12300d, SqlFunctions.sround(12345, -2), 0.001);
    assertEquals(12000d, SqlFunctions.sround(12345, -3), 0.001);
    assertEquals(12000d, SqlFunctions.sround(12000, -3), 0.001);
    assertEquals(12000d, SqlFunctions.sround(12001, -3), 0.001);
    assertEquals(10000d, SqlFunctions.sround(12000, -4), 0.001);
    assertEquals(0d, SqlFunctions.sround(12000, -5), 0.001);
    assertEquals(12000d, SqlFunctions.sround(11999, -3), 0.001);

    assertEquals(-12350d, SqlFunctions.sround(-12345, -1), 0.001);
    assertEquals(-12300d, SqlFunctions.sround(-12345, -2), 0.001);
    assertEquals(-12000d, SqlFunctions.sround(-12345, -3), 0.001);
    assertEquals(-12000d, SqlFunctions.sround(-12000, -3), 0.001);
    assertEquals(-12000d, SqlFunctions.sround(-11999, -3), 0.001);
    assertEquals(-10000d, SqlFunctions.sround(-12000, -4), 0.001);
    assertEquals(0d, SqlFunctions.sround(-12000, -5), 0.001);
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

    thereAndBack(bytes);
    thereAndBack(emptyByteString.getBytes());
    thereAndBack(new byte[]{10, 0, 29, -80});

    assertThat(ByteString.of("ab12", 16).toString(16), equalTo("ab12"));
    assertThat(ByteString.of("AB0001DdeAD3", 16).toString(16),
        equalTo("ab0001ddead3"));
    assertThat(ByteString.of("", 16), equalTo(emptyByteString));
    try {
      ByteString x = ByteString.of("ABg0", 16);
      fail("expected error, got " + x);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), equalTo("invalid hex character: g"));
    }
    try {
      ByteString x = ByteString.of("ABC", 16);
      fail("expected error, got " + x);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), equalTo("hex string has odd length"));
    }

    final byte[] bytes4 = {10, 0, 1, -80};
    final ByteString byteString4 = new ByteString(bytes4);
    final byte[] bytes5 = {10, 0, 1, 127};
    final ByteString byteString5 = new ByteString(bytes5);
    final ByteString byteString6 = new ByteString(bytes4);

    assertThat(byteString4.compareTo(byteString5) > 0, is(true));
    assertThat(byteString4.compareTo(byteString6) == 0, is(true));
    assertThat(byteString5.compareTo(byteString4) < 0, is(true));
  }

  private void thereAndBack(byte[] bytes) {
    final ByteString byteString = new ByteString(bytes);
    final byte[] bytes2 = byteString.getBytes();
    assertThat(bytes, equalTo(bytes2));

    final String base64String = byteString.toBase64String();
    final ByteString byteString1 = ByteString.ofBase64(base64String);
    assertThat(byteString, equalTo(byteString1));
  }

  @Test public void testEasyLog10() {
    assertEquals(1, digitCount(0));
    assertEquals(1, digitCount(1));
    assertEquals(1, digitCount(9));
    assertEquals(2, digitCount(10));
    assertEquals(2, digitCount(11));
    assertEquals(2, digitCount(99));
    assertEquals(3, digitCount(100));
  }

  @Test public void testFloorDiv() {
    assertThat(floorDiv(13, 3), equalTo(4L));
    assertThat(floorDiv(12, 3), equalTo(4L));
    assertThat(floorDiv(11, 3), equalTo(3L));
    assertThat(floorDiv(-13, 3), equalTo(-5L));
    assertThat(floorDiv(-12, 3), equalTo(-4L));
    assertThat(floorDiv(-11, 3), equalTo(-4L));
    assertThat(floorDiv(0, 3), equalTo(0L));
    assertThat(floorDiv(1, 3), equalTo(0L));
    assertThat(floorDiv(-1, 3), is(-1L));
  }

  @Test public void testFloorMod() {
    assertThat(floorMod(13, 3), is(1L));
    assertThat(floorMod(12, 3), is(0L));
    assertThat(floorMod(11, 3), is(2L));
    assertThat(floorMod(-13, 3), is(2L));
    assertThat(floorMod(-12, 3), is(0L));
    assertThat(floorMod(-11, 3), is(1L));
    assertThat(floorMod(0, 3), is(0L));
    assertThat(floorMod(1, 3), is(1L));
    assertThat(floorMod(-1, 3), is(2L));
  }

  @Test public void testEqWithAny() {
    // Non-numeric same type equality check
    assertThat(SqlFunctions.eqAny("hello", "hello"), is(true));

    // Numeric types equality check
    assertThat(SqlFunctions.eqAny(1, 1L), is(true));
    assertThat(SqlFunctions.eqAny(1, 1.0D), is(true));
    assertThat(SqlFunctions.eqAny(1L, 1.0D), is(true));
    assertThat(SqlFunctions.eqAny(new BigDecimal(1L), 1), is(true));
    assertThat(SqlFunctions.eqAny(new BigDecimal(1L), 1L), is(true));
    assertThat(SqlFunctions.eqAny(new BigDecimal(1L), 1.0D), is(true));
    assertThat(SqlFunctions.eqAny(new BigDecimal(1L), new BigDecimal(1.0D)),
        is(true));

    // Non-numeric different type equality check
    assertThat(SqlFunctions.eqAny("2", 2), is(false));
  }

  @Test public void testNeWithAny() {
    // Non-numeric same type inequality check
    assertThat(SqlFunctions.neAny("hello", "world"), is(true));

    // Numeric types inequality check
    assertThat(SqlFunctions.neAny(1, 2L), is(true));
    assertThat(SqlFunctions.neAny(1, 2.0D), is(true));
    assertThat(SqlFunctions.neAny(1L, 2.0D), is(true));
    assertThat(SqlFunctions.neAny(new BigDecimal(2L), 1), is(true));
    assertThat(SqlFunctions.neAny(new BigDecimal(2L), 1L), is(true));
    assertThat(SqlFunctions.neAny(new BigDecimal(2L), 1.0D), is(true));
    assertThat(SqlFunctions.neAny(new BigDecimal(2L), new BigDecimal(1.0D)),
        is(true));

    // Non-numeric different type inequality check
    assertThat(SqlFunctions.neAny("2", 2), is(true));
  }

  @Test public void testLtWithAny() {
    // Non-numeric same type "less then" check
    assertThat(SqlFunctions.ltAny("apple", "banana"), is(true));

    // Numeric types "less than" check
    assertThat(SqlFunctions.ltAny(1, 2L), is(true));
    assertThat(SqlFunctions.ltAny(1, 2.0D), is(true));
    assertThat(SqlFunctions.ltAny(1L, 2.0D), is(true));
    assertThat(SqlFunctions.ltAny(new BigDecimal(1L), 2), is(true));
    assertThat(SqlFunctions.ltAny(new BigDecimal(1L), 2L), is(true));
    assertThat(SqlFunctions.ltAny(new BigDecimal(1L), 2.0D), is(true));
    assertThat(SqlFunctions.ltAny(new BigDecimal(1L), new BigDecimal(2.0D)),
        is(true));

    // Non-numeric different type but both implements Comparable
    // "less than" check
    try {
      assertThat(SqlFunctions.ltAny("1", 2L), is(false));
      fail("'lt' on non-numeric different type is not possible");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(),
          is("Invalid types for comparison: class java.lang.String < "
              + "class java.lang.Long"));
    }
  }

  @Test public void testLeWithAny() {
    // Non-numeric same type "less or equal" check
    assertThat(SqlFunctions.leAny("apple", "banana"), is(true));
    assertThat(SqlFunctions.leAny("apple", "apple"), is(true));

    // Numeric types "less or equal" check
    assertThat(SqlFunctions.leAny(1, 2L), is(true));
    assertThat(SqlFunctions.leAny(1, 1L), is(true));
    assertThat(SqlFunctions.leAny(1, 2.0D), is(true));
    assertThat(SqlFunctions.leAny(1, 1.0D), is(true));
    assertThat(SqlFunctions.leAny(1L, 2.0D), is(true));
    assertThat(SqlFunctions.leAny(1L, 1.0D), is(true));
    assertThat(SqlFunctions.leAny(new BigDecimal(1L), 2), is(true));
    assertThat(SqlFunctions.leAny(new BigDecimal(1L), 1), is(true));
    assertThat(SqlFunctions.leAny(new BigDecimal(1L), 2L), is(true));
    assertThat(SqlFunctions.leAny(new BigDecimal(1L), 1L), is(true));
    assertThat(SqlFunctions.leAny(new BigDecimal(1L), 2.0D), is(true));
    assertThat(SqlFunctions.leAny(new BigDecimal(1L), 1.0D), is(true));
    assertThat(SqlFunctions.leAny(new BigDecimal(1L), new BigDecimal(2.0D)),
        is(true));
    assertThat(SqlFunctions.leAny(new BigDecimal(1L), new BigDecimal(1.0D)),
        is(true));

    // Non-numeric different type but both implements Comparable
    // "less or equal" check
    try {
      assertThat(SqlFunctions.leAny("2", 2L), is(false));
      fail("'le' on non-numeric different type is not possible");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(),
          is("Invalid types for comparison: class java.lang.String <= "
              + "class java.lang.Long"));
    }
  }

  @Test public void testGtWithAny() {
    // Non-numeric same type "greater then" check
    assertThat(SqlFunctions.gtAny("banana", "apple"), is(true));

    // Numeric types "greater than" check
    assertThat(SqlFunctions.gtAny(2, 1L), is(true));
    assertThat(SqlFunctions.gtAny(2, 1.0D), is(true));
    assertThat(SqlFunctions.gtAny(2L, 1.0D), is(true));
    assertThat(SqlFunctions.gtAny(new BigDecimal(2L), 1), is(true));
    assertThat(SqlFunctions.gtAny(new BigDecimal(2L), 1L), is(true));
    assertThat(SqlFunctions.gtAny(new BigDecimal(2L), 1.0D), is(true));
    assertThat(SqlFunctions.gtAny(new BigDecimal(2L), new BigDecimal(1.0D)),
        is(true));

    // Non-numeric different type but both implements Comparable
    // "greater than" check
    try {
      assertThat(SqlFunctions.gtAny("2", 1L), is(false));
      fail("'gt' on non-numeric different type is not possible");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(),
          is("Invalid types for comparison: class java.lang.String > "
              + "class java.lang.Long"));
    }
  }

  @Test public void testGeWithAny() {
    // Non-numeric same type "greater or equal" check
    assertThat(SqlFunctions.geAny("banana", "apple"), is(true));
    assertThat(SqlFunctions.geAny("apple", "apple"), is(true));

    // Numeric types "greater or equal" check
    assertThat(SqlFunctions.geAny(2, 1L), is(true));
    assertThat(SqlFunctions.geAny(1, 1L), is(true));
    assertThat(SqlFunctions.geAny(2, 1.0D), is(true));
    assertThat(SqlFunctions.geAny(1, 1.0D), is(true));
    assertThat(SqlFunctions.geAny(2L, 1.0D), is(true));
    assertThat(SqlFunctions.geAny(1L, 1.0D), is(true));
    assertThat(SqlFunctions.geAny(new BigDecimal(2L), 1), is(true));
    assertThat(SqlFunctions.geAny(new BigDecimal(1L), 1), is(true));
    assertThat(SqlFunctions.geAny(new BigDecimal(2L), 1L), is(true));
    assertThat(SqlFunctions.geAny(new BigDecimal(1L), 1L), is(true));
    assertThat(SqlFunctions.geAny(new BigDecimal(2L), 1.0D), is(true));
    assertThat(SqlFunctions.geAny(new BigDecimal(1L), 1.0D), is(true));
    assertThat(SqlFunctions.geAny(new BigDecimal(2L), new BigDecimal(1.0D)),
        is(true));
    assertThat(SqlFunctions.geAny(new BigDecimal(1L), new BigDecimal(1.0D)),
        is(true));

    // Non-numeric different type but both implements Comparable
    // "greater or equal" check
    try {
      assertThat(SqlFunctions.geAny("2", 2L), is(false));
      fail("'ge' on non-numeric different type is not possible");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(),
          is("Invalid types for comparison: class java.lang.String >= "
              + "class java.lang.Long"));
    }
  }

  @Test public void testPlusAny() {
    // null parameters
    assertNull(SqlFunctions.plusAny(null, null));
    assertNull(SqlFunctions.plusAny(null, 1));
    assertNull(SqlFunctions.plusAny(1, null));

    // Numeric types
    assertThat(SqlFunctions.plusAny(2, 1L), is((Object) new BigDecimal(3)));
    assertThat(SqlFunctions.plusAny(2, 1.0D), is((Object) new BigDecimal(3)));
    assertThat(SqlFunctions.plusAny(2L, 1.0D), is((Object) new BigDecimal(3)));
    assertThat(SqlFunctions.plusAny(new BigDecimal(2L), 1),
        is((Object) new BigDecimal(3)));
    assertThat(SqlFunctions.plusAny(new BigDecimal(2L), 1L),
        is((Object) new BigDecimal(3)));
    assertThat(SqlFunctions.plusAny(new BigDecimal(2L), 1.0D),
        is((Object) new BigDecimal(3)));
    assertThat(SqlFunctions.plusAny(new BigDecimal(2L), new BigDecimal(1.0D)),
        is((Object) new BigDecimal(3)));

    // Non-numeric type
    try {
      SqlFunctions.plusAny("2", 2L);
      fail("'plus' on non-numeric type is not possible");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(),
          is("Invalid types for arithmetic: class java.lang.String + "
              + "class java.lang.Long"));
    }
  }

  @Test public void testMinusAny() {
    // null parameters
    assertNull(SqlFunctions.minusAny(null, null));
    assertNull(SqlFunctions.minusAny(null, 1));
    assertNull(SqlFunctions.minusAny(1, null));

    // Numeric types
    assertThat(SqlFunctions.minusAny(2, 1L), is((Object) new BigDecimal(1)));
    assertThat(SqlFunctions.minusAny(2, 1.0D), is((Object) new BigDecimal(1)));
    assertThat(SqlFunctions.minusAny(2L, 1.0D), is((Object) new BigDecimal(1)));
    assertThat(SqlFunctions.minusAny(new BigDecimal(2L), 1),
        is((Object) new BigDecimal(1)));
    assertThat(SqlFunctions.minusAny(new BigDecimal(2L), 1L),
        is((Object) new BigDecimal(1)));
    assertThat(SqlFunctions.minusAny(new BigDecimal(2L), 1.0D),
        is((Object) new BigDecimal(1)));
    assertThat(SqlFunctions.minusAny(new BigDecimal(2L), new BigDecimal(1.0D)),
        is((Object) new BigDecimal(1)));

    // Non-numeric type
    try {
      SqlFunctions.minusAny("2", 2L);
      fail("'minus' on non-numeric type is not possible");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(),
          is("Invalid types for arithmetic: class java.lang.String - "
              + "class java.lang.Long"));
    }
  }

  @Test public void testMultiplyAny() {
    // null parameters
    assertNull(SqlFunctions.multiplyAny(null, null));
    assertNull(SqlFunctions.multiplyAny(null, 1));
    assertNull(SqlFunctions.multiplyAny(1, null));

    // Numeric types
    assertThat(SqlFunctions.multiplyAny(2, 1L), is((Object) new BigDecimal(2)));
    assertThat(SqlFunctions.multiplyAny(2, 1.0D),
        is((Object) new BigDecimal(2)));
    assertThat(SqlFunctions.multiplyAny(2L, 1.0D),
        is((Object) new BigDecimal(2)));
    assertThat(SqlFunctions.multiplyAny(new BigDecimal(2L), 1),
        is((Object) new BigDecimal(2)));
    assertThat(SqlFunctions.multiplyAny(new BigDecimal(2L), 1L),
        is((Object) new BigDecimal(2)));
    assertThat(SqlFunctions.multiplyAny(new BigDecimal(2L), 1.0D),
        is((Object) new BigDecimal(2)));
    assertThat(SqlFunctions.multiplyAny(new BigDecimal(2L), new BigDecimal(1.0D)),
        is((Object) new BigDecimal(2)));

    // Non-numeric type
    try {
      SqlFunctions.multiplyAny("2", 2L);
      fail("'multiply' on non-numeric type is not possible");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(),
          is("Invalid types for arithmetic: class java.lang.String * "
              + "class java.lang.Long"));
    }
  }

  @Test public void testDivideAny() {
    // null parameters
    assertNull(SqlFunctions.divideAny(null, null));
    assertNull(SqlFunctions.divideAny(null, 1));
    assertNull(SqlFunctions.divideAny(1, null));

    // Numeric types
    assertThat(SqlFunctions.divideAny(5, 2L),
        is((Object) new BigDecimal("2.5")));
    assertThat(SqlFunctions.divideAny(5, 2.0D),
        is((Object) new BigDecimal("2.5")));
    assertThat(SqlFunctions.divideAny(5L, 2.0D),
        is((Object) new BigDecimal("2.5")));
    assertThat(SqlFunctions.divideAny(new BigDecimal(5L), 2),
        is((Object) new BigDecimal(2.5)));
    assertThat(SqlFunctions.divideAny(new BigDecimal(5L), 2L),
        is((Object) new BigDecimal(2.5)));
    assertThat(SqlFunctions.divideAny(new BigDecimal(5L), 2.0D),
        is((Object) new BigDecimal(2.5)));
    assertThat(SqlFunctions.divideAny(new BigDecimal(5L), new BigDecimal(2.0D)),
        is((Object) new BigDecimal(2.5)));

    // Non-numeric type
    try {
      SqlFunctions.divideAny("5", 2L);
      fail("'divide' on non-numeric type is not possible");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(),
          is("Invalid types for arithmetic: class java.lang.String / "
              + "class java.lang.Long"));
    }
  }

}

// End SqlFunctionsTest.java
