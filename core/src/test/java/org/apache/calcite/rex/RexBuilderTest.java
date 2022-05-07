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
package org.apache.calcite.rex;

import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;
import org.apache.calcite.util.TimestampWithTimeZoneString;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Calendar;
import java.util.TimeZone;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Test for {@link RexBuilder}.
 */
class RexBuilderTest {

  private static final int PRECISION = 256;

  /**
   * MySqlTypeFactoryImpl provides a specific implementation of
   * {@link SqlTypeFactoryImpl} which sets precision to 256 for VARCHAR.
   */
  private static class MySqlTypeFactoryImpl extends SqlTypeFactoryImpl {

    MySqlTypeFactoryImpl(RelDataTypeSystem typeSystem) {
      super(typeSystem);
    }

    @Override public RelDataType createTypeWithNullability(
        final RelDataType type,
        final boolean nullable) {
      if (type.getSqlTypeName() == SqlTypeName.VARCHAR) {
        return new BasicSqlType(this.typeSystem, type.getSqlTypeName(),
            PRECISION);
      }
      return super.createTypeWithNullability(type, nullable);
    }
  }


  /**
   * Test RexBuilder.ensureType()
   */
  @Test void testEnsureTypeWithAny() {
    final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RexBuilder builder = new RexBuilder(typeFactory);

    RexNode node =  new RexLiteral(
            Boolean.TRUE, typeFactory.createSqlType(SqlTypeName.BOOLEAN), SqlTypeName.BOOLEAN);
    RexNode ensuredNode = builder.ensureType(
            typeFactory.createSqlType(SqlTypeName.ANY), node, true);

    assertEquals(node, ensuredNode);
  }

  /**
   * Test RexBuilder.ensureType()
   */
  @Test void testEnsureTypeWithItself() {
    final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RexBuilder builder = new RexBuilder(typeFactory);

    RexNode node =  new RexLiteral(
            Boolean.TRUE, typeFactory.createSqlType(SqlTypeName.BOOLEAN), SqlTypeName.BOOLEAN);
    RexNode ensuredNode = builder.ensureType(
            typeFactory.createSqlType(SqlTypeName.BOOLEAN), node, true);

    assertEquals(node, ensuredNode);
  }

  /**
   * Test RexBuilder.ensureType()
   */
  @Test void testEnsureTypeWithDifference() {
    final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RexBuilder builder = new RexBuilder(typeFactory);

    RexNode node =  new RexLiteral(
            Boolean.TRUE, typeFactory.createSqlType(SqlTypeName.BOOLEAN), SqlTypeName.BOOLEAN);
    RexNode ensuredNode = builder.ensureType(
            typeFactory.createSqlType(SqlTypeName.INTEGER), node, true);

    assertNotEquals(node, ensuredNode);
    assertEquals(ensuredNode.getType(), typeFactory.createSqlType(SqlTypeName.INTEGER));
  }

  private static final long MOON = -14159025000L;

  private static final int MOON_DAY = -164;

  private static final int MOON_TIME = 10575000;

  /** Tests {@link RexBuilder#makeTimestampLiteral(TimestampString, int)}. */
  @Test void testTimestampLiteral() {
    final RelDataTypeFactory typeFactory =
        new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    final RelDataType timestampType =
        typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
    final RelDataType timestampType3 =
        typeFactory.createSqlType(SqlTypeName.TIMESTAMP, 3);
    final RelDataType timestampType9 =
        typeFactory.createSqlType(SqlTypeName.TIMESTAMP, 9);
    final RelDataType timestampType18 =
        typeFactory.createSqlType(SqlTypeName.TIMESTAMP, 18);
    final RexBuilder builder = new RexBuilder(typeFactory);

    // Old way: provide a Calendar
    final Calendar calendar = Util.calendar();
    calendar.set(1969, Calendar.JULY, 21, 2, 56, 15); // one small step
    calendar.set(Calendar.MILLISECOND, 0);
    checkTimestamp(builder.makeLiteral(calendar, timestampType));

    // Old way #2: Provide a Long
    checkTimestamp(builder.makeLiteral(MOON, timestampType));

    // The new way
    final TimestampString ts = new TimestampString(1969, 7, 21, 2, 56, 15);
    checkTimestamp(builder.makeLiteral(ts, timestampType));

    // Now with milliseconds
    final TimestampString ts2 = ts.withMillis(56);
    assertThat(ts2.toString(), is("1969-07-21 02:56:15.056"));
    final RexLiteral literal2 = builder.makeLiteral(ts2, timestampType3);
    assertThat(literal2.getValueAs(TimestampString.class).toString(),
        is("1969-07-21 02:56:15.056"));

    // Now with nanoseconds
    final TimestampString ts3 = ts.withNanos(56);
    final RexLiteral literal3 = builder.makeLiteral(ts3, timestampType9);
    assertThat(literal3.getValueAs(TimestampString.class).toString(),
        is("1969-07-21 02:56:15"));
    final TimestampString ts3b = ts.withNanos(2345678);
    final RexLiteral literal3b = builder.makeLiteral(ts3b, timestampType9);
    assertThat(literal3b.getValueAs(TimestampString.class).toString(),
        is("1969-07-21 02:56:15.002"));

    // Now with a very long fraction
    final TimestampString ts4 = ts.withFraction("102030405060708090102");
    final RexLiteral literal4 = builder.makeLiteral(ts4, timestampType18);
    assertThat(literal4.getValueAs(TimestampString.class).toString(),
        is("1969-07-21 02:56:15.102"));

    // toString
    assertThat(ts2.round(1).toString(), is("1969-07-21 02:56:15"));
    assertThat(ts2.round(2).toString(), is("1969-07-21 02:56:15.05"));
    assertThat(ts2.round(3).toString(), is("1969-07-21 02:56:15.056"));
    assertThat(ts2.round(4).toString(), is("1969-07-21 02:56:15.056"));

    assertThat(ts2.toString(6), is("1969-07-21 02:56:15.056000"));
    assertThat(ts2.toString(1), is("1969-07-21 02:56:15.0"));
    assertThat(ts2.toString(0), is("1969-07-21 02:56:15"));

    assertThat(ts2.round(0).toString(), is("1969-07-21 02:56:15"));
    assertThat(ts2.round(0).toString(0), is("1969-07-21 02:56:15"));
    assertThat(ts2.round(0).toString(1), is("1969-07-21 02:56:15.0"));
    assertThat(ts2.round(0).toString(2), is("1969-07-21 02:56:15.00"));

    assertThat(TimestampString.fromMillisSinceEpoch(1456513560123L).toString(),
        is("2016-02-26 19:06:00.123"));
  }

  private void checkTimestamp(RexLiteral literal) {
    assertThat(literal.toString(), is("1969-07-21 02:56:15"));
    assertThat(literal.getValue() instanceof Calendar, is(true));
    assertThat(literal.getValue2() instanceof Long, is(true));
    assertThat(literal.getValue3() instanceof Long, is(true));
    assertThat((Long) literal.getValue2(), is(MOON));
    assertThat(literal.getValueAs(Calendar.class), notNullValue());
    assertThat(literal.getValueAs(TimestampString.class), notNullValue());
  }

  /** Tests
   * {@link RexBuilder#makeTimestampWithLocalTimeZoneLiteral(TimestampString, int)}. */
  @Test void testTimestampWithLocalTimeZoneLiteral() {
    final RelDataTypeFactory typeFactory =
        new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    final RelDataType timestampType =
        typeFactory.createSqlType(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
    final RelDataType timestampType3 =
        typeFactory.createSqlType(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, 3);
    final RelDataType timestampType9 =
        typeFactory.createSqlType(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, 9);
    final RelDataType timestampType18 =
        typeFactory.createSqlType(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, 18);
    final RexBuilder builder = new RexBuilder(typeFactory);

    // The new way
    final TimestampWithTimeZoneString ts = new TimestampWithTimeZoneString(
        1969, 7, 21, 2, 56, 15, TimeZone.getTimeZone("PST").getID());
    checkTimestampWithLocalTimeZone(
        builder.makeLiteral(ts.getLocalTimestampString(), timestampType));

    // Now with milliseconds
    final TimestampWithTimeZoneString ts2 = ts.withMillis(56);
    assertThat(ts2.toString(), is("1969-07-21 02:56:15.056 PST"));
    final RexLiteral literal2 =
        builder.makeLiteral(ts2.getLocalTimestampString(), timestampType3);
    assertThat(literal2.getValue().toString(), is("1969-07-21 02:56:15.056"));

    // Now with nanoseconds
    final TimestampWithTimeZoneString ts3 = ts.withNanos(56);
    final RexLiteral literal3 =
        builder.makeLiteral(ts3.getLocalTimestampString(), timestampType9);
    assertThat(literal3.getValueAs(TimestampString.class).toString(),
        is("1969-07-21 02:56:15"));
    final TimestampWithTimeZoneString ts3b = ts.withNanos(2345678);
    final RexLiteral literal3b =
        builder.makeLiteral(ts3b.getLocalTimestampString(), timestampType9);
    assertThat(literal3b.getValueAs(TimestampString.class).toString(),
        is("1969-07-21 02:56:15.002"));

    // Now with a very long fraction
    final TimestampWithTimeZoneString ts4 = ts.withFraction("102030405060708090102");
    final RexLiteral literal4 =
        builder.makeLiteral(ts4.getLocalTimestampString(), timestampType18);
    assertThat(literal4.getValueAs(TimestampString.class).toString(),
        is("1969-07-21 02:56:15.102"));

    // toString
    assertThat(ts2.round(1).toString(), is("1969-07-21 02:56:15 PST"));
    assertThat(ts2.round(2).toString(), is("1969-07-21 02:56:15.05 PST"));
    assertThat(ts2.round(3).toString(), is("1969-07-21 02:56:15.056 PST"));
    assertThat(ts2.round(4).toString(), is("1969-07-21 02:56:15.056 PST"));

    assertThat(ts2.toString(6), is("1969-07-21 02:56:15.056000 PST"));
    assertThat(ts2.toString(1), is("1969-07-21 02:56:15.0 PST"));
    assertThat(ts2.toString(0), is("1969-07-21 02:56:15 PST"));

    assertThat(ts2.round(0).toString(), is("1969-07-21 02:56:15 PST"));
    assertThat(ts2.round(0).toString(0), is("1969-07-21 02:56:15 PST"));
    assertThat(ts2.round(0).toString(1), is("1969-07-21 02:56:15.0 PST"));
    assertThat(ts2.round(0).toString(2), is("1969-07-21 02:56:15.00 PST"));
  }

  private void checkTimestampWithLocalTimeZone(RexLiteral literal) {
    assertThat(literal.toString(),
        is("1969-07-21 02:56:15:TIMESTAMP_WITH_LOCAL_TIME_ZONE(0)"));
    assertThat(literal.getValue() instanceof TimestampString, is(true));
    assertThat(literal.getValue2() instanceof Long, is(true));
    assertThat(literal.getValue3() instanceof Long, is(true));
  }

  /** Tests {@link RexBuilder#makeTimeLiteral(TimeString, int)}. */
  @Test void testTimeLiteral() {
    final RelDataTypeFactory typeFactory =
        new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType timeType = typeFactory.createSqlType(SqlTypeName.TIME);
    final RelDataType timeType3 =
        typeFactory.createSqlType(SqlTypeName.TIME, 3);
    final RelDataType timeType9 =
        typeFactory.createSqlType(SqlTypeName.TIME, 9);
    final RelDataType timeType18 =
        typeFactory.createSqlType(SqlTypeName.TIME, 18);
    final RexBuilder builder = new RexBuilder(typeFactory);

    // Old way: provide a Calendar
    final Calendar calendar = Util.calendar();
    calendar.set(1969, Calendar.JULY, 21, 2, 56, 15); // one small step
    calendar.set(Calendar.MILLISECOND, 0);
    checkTime(builder.makeLiteral(calendar, timeType));

    // Old way #2: Provide a Long
    checkTime(builder.makeLiteral(MOON_TIME, timeType));

    // The new way
    final TimeString t = new TimeString(2, 56, 15);
    assertThat(t.getMillisOfDay(), is(10575000));
    checkTime(builder.makeLiteral(t, timeType));

    // Now with milliseconds
    final TimeString t2 = t.withMillis(56);
    assertThat(t2.getMillisOfDay(), is(10575056));
    assertThat(t2.toString(), is("02:56:15.056"));
    final RexLiteral literal2 = builder.makeLiteral(t2, timeType3);
    assertThat(literal2.getValueAs(TimeString.class).toString(),
        is("02:56:15.056"));

    // Now with nanoseconds
    final TimeString t3 = t.withNanos(2345678);
    assertThat(t3.getMillisOfDay(), is(10575002));
    final RexLiteral literal3 = builder.makeLiteral(t3, timeType9);
    assertThat(literal3.getValueAs(TimeString.class).toString(),
        is("02:56:15.002"));

    // Now with a very long fraction
    final TimeString t4 = t.withFraction("102030405060708090102");
    assertThat(t4.getMillisOfDay(), is(10575102));
    final RexLiteral literal4 = builder.makeLiteral(t4, timeType18);
    assertThat(literal4.getValueAs(TimeString.class).toString(),
        is("02:56:15.102"));

    // toString
    assertThat(t2.round(1).toString(), is("02:56:15"));
    assertThat(t2.round(2).toString(), is("02:56:15.05"));
    assertThat(t2.round(3).toString(), is("02:56:15.056"));
    assertThat(t2.round(4).toString(), is("02:56:15.056"));

    assertThat(t2.toString(6), is("02:56:15.056000"));
    assertThat(t2.toString(1), is("02:56:15.0"));
    assertThat(t2.toString(0), is("02:56:15"));

    assertThat(t2.round(0).toString(), is("02:56:15"));
    assertThat(t2.round(0).toString(0), is("02:56:15"));
    assertThat(t2.round(0).toString(1), is("02:56:15.0"));
    assertThat(t2.round(0).toString(2), is("02:56:15.00"));

    assertThat(TimeString.fromMillisOfDay(53560123).toString(),
        is("14:52:40.123"));
  }

  private void checkTime(RexLiteral literal) {
    assertThat(literal.toString(), is("02:56:15"));
    assertThat(literal.getValue() instanceof Calendar, is(true));
    assertThat(literal.getValue2() instanceof Integer, is(true));
    assertThat(literal.getValue3() instanceof Integer, is(true));
    assertThat((Integer) literal.getValue2(), is(MOON_TIME));
    assertThat(literal.getValueAs(Calendar.class), notNullValue());
    assertThat(literal.getValueAs(TimeString.class), notNullValue());
  }

  /** Tests {@link RexBuilder#makeDateLiteral(DateString)}. */
  @Test void testDateLiteral() {
    final RelDataTypeFactory typeFactory =
        new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType dateType = typeFactory.createSqlType(SqlTypeName.DATE);
    final RexBuilder builder = new RexBuilder(typeFactory);

    // Old way: provide a Calendar
    final Calendar calendar = Util.calendar();
    calendar.set(1969, Calendar.JULY, 21); // one small step
    calendar.set(Calendar.MILLISECOND, 0);
    checkDate(builder.makeLiteral(calendar, dateType));

    // Old way #2: Provide in Integer
    checkDate(builder.makeLiteral(MOON_DAY, dateType));

    // The new way
    final DateString d = new DateString(1969, 7, 21);
    checkDate(builder.makeLiteral(d, dateType));
  }

  private void checkDate(RexLiteral literal) {
    assertThat(literal.toString(), is("1969-07-21"));
    assertThat(literal.getValue() instanceof Calendar, is(true));
    assertThat(literal.getValue2() instanceof Integer, is(true));
    assertThat(literal.getValue3() instanceof Integer, is(true));
    assertThat((Integer) literal.getValue2(), is(MOON_DAY));
    assertThat(literal.getValueAs(Calendar.class), notNullValue());
    assertThat(literal.getValueAs(DateString.class), notNullValue());
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2306">[CALCITE-2306]
   * AssertionError in {@link RexLiteral#getValue3} with null literal of type
   * DECIMAL</a>. */
  @Test void testDecimalLiteral() {
    final RelDataTypeFactory typeFactory =
        new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    final RelDataType type = typeFactory.createSqlType(SqlTypeName.DECIMAL);
    final RexBuilder builder = new RexBuilder(typeFactory);
    final RexLiteral literal = builder.makeExactLiteral(null, type);
    assertThat(literal.getValue3(), nullValue());
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3587">[CALCITE-3587]
   * RexBuilder may lose decimal fraction for creating literal with DECIMAL type</a>.
   */
  @Test void testDecimal() {
    final RelDataTypeFactory typeFactory =
        new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    final RelDataType type = typeFactory.createSqlType(SqlTypeName.DECIMAL, 4, 2);
    final RexBuilder builder = new RexBuilder(typeFactory);
    try {
      builder.makeLiteral(12.3, type);
      fail();
    } catch (AssertionError e) {
      assertThat(e.getMessage(),
          is("java.lang.Double is not compatible with DECIMAL, try to use makeExactLiteral"));
    }
  }

  /** Tests {@link DateString} year range. */
  @Test void testDateStringYearError() {
    try {
      final DateString dateString = new DateString(11969, 7, 21);
      fail("expected exception, got " + dateString);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString("Year out of range: [11969]"));
    }
    try {
      final DateString dateString = new DateString("12345-01-23");
      fail("expected exception, got " + dateString);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(),
          containsString("Invalid date format: [12345-01-23]"));
    }
  }

  /** Tests {@link DateString} month range. */
  @Test void testDateStringMonthError() {
    try {
      final DateString dateString = new DateString(1969, 27, 21);
      fail("expected exception, got " + dateString);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString("Month out of range: [27]"));
    }
    try {
      final DateString dateString = new DateString("1234-13-02");
      fail("expected exception, got " + dateString);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString("Month out of range: [13]"));
    }
  }

  /** Tests {@link DateString} day range. */
  @Test void testDateStringDayError() {
    try {
      final DateString dateString = new DateString(1969, 7, 41);
      fail("expected exception, got " + dateString);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString("Day out of range: [41]"));
    }
    try {
      final DateString dateString = new DateString("1234-01-32");
      fail("expected exception, got " + dateString);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString("Day out of range: [32]"));
    }
    // We don't worry about the number of days in a month. 30 is in range.
    final DateString dateString = new DateString("1234-02-30");
    assertThat(dateString, notNullValue());
  }

  /** Tests {@link TimeString} hour range. */
  @Test void testTimeStringHourError() {
    try {
      final TimeString timeString = new TimeString(111, 34, 56);
      fail("expected exception, got " + timeString);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString("Hour out of range: [111]"));
    }
    try {
      final TimeString timeString = new TimeString("24:00:00");
      fail("expected exception, got " + timeString);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString("Hour out of range: [24]"));
    }
    try {
      final TimeString timeString = new TimeString("24:00");
      fail("expected exception, got " + timeString);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(),
          containsString("Invalid time format: [24:00]"));
    }
  }

  /** Tests {@link TimeString} minute range. */
  @Test void testTimeStringMinuteError() {
    try {
      final TimeString timeString = new TimeString(12, 334, 56);
      fail("expected exception, got " + timeString);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString("Minute out of range: [334]"));
    }
    try {
      final TimeString timeString = new TimeString("12:60:23");
      fail("expected exception, got " + timeString);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString("Minute out of range: [60]"));
    }
  }

  /** Tests {@link TimeString} second range. */
  @Test void testTimeStringSecondError() {
    try {
      final TimeString timeString = new TimeString(12, 34, 567);
      fail("expected exception, got " + timeString);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString("Second out of range: [567]"));
    }
    try {
      final TimeString timeString = new TimeString(12, 34, -4);
      fail("expected exception, got " + timeString);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString("Second out of range: [-4]"));
    }
    try {
      final TimeString timeString = new TimeString("12:34:60");
      fail("expected exception, got " + timeString);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString("Second out of range: [60]"));
    }
  }

  /**
   * Test string literal encoding.
   */
  @Test void testStringLiteral() {
    final RelDataTypeFactory typeFactory =
        new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    final RelDataType varchar =
        typeFactory.createSqlType(SqlTypeName.VARCHAR);
    final RexBuilder builder = new RexBuilder(typeFactory);

    final NlsString latin1 = new NlsString("foobar", "LATIN1", SqlCollation.IMPLICIT);
    final NlsString utf8 = new NlsString("foobar", "UTF8", SqlCollation.IMPLICIT);

    RexLiteral literal = builder.makePreciseStringLiteral("foobar");
    assertEquals("'foobar'", literal.toString());
    literal = builder.makePreciseStringLiteral(
        new ByteString(new byte[] { 'f', 'o', 'o', 'b', 'a', 'r'}),
        "UTF8",
        SqlCollation.IMPLICIT);
    assertEquals("_UTF8'foobar'", literal.toString());
    assertEquals("_UTF8'foobar':CHAR(6) CHARACTER SET \"UTF-8\"",
        literal.computeDigest(RexDigestIncludeType.ALWAYS));
    literal = builder.makePreciseStringLiteral(
        new ByteString("\u82f1\u56fd".getBytes(StandardCharsets.UTF_8)),
        "UTF8",
        SqlCollation.IMPLICIT);
    assertEquals("_UTF8'\u82f1\u56fd'", literal.toString());
    // Test again to check decode cache.
    literal = builder.makePreciseStringLiteral(
        new ByteString("\u82f1".getBytes(StandardCharsets.UTF_8)),
        "UTF8",
        SqlCollation.IMPLICIT);
    assertEquals("_UTF8'\u82f1'", literal.toString());
    try {
      literal = builder.makePreciseStringLiteral(
          new ByteString("\u82f1\u56fd".getBytes(StandardCharsets.UTF_8)),
          "GB2312",
          SqlCollation.IMPLICIT);
      fail("expected exception, got " + literal);
    } catch (RuntimeException e) {
      assertThat(e.getMessage(), containsString("Failed to encode"));
    }
    literal = builder.makeLiteral(latin1, varchar);
    assertEquals("_LATIN1'foobar'", literal.toString());
    literal = builder.makeLiteral(utf8, varchar);
    assertEquals("_UTF8'foobar'", literal.toString());
  }

  /** Tests {@link RexBuilder#makeExactLiteral(java.math.BigDecimal)}. */
  @Test void testBigDecimalLiteral() {
    final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(new RelDataTypeSystemImpl() {
      @Override public int getMaxPrecision(SqlTypeName typeName) {
        return 38;
      }
    });
    final RexBuilder builder = new RexBuilder(typeFactory);
    checkBigDecimalLiteral(builder, "25");
    checkBigDecimalLiteral(builder, "9.9");
    checkBigDecimalLiteral(builder, "0");
    checkBigDecimalLiteral(builder, "-75.5");
    checkBigDecimalLiteral(builder, "10000000");
    checkBigDecimalLiteral(builder, "100000.111111111111111111");
    checkBigDecimalLiteral(builder, "-100000.111111111111111111");
    checkBigDecimalLiteral(builder, "73786976294838206464"); // 2^66
    checkBigDecimalLiteral(builder, "-73786976294838206464");
  }

  @Test void testMakeIn() {
    final RelDataTypeFactory typeFactory =
            new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    final RexBuilder rexBuilder = new RexBuilder(typeFactory);
    final RelDataType floatType = typeFactory.createSqlType(SqlTypeName.FLOAT);
    RexNode left = rexBuilder.makeInputRef(floatType, 0);
    final RexNode literal1 = rexBuilder.makeLiteral(1.0f, floatType);
    final RexNode literal2 = rexBuilder.makeLiteral(2.0f, floatType);
    RexNode inCall = rexBuilder.makeIn(left, ImmutableList.of(literal1, literal2));
    assertThat(inCall.getKind(), is(SqlKind.SEARCH));
  }

  /** Tests {@link RexCopier#visitOver(RexOver)}. */
  @Test void testCopyOver() {
    final RelDataTypeFactory sourceTypeFactory =
        new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType type = sourceTypeFactory.createSqlType(SqlTypeName.VARCHAR, 65536);

    final RelDataTypeFactory targetTypeFactory =
        new MySqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    final RexBuilder builder = new RexBuilder(targetTypeFactory);

    final RexOver node = (RexOver) builder.makeOver(type,
        SqlStdOperatorTable.COUNT,
        ImmutableList.of(builder.makeInputRef(type, 0)),
        ImmutableList.of(builder.makeInputRef(type, 1)),
        ImmutableList.of(
            new RexFieldCollation(
                builder.makeInputRef(type, 2), ImmutableSet.of())),
        RexWindowBounds.UNBOUNDED_PRECEDING,
        RexWindowBounds.CURRENT_ROW,
        true, true, false, false, false);
    final RexNode copy = builder.copy(node);
    assertTrue(copy instanceof RexOver);

    RexOver result = (RexOver) copy;
    assertThat(result.getType().getSqlTypeName(), is(SqlTypeName.VARCHAR));
    assertThat(result.getType().getPrecision(), is(PRECISION));
    assertThat(result.getWindow(), is(node.getWindow()));
    assertThat(result.getAggOperator(), is(node.getAggOperator()));
    assertThat(result.getAggOperator(), is(node.getAggOperator()));
    assertEquals(node.isDistinct(), result.isDistinct());
    assertEquals(node.ignoreNulls(), result.ignoreNulls());
    for (int i = 0; i < node.getOperands().size(); i++) {
      assertThat(result.getOperands().get(i).getType().getSqlTypeName(),
          is(node.getOperands().get(i).getType().getSqlTypeName()));
      assertThat(result.getOperands().get(i).getType().getPrecision(),
          is(PRECISION));
    }
  }

  /** Tests {@link RexCopier#visitCorrelVariable(RexCorrelVariable)}. */
  @Test void testCopyCorrelVariable() {
    final RelDataTypeFactory sourceTypeFactory =
        new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType type = sourceTypeFactory.createSqlType(SqlTypeName.VARCHAR, 65536);

    final RelDataTypeFactory targetTypeFactory =
        new MySqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    final RexBuilder builder = new RexBuilder(targetTypeFactory);

    final RexCorrelVariable node =
        (RexCorrelVariable) builder.makeCorrel(type, new CorrelationId(0));
    final RexNode copy = builder.copy(node);
    assertTrue(copy instanceof RexCorrelVariable);

    final RexCorrelVariable result = (RexCorrelVariable) copy;
    assertThat(result.id, is(node.id));
    assertThat(result.getType().getSqlTypeName(), is(SqlTypeName.VARCHAR));
    assertThat(result.getType().getPrecision(), is(PRECISION));
  }

  /** Tests {@link RexCopier#visitLocalRef(RexLocalRef)}. */
  @Test void testCopyLocalRef() {
    final RelDataTypeFactory sourceTypeFactory =
        new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType type = sourceTypeFactory.createSqlType(SqlTypeName.VARCHAR, 65536);

    final RelDataTypeFactory targetTypeFactory =
        new MySqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    final RexBuilder builder = new RexBuilder(targetTypeFactory);

    final RexLocalRef node = new RexLocalRef(0, type);
    final RexNode copy = builder.copy(node);
    assertTrue(copy instanceof RexLocalRef);

    final RexLocalRef result = (RexLocalRef) copy;
    assertThat(result.getIndex(), is(node.getIndex()));
    assertThat(result.getType().getSqlTypeName(), is(SqlTypeName.VARCHAR));
    assertThat(result.getType().getPrecision(), is(PRECISION));
  }

  /** Tests {@link RexCopier#visitDynamicParam(RexDynamicParam)}. */
  @Test void testCopyDynamicParam() {
    final RelDataTypeFactory sourceTypeFactory =
        new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType type = sourceTypeFactory.createSqlType(SqlTypeName.VARCHAR, 65536);

    final RelDataTypeFactory targetTypeFactory =
        new MySqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    final RexBuilder builder = new RexBuilder(targetTypeFactory);

    final RexDynamicParam node = builder.makeDynamicParam(type, 0);
    final RexNode copy = builder.copy(node);
    assertTrue(copy instanceof RexDynamicParam);

    final RexDynamicParam result = (RexDynamicParam) copy;
    assertThat(result.getIndex(), is(node.getIndex()));
    assertThat(result.getType().getSqlTypeName(), is(SqlTypeName.VARCHAR));
    assertThat(result.getType().getPrecision(), is(PRECISION));
  }

  /** Tests {@link RexCopier#visitRangeRef(RexRangeRef)}. */
  @Test void testCopyRangeRef() {
    final RelDataTypeFactory sourceTypeFactory =
        new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType type = sourceTypeFactory.createSqlType(SqlTypeName.VARCHAR, 65536);

    final RelDataTypeFactory targetTypeFactory =
        new MySqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    final RexBuilder builder = new RexBuilder(targetTypeFactory);

    final RexRangeRef node = builder.makeRangeReference(type, 1, true);
    final RexNode copy = builder.copy(node);
    assertTrue(copy instanceof RexRangeRef);

    final RexRangeRef result = (RexRangeRef) copy;
    assertThat(result.getOffset(), is(node.getOffset()));
    assertThat(result.getType().getSqlTypeName(), is(SqlTypeName.VARCHAR));
    assertThat(result.getType().getPrecision(), is(PRECISION));
  }

  private void checkBigDecimalLiteral(RexBuilder builder, String val) {
    final RexLiteral literal = builder.makeExactLiteral(new BigDecimal(val));
    assertThat("builder.makeExactLiteral(new BigDecimal(" + val
            + ")).getValueAs(BigDecimal.class).toString()",
        literal.getValueAs(BigDecimal.class).toString(), is(val));
  }

  @Test void testValidateRexFieldAccess() {
    final RelDataTypeFactory typeFactory =
        new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    final RexBuilder builder = new RexBuilder(typeFactory);

    RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
    RelDataType longType = typeFactory.createSqlType(SqlTypeName.BIGINT);

    RelDataType structType = typeFactory.createStructType(
        Arrays.asList(intType, longType), Arrays.asList("x", "y"));
    RexInputRef inputRef = builder.makeInputRef(structType, 0);

    // construct RexFieldAccess fails because of negative index
    IllegalArgumentException e1 = assertThrows(IllegalArgumentException.class, () -> {
      RelDataTypeField field = new RelDataTypeFieldImpl("z", -1, intType);
      new RexFieldAccess(inputRef, field);
    });
    assertThat(e1.getMessage(),
        is("Field #-1: z INTEGER does not exist for expression $0"));

    // construct RexFieldAccess fails because of too large index
    IllegalArgumentException e2 = assertThrows(IllegalArgumentException.class, () -> {
      RelDataTypeField field = new RelDataTypeFieldImpl("z", 2, intType);
      new RexFieldAccess(inputRef, field);
    });
    assertThat(e2.getMessage(),
        is("Field #2: z INTEGER does not exist for expression $0"));

    // construct RexFieldAccess fails because of incorrect type
    IllegalArgumentException e3 = assertThrows(IllegalArgumentException.class, () -> {
      RelDataTypeField field = new RelDataTypeFieldImpl("z", 0, longType);
      new RexFieldAccess(inputRef, field);
    });
    assertThat(e3.getMessage(),
        is("Field #0: z BIGINT does not exist for expression $0"));

    // construct RexFieldAccess successfully
    RelDataTypeField field = new RelDataTypeFieldImpl("x", 0, intType);
    RexFieldAccess fieldAccess = new RexFieldAccess(inputRef, field);
    RexChecker checker = new RexChecker(structType, () -> null, Litmus.THROW);
    assertThat(fieldAccess.accept(checker), is(true));
  }

  /** Emulate a user defined type. */
  private static class UDT extends RelDataTypeImpl {
    UDT() {
      this.digest = "(udt)NOT NULL";
    }

    @Override protected void generateTypeString(StringBuilder sb, boolean withDetail) {
      sb.append("udt");
    }
  }

  @Test void testUDTLiteralDigest() {
    RexLiteral literal = new RexLiteral(new BigDecimal(0L), new UDT(), SqlTypeName.BIGINT);

    // when the space before "NOT NULL" is missing, the digest is not correct
    // and the suffix should not be removed.
    assertThat(literal.digest, is("0L:(udt)NOT NULL"));
  }
}
