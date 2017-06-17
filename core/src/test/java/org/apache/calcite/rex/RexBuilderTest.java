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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;
import org.apache.calcite.util.Util;

import org.junit.Test;

import java.util.Calendar;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;

/**
 * Test for {@link RexBuilder}.
 */
public class RexBuilderTest {

  /**
   * Test RexBuilder.ensureType()
   */
  @Test
  public void testEnsureTypeWithAny() {
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
  @Test
  public void testEnsureTypeWithItself() {
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
  @Test
  public void testEnsureTypeWithDifference() {
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
  @Test public void testTimestampLiteral() {
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
    checkTimestamp(builder.makeLiteral(calendar, timestampType, false));

    // Old way #2: Provide a Long
    checkTimestamp(builder.makeLiteral(MOON, timestampType, false));

    // The new way
    final TimestampString ts = new TimestampString(1969, 7, 21, 2, 56, 15);
    checkTimestamp(builder.makeLiteral(ts, timestampType, false));

    // Now with milliseconds
    final TimestampString ts2 = ts.withMillis(56);
    assertThat(ts2.toString(), is("1969-07-21 02:56:15.056"));
    final RexNode literal2 = builder.makeLiteral(ts2, timestampType3, false);
    assertThat(((RexLiteral) literal2).getValueAs(TimestampString.class)
            .toString(), is("1969-07-21 02:56:15.056"));

    // Now with nanoseconds
    final TimestampString ts3 = ts.withNanos(56);
    final RexNode literal3 = builder.makeLiteral(ts3, timestampType9, false);
    assertThat(((RexLiteral) literal3).getValueAs(TimestampString.class)
            .toString(), is("1969-07-21 02:56:15"));
    final TimestampString ts3b = ts.withNanos(2345678);
    final RexNode literal3b = builder.makeLiteral(ts3b, timestampType9, false);
    assertThat(((RexLiteral) literal3b).getValueAs(TimestampString.class)
            .toString(), is("1969-07-21 02:56:15.002"));

    // Now with a very long fraction
    final TimestampString ts4 = ts.withFraction("102030405060708090102");
    final RexNode literal4 = builder.makeLiteral(ts4, timestampType18, false);
    assertThat(((RexLiteral) literal4).getValueAs(TimestampString.class)
            .toString(), is("1969-07-21 02:56:15.102"));

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

  private void checkTimestamp(RexNode node) {
    assertThat(node.toString(), is("1969-07-21 02:56:15"));
    RexLiteral literal = (RexLiteral) node;
    assertThat(literal.getValue() instanceof Calendar, is(true));
    assertThat(literal.getValue2() instanceof Long, is(true));
    assertThat(literal.getValue3() instanceof Long, is(true));
    assertThat((Long) literal.getValue2(), is(MOON));
    assertThat(literal.getValueAs(Calendar.class), notNullValue());
    assertThat(literal.getValueAs(TimestampString.class), notNullValue());
  }

  /** Tests {@link RexBuilder#makeTimeLiteral(TimeString, int)}. */
  @Test public void testTimeLiteral() {
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
    checkTime(builder.makeLiteral(calendar, timeType, false));

    // Old way #2: Provide a Long
    checkTime(builder.makeLiteral(MOON_TIME, timeType, false));

    // The new way
    final TimeString t = new TimeString(2, 56, 15);
    assertThat(t.getMillisOfDay(), is(10575000));
    checkTime(builder.makeLiteral(t, timeType, false));

    // Now with milliseconds
    final TimeString t2 = t.withMillis(56);
    assertThat(t2.getMillisOfDay(), is(10575056));
    assertThat(t2.toString(), is("02:56:15.056"));
    final RexNode literal2 = builder.makeLiteral(t2, timeType3, false);
    assertThat(((RexLiteral) literal2).getValueAs(TimeString.class)
        .toString(), is("02:56:15.056"));

    // Now with nanoseconds
    final TimeString t3 = t.withNanos(2345678);
    assertThat(t3.getMillisOfDay(), is(10575002));
    final RexNode literal3 = builder.makeLiteral(t3, timeType9, false);
    assertThat(((RexLiteral) literal3).getValueAs(TimeString.class)
        .toString(), is("02:56:15.002"));

    // Now with a very long fraction
    final TimeString t4 = t.withFraction("102030405060708090102");
    assertThat(t4.getMillisOfDay(), is(10575102));
    final RexNode literal4 = builder.makeLiteral(t4, timeType18, false);
    assertThat(((RexLiteral) literal4).getValueAs(TimeString.class)
        .toString(), is("02:56:15.102"));

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

  private void checkTime(RexNode node) {
    assertThat(node.toString(), is("02:56:15"));
    RexLiteral literal = (RexLiteral) node;
    assertThat(literal.getValue() instanceof Calendar, is(true));
    assertThat(literal.getValue2() instanceof Integer, is(true));
    assertThat(literal.getValue3() instanceof Integer, is(true));
    assertThat((Integer) literal.getValue2(), is(MOON_TIME));
    assertThat(literal.getValueAs(Calendar.class), notNullValue());
    assertThat(literal.getValueAs(TimeString.class), notNullValue());
  }

  /** Tests {@link RexBuilder#makeDateLiteral(DateString)}. */
  @Test public void testDateLiteral() {
    final RelDataTypeFactory typeFactory =
        new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType dateType = typeFactory.createSqlType(SqlTypeName.DATE);
    final RexBuilder builder = new RexBuilder(typeFactory);

    // Old way: provide a Calendar
    final Calendar calendar = Util.calendar();
    calendar.set(1969, Calendar.JULY, 21); // one small step
    calendar.set(Calendar.MILLISECOND, 0);
    checkDate(builder.makeLiteral(calendar, dateType, false));

    // Old way #2: Provide in Integer
    checkDate(builder.makeLiteral(MOON_DAY, dateType, false));

    // The new way
    final DateString d = new DateString(1969, 7, 21);
    checkDate(builder.makeLiteral(d, dateType, false));
  }

  private void checkDate(RexNode node) {
    assertThat(node.toString(), is("1969-07-21"));
    RexLiteral literal = (RexLiteral) node;
    assertThat(literal.getValue() instanceof Calendar, is(true));
    assertThat(literal.getValue2() instanceof Integer, is(true));
    assertThat(literal.getValue3() instanceof Integer, is(true));
    assertThat((Integer) literal.getValue2(), is(MOON_DAY));
    assertThat(literal.getValueAs(Calendar.class), notNullValue());
    assertThat(literal.getValueAs(DateString.class), notNullValue());
  }

}

// End RexBuilderTest.java
