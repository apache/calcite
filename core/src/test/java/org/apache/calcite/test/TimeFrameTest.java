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

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.type.TimeFrame;
import org.apache.calcite.rel.type.TimeFrameSet;
import org.apache.calcite.rel.type.TimeFrames;
import org.apache.calcite.util.Pair;

import org.apache.commons.math3.fraction.BigFraction;

import com.google.common.collect.ImmutableMap;

import org.hamcrest.Matcher;
import org.junit.jupiter.api.Test;

import static org.apache.calcite.avatica.util.DateTimeUtils.dateStringToUnixDate;
import static org.apache.calcite.avatica.util.DateTimeUtils.timestampStringToUnixDate;
import static org.apache.calcite.avatica.util.DateTimeUtils.unixDateToString;
import static org.apache.calcite.avatica.util.DateTimeUtils.unixTimestampToString;
import static org.apache.calcite.avatica.util.TimeUnit.CENTURY;
import static org.apache.calcite.avatica.util.TimeUnit.DAY;
import static org.apache.calcite.avatica.util.TimeUnit.DECADE;
import static org.apache.calcite.avatica.util.TimeUnit.HOUR;
import static org.apache.calcite.avatica.util.TimeUnit.ISODOW;
import static org.apache.calcite.avatica.util.TimeUnit.ISOYEAR;
import static org.apache.calcite.avatica.util.TimeUnit.MICROSECOND;
import static org.apache.calcite.avatica.util.TimeUnit.MILLENNIUM;
import static org.apache.calcite.avatica.util.TimeUnit.MILLISECOND;
import static org.apache.calcite.avatica.util.TimeUnit.MINUTE;
import static org.apache.calcite.avatica.util.TimeUnit.MONTH;
import static org.apache.calcite.avatica.util.TimeUnit.NANOSECOND;
import static org.apache.calcite.avatica.util.TimeUnit.QUARTER;
import static org.apache.calcite.avatica.util.TimeUnit.SECOND;
import static org.apache.calcite.avatica.util.TimeUnit.WEEK;
import static org.apache.calcite.avatica.util.TimeUnit.YEAR;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

import static java.util.Objects.requireNonNull;

/** Unit test for {@link org.apache.calcite.rel.type.TimeFrame}. */
public class TimeFrameTest {
  /** Unit test for {@link org.apache.calcite.rel.type.TimeFrames#CORE}. */
  @Test void testAvaticaTimeFrame() {
    final TimeFrameSet timeFrameSet = TimeFrames.CORE;
    final TimeFrame year = timeFrameSet.get(TimeUnit.YEAR);
    assertThat(year, notNullValue());
    assertThat(year.name(), is("YEAR"));
    assertThat(timeFrameSet.getUnit(year), is(YEAR));

    final TimeFrame month = timeFrameSet.get(MONTH);
    assertThat(month, notNullValue());
    assertThat(month.name(), is("MONTH"));
    assertThat(timeFrameSet.getUnit(month), is(MONTH));

    final Number monthPerYear = month.per(year);
    assertThat(monthPerYear, notNullValue());
    assertThat(monthPerYear, is(new BigFraction(12)));
    final Number yearPerMonth = year.per(month);
    assertThat(yearPerMonth, notNullValue());
    assertThat(yearPerMonth, is(BigFraction.ONE.divide(12)));
    final Number monthPerMonth = month.per(month);
    assertThat(monthPerMonth, notNullValue());
    assertThat(monthPerMonth, is(BigFraction.ONE));

    final TimeFrame second = timeFrameSet.get(TimeUnit.SECOND);
    assertThat(second, notNullValue());
    assertThat(second.name(), is("SECOND"));

    final TimeFrame minute = timeFrameSet.get(TimeUnit.MINUTE);
    assertThat(minute, notNullValue());
    assertThat(minute.name(), is("MINUTE"));

    final TimeFrame nano = timeFrameSet.get(TimeUnit.NANOSECOND);
    assertThat(nano, notNullValue());
    assertThat(nano.name(), is("NANOSECOND"));

    final Number secondPerMonth = second.per(month);
    assertThat(secondPerMonth, nullValue());
    final Number nanoPerMinute = nano.per(minute);
    assertThat(nanoPerMinute, notNullValue());
    assertThat(nanoPerMinute,
        is(BigFraction.ONE.multiply(1_000).multiply(1_000).multiply(1_000)
            .multiply(60)));

    // ISOWEEK is the only core time frame without a corresponding time unit.
    // There is no TimeUnit.ISOWEEK.
    final TimeFrame isoWeek = timeFrameSet.get("ISOWEEK");
    assertThat(isoWeek, notNullValue());
    assertThat(isoWeek.name(), is("ISOWEEK"));
    assertThat(timeFrameSet.getUnit(isoWeek), nullValue());

    // FRAC_SECOND is an alias.
    final TimeFrame fracSecond = timeFrameSet.get("FRAC_SECOND");
    assertThat(fracSecond, notNullValue());
    assertThat(fracSecond.name(), is("MICROSECOND"));
    assertThat(timeFrameSet.getUnit(fracSecond), is(MICROSECOND));

    // SQL_TSI_QUARTER is an alias.
    final TimeFrame sqlTsiQuarter = timeFrameSet.get("SQL_TSI_QUARTER");
    assertThat(sqlTsiQuarter, notNullValue());
    assertThat(sqlTsiQuarter.name(), is("QUARTER"));
    assertThat(timeFrameSet.getUnit(sqlTsiQuarter), is(QUARTER));
  }

  @Test void testConflict() {
    TimeFrameSet.Builder b = TimeFrameSet.builder();
    b.addCore("SECOND");
    b.addMultiple("MINUTE", 60, "SECOND");
    b.addMultiple("HOUR", 60, "MINUTE");
    b.addMultiple("DAY", 24, "SECOND");
    b.addDivision("MILLISECOND", 1_000, "SECOND");

    // It's important that TimeFrame.Builder throws when you attempt to add
    // a frame with the same name. It prevents DAGs and cycles.
    try {
      b.addDivision("MILLISECOND", 10_000, "MINUTE");
      fail("expected error");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), is("duplicate frame: MILLISECOND"));
    }

    try {
      b.addCore("SECOND");
      fail("expected error");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), is("duplicate frame: SECOND"));
    }

    try {
      b.addQuotient("SECOND", "MINUTE", "HOUR");
      fail("expected error");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), is("duplicate frame: SECOND"));
    }

    try {
      b.addQuotient("MINUTE_OF_WEEK", "MINUTE", "WEEK");
      fail("expected error");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), is("unknown frame: WEEK"));
    }

    try {
      b.addQuotient("DAY_OF_WEEK", "DAY", "YEAR");
      fail("expected error");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), is("unknown frame: YEAR"));
    }

    try {
      b.addAlias("SECOND", "DAY");
      fail("expected error");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), is("duplicate frame: SECOND"));
    }

    try {
      b.addAlias("FOO", "BAZ");
      fail("expected error");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), is("unknown frame: BAZ"));
    }

    // Can't define NANOSECOND in terms of a frame that has not been defined
    // yet.
    try {
      b.addDivision("NANOSECOND", 1_000, "MICROSECOND");
      fail("expected error");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), is("unknown frame: MICROSECOND"));
    }

    // We can define NANOSECOND and MICROSECOND as long as we define each frame
    // in terms of previous frames.
    b.addDivision("NANOSECOND", 1_000_000, "MILLISECOND");
    b.addMultiple("MICROSECOND", 1_000, "NANOSECOND");

    // Can't define a frame in terms of itself.
    // (I guess you should use a core frame.)
    try {
      b.addDivision("PICOSECOND", 1, "PICOSECOND");
      fail("expected error");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), is("unknown frame: PICOSECOND"));
    }

    final TimeFrameSet timeFrameSet = b.build();

    final TimeFrame second = timeFrameSet.get("SECOND");
    final TimeFrame hour = timeFrameSet.get("HOUR");
    assertThat(hour.per(second), is(BigFraction.ONE.divide(3_600)));
    final TimeFrame millisecond = timeFrameSet.get("MILLISECOND");
    assertThat(hour.per(millisecond), is(BigFraction.ONE.divide(3_600_000)));
    final TimeFrame nanosecond = timeFrameSet.get("NANOSECOND");
    assertThat(nanosecond.per(second),
        is(BigFraction.ONE.multiply(1_000_000_000)));
  }

  @Test void testEvalFloor() {
    final Fixture f = new Fixture();
    f.checkDateFloor("1970-08-01", WEEK, is("1970-07-26")); // saturday
    f.checkDateFloor("1970-08-02", WEEK, is("1970-08-02")); // sunday
    f.checkDateFloor("1970-08-03", WEEK, is("1970-08-02")); // monday
    f.checkDateFloor("1970-08-04", WEEK, is("1970-08-02")); // tuesday

    f.checkDateFloor("1970-08-01", f.isoWeek, is("1970-07-27")); // saturday
    f.checkDateFloor("1970-08-02", f.isoWeek, is("1970-07-27")); // sunday
    f.checkDateFloor("1970-08-03", f.isoWeek, is("1970-08-03")); // monday
    f.checkDateFloor("1970-08-04", f.isoWeek, is("1970-08-03")); // tuesday

    f.checkDateFloor("1970-08-04", "WEEK_MONDAY", is("1970-08-03")); // tuesday
    f.checkDateFloor("1970-08-04", "WEEK_TUESDAY", is("1970-08-04")); // tuesday

    f.checkTimestampFloor("1970-01-01 01:23:45", HOUR,
        0, is("1970-01-01 01:00:00"));
    f.checkTimestampFloor("1970-01-01 01:23:45", MINUTE,
        0, is("1970-01-01 01:23:00"));
    f.checkTimestampFloor("1970-01-01 01:23:45.67", SECOND,
        0, is("1970-01-01 01:23:45"));
    f.checkTimestampFloor("1970-01-01 01:23:45.6789012345", MILLISECOND,
        4, is("1970-01-01 01:23:45.6790"));
    // Time frames can represent unlimited precision, but out representation of
    // timestamp can't represent more than millisecond precision.
    f.checkTimestampFloor("1970-01-01 01:23:45.6789012345", MICROSECOND,
        7, is("1970-01-01 01:23:45.6790000"));

    f.checkTimestampFloor("1971-12-25 01:23:45", DAY,
        0, is("1971-12-25 00:00:00"));
    f.checkTimestampFloor("1971-12-25 01:23:45", WEEK,
        0, is("1971-12-19 00:00:00"));
  }

  @Test void testCanRollUp() {
    final Fixture f = new Fixture();

    // The rollup from DAY to MONTH is special. It provides the bridge between
    // the frames in the SECOND family and those in the MONTH family.
    f.checkCanRollUp(DAY, MONTH, true);
    f.checkCanRollUp(MONTH, DAY, false);

    // Note 0: when we pass TimeUnit.ISODOW to tests, we mean f.ISOWEEK.

    f.checkCanRollUp(NANOSECOND, NANOSECOND, true);
    f.checkCanRollUp(NANOSECOND, MICROSECOND, true);
    f.checkCanRollUp(NANOSECOND, MILLISECOND, true);
    f.checkCanRollUp(NANOSECOND, SECOND, true);
    f.checkCanRollUp(NANOSECOND, MINUTE, true);
    f.checkCanRollUp(NANOSECOND, HOUR, true);
    f.checkCanRollUp(NANOSECOND, DAY, true);
    f.checkCanRollUp(NANOSECOND, WEEK, true);
    f.checkCanRollUp(NANOSECOND, f.isoWeek, true); // see note 0
    f.checkCanRollUp(NANOSECOND, MONTH, true);
    f.checkCanRollUp(NANOSECOND, QUARTER, true);
    f.checkCanRollUp(NANOSECOND, YEAR, true);
    f.checkCanRollUp(NANOSECOND, ISOYEAR, true);
    f.checkCanRollUp(NANOSECOND, CENTURY, true);
    f.checkCanRollUp(NANOSECOND, DECADE, true);
    f.checkCanRollUp(NANOSECOND, MILLENNIUM, true);

    f.checkCanRollUp(MICROSECOND, NANOSECOND, false);
    f.checkCanRollUp(MICROSECOND, MICROSECOND, true);
    f.checkCanRollUp(MICROSECOND, MILLISECOND, true);
    f.checkCanRollUp(MICROSECOND, SECOND, true);
    f.checkCanRollUp(MICROSECOND, MINUTE, true);
    f.checkCanRollUp(MICROSECOND, HOUR, true);
    f.checkCanRollUp(MICROSECOND, DAY, true);
    f.checkCanRollUp(MICROSECOND, WEEK, true);
    f.checkCanRollUp(MICROSECOND, f.isoWeek, true);
    f.checkCanRollUp(MICROSECOND, MONTH, true);
    f.checkCanRollUp(MICROSECOND, QUARTER, true);
    f.checkCanRollUp(MICROSECOND, YEAR, true);
    f.checkCanRollUp(MICROSECOND, ISOYEAR, true);
    f.checkCanRollUp(MICROSECOND, CENTURY, true);
    f.checkCanRollUp(MICROSECOND, DECADE, true);
    f.checkCanRollUp(MICROSECOND, MILLENNIUM, true);

    f.checkCanRollUp(MILLISECOND, NANOSECOND, false);
    f.checkCanRollUp(MILLISECOND, MICROSECOND, false);
    f.checkCanRollUp(MILLISECOND, MILLISECOND, true);
    f.checkCanRollUp(MILLISECOND, SECOND, true);
    f.checkCanRollUp(MILLISECOND, MINUTE, true);
    f.checkCanRollUp(MILLISECOND, HOUR, true);
    f.checkCanRollUp(MILLISECOND, DAY, true);
    f.checkCanRollUp(MILLISECOND, WEEK, true);
    f.checkCanRollUp(MILLISECOND, f.isoWeek, true);
    f.checkCanRollUp(MILLISECOND, MONTH, true);
    f.checkCanRollUp(MILLISECOND, QUARTER, true);
    f.checkCanRollUp(MILLISECOND, YEAR, true);
    f.checkCanRollUp(MILLISECOND, ISOYEAR, true);
    f.checkCanRollUp(MILLISECOND, CENTURY, true);
    f.checkCanRollUp(MILLISECOND, DECADE, true);
    f.checkCanRollUp(MILLISECOND, MILLENNIUM, true);

    f.checkCanRollUp(SECOND, NANOSECOND, false);
    f.checkCanRollUp(SECOND, MICROSECOND, false);
    f.checkCanRollUp(SECOND, MILLISECOND, false);
    f.checkCanRollUp(SECOND, SECOND, true);
    f.checkCanRollUp(SECOND, MINUTE, true);
    f.checkCanRollUp(SECOND, HOUR, true);
    f.checkCanRollUp(SECOND, DAY, true);
    f.checkCanRollUp(SECOND, WEEK, true);
    f.checkCanRollUp(SECOND, f.isoWeek, true);
    f.checkCanRollUp(SECOND, MONTH, true);
    f.checkCanRollUp(SECOND, QUARTER, true);
    f.checkCanRollUp(SECOND, YEAR, true);
    f.checkCanRollUp(SECOND, ISOYEAR, true);
    f.checkCanRollUp(SECOND, CENTURY, true);
    f.checkCanRollUp(SECOND, DECADE, true);
    f.checkCanRollUp(SECOND, MILLENNIUM, true);

    f.checkCanRollUp(MINUTE, NANOSECOND, false);
    f.checkCanRollUp(MINUTE, MICROSECOND, false);
    f.checkCanRollUp(MINUTE, MILLISECOND, false);
    f.checkCanRollUp(MINUTE, SECOND, false);
    f.checkCanRollUp(MINUTE, MINUTE, true);
    f.checkCanRollUp(MINUTE, HOUR, true);
    f.checkCanRollUp(MINUTE, DAY, true);
    f.checkCanRollUp(MINUTE, WEEK, true);
    f.checkCanRollUp(MINUTE, f.isoWeek, true);
    f.checkCanRollUp(MINUTE, MONTH, true);
    f.checkCanRollUp(MINUTE, QUARTER, true);
    f.checkCanRollUp(MINUTE, YEAR, true);
    f.checkCanRollUp(MINUTE, ISOYEAR, true);
    f.checkCanRollUp(MINUTE, CENTURY, true);
    f.checkCanRollUp(MINUTE, DECADE, true);
    f.checkCanRollUp(MINUTE, MILLENNIUM, true);

    f.checkCanRollUp(HOUR, NANOSECOND, false);
    f.checkCanRollUp(HOUR, MICROSECOND, false);
    f.checkCanRollUp(HOUR, MILLISECOND, false);
    f.checkCanRollUp(HOUR, SECOND, false);
    f.checkCanRollUp(HOUR, MINUTE, false);
    f.checkCanRollUp(HOUR, HOUR, true);
    f.checkCanRollUp(HOUR, DAY, true);
    f.checkCanRollUp(HOUR, WEEK, true);
    f.checkCanRollUp(HOUR, f.isoWeek, true);
    f.checkCanRollUp(HOUR, MONTH, true);
    f.checkCanRollUp(HOUR, QUARTER, true);
    f.checkCanRollUp(HOUR, YEAR, true);
    f.checkCanRollUp(HOUR, ISOYEAR, true);
    f.checkCanRollUp(HOUR, DECADE, true);
    f.checkCanRollUp(HOUR, CENTURY, true);
    f.checkCanRollUp(HOUR, MILLENNIUM, true);

    f.checkCanRollUp(DAY, NANOSECOND, false);
    f.checkCanRollUp(DAY, MICROSECOND, false);
    f.checkCanRollUp(DAY, MILLISECOND, false);
    f.checkCanRollUp(DAY, SECOND, false);
    f.checkCanRollUp(DAY, MINUTE, false);
    f.checkCanRollUp(DAY, HOUR, false);
    f.checkCanRollUp(DAY, DAY, true);
    f.checkCanRollUp(DAY, WEEK, true);
    f.checkCanRollUp(DAY, f.isoWeek, true);
    f.checkCanRollUp(DAY, MONTH, true);
    f.checkCanRollUp(DAY, QUARTER, true);
    f.checkCanRollUp(DAY, YEAR, true);
    f.checkCanRollUp(DAY, ISOYEAR, true);
    f.checkCanRollUp(DAY, DECADE, true);
    f.checkCanRollUp(DAY, CENTURY, true);
    f.checkCanRollUp(DAY, MILLENNIUM, true);

    // Note 1. WEEK cannot roll up to MONTH, YEAR or higher.
    // Some weeks cross month, year, decade, century and millennium boundaries.

    // Note 2. WEEK, MONTH, QUARTER, YEAR, DECADE, CENTURY, MILLENNIUM cannot
    // roll up to ISOYEAR. Only f.ISOWEEK can roll up to ISOYEAR.

    f.checkCanRollUp(WEEK, NANOSECOND, false);
    f.checkCanRollUp(WEEK, MICROSECOND, false);
    f.checkCanRollUp(WEEK, MILLISECOND, false);
    f.checkCanRollUp(WEEK, SECOND, false);
    f.checkCanRollUp(WEEK, MINUTE, false);
    f.checkCanRollUp(WEEK, HOUR, false);
    f.checkCanRollUp(WEEK, DAY, false);
    f.checkCanRollUp(WEEK, WEEK, true);
    f.checkCanRollUp(WEEK, f.isoWeek, false);
    f.checkCanRollUp(WEEK, MONTH, false); // see note 1
    f.checkCanRollUp(WEEK, QUARTER, false); // see note 1
    f.checkCanRollUp(WEEK, YEAR, false); // see note 1
    f.checkCanRollUp(WEEK, ISOYEAR, false); // see note 2
    f.checkCanRollUp(WEEK, DECADE, false); // see note 1
    f.checkCanRollUp(WEEK, CENTURY, false); // see note 1
    f.checkCanRollUp(WEEK, MILLENNIUM, false); // see note 1

    f.checkCanRollUp(f.isoWeek, NANOSECOND, false);
    f.checkCanRollUp(f.isoWeek, MICROSECOND, false);
    f.checkCanRollUp(f.isoWeek, MILLISECOND, false);
    f.checkCanRollUp(f.isoWeek, SECOND, false);
    f.checkCanRollUp(f.isoWeek, MINUTE, false);
    f.checkCanRollUp(f.isoWeek, HOUR, false);
    f.checkCanRollUp(f.isoWeek, DAY, false);
    f.checkCanRollUp(f.isoWeek, WEEK, false);
    f.checkCanRollUp(f.isoWeek, f.isoWeek, true);
    f.checkCanRollUp(f.isoWeek, MONTH, false); // see note 1
    f.checkCanRollUp(f.isoWeek, QUARTER, false); // see note 1
    f.checkCanRollUp(f.isoWeek, YEAR, false); // see note 1
    f.checkCanRollUp(f.isoWeek, ISOYEAR, true); // see note 2
    f.checkCanRollUp(f.isoWeek, DECADE, false); // see note 1
    f.checkCanRollUp(f.isoWeek, CENTURY, false); // see note 1
    f.checkCanRollUp(f.isoWeek, MILLENNIUM, false); // see note 1

    f.checkCanRollUp(MONTH, NANOSECOND, false);
    f.checkCanRollUp(MONTH, MICROSECOND, false);
    f.checkCanRollUp(MONTH, MILLISECOND, false);
    f.checkCanRollUp(MONTH, SECOND, false);
    f.checkCanRollUp(MONTH, MINUTE, false);
    f.checkCanRollUp(MONTH, HOUR, false);
    f.checkCanRollUp(MONTH, DAY, false);
    f.checkCanRollUp(MONTH, WEEK, false);
    f.checkCanRollUp(MONTH, f.isoWeek, false);
    f.checkCanRollUp(MONTH, MONTH, true);
    f.checkCanRollUp(MONTH, QUARTER, true);
    f.checkCanRollUp(MONTH, YEAR, true);
    f.checkCanRollUp(MONTH, ISOYEAR, false); // see note 2
    f.checkCanRollUp(MONTH, DECADE, true);
    f.checkCanRollUp(MONTH, CENTURY, true);
    f.checkCanRollUp(MONTH, MILLENNIUM, true);

    f.checkCanRollUp(QUARTER, NANOSECOND, false);
    f.checkCanRollUp(QUARTER, MICROSECOND, false);
    f.checkCanRollUp(QUARTER, MILLISECOND, false);
    f.checkCanRollUp(QUARTER, SECOND, false);
    f.checkCanRollUp(QUARTER, MINUTE, false);
    f.checkCanRollUp(QUARTER, HOUR, false);
    f.checkCanRollUp(QUARTER, DAY, false);
    f.checkCanRollUp(QUARTER, WEEK, false);
    f.checkCanRollUp(QUARTER, f.isoWeek, false);
    f.checkCanRollUp(QUARTER, MONTH, false);
    f.checkCanRollUp(QUARTER, QUARTER, true);
    f.checkCanRollUp(QUARTER, YEAR, true);
    f.checkCanRollUp(QUARTER, ISOYEAR, false); // see note 2
    f.checkCanRollUp(QUARTER, DECADE, true);
    f.checkCanRollUp(QUARTER, CENTURY, true);
    f.checkCanRollUp(QUARTER, MILLENNIUM, true);

    f.checkCanRollUp(YEAR, NANOSECOND, false);
    f.checkCanRollUp(YEAR, MICROSECOND, false);
    f.checkCanRollUp(YEAR, MILLISECOND, false);
    f.checkCanRollUp(YEAR, SECOND, false);
    f.checkCanRollUp(YEAR, MINUTE, false);
    f.checkCanRollUp(YEAR, HOUR, false);
    f.checkCanRollUp(YEAR, DAY, false);
    f.checkCanRollUp(YEAR, WEEK, false);
    f.checkCanRollUp(YEAR, f.isoWeek, false);
    f.checkCanRollUp(YEAR, MONTH, false);
    f.checkCanRollUp(YEAR, QUARTER, false);
    f.checkCanRollUp(YEAR, YEAR, true);
    f.checkCanRollUp(YEAR, ISOYEAR, false); // see note 2
    f.checkCanRollUp(YEAR, DECADE, true);
    f.checkCanRollUp(YEAR, CENTURY, true);
    f.checkCanRollUp(YEAR, MILLENNIUM, true);

    // Note 3. DECADE cannot roll up to CENTURY or MILLENNIUM
    // because decade starts on year 0, the others start on year 1.
    // For example, 2000 is start of a decade, but 2001 is start of a century
    // and millennium.
    f.checkCanRollUp(DECADE, NANOSECOND, false);
    f.checkCanRollUp(DECADE, MICROSECOND, false);
    f.checkCanRollUp(DECADE, MILLISECOND, false);
    f.checkCanRollUp(DECADE, SECOND, false);
    f.checkCanRollUp(DECADE, MINUTE, false);
    f.checkCanRollUp(DECADE, HOUR, false);
    f.checkCanRollUp(DECADE, DAY, false);
    f.checkCanRollUp(DECADE, WEEK, false);
    f.checkCanRollUp(DECADE, f.isoWeek, false);
    f.checkCanRollUp(DECADE, MONTH, false);
    f.checkCanRollUp(DECADE, QUARTER, false);
    f.checkCanRollUp(DECADE, YEAR, false);
    f.checkCanRollUp(DECADE, ISOYEAR, false); // see note 2
    f.checkCanRollUp(DECADE, DECADE, true);
    f.checkCanRollUp(DECADE, CENTURY, false); // see note 3
    f.checkCanRollUp(DECADE, MILLENNIUM, false); // see note 3

    f.checkCanRollUp(CENTURY, NANOSECOND, false);
    f.checkCanRollUp(CENTURY, MICROSECOND, false);
    f.checkCanRollUp(CENTURY, MILLISECOND, false);
    f.checkCanRollUp(CENTURY, SECOND, false);
    f.checkCanRollUp(CENTURY, MINUTE, false);
    f.checkCanRollUp(CENTURY, HOUR, false);
    f.checkCanRollUp(CENTURY, DAY, false);
    f.checkCanRollUp(CENTURY, WEEK, false);
    f.checkCanRollUp(CENTURY, f.isoWeek, false);
    f.checkCanRollUp(CENTURY, MONTH, false);
    f.checkCanRollUp(CENTURY, QUARTER, false);
    f.checkCanRollUp(CENTURY, YEAR, false);
    f.checkCanRollUp(CENTURY, ISOYEAR, false); // see note 2
    f.checkCanRollUp(CENTURY, DECADE, false);
    f.checkCanRollUp(CENTURY, CENTURY, true);
    f.checkCanRollUp(CENTURY, MILLENNIUM, true);

    f.checkCanRollUp(MILLENNIUM, NANOSECOND, false);
    f.checkCanRollUp(MILLENNIUM, MICROSECOND, false);
    f.checkCanRollUp(MILLENNIUM, MILLISECOND, false);
    f.checkCanRollUp(MILLENNIUM, SECOND, false);
    f.checkCanRollUp(MILLENNIUM, MINUTE, false);
    f.checkCanRollUp(MILLENNIUM, HOUR, false);
    f.checkCanRollUp(MILLENNIUM, DAY, false);
    f.checkCanRollUp(MILLENNIUM, WEEK, false);
    f.checkCanRollUp(MILLENNIUM, f.isoWeek, false);
    f.checkCanRollUp(MILLENNIUM, MONTH, false);
    f.checkCanRollUp(MILLENNIUM, QUARTER, false);
    f.checkCanRollUp(MILLENNIUM, YEAR, false);
    f.checkCanRollUp(MILLENNIUM, ISOYEAR, false); // see note 2
    f.checkCanRollUp(MILLENNIUM, DECADE, false);
    f.checkCanRollUp(MILLENNIUM, CENTURY, false);
    f.checkCanRollUp(MILLENNIUM, MILLENNIUM, true);
  }

  /** Test fixture. Contains everything you need to write fluent tests. */
  static class Fixture {
    final TimeUnit isoWeek = TimeUnit.ISODOW;

    final TimeFrameSet timeFrameSet = TimeFrames.CORE;

    private static final ImmutableMap<String, Pair<String, String>> MAP;

    static {
      String[] values = {
          "NANOSECOND", "2022-06-25 12:34:56.123234456",
          "2022-06-25 12:34:56.123234456",
          "MICROSECOND", "2022-06-25 12:34:56.123234",
          "2022-06-25 12:34:56.123234",
          "MILLISECOND", "2022-06-25 12:34:56.123", "2022-06-25 12:34:56.124",
          "SECOND", "2022-06-25 12:34:56", "2022-06-25 12:34:57",
          "MINUTE", "2022-06-25 12:34:00", "2022-06-25 12:35:00",
          "HOUR", "2022-06-25 12:00:00", "2022-06-25 13:00:00",
          "DAY", "2022-06-25 00:00:00", "2022-06-26 00:00:00",
          "WEEK", "2022-06-19 00:00:00", "2022-06-26 00:00:00",
          "ISOWEEK", "2022-06-20 00:00:00", "2022-06-27 00:00:00",
          "MONTH", "2022-06-01 00:00:00", "2022-07-01 00:00:00",
          "QUARTER", "2022-04-01 00:00:00", "2022-07-01 00:00:00",
          "YEAR", "2022-01-01 00:00:00", "2023-01-01 00:00:00",
          "ISOYEAR", "2022-01-03 00:00:00", "2023-01-02 00:00:00",
          "DECADE", "2020-01-01 00:00:00", "2030-01-01 00:00:00",
          "CENTURY", "2001-01-01 00:00:00", "2101-01-01 00:00:00",
          "MILLENNIUM", "2001-01-01 00:00:00", "3001-01-01 00:00:00",
      };
      ImmutableMap.Builder<String, Pair<String, String>> b =
          ImmutableMap.builder();
      for (int i = 0; i < values.length;) {
        b.put(values[i++], Pair.of(values[i++], values[i++]));
      }
      MAP = b.build();
    }

    private TimeFrame frame(TimeUnit unit) {
      if (unit == ISODOW) {
        // Just for testing. We want to test f.ISOWEEK but there is no TimeUnit
        // for it, so we use ISODOW as a stand-in.
        return timeFrameSet.get("ISOWEEK");
      }
      return timeFrameSet.get(unit);
    }

    void checkDateFloor(String in, TimeUnit unit, Matcher<String> matcher) {
      int inDate = dateStringToUnixDate(in);
      int outDate = timeFrameSet.floorDate(inDate, frame(unit));
      assertThat("floor(" + in + " to " + unit + ")",
          unixDateToString(outDate), matcher);
    }

    void checkDateFloor(String in, String timeFrameName, Matcher<String> matcher) {
      int inDate = dateStringToUnixDate(in);
      int outDate = timeFrameSet.floorDate(inDate, timeFrameSet.get(timeFrameName));
      assertThat("floor(" + in + " to " + timeFrameName + ")",
          unixDateToString(outDate), matcher);
    }

    void checkTimestampFloor(String in, TimeUnit unit, int precision,
        Matcher<String> matcher) {
      long inTs = timestampStringToUnixDate(in);
      long outTs = timeFrameSet.floorTimestamp(inTs, frame(unit));
      assertThat("floor(" + in + " to " + unit + ")",
          unixTimestampToString(outTs, precision), matcher);
    }

    void checkTimestampCeil(String in, TimeUnit unit, int precision,
        Matcher<String> matcher) {
      long inTs = timestampStringToUnixDate(in);
      long outTs = timeFrameSet.ceilTimestamp(inTs, frame(unit));
      assertThat("ceil(" + in + " to " + unit + ")",
          unixTimestampToString(outTs, precision), matcher);
    }

    void checkCanRollUp(TimeUnit fromUnit, TimeUnit toUnit, boolean can) {
      TimeFrame fromFrame = frame(fromUnit);
      TimeFrame toFrame = frame(toUnit);
      if (can) {
        assertThat("can roll up " + fromUnit + " to " + toUnit,
            fromFrame.canRollUpTo(toFrame),
            is(true));

        final int precision;
        switch (toUnit) {
        case NANOSECOND:
          precision = 9;
          break;
        case MICROSECOND:
          precision = 6;
          break;
        case MILLISECOND:
          precision = 3;
          break;
        default:
          precision = 0;
        }
        if (precision <= 3) {
          // Cannot test conversion to NANOSECOND or MICROSECOND because the
          // representation is milliseconds.
          final Pair<String, String> fromPair =
              requireNonNull(MAP.get(fromFrame.name()));
          final String timestampString = fromPair.left;
          final Pair<String, String> toPair =
              requireNonNull(MAP.get(toFrame.name()));
          final String floorTimestampString = toPair.left;
          final String ceilTimestampString = toPair.right;
          checkTimestampFloor(timestampString, toUnit, precision,
              is(floorTimestampString));
          checkTimestampCeil(timestampString, toUnit, precision,
              is(ceilTimestampString));

          final String dateString = timestampString.substring(0, 10);
          final String floorDateString = floorTimestampString.substring(0, 10);
          checkDateFloor(dateString, toUnit, is(floorDateString));
        }

        // The 'canRollUpTo' method should be a partial order.
        // A partial order is reflexive (for all x, x = x)
        // and antisymmetric (for all x, y, if x <= y and x != y, then !(y <= x))
        assertThat(toFrame.canRollUpTo(fromFrame), is(fromUnit == toUnit));
      } else {
        assertThat("can roll up " + fromUnit + " to " + toUnit,
            fromFrame.canRollUpTo(toFrame),
            is(false));
      }
    }
  }
}
