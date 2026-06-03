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
package org.apache.calcite.sql.parser;

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlTimeTzLiteral;
import org.apache.calcite.sql.SqlTimestampTzLiteral;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasToString;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests {@link SqlParserUtil}. Currently, this test focuses on tests for the methods that work
 * with {@link SqlIntervalQualifier}. It may be expanded to test other functionality in the future.
 */
public class SqlParserUtilTest {
  private static final SqlParserPos POSITION = SqlParserPos.ZERO;

  @Test void testSecondIntervalToMillis() {
    final SqlIntervalQualifier qualifier =
        new SqlIntervalQualifier(TimeUnit.SECOND, null, POSITION);
    assertThat(SqlParserUtil.intervalToMillis("2.1", qualifier), equalTo(2_100L));
  }

  @Test void testMinuteIntervalToMillis() {
    final SqlIntervalQualifier qualifier =
        new SqlIntervalQualifier(TimeUnit.MINUTE, null, POSITION);
    assertThat(SqlParserUtil.intervalToMillis("2", qualifier), equalTo(120_000L));
  }

  @Test void testTimestampWithTimeZone() {
    SqlParserPos pos = new SqlParserPos(2, 3);
    SqlTimestampTzLiteral lit =
        SqlParserUtil.parseTimestampTzLiteral("2020-01-01 10:10:10 GMT", pos);
    assertThat(lit, hasToString("TIMESTAMP_TZ '2020-01-01 10:10:10 GMT'"));

    lit = SqlParserUtil.parseTimestampTzLiteral("2020-01-01 10:10:10 UTC", pos);
    assertThat(lit, hasToString("TIMESTAMP_TZ '2020-01-01 10:10:10 UTC'"));

    lit = SqlParserUtil.parseTimestampTzLiteral("2020-01-01 10:10:10 America/Los_Angeles", pos);
    assertThat(lit, hasToString("TIMESTAMP_TZ '2020-01-01 10:10:10 America/Los_Angeles'"));

    lit = SqlParserUtil.parseTimestampTzLiteral("2020-01-01 10:10:10 +00:00", pos);
    assertThat(lit, hasToString("TIMESTAMP_TZ '2020-01-01 10:10:10 UTC'"));

    lit = SqlParserUtil.parseTimestampTzLiteral("2020-01-01 10:10:10 Z", pos);
    assertThat(lit, hasToString("TIMESTAMP_TZ '2020-01-01 10:10:10 UTC'"));

    lit = SqlParserUtil.parseTimestampTzLiteral("2020-01-01 10:10:10 -00:30", pos);
    assertThat(lit, hasToString("TIMESTAMP_TZ '2020-01-01 10:10:10 GMT-00:30'"));

    lit = SqlParserUtil.parseTimestampTzLiteral("2020-01-01 10:10:10 +05:45", pos);
    assertThat(lit, hasToString("TIMESTAMP_TZ '2020-01-01 10:10:10 GMT+05:45'"));

    // Test case for [CALCITE-7527] SqlParserUtil.parseTimestampTzLiteral does not validate timezone
    // https://issues.apache.org/jira/browse/CALCITE-7527
    try {
      SqlParserUtil.parseTimestampTzLiteral("2020-06-21 14:23:44.123654+00:00", pos);
      fail("Should be unreachable");
    } catch (CalciteContextException ex) {
      assertThat(
          ex.getMessage(), is("At line 2, column 3: Illegal TIMESTAMP WITH TIME ZONE literal "
              + "'2020-06-21 14:23:44.123654+00:00': not in format 'yyyy-MM-dd HH:mm:ss zone'"));
    }

    try {
      SqlParserUtil.parseTimestampTzLiteral("2020-06-21 14:23:44.123654 incorrect_zone", pos);
      fail("Should be unreachable");
    } catch (CalciteContextException ex) {
      assertThat(
          ex.getMessage(), is("At line 2, column 3: Illegal TIMESTAMP WITH TIME ZONE literal "
              + "'2020-06-21 14:23:44.123654 incorrect_zone': Unknown "
              + "time-zone ID: incorrect_zone"));
    }
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7559">[CALCITE-7559]
   * SqlParserUtil.parseTimeTzLiteral should reject unknown time zones</a>. */
  @Test void testTimeWithTimeZone() {
    SqlParserPos pos = new SqlParserPos(2, 3);
    SqlTimeTzLiteral lit =
        SqlParserUtil.parseTimeTzLiteral("10:10:10 GMT", pos);
    assertThat(lit, hasToString("TIME WITH TIME ZONE '10:10:10 UTC'"));

    // Like parseTimestampTzLiteral, parseTimeTzLiteral should reject unknown
    // time zones instead of silently falling back to GMT.
    try {
      SqlParserUtil.parseTimeTzLiteral("10:10:10 incorrect_zone", pos);
      fail("Should be unreachable");
    } catch (CalciteContextException ex) {
      assertThat(
          ex.getMessage(), is("At line 2, column 3: Illegal TIME WITH TIME ZONE literal "
              + "'10:10:10 incorrect_zone': Unknown time-zone ID: incorrect_zone"));
    }

    try {
      SqlParserUtil.parseTimeTzLiteral("10:10:10", pos);
      fail("Should be unreachable");
    } catch (CalciteContextException ex) {
      assertThat(
          ex.getMessage(), is("At line 2, column 3: Illegal TIME WITH TIME ZONE literal "
              + "'10:10:10': not in format 'HH:mm:ss zone'"));
    }
  }

  @Test void testMinuteToSecondIntervalToMillis() {
    final SqlIntervalQualifier qualifier =
        new SqlIntervalQualifier(TimeUnit.MINUTE, TimeUnit.SECOND, POSITION);
    assertThat(SqlParserUtil.intervalToMillis("2:30", qualifier), equalTo(150_000L));
  }

  @Test void testHourIntervalToMillis() {
    final SqlIntervalQualifier qualifier =
        new SqlIntervalQualifier(TimeUnit.HOUR, null, POSITION);
    assertThat(SqlParserUtil.intervalToMillis("2", qualifier), equalTo(7_200_000L));
  }

  @Test void testHourToMinuteIntervalToMillis() {
    final SqlIntervalQualifier qualifier =
        new SqlIntervalQualifier(TimeUnit.HOUR, TimeUnit.MINUTE, POSITION);
    assertThat(SqlParserUtil.intervalToMillis("2:03", qualifier), equalTo(7_380_000L));
  }

  @Test void testHourToSecondIntervalToMillis() {
    final SqlIntervalQualifier qualifier =
        new SqlIntervalQualifier(TimeUnit.HOUR, TimeUnit.SECOND, POSITION);
    assertThat(SqlParserUtil.intervalToMillis("2:03:30", qualifier), equalTo(7_410_000L));
  }

  @Test void testDayIntervalToMillis() {
    final SqlIntervalQualifier qualifier =
        new SqlIntervalQualifier(TimeUnit.DAY, null, POSITION);
    assertThat(SqlParserUtil.intervalToMillis("2", qualifier), equalTo(172_800_000L));
  }

  @Test void testDayToHourIntervalToMillis() {
    final SqlIntervalQualifier qualifier =
        new SqlIntervalQualifier(TimeUnit.DAY, TimeUnit.HOUR, POSITION);
    assertThat(SqlParserUtil.intervalToMillis("2 1", qualifier), equalTo(176_400_000L));
  }

  @Test void testDayToMinuteIntervalToMillis() {
    final SqlIntervalQualifier qualifier =
        new SqlIntervalQualifier(TimeUnit.DAY, TimeUnit.MINUTE, POSITION);
    assertThat(SqlParserUtil.intervalToMillis("2 1:03", qualifier), equalTo(176_580_000L));
  }

  @Test void testDayToSecondIntervalToMillis() {
    final SqlIntervalQualifier qualifier =
        new SqlIntervalQualifier(TimeUnit.DAY, TimeUnit.SECOND, POSITION);
    assertThat(SqlParserUtil.intervalToMillis("2 1:02:30", qualifier), equalTo(176_550_000L));
  }

  @Test void testWeekIntervalToMillis() {
    final SqlIntervalQualifier qualifier =
        new SqlIntervalQualifier(TimeUnit.WEEK, null, POSITION);
    assertThat(SqlParserUtil.intervalToMillis("2", qualifier), equalTo(1_209_600_000L));
  }

  @Test void testMonthIntervalToMonths() {
    final SqlIntervalQualifier qualifier =
        new SqlIntervalQualifier(TimeUnit.WEEK, null, POSITION);
    assertThat(SqlParserUtil.intervalToMillis("2", qualifier), equalTo(1_209_600_000L));
  }

  @Test void testQuarterIntervalToMonths() {
    final SqlIntervalQualifier qualifier =
        new SqlIntervalQualifier(TimeUnit.QUARTER, null, POSITION);
    assertThat(SqlParserUtil.intervalToMonths("2", qualifier), equalTo(6L));
  }

  @Test void testYearIntervalToMonths() {
    final SqlIntervalQualifier qualifier =
        new SqlIntervalQualifier(TimeUnit.YEAR, null, POSITION);
    assertThat(SqlParserUtil.intervalToMonths("2", qualifier), equalTo(24L));
  }

  @Test void testYearToMonthIntervalToMonths() {
    final SqlIntervalQualifier qualifier =
        new SqlIntervalQualifier(TimeUnit.YEAR, TimeUnit.MONTH, POSITION);
    assertThat(SqlParserUtil.intervalToMonths("2-3", qualifier), equalTo(27L));
  }
}
