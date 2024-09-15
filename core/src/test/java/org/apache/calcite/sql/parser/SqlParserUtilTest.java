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
import org.apache.calcite.sql.SqlIntervalQualifier;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

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
