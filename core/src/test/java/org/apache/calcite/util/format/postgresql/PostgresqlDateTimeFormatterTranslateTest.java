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
package org.apache.calcite.util.format.postgresql;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import java.time.ZonedDateTime;
import java.util.Locale;

import static org.apache.calcite.util.format.postgresql.PostgresqlDateTimeFormatterTest.createDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit test for {@link PostgresqlDateTimeFormatter} focused on testing locale translations.
 * This class is isolated since it will alter the default locale during the tests.
 */
@Isolated
public class PostgresqlDateTimeFormatterTranslateTest {

  @Test void testMonthFullUpperCaseNoTranslate() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = createDateTime(2024, 3, 1, 23, 0, 0, 0);
    final ZonedDateTime date3 = createDateTime(2024, 11, 1, 23, 0, 0, 0);

    final Locale originalLocale = Locale.getDefault();
    try {
      Locale.setDefault(Locale.FRENCH);
      assertEquals("JANUARY  ", PostgresqlDateTimeFormatter.toChar("MONTH", date1));
      assertEquals("MARCH    ", PostgresqlDateTimeFormatter.toChar("MONTH", date2));
      assertEquals("NOVEMBER ", PostgresqlDateTimeFormatter.toChar("MONTH", date3));
    } finally {
      Locale.setDefault(originalLocale);
    }
  }

  @Test void testMonthFullUpperCaseTranslate() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = createDateTime(2024, 3, 1, 23, 0, 0, 0);
    final ZonedDateTime date3 = createDateTime(2024, 11, 1, 23, 0, 0, 0);

    final Locale originalLocale = Locale.getDefault();
    try {
      Locale.setDefault(Locale.FRENCH);
      assertEquals("JANVIER  ", PostgresqlDateTimeFormatter.toChar("TMMONTH", date1));
      assertEquals("MARS     ", PostgresqlDateTimeFormatter.toChar("TMMONTH", date2));
      assertEquals("NOVEMBRE ", PostgresqlDateTimeFormatter.toChar("TMMONTH", date3));
    } finally {
      Locale.setDefault(originalLocale);
    }
  }

  @Test void testDayFullUpperNoTranslate() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = createDateTime(2024, 3, 1, 23, 0, 0, 0);
    final ZonedDateTime date3 = createDateTime(2024, 10, 1, 23, 0, 0, 0);

    final Locale originalLocale = Locale.getDefault();
    try {
      Locale.setDefault(Locale.FRENCH);
      assertEquals("MONDAY   ", PostgresqlDateTimeFormatter.toChar("DAY", date1));
      assertEquals("FRIDAY   ", PostgresqlDateTimeFormatter.toChar("DAY", date2));
      assertEquals("TUESDAY  ", PostgresqlDateTimeFormatter.toChar("DAY", date3));
    } finally {
      Locale.setDefault(originalLocale);
    }
  }

  @Test void testDayFullUpperTranslate() {
    final ZonedDateTime date1 = createDateTime(2024, 1, 1, 23, 0, 0, 0);
    final ZonedDateTime date2 = createDateTime(2024, 3, 1, 23, 0, 0, 0);
    final ZonedDateTime date3 = createDateTime(2024, 10, 1, 23, 0, 0, 0);

    final Locale originalLocale = Locale.getDefault();
    try {
      Locale.setDefault(Locale.FRENCH);
      assertEquals("LUNDI    ", PostgresqlDateTimeFormatter.toChar("TMDAY", date1));
      assertEquals("VENDREDI ", PostgresqlDateTimeFormatter.toChar("TMDAY", date2));
      assertEquals("MARDI    ", PostgresqlDateTimeFormatter.toChar("TMDAY", date3));
    } finally {
      Locale.setDefault(originalLocale);
    }
  }
}
