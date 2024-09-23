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

import org.apache.calcite.util.format.postgresql.format.compiled.CompiledItem;
import org.apache.calcite.util.format.postgresql.format.compiled.CompiledPattern;
import org.apache.calcite.util.format.postgresql.format.compiled.LiteralCompiledItem;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.text.ParseException;
import java.text.ParsePosition;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.time.temporal.IsoFields;
import java.time.temporal.JulianFields;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Contains a parsed date/time format. Able to parse a string into a date/time value,
 * or convert a date/time value to a string.
 */
public class CompiledDateTimeFormat {
  private final ImmutableList<CompiledItem> compiledItems;

  public CompiledDateTimeFormat(ImmutableList<CompiledItem> compiledItems) {
    this.compiledItems = compiledItems;
  }

  /**
   * Parses a date/time value from a string. The format used is in compiledItems.
   *
   * @param input the String to parse
   * @param zoneId timezone to convert the result to
   * @param locale Locale to use for parsing day or month names if the TM modifier was present
   * @return the parsed date/time value
   * @throws ParseException if the string to parse did not meet the required format
   */
  public ZonedDateTime parseDateTime(String input, ZoneId zoneId, Locale locale)
      throws ParseException {
    final ParsePosition parsePosition = new ParsePosition(0);
    final Map<ChronoUnitEnum, Integer> dateTimeParts = new HashMap<>();

    for (int i = 0; i < compiledItems.size(); i++) {
      final CompiledItem currentFormatItem = compiledItems.get(i);
      final boolean nextItemNumeric = isItemNumeric(i + 1);

      if (currentFormatItem instanceof LiteralCompiledItem) {
        final LiteralCompiledItem literal = (LiteralCompiledItem) currentFormatItem;
        parsePosition.setIndex(parsePosition.getIndex() + literal.getFormatPatternLength());
      } else if (currentFormatItem instanceof CompiledPattern) {
        final CompiledPattern pattern = (CompiledPattern) currentFormatItem;
        dateTimeParts.put(
            pattern.getChronoUnit(), pattern.parseValue(
            parsePosition, input, nextItemNumeric, locale));
      }
    }

    return constructDateTimeFromParts(dateTimeParts, zoneId);
  }

  private boolean isItemNumeric(int index) {
    if (index < compiledItems.size() && compiledItems.get(index) instanceof CompiledPattern) {
      return ((CompiledPattern) compiledItems.get(index)).isNumeric();
    }

    return false;
  }

  /**
   * Converts a date/time value to a string. The output format is in compiledItems.
   *
   * @param dateTime date/time value to convert
   * @param locale Locale to use for outputting day and month names if the TM modifier was present
   * @return the date/time value formatted as a String
   */
  public String formatDateTime(ZonedDateTime dateTime, Locale locale) {
    final StringBuilder outputBuilder = new StringBuilder();

    for (CompiledItem compiledItem : compiledItems) {
      outputBuilder.append(compiledItem.convertToString(dateTime, locale));
    }

    return outputBuilder.toString();
  }

  private static ZonedDateTime constructDateTimeFromParts(Map<ChronoUnitEnum, Integer> dateParts,
      ZoneId zoneId) {
    LocalDateTime constructedDateTime = LocalDateTime.now(zoneId)
        .truncatedTo(ChronoUnit.DAYS);

    DateCalendarEnum calendar = DateCalendarEnum.NONE;
    boolean containsCentury = false;
    for (ChronoUnitEnum unit : dateParts.keySet()) {
      if (unit.getCalendars().size() == 1) {
        DateCalendarEnum unitCalendar = unit.getCalendars().iterator().next();
        if (unitCalendar != DateCalendarEnum.NONE) {
          calendar = unitCalendar;
          break;
        }
      } else if (unit == ChronoUnitEnum.CENTURIES) {
        containsCentury = true;
      }
    }

    if (calendar == DateCalendarEnum.NONE && containsCentury) {
      calendar = DateCalendarEnum.GREGORIAN;
    }

    switch (calendar) {
    case NONE:
      constructedDateTime = constructedDateTime
          .withYear(1)
          .withMonth(1)
          .withDayOfMonth(1);
      break;
    case GREGORIAN:
      constructedDateTime = updateWithGregorianFields(constructedDateTime, dateParts);
      break;
    case ISO_8601:
      constructedDateTime = updateWithIso8601Fields(constructedDateTime, dateParts);
      break;
    case JULIAN:
      final Integer julianDays = dateParts.get(ChronoUnitEnum.DAYS_JULIAN);
      if (julianDays != null) {
        constructedDateTime = constructedDateTime.with(JulianFields.JULIAN_DAY, julianDays);
      }
      break;
    }

    constructedDateTime = updateWithTimeFields(constructedDateTime, dateParts);

    if (dateParts.containsKey(ChronoUnitEnum.TIMEZONE_HOURS)
        || dateParts.containsKey(ChronoUnitEnum.TIMEZONE_MINUTES)) {
      final int hours = dateParts.getOrDefault(ChronoUnitEnum.TIMEZONE_HOURS, 0);
      final int minutes = dateParts.getOrDefault(ChronoUnitEnum.TIMEZONE_MINUTES, 0);

      return ZonedDateTime.of(constructedDateTime, ZoneOffset.ofHoursMinutes(hours, minutes))
          .withZoneSameInstant(zoneId);
    }

    return ZonedDateTime.of(constructedDateTime, zoneId);
  }

  private static LocalDateTime updateWithGregorianFields(LocalDateTime dateTime,
      Map<ChronoUnitEnum, Integer> dateParts) {
    LocalDateTime updatedDateTime = dateTime.withYear(getGregorianYear(dateParts)).withDayOfYear(1);

    if (dateParts.containsKey(ChronoUnitEnum.MONTHS_IN_YEAR)) {
      updatedDateTime =
          updatedDateTime.withMonth(dateParts.get(ChronoUnitEnum.MONTHS_IN_YEAR));
    }

    if (dateParts.containsKey(ChronoUnitEnum.DAYS_IN_MONTH)) {
      updatedDateTime =
          updatedDateTime.withDayOfMonth(dateParts.get(ChronoUnitEnum.DAYS_IN_MONTH));
    }

    if (dateParts.containsKey(ChronoUnitEnum.WEEKS_IN_MONTH)) {
      updatedDateTime =
          updatedDateTime.withDayOfMonth(
              dateParts.get(ChronoUnitEnum.WEEKS_IN_MONTH) * 7 - 6);
    }

    if (dateParts.containsKey(ChronoUnitEnum.WEEKS_IN_YEAR)) {
      updatedDateTime =
          updatedDateTime.withDayOfYear(
              dateParts.get(ChronoUnitEnum.WEEKS_IN_YEAR) * 7 - 6);
    }

    if (dateParts.containsKey(ChronoUnitEnum.DAYS_IN_YEAR)) {
      updatedDateTime =
          updatedDateTime.withDayOfYear(dateParts.get(ChronoUnitEnum.DAYS_IN_YEAR));
    }

    return updatedDateTime;
  }

  private static int getGregorianYear(Map<ChronoUnitEnum, Integer> dateParts) {
    int year =
        getYear(
            dateParts.get(ChronoUnitEnum.ERAS),
            dateParts.get(ChronoUnitEnum.YEARS),
            dateParts.get(ChronoUnitEnum.CENTURIES),
            dateParts.get(ChronoUnitEnum.YEARS_IN_MILLENIA),
            dateParts.get(ChronoUnitEnum.YEARS_IN_CENTURY));
    return year == 0 ? 1 : year;
  }

  private static LocalDateTime updateWithIso8601Fields(LocalDateTime dateTime,
      Map<ChronoUnitEnum, Integer> dateParts) {
    final int year = getIso8601Year(dateParts);

    if (!dateParts.containsKey(ChronoUnitEnum.WEEKS_IN_YEAR_ISO_8601)
        && !dateParts.containsKey(ChronoUnitEnum.DAYS_IN_YEAR_ISO_8601)) {
      return dateTime.withYear(year).withDayOfYear(1);
    }

    LocalDateTime updatedDateTime = dateTime
        .with(ChronoField.DAY_OF_WEEK, 1)
        .with(IsoFields.WEEK_BASED_YEAR, year)
        .with(IsoFields.WEEK_OF_WEEK_BASED_YEAR, 1);

    if (dateParts.containsKey(ChronoUnitEnum.WEEKS_IN_YEAR_ISO_8601)) {
      updatedDateTime =
          updatedDateTime.with(IsoFields.WEEK_OF_WEEK_BASED_YEAR,
              dateParts.get(ChronoUnitEnum.WEEKS_IN_YEAR_ISO_8601));

      if (dateParts.containsKey(ChronoUnitEnum.DAYS_IN_WEEK)) {
        updatedDateTime =
            updatedDateTime.with(ChronoField.DAY_OF_WEEK,
                dateParts.get(ChronoUnitEnum.DAYS_IN_WEEK));
      }
    } else if (dateParts.containsKey(ChronoUnitEnum.DAYS_IN_YEAR_ISO_8601)) {
      updatedDateTime =
          updatedDateTime.plusDays(dateParts.get(ChronoUnitEnum.DAYS_IN_YEAR_ISO_8601) - 1);
    }

    return updatedDateTime;
  }

  private static int getIso8601Year(Map<ChronoUnitEnum, Integer> dateParts) {
    int year =
        getYear(
            dateParts.get(ChronoUnitEnum.ERAS),
            dateParts.get(ChronoUnitEnum.YEARS_ISO_8601),
            dateParts.get(ChronoUnitEnum.CENTURIES),
            dateParts.get(ChronoUnitEnum.YEARS_IN_MILLENIA_ISO_8601),
            dateParts.get(ChronoUnitEnum.YEARS_IN_CENTURY_ISO_8601));
    return year == 0 ? 1 : year;
  }

  private static int getYear(@Nullable Integer era, @Nullable Integer years,
      @Nullable Integer centuries, @Nullable Integer yearsInMillenia,
      @Nullable Integer yearsInCentury) {
    int yearSign = 1;
    if (era != null) {
      if (era == 0) {
        yearSign = -1;
      }
    }

    if (yearsInMillenia != null) {
      int year = yearsInMillenia;
      if (year < 520) {
        year += 2000;
      } else {
        year += 1000;
      }

      return yearSign * year;
    }

    if (centuries != null) {
      int year = 100 * (centuries - 1);

      if (yearsInCentury != null) {
        year += yearsInCentury;
      } else {
        year += 1;
      }

      return yearSign * year;
    }

    if (years != null) {
      return yearSign * years;
    }

    if (yearsInCentury != null) {
      int year = yearsInCentury;
      if (year < 70) {
        year += 2000;
      } else if (year < 100) {
        year += 1900;
      }

      return yearSign * year;
    }

    return yearSign;
  }

  private static LocalDateTime updateWithTimeFields(LocalDateTime dateTime,
      Map<ChronoUnitEnum, Integer> dateParts) {
    LocalDateTime updatedDateTime = dateTime;

    if (dateParts.containsKey(ChronoUnitEnum.HOURS_IN_DAY)) {
      updatedDateTime =
          updatedDateTime.withHour(dateParts.get(ChronoUnitEnum.HOURS_IN_DAY));
    }

    if (dateParts.containsKey(ChronoUnitEnum.HALF_DAYS)
        && dateParts.containsKey(ChronoUnitEnum.HOURS_IN_HALF_DAY)) {
      updatedDateTime =
          updatedDateTime.withHour(dateParts.get(ChronoUnitEnum.HALF_DAYS) * 12
              + dateParts.get(ChronoUnitEnum.HOURS_IN_HALF_DAY));
    } else if (dateParts.containsKey(ChronoUnitEnum.HOURS_IN_HALF_DAY)) {
      updatedDateTime =
          updatedDateTime.withHour(dateParts.get(ChronoUnitEnum.HOURS_IN_HALF_DAY));
    }

    if (dateParts.containsKey(ChronoUnitEnum.MINUTES_IN_HOUR)) {
      updatedDateTime =
          updatedDateTime.withMinute(dateParts.get(ChronoUnitEnum.MINUTES_IN_HOUR));
    }

    if (dateParts.containsKey(ChronoUnitEnum.SECONDS_IN_DAY)) {
      updatedDateTime =
          updatedDateTime.with(ChronoField.SECOND_OF_DAY,
              dateParts.get(ChronoUnitEnum.SECONDS_IN_DAY));
    }

    if (dateParts.containsKey(ChronoUnitEnum.SECONDS_IN_MINUTE)) {
      updatedDateTime =
          updatedDateTime.withSecond(dateParts.get(ChronoUnitEnum.SECONDS_IN_MINUTE));
    }

    if (dateParts.containsKey(ChronoUnitEnum.MILLIS)) {
      updatedDateTime =
          updatedDateTime.with(ChronoField.MILLI_OF_SECOND,
              dateParts.get(ChronoUnitEnum.MILLIS));
    }

    if (dateParts.containsKey(ChronoUnitEnum.MICROS)) {
      updatedDateTime =
          updatedDateTime.with(ChronoField.MICRO_OF_SECOND,
              dateParts.get(ChronoUnitEnum.MICROS));
    }

    if (dateParts.containsKey(ChronoUnitEnum.TENTHS_OF_SECOND)) {
      updatedDateTime =
          updatedDateTime.with(ChronoField.MILLI_OF_SECOND,
              100L * dateParts.get(ChronoUnitEnum.TENTHS_OF_SECOND));
    }

    if (dateParts.containsKey(ChronoUnitEnum.HUNDREDTHS_OF_SECOND)) {
      updatedDateTime =
          updatedDateTime.with(ChronoField.MILLI_OF_SECOND,
              10L * dateParts.get(ChronoUnitEnum.HUNDREDTHS_OF_SECOND));
    }

    if (dateParts.containsKey(ChronoUnitEnum.THOUSANDTHS_OF_SECOND)) {
      updatedDateTime =
          updatedDateTime.with(ChronoField.MILLI_OF_SECOND,
              dateParts.get(ChronoUnitEnum.THOUSANDTHS_OF_SECOND));
    }

    if (dateParts.containsKey(ChronoUnitEnum.TENTHS_OF_MS)) {
      updatedDateTime =
          updatedDateTime.with(ChronoField.MICRO_OF_SECOND,
              100L * dateParts.get(ChronoUnitEnum.TENTHS_OF_MS));
    }

    if (dateParts.containsKey(ChronoUnitEnum.HUNDREDTHS_OF_MS)) {
      updatedDateTime =
          updatedDateTime.with(ChronoField.MICRO_OF_SECOND,
              10L * dateParts.get(ChronoUnitEnum.HUNDREDTHS_OF_MS));
    }

    if (dateParts.containsKey(ChronoUnitEnum.THOUSANDTHS_OF_MS)) {
      updatedDateTime =
          updatedDateTime.with(ChronoField.MICRO_OF_SECOND,
              dateParts.get(ChronoUnitEnum.THOUSANDTHS_OF_MS));
    }

    return updatedDateTime;
  }
}
