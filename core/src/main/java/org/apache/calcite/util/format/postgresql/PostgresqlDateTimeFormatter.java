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

import org.apache.calcite.util.format.postgresql.format.AmPmFormatPattern;
import org.apache.calcite.util.format.postgresql.format.BcAdFormatPattern;
import org.apache.calcite.util.format.postgresql.format.DayOfWeekFormatPattern;
import org.apache.calcite.util.format.postgresql.format.FormatPattern;
import org.apache.calcite.util.format.postgresql.format.MonthFormatPattern;
import org.apache.calcite.util.format.postgresql.format.NumberFormatPattern;
import org.apache.calcite.util.format.postgresql.format.RomanNumeralsFormatPattern;
import org.apache.calcite.util.format.postgresql.format.TimeZoneFormatPattern;
import org.apache.calcite.util.format.postgresql.format.TimeZoneHoursFormatPattern;
import org.apache.calcite.util.format.postgresql.format.TimeZoneMinutesFormatPattern;
import org.apache.calcite.util.format.postgresql.format.TimeZoneOffsetFormatPattern;
import org.apache.calcite.util.format.postgresql.format.YearWithCommasFormatPattern;
import org.apache.calcite.util.format.postgresql.format.compiled.CompiledItem;
import org.apache.calcite.util.format.postgresql.format.compiled.CompiledPattern;
import org.apache.calcite.util.format.postgresql.format.compiled.LiteralCompiledItem;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.text.ParseException;
import java.text.ParsePosition;
import java.time.Month;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.time.temporal.IsoFields;
import java.time.temporal.JulianFields;

/**
 * Provides an implementation of toChar that matches PostgreSQL behaviour.
 */
public class PostgresqlDateTimeFormatter {
  /**
   * The format patterns that are supported. Order is very important, since some patterns
   * are prefixes of other patterns.
   */
  private static final FormatPattern[] FORMAT_PATTERNS =
      new FormatPattern[] {
          new NumberFormatPattern(
              "HH24",
              ChronoUnitEnum.HOURS_IN_DAY,
              ZonedDateTime::getHour,
              2,
              2,
              0,
              23),
          new NumberFormatPattern(
              "HH12",
              ChronoUnitEnum.HOURS_IN_HALF_DAY,
              dt -> {
                final int value = dt.get(ChronoField.HOUR_OF_AMPM);
                return value > 0 ? value : 12;
              },
              2,
              2,
              0,
              12),
          new NumberFormatPattern(
              "HH",
              ChronoUnitEnum.HOURS_IN_HALF_DAY,
              dt -> {
                final int value = dt.get(ChronoField.HOUR_OF_AMPM);
                return value > 0 ? value : 12;
              },
              2,
              2,
              0,
              12),
          new NumberFormatPattern(
              "MI",
              ChronoUnitEnum.MINUTES_IN_HOUR,
              ZonedDateTime::getMinute,
              2,
              2,
              0,
              59),
          new NumberFormatPattern(
              "SSSSS",
              ChronoUnitEnum.SECONDS_IN_DAY,
              dt -> dt.get(ChronoField.SECOND_OF_DAY),
              -1,
              5,
              0,
              24 * 60 * 60 - 1),
          new NumberFormatPattern(
              "SSSS",
              ChronoUnitEnum.SECONDS_IN_DAY,
              dt -> dt.get(ChronoField.SECOND_OF_DAY),
              1,
              5,
              0,
              24 * 60 * 60 - 1),
          new NumberFormatPattern(
              "SS",
              ChronoUnitEnum.SECONDS_IN_MINUTE,
              ZonedDateTime::getSecond,
              2,
              2,
              0,
              59),
          new NumberFormatPattern(
              "MS",
              ChronoUnitEnum.MILLIS,
              dt -> dt.get(ChronoField.MILLI_OF_SECOND),
              3,
              3,
              0,
              999),
          new NumberFormatPattern(
              "US",
              ChronoUnitEnum.MICROS,
              dt -> dt.get(ChronoField.MICRO_OF_SECOND),
              6,
              6,
              0,
              999_999),
          new NumberFormatPattern(
              "FF1",
              ChronoUnitEnum.TENTHS_OF_SECOND,
              dt -> dt.get(ChronoField.MILLI_OF_SECOND) / 100,
              1,
              1,
              0,
              9),
          new NumberFormatPattern(
              "FF2",
              ChronoUnitEnum.HUNDREDTHS_OF_SECOND,
              dt -> dt.get(ChronoField.MILLI_OF_SECOND) / 10,
              2,
              2,
              0,
              99),
          new NumberFormatPattern(
              "FF3",
              ChronoUnitEnum.MILLIS,
              dt -> dt.get(ChronoField.MILLI_OF_SECOND),
              3,
              3,
              0,
              999),
          new NumberFormatPattern(
              "FF4",
              ChronoUnitEnum.TENTHS_OF_MS,
              dt -> dt.get(ChronoField.MICRO_OF_SECOND) / 100,
              4,
              4,
              0,
              9_999),
          new NumberFormatPattern(
              "FF5",
              ChronoUnitEnum.HUNDREDTHS_OF_MS,
              dt -> dt.get(ChronoField.MICRO_OF_SECOND) / 10,
              5,
              5,
              0,
              99_999),
          new NumberFormatPattern(
              "FF6",
              ChronoUnitEnum.THOUSANDTHS_OF_MS,
              dt -> dt.get(ChronoField.MICRO_OF_SECOND),
              6,
              6,
              0,
              999_999),
          new AmPmFormatPattern("AM"),
          new AmPmFormatPattern("PM"),
          new AmPmFormatPattern("A.M."),
          new AmPmFormatPattern("P.M."),
          new AmPmFormatPattern("am"),
          new AmPmFormatPattern("pm"),
          new AmPmFormatPattern("a.m."),
          new AmPmFormatPattern("p.m."),
          new YearWithCommasFormatPattern(),
          new NumberFormatPattern(
              "YYYY",
              ChronoUnitEnum.YEARS,
              ZonedDateTime::getYear,
              4,
              -1,
              0,
              Integer.MAX_VALUE),
          new NumberFormatPattern(
              "IYYY",
              ChronoUnitEnum.YEARS_ISO_8601,
              dt -> dt.get(IsoFields.WEEK_BASED_YEAR),
              4,
              -1,
              0,
              Integer.MAX_VALUE),
          new NumberFormatPattern(
              "IYY",
              ChronoUnitEnum.YEARS_IN_MILLENIA_ISO_8601,
              dt -> dt.get(IsoFields.WEEK_BASED_YEAR) % 1000,
              3,
              3,
              0,
              Integer.MAX_VALUE),
          new NumberFormatPattern(
              "IY",
              ChronoUnitEnum.YEARS_IN_CENTURY_ISO_8601,
              dt -> dt.get(IsoFields.WEEK_BASED_YEAR) % 100,
              2,
              2,
              0,
              Integer.MAX_VALUE),
          new NumberFormatPattern(
              "YYY",
              ChronoUnitEnum.YEARS_IN_MILLENIA,
              dt -> dt.getYear() % 1000,
              3,
              3,
              0,
              Integer.MAX_VALUE),
          new NumberFormatPattern(
              "YY",
              ChronoUnitEnum.YEARS_IN_CENTURY,
              dt -> dt.getYear() % 100,
              2,
              2,
              0,
              Integer.MAX_VALUE),
          new NumberFormatPattern(
              "Y",
              ChronoUnitEnum.YEARS_IN_CENTURY,
              ZonedDateTime::getYear,
              1,
              1,
              0,
              Integer.MAX_VALUE),
          new NumberFormatPattern(
              "IW",
              ChronoUnitEnum.WEEKS_IN_YEAR_ISO_8601,
              dt -> dt.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR),
              2,
              2,
              1,
              53),
          new NumberFormatPattern(
              "IDDD",
              ChronoUnitEnum.DAYS_IN_YEAR_ISO_8601,
              dt -> {
                final Month month = dt.getMonth();
                final int dayOfMonth = dt.getDayOfMonth();
                final int weekNumber = dt.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR);

                if (month == Month.JANUARY && dayOfMonth < 4) {
                  if (weekNumber == 1) {
                    return dt.getDayOfWeek().getValue();
                  }
                } else if (month == Month.DECEMBER && dayOfMonth >= 29) {
                  if (weekNumber == 1) {
                    return dt.getDayOfWeek().getValue();
                  }
                }

                return (weekNumber - 1) * 7 + dt.getDayOfWeek().getValue();
              },
              3,
              3,
              0,
              371),
          new NumberFormatPattern(
              "ID",
              ChronoUnitEnum.DAYS_IN_WEEK,
              dt -> dt.getDayOfWeek().getValue(),
              1,
              1,
              1,
              7),
          new NumberFormatPattern(
              "I",
              ChronoUnitEnum.YEARS_IN_CENTURY_ISO_8601,
              dt -> dt.get(IsoFields.WEEK_BASED_YEAR),
              1,
              1,
              0,
              Integer.MAX_VALUE),
          new BcAdFormatPattern("BC"),
          new BcAdFormatPattern("AD"),
          new BcAdFormatPattern("B.C."),
          new BcAdFormatPattern("A.D."),
          new BcAdFormatPattern("bc"),
          new BcAdFormatPattern("ad"),
          new BcAdFormatPattern("b.c."),
          new BcAdFormatPattern("a.d."),
          new MonthFormatPattern("MONTH"),
          new MonthFormatPattern("Month"),
          new MonthFormatPattern("month"),
          new MonthFormatPattern("MON"),
          new MonthFormatPattern("Mon"),
          new MonthFormatPattern("mon"),
          new NumberFormatPattern(
              "MM",
              ChronoUnitEnum.MONTHS_IN_YEAR,
              dt -> dt.getMonth().getValue(),
              2,
              2,
              1,
              12),
          new DayOfWeekFormatPattern("DAY"),
          new DayOfWeekFormatPattern("Day"),
          new DayOfWeekFormatPattern("day"),
          new DayOfWeekFormatPattern("DY"),
          new DayOfWeekFormatPattern("Dy"),
          new DayOfWeekFormatPattern("dy"),
          new NumberFormatPattern(
              "DDD",
              ChronoUnitEnum.DAYS_IN_YEAR,
              ZonedDateTime::getDayOfYear,
              3,
              3,
              1,
              366),
          new NumberFormatPattern(
              "DD",
              ChronoUnitEnum.DAYS_IN_MONTH,
              ZonedDateTime::getDayOfMonth,
              2,
              2,
              1,
              31),
          new NumberFormatPattern(
              "D",
              ChronoUnitEnum.DAYS_IN_WEEK,
              dt -> {
                int dayOfWeek = dt.getDayOfWeek().getValue() + 1;
                if (dayOfWeek == 8) {
                  return 1;
                }
                return dayOfWeek;
              },
              v -> v < 7 ? v + 1 : 1,
              1,
              1,
              1,
              7),
          new NumberFormatPattern(
              "WW",
              ChronoUnitEnum.WEEKS_IN_YEAR,
              dt -> (int) Math.ceil((double) dt.getDayOfYear() / 7),
              -1,
              2,
              1,
              53),
          new NumberFormatPattern(
              "W",
              ChronoUnitEnum.WEEKS_IN_MONTH,
              dt -> (int) Math.ceil((double) dt.getDayOfMonth() / 7),
              1,
              1,
              1,
              5),
          new NumberFormatPattern(
              "CC",
              ChronoUnitEnum.CENTURIES,
              dt -> {
                if (dt.get(ChronoField.ERA) == 0) {
                  return dt.getYear() / 100 - 1;
                } else {
                  return dt.getYear() / 100 + 1;
                }
              },
              2,
              Integer.MAX_VALUE,
              0,
              Integer.MAX_VALUE),
          new NumberFormatPattern(
              "J",
              ChronoUnitEnum.DAYS_JULIAN,
              dt -> {
                final int julianDays = (int) dt.getLong(JulianFields.JULIAN_DAY);
                if (dt.getYear() < 0) {
                  return 365 + julianDays;
                } else {
                  return julianDays;
                }
              },
              -1,
              -1,
              0,
              Integer.MAX_VALUE),
          new NumberFormatPattern(
              "Q",
              ChronoUnitEnum.MONTHS_IN_YEAR,
              dt -> dt.get(IsoFields.QUARTER_OF_YEAR),
              1,
              1,
              1,
              4),
          new RomanNumeralsFormatPattern("RM"),
          new RomanNumeralsFormatPattern("rm"),
          new TimeZoneHoursFormatPattern(),
          new TimeZoneMinutesFormatPattern(),
          new TimeZoneFormatPattern("TZ"),
          new TimeZoneFormatPattern("tz"),
          new TimeZoneOffsetFormatPattern()
      };

  /**
   * Remove access to the default constructor.
   */
  private PostgresqlDateTimeFormatter() {
  }

  public static CompiledDateTimeFormat compilePattern(String format) {
    final ImmutableList.Builder<CompiledItem> compiledPatterns = ImmutableList.builder();
    final ParsePosition parsePosition = new ParsePosition(0);
    final ParsePosition nextPatternPosition = new ParsePosition(parsePosition.getIndex());

    FormatPattern nextPattern = getNextPattern(format, nextPatternPosition);
    while (nextPattern != null) {
      // Add the literal if one exists before the next pattern
      if (parsePosition.getIndex() < nextPatternPosition.getIndex()) {
        compiledPatterns.add(
            new LiteralCompiledItem(
                format.substring(parsePosition.getIndex(), nextPatternPosition.getIndex())));
      }

      try {
        final CompiledPattern compiledPattern =
            nextPattern.compilePattern(format, nextPatternPosition);
        compiledPatterns.add(compiledPattern);
        parsePosition.setIndex(
            nextPatternPosition.getIndex() + compiledPattern.getFormatPatternLength());
        nextPatternPosition.setIndex(parsePosition.getIndex());
        nextPattern = getNextPattern(format, nextPatternPosition);
      } catch (ParseException e) {
        throw new IllegalArgumentException();
      }
    }

    if (parsePosition.getIndex() < format.length()) {
      compiledPatterns.add(new LiteralCompiledItem(format.substring(parsePosition.getIndex())));
    }

    return new CompiledDateTimeFormat(compiledPatterns.build());
  }

  private static @Nullable FormatPattern getNextPattern(String format,
      ParsePosition parsePosition) {
    while (parsePosition.getIndex() < format.length()) {
      String formatTrimmed = format.substring(parsePosition.getIndex());

      boolean prefixFound = true;
      while (prefixFound) {
        prefixFound = false;

        for (PatternModifier modifier : PatternModifier.values()) {
          if (modifier.isPrefix() && formatTrimmed.startsWith(modifier.getModifierString())) {
            formatTrimmed = formatTrimmed.substring(modifier.getModifierString().length());
            prefixFound = true;
          }
        }
      }

      for (FormatPattern formatPattern : FORMAT_PATTERNS) {
        if (formatTrimmed.startsWith(formatPattern.getPattern())) {
          return formatPattern;
        }
      }

      parsePosition.setIndex(parsePosition.getIndex() + 1);
    }

    return null;
  }
}
