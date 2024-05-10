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

import org.checkerframework.checker.nullness.qual.Nullable;

import java.text.ParsePosition;
import java.time.Month;
import java.time.ZonedDateTime;
import java.time.format.TextStyle;
import java.time.temporal.ChronoField;
import java.time.temporal.IsoFields;
import java.time.temporal.JulianFields;
import java.util.Locale;

/**
 * Provides an implementation of toChar that matches PostgreSQL behaviour.
 */
public class PostgresqlDateTimeFormatter {
  /**
   * The format patterns that are supported. Order is very important, since some patterns
   * are prefixes of other patterns.
   */
  @SuppressWarnings("TemporalAccessorGetChronoField")
  private static final FormatPattern[] FORMAT_PATTERNS = new FormatPattern[] {
      new NumberFormatPattern(
          dt -> {
            final int hour = dt.get(ChronoField.HOUR_OF_AMPM);
            return String.format(Locale.ROOT, "%02d", hour == 0 ? 12 : hour);
          },
          "HH12"),
      new NumberFormatPattern(
          dt -> String.format(Locale.ROOT, "%02d", dt.getHour()),
          "HH24"),
      new NumberFormatPattern(
          dt -> {
            final int hour = dt.get(ChronoField.HOUR_OF_AMPM);
            return String.format(Locale.ROOT, "%02d", hour == 0 ? 12 : hour);
          },
          "HH"),
      new NumberFormatPattern(
          dt -> String.format(Locale.ROOT, "%02d", dt.getMinute()),
          "MI"),
      new NumberFormatPattern(
          dt -> Integer.toString(dt.get(ChronoField.SECOND_OF_DAY)),
          "SSSSS", "SSSS"),
      new NumberFormatPattern(
          dt -> String.format(Locale.ROOT, "%02d", dt.getSecond()),
          "SS"),
      new NumberFormatPattern(
          dt -> String.format(Locale.ROOT, "%03d", dt.get(ChronoField.MILLI_OF_SECOND)),
          "MS"),
      new NumberFormatPattern(
          dt -> String.format(Locale.ROOT, "%06d", dt.get(ChronoField.MICRO_OF_SECOND)),
          "US"),
      new NumberFormatPattern(
          dt -> Integer.toString(dt.get(ChronoField.MILLI_OF_SECOND) / 100),
          "FF1"),
      new NumberFormatPattern(
          dt -> String.format(Locale.ROOT, "%02d", dt.get(ChronoField.MILLI_OF_SECOND) / 10),
          "FF2"),
      new NumberFormatPattern(
          dt -> String.format(Locale.ROOT, "%03d", dt.get(ChronoField.MILLI_OF_SECOND)),
          "FF3"),
      new NumberFormatPattern(
          dt -> String.format(Locale.ROOT, "%04d", dt.get(ChronoField.MICRO_OF_SECOND) / 100),
          "FF4"),
      new NumberFormatPattern(
          dt -> String.format(Locale.ROOT, "%05d", dt.get(ChronoField.MICRO_OF_SECOND) / 10),
          "FF5"),
      new NumberFormatPattern(
          dt -> String.format(Locale.ROOT, "%06d", dt.get(ChronoField.MICRO_OF_SECOND)),
          "FF6"),
      new EnumStringFormatPattern(ChronoField.AMPM_OF_DAY, "AM", "PM"),
      new EnumStringFormatPattern(ChronoField.AMPM_OF_DAY, "am", "pm"),
      new EnumStringFormatPattern(ChronoField.AMPM_OF_DAY, "A.M.", "P.M."),
      new EnumStringFormatPattern(ChronoField.AMPM_OF_DAY, "a.m.", "p.m."),
      new NumberFormatPattern(dt -> {
        final String formattedYear = String.format(Locale.ROOT, "%0,4d", dt.getYear());
        if (formattedYear.length() == 4 && formattedYear.charAt(0) == '0') {
          return "0," + formattedYear.substring(1);
        } else {
          return formattedYear;
        }
      }, "Y,YYY") {
        @Override protected String trimLeadingZeros(String value) {
          return value;
        }
      },
      new NumberFormatPattern(
          dt -> String.format(Locale.ROOT, "%04d", dt.getYear()),
          "YYYY"),
      new NumberFormatPattern(
          dt -> Integer.toString(dt.get(IsoFields.WEEK_BASED_YEAR)),
          "IYYY"),
      new NumberFormatPattern(
          dt -> {
            final String yearString =
                String.format(Locale.ROOT, "%03d", dt.get(IsoFields.WEEK_BASED_YEAR));
            return yearString.substring(yearString.length() - 3);
          },
          "IYY"),
      new NumberFormatPattern(
          dt -> {
            final String yearString =
                String.format(Locale.ROOT, "%02d", dt.get(IsoFields.WEEK_BASED_YEAR));
            return yearString.substring(yearString.length() - 2);
          },
          "IY"),
      new NumberFormatPattern(
          dt -> {
            final String formattedYear = String.format(Locale.ROOT, "%03d", dt.getYear());
            if (formattedYear.length() > 3) {
              return formattedYear.substring(formattedYear.length() - 3);
            } else {
              return formattedYear;
            }
          },
          "YYY"),
      new NumberFormatPattern(
          dt -> {
            final String formattedYear = String.format(Locale.ROOT, "%02d", dt.getYear());
            if (formattedYear.length() > 2) {
              return formattedYear.substring(formattedYear.length() - 2);
            } else {
              return formattedYear;
            }
          },
          "YY"),
      new NumberFormatPattern(
          dt -> {
            final String formattedYear = Integer.toString(dt.getYear());
            if (formattedYear.length() > 1) {
              return formattedYear.substring(formattedYear.length() - 1);
            } else {
              return formattedYear;
            }
          },
          "Y"),
      new NumberFormatPattern(
          dt -> String.format(Locale.ROOT, "%02d", dt.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR)),
          "IW"),
      new NumberFormatPattern(
          dt -> {
            final Month month = dt.getMonth();
            final int dayOfMonth = dt.getDayOfMonth();
            final int weekNumber = dt.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR);

            if (month == Month.JANUARY && dayOfMonth < 4) {
              if (weekNumber == 1) {
                return String.format(Locale.ROOT, "%03d", dt.getDayOfWeek().getValue());
              }
            } else if (month == Month.DECEMBER && dayOfMonth >= 29) {
              if (weekNumber == 1) {
                return String.format(Locale.ROOT, "%03d", dt.getDayOfWeek().getValue());
              }
            }

            return String.format(Locale.ROOT, "%03d",
                (weekNumber - 1) * 7 + dt.getDayOfWeek().getValue());
          },
          "IDDD"),
      new NumberFormatPattern(
          dt -> Integer.toString(dt.getDayOfWeek().getValue()),
          "ID"),
      new NumberFormatPattern(
          dt -> {
            final String yearString = Integer.toString(dt.get(IsoFields.WEEK_BASED_YEAR));
            return yearString.substring(yearString.length() - 1);
          },
          "I"),
      new EnumStringFormatPattern(ChronoField.ERA, "BC", "AD"),
      new EnumStringFormatPattern(ChronoField.ERA, "bc", "ad"),
      new EnumStringFormatPattern(ChronoField.ERA, "B.C.", "A.D."),
      new EnumStringFormatPattern(ChronoField.ERA, "b.c.", "a.d."),
      DateStringFormatPattern.forMonth(TextStyle.FULL, CapitalizationEnum.ALL_UPPER, "MONTH"),
      DateStringFormatPattern.forMonth(TextStyle.FULL, CapitalizationEnum.CAPITALIZED, "Month"),
      DateStringFormatPattern.forMonth(TextStyle.FULL, CapitalizationEnum.ALL_LOWER, "month"),
      DateStringFormatPattern.forMonth(TextStyle.SHORT, CapitalizationEnum.ALL_UPPER, "MON"),
      DateStringFormatPattern.forMonth(TextStyle.SHORT, CapitalizationEnum.CAPITALIZED, "Mon"),
      DateStringFormatPattern.forMonth(TextStyle.SHORT, CapitalizationEnum.ALL_LOWER, "mon"),
      new NumberFormatPattern(
          dt -> String.format(Locale.ROOT, "%02d", dt.getMonthValue()),
          "MM"),
      DateStringFormatPattern.forDayOfWeek(TextStyle.FULL, CapitalizationEnum.ALL_UPPER, "DAY"),
      DateStringFormatPattern.forDayOfWeek(TextStyle.FULL, CapitalizationEnum.CAPITALIZED, "Day"),
      DateStringFormatPattern.forDayOfWeek(TextStyle.FULL, CapitalizationEnum.ALL_LOWER, "day"),
      DateStringFormatPattern.forDayOfWeek(TextStyle.SHORT, CapitalizationEnum.ALL_UPPER, "DY"),
      DateStringFormatPattern.forDayOfWeek(TextStyle.SHORT, CapitalizationEnum.CAPITALIZED, "Dy"),
      DateStringFormatPattern.forDayOfWeek(TextStyle.SHORT, CapitalizationEnum.ALL_LOWER, "dy"),
      new NumberFormatPattern(
          dt -> String.format(Locale.ROOT, "%03d", dt.getDayOfYear()),
          "DDD"),
      new NumberFormatPattern(
          dt -> String.format(Locale.ROOT, "%02d", dt.getDayOfMonth()),
          "DD"),
      new NumberFormatPattern(
          dt -> {
            int dayOfWeek = dt.getDayOfWeek().getValue() + 1;
            if (dayOfWeek == 8) {
              dayOfWeek = 1;
            }
            return Integer.toString(dayOfWeek);
          },
          "D"),
      new NumberFormatPattern(
          dt -> Integer.toString((int) Math.ceil((double) dt.getDayOfYear() / 7)),
          "WW"),
      new NumberFormatPattern(
          dt -> Integer.toString((int) Math.ceil((double) dt.getDayOfMonth() / 7)),
          "W"),
      new NumberFormatPattern(
          dt -> {
            if (dt.get(ChronoField.ERA) == 0) {
              return String.format(Locale.ROOT, "-%02d", Math.abs(dt.getYear() / 100 - 1));
            } else {
              return String.format(Locale.ROOT, "%02d", dt.getYear() / 100 + 1);
            }
          },
          "CC"),
      new NumberFormatPattern(
          dt -> {
            final long julianDays = dt.getLong(JulianFields.JULIAN_DAY);
            if (dt.getYear() < 0) {
              return Long.toString(365L + julianDays);
            } else {
              return Long.toString(julianDays);
            }
          },
          "J"),
      new NumberFormatPattern(
          dt -> Integer.toString(dt.get(IsoFields.QUARTER_OF_YEAR)),
          "Q"),
      new RomanNumeralMonthFormatPattern(true, "RM"),
      new RomanNumeralMonthFormatPattern(false, "rm"),
      new TimeZoneHoursFormatPattern(),
      new TimeZoneMinutesFormatPattern(),
      new TimeZoneFormatPattern(true, "TZ"),
      new TimeZoneFormatPattern(false, "tz"),
      new StringFormatPattern("OF") {
        @Override String dateTimeToString(ZonedDateTime dateTime, boolean haveFillMode,
            @Nullable String suffix, Locale locale) {
          final int hours = dateTime.getOffset().get(ChronoField.HOUR_OF_DAY);
          final int minutes = dateTime.getOffset().get(ChronoField.MINUTE_OF_HOUR);

          String formattedHours =
              String.format(Locale.ROOT, "%s%02d", hours < 0 ? "-" : "+", hours);
          if (minutes == 0) {
            return formattedHours;
          } else {
            return String.format(Locale.ROOT, "%s:%02d", formattedHours, minutes);
          }
        }
      }
  };

  /**
   * Remove access to the default constructor.
   */
  private PostgresqlDateTimeFormatter() {
  }

  /**
   * Converts a format string such as "YYYY-MM-DD" with a datetime to a string representation.
   *
   * @see <a href="https://www.postgresql.org/docs/14/functions-formatting.html#FUNCTIONS-FORMATTING-DATETIME-TABLE">PostgreSQL</a>
   *
   * @param formatString input format string
   * @param dateTime datetime to convert
   * @return formatted string representation of the datetime
   */
  public static String toChar(String formatString, ZonedDateTime dateTime) {
    final ParsePosition parsePosition = new ParsePosition(0);
    final StringBuilder sb = new StringBuilder();

    while (parsePosition.getIndex() < formatString.length()) {
      boolean matched = false;

      for (FormatPattern formatPattern : FORMAT_PATTERNS) {
        final String formattedString =
            formatPattern.convert(parsePosition, formatString, dateTime);
        if (formattedString != null) {
          sb.append(formattedString);
          matched = true;
          break;
        }
      }

      if (!matched) {
        sb.append(formatString.charAt(parsePosition.getIndex()));
        parsePosition.setIndex(parsePosition.getIndex() + 1);
      }
    }

    return sb.toString();
  }
}
