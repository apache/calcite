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

import java.text.ParseException;
import java.text.ParsePosition;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.TextStyle;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.time.temporal.IsoFields;
import java.time.temporal.JulianFields;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

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
          ChronoUnitEnum.HOURS_IN_DAY,
          0,
          23,
          2,
          dt -> String.format(Locale.ROOT, "%02d", dt.getHour()),
          "HH24"),
      new NumberFormatPattern(
          ChronoUnitEnum.HOURS_IN_HALF_DAY,
          1,
          12,
          2,
          dt -> {
            final int hour = dt.get(ChronoField.HOUR_OF_AMPM);
            return String.format(Locale.ROOT, "%02d", hour == 0 ? 12 : hour);
          },
          "HH12", "HH"),
      new NumberFormatPattern(
          ChronoUnitEnum.MINUTES_IN_HOUR,
          0,
          59,
          2,
          dt -> String.format(Locale.ROOT, "%02d", dt.getMinute()),
          "MI"),
      new NumberFormatPattern(
          ChronoUnitEnum.SECONDS_IN_DAY,
          0,
          24 * 60 * 60 - 1,
          5,
          dt -> Integer.toString(dt.get(ChronoField.SECOND_OF_DAY)),
          "SSSSS", "SSSS"),
      new NumberFormatPattern(
          ChronoUnitEnum.SECONDS_IN_MINUTE,
          0,
          59,
          2,
          dt -> String.format(Locale.ROOT, "%02d", dt.getSecond()),
          "SS"),
      new NumberFormatPattern(
          ChronoUnitEnum.MILLIS,
          0,
          999,
          3,
          dt -> String.format(Locale.ROOT, "%03d", dt.get(ChronoField.MILLI_OF_SECOND)),
          "MS"),
      new NumberFormatPattern(
          ChronoUnitEnum.MICROS,
          0,
          999_999,
          6,
          dt -> String.format(Locale.ROOT, "%06d", dt.get(ChronoField.MICRO_OF_SECOND)),
          "US"),
      new NumberFormatPattern(
          ChronoUnitEnum.TENTHS_OF_SECOND,
          0,
          9,
          1,
          dt -> Integer.toString(dt.get(ChronoField.MILLI_OF_SECOND) / 100),
          "FF1"),
      new NumberFormatPattern(
          ChronoUnitEnum.HUNDREDTHS_OF_SECOND,
          0,
          99,
          2,
          dt -> String.format(Locale.ROOT, "%02d", dt.get(ChronoField.MILLI_OF_SECOND) / 10),
          "FF2"),
      new NumberFormatPattern(
          ChronoUnitEnum.THOUSANDTHS_OF_SECOND,
          0,
          999,
          3,
          dt -> String.format(Locale.ROOT, "%03d", dt.get(ChronoField.MILLI_OF_SECOND)),
          "FF3"),
      new NumberFormatPattern(
          ChronoUnitEnum.TENTHS_OF_MS,
          0,
          9_999,
          4,
          dt -> String.format(Locale.ROOT, "%04d", dt.get(ChronoField.MICRO_OF_SECOND) / 100),
          "FF4"),
      new NumberFormatPattern(
          ChronoUnitEnum.HUNDREDTHS_OF_MS,
          0,
          99_999,
          5,
          dt -> String.format(Locale.ROOT, "%05d", dt.get(ChronoField.MICRO_OF_SECOND) / 10),
          "FF5"),
      new NumberFormatPattern(
          ChronoUnitEnum.THOUSANDTHS_OF_MS,
          0,
          999_999,
          6,
          dt -> String.format(Locale.ROOT, "%06d", dt.get(ChronoField.MICRO_OF_SECOND)),
          "FF6"),
      new EnumStringFormatPattern(
          ChronoUnitEnum.HALF_DAYS,
          ChronoField.AMPM_OF_DAY,
          "AM", "PM"),
      new EnumStringFormatPattern(
          ChronoUnitEnum.HALF_DAYS,
          ChronoField.AMPM_OF_DAY,
          "am", "pm"),
      new EnumStringFormatPattern(
          ChronoUnitEnum.HALF_DAYS,
          ChronoField.AMPM_OF_DAY,
          "A.M.", "P.M."),
      new EnumStringFormatPattern(
          ChronoUnitEnum.HALF_DAYS,
          ChronoField.AMPM_OF_DAY,
          "a.m.", "p.m."),
      new YearWithCommasFormatPattern(),
      new NumberFormatPattern(
          ChronoUnitEnum.YEARS,
          0,
          Integer.MAX_VALUE,
          4,
          dt -> String.format(Locale.ROOT, "%04d", dt.getYear()),
          "YYYY"),
      new NumberFormatPattern(
          ChronoUnitEnum.YEARS_ISO_8601,
          0,
          Integer.MAX_VALUE,
          4,
          dt -> Integer.toString(dt.get(IsoFields.WEEK_BASED_YEAR)),
          "IYYY"),
      new NumberFormatPattern(
          ChronoUnitEnum.YEARS_IN_MILLENIA_ISO_8601,
          0,
          Integer.MAX_VALUE,
          3,
          dt -> {
            final String yearString =
                String.format(Locale.ROOT, "%03d", dt.get(IsoFields.WEEK_BASED_YEAR));
            return yearString.substring(yearString.length() - 3);
          },
          "IYY"),
      new NumberFormatPattern(
          ChronoUnitEnum.YEARS_IN_CENTURY_ISO_8601,
          0,
          Integer.MAX_VALUE,
          2,
          dt -> {
            final String yearString =
                String.format(Locale.ROOT, "%02d", dt.get(IsoFields.WEEK_BASED_YEAR));
            return yearString.substring(yearString.length() - 2);
          },
          "IY"),
      new NumberFormatPattern(
          ChronoUnitEnum.YEARS_IN_MILLENIA,
          0,
          Integer.MAX_VALUE,
          3,
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
          ChronoUnitEnum.YEARS_IN_CENTURY,
          0,
          Integer.MAX_VALUE,
          2,
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
          ChronoUnitEnum.YEARS_IN_CENTURY,
          0,
          Integer.MAX_VALUE,
          1,
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
          ChronoUnitEnum.WEEKS_IN_YEAR_ISO_8601,
          1,
          53,
          2,
          dt -> String.format(Locale.ROOT, "%02d", dt.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR)),
          "IW"),
      new NumberFormatPattern(
          ChronoUnitEnum.DAYS_IN_YEAR_ISO_8601,
          0,
          371,
          3,
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
          ChronoUnitEnum.DAYS_IN_WEEK,
          1,
          7,
          1,
          dt -> Integer.toString(dt.getDayOfWeek().getValue()),
          "ID"),
      new NumberFormatPattern(
          ChronoUnitEnum.YEARS_IN_CENTURY_ISO_8601,
          0,
          Integer.MAX_VALUE,
          1,
          dt -> {
            final String yearString = Integer.toString(dt.get(IsoFields.WEEK_BASED_YEAR));
            return yearString.substring(yearString.length() - 1);
          },
          "I"),
      new EnumStringFormatPattern(ChronoUnitEnum.ERAS, ChronoField.ERA, "BC", "AD"),
      new EnumStringFormatPattern(ChronoUnitEnum.ERAS, ChronoField.ERA, "bc", "ad"),
      new EnumStringFormatPattern(ChronoUnitEnum.ERAS, ChronoField.ERA, "B.C.", "A.D."),
      new EnumStringFormatPattern(ChronoUnitEnum.ERAS, ChronoField.ERA, "b.c.", "a.d."),
      DateStringFormatPattern.forMonth(TextStyle.FULL, CapitalizationEnum.ALL_UPPER, "MONTH"),
      DateStringFormatPattern.forMonth(TextStyle.FULL, CapitalizationEnum.CAPITALIZED, "Month"),
      DateStringFormatPattern.forMonth(TextStyle.FULL, CapitalizationEnum.ALL_LOWER, "month"),
      DateStringFormatPattern.forMonth(TextStyle.SHORT, CapitalizationEnum.ALL_UPPER, "MON"),
      DateStringFormatPattern.forMonth(TextStyle.SHORT, CapitalizationEnum.CAPITALIZED, "Mon"),
      DateStringFormatPattern.forMonth(TextStyle.SHORT, CapitalizationEnum.ALL_LOWER, "mon"),
      new NumberFormatPattern(
          ChronoUnitEnum.MONTHS_IN_YEAR,
          1,
          12,
          2,
          dt -> String.format(Locale.ROOT, "%02d", dt.getMonthValue()),
          "MM"),
      DateStringFormatPattern.forDayOfWeek(TextStyle.FULL, CapitalizationEnum.ALL_UPPER, "DAY"),
      DateStringFormatPattern.forDayOfWeek(TextStyle.FULL, CapitalizationEnum.CAPITALIZED, "Day"),
      DateStringFormatPattern.forDayOfWeek(TextStyle.FULL, CapitalizationEnum.ALL_LOWER, "day"),
      DateStringFormatPattern.forDayOfWeek(TextStyle.SHORT, CapitalizationEnum.ALL_UPPER, "DY"),
      DateStringFormatPattern.forDayOfWeek(TextStyle.SHORT, CapitalizationEnum.CAPITALIZED, "Dy"),
      DateStringFormatPattern.forDayOfWeek(TextStyle.SHORT, CapitalizationEnum.ALL_LOWER, "dy"),
      new NumberFormatPattern(
          ChronoUnitEnum.DAYS_IN_YEAR,
          1,
          366,
          3,
          dt -> String.format(Locale.ROOT, "%03d", dt.getDayOfYear()),
          "DDD"),
      new NumberFormatPattern(
          ChronoUnitEnum.DAYS_IN_MONTH,
          1,
          31,
          2,
          dt -> String.format(Locale.ROOT, "%02d", dt.getDayOfMonth()),
          "DD"),
      new NumberFormatPattern(
          ChronoUnitEnum.DAYS_IN_WEEK,
          1,
          7,
          1,
          dt -> {
            int dayOfWeek = dt.getDayOfWeek().getValue() + 1;
            if (dayOfWeek == 8) {
              dayOfWeek = 1;
            }
            return Integer.toString(dayOfWeek);
          },
          v -> v < 7 ? v + 1 : 1,
          "D"),
      new NumberFormatPattern(
          ChronoUnitEnum.WEEKS_IN_YEAR,
          1,
          53,
          2,
          dt -> Integer.toString((int) Math.ceil((double) dt.getDayOfYear() / 7)),
          "WW"),
      new NumberFormatPattern(
          ChronoUnitEnum.WEEKS_IN_MONTH,
          1,
          5,
          1,
          dt -> Integer.toString((int) Math.ceil((double) dt.getDayOfMonth() / 7)),
          "W"),
      new NumberFormatPattern(
          ChronoUnitEnum.CENTURIES,
          0,
          Integer.MAX_VALUE,
          2,
          dt -> {
            if (dt.get(ChronoField.ERA) == 0) {
              return String.format(Locale.ROOT, "-%02d", Math.abs(dt.getYear() / 100 - 1));
            } else {
              return String.format(Locale.ROOT, "%02d", dt.getYear() / 100 + 1);
            }
          },
          "CC"),
      new NumberFormatPattern(
          ChronoUnitEnum.DAYS_JULIAN,
          0,
          Integer.MAX_VALUE,
          1,
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
          ChronoUnitEnum.MONTHS_IN_YEAR,
          1,
          4,
          1,
          dt -> Integer.toString(dt.get(IsoFields.QUARTER_OF_YEAR)),
          "Q"),
      new RomanNumeralMonthFormatPattern(true, "RM"),
      new RomanNumeralMonthFormatPattern(false, "rm"),
      new TimeZoneHoursFormatPattern(),
      new TimeZoneMinutesFormatPattern(),
      new StringFormatPattern(ChronoUnitEnum.TIMEZONE_MINUTES, "TZ") {
        @Override protected int parseValue(ParsePosition inputPosition, String input,
            Locale locale, boolean haveFillMode, boolean enforceLength) throws ParseException {
          throw new ParseException("TZ pattern is not supported in parsing datetime values",
              inputPosition.getIndex());
        }

        @Override protected String dateTimeToString(ZonedDateTime dateTime, boolean haveFillMode,
            @Nullable String suffix, Locale locale) {
          return String.format(
              locale,
              "%3s",
              dateTime.getZone().getDisplayName(TextStyle.SHORT, locale).toUpperCase(locale));
        }
      },
      new StringFormatPattern(ChronoUnitEnum.TIMEZONE_MINUTES, "tz") {
        @Override protected int parseValue(ParsePosition inputPosition, String input, Locale locale,
            boolean haveFillMode, boolean enforceLength) throws ParseException {
          throw new ParseException("tz pattern is not supported in parsing datetime values",
              inputPosition.getIndex());
        }

        @Override protected String dateTimeToString(ZonedDateTime dateTime, boolean haveFillMode,
            @Nullable String suffix, Locale locale) {
          return String.format(
              locale,
              "%3s",
              dateTime.getZone().getDisplayName(TextStyle.SHORT, locale).toLowerCase(locale));
        }
      },
      new StringFormatPattern(ChronoUnitEnum.TIMEZONE_MINUTES, "OF") {
        @Override protected int parseValue(ParsePosition inputPosition, String input, Locale locale,
            boolean haveFillMode, boolean enforceLength) throws ParseException {
          throw new ParseException("OF pattern is not supported in parsing datetime values",
              inputPosition.getIndex());
        }

        @Override protected String dateTimeToString(ZonedDateTime dateTime, boolean haveFillMode,
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

  public static ZonedDateTime toTimestamp(String input, String formatString, ZoneId zoneId)
      throws Exception {
    final ParsePosition inputParsePosition = new ParsePosition(0);
    final ParsePosition formatParsePosition = new ParsePosition(0);
    final Map<ChronoUnitEnum, Long> dateTimeParts = new HashMap<>();

    while (inputParsePosition.getIndex() < input.length()
        && formatParsePosition.getIndex() < formatString.length()) {
      if (input.charAt(inputParsePosition.getIndex()) == ' '
          && formatString.charAt(formatParsePosition.getIndex()) == ' ') {
        inputParsePosition.setIndex(inputParsePosition.getIndex() + 1);
        formatParsePosition.setIndex(formatParsePosition.getIndex() + 1);
        continue;
      } else if (input.charAt(inputParsePosition.getIndex()) == ' ') {
        inputParsePosition.setIndex(inputParsePosition.getIndex() + 1);
        continue;
      } else if (formatString.charAt(formatParsePosition.getIndex()) == ' ') {
        formatParsePosition.setIndex(formatParsePosition.getIndex() + 1);
        continue;
      }

      long parsedValue = 0L;
      FormatPattern matchedPattern = null;

      for (FormatPattern formatPattern : FORMAT_PATTERNS) {
        int matchedPatternLength =
            formatPattern.matchedPatternLength(formatString, formatParsePosition);
        if (matchedPatternLength > 0) {
          final FormatPattern nextPattern =
              getNextPattern(formatString, formatParsePosition.getIndex() + matchedPatternLength);

          final boolean enforceLength = nextPattern != null && formatPattern.isNumeric()
              && nextPattern.isNumeric();

          parsedValue =
              formatPattern.parse(inputParsePosition, input, formatParsePosition, formatString,
                  enforceLength);
          matchedPattern = formatPattern;
          break;
        }
      }

      if (matchedPattern == null) {
        if (Character.isLetter(formatString.charAt(formatParsePosition.getIndex()))) {
          throw new IllegalArgumentException();
        } else {
          inputParsePosition.setIndex(inputParsePosition.getIndex() + 1);
          formatParsePosition.setIndex(formatParsePosition.getIndex() + 1);
        }
      } else {
        final Set<ChronoUnitEnum> units = dateTimeParts.keySet();
        if (!matchedPattern.getChronoUnit().isCompatible(units)) {
          throw new IllegalArgumentException();
        }

        dateTimeParts.put(matchedPattern.getChronoUnit(), parsedValue);
      }
    }

    return constructDateTimeFromParts(dateTimeParts, zoneId);
  }

  private static @Nullable FormatPattern getNextPattern(String formatString,
      int formatPosition) {
    String formatTrimmed = formatString.substring(formatPosition);
    for (String prefix : new String[] {"FM", "TM"}) {
      if (formatTrimmed.startsWith(prefix)) {
        formatTrimmed = formatString.substring(prefix.length());
      }
    }

    for (FormatPattern pattern : FORMAT_PATTERNS) {
      for (String patternString : pattern.getPatterns()) {
        if (formatTrimmed.startsWith(patternString)) {
          return pattern;
        }
      }
    }

    return null;
  }

  private static ZonedDateTime constructDateTimeFromParts(Map<ChronoUnitEnum, Long> dateParts,
      ZoneId zoneId) {
    LocalDateTime constructedDateTime = LocalDateTime.now(ZoneId.systemDefault())
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
      final Long julianDays = dateParts.get(ChronoUnitEnum.DAYS_JULIAN);
      if (julianDays != null) {
        constructedDateTime = constructedDateTime.with(JulianFields.JULIAN_DAY, julianDays);
      }
      break;
    }

    constructedDateTime = updateWithTimeFields(constructedDateTime, dateParts);

    if (dateParts.containsKey(ChronoUnitEnum.TIMEZONE_HOURS)
        || dateParts.containsKey(ChronoUnitEnum.TIMEZONE_MINUTES)) {
      final int hours = dateParts.getOrDefault(ChronoUnitEnum.TIMEZONE_HOURS, 0L)
          .intValue();
      final int minutes = dateParts.getOrDefault(ChronoUnitEnum.TIMEZONE_MINUTES, 0L)
          .intValue();

      return ZonedDateTime.of(constructedDateTime, ZoneOffset.ofHoursMinutes(hours, minutes))
          .withZoneSameInstant(zoneId);
    }

    return ZonedDateTime.of(constructedDateTime, zoneId);
  }

  private static LocalDateTime updateWithGregorianFields(LocalDateTime dateTime,
      Map<ChronoUnitEnum, Long> dateParts) {
    LocalDateTime updatedDateTime = dateTime.withYear(getGregorianYear(dateParts)).withDayOfYear(1);

    if (dateParts.containsKey(ChronoUnitEnum.MONTHS_IN_YEAR)) {
      updatedDateTime =
          updatedDateTime.withMonth(dateParts.get(ChronoUnitEnum.MONTHS_IN_YEAR).intValue());
    }

    if (dateParts.containsKey(ChronoUnitEnum.DAYS_IN_MONTH)) {
      updatedDateTime =
          updatedDateTime.withDayOfMonth(dateParts.get(ChronoUnitEnum.DAYS_IN_MONTH).intValue());
    }

    if (dateParts.containsKey(ChronoUnitEnum.WEEKS_IN_MONTH)) {
      updatedDateTime =
          updatedDateTime.withDayOfMonth(
              dateParts.get(ChronoUnitEnum.WEEKS_IN_MONTH).intValue() * 7 - 6);
    }

    if (dateParts.containsKey(ChronoUnitEnum.WEEKS_IN_YEAR)) {
      updatedDateTime =
          updatedDateTime.withDayOfYear(
              dateParts.get(ChronoUnitEnum.WEEKS_IN_YEAR).intValue() * 7 - 6);
    }

    if (dateParts.containsKey(ChronoUnitEnum.DAYS_IN_YEAR)) {
      updatedDateTime =
          updatedDateTime.withDayOfYear(dateParts.get(ChronoUnitEnum.DAYS_IN_YEAR).intValue());
    }

    return updatedDateTime;
  }

  private static int getGregorianYear(Map<ChronoUnitEnum, Long> dateParts) {
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
      Map<ChronoUnitEnum, Long> dateParts) {
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

  private static int getIso8601Year(Map<ChronoUnitEnum, Long> dateParts) {
    int year =
        getYear(
        dateParts.get(ChronoUnitEnum.ERAS),
        dateParts.get(ChronoUnitEnum.YEARS_ISO_8601),
        dateParts.get(ChronoUnitEnum.CENTURIES),
        dateParts.get(ChronoUnitEnum.YEARS_IN_MILLENIA_ISO_8601),
        dateParts.get(ChronoUnitEnum.YEARS_IN_CENTURY_ISO_8601));
    return year == 0 ? 1 : year;
  }

  private static int getYear(@Nullable Long era, @Nullable Long years,
      @Nullable Long centuries, @Nullable Long yearsInMillenia,
      @Nullable Long yearsInCentury) {
    int yearSign = 1;
    if (era != null) {
      if (era == 0) {
        yearSign = -1;
      }
    }

    if (yearsInMillenia != null) {
      int year = yearsInMillenia.intValue();
      if (year < 520) {
        year += 2000;
      } else {
        year += 1000;
      }

      return yearSign * year;
    }

    if (centuries != null) {
      int year = 100 * (centuries.intValue() - 1);

      if (yearsInCentury != null) {
        year += yearsInCentury.intValue();
      } else {
        year += 1;
      }

      return yearSign * year;
    }

    if (years != null) {
      return yearSign * years.intValue();
    }

    if (yearsInCentury != null) {
      int year = yearsInCentury.intValue();
      if (year < 70) {
        year += 2000;
      } else {
        year += 1900;
      }

      return yearSign * year;
    }

    return yearSign;
  }

  private static LocalDateTime updateWithTimeFields(LocalDateTime dateTime,
      Map<ChronoUnitEnum, Long> dateParts) {
    LocalDateTime updatedDateTime = dateTime;

    if (dateParts.containsKey(ChronoUnitEnum.HOURS_IN_DAY)) {
      updatedDateTime =
          updatedDateTime.withHour(dateParts.get(ChronoUnitEnum.HOURS_IN_DAY).intValue());
    }

    if (dateParts.containsKey(ChronoUnitEnum.HALF_DAYS)
        && dateParts.containsKey(ChronoUnitEnum.HOURS_IN_HALF_DAY)) {
      updatedDateTime =
          updatedDateTime.withHour(dateParts.get(ChronoUnitEnum.HALF_DAYS).intValue() * 12
              + dateParts.get(ChronoUnitEnum.HOURS_IN_HALF_DAY).intValue());
    } else if (dateParts.containsKey(ChronoUnitEnum.HOURS_IN_HALF_DAY)) {
      updatedDateTime =
          updatedDateTime.withHour(dateParts.get(ChronoUnitEnum.HOURS_IN_HALF_DAY).intValue());
    }

    if (dateParts.containsKey(ChronoUnitEnum.MINUTES_IN_HOUR)) {
      updatedDateTime =
          updatedDateTime.withMinute(dateParts.get(ChronoUnitEnum.MINUTES_IN_HOUR).intValue());
    }

    if (dateParts.containsKey(ChronoUnitEnum.SECONDS_IN_DAY)) {
      updatedDateTime =
          updatedDateTime.with(ChronoField.SECOND_OF_DAY,
              dateParts.get(ChronoUnitEnum.SECONDS_IN_DAY));
    }

    if (dateParts.containsKey(ChronoUnitEnum.SECONDS_IN_MINUTE)) {
      updatedDateTime =
          updatedDateTime.withSecond(dateParts.get(ChronoUnitEnum.SECONDS_IN_MINUTE).intValue());
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
              100 * dateParts.get(ChronoUnitEnum.TENTHS_OF_SECOND));
    }

    if (dateParts.containsKey(ChronoUnitEnum.HUNDREDTHS_OF_SECOND)) {
      updatedDateTime =
          updatedDateTime.with(ChronoField.MILLI_OF_SECOND,
              10 * dateParts.get(ChronoUnitEnum.HUNDREDTHS_OF_SECOND));
    }

    if (dateParts.containsKey(ChronoUnitEnum.THOUSANDTHS_OF_SECOND)) {
      updatedDateTime =
          updatedDateTime.with(ChronoField.MILLI_OF_SECOND,
              dateParts.get(ChronoUnitEnum.THOUSANDTHS_OF_SECOND));
    }

    if (dateParts.containsKey(ChronoUnitEnum.TENTHS_OF_MS)) {
      updatedDateTime =
          updatedDateTime.with(ChronoField.MICRO_OF_SECOND,
              100 * dateParts.get(ChronoUnitEnum.TENTHS_OF_MS));
    }

    if (dateParts.containsKey(ChronoUnitEnum.HUNDREDTHS_OF_MS)) {
      updatedDateTime =
          updatedDateTime.with(ChronoField.MICRO_OF_SECOND,
              10 * dateParts.get(ChronoUnitEnum.HUNDREDTHS_OF_MS));
    }

    if (dateParts.containsKey(ChronoUnitEnum.THOUSANDTHS_OF_MS)) {
      updatedDateTime =
          updatedDateTime.with(ChronoField.MICRO_OF_SECOND,
              dateParts.get(ChronoUnitEnum.THOUSANDTHS_OF_MS));
    }

    return updatedDateTime;
  }
}
