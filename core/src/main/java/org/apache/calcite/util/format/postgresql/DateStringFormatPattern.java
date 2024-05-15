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
import java.time.DayOfWeek;
import java.time.Month;
import java.time.ZonedDateTime;
import java.time.format.TextStyle;
import java.util.Locale;

/**
 * Converts a non numeric value from a string to a datetime component and can generate
 * a string representation of of a datetime component from a datetime. An example is
 * converting to and from month names.
 *
 * @param <T> a type used by <code>java.time</code> to represent a datetime component
 *           that has a string representation
 */
public class DateStringFormatPattern<T> extends StringFormatPattern {
  /**
   * Provides an abstraction over datetime components that have string representations.
   *
   * @param <T> a type used by <code>java.time</code> to represent a datetime component
   *           that has a string representation
   */
  private interface DateStringConverter<T> {
    /**
     * Get the ChronoUnitEnum value that this converter handles.
     *
     * @return a ChronoUnitEnum value
     */
    ChronoUnitEnum getChronoUnit();

    /**
     * Extract the value of this datetime component from the provided value.
     *
     * @param dateTime where to extract the value from
     * @return the extracted value
     */
    T getValueFromDateTime(ZonedDateTime dateTime);

    /**
     * An array of the possible string values.
     *
     * @return array of possible string values
     */
    T[] values();

    /**
     * Generate the string representation of a value. This may involve formatting as well
     * as translating to the provided locale.
     *
     * @param value value to convert
     * @param textStyle how to format the value
     * @param haveFillMode if false add padding spaces to the correct length
     * @param locale locale to translate to
     * @return converted string value
     */
    String getDisplayName(T value, TextStyle textStyle, boolean haveFillMode, Locale locale);

    /**
     * Get the int value for the provided value (such as a month).
     *
     * @param value value to convert to an int
     * @return result of converting the value to an int
     */
    int getValue(T value);
  }

  /**
   * Can convert between a day of week name and the corresponding datetime component value.
   */
  private static class DayOfWeekConverter implements DateStringConverter<DayOfWeek> {
    @Override public ChronoUnitEnum getChronoUnit() {
      return ChronoUnitEnum.DAYS_IN_WEEK;
    }

    @Override public DayOfWeek getValueFromDateTime(ZonedDateTime dateTime) {
      return dateTime.getDayOfWeek();
    }

    @Override public DayOfWeek[] values() {
      return DayOfWeek.values();
    }

    @Override public String getDisplayName(DayOfWeek value, TextStyle textStyle,
        boolean haveFillMode, Locale locale) {
      final String formattedValue = value.getDisplayName(textStyle, locale);

      if (!haveFillMode && textStyle == TextStyle.FULL) {
        // Pad the day name to 9 characters
        // See the description for DAY, Day or day in the PostgreSQL documentation for TO_CHAR
        return String.format(locale, "%-9s", formattedValue);
      } else {
        return formattedValue;
      }
    }

    @Override public int getValue(DayOfWeek value) {
      return value.getValue();
    }
  }

  /**
   * Can convert between a month name and the corresponding datetime component value.
   */
  private static class MonthConverter implements DateStringConverter<Month> {
    @Override public ChronoUnitEnum getChronoUnit() {
      return ChronoUnitEnum.MONTHS_IN_YEAR;
    }

    @Override public Month getValueFromDateTime(ZonedDateTime dateTime) {
      return dateTime.getMonth();
    }

    @Override public Month[] values() {
      return Month.values();
    }

    @Override public String getDisplayName(Month value, TextStyle textStyle, boolean haveFillMode,
        Locale locale) {
      final String formattedValue = value.getDisplayName(textStyle, locale);

      if (!haveFillMode && textStyle == TextStyle.FULL) {
        // Pad the month name to 9 characters
        // See the description for MONTH, Month or month in the PostgreSQL documentation for
        // TO_CHAR
        return String.format(locale, "%-9s", formattedValue);
      } else {
        return formattedValue;
      }
    }

    @Override public int getValue(Month value) {
      return value.getValue();
    }
  }

  private static final DateStringConverter<DayOfWeek> DAY_OF_WEEK = new DayOfWeekConverter();
  private static final DateStringConverter<Month> MONTH = new MonthConverter();

  private final DateStringConverter<T> dateStringEnum;
  private final CapitalizationEnum capitalization;
  private final TextStyle textStyle;

  private DateStringFormatPattern(
      ChronoUnitEnum chronoUnit, DateStringConverter<T> dateStringEnum,
      TextStyle textStyle, CapitalizationEnum capitalization, String... patterns) {
    super(chronoUnit, patterns);
    this.dateStringEnum = dateStringEnum;
    this.capitalization = capitalization;
    this.textStyle = textStyle;
  }

  public static DateStringFormatPattern<DayOfWeek> forDayOfWeek(TextStyle textStyle,
      CapitalizationEnum capitalization, String... patterns) {
    return new DateStringFormatPattern<>(
        DAY_OF_WEEK.getChronoUnit(),
        DAY_OF_WEEK,
        textStyle,
        capitalization,
        patterns);
  }

  public static DateStringFormatPattern<Month> forMonth(TextStyle textStyle,
      CapitalizationEnum capitalization, String... patterns) {
    return new DateStringFormatPattern<>(
        MONTH.getChronoUnit(),
        MONTH,
        textStyle,
        capitalization,
        patterns);
  }

  @Override protected int parseValue(ParsePosition inputPosition, String input,
      Locale locale, boolean haveFillMode, boolean enforceLength) throws ParseException {
    final String inputTrimmed = input.substring(inputPosition.getIndex());

    for (T value : dateStringEnum.values()) {
      final String formattedValue =
          capitalization.apply(dateStringEnum.getDisplayName(value, textStyle, false, locale),
              locale);
      if (inputTrimmed.startsWith(formattedValue.trim())) {
        inputPosition.setIndex(inputPosition.getIndex() + formattedValue.trim().length());
        return dateStringEnum.getValue(value);
      }
    }

    throw new ParseException("Unable to parse value", inputPosition.getIndex());
  }

  @Override public String dateTimeToString(ZonedDateTime dateTime, boolean haveFillMode,
      @Nullable String suffix, Locale locale) {
    return capitalization.apply(
        dateStringEnum.getDisplayName(
            dateStringEnum.getValueFromDateTime(dateTime),
            textStyle,
            haveFillMode,
            locale), locale);
  }
}
