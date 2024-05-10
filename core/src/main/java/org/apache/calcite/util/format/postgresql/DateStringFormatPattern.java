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
    T getValueFromDateTime(ZonedDateTime dateTime);

    String getDisplayName(T value, TextStyle textStyle, boolean haveFillMode, Locale locale);
  }

  /**
   * Can convert between a day of week name and the corresponding datetime component value.
   */
  private static class DayOfWeekConverter implements DateStringConverter<DayOfWeek> {
    @Override public DayOfWeek getValueFromDateTime(ZonedDateTime dateTime) {
      return dateTime.getDayOfWeek();
    }

    @Override public String getDisplayName(DayOfWeek value, TextStyle textStyle,
        boolean haveFillMode, Locale locale) {
      final String formattedValue = value.getDisplayName(textStyle, locale);

      if (!haveFillMode && textStyle == TextStyle.FULL) {
        return String.format(locale, "%-9s", formattedValue);
      } else {
        return formattedValue;
      }
    }
  }

  /**
   * Can convert between a month name and the corresponding datetime component value.
   */
  private static class MonthConverter implements DateStringConverter<Month> {
    @Override public Month getValueFromDateTime(ZonedDateTime dateTime) {
      return dateTime.getMonth();
    }

    @Override public String getDisplayName(Month value, TextStyle textStyle, boolean haveFillMode,
        Locale locale) {
      final String formattedValue = value.getDisplayName(textStyle, locale);

      if (!haveFillMode && textStyle == TextStyle.FULL) {
        return String.format(locale, "%-9s", formattedValue);
      } else {
        return formattedValue;
      }
    }
  }

  private static final DateStringConverter<DayOfWeek> DAY_OF_WEEK = new DayOfWeekConverter();
  private static final DateStringConverter<Month> MONTH = new MonthConverter();

  private final DateStringConverter<T> dateStringConverter;
  private final CapitalizationEnum capitalization;
  private final TextStyle textStyle;

  private DateStringFormatPattern(DateStringConverter<T> dateStringConverter,
      TextStyle textStyle, CapitalizationEnum capitalization, String... patterns) {
    super(patterns);
    this.dateStringConverter = dateStringConverter;
    this.capitalization = capitalization;
    this.textStyle = textStyle;
  }

  public static DateStringFormatPattern<DayOfWeek> forDayOfWeek(TextStyle textStyle,
      CapitalizationEnum capitalization, String... patterns) {
    return new DateStringFormatPattern<>(
        DAY_OF_WEEK,
        textStyle,
        capitalization,
        patterns);
  }

  public static DateStringFormatPattern<Month> forMonth(TextStyle textStyle,
      CapitalizationEnum capitalization, String... patterns) {
    return new DateStringFormatPattern<>(
        MONTH,
        textStyle,
        capitalization,
        patterns);
  }

  @Override public String dateTimeToString(ZonedDateTime dateTime, boolean haveFillMode,
      @Nullable String suffix, Locale locale) {
    return capitalization.apply(
        dateStringConverter.getDisplayName(
            dateStringConverter.getValueFromDateTime(dateTime),
            textStyle,
            haveFillMode,
            locale), locale);
  }
}
