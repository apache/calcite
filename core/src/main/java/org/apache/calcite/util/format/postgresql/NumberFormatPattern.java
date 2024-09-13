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
import java.time.ZonedDateTime;
import java.util.Locale;
import java.util.function.Function;

import static java.lang.Integer.parseInt;

/**
 * A format element that will produce a number. Numbers can have leading zeroes
 * removed and can have ordinal suffixes.
 */
public class NumberFormatPattern extends FormatPattern {
  private final ChronoUnitEnum chronoUnit;
  private final long minValue;
  private final long maxValue;
  private final int preferredLength;
  private final Function<ZonedDateTime, String> converter;
  private final @Nullable Function<Integer, Integer> valueAdjuster;

  /**
   * Constructs a new NumberFormatPattern for the provided values.
   *
   * @param chronoUnit ChronoUnitEnum value that this pattern parses
   * @param minValue minimum allowed value
   * @param maxValue maximum allowed value
   * @param preferredLength the number input characters that would normally be consumed by this
   *                        pattern. For example YYYY would normally consume 4 characters, but
   *                        can actually consume more or less.
   * @param converter a Function that will extract the value from a datetime and format it
   * @param patterns array of pattern strings
   */
  public NumberFormatPattern(ChronoUnitEnum chronoUnit, long minValue, long maxValue,
      int preferredLength, Function<ZonedDateTime, String> converter, String... patterns) {
    super(patterns);
    this.chronoUnit = chronoUnit;
    this.converter = converter;
    this.valueAdjuster = null;
    this.minValue = minValue;
    this.maxValue = maxValue;
    this.preferredLength = preferredLength;
  }

  /**
   * Constructs a new NumberFormatPattern for the provided values.
   *
   * @param chronoUnit ChronoUnitEnum value that this pattern parses
   * @param minValue minimum allowed value
   * @param maxValue maximum allowed value
   * @param preferredLength the number input characters that would normally be consumed by this
   *                        pattern. For example YYYY would normally consume 4 characters, but
   *                        can actually consume more or less.
   * @param converter a Function that will extract the value from a datetime and format it
   * @param valueAdjuster a Function that can convert the extracted value to the expected
   *                      datetime value.
   * @param patterns array of pattern strings
   */
  protected NumberFormatPattern(ChronoUnitEnum chronoUnit, int minValue,
      int maxValue, int preferredLength, Function<ZonedDateTime, String> converter,
      Function<Integer, Integer> valueAdjuster, String... patterns) {
    super(patterns);
    this.chronoUnit = chronoUnit;
    this.converter = converter;
    this.valueAdjuster = valueAdjuster;
    this.minValue = minValue;
    this.maxValue = maxValue;
    this.preferredLength = preferredLength;
  }

  @Override public @Nullable String convert(ParsePosition parsePosition, String formatString,
      ZonedDateTime dateTime, Locale locale) {
    String formatStringTrimmed = formatString.substring(parsePosition.getIndex());

    boolean haveFillMode = false;
    boolean haveTranslationMode = false;
    if (formatStringTrimmed.startsWith("FMTM") || formatStringTrimmed.startsWith("TMFM")) {
      haveFillMode = true;
      haveTranslationMode = true;
      formatStringTrimmed = formatStringTrimmed.substring(4);
    } else if (formatStringTrimmed.startsWith("FM")) {
      haveFillMode = true;
      formatStringTrimmed = formatStringTrimmed.substring(2);
    } else if (formatStringTrimmed.startsWith("TM")) {
      haveTranslationMode = true;
      formatStringTrimmed = formatStringTrimmed.substring(2);
    }

    String patternToUse = null;
    for (String pattern : getPatterns()) {
      if (formatStringTrimmed.startsWith(pattern)) {
        patternToUse = pattern;
        break;
      }
    }

    if (patternToUse == null) {
      return null;
    }

    parsePosition.setIndex(parsePosition.getIndex() + patternToUse.length()
        + (haveFillMode ? 2 : 0) + (haveTranslationMode ? 2 : 0));

    formatStringTrimmed = formatString.substring(parsePosition.getIndex());

    String ordinalSuffix = null;
    if (formatStringTrimmed.startsWith("TH")) {
      ordinalSuffix = "TH";
      parsePosition.setIndex(parsePosition.getIndex() + 2);
    } else if (formatStringTrimmed.startsWith("th")) {
      ordinalSuffix = "th";
      parsePosition.setIndex(parsePosition.getIndex() + 2);
    }

    String formattedValue = converter.apply(dateTime);
    if (haveFillMode) {
      formattedValue = trimLeadingZeros(formattedValue);
    }

    if (ordinalSuffix != null) {
      String suffix;

      if (formattedValue.length() >= 2
          && formattedValue.charAt(formattedValue.length() - 2) == '1') {
        suffix = "th";
      } else {
        switch (formattedValue.charAt(formattedValue.length() - 1)) {
        case '1':
          suffix = "st";
          break;
        case '2':
          suffix = "nd";
          break;
        case '3':
          suffix = "rd";
          break;
        default:
          suffix = "th";
          break;
        }
      }

      if ("th".equals(ordinalSuffix)) {
        suffix = suffix.toLowerCase(Locale.ROOT);
      } else {
        suffix = suffix.toUpperCase(Locale.ROOT);
      }

      formattedValue += suffix;
      parsePosition.setIndex(parsePosition.getIndex() + 2);
    }

    return formattedValue;
  }

  /**
   * Removes leading zeros from string, while preserving the negative sign if present.
   *
   * @param value String to remove leading zeros from
   * @return input string without leading zeros
   */
  protected String trimLeadingZeros(String value) {
    if (value.isEmpty()) {
      return value;
    }

    boolean isNegative = value.charAt(0) == '-';
    int offset = isNegative ? 1 : 0;
    boolean trimmed = false;
    for (; offset < value.length() - 1; offset++) {
      if (value.charAt(offset) != '0') {
        break;
      }

      trimmed = true;
    }

    if (trimmed) {
      return isNegative ? "-" + value.substring(offset) : value.substring(offset);
    } else {
      return value;
    }
  }

  @Override public ChronoUnitEnum getChronoUnit() {
    return chronoUnit;
  }

  @Override protected int parseValue(final ParsePosition inputPosition, final String input,
      Locale locale, boolean haveFillMode, boolean enforceLength) throws ParseException {
    int endIndex = inputPosition.getIndex();
    for (; endIndex < input.length(); endIndex++) {
      if (input.charAt(endIndex) < '0' || input.charAt(endIndex) > '9') {
        break;
      } else if (enforceLength && endIndex == inputPosition.getIndex() + preferredLength) {
        break;
      }
    }

    if (endIndex == inputPosition.getIndex()) {
      throw new ParseException("Unable to parse value", inputPosition.getIndex());
    }

    int value = parseInt(input.substring(inputPosition.getIndex(), endIndex));
    if (value < minValue || value > maxValue) {
      throw new ParseException("Parsed value outside of valid range", inputPosition.getIndex());
    }

    if (valueAdjuster != null) {
      value = valueAdjuster.apply(value);
    }

    inputPosition.setIndex(endIndex);
    return value;
  }

  @Override protected boolean isNumeric() {
    return true;
  }
}
