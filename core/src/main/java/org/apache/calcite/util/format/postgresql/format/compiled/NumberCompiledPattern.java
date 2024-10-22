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
package org.apache.calcite.util.format.postgresql.format.compiled;

import org.apache.calcite.util.format.postgresql.ChronoUnitEnum;
import org.apache.calcite.util.format.postgresql.PatternModifier;

import java.text.ParseException;
import java.text.ParsePosition;
import java.time.ZonedDateTime;
import java.util.Locale;
import java.util.Set;
import java.util.function.Function;

import static java.lang.Integer.parseInt;

/**
 * A date/time format compiled component that will parse a sequence of digits into
 * a value (such as "DD").
 */
public class NumberCompiledPattern extends CompiledPattern {
  private final Function<ZonedDateTime, Integer> dateTimeToIntConverter;
  private final Function<Integer, Integer> valueAdjuster;
  private final int minCharacters;
  private final int maxCharacters;
  private final int minValue;
  private final int maxValue;
  private final String pattern;

  public NumberCompiledPattern(ChronoUnitEnum chronoUnit,
      Function<ZonedDateTime, Integer> dateTimeToIntConverter,
      Function<Integer, Integer> valueAdjuster, int minCharacters, int maxCharacters, int minValue,
      int maxValue, String pattern, Set<PatternModifier> modifiers) {
    super(chronoUnit, modifiers);
    this.dateTimeToIntConverter = dateTimeToIntConverter;
    this.valueAdjuster = valueAdjuster;
    this.minCharacters = minCharacters;
    this.maxCharacters = maxCharacters;
    this.minValue = minValue;
    this.maxValue = maxValue;
    this.pattern = pattern;
  }

  @Override public String convertToString(ZonedDateTime dateTime, Locale locale) {
    final long intValue = dateTimeToIntConverter.apply(dateTime);
    final String signPrefix = intValue < 0 ? "-" : "";
    String stringValue;
    if (!modifiers.contains(PatternModifier.FM) && minCharacters > 0) {
      stringValue =
          String.format(Locale.ROOT, signPrefix + "%0" + minCharacters + "d", Math.abs(intValue));
    } else {
      stringValue = Long.toString(intValue);
    }

    if (maxCharacters > 0 && stringValue.length() - signPrefix.length() > maxCharacters) {
      stringValue = stringValue.substring(stringValue.length() - maxCharacters);
    }

    if (modifiers.contains(PatternModifier.TH_UPPER)) {
      return stringValue + getOrdinalSuffix(stringValue).toUpperCase(Locale.ROOT);
    } else if (modifiers.contains(PatternModifier.TH_LOWER)) {
      return stringValue + getOrdinalSuffix(stringValue);
    }

    return stringValue;
  }

  private String getOrdinalSuffix(String stringValue) {
    // 10 through 19 have a th suffix
    if (stringValue.length() >= 2 && stringValue.charAt(stringValue.length() - 2) == '1') {
      return "th";
    }

    switch (stringValue.charAt(stringValue.length() - 1)) {
    case '1':
      return "st";
    case '2':
      return "nd";
    case '3':
      return "rd";
    default:
      return "th";
    }
  }

  @Override public int parseValue(ParsePosition inputPosition, String input, boolean enforceLength,
      Locale locale) throws ParseException {
    int endIndex = inputPosition.getIndex();
    for (; endIndex < input.length(); endIndex++) {
      if (input.charAt(endIndex) < '0' || input.charAt(endIndex) > '9') {
        break;
      } else if (enforceLength && endIndex == inputPosition.getIndex() + minCharacters) {
        break;
      }
    }

    if (modifiers.contains(PatternModifier.TH_UPPER)
        || modifiers.contains(PatternModifier.TH_LOWER)) {
      if (endIndex < input.length() - 1) {
        endIndex += 2;
      } else if (endIndex < input.length()) {
        endIndex++;
      }
    }

    if (endIndex == inputPosition.getIndex()) {
      throw new ParseException("Unable to parse value", inputPosition.getIndex());
    }

    int value = parseInt(input.substring(inputPosition.getIndex(), endIndex));
    if (value < minValue || value > maxValue) {
      throw new ParseException("Parsed value outside of valid range", inputPosition.getIndex());
    }

    value = valueAdjuster.apply(value);

    inputPosition.setIndex(endIndex);
    return value;
  }

  @Override protected int getBaseFormatPatternLength() {
    return pattern.length();
  }

  @Override public boolean isNumeric() {
    return true;
  }
}
