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

import org.apache.calcite.util.format.postgresql.CapitalizationEnum;
import org.apache.calcite.util.format.postgresql.ChronoUnitEnum;
import org.apache.calcite.util.format.postgresql.PatternModifier;

import java.text.ParseException;
import java.text.ParsePosition;
import java.time.Month;
import java.time.ZonedDateTime;
import java.time.format.TextStyle;
import java.util.Locale;
import java.util.Set;

/**
 * The date/time format compiled component for a text representation of a month.
 */
public class MonthCompiledPattern extends CompiledPattern {
  private final CapitalizationEnum capitalization;
  private final TextStyle textStyle;

  public MonthCompiledPattern(Set<PatternModifier> modifiers, CapitalizationEnum capitalization,
      TextStyle textStyle) {
    super(ChronoUnitEnum.MONTHS_IN_YEAR, modifiers);
    this.capitalization = capitalization;
    this.textStyle = textStyle;
  }

  @Override public String convertToString(ZonedDateTime dateTime, Locale locale) {
    final Locale localeToUse = modifiers.contains(PatternModifier.TM) ? locale : Locale.US;
    final int intValue = dateTime.getMonthValue();
    final String stringValue =
        capitalization.apply(
            Month.of(intValue).getDisplayName(textStyle, localeToUse), localeToUse);

    if (textStyle == TextStyle.FULL && !modifiers.contains(PatternModifier.FM)
        && !modifiers.contains(PatternModifier.TM)) {
      return String.format(locale, "%-9s", stringValue);
    }

    return stringValue;
  }

  @Override public int parseValue(ParsePosition inputPosition, String input, boolean enforceLength,
      Locale locale) throws ParseException {
    final Locale localeToUse = modifiers.contains(PatternModifier.TM) ? locale : Locale.US;
    final String inputTrimmed = input.substring(inputPosition.getIndex());

    for (Month month : Month.values()) {
      final String expectedValue =
          capitalization.apply(month.getDisplayName(textStyle, localeToUse), localeToUse);
      if (inputTrimmed.startsWith(expectedValue)) {
        inputPosition.setIndex(inputPosition.getIndex() + expectedValue.length());
        return month.getValue();
      }
    }

    throw new ParseException("Unable to parse value", inputPosition.getIndex());
  }

  @Override protected int getBaseFormatPatternLength() {
    return textStyle == TextStyle.FULL ? 5 : 3;
  }
}
