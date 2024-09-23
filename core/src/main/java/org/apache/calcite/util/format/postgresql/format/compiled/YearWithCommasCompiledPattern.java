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

import static java.lang.Integer.parseInt;

/**
 * The date/time format compiled component for the year formatted with a comma after
 * the thousands (such as "2,024").
 */
public class YearWithCommasCompiledPattern extends CompiledPattern {
  public YearWithCommasCompiledPattern(Set<PatternModifier> modifiers) {
    super(ChronoUnitEnum.YEARS, modifiers);
  }

  @Override public String convertToString(ZonedDateTime dateTime, Locale locale) {
    String formattedValue = String.format(Locale.ROOT, "%04d", dateTime.getYear());
    formattedValue = formattedValue.substring(0, formattedValue.length() - 3) + ","
        + formattedValue.substring(formattedValue.length() - 3);

    if (modifiers.contains(PatternModifier.TH_UPPER)
        || modifiers.contains(PatternModifier.TH_LOWER)) {
      String suffix;
      switch (formattedValue.charAt(formattedValue.length() - 1)) {
      case '1':
        suffix = "st";
        break;
      case '2':
        suffix = "nd";
        break;
      case 3:
        suffix = "rd";
        break;
      default:
        suffix = "th";
        break;
      }

      if (modifiers.contains(PatternModifier.TH_UPPER)) {
        return formattedValue + suffix.toUpperCase(Locale.ROOT);
      }

      return formattedValue + suffix;
    }

    return formattedValue;
  }

  @Override public int parseValue(ParsePosition inputPosition, String input, boolean enforceLength,
      Locale locale) throws ParseException {
    final String inputTrimmed = input.substring(inputPosition.getIndex());
    final int commaIndex = inputTrimmed.indexOf(',');

    if (commaIndex <= 0 || commaIndex > 3) {
      throw new ParseException("Unable to parse value", inputPosition.getIndex());
    }

    final String thousands = inputTrimmed.substring(0, commaIndex);
    int endIndex;
    if (enforceLength) {
      if (inputPosition.getIndex() + commaIndex + 4 > input.length()) {
        throw new ParseException("Unable to parse value", inputPosition.getIndex());
      }

      endIndex = commaIndex + 4;
    } else {
      endIndex = commaIndex + 1;
      for (; endIndex < inputTrimmed.length(); endIndex++) {
        if (!Character.isDigit(inputTrimmed.charAt(endIndex))) {
          break;
        }
      }

      if (endIndex == commaIndex + 1 || endIndex > commaIndex + 4) {
        inputPosition.setErrorIndex(inputPosition.getIndex());
        throw new ParseException("Unable to parse value", inputPosition.getIndex());
      }
    }

    final String remainingDigits = inputTrimmed.substring(commaIndex + 1, endIndex);

    if (modifiers.contains(PatternModifier.TH_UPPER)
        || modifiers.contains(PatternModifier.TH_LOWER)) {
      if (endIndex < inputTrimmed.length() - 1) {
        endIndex += 2;
      } else if (endIndex < inputTrimmed.length()) {
        endIndex++;
      }
    }

    inputPosition.setIndex(inputPosition.getIndex() + endIndex);

    return parseInt(thousands) * 1000 + parseInt(remainingDigits);
  }

  @Override protected int getBaseFormatPatternLength() {
    return 5;
  }

  @Override public boolean isNumeric() {
    return true;
  }
}
