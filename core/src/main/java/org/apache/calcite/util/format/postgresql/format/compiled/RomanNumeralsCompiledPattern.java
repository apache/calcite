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

/**
 * The date/time format compiled component for the roman numeral representation of a
 * month.
 */
public class RomanNumeralsCompiledPattern extends CompiledPattern {
  private final boolean upperCase;

  public RomanNumeralsCompiledPattern(Set<PatternModifier> modifiers, boolean upperCase) {
    super(ChronoUnitEnum.MONTHS_IN_YEAR, modifiers);
    this.upperCase = upperCase;
  }

  @Override public String convertToString(ZonedDateTime dateTime, Locale locale) {
    final String romanNumeral;

    switch (dateTime.getMonth().getValue()) {
    case 1:
      romanNumeral = "I";
      break;
    case 2:
      romanNumeral = "II";
      break;
    case 3:
      romanNumeral = "III";
      break;
    case 4:
      romanNumeral = "IV";
      break;
    case 5:
      romanNumeral = "V";
      break;
    case 6:
      romanNumeral = "VI";
      break;
    case 7:
      romanNumeral = "VII";
      break;
    case 8:
      romanNumeral = "VIII";
      break;
    case 9:
      romanNumeral = "IX";
      break;
    case 10:
      romanNumeral = "X";
      break;
    case 11:
      romanNumeral = "XI";
      break;
    default:
      romanNumeral = "XII";
      break;
    }

    if (upperCase) {
      return romanNumeral;
    } else {
      return romanNumeral.toLowerCase(Locale.US);
    }
  }

  @Override public int parseValue(ParsePosition inputPosition, String input, boolean enforceLength,
      Locale locale) throws ParseException {
    final String inputTrimmed = input.substring(inputPosition.getIndex());

    if (inputTrimmed.startsWith(upperCase ? "III" : "iii")) {
      inputPosition.setIndex(inputPosition.getIndex() + 3);
      return 3;
    } else if (inputTrimmed.startsWith(upperCase ? "II" : "ii")) {
      inputPosition.setIndex(inputPosition.getIndex() + 2);
      return 2;
    } else if (inputTrimmed.startsWith(upperCase ? "IV" : "iv")) {
      inputPosition.setIndex(inputPosition.getIndex() + 2);
      return 4;
    } else if (inputTrimmed.startsWith(upperCase ? "IX" : "ix")) {
      inputPosition.setIndex(inputPosition.getIndex() + 2);
      return 9;
    } else if (inputTrimmed.startsWith(upperCase ? "I" : "i")) {
      inputPosition.setIndex(inputPosition.getIndex() + 1);
      return 1;
    } else if (inputTrimmed.startsWith(upperCase ? "VIII" : "viii")) {
      inputPosition.setIndex(inputPosition.getIndex() + 4);
      return 8;
    } else if (inputTrimmed.startsWith(upperCase ? "VII" : "vii")) {
      inputPosition.setIndex(inputPosition.getIndex() + 3);
      return 7;
    } else if (inputTrimmed.startsWith(upperCase ? "VI" : "vi")) {
      inputPosition.setIndex(inputPosition.getIndex() + 2);
      return 6;
    } else if (inputTrimmed.startsWith(upperCase ? "V" : "v")) {
      inputPosition.setIndex(inputPosition.getIndex() + 1);
      return 5;
    } else if (inputTrimmed.startsWith(upperCase ? "XII" : "xii")) {
      inputPosition.setIndex(inputPosition.getIndex() + 3);
      return 12;
    } else if (inputTrimmed.startsWith(upperCase ? "XI" : "xi")) {
      inputPosition.setIndex(inputPosition.getIndex() + 2);
      return 11;
    } else if (inputTrimmed.startsWith(upperCase ? "X" : "x")) {
      inputPosition.setIndex(inputPosition.getIndex() + 1);
      return 10;
    }

    throw new ParseException("Unable to parse value", inputPosition.getIndex());
  }

  @Override protected int getBaseFormatPatternLength() {
    return 2;
  }
}
