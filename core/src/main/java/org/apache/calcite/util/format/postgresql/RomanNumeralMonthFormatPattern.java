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

import java.time.ZonedDateTime;
import java.util.Locale;

/**
 * Converts a Roman numeral value (between 1 and 12) to a month value and back.
 */
public class RomanNumeralMonthFormatPattern extends StringFormatPattern {
  private final boolean upperCase;

  public RomanNumeralMonthFormatPattern(boolean upperCase, String... patterns) {
    super(patterns);
    this.upperCase = upperCase;
  }

  @Override String dateTimeToString(ZonedDateTime dateTime, boolean haveFillMode,
      @Nullable String suffix, Locale locale) {
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
      return romanNumeral.toLowerCase(locale);
    }
  }
}
