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
import java.time.temporal.ChronoField;
import java.util.Locale;
import java.util.Set;

/**
 * The date/time format compiled component for AM/PM (first or second half of day).
 */
public class AmPmCompiledPattern extends CompiledPattern {
  private final boolean upperCase;
  private final boolean includePeriods;

  public AmPmCompiledPattern(Set<PatternModifier> modifiers, boolean upperCase,
      boolean includePeriods) {
    super(ChronoUnitEnum.HALF_DAYS, modifiers);
    this.upperCase = upperCase;
    this.includePeriods = includePeriods;
  }

  @Override public String convertToString(ZonedDateTime dateTime, Locale locale) {
    final int intValue = dateTime.get(ChronoField.AMPM_OF_DAY);
    final String stringValue;
    if (intValue == 0) {
      stringValue = includePeriods ? "a.m." : "am";
    } else {
      stringValue = includePeriods ? "p.m." : "pm";
    }

    if (upperCase) {
      return stringValue.toUpperCase(Locale.ROOT);
    }

    return stringValue;
  }

  @Override public int parseValue(ParsePosition inputPosition, String input, boolean enforceLength,
      Locale locale) throws ParseException {
    String amValue = includePeriods ? "a.m." : "am";
    String pmValue = includePeriods ? "p.m." : "pm";
    if (upperCase) {
      amValue = amValue.toUpperCase(Locale.ROOT);
      pmValue = pmValue.toUpperCase(Locale.ROOT);
    }

    final String inputTrimmed = input.substring(inputPosition.getIndex());
    if (inputTrimmed.startsWith(amValue)) {
      inputPosition.setIndex(inputPosition.getIndex() + amValue.length());
      return 0;
    } else if (inputTrimmed.startsWith(pmValue)) {
      inputPosition.setIndex(inputPosition.getIndex() + pmValue.length());
      return 1;
    }

    throw new ParseException("Unable to parse value", inputPosition.getIndex());
  }

  @Override protected int getBaseFormatPatternLength() {
    return includePeriods ? 4 : 2;
  }
}
