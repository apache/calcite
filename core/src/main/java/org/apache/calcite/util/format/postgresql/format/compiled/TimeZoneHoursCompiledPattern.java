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
 * The date/time format compiled component for the hours of the timezone offset.
 */
public class TimeZoneHoursCompiledPattern extends CompiledPattern {
  public TimeZoneHoursCompiledPattern(Set<PatternModifier> modifiers) {
    super(ChronoUnitEnum.TIMEZONE_HOURS, modifiers);
  }

  @Override public String convertToString(ZonedDateTime dateTime, Locale locale) {
    return String.format(
        Locale.ROOT,
        "%+02d",
        dateTime.getOffset().getTotalSeconds() / 3600);
  }

  @Override public int parseValue(ParsePosition inputPosition, String input, boolean enforceLength,
      Locale locale) throws ParseException {
    int inputOffset = inputPosition.getIndex();
    String inputTrimmed = input.substring(inputOffset);

    boolean isPositive = true;
    if (inputTrimmed.charAt(0) == '-') {
      isPositive = false;
    } else if (inputTrimmed.charAt(0) != '+') {
      throw new ParseException("Unable to parse value", inputPosition.getIndex());
    }

    inputOffset++;
    inputTrimmed = input.substring(inputOffset);

    if (!Character.isDigit(inputTrimmed.charAt(0))) {
      throw new ParseException("Unable to parse value", inputPosition.getIndex());
    }

    int endIndex = inputOffset + 1;
    if (endIndex > input.length() || !Character.isDigit(input.charAt(inputOffset))) {
      throw new ParseException("Unable to parse value", inputPosition.getIndex());
    }

    if (endIndex < input.length() && Character.isDigit(input.charAt(endIndex))) {
      endIndex++;
    }

    int timezoneHours = parseInt(input.substring(inputOffset, endIndex));

    if (timezoneHours > 15) {
      throw new ParseException("Value is outside of valid range", inputPosition.getIndex());
    }

    inputPosition.setIndex(endIndex);
    return isPositive ? timezoneHours : -1 * timezoneHours;
  }

  @Override protected int getBaseFormatPatternLength() {
    return 3;
  }

  @Override public boolean isNumeric() {
    return true;
  }
}
