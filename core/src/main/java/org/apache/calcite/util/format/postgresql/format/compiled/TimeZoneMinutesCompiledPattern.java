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
 * The date/time format compiled component for the minutes of the timezone offset.
 */
public class TimeZoneMinutesCompiledPattern extends CompiledPattern {
  public TimeZoneMinutesCompiledPattern(Set<PatternModifier> modifiers) {
    super(ChronoUnitEnum.TIMEZONE_MINUTES, modifiers);
  }

  @Override public String convertToString(ZonedDateTime dateTime, Locale locale) {
    return String.format(
        Locale.ROOT,
        "%02d",
        (dateTime.getOffset().getTotalSeconds() % 3600) / 60);
  }

  @Override public int parseValue(ParsePosition inputPosition, String input, boolean enforceLength,
      Locale locale) throws ParseException {
    if (inputPosition.getIndex() + 2 > input.length()) {
      throw new ParseException("Unable to parse value", inputPosition.getIndex());
    }

    if (!Character.isDigit(input.charAt(inputPosition.getIndex()))
        || !Character.isDigit(input.charAt(inputPosition.getIndex() + 1))) {
      throw new ParseException("Unable to parse value", inputPosition.getIndex());
    }

    int timezoneMinutes =
        parseInt(
            input.substring(inputPosition.getIndex(),
                inputPosition.getIndex() + 2));

    if (timezoneMinutes >= 60) {
      throw new ParseException("Value outside of valid range", inputPosition.getIndex());
    }

    inputPosition.setIndex(inputPosition.getIndex() + 2);
    return timezoneMinutes;
  }

  @Override protected int getBaseFormatPatternLength() {
    return 3;
  }

  @Override public boolean isNumeric() {
    return true;
  }
}
