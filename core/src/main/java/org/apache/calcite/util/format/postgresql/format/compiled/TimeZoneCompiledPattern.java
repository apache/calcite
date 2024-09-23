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
import java.time.format.TextStyle;
import java.util.Locale;
import java.util.Set;

/**
 * The date/time format compiled component for the 3 letter timezone code (such as UTC).
 * This is only supported when converting a date/time value to a string.
 */
public class TimeZoneCompiledPattern extends CompiledPattern {
  private final boolean upperCase;

  public TimeZoneCompiledPattern(Set<PatternModifier> modifiers, boolean upperCase) {
    super(ChronoUnitEnum.TIMEZONE_MINUTES, modifiers);
    this.upperCase = upperCase;
  }

  @Override public String convertToString(ZonedDateTime dateTime, Locale locale) {
    final String stringValue =
        String.format(
            locale,
            "%3s",
            dateTime.getZone().getDisplayName(TextStyle.SHORT, Locale.US).toUpperCase(locale));

    if (upperCase) {
      return stringValue.toUpperCase(Locale.US);
    }

    return stringValue;
  }

  @Override public int parseValue(ParsePosition inputPosition, String input, boolean enforceLength,
      Locale locale) throws ParseException {
    throw new ParseException("TZ pattern is not supported in parsing datetime values",
        inputPosition.getIndex());
  }

  @Override protected int getBaseFormatPatternLength() {
    return 2;
  }
}
