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
 * The date/time format compiled component for the hours and minutes of the timezone offset.
 * Can be just two digits plus sign if the UTC offset is a whole number of hours. Otherwise,
 * it is a value similar to "+07:30".
 */
public class TimeZoneOffsetCompiledPattern extends CompiledPattern {
  public TimeZoneOffsetCompiledPattern(Set<PatternModifier> modifiers) {
    super(ChronoUnitEnum.TIMEZONE_MINUTES, modifiers);
  }

  @Override public String convertToString(ZonedDateTime dateTime, Locale locale) {
    final int offsetSeconds = dateTime.getOffset().getTotalSeconds();
    final int hours = offsetSeconds / 3600;
    final int minutes = (offsetSeconds % 3600) / 60;

    String formattedHours =
        String.format(Locale.ROOT, "%s%02d", hours < 0 ? "-" : "+", hours);
    if (minutes == 0) {
      return formattedHours;
    } else {
      return String.format(Locale.ROOT, "%s:%02d", formattedHours, minutes);
    }
  }

  @Override public int parseValue(ParsePosition inputPosition, String input, boolean enforceLength,
      Locale locale) throws ParseException {
    throw new ParseException("OF pattern is not supported in parsing datetime values",
        inputPosition.getIndex());
  }

  @Override protected int getBaseFormatPatternLength() {
    return 2;
  }

  @Override public boolean isNumeric() {
    return true;
  }
}
