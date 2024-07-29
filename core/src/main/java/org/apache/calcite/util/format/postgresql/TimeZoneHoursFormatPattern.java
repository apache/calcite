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

import java.text.ParseException;
import java.text.ParsePosition;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.util.Locale;

/**
 * Able to parse timezone hours from string and to generate a string of the timezone
 * hours from a datetime. Timezone hours always have a sign (+/-) and are between
 * -15 and +15.
 */
public class TimeZoneHoursFormatPattern extends StringFormatPattern {
  public TimeZoneHoursFormatPattern() {
    super(ChronoUnitEnum.TIMEZONE_HOURS, "TZH");
  }

  @Override protected int parseValue(final ParsePosition inputPosition, final String input,
      final Locale locale, final boolean haveFillMode, boolean enforceLength)
      throws ParseException {

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

    int timezoneHours = Integer.parseInt(input.substring(inputOffset, endIndex));

    if (timezoneHours > 15) {
      throw new ParseException("Value is outside of valid range", inputPosition.getIndex());
    }

    inputPosition.setIndex(endIndex);
    return isPositive ? timezoneHours : -1 * timezoneHours;
  }

  @Override protected String dateTimeToString(ZonedDateTime dateTime, boolean haveFillMode,
      @Nullable String suffix, Locale locale) {
    return String.format(
        Locale.ROOT,
        "%+02d",
        dateTime.getOffset().get(ChronoField.OFFSET_SECONDS) / 3600);
  }

  @Override protected boolean isNumeric() {
    return true;
  }
}
