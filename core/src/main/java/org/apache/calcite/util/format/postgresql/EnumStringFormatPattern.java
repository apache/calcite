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
 * Uses an array of string values to convert between a string representation and the
 * datetime component value. Examples of this would be AM/PM or BCE/CE. The index
 * of the string in the array is the value.
 */
public class EnumStringFormatPattern extends StringFormatPattern {
  private final ChronoField chronoField;
  private final String[] enumValues;

  /**
   * Constructs a new EnumStringFormatPattern for the provide list of pattern strings and
   * ChronoUnitEnum value.
   *
   * @param chronoUnit ChronoUnitEnum value that this pattern parses
   * @param patterns array of pattern strings
   */
  public EnumStringFormatPattern(ChronoUnitEnum chronoUnit, ChronoField chronoField,
      String... patterns) {
    super(chronoUnit, patterns);
    this.chronoField = chronoField;
    this.enumValues = patterns;
  }

  @Override protected int parseValue(ParsePosition inputPosition, String input, Locale locale,
      boolean haveFillMode, boolean enforceLength) throws ParseException {
    final String inputTrimmed = input.substring(inputPosition.getIndex());

    for (int i = 0; i < enumValues.length; i++) {
      if (inputTrimmed.startsWith(enumValues[i])) {
        inputPosition.setIndex(inputPosition.getIndex() + enumValues[i].length());
        return i;
      }
    }

    throw new ParseException("Unable to parse value", inputPosition.getIndex());
  }

  @Override public String dateTimeToString(ZonedDateTime dateTime, boolean haveFillMode,
      @Nullable String suffix, Locale locale) {
    final int value = dateTime.get(chronoField);
    if (value >= 0 && value < enumValues.length) {
      return enumValues[value];
    }

    throw new IllegalArgumentException();
  }
}
