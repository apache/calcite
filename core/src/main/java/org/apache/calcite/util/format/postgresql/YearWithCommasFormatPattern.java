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
import java.util.Locale;

import static java.lang.Integer.parseInt;

/**
 * Able to parse and generate string of years with commas to separate the thousands. An
 * example year is "1,997".
 */
public class YearWithCommasFormatPattern extends StringFormatPattern {
  public YearWithCommasFormatPattern() {
    super(ChronoUnitEnum.YEARS, "Y,YYY");
  }

  @Override protected int parseValue(ParsePosition inputPosition,  String input, Locale locale,
      boolean haveFillMode, boolean enforceLength) throws ParseException {

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

    inputPosition.setIndex(inputPosition.getIndex() + endIndex);

    final String remainingDigits = inputTrimmed.substring(commaIndex + 1, endIndex);
    return parseInt(thousands) * 1000 + parseInt(remainingDigits);
  }

  @Override protected String dateTimeToString(ZonedDateTime dateTime, boolean haveFillMode,
      @Nullable String suffix, Locale locale) {
    final String stringValue = String.format(locale, "%04d", dateTime.getYear());

    String outputSuffix = "";
    if (suffix != null) {
      switch (stringValue.charAt(stringValue.length() - 1)) {
      case '1':
        outputSuffix = "st";
        break;
      case '2':
        outputSuffix = "nd";
        break;
      case '3':
        outputSuffix = "rd";
        break;
      default:
        outputSuffix = "th";
        break;
      }

      if ("TH".equals(suffix)) {
        outputSuffix = outputSuffix.toUpperCase(locale);
      }
    }

    return stringValue.substring(0, stringValue.length() - 3) + ","
        + stringValue.substring(stringValue.length() - 3) + outputSuffix;
  }

  @Override protected boolean isNumeric() {
    return true;
  }
}
