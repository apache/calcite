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

import java.text.ParsePosition;
import java.time.ZonedDateTime;
import java.util.Locale;

/**
 * A format element that is able to produce a string from a date.
 */
public abstract class FormatPattern {
  private String[] patterns;

  public FormatPattern(String[] patterns) {
    this.patterns = patterns;
  }

  public String[] getPatterns() {
    return patterns;
  }

  /**
   * Checks if this pattern matches the substring starting at the <code>parsePosition</code>
   * in the <code>formatString</code>. If it matches, then the <code>dateTime</code> is
   * converted to a string based on this pattern. For example, "YYYY" will get the year of
   * the <code>dateTime</code> and convert it to a string.
   *
   * @param parsePosition current position in the format string
   * @param formatString input format string
   * @param dateTime datetime to convert
   * @return the string representation of the datetime based on the format pattern
   */
  @Nullable public abstract String convert(ParsePosition parsePosition, String formatString,
      ZonedDateTime dateTime);

  protected abstract @Nullable ChronoUnitEnum getChronoUnit();

  public long parse(ParsePosition inputPosition, String input, ParsePosition formatPosition,
      String formatString, boolean enforceLength) throws Exception {

    boolean haveFillMode = false;
    boolean haveTranslateMode = false;

    String formatTrimmed = formatString.substring(formatPosition.getIndex());
    if (formatTrimmed.startsWith("FMTM") || formatTrimmed.startsWith("TMFM")) {
      haveFillMode = true;
      haveTranslateMode = true;
      formatTrimmed = formatTrimmed.substring(4);
    } else if (formatTrimmed.startsWith("FM")) {
      haveFillMode = true;
      formatTrimmed = formatTrimmed.substring(2);
    } else if (formatTrimmed.startsWith("TM")) {
      haveTranslateMode = true;
      formatTrimmed = formatTrimmed.substring(2);
    }

    for (String pattern : patterns) {
      if (formatTrimmed.startsWith(pattern)) {
        formatTrimmed = formatTrimmed.substring(pattern.length());
        break;
      }
    }

    try {
      final Locale locale = haveTranslateMode ? Locale.getDefault() : Locale.US;
      long parsedValue = parseValue(inputPosition, input, locale, haveFillMode, enforceLength);
      formatPosition.setIndex(formatString.length() - formatTrimmed.length());

      return parsedValue;
    } catch (Exception e) {
      inputPosition.setErrorIndex(inputPosition.getIndex());
      throw e;
    }
  }

  protected abstract int parseValue(ParsePosition inputPosition, String input, Locale locale,
      boolean haveFillMode, boolean enforceLength) throws Exception;

  int getFormatLength(final String formatString) {
    int length = 0;

    for (String prefix : new String[] {"FM", "TM"}) {
      if (formatString.substring(length).startsWith(prefix)) {
        length += 2;
      }
    }

    String formatTrimmed = formatString.substring(length);
    for (String pattern : patterns) {
      if (formatTrimmed.startsWith(pattern)) {
        length += pattern.length();
      }
    }

    formatTrimmed = formatString.substring(length);
    if (formatTrimmed.startsWith("TH") || formatTrimmed.startsWith("th")) {
      length += 2;
    }

    return length;
  }

  int matchedPatternLength(final String formatString, final ParsePosition formatParsePosition) {
    String formatTrimmed = formatString.substring(formatParsePosition.getIndex());

    int prefixLength = 0;
    for (String prefix : new String[] {"FM", "TM"}) {
      if (formatTrimmed.startsWith(prefix)) {
        formatTrimmed = formatTrimmed.substring(prefix.length());
        prefixLength += prefix.length();
      }
    }

    for (String pattern : patterns) {
      if (formatTrimmed.startsWith(pattern)) {
        return prefixLength + pattern.length();
      }
    }

    return -1;
  }

  protected abstract boolean isNumeric();
}
