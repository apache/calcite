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
package org.apache.calcite.util;

import org.apache.calcite.avatica.util.DateTimeUtils;

import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.TimeZone;

/**
 * Utility methods to manipulate String representation of DateTime values.
 */
public class DateTimeStringUtils {

  private DateTimeStringUtils() {}

  /** The SimpleDateFormat string for ISO timestamps,
   * "yyyy-MM-dd'T'HH:mm:ss'Z'". */
  public static final String ISO_DATETIME_FORMAT =
      "yyyy-MM-dd'T'HH:mm:ss'Z'";


  /** The SimpleDateFormat string for ISO timestamps with precisions, "yyyy-MM-dd'T'HH:mm:ss
   * .SSS'Z'"*/
  public static final String ISO_DATETIME_FRACTIONAL_SECOND_FORMAT =
      "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";

  static String pad(int length, long v) {
    StringBuilder s = new StringBuilder(Long.toString(v));
    while (s.length() < length) {
      s.insert(0, "0");
    }
    return s.toString();
  }

  /** Appends hour:minute:second to a buffer; assumes they are valid. */
  static StringBuilder hms(StringBuilder b, int h, int m, int s) {
    int2(b, h);
    b.append(':');
    int2(b, m);
    b.append(':');
    int2(b, s);
    return b;
  }

  /** Appends year-month-day and hour:minute:second to a buffer; assumes they
   * are valid. */
  static StringBuilder ymdhms(StringBuilder b, int year, int month, int day,
      int h, int m, int s) {
    ymd(b, year, month, day);
    b.append(' ');
    hms(b, h, m, s);
    return b;
  }

  /** Appends year-month-day to a buffer; assumes they are valid. */
  static StringBuilder ymd(StringBuilder b, int year, int month, int day) {
    int4(b, year);
    b.append('-');
    int2(b, month);
    b.append('-');
    int2(b, day);
    return b;
  }

  private static void int4(StringBuilder buf, int i) {
    buf.append((char) ('0' + (i / 1000) % 10));
    buf.append((char) ('0' + (i / 100) % 10));
    buf.append((char) ('0' + (i / 10) % 10));
    buf.append((char) ('0' + i % 10));
  }

  private static void int2(StringBuilder buf, int i) {
    buf.append((char) ('0' + (i / 10) % 10));
    buf.append((char) ('0' + i % 10));
  }

  static boolean isValidTimeZone(final String timeZone) {
    if (timeZone.equals("GMT")) {
      return true;
    } else {
      String id = TimeZone.getTimeZone(timeZone).getID();
      if (!id.equals("GMT")) {
        return true;
      }
    }
    return false;
  }

  /**
   * Create a SimpleDateFormat with format string with default time zone UTC.
   */
  public static SimpleDateFormat getDateFormatter(String format) {
    return getDateFormatter(format, DateTimeUtils.UTC_ZONE);
  }

  /**
   * Create a SimpleDateFormat with format string and time zone.
   */
  public static SimpleDateFormat getDateFormatter(String format, TimeZone timeZone) {
    final SimpleDateFormat dateFormatter = new SimpleDateFormat(
        format, Locale.ROOT);
    dateFormatter.setTimeZone(timeZone);
    return dateFormatter;
  }
}
