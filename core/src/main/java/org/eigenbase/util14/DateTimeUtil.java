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
package org.eigenbase.util14;

import java.text.*;

import java.util.*;

/**
 * Utility functions for datetime types: date, time, timestamp. Refactored from
 * SqlParserUtil because they are required by the Jdbc driver. TODO: review
 * methods for performance. Due to allocations required, it may be preferable to
 * introduce a "formatter" with the required state.
 */
public class DateTimeUtil {
  private DateTimeUtil() {}

  //~ Static fields/initializers ---------------------------------------------

  /** The SimpleDateFormat string for ISO dates, "yyyy-MM-dd". */
  public static final String DATE_FORMAT_STRING = "yyyy-MM-dd";

  /** The SimpleDateFormat string for ISO times, "HH:mm:ss". */
  public static final String TIME_FORMAT_STRING = "HH:mm:ss";

  /** The SimpleDateFormat string for ISO timestamps, "yyyy-MM-dd HH:mm:ss". */
  public static final String TIMESTAMP_FORMAT_STRING =
      DATE_FORMAT_STRING + " " + TIME_FORMAT_STRING;

  /** The GMT time zone. */
  public static final TimeZone GMT_ZONE = TimeZone.getTimeZone("GMT");

  /** The Java default time zone. */
  public static final TimeZone DEFAULT_ZONE = TimeZone.getDefault();

  /**
   * The number of milliseconds in a second.
   */
  public static final long MILLIS_PER_SECOND = 1000L;

  /**
   * The number of milliseconds in a minute.
   */
  public static final long MILLIS_PER_MINUTE = 60000L;

  /**
   * The number of milliseconds in an hour.
   */
  public static final long MILLIS_PER_HOUR = 3600000L; // = 60 * 60 * 1000

  /**
   * The number of milliseconds in a day.
   *
   * <p>This is the modulo 'mask' used when converting
   * TIMESTAMP values to DATE and TIME values.
   */
  public static final long MILLIS_PER_DAY = 86400000; // = 24 * 60 * 60 * 1000

  /**
   * Calendar set to the epoch (1970-01-01 00:00:00 UTC). Useful for
   * initializing other values. Calendars are not immutable, so be careful not
   * to screw up this object for everyone else.
   */
  public static final Calendar ZERO_CALENDAR;

  static {
    ZERO_CALENDAR = Calendar.getInstance(DateTimeUtil.GMT_ZONE);
    ZERO_CALENDAR.setTimeInMillis(0);
  }

  /**
   * Calendar set to local time.
   */
  private static final Calendar LOCAL_CALENDAR = Calendar.getInstance();

  //~ Methods ----------------------------------------------------------------

  /**
   * Parses a string using {@link SimpleDateFormat} and a given pattern. This
   * method parses a string at the specified parse position and if successful,
   * updates the parse position to the index after the last character used.
   * The parsing is strict and requires months to be less than 12, days to be
   * less than 31, etc.
   *
   * @param s       string to be parsed
   * @param pattern {@link SimpleDateFormat}  pattern
   * @param tz      time zone in which to interpret string. Defaults to the Java
   *                default time zone
   * @param pp      position to start parsing from
   * @return a Calendar initialized with the parsed value, or null if parsing
   * failed. If returned, the Calendar is configured to the GMT time zone.
   * @pre pattern != null
   */
  private static Calendar parseDateFormat(
      String s,
      String pattern,
      TimeZone tz,
      ParsePosition pp) {
    assert pattern != null;
    SimpleDateFormat df = new SimpleDateFormat(pattern);
    if (tz == null) {
      tz = DEFAULT_ZONE;
    }
    Calendar ret = Calendar.getInstance(tz);
    df.setCalendar(ret);
    df.setLenient(false);

    java.util.Date d = df.parse(s, pp);
    if (null == d) {
      return null;
    }
    ret.setTime(d);
    ret.setTimeZone(GMT_ZONE);
    return ret;
  }

  /**
   * Parses a string using {@link SimpleDateFormat} and a given pattern. The
   * entire string must match the pattern specified.
   *
   * @param s       string to be parsed
   * @param pattern {@link SimpleDateFormat}  pattern
   * @param tz      time zone in which to interpret string. Defaults to the Java
   *                default time zone
   * @return a Calendar initialized with the parsed value, or null if parsing
   * failed. If returned, the Calendar is configured to the GMT time zone.
   */
  public static Calendar parseDateFormat(
      String s,
      String pattern,
      TimeZone tz) {
    assert pattern != null;
    ParsePosition pp = new ParsePosition(0);
    Calendar ret = parseDateFormat(s, pattern, tz, pp);
    if (pp.getIndex() != s.length()) {
      // Didn't consume entire string - not good
      return null;
    }
    return ret;
  }

  /**
   * Parses a string using {@link SimpleDateFormat} and a given pattern, and
   * if present, parses a fractional seconds component. The fractional seconds
   * component must begin with a decimal point ('.') followed by numeric
   * digits. The precision is rounded to a maximum of 3 digits of fractional
   * seconds precision (to obtain milliseconds).
   *
   * @param s       string to be parsed
   * @param pattern {@link SimpleDateFormat}  pattern
   * @param tz      time zone in which to interpret string. Defaults to the
   *                local time zone
   * @return a {@link DateTimeUtil.PrecisionTime PrecisionTime} initialized
   * with the parsed value, or null if parsing failed. The PrecisionTime
   * contains a GMT Calendar and a precision.
   */
  public static PrecisionTime parsePrecisionDateTimeLiteral(
      String s,
      String pattern,
      TimeZone tz) {
    assert pattern != null;
    ParsePosition pp = new ParsePosition(0);
    Calendar cal = parseDateFormat(s, pattern, tz, pp);
    if (cal == null) {
      return null; // Invalid date/time format
    }

    // Note: the Java SimpleDateFormat 'S' treats any number after
    // the decimal as milliseconds. That means 12:00:00.9 has 9
    // milliseconds and 12:00:00.9999 has 9999 milliseconds.
    int p = 0;
    if (pp.getIndex() < s.length()) {
      // Check to see if rest is decimal portion
      if (s.charAt(pp.getIndex()) != '.') {
        return null;
      }

      // Skip decimal sign
      pp.setIndex(pp.getIndex() + 1);

      // Parse decimal portion
      if (pp.getIndex() < s.length()) {
        String secFraction = s.substring(pp.getIndex());
        if (!secFraction.matches("\\d+")) {
          return null;
        }
        NumberFormat nf = NumberFormat.getIntegerInstance();
        Number num = nf.parse(s, pp);
        if ((num == null) || (pp.getIndex() != s.length())) {
          // Invalid decimal portion
          return null;
        }

        // Determine precision - only support prec 3 or lower
        // (milliseconds) Higher precisions are quietly rounded away
        p = Math.min(
            3,
            secFraction.length());

        // Calculate milliseconds
        int ms =
            (int) Math.round(
                num.longValue()
                * Math.pow(10, 3 - secFraction.length()));
        cal.add(Calendar.MILLISECOND, ms);
      }
    }

    assert pp.getIndex() == s.length();
    PrecisionTime ret = new PrecisionTime(cal, p);
    return ret;
  }

  /**
   * Gets the active time zone based on a Calendar argument
   */
  public static TimeZone getTimeZone(Calendar cal) {
    if (cal == null) {
      return DEFAULT_ZONE;
    }
    return cal.getTimeZone();
  }

  /**
   * Checks if the date/time format is valid
   *
   * @param pattern {@link SimpleDateFormat}  pattern
   * @throws IllegalArgumentException if the given pattern is invalid
   */
  public static void checkDateFormat(String pattern) {
    new SimpleDateFormat(pattern);
  }

  /**
   * Creates a new date formatter with Farrago specific options. Farrago
   * parsing is strict and does not allow values such as day 0, month 13, etc.
   *
   * @param format {@link SimpleDateFormat}  pattern
   */
  public static SimpleDateFormat newDateFormat(String format) {
    SimpleDateFormat sdf = new SimpleDateFormat(format);
    sdf.setLenient(false);
    return sdf;
  }

  //~ Inner Classes ----------------------------------------------------------

  /**
   * Helper class for {@link DateTimeUtil#parsePrecisionDateTimeLiteral}
   */
  public static class PrecisionTime {
    private final Calendar cal;
    private final int precision;

    public PrecisionTime(Calendar cal, int precision) {
      this.cal = cal;
      this.precision = precision;
    }

    public Calendar getCalendar() {
      return cal;
    }

    public int getPrecision() {
      return precision;
    }
  }
}

// End DateTimeUtil.java
