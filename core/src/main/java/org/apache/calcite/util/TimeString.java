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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Calendar;
import java.util.regex.Pattern;

import static java.lang.Math.floorMod;

/**
 * Time literal.
 *
 * <p>Immutable, internally represented as a string (in ISO format),
 * and can support unlimited precision (milliseconds, nanoseconds).
 */
public class TimeString implements Comparable<TimeString> {
  private static final Pattern PATTERN =
      Pattern.compile("[0-9][0-9]:[0-9][0-9]:[0-9][0-9](\\.[0-9]*[1-9])?");

  final String v;

  /** Internal constructor, no validation. */
  private TimeString(String v, @SuppressWarnings("unused") boolean ignore) {
    this.v = v;
  }

  /** Creates a TimeString. */
  @SuppressWarnings("method.invocation.invalid")
  public TimeString(String v) {
    this(v, false);
    Preconditions.checkArgument(PATTERN.matcher(v).matches(),
        "Invalid time format:", v);
    Preconditions.checkArgument(getHour() >= 0 && getHour() < 24,
        "Hour out of range:", getHour());
    Preconditions.checkArgument(getMinute() >= 0 && getMinute() < 60,
        "Minute out of range:", getMinute());
    Preconditions.checkArgument(getSecond() >= 0 && getSecond() < 60,
        "Second out of range:", getSecond());
  }

  /** Creates a TimeString for hour, minute, second and millisecond values. */
  public TimeString(int h, int m, int s) {
    this(hms(h, m, s), false);
  }

  /** Validates an hour-minute-second value and converts to a string. */
  private static String hms(int h, int m, int s) {
    Preconditions.checkArgument(h >= 0 && h < 24, "Hour out of range:", h);
    Preconditions.checkArgument(m >= 0 && m < 60, "Minute out of range:", m);
    Preconditions.checkArgument(s >= 0 && s < 60, "Second out of range:", s);
    final StringBuilder b = new StringBuilder();
    DateTimeStringUtils.hms(b, h, m, s);
    return b.toString();
  }

  /** Sets the fraction field of a {@code TimeString} to a given number
   * of milliseconds. Nukes the value set via {@link #withNanos}.
   *
   * <p>For example,
   * {@code new TimeString(1970, 1, 1, 2, 3, 4).withMillis(56)}
   * yields {@code TIME '1970-01-01 02:03:04.056'}. */
  public TimeString withMillis(int millis) {
    Preconditions.checkArgument(millis >= 0 && millis < 1000);
    return withFraction(DateTimeStringUtils.pad(3, millis));
  }

  /** Sets the fraction field of a {@code TimeString} to a given number
   * of nanoseconds. Nukes the value set via {@link #withMillis(int)}.
   *
   * <p>For example,
   * {@code new TimeString(1970, 1, 1, 2, 3, 4).withNanos(56789)}
   * yields {@code TIME '1970-01-01 02:03:04.000056789'}. */
  public TimeString withNanos(int nanos) {
    Preconditions.checkArgument(nanos >= 0 && nanos < 1000000000);
    return withFraction(DateTimeStringUtils.pad(9, nanos));
  }

  /** Sets the fraction field of a {@code TimeString}.
   * The precision is determined by the number of leading zeros.
   * Trailing zeros are stripped.
   *
   * <p>For example,
   * {@code new TimeString(1970, 1, 1, 2, 3, 4).withFraction("00506000")}
   * yields {@code TIME '1970-01-01 02:03:04.00506'}. */
  public TimeString withFraction(String fraction) {
    String v = this.v;
    int i = v.indexOf('.');
    if (i >= 0) {
      v = v.substring(0, i);
    }
    while (fraction.endsWith("0")) {
      fraction = fraction.substring(0, fraction.length() - 1);
    }
    if (fraction.length() > 0) {
      v = v + "." + fraction;
    }
    return new TimeString(v);
  }

  @Override public String toString() {
    return v;
  }

  @Override public boolean equals(@Nullable Object o) {
    // The value is in canonical form (no trailing zeros).
    return o == this
        || o instanceof TimeString
        && ((TimeString) o).v.equals(v);
  }

  @Override public int hashCode() {
    return v.hashCode();
  }

  @Override public int compareTo(TimeString o) {
    return v.compareTo(o.v);
  }

  /** Creates a TimeString from a Calendar. */
  public static TimeString fromCalendarFields(Calendar calendar) {
    return new TimeString(
        calendar.get(Calendar.HOUR_OF_DAY),
        calendar.get(Calendar.MINUTE),
        calendar.get(Calendar.SECOND))
        .withMillis(calendar.get(Calendar.MILLISECOND));
  }

  public static TimeString fromMillisOfDay(int i) {
    return new TimeString(DateTimeUtils.unixTimeToString(i))
        .withMillis((int) floorMod(i, 1000L));
  }

  public TimeString round(int precision) {
    Preconditions.checkArgument(precision >= 0);
    int targetLength = 9 + precision;
    if (v.length() <= targetLength) {
      return this;
    }
    String v = this.v.substring(0, targetLength);
    while (v.length() >= 9 && (v.endsWith("0") || v.endsWith("."))) {
      v = v.substring(0, v.length() - 1);
    }
    return new TimeString(v);
  }

  public int getMillisOfDay() {
    int h = Integer.valueOf(v.substring(0, 2));
    int m = Integer.valueOf(v.substring(3, 5));
    int s = Integer.valueOf(v.substring(6, 8));
    int ms = getMillisInSecond();
    return (int) (h * DateTimeUtils.MILLIS_PER_HOUR
        + m * DateTimeUtils.MILLIS_PER_MINUTE
        + s * DateTimeUtils.MILLIS_PER_SECOND
        + ms);
  }

  private int getMillisInSecond() {
    switch (v.length()) {
    case 8: // "12:34:56"
      return 0;
    case 10: // "12:34:56.7"
      return Integer.valueOf(v.substring(9)) * 100;
    case 11: // "12:34:56.78"
      return Integer.valueOf(v.substring(9)) * 10;
    case 12: // "12:34:56.789"
    default: // "12:34:56.7890000012345"
      return Integer.valueOf(v.substring(9, 12));
    }
  }

  private int getHour() {
    return Integer.parseInt(v.substring(0, 2));
  }

  private int getMinute() {
    return Integer.parseInt(this.v.substring(3, 5));
  }

  private int getSecond() {
    return Integer.parseInt(this.v.substring(6, 8));
  }

  public Calendar toCalendar() {
    return Util.calendar(getMillisOfDay());
  }

  /** Converts this TimestampString to a string, truncated or padded with
   * zeros to a given precision. */
  public String toString(int precision) {
    Preconditions.checkArgument(precision >= 0);
    final int p = precision();
    if (precision < p) {
      return round(precision).toString(precision);
    }
    if (precision > p) {
      String s = v;
      if (p == 0) {
        s += ".";
      }
      return s + Strings.repeat("0", precision - p);
    }
    return v;
  }

  private int precision() {
    return v.length() < 9 ? 0 : (v.length() - 9);
  }
}
