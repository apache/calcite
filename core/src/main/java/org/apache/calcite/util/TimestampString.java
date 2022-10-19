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
 * Timestamp literal.
 *
 * <p>Immutable, internally represented as a string (in ISO format),
 * and can support unlimited precision (milliseconds, nanoseconds).
 */
public class TimestampString implements Comparable<TimestampString> {
  private static final Pattern PATTERN =
      Pattern.compile("[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]"
          + " "
          + "[0-9][0-9]:[0-9][0-9]:[0-9][0-9](\\.[0-9]*[1-9])?");

  final String v;

  /** Creates a TimeString. */
  public TimestampString(String v) {
    this.v = v;
    Preconditions.checkArgument(PATTERN.matcher(v).matches(), v);
  }

  /** Creates a TimestampString for year, month, day, hour, minute, second,
   *  millisecond values. */
  public TimestampString(int year, int month, int day, int h, int m, int s) {
    this(DateTimeStringUtils.ymdhms(new StringBuilder(), year, month, day, h, m, s).toString());
  }

  /** Sets the fraction field of a {@code TimestampString} to a given number
   * of milliseconds. Nukes the value set via {@link #withNanos}.
   *
   * <p>For example,
   * {@code new TimestampString(1970, 1, 1, 2, 3, 4).withMillis(56)}
   * yields {@code TIMESTAMP '1970-01-01 02:03:04.056'}. */
  public TimestampString withMillis(int millis) {
    Preconditions.checkArgument(millis >= 0 && millis < 1000);
    return withFraction(DateTimeStringUtils.pad(3, millis));
  }

  /** Sets the fraction field of a {@code TimestampString} to a given number
   * of nanoseconds. Nukes the value set via {@link #withMillis(int)}.
   *
   * <p>For example,
   * {@code new TimestampString(1970, 1, 1, 2, 3, 4).withNanos(56789)}
   * yields {@code TIMESTAMP '1970-01-01 02:03:04.000056789'}. */
  public TimestampString withNanos(int nanos) {
    Preconditions.checkArgument(nanos >= 0 && nanos < 1000000000);
    return withFraction(DateTimeStringUtils.pad(9, nanos));
  }

  /** Sets the fraction field of a {@code TimestampString}.
   * The precision is determined by the number of leading zeros.
   * Trailing zeros are stripped.
   *
   * <p>For example,
   * {@code new TimestampString(1970, 1, 1, 2, 3, 4).withFraction("00506000")}
   * yields {@code TIMESTAMP '1970-01-01 02:03:04.00506'}. */
  public TimestampString withFraction(String fraction) {
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
    return new TimestampString(v);
  }

  @Override public String toString() {
    return v;
  }

  @Override public boolean equals(@Nullable Object o) {
    // The value is in canonical form (no trailing zeros).
    return o == this
        || o instanceof TimestampString
        && ((TimestampString) o).v.equals(v);
  }

  @Override public int hashCode() {
    return v.hashCode();
  }

  @Override public int compareTo(TimestampString o) {
    return v.compareTo(o.v);
  }

  /** Creates a TimestampString from a Calendar. */
  public static TimestampString fromCalendarFields(Calendar calendar) {
    return new TimestampString(
        calendar.get(Calendar.YEAR),
        calendar.get(Calendar.MONTH) + 1,
        calendar.get(Calendar.DAY_OF_MONTH),
        calendar.get(Calendar.HOUR_OF_DAY),
        calendar.get(Calendar.MINUTE),
        calendar.get(Calendar.SECOND))
        .withMillis(calendar.get(Calendar.MILLISECOND));
  }

  public TimestampString round(int precision) {
    Preconditions.checkArgument(precision >= 0);
    int targetLength = 20 + precision;
    if (v.length() <= targetLength) {
      return this;
    }
    String v = this.v.substring(0, targetLength);
    while (v.length() >= 20 && (v.endsWith("0") || v.endsWith("."))) {
      v = v.substring(0, v.length() - 1);
    }
    return new TimestampString(v);
  }

  /** Returns the number of milliseconds since the epoch. */
  public long getMillisSinceEpoch() {
    final int year = Integer.valueOf(v.substring(0, 4));
    final int month = Integer.valueOf(v.substring(5, 7));
    final int day = Integer.valueOf(v.substring(8, 10));
    final int h = Integer.valueOf(v.substring(11, 13));
    final int m = Integer.valueOf(v.substring(14, 16));
    final int s = Integer.valueOf(v.substring(17, 19));
    final int ms = getMillisInSecond();
    final int d = DateTimeUtils.ymdToUnixDate(year, month, day);
    return d * DateTimeUtils.MILLIS_PER_DAY
        + h * DateTimeUtils.MILLIS_PER_HOUR
        + m * DateTimeUtils.MILLIS_PER_MINUTE
        + s * DateTimeUtils.MILLIS_PER_SECOND
        + ms;
  }

  private int getMillisInSecond() {
    switch (v.length()) {
    case 19: // "1999-12-31 12:34:56"
      return 0;
    case 21: // "1999-12-31 12:34:56.7"
      return Integer.valueOf(v.substring(20)) * 100;
    case 22: // "1999-12-31 12:34:56.78"
      return Integer.valueOf(v.substring(20)) * 10;
    case 23: // "1999-12-31 12:34:56.789"
    default:  // "1999-12-31 12:34:56.789123456"
      return Integer.valueOf(v.substring(20, 23));
    }
  }

  /** Creates a TimestampString that is a given number of milliseconds since
   * the epoch. */
  public static TimestampString fromMillisSinceEpoch(long millis) {
    return new TimestampString(DateTimeUtils.unixTimestampToString(millis))
        .withMillis((int) floorMod(millis, 1000L));
  }

  public Calendar toCalendar() {
    return Util.calendar(getMillisSinceEpoch());
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
    return v.length() < 20 ? 0 : (v.length() - 20);
  }
}
