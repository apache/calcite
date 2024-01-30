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

import org.checkerframework.checker.nullness.qual.Nullable;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Locale;
import java.util.TimeZone;

import static java.lang.Math.floorMod;

/**
 * Time with time-zone literal.
 *
 * <p>Immutable, internally represented as a string (in ISO format),
 * and can support unlimited precision (milliseconds, nanoseconds).
 */
public class TimeWithTimeZoneString implements Comparable<TimeWithTimeZoneString> {

  final TimeString localTime;
  final TimeZone timeZone;
  final String v;

  /** Creates a TimeWithTimeZoneString. */
  public TimeWithTimeZoneString(TimeString localTime, TimeZone timeZone) {
    this.localTime = localTime;
    this.timeZone = timeZone;
    this.v = localTime.toString() + " " + timeZone.getID();
  }

  /** Creates a TimeWithTimeZoneString. */
  public TimeWithTimeZoneString(String v) {
    this.localTime = new TimeString(v.substring(0, 8));
    String timeZoneString = v.substring(9);
    Preconditions.checkArgument(DateTimeStringUtils.isValidTimeZone(timeZoneString));
    this.timeZone = TimeZone.getTimeZone(timeZoneString);
    this.v = v;
  }

  /** Creates a TimeWithTimeZoneString for hour, minute, second and millisecond values
   * in the given time-zone. */
  public TimeWithTimeZoneString(int h, int m, int s, String timeZone) {
    this(DateTimeStringUtils.hms(new StringBuilder(), h, m, s).toString() + " " + timeZone);
  }

  /** Sets the fraction field of a {@code TimeWithTimeZoneString} to a given number
   * of milliseconds. Nukes the value set via {@link #withNanos}.
   *
   * <p>For example,
   * {@code new TimeWithTimeZoneString(1970, 1, 1, 2, 3, 4, "UTC").withMillis(56)}
   * yields {@code TIME WITH LOCAL TIME ZONE '1970-01-01 02:03:04.056 UTC'}. */
  public TimeWithTimeZoneString withMillis(int millis) {
    Preconditions.checkArgument(millis >= 0 && millis < 1000);
    return withFraction(DateTimeStringUtils.pad(3, millis));
  }

  /** Sets the fraction field of a {@code TimeString} to a given number
   * of nanoseconds. Nukes the value set via {@link #withMillis(int)}.
   *
   * <p>For example,
   * {@code new TimeWithTimeZoneString(1970, 1, 1, 2, 3, 4, "UTC").withNanos(56789)}
   * yields {@code TIME WITH LOCAL TIME ZONE '1970-01-01 02:03:04.000056789 UTC'}. */
  public TimeWithTimeZoneString withNanos(int nanos) {
    Preconditions.checkArgument(nanos >= 0 && nanos < 1000000000);
    return withFraction(DateTimeStringUtils.pad(9, nanos));
  }

  /** Sets the fraction field of a {@code TimeWithTimeZoneString}.
   * The precision is determined by the number of leading zeros.
   * Trailing zeros are stripped.
   *
   * <p>For example,
   * {@code new TimeWithTimeZoneString(1970, 1, 1, 2, 3, 4, "UTC").withFraction("00506000")}
   * yields {@code TIME WITH LOCAL TIME ZONE '1970-01-01 02:03:04.00506 UTC'}. */
  public TimeWithTimeZoneString withFraction(String fraction) {
    String v = this.v;
    int i = v.indexOf('.');
    if (i >= 0) {
      v = v.substring(0, i);
    } else {
      v = v.substring(0, 8);
    }
    while (fraction.endsWith("0")) {
      fraction = fraction.substring(0, fraction.length() - 1);
    }
    if (fraction.length() > 0) {
      v = v + "." + fraction;
    }
    v = v + this.v.substring(8); // time-zone
    return new TimeWithTimeZoneString(v);
  }

  public TimeWithTimeZoneString withTimeZone(TimeZone timeZone) {
    if (this.timeZone.equals(timeZone)) {
      return this;
    }
    String localTimeString = localTime.toString();
    String v;
    String fraction;
    int i = localTimeString.indexOf('.');
    if (i >= 0) {
      v = localTimeString.substring(0, i);
      fraction = localTimeString.substring(i + 1);
    } else {
      v = localTimeString;
      fraction = null;
    }
    final DateTimeUtils.PrecisionTime pt =
        DateTimeUtils.parsePrecisionDateTimeLiteral(v,
            new SimpleDateFormat(DateTimeUtils.TIME_FORMAT_STRING, Locale.ROOT),
            this.timeZone, -1);
    pt.getCalendar().setTimeZone(timeZone);
    if (fraction != null) {
      return new TimeWithTimeZoneString(
          pt.getCalendar().get(Calendar.HOUR_OF_DAY),
          pt.getCalendar().get(Calendar.MINUTE),
          pt.getCalendar().get(Calendar.SECOND),
          timeZone.getID())
              .withFraction(fraction);
    }
    return new TimeWithTimeZoneString(
        pt.getCalendar().get(Calendar.HOUR_OF_DAY),
        pt.getCalendar().get(Calendar.MINUTE),
        pt.getCalendar().get(Calendar.SECOND),
        timeZone.getID());
  }

  @Override public String toString() {
    return v;
  }

  @Override public boolean equals(@Nullable Object o) {
    // The value is in canonical form (no trailing zeros).
    return o == this
        || o instanceof TimeWithTimeZoneString
        && ((TimeWithTimeZoneString) o).v.equals(v);
  }

  @Override public int hashCode() {
    return v.hashCode();
  }

  @Override public int compareTo(TimeWithTimeZoneString o) {
    return v.compareTo(o.v);
  }

  public TimeWithTimeZoneString round(int precision) {
    Preconditions.checkArgument(precision >= 0);
    return new TimeWithTimeZoneString(
        localTime.round(precision), timeZone);
  }

  public static TimeWithTimeZoneString fromMillisOfDay(int i) {
    return new TimeWithTimeZoneString(
        DateTimeUtils.unixTimeToString(i) + " " + DateTimeUtils.UTC_ZONE.getID())
            .withMillis((int) floorMod(i, 1000L));
  }

  /** Converts this TimeWithTimeZoneString to a string, truncated or padded with
   * zeros to a given precision. */
  public String toString(int precision) {
    Preconditions.checkArgument(precision >= 0);
    return localTime.toString(precision) + " " + timeZone.getID();
  }

  public TimeString getLocalTimeString() {
    return localTime;
  }

}
