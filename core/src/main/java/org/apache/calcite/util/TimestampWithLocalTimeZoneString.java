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

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Locale;
import java.util.TimeZone;

/**
 * Timestamp with local time-zone literal.
 *
 * <p>Immutable, internally represented as a string (in ISO format),
 * and can support unlimited precision (milliseconds, nanoseconds).
 */
public class TimestampWithLocalTimeZoneString
  implements Comparable<TimestampWithLocalTimeZoneString> {

  final TimestampString localDateTime;
  final TimeZone timeZone;
  final String v;

  /** Creates a TimestampWithLocalTimeZoneString. */
  public TimestampWithLocalTimeZoneString(TimestampString localDateTime, TimeZone timeZone) {
    this.localDateTime = localDateTime;
    this.timeZone = timeZone;
    this.v = localDateTime.toString() + " " + timeZone.getID();
  }

  /** Creates a TimestampWithLocalTimeZoneString. */
  public TimestampWithLocalTimeZoneString(String v) {
    this.localDateTime = new TimestampString(v.substring(0, v.indexOf(' ', 11)));
    String timeZoneString = v.substring(v.indexOf(' ', 11) + 1);
    Preconditions.checkArgument(DateTimeStringUtils.isValidTimeZone(timeZoneString));
    this.timeZone = TimeZone.getTimeZone(timeZoneString);
    this.v = v;
  }

  /** Creates a TimestampWithLocalTimeZoneString for year, month, day, hour, minute, second,
   *  millisecond values in the given time-zone. */
  public TimestampWithLocalTimeZoneString(int year, int month, int day, int h, int m, int s,
      String timeZone) {
    this(DateTimeStringUtils.ymdhms(new StringBuilder(), year, month, day, h, m, s).toString()
        + " " + timeZone);
  }

  /** Sets the fraction field of a {@code TimestampWithLocalTimeZoneString} to a given number
   * of milliseconds. Nukes the value set via {@link #withNanos}.
   *
   * <p>For example,
   * {@code new TimestampWithLocalTimeZoneString(1970, 1, 1, 2, 3, 4, "GMT").withMillis(56)}
   * yields {@code TIMESTAMP WITH LOCAL TIME ZONE '1970-01-01 02:03:04.056 GMT'}. */
  public TimestampWithLocalTimeZoneString withMillis(int millis) {
    Preconditions.checkArgument(millis >= 0 && millis < 1000);
    return withFraction(DateTimeStringUtils.pad(3, millis));
  }

  /** Sets the fraction field of a {@code TimestampWithLocalTimeZoneString} to a given number
   * of nanoseconds. Nukes the value set via {@link #withMillis(int)}.
   *
   * <p>For example,
   * {@code new TimestampWithLocalTimeZoneString(1970, 1, 1, 2, 3, 4, "GMT").withNanos(56789)}
   * yields {@code TIMESTAMP WITH LOCAL TIME ZONE '1970-01-01 02:03:04.000056789 GMT'}. */
  public TimestampWithLocalTimeZoneString withNanos(int nanos) {
    Preconditions.checkArgument(nanos >= 0 && nanos < 1000000000);
    return withFraction(DateTimeStringUtils.pad(9, nanos));
  }

  /** Sets the fraction field of a {@code TimestampString}.
   * The precision is determined by the number of leading zeros.
   * Trailing zeros are stripped.
   *
   * <p>For example, {@code
   * new TimestampWithLocalTimeZoneString(1970, 1, 1, 2, 3, 4, "GMT").withFraction("00506000")}
   * yields {@code TIMESTAMP WITH LOCAL TIME ZONE '1970-01-01 02:03:04.00506 GMT'}. */
  public TimestampWithLocalTimeZoneString withFraction(String fraction) {
    return new TimestampWithLocalTimeZoneString(
        localDateTime.withFraction(fraction), timeZone);
  }

  public TimestampWithLocalTimeZoneString withTimeZone(TimeZone timeZone) {
    if (this.timeZone.equals(timeZone)) {
      return this;
    }
    String localDateTimeString = localDateTime.toString();
    String v;
    String fraction;
    int i = localDateTimeString.indexOf('.');
    if (i >= 0) {
      v = localDateTimeString.substring(0, i);
      fraction = localDateTimeString.substring(i + 1);
    } else {
      v = localDateTimeString;
      fraction = null;
    }
    final DateTimeUtils.PrecisionTime pt =
        DateTimeUtils.parsePrecisionDateTimeLiteral(v,
            new SimpleDateFormat(DateTimeUtils.TIMESTAMP_FORMAT_STRING, Locale.ROOT),
            this.timeZone, -1);
    pt.getCalendar().setTimeZone(timeZone);
    if (fraction != null) {
      return new TimestampWithLocalTimeZoneString(
          pt.getCalendar().get(Calendar.YEAR),
          pt.getCalendar().get(Calendar.MONTH) + 1,
          pt.getCalendar().get(Calendar.DAY_OF_MONTH),
          pt.getCalendar().get(Calendar.HOUR_OF_DAY),
          pt.getCalendar().get(Calendar.MINUTE),
          pt.getCalendar().get(Calendar.SECOND),
          timeZone.getID())
              .withFraction(fraction);
    }
    return new TimestampWithLocalTimeZoneString(
        pt.getCalendar().get(Calendar.YEAR),
        pt.getCalendar().get(Calendar.MONTH) + 1,
        pt.getCalendar().get(Calendar.DAY_OF_MONTH),
        pt.getCalendar().get(Calendar.HOUR_OF_DAY),
        pt.getCalendar().get(Calendar.MINUTE),
        pt.getCalendar().get(Calendar.SECOND),
        timeZone.getID());
  }

  @Override public String toString() {
    return v;
  }

  @Override public boolean equals(Object o) {
    // The value is in canonical form (no trailing zeros).
    return o == this
        || o instanceof TimestampWithLocalTimeZoneString
        && ((TimestampWithLocalTimeZoneString) o).v.equals(v);
  }

  @Override public int hashCode() {
    return v.hashCode();
  }

  @Override public int compareTo(TimestampWithLocalTimeZoneString o) {
    return v.compareTo(o.v);
  }

  public TimestampWithLocalTimeZoneString round(int precision) {
    Preconditions.checkArgument(precision >= 0);
    return new TimestampWithLocalTimeZoneString(
        localDateTime.round(precision), timeZone);
  }

  /** Creates a TimestampWithLocalTimeZoneString that is a given number of milliseconds since
   * the epoch UTC. */
  public static TimestampWithLocalTimeZoneString fromMillisSinceEpoch(long millis) {
    return new TimestampWithLocalTimeZoneString(
        DateTimeUtils.unixTimestampToString(millis) + " " + DateTimeUtils.UTC_ZONE.getID())
            .withMillis((int) DateTimeUtils.floorMod(millis, 1000));
  }

  /** Converts this TimestampWithLocalTimeZoneString to a string, truncated or padded with
   * zeroes to a given precision. */
  public String toString(int precision) {
    Preconditions.checkArgument(precision >= 0);
    return localDateTime.toString(precision) + " " + timeZone.getID();
  }

  public DateString getLocalDateString() {
    return new DateString(localDateTime.toString().substring(0, 10));
  }

  public TimeString getLocalTimeString() {
    return new TimeString(localDateTime.toString().substring(11));
  }

  public TimestampString getLocalTimestampString() {
    return localDateTime;
  }

}

// End TimestampWithLocalTimeZoneString.java
