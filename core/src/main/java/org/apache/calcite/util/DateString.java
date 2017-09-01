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

import java.util.Calendar;
import java.util.regex.Pattern;

/**
 * Date literal.
 *
 * <p>Immutable, internally represented as a string (in ISO format).
 */
public class DateString extends AbstractDateTime {
  private static final Pattern PATTERN =
      Pattern.compile("[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]");

  /** Internal constructor, no validation. */
  private DateString(String v, @SuppressWarnings("unused") boolean ignore) {
    super(v);
  }

  /** Creates a DateString. */
  public DateString(String v) {
    this(v, false);
    Preconditions.checkArgument(PATTERN.matcher(v).matches(),
        "Invalid date format:", v);
    Preconditions.checkArgument(getYear() >= 1 && getYear() <= 9999,
        "Year out of range:", getYear());
    Preconditions.checkArgument(getMonth() >= 1 && getMonth() <= 12,
        "Month out of range:", getMonth());
    Preconditions.checkArgument(getDay() >= 1 && getDay() <= 31,
        "Day out of range:", getDay());
  }

  /** Creates a DateString for year, month, day values. */
  public DateString(int year, int month, int day) {
    this(ymd(year, month, day), true);
  }

  /** Validates a year-month-date and converts to a string. */
  private static String ymd(int year, int month, int day) {
    Preconditions.checkArgument(year >= 1 && year <= 9999,
        "Year out of range:", year);
    Preconditions.checkArgument(month >= 1 && month <= 12,
        "Month out of range:", month);
    Preconditions.checkArgument(day >= 1 && day <= 31,
        "Day out of range:", day);
    final StringBuilder b = new StringBuilder();
    DateTimeStringUtils.ymd(b, year, month, day);
    return b.toString();
  }

  @Override public String toString() {
    return v;
  }

  @Override public boolean equals(Object o) {
    // The value is in canonical form.
    return o == this
        || o instanceof DateString
        && ((DateString) o).v.equals(v);
  }

  @Override public int hashCode() {
    return v.hashCode();
  }

  /** Creates a DateString from a Calendar. */
  public static DateString fromCalendarFields(Calendar calendar) {
    return new DateString(calendar.get(Calendar.YEAR),
        calendar.get(Calendar.MONTH) + 1,
        calendar.get(Calendar.DAY_OF_MONTH));
  }

  /** Returns the number of days since the epoch. */
  public int getDaysSinceEpoch() {
    int year = getYear();
    int month = getMonth();
    int day = getDay();
    return DateTimeUtils.ymdToUnixDate(year, month, day);
  }

  private int getYear() {
    return Integer.parseInt(v.substring(0, 4));
  }

  private int getMonth() {
    return Integer.parseInt(v.substring(5, 7));
  }

  private int getDay() {
    return Integer.parseInt(v.substring(8, 10));
  }

  /** Creates a DateString that is a given number of days since the epoch. */
  public static DateString fromDaysSinceEpoch(int days) {
    return new DateString(DateTimeUtils.unixDateToString(days));
  }

  /** Returns the number of milliseconds since the epoch. Always a multiple of
   * 86,400,000 (the number of milliseconds in a day). */
  public long getMillisSinceEpoch() {
    return getDaysSinceEpoch() * DateTimeUtils.MILLIS_PER_DAY;
  }

  public Calendar toCalendar() {
    return Util.calendar(getMillisSinceEpoch());
  }
}

// End DateString.java
