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

import java.io.Serializable;
import java.text.DateFormat;
import java.util.Calendar;
import java.util.Locale;
import java.util.TimeZone;

/**
 * ZonelessDatetime is an abstract class for dates, times, or timestamps that
 * contain a zoneless time value.
 */
public abstract class ZonelessDatetime implements BasicDatetime, Serializable {
  //~ Static fields/initializers ---------------------------------------------

  /**
   * SerialVersionUID created with JDK 1.5 serialver tool.
   */
  private static final long serialVersionUID = -1274713852537224763L;

  //~ Instance fields --------------------------------------------------------

  /**
   * Treat this as a protected field. It is only made public to simplify Java
   * code generation.
   */
  public long internalTime;

  // The following fields are workspace and are not serialized.

  protected transient Calendar tempCal;
  protected transient DateFormat tempFormatter;
  protected transient String lastFormat;

  //~ Methods ----------------------------------------------------------------

  // implement BasicDatetime
  public long getTime() {
    return internalTime;
  }

  // implement BasicDatetime
  public void setZonelessTime(long value) {
    this.internalTime = value;
  }

  // implement BasicDatetime
  public void setZonedTime(long value, TimeZone zone) {
    this.internalTime = value + zone.getOffset(value);
  }

  /**
   * Gets the time portion of this zoneless datetime.
   */
  public long getTimeValue() {
    // Value must be non-negative, even for negative timestamps, and
    // unfortunately the '%' operator returns a negative value if its LHS
    // is negative.
    long timePart = internalTime % DateTimeUtils.MILLIS_PER_DAY;
    if (timePart < 0) {
      timePart += DateTimeUtils.MILLIS_PER_DAY;
    }
    return timePart;
  }

  /**
   * Gets the date portion of this zoneless datetime.
   */
  public long getDateValue() {
    return internalTime - getTimeValue();
  }

  /**
   * Clears the date component of this datetime
   */
  public void clearDate() {
    internalTime = getTimeValue();
  }

  /**
   * Clears the time component of this datetime
   */
  public void clearTime() {
    internalTime = getDateValue();
  }

  /**
   * Gets the value of this datetime as a milliseconds value for
   * {@link java.sql.Time}.
   *
   * @param zone time zone in which to generate a time value for
   */
  public long getJdbcTime(TimeZone zone) {
    long timeValue = getTimeValue();
    return timeValue - zone.getOffset(timeValue);
  }

  /**
   * Gets the value of this datetime as a milliseconds value for
   * {@link java.sql.Date}.
   *
   * @param zone time zone in which to generate a time value for
   */
  public long getJdbcDate(TimeZone zone) {
    Calendar cal = getCalendar(DateTimeUtils.GMT_ZONE);
    cal.setTimeInMillis(getDateValue());

    int year = cal.get(Calendar.YEAR);
    int doy = cal.get(Calendar.DAY_OF_YEAR);

    cal.clear();
    cal.setTimeZone(zone);
    cal.set(Calendar.YEAR, year);
    cal.set(Calendar.DAY_OF_YEAR, doy);
    return cal.getTimeInMillis();
  }

  /**
   * Gets the value of this datetime as a milliseconds value for
   * {@link java.sql.Timestamp}.
   *
   * @param zone time zone in which to generate a time value for
   */
  public long getJdbcTimestamp(TimeZone zone) {
    Calendar cal = getCalendar(DateTimeUtils.GMT_ZONE);
    cal.setTimeInMillis(internalTime);

    int year = cal.get(Calendar.YEAR);
    int doy = cal.get(Calendar.DAY_OF_YEAR);
    int hour = cal.get(Calendar.HOUR_OF_DAY);
    int minute = cal.get(Calendar.MINUTE);
    int second = cal.get(Calendar.SECOND);
    int millis = cal.get(Calendar.MILLISECOND);

    cal.clear();
    cal.setTimeZone(zone);
    cal.set(Calendar.YEAR, year);
    cal.set(Calendar.DAY_OF_YEAR, doy);
    cal.set(Calendar.HOUR_OF_DAY, hour);
    cal.set(Calendar.MINUTE, minute);
    cal.set(Calendar.SECOND, second);
    cal.set(Calendar.MILLISECOND, millis);
    return cal.getTimeInMillis();
  }

  /**
   * Returns this datetime as a Jdbc object
   */
  public abstract Object toJdbcObject();

  /**
   * Gets a temporary Calendar set to the specified time zone. The same
   * Calendar is returned on subsequent calls.
   */
  protected Calendar getCalendar(TimeZone zone) {
    if (tempCal == null) {
      tempCal = Calendar.getInstance(zone, Locale.ROOT);
    } else {
      tempCal.setTimeZone(zone);
    }
    return tempCal;
  }

  /**
   * Gets a temporary formatter for a zoneless date time. The same formatter
   * is returned on subsequent calls.
   *
   * @param format a {@link java.text.SimpleDateFormat} format string
   */
  protected DateFormat getFormatter(String format) {
    if ((tempFormatter != null) && lastFormat.equals(format)) {
      return tempFormatter;
    }
    tempFormatter = DateTimeUtils.newDateFormat(format);
    tempFormatter.setTimeZone(DateTimeUtils.GMT_ZONE);
    lastFormat = format;
    return tempFormatter;
  }
}

// End ZonelessDatetime.java
