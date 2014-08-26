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

import java.sql.Date;

import java.text.*;

import java.util.Calendar;
import java.util.TimeZone;

/**
 * ZonelessDate is a date value without a time zone.
 */
public class ZonelessDate extends ZonelessDatetime {
  //~ Static fields/initializers ---------------------------------------------

  /**
   * SerialVersionUID created with JDK 1.5 serialver tool.
   */
  private static final long serialVersionUID = -6385775986251759394L;

  //~ Instance fields --------------------------------------------------------

  protected transient Date tempDate;

  //~ Constructors -----------------------------------------------------------

  /**
   * Constructs a ZonelessDate.
   */
  public ZonelessDate() {
  }

  //~ Methods ----------------------------------------------------------------

  // override ZonelessDatetime
  public void setZonelessTime(long value) {
    super.setZonelessTime(value);
    clearTime();
  }

  // override ZonelessDatetime
  public void setZonedTime(long value, TimeZone zone) {
    super.setZonedTime(value, zone);
    clearTime();
  }

  // implement ZonelessDatetime
  public Object toJdbcObject() {
    return new Date(getJdbcDate(DateTimeUtil.DEFAULT_ZONE));
  }

  /**
   * Converts this ZonelessDate to a java.sql.Date and formats it via the
   * {@link java.sql.Date#toString() toString()} method of that class.
   *
   * @return the formatted date string
   */
  public String toString() {
    Date jdbcDate = getTempDate(getJdbcDate(DateTimeUtil.DEFAULT_ZONE));
    return jdbcDate.toString();
  }

  /**
   * Formats this ZonelessDate via a SimpleDateFormat
   *
   * @param format format string, as required by {@link SimpleDateFormat}
   * @return the formatted date string
   */
  public String toString(String format) {
    DateFormat formatter = getFormatter(format);
    Date jdbcDate = getTempDate(getTime());
    return formatter.format(jdbcDate);
  }

  /**
   * Parses a string as a ZonelessDate.
   *
   * @param s a string representing a date in ISO format, i.e. according to
   *          the SimpleDateFormat string "yyyy-MM-dd"
   * @return the parsed date, or null if parsing failed
   */
  public static ZonelessDate parse(String s) {
    return parse(s, DateTimeUtil.DATE_FORMAT_STRING);
  }

  /**
   * Parses a string as a ZonelessDate with a given format string.
   *
   * @param s      a string representing a date in ISO format, i.e. according to
   *               the SimpleDateFormat string "yyyy-MM-dd"
   * @param format format string as per {@link SimpleDateFormat}
   * @return the parsed date, or null if parsing failed
   */
  public static ZonelessDate parse(String s, String format) {
    Calendar cal =
        DateTimeUtil.parseDateFormat(
            s,
            format,
            DateTimeUtil.GMT_ZONE);
    if (cal == null) {
      return null;
    }
    ZonelessDate zd = new ZonelessDate();
    zd.setZonelessTime(cal.getTimeInMillis());
    return zd;
  }

  /**
   * Gets a temporary Date object. The same object is returned every time.
   */
  protected Date getTempDate(long value) {
    if (tempDate == null) {
      tempDate = new Date(value);
    } else {
      tempDate.setTime(value);
    }
    return tempDate;
  }
}

// End ZonelessDate.java
