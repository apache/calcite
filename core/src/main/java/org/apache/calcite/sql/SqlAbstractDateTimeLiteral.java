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
package org.apache.calcite.sql;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ZonelessDate;
import org.apache.calcite.util.ZonelessTime;
import org.apache.calcite.util.ZonelessTimestamp;

import java.util.Calendar;
import java.util.TimeZone;

/**
 * A SQL literal representing a DATE, TIME or TIMESTAMP value.
 *
 * <p>Examples:
 *
 * <ul>
 * <li>DATE '2004-10-22'</li>
 * <li>TIME '14:33:44.567'</li>
 * <li><code>TIMESTAMP '1969-07-21 03:15 GMT'</code></li>
 * </ul>
 */
abstract class SqlAbstractDateTimeLiteral extends SqlLiteral {
  //~ Instance fields --------------------------------------------------------

  protected final boolean hasTimeZone;
  protected final String formatString;
  protected final int precision;

  //~ Constructors -----------------------------------------------------------

  /**
   * Constructs a datetime literal based on a Calendar. If the literal is to
   * represent a Timestamp, the Calendar is expected to follow java.sql
   * semantics. If the Calendar is to represent a Time or Date, the Calendar
   * is expected to follow {@link org.apache.calcite.util.ZonelessTime}
   * and {@link org.apache.calcite.util.ZonelessDate}
   * semantics.
   */
  protected SqlAbstractDateTimeLiteral(
      Calendar d,
      boolean tz,
      SqlTypeName typeName,
      int precision,
      String formatString,
      SqlParserPos pos) {
    super(d, typeName, pos);
    this.hasTimeZone = tz;
    this.precision = precision;
    this.formatString = formatString;
  }

  //~ Methods ----------------------------------------------------------------

  public int getPrec() {
    return precision;
  }

  public String toValue() {
    return Long.toString(getCal().getTimeInMillis());
  }

  public Calendar getCal() {
    return (Calendar) value;
  }

  /**
   * Returns time zone component of this literal. Technically, a SQL date
   * doesn't come with a tz, but time and ts inherit this, and the calendar
   * object has one, so it seems harmless.
   *
   * @return time zone
   */
  public TimeZone getTimeZone() {
    assert hasTimeZone : "Attempt to get time zone on Literal date: "
        + getCal() + ", which has no time zone";
    return getCal().getTimeZone();
  }

  /**
   * Returns e.g. <code>DATE '1969-07-21'</code>.
   */
  public abstract String toString();

  /**
   * Returns e.g. <code>1969-07-21</code>.
   */
  public abstract String toFormattedString();

  public RelDataType createSqlType(RelDataTypeFactory typeFactory) {
    return typeFactory.createSqlType(
        getTypeName(),
        getPrec());
  }

  public void unparse(
      SqlWriter writer,
      int leftPrec,
      int rightPrec) {
    writer.literal(this.toString());
  }

  /**
   * Converts this literal to a
   * {@link org.apache.calcite.util.ZonelessDate} object.
   */
  protected ZonelessDate getDate() {
    ZonelessDate zd = new ZonelessDate();
    zd.setZonelessTime(getCal().getTimeInMillis());
    return zd;
  }

  /**
   * Converts this literal to a
   * {@link org.apache.calcite.util.ZonelessTime} object.
   */
  protected ZonelessTime getTime() {
    ZonelessTime zt = new ZonelessTime();
    zt.setZonelessTime(getCal().getTimeInMillis());
    return zt;
  }

  /**
   * Converts this literal to a
   * {@link org.apache.calcite.util.ZonelessTimestamp} object.
   */
  protected ZonelessTimestamp getTimestamp() {
    ZonelessTimestamp zt = new ZonelessTimestamp();
    zt.setZonelessTime(getCal().getTimeInMillis());
    return zt;
  }
}

// End SqlAbstractDateTimeLiteral.java
