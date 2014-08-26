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
package org.eigenbase.sql;

import java.util.*;

import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.util14.DateTimeUtil;

/**
 * A SQL literal representing a TIMESTAMP value, for example <code>TIMESTAMP
 * '1969-07-21 03:15 GMT'</code>.
 *
 * <p>Create values using {@link SqlLiteral#createTimestamp}.
 */
public class SqlTimestampLiteral extends SqlAbstractDateTimeLiteral {
  //~ Constructors -----------------------------------------------------------

  public SqlTimestampLiteral(
      Calendar cal,
      int precision,
      boolean hasTimeZone,
      SqlParserPos pos) {
    super(
        cal,
        hasTimeZone,
        SqlTypeName.TIMESTAMP,
        precision, DateTimeUtil.TIMESTAMP_FORMAT_STRING,
        pos);
  }

  public SqlTimestampLiteral(
      Calendar cal,
      int precision,
      boolean hasTimeZone,
      String format,
      SqlParserPos pos) {
    super(
        cal, hasTimeZone, SqlTypeName.TIMESTAMP, precision,
        format, pos);
  }

  //~ Methods ----------------------------------------------------------------

/*
  /**
   * Converts this literal to a {@link java.sql.Timestamp} object.
   o/
  public Timestamp getTimestamp() {
    return new Timestamp(getCal().getTimeInMillis());
  }
*/

/*
  /**
   * Converts this literal to a {@link java.sql.Time} object.
   o/
  public Time getTime() {
    long millis = getCal().getTimeInMillis();
    int tzOffset = Calendar.getInstance().getTimeZone().getOffset(millis);
    return new Time(millis - tzOffset);
  }
*/

  public SqlNode clone(SqlParserPos pos) {
    return new SqlTimestampLiteral(
        (Calendar) value,
        precision,
        hasTimeZone,
        formatString,
        pos);
  }

  public String toString() {
    return "TIMESTAMP '" + toFormattedString() + "'";
  }

  /**
   * Returns e.g. '03:05:67.456'.
   */
  public String toFormattedString() {
    String result = getTimestamp().toString(formatString);
    final Calendar cal = getCal();
    if (precision > 0) {
      assert precision <= 3;

      // get the millisecond count.  millisecond => at most 3 digits.
      String digits = Long.toString(cal.getTimeInMillis());
      result =
          result + "."
          + digits.substring(digits.length() - 3,
              digits.length() - 3 + precision);
    } else {
      assert 0 == cal.get(Calendar.MILLISECOND);
    }
    return result;
  }

  public void unparse(
      SqlWriter writer,
      int leftPrec,
      int rightPrec) {
    switch (writer.getDialect().getDatabaseProduct()) {
    case MSSQL:
      writer.literal("'" + this.toFormattedString() + "'");
      break;
    default:
      writer.literal(this.toString());
      break;
    }
  }
}

// End SqlTimestampLiteral.java
