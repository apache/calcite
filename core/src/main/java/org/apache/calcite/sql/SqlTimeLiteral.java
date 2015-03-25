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

import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;

import com.google.common.base.Preconditions;

import java.util.Calendar;

/**
 * A SQL literal representing a TIME value, for example <code>TIME
 * '14:33:44.567'</code>.
 *
 * <p>Create values using {@link SqlLiteral#createTime}.
 */
public class SqlTimeLiteral extends SqlAbstractDateTimeLiteral {
  //~ Constructors -----------------------------------------------------------

  SqlTimeLiteral(
      Calendar t,
      int precision,
      boolean hasTimeZone,
      SqlParserPos pos) {
    this(t, precision, hasTimeZone, DateTimeUtils.TIME_FORMAT_STRING, pos);
  }

  SqlTimeLiteral(
      Calendar t,
      int precision,
      boolean hasTimeZone,
      String format,
      SqlParserPos pos) {
    super(t, hasTimeZone, SqlTypeName.TIME, precision, format, pos);
    Preconditions.checkArgument(this.precision >= 0 && this.precision <= 3);
  }

  //~ Methods ----------------------------------------------------------------

  public SqlNode clone(SqlParserPos pos) {
    return new SqlTimeLiteral(
        (Calendar) value,
        precision,
        hasTimeZone,
        formatString,
        pos);
  }

  public String toString() {
    return "TIME '" + toFormattedString() + "'";
  }

  /**
   * Returns e.g. '03:05:67.456'.
   */
  public String toFormattedString() {
    String result = getTime().toString(formatString);
    final Calendar cal = getCal();
    if (precision > 0) {
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
}

// End SqlTimeLiteral.java
