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

import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Litmus;

import java.util.Objects;

/**
 * A SQL literal representing a time interval.
 *
 * <p>Examples:
 *
 * <ul>
 * <li>INTERVAL '1' SECOND</li>
 * <li>INTERVAL '1:00:05.345' HOUR</li>
 * <li>INTERVAL '3:4' YEAR TO MONTH</li>
 * </ul>
 *
 * <p>YEAR/MONTH intervals are not implemented yet.</p>
 *
 * <p>The interval string, such as '1:00:05.345', is not parsed yet.</p>
 */
public class SqlIntervalLiteral extends SqlLiteral {
  //~ Constructors -----------------------------------------------------------

  protected SqlIntervalLiteral(
      int sign,
      String intervalStr,
      SqlIntervalQualifier intervalQualifier,
      SqlTypeName sqlTypeName,
      SqlParserPos pos) {
    this(
        new IntervalValue(intervalQualifier, sign, intervalStr),
        sqlTypeName,
        pos);
  }

  private SqlIntervalLiteral(
      IntervalValue intervalValue,
      SqlTypeName sqlTypeName,
      SqlParserPos pos) {
    super(
        intervalValue,
        sqlTypeName,
        pos);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public SqlIntervalLiteral clone(SqlParserPos pos) {
    return new SqlIntervalLiteral((IntervalValue) value, getTypeName(), pos);
  }

  public void unparse(
      SqlWriter writer,
      int leftPrec,
      int rightPrec) {
    writer.getDialect().unparseSqlIntervalLiteral(writer, this, leftPrec, rightPrec);
  }

  @SuppressWarnings("deprecation")
  public int signum() {
    return ((IntervalValue) value).signum();
  }

  //~ Inner Classes ----------------------------------------------------------

  /**
   * A Interval value.
   */
  public static class IntervalValue {
    private final SqlIntervalQualifier intervalQualifier;
    private final String intervalStr;
    private final int sign;

    /**
     * Creates an interval value.
     *
     * @param intervalQualifier Interval qualifier
     * @param sign              Sign (+1 or -1)
     * @param intervalStr       Interval string
     */
    IntervalValue(
        SqlIntervalQualifier intervalQualifier,
        int sign,
        String intervalStr) {
      assert (sign == -1) || (sign == 1);
      assert intervalQualifier != null;
      assert intervalStr != null;
      this.intervalQualifier = intervalQualifier;
      this.sign = sign;
      this.intervalStr = intervalStr;
    }

    public boolean equals(Object obj) {
      if (!(obj instanceof IntervalValue)) {
        return false;
      }
      IntervalValue that = (IntervalValue) obj;
      return this.intervalStr.equals(that.intervalStr)
          && (this.sign == that.sign)
          && this.intervalQualifier.equalsDeep(that.intervalQualifier,
              Litmus.IGNORE);
    }

    public int hashCode() {
      return Objects.hash(sign, intervalStr, intervalQualifier);
    }

    public SqlIntervalQualifier getIntervalQualifier() {
      return intervalQualifier;
    }

    public String getIntervalLiteral() {
      return intervalStr;
    }

    public int getSign() {
      return sign;
    }

    public int signum() {
      for (int i = 0; i < intervalStr.length(); i++) {
        char ch = intervalStr.charAt(i);
        if (ch >= '1' && ch <= '9') {
          // If non zero return sign.
          return getSign();
        }
      }
      return 0;
    }

    public String toString() {
      return intervalStr;
    }
  }
}

// End SqlIntervalLiteral.java
