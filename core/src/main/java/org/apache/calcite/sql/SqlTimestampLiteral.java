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
import org.apache.calcite.util.TimestampString;

import static com.google.common.base.Preconditions.checkArgument;

import static java.util.Objects.requireNonNull;

/**
 * A SQL literal representing a TIMESTAMP value, for example <code>TIMESTAMP
 * '1969-07-21 03:15'</code>.
 *
 * <p>Create values using {@link SqlLiteral#createTimestamp}.
 */
public class SqlTimestampLiteral extends SqlAbstractDateTimeLiteral {
  //~ Constructors -----------------------------------------------------------

  SqlTimestampLiteral(TimestampString ts, int precision,
      SqlTypeName typeName, SqlParserPos pos) {
    super(ts, false, typeName, precision, pos);
    checkArgument(this.precision >= 0);
    checkArgument(typeName == SqlTypeName.TIMESTAMP
        || typeName == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public SqlTimestampLiteral clone(SqlParserPos pos) {
    return new SqlTimestampLiteral(
        (TimestampString) requireNonNull(value, "value"),
        precision,
        getTypeName(), pos);
  }

  @Override public String toString() {
    return getTypeName() + " '" + toFormattedString() + "'";
  }

  /**
   * Returns e.g. '03:05:67.456'.
   */
  @Override public String toFormattedString() {
    TimestampString ts = getTimestamp();
    if (precision > 0) {
      ts = ts.round(precision);
    }
    return ts.toString(precision);
  }

  @Override public void unparse(
      SqlWriter writer,
      int leftPrec,
      int rightPrec) {
    writer.getDialect().unparseDateTimeLiteral(writer, this, leftPrec, rightPrec);
  }
}
