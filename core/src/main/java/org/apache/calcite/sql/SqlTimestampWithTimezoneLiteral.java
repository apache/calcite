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
import org.apache.calcite.util.TimestampWithTimeZoneString;

import com.google.common.base.Preconditions;

import static java.util.Objects.requireNonNull;

/**
 * A SQL literal representing a TIMESTAMP WITH TIME ZONE value.
 *
 * <p>Create values using {@link SqlLiteral#createTimestampWithTimeZone}.
 */
public class SqlTimestampWithTimezoneLiteral extends SqlAbstractDateTimeLiteral {
  //~ Constructors -----------------------------------------------------------


  SqlTimestampWithTimezoneLiteral(TimestampWithTimeZoneString ts, int precision,
      boolean hasTimeZone, SqlParserPos pos) {
    super(ts, hasTimeZone, SqlTypeName.TIMESTAMP_WITH_TIME_ZONE, precision, pos);
    Preconditions.checkArgument(this.precision >= 0);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public SqlTimestampWithTimezoneLiteral clone(SqlParserPos pos) {
    return new SqlTimestampWithTimezoneLiteral(
        (TimestampWithTimeZoneString) requireNonNull(value, "value"),
        precision,
        hasTimeZone, pos);
  }

  @Override public String toString() {
    return "TIMESTAMP '" + toFormattedString() + "'";
  }

  @Override public String toFormattedString() {
    TimestampWithTimeZoneString ts = getTimestampWithTimeZoneString();
    return ts.toString();
  }

  @Override public void unparse(
      SqlWriter writer,
      int leftPrec,
      int rightPrec) {
    writer.getDialect().unparseDateTimeLiteral(writer, this, leftPrec, rightPrec);
  }
}
