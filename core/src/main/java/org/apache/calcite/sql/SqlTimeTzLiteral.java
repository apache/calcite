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
import org.apache.calcite.util.TimeWithTimeZoneString;

import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * A SQL literal representing a TIME WITH TIME ZONE value, for example <code>TIME WITH TIME ZONE
 * '14:33:44.567 GMT+08'</code>.
 *
 * <p>Create values using {@link SqlLiteral#createTime}.
 */
public class SqlTimeTzLiteral extends SqlAbstractDateTimeLiteral {
  //~ Constructors -----------------------------------------------------------

  SqlTimeTzLiteral(TimeWithTimeZoneString t, int precision,
                             SqlParserPos pos) {
    super(t, true, SqlTypeName.TIME_TZ, precision, pos);
    checkArgument(this.precision >= 0);
  }

  //~ Methods ----------------------------------------------------------------

  /** Converts this literal to a {@link TimeWithTimeZoneString}. */
  protected TimeWithTimeZoneString getTime() {
    return (TimeWithTimeZoneString) Objects.requireNonNull(value, "value");
  }

  @Override public SqlTimeTzLiteral clone(SqlParserPos pos) {
    return new SqlTimeTzLiteral(getTime(), precision, pos);
  }

  @Override public String toString() {
    return "TIME WITH TIME ZONE '" + toFormattedString() + "'";
  }

  /**
   * Returns e.g. '03:05:67.456 GMT+00:00'.
   */
  @Override public String toFormattedString() {
    return getTime().toString(precision);
  }

  @Override public void unparse(
      SqlWriter writer,
      int leftPrec,
      int rightPrec) {
    writer.getDialect().unparseDateTimeLiteral(writer, this, leftPrec, rightPrec);
  }
}
