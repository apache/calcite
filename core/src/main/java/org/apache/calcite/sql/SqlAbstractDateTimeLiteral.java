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
import org.apache.calcite.util.TimestampString;

import static java.util.Objects.requireNonNull;

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
public abstract class SqlAbstractDateTimeLiteral extends SqlLiteral {
  //~ Instance fields --------------------------------------------------------

  protected final boolean hasTimeZone;
  protected final int precision;

  //~ Constructors -----------------------------------------------------------

  /**
   * Constructs a datetime literal.
   */
  protected SqlAbstractDateTimeLiteral(Object d, boolean tz,
      SqlTypeName typeName, int precision, SqlParserPos pos) {
    super(d, typeName, pos);
    this.hasTimeZone = tz;
    this.precision = precision;
  }

  //~ Methods ----------------------------------------------------------------

  /** Converts this literal to a {@link TimestampString}. */
  protected TimestampString getTimestamp() {
    return (TimestampString) requireNonNull(value, "value");
  }

  public int getPrec() {
    return precision;
  }

  /**
   * Returns e.g. <code>DATE '1969-07-21'</code>.
   */
  @Override public abstract String toString();

  /**
   * Returns e.g. <code>1969-07-21</code>.
   */
  public abstract String toFormattedString();

  @Override public RelDataType createSqlType(RelDataTypeFactory typeFactory) {
    return typeFactory.createSqlType(
        getTypeName(),
        getPrec());
  }

  @Override public void unparse(
      SqlWriter writer,
      int leftPrec,
      int rightPrec) {
    writer.literal(this.toString());
  }
}
