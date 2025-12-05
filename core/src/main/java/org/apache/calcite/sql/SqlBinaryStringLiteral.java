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
import org.apache.calcite.util.BitString;
import org.apache.calcite.util.Util;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * A binary (or hexadecimal) string literal.
 *
 * <p>The {@link #value} field is a {@link BitString} and {@link #getTypeName()}
 * is {@link SqlTypeName#BINARY}.
 */
public class SqlBinaryStringLiteral extends SqlAbstractStringLiteral {

  //~ Constructors -----------------------------------------------------------

  protected SqlBinaryStringLiteral(
      BitString val,
      SqlParserPos pos) {
    super(val, SqlTypeName.BINARY, pos);
  }

  //~ Methods ----------------------------------------------------------------

  /** Returns the underlying {@link BitString}.
   *
   * @deprecated Use {@link SqlLiteral#getValueAs getValueAs(BitString.class)}
   */
  @Deprecated // to be removed before 2.0
  public BitString getBitString() {
    return getValueNonNull();
  }

  private BitString getValueNonNull() {
    return (BitString) requireNonNull(value, "value");
  }

  @Override public SqlBinaryStringLiteral clone(SqlParserPos pos) {
    return new SqlBinaryStringLiteral(getValueNonNull(), pos);
  }

  @Override public void unparse(
      SqlWriter writer,
      int leftPrec,
      int rightPrec) {
    writer.literal("X'" + getValueNonNull().toHexString() + "'");
  }

  @Override protected SqlAbstractStringLiteral concat1(List<SqlLiteral> literals) {
    return new SqlBinaryStringLiteral(
        BitString.concat(
            Util.transform(literals,
                literal -> literal.getValueAs(BitString.class))),
        literals.get(0).getParserPosition());
  }
}
