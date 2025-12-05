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
import org.apache.calcite.util.Util;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Pure;

import java.math.BigDecimal;

import static java.util.Objects.requireNonNull;

/**
 * A numeric SQL literal.
 */
public class SqlNumericLiteral extends SqlLiteral {
  //~ Instance fields --------------------------------------------------------

  private final @Nullable Integer prec;
  private final @Nullable Integer scale;
  private final boolean exact;

  //~ Constructors -----------------------------------------------------------

  protected SqlNumericLiteral(
      BigDecimal value,
      @Nullable Integer prec,
      @Nullable Integer scale,
      boolean exact,
      SqlParserPos pos) {
    super(
        value,
        exact ? SqlTypeName.DECIMAL : SqlTypeName.DOUBLE,
        pos);
    this.prec = prec;
    this.scale = scale;
    this.exact = exact;
  }

  //~ Methods ----------------------------------------------------------------

  private BigDecimal getValueNonNull() {
    return (BigDecimal) requireNonNull(value, "value");
  }

  public @Nullable Integer getPrec() {
    return prec;
  }

  @Pure
  public @Nullable Integer getScale() {
    return scale;
  }

  public boolean isExact() {
    return exact;
  }

  @Override public SqlNumericLiteral clone(SqlParserPos pos) {
    return new SqlNumericLiteral(getValueNonNull(), getPrec(), getScale(),
        exact, pos);
  }

  @Override public void unparse(
      SqlWriter writer,
      int leftPrec,
      int rightPrec) {
    writer.getDialect().unparseNumericLiteral(writer, toValue(), leftPrec, rightPrec);
  }

  @Override public String toValue() {
    final BigDecimal bd = getValueNonNull();
    if (exact) {
      return bd.toPlainString();
    }
    return Util.toScientificNotation(bd);
  }

  @Override public RelDataType createSqlType(RelDataTypeFactory typeFactory) {
    if (exact) {
      int scaleValue = requireNonNull(scale, "scale");
      if (0 == scaleValue) {
        try {
          BigDecimal bd = getValueNonNull();
          SqlTypeName result;
          // Will throw if the number cannot be represented as a long.
          long l = bd.longValueExact();
          if ((l >= Integer.MIN_VALUE) && (l <= Integer.MAX_VALUE)) {
            result = SqlTypeName.INTEGER;
          } else {
            result = SqlTypeName.BIGINT;
          }
          return typeFactory.createSqlType(result);
        } catch (ArithmeticException ex) {
          // This indicates that the value does not fit in any integer type.
          // Fallback to DECIMAL.
        }
      }
      // else we have a decimal
      return typeFactory.createSqlType(
          SqlTypeName.DECIMAL,
          requireNonNull(prec, "prec"),
          scaleValue);
    }

    // else we have a FLOAT, REAL or DOUBLE.  make them all DOUBLE for
    // now.
    return typeFactory.createSqlType(SqlTypeName.DOUBLE);
  }

  public boolean isInteger() {
    return scale != null && 0 == scale.intValue();
  }
}
