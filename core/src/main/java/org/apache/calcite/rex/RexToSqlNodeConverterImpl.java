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
package org.apache.calcite.rex;

import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;

import org.checkerframework.checker.nullness.qual.Nullable;

import static java.util.Objects.requireNonNull;

/**
 * Standard implementation of {@link RexToSqlNodeConverter}.
 */
public class RexToSqlNodeConverterImpl implements RexToSqlNodeConverter {
  //~ Instance fields --------------------------------------------------------

  private final RexSqlConvertletTable convertletTable;

  //~ Constructors -----------------------------------------------------------

  public RexToSqlNodeConverterImpl(RexSqlConvertletTable convertletTable) {
    this.convertletTable = convertletTable;
  }

  //~ Methods ----------------------------------------------------------------

  @Override public @Nullable SqlNode convertNode(RexNode node) {
    if (node instanceof RexLiteral) {
      return convertLiteral((RexLiteral) node);
    } else if (node instanceof RexInputRef) {
      return convertInputRef((RexInputRef) node);
    } else if (node instanceof RexCall) {
      return convertCall((RexCall) node);
    }
    return null;
  }

  // implement RexToSqlNodeConverter
  @Override public @Nullable SqlNode convertCall(RexCall call) {
    final RexSqlConvertlet convertlet = convertletTable.get(call);
    if (convertlet != null) {
      return convertlet.convertCall(this, call);
    }

    return null;
  }

  @Override public @Nullable SqlNode convertLiteral(RexLiteral literal) {
    // Numeric
    if (SqlTypeFamily.EXACT_NUMERIC.getTypeNames().contains(
        literal.getTypeName())) {
      return SqlLiteral.createExactNumeric(
          String.valueOf(literal.getValue()),
          SqlParserPos.ZERO);
    }

    if (SqlTypeFamily.APPROXIMATE_NUMERIC.getTypeNames().contains(
        literal.getTypeName())) {
      return SqlLiteral.createApproxNumeric(
          String.valueOf(literal.getValue()),
          SqlParserPos.ZERO);
    }

    // Timestamp
    if (SqlTypeFamily.TIMESTAMP.getTypeNames().contains(
        literal.getTypeName())) {
      return SqlLiteral.createTimestamp(
          requireNonNull(literal.getValueAs(TimestampString.class),
              "literal.getValueAs(TimestampString.class)"),
          0,
          SqlParserPos.ZERO);
    }

    // Date
    if (SqlTypeFamily.DATE.getTypeNames().contains(
        literal.getTypeName())) {
      return SqlLiteral.createDate(
          requireNonNull(literal.getValueAs(DateString.class),
              "literal.getValueAs(DateString.class)"),
          SqlParserPos.ZERO);
    }

    // Time
    if (SqlTypeFamily.TIME.getTypeNames().contains(
        literal.getTypeName())) {
      return SqlLiteral.createTime(
          requireNonNull(literal.getValueAs(TimeString.class),
              "literal.getValueAs(TimeString.class)"),
          0,
          SqlParserPos.ZERO);
    }

    // String
    if (SqlTypeFamily.CHARACTER.getTypeNames().contains(
        literal.getTypeName())) {
      return SqlLiteral.createCharString(
          requireNonNull((NlsString) literal.getValue(), "literal.getValue()")
              .getValue(),
          SqlParserPos.ZERO);
    }

    // Boolean
    if (SqlTypeFamily.BOOLEAN.getTypeNames().contains(
        literal.getTypeName())) {
      return SqlLiteral.createBoolean(
          (Boolean) requireNonNull(literal.getValue(), "literal.getValue()"),
          SqlParserPos.ZERO);
    }

    // Null
    if (SqlTypeFamily.NULL == literal.getTypeName().getFamily()) {
      return SqlLiteral.createNull(SqlParserPos.ZERO);
    }

    return null;
  }

  @Override public @Nullable SqlNode convertInputRef(RexInputRef ref) {
    return null;
  }
}
