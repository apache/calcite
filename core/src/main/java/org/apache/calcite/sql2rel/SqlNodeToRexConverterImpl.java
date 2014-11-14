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
package org.eigenbase.sql2rel;

import java.math.*;
import java.util.*;

import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.sql.validate.*;
import org.eigenbase.util.*;

import net.hydromatic.avatica.ByteString;

/**
 * Standard implementation of {@link SqlNodeToRexConverter}.
 */
public class SqlNodeToRexConverterImpl implements SqlNodeToRexConverter {
  //~ Instance fields --------------------------------------------------------

  private final SqlRexConvertletTable convertletTable;

  //~ Constructors -----------------------------------------------------------

  SqlNodeToRexConverterImpl(SqlRexConvertletTable convertletTable) {
    this.convertletTable = convertletTable;
  }

  //~ Methods ----------------------------------------------------------------

  public RexNode convertCall(SqlRexContext cx, SqlCall call) {
    final SqlRexConvertlet convertlet = convertletTable.get(call);
    if (convertlet != null) {
      return convertlet.convertCall(cx, call);
    }

    // No convertlet was suitable. (Unlikely, because the standard
    // convertlet table has a fall-back for all possible calls.)
    throw Util.needToImplement(call);
  }

  public RexLiteral convertInterval(
      SqlRexContext cx,
      SqlIntervalQualifier intervalQualifier) {
    RexBuilder rexBuilder = cx.getRexBuilder();

    return rexBuilder.makeIntervalLiteral(intervalQualifier);
  }

  public RexNode convertLiteral(
      SqlRexContext cx,
      SqlLiteral literal) {
    RexBuilder rexBuilder = cx.getRexBuilder();
    RelDataTypeFactory typeFactory = cx.getTypeFactory();
    SqlValidator validator = cx.getValidator();
    final Object value = literal.getValue();
    if (value == null) {
      // Since there is no eq. RexLiteral of SqlLiteral.Unknown we
      // treat it as a cast(null as boolean)
      RelDataType type;
      if (literal.getTypeName() == SqlTypeName.BOOLEAN) {
        type = typeFactory.createSqlType(SqlTypeName.BOOLEAN);
        type = typeFactory.createTypeWithNullability(type, true);
      } else {
        type = validator.getValidatedNodeType(literal);
      }
      return rexBuilder.makeCast(
          type,
          rexBuilder.constantNull());
    }

    BitString bitString;
    SqlIntervalLiteral.IntervalValue intervalValue;
    long l;

    switch (literal.getTypeName()) {
    case DECIMAL:

      // exact number
      BigDecimal bd = (BigDecimal) value;
      return rexBuilder.makeExactLiteral(
          bd,
          literal.createSqlType(typeFactory));
    case DOUBLE:

      // approximate type
      // TODO:  preserve fixed-point precision and large integers
      return rexBuilder.makeApproxLiteral((BigDecimal) value);
    case CHAR:
      return rexBuilder.makeCharLiteral((NlsString) value);
    case BOOLEAN:
      return rexBuilder.makeLiteral(((Boolean) value).booleanValue());
    case BINARY:
      bitString = (BitString) value;
      Util.permAssert(
          (bitString.getBitCount() % 8) == 0,
          "incomplete octet");

      // An even number of hexits (e.g. X'ABCD') makes whole number
      // of bytes.
      ByteString byteString = new ByteString(bitString.getAsByteArray());
      return rexBuilder.makeBinaryLiteral(byteString);
    case SYMBOL:
      return rexBuilder.makeFlag((Enum) value);
    case TIMESTAMP:
      return rexBuilder.makeTimestampLiteral(
          (Calendar) value,
          ((SqlTimestampLiteral) literal).getPrec());
    case TIME:
      return rexBuilder.makeTimeLiteral(
          (Calendar) value,
          ((SqlTimeLiteral) literal).getPrec());
    case DATE:
      return rexBuilder.makeDateLiteral((Calendar) value);

    case INTERVAL_YEAR_MONTH:
      intervalValue =
          (SqlIntervalLiteral.IntervalValue) value;
      l = SqlParserUtil.intervalToMonths(intervalValue);
      return rexBuilder.makeIntervalLiteral(
          BigDecimal.valueOf(l),
          intervalValue.getIntervalQualifier());
    case INTERVAL_DAY_TIME:
      intervalValue =
          (SqlIntervalLiteral.IntervalValue) value;
      l = SqlParserUtil.intervalToMillis(intervalValue);
      return rexBuilder.makeIntervalLiteral(
          BigDecimal.valueOf(l),
          intervalValue.getIntervalQualifier());
    default:
      throw Util.unexpected(literal.getTypeName());
    }
  }
}

// End SqlNodeToRexConverterImpl.java
