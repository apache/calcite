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
package org.apache.calcite.sql.dialect;

import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;

import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CAST;

/**
 * Support unparse logic for Extract function of Decade , Century , DOY , DOW
 */
public class ExtractFunctionFormatUtil {

  public static final String DAY_OF_YEAR = "DOY";
  public static final String DAY_OF_WEEK = "DOW";
  public static final String DECADE = "DECADE";
  public static final String CENTURY = "CENTURY";
  public static final String MILLENNIUM = "MILLENNIUM";
  SqlDialect dialect;
  public SqlCall unparseCall(SqlCall call, SqlDialect dialect) {
    this.dialect = dialect;
    switch (call.operand(0).toString()) {
    case DAY_OF_YEAR:
      return handleExtractWithOperand(call.operand(1), DateTimeUnit.DAYOFYEAR);
    case DAY_OF_WEEK:
      return  handleExtractWithOperand(call.operand(1), DateTimeUnit.DAYOFWEEK);
    case DECADE:
      return handleExtractDeacade(call);
    case CENTURY:
      return handleExtractCentury(call);
    case MILLENNIUM:
      return handleExtractMillennium(call);

    }
    return null;
  }
  private SqlCall handleExtractWithOperand(SqlNode operand, DateTimeUnit dateTimeUnit) {
    return SqlStdOperatorTable.EXTRACT.createCall(SqlParserPos.ZERO,
            SqlLiteral.createSymbol(dateTimeUnit, SqlParserPos.ZERO),
            operand);
  }
  private SqlCall handleExtractDeacade(SqlCall call) {
    SqlCall extractCall =  handleExtractWithOperand(call.operand(1), DateTimeUnit.YEAR);
    SqlNode varcharSqlCall =
            dialect.getCastSpec(
                    new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.VARCHAR, 100));
    SqlCall castCall = CAST.createCall(SqlParserPos.ZERO, extractCall, varcharSqlCall);
    SqlNumericLiteral zeroLiteral = SqlLiteral.createExactNumeric("0",
            SqlParserPos.ZERO);
    SqlNumericLiteral threeLiteral = SqlLiteral.createExactNumeric("3",
            SqlParserPos.ZERO);
    SqlNode[] substrOperand = new SqlNode[] { castCall, zeroLiteral, threeLiteral};
    SqlCall substrCall =  new SqlBasicCall(SqlLibraryOperators.SUBSTR, substrOperand,
            SqlParserPos.ZERO);
    BasicSqlType sqlType = new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.INTEGER);
    return CAST.createCall(SqlParserPos.ZERO, substrCall, SqlTypeUtil.convertTypeToSpec(sqlType));
  }
  private SqlCall handleExtractCentury(SqlCall call) {
    SqlCall extractCall =  handleExtractWithOperand(call.operand(1), DateTimeUnit.YEAR);
    SqlNumericLiteral divideLiteral = SqlLiteral.createExactNumeric("100",
            SqlParserPos.ZERO);
    SqlNode[] substrOperand = new SqlNode[] { extractCall, divideLiteral};
    SqlCall divideCall = new SqlBasicCall(SqlStdOperatorTable.DIVIDE, substrOperand,
            SqlParserPos.ZERO);
    SqlCall ceilCall = new SqlBasicCall(SqlStdOperatorTable.CEIL, new SqlNode[]{divideCall},
            SqlParserPos.ZERO);
    BasicSqlType sqlType = new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.INTEGER);
    return CAST.createCall(SqlParserPos.ZERO, ceilCall, SqlTypeUtil.convertTypeToSpec(sqlType));
  }
  private SqlCall handleExtractMillennium(SqlCall call) {
    SqlCall extractCall =  handleExtractWithOperand(call.operand(1), DateTimeUnit.YEAR);
    SqlNode varcharSqlCall =
            dialect.getCastSpec(
                    new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.VARCHAR, 100));
    SqlCall castCall = CAST.createCall(SqlParserPos.ZERO, extractCall, varcharSqlCall);
    SqlNumericLiteral zeroLiteral = SqlLiteral.createExactNumeric("0",
            SqlParserPos.ZERO);
    SqlNumericLiteral threeLiteral = SqlLiteral.createExactNumeric("1",
            SqlParserPos.ZERO);
    SqlNode[] substrOperand = new SqlNode[] { castCall, zeroLiteral, threeLiteral};
    SqlCall substrCall =  new SqlBasicCall(SqlLibraryOperators.SUBSTR, substrOperand,
            SqlParserPos.ZERO);
    BasicSqlType sqlType = new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.INTEGER);
    return CAST.createCall(SqlParserPos.ZERO, substrCall, SqlTypeUtil.convertTypeToSpec(sqlType));
  }
  /**
   * DateTime Unit for supporting different categories of date and time
   */
  private enum DateTimeUnit {
    DAYOFYEAR("DAYOFYEAR"),
    DAYOFWEEK("DAYOFWEEK"),
    DECADE("DECADE"),
    YEAR("YEAR");

    String value;

    DateTimeUnit(String value) {
      this.value = value;
    }
  }
}

// End ExtractFunctionFormatUtil.java
