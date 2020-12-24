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

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * Support unparse logic for DateTimestamp function
 */
public class DateTimestampFormatUtil {
  public void unparseCall(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    SqlCall extractCall = null;
    switch (call.getOperator().getName()) {
    case "WEEKNUMBER_OF_YEAR":
      extractCall = unparseWeekNumber(call.operand(0), DateTimeUnit.WEEK);
      break;
    case "YEARNUMBER_OF_CALENDAR":
      extractCall = unparseWeekNumber(call.operand(0), DateTimeUnit.YEAR);
      break;
    case "MONTHNUMBER_OF_YEAR":
      extractCall = unparseWeekNumber(call.operand(0), DateTimeUnit.MONTH);
      break;
    case "QUARTERNUMBER_OF_YEAR":
      extractCall = unparseWeekNumber(call.operand(0), DateTimeUnit.QUARTER);
      break;
    case "MONTHNUMBER_OF_QUARTER":
      extractCall = unparseMonthNumberQuarter(call, DateTimeUnit.MONTH);
      break;
    case "WEEKNUMBER_OF_MONTH":
      extractCall = unparseMonthNumber(call, DateTimeUnit.DAY);
      break;
    case "DAYOFYEAR":
      extractCall = unparseDayNumber(call);
      break;
    }
    if (null != extractCall) {
      extractCall.unparse(writer, leftPrec, rightPrec);
    }
  }

  /** returns day of the year for given date */
  private SqlCall unparseDayNumber(SqlCall call) {
    return SqlStdOperatorTable.EXTRACT.createCall(SqlParserPos.ZERO,
       getDayOfYearLiteral(),
       call.operand(0));
  }

  SqlLiteral getDayOfYearLiteral() {
    return SqlLiteral.createSymbol(DateTimeUnit.DAYOFYEAR, SqlParserPos.ZERO);
  }

  private SqlCall unparseMonthNumber(SqlCall call, DateTimeUnit dateTimeUnit) {
    SqlCall extractCall = unparseWeekNumber(call.operand(0), dateTimeUnit);
    SqlNumericLiteral quarterLiteral = SqlLiteral.createExactNumeric("7",
        SqlParserPos.ZERO);
    SqlNode[] modOperand = new SqlNode[] { extractCall, quarterLiteral};
    SqlCall divideSqlCall = new SqlBasicCall(SqlStdOperatorTable.DIVIDE, modOperand,
        SqlParserPos.ZERO);
    SqlNode[] floorOperands = new SqlNode[] { divideSqlCall };
    return new SqlBasicCall(SqlStdOperatorTable.FLOOR, floorOperands,
      SqlParserPos.ZERO);
  }

  /**
   * Parse week number based on value.*/
  protected SqlCall unparseWeekNumber(SqlNode operand, DateTimeUnit dateTimeUnit) {
    SqlNode[] operands = new SqlNode[] {
      SqlLiteral.createSymbol(dateTimeUnit, SqlParserPos.ZERO), operand
    };
    return new SqlBasicCall(SqlStdOperatorTable.EXTRACT, operands,
      SqlParserPos.ZERO);
  }

  private SqlCall unparseMonthNumberQuarter(SqlCall call, DateTimeUnit dateTimeUnit) {
    SqlCall extractCall = unparseWeekNumber(call.operand(0), dateTimeUnit);
    SqlNumericLiteral quarterLiteral = SqlLiteral.createExactNumeric("3",
        SqlParserPos.ZERO);
    SqlNode[] modOperand = new SqlNode[] { extractCall, quarterLiteral};
    SqlCall modSqlCall = new SqlBasicCall(SqlStdOperatorTable.MOD, modOperand, SqlParserPos.ZERO);
    SqlNode[] equalsOperands = new SqlNode[] { modSqlCall, SqlLiteral.createExactNumeric("0",
      SqlParserPos.ZERO)};
    SqlCall equalsSqlCall = new SqlBasicCall(SqlStdOperatorTable.EQUALS, equalsOperands,
        SqlParserPos.ZERO);
    SqlNode[] ifOperands = new SqlNode[] { equalsSqlCall, quarterLiteral, modSqlCall };
    return new SqlBasicCall(SqlStdOperatorTable.IF, ifOperands, SqlParserPos.ZERO);
  }

  /**
   * DateTime Unit for supporting different categories of date and time
   */
  private enum DateTimeUnit {
    DAY("DAY"),
    WEEK("WEEK"),
    DAYOFYEAR("DAYOFYEAR"),
    MONTH("MONTH"),
    MONTHOFYEAR("MONTHOFYEAR"),
    QUARTER("QUARTER"),
    YEAR("YEAR");

    String value;

    DateTimeUnit(String value) {
      this.value = value;
    }
  }
}

// End DateTimestampFormatUtil.java
