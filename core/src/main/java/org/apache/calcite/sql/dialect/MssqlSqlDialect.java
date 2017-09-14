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

import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.sql.SqlAbstractDateTimeLiteral;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.ReturnTypes;

/**
 * A <code>SqlDialect</code> implementation for the Microsoft SQL Server
 * database.
 */
public class MssqlSqlDialect extends SqlDialect {
  public static final SqlDialect DEFAULT =
      new MssqlSqlDialect(EMPTY_CONTEXT
          .withDatabaseProduct(DatabaseProduct.MSSQL)
          .withIdentifierQuoteString("["));

  private static final SqlFunction MSSQL_SUBSTRING =
      new SqlFunction("SUBSTRING", SqlKind.OTHER_FUNCTION,
          ReturnTypes.ARG0_NULLABLE_VARYING, null, null,
          SqlFunctionCategory.STRING);

  /** Creates a MssqlSqlDialect. */
  public MssqlSqlDialect(Context context) {
    super(context);
  }

  @Override public void unparseDateTimeLiteral(SqlWriter writer,
      SqlAbstractDateTimeLiteral literal, int leftPrec, int rightPrec) {
    writer.literal("'" + literal.toFormattedString() + "'");
  }

  @Override public void unparseCall(SqlWriter writer, SqlCall call,
      int leftPrec, int rightPrec) {
    if (call.getOperator() == SqlStdOperatorTable.SUBSTRING) {
      if (call.operandCount() != 3) {
        throw new IllegalArgumentException("MSSQL SUBSTRING requires FROM and FOR arguments");
      }
      SqlUtil.unparseFunctionSyntax(MSSQL_SUBSTRING, writer, call);
    } else {
      switch (call.getKind()) {
      case FLOOR:
        if (call.operandCount() != 2) {
          super.unparseCall(writer, call, leftPrec, rightPrec);
          return;
        }
        unparseFloor(writer, call);
        break;

      default:
        super.unparseCall(writer, call, leftPrec, rightPrec);
      }
    }
  }

  /**
   * Unparses datetime floor for Microsoft SQL Server.
   * There is no TRUNC function, so simulate this using calls to CONVERT.
   *
   * @param writer Writer
   * @param call Call
   */
  private void unparseFloor(SqlWriter writer, SqlCall call) {
    SqlLiteral node = call.operand(1);
    TimeUnitRange unit = (TimeUnitRange) node.getValue();

    switch (unit) {
    case YEAR:
      unparseFloorWithUnit(writer, call, 4, "-01-01");
      break;
    case MONTH:
      unparseFloorWithUnit(writer, call, 7, "-01");
      break;
    case WEEK:
      writer.print("CONVERT(DATETIME, CONVERT(VARCHAR(10), "
          + "DATEADD(day, - (6 + DATEPART(weekday, ");
      call.operand(0).unparse(writer, 0, 0);
      writer.print(")) % 7, ");
      call.operand(0).unparse(writer, 0, 0);
      writer.print("), 126))");
      break;
    case DAY:
      unparseFloorWithUnit(writer, call, 10, "");
      break;
    case HOUR:
      unparseFloorWithUnit(writer, call, 13, ":00:00");
      break;
    case MINUTE:
      unparseFloorWithUnit(writer, call, 16, ":00");
      break;
    case SECOND:
      unparseFloorWithUnit(writer, call, 19, ":00");
      break;
    default:
      throw new IllegalArgumentException("MSSQL does not support FLOOR for time unit: "
          + unit);
    }
  }

  private void unparseFloorWithUnit(SqlWriter writer, SqlCall call, int charLen,
      String offset) {
    writer.print("CONVERT");
    SqlWriter.Frame frame = writer.startList("(", ")");
    writer.print("DATETIME, CONVERT(VARCHAR(" + charLen + "), ");
    call.operand(0).unparse(writer, 0, 0);
    writer.print(", 126)");

    if (offset.length() > 0) {
      writer.print("+'" + offset + "'");
    }
    writer.endList(frame);
  }
}

// End MssqlSqlDialect.java
