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
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.SqlAbstractDateTimeLiteral;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.fun.SqlTrimFunction;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlConformanceEnum;

/**
 * A <code>SqlDialect</code> implementation for the Microsoft SQL Server
 * database.
 */
public class MssqlSqlDialect extends SqlDialect {
  public static final SqlDialect DEFAULT =
      new MssqlSqlDialect(EMPTY_CONTEXT
          .withDatabaseProduct(DatabaseProduct.MSSQL)
          .withNullCollation(NullCollation.LOW)
          .withIdentifierQuoteString("[")
          .withCaseSensitive(false)
          .withConformance(SqlConformanceEnum.SQL_SERVER_2008));

  private final boolean emulateNullDirection;
  private static final SqlFunction MSSQL_SUBSTRING =
      new SqlFunction("SUBSTRING", SqlKind.OTHER_FUNCTION,
          ReturnTypes.ARG0_NULLABLE_VARYING, null, null,
          SqlFunctionCategory.STRING);

  /** Creates a MssqlSqlDialect. */
  public MssqlSqlDialect(Context context) {
    super(context);
    emulateNullDirection = true;
  }

  @Override public SqlNode emulateNullDirection(
          SqlNode node, boolean nullsFirst, boolean desc) {
    if (emulateNullDirection) {
      return emulateNullDirectionWithIsNull(node, nullsFirst, desc);
    }
    return null;
  }

  @Override protected SqlNode emulateNullDirectionWithIsNull(
          SqlNode node, boolean nullsFirst, boolean desc) {
    if (nullCollation.isDefaultOrder(nullsFirst, desc)) {
      return null;
    }
    String caseThenOperand = (nullsFirst && desc) ? "0" : "1";
    String caseElseOperand = caseThenOperand.equals("1") ? "0" : "1";
    node = new SqlCase(
            SqlParserPos.ZERO, null,
            SqlNodeList.of(SqlStdOperatorTable.IS_NULL.createCall(SqlParserPos.ZERO, node)),
            SqlNodeList.of(SqlLiteral.createExactNumeric(caseThenOperand, SqlParserPos.ZERO)),
            SqlLiteral.createExactNumeric(caseElseOperand, SqlParserPos.ZERO)
    );
    return node;
  }

  @Override public void unparseDateTimeLiteral(SqlWriter writer,
      SqlAbstractDateTimeLiteral literal, int leftPrec, int rightPrec) {
    writer.literal("'" + literal.toFormattedString() + "'");
  }

  @Override public void unparseCall(SqlWriter writer, SqlCall call,
      int leftPrec, int rightPrec) {
    switch (call.getKind()) {
    case FLOOR:
      if (call.operandCount() != 2) {
        super.unparseCall(writer, call, leftPrec, rightPrec);
        return;
      }
      unparseFloor(writer, call);
      break;
    case TRIM:
      unparseTrim(writer, call, leftPrec, rightPrec);
      break;
    case SUBSTRING:
      unparseSubstring(writer, call, leftPrec, rightPrec);
      break;
    case CONCAT:
      final SqlWriter.Frame concatFrame = writer.startFunCall("CONCAT");
      for (SqlNode operand : call.getOperandList()) {
        writer.sep(",");
        operand.unparse(writer, leftPrec, rightPrec);
      }
      writer.endFunCall(concatFrame);
      break;
    case OVER:
      call.operand(0).unparse(writer, leftPrec, rightPrec);
      if (((SqlWindow) call.operand(1)).getPartitionList().size() == 0
            && ((SqlWindow) call.operand(1)).getOrderList().size() == 0) {
        final SqlWriter.Frame overFrame = writer.startFunCall("OVER");
        writer.endFunCall(overFrame);
      } else if (((SqlWindow) call.operand(1)).getOrderList().size() == 0) {
        final SqlWriter.Frame overFrame = writer.startFunCall("OVER");
        writer.print("PARTITION BY ");
        ((SqlWindow) call.operand(1)).getPartitionList().unparse(writer, leftPrec, rightPrec);
        writer.endFunCall(overFrame);
      } else {
        writer.print("OVER ");
        call.operand(1).unparse(writer, leftPrec, rightPrec);
      }
      break;
    case OTHER:
      unparseOtherFunction(writer, call, leftPrec, rightPrec);
      break;
    default:
      super.unparseCall(writer, call, leftPrec, rightPrec);
    }
  }

  private void unparseOtherFunction(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    switch (call.getOperator().getName()) {
    case "DAYOFMONTH":
      final SqlWriter.Frame frame = writer.startFunCall("DAY");
      call.operand(0).unparse(writer, leftPrec, rightPrec);
      writer.endFunCall(frame);
    }
  }

  private void unparseSubstring(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    if (call.operandCount() == 3) {
      if (call.operand(0) instanceof SqlBasicCall
              && ((SqlBasicCall) call.operand(0)).getOperator() == SqlLibraryOperators.FORMAT) {
        SqlNode literalVarchar = getCastSpec(
                new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.VARCHAR));
        SqlCall castCall = SqlStdOperatorTable.CAST.createCall(
                SqlParserPos.ZERO, ((SqlBasicCall) call.operand(0)).operand(1), literalVarchar);
        SqlCall substrCall = SqlStdOperatorTable.SUBSTRING.createCall(
                SqlParserPos.ZERO, castCall, call.operand(1), call.operand(2));
        SqlUtil.unparseFunctionSyntax(MSSQL_SUBSTRING, writer, substrCall);
      } else {
        SqlUtil.unparseFunctionSyntax(MSSQL_SUBSTRING, writer, call);
      }
    } else {
      SqlNumericLiteral forNumber = SqlLiteral.createExactNumeric(
              "2147483647", SqlParserPos.ZERO);
      SqlNode substringCall = SqlStdOperatorTable.SUBSTRING.createCall(
              SqlParserPos.ZERO, call.operand(0), call.operand(1), forNumber);
      substringCall.unparse(writer, leftPrec, rightPrec);
    }
  }

  @Override public boolean supportsCharSet() {
    return false;
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
      throw new IllegalArgumentException("MSSQL does not support FLOOR for time unit: " + unit);
    }
  }

  @Override public void unparseSqlDatetimeArithmetic(SqlWriter writer,
      SqlCall call, SqlKind sqlKind, int leftPrec, int rightPrec) {

    final SqlWriter.Frame frame = writer.startFunCall("DATEADD");
    SqlNode operand = call.operand(1);
    if (operand instanceof SqlIntervalLiteral) {
      //There is no DATESUB method available, so change the sign.
      unparseSqlIntervalLiteralMssql(
          writer, (SqlIntervalLiteral) operand, sqlKind == SqlKind.MINUS ? -1 : 1);
    } else {
      operand.unparse(writer, leftPrec, rightPrec);
    }
    writer.sep(",", true);

    call.operand(0).unparse(writer, leftPrec, rightPrec);
    writer.endList(frame);
  }

  @Override public void unparseSqlIntervalQualifier(SqlWriter writer,
      SqlIntervalQualifier qualifier, RelDataTypeSystem typeSystem) {
    switch (qualifier.timeUnitRange) {
    case YEAR:
    case QUARTER:
    case MONTH:
    case WEEK:
    case DAY:
    case HOUR:
    case MINUTE:
    case SECOND:
    case MILLISECOND:
    case MICROSECOND:
      final String timeUnit = qualifier.timeUnitRange.startUnit.name();
      writer.keyword(timeUnit);
      break;
    default:
      throw new AssertionError("Unsupported type: " + qualifier.timeUnitRange);
    }

    if (null != qualifier.timeUnitRange.endUnit) {
      throw new AssertionError("End unit is not supported now: "
          + qualifier.timeUnitRange.endUnit);
    }
  }

  @Override public void unparseSqlIntervalLiteral(
      SqlWriter writer, SqlIntervalLiteral literal, int leftPrec, int rightPrec) {
    unparseSqlIntervalLiteralMssql(writer, literal, 1);
  }

  private void unparseSqlIntervalLiteralMssql(
      SqlWriter writer, SqlIntervalLiteral literal, int sign) {
    final SqlIntervalLiteral.IntervalValue interval =
        (SqlIntervalLiteral.IntervalValue) literal.getValue();
    unparseSqlIntervalQualifier(writer, interval.getIntervalQualifier(),
        RelDataTypeSystem.DEFAULT);
    writer.sep(",", true);
    if (interval.getSign() * sign == -1) {
      writer.print("-");
    }
    writer.literal(literal.getValue().toString());
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

  /**
   * For usage of TRIM in MSSQL
   */
  private void unparseTrim(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    switch (((SqlLiteral) call.operand(0)).getValueAs(SqlTrimFunction.Flag.class)) {
    case BOTH:
      final SqlWriter.Frame frame = writer.startFunCall(call.getOperator().getName());
      call.operand(1).unparse(writer, leftPrec, rightPrec);
      writer.sep("FROM");
      call.operand(2).unparse(writer, leftPrec, rightPrec);
      writer.endFunCall(frame);
      break;
    case LEADING:
      unparseCall(writer, SqlLibraryOperators.LTRIM.
          createCall(SqlParserPos.ZERO, new SqlNode[]{call.operand(2)}), leftPrec, rightPrec);
      break;
    case TRAILING:
      unparseCall(writer, SqlLibraryOperators.RTRIM.
          createCall(SqlParserPos.ZERO, new SqlNode[]{call.operand(2)}), leftPrec, rightPrec);
      break;
    default:
      super.unparseCall(writer, call, leftPrec, rightPrec);
    }
  }

}

// End MssqlSqlDialect.java
