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

import org.apache.calcite.avatica.util.TimeUnit;
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
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.fun.SqlTrimFunction;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.ReturnTypes;

import java.util.Arrays;
import java.util.List;

import static org.apache.calcite.sql.fun.SqlLibraryOperators.ISNULL;

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
          .withCaseSensitive(false));

  private final boolean emulateNullDirection;
  private static final SqlFunction MSSQL_SUBSTRING =
      new SqlFunction("SUBSTRING", SqlKind.OTHER_FUNCTION,
          ReturnTypes.ARG0_NULLABLE_VARYING, null, null,
          SqlFunctionCategory.STRING);

  private static final List<String> DATEPART_CONVERTER_LIST = Arrays.asList(
      TimeUnit.MINUTE.name(),
      TimeUnit.SECOND.name());

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
      case TRIM:
        unparseTrim(writer, call, leftPrec, rightPrec);
        break;
      case OTHER_FUNCTION:
      case TRUNCATE:
        unparseOtherFunction(writer, call, leftPrec, rightPrec);
        break;
      case CEIL:
        final SqlWriter.Frame ceilFrame = writer.startFunCall("CEILING");
        call.operand(0).unparse(writer, leftPrec, rightPrec);
        writer.endFunCall(ceilFrame);
        break;
      case NVL:
        SqlNode[] extractNodeOperands = new SqlNode[]{call.operand(0), call.operand(1)};
        SqlCall sqlCall = new SqlBasicCall(ISNULL, extractNodeOperands,
                SqlParserPos.ZERO);
        unparseCall(writer, sqlCall, leftPrec, rightPrec);
        break;
      case EXTRACT:
        unparseExtract(writer, call, leftPrec, rightPrec);
        break;
      default:
        super.unparseCall(writer, call, leftPrec, rightPrec);
      }
    }
  }

  private void unparseExtract(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    if (DATEPART_CONVERTER_LIST.contains(call.operand(0).toString())) {
      unparseDatePartCall(writer, call, leftPrec, rightPrec);
    } else {
      final SqlWriter.Frame extractFuncCall = writer.startFunCall(call.operand(0).toString());
      call.operand(1).unparse(writer, leftPrec, rightPrec);
      writer.endFunCall(extractFuncCall);
    }
  }

  private void unparseDatePartCall(SqlWriter writer, SqlCall call,
      int leftPrec, int rightPrec) {
    final SqlWriter.Frame datePartFrame = writer.startFunCall("DATEPART");
    call.operand(0).unparse(writer, leftPrec, rightPrec);
    writer.print(",");
    call.operand(1).unparse(writer, leftPrec, rightPrec);
    writer.endFunCall(datePartFrame);
  }

  public void unparseOtherFunction(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    switch (call.getOperator().getName()) {
    case "LN":
      final SqlWriter.Frame logFrame = writer.startFunCall("LOG");
      call.operand(0).unparse(writer, leftPrec, rightPrec);
      writer.endFunCall(logFrame);
      break;
    case "ROUND":
    case "TRUNCATE":
      final SqlWriter.Frame funcFrame = writer.startFunCall("ROUND");
      for (SqlNode operand : call.getOperandList()) {
        writer.sep(",");
        operand.unparse(writer, leftPrec, rightPrec);
      }
      if (call.operandCount() < 2) {
        writer.sep(",");
        writer.print("0");
      }
      writer.endFunCall(funcFrame);
      break;
    case "CURRENT_TIMESTAMP":
      unparseGetDate(writer);
      break;
    case "CURRENT_DATE":
    case "CURRENT_TIME":
      castGetDateToDateTime(writer, call.getOperator().getName().replace("CURRENT_", ""));
      break;
    default:
      super.unparseCall(writer, call, leftPrec, rightPrec);
    }
  }

  private void castGetDateToDateTime(SqlWriter writer, String timeUnit) {
    final SqlWriter.Frame castDateTimeFunc = writer.startFunCall("CAST");
    unparseGetDate(writer);
    writer.print("AS " + timeUnit);
    writer.endFunCall(castDateTimeFunc);
  }

  private void unparseGetDate(SqlWriter writer) {
    final SqlWriter.Frame currentDateFunc = writer.startFunCall("GETDATE");
    writer.endFunCall(currentDateFunc);
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
