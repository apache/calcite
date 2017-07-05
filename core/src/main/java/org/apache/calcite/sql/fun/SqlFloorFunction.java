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
package org.apache.calcite.sql.fun;

import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.validate.SqlMonotonicity;

import com.google.common.base.Preconditions;

/**
 * Definition of the "FLOOR" and "CEIL" built-in SQL functions.
 */
public class SqlFloorFunction extends SqlMonotonicUnaryFunction {
  //~ Constructors -----------------------------------------------------------

  public SqlFloorFunction(SqlKind kind) {
    super(kind.name(), kind, ReturnTypes.ARG0_OR_EXACT_NO_SCALE, null,
        OperandTypes.or(OperandTypes.NUMERIC_OR_INTERVAL,
            OperandTypes.sequence(
                "'" + kind + "(<DATE> TO <TIME_UNIT>)'\n"
                + "'" + kind + "(<TIME> TO <TIME_UNIT>)'\n"
                + "'" + kind + "(<TIMESTAMP> TO <TIME_UNIT>)'",
                OperandTypes.DATETIME,
                OperandTypes.ANY)),
        SqlFunctionCategory.NUMERIC);
    Preconditions.checkArgument(kind == SqlKind.FLOOR || kind == SqlKind.CEIL);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public SqlMonotonicity getMonotonicity(SqlOperatorBinding call) {
    // Monotonic iff its first argument is, but not strict.
    return call.getOperandMonotonicity(0).unstrict();
  }

  @Override public void unparse(SqlWriter writer, SqlCall call, int leftPrec,
      int rightPrec) {
    if (call.operandCount() == 2) {
      unparseDatetime(writer, call);
    } else {
      unparseNumeric(writer, call);
    }
  }

  private void unparseNumeric(SqlWriter writer, SqlCall call) {
    final SqlWriter.Frame frame = writer.startFunCall(getName());
    call.operand(0).unparse(writer, 0, 0);
    writer.endFunCall(frame);
  }

  private void unparseDatetime(SqlWriter writer, SqlCall call) {
    // FLOOR (not CEIL) is the only function that works in most dialects
    if (kind != SqlKind.FLOOR) {
      unparseDatetimeDefault(writer, call);
      return;
    }

    switch (writer.getDialect().getDatabaseProduct()) {
    case UNKNOWN:
    case CALCITE:
      unparseDatetimeDefault(writer, call);
      return;
    }

    final SqlLiteral timeUnitNode = call.operand(1);
    final TimeUnitRange timeUnit = timeUnitNode.getValueAs(TimeUnitRange.class);

    final SqlCall call2;
    switch (writer.getDialect().getDatabaseProduct()) {
    case ORACLE:
      call2 = replaceTimeUnitOperand(call, timeUnit.name(), timeUnitNode.getParserPosition());
      unparseDatetimeFunction(writer, call2, "TRUNC", true);
      break;
    case HSQLDB:
      final String translatedLit = convertToHsqlDb(timeUnit);
      call2 = replaceTimeUnitOperand(call, translatedLit, timeUnitNode.getParserPosition());
      unparseDatetimeFunction(writer, call2, "TRUNC", true);
      break;
    case POSTGRESQL:
      call2 = replaceTimeUnitOperand(call, timeUnit.name(), timeUnitNode.getParserPosition());
      unparseDatetimeFunction(writer, call2, "DATE_TRUNC", false);
      break;
    case MSSQL:
      unparseDatetimeMssql(writer, call);
      break;
    case MYSQL:
      unparseDatetimeMysql(writer, call);
      break;
    default:
      unparseDatetimeDefault(writer, call);
    }
  }

  /**
   * Copies a {@link SqlCall}, replacing the time unit operand with the given
   * literal.
   *
   * @param call Call
   * @param literal Literal to replace time unit with
   * @param pos Parser position
   * @return Modified call
   */
  private SqlCall replaceTimeUnitOperand(SqlCall call, String literal, SqlParserPos pos) {
    SqlLiteral literalNode = SqlLiteral.createCharString(literal, null, pos);
    return call.getOperator().createCall(call.getFunctionQuantifier(), pos,
        call.getOperandList().get(0), literalNode);
  }

  /**
   * Default datetime unparse method if the specific dialect was not matched.
   *
   * @param writer SqlWriter
   * @param call SqlCall
   */
  private void unparseDatetimeDefault(SqlWriter writer, SqlCall call) {
    final SqlWriter.Frame frame = writer.startFunCall(getName());
    call.operand(0).unparse(writer, 0, 100);
    writer.sep("TO");
    call.operand(1).unparse(writer, 100, 0);
    writer.endFunCall(frame);
  }

  /**
   * Most dialects that natively support datetime floor will use this.
   * In those cases the call will look like TRUNC(datetime, 'year').
   *
   * @param writer SqlWriter
   * @param call SqlCall
   * @param funName Name of the sql function to call
   * @param datetimeFirst Specify the order of the datetime &amp; timeUnit
   * arguments
   */
  private void unparseDatetimeFunction(SqlWriter writer, SqlCall call,
      String funName, Boolean datetimeFirst) {
    final SqlWriter.Frame frame = writer.startFunCall(funName);
    int firstOpIndex = datetimeFirst ? 0 : 1;
    int secondOpIndex = datetimeFirst ? 1 : 0;
    call.operand(firstOpIndex).unparse(writer, 0, 0);
    writer.sep(",", true);
    call.operand(secondOpIndex).unparse(writer, 0, 0);
    writer.endFunCall(frame);
  }

  /**
   * Unparse datetime floor for MS SQL. There is no TRUNC function, so simulate this
   * using calls to CONVERT.
   *
   * @param writer SqlWriter
   * @param call SqlCall
   */
  private void unparseDatetimeMssql(SqlWriter writer, SqlCall call) {
    SqlLiteral node = call.operand(1);
    TimeUnitRange unit = (TimeUnitRange) node.getValue();

    switch (unit) {
    case YEAR:
      unparseMssql(writer, call, 4, "-01-01");
      break;
    case MONTH:
      unparseMssql(writer, call, 7, "-01");
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
      unparseMssql(writer, call, 10, "");
      break;
    case HOUR:
      unparseMssql(writer, call, 13, ":00:00");
      break;
    case MINUTE:
      unparseMssql(writer, call, 16, ":00");
      break;
    case SECOND:
      unparseMssql(writer, call, 19, ":00");
      break;
    default:
      throw new AssertionError("MSSQL does not support FLOOR for time unit: "
          + unit);
    }
  }

  private void unparseMssql(SqlWriter writer, SqlCall call, Integer charLen, String offset) {
    writer.print("CONVERT");
    SqlWriter.Frame frame = writer.startList("(", ")");
    writer.print("DATETIME, CONVERT(VARCHAR(" + charLen.toString() + "), ");
    call.operand(0).unparse(writer, 0, 0);
    writer.print(", 126)");

    if (offset.length() > 0) {
      writer.print("+'" + offset + "'");
    }
    writer.endList(frame);
  }

  private static String convertToHsqlDb(TimeUnitRange unit) {
    switch (unit) {
    case YEAR:
      return "YYYY";
    case MONTH:
      return "MM";
    case DAY:
      return "DD";
    case WEEK:
      return "WW";
    case HOUR:
      return "HH24";
    case MINUTE:
      return "MI";
    case SECOND:
      return "SS";
    default:
      throw new AssertionError("could not convert time unit to an HsqlDb equivalent: "
        + unit);
    }
  }

  /**
   * Unparse datetime floor for MySQL. There is no TRUNC function, so simulate this
   * using calls to DATE_FORMAT.
   *
   * @param writer SqlWriter
   * @param call SqlCall
   */
  private void unparseDatetimeMysql(SqlWriter writer, SqlCall call) {
    SqlLiteral node = call.operand(1);
    TimeUnitRange unit = (TimeUnitRange) node.getValue();

    if (unit == TimeUnitRange.WEEK) {
      writer.print("STR_TO_DATE");
      SqlWriter.Frame frame = writer.startList("(", ")");

      writer.print("DATE_FORMAT(");
      call.operand(0).unparse(writer, 0, 0);
      writer.print(", '%x%v-1'), '%x%v-%w'");
      writer.endList(frame);
      return;
    }

    String format;
    switch (unit) {
    case YEAR:
      format = "%Y-01-01";
      break;
    case MONTH:
      format = "%Y-%m-01";
      break;
    case DAY:
      format = "%Y-%m-%d";
      break;
    case HOUR:
      format = "%Y-%m-%d %k:00:00";
      break;
    case MINUTE:
      format = "%Y-%m-%d %k:%i:00";
      break;
    case SECOND:
      format = "%Y-%m-%d %k:%i:%s";
      break;
    default:
      throw new AssertionError("MYSQL does not support FLOOR for time unit: "
          + unit);
    }

    writer.print("DATE_FORMAT");
    SqlWriter.Frame frame = writer.startList("(", ")");
    call.operand(0).unparse(writer, 0, 0);
    writer.sep(",", true);
    writer.print("'" + format + "'");
    writer.endList(frame);
  }
}

// End SqlFloorFunction.java
