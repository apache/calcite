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
    case ORACLE:
      unparseDatetimeFunction(writer, call, "TRUNC", true);
      break;
    case HSQLDB:
      // translate timeUnit literal
      SqlLiteral node = call.operand(1);
      String translatedLit =
          convertToHsqlDb((TimeUnitRange) node.getValue());
      SqlLiteral newNode = SqlLiteral.createCharString(
          translatedLit, null, node.getParserPosition());
      call.setOperand(1, newNode);

      unparseDatetimeFunction(writer, call, "TRUNC", true);
      break;
    case POSTGRESQL:
      unparseDatetimeFunction(writer, call, "DATE_TRUNC", false);
      break;
    default:
      unparseDatetimeDefault(writer, call);
    }
  }

  private void unparseDatetimeDefault(SqlWriter writer, SqlCall call) {
    final SqlWriter.Frame frame = writer.startFunCall(getName());
    call.operand(0).unparse(writer, 0, 100);
    writer.sep("TO");
    call.operand(1).unparse(writer, 100, 0);
    writer.endFunCall(frame);
  }

  private void unparseDatetimeFunction(SqlWriter writer, SqlCall call,
      String funName, Boolean datetimeFirst) {
    final SqlWriter.Frame frame = writer.startFunCall(funName);
    Integer firstOp = datetimeFirst ? 0 : 1;
    Integer secondOp = datetimeFirst ? 1 : 0;

    call.operand(firstOp).unparse(writer, 0, 0);
    writer.sep(",", true);
    call.operand(secondOp).unparse(writer, 0, 0);
    writer.endFunCall(frame);
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
}

// End SqlFloorFunction.java
