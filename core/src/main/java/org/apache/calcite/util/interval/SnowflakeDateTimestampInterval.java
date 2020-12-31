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
package org.apache.calcite.util.interval;

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * Handle Snowflake date timestamp interval
 */
public class SnowflakeDateTimestampInterval {
  public boolean handlePlus(SqlWriter writer, SqlCall call,
                            int leftPrec, int rightPrec) {
    if (call.operand(1) instanceof SqlBasicCall
        && ((SqlBasicCall) call.operand(1)).getOperandList().get(0) instanceof SqlIntervalLiteral
        && SqlKind.PLUS != ((SqlBasicCall) call.operand(1)).getOperator().getKind()) {
      unparseDateAddForInterval(writer, call, leftPrec, rightPrec);
      return true;
    } else {
      return handleMinus(writer, call, leftPrec, rightPrec, "");
    }
    // 11:40 31 Dec comment
    /*else if ("DATE_ADD".equals(call.getOperator().toString())) {
      return handleMinus(writer, call, leftPrec, rightPrec, "");
//      super.unparseCall(writer, call, leftPrec, rightPrec);
    } else {
      return handleMinus(writer, call, leftPrec, rightPrec, "");
    }*/
  }

  private void unparseDateAddForInterval(SqlWriter writer, SqlCall call,
                                         int leftPrec, int rightPrec) {
    String timeUnit = ((SqlIntervalLiteral.IntervalValue)
        ((SqlIntervalLiteral) ((SqlBasicCall) call.operand(1)).operand(0)).getValue()).
        getIntervalQualifier().timeUnitRange.toString();
    SqlCall multipleCall = unparseMultipleInterval(call);
    SqlNode[] sqlNodes = new SqlNode[]{SqlLiteral.createSymbol(TimeUnit.valueOf(timeUnit),
        SqlParserPos.ZERO), multipleCall, call.operand(0)};
    unparseDateAdd(writer, leftPrec, rightPrec, sqlNodes);
  }

  public SqlCall unparseMultipleInterval(SqlCall call) {
    SqlNode[] timesNodes = null;
    if (call.operand(1) instanceof SqlBasicCall) {
      timesNodes = new SqlNode[] {
          SqlLiteral.createCharString(
              ((SqlIntervalLiteral) ((SqlBasicCall) call.operand(1)).operand(0)).
                  getValue().toString(), SqlParserPos.ZERO),
          ((SqlBasicCall) call.operand(1)).operand(1)
      };
    } /* else if (call.operand(0) instanceof SqlIntervalLiteral) {
      timesNodes = new SqlNode[] {
          SqlLiteral.createCharString(
              ((SqlIntervalLiteral) call.operand(0)).
                  getValue().toString(), SqlParserPos.ZERO), call.operand(1)
      };
    }*/
    return new SqlBasicCall(SqlStdOperatorTable.MULTIPLY, timesNodes,
        SqlParserPos.ZERO);
  }

  private void unparseDateAdd(SqlWriter writer, int leftPrec, int rightPrec, SqlNode[] sqlNodes) {
    final SqlWriter.Frame dateAddFrame = writer.startFunCall("DATEADD");
    for (SqlNode operand : sqlNodes) {
      writer.sep(",");
      operand.unparse(writer, leftPrec, rightPrec);
    }
    writer.endFunCall(dateAddFrame);
  }

  public boolean handleMinus(SqlWriter writer, SqlCall call, int leftPrec,
                             int rightPrec, String sign) {
    if ("TIMESTAMP_SUB".equals(call.getOperator().getName())
        || "TIMESTAMP_ADD".equals(call.getOperator().getName())) {
      return handleTimestampInterval(writer, call, leftPrec, rightPrec, sign);
    } else if ("DATE_SUB".equals(call.getOperator().getName())
        || "DATE_ADD".equals(call.getOperator().getName())) {
      return handleDateOperation(writer, call, leftPrec, rightPrec, sign);
    } else {
      return handleMinusIntervalOperand(writer, call, leftPrec, rightPrec, sign);
    }
  }

  private boolean handleMinusIntervalOperand(SqlWriter writer, SqlCall call,
                                             int leftPrec, int rightPrec, String sign) {
    if (call.operand(1) instanceof SqlIntervalLiteral) {
      switch (((SqlIntervalLiteral) call.operand(1)).getTypeName().toString()) {
      case "INTERVAL_DAY":
      case "INTERVAL_MONTH":
      case "INTERVAL_YEAR":
        unparseDateTimeIntervalWithActualOperand(writer, call, leftPrec, rightPrec,
            call.operand(0), sign);
        break;
      case "INTERVAL_YEAR_MONTH":
        String value = ((SqlIntervalLiteral) call.operand(1)).getValue().toString();
        String[] dayTimeSplit = value.split("-");
        unparseDateAddBasedonTimeUnit(writer, "YEAR", dayTimeSplit[0], sign);
        unparseDateAddBasedonTimeUnit(writer, "MONTH", dayTimeSplit[1], sign);
        call.operand(0).unparse(writer, leftPrec, rightPrec);
        writer.print("))");
        break;
      }
    } else {
      return false;
    }
    return true;
  }

  private boolean handleDateOperation(SqlWriter writer, SqlCall call,
                                      int leftPrec, int rightPrec, String sign) {
    if (call.operand(1) instanceof SqlIntervalLiteral) {
      switch (((SqlIntervalLiteral) call.operand(1)).getTypeName().toString()) {
      case "INTERVAL_YEAR_MONTH":
        String value = ((SqlIntervalLiteral) call.operand(1)).getValue().toString();
        String[] dayTimeSplit = value.split("-");
        unparseDateAddBasedonTimeUnit(writer, "YEAR", dayTimeSplit[0], sign);
        unparseDateAddBasedonTimeUnit(writer, "MONTH", dayTimeSplit[1], sign);
        call.operand(0).unparse(writer, leftPrec, rightPrec);
        writer.print("))");
        break;
      case "INTERVAL_MONTH":
      case "INTERVAL_DAY":
      case "INTERVAL_YEAR":
        unparseDateTimeIntervalWithActualOperand(writer, call, leftPrec, rightPrec,
            call.operand(0), sign);
        break;
      }
    } else {
      return false;
    }
    return true;
  }

  private boolean handleTimestampInterval(SqlWriter writer, SqlCall call,
                                          int leftPrec, int rightPrec, String sign) {
    if (call.operand(1) instanceof SqlIntervalLiteral) {
      switch (((SqlIntervalLiteral) call.operand(1)).getTypeName().toString()) {
      case "INTERVAL_DAY_SECOND":
        String value = ((SqlIntervalLiteral) call.operand(1)).getValue().toString();
        String[] dayTimeSplit = value.split(" ");
        String[] timeSplit = dayTimeSplit[1].split(":");
        unparseDateAddBasedonTimeUnit(writer, "DAY", dayTimeSplit[0], sign);
        unparseDateAddBasedonTimeUnit(writer, "HOUR", timeSplit[0], sign);
        unparseDateAddBasedonTimeUnit(writer, "MINUTE", timeSplit[1], sign);
        unparseDateAddBasedonTimeUnit(writer, "SECOND", timeSplit[2], sign);
        call.operand(0).unparse(writer, leftPrec, rightPrec);
        writer.print("))))");
        break;
      case "INTERVAL_MINUTE_SECOND":
        String value1 = ((SqlIntervalLiteral) call.operand(1)).getValue().toString();
        String[] dayTimeSplit1 = value1.split(":");
        unparseDateAddBasedonTimeUnit(writer, "MINUTE", dayTimeSplit1[0], sign);
        unparseDateAddBasedonTimeUnit(writer, "SECOND", dayTimeSplit1[1], sign);
        call.operand(0).unparse(writer, leftPrec, rightPrec);
        writer.print("))");
        break;
      case "INTERVAL_SECOND":
      case "INTERVAL_MINUTE":
      case "INTERVAL_HOUR":
      case "INTERVAL_DAY":
      case "INTERVAL_MONTH":
      case "INTERVAL_YEAR":
        unparseDateTimeIntervalWithActualOperand(writer, call, leftPrec, rightPrec,
            call.operand(0), sign);
        break;
      case "INTERVAL_HOUR_SECOND":
        String hourToSecond = ((SqlIntervalLiteral) call.operand(1)).getValue().toString();
        String[] hourToSecondSplit = hourToSecond.split(":");
        unparseDateAddBasedonTimeUnit(writer, "HOUR", hourToSecondSplit[0], sign);
        unparseDateAddBasedonTimeUnit(writer, "MINUTE", hourToSecondSplit[1], sign);
        unparseDateAddBasedonTimeUnit(writer, "SECOND", hourToSecondSplit[2], sign);
        call.operand(0).unparse(writer, leftPrec, rightPrec);
        writer.print("))");
        break;
      case "INTERVAL_DAY_HOUR":
        String dayToHour = ((SqlIntervalLiteral) call.operand(1)).getValue().toString();
        String[] dayToHourSplit = dayToHour.split(" ");
        unparseDateAddBasedonTimeUnit(writer, "DAY", dayToHourSplit[0], sign);
        unparseDateAddBasedonTimeUnit(writer, "HOUR", dayToHourSplit[1], sign);
        call.operand(0).unparse(writer, leftPrec, rightPrec);
        writer.print("))");
        break;
      }
    } else if (call.operand(1) instanceof SqlBasicCall) {
      SqlCall node1 = (SqlBasicCall) call.operand(1);
      if (node1 instanceof SqlBasicCall) {
        SqlCall intervalNode = node1;
        if (node1.operand(0) instanceof SqlCall) {
          intervalNode = node1.operand(0);
        }
        if (intervalNode.operand(0) instanceof SqlIntervalLiteral) {
          unparseDateAddBasedonTimeUnit(writer,
              ((SqlIntervalLiteral) intervalNode.operand(0)).getTypeName().toString(),
              ((SqlIntervalLiteral) intervalNode.operand(0)).getValue().toString(), sign);
        }
        if (node1.operand(0) instanceof SqlCall
            && intervalNode.operand(1) instanceof SqlIntervalLiteral) {
          unparseDateAddBasedonTimeUnit(writer,
              ((SqlIntervalLiteral) intervalNode.operand(1)).getTypeName().toString(),
              ((SqlIntervalLiteral) intervalNode.operand(1)).getValue().toString(), sign);
        }
        if (node1.operand(1) instanceof SqlIntervalLiteral) {
          unparseDateTimeIntervalWithActualOperand(writer, node1,
              leftPrec, rightPrec, call.operand(0), sign);
          writer.print(")");
        }
        if (node1.operand(0) instanceof SqlCall) {
          writer.print(")");
        }
      } else {
        if (call.operand(1) instanceof SqlIntervalLiteral) {
          unparseDateTimeIntervalWithActualOperand(writer, call,
              leftPrec, rightPrec, call.operand(0), sign);
        }
      }
    } else {
      return false;
    }
    return true;
  }

  private void unparseDateTimeIntervalWithActualOperand(SqlWriter writer, SqlCall call,
          int leftPrec, int rightPrec, SqlNode operand, String sign) {
    final SqlWriter.Frame dateAddFrame = writer.startFunCall("DATEADD");
    writer.print(((SqlIntervalLiteral) call.operand(1)).getTypeName().toString()
        .replace("INTERVAL_", ""));
    String intervalSign = String.valueOf(((SqlIntervalLiteral.IntervalValue)
        ((SqlIntervalLiteral) call.operand(1))
            .getValue()).getSign()).replace("1", "");
    if ("-".equals(intervalSign)) {
      sign = intervalSign;
    }
    writer.print(", " + sign);
    writer.print(((SqlIntervalLiteral) call.operand(1)).getValue().toString());
    writer.print(", ");
    operand.unparse(writer, leftPrec, rightPrec);
    writer.endFunCall(dateAddFrame);
  }

  private void unparseDateAddBasedonTimeUnit(SqlWriter writer, String typeName, String value,
                                             String sign) {
    writer.print("DATEADD(");
    writer.print(typeName.replace("INTERVAL_", ""));
    writer.print(", " + sign);
    writer.print(value);
    writer.print(", ");
  }
}

// End SnowflakeDateTimestampInterval.java
