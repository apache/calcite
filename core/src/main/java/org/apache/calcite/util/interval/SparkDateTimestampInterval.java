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

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.dialect.SparkSqlDialect;

import java.util.Queue;

import static org.apache.calcite.util.interval.DateTimestampIntervalUtil.generateQueueForInterval;
import static org.apache.calcite.util.interval.DateTimestampIntervalUtil.getTypeName;
import static org.apache.calcite.util.interval.DateTimestampIntervalUtil.intValue;

/**
 * Datetimestamp with interval unparse for Spark.
 */
public class SparkDateTimestampInterval {

  public boolean unparseDateTimeMinus(SqlWriter writer, SqlCall call,
                                    int leftPrec, int rightPrec, String sign) {
    if (call.operand(1) instanceof SqlIntervalLiteral) {
      String typeName = getTypeName(call, 1);
      switch (typeName) {
      case "INTERVAL_DAY_SECOND":
        handleIntervalDaySecond(writer, call, leftPrec, rightPrec, sign, typeName);
        break;
      case "INTERVAL_DAY_MINUTE":
        handleDayMinute(writer, call, leftPrec, rightPrec, sign, typeName);
        break;
      case "INTERVAL_HOUR_SECOND":
        handleHourSecond(writer, call, leftPrec, rightPrec, sign, typeName);
        break;
      case "INTERVAL_DAY_HOUR":
        handleOperandArg0(writer, call, leftPrec, rightPrec, sign);
        handleTwoIntervalCombination(writer, call, typeName, " ");
        break;
      case "INTERVAL_MINUTE_SECOND":
      case "INTERVAL_HOUR_MINUTE":
        handleOperandArg0(writer, call, leftPrec, rightPrec, sign);
        handleTwoIntervalCombination(writer, call, typeName, ":");
        break;
      case "INTERVAL_YEAR_MONTH":
        handleOperandArg0(writer, call, leftPrec, rightPrec, sign);
        handleTwoIntervalCombination(writer, call, typeName, "-");
        break;
      case "INTERVAL_YEAR":
        handleIntervalYear(writer, call, leftPrec, rightPrec, sign);
        break;
      case "INTERVAL_MONTH":
        handleIntervalMonth(writer, call, leftPrec, rightPrec, sign);
        break;
      case "INTERVAL_DAY":
      case "INTERVAL_HOUR":
      case "INTERVAL_MINUTE":
      case "INTERVAL_SECOND":
        handleIntervalDatetimeUnit(writer, call, leftPrec, rightPrec, sign);
        break;
      }
    } else if ("ADD_MONTHS".equals(call.getOperator().getName())) {
      new IntervalUtils().unparse(writer, call, leftPrec, rightPrec,
          new SparkSqlDialect(SqlDialect.EMPTY_CONTEXT));
    } else {
      return false;
    }
    return true;
  }

  private void handleIntervalDatetimeUnit(SqlWriter writer, SqlCall call,
      int leftPrec, int rightPrec, String sign) {
    if ("DATE_ADD".equals(call.getOperator().getName())
        || "DATE_SUB".equals(call.getOperator().getName())) {
      call.operand(0).unparse(writer, leftPrec, rightPrec);
      writer.sep(sign);
      writeIntervalLiteral(writer, call);
    } else {
      handleTimeUnitInterval(writer, call, leftPrec, rightPrec, sign);
    }
  }

  private void writeIntervalLiteral(SqlWriter writer, SqlCall call) {
    SqlIntervalLiteral.IntervalValue intervalValue = getIntervalValueFromCall(call);
    if (isNegativeInterval(intervalValue)) {
      writer.print("-");
    }
    writer.literal(intervalValue.getIntervalLiteral());
  }

  private SqlIntervalLiteral.IntervalValue getIntervalValueFromCall(SqlCall call) {
    return ((SqlIntervalLiteral) call.operand(1)).getValueAs(SqlIntervalLiteral.IntervalValue.class);
  }

  private boolean isNegativeInterval(SqlIntervalLiteral.IntervalValue intervalValue) {
    return intervalValue.getSign() == -1;
  }

  private void handleIntervalMonth(SqlWriter writer, SqlCall call,
      int leftPrec, int rightPrec, String sign) {
    if ("ADD_MONTHS".equals(call.getOperator().getName())) {
      unparseAddMonths(writer, call, leftPrec, rightPrec);
    } else {
      handleTimeUnitInterval(writer, call, leftPrec, rightPrec, sign);
    }
  }

  private void unparseAddMonths(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    final SqlWriter.Frame addMonthFrame = writer.startFunCall("ADD_MONTHS");
    call.operand(0).unparse(writer, leftPrec, rightPrec);
    writer.sep(",");
    if (call.operand(1) instanceof SqlIntervalLiteral) {
      String valueSign =
          String.valueOf(
              (
              (SqlIntervalLiteral.IntervalValue) (
          (SqlIntervalLiteral) call.operand(1)).getValue()).getSign()).replace("1", "");
      writer.print("-".equals(valueSign) ? valueSign : "");
      writer.print(((SqlIntervalLiteral) call.operand(1)).getValue().toString());
    } else if (call.operand(1) instanceof SqlBasicCall) {
      SqlBasicCall sqlBasicCall = call.operand(1);
      sqlBasicCall.operand(0).unparse(writer, leftPrec, rightPrec);
      writer.print(sqlBasicCall.getOperator().getName());
      String valueSign =
          String.valueOf(
              (
              (SqlIntervalLiteral.IntervalValue) (
          (SqlIntervalLiteral) sqlBasicCall.operand(1)).getValue()).getSign()).replace("1", "");
      writer.print("-".equals(valueSign) ? valueSign : "" + " ");
      writer.print(((SqlIntervalLiteral) sqlBasicCall.operand(1)).getValue().toString());
    } else {
      call.operand(1).unparse(writer, leftPrec, rightPrec);
    }
    writer.endFunCall(addMonthFrame);
  }

  private void handleIntervalYear(SqlWriter writer, SqlCall call,
      int leftPrec, int rightPrec, String sign) {
    if ("-".equals(call.getOperator().getName())
        || "DATE_ADD".equals(call.getOperator().getName())) {
      final SqlWriter.Frame castFrame = writer.startFunCall("CAST");
      call.operand(0).unparse(writer, leftPrec, rightPrec);
      writer.print(sign);
      String timeUnitTypeName = ((SqlIntervalLiteral) call.operand(1)).getTypeName().toString()
          .replaceAll("INTERVAL_", "");
      String timeUnitValue = ((SqlIntervalLiteral) call.operand(1)).getValue().toString();
      writer.print(" INTERVAL '" + timeUnitValue + "' " + timeUnitTypeName);
      writer.print(" AS DATE");
      writer.endFunCall(castFrame);
    } else {
      handleTimeUnitInterval(writer, call, leftPrec, rightPrec, sign);
    }
  }

  private void handleHourSecond(SqlWriter writer, SqlCall call, int leftPrec,
        int rightPrec, String sign, String typeName) {
    handleOperandArg0(writer, call, leftPrec, rightPrec, sign);
    String value2 = ((SqlIntervalLiteral) call.operand(1)).getValue().toString();
    String[] timeSplit2 = value2.split(":");
    Queue<String> queue2 = generateQueueForInterval(typeName);
    writer.print(" (INTERVAL '" + intValue(timeSplit2[0]) + "' " + queue2.poll() + " + ");
    writer.print("INTERVAL '" + intValue(timeSplit2[1]) + "' " + queue2.poll() + " + ");
    writer.print("INTERVAL '" + intValue(timeSplit2[2]) + "' " + queue2.poll());
    writer.print(")");
  }

  private void handleDayMinute(SqlWriter writer, SqlCall call, int leftPrec,
      int rightPrec, String sign, String typeName) {
    handleOperandArg0(writer, call, leftPrec, rightPrec, sign);
    String value1 = ((SqlIntervalLiteral) call.operand(1)).getValue().toString();
    String[] dayTimeSplit1 = value1.split(" ");
    String[] timeSplit1 = dayTimeSplit1[1].split(":");
    Queue<String> queue1 = generateQueueForInterval(typeName);
    writer.print(" (INTERVAL '" + intValue(dayTimeSplit1[0]) + "' " + queue1.poll() + " + ");
    writer.print("INTERVAL '" + intValue(timeSplit1[0]) + "' " + queue1.poll() + " + ");
    writer.print("INTERVAL '" + intValue(timeSplit1[1]) + "' " + queue1.poll());
    writer.print(")");
  }

  private void handleIntervalDaySecond(SqlWriter writer, SqlCall call, int leftPrec,
      int rightPrec, String sign, String typeName) {
    handleOperandArg0(writer, call, leftPrec, rightPrec, sign);
    String value = ((SqlIntervalLiteral) call.operand(1)).getValue().toString();
    String[] dayTimeSplit = value.split(" ");
    String[] timeSplit = dayTimeSplit[1].split(":");
    Queue<String> queue = generateQueueForInterval(typeName);
    writer.print(" (INTERVAL '" + intValue(dayTimeSplit[0]) + "' " + queue.poll() + " + ");
    writer.print("INTERVAL '" + intValue(timeSplit[0]) + "' " + queue.poll() + " + ");
    writer.print("INTERVAL '" + intValue(timeSplit[1]) + "' " + queue.poll() + " + ");
    writer.print("INTERVAL '" + intValue(timeSplit[2]) + "' " + queue.poll());
    writer.print(")");
  }

  private void handleTimeUnitInterval(SqlWriter writer, SqlCall call,
      int leftPrec, int rightPrec, String sign) {
    handleOperandArg0(writer, call, leftPrec, rightPrec, sign);
    String timeUnitTypeName = ((SqlIntervalLiteral) call.operand(1)).getTypeName().toString()
        .replaceAll("INTERVAL_", "");
    String timeUnitValue = ((SqlIntervalLiteral) call.operand(1)).getValue().toString();
    writer.print(" INTERVAL '" + timeUnitValue + "' " + timeUnitTypeName);
    writer.setNeedWhitespace(true);
  }

  private void handleOperandArg0(SqlWriter writer, SqlCall call,
      int leftPrec, int rightPrec, String sign) {
    call.operand(0).unparse(writer, leftPrec, rightPrec);
    writer.print(sign);
  }

  private void handleTwoIntervalCombination(SqlWriter writer, SqlCall call,
      String typeName, String separator) {
    String value = ((SqlIntervalLiteral) call.operand(1)).getValue().toString();
    String[] dayTimeSplit = value.split(separator);
    int sign =
        ((SqlIntervalLiteral.IntervalValue) ((SqlIntervalLiteral) call.operand(1)).getValue()).getSign();
    Queue<String> queue = generateQueueForInterval(typeName);
    String signString = sign == -1 ? "-" : "";
    writer.print(" (INTERVAL '" + signString + intValue(dayTimeSplit[0]) + "' " + queue.poll() + " + ");
    writer.print("INTERVAL '" + signString + intValue(dayTimeSplit[1]) + "' " + queue.poll());
    writer.print(")");
  }
}
