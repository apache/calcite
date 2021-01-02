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
import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlLibraryOperators;

import java.util.LinkedList;
import java.util.Queue;

/**
 * Handle BigQuery date timestamp interval
 */
public class BigQueryDateTimestampInterval {
  public boolean unparseTimestampadd(SqlWriter writer, SqlCall call,
      int leftPrec, int rightPrec, String sign) {
    String operator = call.getOperator().toString();
    if (SqlLibraryOperators.TIMESTAMP_ADD.getName().equals(operator)
        || SqlLibraryOperators.TIMESTAMP_SUB.getName().equals(operator)
        || SqlLibraryOperators.DATE_ADD.getName().equals(operator)
        || SqlLibraryOperators.DATE_SUB.getName().equals(operator)) {
      if (call.operand(1) instanceof SqlBasicCall) {
        return handleIntervalArithmeticCombination(writer, call, leftPrec, rightPrec, operator);
      } else if (call.operand(1) instanceof SqlIntervalLiteral) {
        return handleIntervalCombination(writer, call, leftPrec, rightPrec, operator, sign);
      } else {
        return false;
      }
    } else if (SqlKind.MINUS == call.getOperator().getKind()) {
      return handleMinusDateInterval(writer, call, leftPrec, rightPrec, operator);
    }
    return false;
  }

  private boolean handleIntervalArithmeticCombination(SqlWriter writer, SqlCall call,
      int leftPrec, int rightPrec, String operator) {
    SqlCall operand1 = call.operand(1);
    if (operand1.operand(0) instanceof SqlIntervalLiteral
        && operand1.operand(1) instanceof SqlIntervalLiteral) {
      String typeName = getTypeName(operand1, 0);
      String typeName2 = getTypeName(operand1, 1);
      final SqlWriter.Frame frame = writer.startFunCall(operator);
      final SqlWriter.Frame frame2 = writer.startFunCall(operator);
      call.operand(0).unparse(writer, leftPrec, rightPrec);
      writer.print(", INTERVAL ");
      writer.print(((SqlIntervalLiteral) operand1.operand(0)).getValue().toString());
      writer.print(" " + typeName.replace("INTERVAL_", ""));
      writer.endFunCall(frame2);
      writer.print(", INTERVAL " + ((SqlIntervalLiteral) operand1.operand(1))
          .getValue().toString());
      writer.print(" " + typeName2.replace("INTERVAL_", ""));
      writer.endFunCall(frame);
    } else {
      return false;
    }
    return true;
  }

  private String getTypeName(SqlCall operand1, int i) {
    return ((SqlIntervalLiteral) operand1.operand(i)).getTypeName().toString();
  }

  private boolean handleMinusDateInterval(SqlWriter writer, SqlCall call,
      int leftPrec, int rightPrec, String operator) {
    if (call.operand(1) instanceof SqlIntervalLiteral) {
      operator = "-".equals(operator) ? "DATE_SUB" : "DATE_ADD";
      writer.print(operator  + "(");
      call.operand(0).unparse(writer, leftPrec, rightPrec);
      writer.print(", INTERVAL ");
      writer.print(((SqlIntervalLiteral) call.operand(1)).getValue().toString());
      writer.print(" " + getTypeName(call, 1).replace("INTERVAL_", ""));
      writer.print(")");
      return true;
    }
    return false;
  }

  private boolean handleIntervalCombination(SqlWriter writer, SqlCall call,
      int leftPrec, int rightPrec, String operator, String sign) {
    String typeName = getTypeName(call, 1);
    switch (typeName) {
    case "INTERVAL_DAY_SECOND":
    case "INTERVAL_DAY_MINUTE":
      handleDayMinuteSecondInterval(writer, call, leftPrec, rightPrec, operator, typeName);
      break;
    case "INTERVAL_YEAR_MONTH":
      handleYearMonthInterval(writer, call, leftPrec, rightPrec, operator, typeName);
      break;
    case "INTERVAL_YEAR":
    case "INTERVAL_MONTH":
      return handleDateTimeIntervalTimeUnit(writer, call,
          leftPrec, rightPrec, operator, sign, typeName);
    default:
      return false;
    }
    return true;
  }

  private void handleYearMonthInterval(SqlWriter writer, SqlCall call,
      int leftPrec, int rightPrec, String operator, String typeName) {
    String operand1 = ((SqlIntervalLiteral) call.operand(1)).getValue().toString();
    String[] valueSplit = operand1.split("-");
    Queue<String> queue1 = generateQueueForInterval(typeName);
    int index1 = valueSplit.length - 1;
    String timeUnit1 = queue1.poll();
    while (index1 > 0) {
      writer.print(operator + "(");
      index1--;
    }
    unparseIntervalCombination(writer, call, leftPrec, rightPrec, operator,
        valueSplit[0], timeUnit1);
    int timeIndex1 = 1;
    while (timeIndex1 < valueSplit.length) {
      writer.print(", INTERVAL ");
      writer.print(valueSplit[timeIndex1]);
      writer.print(" " + queue1.poll());
      writer.print(")");
      timeIndex1++;
    }
  }

  private void handleDayMinuteSecondInterval(SqlWriter writer, SqlCall call,
      int leftPrec, int rightPrec, String operator, String typeName) {
    String value = ((SqlIntervalLiteral) call.operand(1)).getValue().toString();
    String[] dayTimeSplit = value.split(" ");
    String[] timeSplit = dayTimeSplit[1].split(":");
    Queue<String> queue = generateQueueForInterval(typeName);
    int index = timeSplit.length;
    String timeUnit = queue.poll();
    while (index > 0) {
      writer.print(operator + "(");
      index--;
    }
    unparseIntervalCombination(writer, call, leftPrec, rightPrec, operator,
        dayTimeSplit[0], timeUnit);
    int timeIndex = 0;
    while (timeIndex < timeSplit.length) {
      writer.print(", INTERVAL ");
      writer.print(timeSplit[timeIndex]);
      writer.print(" " + queue.poll());
      writer.print(")");
      timeIndex++;
    }
  }

  private boolean handleDateTimeIntervalTimeUnit(SqlWriter writer, SqlCall call,
      int leftPrec, int rightPrec, String operator, String sign, String typeName) {
    if ("DATE_ADD".equals(operator) || "DATE_SUB".equals(operator)) {
      return false;
    }
    final SqlWriter.Frame castFrame = writer.startFunCall("CAST");
    final SqlWriter.Frame dateDiffFrame;
    if ("-".equals(sign)) {
      dateDiffFrame = writer.startFunCall("DATETIME_SUB");
    } else {
      dateDiffFrame = writer.startFunCall("DATETIME_ADD");
    }
    writer.print("CAST(");
    call.operand(0).unparse(writer, leftPrec, rightPrec);
    writer.print("AS DATETIME),");
    writer.print(" INTERVAL");
    writer.print(" " + ((SqlIntervalLiteral) call.operand(1)).getValue().toString());
    writer.print(" " + typeName
        .replace("INTERVAL_", ""));
    writer.endFunCall(dateDiffFrame);
    writer.print("AS TIMESTAMP");
    writer.endFunCall(castFrame);
    return true;
  }

  private Queue<String> generateQueueForInterval(String typeName) {
    Queue<String> queue = new LinkedList<>();
    String[] typeNameSplit = typeName.split("_");
    if (typeNameSplit.length == 1) {
      return queue;
    }
    int startTypeOrdinal = DateTimeTypeName.valueOf(typeNameSplit[1]).ordinal;
    int endTypeOrdinal = DateTimeTypeName.valueOf(typeNameSplit[2]).ordinal;
    if (DateTimeTypeName.valueOf("YEAR").ordinal == startTypeOrdinal) {
      queue.add("YEAR");
    }
    if (checkDateRange("MONTH", startTypeOrdinal, endTypeOrdinal)) {
      queue.add("MONTH");
      if (checkRangeEnd(endTypeOrdinal, "MONTH")) {
        return queue;
      }
    }
    if (checkDateRange("DAY", startTypeOrdinal, endTypeOrdinal)) {
      queue.add("DAY");
      if (checkRangeEnd(endTypeOrdinal, "DAY")) {
        return queue;
      }
    }
    if (checkDateRange("HOUR", startTypeOrdinal, endTypeOrdinal)) {
      queue.add("HOUR");
      if (checkRangeEnd(endTypeOrdinal, "HOUR")) {
        return queue;
      }
    }
    if (checkDateRange("MINUTE", startTypeOrdinal, endTypeOrdinal)) {
      queue.add("MINUTE");
      if (checkRangeEnd(endTypeOrdinal, "MINUTE")) {
        return queue;
      }
    }
    if (checkDateRange("SECOND", startTypeOrdinal, endTypeOrdinal)) {
      queue.add("SECOND");
    }
    return queue;
  }

  /**
   * DateTime type name
   */
  enum DateTimeTypeName {
    YEAR(1, "YEAR"),
    MONTH(2, "MONTH"),
    DAY(3, "DAY"),
    HOUR(4, "HOUR"),
    MINUTE(5, "MINUTE"),
    SECOND(6, "SECOND");

    int ordinal;
    String dateTime;

    DateTimeTypeName(int ordinal, String dateTime) {
      this.ordinal = ordinal;
      this.dateTime = dateTime;
    }
  }

  private boolean checkDateRange(String currentDateTypeName, int startOrdinal, int endOrdinal) {
    return DateTimeTypeName.valueOf(currentDateTypeName).ordinal >= startOrdinal
        && endOrdinal <= endOrdinal;
  }

  private boolean checkRangeEnd(int endTypeOrdinal, String currentDateTypeName) {
    return endTypeOrdinal == DateTimeTypeName.valueOf(currentDateTypeName).ordinal;
  }

  private void unparseIntervalCombination(SqlWriter writer, SqlCall call,
      int leftPrec, int rightPrec, String operator, String value, String timeUnit) {
    writer.print(operator + "(");
    call.operand(0).unparse(writer, leftPrec, rightPrec);
    writer.print(", INTERVAL ");
    writer.print(value + " ");
    writer.print(timeUnit);
    writer.print(")");
  }
}

// End BigQueryDateTimestampInterval.java
