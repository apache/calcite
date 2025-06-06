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
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.dialect.BigQuerySqlDialect;
import org.apache.calcite.sql.fun.SqlLibraryOperators;

import java.util.Queue;

import static org.apache.calcite.util.interval.DateTimestampIntervalUtil.generateQueueForInterval;
import static org.apache.calcite.util.interval.DateTimestampIntervalUtil.getTypeName;

/**
 * Handle BigQuery date timestamp interval.
 */
public class BigQueryDateTimestampInterval {
  public boolean handlePlusMinus(SqlWriter writer, SqlCall call,
                                 int leftPrec, int rightPrec, String sign) {
    String operator = call.getOperator().getName();
    if (checkValidOperator(operator)) {
      return handleInterval(writer, call, leftPrec, rightPrec, sign, operator);
    }
    return handleViaIntervalUtil(writer, call, leftPrec, rightPrec, operator);
  }

  private boolean handleViaIntervalUtil(SqlWriter writer, SqlCall call,
      int leftPrec, int rightPrec, String operator) {
    IntervalUtils utils = new IntervalUtils();
    switch (operator) {
    case "DATETIME_ADD":
    case "DATETIME_SUB":
    case "TIMESTAMP_SUB":
    case "TIMESTAMP_ADD":
    case "DATE_ADD":
    case "DATE_SUB":
      utils.unparse(writer, call, leftPrec, rightPrec,
          new BigQuerySqlDialect(SqlDialect.EMPTY_CONTEXT));
      return true;
    }
    return false;
  }

  private boolean handleInterval(SqlWriter writer, SqlCall call,
      int leftPrec, int rightPrec, String sign, String operator) {
    if (call.operand(1) instanceof SqlBasicCall) {
      return handleIntervalArithmeticCombination(writer, call, leftPrec, rightPrec, operator);
    } else if (call.operand(1) instanceof SqlIntervalLiteral) {
      return handleIntervalCombination(writer, call, leftPrec, rightPrec, operator, sign);
    } else if (call.operand(1) instanceof SqlNumericLiteral) {
      return handleViaIntervalUtil(writer, call, leftPrec, rightPrec, operator);
    }
    return false;
  }

  private boolean checkValidOperator(String operator) {
    return SqlLibraryOperators.DM_TIMESTAMP_ADD.getName().equals(operator)
        || SqlLibraryOperators.TIMESTAMP_SUB.getName().equals(operator)
        || SqlLibraryOperators.DATE_ADD.getName().equals(operator)
        || SqlLibraryOperators.DATE_SUB.getName().equals(operator)
        || SqlLibraryOperators.TIME_ADD.getName().equals(operator)
        || SqlLibraryOperators.TIME_SUB.getName().equals(operator);
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
      writer.sep(",");
      writer.print("INTERVAL ");
      writer.print(((SqlIntervalLiteral) operand1.operand(0)).getValue().toString());
      writer.print(" " + typeName.replace("INTERVAL_", ""));
      writer.endFunCall(frame2);
      writer.sep(",");
      writer.print("INTERVAL " + ((SqlIntervalLiteral) operand1.operand(1))
          .getValue().toString());
      writer.print(" " + typeName2.replace("INTERVAL_", ""));
      writer.endFunCall(frame);
    } else {
      return false;
    }
    return true;
  }

  private boolean handleIntervalCombination(SqlWriter writer, SqlCall call,
      int leftPrec, int rightPrec, String operator, String sign) {
    String typeName = getTypeName(call, 1);
    switch (typeName) {
    case "INTERVAL_DAY_SECOND":
    case "INTERVAL_DAY_HOUR":
    case "INTERVAL_DAY_MINUTE":
      handleDayMinuteSecondInterval(writer, call, leftPrec, rightPrec, operator, typeName);
      break;
    case "INTERVAL_YEAR_MONTH":
      handleYearMonthInterval(writer, call, leftPrec, rightPrec, operator, typeName, "-");
      break;
    case "INTERVAL_HOUR_SECOND":
    case "INTERVAL_MINUTE_SECOND":
    case "INTERVAL_HOUR_MINUTE":
      handleYearMonthInterval(writer, call, leftPrec, rightPrec, operator, typeName, ":");
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
      int leftPrec, int rightPrec, String operator, String typeName, String separator) {
    String operand1 = ((SqlIntervalLiteral) call.operand(1)).getValue().toString();
    String[] valueSplit = operand1.split(separator);
    Queue<String> queue = generateQueueForInterval(typeName);
    int index = valueSplit.length - 1;
    String timeUnit = queue.poll();
    while (index > 0) {
      writer.print(operator + "(");
      index--;
    }
    unparseIntervalCombination(writer, call, leftPrec, rightPrec, operator,
        valueSplit[0], timeUnit);
    int timeIndex = 1;
    while (timeIndex < valueSplit.length) {
      writer.sep(",");
      writer.print("INTERVAL ");
      writer.print(Integer.valueOf(valueSplit[timeIndex]));
      writer.print(" " + queue.poll());
      writer.print(")");
      timeIndex++;
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
      writer.sep(",");
      writer.print("INTERVAL ");
      writer.print(Integer.valueOf(timeSplit[timeIndex]));
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

  private void unparseIntervalCombination(SqlWriter writer, SqlCall call,
      int leftPrec, int rightPrec, String operator, String value, String timeUnit) {
    writer.print(operator + "(");
    call.operand(0).unparse(writer, leftPrec, rightPrec);
    writer.sep(",");
    writer.print("INTERVAL ");
    writer.print(value + " ");
    writer.print(timeUnit);
    writer.print(")");
  }
}
