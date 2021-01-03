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

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIntervalLiteral;

import java.util.LinkedList;
import java.util.Queue;

/**
 * Utility for Datetimestamp interval
 */
public class DateTimestampIntervalUtil {

  private DateTimestampIntervalUtil() {}

  public static String getTypeName(SqlCall call, int index) {
    return ((SqlIntervalLiteral) call.operand(index)).getTypeName().toString();
  }

  public static Queue<String> generateQueueForInterval(String typeName) {
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

  public static boolean checkDateRange(String currentDateTypeName, int startOrdinal,
      int endOrdinal) {
    return DateTimeTypeName.valueOf(currentDateTypeName).ordinal >= startOrdinal
        && endOrdinal <= endOrdinal;
  }

  public static boolean checkRangeEnd(int endTypeOrdinal, String currentDateTypeName) {
    return endTypeOrdinal == DateTimeTypeName.valueOf(currentDateTypeName).ordinal;
  }

  public static int intValue(String value) {
    return Integer.valueOf(value);
  }
}

// End DateTimestampIntervalUtil.java
