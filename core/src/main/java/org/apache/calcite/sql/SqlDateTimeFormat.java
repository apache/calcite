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
package org.apache.calcite.sql;

import java.util.HashSet;
import java.util.Set;

/**
 * Enumeration of Standard date time format
 */

public enum SqlDateTimeFormat {

  DAYOFMONTH("DD"),
  DAYOFYEAR("DDD"),
  NUMERICMONTH("MM"),
  ABBREVIATEDMONTH("MMM"),
  MONTHNAME("MMMM"),
  TWODIGITYEAR("YY"),
  FOURDIGITYEAR("YYYY"),
  DDMMYYYY("DDMMYYYY"),
  DDMMYY("DDMMYY"),
  MMDDYYYY("MMDDYYYY"),
  MMDDYY("MMDDYY"),
  YYYYMMDD("YYYYMMDD"),
  YYMMDD("YYMMDD"),
  DAYOFWEEK("EEEE"),
  ABBREVIATEDDAYOFWEEK("EEE"),
  TWENTYFOURHOUR("HH24"),
  HOUR("HH"),
  MINUTE("MI"),
  SECOND("SS"),
  FRACTIONONE("S(1)"),
  FRACTIONTWO("S(2)"),
  FRACTIONTHREE("S(3)"),
  FRACTIONFOUR("S(4)"),
  FRACTIONFIVE("S(5)"),
  FRACTIONSIX("S(6)"),
  AMPM("T"),
  TIMEZONE("Z");

  public final String value;

  SqlDateTimeFormat(String value) {
    this.value = value;
  }

  static {
    Set<String> usedEnums = new HashSet<>();
    for (SqlDateTimeFormat dateTimeFormat : values()) {
      if (!usedEnums.add(dateTimeFormat.value)) {
        throw new IllegalArgumentException(dateTimeFormat.value + " is already used in the Enum!");
      }
    }
  }

  static SqlDateTimeFormat of(String value) {
    for (SqlDateTimeFormat dateTimeFormat : values()) {
      if (dateTimeFormat.value.equals(value)) {
        return dateTimeFormat;
      }
    }
    throw new IllegalArgumentException("No SqlDateTimeFormat enum found with value" + value);
  }

}

// End SqlDateTimeFormat.java
