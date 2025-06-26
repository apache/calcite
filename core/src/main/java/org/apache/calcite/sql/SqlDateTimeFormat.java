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
 * Enumeration of Standard date time format.
 */

public enum SqlDateTimeFormat {

  DAYOFMONTH("DD"),
  DAYOFYEAR("DDD"),
  NUMERICMONTH("MM"),
  ABBREVIATEDMONTH("MMM"),
  MONTHNAME("MMMM"),
  TWODIGITYEAR("YY"),
  FOURDIGITISOYEAR("IYYY"),
  THREEDIGITYEAR("YYY"),
  FOURDIGITYEAR("YYYY"),
  DDMMYYYY("DDMMYYYY"),
  DDYYYYMM("DDYYYYMM"),
  DDMMYY("DDMMYY"),
  MMDDYYYY("MMDDYYYY"),
  MMDDYY("MMDDYY"),
  YYYYMM("YYYYMM"),
  YYYYMMDD("YYYYMMDD"),
  MMYYYYDD("MMYYYYDD"),
  MMDDRR("MMDDRR"),
  RRRR("RRRR"),
  RR("RR"),
  RRRRMMDD("RRRRMMDD"),
  YYMMDD("YYMMDD"),
  MMYY("MMYY"),
  DDMON("DDMON"),
  MONYY("MONYY"),
  MONYYYY("MONYYYY"),
  DDMONYYYY("DDMONYYYY"),
  DDMONYY("DDMONYY"),
  DAYOFWEEK("EEEE"),
  ISOWEEK("IW"),
  ABBREVIATEDDAYOFWEEK("EEE"),
  TWENTYFOURHOUR("HH24"),
  HOUR("HH"),
  TWENTYFOURHOURMIN("HH24MI"),
  TWENTYFOURHOURMINSEC("HH24MISS"),
  YYYYMMDDHH24MISS("YYYYMMDDHH24MISS"),
  SECONDS_PRECISION("MS"),
  YYYYMMDDHHMISS("YYYYMMDDHHMISS"),
  YYYYMMDDHH24MI("YYYYMMDDHH24MI"),
  YYYYMMDDHH24("YYYYMMDDHH24"),
  HOURMINSEC("HHMISS"),
  MINUTE("MI"),
  SECOND("SS"),
  FRACTIONONE("S(1)"),
  FRACTIONTWO("S(2)"),
  FRACTIONTHREE("S(3)"),
  FRACTIONFOUR("S(4)"),
  FRACTIONFIVE("S(5)"),
  FRACTIONSIX("S(6)"),
  FRACTIONEIGHT("S(8)"),
  FRACTIONNINE("S(9)"),
  AMPM("T"),
  TIMEZONE("Z"),
  MONTH_NAME("MONTH"),
  ABBREVIATED_MONTH("MON"),
  NAME_OF_DAY("DAY"),
  ABBREVIATED_NAME_OF_DAY("DY"),
  HOUR_OF_DAY_12("HH12"),
  POST_MERIDIAN_INDICATOR("PM"),
  POST_MERIDIAN_INDICATOR_WITH_DOT("P.M."),
  ANTE_MERIDIAN_INDICATOR("AM"),
  ANTE_MERIDIAN_INDICATOR_WITH_DOT("A.M."),
  MILLISECONDS_5("sssss"),
  MILLISECONDS_4("ssss"),
  SEC_FROM_MIDNIGHT("SEC_FROM_MIDNIGHT"),
  E4("E4"),
  E3("E3"),
  JULIAN("J"),
  U("u"),
  NUMERIC_TIME_ZONE("ZZ"),
  QUARTER("QUARTER"),
  WEEK_OF_YEAR("WW"),
  WEEK_OF_MONTH("W"),
  TIMEOFDAY("TIMEOFDAY"),
  YYYYDDD("YYYYDDD"),
  YYDDD("YYDDD"),
  YYYYDDMM("YYYYDDMM"),
  TIMEWITHTIMEZONE("%c%z"),
  TIME("%c"),
  ABBREVIATED_MONTH_UPPERCASE("MONU"),
  HH24("%H"),
  DDMMYYYYHH24("DDMMYYYYHH24"),
  YYMMDDHH24MISS("YYMMDDHH24MISS"),
  FORMAT_1YYMMDD("1YYMMDD"),
  FORMAT_QQYY("QQYY"),
  FORMAT_QQYYYY("QQYYYY");


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
      if (dateTimeFormat.value.equalsIgnoreCase(value)) {
        return dateTimeFormat;
      }
    }
    throw new IllegalArgumentException("No SqlDateTimeFormat enum found with value" + value);
  }

}
