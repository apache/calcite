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
package org.apache.calcite.util.format;

import com.google.common.collect.ImmutableMap;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

/**
 * Implementation of {@link FormatElement} containing the standard format elements. These are based
 * on Oracle's format model documentation.
 *
 * <p><a
 * href="https://docs.oracle.com/en/database/oracle/oracle-database/21/sqlqr/Format-Models.html">
 * Oracle format model reference.</a>
 */
public enum FormatElementEnum implements FormatElement {
  D("The weekday (Monday as the first day of the week) as a decimal number (1-7)") {
    @Override public String format(Date date) {
      CAL.setTime(date);
      return String.format("%d", CAL.get(Calendar.DAY_OF_WEEK));
    }
  },
  DAY("The full weekday name") {
    @Override public String format(Date date) {
      DateFormat formatter = new SimpleDateFormat("EEEE", Locale.ROOT);
      return formatter.format(date);
    }
  },
  DD("The day of the month as a decimal number (01-31)") {
    @Override public String format(Date date) {
      CAL.setTime(date);
      return String.format("%02d", CAL.get(Calendar.DAY_OF_MONTH));
    }
  },
  DDD("The day of the year as a decimal number (001-366)") {
    @Override public String format(Date date) {
      CAL.setTime(date);
      return String.format("%03d", CAL.get(Calendar.DAY_OF_YEAR));
    }
  },
  DY("The abbreviated weekday name") {
    @Override public String format(Date date) {
      DateFormat formatter = new SimpleDateFormat("EEE", Locale.ROOT);
      return formatter.format(date);
    }
  },
  HH24("The hour (24-hour clock) as a decimal number (00-23)") {
    @Override public String format(Date date) {
      CAL.setTime(date);
      return String.format("%02d", CAL.get(Calendar.HOUR_OF_DAY));
    }
  },
  IW("The ISO 8601 week number of the year (Monday as the first day of the week) "
      + "as a decimal number (01-53)") {
    @Override public String format(Date date) {
      // TODO: ensure this is isoweek
      CAL.setTime(date);
      CAL.setFirstDayOfWeek(Calendar.MONDAY);
      return String.format("%02d", CAL.get(Calendar.WEEK_OF_YEAR));
    }
  },
  MI("The minute as a decimal number (00-59)") {
    @Override public String format(Date date) {
      CAL.setTime(date);
      return String.format("%02d", CAL.get(Calendar.MINUTE));
    }
  },
  MM("The month as a decimal number (01-12)") {
    @Override public String format(Date date) {
      CAL.setTime(date);
      return String.format("%02d", CAL.get(Calendar.MONTH) + 1);
    }
  },
  MON("The abbreviated month name") {
    @Override public String format(Date date) {
      DateFormat formatter = new SimpleDateFormat("MMM", Locale.ROOT);
      return formatter.format(date);
    }
  },
  MONTH("The full month name (English)") {
    @Override public String format(Date date) {
      DateFormat formatter = new SimpleDateFormat("MMMM", Locale.ROOT);
      return formatter.format(date);
    }
  },
  Q("The quarter as a decimal number (1-4)") {
    @Override public String format(Date date) {
      CAL.setTime(date);
      return String.format("%d", (CAL.get(Calendar.MONTH) / 3) + 1);
    }
  },
  SS("The second as a decimal number (00-60)") {
    @Override public String format(Date date) {
      CAL.setTime(date);
      return String.format("%02d", CAL.get(Calendar.SECOND));
    }
  },
  TZR("The time zone name") {
    @Override public String format(Date date) {
      // TODO: how to support timezones?
      throw new UnsupportedOperationException();
    }
  },
  WW("The week number of the year (Sunday as the first day of the week) as a decimal "
      + "number (00-53)") {
    @Override public String format(Date date) {
      CAL.setTime(date);
      CAL.setFirstDayOfWeek(Calendar.SUNDAY);
      return String.format("%02d", CAL.get(Calendar.WEEK_OF_YEAR));
    }
  },
  YY("Last 2 digits of year") {
    @Override public String format(Date date) {
      DateFormat formatter = new SimpleDateFormat("yy");
      return formatter.format(date);

    }
  },
  YYYY("The year with century as a decimal number") {
    @Override public String format(Date date) {
      CAL.setTime(date);
      return String.format("%d", CAL.get(Calendar.YEAR));
    }
  };

  private final String description;
  // TODO: be sure to deal with TZ
  private static final Calendar CAL = Calendar.getInstance(Locale.ROOT);

  FormatElementEnum(String description) {
    this.description = description;
  }

  @Override public String getDescription() {
    return description;
  }

  /**
   * Helper class that statically generates the parse map for {@link FormatElementEnum}.
   */
  public static class ParseMap {

    private static final Map<String, FormatElement> PARSE_MAP = makeMap();

    private static Map<String, FormatElement> makeMap() {
      return Arrays.stream(FormatElementEnum.values())
          .collect(ImmutableMap.toImmutableMap(FormatElementEnum::name, Function.identity()));
    }

    public static Map<String, FormatElement> getMap() {
      return PARSE_MAP;
    }
  }
}
