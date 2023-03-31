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

import org.apache.calcite.avatica.util.DateTimeUtils;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

import static org.apache.calcite.linq4j.Nullness.castNonNull;

/**
 * Implementation of {@link FormatElement} containing the standard format
 * elements. These are based on Oracle's format model documentation.
 *
 * <p>See
 * <a href="https://docs.oracle.com/en/database/oracle/oracle-database/21/sqlqr/Format-Models.html">
 * Oracle format model reference.</a>
 *
 * @see FormatModels#DEFAULT
 */
public enum FormatElementEnum implements FormatElement {
  D("The weekday (Monday as the first day of the week) as a decimal number (1-7)") {
    @Override public String format(Date date) {
      final Calendar calendar = Work.get().calendar;
      calendar.setTime(date);
      return String.format(Locale.ROOT, "%d", calendar.get(Calendar.DAY_OF_WEEK));
    }
  },
  DAY("The full weekday name") {
    @Override public String format(Date date) {
      final Work work = Work.get();
      return work.eeeeFormat.format(date);
    }
  },
  DD("The day of the month as a decimal number (01-31)") {
    @Override public String format(Date date) {
      final Calendar calendar = Work.get().calendar;
      calendar.setTime(date);
      return String.format(Locale.ROOT, "%02d", calendar.get(Calendar.DAY_OF_MONTH));
    }
  },
  DDD("The day of the year as a decimal number (001-366)") {
    @Override public String format(Date date) {
      final Calendar calendar = Work.get().calendar;
      calendar.setTime(date);
      return String.format(Locale.ROOT, "%03d", calendar.get(Calendar.DAY_OF_YEAR));
    }
  },
  DY("The abbreviated weekday name") {
    @Override public String format(Date date) {
      final Work work = Work.get();
      return work.eeeFormat.format(date);
    }
  },
  FF1("Fractional seconds to 1 digit") {
    @Override public String format(Date date) {
      final Work work = Work.get();
      return work.sFormat.format(date);
    }
  },
  FF2("Fractional seconds to 2 digits") {
    @Override public String format(Date date) {
      final Work work = Work.get();
      return work.ssFormat.format(date);
    }
  },
  FF3("Fractional seconds to 3 digits") {
    @Override public String format(Date date) {
      final Work work = Work.get();
      return work.sssFormat.format(date);
    }
  },
  FF4("Fractional seconds to 4 digits") {
    @Override public String format(Date date) {
      final Work work = Work.get();
      return work.ssssFormat.format(date);
    }
  },
  FF5("Fractional seconds to 5 digits") {
    @Override public String format(Date date) {
      final Work work = Work.get();
      return work.sssssFormat.format(date);
    }
  },
  FF6("Fractional seconds to 6 digits") {
    @Override public String format(Date date) {
      final Work work = Work.get();
      return work.ssssssFormat.format(date);
    }
  },
  HH12("The hour (12-hour clock) as a decimal number (01-12)") {
    @Override public String format(Date date) {
      final Calendar calendar = Work.get().calendar;
      calendar.setTime(date);
      int hour = calendar.get(Calendar.HOUR);
      return String.format(Locale.ROOT, "%02d", hour == 0 ? 12 : hour);
    }
  },
  HH24("The hour (24-hour clock) as a decimal number (00-23)") {
    @Override public String format(Date date) {
      final Calendar calendar = Work.get().calendar;
      calendar.setTime(date);
      return String.format(Locale.ROOT, "%02d", calendar.get(Calendar.HOUR_OF_DAY));
    }
  },
  IW("The ISO 8601 week number of the year (Monday as the first day of the week) "
      + "as a decimal number (01-53)") {
    @Override public String format(Date date) {
      // TODO: ensure this is isoweek
      final Calendar calendar = Work.get().calendar;
      calendar.setTime(date);
      calendar.setFirstDayOfWeek(Calendar.MONDAY);
      return String.format(Locale.ROOT, "%02d", calendar.get(Calendar.WEEK_OF_YEAR));
    }
  },
  MI("The minute as a decimal number (00-59)") {
    @Override public String format(Date date) {
      final Calendar calendar = Work.get().calendar;
      calendar.setTime(date);
      return String.format(Locale.ROOT, "%02d", calendar.get(Calendar.MINUTE));
    }
  },
  MM("The month as a decimal number (01-12)") {
    @Override public String format(Date date) {
      final Calendar calendar = Work.get().calendar;
      calendar.setTime(date);
      return String.format(Locale.ROOT, "%02d", calendar.get(Calendar.MONTH) + 1);
    }
  },
  MON("The abbreviated month name") {
    @Override public String format(Date date) {
      final Work work = Work.get();
      return work.mmmFormat.format(date);
    }
  },
  MONTH("The full month name (English)") {
    @Override public String format(Date date) {
      final Work work = Work.get();
      return work.mmmmFormat.format(date);
    }
  },
  Q("The quarter as a decimal number (1-4)") {
    @Override public String format(Date date) {
      final Calendar calendar = Work.get().calendar;
      calendar.setTime(date);
      return String.format(Locale.ROOT, "%d", (calendar.get(Calendar.MONTH) / 3) + 1);
    }
  },
  SS("The second as a decimal number (00-60)") {
    @Override public String format(Date date) {
      final Calendar calendar = Work.get().calendar;
      calendar.setTime(date);
      return String.format(Locale.ROOT, "%02d", calendar.get(Calendar.SECOND));
    }
  },
  MS("The millisecond as a decimal number (000-999)") {
    @Override public String format(Date date) {
      final Calendar calendar = Work.get().calendar;
      calendar.setTime(date);
      return String.format(Locale.ROOT, "%03d", calendar.get(Calendar.MILLISECOND));
    }
  },
  TZR("The time zone name") {
    @Override public String format(Date date) {
      // TODO: how to support timezones?
      return "";
    }
  },
  WW("The week number of the year (Sunday as the first day of the week) as a decimal "
      + "number (00-53)") {
    @Override public String format(Date date) {
      final Calendar calendar = Work.get().calendar;
      calendar.setTime(date);
      calendar.setFirstDayOfWeek(Calendar.SUNDAY);
      return String.format(Locale.ROOT, "%02d", calendar.get(Calendar.WEEK_OF_YEAR));
    }
  },
  YY("Last 2 digits of year") {
    @Override public String format(Date date) {
      final Work work = Work.get();
      return work.yyFormat.format(date);
    }
  },
  YYYY("The year with century as a decimal number") {
    @Override public String format(Date date) {
      final Calendar calendar = Work.get().calendar;
      calendar.setTime(date);
      return String.format(Locale.ROOT, "%d", calendar.get(Calendar.YEAR));
    }
  };

  private final String description;

  // TODO: be sure to deal with TZ

  FormatElementEnum(String description) {
    this.description = description;
  }

  @Override public String getDescription() {
    return description;
  }

  /** Work space. Provides a value for each mutable data structure that might
   * be needed by a format element. Ensures thread-safety. */
  static class Work {
    private static final ThreadLocal<@Nullable Work> THREAD_WORK =
        ThreadLocal.withInitial(Work::new);

    /** Returns an instance of Work for this thread. */
    static Work get() {
      return castNonNull(THREAD_WORK.get());
    }

    final Calendar calendar =
        Calendar.getInstance(DateTimeUtils.DEFAULT_ZONE, Locale.ROOT);
    final DateFormat eeeeFormat = new SimpleDateFormat("EEEE", Locale.US);
    final DateFormat eeeFormat = new SimpleDateFormat("EEE", Locale.ROOT);
    final DateFormat mmmFormat = new SimpleDateFormat("MMM", Locale.ROOT);
    final DateFormat mmmmFormat = new SimpleDateFormat("MMMM", Locale.ROOT);
    final DateFormat sFormat = new SimpleDateFormat("S", Locale.ROOT);
    final DateFormat ssFormat = new SimpleDateFormat("SS", Locale.ROOT);
    final DateFormat sssFormat = new SimpleDateFormat("SSS", Locale.ROOT);
    final DateFormat ssssFormat = new SimpleDateFormat("SSSS", Locale.ROOT);
    final DateFormat sssssFormat = new SimpleDateFormat("SSSSS", Locale.ROOT);
    final DateFormat ssssssFormat = new SimpleDateFormat("SSSSSS", Locale.ROOT);
    final DateFormat yyFormat = new SimpleDateFormat("yy", Locale.ROOT);
  }
}
