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
import java.time.LocalDate;
import java.time.format.TextStyle;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

import static org.apache.calcite.linq4j.Nullness.castNonNull;

import static java.util.Objects.requireNonNull;

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
  CC("cc", "century (2 digits) (the twenty-first century starts on 2001-01-01)") {
    @Override public void format(StringBuilder sb, Date date) {
      final Calendar calendar = Work.get().calendar;
      calendar.setTime(date);
      sb.append(String.format(Locale.ROOT, "%2d", calendar.get(Calendar.YEAR) / 100 + 1));
    }
  },
  D("F", "The weekday (Monday as the first day of the week) as a decimal number (1-7)") {
    @Override public void format(StringBuilder sb, Date date) {
      final Calendar calendar = Work.get().calendar;
      calendar.setTime(date);
      sb.append(String.format(Locale.ROOT, "%d", calendar.get(Calendar.DAY_OF_WEEK)));
    }
  },
  DAY("EEEE", "The full weekday name") {
    @Override public void format(StringBuilder sb, Date date) {
      final Calendar calendar = Work.get().calendar;
      calendar.setTime(date);
      // The Calendar and SimpleDateFormatter do not seem to give correct results
      // for the day of the week prior to the Julian to Gregorian date change.
      // So we resort to using a LocalDate representation.
      LocalDate ld =
          LocalDate.of(calendar.get(Calendar.YEAR),
              // Calendar months are numbered from 0
              calendar.get(Calendar.MONTH) + 1,
              calendar.get(Calendar.DAY_OF_MONTH));
      sb.append(ld.getDayOfWeek().getDisplayName(TextStyle.FULL, Locale.ENGLISH));
    }
  },
  DD("dd", "The day of the month as a decimal number (01-31)") {
    @Override public void format(StringBuilder sb, Date date) {
      final Calendar calendar = Work.get().calendar;
      calendar.setTime(date);
      sb.append(String.format(Locale.ROOT, "%02d", calendar.get(Calendar.DAY_OF_MONTH)));
    }
  },
  DDD("D", "The day of the year as a decimal number (001-366)") {
    @Override public void format(StringBuilder sb, Date date) {
      final Calendar calendar = Work.get().calendar;
      calendar.setTime(date);
      sb.append(String.format(Locale.ROOT, "%03d", calendar.get(Calendar.DAY_OF_YEAR)));
    }
  },
  DY("EEE", "The abbreviated weekday name") {
    @Override public void format(StringBuilder sb, Date date) {
      final Calendar calendar = Work.get().calendar;
      calendar.setTime(date);
      // The Calendar and SimpleDateFormatter do not seem to give correct results
      // for the day of the week prior to the Julian to Gregorian date change.
      // So we resort to using a LocalDate representation.
      LocalDate ld =
          LocalDate.of(calendar.get(Calendar.YEAR),
              // Calendar months are numbered from 0
              calendar.get(Calendar.MONTH) + 1,
              calendar.get(Calendar.DAY_OF_MONTH));
      sb.append(ld.getDayOfWeek().getDisplayName(TextStyle.SHORT, Locale.ENGLISH));
    }
  },
  E("d", "The day of the month as a decimal number (1-31); "
      + "single digits are left-padded with space.") {
    @Override public void format(StringBuilder sb, Date date) {
      final Calendar calendar = Work.get().calendar;
      calendar.setTime(date);
      sb.append(String.format(Locale.ROOT, "%2d", calendar.get(Calendar.DAY_OF_MONTH)));
    }
  },
  FF1("S", "Fractional seconds to 1 digit") {
    @Override public void format(StringBuilder sb, Date date) {
      final Work work = Work.get();
      sb.append(work.sFormat.format(date));
    }
  },
  FF2("SS", "Fractional seconds to 2 digits") {
    @Override public void format(StringBuilder sb, Date date) {
      final Work work = Work.get();
      sb.append(work.ssFormat.format(date));
    }
  },
  FF3("SSS", "Fractional seconds to 3 digits") {
    @Override public void format(StringBuilder sb, Date date) {
      final Work work = Work.get();
      sb.append(work.sssFormat.format(date));
    }
  },
  FF4("SSSS", "Fractional seconds to 4 digits") {
    @Override public void format(StringBuilder sb, Date date) {
      final Work work = Work.get();
      sb.append(work.ssssFormat.format(date));
    }
  },
  FF5("SSSSS", "Fractional seconds to 5 digits") {
    @Override public void format(StringBuilder sb, Date date) {
      final Work work = Work.get();
      sb.append(work.sssssFormat.format(date));
    }
  },
  FF6("SSSSSS", "Fractional seconds to 6 digits") {
    @Override public void format(StringBuilder sb, Date date) {
      final Work work = Work.get();
      sb.append(work.ssssssFormat.format(date));
    }
  },
  HH12("h", "The hour (12-hour clock) as a decimal number (01-12)") {
    @Override public void format(StringBuilder sb, Date date) {
      final Calendar calendar = Work.get().calendar;
      calendar.setTime(date);
      int hour = calendar.get(Calendar.HOUR);
      sb.append(String.format(Locale.ROOT, "%02d", hour == 0 ? 12 : hour));
    }
  },
  HH24("H", "The hour (24-hour clock) as a decimal number (00-23)") {
    @Override public void format(StringBuilder sb, Date date) {
      final Calendar calendar = Work.get().calendar;
      calendar.setTime(date);
      sb.append(String.format(Locale.ROOT, "%02d", calendar.get(Calendar.HOUR_OF_DAY)));
    }
  },
  // TODO: Ensure ISO 8601 for parsing
  IW("w", "The ISO 8601 week number of the year (Monday as the first day of the week) "
      + "as a decimal number (01-53)") {
    @Override public void format(StringBuilder sb, Date date) {
      // TODO: ensure this is isoweek
      final Calendar calendar = Work.get().calendar;
      calendar.setTime(date);
      calendar.setFirstDayOfWeek(Calendar.MONDAY);
      sb.append(String.format(Locale.ROOT, "%02d", calendar.get(Calendar.WEEK_OF_YEAR)));
    }
  },
  MI("m", "The minute as a decimal number (00-59)") {
    @Override public void format(StringBuilder sb, Date date) {
      final Calendar calendar = Work.get().calendar;
      calendar.setTime(date);
      sb.append(String.format(Locale.ROOT, "%02d", calendar.get(Calendar.MINUTE)));
    }
  },
  MM("MM", "The month as a decimal number (01-12)") {
    @Override public void format(StringBuilder sb, Date date) {
      final Calendar calendar = Work.get().calendar;
      calendar.setTime(date);
      sb.append(String.format(Locale.ROOT, "%02d", calendar.get(Calendar.MONTH) + 1));
    }
  },
  MON("MMM", "The abbreviated month name") {
    @Override public void format(StringBuilder sb, Date date) {
      final Work work = Work.get();
      sb.append(work.mmmFormat.format(date));
    }
  },
  MONTH("MMMM", "The full month name (English)") {
    @Override public void format(StringBuilder sb, Date date) {
      final Work work = Work.get();
      sb.append(work.mmmmFormat.format(date));
    }
  },
  // PM can represent both AM and PM
  PM("a", "Meridian indicator without periods") {
    @Override public void format(StringBuilder sb, Date date) {
      final Calendar calendar = Work.get().calendar;
      calendar.setTime(date);
      String meridian = calendar.get(Calendar.HOUR_OF_DAY) < 12 ? "AM" : "PM";
      sb.append(meridian);
    }
  },
  Q("", "The quarter as a decimal number (1-4)") {
    // TODO: Allow parsing of quarters.
    @Override public void toPattern(StringBuilder sb) throws UnsupportedOperationException {
      throw new UnsupportedOperationException("Cannot convert 'Q' FormatElement to Java pattern");
    }
    @Override public void format(StringBuilder sb, Date date) {
      final Calendar calendar = Work.get().calendar;
      calendar.setTime(date);
      sb.append(String.format(Locale.ROOT, "%d", (calendar.get(Calendar.MONTH) / 3) + 1));
    }
  },
  MS("SSS", "The millisecond as a decimal number (000-999)") {
    @Override public void format(StringBuilder sb, Date date) {
      final Calendar calendar = Work.get().calendar;
      calendar.setTime(date);
      sb.append(String.format(Locale.ROOT, "%03d", calendar.get(Calendar.MILLISECOND)));
    }
  },
  SS("s", "The second as a decimal number (00-60)") {
    @Override public void format(StringBuilder sb, Date date) {
      final Calendar calendar = Work.get().calendar;
      calendar.setTime(date);
      sb.append(String.format(Locale.ROOT, "%02d", calendar.get(Calendar.SECOND)));
    }
  },
  TZR("z", "The time zone name") {
    @Override public void format(StringBuilder sb, Date date) {
      // TODO: how to support timezones?
    }
  },
  W("W", "The week number of the month (Sunday as the first day of the week) as a decimal "
      + "number (1-5)") {
    @Override public void format(StringBuilder sb, Date date) {
      final Calendar calendar = Work.get().calendar;
      calendar.setTime(date);
      sb.append(String.format(Locale.ROOT, "%d", calendar.get(Calendar.WEEK_OF_MONTH)));
    }
  },
  WW("w", "The week number of the year (Sunday as the first day of the week) as a decimal "
      + "number (00-53)") {
    @Override public void format(StringBuilder sb, Date date) {
      final Calendar calendar = Work.get().calendar;
      calendar.setTime(date);
      calendar.setFirstDayOfWeek(Calendar.SUNDAY);
      sb.append(String.format(Locale.ROOT, "%02d", calendar.get(Calendar.WEEK_OF_YEAR)));
    }
  },
  YY("yy", "Last 2 digits of year") {
    @Override public void format(StringBuilder sb, Date date) {
      final Work work = Work.get();
      sb.append(work.yyFormat.format(date));
    }
  },
  YYYY("yyyy", "The year with century as a decimal number") {
    @Override public void format(StringBuilder sb, Date date) {
      final Calendar calendar = Work.get().calendar;
      calendar.setTime(date);
      sb.append(String.format(Locale.ROOT, "%d", calendar.get(Calendar.YEAR)));
    }
  };

  private final String description;
  final String javaFmt;

  // TODO: be sure to deal with TZ

  FormatElementEnum(String javaFmt, String description) {
    this.javaFmt = requireNonNull(javaFmt, "javaFmt");
    this.description = requireNonNull(description, "description");
  }

  @Override public String getDescription() {
    return description;
  }

  @Override public void toPattern(StringBuilder sb) {
    sb.append(this.javaFmt);
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

    final DateFormat mmmFormat = new SimpleDateFormat(MON.javaFmt, Locale.US);
    /* Need to sse Locale.US instead of Locale.ROOT, because Locale.ROOT
     * may actually return the *short* month name instead of the long name.
     * See [CALCITE-6252] BigQuery FORMAT_DATE uses the wrong calendar for Julian dates:
     * https://issues.apache.org/jira/browse/CALCITE-6252.  This may be
     * specific to Java 11. */
    final DateFormat mmmmFormat = new SimpleDateFormat(MONTH.javaFmt, Locale.US);
    final DateFormat sFormat = new SimpleDateFormat(FF1.javaFmt, Locale.ROOT);
    final DateFormat ssFormat = new SimpleDateFormat(FF2.javaFmt, Locale.ROOT);
    final DateFormat sssFormat = new SimpleDateFormat(FF3.javaFmt, Locale.ROOT);
    final DateFormat ssssFormat = new SimpleDateFormat(FF4.javaFmt, Locale.ROOT);
    final DateFormat sssssFormat = new SimpleDateFormat(FF5.javaFmt, Locale.ROOT);
    final DateFormat ssssssFormat = new SimpleDateFormat(FF6.javaFmt, Locale.ROOT);
    final DateFormat yyFormat = new SimpleDateFormat(YY.javaFmt, Locale.ROOT);
  }
}
