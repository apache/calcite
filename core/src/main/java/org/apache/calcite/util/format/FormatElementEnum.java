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
import org.apache.calcite.util.TryThreadLocal;

import org.apache.commons.lang3.StringUtils;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.TextStyle;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

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
  D("", "The weekday (Sunday as the first day of the week) as a decimal number (1-7)") {
    @Override public void toPattern(StringBuilder sb) throws UnsupportedOperationException {
      throwToPatternNotImplemented();
    }
    @Override public void format(StringBuilder sb, Date date) {
      final Calendar calendar = Work.get().calendar;
      calendar.setTime(date);
      sb.append(String.format(Locale.ROOT, "%d", calendar.get(Calendar.DAY_OF_WEEK)));
    }
  },
  DAY("EEEE", "The full weekday name, in uppercase") {
    @Override public void format(StringBuilder sb, Date date) {
      final Work work = Work.get();
      sb.append(work.getDayFromDate(date, TextStyle.FULL).toUpperCase(Locale.ROOT));
    }
  },
  Day("EEEE", "The full weekday name, capitalized") {
    @Override public void format(StringBuilder sb, Date date) {
      final Work work = Work.get();
      sb.append(work.getDayFromDate(date, TextStyle.FULL));
    }
  },
  day("EEEE", "The full weekday name, in lowercase") {
    @Override public void format(StringBuilder sb, Date date) {
      final Work work = Work.get();
      sb.append(work.getDayFromDate(date, TextStyle.FULL).toLowerCase(Locale.ROOT));
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
  DY("EEE", "The abbreviated weekday name, in uppercase") {
    @Override public void format(StringBuilder sb, Date date) {
      final Work work = Work.get();
      sb.append(work.getDayFromDate(date, TextStyle.SHORT).toUpperCase(Locale.ROOT));
    }
  },
  Dy("EEE", "The abbreviated weekday name, capitalized") {
    @Override public void format(StringBuilder sb, Date date) {
      final Work work = Work.get();
      sb.append(work.getDayFromDate(date, TextStyle.SHORT));
    }
  },
  dy("EEE", "The abbreviated weekday name, in lowercase") {
    @Override public void format(StringBuilder sb, Date date) {
      final Work work = Work.get();
      sb.append(work.getDayFromDate(date, TextStyle.SHORT).toLowerCase(Locale.ROOT));
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
      // Extracting 1 decimal place as SimpleDateFormat returns precision with 3 places.
      // Refer to <a href="https://issues.apache.org/jira/projects/CALCITE/issues/CALCITE-6269">
      // [CALCITE-6269] Fix missing/broken BigQuery date-time format elements</a>.
      sb.append(work.sssFormat.format(date).charAt(0));
    }
  },
  FF2("S", "Fractional seconds to 2 digits") {
    @Override public void format(StringBuilder sb, Date date) {
      final Work work = Work.get();
      // Extracting 2 decimal places as SimpleDateFormat returns precision with 3 places.
      // Refer to <a href="https://issues.apache.org/jira/projects/CALCITE/issues/CALCITE-6269">
      // [CALCITE-6269] Fix missing/broken BigQuery date-time format elements</a>.
      sb.append(work.sssFormat.format(date), 0, 2);
    }
  },
  FF3("S", "Fractional seconds to 3 digits") {
    @Override public void format(StringBuilder sb, Date date) {
      final Work work = Work.get();
      sb.append(work.sssFormat.format(date));
    }
  },
  FF4("S", "Fractional seconds to 4 digits") {
    @Override public void format(StringBuilder sb, Date date) {
      final Work work = Work.get();
      // Padding zeroes to right as SimpleDateFormat supports precision only up to 3 places.
      // Refer to <a href="https://issues.apache.org/jira/projects/CALCITE/issues/CALCITE-6269">
      // [CALCITE-6269] Fix missing/broken BigQuery date-time format elements</a>.
      sb.append(StringUtils.rightPad(work.sssFormat.format(date), 4, "0"));
    }
  },
  FF5("S", "Fractional seconds to 5 digits") {
    @Override public void format(StringBuilder sb, Date date) {
      final Work work = Work.get();
      // Padding zeroes to right as SimpleDateFormat supports precision only up to 3 places.
      // Refer to <a href="https://issues.apache.org/jira/projects/CALCITE/issues/CALCITE-6269">
      // [CALCITE-6269] Fix missing/broken BigQuery date-time format elements</a>.
      sb.append(StringUtils.rightPad(work.sssFormat.format(date), 5, "0"));
    }
  },
  FF6("S", "Fractional seconds to 6 digits") {
    @Override public void format(StringBuilder sb, Date date) {
      final Work work = Work.get();
      // Padding zeroes to right as SimpleDateFormat supports precision only up to 3 places.
      // Refer to <a href="https://issues.apache.org/jira/projects/CALCITE/issues/CALCITE-6269">
      // [CALCITE-6269] Fix missing/broken BigQuery date-time format elements</a>.
      sb.append(StringUtils.rightPad(work.sssFormat.format(date), 6, "0"));
    }
  },
  FF7("S", "Fractional seconds to 6 digits") {
    @Override public void format(StringBuilder sb, Date date) {
      final Work work = Work.get();
      // Padding zeroes to right as SimpleDateFormat supports precision only up to 3 places.
      // Refer to <a href="https://issues.apache.org/jira/projects/CALCITE/issues/CALCITE-6269">
      // [CALCITE-6269] Fix missing/broken BigQuery date-time format elements</a>.
      sb.append(StringUtils.rightPad(work.sssFormat.format(date), 7, "0"));
    }
  },
  FF8("S", "Fractional seconds to 6 digits") {
    @Override public void format(StringBuilder sb, Date date) {
      final Work work = Work.get();
      // Padding zeroes to right as SimpleDateFormat supports precision only up to 3 places.
      // Refer to <a href="https://issues.apache.org/jira/projects/CALCITE/issues/CALCITE-6269">
      // [CALCITE-6269] Fix missing/broken BigQuery date-time format elements</a>.
      sb.append(StringUtils.rightPad(work.sssFormat.format(date), 8, "0"));
    }
  },
  FF9("S", "Fractional seconds to 6 digits") {
    @Override public void format(StringBuilder sb, Date date) {
      final Work work = Work.get();
      // Padding zeroes to right as SimpleDateFormat supports precision only up to 3 places.
      // Refer to <a href="https://issues.apache.org/jira/projects/CALCITE/issues/CALCITE-6269">
      // [CALCITE-6269] Fix missing/broken BigQuery date-time format elements</a>.
      sb.append(StringUtils.rightPad(work.sssFormat.format(date), 9, "0"));
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
  ID("u", "The weekday (Monday as the first day of the week) as a decimal number (1-7)") {
    @Override public void format(StringBuilder sb, Date date) {
      final Calendar calendar = Work.get().calendar;
      calendar.setTime(date);
      int weekDay = calendar.get(Calendar.DAY_OF_WEEK);
      // Converting Sun(1)...Sat(7) to Mon(1)...Sun(7)
      sb.append(weekDay == 1 ? 7 : weekDay - 1);
    }
  },
  IW("", "The ISO 8601 week number of the year (Monday as the first day of the week) "
      + "as a decimal number (01-53). If the week containing January 1 has four or more days "
      + "in the new year, then it is week 1; otherwise it is week 53 of the previous year, "
      + "and the next week is week 1.") {
    @Override public void toPattern(StringBuilder sb) throws UnsupportedOperationException {
      throwToPatternNotImplemented();
    }
    @Override public void format(StringBuilder sb, Date date) {
      final Calendar calendar = Work.get().iso8601Calendar;
      calendar.setTime(date);
      sb.append(String.format(Locale.ROOT, "%02d", calendar.get(Calendar.WEEK_OF_YEAR)));
    }
  },
  IYY("YY", "The ISO 8601 year without century as a decimal number. Each ISO year begins on "
      + "the Monday before the first Thursday of the Gregorian calendar year.") {
    @Override public void format(StringBuilder sb, Date date) {
      final Calendar calendar = Work.get().iso8601Calendar;
      calendar.setTime(date);
      sb.append(String.format(Locale.ROOT, "%02d", calendar.getWeekYear() % 100));
    }
  },
  IYYYY("YYYY", "The ISO 8601 year with century as a decimal number. Each ISO year begins on "
      + "the Monday before the first Thursday of the Gregorian calendar year.") {
    @Override public void format(StringBuilder sb, Date date) {
      final Calendar calendar = Work.get().iso8601Calendar;
      calendar.setTime(date);
      sb.append(calendar.getWeekYear());
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
  MON("MMM", "The abbreviated month name, in uppercase") {
    @Override public void format(StringBuilder sb, Date date) {
      final Work work = Work.get();
      sb.append(work.mmmFormat.format(date).toUpperCase(Locale.ROOT));
    }
  },
  Mon("MMM", "The abbreviated month name, capitalized") {
    @Override public void format(StringBuilder sb, Date date) {
      final Work work = Work.get();
      sb.append(work.mmmFormat.format(date));
    }
  },
  mon("MMM", "The abbreviated month name, in lowercase") {
    @Override public void format(StringBuilder sb, Date date) {
      final Work work = Work.get();
      sb.append(work.mmmFormat.format(date).toLowerCase(Locale.ROOT));
    }
  },
  MONTH("MMMM", "The full month name (English), in uppercase") {
    @Override public void format(StringBuilder sb, Date date) {
      final Work work = Work.get();
      sb.append(work.mmmmFormat.format(date).toUpperCase(Locale.ROOT));
    }
  },
  Month("MMMM", "The full month name (English), capitalized") {
    @Override public void format(StringBuilder sb, Date date) {
      final Work work = Work.get();
      sb.append(work.mmmmFormat.format(date));
    }
  },
  month("MMMM", "The full month name (English), in lowercase") {
    @Override public void format(StringBuilder sb, Date date) {
      final Work work = Work.get();
      sb.append(work.mmmmFormat.format(date).toLowerCase(Locale.ROOT));
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
    @Override public void toPattern(StringBuilder sb) throws UnsupportedOperationException {
      throwToPatternNotImplemented();
    }
    @Override public void format(StringBuilder sb, Date date) {
      final Calendar calendar = Work.get().calendar;
      calendar.setTime(date);
      sb.append(String.format(Locale.ROOT, "%d", (calendar.get(Calendar.MONTH) / 3) + 1));
    }
  },
  AMPM("", "The time as Meridian Indicator in uppercase") {
    @Override public void format(StringBuilder sb, Date date) {
      final Calendar calendar = Work.get().calendar;
      calendar.setTime(date);
      sb.append(calendar.get(Calendar.AM_PM) == Calendar.AM ? "AM" : "PM");
    }
  },
  AM_PM("", "The time as Meridian Indicator in uppercase with dot") {
    @Override public void format(StringBuilder sb, Date date) {
      final Calendar calendar = Work.get().calendar;
      calendar.setTime(date);
      sb.append(calendar.get(Calendar.AM_PM) == Calendar.AM ? "A.M." : "P.M.");
    }
  },
  ampm("", "The time as Meridian Indicator in lowercase") {
    @Override public void format(StringBuilder sb, Date date) {
      final Calendar calendar = Work.get().calendar;
      calendar.setTime(date);
      sb.append(calendar.get(Calendar.AM_PM) == Calendar.AM ? "am" : "pm");
    }
  },
  am_pm("", "The time as Meridian Indicator in uppercase") {
    @Override public void format(StringBuilder sb, Date date) {
      final Calendar calendar = Work.get().calendar;
      calendar.setTime(date);
      sb.append(calendar.get(Calendar.AM_PM) == Calendar.AM ? "a.m." : "p.m.");
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
  SSSSS("s", "The seconds of the day (00000-86400)") {
    @Override public void format(StringBuilder sb, Date date) {
      Calendar calendar = Work.get().calendar;
      calendar.setTime(date);
      long timeInMillis = calendar.getTimeInMillis();

      // Set calendar to start of day for input date
      calendar.set(Calendar.HOUR_OF_DAY, 0);
      calendar.set(Calendar.MINUTE, 0);
      calendar.set(Calendar.SECOND, 0);
      calendar.set(Calendar.MILLISECOND, 0);
      long dayStartInMillis = calendar.getTimeInMillis();

      // Get seconds of the day as difference from day start time
      long secondsPassed = (timeInMillis - dayStartInMillis) / 1000;
      sb.append(String.format(Locale.ROOT, "%05d", secondsPassed));
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
      sb.append(String.format(Locale.ROOT, "%02d", calendar.get(Calendar.WEEK_OF_YEAR)));
    }
  },
  Y("y", "Last digit of year") {
    @Override public void format(StringBuilder sb, Date date) {
      final Work work = Work.get();
      String formattedYear = work.yyFormat.format(date);
      sb.append(formattedYear.substring(formattedYear.length() - 1));
    }
  },
  YY("yy", "Last 2 digits of year") {
    @Override public void format(StringBuilder sb, Date date) {
      final Work work = Work.get();
      sb.append(work.yyFormat.format(date));
    }
  },
  YYY("yyy", "Last 3 digits of year") {
    @Override public void format(StringBuilder sb, Date date) {
      final Work work = Work.get();
      String formattedYear = work.yyyyFormat.format(date);
      sb.append(formattedYear.substring(formattedYear.length() - 3));
    }
  },
  YYYY("yyyy", "The year with century as a decimal number") {
    @Override public void format(StringBuilder sb, Date date) {
      final Work work = Work.get();
      sb.append(work.yyyyFormat.format(date));
    }
  },
  pctY("yyyy", "The year with century as a decimal number") {
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

  final void throwToPatternNotImplemented() {
    throw new UnsupportedOperationException("Cannot convert '"
        + this.name().toUpperCase(Locale.ROOT) + "' FormatElement to Java pattern");
  }

  /** Work space. Provides a value for each mutable data structure that might
   * be needed by a format element. Ensures thread-safety. */
  static class Work {
    private static final TryThreadLocal<Work> THREAD_WORK =
        TryThreadLocal.withInitial(Work::new);

    /** Returns an instance of Work for this thread. */
    static Work get() {
      return THREAD_WORK.get();
    }

    final Calendar calendar = new Calendar.Builder()
        .setWeekDefinition(Calendar.SUNDAY, 1)
        .setTimeZone(DateTimeUtils.DEFAULT_ZONE)
        .setLocale(Locale.ROOT).build();

    final Calendar iso8601Calendar = new Calendar.Builder()
        .setCalendarType("iso8601")
        .setTimeZone(DateTimeUtils.DEFAULT_ZONE)
        .setLocale(Locale.ROOT).build();

    final DateFormat mmmFormat = new SimpleDateFormat(MON.javaFmt, Locale.US);
    /* Need to sse Locale.US instead of Locale.ROOT, because Locale.ROOT
     * may actually return the *short* month name instead of the long name.
     * See [CALCITE-6252] BigQuery FORMAT_DATE uses the wrong calendar for Julian dates:
     * https://issues.apache.org/jira/browse/CALCITE-6252.  This may be
     * specific to Java 11. */
    final DateFormat mmmmFormat = new SimpleDateFormat(MONTH.javaFmt, Locale.US);
    final DateFormat sssFormat = new SimpleDateFormat(FF3.javaFmt, Locale.ROOT);
    final DateFormat yyFormat = new SimpleDateFormat(YY.javaFmt, Locale.ROOT);
    final DateFormat yyyyFormat = new SimpleDateFormat(YYYY.javaFmt, Locale.ROOT);

    /** Util to return the full or abbreviated weekday name from date and expected TextStyle. */
    private String getDayFromDate(Date date, TextStyle style) {
      calendar.setTime(date);
      // The Calendar and SimpleDateFormatter do not seem to give correct results
      // for the day of the week prior to the Julian to Gregorian date change.
      // So we resort to using a LocalDate representation.
      LocalDate ld =
          LocalDate.of(calendar.get(Calendar.YEAR),
              // Calendar months are numbered from 0
              calendar.get(Calendar.MONTH) + 1,
              calendar.get(Calendar.DAY_OF_MONTH));
      return ld.getDayOfWeek().getDisplayName(style, Locale.ENGLISH);
    }
  }
}
