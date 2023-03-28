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
package org.apache.calcite.rel.type;

import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.util.NameMap;
import org.apache.calcite.util.TimestampString;
import org.apache.calcite.util.Util;

import org.apache.commons.math3.fraction.BigFraction;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Iterables;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.math.BigInteger;
import java.util.NavigableMap;
import java.util.Objects;

import static org.apache.calcite.avatica.util.DateTimeUtils.MILLIS_PER_DAY;

import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.util.Objects.requireNonNull;

/** Set of {@link TimeFrame} definitions.
 *
 * <p>Every SQL statement has a time frame set, and is accessed via
 * {@link RelDataTypeSystem#deriveTimeFrameSet(TimeFrameSet)}. If you want to
 * use a custom set of time frames, you should override that method. */
public class TimeFrameSet {
  final ImmutableMap<String, TimeFrames.TimeFrameImpl> map;
  final ImmutableMultimap<TimeFrames.TimeFrameImpl, TimeFrames.TimeFrameImpl> rollupMap;
  private final NameMap<TimeFrames.TimeFrameImpl> nameMap;

  TimeFrameSet(ImmutableMap<String, TimeFrames.TimeFrameImpl> map,
      ImmutableMultimap<TimeFrames.TimeFrameImpl, TimeFrames.TimeFrameImpl> rollupMap) {
    this.map = requireNonNull(map, "map");
    this.nameMap = NameMap.immutableCopyOf(map);
    this.rollupMap = requireNonNull(rollupMap, "rollupMap");
  }

  /** Creates a Builder. */
  public static Builder builder() {
    return new TimeFrames.BuilderImpl();
  }

  /** Returns the time frame with the given name (case-insensitive),
   * or returns null. */
  public @Nullable TimeFrame getOpt(String name) {
    final NavigableMap<String, TimeFrames.TimeFrameImpl> range =
        nameMap.range(name, false);
    @Nullable TimeFrame timeFrame =
        Iterables.getFirst(range.values(), null);
    while (timeFrame instanceof TimeFrames.AliasFrame) {
      timeFrame = ((TimeFrames.AliasFrame) timeFrame).frame;
    }
    return timeFrame;
  }

  /** Returns the time frame with the given name,
   * or throws {@link IllegalArgumentException} if not found.
   * If {@code name} is an alias, resolves to the underlying frame. */
  public TimeFrame get(String name) {
    TimeFrame timeFrame = getOpt(name);
    if (timeFrame == null) {
      throw new IllegalArgumentException("unknown frame: " + name);
    }
    return timeFrame;
  }

  /** Returns the time frame with the given name,
   * or throws {@link IllegalArgumentException}. */
  public TimeFrame get(TimeUnit timeUnit) {
    return get(timeUnit.name());
  }

  /** Computes "FLOOR(date TO frame)", where {@code date} is the number of
   * days since UNIX Epoch. */
  public int floorDate(int date, TimeFrame frame) {
    return floorCeilDate(date, frame, false);
  }

  /** Computes "FLOOR(date TO frame)", where {@code date} is the number of
   * days since UNIX Epoch. */
  public int ceilDate(int date, TimeFrame frame) {
    return floorCeilDate(date, frame, true);
  }

  /** Computes "FLOOR(timestamp TO frame)" or "FLOOR(timestamp TO frame)",
   * where {@code date} is the number of days since UNIX Epoch. */
  private int floorCeilDate(int date, TimeFrame frame, boolean ceil) {
    final TimeFrame dayFrame = get(TimeUnit.DAY);
    final BigFraction perDay = frame.per(dayFrame);
    if (perDay != null
        && perDay.getNumerator().equals(BigInteger.ONE)) {
      final int m = perDay.getDenominator().intValueExact(); // 7 for WEEK
      final int mod = floorMod(date - frame.dateEpoch(), m);
      return date - mod + (ceil ? m : 0);
    }
    final TimeFrame monthFrame = get(TimeUnit.MONTH);
    final BigFraction perMonth = frame.per(monthFrame);
    if (perMonth != null
        && perMonth.getNumerator().equals(BigInteger.ONE)) {
      final int y2 =
          (int) DateTimeUtils.unixDateExtract(TimeUnitRange.YEAR, date);
      final int m2 =
          (int) DateTimeUtils.unixDateExtract(TimeUnitRange.MONTH, date);
      final int fullMonth = TimeFrames.fullMonth(y2, m2);

      final int m = perMonth.getDenominator().intValueExact(); // e.g. 12 for YEAR
      final int mod = floorMod(fullMonth - frame.monthEpoch(), m);
      return TimeFrames.mdToUnixDate(fullMonth - mod + (ceil ? m : 0), 1);
    }
    final TimeFrame isoYearFrame = get(TimeUnit.ISOYEAR);
    final BigFraction perIsoYear = frame.per(isoYearFrame);
    if (perIsoYear != null
        && perIsoYear.getNumerator().equals(BigInteger.ONE)) {
      return TimeFrames.floorCeilIsoYear(date, ceil);
    }
    return date;
  }

  /** Computes "FLOOR(timestamp TO frame)", where {@code ts} is the number of
   * milliseconds since UNIX Epoch. */
  public long floorTimestamp(long ts, TimeFrame frame) {
    return floorCeilTimestamp(ts, frame, false);
  }

  /** Computes "CEIL(timestamp TO frame)", where {@code ts} is the number of
   * milliseconds since UNIX Epoch. */
  public long ceilTimestamp(long ts, TimeFrame frame) {
    return floorCeilTimestamp(ts, frame, true);
  }

  /** Computes "FLOOR(ts TO frame)" or "CEIL(ts TO frame)",
   * where {@code ts} is the number of milliseconds since UNIX Epoch. */
  private long floorCeilTimestamp(long ts, TimeFrame frame, boolean ceil) {
    final TimeFrame millisecondFrame = get(TimeUnit.MILLISECOND);
    final BigFraction perMillisecond = frame.per(millisecondFrame);
    if (perMillisecond != null
        && perMillisecond.getNumerator().equals(BigInteger.ONE)) {
      final long m = perMillisecond.getDenominator().longValue(); // e.g. 60,000 for MINUTE
      final long mod = floorMod(ts - frame.timestampEpoch(), m);
      return ts - mod + (ceil ? m : 0);
    }
    final TimeFrame monthFrame = get(TimeUnit.MONTH);
    final BigFraction perMonth = frame.per(monthFrame);
    if (perMonth != null
        && perMonth.getNumerator().equals(BigInteger.ONE)) {
      final long ts2 = floorTimestamp(ts, get(TimeUnit.DAY));
      final int d2 = (int) (ts2 / DateTimeUtils.MILLIS_PER_DAY);
      final int y2 =
          (int) DateTimeUtils.unixDateExtract(TimeUnitRange.YEAR, d2);
      final int m2 =
          (int) DateTimeUtils.unixDateExtract(TimeUnitRange.MONTH, d2);
      final int fullMonth = TimeFrames.fullMonth(y2, m2);

      final int m = perMonth.getDenominator().intValueExact(); // e.g. 12 for YEAR
      final int mod = floorMod(fullMonth - frame.monthEpoch(), m);
      return TimeFrames.unixTimestamp(fullMonth - mod + (ceil ? m : 0), 1, 0, 0, 0);
    }
    final TimeFrame isoYearFrame = get(TimeUnit.ISOYEAR);
    final BigFraction perIsoYear = frame.per(isoYearFrame);
    if (perIsoYear != null
        && perIsoYear.getNumerator().equals(BigInteger.ONE)) {
      final long ts2 = floorTimestamp(ts, get(TimeUnit.DAY));
      final int d2 = (int) (ts2 / DateTimeUtils.MILLIS_PER_DAY);
      return (long) TimeFrames.floorCeilIsoYear(d2, ceil) * MILLIS_PER_DAY;
    }
    return ts;
  }

  /** Returns the time unit that this time frame is based upon, or null. */
  public @Nullable TimeUnit getUnit(TimeFrame timeFrame) {
    final TimeUnit timeUnit = Util.enumVal(TimeUnit.class, timeFrame.name());
    if (timeUnit == null) {
      return null;
    }
    TimeFrame timeFrame1 = getOpt(timeUnit.name());
    return Objects.equals(timeFrame1, timeFrame) ? timeUnit : null;
  }

  public int addDate(int date, int interval, TimeFrame frame) {
    final TimeFrame dayFrame = get(TimeUnit.DAY);
    final BigFraction perDay = frame.per(dayFrame);
    if (perDay != null
        && perDay.getNumerator().equals(BigInteger.ONE)) {
      final int m = perDay.getDenominator().intValueExact(); // 7 for WEEK
      return date + interval * m;
    }

    final TimeFrame monthFrame = get(TimeUnit.MONTH);
    final BigFraction perMonth = frame.per(monthFrame);
    if (perMonth != null
        && perMonth.getNumerator().equals(BigInteger.ONE)) {
      final int m = perMonth.getDenominator().intValueExact(); // e.g. 12 for YEAR
      return DateTimeUtils.addMonths(date, interval * m);
    }

    // Unknown time frame. Return the original value unchanged.
    return date;
  }

  public long addTimestamp(long timestamp, long interval, TimeFrame frame) {
    final TimeFrame msFrame = get(TimeUnit.MILLISECOND);
    final BigFraction perMilli = frame.per(msFrame);
    if (perMilli != null
        && perMilli.getNumerator().equals(BigInteger.ONE)) {
      // 1,000 for SECOND, 86,400,000 for DAY
      final long m = perMilli.getDenominator().longValueExact();
      return timestamp + interval * m;
    }
    final TimeFrame monthFrame = get(TimeUnit.MONTH);
    final BigFraction perMonth = frame.per(monthFrame);
    if (perMonth != null
        && perMonth.getNumerator().equals(BigInteger.ONE)) {
      final long m = perMonth.getDenominator().longValueExact(); // e.g. 12 for YEAR
      return DateTimeUtils.addMonths(timestamp, (int) (interval * m));
    }

    // Unknown time frame. Return the original value unchanged.
    return timestamp;
  }

  public int diffDate(int date, int date2, TimeFrame frame) {
    final TimeFrame dayFrame = get(TimeUnit.DAY);
    final BigFraction perDay = frame.per(dayFrame);
    if (perDay != null
        && perDay.getNumerator().equals(BigInteger.ONE)) {
      final int m = perDay.getDenominator().intValueExact(); // 7 for WEEK
      final int delta = date2 - date;
      return floorDiv(delta, m);
    }

    final TimeFrame monthFrame = get(TimeUnit.MONTH);
    final BigFraction perMonth = frame.per(monthFrame);
    if (perMonth != null
        && perMonth.getNumerator().equals(BigInteger.ONE)) {
      final int m = perMonth.getDenominator().intValueExact(); // e.g. 12 for YEAR
      final int delta = DateTimeUtils.subtractMonths(date2, date);
      return floorDiv(delta, m);
    }

    // Unknown time frame. Return the original value unchanged.
    return date;
  }

  public long diffTimestamp(long timestamp, long timestamp2, TimeFrame frame) {
    final TimeFrame msFrame = get(TimeUnit.MILLISECOND);
    final BigFraction perMilli = frame.per(msFrame);
    if (perMilli != null
        && perMilli.getNumerator().equals(BigInteger.ONE)) {
      // 1,000 for SECOND, 86,400,000 for DAY
      final long m = perMilli.getDenominator().longValueExact();
      final long delta = timestamp2 - timestamp;
      return floorDiv(delta, m);
    }
    final TimeFrame monthFrame = get(TimeUnit.MONTH);
    final BigFraction perMonth = frame.per(monthFrame);
    if (perMonth != null
        && perMonth.getNumerator().equals(BigInteger.ONE)) {
      final long m = perMonth.getDenominator().longValueExact(); // e.g. 12 for YEAR
      final long delta = DateTimeUtils.subtractMonths(timestamp2, timestamp);
      return floorDiv(delta, m);
    }

    // Unknown time frame. Return the original value unchanged.
    return timestamp;
  }

  /** For ISOWEEK and WEEK(WEEKDAY), EXTRACT can be rewritten
   * as the composition of DATE_DIFF and DATE_TRUNC. DAYOFWEEK and DAYOFYEAR
   * are just translated to DOW and DOY respectively. */
  public long extractDate(int date, TimeFrame timeFrame) {
    TimeUnitRange timeUnitRange;
    int offset = 0;
    // Date will be truncated to either ISOYEAR or YEAR depending on time frame.
    switch (timeFrame.name()) {
    case "DAYOFWEEK":
      return DateTimeUtils.unixDateExtract(TimeUnitRange.DOW, date);
    case "DAYOFYEAR":
      return DateTimeUtils.unixDateExtract(TimeUnitRange.DOY, date);
    case "ISOWEEK":
      timeUnitRange = TimeUnitRange.ISOYEAR;
      offset += 1;
      break;
    default:
      if (TimeFrames.WEEK_FRAME_NAMES.contains(timeFrame.name())) {
        timeUnitRange = TimeUnitRange.YEAR;
      } else {
        throw new IllegalArgumentException("Unsupported frame for EXTRACT: " + timeFrame.name());
      }
      break;
    }
    // Date is truncated first to year/isoyear and then to the provided time frame.
    int y0 = (int) DateTimeUtils.unixDateFloor(timeUnitRange, date);
    int date2 = floorDate(y0, timeFrame);

    // For WEEK(WEEKDAY), if year starts on WEEKDAY then offset of +1 is required.
    if (date2 == y0 && timeUnitRange != TimeUnitRange.ISOYEAR) {
      offset += 1;
    }
    return (long) diffDate(date2, date, timeFrame) + offset;
  }

  /** Builds a collection of time frames. */
  public interface Builder {
    /** Creates a {@code TimeFrameSet}. */
    TimeFrameSet build();

    /** Defines a core time frame. */
    Builder addCore(String name);

    /** Defines a time frame that is the number of a minor unit within a major
     * frame. For example, the "DOY" frame has minor "DAY" and major "YEAR". */
    Builder addQuotient(String name, String minorName,
        String majorName);

    /** Defines a time frame that consists of {@code count} instances of
     * a base frame. */
    Builder addMultiple(String name, Number count, String baseName);

    /** Defines a time frame such that each base frame consists of {@code count}
     * instances of the new frame. */
    Builder addDivision(String name, Number count, String baseName);

    /** Defines a rollup from one frame to another.
     *
     * <p>An explicit rollup is not necessary for frames where one is a multiple
     * of another (such as {@code MILLISECOND} to {@code HOUR}). Only use this
     * method for frames that are not multiples (such as {@code DAY} to
     * {@code MONTH}).
     *
     * <p>How do we automatically roll up from say, "minute15" to "hour7"?
     * Because we know the following:
     * <ul>
     *   <li>"minute15" and "hour7" are based on the same core frame (seconds);
     *   <li>"minute15" is 15 * 60 seconds, "hour7" is 7 * 60 * 60 seconds,
     *     and the one divides the other;
     *   <li>They have the same offset, 1970-01-01 00:00:00. (Different offsets
     *     would be OK too, as they are a whole multiple apart.)
     * </ul>
     *
     * <p>A month is not a fixed multiple of days, but a rollup is still
     * possible, because the start of a month is always aligned with the start
     * of a day. This means that you can compute a month total by adding up the
     * day totals for all days in that month. This is useful if you have an
     * aggregate table on DAY and you want to answer a query on {@code MONTH}.
     *
     * <p>There is no rollup from {@code WEEK} to {@code YEAR}, because of a
     * lack of alignment: a year does not start on the first day of a week, and
     * so you cannot compute the total for, say, the year 2022 by adding the
     * totals for all weeks that fall in 2022.
     *
     * <p>Incidentally, {@code ISOWEEK} and {@code ISOYEAR} are designed so that
     * {@code ISOWEEK} can roll up to {@code ISOYEAR}. Every {@code ISOYEAR} and
     * {@code ISOWEEK} start on a Monday, so they are aligned. An
     * {@code ISOYEAR} consists of either 52 or 53 {@code ISOWEEK} instances,
     * but the variable multiple is not a problem; the alignment ensures that
     * rollup is valid. */
    Builder addRollup(String fromName, String toName);

    /** Adds all time frames in {@code timeFrameSet} to this {@code Builder}. */
    Builder addAll(TimeFrameSet timeFrameSet);

    /** Replaces the epoch of the most recently added frame. */
    Builder withEpoch(TimestampString epoch);

    /** Defines an alias for an existing frame.
     *
     * <p>For example, {@code add("Y", "YEAR")} adds "Y" as an alias for the
     * built-in frame {@code YEAR}.
     *
     * @param name The alias
     * @param originalName Name of the existing frame
     */
    Builder addAlias(String name, String originalName);
  }

}
