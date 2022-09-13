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
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.util.MonotonicSupplier;
import org.apache.calcite.util.NameMap;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.TimestampString;
import org.apache.calcite.util.Util;

import org.apache.commons.math3.fraction.BigFraction;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Iterables;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.math.BigInteger;
import java.util.Calendar;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.function.Supplier;

import static org.apache.calcite.avatica.util.DateTimeUtils.EPOCH_JULIAN;
import static org.apache.calcite.avatica.util.DateTimeUtils.MILLIS_PER_DAY;

import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.util.Objects.requireNonNull;

/** Set of {@link TimeFrame} definitions. */
public class TimeFrameSet {
  private final ImmutableMap<String, TimeFrameImpl> map;
  private final ImmutableMultimap<TimeFrameImpl, TimeFrameImpl> rollupMap;
  private final NameMap<TimeFrameImpl> nameMap;

  private TimeFrameSet(ImmutableMap<String, TimeFrameImpl> map,
      ImmutableMultimap<TimeFrameImpl, TimeFrameImpl> rollupMap) {
    this.map = requireNonNull(map, "map");
    this.nameMap = NameMap.immutableCopyOf(map);
    this.rollupMap = requireNonNull(rollupMap, "rollupMap");
  }

  /** Creates a Builder. */
  public static Builder builder() {
    return new Builder();
  }

  /** Returns the time frame with the given name (case-insensitive),
   * or returns null. */
  public @Nullable TimeFrame getOpt(String name) {
    final NavigableMap<String, TimeFrameImpl> range =
        nameMap.range(name, false);
    @Nullable TimeFrame timeFrame =
        Iterables.getFirst(range.values(), null);
    while (timeFrame instanceof AliasFrame) {
      timeFrame = ((AliasFrame) timeFrame).frame;
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
      final int fullMonth = fullMonth(y2, m2);

      final int m = perMonth.getDenominator().intValueExact(); // e.g. 12 for YEAR
      final int mod = floorMod(fullMonth - frame.monthEpoch(), m);
      return mdToUnixDate(fullMonth - mod + (ceil ? m : 0), 1);
    }
    final TimeFrame isoYearFrame = get(TimeUnit.ISOYEAR);
    final BigFraction perIsoYear = frame.per(isoYearFrame);
    if (perIsoYear != null
        && perIsoYear.getNumerator().equals(BigInteger.ONE)) {
      return floorCeilIsoYear(date, ceil);
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
      final int fullMonth = fullMonth(y2, m2);

      final int m = perMonth.getDenominator().intValueExact(); // e.g. 12 for YEAR
      final int mod = floorMod(fullMonth - frame.monthEpoch(), m);
      return unixTimestamp(fullMonth - mod + (ceil ? m : 0), 1, 0, 0, 0);
    }
    final TimeFrame isoYearFrame = get(TimeUnit.ISOYEAR);
    final BigFraction perIsoYear = frame.per(isoYearFrame);
    if (perIsoYear != null
        && perIsoYear.getNumerator().equals(BigInteger.ONE)) {
      final long ts2 = floorTimestamp(ts, get(TimeUnit.DAY));
      final int d2 = (int) (ts2 / DateTimeUtils.MILLIS_PER_DAY);
      return (long) floorCeilIsoYear(d2, ceil) * MILLIS_PER_DAY;
    }
    return ts;
  }

  /** Given a date, returns the date of the first day of its ISO Year.
   * Usually occurs in the same calendar year, but may be as early as Dec 29
   * of the previous calendar year. */
  // TODO: move it into DateTimeUtils.julianExtract,
  // so that it can be called from DateTimeUtils.unixDateExtract
  private static int floorCeilIsoYear(int date, boolean ceil) {
    final int year =
        (int) DateTimeUtils.unixDateExtract(TimeUnitRange.YEAR, date);
    return (int) firstMondayOfFirstWeek(year + (ceil ? 1 : 0)) - EPOCH_JULIAN;
  }

  /** Returns the first day of the first week of a year.
   * Per ISO-8601 it is the Monday of the week that contains Jan 4,
   * or equivalently, it is a Monday between Dec 29 and Jan 4.
   * Sometimes it is in the year before the given year. */
  // Note: copied from DateTimeUtils
  private static long firstMondayOfFirstWeek(int year) {
    final long janFirst = DateTimeUtils.ymdToJulian(year, 1, 1);
    final long janFirstDow = floorMod(janFirst + 1, (long) 7); // sun=0, sat=6
    return janFirst + (11 - janFirstDow) % 7 - 3;
  }

  /** Returns the number of months since 1 BCE.
   *
   * <p>Parameters mean the same as in
   * {@link DateTimeUtils#ymdToJulian(int, int, int)}.
   *
   * @param year Year (e.g. 2020 means 2020 CE, 0 means 1 BCE)
   * @param month Month (e.g. 1 means January)
   */
  public static int fullMonth(int year, int month) {
    return year * 12 + (month - 1);
  }

  /** Given a {@link #fullMonth(int, int)} value, returns the month
   * (1 means January). */
  private static int fullMonthToMonth(int fullMonth) {
    return floorMod(fullMonth, 12) + 1;
  }

  /** Given a {@link #fullMonth(int, int)} value, returns the year
   * (2020 means 2020 CE). */
  private static int fullMonthToYear(int fullMonth) {
    return floorDiv(fullMonth, 12);
  }

  /** As {@link DateTimeUtils#unixTimestamp(int, int, int, int, int, int)}
   * but based on a fullMonth value (per {@link #fullMonth(int, int)}). */
  private static long unixTimestamp(int fullMonth, int day, int hour,
      int minute, int second) {
    final int year = fullMonthToYear(fullMonth);
    final int month = fullMonthToMonth(fullMonth);
    return DateTimeUtils.unixTimestamp(year, month, day, hour, minute, second);
  }

  public static int mdToUnixDate(int fullMonth, int day) {
    final int year = fullMonthToYear(fullMonth);
    final int month = fullMonthToMonth(fullMonth);
    return DateTimeUtils.ymdToUnixDate(year, month, day);
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
      return SqlFunctions.addMonths(date, interval * m);
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
      return SqlFunctions.addMonths(timestamp, (int) (interval * m));
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

  /** Builds a collection of time frames. */
  public static class Builder {
    Builder() {
    }

    final MonotonicSupplier<TimeFrameSet> frameSetSupplier =
        new MonotonicSupplier<>();
    final Map<String, TimeFrameImpl> map = new LinkedHashMap<>();
    final ImmutableMultimap.Builder<TimeFrameImpl, TimeFrameImpl> rollupList =
        ImmutableMultimap.builder();

    public TimeFrameSet build() {
      final TimeFrameSet frameSet =
          new TimeFrameSet(ImmutableMap.copyOf(map), rollupList.build());
      frameSetSupplier.accept(frameSet);
      return frameSet;
    }

    /** Converts a number to an exactly equivalent {@code BigInteger}.
     * May silently lose precision if n is a {@code Float} or {@code Double}. */
    static BigInteger toBigInteger(Number number) {
      return number instanceof BigInteger ? (BigInteger) number
          : BigInteger.valueOf(number.longValue());
    }

    public Builder addCore(String name) {
      map.put(name, new CoreFrame(frameSetSupplier, name));
      return this;
    }

    /** Defines a time unit that consists of {@code count} instances of
     * {@code baseUnit}. */
    Builder addSub(String name, boolean divide, Number count,
        String baseName, TimestampString epoch) {
      final TimeFrameImpl baseFrame = get(baseName);
      final BigInteger factor = toBigInteger(count);

      final CoreFrame coreFrame = baseFrame.core();
      final BigFraction coreFactor = divide
          ? baseFrame.coreMultiplier().divide(factor)
          : baseFrame.coreMultiplier().multiply(factor);

      map.put(name,
          new SubFrame(name, baseFrame, divide, factor, coreFrame,
              coreFactor, epoch));
      return this;
    }

    /** Defines a time unit that is the number of a minor unit within a major
     * unit. For example, the "DOY" frame has minor "DAY" and major "YEAR". */
    Builder addQuotient(String name, String minorName, String majorName) {
      final TimeFrameImpl minorFrame = get(minorName);
      final TimeFrameImpl majorFrame = get(majorName);
      map.put(name, new QuotientFrame(name, minorFrame, majorFrame));
      return this;
    }

    /** Returns the time frame with the given name,
     * or throws {@link IllegalArgumentException}. */
    TimeFrameImpl get(String name) {
      final TimeFrameImpl timeFrame = map.get(name);
      if (timeFrame == null) {
        throw new IllegalArgumentException("unknown frame: " + name);
      }
      return timeFrame;
    }

    /** Defines a time unit that consists of {@code count} instances of
     * {@code baseUnit}. */
    public Builder addMultiple(String name, Number count,
        String baseName) {
      return addSub(name, false, count, baseName, TimestampString.EPOCH);
    }

    /** Defines such that each {@code baseUnit} consists of {@code count}
     * instances of the new unit. */
    public Builder addDivision(String name, Number count, String baseName) {
      return addSub(name, true, count, baseName, TimestampString.EPOCH);
    }

    /** Defines a rollup from one frame to another.
     *
     * <p>An explicit rollup is not necessary for frames where one is a multiple
     * of another (such as MILLISECOND to HOUR). Only use this method for frames
     * that are not multiples (such as DAY to MONTH). */
    public Builder addRollup(String fromName, String toName) {
      final TimeFrameImpl fromFrame = get(fromName);
      final TimeFrameImpl toFrame = get(toName);
      rollupList.put(fromFrame, toFrame);
      return this;
    }

    /** Adds all time frames in {@code timeFrameSet} to this Builder. */
    public Builder addAll(TimeFrameSet timeFrameSet) {
      timeFrameSet.map.values().forEach(frame -> frame.replicate(this));
      return this;
    }

    /** Replaces the epoch of the most recently added frame. */
    public Builder withEpoch(TimestampString epoch) {
      final Map.Entry<String, TimeFrameImpl> entry =
          Iterables.getLast(map.entrySet());
      final String name = entry.getKey();
      final SubFrame value =
          requireNonNull((SubFrame) map.remove(name));
      value.replicateWithEpoch(this, epoch);
      return this;
    }

    /** Defines an alias for an existing frame.
     *
     * <p>For example, {@code add("Y", "YEAR")} adds "Y" as an alias for the
     * built-in frame "YEAR".
     *
     * @param name The alias
     * @param originalName Name of the existing frame
     */
    public Builder addAlias(String name, String originalName) {
      final TimeFrameImpl frame =
          requireNonNull(map.get(originalName),
              () -> "unknown frame " + originalName);
      map.put(name, new AliasFrame(name, frame));
      return this;
    }
  }

  /** Implementation of {@link TimeFrame}. */
  abstract static class TimeFrameImpl implements TimeFrame {
    final String name;
    final Supplier<TimeFrameSet> frameSetSupplier;

    TimeFrameImpl(Supplier<TimeFrameSet> frameSetSupplier, String name) {
      this.frameSetSupplier =
          requireNonNull(frameSetSupplier, "frameSetSupplier");
      this.name = requireNonNull(name, "name");
    }

    @Override public String toString() {
      return name;
    }

    @Override public TimeFrameSet frameSet() {
      return frameSetSupplier.get();
    }

    @Override public String name() {
      return name;
    }

    @Override public @Nullable BigFraction per(TimeFrame timeFrame) {
      final Map<TimeFrame, BigFraction> map = new HashMap<>();
      final Map<TimeFrame, BigFraction> map2 = new HashMap<>();
      expand(map, BigFraction.ONE);
      ((TimeFrameImpl) timeFrame).expand(map2, BigFraction.ONE);
      for (Map.Entry<TimeFrame, BigFraction> entry : map.entrySet()) {
        // We assume that if there are multiple units in common, the multipliers
        // are the same for all.
        //
        // If they are not, it will be because the user defined the units
        // inconsistently. TODO: check for that in the builder.
        final BigFraction value2 = map2.get(entry.getKey());
        if (value2 != null) {
          return value2.divide(entry.getValue());
        }
      }
      return null;
    }

    protected void expand(Map<TimeFrame, BigFraction> map, BigFraction f) {
      map.put(this, f);
    }

    /** Adds a time frame like this to a builder. */
    abstract void replicate(Builder b);

    protected abstract CoreFrame core();

    protected abstract BigFraction coreMultiplier();

    @Override public boolean canRollUpTo(TimeFrame toFrame) {
      if (toFrame == this) {
        return true;
      }
      if (toFrame instanceof TimeFrameImpl) {
        final TimeFrameImpl toFrame1 = (TimeFrameImpl) toFrame;
        if (canDirectlyRollUp(this, toFrame1)) {
          return true;
        }
        final TimeFrameSet frameSet = frameSet();
        if (frameSet.rollupMap.entries().contains(Pair.of(this, toFrame1))) {
          return true;
        }
        // Hard-code roll-up via DAY-to-MONTH bridge, for now.
        final TimeFrameImpl day =
            requireNonNull(frameSet.map.get(TimeUnit.DAY.name()));
        final TimeFrameImpl month =
            requireNonNull(frameSet.map.get(TimeUnit.MONTH.name()));
        if (canDirectlyRollUp(this, day)
            && canDirectlyRollUp(month, toFrame1)) {
          return true;
        }
        // Hard-code roll-up via ISOWEEK-to-ISOYEAR bridge, for now.
        final TimeFrameImpl isoYear =
            requireNonNull(frameSet.map.get(TimeUnit.ISOYEAR.name()));
        final TimeFrameImpl isoWeek =
            requireNonNull(frameSet.map.get("ISOWEEK"));
        if (canDirectlyRollUp(this, isoWeek)
            && canDirectlyRollUp(isoYear, toFrame1)) {
          return true;
        }
      }
      return false;
    }

    private static boolean canDirectlyRollUp(TimeFrameImpl from,
        TimeFrameImpl to) {
      if (from.core().equals(to.core())) {
        if (divisible(from.coreMultiplier(), to.coreMultiplier())) {
          BigFraction diff = new BigFraction(from.core().epochDiff(from, to));
          return divisible(from.coreMultiplier(), diff);
        }
        return false;
      }
      return false;
    }

    /** Returns whether {@code numerator} is divisible by {@code denominator}.
     *
     * <p>For example, {@code divisible(6, 2)} returns {@code true};
     * {@code divisible(0, 2)} also returns {@code true};
     * {@code divisible(2, 6)} returns {@code false}. */
    private static boolean divisible(BigFraction numerator,
        BigFraction denominator) {
      return denominator.equals(BigFraction.ZERO)
          || numerator
          .divide(denominator)
          .getNumerator()
          .abs()
          .equals(BigInteger.ONE);
    }
  }

  /** Core time frame (such as SECOND, MONTH, ISOYEAR). */
  static class CoreFrame extends TimeFrameImpl {
    CoreFrame(Supplier<TimeFrameSet> frameSetSupplier, String name) {
      super(frameSetSupplier, name);
    }

    @Override void replicate(Builder b) {
      b.addCore(name);
    }

    @Override protected CoreFrame core() {
      return this;
    }

    @Override protected BigFraction coreMultiplier() {
      return BigFraction.ONE;
    }

    /** Returns the difference between the epochs of two frames, in the
     * units of this core frame. */
    BigInteger epochDiff(TimeFrameImpl from, TimeFrameImpl to) {
      assert from.core() == this;
      assert to.core() == this;
      switch (name) {
      case "MONTH":
        return BigInteger.valueOf(from.monthEpoch())
            .subtract(BigInteger.valueOf(to.monthEpoch()));
      default:
        return BigInteger.valueOf(from.timestampEpoch())
            .subtract(BigInteger.valueOf(to.timestampEpoch()));
      }
    }
  }

  /** A time frame that is composed of another time frame.
   *
   * <p>For example, {@code MINUTE} is composed of 60 {@code SECOND};
   * (factor = 60, divide = false);
   * {@code MILLISECOND} is composed of 1 / 1000 {@code SECOND}
   * (factor = 1000, divide = true).
   *
   * <p>A sub-time frame S is aligned with its parent frame P;
   * that is, every instance of S belongs to one instance of P.
   * Every {@code MINUTE} belongs to one {@code HOUR};
   * not every {@code WEEK} belongs to precisely one {@code MONTH} or
   * {@code MILLENNIUM}.
   */
  static class SubFrame extends TimeFrameImpl {
    private final TimeFrameImpl base;
    private final boolean divide;
    private final BigInteger multiplier;
    private final CoreFrame coreFrame;

    /** The number of core frames that are equivalent to one of these. For
     * example, MINUTE, HOUR, MILLISECOND all have core = SECOND, and have
     * multipliers 60, 3,600, 1 / 1,000 respectively. */
    private final BigFraction coreMultiplier;
    private final TimestampString epoch;

    SubFrame(String name, TimeFrameImpl base, boolean divide,
        BigInteger multiplier, CoreFrame coreFrame,
        BigFraction coreMultiplier, TimestampString epoch) {
      super(base.frameSetSupplier, name);
      this.base = requireNonNull(base, "base");
      this.divide = divide;
      this.multiplier = requireNonNull(multiplier, "multiplier");
      this.coreFrame = requireNonNull(coreFrame, "coreFrame");
      this.coreMultiplier = requireNonNull(coreMultiplier, "coreMultiplier");
      this.epoch = requireNonNull(epoch, "epoch");
    }

    @Override public String toString() {
      return name + ", composedOf " + multiplier + " " + base.name;
    }

    @Override public int dateEpoch() {
      return (int) floorDiv(epoch.getMillisSinceEpoch(),
          DateTimeUtils.MILLIS_PER_DAY);
    }

    @Override public int monthEpoch() {
      final Calendar calendar = epoch.toCalendar();
      int y = calendar.get(Calendar.YEAR); // 2020 CE is represented by 2020
      int m = calendar.get(Calendar.MONTH) + 1; // January is represented by 1
      return fullMonth(y, m);
    }

    @Override public long timestampEpoch() {
      return epoch.getMillisSinceEpoch();
    }

    @Override void replicate(Builder b) {
      b.addSub(name, divide, multiplier, base.name, epoch);
    }

    /** Returns a copy of this TimeFrameImpl with a given epoch. */
    void replicateWithEpoch(Builder b, TimestampString epoch) {
      b.addSub(name, divide, multiplier, base.name, epoch);
    }

    @Override protected void expand(Map<TimeFrame, BigFraction> map,
        BigFraction f) {
      super.expand(map, f);
      base.expand(map, divide ? f.divide(multiplier) : f.multiply(multiplier));
    }

    @Override protected CoreFrame core() {
      return coreFrame;
    }

    @Override protected BigFraction coreMultiplier() {
      return coreMultiplier;
    }
  }

  /** Frame that defines is based on a minor frame and resets whenever the major
   * frame resets. For example, "DOY" (day of year) is based on DAY and resets
   * every YEAR. */
  static class QuotientFrame extends TimeFrameImpl {
    private final TimeFrameImpl minorFrame;
    private final TimeFrameImpl majorFrame;

    QuotientFrame(String name, TimeFrameImpl minorFrame,
        TimeFrameImpl majorFrame) {
      super(minorFrame.frameSetSupplier, name);
      this.minorFrame = requireNonNull(minorFrame, "minorFrame");
      this.majorFrame = requireNonNull(majorFrame, "majorFrame");
    }

    @Override void replicate(Builder b) {
      b.addQuotient(name, minorFrame.name, majorFrame.name);
    }

    @Override protected CoreFrame core() {
      return minorFrame.core();
    }

    @Override protected BigFraction coreMultiplier() {
      return minorFrame.coreMultiplier();
    }
  }

  /** Frame that defines an alias. */
  static class AliasFrame extends TimeFrameImpl {
    private final TimeFrameImpl frame;

    AliasFrame(String name, TimeFrameImpl frame) {
      super(frame.frameSetSupplier, name);
      this.frame = requireNonNull(frame, "frame");
    }

    @Override void replicate(Builder b) {
      b.addAlias(name, frame.name);
    }

    @Override protected CoreFrame core() {
      throw new UnsupportedOperationException();
    }

    @Override protected BigFraction coreMultiplier() {
      throw new UnsupportedOperationException();
    }
  }

}
