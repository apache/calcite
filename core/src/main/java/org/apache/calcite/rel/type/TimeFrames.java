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
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.util.MonotonicSupplier;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.TimestampString;

import org.apache.commons.math3.fraction.BigFraction;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Iterables;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.math.BigInteger;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static org.apache.calcite.avatica.util.DateTimeUtils.EPOCH_JULIAN;

import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.util.Objects.requireNonNull;

/** Utilities for {@link TimeFrame}. */
public class TimeFrames {
  private TimeFrames() {
  }

  /** The names of the frames that are WEEK starting on each week day.
   * Entry 0 is "WEEK_SUNDAY" and entry 6 is "WEEK_SATURDAY". */
  public static final List<String> WEEK_FRAME_NAMES =
      ImmutableList.of("WEEK_SUNDAY",
          "WEEK_MONDAY",
          "WEEK_TUESDAY",
          "WEEK_WEDNESDAY",
          "WEEK_THURSDAY",
          "WEEK_FRIDAY",
          "WEEK_SATURDAY");

  /** The core time frame set. Includes the time frames for all Avatica time
   * units plus ISOWEEK and week offset for each week day:
   *
   * <ul>
   *   <li>SECOND, and multiples MINUTE, HOUR, DAY, WEEK (starts on a Sunday),
   *   sub-multiples MILLISECOND, MICROSECOND, NANOSECOND,
   *   quotients DOY, DOW;
   *   <li>MONTH, and multiples QUARTER, YEAR, DECADE, CENTURY, MILLENNIUM;
   *   <li>ISOYEAR, and sub-unit ISOWEEK (starts on a Monday), quotient ISODOW;
   *   <li>WEEK(<i>weekday</i>) with <i>weekday</i> being one of
   *   SUNDAY, MONDAY, TUESDAY, WEDNESDAY, THURSDAY, FRIDAY, SATURDAY.
   * </ul>
   *
   * <p>Does not include EPOCH.
   */
  public static final TimeFrameSet CORE =
      addTsi(addCore(new BuilderImpl())).build();

  private static BuilderImpl addCore(BuilderImpl b) {
    b.addCore(TimeUnit.SECOND);
    b.addSub(TimeUnit.MINUTE, false, 60, TimeUnit.SECOND);
    b.addSub(TimeUnit.HOUR, false, 60, TimeUnit.MINUTE);
    b.addSub(TimeUnit.DAY, false, 24, TimeUnit.HOUR);
    b.addSub(TimeUnit.WEEK, false, 7, TimeUnit.DAY,
        new TimestampString(1970, 1, 4, 0, 0, 0)); // a sunday
    b.addSub(TimeUnit.MILLISECOND, true, 1_000, TimeUnit.SECOND);
    b.addSub(TimeUnit.MICROSECOND, true, 1_000, TimeUnit.MILLISECOND);
    b.addSub(TimeUnit.NANOSECOND, true, 1_000, TimeUnit.MICROSECOND);

    b.addSub(TimeUnit.EPOCH, false, 1, TimeUnit.SECOND,
        new TimestampString(1970, 1, 1, 0, 0, 0));

    b.addCore(TimeUnit.MONTH);
    b.addSub(TimeUnit.QUARTER, false, 3, TimeUnit.MONTH);
    b.addSub(TimeUnit.YEAR, false, 12, TimeUnit.MONTH);
    b.addSub(TimeUnit.DECADE, false, 10, TimeUnit.YEAR);
    b.addSub(TimeUnit.CENTURY, false, 100, TimeUnit.YEAR,
        new TimestampString(2001, 1, 1, 0, 0, 0));
    b.addSub(TimeUnit.MILLENNIUM, false, 1_000, TimeUnit.YEAR,
        new TimestampString(2001, 1, 1, 0, 0, 0));

    b.addCore(TimeUnit.ISOYEAR);
    b.addSub("ISOWEEK", false, 7, TimeUnit.DAY.name(),
        new TimestampString(1970, 1, 5, 0, 0, 0)); // a monday

    // Add "WEEK(SUNDAY)" through "WEEK(SATURDAY)"
    Ord.forEach(WEEK_FRAME_NAMES, (frameName, i) ->
        b.addSub(frameName, false, 7,
            "DAY", new TimestampString(1970, 1, 4 + i, 0, 0, 0)));

    b.addQuotient(TimeUnit.DOY, TimeUnit.DAY, TimeUnit.YEAR);
    b.addQuotient(TimeUnit.DOW, TimeUnit.DAY, TimeUnit.WEEK);
    b.addQuotient("DAYOFYEAR", TimeUnit.DAY.name(), TimeUnit.YEAR.name());
    b.addQuotient("DAYOFWEEK", TimeUnit.DAY.name(), TimeUnit.WEEK.name());
    b.addQuotient(TimeUnit.ISODOW.name(), TimeUnit.DAY.name(), "ISOWEEK");

    b.addRollup(TimeUnit.DAY, TimeUnit.MONTH);
    b.addRollup("ISOWEEK", TimeUnit.ISOYEAR.name());

    return b;
  }

  /** Adds abbreviations used by {@code TIMESTAMPADD}, {@code TIMESTAMPDIFF}
   * functions. */
  private static BuilderImpl addTsi(BuilderImpl b) {
    b.addAlias("FRAC_SECOND", TimeUnit.MICROSECOND.name());
    b.addAlias("SQL_TSI_FRAC_SECOND", TimeUnit.NANOSECOND.name());
    b.addAlias("SQL_TSI_MICROSECOND", TimeUnit.MICROSECOND.name());
    b.addAlias("SQL_TSI_SECOND", TimeUnit.SECOND.name());
    b.addAlias("SQL_TSI_MINUTE", TimeUnit.MINUTE.name());
    b.addAlias("SQL_TSI_HOUR", TimeUnit.HOUR.name());
    b.addAlias("SQL_TSI_DAY", TimeUnit.DAY.name());
    b.addAlias("SQL_TSI_WEEK", TimeUnit.WEEK.name());
    b.addAlias("SQL_TSI_MONTH", TimeUnit.MONTH.name());
    b.addAlias("SQL_TSI_QUARTER", TimeUnit.QUARTER.name());
    b.addAlias("SQL_TSI_YEAR", TimeUnit.YEAR.name());
    return b;
  }

  /** Given a date, returns the date of the first day of its ISO Year.
   * Usually occurs in the same calendar year, but may be as early as Dec 29
   * of the previous calendar year.
   *
   * <p>After
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5369">[CALCITE-5369]
   * In Avatica DateTimeUtils, add support for FLOOR and CEIL to ISOYEAR</a> is
   * fixed, we can use {@link DateTimeUtils#unixDateFloor} instead of this
   * method. */
  static int floorCeilIsoYear(int date, boolean ceil) {
    final int year =
        (int) DateTimeUtils.unixDateExtract(TimeUnitRange.YEAR, date);
    return (int) firstMondayOfFirstWeek(year + (ceil ? 1 : 0)) - EPOCH_JULIAN;
  }

  /** Returns the first day of the first week of a year.
   * Per ISO-8601 it is the Monday of the week that contains Jan 4,
   * or equivalently, it is a Monday between Dec 29 and Jan 4.
   * Sometimes it is in the year before the given year. */
  // Note: copied from DateTimeUtils
  static long firstMondayOfFirstWeek(int year) {
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
  static int fullMonth(int year, int month) {
    return year * 12 + (month - 1);
  }

  /** Given a {@link #fullMonth(int, int)} value, returns the month
   * (1 means January). */
  static int fullMonthToMonth(int fullMonth) {
    return floorMod(fullMonth, 12) + 1;
  }

  /** Given a {@link #fullMonth(int, int)} value, returns the year
   * (2020 means 2020 CE). */
  static int fullMonthToYear(int fullMonth) {
    return floorDiv(fullMonth, 12);
  }

  /** As {@link DateTimeUtils#unixTimestamp(int, int, int, int, int, int)}
   * but based on a fullMonth value (per {@link #fullMonth(int, int)}). */
  static long unixTimestamp(int fullMonth, int day, int hour,
      int minute, int second) {
    final int year = fullMonthToYear(fullMonth);
    final int month = fullMonthToMonth(fullMonth);
    return DateTimeUtils.unixTimestamp(year, month, day, hour, minute, second);
  }

  static int mdToUnixDate(int fullMonth, int day) {
    final int year = fullMonthToYear(fullMonth);
    final int month = fullMonthToMonth(fullMonth);
    return DateTimeUtils.ymdToUnixDate(year, month, day);
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

  /** Implementation of {@link TimeFrameSet.Builder}. */
  static class BuilderImpl implements TimeFrameSet.Builder {
    BuilderImpl() {
    }

    final MonotonicSupplier<TimeFrameSet> frameSetSupplier =
        new MonotonicSupplier<>();
    final Map<String, TimeFrameImpl> map = new LinkedHashMap<>();
    final ImmutableMultimap.Builder<TimeFrameImpl, TimeFrameImpl> rollupList =
        ImmutableMultimap.builder();

    @Override public TimeFrameSet build() {
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

    /** Returns the time frame with the given name,
     * or throws {@link IllegalArgumentException}. */
    TimeFrameImpl getFrame(String name) {
      final TimeFrameImpl timeFrame = map.get(name);
      if (timeFrame == null) {
        throw new IllegalArgumentException("unknown frame: " + name);
      }
      return timeFrame;
    }

    /** Adds a frame.
     *
     * <p>If a frame with this name already exists, throws
     * {@link IllegalArgumentException} and leaves the builder in the same
     * state.
     *
     * <p>It is very important that we don't allow replacement of frames.
     * If replacement were allowed, people would be able to create a DAG
     * (e.g. two routes from DAY to MONTH with different multipliers)
     * or a cycle (e.g. one SECOND equals 1,000 MILLISECOND
     * and one MILLISECOND equals 20 SECOND). Those scenarios give rise to
     * inconsistent multipliers. */
    private BuilderImpl addFrame(String name, TimeFrameImpl frame) {
      final TimeFrameImpl previousFrame =
          map.put(name, requireNonNull(frame, "frame"));
      if (previousFrame != null) {
        // There was already a frame with that name. Replace the old frame
        // (so that that builder is still valid usable) and throw.
        map.put(name, previousFrame);
        throw new IllegalArgumentException("duplicate frame: " + name);
      }
      return this;
    }

    @Override public BuilderImpl addCore(String name) {
      return addFrame(name, new CoreFrame(frameSetSupplier, name));
    }

    /** Defines a time unit that consists of {@code count} instances of
     * {@code baseUnit}. */
    BuilderImpl addSub(String name, boolean divide, Number count,
        String baseName, TimestampString epoch) {
      final TimeFrameImpl baseFrame = getFrame(baseName);
      final BigInteger factor = toBigInteger(count);

      final CoreFrame coreFrame = baseFrame.core();
      final BigFraction coreFactor = divide
          ? baseFrame.coreMultiplier().divide(factor)
          : baseFrame.coreMultiplier().multiply(factor);

      return addFrame(name,
          new SubFrame(name, baseFrame, divide, factor, coreFrame, coreFactor,
              epoch));
    }

    @Override public BuilderImpl addQuotient(String name,
        String minorName, String majorName) {
      final TimeFrameImpl minorFrame = getFrame(minorName);
      final TimeFrameImpl majorFrame = getFrame(majorName);
      return addFrame(name, new QuotientFrame(name, minorFrame, majorFrame));
    }

    @Override public BuilderImpl addMultiple(String name, Number count,
        String baseName) {
      return addSub(name, false, count, baseName, TimestampString.EPOCH);
    }

    @Override public BuilderImpl addDivision(String name, Number count,
        String baseName) {
      return addSub(name, true, count, baseName, TimestampString.EPOCH);
    }

    @Override public BuilderImpl addRollup(String fromName, String toName) {
      final TimeFrameImpl fromFrame = getFrame(fromName);
      final TimeFrameImpl toFrame = getFrame(toName);
      rollupList.put(fromFrame, toFrame);
      return this;
    }

    @Override public BuilderImpl addAll(TimeFrameSet timeFrameSet) {
      timeFrameSet.map.values().forEach(frame -> frame.replicate(this));
      return this;
    }

    @Override public BuilderImpl withEpoch(TimestampString epoch) {
      final Map.Entry<String, TimeFrameImpl> entry =
          Iterables.getLast(map.entrySet());
      final String name = entry.getKey();
      final SubFrame value =
          requireNonNull((SubFrame) map.remove(name));
      value.replicateWithEpoch(this, epoch);
      return this;
    }

    @Override public BuilderImpl addAlias(String name, String originalName) {
      final TimeFrameImpl frame = getFrame(originalName);
      return addFrame(name, new AliasFrame(name, frame));
    }

    // Extra methods for Avatica's built-in time frames.

    void addCore(TimeUnit unit) {
      addCore(unit.name());
    }

    void addSub(TimeUnit unit, boolean divide, Number count,
        TimeUnit baseUnit) {
      addSub(unit, divide, count, baseUnit, TimestampString.EPOCH);
    }

    void addSub(TimeUnit unit, boolean divide, Number count,
        TimeUnit baseUnit, TimestampString epoch) {
      addSub(unit.name(), divide, count, baseUnit.name(), epoch);
    }

    void addRollup(TimeUnit fromUnit, TimeUnit toUnit) {
      addRollup(fromUnit.name(), toUnit.name());
    }

    void addQuotient(TimeUnit unit, TimeUnit minor, TimeUnit major) {
      addQuotient(unit.name(), minor.name(), major.name());
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
      // Note: The following algorithm is not very efficient. It becomes less
      // efficient as the number of time frames increases. A more efficient
      // algorithm would be for TimeFrameSet.Builder.build() to call this method
      // for each pair of time frames and cache the results in a list:
      //
      //   (coreFrame,
      //   [(subFrame0, multiplier0),
      //    ...
      //    (subFrameN, multiplierN)])

      final Map<TimeFrame, BigFraction> map = new HashMap<>();
      final Map<TimeFrame, BigFraction> map2 = new HashMap<>();
      expand(map, BigFraction.ONE);
      ((TimeFrameImpl) timeFrame).expand(map2, BigFraction.ONE);
      final Set<BigFraction> fractions = new HashSet<>();
      for (Map.Entry<TimeFrame, BigFraction> entry : map.entrySet()) {
        final BigFraction value2 = map2.get(entry.getKey());
        if (value2 != null) {
          fractions.add(value2.divide(entry.getValue()));
        }
      }

      switch (fractions.size()) {
      case 0:
        // There is no path from this TimeFrame to that.
        return null;
      case 1:
        return Iterables.getOnlyElement(fractions);
      default:
        // If there are multiple units in common, the multipliers must be the
        // same for all. If they are not, the must have somehow created a
        // TimeFrameSet that has multiple paths between units (i.e. a DAG),
        // or has a cycle. TimeFrameSet.Builder is supposed to prevent all of
        // these, and so we throw an AssertionError.
        throw new AssertionError("inconsistent multipliers for " + this
            + ".per(" + timeFrame + "): " + fractions);
      }
    }

    protected void expand(Map<TimeFrame, BigFraction> map, BigFraction f) {
      map.put(this, f);
    }

    /** Adds a time frame like this to a builder. */
    abstract void replicate(BuilderImpl b);

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
  }

  /** Core time frame (such as SECOND, MONTH, ISOYEAR). */
  static class CoreFrame extends TimeFrameImpl {
    CoreFrame(Supplier<TimeFrameSet> frameSetSupplier, String name) {
      super(frameSetSupplier, name);
    }

    @Override void replicate(BuilderImpl b) {
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

    @Override void replicate(BuilderImpl b) {
      b.addSub(name, divide, multiplier, base.name, epoch);
    }

    /** Returns a copy of this TimeFrameImpl with a given epoch. */
    void replicateWithEpoch(BuilderImpl b, TimestampString epoch) {
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

    @Override void replicate(BuilderImpl b) {
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
    final TimeFrameImpl frame;

    AliasFrame(String name, TimeFrameImpl frame) {
      super(frame.frameSetSupplier, name);
      this.frame = requireNonNull(frame, "frame");
    }

    @Override void replicate(BuilderImpl b) {
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
