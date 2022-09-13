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

import org.apache.calcite.avatica.util.TimeUnit;

import org.apache.commons.math3.fraction.BigFraction;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collection;

/** Time frame.
 *
 */
public interface TimeFrame {
  /** Returns the time frame set that this frame belongs to. */
  TimeFrameSet frameSet();

  /** Name of this time frame.
   *
   * <p>A time unit based on a built-in Avatica
   * {@link org.apache.calcite.avatica.util.TimeUnit} will have the same
   * name. */
  String name();

  /** A collection of time frames that this time frame aligns with.
   *
   * <p>For example:
   * DAY aligns with WEEK, MONTH, YEAR (because each day belongs to
   * exactly one week, month and year);
   * WEEK does not align with MONTH or YEAR (because a particular week can cross
   * month or year boundaries);
   * MONTH aligns with YEAR.
   * Therefore, DAY declares that it aligns with WEEK, MONTH,
   * and DAY's alignment with YEAR can be inferred from MONTH's alignment with
   * YEAR.
   */
  default Collection<TimeFrame> alignsWith() {
    throw new UnsupportedOperationException();
  }

  /** If this time frame has units in common with another time frame, returns
   * the number of this time frame in one of that time frame.
   *
   * <p>For example, MONTH.per(YEAR) returns 12; YEAR.per(MONTH) returns 1 / 12.
   */
  @Nullable BigFraction per(TimeFrame timeFrame);

  /** Returns a date where this time frame is at the start of a cycle.
   *
   * <p>For example, the WEEK time frame starts on a Monday, and 1970/01/05 was
   * a Monday, and the date 1970/01/05 is represented as integer 5, so the WEEK
   * time frame returns 5. But it would also be valid to return the date value
   * of 1900/01/01, which was also a Monday.  Because we know that a week is 7
   * days, we can compute every other point at which a week advances. */
  default int dateEpoch() {
    return 0;
  }

  /** Returns a timestamp where this time frame is at the start of a cycle.
   *
   * @see #dateEpoch() */
  default long timestampEpoch() {
    return 0L;
  }

  /** Returns a month number where this time frame is at the start of a cycle.
   *
   * @see #dateEpoch()
   */
  default int monthEpoch() {
    return 0;
  }

  /** Whether this frame can roll up to {@code toFrame}.
   *
   * <p>Examples:
   * <ul>
   *   <li>SECOND can roll up to MINUTE, HOUR, DAY, WEEK, MONTH, MILLENNIUM;
   *   <li>SECOND cannot roll up to MILLISECOND (because it is finer grained);
   *   <li>WEEK cannot roll up to MONTH, YEAR, MILLENNIUM (because weeks cross
   *   month boundaries)
   * </ul>
   */
  boolean canRollUpTo(TimeFrame toFrame);

  default @Nullable TimeUnit unit() {
    return frameSet().getUnit(this);
  }
}
