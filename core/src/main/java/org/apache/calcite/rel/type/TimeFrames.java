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
import org.apache.calcite.util.TimestampString;

/** Utilities for {@link TimeFrame}. */
public class TimeFrames {
  private TimeFrames() {
  }

  /** The core time frame set. Includes the time frames for all Avatica time
   * units plus ISOWEEK:
   *
   * <ul>
   *   <li>SECOND, and multiples MINUTE, HOUR, DAY, WEEK (starts on a Sunday),
   *   sub-multiples MILLISECOND, MICROSECOND, NANOSECOND,
   *   quotients DOY, DOW;
   *   <li>MONTH, and multiples QUARTER, YEAR, DECADE, CENTURY, MILLENNIUM;
   *   <li>ISOYEAR, and sub-unit ISOWEEK (starts on a Monday), quotient ISODOW;
   * </ul>
   *
   * <p>Does not include EPOCH.
   */
  public static final TimeFrameSet CORE =
      addTsi(addCore(new MyBuilder())).build();

  private static MyBuilder addCore(MyBuilder b) {
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

    b.addQuotient(TimeUnit.DOY, TimeUnit.DAY, TimeUnit.YEAR);
    b.addQuotient(TimeUnit.DOW, TimeUnit.DAY, TimeUnit.WEEK);
    b.addQuotient(TimeUnit.ISODOW.name(), TimeUnit.DAY.name(), "ISOWEEK");

    b.addRollup(TimeUnit.DAY, TimeUnit.MONTH);
    b.addRollup("ISOWEEK", TimeUnit.ISOYEAR.name());
    return b;
  }

  /** Adds abbreviations used by {@code TIMESTAMPADD}, {@code TIMESTAMPDIFF}
   * functions. */
  private static MyBuilder addTsi(MyBuilder b) {
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

  /** Specialization of {@link org.apache.calcite.rel.type.TimeFrameSet.Builder}
   * for Avatica's built-in time frames. */
  private static class MyBuilder extends TimeFrameSet.Builder {
    void addCore(TimeUnit unit) {
      super.addCore(unit.name());
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
}
