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
package org.apache.calcite.avatica.util;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/** A range of time units. The first is more significant than the
 * other (e.g. year-to-day) or the same as the other (e.g. month). */
public enum TimeUnitRange {
  YEAR(TimeUnit.YEAR, null),
  YEAR_TO_MONTH(TimeUnit.YEAR, TimeUnit.MONTH),
  MONTH(TimeUnit.MONTH, null),
  DAY(TimeUnit.DAY, null),
  DAY_TO_HOUR(TimeUnit.DAY, TimeUnit.HOUR),
  DAY_TO_MINUTE(TimeUnit.DAY, TimeUnit.MINUTE),
  DAY_TO_SECOND(TimeUnit.DAY, TimeUnit.SECOND),
  HOUR(TimeUnit.HOUR, null),
  HOUR_TO_MINUTE(TimeUnit.HOUR, TimeUnit.MINUTE),
  HOUR_TO_SECOND(TimeUnit.HOUR, TimeUnit.SECOND),
  MINUTE(TimeUnit.MINUTE, null),
  MINUTE_TO_SECOND(TimeUnit.MINUTE, TimeUnit.SECOND),
  SECOND(TimeUnit.SECOND, null),

  // non-standard time units cannot participate in ranges
  QUARTER(TimeUnit.QUARTER, null),
  WEEK(TimeUnit.WEEK, null),
  MILLISECOND(TimeUnit.MILLISECOND, null),
  MICROSECOND(TimeUnit.MICROSECOND, null),
  DOW(TimeUnit.DOW, null),
  DOY(TimeUnit.DOY, null),
  EPOCH(TimeUnit.EPOCH, null),
  DECADE(TimeUnit.DECADE, null),
  CENTURY(TimeUnit.CENTURY, null),
  MILLENNIUM(TimeUnit.MILLENNIUM, null);

  public final TimeUnit startUnit;
  public final TimeUnit endUnit;

  private static final Map<Pair<TimeUnit>, TimeUnitRange> MAP = createMap();

  /**
   * Creates a TimeUnitRange.
   *
   * @param startUnit Start time unit
   * @param endUnit   End time unit
   */
  TimeUnitRange(TimeUnit startUnit, TimeUnit endUnit) {
    assert startUnit != null;
    this.startUnit = startUnit;
    this.endUnit = endUnit;
  }

  /**
   * Returns a {@code TimeUnitRange} with a given start and end unit.
   *
   * @param startUnit Start unit
   * @param endUnit   End unit
   * @return Time unit range, or null if not valid
   */
  public static TimeUnitRange of(TimeUnit startUnit, TimeUnit endUnit) {
    return MAP.get(new Pair<>(startUnit, endUnit));
  }

  private static Map<Pair<TimeUnit>, TimeUnitRange> createMap() {
    Map<Pair<TimeUnit>, TimeUnitRange> map = new HashMap<>();
    for (TimeUnitRange value : values()) {
      map.put(new Pair<>(value.startUnit, value.endUnit), value);
    }
    return Collections.unmodifiableMap(map);
  }

  /** Whether this is in the YEAR-TO-MONTH family of intervals. */
  public boolean monthly() {
    return ordinal() <= MONTH.ordinal();
  }

  /** Immutable pair of values of the same type. */
  private static class Pair<E> {
    final E left;
    final E right;

    private Pair(E left, E right) {
      this.left = left;
      this.right = right;
    }

    @Override public int hashCode() {
      int k = (left == null) ? 0 : left.hashCode();
      int k1 = (right == null) ? 0 : right.hashCode();
      return ((k << 4) | k) ^ k1;
    }

    @Override public boolean equals(Object obj) {
      return obj == this
          || obj instanceof Pair
          && Objects.equals(left, ((Pair) obj).left)
          && Objects.equals(right, ((Pair) obj).right);
    }
  }
}

// End TimeUnitRange.java
