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

import java.math.BigDecimal;

/**
 * Enumeration of time units used to construct an interval.
 */
public enum TimeUnit {
  YEAR(true, ' ', 12 /* months */, null),
  MONTH(true, '-', 1 /* months */, BigDecimal.valueOf(12)),
  DAY(false, '-', DateTimeUtils.MILLIS_PER_DAY, null),
  HOUR(false, ' ', DateTimeUtils.MILLIS_PER_HOUR, BigDecimal.valueOf(24)),
  MINUTE(false, ':', DateTimeUtils.MILLIS_PER_MINUTE, BigDecimal.valueOf(60)),
  SECOND(false, ':', DateTimeUtils.MILLIS_PER_SECOND, BigDecimal.valueOf(60));

  public final boolean yearMonth;
  public final char separator;
  public final long multiplier;
  private final BigDecimal limit;

  private static final TimeUnit[] CACHED_VALUES = values();

  TimeUnit(boolean yearMonth, char separator, long multiplier,
      BigDecimal limit) {
    this.yearMonth = yearMonth;
    this.separator = separator;
    this.multiplier = multiplier;
    this.limit = limit;
  }

  /**
   * Returns the TimeUnit associated with an ordinal. The value returned
   * is null if the ordinal is not a member of the TimeUnit enumeration.
   */
  public static TimeUnit getValue(int ordinal) {
    return ordinal < 0 || ordinal >= CACHED_VALUES.length
        ? null
        : CACHED_VALUES[ordinal];
  }

  /**
   * Returns whether a given value is valid for a field of this time unit.
   *
   * @param field Field value
   * @return Whether value
   */
  public boolean isValidValue(BigDecimal field) {
    return field.compareTo(BigDecimal.ZERO) >= 0
        && (limit == null
        || field.compareTo(limit) < 0);
  }
}

// End TimeUnit.java
