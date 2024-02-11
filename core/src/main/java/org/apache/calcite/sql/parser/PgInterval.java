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
package org.apache.calcite.sql.parser;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Objects;

/**
 * A parsed PostgreSQL interval.
 */
public class PgInterval {
  boolean present;
  int years;
  int months;
  int days;
  int hours;
  int minutes;
  int seconds;
  int milliseconds;

  PgInterval(
      boolean present,
      int years,
      int months,
      int days,
      int hours,
      int minutes,
      int seconds,
      int milliseconds) {
    this.present = present;
    this.years = years;
    this.months = months;
    this.days = days;
    this.hours = hours;
    this.minutes = minutes;
    this.seconds = seconds;
    this.milliseconds = milliseconds;
  }

  @Override public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    } else if (o == null || getClass() != o.getClass()) {
      return false;
    }

    PgInterval that = (PgInterval) o;
    return present == that.present
        && years == that.years
        && months == that.months
        && days == that.days
        && hours == that.hours
        && minutes == that.minutes
        && seconds == that.seconds
        && milliseconds == that.milliseconds;
  }

  @Override public int hashCode() {
    return Objects.hash(present, years, months, days, hours, minutes, seconds, milliseconds);
  }
}
