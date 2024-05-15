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
package org.apache.calcite.util.format.postgresql;

import com.google.common.collect.ImmutableSet;

import java.time.temporal.ChronoUnit;
import java.util.Set;

import static org.apache.calcite.util.format.postgresql.DateCalendarEnum.GREGORIAN;
import static org.apache.calcite.util.format.postgresql.DateCalendarEnum.ISO_8601;
import static org.apache.calcite.util.format.postgresql.DateCalendarEnum.JULIAN;
import static org.apache.calcite.util.format.postgresql.DateCalendarEnum.NONE;

/**
 * A component of a datetime. May belong to a type of calendar. Also contains
 * a list of parent values. For example months are in a year. A datetime can be
 * reconstructed from one or more <code>ChronUnitEnum</code> items along with
 * their associated values.
 *
 * <p>Some <code>ChronoUnitEnum</code> items conflict with others.
 */
public enum ChronoUnitEnum {
  ERAS(ChronoUnit.ERAS, NONE),
  CENTURIES(
      ChronoUnit.CENTURIES,
      ImmutableSet.of(ISO_8601, GREGORIAN),
      ERAS),
  YEARS_ISO_8601(
      ChronoUnit.YEARS,
      ISO_8601,
      CENTURIES),
  YEARS_IN_MILLENIA_ISO_8601(
      ChronoUnit.YEARS,
      ISO_8601,
      CENTURIES),
  YEARS_IN_CENTURY_ISO_8601(
      ChronoUnit.YEARS,
      ISO_8601,
      CENTURIES),
  DAYS_IN_YEAR_ISO_8601(
      ChronoUnit.DAYS,
      ISO_8601,
      YEARS_ISO_8601),
  WEEKS_IN_YEAR_ISO_8601(
      ChronoUnit.WEEKS,
      ISO_8601,
      YEARS_ISO_8601),
  DAYS_JULIAN(
      ChronoUnit.DAYS,
      JULIAN),
  YEARS(
      ChronoUnit.YEARS,
      GREGORIAN,
      CENTURIES),
  YEARS_IN_MILLENIA(
      ChronoUnit.YEARS,
      GREGORIAN,
      CENTURIES),
  YEARS_IN_CENTURY(
      ChronoUnit.YEARS,
      GREGORIAN,
      CENTURIES),
  MONTHS_IN_YEAR(
      ChronoUnit.MONTHS,
      GREGORIAN,
      YEARS, YEARS_IN_CENTURY),
  DAYS_IN_YEAR(
      ChronoUnit.DAYS,
      GREGORIAN,
      YEARS, YEARS_IN_CENTURY),
  DAYS_IN_MONTH(
      ChronoUnit.DAYS,
      GREGORIAN,
      MONTHS_IN_YEAR),
  WEEKS_IN_YEAR(
      ChronoUnit.WEEKS,
      GREGORIAN,
      YEARS, YEARS_IN_CENTURY),
  WEEKS_IN_MONTH(
      ChronoUnit.WEEKS,
      GREGORIAN,
      MONTHS_IN_YEAR),
  DAYS_IN_WEEK(
      ChronoUnit.DAYS,
      NONE,
      YEARS_ISO_8601, WEEKS_IN_MONTH, WEEKS_IN_YEAR),
  HOURS_IN_DAY(
      ChronoUnit.HOURS,
      NONE,
      DAYS_IN_YEAR, DAYS_IN_MONTH, DAYS_IN_WEEK),
  HALF_DAYS(
      ChronoUnit.HALF_DAYS,
      NONE,
      DAYS_IN_YEAR, DAYS_IN_MONTH, DAYS_IN_WEEK),
  HOURS_IN_HALF_DAY(
      ChronoUnit.HOURS,
      NONE,
      HALF_DAYS),
  MINUTES_IN_HOUR(
      ChronoUnit.MINUTES,
      NONE,
      HOURS_IN_DAY, HOURS_IN_HALF_DAY),
  SECONDS_IN_DAY(
      ChronoUnit.SECONDS,
      NONE,
      DAYS_IN_YEAR, DAYS_IN_MONTH, DAYS_IN_WEEK),
  SECONDS_IN_MINUTE(
      ChronoUnit.SECONDS,
      NONE,
      MINUTES_IN_HOUR),
  MILLIS(
      ChronoUnit.MILLIS,
      NONE,
      SECONDS_IN_DAY, SECONDS_IN_MINUTE),
  MICROS(
      ChronoUnit.MICROS,
      NONE,
      MILLIS),
  TENTHS_OF_SECOND(
      ChronoUnit.MILLIS,
      NONE,
      SECONDS_IN_DAY, SECONDS_IN_MINUTE),
  HUNDREDTHS_OF_SECOND(
      ChronoUnit.MILLIS,
      NONE,
      SECONDS_IN_DAY, SECONDS_IN_MINUTE),
  THOUSANDTHS_OF_SECOND(
      ChronoUnit.MILLIS,
      NONE,
      SECONDS_IN_DAY, SECONDS_IN_MINUTE),
  TENTHS_OF_MS(
      ChronoUnit.MICROS,
      NONE,
      SECONDS_IN_DAY, SECONDS_IN_MINUTE),
  HUNDREDTHS_OF_MS(
      ChronoUnit.MICROS,
      NONE,
      SECONDS_IN_DAY, SECONDS_IN_MINUTE),
  THOUSANDTHS_OF_MS(
      ChronoUnit.MICROS,
      NONE,
      SECONDS_IN_DAY, SECONDS_IN_MINUTE),
  TIMEZONE_HOURS(
      ChronoUnit.HOURS,
      NONE),
  TIMEZONE_MINUTES(
      ChronoUnit.MINUTES,
      NONE,
      TIMEZONE_HOURS);

  private final ChronoUnit chronoUnit;
  private final ImmutableSet<ChronoUnitEnum> parentUnits;
  private final ImmutableSet<DateCalendarEnum> calendars;

  ChronoUnitEnum(ChronoUnit chronoUnit, DateCalendarEnum calendar,
      ChronoUnitEnum... parentUnits) {
    this.chronoUnit = chronoUnit;
    this.parentUnits = ImmutableSet.copyOf(parentUnits);
    this.calendars = ImmutableSet.of(calendar);
  }

  ChronoUnitEnum(ChronoUnit chronoUnit, Set<DateCalendarEnum> calendars,
      ChronoUnitEnum... parentUnits) {
    this.chronoUnit = chronoUnit;
    this.parentUnits = ImmutableSet.copyOf(parentUnits);
    this.calendars = ImmutableSet.<DateCalendarEnum>builder().addAll(calendars).build();
  }

  /**
   * Get the ChronoUnit value that corresponds to this value.
   *
   * @return a ChronoUnit value
   */
  public ChronoUnit getChronoUnit() {
    return chronoUnit;
  }

  /**
   * Get the set of calendars that this value can be in.
   *
   * @return set of calendars that this value can be in
   */
  public Set<DateCalendarEnum> getCalendars() {
    return calendars;
  }

  /**
   * Checks if the current item can be added to <code>units</code> without causing
   * any conflicts.
   *
   * @param units a <code>Set</code> of items to test against
   * @return <code>true</code> if this item does not conflict with <code>units</code>
   */
  public boolean isCompatible(Set<ChronoUnitEnum> units) {
    if (!calendars.isEmpty()) {
      for (ChronoUnitEnum unit : units) {
        boolean haveCompatibleCalendar = false;

        for (DateCalendarEnum unitCalendar : unit.getCalendars()) {
          for (DateCalendarEnum calendar : calendars) {
            if (unitCalendar == NONE || calendar == NONE
                || unitCalendar.isCalendarCompatible(calendar)) {
              haveCompatibleCalendar = true;
              break;
            }
          }

          if (haveCompatibleCalendar) {
            break;
          }
        }

        if (!haveCompatibleCalendar) {
          return false;
        }
      }
    }

    for (ChronoUnitEnum unit : units) {
      if (parentUnits.equals(unit.parentUnits)) {
        return false;
      }
    }

    return true;
  }
}
