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
package org.apache.calcite.util;

import java.io.Serializable;
import java.util.Objects;
import java.util.TimeZone;

/**
 * A Time with time zone. It is immutable.
 */
public class TimeWithTimeZone implements Comparable<TimeWithTimeZone>, Serializable {
  //~ Instance fields --------------------------------------------------------

  private final int milliOfDay;
  private final TimeZone timeZone;

  /**
   * Constructor. Obtains an instance of {@link TimeWithTimeZone} from a millisecond of day
   * and a {@link TimeZone}.
   *
   * @param milliOfDay the number of milliseconds of the day.
   * @param timeZone   the time-zone to use.
   */
  public TimeWithTimeZone(int milliOfDay, TimeZone timeZone) {
    this.milliOfDay = milliOfDay;
    this.timeZone = timeZone;
  }

  /**
   * Constructor. Obtains an instance of {@link TimeWithTimeZone} from a millisecond of day
   * and a valid time-zone id.
   * @param milliOfDay the number of milliseconds of the day.
   * @param tz the time-zone id to use.
   */
  public TimeWithTimeZone(int milliOfDay, String tz) {
    this.milliOfDay = milliOfDay;
    this.timeZone = TimeZone.getTimeZone(tz);
  }

  /**
   * Converts this time to epoch millis based on 1970-01-01Z.
   *
   * @return the epoch milli value
   */
  private int toEpochMilli() {
    int nod = milliOfDay;
    int offsetMillis = timeZone.getOffset(nod);
    return nod - offsetMillis;
  }

  /**
   * Compare the {@link TimeWithTimeZone} to another time.
   * <p>
   * The comparison is based first on the UTC equivalent instant, then on the local time.
   * See {@link java.time.OffsetTime}
   *
   * @param o the other time to compare to.
   * @return the comparator value, negative if less, positive if greater.
   */
  @Override public int compareTo(TimeWithTimeZone o) {
    if (timeZone.equals(o.timeZone)) {
      return Integer.compare(milliOfDay, o.milliOfDay);
    }
    int compare = Integer.compare(toEpochMilli(), o.toEpochMilli());
    if (compare == 0) {
      compare = Integer.compare(milliOfDay, o.milliOfDay);
    }
    return compare;
  }

  @Override public int hashCode() {
    return Objects.hash(milliOfDay, timeZone);
  }

  @Override public boolean equals(Object obj) {
    if (!(obj instanceof TimeWithTimeZone)) {
      return false;
    }
    TimeWithTimeZone that = (TimeWithTimeZone) obj;
    return milliOfDay == that.milliOfDay && timeZone.equals(that.timeZone);
  }

  @Override public String toString() {
    return new TimeWithTimeZoneString(
      TimeString.fromMillisOfDay(milliOfDay),
      timeZone).toString();
  }

  public int getMilliOfDay() {
    return milliOfDay;
  }

  public TimeZone getTimeZone() {
    return timeZone;
  }
}
