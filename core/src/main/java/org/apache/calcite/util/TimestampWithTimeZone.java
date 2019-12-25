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
 * A Timestamp with time zone. It is immutable.
 */
public class TimestampWithTimeZone implements Comparable<TimestampWithTimeZone>, Serializable {
  //~ Instance fields --------------------------------------------------------

  private final long millisecond;
  private final TimeZone timeZone;

  /**
   * Constructor. Obtains an instance of {@link TimestampWithTimeZone} from a millisecond of
   * zoneless timestamp and a {@link TimeZone}.
   *
   * @param millisecond the number of milliseconds of a zoneless timestamp.
   * @param timeZone    the time-zone to use.
   */
  public TimestampWithTimeZone(long millisecond, TimeZone timeZone) {
    this.millisecond = millisecond;
    this.timeZone = timeZone;
  }

  /**
   * Constructor. Obtains an instance of {@link TimestampWithTimeZone} from a millisecond of
   * zoneless timestmap and a valid time-zone id.
   *
   * @param millisecond the number of milliseconds of a zoneless timestamp.
   * @param tz the time-zone id to use.
   */
  public TimestampWithTimeZone(long millisecond, String tz) {
    this.millisecond = millisecond;
    this.timeZone = TimeZone.getTimeZone(tz);
  }

  /**
   * Converts this time to epoch millis based on 1970-01-01Z.
   *
   * @return the epoch milli value
   */
  private long toEpochMilli() {
    long nod = millisecond;
    int offsetMillis = timeZone.getOffset(nod);
    return nod - offsetMillis;
  }

  /**
   * Compares this date-time to another date-time.
   * <p>
   * The comparison is based on the instant then on the local date-time.
   * See {@link java.time.OffsetDateTime}
   *
   * @param o the other date-time to compare to.
   * @return the comparator value, negative if less, positive if greater.
   */
  @Override public int compareTo(TimestampWithTimeZone o) {
    if (timeZone.equals(o.timeZone)) {
      return Long.compare(millisecond, o.millisecond);
    }
    int compare = Long.compare(toEpochMilli(), o.toEpochMilli());
    if (compare == 0) {
      compare = Long.compare(millisecond, o.millisecond);
    }
    return compare;
  }

  @Override public int hashCode() {
    return Objects.hash(millisecond, timeZone);
  }

  @Override public boolean equals(Object obj) {
    if (!(obj instanceof TimestampWithTimeZone)) {
      return false;
    }
    TimestampWithTimeZone that = (TimestampWithTimeZone) obj;
    return this.millisecond == that.millisecond && this.timeZone.equals(that.timeZone);
  }

  @Override public String toString() {
    return new TimestampWithTimeZoneString(
      TimestampString.fromMillisSinceEpoch(millisecond),
      timeZone).toString();
  }

  public long getMillisecond() {
    return millisecond;
  }

  public TimeZone getTimeZone() {
    return timeZone;
  }
}
