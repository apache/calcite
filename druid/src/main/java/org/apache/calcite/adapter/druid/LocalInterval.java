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
package org.apache.calcite.adapter.druid;

import org.joda.time.DateTime;
import org.joda.time.Instant;
import org.joda.time.Interval;
import org.joda.time.LocalDateTime;
import org.joda.time.chrono.ISOChronology;

/**
 * Similar to {@link Interval} but the end points are {@link LocalDateTime}
 * not {@link Instant}.
 */
public class LocalInterval {
  private final long start;
  private final long end;

  /** Creates a LocalInterval. */
  private LocalInterval(long start, long end) {
    this.start = start;
    this.end = end;
  }

  /** Creates a LocalInterval based on two {@link DateTime} values. */
  public static LocalInterval create(DateTime start, DateTime end) {
    return new LocalInterval(start.getMillis(), end.getMillis());
  }

  /** Creates a LocalInterval based on millisecond start end end points. */
  public static LocalInterval create(long start, long end) {
    return new LocalInterval(start, end);
  }

  /** Creates a LocalInterval based on an interval string. */
  public static LocalInterval create(String intervalString) {
    Interval i = new Interval(intervalString, ISOChronology.getInstanceUTC());
    return new LocalInterval(i.getStartMillis(), i.getEndMillis());
  }

  /** Creates a LocalInterval based on start and end time strings. */
  public static LocalInterval create(String start, String end) {
    return create(
        new DateTime(start, ISOChronology.getInstanceUTC()),
        new DateTime(end, ISOChronology.getInstanceUTC()));
  }

  /** Writes a value such as "1900-01-01T00:00:00.000/2015-10-12T00:00:00.000".
   * Note that there are no "Z"s; the value is in the (unspecified) local
   * time zone, not UTC. */
  @Override public String toString() {
    final LocalDateTime start =
        new LocalDateTime(this.start, ISOChronology.getInstanceUTC());
    final LocalDateTime end =
        new LocalDateTime(this.end, ISOChronology.getInstanceUTC());
    return start + "/" + end;
  }

  @Override public int hashCode() {
    int result = 97;
    result = 31 * result + ((int) (start ^ (start >>> 32)));
    result = 31 * result + ((int) (end ^ (end >>> 32)));
    return result;
  }

  @Override public boolean equals(Object o) {
    return o == this
        || o instanceof LocalInterval
        && start == ((LocalInterval) o).start
        && end == ((LocalInterval) o).end;
  }

  /** Analogous to {@link Interval#getStartMillis}. */
  public long getStartMillis() {
    return start;
  }

  /** Analogous to {@link Interval#getEndMillis()}. */
  public long getEndMillis() {
    return end;
  }
}

// End LocalInterval.java
