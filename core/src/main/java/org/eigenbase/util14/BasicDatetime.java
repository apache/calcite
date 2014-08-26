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
package org.eigenbase.util14;

import java.util.*;

/**
 * BasicDatetime is an interface for dates, times, or timestamps that can be
 * assigned from a long value. The value to be assigned may either be a zoneless
 * time, or it may be a zoned time.
 *
 * <p>A zoneless time is based on milliseconds. It may contain date and/or time
 * components as follows:
 *
 * <pre>
 * The time component = value % milliseconds in a day
 * The date component = value / milliseconds in a day
 * </pre>
 *
 * If a date component is specified, it is relative to the epoch (1970-01-01).
 *
 * <p>A zoned time represents a time that was created in a particular time zone.
 * It may contain date and/or time components that are valid when interpreted
 * relative to a specified time zone, according to a {@link java.util.Calendar
 * Calendar}. Jdbc types, such as {@link java.sql.Date} typically contain zoned
 * times.
 */
public interface BasicDatetime {
  //~ Methods ----------------------------------------------------------------

  /**
   * Gets the internal value of this datetime
   */
  long getTime();

  /**
   * Sets this datetime via a zoneless time value. See class comments for more
   * information.
   */
  void setZonelessTime(long value);

  /**
   * Sets this datetime via a zoned time value. See class comments for more
   * information.
   */
  void setZonedTime(long value, TimeZone zone);
}

// End BasicDatetime.java
