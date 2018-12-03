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

import java.util.Locale;
import javax.annotation.Nonnull;

/**
 * A strategy by which Druid rolls up rows into sub-totals based on their
 * timestamp values.
 *
 * <p>Typical granularities are based upon time units (e.g. 1 day or
 * 15 minutes). A special granularity, all, combines all rows into a single
 * total.
 *
 * <p>A Granularity instance is immutable, and generates a JSON string as
 * part of a Druid query.
 *
 * @see Granularities
 */
public interface Granularity extends DruidJson {
  /** Type of supported periods for granularity. */
  enum Type {
    ALL,
    YEAR,
    QUARTER,
    MONTH,
    WEEK,
    DAY,
    HOUR,
    MINUTE,
    SECOND;

    /** Lower-case name, e.g. "all", "minute". */
    public final String lowerName = name().toLowerCase(Locale.ROOT);
  }

  @Nonnull Type getType();
}

// End Granularity.java
