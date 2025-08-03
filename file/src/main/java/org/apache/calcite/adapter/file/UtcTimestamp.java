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
package org.apache.calcite.adapter.file;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * A wrapper for timestamp values that ensures UTC string representation.
 *
 * This matches Parquet's behavior where TIMESTAMP(isAdjustedToUTC=true)
 * displays timestamps as UTC rather than local time.
 */
public class UtcTimestamp extends Timestamp {
  private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

  public UtcTimestamp(long time) {
    super(time);
  }

  @Override public String toString() {
    // Convert to UTC string representation
    // This matches Parquet's TIMESTAMP(isAdjustedToUTC=true) behavior
    Instant instant = Instant.ofEpochMilli(getTime());
    LocalDateTime utcDateTime = LocalDateTime.ofInstant(instant, ZoneId.of("UTC"));
    return FORMATTER.format(utcDateTime);
  }
}
