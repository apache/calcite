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

import org.checkerframework.checker.nullness.qual.Nullable;

import java.time.Duration;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility class for parsing refresh interval strings.
 * Supports formats like:
 * - "5 minutes"
 * - "1 hour"
 * - "30 seconds"
 * - "2 days"
 */
public class RefreshInterval {
  private static final Pattern INTERVAL_PATTERN =
      Pattern.compile("(\\d+)\\s+(second|minute|hour|day)s?", Pattern.CASE_INSENSITIVE);

  /**
   * Parses a refresh interval string into a Duration.
   *
   * @param intervalStr Interval string like "5 minutes"
   * @return Parsed duration, or null if invalid format
   */
  public static @Nullable Duration parse(@Nullable String intervalStr) {
    if (intervalStr == null || intervalStr.trim().isEmpty()) {
      return null;
    }

    Matcher matcher = INTERVAL_PATTERN.matcher(intervalStr.trim());
    if (!matcher.matches()) {
      return null;
    }

    long value = Long.parseLong(matcher.group(1));
    String unit = matcher.group(2).toLowerCase(Locale.ROOT);

    switch (unit) {
      case "second":
        return Duration.ofSeconds(value);
      case "minute":
        return Duration.ofMinutes(value);
      case "hour":
        return Duration.ofHours(value);
      case "day":
        return Duration.ofDays(value);
      default:
        return null;
    }
  }

  /**
   * Gets the effective refresh interval, considering inheritance.
   *
   * @param tableInterval Table-specific interval (may be null)
   * @param schemaInterval Schema-level default interval (may be null)
   * @return Effective interval, or null if no refresh configured
   */
  public static @Nullable Duration getEffectiveInterval(
      @Nullable String tableInterval,
      @Nullable String schemaInterval) {
    // Table level takes precedence
    Duration interval = parse(tableInterval);
    if (interval != null) {
      return interval;
    }
    // Fall back to schema level
    return parse(schemaInterval);
  }
}
