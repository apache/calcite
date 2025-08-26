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
package org.apache.calcite.adapter.ops;

import org.apache.calcite.sql.type.SqlTypeName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.Date;

/**
 * Data converter for Cloud Governance adapter.
 * Handles conversion of cloud provider SDK types to Calcite-compatible types.
 *
 * <p>This converter ensures that temporal types from various cloud SDKs
 * (AWS, Azure, GCP) are properly converted to the types expected by Calcite's
 * internal representation and JDBC layer.</p>
 */
public class CloudOpsDataConverter {
  private static final Logger LOGGER = LoggerFactory.getLogger(CloudOpsDataConverter.class);

  /**
   * Converts a value to the appropriate type based on the target SQL type.
   *
   * @param value The value to convert
   * @param targetType The target SQL type
   * @return The converted value, or null if the input is null
   */
  public static Object convertValue(Object value, SqlTypeName targetType) {
    if (value == null) {
      return null;
    }

    try {
      switch (targetType) {
        case TIMESTAMP:
        case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
          return convertToTimestampMillis(value);

        case DATE:
          return convertToDateDays(value);

        case TIME:
          return convertToTimeMillis(value);

        default:
          return value;
      }
    } catch (Exception e) {
      LOGGER.debug("Error converting value {} to type {}: {}",
          value, targetType, e.getMessage());
      return value;
    }
  }

  /**
   * Converts various temporal types to milliseconds since epoch for TIMESTAMP fields.
   * This is the format expected by Calcite internally.
   *
   * @param value The temporal value to convert
   * @return Long representing milliseconds since epoch
   */
  private static Long convertToTimestampMillis(Object value) {
    if (value == null) {
      return null;
    }

    // Handle various Java time API types
    if (value instanceof Instant) {
      return ((Instant) value).toEpochMilli();
    }
    if (value instanceof ZonedDateTime) {
      return ((ZonedDateTime) value).toInstant().toEpochMilli();
    }
    if (value instanceof OffsetDateTime) {
      return ((OffsetDateTime) value).toInstant().toEpochMilli();
    }
    if (value instanceof LocalDateTime) {
      // LocalDateTime doesn't have timezone info, assume system default
      return ((LocalDateTime) value).atZone(java.time.ZoneId.systemDefault())
          .toInstant().toEpochMilli();
    }

    // Handle legacy Java date types
    if (value instanceof Timestamp) {
      return ((Timestamp) value).getTime();
    }
    if (value instanceof Date) {
      return ((Date) value).getTime();
    }
    if (value instanceof java.sql.Date) {
      return ((java.sql.Date) value).getTime();
    }

    // Handle numeric epoch values
    if (value instanceof Long) {
      return (Long) value;
    }
    if (value instanceof Number) {
      return ((Number) value).longValue();
    }

    // If already the correct type, return as-is
    return value instanceof Long ? (Long) value : null;
  }

  /**
   * Converts temporal values to days since epoch for DATE fields.
   *
   * @param value The temporal value to convert
   * @return Integer representing days since epoch
   */
  private static Integer convertToDateDays(Object value) {
    Long millis = convertToTimestampMillis(value);
    if (millis == null) {
      return null;
    }
    // Convert milliseconds to days since epoch
    return (int) (millis / (24 * 60 * 60 * 1000L));
  }

  /**
   * Converts temporal values to milliseconds since midnight for TIME fields.
   *
   * @param value The temporal value to convert
   * @return Integer representing milliseconds since midnight
   */
  private static Integer convertToTimeMillis(Object value) {
    Long millis = convertToTimestampMillis(value);
    if (millis == null) {
      return null;
    }
    // Get milliseconds since midnight
    return (int) (millis % (24 * 60 * 60 * 1000L));
  }

  /**
   * Converts a row of values according to their SQL types.
   *
   * @param row The row of values to convert
   * @param types The SQL types for each column
   * @return The converted row
   */
  public static Object[] convertRow(Object[] row, SqlTypeName[] types) {
    if (row == null || types == null) {
      return row;
    }

    Object[] convertedRow = new Object[row.length];
    for (int i = 0; i < row.length && i < types.length; i++) {
      convertedRow[i] = convertValue(row[i], types[i]);
    }
    return convertedRow;
  }
}
