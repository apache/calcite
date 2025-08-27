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

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.Locale;

/**
 * Comprehensive data converter for Cloud Governance adapter.
 * Handles conversion of all cloud provider SDK types to Calcite-compatible types.
 *
 * <p>This converter ensures that all data types from various cloud SDKs
 * (AWS, Azure, GCP) are properly converted to the types expected by Calcite's
 * internal representation and JDBC layer. Supports:</p>
 * <ul>
 *   <li><strong>Boolean types:</strong> Handles strings ("true"/"false", "1"/"0", "enabled"/"disabled"), 
 *       integers (0=false, non-zero=true), and natural language variants</li>
 *   <li><strong>Numeric types:</strong> Integer, Long, Float, Double, BigDecimal with string parsing</li>
 *   <li><strong>Temporal types:</strong> Timestamp, Date, Time with comprehensive format support</li>
 *   <li><strong>String types:</strong> VARCHAR, CHAR with null-safe conversion</li>
 * </ul>
 *
 * <p>This addresses the boolean datatype issue where cloud provider APIs return
 * boolean values in various formats that standard JDBC boolean handling cannot
 * automatically convert.</p>
 */
public class CloudOpsDataConverter {
  private static final Logger LOGGER = LoggerFactory.getLogger(CloudOpsDataConverter.class);

  /**
   * Determines if a value represents null/missing/blank data from cloud provider APIs
   * for non-string data types. For VARCHAR/CHAR, we preserve all string representations.
   * Cloud providers often return various representations for missing data.
   * 
   * @param value The value to check
   * @return true if the value should be treated as SQL NULL
   */
  private static boolean isNullValue(Object value) {
    if (value == null) {
      return true;
    }
    
    if (value instanceof String) {
      String strValue = ((String) value).trim();
      return strValue.isEmpty() || 
             "null".equalsIgnoreCase(strValue) ||
             "nil".equalsIgnoreCase(strValue) ||
             "none".equalsIgnoreCase(strValue) ||
             "undefined".equalsIgnoreCase(strValue) ||
             "n/a".equalsIgnoreCase(strValue) ||
             "na".equalsIgnoreCase(strValue) ||
             "-".equals(strValue) ||
             "—".equals(strValue); // em dash, sometimes used in cloud UIs
    }
    
    return false;
  }

  /**
   * Converts a value to the appropriate type based on the target SQL type.
   * Handles all data types used by cloud providers with comprehensive format support.
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
        // Boolean types - handle cloud provider boolean formats
        case BOOLEAN:
          return convertToBoolean(value);

        // Integer types - handle various numeric formats from cloud APIs
        case TINYINT:
        case SMALLINT:
        case INTEGER:
          return convertToInteger(value);

        case BIGINT:
          return convertToBigInteger(value);

        // Floating point types - handle cloud provider numeric formats  
        case REAL:
        case FLOAT:
          return convertToFloat(value);

        case DOUBLE:
          return convertToDouble(value);

        case DECIMAL:
          return convertToDecimal(value);

        // Temporal types - existing implementation
        case TIMESTAMP:
        case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
          return convertToTimestampMillis(value);

        case DATE:
          return convertToDateDays(value);

        case TIME:
          return convertToTimeMillis(value);

        // Character types - ensure proper string conversion
        case VARCHAR:
        case CHAR:
          return convertToString(value);

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
   * Converts various formats to Boolean values.
   * Handles cloud provider specific formats like strings, integers, and natural language.
   *
   * @param value The value to convert to Boolean
   * @return Boolean value or null
   */
  private static Boolean convertToBoolean(Object value) {
    if (isNullValue(value)) {
      return null;
    }

    // Direct boolean values
    if (value instanceof Boolean) {
      return (Boolean) value;
    }

    // Numeric values - 0 = false, non-zero = true
    if (value instanceof Number) {
      return ((Number) value).intValue() != 0;
    }

    // String representations - common in cloud APIs
    if (value instanceof String) {
      String strValue = ((String) value).trim().toLowerCase(Locale.ROOT);
      
      // True values
      if ("true".equals(strValue) || "1".equals(strValue) || 
          "yes".equals(strValue) || "y".equals(strValue) ||
          "enabled".equals(strValue) || "active".equals(strValue) ||
          "on".equals(strValue)) {
        return Boolean.TRUE;
      }
      
      // False values  
      if ("false".equals(strValue) || "0".equals(strValue) || 
          "no".equals(strValue) || "n".equals(strValue) ||
          "disabled".equals(strValue) || "inactive".equals(strValue) ||
          "off".equals(strValue)) {
        return Boolean.FALSE;
      }
      
      throw new IllegalArgumentException("Cannot parse boolean from: " + value);
    }
    
    throw new IllegalArgumentException("Unsupported boolean type: " + value.getClass());
  }

  /**
   * Converts various formats to Integer values.
   * Handles cloud provider numeric formats including strings and floating point values.
   *
   * @param value The value to convert to Integer
   * @return Integer value or null
   */
  private static Integer convertToInteger(Object value) {
    if (isNullValue(value)) {
      return null;
    }

    // Direct integer types
    if (value instanceof Integer) {
      return (Integer) value;
    }
    if (value instanceof Number) {
      return ((Number) value).intValue();
    }

    // String representations
    if (value instanceof String) {
      String strValue = ((String) value).trim();
      try {
        return Integer.valueOf(strValue);
      } catch (NumberFormatException e) {
        // Try parsing as double first, then convert to int
        return (int) Double.parseDouble(strValue);
      }
    }

    throw new IllegalArgumentException("Cannot convert to Integer: " + value);
  }

  /**
   * Converts various formats to Long values.
   * Handles cloud provider numeric formats for large integers.
   *
   * @param value The value to convert to Long
   * @return Long value or null
   */
  private static Long convertToBigInteger(Object value) {
    if (value == null) {
      return null;
    }

    // Direct long types
    if (value instanceof Long) {
      return (Long) value;
    }
    if (value instanceof Number) {
      return ((Number) value).longValue();
    }

    // String representations
    if (value instanceof String) {
      String strValue = ((String) value).trim();
      if (strValue.isEmpty() || "null".equalsIgnoreCase(strValue)) {
        return null;
      }
      try {
        return Long.valueOf(strValue);
      } catch (NumberFormatException e) {
        // Try parsing as double first, then convert to long
        return (long) Double.parseDouble(strValue);
      }
    }

    throw new IllegalArgumentException("Cannot convert to Long: " + value);
  }

  /**
   * Converts various formats to Float values.
   * Handles cloud provider numeric formats.
   *
   * @param value The value to convert to Float
   * @return Float value or null
   */
  private static Float convertToFloat(Object value) {
    if (value == null) {
      return null;
    }

    // Direct float types
    if (value instanceof Float) {
      return (Float) value;
    }
    if (value instanceof Number) {
      return ((Number) value).floatValue();
    }

    // String representations
    if (value instanceof String) {
      String strValue = ((String) value).trim();
      if (strValue.isEmpty() || "null".equalsIgnoreCase(strValue)) {
        return null;
      }
      return Float.valueOf(strValue);
    }

    throw new IllegalArgumentException("Cannot convert to Float: " + value);
  }

  /**
   * Converts various formats to Double values.
   * Handles cloud provider numeric formats.
   *
   * @param value The value to convert to Double
   * @return Double value or null
   */
  private static Double convertToDouble(Object value) {
    if (value == null) {
      return null;
    }

    // Direct double types
    if (value instanceof Double) {
      return (Double) value;
    }
    if (value instanceof Number) {
      return ((Number) value).doubleValue();
    }

    // String representations
    if (value instanceof String) {
      String strValue = ((String) value).trim();
      if (strValue.isEmpty() || "null".equalsIgnoreCase(strValue)) {
        return null;
      }
      return Double.valueOf(strValue);
    }

    throw new IllegalArgumentException("Cannot convert to Double: " + value);
  }

  /**
   * Converts various formats to BigDecimal values.
   * Handles cloud provider precise numeric formats.
   *
   * @param value The value to convert to BigDecimal
   * @return BigDecimal value or null
   */
  private static BigDecimal convertToDecimal(Object value) {
    if (value == null) {
      return null;
    }

    // Direct BigDecimal types
    if (value instanceof BigDecimal) {
      return (BigDecimal) value;
    }
    if (value instanceof Number) {
      return BigDecimal.valueOf(((Number) value).doubleValue());
    }

    // String representations
    if (value instanceof String) {
      String strValue = ((String) value).trim();
      if (strValue.isEmpty() || "null".equalsIgnoreCase(strValue)) {
        return null;
      }
      return new BigDecimal(strValue);
    }

    throw new IllegalArgumentException("Cannot convert to BigDecimal: " + value);
  }

  /**
   * Converts values to String representation.
   * For VARCHAR/CHAR fields, we preserve ALL string representations including
   * "null", "N/A", "-", etc. as these may be valid business values.
   *
   * @param value The value to convert to String
   * @return String value or null (only for actual Java null)
   */
  private static String convertToString(Object value) {
    if (value == null) {
      return null;
    }

    return value.toString();
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
