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
package org.apache.calcite.adapter.splunk;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

/**
 * Utility class for converting values from Splunk JSON to appropriate Java types
 * based on the expected schema data types.
 *
 *
 * <p>Optimized for ISO timestamp strings from Splunk.
 */
public final class SplunkDataConverter {

  private SplunkDataConverter() {
    // Utility class, prevent instantiation
  }
  private static final Logger LOGGER = LoggerFactory.getLogger(SplunkDataConverter.class);

  /**
   * Converts string to days since epoch for DATE fields.
   * FIXED: Handle the new Timestamp return type.
   */
  private static Integer convertToDateDays(String value) {
    java.sql.Timestamp timestamp = convertIsoStringToTimestampMillis(value);
    long millis = timestamp.getTime();
    // Convert milliseconds to days since epoch
    return (int) (millis / (24 * 60 * 60 * 1000L));
  }

  /**
   * Converts string to milliseconds since midnight for TIME fields.
   * FIXED: Handle the new Timestamp return type.
   */
  private static Integer convertToTimeMillis(String value) {
    try {
      // Try parsing as HH:mm:ss format
      SimpleDateFormat timeFormat = new SimpleDateFormat("HH:mm:ss", Locale.ROOT);
      java.util.Date parsed = timeFormat.parse(value);
      // Get milliseconds since midnight
      return (int) (parsed.getTime() % (24 * 60 * 60 * 1000L));
    } catch (ParseException e) {
      // Try parsing as full timestamp and extract time portion
      java.sql.Timestamp timestamp = convertIsoStringToTimestampMillis(value);
      long millis = timestamp.getTime();
      return (int) (millis % (24 * 60 * 60 * 1000L));
    }
  }

  /**
   * Convert ISO timestamp string to java.sql.Timestamp.
   * Returns Timestamp for proper JDBC compatibility.
   */
  private static java.sql.Timestamp convertIsoStringToTimestampMillis(String value) {
    // Handle null, empty, or clearly invalid values early
    if (value == null || value.trim().isEmpty()) {
      throw new IllegalArgumentException("Cannot parse null or empty timestamp value");
    }

    // Handle obvious malformed values that would cause parsing errors
    String trimmed = value.trim();
    if (trimmed.equals(".E0") || trimmed.startsWith(".E") || trimmed.equals("null")) {
      throw new IllegalArgumentException("Cannot parse malformed timestamp value: " + value);
    }

    // First try modern ISO 8601 parsing (faster and more accurate)
    for (DateTimeFormatter formatter : ISO_FORMATTERS) {
      try {
        Instant instant = Instant.from(formatter.parse(trimmed));
        return new java.sql.Timestamp(instant.toEpochMilli());
      } catch (DateTimeParseException e) {
        // Try next formatter
      }
    }

    // Try parsing as epoch timestamp (numeric string)
    if (EPOCH_PATTERN.matcher(trimmed).matches()) {
      try {
        double epochValue = Double.parseDouble(trimmed);
        long millis = convertNumberToTimestampMillis(epochValue);
        return new java.sql.Timestamp(millis);
      } catch (NumberFormatException e) {
        // Fall through to legacy parsing
      }
    }

    // Fallback to legacy SimpleDateFormat parsing
    for (SimpleDateFormat format : LEGACY_FORMATS) {
      try {
        return new java.sql.Timestamp(format.parse(trimmed).getTime());
          // Timestamp object
      } catch (ParseException e) {
        // Try next format
      }
    }

    throw new IllegalArgumentException("Unable to parse timestamp: " + value);
  }

  /**
   * MODIFIED: Update the TIMESTAMP case to return Timestamp objects.
   */
  public static Object convertValue(Object value, SqlTypeName targetType) {
    if (value == null) {
      return null;
    }

    // If already the correct type, return as-is
    if (isCorrectType(value, targetType)) {
      return value;
    }

    // Handle direct numeric conversions from Jackson
    switch (targetType) {
    case INTEGER:
      if (value instanceof Number) {
        return ((Number) value).intValue();
      }
      break;
    case BIGINT:
      if (value instanceof Number) {
        return ((Number) value).longValue();
      }
      break;
    case DOUBLE:
      if (value instanceof Number) {
        return ((Number) value).doubleValue();
      }
      break;
    case FLOAT:
    case REAL:
      if (value instanceof Number) {
        return ((Number) value).floatValue();
      }
      break;
    case BOOLEAN:
      if (value instanceof Boolean) {
        return value;
      }
      break;
    case TIMESTAMP:
      // Return Long (milliseconds since epoch) for TIMESTAMP fields
      // This is consistent with Calcite's internal representation
      // The JDBC layer will handle conversion to java.sql.Timestamp when needed
      if (value instanceof String) {
        String strValue = ((String) value).trim();
        // Handle empty or clearly invalid timestamp strings early
        if (strValue.isEmpty() || "null".equals(strValue) || strValue.startsWith(".E")) {
          return null; // Return null for invalid timestamp strings
        }
        return convertIsoStringToTimestampMillis(strValue).getTime();
      } else if (value instanceof Number) {
        // Fallback for numeric epoch timestamps
        return convertNumberToTimestampMillis((Number) value);
      } else if (value instanceof java.sql.Timestamp) {
        return ((java.sql.Timestamp) value).getTime();
      }
      break;
    }

    // Convert to string and parse (for string values from JSON)
    String stringValue = value.toString().trim();

    // Handle empty strings and literal "null" strings
    if (stringValue.isEmpty() || "null".equals(stringValue)) {
      switch (targetType) {
      case VARCHAR:
      case CHAR:
        return stringValue; // Preserve for text fields
      default:
        return null; // Convert to NULL for other types
      }
    }

    try {
      switch (targetType) {
      case TIMESTAMP:
        // Handle empty or clearly invalid timestamp strings early
        if (stringValue.isEmpty() || "null".equals(stringValue) || stringValue.startsWith(".E")) {
          return null; // Return null for invalid timestamp strings
        }
        return convertIsoStringToTimestampMillis(stringValue).getTime();  // Return Long milliseconds

      case DATE:
        return convertToDateDays(stringValue);

      case TIME:
        return convertToTimeMillis(stringValue);

      case INTEGER:
        return convertToInteger(stringValue);

      case BIGINT:
        return convertToBigInt(stringValue);

      case DECIMAL:
        return convertToDecimal(stringValue);

      case DOUBLE:
        return convertToDouble(stringValue);

      case FLOAT:
      case REAL:
        return convertToFloat(stringValue);

      case BOOLEAN:
        return convertToBoolean(stringValue);

      case VARCHAR:
      case CHAR:
        return stringValue; // Keep as string

      default:
        return stringValue; // Keep as string
      }

    } catch (Exception e) {
      // Re-throw the exception so it can be handled properly at the row level
      throw new RuntimeException("Failed to convert value '" + stringValue
          + "' to type " + targetType + ": " + e.getMessage(), e);
    }
  }

  // ISO 8601 timestamp patterns (most common from Splunk)
  private static final DateTimeFormatter[] ISO_FORMATTERS = {
      DateTimeFormatter.ISO_INSTANT,                          // 2025-06-07T14:07:02.975Z
      DateTimeFormatter.ISO_OFFSET_DATE_TIME,                 // 2025-06-07T14:07:02.975+00:00
      DateTimeFormatter.ISO_LOCAL_DATE_TIME,                  // 2025-06-07T14:07:02.975
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.ROOT), // 2025-06-07T14:07:02Z
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssXXX", Locale.ROOT), // ISO with timezone
  };

  // Fallback legacy formats
  private static final SimpleDateFormat[] LEGACY_FORMATS = {
      new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS", Locale.ROOT),       // 2025-06-07 14:07:02.975
      new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ROOT),           // 2025-06-07 14:07:02
      new SimpleDateFormat("MM/dd/yyyy HH:mm:ss", Locale.ROOT),           // 06/07/2025 14:07:02
      new SimpleDateFormat("dd/MM/yyyy HH:mm:ss", Locale.ROOT),           // 07/06/2025 14:07:02
      new SimpleDateFormat("yyyy/MM/dd HH:mm:ss", Locale.ROOT),           // 2025/06/07 14:07:02
  };

  // Pattern for detecting numeric epoch timestamps (fallback)
  private static final Pattern EPOCH_PATTERN = Pattern.compile("^\\d{10}(\\.\\d+)?$|^\\d{13}$");

  /**
   * Converts a row of values to appropriate types based on the schema.
   *
   * @param row    The row data from Jackson JSON parsing
   * @param schema The expected schema
   * @return Converted row with appropriate data types
   */
  public static Object[] convertRow(Object[] row, RelDataType schema) {
    if (row == null || schema == null) {
      return row;
    }

    Object[] converted = new Object[row.length];
    List<RelDataTypeField> fields = schema.getFieldList();

    for (int i = 0; i < row.length && i < fields.size(); i++) {
      RelDataTypeField field = fields.get(i);
      Object value = row[i];

      // Debug logging for timestamp and integer conversions
      boolean debugThis = field.getType().getSqlTypeName() == SqlTypeName.TIMESTAMP
          || (field.getType().getSqlTypeName() == SqlTypeName.INTEGER && value instanceof String);

      if (debugThis && value != null) {
        LOGGER.debug("DEBUG: Converting field '{}' at index {}", field.getName(), i);
        LOGGER.debug("  Input value: '{}' (type: {})", value, value.getClass().getSimpleName());
        LOGGER.debug("  Expected type: {}", field.getType().getSqlTypeName());
      }

      if (value == null) {
        converted[i] = null;
        if (debugThis) {
          LOGGER.debug("  Result: null (was null input)");
        }
        continue;
      }

      try {
        Object convertedValue = convertValue(value, field.getType().getSqlTypeName());
        converted[i] = convertedValue;

        if (debugThis) {
          LOGGER.debug("  Result: '{}' (type: {})", convertedValue,
              convertedValue != null ? convertedValue.getClass().getSimpleName() : "null");
        }
      } catch (Exception e) {
        // Log the conversion error and provide a safe default
        LOGGER.warn("Warning: Failed to convert field '{}' value '{}' ({}) to type {}: {}",
            field.getName(), value, value.getClass().getSimpleName(),
            field.getType().getSqlTypeName(), e.getMessage());

        // For type safety, return null instead of the original value
        converted[i] = null;

        if (debugThis) {
          LOGGER.debug("  Result: null (conversion failed)");
        }
      }
    }

    return converted;
  }


  /**
   * Type checking for Jackson-parsed values.
   */
  private static boolean isCorrectType(Object value, SqlTypeName targetType) {
    switch (targetType) {
    case TIMESTAMP:
      return value instanceof Long;
    case DATE:
      return value instanceof Integer;
    case TIME:
      return value instanceof Integer;
    case INTEGER:
      return value instanceof Integer;
    case BIGINT:
      return value instanceof Long;
    case DECIMAL:
      return value instanceof BigDecimal;
    case DOUBLE:
      return value instanceof Double;
    case FLOAT:
    case REAL:
      return value instanceof Float;
    case BOOLEAN:
      return value instanceof Boolean;
    case VARCHAR:
    case CHAR:
      return value instanceof String;
    default:
      return false;
    }
  }

  /**
   * Convert a numeric value to timestamp milliseconds (fallback).
   */
  private static Long convertNumberToTimestampMillis(Number number) {
    double value = number.doubleValue();

    // If value is less than 1e12, assume it's in seconds and convert to milliseconds
    if (value < 1e12) {
      return Math.round(value * 1000);
    } else {
      // Assume it's already in milliseconds
      return Math.round(value);
    }
  }


  /**
   * Converts string to Integer.
   */
  private static Integer convertToInteger(String value) {
    try {
      // Handle decimal values by truncating
      if (value.contains(".")) {
        return (int) Double.parseDouble(value);
      }
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Unable to parse integer: " + value);
    }
  }

  /**
   * Converts string to Long.
   */
  private static Long convertToBigInt(String value) {
    try {
      // Handle decimal values by truncating
      if (value.contains(".")) {
        return (long) Double.parseDouble(value);
      }
      return Long.parseLong(value);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Unable to parse long: " + value);
    }
  }

  /**
   * Converts string to BigDecimal.
   */
  private static BigDecimal convertToDecimal(String value) {
    try {
      return new BigDecimal(value);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Unable to parse decimal: " + value);
    }
  }

  /**
   * Converts string to Double.
   */
  private static Double convertToDouble(String value) {
    try {
      return Double.parseDouble(value);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Unable to parse double: " + value);
    }
  }

  /**
   * Converts string to Float.
   */
  private static Float convertToFloat(String value) {
    try {
      return Float.parseFloat(value);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Unable to parse float: " + value);
    }
  }

  /**
   * Converts string to Boolean.
   */
  private static Boolean convertToBoolean(String value) {
    String lower = value.toLowerCase(Locale.ROOT);
    if ("true".equals(lower) || "1".equals(lower) || "yes".equals(lower) || "y".equals(lower)) {
      return Boolean.TRUE;
    } else if ("false".equals(lower) || "0".equals(lower) || "no".equals(lower)
        || "n".equals(lower)) {
      return Boolean.FALSE;
    } else {
      throw new IllegalArgumentException("Unable to parse boolean: " + value);
    }
  }
}
