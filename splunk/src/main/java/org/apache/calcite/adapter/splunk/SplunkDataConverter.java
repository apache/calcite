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

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Utility class for converting string values from Splunk to appropriate Java types
 * based on the expected schema data types.
 */
public class SplunkDataConverter {

  // Common Splunk timestamp formats
  private static final SimpleDateFormat[] TIMESTAMP_FORMATS = {
      new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),  // ISO 8601 with timezone
      new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX"),      // ISO 8601 without millis
      new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS"),       // Standard format with millis
      new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"),           // Standard format
      new SimpleDateFormat("MM/dd/yyyy HH:mm:ss"),           // US format
      new SimpleDateFormat("dd/MM/yyyy HH:mm:ss"),           // EU format
      new SimpleDateFormat("yyyy/MM/dd HH:mm:ss"),           // Alternative format
  };

  // Pattern for detecting numeric epoch timestamps (10 or 13 digits)
  private static final Pattern EPOCH_PATTERN = Pattern.compile("^\\d{10}(\\.\\d+)?$|^\\d{13}$");

  /**
   * Converts a row of string values to appropriate types based on the schema.
   *
   * @param row The row data as strings (or other objects)
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

      if (value == null) {
        converted[i] = null;
        continue;
      }

      try {
        converted[i] = convertValue(value, field.getType().getSqlTypeName());
      } catch (Exception e) {
        // Log the conversion error but continue with original value
        System.err.println("Warning: Failed to convert field '" + field.getName() +
            "' value '" + value + "' to type " + field.getType().getSqlTypeName() +
            ": " + e.getMessage());
        converted[i] = value; // Keep original value
      }
    }

    return converted;
  }

  /**
   * Converts a single value to the specified SQL type.
   *
   * @param value The value to convert
   * @param targetType The target SQL type
   * @return Converted value
   */
  public static Object convertValue(Object value, SqlTypeName targetType) {
    if (value == null) {
      return null;
    }

    // If already the correct type, return as-is
    if (isCorrectType(value, targetType)) {
      return value;
    }

    String stringValue = value.toString().trim();

    // Handle empty strings
    if (stringValue.isEmpty()) {
      return getDefaultValue(targetType);
    }

    switch (targetType) {
    case TIMESTAMP:
        return convertToTimestampMillis(stringValue);

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
    default:
      return stringValue; // Keep as string
    }
  }

  /**
   * Checks if the value is already the correct type for the target SQL type.
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
    default:
      return false;
    }
  }

  /**
   * Converts string to epoch milliseconds for TIMESTAMP fields.
   * Avatica expects TIMESTAMP fields to be Long values representing epoch milliseconds.
   */
  private static Long convertToTimestampMillis(String value) {
    // Try parsing as epoch timestamp first
    if (EPOCH_PATTERN.matcher(value).matches()) {
      try {
        double epochValue = Double.parseDouble(value);
        long millis;

        // If value is less than 1e12, assume it's in seconds
        if (epochValue < 1e12) {
          millis = (long) (epochValue * 1000);
        } else {
          millis = (long) epochValue;
        }

        return millis;
      } catch (NumberFormatException e) {
        // Fall through to string parsing
      }
    }

    // Try parsing as formatted date string
    for (SimpleDateFormat format : TIMESTAMP_FORMATS) {
      try {
        return format.parse(value).getTime();
      } catch (ParseException e) {
        // Try next format
      }
    }

    throw new IllegalArgumentException("Unable to parse timestamp: " + value);
  }

  /**
   * Converts string to days since epoch for DATE fields.
   * Avatica expects DATE fields to be Integer values representing days since 1970-01-01.
   */
  private static Integer convertToDateDays(String value) {
    long millis = convertToTimestampMillis(value);
    // Convert milliseconds to days since epoch
    return (int) (millis / (24 * 60 * 60 * 1000L));
  }

  /**
   * Converts string to milliseconds since midnight for TIME fields.
   * Avatica expects TIME fields to be Integer values representing milliseconds since midnight.
   */
  private static Integer convertToTimeMillis(String value) {
    try {
      // Try parsing as HH:mm:ss format
      SimpleDateFormat timeFormat = new SimpleDateFormat("HH:mm:ss");
      java.util.Date parsed = timeFormat.parse(value);
      // Get milliseconds since midnight
      return (int) (parsed.getTime() % (24 * 60 * 60 * 1000L));
    } catch (ParseException e) {
      // Try parsing as full timestamp and extract time portion
      long millis = convertToTimestampMillis(value);
      return (int) (millis % (24 * 60 * 60 * 1000L));
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
    String lower = value.toLowerCase();
    if ("true".equals(lower) || "1".equals(lower) || "yes".equals(lower) || "y".equals(lower)) {
      return Boolean.TRUE;
    } else if ("false".equals(lower) || "0".equals(lower) || "no".equals(lower) || "n".equals(lower)) {
      return Boolean.FALSE;
    } else {
      throw new IllegalArgumentException("Unable to parse boolean: " + value);
    }
  }

  /**
   * Returns a default value for null/empty values based on the SQL type.
   */
  private static Object getDefaultValue(SqlTypeName targetType) {
    switch (targetType) {
    case TIMESTAMP:
        return 0L; // Epoch start
    case DATE:
        return 0; // Days since epoch start
    case TIME:
        return 0; // Milliseconds since midnight start
    case INTEGER:
      return 0;
    case BIGINT:
      return 0L;
    case DECIMAL:
      return BigDecimal.ZERO;
    case DOUBLE:
      return 0.0;
    case FLOAT:
    case REAL:
      return 0.0f;
    case BOOLEAN:
      return Boolean.FALSE;
    default:
      return null;
    }
  }
}
