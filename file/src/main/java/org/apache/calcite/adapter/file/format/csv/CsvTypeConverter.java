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
package org.apache.calcite.adapter.file.format.csv;

import org.apache.calcite.adapter.file.util.NullEquivalents;
import org.apache.calcite.sql.type.SqlTypeName;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Map;
import java.util.Set;

/**
 * Utility class for converting CSV string values to typed objects using
 * the same parsing logic and formatters that were successful during type inference.
 *
 * <p>This ensures consistency between type inference and runtime conversion,
 * avoiding duplicate parsing logic and potential inconsistencies.
 */
public final class CsvTypeConverter {
  private static final Logger LOGGER = LoggerFactory.getLogger(CsvTypeConverter.class);

  // Common timestamp formats to try during parsing
  private static final DateTimeFormatter[] TIMESTAMP_FORMATTERS = {
      DateTimeFormatter.ISO_LOCAL_DATE_TIME,
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"),
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"),
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss'Z'"),
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'"),
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssXXX"),
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssXXX"),
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss z"),  // Timezone abbreviation like EST, PST
      DateTimeFormatter.ofPattern("EEE, dd MMM yyyy HH:mm:ss Z"),  // RFC 2822 format
      DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm:ss"),
      DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss")
  };

  private static final DateTimeFormatter[] DATE_FORMATTERS = {
      DateTimeFormatter.ISO_LOCAL_DATE,
      DateTimeFormatter.ofPattern("yyyy-MM-dd"),
      DateTimeFormatter.ofPattern("yyyy/MM/dd"),
      DateTimeFormatter.ofPattern("yyyy.MM.dd"),
      DateTimeFormatter.ofPattern("MM/dd/yyyy"),
      DateTimeFormatter.ofPattern("dd/MM/yyyy")
  };

  private final Map<SqlTypeName, DateTimeFormatter> formatters;
  private final Set<String> nullEquivalents;
  private final boolean blankStringsAsNull;

  public CsvTypeConverter(Map<SqlTypeName, DateTimeFormatter> formatters, Set<String> nullEquivalents, boolean blankStringsAsNull) {
    this.formatters = formatters;
    this.nullEquivalents = nullEquivalents;
    this.blankStringsAsNull = blankStringsAsNull;
  }

  /**
   * Converts a string value to the target SQL type using the proven successful formatters.
   *
   * @param value the string value to convert
   * @param targetType the target SQL type
   * @return the converted value, or null if the value represents null
   */
  public @Nullable Object convert(String value, SqlTypeName targetType) {
    LOGGER.debug("CsvTypeConverter.convert() called with value='{}' targetType={}", value, targetType);
    Object finalResult;
    switch (targetType) {
    case BOOLEAN:
      if (isNullRepresentation(value)) {
        finalResult = null;
      } else {
        finalResult = parseBoolean(value);
      }
      break;
    case TINYINT:
      if (isNullRepresentation(value)) {
        finalResult = null;
      } else {
        finalResult = parseByte(value);
      }
      break;
    case SMALLINT:
      if (isNullRepresentation(value)) {
        finalResult = null;
      } else {
        finalResult = parseShort(value);
      }
      break;
    case INTEGER:
      if (isNullRepresentation(value)) {
        finalResult = null;
      } else {
        finalResult = parseInt(value);
      }
      break;
    case BIGINT:
      if (isNullRepresentation(value)) {
        finalResult = null;
      } else {
        finalResult = parseLong(value);
      }
      break;
    case REAL:
      if (isNullRepresentation(value)) {
        finalResult = null;
      } else {
        finalResult = parseFloat(value);
      }
      break;
    case FLOAT:
    case DOUBLE:
      if (isNullRepresentation(value)) {
        finalResult = null;
      } else {
        finalResult = parseDouble(value);
      }
      break;
    case DECIMAL:
      if (isNullRepresentation(value)) {
        finalResult = null;
      } else {
        finalResult = parseDecimal(value);
      }
      break;
    case DATE:
      if (isNullRepresentation(value)) {
        LOGGER.debug("DATE conversion: value '{}' is null representation, returning null", value);
        finalResult = null;
      } else {
        Object result = parseDate(value);
        LOGGER.debug("DATE conversion: value '{}' converted to: {} (type: {})", value, result, result.getClass().getSimpleName());
        finalResult = result;
      }
      break;
    case TIME:
      if (isNullRepresentation(value)) {
        finalResult = null;
      } else {
        finalResult = parseTime(value);
      }
      break;
    case TIMESTAMP:
      LOGGER.debug("=== TIMESTAMPTZ DEBUG: Processing {} field with value='{}' ===", targetType, value);
      if (isNullRepresentation(value)) {
        finalResult = null;
        LOGGER.debug("=== TIMESTAMPTZ DEBUG: Value is null representation for {} ===", targetType);
      } else {
        // TIMESTAMP = wall clock time, no timezone conversion
        finalResult = parseTimestamp(value);
        LOGGER.debug("=== TIMESTAMPTZ DEBUG: parseTimestamp returned {} for {} field ===", finalResult, targetType);
      }
      break;
    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      LOGGER.debug("=== TIMESTAMPTZ DEBUG: Processing {} field with value='{}' ===", targetType, value);
      if (isNullRepresentation(value)) {
        finalResult = null;
        LOGGER.debug("=== TIMESTAMPTZ DEBUG: Value is null representation for {} ===", targetType);
      } else {
        // TIMESTAMP_WITH_LOCAL_TIME_ZONE = timezone-aware, must convert to UTC
        finalResult = parseTimestampWithTimezone(value);
        LOGGER.debug("=== TIMESTAMPTZ DEBUG: parseTimestampWithTimezone returned {} for {} field ===", finalResult, targetType);
      }
      break;
    case VARCHAR:
    case CHAR:
      // For string types, respect the blankStringsAsNull setting
      if (value != null && value.length() == 0) {
        finalResult = blankStringsAsNull ? null : value;
      } else {
        finalResult = value;
      }
      break;
    default:
      LOGGER.warn("Unknown target type: {}, returning string value", targetType);
      finalResult = value;
      break;
    }
    LOGGER.debug("CsvTypeConverter.convert() final result: {} (type: {}) for value='{}' targetType={}",
                finalResult, (finalResult != null ? finalResult.getClass().getSimpleName() : "null"), value, targetType);
    return finalResult;
  }

  private boolean isNullRepresentation(String value) {
    return NullEquivalents.isNullRepresentation(value, nullEquivalents);
  }

  private Boolean parseBoolean(String value) {
    String lower = value.toLowerCase();
    if ("true".equals(lower) || "1".equals(value) || "yes".equals(lower) || "y".equals(lower)) {
      return Boolean.TRUE;
    }
    if ("false".equals(lower) || "0".equals(value) || "no".equals(lower) || "n".equals(lower)) {
      return Boolean.FALSE;
    }
    throw new NumberFormatException("Cannot parse boolean: " + value);
  }

  private Byte parseByte(String value) {
    return Byte.valueOf(value);
  }

  private Short parseShort(String value) {
    return Short.valueOf(value);
  }

  private Integer parseInt(String value) {
    return Integer.valueOf(value);
  }

  private Long parseLong(String value) {
    return Long.valueOf(value);
  }

  private Float parseFloat(String value) {
    return Float.valueOf(value);
  }

  private Double parseDouble(String value) {
    return Double.valueOf(value);
  }

  private BigDecimal parseDecimal(String value) {
    return new BigDecimal(value);
  }

  private Integer parseDate(String value) {
    DateTimeFormatter formatter = formatters.get(SqlTypeName.DATE);
    if (formatter != null) {
      try {
        LocalDate localDate = LocalDate.parse(value, formatter);
        LOGGER.debug("Successfully parsed date '{}' using stored formatter, returning epoch day: {}", value, (int) localDate.toEpochDay());
        return Integer.valueOf((int) localDate.toEpochDay());
      } catch (DateTimeParseException e) {
        LOGGER.debug("Failed to parse date '{}' with stored formatter: {}", value, e.getMessage());
      }
    }

    // Try common date formats
    for (DateTimeFormatter dateFormatter : DATE_FORMATTERS) {
      try {
        LocalDate localDate = LocalDate.parse(value, dateFormatter);
        LOGGER.debug("Successfully parsed date '{}' using built-in formatter, returning epoch day: {}", value, (int) localDate.toEpochDay());
        return Integer.valueOf((int) localDate.toEpochDay());
      } catch (DateTimeParseException e) {
        LOGGER.debug("Failed to parse date '{}' with formatter {}: {}", value, dateFormatter, e.getMessage());
      }
    }

    LOGGER.warn("Failed to parse date: '{}' - returning null", value);
    return null;
  }

  private Integer parseTime(String value) {
    DateTimeFormatter formatter = formatters.get(SqlTypeName.TIME);
    if (formatter != null) {
      try {
        LocalTime localTime = LocalTime.parse(value, formatter);
        // Convert to milliseconds since midnight correctly
        // toNanoOfDay() gives nanoseconds, divide by 1_000_000 to get milliseconds
        int millisSinceMidnight = (int) (localTime.toNanoOfDay() / 1_000_000L);
        LOGGER.debug("Successfully parsed time '{}' using stored formatter, returning millis: {}", value, millisSinceMidnight);
        return Integer.valueOf(millisSinceMidnight);
      } catch (DateTimeParseException e) {
        LOGGER.debug("Failed to parse time '{}' with stored formatter: {}", value, e.getMessage());
      }
    }


    // Fallback: try the most common format
    try {
      LocalTime localTime = LocalTime.parse(value);
      // Convert to milliseconds since midnight correctly
      int millisSinceMidnight = (int) (localTime.toNanoOfDay() / 1_000_000L);
      LOGGER.debug("Successfully parsed time '{}' using default formatter, returning millis: {}", value, millisSinceMidnight);
      return Integer.valueOf(millisSinceMidnight);
    } catch (DateTimeParseException e) {
      LOGGER.warn("Failed to parse time: '{}' - returning null", value);
      return null;
    }
  }

  private Long parseTimestamp(String value) {
    LOGGER.debug("=== TIMESTAMP DEBUG: parseTimestamp called with value='{}' ===", value);

    // Try common timestamp formats
    for (DateTimeFormatter formatter : TIMESTAMP_FORMATTERS) {
      try {
        LocalDateTime ldt = LocalDateTime.parse(value, formatter);
        // For TIMESTAMP WITHOUT TIME ZONE (wall clock time):
        // Store the wall clock time as if it were UTC
        // This preserves the wall clock value regardless of the JVM's timezone
        long millis = ldt.toInstant(ZoneOffset.UTC).toEpochMilli();
        LOGGER.debug("=== TIMESTAMP DEBUG: Parsed timestamp '{}' as UTC wall clock (formatter={}), storing millis: {} ===",
            value, formatter, millis);
        return Long.valueOf(millis);
      } catch (DateTimeParseException e) {
        LOGGER.debug("Failed to parse timestamp '{}' with formatter {}: {}", value, formatter, e.getMessage());
      }
    }

    // Try parsing as date-only (assume midnight)
    for (DateTimeFormatter dateFormatter : DATE_FORMATTERS) {
      try {
        LocalDate localDate = LocalDate.parse(value, dateFormatter);
        LocalDateTime ldt = localDate.atStartOfDay();
        LOGGER.warn("Parsed date-only value '{}' as timestamp by assuming midnight: {}", value, ldt);
        // For TIMESTAMP WITHOUT TIME ZONE: store as UTC wall clock
        long millis = ldt.toInstant(ZoneOffset.UTC).toEpochMilli();
        return Long.valueOf(millis);
      } catch (DateTimeParseException e) {
        // Continue trying other formats
      }
    }

    LOGGER.warn("Failed to parse timestamp: '{}' - returning null", value);
    return null;
  }

  private Long parseTimestampWithTimezone(String value) {
    LOGGER.debug("=== TIMESTAMPTZ DEBUG: parseTimestampWithTimezone called with value='{}' ===", value);

    // Timezone-aware formatters for TIMESTAMP_WITH_LOCAL_TIME_ZONE
    DateTimeFormatter[] TIMEZONE_AWARE_FORMATTERS = {
      DateTimeFormatter.ISO_OFFSET_DATE_TIME,  // 2024-03-15T10:30:45Z, 2024-03-15T10:30:45+05:30
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssXXX"),  // 2024-03-15 10:30:45+05:30
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssXXX"), // 2024-03-15T10:30:45+05:30
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss'Z'"),   // 2024-03-15 10:30:45Z (literal Z)
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'"), // 2024-03-15T10:30:45Z (literal Z)
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss z"),    // 2024-03-15 10:30:45 EST
      DateTimeFormatter.RFC_1123_DATE_TIME,  // Fri, 15 Mar 2024 10:30:45 +0000
      DateTimeFormatter.ofPattern("EEE, dd MMM yyyy HH:mm:ss Z")  // RFC 2822 format
    };

    // First try parsing with timezone information
    for (DateTimeFormatter formatter : TIMEZONE_AWARE_FORMATTERS) {
      try {
        if (formatter == DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss'Z'") ||
            formatter == DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'")) {
          // Special handling for literal 'Z' - treat as UTC
          String normalizedValue = value.replace("Z", "+00:00");
          OffsetDateTime odt = OffsetDateTime.parse(normalizedValue,
              DateTimeFormatter.ofPattern(formatter.toString().replace("'Z'", "XXX")));
          long utcMillis = odt.toInstant().toEpochMilli();
          LOGGER.debug("=== TIMESTAMPTZ DEBUG: Parsed '{}' with timezone as UTC (literal Z), UTC millis: {} ===",
              value, utcMillis);
          return Long.valueOf(utcMillis);
        } else {
          // Try parsing as OffsetDateTime first
          OffsetDateTime odt = OffsetDateTime.parse(value, formatter);
          long utcMillis = odt.toInstant().toEpochMilli();
          LOGGER.debug("=== TIMESTAMPTZ DEBUG: Parsed '{}' with timezone offset using formatter={}, UTC millis: {} ===",
              value, formatter, utcMillis);
          return Long.valueOf(utcMillis);
        }
      } catch (DateTimeParseException e) {
        try {
          // Try parsing as ZonedDateTime for timezone abbreviations
          ZonedDateTime zdt = ZonedDateTime.parse(value, formatter);
          long utcMillis = zdt.toInstant().toEpochMilli();
          LOGGER.debug("=== TIMESTAMPTZ DEBUG: Parsed '{}' with timezone name, UTC millis: {} ===",
              value, utcMillis);
          return Long.valueOf(utcMillis);
        } catch (DateTimeParseException e2) {
          LOGGER.debug("Failed to parse timestamptz '{}' with formatter {}: {}", value, formatter, e2.getMessage());
        }
      }
    }

    // If no timezone parsing worked, log warning and fall back to wall clock parsing
    LOGGER.warn("=== TIMESTAMPTZ DEBUG: Failed to parse timezone from '{}', falling back to wall clock time ===", value);
    return parseTimestamp(value);
  }
}
