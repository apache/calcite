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
package org.apache.calcite.adapter.file.converters;

import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.File;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

/**
 * Shared utility methods for file format converters.
 *
 * <p>Configuration options (via system properties):</p>
 * <ul>
 *   <li>{@code calcite.file.converter.timezone} - Default timezone for datetime values without TZ info
 *       (e.g., "UTC", "America/New_York"). If not set, datetimes are kept as LOCAL.</li>
 *   <li>{@code calcite.file.converter.date.strict} - If "true", only parse obvious date formats</li>
 * </ul>
 */
public final class ConverterUtils {

  /**
   * System property to specify default timezone for datetime values without timezone info.
   * If not set, datetime values are kept as LOCAL (no timezone).
   * Examples: "UTC", "America/New_York", "Europe/London"
   */
  public static final String TIMEZONE_PROPERTY = "calcite.file.converter.timezone";

  /**
   * System property to enable strict date parsing (only obvious formats).
   * Default is false (permissive parsing).
   */
  public static final String STRICT_DATE_PROPERTY = "calcite.file.converter.date.strict";

  private ConverterUtils() {
    // Utility class should not be instantiated
  }

  /**
   * Extracts the base name from a file by removing its extension.
   * Handles common file extensions like .html, .htm, .xlsx, .xls, etc.
   *
   * @param file The file to extract base name from
   * @return The base name without extension
   */
  public static String getBaseFileName(File file) {
    String fileName = file.getName();
    int lastDot = fileName.lastIndexOf('.');
    if (lastDot > 0) {
      return fileName.substring(0, lastDot);
    }
    return fileName;
  }

  /**
   * Extracts the base name from a file with specific extension handling.
   * This is used for HTML files where we check for both .html and .htm
   *
   * @param fileName The file name to process
   * @param extensions Extensions to check for (e.g., ".html", ".htm")
   * @return The base name without extension
   */
  public static String getBaseFileName(String fileName, String... extensions) {
    String lowerFileName = fileName.toLowerCase(Locale.ROOT);
    for (String ext : extensions) {
      if (lowerFileName.endsWith(ext.toLowerCase(Locale.ROOT))) {
        return fileName.substring(0, fileName.length() - ext.length());
      }
    }
    // Fall back to general extension removal
    int lastDot = fileName.lastIndexOf('.');
    if (lastDot > 0) {
      return fileName.substring(0, lastDot);
    }
    return fileName;
  }

  /**
   * Sets a value in a JSON object node with automatic type inference.
   * Attempts to parse the string value as date/time, numeric, or boolean before
   * storing as string. Dates are converted to ISO 8601 format.
   *
   * @param node The ObjectNode to set the value in
   * @param key The key for the value
   * @param value The string value to parse and set
   */
  public static void setJsonValueWithTypeInference(ObjectNode node, String key, String value) {
    if (value == null || value.isEmpty()) {
      node.putNull(key);
      return;
    }

    // Try to parse as date/time first
    String dateTimeResult = tryParseDateTimeToISO(value);
    if (dateTimeResult != null) {
      node.put(key, dateTimeResult);
      return;
    }

    // Try numeric
    if (isNumeric(value)) {
      if (value.contains(".")) {
        try {
          node.put(key, Double.parseDouble(value));
          return;
        } catch (NumberFormatException e) {
          // Fall through to string
        }
      } else {
        try {
          node.put(key, Long.parseLong(value));
          return;
        } catch (NumberFormatException e) {
          // Fall through to string
        }
      }
    }

    // Try boolean
    if ("true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value)) {
      node.put(key, Boolean.parseBoolean(value));
      return;
    }

    // Default to string
    node.put(key, value);
  }

  /**
   * Checks if a string represents a numeric value.
   *
   * @param str The string to check
   * @return true if the string can be parsed as a number
   */
  public static boolean isNumeric(String str) {
    if (str == null || str.isEmpty()) {
      return false;
    }
    try {
      Double.parseDouble(str);
      return true;
    } catch (NumberFormatException e) {
      return false;
    }
  }

  /**
   * Sanitizes a string to be used as a valid identifier.
   * Replaces non-alphanumeric characters with underscores,
   * collapses multiple underscores, and ensures valid identifier format.
   *
   * @param identifier The string to sanitize
   * @return A valid identifier string
   */
  public static String sanitizeIdentifier(String identifier) {
    if (identifier == null || identifier.isEmpty()) {
      return "column";
    }

    // Replace non-alphanumeric characters with underscore
    StringBuilder result = new StringBuilder();
    for (char c : identifier.toCharArray()) {
      if (Character.isLetterOrDigit(c) || c == '_') {
        result.append(c);
      } else {
        result.append('_');
      }
    }

    String str = result.toString();

    // Collapse multiple underscores (3 or more) but preserve double underscores for hierarchy
    // Replace 3+ underscores with double underscore (preserving hierarchy separator)
    str = str.replaceAll("_{3,}", "__");

    // Remove leading/trailing underscores
    str = str.replaceAll("^_+|_+$", "");

    // Ensure it starts with a letter or underscore
    if (!str.isEmpty() && Character.isDigit(str.charAt(0))) {
      str = "_" + str;
    }

    return str.isEmpty() ? "column" : str;
  }

  /**
   * Converts a string to PascalCase.
   * Capitalizes the first letter after spaces, underscores, or hyphens.
   *
   * @param input The string to convert
   * @return The PascalCase string
   */
  public static String toPascalCase(String input) {
    if (input == null || input.isEmpty()) {
      return input;
    }

    StringBuilder result = new StringBuilder();
    boolean capitalizeNext = true;

    for (char c : input.toCharArray()) {
      if (Character.isWhitespace(c) || c == '_' || c == '-') {
        capitalizeNext = true;
      } else if (capitalizeNext) {
        result.append(Character.toUpperCase(c));
        capitalizeNext = false;
      } else {
        result.append(Character.toLowerCase(c));
      }
    }

    return result.toString();
  }

  // Common date formats found in real-world data
  private static final List<DateTimeFormatter> DATE_TIME_FORMATTERS =
      Arrays.asList(// ISO formats
      DateTimeFormatter.ISO_LOCAL_DATE_TIME,
      DateTimeFormatter.ISO_DATE_TIME,
      DateTimeFormatter.ISO_INSTANT,

      // Common US formats
      DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm:ss"),
      DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm"),
      DateTimeFormatter.ofPattern("MM/dd/yyyy h:mm:ss a"),
      DateTimeFormatter.ofPattern("MM/dd/yyyy h:mm a"),
      DateTimeFormatter.ofPattern("M/d/yyyy HH:mm:ss"),
      DateTimeFormatter.ofPattern("M/d/yyyy HH:mm"),
      DateTimeFormatter.ofPattern("M/d/yyyy h:mm:ss a"),
      DateTimeFormatter.ofPattern("M/d/yyyy h:mm a"),

      // Common European formats
      DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss"),
      DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm"),
      DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss"),
      DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm"),
      DateTimeFormatter.ofPattern("d/M/yyyy HH:mm:ss"),
      DateTimeFormatter.ofPattern("d/M/yyyy HH:mm"),

      // With month names
      DateTimeFormatter.ofPattern("MMM d, yyyy HH:mm:ss"),
      DateTimeFormatter.ofPattern("MMM d, yyyy h:mm:ss a"),
      DateTimeFormatter.ofPattern("MMMM d, yyyy HH:mm:ss"),
      DateTimeFormatter.ofPattern("MMMM d, yyyy h:mm:ss a"),
      DateTimeFormatter.ofPattern("d MMM yyyy HH:mm:ss"),
      DateTimeFormatter.ofPattern("d MMMM yyyy HH:mm:ss"),

      // Database/log formats
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"),
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"),
      DateTimeFormatter.ofPattern("yyyyMMdd HHmmss"));

  private static final List<DateTimeFormatter> DATE_FORMATTERS =
      Arrays.asList(// ISO format
      DateTimeFormatter.ISO_LOCAL_DATE,

      // Common US formats
      DateTimeFormatter.ofPattern("MM/dd/yyyy"),
      DateTimeFormatter.ofPattern("M/d/yyyy"),
      DateTimeFormatter.ofPattern("MM-dd-yyyy"),
      DateTimeFormatter.ofPattern("M-d-yyyy"),

      // Common European formats
      DateTimeFormatter.ofPattern("dd/MM/yyyy"),
      DateTimeFormatter.ofPattern("d/M/yyyy"),
      DateTimeFormatter.ofPattern("dd-MM-yyyy"),
      DateTimeFormatter.ofPattern("d-M-yyyy"),
      DateTimeFormatter.ofPattern("dd.MM.yyyy"),
      DateTimeFormatter.ofPattern("d.M.yyyy"),

      // With month names
      DateTimeFormatter.ofPattern("MMM d, yyyy"),
      DateTimeFormatter.ofPattern("MMMM d, yyyy"),
      DateTimeFormatter.ofPattern("d MMM yyyy"),
      DateTimeFormatter.ofPattern("d MMMM yyyy"),
      DateTimeFormatter.ofPattern("MMM dd, yyyy"),
      DateTimeFormatter.ofPattern("MMMM dd, yyyy"),
      DateTimeFormatter.ofPattern("dd MMM yyyy"),
      DateTimeFormatter.ofPattern("dd MMMM yyyy"),

      // Compact formats
      DateTimeFormatter.ofPattern("yyyyMMdd"),
      DateTimeFormatter.ofPattern("yyyy-MM-dd"),
      DateTimeFormatter.ofPattern("yyyy/MM/dd"));

  private static final List<DateTimeFormatter> TIME_FORMATTERS =
      Arrays.asList(// ISO format
      DateTimeFormatter.ISO_LOCAL_TIME,

      // 24-hour formats
      DateTimeFormatter.ofPattern("HH:mm:ss.SSS"),
      DateTimeFormatter.ofPattern("HH:mm:ss"),
      DateTimeFormatter.ofPattern("HH:mm"),
      DateTimeFormatter.ofPattern("HHmmss"),

      // 12-hour formats with AM/PM
      DateTimeFormatter.ofPattern("h:mm:ss a"),
      DateTimeFormatter.ofPattern("h:mm a"),
      DateTimeFormatter.ofPattern("hh:mm:ss a"),
      DateTimeFormatter.ofPattern("hh:mm a"));

  // Pattern to detect if string might be a date/time
  private static final Pattern POTENTIAL_DATE_PATTERN =
      Pattern.compile(".*(?:\\d{1,4}[-/.]\\d{1,2}[-/.]\\d{1,4}|" +  // Date patterns
      "\\d{1,2}:\\d{2}|" +                           // Time patterns
      "(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*|" + // Month names
      "(?:AM|PM|am|pm)).*",                          // AM/PM markers
      Pattern.CASE_INSENSITIVE);

  /**
   * Attempts to parse a string as a date, time, or datetime and convert to ISO 8601 format.
   * Returns null if the string doesn't match any known date/time pattern.
   *
   * Important: DateTime values without explicit timezone information are kept as LOCAL datetime
   * (without timezone) to avoid incorrect assumptions about the data's timezone.
   *
   * @param value The string to parse
   * @return ISO 8601 formatted string or null if not a date/time
   */
  private static String tryParseDateTimeToISO(String value) {
    if (value == null || value.trim().isEmpty()) {
      return null;
    }

    String trimmed = value.trim();

    // Quick check to avoid expensive parsing for obviously non-date strings
    if (!POTENTIAL_DATE_PATTERN.matcher(trimmed).matches()) {
      return null;
    }

    // Check if strict parsing is enabled
    boolean strictParsing = Boolean.getBoolean(STRICT_DATE_PROPERTY);
    if (strictParsing && !isObviousDateFormat(trimmed)) {
      return null;
    }

    // Check if the string contains explicit timezone information
    boolean hasTimezone = trimmed.matches(".*[+-]\\d{2}:\\d{2}.*") ||
                         trimmed.matches(".*[+-]\\d{4}.*") ||
                         trimmed.matches(".*\\s+[A-Z]{3,4}$");  // Like EST, PST, GMT

    // Get configured default timezone (if any)
    String defaultTimezone = System.getProperty(TIMEZONE_PROPERTY);
    ZoneId configuredZone = null;
    if (defaultTimezone != null && !defaultTimezone.isEmpty()) {
      try {
        configuredZone = ZoneId.of(defaultTimezone);
      } catch (Exception e) {
        // Invalid timezone, ignore and use LOCAL
      }
    }

    // Try parsing as datetime first
    for (DateTimeFormatter formatter : DATE_TIME_FORMATTERS) {
      try {
        if (hasTimezone && formatter == DateTimeFormatter.ISO_DATE_TIME) {
          // Parse with timezone if present
          ZonedDateTime zoned = ZonedDateTime.parse(trimmed, formatter);
          return zoned.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        } else {
          // Parse as local datetime
          LocalDateTime dateTime = LocalDateTime.parse(trimmed, formatter);

          // Apply configured timezone if set, otherwise keep as LOCAL
          if (configuredZone != null) {
            ZonedDateTime zoned = dateTime.atZone(configuredZone);
            return zoned.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
          } else {
            // Return ISO 8601 LOCAL datetime format (no timezone)
            return dateTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
          }
        }
      } catch (DateTimeParseException e) {
        // Try next formatter
      }
    }

    // Try parsing as date only
    for (DateTimeFormatter formatter : DATE_FORMATTERS) {
      try {
        LocalDate date = LocalDate.parse(trimmed, formatter);
        // Convert to ISO 8601 date format
        return date.format(DateTimeFormatter.ISO_LOCAL_DATE);
      } catch (DateTimeParseException e) {
        // Try next formatter
      }
    }

    // Try parsing as time only
    for (DateTimeFormatter formatter : TIME_FORMATTERS) {
      try {
        LocalTime time = LocalTime.parse(trimmed, formatter);
        // Convert to ISO 8601 time format
        return time.format(DateTimeFormatter.ISO_LOCAL_TIME);
      } catch (DateTimeParseException e) {
        // Try next formatter
      }
    }

    return null;
  }

  /**
   * Checks if a string is in an obvious date format that should be parsed.
   * Used for strict parsing mode to avoid false positives.
   */
  private static boolean isObviousDateFormat(String value) {
    // Check for common date separators and patterns
    return value.matches("\\d{1,4}[-/.]\\d{1,2}[-/.]\\d{1,4}.*") ||  // Numeric dates
           value.matches(".*\\d{1,2}:\\d{2}.*") ||                    // Times
           value.matches(".*(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec).*") || // Month names
           value.matches("\\d{8}");                                    // Compact format YYYYMMDD
  }
}
