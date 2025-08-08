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

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.trace.CalciteLogger;

import com.google.common.annotations.VisibleForTesting;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvValidationException;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;

import static org.apache.calcite.linq4j.Nullness.castNonNull;

import static java.lang.Boolean.parseBoolean;
import static java.lang.Byte.parseByte;
import static java.lang.Double.parseDouble;
import static java.lang.Float.parseFloat;
import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;
import static java.lang.Short.parseShort;
import static java.util.Objects.requireNonNull;

/** Enumerator that reads from a CSV file.
 *
 * @param <E> Row type
 */
public class CsvEnumerator<E> implements Enumerator<E> {
  private static final CalciteLogger LOGGER =
      new CalciteLogger(LoggerFactory.getLogger(CsvEnumerator.class));

  private final CSVReader reader;
  private final @Nullable List<@Nullable String> filterValues;
  private final AtomicBoolean cancelFlag;
  private final RowConverter<E> rowConverter;
  private @Nullable E current;
  private final SourceFileLockManager.@Nullable LockHandle lockHandle;

  private static final ThreadLocal<SimpleDateFormat> TIME_FORMAT_DATE;
  private static final ThreadLocal<SimpleDateFormat> TIME_FORMAT_TIME;
  private static final ThreadLocal<SimpleDateFormat> TIME_FORMAT_TIMESTAMP;
  private static final ThreadLocal<SimpleDateFormat> TIME_FORMAT_TIMESTAMP_UTC;
  private static final Pattern DECIMAL_TYPE_PATTERN = Pattern
      .compile("\"decimal\\(([0-9]+),([0-9]+)\\)");
  private static final Pattern TIMEZONE_PATTERN = Pattern
      .compile(".*[+-]\\d{2}:?\\d{2}$|.*Z$|.*\\s[A-Z]{3,4}$|.*,\\s*\\d+\\s+[+-]\\d{4}$|.*,\\s*\\d+\\s+[A-Z]{3,4}$");

  static {
    // For DATE and TIME types, use GMT to avoid timezone issues
    // TIME represents time-of-day without timezone
    // DATE represents a calendar date without timezone
    final TimeZone gmt = TimeZone.getTimeZone("GMT");

    TIME_FORMAT_DATE = ThreadLocal.withInitial(() -> {
      SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
      sdf.setTimeZone(gmt);
      return sdf;
    });

    TIME_FORMAT_TIME = ThreadLocal.withInitial(() -> {
      SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
      sdf.setTimeZone(gmt);
      return sdf;
    });

    // For TIMESTAMP, use local timezone for timezone-naive timestamps to match Timestamp.valueOf() behavior
    TIME_FORMAT_TIMESTAMP = ThreadLocal.withInitial(() -> {
      SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      // Always get the current default timezone when creating the formatter
      sdf.setTimeZone(TimeZone.getDefault());
      return sdf;
    });

    // For timezone-aware timestamps, use UTC
    TIME_FORMAT_TIMESTAMP_UTC = ThreadLocal.withInitial(() -> {
      SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
      return sdf;
    });
  }

  public CsvEnumerator(Source source, AtomicBoolean cancelFlag,
      List<RelDataType> fieldTypes, List<Integer> fields) {
    //noinspection unchecked
    this(source, cancelFlag, false, null,
        (RowConverter<E>) converter(fieldTypes, fields));
  }

  public CsvEnumerator(Source source, AtomicBoolean cancelFlag, boolean stream,
      @Nullable String @Nullable [] filterValues, RowConverter<E> rowConverter) {
    this.cancelFlag = cancelFlag;
    this.rowConverter = rowConverter;
    this.filterValues =
        filterValues == null ? null
            : ImmutableNullableList.copyOf(filterValues);

    // Acquire read lock on source file if it's a local file
    SourceFileLockManager.LockHandle tempLock = null;
    if (!stream && "file".equals(source.protocol())) {
      try {
        tempLock = SourceFileLockManager.acquireReadLock(source.file());
        LOGGER.debug("Acquired read lock on file: " + source.path());
      } catch (IOException e) {
        LOGGER.warn("Could not acquire lock on file: " + source.path()
            + " - proceeding without lock. Error: " + e.getMessage());
        // Continue without lock - don't fail the query
      }
    }
    this.lockHandle = tempLock;

    try {
      if (stream) {
        this.reader = new CsvStreamReader(source);
      } else {
        this.reader = openCsv(source);
      }
      String[] header = this.reader.readNext(); // skip header row
      LOGGER.debug("[CsvEnumerator] Header row read: {}", 
                  (header != null ? "length=" + header.length + ", content=" + String.join(",", header) : "null"));
    } catch (IOException | CsvValidationException e) {
      // Release lock if we failed to open the file
      if (this.lockHandle != null) {
        this.lockHandle.close();
      }
      LOGGER.warn("[CsvEnumerator] Error reading CSV: {}", e.getMessage());
      throw new RuntimeException(e);
    }
  }

  private static RowConverter<?> converter(List<RelDataType> fieldTypes,
      List<Integer> fields) {
    if (fields.size() == 1) {
      final int field = fields.get(0);
      return new SingleColumnRowConverter(fieldTypes.get(field), field);
    } else {
      return arrayConverter(fieldTypes, fields, false);
    }
  }

  public static RowConverter<@Nullable Object[]> arrayConverter(
      List<RelDataType> fieldTypes, List<Integer> fields, boolean stream) {
    return new ArrayRowConverter(fieldTypes, fields, stream);
  }

  /** Deduces the names and types of a table's columns by reading the first line
   * of a CSV file. */
  public static RelDataType deduceRowType(JavaTypeFactory typeFactory,
      Source source, @Nullable List<RelDataType> fieldTypes, Boolean stream) {
    return deduceRowType(typeFactory, source, fieldTypes, stream, "UNCHANGED");
  }

  /** Deduces the names and types of a table's columns by reading the first line
   * of a CSV file, with configurable column name casing. */
  public static RelDataType deduceRowType(JavaTypeFactory typeFactory,
      Source source, @Nullable List<RelDataType> fieldTypes, Boolean stream,
      String columnCasing) {
    final List<RelDataType> types = new ArrayList<>();
    final List<String> names = new ArrayList<>();
    if (stream) {
      names.add(FileSchemaFactory.ROWTIME_COLUMN_NAME);
      types.add(typeFactory.createSqlType(SqlTypeName.TIMESTAMP));
    }
    try (CSVReader reader = openCsv(source)) {
      String[] strings = reader.readNext();
      LOGGER.debug("[CsvEnumerator.deduceRowType] Read header: {}", 
                  (strings != null ? "length=" + strings.length + ", content=" + String.join(",", strings) : "null"));
      if (strings == null) {
        strings = new String[]{"EmptyFileHasNoColumns:boolean"};
      } else if (strings.length == 0) {
        LOGGER.warn("[CsvEnumerator.deduceRowType] ERROR: Empty header row read from source: {}", source.path());
        strings = new String[]{"EmptyFileHasNoColumns:boolean"};
      }
      for (String string : strings) {
        final String name;
        final RelDataType fieldType;
        final int colon = string.indexOf(':');
        if (colon >= 0) {
          name = string.substring(0, colon);
          String typeString = string.substring(colon + 1);
          Matcher decimalMatcher = DECIMAL_TYPE_PATTERN.matcher(typeString);
          if (decimalMatcher.matches()) {
            int precision = parseInt(decimalMatcher.group(1));
            int scale = parseInt(decimalMatcher.group(2));
            fieldType = parseDecimalSqlType(typeFactory, precision, scale);
          } else {
            switch (typeString) {
            case "string":
              fieldType = toNullableRelDataType(typeFactory, SqlTypeName.VARCHAR);
              break;
            case "boolean":
              fieldType = toNullableRelDataType(typeFactory, SqlTypeName.BOOLEAN);
              break;
            case "byte":
              fieldType = toNullableRelDataType(typeFactory, SqlTypeName.TINYINT);
              break;
            case "char":
              fieldType = toNullableRelDataType(typeFactory, SqlTypeName.CHAR);
              break;
            case "short":
              fieldType = toNullableRelDataType(typeFactory, SqlTypeName.SMALLINT);
              break;
            case "int":
              fieldType = toNullableRelDataType(typeFactory, SqlTypeName.INTEGER);
              break;
            case "long":
              fieldType = toNullableRelDataType(typeFactory, SqlTypeName.BIGINT);
              break;
            case "float":
              fieldType = toNullableRelDataType(typeFactory, SqlTypeName.REAL);
              break;
            case "double":
              fieldType = toNullableRelDataType(typeFactory, SqlTypeName.DOUBLE);
              break;
            case "date":
              fieldType = toNullableRelDataType(typeFactory, SqlTypeName.DATE);
              break;
            case "timestamp":
              // Timezone-naive timestamp (local wall-clock time)
              fieldType = toNullableRelDataType(typeFactory, SqlTypeName.TIMESTAMP);
              break;
            case "timestamptz":
              // Timezone-aware timestamp (specific instant in time)
              // Use TIMESTAMP_WITH_LOCAL_TIME_ZONE for proper timezone handling
              fieldType = toNullableRelDataType(typeFactory, SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
              break;
            case "time":
              fieldType = toNullableRelDataType(typeFactory, SqlTypeName.TIME);
              break;
            default:
              LOGGER.warn(
                  "Found unknown type: {} in file: {} for column: {}. Will assume the type of "
                      + "column is string.",
                  typeString, source.path(), name);
              fieldType = toNullableRelDataType(typeFactory, SqlTypeName.VARCHAR);
              break;
            }
          }
        } else {
          name = string;
          fieldType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
        }
        names.add(applyCasing(name, columnCasing));
        types.add(fieldType);
        if (fieldTypes != null) {
          fieldTypes.add(fieldType);
        }
      }
    } catch (IOException | CsvValidationException e) {
      // ignore
    }
    if (names.isEmpty()) {
      names.add("line");
      types.add(typeFactory.createSqlType(SqlTypeName.VARCHAR));
    }
    return typeFactory.createStructType(Pair.zip(names, types));
  }

  static CSVReader openCsv(Source source) throws IOException {
    requireNonNull(source, "source");
    LOGGER.debug("[CsvEnumerator.openCsv] Opening CSV from source: {}, protocol: {}", source.path(), source.protocol());
    
    // Check if this is a TSV file based on the file extension
    if (source.path().endsWith(".tsv")) {
      return new CSVReaderBuilder(source.reader())
          .withCSVParser(new CSVParserBuilder().withSeparator('\t').build())
          .build();
    }
    
    Reader reader = source.reader();
    LOGGER.debug("[CsvEnumerator.openCsv] Reader created: {}", (reader != null ? reader.getClass().getName() : "null"));
    
    CSVReader csvReader = new CSVReaderBuilder(reader).build();
    
    // Debug: Try to peek at the first line
    try {
      reader.mark(1000);
      char[] buffer = new char[100];
      int charsRead = reader.read(buffer);
      LOGGER.debug("[CsvEnumerator.openCsv] First {} chars: {}", charsRead, new String(buffer, 0, Math.max(0, charsRead)));
      reader.reset();
    } catch (Exception e) {
      LOGGER.debug("[CsvEnumerator.openCsv] Could not peek at reader: {}", e.getMessage());
    }
    
    return csvReader;
  }

  @Override public E current() {
    return castNonNull(current);
  }

  @Override public boolean moveNext() {
    try {
    outer:
      for (;;) {
        if (cancelFlag.get()) {
          return false;
        }
        final String[] strings = reader.readNext();
        if (strings == null) {
          if (reader instanceof CsvStreamReader) {
            try {
              Thread.sleep(CsvStreamReader.DEFAULT_MONITOR_DELAY);
            } catch (InterruptedException e) {
              throw new RuntimeException(e);
            }
            continue;
          }
          current = null;
          reader.close();
          return false;
        }
        if (filterValues != null) {
          for (int i = 0; i < strings.length; i++) {
            String filterValue = filterValues.get(i);
            if (filterValue != null) {
              if (!filterValue.equals(strings[i])) {
                continue outer;
              }
            }
          }
        }
        current = rowConverter.convertRow(strings);
        return true;
      }
    } catch (IOException | CsvValidationException e) {
      throw new RuntimeException(e);
    }
  }

  @Override public void reset() {
    throw new UnsupportedOperationException();
  }

  @Override public void close() {
    try {
      reader.close();
    } catch (IOException e) {
      throw new RuntimeException("Error closing CSV reader", e);
    } finally {
      // Release the file lock
      if (lockHandle != null) {
        lockHandle.close();
        LOGGER.debug("Released read lock on file");
      }
    }
  }

  /** Returns an array of integers {0, ..., n - 1}. */
  public static int[] identityList(int n) {
    int[] integers = new int[n];
    for (int i = 0; i < n; i++) {
      integers[i] = i;
    }
    return integers;
  }

  private static RelDataType toNullableRelDataType(JavaTypeFactory typeFactory,
      SqlTypeName sqlTypeName) {
    return typeFactory.createTypeWithNullability(typeFactory.createSqlType(sqlTypeName), true);
  }

  /** Applies the configured casing transformation to a column name. */
  private static String applyCasing(String name, String casing) {
    if (name == null) {
      return null;
    }
    switch (casing) {
    case "UPPER":
      return name.toUpperCase(Locale.ROOT);
    case "LOWER":
      return name.toLowerCase(Locale.ROOT);
    case "UNCHANGED":
    default:
      return name;
    }
  }

  /** Row converter.
   *
   * @param <E> element type */
  abstract static class RowConverter<E> {
    abstract E convertRow(@Nullable String[] rows);

    @SuppressWarnings("JavaUtilDate")
    protected @Nullable Object convert(@Nullable RelDataType fieldType, @Nullable String string) {
      if (fieldType == null || string == null) {
        return string;
      }
      switch (fieldType.getSqlTypeName()) {
      case BOOLEAN:
        if (string.length() == 0) {
          return null;
        }
        return parseBoolean(string);
      case TINYINT:
        if (string.length() == 0) {
          return null;
        }
        return parseByte(string);
      case SMALLINT:
        if (string.length() == 0) {
          return null;
        }
        return parseShort(string);
      case INTEGER:
        if (string.length() == 0) {
          return null;
        }
        return parseInt(string);
      case BIGINT:
        if (string.length() == 0) {
          return null;
        }
        return parseLong(string);
      case REAL:
        if (string.length() == 0) {
          return null;
        }
        return parseFloat(string);
      case FLOAT:
      case DOUBLE:
        if (string.length() == 0) {
          return null;
        }
        return parseDouble(string);
      case DECIMAL:
        if (string.length() == 0) {
          return null;
        }
        return parseDecimal(fieldType.getPrecision(), fieldType.getScale(), string);
      case DATE:
        if (string == null || string.length() == 0) {
          return null;
        }
        try {
          // Try multiple date formats
          String[] dateFormats = {
              "yyyy-MM-dd",    // 2024-03-15
              "yyyy/MM/dd",    // 2024/03/15
              "yyyy.MM.dd",    // 2024.03.15
              "MM/dd/yyyy",    // 03/15/2024
              "MM-dd-yyyy",    // 03-15-2024
              "dd/MM/yyyy",    // 15/03/2024
              "dd-MM-yyyy",    // 15-03-2024
              "dd.MM.yyyy"     // 15.03.2024
          };

          Date date = null;
          ParseException lastException = null;

          for (String format : dateFormats) {
            try {
              SimpleDateFormat sdf = new SimpleDateFormat(format);
              sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
              sdf.setLenient(false); // Strict date parsing
              date = sdf.parse(string);
              break; // Success, exit loop
            } catch (ParseException e) {
              lastException = e;
              // Try next format
            }
          }

          if (date == null) {
            // Try the default format as last resort
            date = TIME_FORMAT_DATE.get().parse(string);
          }

          // Return Integer (not int) to support null values
          // Try to parse as LocalDate first for common ISO format
          if (string.matches("\\d{4}-\\d{2}-\\d{2}")) {
            java.time.LocalDate localDate = java.time.LocalDate.parse(string);
            return Integer.valueOf((int) localDate.toEpochDay());
          }
          // For other formats, use the parsed date but convert carefully
          // The date was parsed in GMT timezone, so convert directly
          long millis = date.getTime();
          // Use Math.floorDiv for proper handling of negative values (dates before 1970)
          int daysSinceEpoch = Math.toIntExact(Math.floorDiv(millis, DateTimeUtils.MILLIS_PER_DAY));
          return Integer.valueOf(daysSinceEpoch);
        } catch (ParseException e) {
          return null;
        }
      case TIME:
        if (string == null || string.length() == 0) {
          return null;
        }
        try {
          // Try multiple time formats
          String[] timeFormats = {
              "HH:mm:ss.SSS",  // 10:30:45.123
              "HH:mm:ss",      // 10:30:45
              "HH:mm",         // 10:30
              "H:mm:ss",       // 9:30:45 (single digit hour)
              "H:mm"           // 9:30 (single digit hour)
          };

          Date date = null;

          for (String format : timeFormats) {
            try {
              SimpleDateFormat sdf = new SimpleDateFormat(format);
              sdf.setTimeZone(TimeZone.getTimeZone("GMT")); // TIME is timezone-naive
              sdf.setLenient(false); // Strict time parsing
              date = sdf.parse(string);
              break; // Success, exit loop
            } catch (ParseException e) {
              // Try next format
            }
          }

          if (date == null) {
            // Try the default format as last resort
            date = TIME_FORMAT_TIME.get().parse(string);
          }

          // Return Integer (not int) to support null values
          return Integer.valueOf((int) date.getTime());
        } catch (ParseException e) {
          return null;
        }
      case TIMESTAMP:
        // TIMESTAMP WITHOUT TIME ZONE - must not have timezone info
        if (string == null || string.length() == 0) {
          return null;
        }
        try {
          if (hasTimezoneInfo(string)) {
            throw new IllegalArgumentException(
                "TIMESTAMP column cannot contain timezone information. " +
                "Use TIMESTAMPTZ type for timezone-aware timestamps. " +
                "Invalid value: '" + string + "'");
          }

          // Parse as local timestamp (no timezone conversion)
          String[] localFormats = {
              "yyyy-MM-dd HH:mm:ss",         // 2024-03-15 10:30:45
              "yyyy-MM-dd'T'HH:mm:ss",       // 2024-03-15T10:30:45
              "yyyy-MM-dd HH:mm:ss.SSS",     // 2024-03-15 10:30:45.123
              "yyyy-MM-dd'T'HH:mm:ss.SSS",   // 2024-03-15T10:30:45.123
              "yyyy/MM/dd HH:mm:ss",         // 2024/03/15 10:30:45
              "MM/dd/yyyy HH:mm:ss",         // 03/15/2024 10:30:45
              "dd/MM/yyyy HH:mm:ss",         // 15/03/2024 10:30:45
              "yyyy-MM-dd"                   // 2024-03-15 (date only, assumes 00:00:00)
          };

          for (String format : localFormats) {
            try {
              SimpleDateFormat sdf = new SimpleDateFormat(format);
              sdf.setTimeZone(TimeZone.getDefault()); // Parse as local time
              sdf.setLenient(false); // Strict parsing
              Date date = sdf.parse(string);
              // Return as Long for timezone-naive timestamp
              // This represents the local time, not UTC
              return date.getTime();
            } catch (ParseException ignored) {
              // Try next format
            }
          }

          // Last resort: try the standard format
          SimpleDateFormat sdf = TIME_FORMAT_TIMESTAMP.get();
          Date date = sdf.parse(string);
          return date.getTime();
        } catch (ParseException e) {
          return null;
        } catch (IllegalArgumentException e) {
          // Re-throw to propagate validation errors
          throw new RuntimeException(e);
        }

      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        // TIMESTAMPTZ - must have timezone info
        if (string == null || string.length() == 0) {
          return null;
        }
        try {
          if (!hasTimezoneInfo(string)) {
            throw new IllegalArgumentException(
                "TIMESTAMPTZ column must contain timezone information. " +
                "Use TIMESTAMP type for timezone-naive timestamps. " +
                "Invalid value: '" + string + "'");
          }

          // Parse timestamp with timezone and convert to UTC
          String[] tzFormats = {
              // Basic formats with timezone
              "yyyy-MM-dd HH:mm:ss z",          // 2024-03-15 10:30:45 EST
              "yyyy-MM-dd HH:mm:ss Z",          // 2024-03-15 10:30:45 +0530
              "yyyy-MM-dd HH:mm:ss XXX",        // 2024-03-15 10:30:45 +05:30
              "yyyy-MM-dd HH:mm:ssZ",           // 2024-03-15 10:30:45+0530
              "yyyy-MM-dd HH:mm:ssXXX",         // 2024-03-15 10:30:45+05:30

              // ISO 8601 formats
              "yyyy-MM-dd'T'HH:mm:ssXXX",       // 2024-03-15T10:30:45+05:30
              "yyyy-MM-dd'T'HH:mm:ssZ",         // 2024-03-15T10:30:45+0530
              "yyyy-MM-dd'T'HH:mm:ssX",         // 2024-03-15T10:30:45Z
              "yyyy-MM-dd'T'HH:mm:ss.SSSXXX",   // 2024-03-15T10:30:45.123+05:30
              "yyyy-MM-dd'T'HH:mm:ss.SSSX",     // 2024-03-15T10:30:45.123Z

              // RFC 2822 format
              "EEE, dd MMM yyyy HH:mm:ss Z",    // Fri, 15 Mar 2024 10:30:45 +0000
              "EEE, dd MMM yyyy HH:mm:ss z",    // Fri, 15 Mar 2024 10:30:45 EST

              // With milliseconds
              "yyyy-MM-dd HH:mm:ss.SSS Z",      // 2024-03-15 10:30:45.123 +0530
              "yyyy-MM-dd HH:mm:ss.SSS z",      // 2024-03-15 10:30:45.123 EST
              "yyyy-MM-dd HH:mm:ss.SSSXXX"      // 2024-03-15 10:30:45.123+05:30
          };

          for (String format : tzFormats) {
            try {
              SimpleDateFormat sdf = new SimpleDateFormat(format);
              Date date = sdf.parse(string);
              // Return UTC timestamp wrapped in UtcTimestamp for consistent display
              return new UtcTimestamp(date.getTime());
            } catch (ParseException ignored) {
              // Try next format
            }
          }

          throw new IllegalArgumentException(
              "Unable to parse TIMESTAMPTZ value: '" + string + "'. " +
              "Expected format with timezone (e.g., '2024-03-15T10:30:45Z' or '2024-03-15 10:30:45 EST')");

        } catch (IllegalArgumentException e) {
          // Re-throw to propagate validation errors
          throw new RuntimeException(e);
        }

      case VARCHAR:
      default:
        return string;
      }
    }
  }

  private static RelDataType parseDecimalSqlType(JavaTypeFactory typeFactory, int precision,
      int scale) {
    checkArgument(precision > 0, "DECIMAL type must have precision > 0. Found %s", precision);
    checkArgument(scale >= 0, "DECIMAL type must have scale >= 0. Found %s", scale);
    checkArgument(precision >= scale,
        "DECIMAL type must have precision >= scale. Found precision (%s) and scale (%s).",
        precision, scale);
    return typeFactory.createTypeWithNullability(
        typeFactory.createSqlType(SqlTypeName.DECIMAL, precision, scale), true);
  }

  /**
   * Detects if a timestamp string contains timezone information.
   * This method checks for common timezone formats:
   * - ISO 8601 with offset: +05:30, -08:00, +0530, -0800
   * - UTC indicator: Z
   * - Named timezones: EST, PST, GMT, etc.
   */
  private static boolean hasTimezoneInfo(String timestampStr) {
    return TIMEZONE_PATTERN.matcher(timestampStr).matches();
  }

  @VisibleForTesting
  protected static BigDecimal parseDecimal(int precision, int scale, String string) {
    BigDecimal result = new BigDecimal(string);
    // If the parsed value has more fractional digits than the specified scale, round ties away
    // from 0.
    if (result.scale() > scale) {
      LOGGER.warn(
          "Decimal value {} exceeds declared scale ({}). Performing rounding to keep the "
              + "first {} fractional digits.",
          result, scale, scale);
      result = result.setScale(scale, RoundingMode.HALF_UP);
    }
    // Throws an exception if the parsed value has more digits to the left of the decimal point
    // than the specified value.
    if (result.precision() - result.scale() > precision - scale) {
      throw new IllegalArgumentException(String
          .format(Locale.ROOT, "Decimal value %s exceeds declared precision (%d) and scale (%d).",
              result, precision, scale));
    }
    return result;
  }

  /** Array row converter. */
  static class ArrayRowConverter extends RowConverter<@Nullable Object[]> {

    /** Field types. List must not be null, but any element may be null. */
    private final List<RelDataType> fieldTypes;
    private final ImmutableIntList fields;
    /** Whether the row to convert is from a stream. */
    private final boolean stream;

    ArrayRowConverter(List<RelDataType> fieldTypes, List<Integer> fields,
        boolean stream) {
      this.fieldTypes = ImmutableNullableList.copyOf(fieldTypes);
      this.fields = ImmutableIntList.copyOf(fields);
      this.stream = stream;
    }

    @Override public @Nullable Object[] convertRow(@Nullable String[] strings) {
      if (stream) {
        return convertStreamRow(strings);
      } else {
        return convertNormalRow(strings);
      }
    }

    public @Nullable Object[] convertNormalRow(@Nullable String[] strings) {
      if (strings == null) {
        LOGGER.debug("[ArrayRowConverter] Received null strings array");
        return null;
      }
      LOGGER.debug("[ArrayRowConverter] Converting row with {} columns, need fields: {}", strings.length, fields);
      
      final @Nullable Object[] objects = new Object[fields.size()];
      for (int i = 0; i < fields.size(); i++) {
        int field = fields.get(i);
        if (field >= strings.length) {
          LOGGER.warn("[ArrayRowConverter] ERROR: Field index {} out of bounds for strings array of length {}", field, strings.length);
          LOGGER.warn("[ArrayRowConverter] Fields list: {}", fields);
          LOGGER.warn("[ArrayRowConverter] FieldTypes: {}", fieldTypes);
          LOGGER.warn("[ArrayRowConverter] Row data: {}", java.util.Arrays.toString(strings));
          throw new ArrayIndexOutOfBoundsException("Index " + field + " out of bounds for length " + strings.length);
        }
        objects[i] = convert(fieldTypes.get(field), strings[field]);
      }
      return objects;
    }

    public @Nullable Object[] convertStreamRow(@Nullable String[] strings) {
      final @Nullable Object[] objects = new Object[fields.size() + 1];
      objects[0] = System.currentTimeMillis();
      for (int i = 0; i < fields.size(); i++) {
        int field = fields.get(i);
        objects[i + 1] = convert(fieldTypes.get(field), strings[field]);
      }
      return objects;
    }
  }

  /** Single column row converter. */
  private static class SingleColumnRowConverter extends RowConverter<Object> {
    private final RelDataType fieldType;
    private final int fieldIndex;

    private SingleColumnRowConverter(RelDataType fieldType, int fieldIndex) {
      this.fieldType = fieldType;
      this.fieldIndex = fieldIndex;
    }

    @Override public @Nullable Object convertRow(@Nullable String[] strings) {
      return convert(fieldType, strings[fieldIndex]);
    }
  }
}
