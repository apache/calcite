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

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Source;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvValidationException;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Reader;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Infers column types from CSV data by sampling rows.
 */
public class CsvTypeInferrer {
  private static final Logger LOGGER = LoggerFactory.getLogger(CsvTypeInferrer.class);
  
  // Common date/time formats to try
  private static final DateTimeFormatter[] DATE_FORMATTERS = {
      DateTimeFormatter.ISO_LOCAL_DATE,
      DateTimeFormatter.ofPattern("yyyy-MM-dd"),
      DateTimeFormatter.ofPattern("M/d/yyyy"),
      DateTimeFormatter.ofPattern("MM/dd/yyyy"),
      DateTimeFormatter.ofPattern("d/M/yyyy"),
      DateTimeFormatter.ofPattern("dd/MM/yyyy"),
      DateTimeFormatter.ofPattern("yyyy/MM/dd")
  };
  
  private static final DateTimeFormatter[] TIME_FORMATTERS = {
      DateTimeFormatter.ISO_LOCAL_TIME,
      DateTimeFormatter.ofPattern("HH:mm:ss"),
      DateTimeFormatter.ofPattern("HH:mm:ss.SSS"),
      DateTimeFormatter.ofPattern("h:mm:ss a"),
      DateTimeFormatter.ofPattern("h:mm a")
  };
  
  private static final DateTimeFormatter[] DATETIME_FORMATTERS = {
      DateTimeFormatter.ISO_LOCAL_DATE_TIME,
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"),
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"),
      DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm:ss"),
      DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss")
  };
  
  // RFC formatted timestamps with timezone
  private static final DateTimeFormatter[] RFC_DATETIME_FORMATTERS = {
      DateTimeFormatter.RFC_1123_DATE_TIME,
      DateTimeFormatter.ISO_OFFSET_DATE_TIME,
      DateTimeFormatter.ISO_ZONED_DATE_TIME,
      new DateTimeFormatterBuilder()
          .appendPattern("yyyy-MM-dd'T'HH:mm:ss")
          .appendOffset("+HH:MM", "Z")
          .toFormatter()
  };
  
  private static final Pattern INTEGER_PATTERN = Pattern.compile("^-?\\d+$");
  private static final Pattern FLOAT_PATTERN = Pattern.compile("^-?\\d*\\.\\d+([eE][+-]?\\d+)?$");
  private static final Pattern BOOLEAN_PATTERN = Pattern.compile("^(true|false|TRUE|FALSE|True|False|0|1)$");
  
  /**
   * Configuration for type inference.
   */
  public static class TypeInferenceConfig {
    private final boolean enabled;
    private final double samplingRate;
    private final int maxSampleRows;
    private final double confidenceThreshold;
    private final boolean inferDates;
    private final boolean inferTimes;
    private final boolean inferTimestamps;
    private final boolean makeAllNullable;
    private final double nullableThreshold;
    
    public TypeInferenceConfig(boolean enabled, double samplingRate, int maxSampleRows,
        double confidenceThreshold, boolean inferDates, boolean inferTimes, boolean inferTimestamps,
        boolean makeAllNullable, double nullableThreshold) {
      this.enabled = enabled;
      this.samplingRate = Math.max(0.0, Math.min(1.0, samplingRate));
      this.maxSampleRows = Math.max(1, maxSampleRows);
      this.confidenceThreshold = Math.max(0.0, Math.min(1.0, confidenceThreshold));
      this.inferDates = inferDates;
      this.inferTimes = inferTimes;
      this.inferTimestamps = inferTimestamps;
      this.makeAllNullable = makeAllNullable;
      this.nullableThreshold = Math.max(0.0, Math.min(1.0, nullableThreshold));
    }
    
    /**
     * Returns default configuration with safe defaults:
     * - Type inference enabled
     * - 10% sampling rate
     * - Max 1000 rows sampled
     * - 95% confidence threshold
     * - All temporal types inferred
     * - All types nullable (safe default)
     */
    public static TypeInferenceConfig defaultConfig() {
      return new TypeInferenceConfig(true, 0.1, 1000, 0.95, true, true, true, true, 0.0);
    }
    
    /**
     * Returns a disabled configuration (no type inference).
     */
    public static TypeInferenceConfig disabled() {
      return new TypeInferenceConfig(false, 0, 0, 0, false, false, false, false, 0);
    }
    
    /**
     * Creates a configuration from a map (typically from model.json).
     */
    public static TypeInferenceConfig fromMap(@Nullable Map<String, Object> config) {
      if (config == null || !Boolean.TRUE.equals(config.get("enabled"))) {
        return disabled();
      }
      
      boolean enabled = true;
      double samplingRate = getDouble(config, "samplingRate", 0.1);
      int maxSampleRows = getInt(config, "maxSampleRows", 1000);
      double confidenceThreshold = getDouble(config, "confidenceThreshold", 0.95);
      boolean inferDates = getBoolean(config, "inferDates", true);
      boolean inferTimes = getBoolean(config, "inferTimes", true);
      boolean inferTimestamps = getBoolean(config, "inferTimestamps", true);
      boolean makeAllNullable = getBoolean(config, "makeAllNullable", true);
      double nullableThreshold = getDouble(config, "nullableThreshold", 0.0);
      
      return new TypeInferenceConfig(enabled, samplingRate, maxSampleRows,
          confidenceThreshold, inferDates, inferTimes, inferTimestamps,
          makeAllNullable, nullableThreshold);
    }
    
    private static boolean getBoolean(Map<String, Object> map, String key, boolean defaultValue) {
      Object value = map.get(key);
      return value instanceof Boolean ? (Boolean) value : defaultValue;
    }
    
    private static double getDouble(Map<String, Object> map, String key, double defaultValue) {
      Object value = map.get(key);
      return value instanceof Number ? ((Number) value).doubleValue() : defaultValue;
    }
    
    private static int getInt(Map<String, Object> map, String key, int defaultValue) {
      Object value = map.get(key);
      return value instanceof Number ? ((Number) value).intValue() : defaultValue;
    }
    
    public boolean isEnabled() { return enabled; }
    public double getSamplingRate() { return samplingRate; }
    public int getMaxSampleRows() { return maxSampleRows; }
    public double getConfidenceThreshold() { return confidenceThreshold; }
    public boolean isInferDates() { return inferDates; }
    public boolean isInferTimes() { return inferTimes; }
    public boolean isInferTimestamps() { return inferTimestamps; }
    public boolean isMakeAllNullable() { return makeAllNullable; }
    public double getNullableThreshold() { return nullableThreshold; }
  }
  
  /**
   * Result of type inference for a column.
   */
  public static class ColumnTypeInfo {
    public final String columnName;
    public final SqlTypeName inferredType;
    public final boolean nullable;
    public final @Nullable DateTimeFormatter dateTimeFormatter;
    public final double confidence;
    public final int sampledRows;
    public final int nullCount;
    public final double nullRatio;
    
    public ColumnTypeInfo(String columnName, SqlTypeName inferredType, boolean nullable,
        @Nullable DateTimeFormatter dateTimeFormatter, double confidence, 
        int sampledRows, int nullCount) {
      this.columnName = columnName;
      this.inferredType = inferredType;
      this.nullable = nullable;
      this.dateTimeFormatter = dateTimeFormatter;
      this.confidence = confidence;
      this.sampledRows = sampledRows;
      this.nullCount = nullCount;
      this.nullRatio = sampledRows > 0 ? (double) nullCount / sampledRows : 0.0;
    }
  }
  
  /**
   * Infers column types from a CSV source.
   */
  public static List<ColumnTypeInfo> inferTypes(Source source, TypeInferenceConfig config,
      String columnCasing) throws IOException, CsvValidationException {
    if (!config.enabled) {
      return new ArrayList<>();
    }
    
    List<ColumnTypeInfo> results = new ArrayList<>();
    
    try (Reader reader = source.reader();
         CSVReader csvReader = createCsvReader(reader)) {
      
      // Read header
      String[] header = csvReader.readNext();
      if (header == null || header.length == 0) {
        return results;
      }
      
      // Initialize column type tracking
      int numColumns = header.length;
      List<ColumnTypeTracker> trackers = new ArrayList<>();
      for (int i = 0; i < numColumns; i++) {
        trackers.add(new ColumnTypeTracker(header[i], config));
      }
      
      // Sample rows
      String[] row;
      int rowCount = 0;
      int sampledCount = 0;
      
      while ((row = csvReader.readNext()) != null && sampledCount < config.maxSampleRows) {
        rowCount++;
        
        // Apply sampling rate
        if (config.samplingRate < 1.0 && Math.random() > config.samplingRate) {
          continue;
        }
        
        sampledCount++;
        
        // Analyze each column value
        for (int i = 0; i < Math.min(row.length, numColumns); i++) {
          trackers.get(i).analyzeValue(row[i]);
        }
      }
      
      // Determine final types based on analysis
      for (ColumnTypeTracker tracker : trackers) {
        ColumnTypeInfo typeInfo = tracker.determineType(config);
        results.add(typeInfo);
      }
      
      LOGGER.info("Type inference complete: sampled {} of {} rows", sampledCount, rowCount);
      for (ColumnTypeInfo info : results) {
        LOGGER.debug("Column '{}': {} (nullable={}, confidence={:.2f}, nulls={}/{})", 
            info.columnName, info.inferredType, info.nullable, 
            info.confidence, info.nullCount, info.sampledRows);
      }
    }
    
    return results;
  }
  
  /**
   * Tracks type information for a single column during inference.
   */
  private static class ColumnTypeTracker {
    private final String columnName;
    private final TypeInferenceConfig config;
    private final Map<SqlTypeName, Integer> typeCounts = new HashMap<>();
    private final Map<SqlTypeName, DateTimeFormatter> dateTimeFormatters = new HashMap<>();
    private int totalValues = 0;
    private int nullValues = 0;
    private int emptyStringValues = 0;
    
    ColumnTypeTracker(String columnName, TypeInferenceConfig config) {
      this.columnName = columnName;
      this.config = config;
    }
    
    void analyzeValue(@Nullable String value) {
      totalValues++;
      
      // Track nulls and empty strings separately
      if (value == null) {
        nullValues++;
        return;
      }
      
      value = value.trim();
      
      if (value.isEmpty()) {
        emptyStringValues++;
        nullValues++; // Treat empty strings as nulls for type inference
        return;
      }
      
      // Check for common null representations
      if (isNullRepresentation(value)) {
        nullValues++;
        return;
      }
      
      // Try integer first (before boolean, since "0" and "1" could be either)
      if (INTEGER_PATTERN.matcher(value).matches()) {
        try {
          long longVal = Long.parseLong(value);
          if (longVal >= Integer.MIN_VALUE && longVal <= Integer.MAX_VALUE) {
            incrementType(SqlTypeName.INTEGER);
          } else {
            incrementType(SqlTypeName.BIGINT);
          }
          return;
        } catch (NumberFormatException e) {
          // Fall through
        }
      }
      
      // Try float/double
      if (FLOAT_PATTERN.matcher(value).matches()) {
        try {
          Double.parseDouble(value);
          incrementType(SqlTypeName.DOUBLE);
          return;
        } catch (NumberFormatException e) {
          // Fall through
        }
      }
      
      // Try to parse as boolean (after numeric types, since "0" and "1" are ambiguous)
      if (BOOLEAN_PATTERN.matcher(value).matches()) {
        incrementType(SqlTypeName.BOOLEAN);
        return;
      }
      
      // Try temporal types
      if (config.inferTimestamps && tryParseDateTime(value)) {
        return;
      }
      
      if (config.inferDates && tryParseDate(value)) {
        return;
      }
      
      if (config.inferTimes && tryParseTime(value)) {
        return;
      }
      
      // Default to VARCHAR
      incrementType(SqlTypeName.VARCHAR);
    }
    
    private boolean isNullRepresentation(String value) {
      String upper = value.toUpperCase(Locale.ROOT);
      return upper.equals("NULL") || upper.equals("NA") || upper.equals("N/A") 
          || upper.equals("NONE") || upper.equals("NIL");
    }
    
    private boolean tryParseDate(String value) {
      for (DateTimeFormatter formatter : DATE_FORMATTERS) {
        try {
          LocalDate.parse(value, formatter);
          incrementType(SqlTypeName.DATE);
          dateTimeFormatters.putIfAbsent(SqlTypeName.DATE, formatter);
          return true;
        } catch (DateTimeParseException e) {
          // Try next formatter
        }
      }
      return false;
    }
    
    private boolean tryParseTime(String value) {
      for (DateTimeFormatter formatter : TIME_FORMATTERS) {
        try {
          LocalTime.parse(value, formatter);
          incrementType(SqlTypeName.TIME);
          dateTimeFormatters.putIfAbsent(SqlTypeName.TIME, formatter);
          return true;
        } catch (DateTimeParseException e) {
          // Try next formatter
        }
      }
      return false;
    }
    
    private boolean tryParseDateTime(String value) {
      // Try RFC formatted timestamps first (with timezone)
      for (DateTimeFormatter formatter : RFC_DATETIME_FORMATTERS) {
        try {
          formatter.parse(value);
          incrementType(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
          dateTimeFormatters.putIfAbsent(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, formatter);
          return true;
        } catch (DateTimeParseException e) {
          // Try next formatter
        }
      }
      
      // Try regular timestamps (without timezone)
      for (DateTimeFormatter formatter : DATETIME_FORMATTERS) {
        try {
          LocalDateTime.parse(value, formatter);
          incrementType(SqlTypeName.TIMESTAMP);
          dateTimeFormatters.putIfAbsent(SqlTypeName.TIMESTAMP, formatter);
          return true;
        } catch (DateTimeParseException e) {
          // Try next formatter
        }
      }
      
      return false;
    }
    
    private void incrementType(SqlTypeName type) {
      typeCounts.merge(type, 1, Integer::sum);
    }
    
    ColumnTypeInfo determineType(TypeInferenceConfig config) {
      if (totalValues == 0 || totalValues == nullValues) {
        // All nulls or no data - default to nullable VARCHAR
        return new ColumnTypeInfo(columnName, SqlTypeName.VARCHAR, true, null, 0.0, totalValues, nullValues);
      }
      
      int nonNullValues = totalValues - nullValues;
      
      // Find the most common type
      SqlTypeName bestType = SqlTypeName.VARCHAR;
      int bestCount = 0;
      
      for (Map.Entry<SqlTypeName, Integer> entry : typeCounts.entrySet()) {
        if (entry.getValue() > bestCount) {
          bestCount = entry.getValue();
          bestType = entry.getKey();
        }
      }
      
      double confidence = (double) bestCount / nonNullValues;
      
      // Only use inferred type if confidence is high enough
      if (confidence < config.confidenceThreshold) {
        bestType = SqlTypeName.VARCHAR;
        confidence = 1.0; // VARCHAR is always safe
      }
      
      // Determine nullability (default to true for safety)
      boolean nullable = config.makeAllNullable;
      if (!nullable) {
        // Check if we should make nullable based on null ratio
        double nullRatio = (double) nullValues / totalValues;
        nullable = nullRatio > config.nullableThreshold;
      }
      
      DateTimeFormatter formatter = dateTimeFormatters.get(bestType);
      
      return new ColumnTypeInfo(columnName, bestType, nullable, formatter, 
          confidence, totalValues, nullValues);
    }
  }
  
  private static CSVReader createCsvReader(Reader reader) {
    CSVParser parser = new CSVParserBuilder()
        .withSeparator(',')
        .build();
    
    return new CSVReaderBuilder(reader)
        .withCSVParser(parser)
        .build();
  }
  
  /**
   * Creates RelDataTypes from type inference results.
   */
  public static List<RelDataType> createRelDataTypes(JavaTypeFactory typeFactory,
      List<ColumnTypeInfo> typeInfos) {
    List<RelDataType> types = new ArrayList<>();
    
    for (ColumnTypeInfo info : typeInfos) {
      RelDataType type = typeFactory.createSqlType(info.inferredType);
      // Apply nullability from the type info
      type = typeFactory.createTypeWithNullability(type, info.nullable);
      types.add(type);
    }
    
    return types;
  }
}