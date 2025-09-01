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
package org.apache.calcite.adapter.file.schema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Parses schema strategy configuration from model operands.
 */
public class SchemaConfigParser {
  private static final Logger LOGGER = LoggerFactory.getLogger(SchemaConfigParser.class);

  /**
   * Parses schema strategy from configuration map.
   */
  public static SchemaStrategy parseSchemaStrategy(Map<String, Object> config) {
    if (config == null || !config.containsKey("schemaStrategy")) {
      // Use default strategy
      return SchemaStrategy.PARQUET_DEFAULT;
    }

    Object strategyConfig = config.get("schemaStrategy");

    // Simple string configuration
    if (strategyConfig instanceof String) {
      return parseSimpleStrategy((String) strategyConfig);
    }

    // Complex configuration object
    if (strategyConfig instanceof Map) {
      @SuppressWarnings("unchecked")
      Map<String, Object> strategyMap = (Map<String, Object>) strategyConfig;
      return parseComplexStrategy(strategyMap);
    }

    LOGGER.warn("Invalid schemaStrategy configuration: {}, using default", strategyConfig);
    return SchemaStrategy.PARQUET_DEFAULT;
  }

  private static SchemaStrategy parseSimpleStrategy(String strategyName) {
    switch (strategyName.toLowerCase()) {
      case "default":
      case "parquet_default":
        return SchemaStrategy.PARQUET_DEFAULT;
      case "conservative":
        return SchemaStrategy.CONSERVATIVE;
      case "aggressive_union":
      case "union":
        return SchemaStrategy.AGGRESSIVE_UNION;
      case "latest_schema_wins":
      case "latest":
        return new SchemaStrategy(
            SchemaStrategy.ParquetStrategy.LATEST_SCHEMA_WINS,
            SchemaStrategy.CsvStrategy.LATEST_FILE,
            SchemaStrategy.JsonStrategy.LATEST_FILE);
      case "union_all":
        return new SchemaStrategy(
            SchemaStrategy.ParquetStrategy.UNION_ALL_COLUMNS,
            SchemaStrategy.CsvStrategy.UNION_COMMON,
            SchemaStrategy.JsonStrategy.UNION_KEYS);
      default:
        LOGGER.warn("Unknown simple strategy: {}, using default", strategyName);
        return SchemaStrategy.PARQUET_DEFAULT;
    }
  }

  @SuppressWarnings("unchecked")
  private static SchemaStrategy parseComplexStrategy(Map<String, Object> config) {
    // Parse individual format strategies
    SchemaStrategy.ParquetStrategy parquetStrategy =
        parseParquetStrategy((String) config.getOrDefault("parquet", "LATEST_SCHEMA_WINS"));

    SchemaStrategy.CsvStrategy csvStrategy =
        parseCsvStrategy((String) config.getOrDefault("csv", "RICHEST_FILE"));

    SchemaStrategy.JsonStrategy jsonStrategy =
        parseJsonStrategy((String) config.getOrDefault("json", "LATEST_FILE"));

    // Parse format priority
    List<String> formatPriority =
        parseFormatPriority((List<String>) config.get("formatPriority"));

    // Parse validation level
    SchemaStrategy.ValidationLevel validationLevel =
        parseValidationLevel((String) config.getOrDefault("validation", "WARN"));

    return new SchemaStrategy(parquetStrategy, csvStrategy, jsonStrategy,
                             formatPriority, validationLevel);
  }

  private static SchemaStrategy.ParquetStrategy parseParquetStrategy(String strategy) {
    try {
      return SchemaStrategy.ParquetStrategy.valueOf(strategy.toUpperCase());
    } catch (IllegalArgumentException e) {
      LOGGER.warn("Unknown Parquet strategy: {}, using LATEST_SCHEMA_WINS", strategy);
      return SchemaStrategy.ParquetStrategy.LATEST_SCHEMA_WINS;
    }
  }

  private static SchemaStrategy.CsvStrategy parseCsvStrategy(String strategy) {
    try {
      return SchemaStrategy.CsvStrategy.valueOf(strategy.toUpperCase());
    } catch (IllegalArgumentException e) {
      LOGGER.warn("Unknown CSV strategy: {}, using RICHEST_FILE", strategy);
      return SchemaStrategy.CsvStrategy.RICHEST_FILE;
    }
  }

  private static SchemaStrategy.JsonStrategy parseJsonStrategy(String strategy) {
    try {
      return SchemaStrategy.JsonStrategy.valueOf(strategy.toUpperCase());
    } catch (IllegalArgumentException e) {
      LOGGER.warn("Unknown JSON strategy: {}, using LATEST_FILE", strategy);
      return SchemaStrategy.JsonStrategy.LATEST_FILE;
    }
  }

  private static List<String> parseFormatPriority(List<String> priority) {
    if (priority == null || priority.isEmpty()) {
      return Arrays.asList("parquet", "csv", "json"); // Default priority
    }
    return priority;
  }

  private static SchemaStrategy.ValidationLevel parseValidationLevel(String level) {
    try {
      return SchemaStrategy.ValidationLevel.valueOf(level.toUpperCase());
    } catch (IllegalArgumentException e) {
      LOGGER.warn("Unknown validation level: {}, using WARN", level);
      return SchemaStrategy.ValidationLevel.WARN;
    }
  }
}
