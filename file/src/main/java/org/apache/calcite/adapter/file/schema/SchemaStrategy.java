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

import java.util.Arrays;
import java.util.List;

/**
 * Configuration for schema resolution strategies across different file formats.
 */
public class SchemaStrategy {

  // Default strategies by format
  public static final SchemaStrategy PARQUET_DEFAULT =
      new SchemaStrategy(ParquetStrategy.LATEST_SCHEMA_WINS,
      CsvStrategy.RICHEST_FILE,
      JsonStrategy.LATEST_FILE);

  public static final SchemaStrategy CONSERVATIVE =
      new SchemaStrategy(ParquetStrategy.LATEST_FILE,
      CsvStrategy.RICHEST_FILE,
      JsonStrategy.LATEST_FILE);

  public static final SchemaStrategy AGGRESSIVE_UNION =
      new SchemaStrategy(ParquetStrategy.UNION_ALL_COLUMNS,
      CsvStrategy.UNION_COMMON,
      JsonStrategy.UNION_KEYS);

  private final ParquetStrategy parquetStrategy;
  private final CsvStrategy csvStrategy;
  private final JsonStrategy jsonStrategy;
  private final List<String> formatPriority;
  private final ValidationLevel validationLevel;

  public SchemaStrategy(ParquetStrategy parquetStrategy,
                       CsvStrategy csvStrategy,
                       JsonStrategy jsonStrategy) {
    this(parquetStrategy, csvStrategy, jsonStrategy,
         Arrays.asList("parquet", "csv", "json"), ValidationLevel.WARN);
  }

  public SchemaStrategy(ParquetStrategy parquetStrategy,
                       CsvStrategy csvStrategy,
                       JsonStrategy jsonStrategy,
                       List<String> formatPriority,
                       ValidationLevel validationLevel) {
    this.parquetStrategy = parquetStrategy;
    this.csvStrategy = csvStrategy;
    this.jsonStrategy = jsonStrategy;
    this.formatPriority = formatPriority;
    this.validationLevel = validationLevel;
  }

  // Getters
  public ParquetStrategy getParquetStrategy() { return parquetStrategy; }
  public CsvStrategy getCsvStrategy() { return csvStrategy; }
  public JsonStrategy getJsonStrategy() { return jsonStrategy; }
  public List<String> getFormatPriority() { return formatPriority; }
  public ValidationLevel getValidationLevel() { return validationLevel; }

  /**
   * Strategies for Parquet file schema resolution.
   */
  public enum ParquetStrategy {
    /**
     * Latest schema wins - use the most recent file's schema as the unified schema.
     * This is the DEFAULT and RECOMMENDED strategy for Parquet.
     *
     * Example:
     * - file1.parquet (2024-01-01): [id INT, name VARCHAR, amount INT, old_field VARCHAR]
     * - file2.parquet (2024-01-02): [id INT, name VARCHAR, amount DECIMAL(10,2), discount DECIMAL(5,2)]
     * - Result: [id INT, name VARCHAR, amount DECIMAL(10,2), discount DECIMAL(5,2)]  # Latest schema
     *
     * Behavior:
     * - Uses schema from most recent file (latest schema wins)
     * - Deleted fields: old_field is REMOVED (not in latest schema)
     * - New fields: discount is ADDED
     * - Type evolution: amount becomes DECIMAL (latest type)
     * - Older files get NULL for new columns (discount)
     * - Older files lose deleted columns (old_field)
     *
     * Benefits:
     * - Clean schema evolution (follows latest structure)
     * - Handles field deletion properly
     * - Uses latest type definitions
     * - Follows Parquet/Iceberg patterns
     */
    LATEST_SCHEMA_WINS,

    /**
     * Union ALL columns from all files, preserving all fields ever seen.
     * Uses latest type definition for each column, never removes fields.
     *
     * Example:
     * - file1.parquet (2024-01-01): [id INT, name VARCHAR, amount INT, old_field VARCHAR]
     * - file2.parquet (2024-01-02): [id INT, name VARCHAR, amount DECIMAL(10,2), discount DECIMAL(5,2)]
     * - Result: [id INT, name VARCHAR, amount DECIMAL(10,2), discount DECIMAL(5,2), old_field VARCHAR]
     *
     * Behavior:
     * - Includes ALL columns from ALL files (true union)
     * - Never removes fields (old_field is preserved)
     * - Uses latest type for each column (amount becomes DECIMAL)
     * - Files get NULL for missing columns
     *
     * Use case: When you need access to historical fields
     */
    UNION_ALL_COLUMNS,

    /**
     * Union all columns but promote conflicting types using SQL standard promotion rules.
     *
     * Example:
     * - file1: [amount INT]
     * - file2: [amount BIGINT]
     * - Result: [amount BIGINT] (INT promoted to BIGINT)
     */
    UNION_WITH_PROMOTION,

    /**
     * Use schema from the most recently modified Parquet file.
     * Conservative approach - predictable but may miss columns from older files.
     */
    LATEST_FILE,

    /**
     * Use schema from the first Parquet file encountered (original behavior).
     * Not recommended as it's filesystem-order dependent.
     */
    FIRST_FILE,

    /**
     * Only include columns that exist in ALL Parquet files with compatible types.
     * Most restrictive - guarantees all files can be read but may lose columns.
     */
    INTERSECTION_ONLY
  }

  /**
   * Strategies for CSV file schema resolution.
   */
  public enum CsvStrategy {
    /**
     * Use the CSV file with the most columns as the schema authority.
     * This is the DEFAULT strategy for CSV files.
     *
     * Rationale: CSV schema is inferred, so the file with the most columns
     * likely represents the most complete schema.
     */
    RICHEST_FILE,

    /**
     * Use the most recently modified CSV file.
     */
    LATEST_FILE,

    /**
     * Union common columns across all CSV files.
     * Only includes columns present in all files.
     */
    UNION_COMMON,

    /**
     * Use explicitly defined schema from configuration.
     */
    USER_DEFINED
  }

  /**
   * Strategies for JSON file schema resolution.
   */
  public enum JsonStrategy {
    /**
     * Use schema from the most recently modified JSON file.
     * This is the DEFAULT strategy for JSON files.
     *
     * Rationale: JSON schemas can evolve rapidly, and the latest
     * structure likely represents the current expected format.
     */
    LATEST_FILE,

    /**
     * Union all keys seen across all JSON files.
     * Creates a schema with all possible fields.
     */
    UNION_KEYS,

    /**
     * Use the first JSON file encountered.
     */
    FIRST_FILE,

    /**
     * Only include keys that exist in ALL JSON files.
     */
    INTERSECTION_ONLY
  }

  /**
   * Validation levels for schema compatibility checking.
   */
  public enum ValidationLevel {
    /** No validation performed */
    NONE,

    /** Log warnings for schema mismatches */
    WARN,

    /** Throw exceptions for serious schema conflicts */
    ERROR
  }

  @Override public String toString() {
    return String.format("SchemaStrategy{parquet=%s, csv=%s, json=%s, priority=%s, validation=%s}",
                        parquetStrategy, csvStrategy, jsonStrategy, formatPriority, validationLevel);
  }
}
