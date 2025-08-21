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
package org.apache.calcite.adapter.file.execution;

import org.apache.calcite.adapter.file.execution.duckdb.DuckDBConfig;
import org.apache.calcite.adapter.file.execution.duckdb.DuckDBExecutionEngine;

import java.io.File;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configuration for execution engines in the file adapter.
 *
 * <p>Supports different execution engines for processing file data:
 * <ul>
 *   <li><b>DUCKDB</b>: DuckDB-based analytical processing with SQL pushdown (default when available)</li>
 *   <li><b>PARQUET</b>: Parquet-based columnar processing with streaming support (fallback)</li>
 *   <li><b>LINQ4J</b>: Traditional row-by-row processing</li>
 *   <li><b>ARROW</b>: Arrow-based columnar processing</li>
 *   <li><b>VECTORIZED</b>: Optimized vectorized Arrow processing</li>
 * </ul>
 */
public class ExecutionEngineConfig {
  private static final Logger LOGGER = LoggerFactory.getLogger(ExecutionEngineConfig.class);

  /** Default execution engine if not specified. */
  public static final String DEFAULT_EXECUTION_ENGINE = getDefaultEngine();

  /** Default batch size for columnar engines. */
  public static final int DEFAULT_BATCH_SIZE = 2048;

  /** Default memory threshold before spillover (64MB). */
  public static final long DEFAULT_MEMORY_THRESHOLD = 64L * 1024 * 1024;

  private final ExecutionEngineType engineType;
  private final int batchSize;
  private final long memoryThreshold;
  private final String materializedViewStoragePath;
  private final boolean useCustomStoragePath;
  private final DuckDBConfig duckdbConfig;
  private final String parquetCacheDirectory;

  public ExecutionEngineConfig(String executionEngine, int batchSize) {
    this(executionEngine, batchSize, DEFAULT_MEMORY_THRESHOLD, null);
  }

  public ExecutionEngineConfig(String executionEngine, int batchSize,
      String materializedViewStoragePath) {
    this(executionEngine, batchSize, DEFAULT_MEMORY_THRESHOLD, materializedViewStoragePath);
  }

  public ExecutionEngineConfig(String executionEngine, int batchSize,
      long memoryThreshold, String materializedViewStoragePath) {
    this(executionEngine, batchSize, memoryThreshold, materializedViewStoragePath, null);
  }

  public ExecutionEngineConfig(String executionEngine, int batchSize,
      long memoryThreshold, String materializedViewStoragePath, DuckDBConfig duckdbConfig) {
    this(executionEngine, batchSize, memoryThreshold, materializedViewStoragePath, duckdbConfig, null);
  }

  public ExecutionEngineConfig(String executionEngine, int batchSize,
      long memoryThreshold, String materializedViewStoragePath, DuckDBConfig duckdbConfig,
      String parquetCacheDirectory) {
    this.engineType = parseExecutionEngine(executionEngine);
    this.batchSize = batchSize;
    this.memoryThreshold = memoryThreshold;
    this.materializedViewStoragePath = materializedViewStoragePath;
    this.useCustomStoragePath = materializedViewStoragePath != null;
    this.duckdbConfig = duckdbConfig != null ? duckdbConfig : new DuckDBConfig();
    this.parquetCacheDirectory = parquetCacheDirectory;
  }

  public ExecutionEngineConfig() {
    this(DEFAULT_EXECUTION_ENGINE, DEFAULT_BATCH_SIZE, DEFAULT_MEMORY_THRESHOLD, null, null);
  }

  private static ExecutionEngineType parseExecutionEngine(String executionEngine) {
    try {
      ExecutionEngineType engineType = ExecutionEngineType.valueOf(executionEngine.toUpperCase(Locale.ROOT));

      // Warn when using non-recommended engines (neither DUCKDB nor PARQUET)
      if (engineType != ExecutionEngineType.DUCKDB && engineType != ExecutionEngineType.PARQUET) {
        LOGGER.warn("WARNING: Using execution engine '{}' is not recommended for production use.", executionEngine);
        LOGGER.warn("         Recommended engines:");
        LOGGER.warn("         - DUCKDB: Best performance for analytics (10-100x faster)");
        LOGGER.warn("         - PARQUET: Good performance with streaming support");
        LOGGER.warn("         Other engines are primarily for benchmarking or compatibility purposes.");
      }

      return engineType;
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Invalid execution engine: " + executionEngine
          + ". Valid options: " + String.join(", ", getAvailableEngineTypes()), e);
    }
  }

  public ExecutionEngineType getEngineType() {
    return engineType;
  }

  public int getBatchSize() {
    return batchSize;
  }

  public long getMemoryThreshold() {
    return memoryThreshold;
  }

  public String getMaterializedViewStoragePath() {
    return materializedViewStoragePath;
  }

  public boolean hasCustomStoragePath() {
    return useCustomStoragePath;
  }

  public DuckDBConfig getDuckDBConfig() {
    return duckdbConfig;
  }

  public String getParquetCacheDirectory() {
    return parquetCacheDirectory;
  }

  /**
   * Determines the default execution engine based on driver availability.
   * Defaults to DuckDB if the driver is in the classpath, otherwise falls back to PARQUET.
   * 
   * @return The default execution engine name
   */
  private static String getDefaultEngine() {
    // Default to parquet engine
    return "parquet";
  }

  /**
   * Get all available execution engine types.
   * @return Array of available execution engine type names
   */
  public static String[] getAvailableEngineTypes() {
    return new String[]{"linq4j", "arrow", "vectorized", "parquet", "duckdb"};
  }

  /**
   * Supported execution engine types for file processing.
   */
  public enum ExecutionEngineType {
    /**
     * Traditional row-by-row processing using Linq4j enumerables.
     * Best for: OLTP workloads, small datasets, row-wise operations.
     */
    LINQ4J,

    /**
     * Arrow-based columnar processing with standard enumerable interface.
     * Best for: Medium datasets, mixed workloads.
     */
    ARROW,

    /**
     * Vectorized Arrow processing with optimized columnar operations.
     * Best for: OLAP workloads, large datasets, analytical queries.
     */
    VECTORIZED,

    /**
     * Parquet-based columnar processing with streaming support.
     * Best for: Very large datasets, streaming workloads, compressed data.
     * Supports row group-based streaming and efficient predicate pushdown.
     */
    PARQUET,

    /**
     * DuckDB-based analytical processing with SQL pushdown.
     * Best for: Complex analytics, high performance aggregations, joins.
     * Leverages DuckDB's columnar execution engine for Parquet files.
     * Requires DuckDB JDBC driver dependency.
     */
    DUCKDB
  }
}
