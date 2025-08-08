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

import java.util.Locale;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configuration for execution engines in the file adapter.
 *
 * <p>Supports different execution engines for processing file data:
 * <ul>
 *   <li><b>LINQ4J</b>: Traditional row-by-row processing (default)</li>
 *   <li><b>ARROW</b>: Arrow-based columnar processing</li>
 *   <li><b>VECTORIZED</b>: Optimized vectorized Arrow processing</li>
 * </ul>
 */
public class ExecutionEngineConfig {
  private static final Logger LOGGER = LoggerFactory.getLogger(ExecutionEngineConfig.class);

  /** Default execution engine if not specified. */
  public static final String DEFAULT_EXECUTION_ENGINE = "parquet";

  /** Default batch size for columnar engines. */
  public static final int DEFAULT_BATCH_SIZE = 2048;

  /** Default memory threshold before spillover (64MB). */
  public static final long DEFAULT_MEMORY_THRESHOLD = 64L * 1024 * 1024;

  private final ExecutionEngineType engineType;
  private final int batchSize;
  private final long memoryThreshold;
  private final String materializedViewStoragePath;
  private final boolean useCustomStoragePath;

  public ExecutionEngineConfig(String executionEngine, int batchSize) {
    this(executionEngine, batchSize, DEFAULT_MEMORY_THRESHOLD, null);
  }

  public ExecutionEngineConfig(String executionEngine, int batchSize,
      String materializedViewStoragePath) {
    this(executionEngine, batchSize, DEFAULT_MEMORY_THRESHOLD, materializedViewStoragePath);
  }

  public ExecutionEngineConfig(String executionEngine, int batchSize,
      long memoryThreshold, String materializedViewStoragePath) {
    this.engineType = parseExecutionEngine(executionEngine);
    this.batchSize = batchSize;
    this.memoryThreshold = memoryThreshold;
    this.materializedViewStoragePath = materializedViewStoragePath;
    this.useCustomStoragePath = materializedViewStoragePath != null;
  }

  public ExecutionEngineConfig() {
    this(DEFAULT_EXECUTION_ENGINE, DEFAULT_BATCH_SIZE, DEFAULT_MEMORY_THRESHOLD, null);
  }

  private static ExecutionEngineType parseExecutionEngine(String executionEngine) {
    try {
      ExecutionEngineType engineType = ExecutionEngineType.valueOf(executionEngine.toUpperCase(Locale.ROOT));

      // Warn when using non-PARQUET engines
      if (engineType != ExecutionEngineType.PARQUET) {
        LOGGER.warn("WARNING: Using execution engine '{}' is not recommended for production use.", executionEngine);
        LOGGER.warn("         The PARQUET engine is the default and recommended choice for:");
        LOGGER.warn("         - Best performance (1.6x faster)");
        LOGGER.warn("         - Automatic file update detection");
        LOGGER.warn("         - Disk spillover for unlimited dataset sizes");
        LOGGER.warn("         - Redis distributed cache support");
        LOGGER.warn("         Other engines are primarily for benchmarking purposes.");
      }

      return engineType;
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Invalid execution engine: " + executionEngine
          + ". Valid options: linq4j, arrow, vectorized, parquet", e);
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
    PARQUET
  }
}
