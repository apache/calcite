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
package org.apache.calcite.adapter.file.execution.duckdb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DuckDB-based execution engine for high-performance analytical processing.
 *
 * <p>This engine leverages DuckDB's high-performance columnar query engine
 * for analytical workloads on Parquet files.
 *
 * <p>Key advantages:
 * <ul>
 *   <li>10-100x performance improvements for complex analytics</li>
 *   <li>Native support for complex aggregations, joins, and window functions</li>
 *   <li>Automatic vectorization and parallelization</li>
 *   <li>Efficient handling of compressed Parquet data</li>
 *   <li>Fallback to Parquet engine when DuckDB unavailable</li>
 * </ul>
 *
 * <p>Usage:
 * <pre>{@code
 * {
 *   "schemas": [{
 *     "name": "analytics",
 *     "operand": {
 *       "directory": "/data",
 *       "executionEngine": "DUCKDB",
 *       "duckdbConfig": {
 *         "memory_limit": "4GB",
 *         "threads": 4
 *       }
 *     }
 *   }]
 * }
 * }</pre>
 */
public final class DuckDBExecutionEngine {
  private static final Logger LOGGER = LoggerFactory.getLogger(DuckDBExecutionEngine.class);

  /**
   * Check if DuckDB JDBC driver is available on the classpath.
   *
   * @return true if DuckDB is available, false otherwise
   */
  public static boolean isAvailable() {
    try {
      Class.forName("org.duckdb.DuckDBDriver");
      return true;
    } catch (ClassNotFoundException e) {
      LOGGER.debug("DuckDB JDBC driver not found on classpath: {}", e.getMessage());
      return false;
    } catch (Exception e) {
      LOGGER.debug("Error checking DuckDB availability: {}", e.getMessage());
      return false;
    }
  }

  /**
   * Get the engine type identifier.
   *
   * @return "DUCKDB"
   */
  public static String getEngineType() {
    return "DUCKDB";
  }

  private DuckDBExecutionEngine() {
    // Utility class - all methods are static
  }
}
