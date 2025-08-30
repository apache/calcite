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

/**
 * Execution engines for the Apache Calcite file adapter.
 *
 * <p>This package contains various execution engine implementations that determine
 * how file data is processed and queried. Different engines offer different
 * performance characteristics and capabilities for various file formats and
 * query patterns.</p>
 *
 * <h2>Execution Engine Types</h2>
 *
 * <h3>Traditional Row-based (LINQ4J)</h3>
 * <p>The default execution model using Calcite's LINQ4J framework:</p>
 * <ul>
 *   <li>Row-by-row processing</li>
 *   <li>Lower memory footprint</li>
 *   <li>Good for small to medium datasets</li>
 *   <li>Supports all Calcite operators</li>
 * </ul>
 *
 * <h3>Parquet Execution Engine</h3>
 * <p>{@link org.apache.calcite.adapter.file.execution.ParquetExecutionEngine} provides:</p>
 * <ul>
 *   <li>Columnar data processing</li>
 *   <li>Efficient compression</li>
 *   <li>Predicate pushdown capabilities</li>
 *   <li>Optimized for analytical queries</li>
 *   <li>In-memory Parquet format conversion</li>
 * </ul>
 *
 * <h3>Vectorized Arrow Execution</h3>
 * <p>{@link org.apache.calcite.adapter.file.execution.VectorizedArrowExecutionEngine} features:</p>
 * <ul>
 *   <li>Apache Arrow-based columnar processing</li>
 *   <li>SIMD optimizations</li>
 *   <li>Batch processing (default 2048 rows)</li>
 *   <li>Zero-copy data transfers</li>
 *   <li>Best for large-scale analytical workloads</li>
 * </ul>
 *
 * <h2>Configuration</h2>
 * <p>{@link org.apache.calcite.adapter.file.execution.ExecutionEngineConfig} controls:</p>
 * <ul>
 *   <li><b>engineType</b> - LINQ4J, PARQUET, or VECTORIZED</li>
 *   <li><b>batchSize</b> - Rows per batch for columnar engines (default: 2048)</li>
 *   <li><b>memoryThreshold</b> - Memory limit before spillover (default: 64MB)</li>
 *   <li><b>materializedViewStoragePath</b> - Path for materialized view storage</li>
 * </ul>
 *
 * <h2>File Enumerators</h2>
 * <ul>
 *   <li>{@link org.apache.calcite.adapter.file.execution.ParquetFileEnumerator} - Enumerator for Parquet-based processing</li>
 *   <li>{@link org.apache.calcite.adapter.file.execution.VectorizedFileEnumerator} - Universal vectorized enumerator</li>
 * </ul>
 *
 * <h2>Usage Example</h2>
 * <pre>{@code
 * // Configure vectorized execution
 * ExecutionEngineConfig config = new ExecutionEngineConfig()
 *     .withEngineType(ExecutionEngineType.VECTORIZED)
 *     .withBatchSize(4096)
 *     .withMemoryThreshold(128 * 1024 * 1024);
 *
 * // Create schema with execution engine
 * FileSchema schema = new FileSchema(
 *     parentSchema, name, directory, pattern, tables,
 *     config, recursive, materializations, views
 *);
 * }</pre>
 *
 * <h2>Performance Considerations</h2>
 * <table>
 *   <tr>
 *     <th>Engine</th>
 *     <th>Best For</th>
 *     <th>Memory Usage</th>
 *     <th>CPU Usage</th>
 *   </tr>
 *   <tr>
 *     <td>LINQ4J</td>
 *     <td>Small datasets, complex queries</td>
 *     <td>Low</td>
 *     <td>Moderate</td>
 *   </tr>
 *   <tr>
 *     <td>PARQUET</td>
 *     <td>Column-selective queries</td>
 *     <td>Moderate</td>
 *     <td>Low</td>
 *   </tr>
 *   <tr>
 *     <td>VECTORIZED</td>
 *     <td>Large analytical queries</td>
 *     <td>High</td>
 *     <td>Optimized</td>
 *   </tr>
 * </table>
 *
 * <h2>Data Conversion Flow</h2>
 * <ol>
 *   <li>File data is read by format-specific readers</li>
 *   <li>Data is converted to execution engine format</li>
 *   <li>Query operators process data in engine-specific way</li>
 *   <li>Results are converted back to row format if needed</li>
 * </ol>
 */
package org.apache.calcite.adapter.file.execution;
