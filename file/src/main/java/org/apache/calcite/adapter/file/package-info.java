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
 * Apache Calcite file adapter for querying various file formats as relational tables.
 *
 * <p>The file adapter enables Apache Calcite to treat files in various formats
 * (CSV, JSON, HTML, Excel, Parquet, etc.) as queryable relational tables. It provides
 * a comprehensive framework for file-based data access with support for both local
 * and remote storage systems.</p>
 *
 * <h2>Package Structure</h2>
 * <p>The adapter is organized into specialized sub-packages:</p>
 * <ul>
 *   <li>{@link org.apache.calcite.adapter.file.csv} - CSV file processing and tables</li>
 *   <li>{@link org.apache.calcite.adapter.file.json} - JSON file processing with multi-table support</li>
 *   <li>{@link org.apache.calcite.adapter.file.execution} - Execution engines (row-based, columnar, vectorized)</li>
 *   <li>{@link org.apache.calcite.adapter.file.storage} - Storage providers for various file systems</li>
 * </ul>
 *
 * <h2>Supported File Formats</h2>
 * <ul>
 *   <li><b>CSV</b> - Comma-separated values with configurable delimiters</li>
 *   <li><b>JSON</b> - JavaScript Object Notation with nested structure support</li>
 *   <li><b>YAML</b> - YAML Ain't Markup Language</li>
 *   <li><b>HTML</b> - HTML tables extraction</li>
 *   <li><b>Excel</b> - Microsoft Excel files (.xlsx, .xls)</li>
 *   <li><b>Parquet</b> - Apache Parquet columnar format</li>
 *   <li><b>Arrow</b> - Apache Arrow in-memory format</li>
 *   <li><b>Markdown</b> - Tables in Markdown documents</li>
 *   <li><b>Word</b> - Tables in Microsoft Word documents (.docx)</li>
 *   <li><b>PowerPoint</b> - Tables in PowerPoint presentations (.pptx)</li>
 * </ul>
 *
 * <h2>Core Components</h2>
 * <ul>
 *   <li>{@link org.apache.calcite.adapter.file.FileSchema} - Schema that discovers and manages file tables</li>
 *   <li>{@link org.apache.calcite.adapter.file.FileSchemaFactory} - Factory for creating file schemas</li>
 *   <li>{@link org.apache.calcite.adapter.file.FileTable} - Base class for file-based tables</li>
 *   <li>{@link org.apache.calcite.adapter.file.FileRules} - Query optimization rules</li>
 * </ul>
 *
 * <h2>Advanced Features</h2>
 *
 * <h3>Multi-Table JSON Support</h3>
 * <p>Extract multiple tables from a single JSON file using JSONPath expressions
 * or automatic discovery. See {@link org.apache.calcite.adapter.file.json}.</p>
 *
 * <h3>Storage Abstraction</h3>
 * <p>Access files from various sources (S3, HTTP, FTP, SharePoint) transparently
 * through the storage provider interface. See {@link org.apache.calcite.adapter.file.storage}.</p>
 *
 * <h3>Execution Engines</h3>
 * <p>Choose between different execution strategies based on query patterns:
 * row-based (LINQ4J), columnar (Parquet), or vectorized (Arrow).
 * See {@link org.apache.calcite.adapter.file.execution}.</p>
 *
 * <h3>Materialized Views</h3>
 * <p>Support for materialized views with automatic refresh:</p>
 * <ul>
 *   <li>{@link org.apache.calcite.adapter.file.MaterializedViewTable}</li>
 *   <li>{@link org.apache.calcite.adapter.file.RefreshableTable}</li>
 * </ul>
 *
 * <h3>Partitioned Tables</h3>
 * <p>Support for partitioned Parquet tables with partition pruning:</p>
 * <ul>
 *   <li>{@link org.apache.calcite.adapter.file.PartitionedParquetTable}</li>
 *   <li>{@link org.apache.calcite.adapter.file.PartitionDetector}</li>
 * </ul>
 *
 * <h2>Configuration Example</h2>
 * <pre>{@code
 * {
 *   "version": "1.0",
 *   "defaultSchema": "files",
 *   "schemas": [{
 *     "name": "files",
 *     "type": "custom",
 *     "factory": "org.apache.calcite.adapter.file.FileSchemaFactory",
 *     "operand": {
 *       "directory": "/data",
 *       "recursive": true,
 *       "directoryPattern": ".*\\.csv",
 *       "executionEngine": "VECTORIZED",
 *       "storageType": "s3",
 *       "storageConfig": {
 *         "bucket": "my-data-bucket",
 *         "region": "us-west-1"
 *       }
 *     }
 *   }]
 * }
 * }</pre>
 *
 * <h2>Query Examples</h2>
 * <pre>{@code
 * -- Query CSV file
 * SELECT * FROM employees WHERE salary > 50000;
 *
 * -- Query JSON with multiple tables
 * SELECT u.name, o.total
 * FROM users u JOIN orders o ON u.id = o.user_id;
 *
 * -- Query partitioned Parquet
 * SELECT * FROM sales WHERE year = 2024 AND month = 3;
 * }</pre>
 *
 * <h2>Performance Optimization</h2>
 * <ul>
 *   <li>Predicate pushdown for filtering at source</li>
 *   <li>Projection pushdown to read only needed columns</li>
 *   <li>Partition pruning for partitioned tables</li>
 *   <li>Vectorized processing for analytical queries</li>
 *   <li>Materialized views for frequently accessed aggregations</li>
 * </ul>
 */
package org.apache.calcite.adapter.file;
