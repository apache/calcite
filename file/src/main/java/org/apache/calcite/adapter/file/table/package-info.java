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
 * Table implementations for the file adapter.
 *
 * <p>This package contains various table implementations that provide
 * access to data from different file formats including CSV, JSON, and Parquet.
 * Tables can be scannable, translatable, or both, and may support features
 * like partitioning and glob patterns.</p>
 *
 * <p>Key components:</p>
 * <ul>
 *   <li>{@link org.apache.calcite.adapter.file.table.FileTable} - Base table implementation</li>
 *   <li>{@link org.apache.calcite.adapter.file.table.CsvTable} - CSV file tables</li>
 *   <li>{@link org.apache.calcite.adapter.file.table.JsonTable} - JSON file tables</li>
 *   <li>{@link org.apache.calcite.adapter.file.table.ParquetScannableTable} - Parquet scannable tables</li>
 *   <li>{@link org.apache.calcite.adapter.file.table.GlobParquetTable} - Glob pattern Parquet tables</li>
 *   <li>{@link org.apache.calcite.adapter.file.table.PartitionedParquetTable} - Partitioned Parquet tables</li>
 * </ul>
 */
package org.apache.calcite.adapter.file.table;
