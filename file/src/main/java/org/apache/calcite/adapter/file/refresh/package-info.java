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
 * Auto-refreshing table support for dynamic file sources.
 *
 * <p>This package provides components for creating tables that automatically
 * refresh their data when the underlying files change or at specified intervals.
 * This is particularly useful for monitoring log files or data feeds.</p>
 *
 * <p>Key components:</p>
 * <ul>
 *   <li>{@link org.apache.calcite.adapter.file.refresh.RefreshableTable} - Interface for refreshable tables</li>
 *   <li>{@link org.apache.calcite.adapter.file.refresh.AbstractRefreshableTable} - Base implementation for refreshable tables</li>
 *   <li>{@link org.apache.calcite.adapter.file.refresh.RefreshInterval} - Configuration for refresh intervals</li>
 *   <li>{@link org.apache.calcite.adapter.file.refresh.RefreshableCsvTable} - Auto-refreshing CSV tables</li>
 *   <li>{@link org.apache.calcite.adapter.file.refresh.RefreshableJsonTable} - Auto-refreshing JSON tables</li>
 *   <li>{@link org.apache.calcite.adapter.file.refresh.RefreshablePartitionedParquetTable} - Auto-refreshing partitioned Parquet tables</li>
 * </ul>
 */
package org.apache.calcite.adapter.file.refresh;