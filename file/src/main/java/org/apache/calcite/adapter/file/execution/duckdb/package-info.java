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
 * DuckDB-based execution engine for high-performance analytical queries.
 *
 * <p>This package provides:
 * <ul>
 *   <li>SQL pushdown to DuckDB's columnar engine</li>
 *   <li>Reuse of Parquet conversion pipeline</li>
 *   <li>DuckDB connection management</li>
 *   <li>Fallback to Parquet engine when DuckDB unavailable</li>
 * </ul>
 */
package org.apache.calcite.adapter.file.execution.duckdb;