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
 * Parquet format-specific utilities and converters.
 *
 * <p>This package provides utilities for working with Apache Parquet files,
 * including conversion utilities, caching mechanisms, and direct writers.</p>
 *
 * <p>Key components:</p>
 * <ul>
 *   <li>{@link org.apache.calcite.adapter.file.format.parquet.ParquetConversionUtil} - Utilities for converting to/from Parquet</li>
 *   <li>{@link org.apache.calcite.adapter.file.format.parquet.DirectParquetWriter} - Direct Parquet file writing</li>
 *   <li>{@link org.apache.calcite.adapter.file.format.parquet.ConcurrentParquetCache} - Thread-safe Parquet caching</li>
 * </ul>
 */
package org.apache.calcite.adapter.file.format.parquet;
