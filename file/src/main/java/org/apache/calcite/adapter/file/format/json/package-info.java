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
 * JSON format-specific utilities and processing.
 *
 * <p>This package provides utilities for working with JSON files,
 * including flattening nested structures, multi-table extraction,
 * and shared data management.</p>
 *
 * <p>Key components:</p>
 * <ul>
 *   <li>{@link org.apache.calcite.adapter.file.format.json.JsonFlattener} - Flattens nested JSON structures</li>
 *   <li>{@link org.apache.calcite.adapter.file.format.json.JsonMultiTableFactory} - Extracts multiple tables from JSON</li>
 *   <li>{@link org.apache.calcite.adapter.file.format.json.JsonSearchConfig} - Configuration for JSON searching</li>
 *   <li>{@link org.apache.calcite.adapter.file.format.json.SharedJsonData} - Manages shared JSON data across tables</li>
 * </ul>
 */
package org.apache.calcite.adapter.file.format.json;