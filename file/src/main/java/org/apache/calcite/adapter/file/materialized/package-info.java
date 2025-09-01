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
 * Materialized view support for the file adapter.
 *
 * <p>This package provides components for creating and managing materialized
 * views over file-based data sources. Materialized views can significantly
 * improve query performance by pre-computing and caching results.</p>
 *
 * <p>Key components:</p>
 * <ul>
 *   <li>{@link org.apache.calcite.adapter.file.materialized.MaterializedViewTable} - Table representing a materialized view</li>
 *   <li>{@link org.apache.calcite.adapter.file.materialized.MaterializedViewUtil} - Utilities for materialized view management</li>
 *   <li>{@link org.apache.calcite.adapter.file.materialized.RefreshableMaterializedViewTable} - Auto-refreshing materialized views</li>
 * </ul>
 */
package org.apache.calcite.adapter.file.materialized;
