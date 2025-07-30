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

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.File;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;

/**
 * Refreshable materialized view table that can re-execute its query
 * when the refresh interval has elapsed.
 */
public class RefreshableMaterializedViewTable extends MaterializedViewTable
    implements RefreshableTable {
  private final String tableName;
  private final @Nullable Duration refreshInterval;
  private @Nullable Instant lastRefreshTime;
  private volatile boolean needsRematerialization = false;

  public RefreshableMaterializedViewTable(SchemaPlus parentSchema, String schemaName,
      String viewName, String sql, File parquetFile, Map<String, Table> existingTables,
      @Nullable Duration refreshInterval) {
    super(parentSchema, schemaName, viewName, sql, parquetFile, existingTables);
    this.tableName = viewName;
    this.refreshInterval = refreshInterval;
  }

  @Override public @Nullable Duration getRefreshInterval() {
    return refreshInterval;
  }

  @Override public @Nullable Instant getLastRefreshTime() {
    return lastRefreshTime;
  }

  @Override public boolean needsRefresh() {
    if (refreshInterval == null) {
      return false;
    }

    // First time - always refresh
    if (lastRefreshTime == null) {
      return true;
    }

    // Check if interval has elapsed
    return Instant.now().isAfter(lastRefreshTime.plus(refreshInterval));
  }

  @Override public void refresh() {
    if (!needsRefresh()) {
      return;
    }

    // Mark for re-materialization on next access
    needsRematerialization = true;
    lastRefreshTime = Instant.now();
  }

  @Override public RefreshBehavior getRefreshBehavior() {
    return RefreshBehavior.MATERIALIZED_VIEW;
  }

  @Override public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
    // Check if refresh needed before accessing
    refresh();

    if (needsRematerialization && parquetFile.exists()) {
      // Delete old materialized view to force re-materialization
      parquetFile.delete();
      needsRematerialization = false;
      // Reset materialized flag in parent
      materialized.set(false);
    }

    return super.toRel(context, relOptTable);
  }

  @Override public String toString() {
    return "RefreshableMaterializedViewTable(" + tableName + ")";
  }
}
