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
package org.apache.calcite.adapter.file.refresh;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.file.metadata.RemoteFileMetadata;
import org.apache.calcite.adapter.file.table.CsvTable;
import org.apache.calcite.adapter.file.table.CsvTranslatableTable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.util.Source;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Refreshable CSV table that re-reads the source file when modified.
 */
public class RefreshableCsvTable extends CsvTranslatableTable implements RefreshableTable {
  private static final Logger LOGGER = Logger.getLogger(RefreshableCsvTable.class.getName());

  private final String tableName;
  private final @Nullable Duration refreshInterval;
  private @Nullable Instant lastRefreshTime;
  private long lastModifiedTime;
  private @Nullable RemoteFileMetadata lastRemoteMetadata;
  private volatile boolean dataStale = false;

  public RefreshableCsvTable(Source source, String tableName,
      @Nullable RelProtoDataType protoRowType, @Nullable Duration refreshInterval) {
    super(source, protoRowType);
    this.tableName = tableName;
    this.refreshInterval = refreshInterval;
    this.lastModifiedTime = 0;
    this.lastRemoteMetadata = null;
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

    // Check if file has been modified
    String protocol = source.protocol();
    if ("file".equals(protocol)) {
      // Local file - use timestamp
      File file = source.file();
      if (file != null && file.exists()) {
        long currentModified = file.lastModified();
        if (currentModified > lastModifiedTime) {
          dataStale = true;
          lastModifiedTime = currentModified;
        }
      }
    } else if ("http".equals(protocol) || "https".equals(protocol)
               || "s3".equals(protocol) || "ftp".equals(protocol)) {
      // Remote file - use metadata checking
      try {
        RemoteFileMetadata currentMetadata = RemoteFileMetadata.fetch(source);

        if (lastRemoteMetadata == null) {
          // First time checking
          dataStale = true;
          lastRemoteMetadata = currentMetadata;
        } else if (currentMetadata.hasChanged(lastRemoteMetadata)) {
          dataStale = true;
          lastRemoteMetadata = currentMetadata;
          LOGGER.info("Remote file changed: " + source.path());
        }
      } catch (IOException e) {
        LOGGER.log(Level.WARNING, "Failed to check remote file metadata: " + source.path(), e);
        // Assume it might have changed to be safe
        dataStale = true;
      }
    }

    lastRefreshTime = Instant.now();
  }

  @Override public RefreshBehavior getRefreshBehavior() {
    return RefreshBehavior.SINGLE_FILE;
  }

  @Override public Enumerable<Object> project(final DataContext root, final int[] fields) {
    // Check and refresh if needed
    refresh();

    // If data is stale, we need to create a completely new enumerator
    // to ensure the file is re-read
    if (dataStale) {
      dataStale = false;
      // Clear any cached row type to force re-deduction
      clearCachedRowType();
    }

    return super.project(root, fields);
  }

  /** Clear cached row type to force re-reading the file. */
  private void clearCachedRowType() {
    try {
      // Use reflection to clear the cached rowType in parent CsvTable
      java.lang.reflect.Field rowTypeField = CsvTable.class.getDeclaredField("rowType");
      rowTypeField.setAccessible(true);
      rowTypeField.set(this, null);

      java.lang.reflect.Field fieldTypesField = CsvTable.class.getDeclaredField("fieldTypes");
      fieldTypesField.setAccessible(true);
      fieldTypesField.set(this, null);
    } catch (Exception e) {
      LOGGER.log(Level.WARNING, "Failed to clear cached row type", e);
    }
  }

  @Override public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
    // Check and refresh if needed
    refresh();

    return super.toRel(context, relOptTable);
  }

  @Override public String toString() {
    return "RefreshableCsvTable(" + tableName + ")";
  }
}
