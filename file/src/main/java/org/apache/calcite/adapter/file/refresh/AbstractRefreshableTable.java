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

import org.apache.calcite.adapter.file.metadata.RemoteFileMetadata;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.Source;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Abstract base class for tables that support refresh operations.
 */
public abstract class AbstractRefreshableTable extends AbstractTable implements RefreshableTable {
  private static final Logger LOGGER = Logger.getLogger(AbstractRefreshableTable.class.getName());

  protected final @Nullable Duration refreshInterval;
  protected final String tableName;
  protected @Nullable Instant lastRefreshTime;
  protected long lastModifiedTime;
  protected @Nullable RemoteFileMetadata lastRemoteMetadata;

  protected AbstractRefreshableTable(String tableName, @Nullable Duration refreshInterval) {
    this.tableName = tableName;
    this.refreshInterval = refreshInterval;
    this.lastRefreshTime = null;
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

  /**
   * Checks if a file has been modified since last refresh.
   *
   * @param file File to check
   * @return true if modified, false otherwise
   */
  protected boolean isFileModified(File file) {
    if (!file.exists()) {
      return false;
    }
    return file.lastModified() > lastModifiedTime;
  }

  /**
   * Checks if a remote file has been modified since last refresh.
   *
   * @param source Source to check
   * @return true if modified, false otherwise, or true if cannot determine
   */
  protected boolean isRemoteFileModified(Source source) {
    try {
      RemoteFileMetadata currentMetadata = RemoteFileMetadata.fetch(source);

      if (lastRemoteMetadata == null) {
        // First time checking
        lastRemoteMetadata = currentMetadata;
        return true;
      }

      boolean changed = currentMetadata.hasChanged(lastRemoteMetadata);
      if (changed) {
        lastRemoteMetadata = currentMetadata;
      }

      return changed;
    } catch (IOException e) {
      LOGGER.log(Level.WARNING, "Failed to fetch remote file metadata for " + source.path(), e);
      // If we can't check, assume it might have changed
      return true;
    }
  }

  /**
   * Updates the last modified timestamp after a successful refresh.
   *
   * @param file File that was refreshed
   */
  protected void updateLastModified(File file) {
    if (file.exists()) {
      lastModifiedTime = file.lastModified();
    }
    lastRefreshTime = Instant.now();
  }

  /**
   * Updates the remote metadata after a successful refresh.
   *
   * @param metadata New metadata
   */
  protected void updateRemoteMetadata(RemoteFileMetadata metadata) {
    lastRemoteMetadata = metadata;
    lastRefreshTime = Instant.now();
  }

  /**
   * Common refresh logic - checks if refresh is needed before calling
   * the specific implementation.
   */
  @Override public void refresh() {
    // The refresh interval prevents thrashing:
    // - If file hasn't changed: never update cache (regardless of interval)
    // - If file has changed AND interval has elapsed: synchronously update cache
    // - If file has changed BUT interval hasn't elapsed: don't update (prevent thrashing)
    
    if (!needsRefresh()) {
      LOGGER.log(Level.FINE, "Refresh interval not elapsed, skipping refresh");
      return;
    }

    LOGGER.log(Level.INFO, "Refresh interval elapsed, calling doRefresh()");
    doRefresh();
    lastRefreshTime = Instant.now();
  }

  /**
   * Subclasses implement specific refresh logic here.
   */
  protected abstract void doRefresh();
}
