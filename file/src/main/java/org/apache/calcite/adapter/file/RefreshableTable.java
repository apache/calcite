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

import org.apache.calcite.schema.Table;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.time.Duration;
import java.time.Instant;

/**
 * Interface for tables that support refresh operations.
 * Tables implementing this interface can be refreshed based on
 * a configured interval.
 */
public interface RefreshableTable extends Table {
  /**
   * Gets the refresh interval for this table.
   *
   * @return Refresh interval, or null if no refresh is configured
   */
  @Nullable Duration getRefreshInterval();

  /**
   * Gets the last refresh time.
   *
   * @return Last refresh timestamp, or null if never refreshed
   */
  @Nullable Instant getLastRefreshTime();

  /**
   * Checks if this table needs to be refreshed based on its interval.
   *
   * @return true if refresh is needed, false otherwise
   */
  boolean needsRefresh();

  /**
   * Refreshes the table data.
   * This method should check for changes and update the table data
   * if necessary.
   */
  void refresh();

  /**
   * Gets the refresh behavior for this table type.
   *
   * @return Description of what happens during refresh
   */
  RefreshBehavior getRefreshBehavior();

  /**
   * Enum describing refresh behaviors for different table types.
   */
  enum RefreshBehavior {
    /**
     * Single file: Re-read if modified timestamp changed.
     */
    SINGLE_FILE("Re-reads file if modified"),

    /**
     * Directory scan: Update modified files only (no new/deleted files).
     */
    DIRECTORY_SCAN("Updates existing files only, ignores new/deleted files"),

    /**
     * Partitioned table: Scan for new partitions and update existing.
     */
    PARTITIONED_TABLE("Discovers new partitions and updates existing files"),

    /**
     * Materialized view: Re-execute SQL query if sources changed.
     */
    MATERIALIZED_VIEW("Re-executes query if source tables changed");

    private final String description;

    RefreshBehavior(String description) {
      this.description = description;
    }

    public String getDescription() {
      return description;
    }
  }
}
