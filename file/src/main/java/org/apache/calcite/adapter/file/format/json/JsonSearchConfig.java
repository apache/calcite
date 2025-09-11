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
package org.apache.calcite.adapter.file.format.json;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Map;

/**
 * Configuration for JSON table discovery and processing.
 * Allows extracting multiple tables from a single JSON file using JSONPath expressions.
 */
public class JsonSearchConfig {
  /** Default table name pattern using last path segment */
  public static final String DEFAULT_TABLE_NAME_PATTERN = "{pathSegment}";

  /** JSONPath expressions to identify tables within JSON structure */
  private @Nullable List<String> jsonSearchPaths;

  /** Whether to automatically discover tables in JSON structure */
  private boolean autoDiscoverTables = false;

  /** Pattern for generating table names from paths */
  private String tableNamePattern = DEFAULT_TABLE_NAME_PATTERN;

  /** Maximum depth for auto-discovery */
  private int maxDiscoveryDepth = 5;

  /** Minimum array size to consider as table during auto-discovery */
  private int minArraySize = 1;

  /** Original options map for backwards compatibility */
  private @Nullable Map<String, Object> options;

  public JsonSearchConfig() {
  }

  public JsonSearchConfig(@Nullable Map<String, Object> options) {
    this.options = options;
    parseOptions(options);
  }

  /**
   * Create a JsonSearchConfig from a table definition map.
   * This is used when processing table definitions from model.json files.
   */
  public static JsonSearchConfig fromTableDefinition(@Nullable Map<String, Object> tableDef) {
    return new JsonSearchConfig(tableDef);
  }

  private void parseOptions(@Nullable Map<String, Object> options) {
    if (options == null) {
      return;
    }

    Object paths = options.get("jsonSearchPaths");
    if (paths instanceof List) {
      this.jsonSearchPaths = (List<String>) paths;
    }

    Object autoDiscover = options.get("autoDiscoverTables");
    if (autoDiscover instanceof Boolean) {
      this.autoDiscoverTables = (Boolean) autoDiscover;
    }

    Object pattern = options.get("tableNamePattern");
    if (pattern instanceof String) {
      this.tableNamePattern = (String) pattern;
    }

    Object maxDepth = options.get("maxDiscoveryDepth");
    if (maxDepth instanceof Number) {
      this.maxDiscoveryDepth = ((Number) maxDepth).intValue();
    }

    Object minSize = options.get("minArraySize");
    if (minSize instanceof Number) {
      this.minArraySize = ((Number) minSize).intValue();
    }
  }

  // Fluent API methods

  public JsonSearchConfig withJsonSearchPaths(@Nullable List<String> paths) {
    this.jsonSearchPaths = paths;
    return this;
  }

  public JsonSearchConfig withAutoDiscoverTables(boolean autoDiscover) {
    this.autoDiscoverTables = autoDiscover;
    return this;
  }

  public JsonSearchConfig withTableNamePattern(String pattern) {
    this.tableNamePattern = pattern;
    return this;
  }

  public JsonSearchConfig withMaxDiscoveryDepth(int depth) {
    this.maxDiscoveryDepth = depth;
    return this;
  }

  public JsonSearchConfig withMinArraySize(int size) {
    this.minArraySize = size;
    return this;
  }

  // Getters

  public @Nullable List<String> getJsonSearchPaths() {
    return jsonSearchPaths;
  }

  public boolean isAutoDiscoverTables() {
    return autoDiscoverTables;
  }

  public String getTableNamePattern() {
    return tableNamePattern;
  }

  public int getMaxDiscoveryDepth() {
    return maxDiscoveryDepth;
  }

  public int getMinArraySize() {
    return minArraySize;
  }

  public @Nullable Map<String, Object> getOptions() {
    return options;
  }

  /**
   * Check if multi-table mode is enabled.
   * Multi-table mode is active when either explicit paths are provided or auto-discovery is enabled.
   */
  public boolean isMultiTableMode() {
    return (jsonSearchPaths != null && !jsonSearchPaths.isEmpty()) || autoDiscoverTables;
  }
}
