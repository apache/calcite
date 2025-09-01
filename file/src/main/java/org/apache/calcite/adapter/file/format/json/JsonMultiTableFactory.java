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

import org.apache.calcite.adapter.file.table.JsonTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.util.Source;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Reader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Factory for creating multiple tables from a single JSON file.
 * Supports JSONPath-based table extraction and automatic table discovery.
 */
public class JsonMultiTableFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(JsonMultiTableFactory.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   * Creates multiple tables from a JSON source based on configuration.
   *
   * @param source The JSON source file
   * @param config The JSON search configuration
   * @return Map of table names to Table instances
   */
  public static Map<String, Table> createTables(Source source, JsonSearchConfig config) {
    try {
      // Parse JSON once into memory
      JsonNode rootNode = parseJsonOnce(source);

      // Create shared data container
      SharedJsonData sharedData = new SharedJsonData(rootNode);

      // Create tables based on configuration
      Map<String, Table> tables = new LinkedHashMap<>();

      if (config.getJsonSearchPaths() != null && !config.getJsonSearchPaths().isEmpty()) {
        // Use explicit paths
        createTablesFromPaths(tables, sharedData, source, config);
      } else if (config.isAutoDiscoverTables()) {
        // Auto-discover tables
        autoDiscoverTables(tables, sharedData, rootNode, source, config);
      } else {
        // Default: entire JSON as single table
        // For default behavior with no path, use file name as the table name
        String pattern = config.getTableNamePattern().equals(JsonSearchConfig.DEFAULT_TABLE_NAME_PATTERN)
            ? "{fileName}"
            : config.getTableNamePattern();
        String tableName = deriveTableName(source.path(), null, pattern);
        tables.put(tableName, new JsonTable(source, config.getOptions()));
      }

      return tables;

    } catch (IOException e) {
      LOGGER.error("Failed to parse JSON source: {}", source.path(), e);
      throw new RuntimeException("Failed to create tables from JSON", e);
    }
  }

  /**
   * Parse JSON source into a JsonNode tree.
   *
   * @param source The JSON source
   * @return The parsed JsonNode
   * @throws IOException if parsing fails
   */
  private static JsonNode parseJsonOnce(Source source) throws IOException {
    try (Reader reader = source.reader()) {
      return MAPPER.readTree(reader);
    }
  }

  /**
   * Create tables from explicit JSONPath expressions.
   *
   * @param tables The table map to populate
   * @param sharedData The shared JSON data
   * @param config The configuration
   */
  private static void createTablesFromPaths(
      Map<String, Table> tables,
      SharedJsonData sharedData,
      Source source,
      JsonSearchConfig config) {

    List<String> paths = config.getJsonSearchPaths();
    if (paths == null) {
      return;
    }

    Map<String, Integer> nameConflicts = new HashMap<>();

    for (String path : paths) {
      if (!sharedData.pathExists(path)) {
        LOGGER.warn("JSONPath '{}' not found in JSON structure, skipping", path);
        continue;
      }

      // Check minimum size requirement
      int pathSize = sharedData.getPathSize(path);
      if (pathSize < config.getMinArraySize()) {
        LOGGER.debug("Path '{}' has size {} which is below minimum {}, skipping",
            path, pathSize, config.getMinArraySize());
        continue;
      }

      // Generate table name
      String baseName = deriveTableName(source.path(), path, config.getTableNamePattern());
      String tableName = resolveNameConflict(baseName, nameConflicts);

      // Create table with path-specific view
      JsonTable table = new JsonTable(sharedData, path, config);
      tables.put(tableName, table);

      LOGGER.info("Created table '{}' from JSONPath '{}'", tableName, path);
    }
  }

  /**
   * Auto-discover tables in JSON structure.
   *
   * @param tables The table map to populate
   * @param sharedData The shared JSON data
   * @param rootNode The root JSON node
   * @param config The configuration
   */
  private static void autoDiscoverTables(
      Map<String, Table> tables,
      SharedJsonData sharedData,
      JsonNode rootNode,
      Source source,
      JsonSearchConfig config) {

    Map<String, String> discoveredPaths =
        discoverArrayPaths(rootNode, "$", 0, config.getMaxDiscoveryDepth(), config.getMinArraySize());

    Map<String, Integer> nameConflicts = new HashMap<>();

    for (Map.Entry<String, String> entry : discoveredPaths.entrySet()) {
      String path = entry.getKey();
      String suggestedName = entry.getValue();

      // Generate table name
      String baseName = config.getTableNamePattern().equals(JsonSearchConfig.DEFAULT_TABLE_NAME_PATTERN)
          ? suggestedName
          : deriveTableName(source.path(), path, config.getTableNamePattern());

      String tableName = resolveNameConflict(baseName, nameConflicts);

      // Create table with path-specific view
      JsonTable table = new JsonTable(sharedData, path, config);
      tables.put(tableName, table);

      LOGGER.info("Auto-discovered table '{}' from JSONPath '{}'", tableName, path);
    }
  }

  /**
   * Recursively discover array paths in JSON structure.
   *
   * @param node Current JSON node
   * @param currentPath Current JSONPath
   * @param depth Current depth
   * @param maxDepth Maximum depth to explore
   * @param minArraySize Minimum array size to consider as table
   * @return Map of JSONPath to suggested table name
   */
  private static Map<String, String> discoverArrayPaths(
      JsonNode node, String currentPath, int depth, int maxDepth, int minArraySize) {

    Map<String, String> paths = new LinkedHashMap<>();

    if (node.isArray() && node.size() >= minArraySize) {
      // Check if array contains objects (potential table)
      boolean hasObjects = false;
      for (JsonNode element : node) {
        if (element.isObject()) {
          hasObjects = true;
          break;
        }
      }

      if (hasObjects) {
        // Extract table name from path
        String tableName = extractTableNameFromPath(currentPath);
        paths.put(currentPath, tableName);
      }
    }

    // Only explore children if we haven't exceeded max depth
    if (depth < maxDepth && node.isObject()) {
      // Explore object fields
      Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
      while (fields.hasNext()) {
        Map.Entry<String, JsonNode> field = fields.next();
        String fieldPath = currentPath + "." + field.getKey();
        paths.putAll(
            discoverArrayPaths(
            field.getValue(), fieldPath, depth + 1, maxDepth, minArraySize));
      }
    }

    return paths;
  }

  /**
   * Extract a table name from a JSONPath.
   *
   * @param path The JSONPath
   * @return The suggested table name
   */
  private static String extractTableNameFromPath(String path) {
    // Remove $ and split by .
    String cleanPath = path.startsWith("$") ? path.substring(1) : path;
    if (cleanPath.startsWith(".")) {
      cleanPath = cleanPath.substring(1);
    }

    String[] segments = cleanPath.split("\\.");
    if (segments.length > 0) {
      return segments[segments.length - 1];
    }

    return "table";
  }

  /**
   * Derive a table name based on pattern and path.
   *
   * @param filePath The file path (optional)
   * @param jsonPath The JSONPath (optional)
   * @param pattern The naming pattern
   * @return The derived table name
   */
  private static String deriveTableName(
      @Nullable String filePath,
      @Nullable String jsonPath,
      String pattern) {

    String result = pattern;

    // Extract path segments
    String pathSegment = "table";
    String parentSegment = "";
    String fileName = "";

    if (jsonPath != null && !jsonPath.equals("$")) {
      String cleanPath = jsonPath.startsWith("$") ? jsonPath.substring(1) : jsonPath;
      if (cleanPath.startsWith(".")) {
        cleanPath = cleanPath.substring(1);
      }

      String[] segments = cleanPath.split("\\.");
      if (segments.length > 0) {
        pathSegment = segments[segments.length - 1];
        if (segments.length > 1) {
          parentSegment = segments[segments.length - 2];
        }
      }
    }

    if (filePath != null) {
      int lastSlash = filePath.lastIndexOf('/');
      String name = lastSlash >= 0 ? filePath.substring(lastSlash + 1) : filePath;
      int lastDot = name.lastIndexOf('.');
      fileName = lastDot >= 0 ? name.substring(0, lastDot) : name;
    }

    // Replace pattern variables
    result = result.replace("{pathSegment}", pathSegment);
    result = result.replace("{parentSegment}", parentSegment);
    result = result.replace("{fileName}", fileName);

    // Clean up empty placeholders
    result = result.replace("_{}", "").replace("{}_", "");

    return result.isEmpty() ? "table" : result;
  }

  /**
   * Resolve naming conflicts by adding index suffix.
   *
   * @param baseName The base table name
   * @param nameConflicts Map tracking name conflicts
   * @return The resolved unique table name
   */
  private static String resolveNameConflict(String baseName, Map<String, Integer> nameConflicts) {
    if (!nameConflicts.containsKey(baseName)) {
      nameConflicts.put(baseName, 0);
      return baseName;
    }

    int count = nameConflicts.get(baseName) + 1;
    nameConflicts.put(baseName, count);
    return baseName + "_" + count;
  }
}
