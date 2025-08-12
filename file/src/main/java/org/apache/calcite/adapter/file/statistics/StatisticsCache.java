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
package org.apache.calcite.adapter.file.statistics;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Handles serialization and caching of table statistics to/from disk.
 * Statistics are stored in JSON format for readability and portability.
 */
public class StatisticsCache {
  private static final Logger LOGGER = LoggerFactory.getLogger(StatisticsCache.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  
  private StatisticsCache() {
    // Utility class
  }

  /**
   * Save table statistics to a file.
   * 
   * @param statistics The statistics to save
   * @param file The file to save to
   */
  public static void saveStatistics(TableStatistics statistics, File file) throws IOException {
    ObjectNode root = MAPPER.createObjectNode();
    
    // Basic table info
    root.put("version", "1.0");
    root.put("rowCount", statistics.getRowCount());
    root.put("dataSize", statistics.getDataSize());
    root.put("lastUpdated", statistics.getLastUpdated());
    root.put("sourceHash", statistics.getSourceHash());
    
    // Column statistics
    ObjectNode columnsNode = root.putObject("columns");
    for (Map.Entry<String, ColumnStatistics> entry : statistics.getColumnStatistics().entrySet()) {
      ObjectNode columnNode = columnsNode.putObject(entry.getKey());
      serializeColumnStatistics(entry.getValue(), columnNode);
    }
    
    // Write to file atomically
    File tempFile = new File(file.getParentFile(), file.getName() + ".tmp");
    MAPPER.writerWithDefaultPrettyPrinter().writeValue(tempFile, root);
    
    if (!tempFile.renameTo(file)) {
      throw new IOException("Failed to atomically move statistics file");
    }
    
    LOGGER.debug("Saved statistics to {}", file);
  }

  /**
   * Load table statistics from a file.
   * 
   * @param file The file to load from
   * @return The loaded statistics
   */
  public static TableStatistics loadStatistics(File file) throws IOException {
    if (!file.exists()) {
      throw new IOException("Statistics file does not exist: " + file);
    }
    
    // Check if file is too old (default: 7 days)
    long maxAge = TimeUnit.DAYS.toMillis(7);
    if (System.currentTimeMillis() - file.lastModified() > maxAge) {
      LOGGER.warn("Statistics file {} is older than {} days, may be stale", 
                  file, TimeUnit.MILLISECONDS.toDays(maxAge));
    }
    
    JsonNode root = MAPPER.readTree(file);
    
    // Validate version
    String version = root.path("version").asText();
    if (!"1.0".equals(version)) {
      throw new IOException("Unsupported statistics file version: " + version);
    }
    
    // Load basic table info
    long rowCount = root.path("rowCount").asLong();
    long dataSize = root.path("dataSize").asLong();
    String sourceHash = root.path("sourceHash").asText(null);
    
    // Load column statistics
    Map<String, ColumnStatistics> columnStats = new HashMap<>();
    JsonNode columnsNode = root.path("columns");
    if (columnsNode.isObject()) {
      columnsNode.fieldNames().forEachRemaining(columnName -> {
        try {
          JsonNode columnNode = columnsNode.get(columnName);
          ColumnStatistics colStats = deserializeColumnStatistics(columnName, columnNode);
          columnStats.put(columnName, colStats);
        } catch (Exception e) {
          LOGGER.warn("Failed to deserialize column statistics for {}: {}", 
                      columnName, e.getMessage());
        }
      });
    }
    
    LOGGER.debug("Loaded statistics from {}: {} rows, {} columns", 
                 file, rowCount, columnStats.size());
    
    return new TableStatistics(rowCount, dataSize, columnStats, sourceHash);
  }

  private static void serializeColumnStatistics(ColumnStatistics colStats, ObjectNode node) {
    node.put("nullCount", colStats.getNullCount());
    node.put("totalCount", colStats.getTotalCount());
    
    // Serialize min/max values
    if (colStats.getMinValue() != null) {
      node.put("minValue", colStats.getMinValue().toString());
      node.put("minType", colStats.getMinValue().getClass().getSimpleName());
    }
    if (colStats.getMaxValue() != null) {
      node.put("maxValue", colStats.getMaxValue().toString());
      node.put("maxType", colStats.getMaxValue().getClass().getSimpleName());
    }
    
    // Serialize HLL sketch
    HyperLogLogSketch hll = colStats.getHllSketch();
    if (hll != null) {
      ObjectNode hllNode = node.putObject("hll");
      hllNode.put("precision", hll.getPrecision());
      hllNode.put("estimate", hll.getEstimate());
      
      // Serialize bucket data as base64
      byte[] buckets = hll.getBuckets();
      String bucketsBase64 = java.util.Base64.getEncoder().encodeToString(buckets);
      hllNode.put("buckets", bucketsBase64);
    }
  }

  private static ColumnStatistics deserializeColumnStatistics(String columnName, JsonNode node) {
    long nullCount = node.path("nullCount").asLong();
    long totalCount = node.path("totalCount").asLong();
    
    // Deserialize min/max values
    Object minValue = deserializeValue(node.path("minValue"), node.path("minType"));
    Object maxValue = deserializeValue(node.path("maxValue"), node.path("maxType"));
    
    // Deserialize HLL sketch
    HyperLogLogSketch hllSketch = null;
    JsonNode hllNode = node.path("hll");
    if (!hllNode.isMissingNode()) {
      int precision = hllNode.path("precision").asInt();
      String bucketsBase64 = hllNode.path("buckets").asText();
      
      if (bucketsBase64 != null && !bucketsBase64.isEmpty()) {
        try {
          byte[] buckets = java.util.Base64.getDecoder().decode(bucketsBase64);
          hllSketch = new HyperLogLogSketch(precision, buckets);
        } catch (Exception e) {
          LOGGER.warn("Failed to deserialize HLL for column {}: {}", columnName, e.getMessage());
        }
      }
    }
    
    return new ColumnStatistics(columnName, minValue, maxValue, nullCount, totalCount, hllSketch);
  }

  private static Object deserializeValue(JsonNode valueNode, JsonNode typeNode) {
    if (valueNode.isMissingNode() || typeNode.isMissingNode()) {
      return null;
    }
    
    String value = valueNode.asText();
    String type = typeNode.asText();
    
    try {
      switch (type) {
        case "Long":
          return Long.parseLong(value);
        case "Integer":
          return Integer.parseInt(value);
        case "Double":
          return Double.parseDouble(value);
        case "Float":
          return Float.parseFloat(value);
        case "Boolean":
          return Boolean.parseBoolean(value);
        case "String":
        default:
          return value;
      }
    } catch (Exception e) {
      LOGGER.warn("Failed to deserialize value '{}' as type '{}': {}", value, type, e.getMessage());
      return value; // Return as string if parsing fails
    }
  }

  /**
   * Clean up old statistics files in a directory.
   * 
   * @param cacheDir The cache directory to clean
   * @param maxAgeMillis Maximum age of files to keep
   */
  public static void cleanupOldStatistics(File cacheDir, long maxAgeMillis) {
    if (!cacheDir.exists() || !cacheDir.isDirectory()) {
      return;
    }
    
    File[] statsFiles = cacheDir.listFiles((dir, name) -> name.endsWith(".apericio_stats"));
    if (statsFiles == null) {
      return;
    }
    
    long cutoffTime = System.currentTimeMillis() - maxAgeMillis;
    int deletedCount = 0;
    
    for (File statsFile : statsFiles) {
      if (statsFile.lastModified() < cutoffTime) {
        if (statsFile.delete()) {
          deletedCount++;
          LOGGER.debug("Deleted old statistics file: {}", statsFile);
        } else {
          LOGGER.warn("Failed to delete old statistics file: {}", statsFile);
        }
      }
    }
    
    if (deletedCount > 0) {
      LOGGER.info("Cleaned up {} old statistics files from {}", deletedCount, cacheDir);
    }
  }

  /**
   * Get the estimated size of statistics files in a directory.
   */
  public static long getStatisticsCacheSize(File cacheDir) {
    if (!cacheDir.exists() || !cacheDir.isDirectory()) {
      return 0;
    }
    
    File[] statsFiles = cacheDir.listFiles((dir, name) -> name.endsWith(".apericio_stats"));
    if (statsFiles == null) {
      return 0;
    }
    
    long totalSize = 0;
    for (File statsFile : statsFiles) {
      totalSize += statsFile.length();
    }
    
    return totalSize;
  }
  
  /**
   * Save a single HLL sketch to a file.
   */
  public static void saveHLLSketch(HyperLogLogSketch sketch, File file) throws IOException {
    ObjectNode root = MAPPER.createObjectNode();
    root.put("version", "1.0");
    root.put("precision", sketch.getPrecision());
    root.put("estimate", sketch.getEstimate());
    
    // Serialize bucket data as base64
    byte[] buckets = sketch.getBuckets();
    String bucketsBase64 = java.util.Base64.getEncoder().encodeToString(buckets);
    root.put("buckets", bucketsBase64);
    
    // Write to file atomically
    File tempFile = new File(file.getParentFile(), file.getName() + ".tmp");
    MAPPER.writeValue(tempFile, root);
    
    if (!tempFile.renameTo(file)) {
      throw new IOException("Failed to atomically move HLL sketch file");
    }
    
    LOGGER.debug("Saved HLL sketch to {}", file);
  }
  
  /**
   * Load a single HLL sketch from a file.
   */
  public static HyperLogLogSketch loadHLLSketch(File file) throws IOException {
    if (!file.exists()) {
      throw new IOException("HLL sketch file does not exist: " + file);
    }
    
    JsonNode root = MAPPER.readTree(file);
    
    int precision = root.path("precision").asInt();
    String bucketsBase64 = root.path("buckets").asText();
    
    if (bucketsBase64 == null || bucketsBase64.isEmpty()) {
      throw new IOException("Invalid HLL sketch file: missing bucket data");
    }
    
    try {
      byte[] buckets = java.util.Base64.getDecoder().decode(bucketsBase64);
      HyperLogLogSketch sketch = new HyperLogLogSketch(precision, buckets);
      LOGGER.debug("Loaded HLL sketch from {}: precision={}, estimate={}", 
                   file, precision, sketch.getEstimate());
      return sketch;
    } catch (Exception e) {
      throw new IOException("Failed to deserialize HLL sketch: " + e.getMessage(), e);
    }
  }
}