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
package org.apache.calcite.adapter.file.execution.duckdb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

/**
 * Configuration options specific to DuckDB execution engine.
 *
 * <p>DuckDB has its own set of configuration parameters that control
 * memory usage, parallelism, and query optimization. These are separate
 * from the general ExecutionEngineConfig options.
 *
 * <p>Example configuration:
 * <pre>{@code
 * {
 *   "executionEngine": "DUCKDB",
 *   "duckdbConfig": {
 *     "memory_limit": "4GB",
 *     "threads": 4,
 *     "max_memory": "80%",
 *     "temp_directory": "/tmp/duckdb",
 *     "enable_progress_bar": false,
 *     "preserve_insertion_order": true
 *   }
 * }
 * }</pre>
 */
public class DuckDBConfig {
  private static final Logger LOGGER = LoggerFactory.getLogger(DuckDBConfig.class);

  // Default configuration values
  public static final String DEFAULT_MEMORY_LIMIT = "1GB";
  public static final int DEFAULT_THREADS = Runtime.getRuntime().availableProcessors();
  public static final String DEFAULT_MAX_MEMORY = "80%";
  public static final boolean DEFAULT_ENABLE_PROGRESS_BAR = false;
  public static final boolean DEFAULT_PRESERVE_INSERTION_ORDER = true;
  public static final boolean DEFAULT_USE_ARROW_OPTIMIZATION = true;
  public static final int DEFAULT_ARROW_BATCH_SIZE = 1024;

  private final String memoryLimit;
  private final int threads;
  private final String maxMemory;
  private final String tempDirectory;
  private final boolean enableProgressBar;
  private final boolean preserveInsertionOrder;
  private final boolean useArrowOptimization;
  private final int arrowBatchSize;
  private final Properties additionalSettings;

  /**
   * Creates DuckDB configuration with default values.
   */
  public DuckDBConfig() {
    this(DEFAULT_MEMORY_LIMIT, DEFAULT_THREADS, DEFAULT_MAX_MEMORY, null,
        DEFAULT_ENABLE_PROGRESS_BAR, DEFAULT_PRESERVE_INSERTION_ORDER,
        DEFAULT_USE_ARROW_OPTIMIZATION, DEFAULT_ARROW_BATCH_SIZE, null);
  }

  /**
   * Creates DuckDB configuration from a map (typically from JSON/YAML config).
   *
   * @param configMap configuration map with DuckDB settings
   */
  public DuckDBConfig(Map<String, Object> configMap) {
    this.memoryLimit = (String) configMap.getOrDefault("memory_limit", DEFAULT_MEMORY_LIMIT);

    Object threadsObj = configMap.get("threads");
    this.threads = threadsObj instanceof Number
        ? ((Number) threadsObj).intValue()
        : DEFAULT_THREADS;

    this.maxMemory = (String) configMap.getOrDefault("max_memory", DEFAULT_MAX_MEMORY);
    this.tempDirectory = (String) configMap.get("temp_directory");

    this.enableProgressBar = Boolean.TRUE.equals(configMap.get("enable_progress_bar"));
    this.preserveInsertionOrder =
        Boolean.TRUE.equals(configMap.getOrDefault("preserve_insertion_order", DEFAULT_PRESERVE_INSERTION_ORDER));

    this.useArrowOptimization =
        Boolean.TRUE.equals(configMap.getOrDefault("use_arrow_optimization", DEFAULT_USE_ARROW_OPTIMIZATION));

    Object batchSizeObj = configMap.get("arrow_batch_size");
    this.arrowBatchSize = batchSizeObj instanceof Number
        ? ((Number) batchSizeObj).intValue()
        : DEFAULT_ARROW_BATCH_SIZE;

    // Store any additional settings that aren't explicitly handled
    this.additionalSettings = new Properties();
    for (Map.Entry<String, Object> entry : configMap.entrySet()) {
      String key = entry.getKey();
      if (!isKnownSetting(key) && entry.getValue() != null) {
        additionalSettings.setProperty(key, entry.getValue().toString());
      }
    }
  }

  /**
   * Creates DuckDB configuration with all parameters.
   */
  public DuckDBConfig(String memoryLimit, int threads, String maxMemory,
      String tempDirectory, boolean enableProgressBar, boolean preserveInsertionOrder,
      boolean useArrowOptimization, int arrowBatchSize, Properties additionalSettings) {
    this.memoryLimit = memoryLimit != null ? memoryLimit : DEFAULT_MEMORY_LIMIT;
    this.threads = threads > 0 ? threads : DEFAULT_THREADS;
    this.maxMemory = maxMemory != null ? maxMemory : DEFAULT_MAX_MEMORY;
    this.tempDirectory = tempDirectory;
    this.enableProgressBar = enableProgressBar;
    this.preserveInsertionOrder = preserveInsertionOrder;
    this.useArrowOptimization = useArrowOptimization;
    this.arrowBatchSize = arrowBatchSize > 0 ? arrowBatchSize : DEFAULT_ARROW_BATCH_SIZE;
    this.additionalSettings = additionalSettings != null ? additionalSettings : new Properties();
  }

  /**
   * Converts this configuration to DuckDB SET statements that can be executed.
   *
   * @return array of SQL SET statements to configure DuckDB
   */
  public String[] toDuckDBSettings() {
    java.util.List<String> settings = new java.util.ArrayList<>();

    // Core memory and threading settings
    settings.add("SET memory_limit = '" + memoryLimit + "'");
    settings.add("SET threads = " + threads);
    settings.add("SET max_memory = '" + maxMemory + "'");

    if (tempDirectory != null) {
      settings.add("SET temp_directory = '" + tempDirectory + "'");
    }

    settings.add("SET enable_progress_bar = " + enableProgressBar);
    settings.add("SET preserve_insertion_order = " + preserveInsertionOrder);

    // Add any additional settings
    for (Map.Entry<Object, Object> entry : additionalSettings.entrySet()) {
      String key = entry.getKey().toString();
      String value = entry.getValue().toString();

      // Try to determine if value needs quotes
      if (isNumericValue(value) || isBooleanValue(value)) {
        settings.add("SET " + key + " = " + value);
      } else {
        settings.add("SET " + key + " = '" + value + "'");
      }
    }

    return settings.toArray(new String[0]);
  }

  private boolean isKnownSetting(String key) {
    return "memory_limit".equals(key) || "threads".equals(key) || "max_memory".equals(key)
        || "temp_directory".equals(key) || "enable_progress_bar".equals(key)
        || "preserve_insertion_order".equals(key) || "use_arrow_optimization".equals(key)
        || "arrow_batch_size".equals(key);
  }

  private boolean isNumericValue(String value) {
    try {
      Double.parseDouble(value);
      return true;
    } catch (NumberFormatException e) {
      return false;
    }
  }

  private boolean isBooleanValue(String value) {
    return "true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value);
  }

  // Getters
  public String getMemoryLimit() {
    return memoryLimit;
  }

  public int getThreads() {
    return threads;
  }

  public String getMaxMemory() {
    return maxMemory;
  }

  public String getTempDirectory() {
    return tempDirectory;
  }

  public boolean isEnableProgressBar() {
    return enableProgressBar;
  }

  public boolean isPreserveInsertionOrder() {
    return preserveInsertionOrder;
  }

  public Properties getAdditionalSettings() {
    return additionalSettings;
  }

  public boolean isUseArrowOptimization() {
    return useArrowOptimization;
  }

  public int getArrowBatchSize() {
    return arrowBatchSize;
  }

  @Override public String toString() {
    return "DuckDBConfig{" +
        "memoryLimit='" + memoryLimit + '\'' +
        ", threads=" + threads +
        ", maxMemory='" + maxMemory + '\'' +
        ", tempDirectory='" + tempDirectory + '\'' +
        ", enableProgressBar=" + enableProgressBar +
        ", preserveInsertionOrder=" + preserveInsertionOrder +
        ", useArrowOptimization=" + useArrowOptimization +
        ", arrowBatchSize=" + arrowBatchSize +
        ", additionalSettings=" + additionalSettings.size() + " items" +
        '}';
  }
}
