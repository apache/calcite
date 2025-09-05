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
package org.apache.calcite.adapter.sec;

import org.apache.calcite.adapter.file.FileSchemaFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Schema factory for XBRL with embeddings that provides smart defaults.
 *
 * This factory automatically configures:
 * - DuckDB as the execution engine
 * - Embedding configuration with static model files
 * - Vector similarity functions
 * - Standard XBRL tables
 *
 * Users only need to specify the data directory in their model files.
 */
public class SecEmbeddingSchemaFactory implements SchemaFactory {
  private static final Logger LOGGER = Logger.getLogger(SecEmbeddingSchemaFactory.class.getName());

  // Default configurations
  private static final Map<String, Object> DEFAULT_CONFIG = new HashMap<>();

  static {
    // Default execution engine
    DEFAULT_CONFIG.put("executionEngine", "duckdb");

    // Default schema name if not provided
    DEFAULT_CONFIG.put("name", "XBRL");

    // Default factory (self-reference for simplified configs)
    DEFAULT_CONFIG.put("factory", "org.apache.calcite.adapter.sec.SecEmbeddingSchemaFactory");

    // Default version
    DEFAULT_CONFIG.put("version", "1.0");

    // Default embedding configuration
    Map<String, Object> embeddingConfig = new HashMap<>();
    embeddingConfig.put("enabled", true);
    embeddingConfig.put("modelLoader", "org.apache.calcite.adapter.sec.EmbeddingModelLoader");
    embeddingConfig.put("vocabularyPath", "/models/financial-vocabulary.txt");
    embeddingConfig.put("contextMappingsPath", "/models/context-mappings.json");
    embeddingConfig.put("embeddingDimension", 128);
    embeddingConfig.put("generateOnConversion", true);
    DEFAULT_CONFIG.put("embeddingConfig", embeddingConfig);

    // Default vector functions
    String[] vectorFunctions = {
        "COSINE_SIMILARITY",
        "COSINE_DISTANCE",
        "EUCLIDEAN_DISTANCE",
        "DOT_PRODUCT",
        "VECTORS_SIMILAR",
        "VECTOR_NORM",
        "NORMALIZE_VECTOR"
    };
    DEFAULT_CONFIG.put("vectorFunctions", vectorFunctions);

    // Default tables
    Map<String, Object>[] tables = new Map[4];

    tables[0] = new HashMap<>();
    tables[0].put("name", "financial_line_items");
    tables[0].put("partitioned", true);

    tables[1] = new HashMap<>();
    tables[1].put("name", "company_info");
    tables[1].put("partitioned", false);

    tables[2] = new HashMap<>();
    tables[2].put("name", "footnotes");
    tables[2].put("partitioned", true);

    tables[3] = new HashMap<>();
    tables[3].put("name", "document_embeddings");
    tables[3].put("partitioned", true);
    tables[3].put("vectorColumn", "embedding_vector");
    tables[3].put("contextColumn", "chunk_context");

    DEFAULT_CONFIG.put("tables", tables);
  }

  @Override public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
    LOGGER.info("Creating XBRL schema with embeddings: " + name);

    // Merge defaults with user-provided configuration
    Map<String, Object> config = new HashMap<>(DEFAULT_CONFIG);

    // Apply connection parameters if available (these override environment variables)
    Map<String, String> connectionParams = getConnectionParameters();
    applyConnectionParameters(config, connectionParams);

    // Check for dataDirectory environment variable (if not set by connection params)
    if (!config.containsKey("dataDirectory")) {
      String envDataDir = System.getenv("XBRL_DATA_DIRECTORY");
      if (envDataDir != null && !envDataDir.isEmpty()) {
        LOGGER.info("Using XBRL_DATA_DIRECTORY from environment: " + envDataDir);
        config.put("dataDirectory", envDataDir);
      }
    }

    // User config overrides defaults
    for (Map.Entry<String, Object> entry : operand.entrySet()) {
      String key = entry.getKey();
      Object value = entry.getValue();

      // For nested configs like embeddingConfig, merge instead of replace
      if (key.equals("embeddingConfig") && value instanceof Map) {
        Map<String, Object> defaultEmbedding = (Map<String, Object>) config.get("embeddingConfig");
        Map<String, Object> userEmbedding = (Map<String, Object>) value;
        Map<String, Object> mergedEmbedding = new HashMap<>(defaultEmbedding);
        mergedEmbedding.putAll(userEmbedding);
        config.put("embeddingConfig", mergedEmbedding);
      } else {
        config.put(key, value);
      }
    }

    // Handle dataDirectory for relative directory paths
    String dataDirectory = (String) config.get("dataDirectory");
    String directory = (String) config.get("directory");

    // If no directory specified, default to "sec-data/parquet" in working directory
    if (directory == null || directory.isEmpty()) {
      if (dataDirectory != null) {
        directory = new File(dataDirectory, "parquet").getPath();
      } else {
        directory = new File(System.getProperty("user.dir"), "sec-data/parquet").getPath();
      }
      config.put("directory", directory);
      LOGGER.info("Using default directory: " + directory);
    } else if (dataDirectory != null && !new File(directory).isAbsolute()) {
      // Combine dataDirectory with relative directory path
      File fullPath = new File(dataDirectory, directory);
      config.put("directory", fullPath.getAbsolutePath());
      LOGGER.info("Resolved directory path: " + fullPath.getAbsolutePath());
    }

    // Log effective configuration
    LOGGER.fine("Effective configuration:");
    LOGGER.fine("  Directory: " + config.get("directory"));
    LOGGER.fine("  Execution Engine: " + config.get("executionEngine"));
    LOGGER.fine("  Embeddings Enabled: " +
        ((Map<String, Object>)config.get("embeddingConfig")).get("enabled"));

    // Create the actual schema using FileSchemaFactory
    Schema schema;
    try {
      // Use FileSchemaFactory for the actual schema creation
      FileSchemaFactory fileFactory = FileSchemaFactory.INSTANCE;
      schema = fileFactory.create(parentSchema, name, config);

      // Register vector functions
      if (parentSchema != null && name != null) {
        registerVectorFunctions(parentSchema.add(name, schema));
      }

      LOGGER.info("Successfully created XBRL schema with embeddings and vector functions");

    } catch (Exception e) {
      LOGGER.warning("Failed to create schema with FileSchemaFactory, falling back to basic schema: "
          + e.getMessage());
      // Fallback to a basic schema if FileSchemaFactory fails
      schema = createBasicSchema(config);
    }

    return schema;
  }

  /**
   * Get connection parameters from the current thread context.
   * Supports both short names (ciks, startYear) and fully qualified names (calcite.sec.ciks).
   * Short names take precedence for ease of use.
   */
  private Map<String, String> getConnectionParameters() {
    Map<String, String> params = new HashMap<>();

    // Common parameter aliases - these are the most frequently used
    String[] commonParams = {
        "ciks", "startYear", "endYear", "filingTypes",
        "dataDirectory", "debug", "embeddingDimension", "executionEngine"
    };

    // First check for short parameter names (higher priority)
    for (String param : commonParams) {
      String value = System.getProperty(param);
      if (value != null) {
        params.put(param, value);
        LOGGER.fine("Found short parameter: " + param + "=" + value);
      }
    }

    // Then check for fully qualified names with "calcite.sec." prefix
    // These can override the short names if both are present
    for (String key : System.getProperties().stringPropertyNames()) {
      if (key.startsWith("calcite.sec.")) {
        String paramName = key.substring("calcite.sec.".length());
        String value = System.getProperty(key);
        // Only override if not already set by short name, or if explicitly qualified
        if (!params.containsKey(paramName) || key.startsWith("calcite.sec.")) {
          params.put(paramName, value);
          LOGGER.fine("Found qualified parameter: " + key + "=" + value);
        }
      }
    }

    // Also check for legacy "calcite." prefix without "sec"
    for (String key : System.getProperties().stringPropertyNames()) {
      if (key.startsWith("calcite.") && !key.startsWith("calcite.sec.")) {
        // Map common Calcite parameters
        if (key.equals("calcite.model")) continue; // Skip model parameter
        String paramName = key.substring("calcite.".length());
        if (!params.containsKey(paramName)) {
          params.put(paramName, System.getProperty(key));
          LOGGER.fine("Found legacy parameter: " + key + "=" + System.getProperty(key));
        }
      }
    }

    return params;
  }

  /**
   * Apply connection parameters to configuration.
   * Connection parameters mirror environment variables and override them.
   *
   * Supported parameters:
   * - dataDirectory: Base directory for XBRL data
   * - ciks: Company identifiers (CIKs, tickers, or groups)
   * - filingTypes: Comma-separated list of filing types
   * - startYear: Start year for data collection
   * - endYear: End year for data collection
   * - embeddingDimension: Dimension of embedding vectors
   * - executionEngine: Query execution engine (duckdb, parquet, etc.)
   */
  private void applyConnectionParameters(Map<String, Object> config, Map<String, String> params) {
    // Data directory
    if (params.containsKey("dataDirectory")) {
      config.put("dataDirectory", params.get("dataDirectory"));
      LOGGER.info("Using dataDirectory from connection: " + params.get("dataDirectory"));
    }

    // CIKs - support multiple formats
    if (params.containsKey("ciks")) {
      String cikParam = params.get("ciks");
      if (cikParam.contains(",")) {
        config.put("ciks", Arrays.asList(cikParam.split(",")));
      } else {
        config.put("ciks", cikParam);
      }
      LOGGER.info("Using CIKs from connection: " + cikParam);
    }

    // Filing types
    if (params.containsKey("filingTypes")) {
      config.put("filingTypes", Arrays.asList(params.get("filingTypes").split(",")));
      LOGGER.info("Using filingTypes from connection: " + params.get("filingTypes"));
    }

    // Year range
    if (params.containsKey("startYear")) {
      config.put("startYear", Integer.parseInt(params.get("startYear")));
      LOGGER.info("Using startYear from connection: " + params.get("startYear"));
    }
    if (params.containsKey("endYear")) {
      config.put("endYear", Integer.parseInt(params.get("endYear")));
      LOGGER.info("Using endYear from connection: " + params.get("endYear"));
    }

    // Embedding configuration
    if (params.containsKey("embeddingDimension")) {
      Map<String, Object> embeddingConfig = (Map<String, Object>) config.get("embeddingConfig");
      embeddingConfig.put("embeddingDimension", Integer.parseInt(params.get("embeddingDimension")));
      LOGGER.info("Using embeddingDimension from connection: " + params.get("embeddingDimension"));
    }

    // Execution engine
    if (params.containsKey("executionEngine")) {
      config.put("executionEngine", params.get("executionEngine"));
      LOGGER.info("Using executionEngine from connection: " + params.get("executionEngine"));
    }

    // Debug mode
    if (params.containsKey("debug")) {
      boolean debug = Boolean.parseBoolean(params.get("debug"));
      if (debug) {
        Logger.getLogger("org.apache.calcite.adapter.sec").setLevel(java.util.logging.Level.FINE);
        LOGGER.info("Debug mode enabled");
      }
    }
  }

  /**
   * Register vector similarity functions with the schema.
   */
  private void registerVectorFunctions(SchemaPlus schema) {
    if (schema == null) return;

    try {
      // Register all vector functions
      schema.add("COSINE_SIMILARITY",
          ScalarFunctionImpl.create(SecVectorFunctions.class, "cosineSimilarity"));
      schema.add("COSINE_DISTANCE",
          ScalarFunctionImpl.create(SecVectorFunctions.class, "cosineDistance"));
      schema.add("EUCLIDEAN_DISTANCE",
          ScalarFunctionImpl.create(SecVectorFunctions.class, "euclideanDistance"));
      schema.add("DOT_PRODUCT",
          ScalarFunctionImpl.create(SecVectorFunctions.class, "dotProduct"));
      schema.add("VECTORS_SIMILAR",
          ScalarFunctionImpl.create(SecVectorFunctions.class, "vectorsSimilar"));
      schema.add("VECTOR_NORM",
          ScalarFunctionImpl.create(SecVectorFunctions.class, "vectorNorm"));
      schema.add("NORMALIZE_VECTOR",
          ScalarFunctionImpl.create(SecVectorFunctions.class, "normalizeVector"));

      LOGGER.fine("Registered 7 vector functions");
    } catch (Exception e) {
      LOGGER.warning("Failed to register vector functions: " + e.getMessage());
    }
  }

  /**
   * Create a basic schema if the file factory fails.
   */
  private Schema createBasicSchema(Map<String, Object> config) {
    return new org.apache.calcite.schema.impl.AbstractSchema() {
      @Override protected Map<String, org.apache.calcite.schema.Table> getTableMap() {
        // Return empty table map for now
        return new HashMap<>();
      }
    };
  }
}
