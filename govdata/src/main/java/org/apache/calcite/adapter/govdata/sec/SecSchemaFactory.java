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
package org.apache.calcite.adapter.govdata.sec;

import org.apache.calcite.adapter.file.FileSchemaFactory;
import org.apache.calcite.adapter.file.metadata.ConversionMetadata;
import org.apache.calcite.model.JsonTable;
import org.apache.calcite.schema.ConstraintCapableSchemaFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.time.Year;

/**
 * Factory for SEC schemas that extends FileSchema with SEC-specific capabilities.
 * Uses file adapter's infrastructure for HTTP operations, HTML processing, and
 * partitioned Parquet storage.
 *
 * <p>This factory leverages the file adapter's FileConversionManager for XBRL
 * processing and HtmlToJsonConverter for HTML table extraction.
 */
public class SecSchemaFactory implements ConstraintCapableSchemaFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(SecSchemaFactory.class);

  static {
    LOGGER.debug("SecSchemaFactory class loaded");
  }

  // Standard SEC data cache directory - XBRL files are immutable, cache forever
  // Default to /Volumes/T9 for testing, can be overridden with -Dsec.data.home
  private static final String DEFAULT_SEC_HOME = "/Volumes/T9/calcite-sec-cache";
  private static final String SEC_DATA_HOME = System.getProperty("sec.data.home", DEFAULT_SEC_HOME);
  private static final String SEC_RAW_DIR = SEC_DATA_HOME + "/sec-data";
  private static final String SEC_PARQUET_DIR = SEC_DATA_HOME + "/sec-parquet";

  // Parallel processing configuration
  private static final int DOWNLOAD_THREADS = 3; // Reduced to 3 concurrent downloads for better rate limiting
  private static final int CONVERSION_THREADS = 8; // 8 concurrent conversions
  private static final int SEC_RATE_LIMIT_PER_SECOND = 8; // Conservative: 8 requests/sec (SEC allows 10)
  private static final long INITIAL_RATE_LIMIT_DELAY_MS = 125; // Initial: 125ms between requests (8/sec)
  private static final long MAX_RATE_LIMIT_DELAY_MS = 500; // Max: 500ms between requests (2/sec)

  // Dynamic rate limiting
  private final AtomicLong currentRateLimitDelayMs = new AtomicLong(INITIAL_RATE_LIMIT_DELAY_MS);

  // Thread pools and rate limiting
  private ExecutorService downloadExecutor;
  private ExecutorService conversionExecutor;
  private final Semaphore rateLimiter = new Semaphore(1); // One permit, released every 125ms
  private final ConcurrentLinkedQueue<CompletableFuture<Void>> downloadFutures = new ConcurrentLinkedQueue<>();
  private final ConcurrentLinkedQueue<CompletableFuture<Void>> conversionFutures = new ConcurrentLinkedQueue<>();
  private final ConcurrentLinkedQueue<FilingToDownload> retryQueue = new ConcurrentLinkedQueue<>();
  private final List<File> scheduledForReconversion = new ArrayList<>();
  private final Set<String> downloadedInThisCycle = java.util.concurrent.ConcurrentHashMap.newKeySet();
  private final AtomicInteger totalDownloads = new AtomicInteger(0);
  private final AtomicInteger completedDownloads = new AtomicInteger(0);
  private final AtomicInteger totalConversions = new AtomicInteger(0);
  private final AtomicInteger completedConversions = new AtomicInteger(0);
  private final AtomicInteger rateLimitHits = new AtomicInteger(0);
  private Map<String, Object> currentOperand; // Store operand for table auto-discovery
  private RSSRefreshMonitor rssMonitor; // RSS monitor for automatic refresh

  public static final SecSchemaFactory INSTANCE = new SecSchemaFactory();

  // Constraint metadata support
  private Map<String, Map<String, Object>> tableConstraints = new HashMap<>();

  static {
    LOGGER.debug("SecSchemaFactory class loaded");
  }

  /**
   * Returns true to enable constraint metadata support in SEC adapter.
   * This allows model files to define primary keys, foreign keys, and unique keys
   * for SEC tables to support query optimization and JDBC metadata.
   */
  @Override
  public boolean supportsConstraints() {
    return true;
  }

  /**
   * Receives constraint metadata from ModelHandler for tables defined in model files.
   * The constraints are stored and later used by the FileSchemaFactory to provide
   * table statistics and constraint metadata.
   */
  @Override
  public void setTableConstraints(Map<String, Map<String, Object>> tableConstraints, List<JsonTable> tableDefinitions) {
    LOGGER.debug("Received constraint metadata for {} tables", tableConstraints.size());
    this.tableConstraints = new HashMap<>(tableConstraints);
    
    // Log constraints for debugging
    for (Map.Entry<String, Map<String, Object>> entry : tableConstraints.entrySet()) {
      LOGGER.debug("Table '{}' has constraints: {}", entry.getKey(), entry.getValue());
    }
  }

  private synchronized void initializeExecutors() {
    if (downloadExecutor == null || downloadExecutor.isShutdown()) {
      downloadExecutor = Executors.newFixedThreadPool(DOWNLOAD_THREADS);
    }
    if (conversionExecutor == null || conversionExecutor.isShutdown()) {
      conversionExecutor = Executors.newFixedThreadPool(CONVERSION_THREADS);
    }
  }

  private void shutdownExecutors() {
    try {
      if (downloadExecutor != null) {
        downloadExecutor.shutdown();
        if (!downloadExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
          downloadExecutor.shutdownNow();
        }
      }
      if (conversionExecutor != null) {
        conversionExecutor.shutdown();
        if (!conversionExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
          conversionExecutor.shutdownNow();
        }
      }
    } catch (InterruptedException e) {
      if (downloadExecutor != null) {
        downloadExecutor.shutdownNow();
      }
      if (conversionExecutor != null) {
        conversionExecutor.shutdownNow();
      }
      Thread.currentThread().interrupt();
    }
  }

  public SecSchemaFactory() {
    LOGGER.debug("SecSchemaFactory constructor called");
    LOGGER.debug("SecSchemaFactory constructor called");
  }

  /**
   * Loads default configuration from sec-schema-factory.defaults.json resource.
   * Returns null if the resource cannot be loaded.
   */
  private Map<String, Object> loadDefaults() {
    try (InputStream is = getClass().getResourceAsStream("/sec-schema-factory.defaults.json")) {
      if (is == null) {
        LOGGER.debug("No defaults file found at /sec-schema-factory.defaults.json");
        return null;
      }

      ObjectMapper mapper = new ObjectMapper();
      JsonNode root = mapper.readTree(is);
      JsonNode operandNode = root.get("operand");
      if (operandNode == null) {
        LOGGER.debug("No 'operand' section in defaults file");
        return null;
      }

      // Convert JsonNode to Map
      Map<String, Object> defaults = mapper.convertValue(operandNode, Map.class);

      // Process environment variable substitutions
      processEnvironmentVariables(defaults);

      return defaults;
    } catch (Exception e) {
      LOGGER.warn("Failed to load defaults from sec-schema-factory.defaults.json: " + e.getMessage());
      return null;
    }
  }

  /**
   * Recursively processes a map to substitute environment variables.
   * Supports ${VAR_NAME} syntax for environment variable substitution.
   */
  private void processEnvironmentVariables(Map<String, Object> map) {
    Pattern envPattern = Pattern.compile("\\$\\{([A-Z_][A-Z0-9_]*)\\}");

    for (Map.Entry<String, Object> entry : map.entrySet()) {
      Object value = entry.getValue();

      if (value instanceof String) {
        String strValue = (String) value;
        Matcher matcher = envPattern.matcher(strValue);

        if (matcher.matches()) {
          // Full match - replace entire value
          String envVar = matcher.group(1);
          String envValue = System.getenv(envVar);

          if (envValue == null && "CURRENT_YEAR".equals(envVar)) {
            // Special handling for CURRENT_YEAR
            envValue = String.valueOf(Year.now().getValue());
          }

          if (envValue != null) {
            // Try to parse as integer if it looks like a number
            if (envValue.matches("\\d+")) {
              try {
                entry.setValue(Integer.parseInt(envValue));
              } catch (NumberFormatException e) {
                entry.setValue(envValue);
              }
            } else {
              entry.setValue(envValue);
            }
          } else {
            LOGGER.debug("Environment variable {} not set, keeping placeholder", envVar);
          }
        } else if (matcher.find()) {
          // Partial match - substitute within string
          StringBuffer sb = new StringBuffer();
          matcher.reset();
          while (matcher.find()) {
            String envVar = matcher.group(1);
            String envValue = System.getenv(envVar);

            if (envValue == null && "CURRENT_YEAR".equals(envVar)) {
              envValue = String.valueOf(Year.now().getValue());
            }

            if (envValue != null) {
              matcher.appendReplacement(sb, envValue);
            } else {
              matcher.appendReplacement(sb, matcher.group(0)); // Keep original
            }
          }
          matcher.appendTail(sb);
          entry.setValue(sb.toString());
        }
      } else if (value instanceof Map) {
        // Recursive processing for nested maps
        processEnvironmentVariables((Map<String, Object>) value);
      } else if (value instanceof List) {
        // Process lists (though we don't expect env vars in lists for SEC adapter)
        List<?> list = (List<?>) value;
        for (int i = 0; i < list.size(); i++) {
          if (list.get(i) instanceof String) {
            String strValue = (String) list.get(i);
            Matcher matcher = envPattern.matcher(strValue);
            if (matcher.matches()) {
              String envVar = matcher.group(1);
              String envValue = System.getenv(envVar);
              if (envValue == null && "CURRENT_YEAR".equals(envVar)) {
                envValue = String.valueOf(Year.now().getValue());
              }
              if (envValue != null) {
                ((List<Object>) list).set(i, envValue);
              }
            }
          }
        }
      }
    }
  }

  /**
   * Merges default values into the operand map, without overwriting existing values.
   * This ensures that explicit configuration takes precedence over defaults.
   */
  private void applyDefaults(Map<String, Object> operand, Map<String, Object> defaults) {
    if (defaults == null) {
      return;
    }

    for (Map.Entry<String, Object> entry : defaults.entrySet()) {
      String key = entry.getKey();
      Object defaultValue = entry.getValue();

      if (!operand.containsKey(key)) {
        // Key not present, add default
        operand.put(key, defaultValue);
      } else if (defaultValue instanceof Map && operand.get(key) instanceof Map) {
        // Both are maps, merge recursively
        Map<String, Object> existingMap = (Map<String, Object>) operand.get(key);
        Map<String, Object> defaultMap = (Map<String, Object>) defaultValue;
        applyDefaults(existingMap, defaultMap);
      }
      // If operand already has the key with a non-map value, keep the existing value
    }
  }

  @Override public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
    LOGGER.debug("SecSchemaFactory.create() called with operand: {}", operand);
    LOGGER.debug("SecSchemaFactory.create() called");
    LOGGER.debug("Operand: {}", operand);

    // Create mutable copy of operand to allow modifications
    Map<String, Object> mutableOperand = new HashMap<>(operand);

    // Load and apply defaults before processing
    Map<String, Object> defaults = loadDefaults();
    if (defaults != null) {
      LOGGER.debug("Applying defaults from sec-schema-factory.defaults.json");
      applyDefaults(mutableOperand, defaults);
      LOGGER.debug("Operand after applying defaults: {}", mutableOperand);
    }

    this.currentOperand = mutableOperand; // Store for table auto-discovery

    // Determine cache directory
    String configuredDir = (String) mutableOperand.get("directory");
    if (configuredDir == null) {
      configuredDir = (String) mutableOperand.get("cacheDirectory");
    }
    String cacheHome = configuredDir != null ? configuredDir : SEC_DATA_HOME;

    // Handle SEC data download if configured
    LOGGER.debug("About to check shouldDownloadData");
    LOGGER.debug("Checking shouldDownloadData...");
    if (shouldDownloadData(mutableOperand)) {
      LOGGER.debug("shouldDownloadData = true, calling downloadSecData");
      // Get base directory from operand
      if (configuredDir != null) {
        File baseDir = new File(configuredDir);
        // Rebuild manifest from existing files first
        rebuildManifestFromExistingFiles(baseDir);
      }
      downloadSecData(mutableOperand);

      // CRITICAL: Wait for all downloads and conversions to complete
      // The downloadSecData method starts async tasks but returns immediately
      // We need to wait for them to complete before creating the FileSchema
      waitForAllConversions();
    } else {
      LOGGER.debug("shouldDownloadData = false");
    }

    // Pre-define partitioned tables using the partitionedTables format
    // These will be resolved by FileSchemaFactory after downloads complete
    List<Map<String, Object>> partitionedTables = new ArrayList<>();

    // Define partition configuration (shared by both tables)
    Map<String, Object> partitionConfig = new HashMap<>();
    partitionConfig.put("style", "hive");  // Use Hive-style partitioning (key=value)

    // Define partition columns
    List<Map<String, Object>> columnDefs = new ArrayList<>();

    Map<String, Object> cikCol = new HashMap<>();
    cikCol.put("name", "cik");
    cikCol.put("type", "VARCHAR");
    columnDefs.add(cikCol);

    Map<String, Object> filingTypeCol = new HashMap<>();
    filingTypeCol.put("name", "filing_type");
    filingTypeCol.put("type", "VARCHAR");
    columnDefs.add(filingTypeCol);

    Map<String, Object> yearCol = new HashMap<>();
    yearCol.put("name", "year");
    yearCol.put("type", "INTEGER");
    columnDefs.add(yearCol);

    partitionConfig.put("columnDefinitions", columnDefs);

    // Define financial_line_items as a partitioned table with constraints
    Map<String, Object> financialLineItems = new HashMap<>();
    financialLineItems.put("name", "financial_line_items");
    financialLineItems.put("pattern", "cik=*/filing_type=*/year=*/[!.]*_facts.parquet");
    financialLineItems.put("partitions", partitionConfig);

    // Add constraint definitions if enabled
    Boolean enableConstraints = (Boolean) mutableOperand.get("enableConstraints");
    if (enableConstraints == null || enableConstraints) {
      Map<String, Object> constraints = new HashMap<>();
      // Primary key: (cik, filing_type, year, filing_date, concept, context_ref)
      constraints.put("primaryKey", Arrays.asList("cik", "filing_type", "year",
          "filing_date", "concept", "context_ref"));

      // Foreign key to filing_contexts table
      Map<String, Object> fk = new HashMap<>();
      fk.put("sourceTable", Arrays.asList("SEC", "financial_line_items"));
      fk.put("columns", Arrays.asList("cik", "filing_type", "year", "filing_date", "context_ref"));
      fk.put("targetTable", Arrays.asList("SEC", "filing_contexts"));
      fk.put("targetColumns", Arrays.asList("cik", "filing_type", "year", "filing_date", "context_id"));
      constraints.put("foreignKeys", Arrays.asList(fk));

      financialLineItems.put("constraints", constraints);
    }

    partitionedTables.add(financialLineItems);

    // Define filing_contexts as a partitioned table with constraints
    Map<String, Object> filingContexts = new HashMap<>();
    filingContexts.put("name", "filing_contexts");
    filingContexts.put("pattern", "cik=*/filing_type=*/year=*/[!.]*_contexts.parquet");
    filingContexts.put("partitions", partitionConfig);

    if (enableConstraints == null || enableConstraints) {
      Map<String, Object> constraints = new HashMap<>();
      // Primary key: (cik, filing_type, year, filing_date, context_id)
      constraints.put("primaryKey", Arrays.asList("cik", "filing_type", "year",
          "filing_date", "context_id"));
      filingContexts.put("constraints", constraints);
    }

    partitionedTables.add(filingContexts);

    // Define mda_sections as a partitioned table for MD&A paragraphs
    Map<String, Object> mdaSections = new HashMap<>();
    mdaSections.put("name", "mda_sections");
    mdaSections.put("pattern", "cik=*/filing_type=*/year=*/[!.]*_mda.parquet");
    mdaSections.put("partitions", partitionConfig);
    if (enableConstraints == null || enableConstraints) {
      Map<String, Object> constraints = new HashMap<>();
      constraints.put("primaryKey", Arrays.asList("cik", "filing_type", "year",
          "filing_date", "section_id"));
      mdaSections.put("constraints", constraints);
    }
    partitionedTables.add(mdaSections);

    // Define xbrl_relationships as a partitioned table
    Map<String, Object> xbrlRelationships = new HashMap<>();
    xbrlRelationships.put("name", "xbrl_relationships");
    xbrlRelationships.put("pattern", "cik=*/filing_type=*/year=*/[!.]*_relationships.parquet");
    xbrlRelationships.put("partitions", partitionConfig);
    if (enableConstraints == null || enableConstraints) {
      Map<String, Object> constraints = new HashMap<>();
      constraints.put("primaryKey", Arrays.asList("cik", "filing_type", "year",
          "filing_date", "relationship_id"));
      xbrlRelationships.put("constraints", constraints);
    }
    partitionedTables.add(xbrlRelationships);

    // Define insider_transactions table for Forms 3, 4, 5
    Map<String, Object> insiderTransactions = new HashMap<>();
    insiderTransactions.put("name", "insider_transactions");
    insiderTransactions.put("pattern", "cik=*/filing_type=*/year=*/[!.]*_insider.parquet");
    insiderTransactions.put("partitions", partitionConfig);
    if (enableConstraints == null || enableConstraints) {
      Map<String, Object> constraints = new HashMap<>();
      constraints.put("primaryKey", Arrays.asList("cik", "filing_type", "year",
          "filing_date", "transaction_id"));
      insiderTransactions.put("constraints", constraints);
    }
    partitionedTables.add(insiderTransactions);

    // Define earnings_transcripts table for 8-K exhibits
    Map<String, Object> earningsTranscripts = new HashMap<>();
    earningsTranscripts.put("name", "earnings_transcripts");
    earningsTranscripts.put("pattern", "cik=*/filing_type=*/year=*/[!.]*_earnings.parquet");
    earningsTranscripts.put("partitions", partitionConfig);
    if (enableConstraints == null || enableConstraints) {
      Map<String, Object> constraints = new HashMap<>();
      constraints.put("primaryKey", Arrays.asList("cik", "filing_type", "year",
          "filing_date", "transcript_id"));
      earningsTranscripts.put("constraints", constraints);
    }
    partitionedTables.add(earningsTranscripts);

    // Define stock_prices table for daily EOD prices
    Map<String, Object> stockPricesPartitionConfig = new HashMap<>();
    stockPricesPartitionConfig.put("style", "hive");  // Use Hive-style partitioning (key=value)

    // Define partition columns for stock_prices (ticker and year only)
    // CIK will be a regular column in the Parquet files for joins
    List<Map<String, Object>> stockPricesColumnDefs = new ArrayList<>();

    Map<String, Object> tickerCol = new HashMap<>();
    tickerCol.put("name", "ticker");
    tickerCol.put("type", "VARCHAR");
    stockPricesColumnDefs.add(tickerCol);

    Map<String, Object> stockYearCol = new HashMap<>();
    stockYearCol.put("name", "year");
    stockYearCol.put("type", "INTEGER");
    stockPricesColumnDefs.add(stockYearCol);

    stockPricesPartitionConfig.put("columnDefinitions", stockPricesColumnDefs);

    Map<String, Object> stockPrices = new HashMap<>();
    stockPrices.put("name", "stock_prices");
    stockPrices.put("pattern", "stock_prices/ticker=*/year=*/[!.]*_prices.parquet");
    stockPrices.put("partitions", stockPricesPartitionConfig);
    partitionedTables.add(stockPrices);

    // Add vectorized_blobs table when text similarity is enabled
    Map<String, Object> textSimilarityConfig = (Map<String, Object>) operand.get("textSimilarity");
    if (textSimilarityConfig != null && Boolean.TRUE.equals(textSimilarityConfig.get("enabled"))) {
      Map<String, Object> vectorizedBlobsTable = new HashMap<>();
      vectorizedBlobsTable.put("name", "vectorized_blobs");
      vectorizedBlobsTable.put("pattern", "cik=*/filing_type=*/year=*/*_vectorized.parquet");
      vectorizedBlobsTable.put("partitions", partitionConfig);
      partitionedTables.add(vectorizedBlobsTable);
      LOGGER.info("Added vectorized_blobs table configuration for text similarity");
    }

    // Add partitioned tables to operand
    mutableOperand.put("partitionedTables", partitionedTables);

    // Set the directory for FileSchemaFactory to search
    File parquetDir = new File(cacheHome, "sec-parquet");
    mutableOperand.put("directory", parquetDir.getAbsolutePath());

    LOGGER.info("Pre-defined {} partitioned table patterns", partitionedTables.size());

    // Ensure the parquet directory exists
    File secParquetDir = new File(cacheHome, "sec-parquet");

    if (secParquetDir.exists() && secParquetDir.isDirectory()) {
      LOGGER.info("Using SEC parquet cache directory: {}", secParquetDir.getAbsolutePath());

      // Debug: List all .parquet files
      LOGGER.info("DEBUG: Listing all .parquet files in directory:");
      File[] parquetFiles = secParquetDir.listFiles((dir, fileName) -> fileName.endsWith(".parquet"));
      if (parquetFiles != null && parquetFiles.length > 0) {
        for (File f : parquetFiles) {
          LOGGER.info("DEBUG: Found table file: " + f.getName() + " (size=" + f.length() + ")");
        }
      } else {
        LOGGER.warn("DEBUG: No .parquet files found in " + secParquetDir.getAbsolutePath());
      }
    } else {
      // Parquet dir doesn't exist yet, use the base cache directory
      mutableOperand.put("directory", cacheHome);
      LOGGER.info("SEC parquet directory not ready yet, using base: {}", cacheHome);
    }

    // Use parquet for SEC adapter, but pass through original engine to FileSchema
    String originalEngine = (String) operand.get("executionEngine");
    mutableOperand.put("executionEngine", "parquet");
    if (originalEngine != null) {
      mutableOperand.put("secExecutionEngine", originalEngine);
    }

    // Preserve text similarity configuration if present, or enable it by default
    // This ensures COSINE_SIMILARITY and other vector functions are registered
    Map<String, Object> textSimConfig = (Map<String, Object>) mutableOperand.get("textSimilarity");
    if (textSimConfig == null) {
      textSimConfig = new HashMap<>();
      textSimConfig.put("enabled", true);
      mutableOperand.put("textSimilarity", textSimConfig);
    } else if (!Boolean.TRUE.equals(textSimConfig.get("enabled"))) {
      // Ensure it's enabled even if config exists but disabled
      textSimConfig.put("enabled", true);
    }
    LOGGER.info("Text similarity functions configuration: " + textSimConfig);

    // Start RSS monitor if configured
    Map<String, Object> refreshConfig = (Map<String, Object>) mutableOperand.get("refreshMonitoring");
    if (refreshConfig != null && Boolean.TRUE.equals(refreshConfig.get("enabled"))) {
      if (rssMonitor == null) {
        LOGGER.info("Starting RSS refresh monitor");
        rssMonitor = new RSSRefreshMonitor(mutableOperand);
        rssMonitor.start();
      }
    }

    // Now delegate to FileSchemaFactory to create the actual schema
    // with our pre-defined tables and configured directory
    LOGGER.info("Delegating to FileSchemaFactory with modified operand");

    // Add constraint metadata to operand if available
    if (!tableConstraints.isEmpty()) {
      LOGGER.debug("Adding constraint metadata for {} tables to FileSchemaFactory operand", tableConstraints.size());
      mutableOperand.put("tableConstraints", tableConstraints);
    }

    // Check if text similarity is enabled before creating schema
    boolean similarityEnabled = mutableOperand.containsKey("textSimilarity") &&
        mutableOperand.get("textSimilarity") instanceof Map &&
        Boolean.TRUE.equals(((Map<?,?>)mutableOperand.get("textSimilarity")).get("enabled"));

    if (similarityEnabled) {
      // Register the functions on the parent schema BEFORE creating the FileSchema
      // This ensures they're available in the schema resolution chain
      try {
        LOGGER.info("Registering text similarity functions on parent schema before FileSchema creation");
        org.apache.calcite.adapter.file.similarity.SimilarityFunctions.registerFunctions(parentSchema);
      } catch (Exception e) {
        LOGGER.warn("Failed to register similarity functions on parent schema: " + e.getMessage());
      }
    }

    Schema fileSchema = FileSchemaFactory.INSTANCE.create(parentSchema, name, mutableOperand);

    // The FileSchemaFactory should have already registered functions if textSimilarity is enabled
    // The functions should be available through both parent schema and FileSchema registration
    if (similarityEnabled) {
      LOGGER.info("Text similarity enabled - functions should be available through FileSchemaFactory registration");
    }

    return fileSchema;
  }


  private boolean shouldDownloadData(Map<String, Object> operand) {
    Boolean autoDownload = (Boolean) operand.get("autoDownload");
    Boolean useMockData = (Boolean) operand.get("useMockData");
    boolean result = (autoDownload != null && autoDownload) || (useMockData != null && useMockData);
    LOGGER.debug("shouldDownloadData: autoDownload={}, useMockData={}, result={}", autoDownload, useMockData, result);
    return result;
  }

  /**
   * Wait for all downloads and conversions to complete.
   * This is critical to ensure the FileSchema sees the converted Parquet files.
   */
  private void waitForAllConversions() {
    try {
      // Wait for download futures if any are still running
      if (!downloadFutures.isEmpty()) {
        LOGGER.info("Waiting for {} downloads to complete...", downloadFutures.size());
        CompletableFuture<Void> allDownloads =
            CompletableFuture.allOf(downloadFutures.toArray(new CompletableFuture[0]));
        allDownloads.get(45, TimeUnit.MINUTES);
        LOGGER.info("All downloads completed");
      }

      // Wait for conversion futures if any are still running
      if (!conversionFutures.isEmpty()) {
        LOGGER.info("Waiting for {} conversions to complete...", conversionFutures.size());
        CompletableFuture<Void> allConversions =
            CompletableFuture.allOf(conversionFutures.toArray(new CompletableFuture[0]));
        allConversions.get(30, TimeUnit.MINUTES);
        LOGGER.info("All conversions completed: {} files", completedConversions.get());
      }
    } catch (Exception e) {
      LOGGER.warn("Error waiting for downloads/conversions to complete: " + e.getMessage());
    }
  }

  private void addToManifest(File baseDirectory, File xbrlFile) {
    try {
      // Extract CIK and accession from file path
      // Path structure: baseDir/sec-cache/raw/{cik}/{accession}/{file}
      File accessionDir = xbrlFile.getParentFile();
      File cikDir = accessionDir.getParentFile();

      String cik = cikDir.getName();
      String accession = accessionDir.getName();
      // Format accession with dashes
      if (accession.length() == 18) {
        accession = accession.substring(0, 10) + "-" +
                   accession.substring(10, 12) + "-" +
                   accession.substring(12);
      }

      // Check which files were actually created for this accession
      boolean hasVectorized = false;
      Map<String, Object> textSimilarityConfig = (Map<String, Object>) currentOperand.get("textSimilarity");
      if (textSimilarityConfig != null && Boolean.TRUE.equals(textSimilarityConfig.get("enabled"))) {
        // Check if vectorized file exists for this accession in the parquet directory
        File parquetDir = new File(baseDirectory.getParentFile(), "sec-parquet");
        if (parquetDir.exists()) {
          // Look for vectorized files with this accession number
          String accessionNoHyphens = accession.replace("-", "");
          File[] cikDirs = parquetDir.listFiles((dir, name) -> name.startsWith("cik="));
          for (File ckDir : cikDirs != null ? cikDirs : new File[0]) {
            File[] filingTypeDirs = ckDir.listFiles((dir, name) -> name.startsWith("filing_type="));
            for (File ftDir : filingTypeDirs != null ? filingTypeDirs : new File[0]) {
              File[] yearDirs = ftDir.listFiles((dir, name) -> name.startsWith("year="));
              for (File yDir : yearDirs != null ? yearDirs : new File[0]) {
                File[] vectorizedFiles = yDir.listFiles((dir, name) ->
                    name.contains(accessionNoHyphens) && name.endsWith("_vectorized.parquet"));
                if (vectorizedFiles != null && vectorizedFiles.length > 0) {
                  hasVectorized = true;
                  break;
                }
              }
              if (hasVectorized) break;
            }
            if (hasVectorized) break;
          }
        }
      }

      // Include vectorized status in manifest key
      String fileTypes = hasVectorized ? "PROCESSED_WITH_VECTORS" : "PROCESSED";
      String manifestKey = cik + "|" + accession + "|" + fileTypes + "|" + System.currentTimeMillis();

      File manifestFile = new File(baseDirectory, "processed_filings.manifest");
      synchronized (SecSchemaFactory.class) {
        try (PrintWriter pw = new PrintWriter(new FileOutputStream(manifestFile, true))) {
          pw.println(manifestKey);
        }
        LOGGER.debug("Added to manifest after Parquet conversion: " + manifestKey + " (vectorized=" + hasVectorized + ")");
      }
    } catch (Exception e) {
      LOGGER.debug("Could not update manifest: " + e.getMessage());
    }
  }

  private void rebuildManifestFromExistingFiles(File baseDirectory) {
    // Rebuild manifest from existing files on startup
    File manifestFile = new File(baseDirectory, "processed_filings.manifest");
    if (manifestFile.exists()) {
      try {
        long count = Files.lines(manifestFile.toPath()).count();
        LOGGER.info("Manifest already exists with {} entries", count);
        return;
      } catch (Exception e) {
        LOGGER.warn("Could not read manifest: " + e.getMessage());
      }
    }

    LOGGER.info("Building manifest from existing files...");
    Set<String> processedFilings = new HashSet<>();

    File secCacheDir = new File(baseDirectory, "sec-cache");
    if (secCacheDir.exists() && secCacheDir.isDirectory()) {
      File rawDir = new File(secCacheDir, "raw");
      if (rawDir.exists()) {
        // Scan all CIK directories
        File[] cikDirs = rawDir.listFiles(File::isDirectory);
        if (cikDirs != null) {
          for (File cikDir : cikDirs) {
            String cik = cikDir.getName();
            // Scan all accession directories
            File[] accessionDirs = cikDir.listFiles(File::isDirectory);
            if (accessionDirs != null) {
              for (File accessionDir : accessionDirs) {
                String accession = accessionDir.getName();
                // Format accession with dashes
                if (accession.length() == 18) {
                  accession = accession.substring(0, 10) + "-" +
                             accession.substring(10, 12) + "-" +
                             accession.substring(12);
                }

                // Check for HTML files (indicates a filing was downloaded)
                // Exclude macOS metadata files
                File[] htmlFiles = accessionDir.listFiles((dir, name) ->
                    !name.startsWith("._") && (name.endsWith(".htm") || name.endsWith(".html")));
                if (htmlFiles != null && htmlFiles.length > 0) {
                  // For now, add with generic form/date - actual values would need parsing
                  String manifestKey = cik + "|" + accession + "|UNKNOWN|UNKNOWN";
                  processedFilings.add(manifestKey);
                }
              }
            }
          }
        }
      }
    }

    // Write manifest
    if (!processedFilings.isEmpty()) {
      try (PrintWriter pw = new PrintWriter(manifestFile)) {
        for (String filing : processedFilings) {
          pw.println(filing);
        }
        LOGGER.info("Created manifest with {} existing filings", processedFilings.size());
      } catch (Exception e) {
        LOGGER.warn("Could not create manifest: " + e.getMessage());
      }
    }
  }

  private void createMockParquetFiles(File baseDir) {
    try {
      // Create parquet directory structure for mock data
      File secParquetDir = new File(baseDir.getParentFile(), "sec-parquet");
      secParquetDir.mkdirs();

      // Create directories and minimal Parquet files for each table pattern
      // We need at least one file matching each pattern for the table to be discovered

      // Create financial_line_items mock file
      File financialDir = new File(secParquetDir, "cik=0000000001/filing_type=10K/year=2023");
      financialDir.mkdirs();
      File financialFile = new File(financialDir, "0000000001_2023-12-31_facts.parquet");
      createMinimalParquetFile(financialFile);

      // Create filing_contexts mock file
      File contextsFile = new File(financialDir, "0000000001_2023-12-31_contexts.parquet");
      createMinimalParquetFile(contextsFile);

      // Create footnotes mock file
      File footnotesFile = new File(financialDir, "0000000001_2023-12-31_footnotes.parquet");
      createMinimalParquetFile(footnotesFile);

      // Create labels mock file
      File labelsFile = new File(financialDir, "0000000001_2023-12-31_labels.parquet");
      createMinimalParquetFile(labelsFile);

      // Create presentations mock file
      File presentationsFile = new File(financialDir, "0000000001_2023-12-31_presentations.parquet");
      createMinimalParquetFile(presentationsFile);

      // Create company_info mock file (non-partitioned)
      File companyFile = new File(secParquetDir, "company_info.parquet");
      createMinimalParquetFile(companyFile);

      LOGGER.info("Created mock Parquet files in: {}", secParquetDir);
    } catch (Exception e) {
      LOGGER.error("Failed to create mock Parquet files", e);
    }
  }

  private void createMinimalParquetFile(File file) {
    try {
      // Create a minimal Parquet file with proper schema
      // Using Avro schema to create the Parquet file
      org.apache.avro.Schema schema = org.apache.avro.Schema.createRecord("MockRecord", "", "", false);
      List<org.apache.avro.Schema.Field> fields = new ArrayList<>();

      // Add minimal fields based on the table type
      if (file.getName().contains("facts")) {
        // financial_line_items table fields
        // NOTE: Don't include partition columns (cik, filing_type, year) as they come from the path
        fields.add(new org.apache.avro.Schema.Field("filing_date",
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING), null, null));
        fields.add(new org.apache.avro.Schema.Field("concept",
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING), null, null));
        fields.add(new org.apache.avro.Schema.Field("context_ref",
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING), null, null));
        fields.add(new org.apache.avro.Schema.Field("value",
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.DOUBLE), null, null));
        fields.add(new org.apache.avro.Schema.Field("label",
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING), null, null));
        fields.add(new org.apache.avro.Schema.Field("units",
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING), null, null));
        fields.add(new org.apache.avro.Schema.Field("decimals",
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT), null, null));
      } else if (file.getName().contains("contexts")) {
        // filing_contexts table fields
        // NOTE: Don't include partition columns (cik, filing_type, year) as they come from the path
        fields.add(new org.apache.avro.Schema.Field("filing_date",
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING), null, null));
        fields.add(new org.apache.avro.Schema.Field("context_id",
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING), null, null));
        fields.add(new org.apache.avro.Schema.Field("entity",
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING), null, null));
        fields.add(new org.apache.avro.Schema.Field("segment",
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING), null, null));
        fields.add(new org.apache.avro.Schema.Field("period",
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING), null, null));
        fields.add(new org.apache.avro.Schema.Field("instant",
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING), null, null));
      } else {
        // Default minimal schema
        fields.add(new org.apache.avro.Schema.Field("id",
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING), null, null));
      }

      schema.setFields(fields);

      // Write empty Parquet file with schema
      org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
      org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(file.getAbsolutePath());
      org.apache.parquet.io.OutputFile outputFile = org.apache.parquet.hadoop.util.HadoopOutputFile.fromPath(path, conf);
      org.apache.parquet.hadoop.ParquetWriter<org.apache.avro.generic.GenericRecord> writer =
          org.apache.parquet.avro.AvroParquetWriter.<org.apache.avro.generic.GenericRecord>builder(outputFile)
              .withSchema(schema)
              .withConf(conf)
              .build();

      // Close immediately - we just need the file with schema
      writer.close();

      LOGGER.debug("Created mock Parquet file: {}", file);
    } catch (Exception e) {
      LOGGER.error("Failed to create mock Parquet file: " + file, e);
    }
  }

  /**
   * Public static method to trigger SEC data download from external components like RSS monitor.
   */
  public static void triggerDownload(Map<String, Object> operand) {
    SecSchemaFactory factory = new SecSchemaFactory();
    factory.downloadSecData(operand);
  }

  private void downloadSecData(Map<String, Object> operand) {
    LOGGER.info("Starting SEC data download");
    LOGGER.info("downloadSecData() called - STARTING SEC DATA DOWNLOAD");

    // Clear download tracking for new cycle
    downloadedInThisCycle.clear();

    try {
      // Check if directory is specified in config, otherwise use default
      String configuredDir = (String) operand.get("directory");
      if (configuredDir == null) {
        configuredDir = (String) operand.get("cacheDirectory");
      }
      String cacheHome = configuredDir != null ? configuredDir : SEC_DATA_HOME;

      // XBRL files are immutable - once downloaded, they never change
      // Use configured cache directory
      File baseDir = new File(cacheHome, "sec-data");
      baseDir.mkdirs();

      LOGGER.info("Using SEC cache directory: {}", baseDir.getAbsolutePath());

      // Update operand to use our cache directory
      operand.put("directory", baseDir.getAbsolutePath());

      // Check if we should use mock data instead of downloading
      Boolean useMockData = (Boolean) operand.get("useMockData");
      if (useMockData != null && useMockData) {
        LOGGER.info("Using mock data for testing");
        List<String> ciks = getCiksFromConfig(operand);
        int startYear = (Integer) operand.getOrDefault("startYear", 2020);
        int endYear = (Integer) operand.getOrDefault("endYear", 2023);
        LOGGER.info("DEBUG: Calling createSecTablesFromXbrl for mock data");
        LOGGER.info("DEBUG: baseDir=" + baseDir.getAbsolutePath());
        // Create minimal mock Parquet files so tables are discovered
        createMockParquetFiles(baseDir);
        LOGGER.info("DEBUG: Mock data mode - created mock Parquet files");

        // Also create mock stock prices if enabled
        boolean fetchStockPrices = (Boolean) operand.getOrDefault("fetchStockPrices", true);
        if (fetchStockPrices) {
          LOGGER.info("Creating mock stock prices for testing");
          createMockStockPrices(baseDir, ciks, startYear, endYear);
        }

        return;
      }

      // Get CIKs to download
      List<String> ciks = getCiksFromConfig(operand);
      LOGGER.debug("CIKs: {}", ciks);
      List<String> filingTypes = getFilingTypes(operand);
      LOGGER.debug("Filing types: {}", filingTypes);
      int startYear = (Integer) operand.getOrDefault("startYear", 2020);
      int endYear = (Integer) operand.getOrDefault("endYear", 2023);
      LOGGER.debug("Year range: {} - {}", startYear, endYear);

      // Use SEC storage provider to download filings
      SecHttpStorageProvider provider = SecHttpStorageProvider.forEdgar();

      for (String cik : ciks) {
        downloadCikFilings(provider, cik, filingTypes, startYear, endYear, baseDir);
      }

      // Wait for all downloads to complete before proceeding to conversion
      if (!downloadFutures.isEmpty()) {
        LOGGER.info("Waiting for " + downloadFutures.size() + " download tasks to complete...");
        CompletableFuture<Void> allDownloads =
            CompletableFuture.allOf(downloadFutures.toArray(new CompletableFuture[0]));
        try {
          allDownloads.get(45, TimeUnit.MINUTES); // Wait up to 45 minutes for downloads
          LOGGER.info("All downloads completed: " + completedDownloads.get() + " files");
        } catch (Exception e) {
          LOGGER.warn("Some downloads may have failed: " + e.getMessage());
        }
      }

      // Create all SEC tables from XBRL data (with parallel conversion)
      LOGGER.info("Creating SEC tables from XBRL data");
      createSecTablesFromXbrl(baseDir, ciks, startYear, endYear);

      // Download or create stock prices if enabled
      boolean fetchStockPrices = (Boolean) operand.getOrDefault("fetchStockPrices", true);
      Boolean testModeObj = (Boolean) operand.get("testMode");
      boolean isTestMode = testModeObj != null && testModeObj;
      if (fetchStockPrices) {
        if (isTestMode) {
          LOGGER.info("Creating mock stock prices for testing (testMode=true)");
          createMockStockPrices(baseDir, ciks, startYear, endYear);
        } else {
          LOGGER.info("Downloading stock prices for configured CIKs");
          downloadStockPrices(baseDir, ciks, startYear, endYear);
        }
      }

    } catch (Exception e) {
      LOGGER.error("Error in downloadSecData", e);
      LOGGER.warn("Failed to download SEC data: " + e.getMessage());
    }
  }

  private void downloadCikFilings(SecHttpStorageProvider provider, String cik,
      List<String> filingTypes, int startYear, int endYear, File baseDir) {
    // Ensure executors are initialized
    initializeExecutors();

    try {
      // Normalize CIK
      String normalizedCik = String.format("%010d", Long.parseLong(cik.replaceAll("[^0-9]", "")));

      // Create CIK directory in the configured cache location
      // Use the baseDir passed in from downloadSecData
      File cikDir = new File(baseDir, normalizedCik);
      cikDir.mkdirs();

      // Download submissions metadata first (with rate limiting)
      String submissionsUrl =
          String.format("https://data.sec.gov/submissions/CIK%s.json", normalizedCik);
      File submissionsFile = new File(cikDir, "submissions.json");

      // Apply rate limiting
      try {
        rateLimiter.acquire();
        Thread.sleep(currentRateLimitDelayMs.get()); // Use dynamic rate limit delay
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }

      try (InputStream is = provider.openInputStream(submissionsUrl)) {
        try (FileOutputStream fos = new FileOutputStream(submissionsFile)) {
          byte[] buffer = new byte[8192];
          int bytesRead;
          while ((bytesRead = is.read(buffer)) != -1) {
            fos.write(buffer, 0, bytesRead);
          }
        }
        // Clean up macOS metadata files after writing
        cleanupMacOSMetadataFilesRecursive(cikDir);
        LOGGER.info("Downloaded submissions metadata for CIK " + normalizedCik);
      } finally {
        rateLimiter.release();
      }

      // Parse submissions.json to get filing details
      ObjectMapper mapper = new ObjectMapper();
      JsonNode submissionsJson = mapper.readTree(submissionsFile);
      JsonNode filings = submissionsJson.get("filings");

      if (filings == null || !filings.has("recent")) {
        LOGGER.warn("No recent filings found in submissions for CIK " + normalizedCik);
        return;
      }

      JsonNode recent = filings.get("recent");
      JsonNode accessionNumbers = recent.get("accessionNumber");
      JsonNode filingDates = recent.get("filingDate");
      JsonNode forms = recent.get("form");
      JsonNode primaryDocuments = recent.get("primaryDocument");

      if (accessionNumbers == null || !accessionNumbers.isArray()) {
        LOGGER.warn("No accession numbers found for CIK " + normalizedCik);
        return;
      }

      // Collect all filings to download
      List<FilingToDownload> filingsToDownload = new ArrayList<>();
      for (int i = 0; i < accessionNumbers.size(); i++) {
        String accession = accessionNumbers.get(i).asText();
        String filingDate = filingDates.get(i).asText();
        String form = forms.get(i).asText();
        String primaryDoc = primaryDocuments.get(i).asText();

        // Check if filing type matches and is in date range
        // Normalize form comparison (SEC uses "10K", config uses "10-K")
        String normalizedForm = form.replace("-", "");
        boolean matchesType = filingTypes.stream()
            .anyMatch(type -> type.replace("-", "").equalsIgnoreCase(normalizedForm));
        if (!matchesType) {
          continue;
        }

        int filingYear = Integer.parseInt(filingDate.substring(0, 4));
        if (filingYear < startYear || filingYear > endYear) {
          continue;
        }

        filingsToDownload.add(
            new FilingToDownload(normalizedCik, accession,
            primaryDoc, form, filingDate, cikDir));
      }

      // Download filings in parallel with rate limiting
      totalDownloads.addAndGet(filingsToDownload.size());
      LOGGER.info("Scheduling " + filingsToDownload.size() + " filings for download for CIK " + normalizedCik);

      for (FilingToDownload filing : filingsToDownload) {
        // Create unique key for deduplication
        String filingKey = filing.cik + "|" + filing.accession + "|" + filing.form + "|" + filing.filingDate;

        // Skip if already scheduled for download in this cycle
        if (!downloadedInThisCycle.add(filingKey)) {
          LOGGER.debug("Skipping duplicate download task for: " + filing.form + " " + filing.filingDate);
          continue;
        }

        CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
          downloadFilingDocumentWithRateLimit(provider, filing);
          int completed = completedDownloads.incrementAndGet();
          if (completed % 10 == 0) {
            LOGGER.info("Download progress: " + completed + "/" + totalDownloads.get() + " filings");
          }
        }, downloadExecutor);
        downloadFutures.add(future);
      }

    } catch (Exception e) {
      LOGGER.warn("Failed to download filings for CIK " + cik + ": " + e.getMessage());
      e.printStackTrace();
    }
  }

  // Helper class to hold filing download information
  private static class FilingToDownload {
    final String cik;
    final String accession;
    final String primaryDoc;
    final String form;
    final String filingDate;
    final File cikDir;

    FilingToDownload(String cik, String accession, String primaryDoc,
        String form, String filingDate, File cikDir) {
      this.cik = cik;
      this.accession = accession;
      this.primaryDoc = primaryDoc;
      this.form = form;
      this.filingDate = filingDate;
      this.cikDir = cikDir;
    }
  }

  private void downloadFilingDocumentWithRateLimit(SecHttpStorageProvider provider, FilingToDownload filing) {
    int maxAttempts = 3;
    int attempt = 0;

    while (attempt < maxAttempts) {
      try {
        // Apply rate limiting with current delay
        rateLimiter.acquire();
        Thread.sleep(currentRateLimitDelayMs.get());
        try {
          downloadFilingDocument(provider, filing.cik, filing.accession,
              filing.primaryDoc, filing.form, filing.filingDate, filing.cikDir);
          break; // Success - exit retry loop
        } finally {
          rateLimiter.release();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOGGER.warn("Download interrupted for " + filing.accession);
        break;
      } catch (Exception e) {
        if (e.getMessage() != null && e.getMessage().contains("429")) {
          // Rate limit hit - dynamically expand rate limit delay
          rateLimitHits.incrementAndGet();
          long currentDelay = currentRateLimitDelayMs.get();
          long newDelay = Math.min(currentDelay + 50, MAX_RATE_LIMIT_DELAY_MS); // Increase by 50ms up to max
          currentRateLimitDelayMs.set(newDelay);
          LOGGER.warn("Rate limit hit for " + filing.accession +
              ". Expanding rate limit delay from " + currentDelay + "ms to " + newDelay + "ms. Waiting 10 seconds...");
          try {
            Thread.sleep(10000); // Wait 10 seconds for rate limit
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            break;
          }
          attempt++;
        } else {
          LOGGER.warn("Download failed for " + filing.accession + ": " + e.getMessage());
          break;
        }
      }
    }

    if (attempt >= maxAttempts) {
      LOGGER.error("Failed to download " + filing.accession + " after " + maxAttempts + " attempts");
    }
  }

  private void downloadFilingDocument(SecHttpStorageProvider provider, String cik,
      String accession, String primaryDoc, String form, String filingDate, File cikDir) {
    try {
      // Check manifest first to see if this filing was already fully processed
      File manifestFile = new File(cikDir.getParentFile(), "processed_filings.manifest");
      String manifestKey = cik + "|" + accession + "|" + form + "|" + filingDate;

      // Quick check if already in manifest (meaning fully downloaded AND converted to Parquet)
      if (manifestFile.exists()) {
        try {
          Set<String> processedFilings = new HashSet<>(Files.readAllLines(manifestFile.toPath()));

          // Check if this filing is in the manifest
          boolean isProcessed = false;
          boolean hasVectorized = false;
          for (String entry : processedFilings) {
            if (entry.startsWith(cik + "|" + accession + "|")) {
              isProcessed = true;
              hasVectorized = entry.contains("PROCESSED_WITH_VECTORS");
              break;
            }
          }

          // If text similarity is enabled but vectorized files don't exist, need reprocessing
          Map<String, Object> textSimilarityConfig = (Map<String, Object>) currentOperand.get("textSimilarity");
          boolean needsVectorized = textSimilarityConfig != null &&
              Boolean.TRUE.equals(textSimilarityConfig.get("enabled"));

          if (isProcessed && (!needsVectorized || hasVectorized)) {
            LOGGER.debug("Filing fully processed (in manifest), skipping: " + form + " " + filingDate);
            return;
          } else if (isProcessed && needsVectorized && !hasVectorized) {
            LOGGER.debug("Filing needs vectorization, will reprocess: " + form + " " + filingDate);
            // Continue with processing to add vectorized files
          }
        } catch (Exception e) {
          LOGGER.debug("Could not read manifest: " + e.getMessage());
        }
      }

      // Not in manifest - need to do detailed verification

      // Create accession directory
      String accessionClean = accession.replace("-", "");
      File accessionDir = new File(cikDir, accessionClean);
      accessionDir.mkdirs();

      // Download both HTML (for preview) and XBRL (for data extraction)
      boolean needHtml = false;
      boolean needXbrl = false;

      // For Forms 3/4/5, we need to download the .txt file and extract the raw XML
      boolean isInsiderForm = form.equals("3") || form.equals("4") || form.equals("5");

      // Check if HTML file exists (for human-readable preview)
      File htmlFile = new File(accessionDir, primaryDoc);
      // Create parent directory if primaryDoc contains path (e.g., xslF345X05/wk-form4_*.xml for Form 4)
      if (primaryDoc.contains("/")) {
        htmlFile.getParentFile().mkdirs();
      }
      if (!isInsiderForm && (!htmlFile.exists() || htmlFile.length() == 0)) {
        needHtml = true;
      }

      // Check if XBRL file exists (for structured data)
      String xbrlDoc;
      File xbrlFile;
      if (isInsiderForm) {
        // For Forms 3/4/5, we'll save the extracted XML as ownership.xml
        xbrlDoc = "ownership.xml";
        xbrlFile = new File(accessionDir, xbrlDoc);
      } else {
        // For other forms, look for separate XBRL file
        xbrlDoc = primaryDoc.replace(".htm", "_htm.xml");
        xbrlFile = new File(accessionDir, xbrlDoc);
      }

      // Create parent directory if xbrlDoc contains path
      if (xbrlDoc.contains("/")) {
        xbrlFile.getParentFile().mkdirs();
      }

      File xbrlNotFoundMarker = new File(accessionDir, xbrlDoc + ".notfound");
      // Only need XBRL if: file doesn't exist AND we haven't already marked it as not found
      if ((!xbrlFile.exists() || xbrlFile.length() == 0) && !xbrlNotFoundMarker.exists()) {
        needXbrl = true;
      }

      // Check if Parquet conversion was successful
      boolean needParquetReprocessing = false;
      String year = String.valueOf(java.time.LocalDate.parse(filingDate).getYear());
      File parquetDir = new File(cikDir.getParentFile().getParentFile(), "sec-parquet");
      File cikParquetDir = new File(parquetDir, "cik=" + cik);
      // Need to include filing_type in the path
      File filingTypeDir = new File(cikParquetDir, "filing_type=" + form.replace("-", ""));
      File yearDir = new File(filingTypeDir, "year=" + year);

      // Check for the appropriate parquet file based on form type
      // Forms 3/4/5 create _insider.parquet files, others create _facts.parquet files
      String filenameSuffix = isInsiderForm ? "insider" : "facts";
      File parquetFile = new File(yearDir, String.format("%s_%s_%s.parquet", cik, filingDate, filenameSuffix));

      if (!parquetFile.exists() || parquetFile.length() == 0) {
        needParquetReprocessing = true;
        LOGGER.debug("Missing or empty Parquet file: " + parquetFile.getPath());
      }

      // Also check for vectorized file if text similarity is enabled
      Map<String, Object> textSimilarityConfig = (Map<String, Object>) currentOperand.get("textSimilarity");
      if (textSimilarityConfig != null && Boolean.TRUE.equals(textSimilarityConfig.get("enabled"))) {
        File vectorizedFile = new File(yearDir, String.format("%s_%s_vectorized.parquet", cik, filingDate));
        if (!vectorizedFile.exists() || vectorizedFile.length() == 0) {
          needParquetReprocessing = true;
          LOGGER.debug("Missing or empty vectorized Parquet file: " + vectorizedFile.getPath());
        }
      }

      if (!needHtml && !needXbrl && !needParquetReprocessing) {
        LOGGER.info("Filing already fully cached: " + form + " " + filingDate);
        return; // Both files already downloaded and converted
      }

      if (needParquetReprocessing) {
        LOGGER.info("Parquet reprocessing needed for: " + form + " " + filingDate);
        // Force reprocessing by treating as if we need XBRL (even if files exist)
        // This ensures the conversion will run again
        needXbrl = true;

        // If XBRL file already exists, we need to schedule it for conversion
        // since it won't be downloaded again
        if (xbrlFile.exists() && xbrlFile.length() > 0) {
          LOGGER.info("Scheduling existing XBRL file for re-conversion: " + xbrlFile.getName());
          // Add to a list that will be processed later
          if (!scheduledForReconversion.contains(xbrlFile)) {
            scheduledForReconversion.add(xbrlFile);
          }
        }
      }

      // Download HTML file first if needed (for preview and iXBRL check)
      if (needHtml) {
        String htmlUrl =
            String.format("https://www.sec.gov/Archives/edgar/data/%s/%s/%s",
            cik, accessionClean, primaryDoc);

        try (InputStream is = provider.openInputStream(htmlUrl)) {
          try (FileOutputStream fos = new FileOutputStream(htmlFile)) {
            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = is.read(buffer)) != -1) {
              fos.write(buffer, 0, bytesRead);
            }
          }
          LOGGER.info("Downloaded HTML filing: " + form + " " + filingDate + " (" + primaryDoc + ")");
        } catch (Exception e) {
          LOGGER.info("Could not download HTML: " + e.getMessage());
          return; // Can't proceed without HTML
        }
      }

      // Check if HTML contains inline XBRL (iXBRL) - if it does, we don't need separate XBRL
      boolean hasInlineXbrl = false;
      if (htmlFile.exists()) {
        try {
          // Quick check for iXBRL markers in the first 10KB of the HTML
          byte[] headerBytes = new byte[10240];
          try (FileInputStream fis = new FileInputStream(htmlFile)) {
            int bytesRead = fis.read(headerBytes);
            if (bytesRead > 0) {
              String header = new String(headerBytes, 0, bytesRead);
              hasInlineXbrl = header.contains("xmlns:ix=") ||
                             header.contains("http://www.xbrl.org/2013/inlineXBRL") ||
                             header.contains("<ix:") ||
                             header.contains("iXBRL");
              if (hasInlineXbrl) {
                LOGGER.info("HTML file contains inline XBRL (iXBRL), skipping separate XBRL download: " + primaryDoc);
                // Create marker to avoid checking XBRL in future
                if (!xbrlNotFoundMarker.exists()) {
                  xbrlNotFoundMarker.createNewFile();
                }
                return; // No need to download separate XBRL
              }
            }
          }
        } catch (Exception e) {
          LOGGER.debug("Could not check HTML for iXBRL: " + e.getMessage());
        }
      }

      // Download XBRL file if needed (only if HTML doesn't have iXBRL)
      if (needXbrl && !hasInlineXbrl) {
        // Check if we already know this XBRL doesn't exist
        if (xbrlNotFoundMarker.exists()) {
          LOGGER.debug("Skipping XBRL download - already marked as not found: " + xbrlDoc);
        } else {
          if (isInsiderForm) {
            // For Forms 3/4/5, download the .txt file and extract the XML
            String txtUrl = String.format("https://www.sec.gov/Archives/edgar/data/%s/%s.txt",
                cik, accession); // Use hyphenated accession number for .txt files

            try (InputStream is = provider.openInputStream(txtUrl)) {
              // Read the entire .txt file to extract the XML
              ByteArrayOutputStream baos = new ByteArrayOutputStream();
              byte[] buffer = new byte[8192];
              int bytesRead;
              while ((bytesRead = is.read(buffer)) != -1) {
                baos.write(buffer, 0, bytesRead);
              }
              String txtContent = baos.toString("UTF-8");

              // Extract the ownershipDocument XML from the .txt file
              int xmlStart = txtContent.indexOf("<ownershipDocument>");
              int xmlEnd = txtContent.indexOf("</ownershipDocument>");

              if (xmlStart != -1 && xmlEnd != -1) {
                String xmlContent = "<?xml version=\"1.0\"?>\n" +
                    txtContent.substring(xmlStart, xmlEnd + "</ownershipDocument>".length());

                // Save the extracted XML
                try (FileWriter writer = new FileWriter(xbrlFile)) {
                  writer.write(xmlContent);
                }
                LOGGER.info("Extracted ownership XML for Form " + form + " " + filingDate);
              } else {
                LOGGER.warn("Could not find ownershipDocument in Form " + form + " .txt file");
                xbrlNotFoundMarker.createNewFile();
              }
            } catch (Exception e) {
              LOGGER.warn("Failed to download/extract Form " + form + " ownership XML: " + e.getMessage());
              try {
                xbrlNotFoundMarker.createNewFile();
              } catch (IOException ioe) {
                LOGGER.debug("Could not create .notfound marker: " + ioe.getMessage());
              }
            }
          } else {
            // For other forms, download the XBRL file directly
            String xbrlUrl =
                String.format("https://www.sec.gov/Archives/edgar/data/%s/%s/%s",
                cik, accessionClean, xbrlDoc);

            try (InputStream is = provider.openInputStream(xbrlUrl)) {
              try (FileOutputStream fos = new FileOutputStream(xbrlFile)) {
                byte[] buffer = new byte[8192];
                int bytesRead;
                while ((bytesRead = is.read(buffer)) != -1) {
                  fos.write(buffer, 0, bytesRead);
                }
              }
              LOGGER.info("Downloaded XBRL filing: " + form + " " + filingDate + " (" + xbrlDoc + ")");
            } catch (Exception e) {
              // XBRL doesn't exist for this filing - create marker to avoid retrying
              LOGGER.info("XBRL not available for " + form + " " + filingDate + ", will use HTML with inline XBRL");
              try {
                xbrlNotFoundMarker.createNewFile();
                LOGGER.debug("Created .notfound marker for: " + xbrlDoc);
              } catch (IOException ioe) {
                LOGGER.debug("Could not create .notfound marker: " + ioe.getMessage());
              }
            }
          }
        }
      }

      // Don't add to manifest here - only add after Parquet conversion is complete
      // The manifest should only contain fully processed filings (downloaded + converted)
      LOGGER.debug("Filing downloaded, but not adding to manifest until Parquet conversion complete");

    } catch (Exception e) {
      LOGGER.warn("Failed to download filing " + accession + ": " + e.getMessage());
    }
  }

  private void downloadInlineXbrl(SecHttpStorageProvider provider, String cik,
      String accession, String primaryDoc, String form, String filingDate, File cikDir) throws Exception {
    // Some filings use inline XBRL embedded in HTML
    // Try to download the HTML and extract XBRL data
    String accessionClean = accession.replace("-", "");
    File accessionDir = new File(cikDir, accessionClean);

    File outputFile = new File(accessionDir, primaryDoc);

    // Check if file already exists - XBRL files are immutable
    if (outputFile.exists() && outputFile.length() > 0) {
      LOGGER.info("Inline XBRL filing already cached: " + form + " " + filingDate + " (" + primaryDoc + ")");
      return;
    }

    String htmlUrl =
        String.format("https://www.sec.gov/Archives/edgar/data/%s/%s/%s",
        cik, accessionClean, primaryDoc);
    try (InputStream is = provider.openInputStream(htmlUrl)) {
      try (FileOutputStream fos = new FileOutputStream(outputFile)) {
        byte[] buffer = new byte[8192];
        int bytesRead;
        while ((bytesRead = is.read(buffer)) != -1) {
          fos.write(buffer, 0, bytesRead);
        }
      }
    }

    LOGGER.info("Downloaded inline XBRL (HTML): " + form + " " + filingDate + " (" + primaryDoc + ")");
  }

  private void createSecTablesFromXbrl(File baseDir, List<String> ciks, int startYear, int endYear) {
    // Ensure executors are initialized
    initializeExecutors();

    LOGGER.debug("Creating SEC tables from XBRL data");
    LOGGER.info("DEBUG: createSecTablesFromXbrl START");
    LOGGER.info("DEBUG: baseDir=" + baseDir.getAbsolutePath());
    LOGGER.info("DEBUG: ciks.size()=" + ciks.size());
    LOGGER.info("DEBUG: startYear=" + startYear + ", endYear=" + endYear);

    try {
      // baseDir is already the sec-data directory, don't nest another level
      File secRawDir = baseDir;
      // Parquet directory is a sibling of sec-data
      File secParquetDir = new File(baseDir.getParentFile(), "sec-parquet");
      secParquetDir.mkdirs();

      LOGGER.info("DEBUG: secRawDir=" + secRawDir.getAbsolutePath() + " exists=" + secRawDir.exists());
      LOGGER.info("DEBUG: secParquetDir=" + secParquetDir.getAbsolutePath() + " exists=" + secParquetDir.exists());
      LOGGER.info("Processing XBRL files from " + secRawDir + " to create Parquet tables");

      // Collect all XBRL files to convert
      List<File> xbrlFilesToConvert = new ArrayList<>();

      // Process all downloaded XBRL files
      for (String cik : ciks) {
        String normalizedCik = String.format("%010d", Long.parseLong(cik.replaceAll("[^0-9]", "")));
        File cikDir = new File(secRawDir, normalizedCik);

        if (!cikDir.exists()) {
          LOGGER.info("No data directory found for CIK " + normalizedCik);
          continue;
        }

        // Process each filing in the CIK directory
        File[] accessionDirs = cikDir.listFiles(File::isDirectory);
        if (accessionDirs != null) {
          for (File accessionDir : accessionDirs) {
            // Find XBRL and HTML files in this accession directory
            // Exclude macOS metadata files that start with ._
            File[] xbrlFiles = accessionDir.listFiles((dir, name) ->
                !name.startsWith("._") && (
                name.endsWith("_htm.xml") || name.endsWith(".xml") ||
                name.endsWith(".htm") || name.endsWith(".html")));

            if (xbrlFiles != null) {
              for (File xbrlFile : xbrlFiles) {
                xbrlFilesToConvert.add(xbrlFile);
              }
            }
          }
        }
      }

      // Add any files scheduled for reconversion (missing Parquet files)
      if (!scheduledForReconversion.isEmpty()) {
        LOGGER.info("Adding " + scheduledForReconversion.size() + " existing XBRL files for reconversion");
        xbrlFilesToConvert.addAll(scheduledForReconversion);
        scheduledForReconversion.clear();
      }

      // Convert XBRL files in parallel
      totalConversions.set(xbrlFilesToConvert.size());
      LOGGER.info("Scheduling " + xbrlFilesToConvert.size() + " XBRL files for conversion");

      for (File xbrlFile : xbrlFilesToConvert) {
        CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
          try {
            XbrlToParquetConverter converter = new XbrlToParquetConverter();

            // Extract accession number from file path (parent directory name)
            // Path is like: /sec-data/0000789019/000078901922000007/ownership.xml
            String accession = xbrlFile.getParentFile().getName();

            // Create a simple ConversionMetadata to pass accession to converter
            ConversionMetadata metadata = new ConversionMetadata(secParquetDir);
            ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
            record.originalFile = xbrlFile.getAbsolutePath();
            // Store accession in the sourceFile field for now - converter can extract it
            record.sourceFile = accession;
            metadata.recordConversion(xbrlFile, record);

            // The converter now properly extracts metadata from the XML content itself
            List<File> outputFiles = converter.convert(xbrlFile, secParquetDir, metadata);
            LOGGER.info("Converted " + xbrlFile.getName() + " to " +
                outputFiles.size() + " Parquet files");

            // Add to manifest after successful conversion
            addToManifest(xbrlFile.getParentFile().getParentFile().getParentFile(), xbrlFile);
          } catch (java.nio.channels.OverlappingFileLockException e) {
            // Non-fatal: Another thread is processing this file, skip it
            LOGGER.debug("Skipping " + xbrlFile.getName() + " - already being processed by another thread");
          } catch (Exception e) {
            // Check if the cause is an OverlappingFileLockException
            Throwable cause = e.getCause();
            if (cause instanceof java.nio.channels.OverlappingFileLockException) {
              // Non-fatal: Another thread is processing this file, skip it
              LOGGER.debug("Skipping " + xbrlFile.getName() + " - already being processed by another thread");
            } else {
              LOGGER.info("Failed to convert " + xbrlFile.getName() + ": " + e.getMessage());
            }
          }
          int completed = completedConversions.incrementAndGet();
          if (completed % 10 == 0) {
            LOGGER.info("Conversion progress: " + completed + "/" + totalConversions.get() + " files");
          }
        }, conversionExecutor);
        conversionFutures.add(future);
      }

      // Wait for all conversions to complete
      CompletableFuture<Void> allConversions =
          CompletableFuture.allOf(conversionFutures.toArray(new CompletableFuture[0]));
      allConversions.get(30, TimeUnit.MINUTES); // Wait up to 30 minutes

      LOGGER.info("Processed " + completedConversions.get() + " XBRL files into Parquet tables in: " + secParquetDir);

      // Bulk cleanup of macOS metadata files at the end of processing
      cleanupAllMacOSMetadataFiles(secParquetDir);

      LOGGER.info("DEBUG: Checking what was created in secParquetDir after conversion");
      File[] afterConversion = secParquetDir.listFiles();
      if (afterConversion != null) {
        LOGGER.info("DEBUG: Found " + afterConversion.length + " items in secParquetDir");
        for (File f : afterConversion) {
          LOGGER.info("DEBUG: - " + f.getName() + " (isDir=" + f.isDirectory() + ", size=" + f.length() + ")");
        }
      }

    } catch (Exception e) {
      LOGGER.error("Failed to create SEC tables from XBRL", e);
      throw new RuntimeException("Failed to create SEC tables", e);
    }
  }


  // REMOVED: consolidateFactsIntoFinancialLineItems method
  // The FileSchema will automatically discover individual *_facts.parquet files
  // No consolidation needed - each filing remains in its own partition











  private void createSecFilingsTable(File baseDir, Map<String, Object> operand) {
    try {
      File secRawDir = new File(baseDir, "sec-raw");
      File secParquetDir = new File(baseDir, "sec-parquet");
      secParquetDir.mkdirs();

      if (!secRawDir.exists() || !secRawDir.isDirectory()) {
        LOGGER.warn("No sec-raw directory found: " + secRawDir);
        return;
      }

      // Create Avro schema for SEC filings metadata
      org.apache.avro.Schema schema = SchemaBuilder.record("SecFiling")
          .fields()
          .requiredString("cik")
          .requiredString("accession_number")
          .requiredString("filing_type")
          .requiredString("filing_date")
          .optionalString("primary_document")
          .optionalString("company_name")
          .optionalString("period_of_report")
          .optionalString("acceptance_datetime")
          .optionalLong("file_size")
          .requiredInt("fiscal_year")
          .endRecord();

      List<GenericRecord> allRecords = new ArrayList<>();
      ObjectMapper mapper = new ObjectMapper();

      // Process each CIK directory
      File[] cikDirs = secRawDir.listFiles(File::isDirectory);
      if (cikDirs != null) {
        for (File cikDir : cikDirs) {
          File submissionsFile = new File(cikDir, "submissions.json");
          if (!submissionsFile.exists()) {
            continue;
          }

          try {
            JsonNode submissionsJson = mapper.readTree(submissionsFile);
            String cik = cikDir.getName();
            String companyName = submissionsJson.path("name").asText("");

            JsonNode filings = submissionsJson.get("filings");
            if (filings == null || !filings.has("recent")) {
              continue;
            }

            JsonNode recent = filings.get("recent");
            JsonNode accessionNumbers = recent.get("accessionNumber");
            JsonNode filingDates = recent.get("filingDate");
            JsonNode forms = recent.get("form");
            JsonNode primaryDocuments = recent.get("primaryDocument");
            JsonNode periodsOfReport = recent.get("reportDate");
            JsonNode acceptanceDatetimes = recent.get("acceptanceDateTime");
            JsonNode fileSizes = recent.get("size");

            if (accessionNumbers == null || !accessionNumbers.isArray()) {
              continue;
            }

            for (int i = 0; i < accessionNumbers.size(); i++) {
              GenericRecord record = new GenericData.Record(schema);
              record.put("cik", cik);
              record.put("accession_number", accessionNumbers.get(i).asText());
              record.put("filing_type", forms.get(i).asText());
              record.put("filing_date", filingDates.get(i).asText());
              record.put("primary_document", primaryDocuments.get(i).asText(""));
              record.put("company_name", companyName);
              record.put("period_of_report", periodsOfReport != null && i < periodsOfReport.size()
                  ? periodsOfReport.get(i).asText("") : "");
              record.put("acceptance_datetime", acceptanceDatetimes != null && i < acceptanceDatetimes.size()
                  ? acceptanceDatetimes.get(i).asText("") : "");
              record.put("file_size", fileSizes != null && i < fileSizes.size()
                  ? fileSizes.get(i).asLong(0L) : 0L);

              String filingDate = filingDates.get(i).asText();
              int fiscalYear = filingDate.length() >= 4 ?
                  Integer.parseInt(filingDate.substring(0, 4)) : 0;
              record.put("fiscal_year", fiscalYear);

              allRecords.add(record);
            }
          } catch (Exception e) {
            LOGGER.warn("Failed to process submissions for CIK " + cikDir.getName() + ": " + e.getMessage());
          }
        }
      }

      // Write consolidated SEC filings table
      File outputFile = new File(secParquetDir, "sec_filings.parquet");
      @SuppressWarnings("deprecation")
      ParquetWriter<GenericRecord> writer = AvroParquetWriter
          .<GenericRecord>builder(new org.apache.hadoop.fs.Path(outputFile.toURI()))
          .withSchema(schema)
          .withCompressionCodec(CompressionCodecName.SNAPPY)
          .build();

      try {
        for (GenericRecord record : allRecords) {
          writer.write(record);
        }
      } finally {
        writer.close();
      }

      LOGGER.info("Created SEC filings table with " + allRecords.size() + " records: " + outputFile);

    } catch (Exception e) {
      LOGGER.warn("Failed to create SEC filings table: " + e.getMessage());
      e.printStackTrace();
    }
  }


  private List<File> findXbrlFiles(File directory) {
    List<File> xbrlFiles = new ArrayList<>();
    File[] files = directory.listFiles();

    if (files != null) {
      for (File file : files) {
        if (file.isDirectory()) {
          xbrlFiles.addAll(findXbrlFiles(file));
        } else if (file.getName().endsWith(".xml") || file.getName().endsWith(".xbrl")) {
          xbrlFiles.add(file);
        } else if (file.getName().endsWith(".htm") || file.getName().endsWith(".html")) {
          // Include HTML files - XbrlToParquetConverter can handle inline XBRL
          xbrlFiles.add(file);
        }
      }
    }

    return xbrlFiles;
  }

  /**
   * Creates mock stock prices for testing.
   */
  private void createMockStockPrices(File baseDir, List<String> ciks, int startYear, int endYear) {
    try {
      // baseDir is the cache directory (sec-cache)
      // We need to create files in sec-parquet/stock_prices
      File parquetDir = new File(baseDir.getParentFile(), "sec-parquet");
      File stockPricesDir = new File(parquetDir, "stock_prices");
      LOGGER.info("Creating mock stock prices in: {}", stockPricesDir.getAbsolutePath());

      // For each CIK, create mock price data
      for (String cik : ciks) {
        String normalizedCik = cik.replaceAll("[^0-9]", "");
        while (normalizedCik.length() < 10) {
          normalizedCik = "0" + normalizedCik;
        }

        // Get ticker for this CIK (or use CIK as ticker if not found)
        List<String> tickers = CikRegistry.getTickersForCik(normalizedCik);
        String ticker = tickers.isEmpty() ? cik.toUpperCase() : tickers.get(0);

        // Create mock data for each year
        for (int year = startYear; year <= endYear; year++) {
          // Create directory structure: ticker=*/year=*/
          File tickerDir = new File(stockPricesDir, "ticker=" + ticker);
          File yearDir = new File(tickerDir, "year=" + year);
          yearDir.mkdirs();

          File parquetFile = new File(yearDir, ticker + "_" + year + "_prices.parquet");
          LOGGER.info("About to create/check file: {}", parquetFile.getAbsolutePath());
          LOGGER.info("Parent directory exists: {}, isDirectory: {}", yearDir.exists(), yearDir.isDirectory());
          if (!parquetFile.exists()) {
            createMockPriceParquetFile(parquetFile, ticker, normalizedCik, year);
            LOGGER.info("Created mock stock price file: {}, exists now: {}", parquetFile.getAbsolutePath(), parquetFile.exists());
          } else {
            LOGGER.info("Mock stock price file already exists: {}", parquetFile.getAbsolutePath());
          }
        }
      }
    } catch (Exception e) {
      LOGGER.warn("Failed to create mock stock prices: " + e.getMessage());
    }
  }

  /**
   * Creates a mock Parquet file with sample stock price data.
   */
  @SuppressWarnings("deprecation")
  private void createMockPriceParquetFile(File parquetFile, String ticker, String cik, int year)
      throws IOException {
    // Note: ticker and year are partition columns from directory structure,
    // so they are NOT in the Parquet file. CIK is included as a regular column for joins.
    String schemaString = "{"
        + "\"type\": \"record\","
        + "\"name\": \"StockPrice\","
        + "\"fields\": ["
        + "{\"name\": \"cik\", \"type\": \"string\"},"
        + "{\"name\": \"date\", \"type\": \"string\"},"
        + "{\"name\": \"open\", \"type\": [\"null\", \"double\"], \"default\": null},"
        + "{\"name\": \"high\", \"type\": [\"null\", \"double\"], \"default\": null},"
        + "{\"name\": \"low\", \"type\": [\"null\", \"double\"], \"default\": null},"
        + "{\"name\": \"close\", \"type\": [\"null\", \"double\"], \"default\": null},"
        + "{\"name\": \"adj_close\", \"type\": [\"null\", \"double\"], \"default\": null},"
        + "{\"name\": \"volume\", \"type\": [\"null\", \"long\"], \"default\": null}"
        + "]"
        + "}";

    org.apache.avro.Schema schema = new org.apache.avro.Schema.Parser().parse(schemaString);

    try (org.apache.parquet.hadoop.ParquetWriter<org.apache.avro.generic.GenericRecord> writer =
        org.apache.parquet.avro.AvroParquetWriter
            .<org.apache.avro.generic.GenericRecord>builder(
                new org.apache.hadoop.fs.Path(parquetFile.getAbsolutePath()))
            .withSchema(schema)
            .withCompressionCodec(org.apache.parquet.hadoop.metadata.CompressionCodecName.SNAPPY)
            .build()) {

      // Create a few sample records
      double basePrice = 100.0 + (ticker.hashCode() % 100);
      for (int month = 1; month <= 3; month++) { // Just 3 months of data for testing
        String date = String.format("%04d-%02d-%02d", year, month, 15);

        org.apache.avro.generic.GenericRecord record =
            new org.apache.avro.generic.GenericData.Record(schema);
        // Include CIK as regular column, ticker and year come from directory structure
        record.put("cik", cik);
        record.put("date", date);
        record.put("open", basePrice + month);
        record.put("high", basePrice + month + 2);
        record.put("low", basePrice + month - 1);
        record.put("close", basePrice + month + 1);
        record.put("adj_close", basePrice + month + 0.5);
        record.put("volume", 1000000L * month);

        writer.write(record);
      }
    }
  }

  /**
   * Downloads stock prices for all configured CIKs.
   */
  private void downloadStockPrices(File baseDir, List<String> ciks, int startYear, int endYear) {
    try {
      File parquetDir = new File(baseDir.getParentFile(), "sec-parquet");

      // Build list of ticker-CIK pairs
      List<YahooFinanceDownloader.TickerCikPair> tickerCikPairs = new ArrayList<>();
      Set<String> processedTickers = new HashSet<>();

      for (String cik : ciks) {
        // Normalize CIK to 10 digits with leading zeros
        String normalizedCik = cik.replaceAll("[^0-9]", "");
        while (normalizedCik.length() < 10) {
          normalizedCik = "0" + normalizedCik;
        }

        // Get tickers for this CIK
        List<String> tickers = CikRegistry.getTickersForCik(normalizedCik);

        if (tickers.isEmpty()) {
          // Try to resolve the CIK as a ticker first
          List<String> resolvedCiks = CikRegistry.resolveCiks(cik);
          if (!resolvedCiks.isEmpty() && !cik.equals(resolvedCiks.get(0))) {
            // This was actually a ticker, use it
            String ticker = cik.toUpperCase();
            if (!processedTickers.contains(ticker)) {
              tickerCikPairs.add(new YahooFinanceDownloader.TickerCikPair(ticker, normalizedCik));
              processedTickers.add(ticker);
            }
          } else {
            LOGGER.debug("No ticker found for CIK {}, skipping stock prices", normalizedCik);
          }
        } else {
          // Add all tickers for this CIK
          for (String ticker : tickers) {
            if (!processedTickers.contains(ticker)) {
              tickerCikPairs.add(new YahooFinanceDownloader.TickerCikPair(ticker, normalizedCik));
              processedTickers.add(ticker);
            }
          }
        }
      }

      if (!tickerCikPairs.isEmpty()) {
        LOGGER.info("Downloading stock prices for {} tickers", tickerCikPairs.size());
        YahooFinanceDownloader downloader = new YahooFinanceDownloader();
        downloader.downloadStockPrices(parquetDir, tickerCikPairs, startYear, endYear);
      } else {
        LOGGER.info("No tickers found for stock price download");
      }

    } catch (Exception e) {
      LOGGER.warn("Failed to download stock prices: " + e.getMessage());
      // Don't fail the schema creation if stock prices fail
    }
  }

  /**
   * Extract and resolve company identifiers from schema operand.
   * 
   * <p>Automatically resolves identifiers using CikRegistry:
   * <ul>
   *   <li>Ticker symbols: "AAPL" → "0000320193" (Apple's CIK)</li>
   *   <li>Company groups: "FAANG" → ["0001326801", "0000320193", "0001018724", "0001065280", "0001652044"]</li>
   *   <li>Raw CIKs: "0000320193" → "0000320193" (normalized to 10 digits)</li>
   *   <li>Mixed inputs: ["AAPL", "FAANG", "0001018724"] → all resolved to CIKs</li>
   * </ul>
   * 
   * @param operand Schema operand map containing 'ciks' parameter
   * @return List of resolved 10-digit CIK strings ready for SEC data fetching
   */
  private List<String> getCiksFromConfig(Map<String, Object> operand) {
    Object ciks = operand.get("ciks");
    if (ciks instanceof List) {
      // Handle array of identifiers: ["AAPL", "MSFT", "FAANG"]
      List<String> cikList = new ArrayList<>();
      for (Object cik : (List<?>) ciks) {
        // Each identifier resolved via CikRegistry: ticker→CIK, group→multiple CIKs, raw CIK→normalized
        cikList.addAll(CikRegistry.resolveCiks(cik.toString()));
      }
      return cikList;
    } else if (ciks instanceof String) {
      // Handle single identifier: "AAPL" or "FAANG" or "0000320193"
      return CikRegistry.resolveCiks((String) ciks);
    }
    return new ArrayList<>();
  }

  private List<String> getFilingTypes(Map<String, Object> operand) {
    Object types = operand.get("filingTypes");
    if (types instanceof List) {
      return (List<String>) types;
    }
    // Return empty list to signal "download all filing types"
    // EdgarDownloader will interpret empty list as "all types"
    return new ArrayList<>();
  }

  /**
   * Bulk cleanup of all macOS metadata files in the parquet directory.
   * This is run at the end of processing to clean up any ._* files that were created.
   */
  private void cleanupAllMacOSMetadataFiles(File directory) {
    if (directory == null || !directory.exists()) {
      return;
    }

    int totalDeleted = 0;
    try {
      totalDeleted = cleanupMacOSMetadataFilesRecursive(directory);
      if (totalDeleted > 0) {
        LOGGER.info("Bulk cleanup: Removed " + totalDeleted + " macOS metadata files from " + directory.getAbsolutePath());
      }
    } catch (Exception e) {
      LOGGER.debug("Error during bulk macOS metadata cleanup: " + e.getMessage());
    }
  }

  /**
   * Recursively delete macOS metadata files (._* files).
   * @return Number of files deleted
   */
  private int cleanupMacOSMetadataFilesRecursive(File directory) {
    int deleted = 0;

    File[] files = directory.listFiles();
    if (files != null) {
      for (File file : files) {
        if (file.isDirectory()) {
          // Recurse into subdirectories
          deleted += cleanupMacOSMetadataFilesRecursive(file);
        } else if (file.getName().startsWith("._")) {
          // Delete macOS metadata file
          if (file.delete()) {
            deleted++;
          }
        }
      }
    }

    return deleted;
  }
}
