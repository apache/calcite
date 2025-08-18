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

import org.apache.calcite.adapter.file.table.ParquetTranslatableTable;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * Utility to prime statistics cache for optimal performance testing and production use.
 * Loads statistics from smallest to largest tables to maximize cache effectiveness.
 */
public class CachePrimer {
  private static final Logger LOGGER = LoggerFactory.getLogger(CachePrimer.class);
  
  /**
   * Table information for cache priming.
   */
  public static class TableInfo {
    public final String schemaName;
    public final String tableName;
    public final Table table;
    public final File file;
    public final long fileSize;
    
    public TableInfo(String schemaName, String tableName, Table table, File file) {
      this.schemaName = schemaName;
      this.tableName = tableName;
      this.table = table;
      this.file = file;
      this.fileSize = file != null ? file.length() : 0;
    }
    
    @Override
    public String toString() {
      return String.format("%s.%s (%.2f MB)", 
          schemaName, tableName, fileSize / (1024.0 * 1024.0));
    }
  }
  
  /**
   * Statistics cache priming result.
   */
  public static class PrimingResult {
    public final int totalTables;
    public final int successfullyPrimed;
    public final int failed;
    public final long totalTimeMs;
    public final long totalBytesProcessed;
    public final Map<String, Long> tableTimings;
    public final List<String> failures;
    
    public PrimingResult(int totalTables, int successfullyPrimed, int failed,
                         long totalTimeMs, long totalBytesProcessed,
                         Map<String, Long> tableTimings, List<String> failures) {
      this.totalTables = totalTables;
      this.successfullyPrimed = successfullyPrimed;
      this.failed = failed;
      this.totalTimeMs = totalTimeMs;
      this.totalBytesProcessed = totalBytesProcessed;
      this.tableTimings = tableTimings;
      this.failures = failures;
    }
    
    public void printSummary() {
      LOGGER.info("\n=== Cache Priming Summary ===");
      LOGGER.info("Total tables: {}", totalTables);
      LOGGER.info("Successfully primed: {}", successfullyPrimed);
      LOGGER.info("Failed: {}", failed);
      LOGGER.info("Total time: {} ms", totalTimeMs);
      LOGGER.info("Total data processed: {} MB", 
          String.format("%.2f", totalBytesProcessed / (1024.0 * 1024.0)));
      
      if (successfullyPrimed > 0) {
        LOGGER.info("Average time per table: {} ms", 
            totalTimeMs / successfullyPrimed);
      }
      
      if (!failures.isEmpty()) {
        LOGGER.info("\nFailed tables:");
        failures.forEach(f -> LOGGER.info("  - {}", f));
      }
      
      // Show slowest tables
      if (!tableTimings.isEmpty()) {
        LOGGER.info("\nSlowest tables to prime:");
        tableTimings.entrySet().stream()
            .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
            .limit(5)
            .forEach(e -> LOGGER.info("  - {}: {} ms", e.getKey(), e.getValue()));
      }
    }
  }
  
  /**
   * Prime statistics cache for all tables in a schema, processing from smallest to largest.
   * This ensures that larger tables (which are more important for performance) are more
   * likely to remain in cache.
   * 
   * @param connection Calcite connection
   * @param schemaName Schema to prime
   * @return Priming result with statistics
   */
  @SuppressWarnings("deprecation")
  public static PrimingResult primeSchema(Connection connection, String schemaName) 
      throws Exception {
    CalciteConnection calciteConn = connection.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConn.getRootSchema();
    SchemaPlus schema = rootSchema.getSubSchema(schemaName);
    
    if (schema == null) {
      throw new IllegalArgumentException("Schema not found: " + schemaName);
    }
    
    // Collect all tables with their file sizes
    List<TableInfo> tables = collectTables(schemaName, schema);
    
    // Sort by file size (smallest first)
    tables.sort(Comparator.comparingLong(t -> t.fileSize));
    
    LOGGER.info("Priming statistics cache for {} tables in schema '{}', " +
                "ordered from smallest to largest", tables.size(), schemaName);
    
    // Prime the cache
    return primeTablesInOrder(connection, tables);
  }
  
  /**
   * Prime statistics cache for multiple schemas, processing tables from smallest to largest
   * across all schemas.
   * 
   * @param connection Calcite connection
   * @param schemaNames Schemas to prime
   * @return Combined priming result
   */
  @SuppressWarnings("deprecation")
  public static PrimingResult primeSchemas(Connection connection, String... schemaNames) 
      throws Exception {
    CalciteConnection calciteConn = connection.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConn.getRootSchema();
    
    // Collect all tables from all schemas
    List<TableInfo> allTables = new ArrayList<>();
    for (String schemaName : schemaNames) {
      SchemaPlus schema = rootSchema.getSubSchema(schemaName);
      if (schema != null) {
        allTables.addAll(collectTables(schemaName, schema));
      }
    }
    
    // Sort by file size globally (smallest first across all schemas)
    allTables.sort(Comparator.comparingLong(t -> t.fileSize));
    
    LOGGER.info("Priming statistics cache for {} tables across {} schemas, " +
                "globally ordered from smallest to largest", 
                allTables.size(), schemaNames.length);
    
    return primeTablesInOrder(connection, allTables);
  }
  
  /**
   * Prime cache with parallel loading for better performance.
   * Still processes in size order but uses parallelism within size buckets.
   * 
   * @param connection Calcite connection
   * @param tables Tables to prime
   * @param parallelism Number of parallel threads
   * @return Priming result
   */
  public static PrimingResult primeTablesParallel(Connection connection, 
                                                  List<TableInfo> tables,
                                                  int parallelism) throws Exception {
    // Sort by size
    tables.sort(Comparator.comparingLong(t -> t.fileSize));
    
    // Group into buckets for parallel processing
    int bucketSize = Math.max(1, tables.size() / (parallelism * 2));
    List<List<TableInfo>> buckets = new ArrayList<>();
    
    for (int i = 0; i < tables.size(); i += bucketSize) {
      buckets.add(tables.subList(i, Math.min(i + bucketSize, tables.size())));
    }
    
    ExecutorService executor = Executors.newFixedThreadPool(parallelism);
    Map<String, Long> tableTimings = new ConcurrentHashMap<>();
    List<String> failures = Collections.synchronizedList(new ArrayList<>());
    long totalBytes = 0;
    int successCount = 0;
    
    long startTime = System.currentTimeMillis();
    
    try {
      // Process buckets in order
      for (List<TableInfo> bucket : buckets) {
        List<Future<Boolean>> futures = new ArrayList<>();
        
        // Submit tables in current bucket for parallel processing
        for (TableInfo table : bucket) {
          futures.add(executor.submit(() -> {
            long tableStart = System.currentTimeMillis();
            try {
              primeTable(connection, table);
              long duration = System.currentTimeMillis() - tableStart;
              tableTimings.put(table.toString(), duration);
              LOGGER.debug("Primed {} in {} ms", table, duration);
              return true;
            } catch (Exception e) {
              failures.add(table.toString() + ": " + e.getMessage());
              LOGGER.warn("Failed to prime {}: {}", table, e.getMessage());
              return false;
            }
          }));
          totalBytes += table.fileSize;
        }
        
        // Wait for current bucket to complete before moving to next
        for (Future<Boolean> future : futures) {
          if (future.get()) {
            successCount++;
          }
        }
      }
    } finally {
      executor.shutdown();
      executor.awaitTermination(1, TimeUnit.MINUTES);
    }
    
    long totalTime = System.currentTimeMillis() - startTime;
    
    return new PrimingResult(
        tables.size(), 
        successCount,
        tables.size() - successCount,
        totalTime,
        totalBytes,
        tableTimings,
        failures
    );
  }
  
  /**
   * Prime tables in order with detailed progress reporting.
   */
  private static PrimingResult primeTablesInOrder(Connection connection, 
                                                  List<TableInfo> tables) {
    Map<String, Long> tableTimings = new HashMap<>();
    List<String> failures = new ArrayList<>();
    long totalBytes = 0;
    int successCount = 0;
    
    long startTime = System.currentTimeMillis();
    
    for (int i = 0; i < tables.size(); i++) {
      TableInfo table = tables.get(i);
      long tableStart = System.currentTimeMillis();
      
      try {
        // Show progress
        if (i % 10 == 0 || table.fileSize > 100 * 1024 * 1024) { // Every 10 tables or large files
          LOGGER.info("Priming table {}/{}: {} ({})", 
              i + 1, tables.size(), table,
              i > 0 ? String.format("%.1f%% complete", 100.0 * i / tables.size()) : "starting");
        }
        
        primeTable(connection, table);
        
        long duration = System.currentTimeMillis() - tableStart;
        tableTimings.put(table.toString(), duration);
        totalBytes += table.fileSize;
        successCount++;
        
        LOGGER.debug("Primed {} in {} ms", table, duration);
        
      } catch (Exception e) {
        failures.add(table.toString() + ": " + e.getMessage());
        LOGGER.warn("Failed to prime {}: {}", table, e.getMessage());
      }
    }
    
    long totalTime = System.currentTimeMillis() - startTime;
    
    return new PrimingResult(
        tables.size(), 
        successCount,
        tables.size() - successCount,
        totalTime,
        totalBytes,
        tableTimings,
        failures
    );
  }
  
  /**
   * Prime a single table's statistics cache.
   */
  private static void primeTable(Connection connection, TableInfo tableInfo) 
      throws Exception {
    // For Parquet tables with StatisticsProvider, trigger statistics loading
    if (tableInfo.table instanceof StatisticsProvider) {
      StatisticsProvider statsProvider = (StatisticsProvider) tableInfo.table;
      
      // This will load statistics into memory cache
      TableStatistics stats = statsProvider.getTableStatistics(null);
      
      if (stats != null) {
        LOGGER.trace("Loaded statistics for {}: {} rows, {} columns",
            tableInfo, stats.getRowCount(), 
            stats.getColumnStatistics().size());
      }
    } else {
      // For other tables, execute a simple metadata query to trigger any caching
      String query = String.format(
          "SELECT * FROM %s.%s WHERE 1=0",
          tableInfo.schemaName, tableInfo.tableName
      );
      
      try (Statement stmt = connection.createStatement();
           ResultSet rs = stmt.executeQuery(query)) {
        // Just getting metadata triggers cache loading
        rs.getMetaData();
      }
    }
  }
  
  /**
   * Collect all tables from a schema with their file information.
   */
  @SuppressWarnings("deprecation")
  private static List<TableInfo> collectTables(String schemaName, SchemaPlus schema) {
    List<TableInfo> tables = new ArrayList<>();
    
    for (String tableName : schema.getTableNames()) {
      Table table = schema.getTable(tableName);
      File file = null;
      
      // Extract file information for size-based sorting
      if (table instanceof ParquetTranslatableTable) {
        try {
          // Use reflection to get the parquetFile field
          java.lang.reflect.Field fileField = 
              ParquetTranslatableTable.class.getDeclaredField("parquetFile");
          fileField.setAccessible(true);
          file = (File) fileField.get(table);
        } catch (Exception e) {
          LOGGER.debug("Could not extract file info for {}.{}", schemaName, tableName);
        }
      }
      
      tables.add(new TableInfo(schemaName, tableName, table, file));
    }
    
    return tables;
  }
  
  /**
   * Utility method for tests to prime cache before performance measurements.
   * 
   * @param jdbcUrl JDBC URL for Calcite connection
   * @param schemaName Schema to prime
   * @return Priming result
   */
  public static PrimingResult primeForTesting(String jdbcUrl, String schemaName) {
    try (Connection conn = DriverManager.getConnection(jdbcUrl)) {
      return primeSchema(conn, schemaName);
    } catch (Exception e) {
      LOGGER.error("Failed to prime cache for testing", e);
      return new PrimingResult(0, 0, 0, 0, 0, 
          Collections.emptyMap(), Collections.singletonList(e.getMessage()));
    }
  }
  
  /**
   * Clear all statistics caches (useful for testing cold-start performance).
   */
  @SuppressWarnings("deprecation")
  public static void clearAllCaches(Connection connection) throws Exception {
    CalciteConnection calciteConn = connection.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConn.getRootSchema();
    
    // Iterate through all schemas and tables
    for (String schemaName : rootSchema.getSubSchemaNames()) {
      SchemaPlus schema = rootSchema.getSubSchema(schemaName);
      for (String tableName : schema.getTableNames()) {
        Table table = schema.getTable(tableName);
        
        // Clear in-memory cache for tables with statistics
        if (table instanceof ParquetTranslatableTable) {
          try {
            java.lang.reflect.Field cacheField = 
                ParquetTranslatableTable.class.getDeclaredField("cachedStatistics");
            cacheField.setAccessible(true);
            cacheField.set(table, null);
          } catch (Exception e) {
            LOGGER.debug("Could not clear cache for {}.{}", schemaName, tableName);
          }
        }
      }
    }
    
    LOGGER.info("Cleared all statistics caches");
  }
}