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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test XBRL adapter with document embeddings and DuckDB.
 */
@Tag("integration")
public class SecEmbeddingTest {

  private static final String DATA_DIR = getDataDirectory();

  private static String getDataDirectory() {
    // Check for environment variable first
    String envDataDir = System.getenv("XBRL_DATA_DIRECTORY");
    if (envDataDir != null && !envDataDir.isEmpty()) {
      System.out.println("Using XBRL_DATA_DIRECTORY from environment: " + envDataDir);
      return envDataDir;
    }
    // Default to current working directory with sec-data subdirectory
    String workingDir = System.getProperty("user.dir");
    String defaultDataDir = new File(workingDir, "sec-data").getPath();
    System.out.println("Using default data directory: " + defaultDataDir);
    return defaultDataDir;
  }

  @BeforeAll
  public static void ensureDataExists() throws Exception {
    File dataDir = new File(DATA_DIR);
    File secDir = new File(dataDir, "sec");
    File parquetDir = new File(dataDir, "parquet");

    // Clean up old parquet files to ensure fresh generation with embeddings
    if (parquetDir.exists()) {
      deleteDirectory(parquetDir);
    }

    // Ensure XBRL data exists
    if (!secDir.exists() || secDir.listFiles().length == 0) {
      System.out.println("Data not found, downloading XBRL files...");
      downloadSecData(secDir);
    }

    // Convert XBRL to Parquet with embeddings
    System.out.println("Converting XBRL to Parquet with embeddings...");
    System.out.println("XBRL directory: " + secDir.getAbsolutePath());
    System.out.println("Parquet directory: " + parquetDir.getAbsolutePath());
    SecToParquetConverter converter = new SecToParquetConverter(secDir.getAbsoluteFile(), parquetDir.getAbsoluteFile());
    int processed = converter.processAllSecFiles();
    System.out.println("Conversion complete. Processed " + processed + " XBRL files with embeddings.");
  }

  private static void downloadSecData(File secDir) throws Exception {
    if (!secDir.exists()) {
      secDir.mkdirs();
    }

    System.out.println("Downloading to: " + secDir.getAbsolutePath());

    // Minimal configuration - only CIK is required
    Map<String, Object> edgarConfig = new HashMap<>();

    // Use environment variable or default to Apple
    String ciks = System.getenv("EDGAR_CIKS");
    if (ciks == null || ciks.isEmpty()) {
      ciks = "AAPL";  // Default to Apple ticker
    }
    edgarConfig.put("cik", ciks);

    // Everything else uses smart defaults:
    // - filingTypes: ALL 15 filing types
    // - startYear: 2009
    // - endYear: current year
    // - autoDownload: true

    // Allow test-specific overrides if needed
    String testStartYear = System.getenv("TEST_START_YEAR");
    if (testStartYear != null) {
      edgarConfig.put("startYear", Integer.parseInt(testStartYear));
    }
    String testEndYear = System.getenv("TEST_END_YEAR");
    if (testEndYear != null) {
      edgarConfig.put("endYear", Integer.parseInt(testEndYear));
    }

    // Download filings
    EdgarDownloader downloader = new EdgarDownloader(edgarConfig, secDir);
    List<File> downloaded = downloader.downloadFilings();

    System.out.println("Downloaded " + downloaded.size() + " XBRL files");
  }

  private static void deleteDirectory(File dir) {
    if (dir.exists()) {
      File[] files = dir.listFiles();
      if (files != null) {
        for (File file : files) {
          if (file.isDirectory()) {
            deleteDirectory(file);
          } else {
            file.delete();
          }
        }
      }
      dir.delete();
    }
  }

  @Test public void testSecWithEmbeddings() throws Exception {
    System.out.println("\n=== Testing XBRL Adapter with Embeddings (DuckDB) ===\n");

    // Create model with DuckDB
    File modelFile = createModel();

    Properties info = new Properties();
    info.setProperty("lex", "ORACLE");
    info.setProperty("unquotedCasing", "TO_LOWER");

    String url = "jdbc:calcite:model=" + modelFile.getAbsolutePath();

    // Register driver
    Class.forName("org.apache.calcite.jdbc.Driver");
    try (Connection conn = DriverManager.getConnection(url, info)) {
      System.out.println("Connected via DuckDB to XBRL Parquet data\n");

      // Test 1: List all tables
      System.out.println("Test 1: Available Tables");
      System.out.println("========================");
      DatabaseMetaData md = conn.getMetaData();
      ResultSet tables = md.getTables(null, "SEC", "%", null);
      int tableCount = 0;
      while (tables.next()) {
        String tableName = tables.getString("TABLE_NAME");
        System.out.println("  - " + tableName);
        tableCount++;
      }
      assertTrue(tableCount >= 4, "Should have at least 4 tables (financial_line_items, company_info, document_embeddings, footnotes)");

      // Test 2: Query financial metrics with fiscal period partitioning
      System.out.println("\nTest 2: Financial Metrics by Fiscal Period");
      System.out.println("===========================================");
      String sql = "SELECT fiscal_year, fiscal_period, concept, \"value\" " +
                   "FROM financial_line_items " +
                   "WHERE cik = '0000320193' " +
                   "  AND filing_type = '10-K' " +
                   "  AND fiscal_year >= '2020' " +
                   "  AND concept IN ('Revenue', 'NetIncomeLoss', 'Assets') " +
                   "ORDER BY fiscal_year DESC, concept";

      int metricCount = executeAndCount(conn, sql, true);
      assertTrue(metricCount > 0, "Should find financial metrics");

      // Test 3: Query contextual embeddings
      System.out.println("\nTest 3: Document Embeddings - Contextual Chunks");
      System.out.println("================================================");
      sql = "SELECT chunk_context, chunk_length, " +
            "       has_revenue_data, has_risk_factors, has_footnotes " +
            "FROM document_embeddings " +
            "WHERE cik = '0000320193' " +
            "  AND filing_type = '10-K' " +
            "  AND fiscal_year = '2023' " +
            "ORDER BY chunk_context";

      int chunkCount = executeAndCount(conn, sql, true);
      assertTrue(chunkCount > 0, "Should have contextual chunks for 2023");

      // Test 4: Find similar embeddings using vector functions
      System.out.println("\nTest 4: Find Similar Documents Using Vector Similarity");
      System.out.println("======================================================");
      // First get a reference embedding
      sql = "SELECT embedding_vector FROM document_embeddings " +
            "WHERE cik = '0000320193' AND fiscal_year = '2023' " +
            "  AND chunk_context = 'Revenue' LIMIT 1";

      String referenceVector = null;
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(sql)) {
        if (rs.next()) {
          referenceVector = rs.getString(1);
        }
      }

      if (referenceVector != null) {
        // Find similar chunks using cosine similarity
        sql = "SELECT fiscal_year, chunk_context, " +
              "       COSINE_SIMILARITY(embedding_vector, '" + referenceVector + "') as similarity, " +
              "       SUBSTRING(chunk_text, 1, 100) as text_preview " +
              "FROM document_embeddings " +
              "WHERE cik = '0000320193' " +
              "  AND chunk_context != 'FULL_DOCUMENT' " +
              "  AND fiscal_year != '2023' " +
              "ORDER BY similarity DESC " +
              "LIMIT 5";

        int similarCount = executeAndCount(conn, sql, true);
        assertTrue(similarCount > 0, "Should find similar embeddings");
      }

      // Test 4b: Original revenue-related test
      System.out.println("\nTest 4b: Revenue-Related Text Chunks");
      System.out.println("=====================================");
      sql = "SELECT fiscal_year, chunk_context, " +
            "       SUBSTRING(chunk_text, 1, 200) as text_preview " +
            "FROM document_embeddings " +
            "WHERE cik = '0000320193' " +
            "  AND has_revenue_data = true " +
            "  AND chunk_context != 'FULL_DOCUMENT' " +
            "ORDER BY fiscal_year DESC " +
            "LIMIT 5";

      int revenueChunks = executeAndCount(conn, sql, true);
      assertTrue(revenueChunks > 0, "Should find revenue-related chunks");

      // Test 5: Join financial metrics with embeddings
      System.out.println("\nTest 5: Correlate Metrics with Narrative");
      System.out.println("=========================================");
      sql = "SELECT f.fiscal_year, f.concept, f.\"value\", " +
            "       e.chunk_context, e.has_risk_factors " +
            "FROM financial_line_items f " +
            "JOIN document_embeddings e ON " +
            "     f.cik = e.cik " +
            "     AND f.filing_type = e.filing_type " +
            "     AND f.fiscal_year = e.fiscal_year " +
            "WHERE f.cik = '0000320193' " +
            "  AND f.filing_type = '10-K' " +
            "  AND f.fiscal_year = '2023' " +
            "  AND f.concept LIKE '%Revenue%' " +
            "  AND e.chunk_context = 'Revenue' " +
            "LIMIT 5";

      int correlatedCount = executeAndCount(conn, sql, true);

      // Test 6: Analyze filing complexity over time
      System.out.println("\nTest 6: Document Complexity Analysis");
      System.out.println("====================================");
      sql = "SELECT fiscal_year, " +
            "       COUNT(*) as chunk_count, " +
            "       AVG(chunk_length) as avg_chunk_length, " +
            "       SUM(CASE WHEN has_risk_factors THEN 1 ELSE 0 END) as risk_chunks " +
            "FROM document_embeddings " +
            "WHERE cik = '0000320193' " +
            "  AND filing_type = '10-K' " +
            "GROUP BY fiscal_year " +
            "ORDER BY fiscal_year DESC";

      executeAndCount(conn, sql, true);

      // Test 7: Event-driven filings (8-K) with different partitioning
      System.out.println("\nTest 7: Event-Driven Filings (8-K)");
      System.out.println("===================================");
      sql = "SELECT filing_year, filing_month, COUNT(*) as filing_count " +
            "FROM financial_line_items " +
            "WHERE cik = '0000320193' " +
            "  AND filing_type = '8-K' " +
            "GROUP BY filing_year, filing_month " +
            "ORDER BY filing_year DESC, filing_month DESC " +
            "LIMIT 10";

      executeAndCount(conn, sql, true);

      // Test 8: Full document embeddings
      System.out.println("\nTest 8: Full Document Embeddings");
      System.out.println("=================================");
      sql = "SELECT fiscal_year, chunk_length as doc_length " +
            "FROM document_embeddings " +
            "WHERE cik = '0000320193' " +
            "  AND filing_type = '10-K' " +
            "  AND chunk_context = 'FULL_DOCUMENT' " +
            "ORDER BY fiscal_year DESC";

      int fullDocCount = executeAndCount(conn, sql, true);
      assertTrue(fullDocCount > 0, "Should have full document embeddings");

      System.out.println("\n=== All Tests Passed! ===");
      System.out.println("Successfully queried:");
      System.out.println("  - Financial metrics with fiscal period partitioning");
      System.out.println("  - Contextual document embeddings");
      System.out.println("  - Revenue-related text chunks");
      System.out.println("  - Correlated metrics and narratives");
      System.out.println("  - Document complexity over time");
      System.out.println("  - Event-driven filings");
      System.out.println("  - Full document embeddings");
    }
  }

  private File createModel() throws Exception {
    // Use the test embedding model from resources
    InputStream modelStream = getClass().getResourceAsStream("/test-embedding-model.json");
    if (modelStream == null) {
      throw new IOException("Cannot find test-embedding-model.json in resources");
    }

    // Read the model template
    String modelTemplate = new String(modelStream.readAllBytes());

    // Replace the directory path with the actual test data directory
    File dataDir = new File(DATA_DIR);
    if (!dataDir.exists()) {
      dataDir.mkdirs();
    }

    // When XBRL_DATA_DIRECTORY is set, use relative path "parquet"
    // Otherwise use absolute path
    String parquetPath;
    if (System.getenv("XBRL_DATA_DIRECTORY") != null) {
      parquetPath = "parquet";  // Relative to dataDirectory
    } else {
      parquetPath = new File(dataDir, "parquet").getAbsolutePath().replace("\\", "/");
    }

    String modelJson =
        modelTemplate.replace("\"directory\": \"build/apple-edgar-test-data/parquet\"",
        "\"directory\": \"" + parquetPath + "\"");

    // Write the customized model file
    File modelFile = new File(dataDir, "test-model.json");
    java.nio.file.Files.write(modelFile.toPath(), modelJson.getBytes());

    return modelFile;
  }

  private int executeAndCount(Connection conn, String sql, boolean print) throws SQLException {
    try (Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {

      ResultSetMetaData rsmd = rs.getMetaData();
      int columnCount = rsmd.getColumnCount();

      if (print) {
        // Print headers
        for (int i = 1; i <= columnCount; i++) {
          System.out.printf("%-20s ", rsmd.getColumnName(i));
        }
        System.out.println();
        for (int i = 1; i <= columnCount; i++) {
          System.out.print("-------------------- ");
        }
        System.out.println();
      }

      int rowCount = 0;
      while (rs.next()) {
        if (print && rowCount < 10) { // Only print first 10 rows
          for (int i = 1; i <= columnCount; i++) {
            Object value = rs.getObject(i);
            if (value instanceof Number) {
              System.out.printf("%-20.0f ", ((Number) value).doubleValue());
            } else if (value instanceof String) {
              String str = value.toString();
              if (str.length() > 20) {
                str = str.substring(0, 17) + "...";
              }
              System.out.printf("%-20s ", str);
            } else {
              System.out.printf("%-20s ", value != null ? value.toString() : "NULL");
            }
          }
          System.out.println();
        }
        rowCount++;
      }

      if (print) {
        if (rowCount > 10) {
          System.out.println("... (" + (rowCount - 10) + " more rows)");
        }
        System.out.println("Total: " + rowCount + " rows");
      }

      return rowCount;
    }
  }
}
