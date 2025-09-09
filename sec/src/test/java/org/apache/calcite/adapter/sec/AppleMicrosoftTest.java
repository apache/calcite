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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test SEC adapter with Apple and Microsoft 10-K data for 2022-2023.
 */
@Tag("integration")
public class AppleMicrosoftTest {

  private String modelPath;

  @BeforeEach
  void setUp(TestInfo testInfo) throws Exception {
    // Use the test model with Apple and Microsoft
    modelPath = AppleMicrosoftTest.class.getResource("/test-aapl-msft-model.json").getPath();
    assertTrue(Files.exists(Paths.get(modelPath)), "Model file should exist");
  }

  @Test void testFinancialLineItemsQuery() throws Exception {
    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    props.setProperty("conformance", "DEFAULT");

    String jdbcUrl = "jdbc:calcite:model=" + modelPath;

    try (Connection conn = DriverManager.getConnection(jdbcUrl, props);
         Statement stmt = conn.createStatement()) {

      // First, check if the table exists
      System.out.println("Testing financial_line_items table access...");

      String query =
        "SELECT cik, filing_type, \"year\", concept, \"value\", numeric_value " +
        "FROM sec.financial_line_items " +
        "WHERE cik IN ('0000320193', '0000789019') " +
        "  AND filing_type = '10K' " +
        "  AND LOWER(concept) LIKE '%netincome%' " +
        "  AND \"year\" >= 2022 " +
        "ORDER BY cik, \"year\" " +
        "LIMIT 10";

      System.out.println("Executing query: " + query);

      try (ResultSet rs = stmt.executeQuery(query)) {
        ResultSetMetaData meta = rs.getMetaData();
        System.out.println("Column count: " + meta.getColumnCount());

        // Find and verify year column type
        int yearColumnIndex = -1;
        for (int i = 1; i <= meta.getColumnCount(); i++) {
          String colName = meta.getColumnName(i);
          String colType = meta.getColumnTypeName(i);
          System.out.println("Column " + i + ": " + colName + " (" + colType + ")");
          if (colName.equalsIgnoreCase("year")) {
            yearColumnIndex = i;
            // Verify year is typed as INTEGER
            assertTrue(colType.contains("INT") || colType.equals("INTEGER"), 
                      "Year column should be INTEGER type, but was: " + colType);
          }
        }
        assertTrue(yearColumnIndex > 0, "Year column should exist");

        int rowCount = 0;
        while (rs.next()) {
          String cik = rs.getString("cik");
          String filingType = rs.getString("filing_type");
          // Now year should be retrievable as an integer directly
          int year = rs.getInt("year");
          String concept = rs.getString("concept");
          String valueStr = rs.getString(5);  // "value" column
          double numericValue = rs.getDouble("numeric_value");

          System.out.printf("Row %d: CIK=%s, Filing=%s, Year=%d, Concept=%s, Value=%s, NumericValue=%,.0f%n",
              ++rowCount, cik, filingType, year, concept, valueStr, numericValue);

          // Validate data
          assertTrue(cik.equals("0000320193") || cik.equals("0000789019"), "Should be Apple or Microsoft");
          assertTrue(year >= 2022 && year <= 2024, "Should be 2022-2024");
          assertTrue(numericValue > 0, "Net income should be positive");
        }

        assertTrue(rowCount > 0, "Should have found some net income data");
        System.out.println("Successfully queried " + rowCount + " rows");
      }
    }
  }

  @Test void testAllTablesAvailable() throws Exception {
    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");

    String jdbcUrl = "jdbc:calcite:model=" + modelPath;

    try (Connection conn = DriverManager.getConnection(jdbcUrl, props)) {
      
      // Test each expected table with a simple count query
      System.out.println("Testing available SEC tables:");
      
      // Test financial_line_items
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM sec.financial_line_items")) {
        assertTrue(rs.next());
        int count = rs.getInt(1);
        System.out.println("  - financial_line_items: " + count + " rows");
        assertTrue(count > 0, "financial_line_items should have data");
      }
      
      // Test filing_contexts
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM sec.filing_contexts")) {
        assertTrue(rs.next());
        int count = rs.getInt(1);
        System.out.println("  - filing_contexts: " + count + " rows");
        assertTrue(count > 0, "filing_contexts should have data");
      }
      
      // Test mda_sections
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM sec.mda_sections")) {
        assertTrue(rs.next());
        int count = rs.getInt(1);
        System.out.println("  - mda_sections: " + count + " rows");
        assertTrue(count > 0, "mda_sections should have data");
      }
      
      // Test xbrl_relationships - this is the one user said is missing
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM sec.xbrl_relationships")) {
        assertTrue(rs.next());
        int count = rs.getInt(1);
        System.out.println("  - xbrl_relationships: " + count + " rows");
        assertTrue(count > 0, "xbrl_relationships should have data");
      }
      
      // Test vectorized_blobs table - should exist when enableVectorization=true
      System.out.println("\n== Vectorization Tables (enableVectorization=true) ==");
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM sec.vectorized_blobs")) {
        assertTrue(rs.next());
        int count = rs.getInt(1);
        System.out.println("  - vectorized_blobs: " + count + " rows");
        assertTrue(count > 0, "vectorized_blobs should have data when enableVectorization=true");
        
        // Check that we have footnotes and MD&A
        try (Statement stmt2 = conn.createStatement();
             ResultSet rs2 = stmt2.executeQuery(
               "SELECT blob_type, COUNT(*) as cnt FROM sec.vectorized_blobs " +
               "GROUP BY blob_type ORDER BY blob_type")) {
          System.out.println("  Blob types:");
          while (rs2.next()) {
            System.out.println("    - " + rs2.getString("blob_type") + ": " + rs2.getInt("cnt"));
          }
        }
      } catch (SQLException e) {
        fail("vectorized_blobs table should exist when enableVectorization=true: " + e.getMessage());
      }
      
      // Check if insider_transactions table exists (might not have data for 10-K filings)
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM sec.insider_transactions")) {
        assertTrue(rs.next());
        int count = rs.getInt(1);
        System.out.println("  - insider_transactions: " + count + " rows (may be 0 for 10-K only)");
      } catch (SQLException e) {
        System.out.println("  - insider_transactions: TABLE NOT FOUND");
      }
      
      // Check if earnings_transcripts table exists (might not have data for 10-K filings)  
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM sec.earnings_transcripts")) {
        assertTrue(rs.next());
        int count = rs.getInt(1);
        System.out.println("  - earnings_transcripts: " + count + " rows (may be 0 for 10-K only)");
      } catch (SQLException e) {
        System.out.println("  - earnings_transcripts: TABLE NOT FOUND");
      }
      
      System.out.println("\nSummary: 4 core tables + vectorization tables found. " + 
                         "Note: insider_transactions and earnings_transcripts tables are only " +
                         "populated when Forms 3/4/5 and 8-K filings are downloaded.");
    }
  }

  @Test void testRelationshipsTable() throws Exception {
    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");

    String jdbcUrl = "jdbc:calcite:model=" + modelPath;

    try (Connection conn = DriverManager.getConnection(jdbcUrl, props);
         Statement stmt = conn.createStatement()) {

      // Test that xbrl_relationships table exists and has data
      String query = 
        "SELECT cik, filing_type, \"year\", from_concept, to_concept, arc_role " +
        "FROM sec.xbrl_relationships " +
        "WHERE cik = '0000320193' " +
        "LIMIT 10";

      System.out.println("Testing xbrl_relationships table...");
      
      try (ResultSet rs = stmt.executeQuery(query)) {
        ResultSetMetaData meta = rs.getMetaData();
        System.out.println("xbrl_relationships columns:");
        
        for (int i = 1; i <= meta.getColumnCount(); i++) {
          System.out.println("  " + meta.getColumnName(i) + " - " + meta.getColumnTypeName(i));
        }
        
        int rowCount = 0;
        while (rs.next()) {
          rowCount++;
          if (rowCount <= 3) {
            System.out.printf("  Row %d: from=%s, to=%s, role=%s%n",
                rowCount,
                rs.getString("from_concept"),
                rs.getString("to_concept"),
                rs.getString("arc_role"));
          }
        }
        
        assertTrue(rowCount > 0, "Should have relationship data");
        System.out.println("Found " + rowCount + " relationships");
      }
    }
  }

  @Test void testTableSchema() throws Exception {
    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");

    String jdbcUrl = "jdbc:calcite:model=" + modelPath;

    try (Connection conn = DriverManager.getConnection(jdbcUrl, props);
         Statement stmt = conn.createStatement()) {

      // Check table metadata
      String query =
        "SELECT * " +
        "FROM sec.financial_line_items " +
        "WHERE 1=0";

      try (ResultSet rs = stmt.executeQuery(query)) {
        ResultSetMetaData meta = rs.getMetaData();

        System.out.println("Table schema for financial_line_items:");
        System.out.println("Column count: " + meta.getColumnCount());

        // Verify expected columns exist
        boolean hasCik = false;
        boolean hasFilingType = false;
        boolean hasYear = false;
        boolean hasLineItem = false;
        boolean hasValue = false;

        for (int i = 1; i <= meta.getColumnCount(); i++) {
          String colName = meta.getColumnName(i).toLowerCase();
          System.out.println("  Column " + i + ": " + colName + " - " + meta.getColumnTypeName(i));

          if (colName.equals("cik")) hasCik = true;
          if (colName.equals("filing_type")) hasFilingType = true;
          if (colName.contains("year")) hasYear = true;
          if (colName.contains("line_item") || colName.contains("lineitem") || colName.contains("metric") || colName.contains("concept")) hasLineItem = true;
          if (colName.equals("value") || colName.contains("amount")) hasValue = true;
        }

        assertTrue(hasCik, "Should have cik column");
        assertTrue(hasFilingType, "Should have filing_type column");
        assertTrue(hasYear, "Should have year column");
        assertTrue(hasLineItem, "Should have line_item column");
        assertTrue(hasValue, "Should have value column");
      }
    }
  }

  @Test void testConceptGroupCosineSimilaritySearch() throws Exception {
    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");

    String jdbcUrl = "jdbc:calcite:model=" + modelPath;

    try (Connection conn = DriverManager.getConnection(jdbcUrl, props);
         Statement stmt = conn.createStatement()) {

      System.out.println("\n=== Testing Cosine Similarity Search on Footnotes ===");

      // First, verify we have footnote embeddings in vectorized_blobs
      String checkQuery = 
        "SELECT blob_type, COUNT(*) as cnt " +
        "FROM sec.vectorized_blobs " +
        "WHERE blob_type = 'footnote' " +
        "GROUP BY blob_type";

      try (ResultSet rs = stmt.executeQuery(checkQuery)) {
        assertTrue(rs.next(), "Should have footnote embeddings");
        int footnoteCount = rs.getInt("cnt");
        System.out.println("Found " + footnoteCount + " footnote embeddings");
        assertTrue(footnoteCount > 0, "Should have at least some footnote embeddings");
      }

      // Now test cosine similarity search
      // Search for footnotes related to "revenue recognition" or "accounting policies"
      String searchQuery = 
        "SELECT " +
        "  v1.cik, " +
        "  v1.filing_type, " +
        "  v1.\"year\", " +
        "  v1.original_blob_id as blob_id, " +
        "  v1.original_text, " +
        "  COSINE_SIMILARITY(v1.embedding, v2.embedding) as similarity_score " +
        "FROM sec.vectorized_blobs v1, " +
        "     (SELECT embedding FROM sec.vectorized_blobs " +
        "      WHERE blob_type = 'footnote' " +
        "      AND (LOWER(original_text) LIKE '%revenue recognition%' " +
        "           OR LOWER(original_text) LIKE '%accounting polic%') " +
        "      LIMIT 1) v2 " +
        "WHERE v1.blob_type = 'footnote' " +
        "  AND v1.cik IN ('0000320193', '0000789019') " +
        "  AND COSINE_SIMILARITY(v1.embedding, v2.embedding) > 0.7 " +
        "ORDER BY similarity_score DESC " +
        "LIMIT 10";

      System.out.println("\nSearching for footnotes similar to 'revenue recognition' or 'accounting policies'...");
      System.out.println("Query: " + searchQuery);

      try (ResultSet rs = stmt.executeQuery(searchQuery)) {
        int resultCount = 0;
        System.out.println("\nTop similar footnotes:");
        System.out.println("=" + "=".repeat(80));
        
        while (rs.next()) {
          resultCount++;
          String cik = rs.getString("cik");
          String filingType = rs.getString("filing_type");
          int year = rs.getInt("year");
          String blobId = rs.getString("blob_id");
          String textContent = rs.getString("original_text");
          double similarity = rs.getDouble("similarity_score");
          
          // Truncate text for display
          String displayText = textContent.length() > 200 ? 
            textContent.substring(0, 200) + "..." : textContent;
          
          System.out.printf("\n%d. CIK: %s | Filing: %s | Year: %d | Similarity: %.4f%n",
              resultCount, cik, filingType, year, similarity);
          System.out.println("   Blob ID: " + blobId);
          System.out.println("   Text: " + displayText.replaceAll("\\s+", " "));
          System.out.println("-" + "-".repeat(79));
        }
        
        assertTrue(resultCount > 0, "Should find similar footnotes using cosine similarity");
        System.out.println("\nFound " + resultCount + " similar footnotes");
      }

      // Test another similarity search - look for footnotes about "stock compensation"
      String stockCompQuery = 
        "SELECT " +
        "  v1.cik, " +
        "  v1.filing_type, " +
        "  v1.\"year\", " +
        "  v1.blob_id, " +
        "  SUBSTRING(v1.original_text, 1, 150) as snippet, " +
        "  COSINE_SIMILARITY(v1.embedding, v2.embedding) as similarity_score " +
        "FROM sec.vectorized_blobs v1, " +
        "     (SELECT embedding FROM sec.vectorized_blobs " +
        "      WHERE blob_type = 'footnote' " +
        "      AND LOWER(original_text) LIKE '%stock%compensation%' " +
        "      LIMIT 1) v2 " +
        "WHERE v1.blob_type = 'footnote' " +
        "  AND v1.cik = '0000320193' " +  // Apple only for this test
        "  AND v1.\"year\" >= 2022 " +
        "  AND COSINE_SIMILARITY(v1.embedding, v2.embedding) > 0.75 " +
        "ORDER BY similarity_score DESC " +
        "LIMIT 5";

      System.out.println("\n\nSearching for Apple footnotes similar to 'stock compensation'...");
      
      try (ResultSet rs = stmt.executeQuery(stockCompQuery)) {
        int resultCount = 0;
        System.out.println("\nTop similar footnotes about stock compensation:");
        System.out.println("=" + "=".repeat(80));
        
        while (rs.next()) {
          resultCount++;
          int year = rs.getInt("year");
          String snippet = rs.getString("snippet");
          double similarity = rs.getDouble("similarity_score");
          
          System.out.printf("\n%d. Year: %d | Similarity: %.4f%n", resultCount, year, similarity);
          System.out.println("   Text: " + snippet.replaceAll("\\s+", " "));
        }
        
        assertTrue(resultCount > 0, "Should find footnotes about stock compensation");
        System.out.println("\nFound " + resultCount + " footnotes about stock compensation");
      }

      // Test that the COSINE_SIMILARITY function works with proper vector dimensions
      String dimensionTestQuery = 
        "SELECT " +
        "  COUNT(*) as total_footnotes, " +
        "  AVG(CARDINALITY(embedding)) as avg_dimension " +
        "FROM sec.vectorized_blobs " +
        "WHERE blob_type = 'footnote' " +
        "  AND cik IN ('0000320193', '0000789019')";

      try (ResultSet rs = stmt.executeQuery(dimensionTestQuery)) {
        assertTrue(rs.next());
        int totalFootnotes = rs.getInt("total_footnotes");
        double avgDimension = rs.getDouble("avg_dimension");
        
        System.out.println("\n\nEmbedding Statistics:");
        System.out.println("  Total footnotes with embeddings: " + totalFootnotes);
        System.out.println("  Average embedding dimension: " + avgDimension);
        
        // The model config specified 256 dimensions
        assertEquals(256.0, avgDimension, 0.01, 
            "Embeddings should be 256-dimensional as configured");
      }

      System.out.println("\n=== Cosine Similarity Search Test Completed Successfully ===");
    }
  }
}
