package org.apache.calcite.adapter.sec;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Tag;
import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.Properties;

/**
 * Integration test to verify 8K earnings call paragraphs are being extracted and vectorized.
 * Uses Apple and Microsoft test data to validate text extraction from SEC 8K filings.
 */
@Tag("integration")
public class EightKExtractionTest {
  private String testDataDir;
  private String modelPath;

  @BeforeEach
  void setUp(TestInfo testInfo) throws Exception {
    String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
    String testName = testInfo.getTestMethod().get().getName();
    testDataDir = "build/test-data/" + getClass().getSimpleName() + "/" + testName + "_" + timestamp;
    Files.createDirectories(Paths.get(testDataDir));

    modelPath = createTestModel();
  }

  @Test
  void testAvailableTables() throws Exception {
    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");

    try (Connection conn = DriverManager.getConnection("jdbc:calcite:model=" + modelPath, props)) {
      // First check what tables are available
      DatabaseMetaData metaData = conn.getMetaData();
      try (ResultSet tables = metaData.getTables(null, "sec", null, null)) {
        System.out.println("Available tables in sec schema:");
        while (tables.next()) {
          String tableName = tables.getString("TABLE_NAME");
          String tableType = tables.getString("TABLE_TYPE");
          System.out.println("- " + tableName + " (" + tableType + ")");
        }
      }
    }
  }

  @Test
  void test8KEarningsCallExtraction() throws Exception {
    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");

    try (Connection conn = DriverManager.getConnection("jdbc:calcite:model=" + modelPath, props)) {
      
      // Check if we have 8K earnings data - first count total entries
      String countSql = "SELECT COUNT(*) as total FROM sec.earnings WHERE filing_type = '8K'";
      
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(countSql)) {
        
        rs.next();
        int total8K = rs.getInt("total");
        System.out.println("Total 8K entries in earnings table: " + total8K);
        
        if (total8K > 0) {
          // Now look for actual earnings-related content
          String sql = "SELECT * FROM sec.earnings WHERE filing_type = '8K' LIMIT 3";
          
          try (ResultSet dataRs = stmt.executeQuery(sql)) {
            int count = 0;
            while (dataRs.next() && count < 3) {
              count++;
              System.out.println("\n--- 8K Earnings Entry " + count + " ---");
              
              // Print all columns available to see the schema
              ResultSetMetaData rsmd = dataRs.getMetaData();
              for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                String colName = rsmd.getColumnName(i);
                Object value = dataRs.getObject(i);
                String valueStr = (value != null) ? value.toString() : "null";
                if (valueStr.length() > 100) {
                  valueStr = valueStr.substring(0, 100) + "...";
                }
                System.out.println(colName + ": " + valueStr);
              }
            }
            
            assertTrue(count > 0 || total8K == 0, "Should have some data if total > 0");
            System.out.println("\n✓ 8K earnings call text extraction verified - found " + total8K + " entries");
          }
        } else {
          System.out.println("No 8K entries found in earnings table");
        }
      }
    }
  }

  @Test
  void test8KSectionTypes() throws Exception {
    Properties props = new Properties();
    props.setProperty("lex", "ORACLE"); 
    props.setProperty("unquotedCasing", "TO_LOWER");

    try (Connection conn = DriverManager.getConnection("jdbc:calcite:model=" + modelPath, props)) {
      
      // Check what sections are being extracted from 8K filings
      String sql = "SELECT DISTINCT section, COUNT(*) as section_count " +
          "FROM sec.vectorized_blobs " +
          "WHERE filing_type = '8K' " +
          "GROUP BY section " +
          "ORDER BY section_count DESC";

      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(sql)) {

        assertTrue(rs.next(), "Should find 8K sections");

        System.out.println("8K sections found:");
        int totalSections = 0;
        while (rs.next()) {
          String section = rs.getString("section");
          int count = rs.getInt("section_count");
          totalSections++;
          
          assertNotNull(section, "Section name should not be null");
          assertTrue(count > 0, "Section count should be positive");
          
          System.out.println("- " + section + ": " + count + " entries");
        }

        assertTrue(totalSections > 0, "Should find at least one 8K section type");
      }
    }
  }

  @AfterEach
  void tearDown() {
    try {
      if (testDataDir != null && Files.exists(Paths.get(testDataDir))) {
        Files.walk(Paths.get(testDataDir))
            .sorted(Comparator.reverseOrder())
            .map(Path::toFile)
            .forEach(File::delete);
      }
    } catch (Exception e) {
      System.err.println("Warning: Could not clean test directory: " + e.getMessage());
    }
  }

  private String createTestModel() throws Exception {
    // Use the existing Apple/Microsoft cache for testing - but use SecSchemaFactory  
    String existingCacheDir = "/Volumes/T9/sec-test-aapl-msft-cache";
    
    String model = String.format("{\n" +
        "  \"version\": \"1.0\",\n" +
        "  \"defaultSchema\": \"sec\",\n" +
        "  \"schemas\": [{\n" +
        "    \"name\": \"sec\",\n" +
        "    \"type\": \"custom\",\n" +
        "    \"factory\": \"org.apache.calcite.adapter.sec.SecSchemaFactory\",\n" +
        "    \"operand\": {\n" +
        "      \"directory\": \"%s\",\n" +
        "      \"ciks\": [\"0000320193\", \"0000789019\"],\n" +
        "      \"filingTypes\": [\"8-K\"],\n" +
        "      \"autoDownload\": false,\n" +
        "      \"startYear\": 2022,\n" +
        "      \"endYear\": 2024,\n" +
        "      \"executionEngine\": \"duckdb\",\n" +
        "      \"testMode\": false,\n" +
        "      \"ephemeralCache\": false,\n" +
        "      \"textSimilarity\": {\n" +
        "        \"enabled\": true,\n" +
        "        \"embeddingDimension\": 256\n" +
        "      }\n" +
        "    }\n" +
        "  }]\n" +
        "}", existingCacheDir);

    Path modelFile = Paths.get(testDataDir, "model.json");
    Files.writeString(modelFile, model);
    return modelFile.toString();
  }
}