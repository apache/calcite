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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.File;
import java.io.FileWriter;
import java.nio.charset.StandardCharsets;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test for XBRL adapter functionality.
 */
@Tag("unit")
public class SecSchemaTest {

  private String testDataDir;
  private File secSampleDir;
  private File modelFile;
  private TestInfo testInfo;

  @BeforeEach
  public void setUp(TestInfo testInfo) throws Exception {
    this.testInfo = testInfo;
    // Create unique test directory - NEVER use @TempDir
    String timestamp = String.valueOf(System.currentTimeMillis());
    String testName = testInfo.getTestMethod().get().getName();
    testDataDir = "build/test-data/" + getClass().getSimpleName() + "/" + testName + "_" + timestamp;
    File testDir = new File(testDataDir);
    testDir.mkdirs();
    
    // Create directories
    secSampleDir = new File(testDir, "sec-samples");
    secSampleDir.mkdirs();

    // Create a simple test XBRL file
    createSampleSecFile();

    // Create model.json
    modelFile = new File(testDir, "model.json");
    try (FileWriter writer = new FileWriter(modelFile, StandardCharsets.UTF_8)) {
      writer.write("{\n");
      writer.write("  \"version\": \"1.0\",\n");
      writer.write("  \"defaultSchema\": \"EDGAR\",\n");
      writer.write("  \"schemas\": [{\n");
      writer.write("    \"name\": \"EDGAR\",\n");
      writer.write("    \"type\": \"custom\",\n");
      writer.write("    \"factory\": \"org.apache.calcite.adapter.sec.SecSchemaFactory\",\n");
      writer.write("    \"operand\": {\n");
      writer.write("      \"directory\": \"" + testDataDir.replace("\\", "\\\\") + "\",\n");
      writer.write("      \"executionEngine\": \"linq4j\",\n");
      writer.write("      \"enableSecProcessing\": true,\n");
      writer.write("      \"secSourceDirectory\": \"" + secSampleDir.getAbsolutePath().replace("\\", "\\\\") + "\",\n");
      writer.write("      \"processSecOnInit\": true,\n");
      writer.write("      \"ephemeralCache\": true,\n");
      writer.write("      \"testMode\": true,\n");
      writer.write("      \"edgarSource\": {\n");
      writer.write("        \"autoDownload\": false,\n");
      writer.write("        \"useMockData\": true\n");
      writer.write("      }\n");
      writer.write("    }\n");
      writer.write("  }]\n");
      writer.write("}\n");
    }
  }

  private void createSampleSecFile() throws Exception {
    File secFile = new File(secSampleDir, "0000320193-20230930-10K.xml");
    try (FileWriter writer = new FileWriter(secFile, StandardCharsets.UTF_8)) {
      writer.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
      writer.write("<sec xmlns=\"http://www.sec.org/2003/instance\"\n");
      writer.write("      xmlns:us-gaap=\"http://fasb.org/us-gaap/2023\"\n");
      writer.write("      xmlns:dei=\"http://sec.sec.gov/dei/2023\">\n");
      writer.write("  <context id=\"c1\">\n");
      writer.write("    <entity>\n");
      writer.write("      <identifier scheme=\"http://www.sec.gov/CIK\">0000320193</identifier>\n");
      writer.write("    </entity>\n");
      writer.write("    <period>\n");
      writer.write("      <instant>2023-09-30</instant>\n");
      writer.write("    </period>\n");
      writer.write("  </context>\n");
      writer.write("  <unit id=\"usd\">\n");
      writer.write("    <measure>iso4217:USD</measure>\n");
      writer.write("  </unit>\n");
      writer.write("  <unit id=\"shares\">\n");
      writer.write("    <measure>seci:shares</measure>\n");
      writer.write("  </unit>\n");
      writer.write("  <!-- Company Info -->\n");
      writer.write("  <dei:EntityRegistrantName contextRef=\"c1\">Apple Inc.</dei:EntityRegistrantName>\n");
      writer.write("  <dei:EntityCentralIndexKey contextRef=\"c1\">0000320193</dei:EntityCentralIndexKey>\n");
      writer.write("  <dei:CurrentFiscalYearEndDate contextRef=\"c1\">--09-30</dei:CurrentFiscalYearEndDate>\n");
      writer.write("  <!-- Financial Data -->\n");
      writer.write("  <us-gaap:Revenue contextRef=\"c1\" unitRef=\"usd\" decimals=\"-6\">383285000000</us-gaap:Revenue>\n");
      writer.write("  <us-gaap:CostOfRevenue contextRef=\"c1\" unitRef=\"usd\" decimals=\"-6\">214137000000</us-gaap:CostOfRevenue>\n");
      writer.write("  <us-gaap:GrossProfit contextRef=\"c1\" unitRef=\"usd\" decimals=\"-6\">169148000000</us-gaap:GrossProfit>\n");
      writer.write("  <us-gaap:NetIncome contextRef=\"c1\" unitRef=\"usd\" decimals=\"-6\">96995000000</us-gaap:NetIncome>\n");
      writer.write("  <us-gaap:EarningsPerShareBasic contextRef=\"c1\" unitRef=\"usd\" decimals=\"2\">6.16</us-gaap:EarningsPerShareBasic>\n");
      writer.write("  <us-gaap:WeightedAverageNumberOfSharesOutstandingBasic contextRef=\"c1\" unitRef=\"shares\" decimals=\"-6\">15744231000</us-gaap:WeightedAverageNumberOfSharesOutstandingBasic>\n");
      writer.write("</sec>\n");
    }
  }

  @AfterEach
  void tearDown() {
    // Manual cleanup - NEVER rely on @TempDir
    try {
      if (testDataDir != null && Files.exists(Paths.get(testDataDir))) {
        Files.walk(Paths.get(testDataDir))
            .sorted(Comparator.reverseOrder())
            .map(Path::toFile)
            .forEach(File::delete);
      }
    } catch (IOException e) {
      // Log but don't fail test
      System.err.println("Warning: Could not clean test directory: " + e.getMessage());
    }
  }

  @Test public void testSecSchemaCreation() throws Exception {
    // Unit test - directly test the schema factory
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", testDataDir);
    operand.put("enableSecProcessing", true);
    operand.put("testMode", true);
    operand.put("ephemeralCache", true);
    
    SecSchemaFactory factory = new SecSchemaFactory();
    assertNotNull(factory);
    
    // For unit test, we're just verifying the factory can be instantiated
    // Actual schema creation would be an integration test
  }

  @Test public void testPartitionStrategy() {
    EdgarPartitionStrategy strategy = new EdgarPartitionStrategy();

    // Test partition path generation - uses fiscal year/period partitioning
    String path = strategy.getPartitionPath("320193", "10-K", "2023-09-30");
    assertEquals("cik=0000320193/filing_type=10-K/fiscal_year=2023/fiscal_period=FY/", path);

    // Test CIK normalization with quarterly filing
    path = strategy.getPartitionPath("1234", "10-Q", "2023-06-30");
    assertEquals("cik=0000001234/filing_type=10-Q/fiscal_year=2023/fiscal_period=Q2/", path);

    // Test filing type normalization (DEF 14A is not periodic, uses year/month)
    path = strategy.getPartitionPath("0000320193", "DEF 14A", "2023-03-15");
    assertEquals("cik=0000320193/filing_type=DEF_14A/filing_year=2023/filing_month=03/", path);
  }

  @Test 
  @Tag("integration")
  public void testSecToParquetConversion() throws Exception {
    // This test requires actual SEC to Parquet conversion
    // which involves processing XBRL files
    File targetDir = new File(testDataDir, "parquet-output");
    targetDir.mkdirs();

    SecToParquetConverter converter = new SecToParquetConverter(secSampleDir, targetDir);

    // Process XBRL files
    int processed = converter.processAllSecFiles();
    assertEquals(1, processed);

    // Check that Parquet files were created
    File lineItemsDir = new File(targetDir, "financial_line_items");
    assertTrue(lineItemsDir.exists());

    // Check partition structure
    File cikPartition = new File(lineItemsDir, "cik=0000320193");
    assertTrue(cikPartition.exists());

    File typePartition = new File(cikPartition, "filing_type=10-K");
    assertTrue(typePartition.exists());

    File datePartition = new File(typePartition, "filing_date=2023-09-30");
    assertTrue(datePartition.exists());

    // Check that Parquet file exists
    File[] parquetFiles = datePartition.listFiles((dir, name) -> name.endsWith(".parquet"));
    assertNotNull(parquetFiles);
    assertEquals(1, parquetFiles.length);
  }

  @Test 
  @Tag("integration")
  public void testQueryingSecData() throws Exception {
    // This test would require the full stack to be working
    // Including DuckDB or another engine to query the Parquet files

    Properties info = new Properties();
    info.setProperty("lex", "ORACLE");
    info.setProperty("unquotedCasing", "TO_LOWER");

    String url = "jdbc:calcite:model=" + modelFile.getAbsolutePath();

    try (Connection connection = DriverManager.getConnection(url, info)) {
      // Query financial line items with partition pruning
      // Note: "value" is a reserved word and must be quoted
      String sql = "SELECT concept, \"value\", unit " +
                   "FROM financial_line_items " +
                   "WHERE cik = '0000320193' " +
                   "  AND filing_type = '10-K' " +
                   "  AND filing_date = '2023-09-30'";

      try (PreparedStatement stmt = connection.prepareStatement(sql);
           ResultSet rs = stmt.executeQuery()) {

        int rowCount = 0;
        while (rs.next()) {
          String concept = rs.getString("concept");
          double value = rs.getDouble(2); // Use column index to avoid reserved word issue
          String unit = rs.getString("unit");

          assertNotNull(concept);
          assertTrue(value > 0 || rs.wasNull());
          assertNotNull(unit);

          rowCount++;
        }

        // Should have found some financial data
        assertTrue(rowCount > 0);
      }
    }
  }
}
