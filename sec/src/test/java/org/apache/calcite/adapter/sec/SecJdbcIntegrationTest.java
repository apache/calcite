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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInfo;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * JDBC integration test for SEC adapter following standard pattern.
 * Tests Model → Connection → Query → Assert pattern.
 */
@Tag("integration")
public class SecJdbcIntegrationTest {
    private String testDataDir;
    private String modelPath;
    private TestInfo testInfo;

    @BeforeEach
    void setUp(TestInfo testInfo) throws Exception {
        this.testInfo = testInfo;
        // Create unique test directory - NEVER use @TempDir
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
        String testName = testInfo.getTestMethod().get().getName();
        testDataDir = "build/test-data/" + getClass().getSimpleName() + "/" + testName + "_" + timestamp;
        Files.createDirectories(Paths.get(testDataDir));
        
        // Generate mock SEC data
        generateMockSecData();
        
        // Create test model
        modelPath = createTestModel();
    }
    
    private void generateMockSecData() throws Exception {
        MockSecDataGenerator generator = new MockSecDataGenerator(new File(testDataDir));
        
        // Generate mock Parquet files only - no fallbacks
        List<String> ciks = List.of("0000320193", "0000789019", "0001018724");  // Apple, Microsoft, Amazon
        List<String> filingTypes = List.of("10-K", "10-Q");
        generator.generateMockData(ciks, filingTypes, 2022, 2023);
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

    @Test
    void testBasicSecQuery() throws Exception {
        Properties props = new Properties();
        props.setProperty("lex", "ORACLE");
        props.setProperty("unquotedCasing", "TO_LOWER");
        
        try (Connection conn = DriverManager.getConnection(
                "jdbc:calcite:model=" + modelPath, props)) {
            
            // Test schema discovery
            DatabaseMetaData dbMeta = conn.getMetaData();
            try (ResultSet schemas = dbMeta.getSchemas()) {
                boolean foundSec = false;
                while (schemas.next()) {
                    if ("SEC".equalsIgnoreCase(schemas.getString("TABLE_SCHEM"))) {
                        foundSec = true;
                        break;
                    }
                }
                assertTrue(foundSec, "Should find SEC schema");
            }
            
            // Test table discovery
            try (ResultSet tables = dbMeta.getTables(null, "SEC", null, null)) {
                assertTrue(tables.next(), "Should have at least one table in SEC schema");
            }
        }
    }

    @Test
    void testSecFilingsQuery() throws Exception {
        Properties props = new Properties();
        props.setProperty("lex", "ORACLE");
        props.setProperty("unquotedCasing", "TO_LOWER");
        
        try (Connection conn = DriverManager.getConnection(
                "jdbc:calcite:model=" + modelPath, props)) {
            
            String sql = "SELECT cik, filing_type, filing_date " +
                        "FROM sec.sec_filings " +
                        "WHERE cik = ? " +
                        "ORDER BY filing_date DESC " +
                        "LIMIT 10";
            
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                pstmt.setString(1, "0000320193");  // Apple's CIK
                
                try (ResultSet rs = pstmt.executeQuery()) {
                    // Validate metadata
                    ResultSetMetaData meta = rs.getMetaData();
                    assertEquals(3, meta.getColumnCount());
                    assertEquals("cik", meta.getColumnName(1).toLowerCase());
                    assertEquals("filing_type", meta.getColumnName(2).toLowerCase());
                    assertEquals("filing_date", meta.getColumnName(3).toLowerCase());
                    
                    // If we have results, validate them
                    if (rs.next()) {
                        assertEquals("0000320193", rs.getString("cik"));
                        assertNotNull(rs.getString("filing_type"));
                        assertNotNull(rs.getString("filing_date"));
                    }
                }
            }
        }
    }

    @Test
    void testInformationSchemaQueries() throws Exception {
        Properties props = new Properties();
        props.setProperty("lex", "ORACLE");
        props.setProperty("unquotedCasing", "TO_LOWER");
        
        try (Connection conn = DriverManager.getConnection(
                "jdbc:calcite:model=" + modelPath, props)) {
            
            // Test information_schema.tables
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(
                     "SELECT table_schema, table_name " +
                     "FROM information_schema.tables " +
                     "WHERE table_schema = 'SEC'")) {
                
                // Should have some tables
                if (rs.next()) {
                    assertEquals("SEC", rs.getString("table_schema"));
                    assertNotNull(rs.getString("table_name"));
                }
            }
            
            // Test information_schema.columns
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(
                     "SELECT column_name, data_type " +
                     "FROM information_schema.columns " +
                     "WHERE table_schema = 'SEC' " +
                     "LIMIT 10")) {
                
                // Should have some columns
                if (rs.next()) {
                    assertNotNull(rs.getString("column_name"));
                    assertNotNull(rs.getString("data_type"));
                }
            }
        }
    }

    private String createTestModel() throws Exception {
        String model = "{"
            + "\"version\":\"1.0\","
            + "\"defaultSchema\":\"SEC\","
            + "\"schemas\":[{"
            + "\"name\":\"SEC\","
            + "\"type\":\"custom\","
            + "\"factory\":\"org.apache.calcite.adapter.file.FileSchemaFactory\","
            + "\"operand\":{"
            + "\"directory\":\"" + testDataDir + "\","
            + "\"smart\":true"
            + "}"
            + "}]"
            + "}";
        
        Path modelFile = Paths.get(testDataDir, "model.json");
        Files.writeString(modelFile, model);
        return modelFile.toString();
    }
}