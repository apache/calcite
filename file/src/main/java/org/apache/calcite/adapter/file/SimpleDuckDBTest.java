package org.apache.calcite.adapter.file;

import java.sql.*;
import java.nio.file.*;
import java.util.*;
import java.io.*;

/**
 * Simple test to verify DuckDB query pushdown is working.
 * This test creates Parquet files and runs queries with both PARQUET and DUCKDB engines.
 */
public class SimpleDuckDBTest {
    public static void main(String[] args) throws Exception {
        System.out.println("Testing DuckDB Query Pushdown Integration");
        System.out.println("========================================\n");
        
        // Create test directory
        Path tempDir = Files.createTempDirectory("duckdb_pushdown_test_");
        System.out.println("Test directory: " + tempDir);
        
        try {
            createTestData(tempDir);
            
            System.out.println("\n--- Testing with PARQUET engine (baseline) ---");
            testWithEngine(tempDir, "PARQUET");
            
            // Create a new temp directory for DUCKDB test to avoid any caching issues
            Path duckdbTempDir = Files.createTempDirectory("duckdb_test_");
            createTestData(duckdbTempDir);
            
            System.out.println("\n--- Testing with DUCKDB engine (should show pushdown messages) ---");
            testWithEngine(duckdbTempDir, "DUCKDB");
            
            // Cleanup second directory
            cleanup(duckdbTempDir);
            
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            cleanup(tempDir);
        }
    }
    
    private static void createTestData(Path tempDir) throws Exception {
        System.out.println("\nCreating test Parquet files...");
        
        // Load DuckDB driver
        Class.forName("org.duckdb.DuckDBDriver");
        
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
            // Create test data
            try (PreparedStatement stmt = conn.prepareStatement(
                "CREATE TABLE test_data AS " +
                "SELECT i as id, 'Test_' || i as name, i * 100 as amount " +
                "FROM generate_series(1, 50) as t(i)")) {
                stmt.execute();
            }
            
            // Export to Parquet
            String parquetPath = tempDir.resolve("test_data.parquet").toString();
            try (PreparedStatement stmt = conn.prepareStatement(
                "COPY test_data TO '" + parquetPath + "' (FORMAT PARQUET)")) {
                stmt.execute();
            }
            
            System.out.println("✓ Created test_data.parquet (50 rows)");
        }
    }
    
    private static void testWithEngine(Path tempDir, String engine) throws Exception {
        // Create model configuration
        // Use different schema names to avoid cache conflicts
        String schemaName = "testdb_" + engine.toLowerCase();
        String modelJson = String.format(
            "{\n" +
            "  \"version\": \"1.0\",\n" +
            "  \"defaultSchema\": \"%s\",\n" +
            "  \"schemas\": [{\n" +
            "    \"name\": \"%s\",\n" +
            "    \"type\": \"custom\",\n" +
            "    \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\",\n" +
            "    \"operand\": {\n" +
            "      \"directory\": \"%s\",\n" +
            "      \"executionEngine\": \"%s\"\n" +
            "    }\n" +
            "  }]\n" +
            "}", schemaName, schemaName, tempDir.toString(), engine);
        
        Path modelPath = tempDir.resolve("model_" + engine.toLowerCase() + ".json");
        Files.write(modelPath, modelJson.getBytes());
        
        // Setup connection properties
        Properties props = new Properties();
        props.put("model", modelPath.toString());
        props.put("lex", "ORACLE");
        props.put("unquotedCasing", "TO_LOWER");
        
        System.out.println("Engine: " + engine);
        System.out.println("Looking for DuckDB pushdown messages...\n");
        
        // Load Calcite driver
        Class.forName("org.apache.calcite.jdbc.Driver");
        
        try (Connection conn = DriverManager.getConnection("jdbc:calcite:", props)) {
            // Test simple query - use fully qualified table name
            String qualifiedTableName = schemaName + ".test_data";
            System.out.println("Executing: SELECT COUNT(*) FROM " + qualifiedTableName);
            try (PreparedStatement stmt = conn.prepareStatement("SELECT COUNT(*) as cnt FROM " + qualifiedTableName);
                 ResultSet rs = stmt.executeQuery()) {
                
                if (rs.next()) {
                    System.out.println("Result: " + rs.getInt("cnt") + " rows");
                }
            }
            
            System.out.println();
            
            // Test aggregation query with different column to avoid the 'value' reserved word issue
            System.out.println("Executing: SELECT AVG(amount) FROM " + qualifiedTableName);
            try (PreparedStatement stmt = conn.prepareStatement("SELECT AVG(amount) as avg_val FROM " + qualifiedTableName);
                 ResultSet rs = stmt.executeQuery()) {
                
                if (rs.next()) {
                    System.out.println("Result: Average amount = " + rs.getDouble("avg_val"));
                }
            }
            
        } catch (Exception e) {
            System.out.println("ERROR: " + e.getMessage());
            e.printStackTrace();
        }
        
        System.out.println();
    }
    
    private static void cleanup(Path tempDir) {
        try {
            Files.walk(tempDir)
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);
            System.out.println("✓ Cleanup completed");
        } catch (Exception e) {
            System.out.println("Cleanup warning: " + e.getMessage());
        }
    }
}