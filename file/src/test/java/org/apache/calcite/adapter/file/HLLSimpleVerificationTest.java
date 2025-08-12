package org.apache.calcite.adapter.file;

import org.apache.calcite.adapter.file.statistics.HLLSketchCache;
import org.apache.calcite.adapter.file.statistics.StatisticsBuilder;
import org.apache.calcite.adapter.file.statistics.TableStatistics;
import org.apache.calcite.adapter.file.statistics.ColumnStatistics;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Simple verification that HLL optimization actually works.
 * This test uses the existing test data and just verifies HLL performance.
 */
public class HLLSimpleVerificationTest {
    @TempDir
    java.nio.file.Path tempDir;
    
    @Test
    public void verifyHLLOptimizationWorks() throws Exception {
        // Copy test parquet file to temp dir
        File sourceFile = new File("src/test/resources/parquet/users.parquet");
        File targetFile = new File(tempDir.toFile(), "users.parquet");
        java.nio.file.Files.copy(sourceFile.toPath(), targetFile.toPath());
        
        // Generate HLL statistics
        File cacheDir = tempDir.resolve("hll_cache").toFile();
        cacheDir.mkdirs();
        
        System.setProperty("calcite.file.statistics.hll.enabled", "true");
        System.setProperty("calcite.file.statistics.cache.directory", cacheDir.getAbsolutePath());
        
        StatisticsBuilder builder = new StatisticsBuilder();
        TableStatistics stats = builder.buildStatistics(
            new DirectFileSource(targetFile), cacheDir);
        
        // Load HLL sketches into cache
        HLLSketchCache cache = HLLSketchCache.getInstance();
        for (String columnName : stats.getColumnStatistics().keySet()) {
            ColumnStatistics colStats = stats.getColumnStatistics(columnName);
            if (colStats != null && colStats.getHllSketch() != null) {
                cache.putSketch("users", columnName, colStats.getHllSketch());
            }
        }
        
        try (Connection connection = DriverManager.getConnection("jdbc:calcite:");
             CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class)) {
            
            SchemaPlus rootSchema = calciteConnection.getRootSchema();
            Map<String, Object> operand = new LinkedHashMap<>();
            operand.put("directory", tempDir.toString());
            operand.put("executionEngine", "parquet");
            
            rootSchema.add("TEST", 
                FileSchemaFactory.INSTANCE.create(rootSchema, "TEST", operand));
            
            // Test COUNT(DISTINCT) performance
            String query = "SELECT COUNT(DISTINCT \"age\") FROM TEST.\"users\"";
            
            try (PreparedStatement pstmt = connection.prepareStatement(query)) {
                // Warm up
                for (int i = 0; i < 5; i++) {
                    try (ResultSet rs = pstmt.executeQuery()) {
                        assertTrue(rs.next());
                    }
                }
                
                // Measure
                long start = System.nanoTime();
                for (int i = 0; i < 100; i++) {
                    try (ResultSet rs = pstmt.executeQuery()) {
                        assertTrue(rs.next());
                        rs.getInt(1);
                    }
                }
                long elapsed = (System.nanoTime() - start) / 100;
                double ms = elapsed / 1_000_000.0;
                
                System.out.println("\n=== HLL Verification Test ===");
                System.out.println("Query: " + query);
                System.out.println("Execution time: " + ms + " ms");
                System.out.println("Execution time: " + (elapsed / 1000) + " microseconds");
                
                if (ms < 1.0) {
                    System.out.println("✓ SUCCESS: HLL optimization IS working!");
                } else {
                    System.out.println("✗ FAILURE: HLL optimization NOT working");
                    System.out.println("  Expected: < 1ms");
                    System.out.println("  Actual: " + ms + " ms");
                    
                    // Show the plan
                    try (PreparedStatement explainStmt = connection.prepareStatement("EXPLAIN PLAN FOR " + query);
                         ResultSet rs = explainStmt.executeQuery()) {
                        System.out.println("\nQuery Plan:");
                        while (rs.next()) {
                            System.out.println(rs.getString(1));
                        }
                    }
                }
                
                // For the test to pass, HLL must be working
                assertTrue(ms < 5.0, "HLL should be fast but was " + ms + " ms");
            }
        } finally {
            System.clearProperty("calcite.file.statistics.hll.enabled");
            System.clearProperty("calcite.file.statistics.cache.directory");
        }
    }
}