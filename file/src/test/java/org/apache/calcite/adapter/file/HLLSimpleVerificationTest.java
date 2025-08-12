package org.apache.calcite.adapter.file;

import org.apache.calcite.adapter.file.statistics.HLLSketchCache;
import org.apache.calcite.adapter.file.statistics.HyperLogLogSketch;
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
 * 
 * NOTE: This test is currently expected to fail because HLL optimization
 * is not fully integrated into query execution. The infrastructure exists
 * (HLLSketchCache, HyperLogLogSketch, HLLCountDistinctRule) but the rule
 * is not being applied during query planning to transform COUNT(DISTINCT)
 * into HLL sketch lookups.
 * 
 * When HLL is properly configured and working, COUNT(DISTINCT) should
 * complete in sub-millisecond time by using pre-computed HLL sketches
 * instead of scanning all data.
 */
public class HLLSimpleVerificationTest {
    @TempDir
    java.nio.file.Path tempDir;
    
    @Test
    public void verifyHLLOptimizationWorks() throws Exception {
        // CONFIG-DRIVEN APPROACH: Use configuration to auto-generate HLL sketches
        // No manual code to create sketches - let the system do it
        
        // Create a simple test CSV file
        File targetFile = new File(tempDir.toFile(), "users.csv");
        try (java.io.PrintWriter writer = new java.io.PrintWriter(targetFile)) {
            writer.println("id:int,name:string,age:int,department:string");
            for (int i = 1; i <= 1000; i++) {
                writer.println(i + ",User" + i + "," + (20 + i % 50) + ",Dept" + (i % 10));
            }
        }
        
        // CONFIG-DRIVEN: Set system properties to enable HLL auto-generation
        System.setProperty("calcite.file.statistics.hll.enabled", "true");
        System.setProperty("calcite.file.statistics.hll.threshold", "1"); // Very low threshold for test data
        System.setProperty("calcite.file.statistics.auto.generate", "true"); // Auto-generate statistics
        System.setProperty("calcite.file.statistics.cache.directory", tempDir.toString());
        
        try (Connection connection = DriverManager.getConnection("jdbc:calcite:");
             CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class)) {
            
            SchemaPlus rootSchema = calciteConnection.getRootSchema();
            Map<String, Object> operand = new LinkedHashMap<>();
            operand.put("directory", tempDir.toString());
            operand.put("executionEngine", "parquet");
            
            // CONFIG-DRIVEN: These settings should trigger auto-generation and priming
            operand.put("primeCache", true);  // Enable cache priming to load HLL sketches
            operand.put("autoGenerateStatistics", true); // Auto-generate statistics
            operand.put("hllThreshold", 1); // Low threshold for test data
            
            SchemaPlus testSchema = rootSchema.add("TEST", 
                FileSchemaFactory.INSTANCE.create(rootSchema, "TEST", operand));
            
            // Give cache priming time to complete (CONFIG-DRIVEN auto-generation)
            Thread.sleep(3000);  // Give enough time for async priming
            
            // Verify HLL sketches were auto-generated and loaded
            HLLSketchCache hllCache = HLLSketchCache.getInstance();
            HyperLogLogSketch ageSketch = hllCache.getSketch("TEST", "users", "age");
            if (ageSketch != null) {
                System.out.println("CONFIG-DRIVEN: HLL Sketch auto-loaded for users.age with estimate: " + ageSketch.getEstimate());
            } else {
                System.out.println("WARNING: HLL Sketch not found in cache for users.age");
            }
            
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
                assertTrue(ms < 1.0, "HLL optimization must provide sub-millisecond performance but was " + ms + " ms");
            }
        } finally {
            // Clean up system properties
            System.clearProperty("calcite.file.statistics.hll.enabled");
            System.clearProperty("calcite.file.statistics.hll.threshold");
            System.clearProperty("calcite.file.statistics.auto.generate");
            System.clearProperty("calcite.file.statistics.cache.directory");
            
            // CRITICAL: Clear HLL cache to prevent test interference
            HLLSketchCache.getInstance().invalidateAll();
        }
    }
}