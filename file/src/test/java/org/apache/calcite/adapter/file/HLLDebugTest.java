package org.apache.calcite.adapter.file;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.calcite.adapter.file.statistics.HLLSketchCache;

/**
 * Debug test to see why HLL isn't working.
 */
public class HLLDebugTest {
    @TempDir
    java.nio.file.Path tempDir;
    
    @Test
    public void debugWhyHLLNotWorking() throws Exception {
        // Enable HLL
        System.setProperty("calcite.file.statistics.hll.enabled", "true");
        System.setProperty("calcite.file.statistics.cache.directory", tempDir.toString());
        System.setProperty("calcite.file.statistics.hll.threshold", "1"); // Always generate HLL
        
        try (Connection connection = DriverManager.getConnection("jdbc:calcite:");
             CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class)) {
            
            SchemaPlus rootSchema = calciteConnection.getRootSchema();
            
            // Create test data
            java.io.File testFile = new java.io.File(tempDir.toFile(), "users.csv");
            try (java.io.PrintWriter writer = new java.io.PrintWriter(testFile)) {
                writer.println("id:int,name:string,age:int");
                for (int i = 1; i <= 100; i++) {
                    writer.println(i + ",User" + i + "," + (20 + i % 50));
                }
            }
            
            // Create a simple test schema
            Map<String, Object> operand = new LinkedHashMap<>();
            operand.put("directory", tempDir.toString());
            operand.put("executionEngine", "parquet");
            
            rootSchema.add("TEST", 
                FileSchemaFactory.INSTANCE.create(rootSchema, "TEST", operand));
            
            // Check the plan for COUNT(DISTINCT)
            String query = "SELECT COUNT(DISTINCT \"age\") FROM TEST.\"users\"";
            String explainQuery = "EXPLAIN PLAN FOR " + query;
            
            System.out.println("\n=== Query Plan for COUNT(DISTINCT) ===");
            try (PreparedStatement explainStmt = connection.prepareStatement(explainQuery);
                 ResultSet rs = explainStmt.executeQuery()) {
                while (rs.next()) {
                    String plan = rs.getString(1);
                    System.out.println(plan);
                    
                    // Check what we're seeing in the plan
                    if (plan.contains("EnumerableValues")) {
                        System.out.println("  ✓ VALUES node found - HLL optimization applied!");
                    }
                    if (plan.contains("EnumerableTableScan")) {
                        System.out.println("  ✗ TableScan found - HLL optimization NOT applied!");
                    }
                    if (plan.contains("EnumerableAggregate")) {
                        System.out.println("  → Aggregate node found");
                    }
                    if (plan.contains("COUNT(DISTINCT")) {
                        System.out.println("  → COUNT(DISTINCT) still in plan");
                    }
                }
            }
            
            // Now check with APPROX_COUNT_DISTINCT
            String approxQuery = "SELECT APPROX_COUNT_DISTINCT(\"age\") FROM TEST.\"users\"";
            String explainApproxQuery = "EXPLAIN PLAN FOR " + approxQuery;
            
            System.out.println("\n=== Query Plan for APPROX_COUNT_DISTINCT ===");
            try (PreparedStatement explainStmt = connection.prepareStatement(explainApproxQuery);
                 ResultSet rs = explainStmt.executeQuery()) {
                while (rs.next()) {
                    String plan = rs.getString(1);
                    System.out.println(plan);
                    
                    if (plan.contains("EnumerableValues")) {
                        System.out.println("  ✓ VALUES node found - HLL optimization applied!");
                    }
                    if (plan.contains("EnumerableTableScan")) {
                        System.out.println("  ✗ TableScan found - HLL optimization NOT applied!");
                    }
                }
            }
            
            // Test execution time
            System.out.println("\n=== Execution Time Test ===");
            try (PreparedStatement pstmt = connection.prepareStatement(query)) {
                // Warm up
                for (int i = 0; i < 3; i++) {
                    try (ResultSet rs = pstmt.executeQuery()) {
                        assertTrue(rs.next());
                    }
                }
                
                // Measure
                long start = System.nanoTime();
                try (ResultSet rs = pstmt.executeQuery()) {
                    assertTrue(rs.next());
                    int count = rs.getInt(1);
                    System.out.println("Result: " + count);
                }
                long elapsed = System.nanoTime() - start;
                double ms = elapsed / 1_000_000.0;
                
                System.out.println("Execution time: " + ms + " ms");
                if (ms > 1.0) {
                    System.out.println("✗ HLL is NOT working (should be < 1ms)");
                } else {
                    System.out.println("✓ HLL IS working!");
                }
            }
        } finally {
            System.clearProperty("calcite.file.statistics.hll.enabled");
            System.clearProperty("calcite.file.statistics.cache.directory");
            System.clearProperty("calcite.file.statistics.hll.threshold");
            // Clear HLL cache to prevent test interference
            HLLSketchCache.getInstance().invalidateAll();
        }
    }
}