package org.apache.calcite.adapter.file;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test that Statement queries benefit from plan caching.
 */
public class StatementCacheTest {
    @TempDir
    java.nio.file.Path tempDir;
    
    @Test
    public void testStatementCachePerformance() throws Exception {
        // Enable the cache
        System.setProperty("calcite.statement.cache.enabled", "true");
        System.setProperty("calcite.statement.cache.size", "100");
        
        try (Connection connection = DriverManager.getConnection("jdbc:calcite:");
             CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class)) {
            
            SchemaPlus rootSchema = calciteConnection.getRootSchema();
            
            // Create a simple test schema
            Map<String, Object> operand = new LinkedHashMap<>();
            operand.put("directory", tempDir.toString());
            operand.put("executionEngine", "parquet");
            
            rootSchema.add("TEST", 
                FileSchemaFactory.INSTANCE.create(rootSchema, "TEST", operand));
            
            String query = "SELECT 1 + 1 AS sum_value";
            
            // First, measure PreparedStatement as baseline
            long preparedTime;
            try (PreparedStatement pstmt = connection.prepareStatement(query)) {
                // Warm up
                for (int i = 0; i < 10; i++) {
                    try (ResultSet rs = pstmt.executeQuery()) {
                        assertTrue(rs.next());
                        assertEquals(2, rs.getInt(1));
                    }
                }
                
                // Measure
                long start = System.nanoTime();
                for (int i = 0; i < 100; i++) {
                    try (ResultSet rs = pstmt.executeQuery()) {
                        assertTrue(rs.next());
                        assertEquals(2, rs.getInt(1));
                    }
                }
                preparedTime = (System.nanoTime() - start) / 100;
            }
            
            // Now measure regular Statement
            long statementTime;
            try (Statement stmt = connection.createStatement()) {
                // Warm up - first execution will cache the plan
                for (int i = 0; i < 10; i++) {
                    try (ResultSet rs = stmt.executeQuery(query)) {
                        assertTrue(rs.next());
                        assertEquals(2, rs.getInt(1));
                    }
                }
                
                // Measure - should use cached plan
                long start = System.nanoTime();
                for (int i = 0; i < 100; i++) {
                    try (ResultSet rs = stmt.executeQuery(query)) {
                        assertTrue(rs.next());
                        assertEquals(2, rs.getInt(1));
                    }
                }
                statementTime = (System.nanoTime() - start) / 100;
            }
            
            // Now test with cache disabled
            System.setProperty("calcite.statement.cache.enabled", "false");
            
            // Create new connection to pick up the setting
            try (Connection conn2 = DriverManager.getConnection("jdbc:calcite:");
                 Statement stmt2 = conn2.createStatement()) {
                
                // Warm up
                for (int i = 0; i < 10; i++) {
                    try (ResultSet rs = stmt2.executeQuery(query)) {
                        assertTrue(rs.next());
                        assertEquals(2, rs.getInt(1));
                    }
                }
                
                // Measure - should re-plan every time
                long start = System.nanoTime();
                for (int i = 0; i < 100; i++) {
                    try (ResultSet rs = stmt2.executeQuery(query)) {
                        assertTrue(rs.next());
                        assertEquals(2, rs.getInt(1));
                    }
                }
                long noCacheTime = (System.nanoTime() - start) / 100;
                
                System.out.println("\n=== Statement Cache Performance ===");
                System.out.println("PreparedStatement:      " + (preparedTime / 1000) + " μs");
                System.out.println("Statement (cached):     " + (statementTime / 1000) + " μs");
                System.out.println("Statement (no cache):   " + (noCacheTime / 1000) + " μs");
                System.out.println("\nCached Statement vs PreparedStatement: " + 
                    String.format("%.1fx", (double) statementTime / preparedTime));
                System.out.println("Cached vs Uncached Statement: " + 
                    String.format("%.1fx faster", (double) noCacheTime / statementTime));
                
                // Cached statements should be much faster than uncached
                assertTrue(statementTime < noCacheTime / 2, 
                    "Cached statements should be at least 2x faster than uncached");
                
                // Cached statements should be comparable to PreparedStatements
                assertTrue(statementTime < preparedTime * 3, 
                    "Cached statements should be within 3x of PreparedStatement performance");
            }
        } finally {
            System.clearProperty("calcite.statement.cache.enabled");
            System.clearProperty("calcite.statement.cache.size");
        }
    }
}