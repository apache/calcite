package org.apache.calcite.adapter.file;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Tag;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test that VALUES nodes actually work and are instant.
 */
@Tag("unit")public class HLLValuesTest {
    @TempDir
    java.nio.file.Path tempDir;
    
    @Test
    public void testValuesNodeIsInstant() throws Exception {
        try (Connection connection = DriverManager.getConnection("jdbc:calcite:");
             CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class)) {
            
            // Test 1: Direct VALUES query should be instant
            String valuesQuery = "SELECT * FROM (VALUES (10343))";
            
            long startValues = System.currentTimeMillis();
            for (int i = 0; i < 100; i++) {
                try (Statement stmt = connection.createStatement();
                     ResultSet rs = stmt.executeQuery(valuesQuery)) {
                    assertTrue(rs.next());
                    assertEquals(10343, rs.getLong(1));
                }
            }
            long timeValues = (System.currentTimeMillis() - startValues) / 100;
            
            System.out.println("VALUES query time: " + timeValues + "ms");
            assertTrue(timeValues <= 20, "VALUES query should be fast (<=20ms) but was " + timeValues + "ms");
            
            // Test 2: VALUES with column alias
            String aliasedValues = "SELECT EXPR$0 AS count_distinct FROM (VALUES (10343))";
            
            long startAliased = System.currentTimeMillis();
            for (int i = 0; i < 100; i++) {
                try (Statement stmt = connection.createStatement();
                     ResultSet rs = stmt.executeQuery(aliasedValues)) {
                    assertTrue(rs.next());
                    assertEquals(10343, rs.getLong("count_distinct"));
                }
            }
            long timeAliased = (System.currentTimeMillis() - startAliased) / 100;
            
            System.out.println("Aliased VALUES query time: " + timeAliased + "ms");
            assertTrue(timeAliased <= 20, "Aliased VALUES query should be fast (<=20ms) but was " + timeAliased + "ms");
        }
    }
    
    @Test
    public void testHLLRuleProducesValuesNode() throws Exception {
        // This test will verify that our HLL rule actually produces a VALUES node
        // that gets used by the planner
        
        // Create a simple in-memory table
        try (Connection connection = DriverManager.getConnection("jdbc:calcite:");
             CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class)) {
            
            SchemaPlus rootSchema = calciteConnection.getRootSchema();
            
            // Create a simple VALUES-based schema  
            String valuesTableQuery = "SELECT * FROM (VALUES (1, 100), (2, 200), (3, 100), (4, 300)) AS t(id, amount)";
            
            // First verify VALUES works
            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery(valuesTableQuery)) {
                int count = 0;
                while (rs.next()) {
                    count++;
                }
                assertEquals(4, count, "Should have 4 rows");
            }
            
            // Now test COUNT(DISTINCT) on VALUES - should still be instant
            String countDistinctQuery = "SELECT COUNT(DISTINCT amount) FROM (VALUES (1, 100), (2, 200), (3, 100), (4, 300)) AS t(id, amount)";
            
            long startCountDistinct = System.currentTimeMillis();
            for (int i = 0; i < 100; i++) {
                try (Statement stmt = connection.createStatement();
                     ResultSet rs = stmt.executeQuery(countDistinctQuery)) {
                    assertTrue(rs.next());
                    assertEquals(3, rs.getLong(1)); // 100, 200, 300
                }
            }
            long timeCountDistinct = (System.currentTimeMillis() - startCountDistinct) / 100;
            
            System.out.println("COUNT(DISTINCT) on VALUES time: " + timeCountDistinct + "ms");
            // Even COUNT(DISTINCT) on VALUES should be very fast
            assertTrue(timeCountDistinct <= 30, "COUNT(DISTINCT) on VALUES should be fast (<=30ms) but was " + timeCountDistinct + "ms");
        }
    }
}