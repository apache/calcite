package org.apache.calcite.adapter.file;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("integration")
public class CompareTimestampsTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(CompareTimestampsTest.class);
    
    @Test
    public void testCompareTimestamps() throws Exception {
        // Register FileJdbcDriver
        Class.forName("org.apache.calcite.adapter.file.FileJdbcDriver");
        DriverManager.registerDriver(new FileJdbcDriver());
        
        Properties info = new Properties();
        info.put("model", FileAdapterTests.jsonPath("BUG"));
        
        String sql = "select * from \"date\" where \"empno\" = 140";
        
        // Test with jdbc:calcite:
        LOGGER.debug("Testing with jdbc:calcite:");
        long calciteMillis = 0;
        try (Connection conn = DriverManager.getConnection("jdbc:calcite:", info);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                Timestamp ts = rs.getTimestamp(4);
                calciteMillis = ts.getTime();
                LOGGER.debug("  Timestamp: {}", ts);
                LOGGER.debug("  Millis: {}", calciteMillis);
            }
        }
        
        // Test with jdbc:file:
        LOGGER.debug("Testing with jdbc:file:");
        long fileMillis = 0;
        try (Connection conn = DriverManager.getConnection("jdbc:file:", info);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                Timestamp ts = rs.getTimestamp(4);
                fileMillis = ts.getTime();
                LOGGER.debug("  Timestamp: {}", ts);
                LOGGER.debug("  Millis: {}", fileMillis);
            }
        }
        
        LOGGER.debug("Difference: {} ms", fileMillis - calciteMillis);
        
        // Both drivers should return the same timestamp value
        assertEquals(calciteMillis, fileMillis, 
            "jdbc:file: and jdbc:calcite: should return the same timestamp value");
    }
}
