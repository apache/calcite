package org.apache.calcite.adapter.file;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.*;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

/**
 * Analyze what timestamp values actually represent
 */
@Tag("temp")
public class TimestampAnalysisTest {
  
  @Test
  public void analyzeTimestampValues() throws Exception {
    // Register FileJdbcDriver
    Class.forName("org.apache.calcite.adapter.file.FileJdbcDriver");
    DriverManager.registerDriver(new FileJdbcDriver());
    
    Properties info = new Properties();
    info.put("model", FileAdapterTests.jsonPath("BUG"));
    
    String sql = "select * from \"date\" where \"empno\" IN (140, 150)";
    
    try (Connection conn = DriverManager.getConnection("jdbc:file:", info);
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {
      
      while (rs.next()) {
        int empno = rs.getInt(1);
        Timestamp ts = rs.getTimestamp(4);
        long millis = ts.getTime();
        
        // Convert millis back to LocalDateTime to see what it represents
        Instant instant = Instant.ofEpochMilli(millis);
        LocalDateTime ldt = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
        
        System.out.println("EMPNO: " + empno);
        System.out.println("  Raw millis: " + millis);
        System.out.println("  Timestamp object: " + ts);
        System.out.println("  As LocalDateTime in system TZ: " + ldt.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        System.out.println("  Expected from CSV: " + (empno == 140 ? "2015-12-30 07:15:56" : "2015-12-30 13:31:21"));
        System.out.println();
      }
    }
  }
}