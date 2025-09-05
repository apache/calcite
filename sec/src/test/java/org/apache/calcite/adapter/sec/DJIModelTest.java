package org.apache.calcite.adapter.sec;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.*;
import java.util.Properties;
import java.io.File;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test DJI 5-year model.
 */
@Tag("integration")
public class DJIModelTest {
  @Test
  public void testDJI5YearModel() throws Exception {
    System.out.println("\n" + "=".repeat(80));
    System.out.println("TESTING DJI 5-YEAR MODEL");
    System.out.println("=".repeat(80) + "\n");
    
    // Register the Calcite driver
    Class.forName("org.apache.calcite.jdbc.Driver");
      
      // Point to the model file
      String modelPath = "/Users/kennethstott/calcite/sec/src/main/resources/dji-5year-model.json";
      
      Properties info = new Properties();
      info.put("model", modelPath);
      info.put("lex", "ORACLE");
      info.put("unquotedCasing", "TO_LOWER");
      
      System.out.println("Connecting to SEC adapter with DJI 5-year model...");
      System.out.println("This will download filings for all DJI companies (2020-2024)");
      System.out.println("Data directory: /Volumes/T9/sec-data/dji-5year\n");
      
      try (Connection conn = DriverManager.getConnection("jdbc:calcite:", info)) {
        System.out.println("✓ Connected successfully\n");
        
        // Get metadata to trigger download if needed
        DatabaseMetaData meta = conn.getMetaData();
        System.out.println("Database: " + meta.getDatabaseProductName());
        System.out.println("Schema: sec_dji\n");
        
        // Simple query to test the connection
        System.out.println("Running test query...");
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
               "SELECT COUNT(*) as total FROM financial_line_items")) {
          
          if (rs.next()) {
            int count = rs.getInt("total");
            System.out.println("Total financial line items: " + count);
            
            if (count > 0) {
              System.out.println("✓ Data loaded successfully!");
            } else {
              System.out.println("⚠ No data found - downloads may be in progress");
            }
          }
        }
        
        // Check if files were downloaded
        File dataDir = new File("/Volumes/T9/sec-data/dji-5year/sec");
        if (dataDir.exists()) {
          File[] xmlFiles = dataDir.listFiles((dir, name) -> name.endsWith(".xml"));
          if (xmlFiles != null && xmlFiles.length > 0) {
            System.out.println("\nDownloaded " + xmlFiles.length + " XBRL files");
            System.out.println("Sample files:");
            for (int i = 0; i < Math.min(3, xmlFiles.length); i++) {
              System.out.printf("  - %s (%,d bytes)\n", 
                xmlFiles[i].getName(), xmlFiles[i].length());
            }
          }
        } else {
          System.out.println("\nData directory not found: " + dataDir);
        }
        
      }
      
      System.out.println("\n" + "=".repeat(80));
      System.out.println("✓ TEST COMPLETE");
      System.out.println("=".repeat(80));
      
  }
}