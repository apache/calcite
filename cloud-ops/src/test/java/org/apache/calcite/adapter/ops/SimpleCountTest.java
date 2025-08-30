package org.apache.calcite.adapter.ops;

import org.junit.jupiter.api.Test;
import java.sql.*;
import java.util.Properties;

public class SimpleCountTest {
  public static void main(String[] args) throws Exception {
    new SimpleCountTest().getCounts();
  }
  
  @Test
  public void getCounts() throws Exception {
    CloudOpsConfig config = CloudOpsTestUtils.loadTestConfig();
    String modelJson = CloudOpsTestUtils.createModelJson(config);
    Properties info = new Properties();
    info.setProperty("model", "inline:" + modelJson);
    info.setProperty("lex", "ORACLE");
    info.setProperty("unquotedCasing", "TO_LOWER");

    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", info)) {
      String[] tables = {
        "kubernetes_clusters",
        "storage_resources", 
        "compute_instances",
        "network_resources",
        "iam_resources",
        "database_resources",
        "container_registries"
      };
      
      for (String table : tables) {
        System.out.println("\n" + table.toUpperCase() + ":");
        System.out.println("-".repeat(40));
        
        // Total count
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + table)) {
          if (rs.next()) {
            System.out.println("TOTAL: " + rs.getInt(1));
          }
        }
        
        // Count by provider
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
               "SELECT cloud_provider, COUNT(*) FROM " + table + 
               " GROUP BY cloud_provider ORDER BY cloud_provider")) {
          while (rs.next()) {
            System.out.println("  " + rs.getString(1) + ": " + rs.getInt(2));
          }
        } catch (Exception e) {
          System.out.println("  Error: " + e.getMessage());
        }
      }
    }
  }
}