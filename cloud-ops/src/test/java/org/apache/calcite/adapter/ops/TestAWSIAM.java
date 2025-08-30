package org.apache.calcite.adapter.ops;

import org.apache.calcite.adapter.ops.provider.AWSProvider;
import org.junit.jupiter.api.Test;
import java.sql.*;
import java.util.*;

public class TestAWSIAM {
  @Test
  public void testIAM() throws Exception {
    CloudOpsConfig config = CloudOpsTestUtils.loadTestConfig();
    if (config == null || config.aws == null) {
      System.out.println("No AWS config");
      return;
    }
    
    System.out.println("AWS Account IDs: " + config.aws.accountIds);
    System.out.println("AWS Region: " + config.aws.region);
    
    AWSProvider provider = new AWSProvider(config.aws);
    List<Map<String, Object>> results = provider.queryIAMResources(config.aws.accountIds);
    
    System.out.println("\nAWS IAM Resources from Provider: " + results.size());
    for (Map<String, Object> iam : results) {
      System.out.println("  - " + iam.get("IAMResourceType") + ": " + iam.get("IAMResource"));
    }
    
    // Now test via SQL
    System.out.println("\nQuerying via SQL:");
    String modelJson = CloudOpsTestUtils.createModelJson(config);
    Properties info = new Properties();
    info.setProperty("model", "inline:" + modelJson);
    info.setProperty("lex", "ORACLE");
    info.setProperty("unquotedCasing", "TO_LOWER");
    
    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", info)) {
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM iam_resources WHERE cloud_provider = 'aws'")) {
        if (rs.next()) {
          System.out.println("AWS IAM count via SQL: " + rs.getInt(1));
        }
      }
      
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery("SELECT cloud_provider, COUNT(*) FROM iam_resources GROUP BY cloud_provider")) {
        System.out.println("IAM by provider:");
        while (rs.next()) {
          System.out.println("  " + rs.getString(1) + ": " + rs.getInt(2));
        }
      }
    }
  }
}