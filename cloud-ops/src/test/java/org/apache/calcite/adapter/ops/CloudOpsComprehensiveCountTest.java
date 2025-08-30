/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.ops;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

/**
 * Comprehensive test to get counts for all CloudOps tables by provider.
 */
public class CloudOpsComprehensiveCountTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(CloudOpsComprehensiveCountTest.class);

  // All CloudOps tables
  private static final String[] TABLES = {
      "kubernetes_clusters",
      "storage_resources",
      "compute_instances",
      "network_resources",
      "iam_resources",
      "database_resources",
      "container_registries"
  };

  @Test
  public void testComprehensiveCounts() throws Exception {
    // Load test configuration
    CloudOpsConfig config = CloudOpsTestUtils.loadTestConfig();
    if (config == null) {
      LOGGER.warn("CloudOps test configuration not found. Skipping test.");
      return;
    }

    // Create schema
    String modelJson = CloudOpsTestUtils.createModelJson(config);
    Properties info = new Properties();
    info.setProperty("model", "inline:" + modelJson);
    info.setProperty("lex", "ORACLE");
    info.setProperty("unquotedCasing", "TO_LOWER");

    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", info)) {
      LOGGER.info("=" .repeat(80));
      LOGGER.info("CLOUDOPS COMPREHENSIVE TABLE COUNTS");
      LOGGER.info("=" .repeat(80));
      
      for (String table : TABLES) {
        LOGGER.info("\n" + "=".repeat(60));
        LOGGER.info("TABLE: {}", table.toUpperCase());
        LOGGER.info("=".repeat(60));
        
        try (Statement stmt = conn.createStatement()) {
          // Total count
          try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as total FROM " + table)) {
            if (rs.next()) {
              int total = rs.getInt("total");
              LOGGER.info("TOTAL RECORDS: {}", total);
              
              if (total > 0) {
                // Count by provider
                try (ResultSet providerRs = stmt.executeQuery(
                    "SELECT cloud_provider, COUNT(*) as cnt " +
                    "FROM " + table + " " +
                    "GROUP BY cloud_provider " +
                    "ORDER BY cloud_provider")) {
                  LOGGER.info("By Provider:");
                  while (providerRs.next()) {
                    LOGGER.info("  {:10s}: {:5d}", 
                        providerRs.getString("cloud_provider"), 
                        providerRs.getInt("cnt"));
                  }
                }
                
                // For specific tables, get additional breakdowns
                if (table.equals("storage_resources")) {
                  try (ResultSet typeRs = stmt.executeQuery(
                      "SELECT storage_type, COUNT(*) as cnt " +
                      "FROM " + table + " " +
                      "GROUP BY storage_type " +
                      "ORDER BY cnt DESC")) {
                    LOGGER.info("By Storage Type:");
                    while (typeRs.next()) {
                      String type = typeRs.getString("storage_type");
                      if (type != null) {
                        LOGGER.info("  {:30s}: {:5d}", type, typeRs.getInt("cnt"));
                      }
                    }
                  }
                } else if (table.equals("compute_instances")) {
                  try (ResultSet typeRs = stmt.executeQuery(
                      "SELECT instance_type, COUNT(*) as cnt " +
                      "FROM " + table + " " +
                      "GROUP BY instance_type " +
                      "ORDER BY cnt DESC " +
                      "LIMIT 5")) {
                    LOGGER.info("Top 5 Instance Types:");
                    while (typeRs.next()) {
                      String type = typeRs.getString("instance_type");
                      if (type != null) {
                        LOGGER.info("  {:30s}: {:5d}", type, typeRs.getInt("cnt"));
                      }
                    }
                  }
                } else if (table.equals("network_resources")) {
                  try (ResultSet typeRs = stmt.executeQuery(
                      "SELECT resource_type, COUNT(*) as cnt " +
                      "FROM " + table + " " +
                      "GROUP BY resource_type " +
                      "ORDER BY cnt DESC")) {
                    LOGGER.info("By Resource Type:");
                    while (typeRs.next()) {
                      String type = typeRs.getString("resource_type");
                      if (type != null) {
                        LOGGER.info("  {:30s}: {:5d}", type, typeRs.getInt("cnt"));
                      }
                    }
                  }
                } else if (table.equals("database_resources")) {
                  try (ResultSet typeRs = stmt.executeQuery(
                      "SELECT database_engine, COUNT(*) as cnt " +
                      "FROM " + table + " " +
                      "GROUP BY database_engine " +
                      "ORDER BY cnt DESC")) {
                    LOGGER.info("By Database Engine:");
                    while (typeRs.next()) {
                      String engine = typeRs.getString("database_engine");
                      if (engine != null) {
                        LOGGER.info("  {:30s}: {:5d}", engine, typeRs.getInt("cnt"));
                      }
                    }
                  }
                }
                
                // Sample records
                String sampleQuery = "SELECT * FROM " + table + " LIMIT 2";
                try (ResultSet sampleRs = stmt.executeQuery(sampleQuery)) {
                  int sampleCount = 0;
                  LOGGER.info("Sample Records:");
                  while (sampleRs.next()) {
                    sampleCount++;
                    if (table.equals("kubernetes_clusters")) {
                      LOGGER.info("  - {}/{} in {} ({} nodes)", 
                          sampleRs.getString("cloud_provider"),
                          sampleRs.getString("cluster_name"),
                          sampleRs.getString("region"),
                          sampleRs.getObject("node_count"));
                    } else if (table.equals("storage_resources")) {
                      LOGGER.info("  - {}/{} ({}) in {}", 
                          sampleRs.getString("cloud_provider"),
                          sampleRs.getString("resource_name"),
                          sampleRs.getString("storage_type"),
                          sampleRs.getString("region"));
                    } else if (table.equals("compute_instances")) {
                      LOGGER.info("  - {}/{} ({}) in {}", 
                          sampleRs.getString("cloud_provider"),
                          sampleRs.getString("instance_name"),
                          sampleRs.getString("instance_type"),
                          sampleRs.getString("region"));
                    } else if (table.equals("network_resources")) {
                      LOGGER.info("  - {}/{} ({}) in {}", 
                          sampleRs.getString("cloud_provider"),
                          sampleRs.getString("resource_name"),
                          sampleRs.getString("resource_type"),
                          sampleRs.getString("region"));
                    } else if (table.equals("iam_resources")) {
                      LOGGER.info("  - {}/{} ({})", 
                          sampleRs.getString("cloud_provider"),
                          sampleRs.getString("resource_name"),
                          sampleRs.getString("resource_type"));
                    } else if (table.equals("database_resources")) {
                      LOGGER.info("  - {}/{} ({}) in {}", 
                          sampleRs.getString("cloud_provider"),
                          sampleRs.getString("database_name"),
                          sampleRs.getString("database_engine"),
                          sampleRs.getString("region"));
                    } else if (table.equals("container_registries")) {
                      LOGGER.info("  - {}/{} in {}", 
                          sampleRs.getString("cloud_provider"),
                          sampleRs.getString("registry_name"),
                          sampleRs.getString("region"));
                    }
                  }
                }
              }
            }
          }
        } catch (Exception e) {
          LOGGER.error("Failed to query {}: {}", table, e.getMessage());
        }
      }
      
      LOGGER.info("\n" + "=".repeat(80));
      LOGGER.info("SUMMARY");
      LOGGER.info("=".repeat(80));
      
      // Overall summary
      int grandTotal = 0;
      for (String table : TABLES) {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as cnt FROM " + table)) {
          if (rs.next()) {
            int count = rs.getInt("cnt");
            grandTotal += count;
            LOGGER.info("{:30s}: {:5d}", table, count);
          }
        } catch (Exception e) {
          LOGGER.info("{:30s}: ERROR - {}", table, e.getMessage());
        }
      }
      LOGGER.info("{:30s}: {:5d}", "GRAND TOTAL", grandTotal);
    }
  }
}