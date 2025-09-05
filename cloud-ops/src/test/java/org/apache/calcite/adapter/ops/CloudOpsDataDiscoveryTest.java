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
 * Simple test to discover what data is available in CloudOps tables.
 */
public class CloudOpsDataDiscoveryTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(CloudOpsDataDiscoveryTest.class);

  @Test public void testDataDiscovery() throws Exception {
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
      LOGGER.info("Connected to CloudOps schema");

      // Query kubernetes_clusters
      LOGGER.info("\n=== KUBERNETES CLUSTERS ===");
      try (Statement stmt = conn.createStatement()) {
        // Total count
        try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as total FROM kubernetes_clusters")) {
          if (rs.next()) {
            LOGGER.info("Total clusters: {}", rs.getInt("total"));
          }
        }

        // By provider
        try (ResultSet rs =
            stmt.executeQuery("SELECT cloud_provider, COUNT(*) as cnt " +
            "FROM kubernetes_clusters " +
            "GROUP BY cloud_provider " +
            "ORDER BY cloud_provider")) {
          LOGGER.info("Clusters by provider:");
          while (rs.next()) {
            LOGGER.info("  {}: {}", rs.getString("cloud_provider"), rs.getInt("cnt"));
          }
        }

        // Sample data
        try (ResultSet rs =
            stmt.executeQuery("SELECT cloud_provider, cluster_name, region, node_count " +
            "FROM kubernetes_clusters " +
            "LIMIT 5")) {
          LOGGER.info("Sample clusters:");
          while (rs.next()) {
            LOGGER.info("  {}/{} in {} with {} nodes",
                rs.getString("cloud_provider"),
                rs.getString("cluster_name"),
                rs.getString("region"),
                rs.getInt("node_count"));
          }
        }
      } catch (Exception e) {
        LOGGER.error("Failed to query kubernetes_clusters: {}", e.getMessage());
      }

      // Query storage_resources
      LOGGER.info("\n=== STORAGE RESOURCES ===");
      try (Statement stmt = conn.createStatement()) {
        // Total count
        try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as total FROM storage_resources")) {
          if (rs.next()) {
            LOGGER.info("Total storage resources: {}", rs.getInt("total"));
          }
        }

        // By provider
        try (ResultSet rs =
            stmt.executeQuery("SELECT cloud_provider, COUNT(*) as cnt " +
            "FROM storage_resources " +
            "GROUP BY cloud_provider " +
            "ORDER BY cloud_provider")) {
          LOGGER.info("Storage resources by provider:");
          while (rs.next()) {
            LOGGER.info("  {}: {}", rs.getString("cloud_provider"), rs.getInt("cnt"));
          }
        }

        // By type
        try (ResultSet rs =
            stmt.executeQuery("SELECT storage_type, COUNT(*) as cnt " +
            "FROM storage_resources " +
            "GROUP BY storage_type " +
            "ORDER BY storage_type")) {
          LOGGER.info("Storage resources by type:");
          while (rs.next()) {
            LOGGER.info("  {}: {}", rs.getString("storage_type"), rs.getInt("cnt"));
          }
        }

        // Sample data
        try (ResultSet rs =
            stmt.executeQuery("SELECT cloud_provider, resource_name, storage_type, region " +
            "FROM storage_resources " +
            "LIMIT 5")) {
          LOGGER.info("Sample storage resources:");
          while (rs.next()) {
            LOGGER.info("  {}/{} ({}) in {}",
                rs.getString("cloud_provider"),
                rs.getString("resource_name"),
                rs.getString("storage_type"),
                rs.getString("region"));
          }
        }
      } catch (Exception e) {
        LOGGER.error("Failed to query storage_resources: {}", e.getMessage());
      }
    }
  }
}
