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

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test case sensitivity behavior for metadata schemas.
 */
@Tag("unit")
public class CaseSensitivityTest {

  @Test public void testInformationSchemaWithQuotedIdentifiers() throws SQLException {
    try (Connection connection = createTestConnection()) {
      Statement statement = connection.createStatement();
      ResultSet resultSet =
          statement.executeQuery("SELECT \"TABLE_NAME\" FROM information_schema.\"TABLES\" WHERE \"TABLE_SCHEMA\" = 'public'");

      int count = 0;
      while (resultSet.next()) {
        count++;
        System.out.println("Found table: " + resultSet.getString("TABLE_NAME"));
      }

      assertTrue(count > 0, "Should find tables with quoted identifiers");
    }
  }

  @Test public void testPgCatalogWithUnquotedIdentifiers() throws SQLException {
    try (Connection connection = createTestConnection()) {
      Statement statement = connection.createStatement();
      ResultSet resultSet =
          statement.executeQuery("SELECT tablename FROM pg_catalog.\"PG_TABLES\" WHERE schemaname = 'public'");

      int count = 0;
      while (resultSet.next()) {
        count++;
        System.out.println("Found table: " + resultSet.getString("tablename"));
      }

      assertTrue(count > 0, "Should find tables with unquoted identifiers");
    }
  }

  private Connection createTestConnection() throws SQLException {
    CloudOpsConfig config = createTestConfig();

    Properties info = new Properties();
    info.setProperty("lex", "MYSQL_ANSI"); // Case-insensitive with double quote support

    Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
    CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();

    CloudOpsSchemaFactory factory = new CloudOpsSchemaFactory();
    rootSchema.add(
        "public", factory.create(rootSchema, "public",
        configToOperands(config)));

    return connection;
  }

  private CloudOpsConfig createTestConfig() {
    // Only use real credentials from local properties file
    CloudOpsConfig config = CloudOpsTestUtils.loadTestConfig();
    if (config == null) {
      throw new IllegalStateException("Real credentials required from local-test.properties file");
    }
    return config;
  }

  private java.util.Map<String, Object> configToOperands(CloudOpsConfig config) {
    java.util.Map<String, Object> operands = new java.util.HashMap<>();

    if (config.azure != null) {
      operands.put("azure.tenantId", config.azure.tenantId);
      operands.put("azure.clientId", config.azure.clientId);
      operands.put("azure.clientSecret", config.azure.clientSecret);
      operands.put("azure.subscriptionIds", String.join(",", config.azure.subscriptionIds));
    }

    if (config.gcp != null) {
      operands.put("gcp.credentialsPath", config.gcp.credentialsPath);
      operands.put("gcp.projectIds", String.join(",", config.gcp.projectIds));
    }

    if (config.aws != null) {
      operands.put("aws.accessKeyId", config.aws.accessKeyId);
      operands.put("aws.secretAccessKey", config.aws.secretAccessKey);
      operands.put("aws.region", config.aws.region);
      operands.put("aws.accountIds", String.join(",", config.aws.accountIds));
      if (config.aws.roleArn != null) {
        operands.put("aws.roleArn", config.aws.roleArn);
      }
    }

    return operands;
  }
}
