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
package org.apache.calcite.adapter.sharepoint;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for CAST operations with SharePoint adapter schema.
 * 
 * These tests verify that CAST operations are properly handled by the Calcite
 * query planner when using SharePoint schema metadata, without requiring
 * actual SharePoint connectivity.
 */
public class SharePointCastProjectionTest {

  private Connection connection;

  @BeforeEach
  public void setUp() throws SQLException {
    Properties info = new Properties();
    info.setProperty("lex", "JAVA");

    connection = DriverManager.getConnection("jdbc:calcite:", info);
    CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();

    // Create a mock SharePoint schema for testing CAST operations
    // This doesn't require actual SharePoint connectivity
    rootSchema.add("sharepoint", new MockSharePointSchema());
  }

  @Test
  public void testCastOperationsParsing() throws SQLException {
    // Test that various CAST operations can be parsed and planned
    // without actually executing against SharePoint

    String[] castQueries = {
        "SELECT CAST(title AS VARCHAR(100)) FROM sharepoint.tasks",
        "SELECT CAST(priority AS INTEGER) FROM sharepoint.tasks",
        "SELECT CAST(created_date AS TIMESTAMP) FROM sharepoint.tasks",
        "SELECT CAST(is_complete AS BOOLEAN) FROM sharepoint.tasks",
        "SELECT CAST(budget AS DOUBLE) FROM sharepoint.tasks",
        "SELECT CAST(description AS VARCHAR) FROM sharepoint.tasks WHERE CAST(priority AS INTEGER) > 1",
        "SELECT title, CAST(priority AS VARCHAR) || '-' || title AS priority_title FROM sharepoint.tasks",
        "SELECT COUNT(*) FROM sharepoint.tasks WHERE is_complete = true"
    };

    Statement stmt = connection.createStatement();

    for (String query : castQueries) {
      // Test that the query can be prepared (parsed and planned) without error
      assertDoesNotThrow(() -> {
        PreparedStatement pstmt = connection.prepareStatement(query);
        assertNotNull(pstmt, "Query should be preparable: " + query);
        pstmt.close();
      }, "Query should parse and plan successfully: " + query);
    }
  }

  @Test
  public void testComplexCastExpressions() throws SQLException {
    // Test complex CAST expressions in various SQL clauses
    String[] complexCastQueries = {
        "SELECT CASE WHEN CAST(priority AS INTEGER) > 5 THEN 'HIGH' ELSE 'LOW' END AS priority_level FROM sharepoint.tasks",
        "SELECT title FROM sharepoint.tasks ORDER BY CAST(created_date AS TIMESTAMP) DESC",
        "SELECT AVG(CAST(priority AS DOUBLE)) AS avg_priority FROM sharepoint.tasks",
        "SELECT title, CAST(priority AS INTEGER) + 1 AS next_priority FROM sharepoint.tasks",
        "SELECT COALESCE(CAST(description AS VARCHAR), 'No description') AS description FROM sharepoint.tasks"
    };

    Statement stmt = connection.createStatement();

    for (String query : complexCastQueries) {
      assertDoesNotThrow(() -> {
        PreparedStatement pstmt = connection.prepareStatement(query);
        assertNotNull(pstmt, "Complex CAST query should be preparable: " + query);
        pstmt.close();
      }, "Complex CAST query should parse successfully: " + query);
    }
  }

  @Test
  public void testCastWithJoins() throws SQLException {
    // Test CAST operations in JOIN conditions and multi-table queries
    String joinQuery = "SELECT t1.title, CAST(t1.priority AS VARCHAR) AS priority_str "
        + "FROM sharepoint.tasks t1 "
        + "JOIN sharepoint.users u ON CAST(t1.assigned_to AS VARCHAR) = u.username "
        + "WHERE CAST(t1.created_date AS TIMESTAMP) > TIMESTAMP '2023-01-01 00:00:00'";

    assertDoesNotThrow(() -> {
      PreparedStatement pstmt = connection.prepareStatement(joinQuery);
      assertNotNull(pstmt, "JOIN query with CAST should be preparable");
      pstmt.close();
    }, "JOIN query with CAST operations should parse successfully");
  }

  @Test
  public void testCastInSubqueries() throws SQLException {
    // Test CAST operations in subqueries
    String subqueryWithCast = "SELECT title FROM sharepoint.tasks "
        + "WHERE CAST(priority AS INTEGER) > ("
        + "  SELECT AVG(CAST(priority AS INTEGER)) FROM sharepoint.tasks"
        + ")";

    assertDoesNotThrow(() -> {
      PreparedStatement pstmt = connection.prepareStatement(subqueryWithCast);
      assertNotNull(pstmt, "Subquery with CAST should be preparable");
      pstmt.close();
    }, "Subquery with CAST operations should parse successfully");
  }

  /**
   * Mock SharePoint schema that provides table metadata for CAST testing
   * without requiring actual SharePoint connectivity.
   */
  private static class MockSharePointSchema extends AbstractSchema {
    @Override
    protected Map<String, org.apache.calcite.schema.Table> getTableMap() {
      Map<String, org.apache.calcite.schema.Table> tables = new HashMap<>();
      
      // Create mock tables with various column types for CAST testing
      tables.put("tasks", new MockSharePointTable("tasks"));
      tables.put("users", new MockSharePointTable("users"));
      
      return tables;
    }
  }

  /**
   * Mock SharePoint table that provides column metadata for CAST testing.
   */
  private static class MockSharePointTable extends org.apache.calcite.schema.impl.AbstractTable
      implements org.apache.calcite.schema.ScannableTable {
    private final String tableName;

    MockSharePointTable(String tableName) {
      this.tableName = tableName;
    }

    @Override
    public org.apache.calcite.rel.type.RelDataType getRowType(
        org.apache.calcite.rel.type.RelDataTypeFactory typeFactory) {
      
      org.apache.calcite.rel.type.RelDataTypeFactory.Builder builder = typeFactory.builder();

      if ("tasks".equals(tableName)) {
        // Mock SharePoint Tasks list structure
        builder.add("id", typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.VARCHAR));
        builder.add("title", typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.VARCHAR));
        builder.add("description", typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.VARCHAR));
        builder.add("priority", typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.INTEGER));
        builder.add("budget", typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.DOUBLE));
        builder.add("is_complete", typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.BOOLEAN));
        builder.add("created_date", typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.TIMESTAMP));
        builder.add("assigned_to", typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.VARCHAR));
      } else if ("users".equals(tableName)) {
        // Mock SharePoint Users list structure
        builder.add("id", typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.VARCHAR));
        builder.add("username", typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.VARCHAR));
        builder.add("display_name", typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.VARCHAR));
        builder.add("email", typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.VARCHAR));
      }

      return builder.build();
    }

    @Override
    public org.apache.calcite.linq4j.Enumerable<Object[]> scan(
        org.apache.calcite.DataContext root) {
      // Return empty enumerable for mock data
      return org.apache.calcite.linq4j.Linq4j.emptyEnumerable();
    }
  }
}