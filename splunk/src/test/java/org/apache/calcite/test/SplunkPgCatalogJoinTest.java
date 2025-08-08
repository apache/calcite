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
package org.apache.calcite.test;

import org.apache.calcite.adapter.splunk.SplunkSchemaFactory;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Test pg_catalog JOINs work correctly with consistent OIDs.
 */
@Tag("unit")
class SplunkPgCatalogJoinTest {

  @Test void testPgCatalogJoins() throws SQLException {
    Properties info = new Properties();

    // Use inline model JSON
    String model = "inline:{\n"
        + "  \"version\": \"1.0\",\n"
        + "  \"defaultSchema\": \"splunk\",\n"
        + "  \"schemas\": [\n"
        + "    {\n"
        + "      \"name\": \"splunk\",\n"
        + "      \"type\": \"custom\",\n"
        + "      \"factory\": \"" + SplunkSchemaFactory.class.getName() + "\",\n"
        + "      \"operand\": {\n"
        + "        \"url\": \"https://nonexistent.splunk.server:8089\",\n"
        + "        \"user\": \"admin\",\n"
        + "        \"password\": \"changeme\",\n"
        + "        \"disableSslValidation\": true,\n"
        + "        \"searchCacheEnabled\": false\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}";

    info.setProperty("model", model);

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", info)) {
      // First test that pg_catalog tables exist and have data
      String testQuery = "SELECT COUNT(*) FROM pg_catalog.pg_namespace";
      try (PreparedStatement stmt = connection.prepareStatement(testQuery);
           ResultSet rs = stmt.executeQuery()) {
        rs.next();
        int count = rs.getInt(1);
        assertThat("pg_namespace should have entries", count, greaterThan(0));
      }

      // Test the exact query that was failing
      String query = "SELECT "
          + "a.attname as name, "
          + "t.typname as type, "
          + "NOT a.attnotnull as nullable, "
          + "CAST(NULL AS INTEGER) as numeric_precision, "
          + "CAST(NULL AS INTEGER) as numeric_scale, "
          + "CAST(NULL AS VARCHAR) as description, "
          + "false as auto_increment, "
          + "false as is_primarykey "
          + "FROM pg_catalog.pg_attribute a "
          + "JOIN pg_catalog.pg_class c ON a.attrelid = c.oid "
          + "JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid "
          + "JOIN pg_catalog.pg_type t ON a.atttypid = t.oid "
          + "WHERE n.nspname = ? "
          + "AND c.relname = ? "
          + "AND a.attnum > 0 "
          + "AND NOT a.attisdropped "
          + "ORDER BY a.attnum";

      try (PreparedStatement stmt = connection.prepareStatement(query)) {
        stmt.setString(1, "splunk");
        stmt.setString(2, "web");

        try (ResultSet rs = stmt.executeQuery()) {
          int columnCount = 0;
          while (rs.next()) {
            String name = rs.getString("name");
            String type = rs.getString("type");
            boolean nullable = rs.getBoolean("nullable");

            assertThat("Column name should not be null", name, notNullValue());
            assertThat("Column type should not be null", type, notNullValue());

            columnCount++;
          }

          // We should get at least some columns
          assertThat("Should retrieve columns from joined pg_catalog tables",
              columnCount, greaterThan(0));
        }
      }

      // Test a simpler join to verify OID consistency
      String simpleQuery = "SELECT c.relname, n.nspname "
          + "FROM pg_catalog.pg_class c "
          + "JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid "
          + "WHERE n.nspname = 'splunk' "
          + "LIMIT 5";

      try (PreparedStatement stmt = connection.prepareStatement(simpleQuery);
           ResultSet rs = stmt.executeQuery()) {

        int tableCount = 0;
        while (rs.next()) {
          String tableName = rs.getString(1);
          String schemaName = rs.getString(2);

          assertThat("Table name should not be null", tableName, notNullValue());
          assertThat("Schema should be 'splunk'", schemaName, is("splunk"));

          tableCount++;
        }

        assertThat("Should retrieve tables from joined pg_class and pg_namespace",
            tableCount, greaterThan(0));
      }
    }
  }
}
