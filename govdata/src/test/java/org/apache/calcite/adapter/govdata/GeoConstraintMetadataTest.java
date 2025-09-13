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
package org.apache.calcite.adapter.govdata;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for GEO adapter constraint metadata.
 * Verifies that primary key and foreign key definitions are properly exposed
 * through JDBC metadata APIs.
 */
@Tag("unit")
public class GeoConstraintMetadataTest {

  @Test
  void testGeoTableConstraints() throws Exception {
    // Create model with constraint support enabled
    String model = "{\n" +
        "  \"version\": \"1.0\",\n" +
        "  \"defaultSchema\": \"GEO\",\n" +
        "  \"schemas\": [{\n" +
        "    \"name\": \"GEO\",\n" +
        "    \"type\": \"custom\",\n" +
        "    \"factory\": \"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\",\n" +
        "    \"operand\": {\n" +
        "      \"dataSource\": \"geo\",\n" +
        "      \"cacheDir\": \"/tmp/geo-test\",\n" +
        "      \"enableConstraints\": true,\n" +
        "      \"enabledSources\": [\"tiger\", \"census\", \"hud\"],\n" +
        "      \"dataYear\": 2024,\n" +
        "      \"autoDownload\": false\n" +
        "    }\n" +
        "  }]\n" +
        "}";

    // Write model to temp file
    java.nio.file.Path modelFile = java.nio.file.Files.createTempFile("geo-model", ".json");
    java.nio.file.Files.writeString(modelFile, model);

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");

    try (Connection conn = DriverManager.getConnection(
            "jdbc:calcite:model=" + modelFile, props)) {

      DatabaseMetaData metadata = conn.getMetaData();
      assertNotNull(metadata);

      // Check primary keys for tiger_states table
      try (ResultSet rs = metadata.getPrimaryKeys(null, "GEO", "tiger_states")) {
        List<String> pkColumns = new ArrayList<>();
        while (rs.next()) {
          String columnName = rs.getString("COLUMN_NAME");
          pkColumns.add(columnName);
        }
        assertTrue(pkColumns.contains("state_fips"), "PK should include state_fips");
      }

      // Check primary keys for tiger_counties table
      try (ResultSet rs = metadata.getPrimaryKeys(null, "GEO", "tiger_counties")) {
        List<String> pkColumns = new ArrayList<>();
        while (rs.next()) {
          String columnName = rs.getString("COLUMN_NAME");
          pkColumns.add(columnName);
        }
        assertTrue(pkColumns.contains("county_fips"), "PK should include county_fips");
      }

      // Check foreign keys for tiger_counties table
      try (ResultSet rs = metadata.getImportedKeys(null, "GEO", "tiger_counties")) {
        boolean foundStateFk = false;
        while (rs.next()) {
          String pkTableName = rs.getString("PKTABLE_NAME");
          String pkColumnName = rs.getString("PKCOLUMN_NAME");
          String fkColumnName = rs.getString("FKCOLUMN_NAME");

          if ("tiger_states".equals(pkTableName) && 
              "state_fips".equals(pkColumnName) &&
              "state_fips".equals(fkColumnName)) {
            foundStateFk = true;
          }
        }
        assertTrue(foundStateFk, 
            "Should find foreign key from tiger_counties.state_fips to tiger_states.state_fips");
      }

      // Check primary keys for census_places table
      try (ResultSet rs = metadata.getPrimaryKeys(null, "GEO", "census_places")) {
        List<String> pkColumns = new ArrayList<>();
        while (rs.next()) {
          String columnName = rs.getString("COLUMN_NAME");
          pkColumns.add(columnName);
        }
        assertTrue(pkColumns.contains("place_code"), "PK should include place_code");
        assertTrue(pkColumns.contains("state_code"), "PK should include state_code");
      }

      // Check foreign keys for hud_zip_county table
      try (ResultSet rs = metadata.getImportedKeys(null, "GEO", "hud_zip_county")) {
        boolean foundCountyFk = false;
        while (rs.next()) {
          String pkTableName = rs.getString("PKTABLE_NAME");
          String pkColumnName = rs.getString("PKCOLUMN_NAME");
          String fkColumnName = rs.getString("FKCOLUMN_NAME");

          if ("tiger_counties".equals(pkTableName) && 
              "county_fips".equals(pkColumnName) &&
              "county_fips".equals(fkColumnName)) {
            foundCountyFk = true;
          }
        }
        assertTrue(foundCountyFk, 
            "Should find foreign key from hud_zip_county.county_fips to tiger_counties.county_fips");
      }
    }

    // Clean up
    java.nio.file.Files.deleteIfExists(modelFile);
  }

  @Test
  void testCrossDomainConstraints() throws Exception {
    // Create model with both SEC and GEO schemas to test cross-domain FKs
    String model = "{\n" +
        "  \"version\": \"1.0\",\n" +
        "  \"defaultSchema\": \"SEC\",\n" +
        "  \"schemas\": [\n" +
        "    {\n" +
        "      \"name\": \"SEC\",\n" +
        "      \"type\": \"custom\",\n" +
        "      \"factory\": \"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\",\n" +
        "      \"operand\": {\n" +
        "        \"dataSource\": \"sec\",\n" +
        "        \"directory\": \"/tmp/sec-test\",\n" +
        "        \"enableConstraints\": true,\n" +
        "        \"testMode\": true,\n" +
        "        \"useMockData\": true,\n" +
        "        \"ciks\": [\"TEST\"],\n" +
        "        \"startYear\": 2023,\n" +
        "        \"endYear\": 2023\n" +
        "      }\n" +
        "    },\n" +
        "    {\n" +
        "      \"name\": \"GEO\",\n" +
        "      \"type\": \"custom\",\n" +
        "      \"factory\": \"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\",\n" +
        "      \"operand\": {\n" +
        "        \"dataSource\": \"geo\",\n" +
        "        \"cacheDir\": \"/tmp/geo-test\",\n" +
        "        \"enableConstraints\": true,\n" +
        "        \"enabledSources\": [\"tiger\"],\n" +
        "        \"dataYear\": 2024,\n" +
        "        \"autoDownload\": false\n" +
        "      }\n" +
        "    }\n" +
        "  ]\n" +
        "}";

    // Write model to temp file
    java.nio.file.Path modelFile = java.nio.file.Files.createTempFile("cross-domain-model", ".json");
    java.nio.file.Files.writeString(modelFile, model);

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");

    try (Connection conn = DriverManager.getConnection(
            "jdbc:calcite:model=" + modelFile, props)) {

      DatabaseMetaData metadata = conn.getMetaData();
      assertNotNull(metadata);

      // Verify both schemas exist
      try (ResultSet rs = metadata.getSchemas()) {
        List<String> schemas = new ArrayList<>();
        while (rs.next()) {
          schemas.add(rs.getString("TABLE_SCHEM"));
        }
        assertTrue(schemas.contains("SEC"), "Should have SEC schema");
        assertTrue(schemas.contains("GEO"), "Should have GEO schema");
      }

      // Check that we can see tables from both schemas
      try (ResultSet rs = metadata.getTables(null, "SEC", "%", new String[]{"TABLE"})) {
        boolean foundFinancialTable = false;
        while (rs.next()) {
          String tableName = rs.getString("TABLE_NAME");
          if ("financial_line_items".equals(tableName)) {
            foundFinancialTable = true;
          }
        }
        assertTrue(foundFinancialTable, "Should find SEC financial_line_items table");
      }

      try (ResultSet rs = metadata.getTables(null, "GEO", "%", new String[]{"TABLE"})) {
        boolean foundStatesTable = false;
        while (rs.next()) {
          String tableName = rs.getString("TABLE_NAME");
          if ("tiger_states".equals(tableName)) {
            foundStatesTable = true;
          }
        }
        assertTrue(foundStatesTable, "Should find GEO tiger_states table");
      }
    }

    // Clean up
    java.nio.file.Files.deleteIfExists(modelFile);
  }

  @Test
  void testConstraintsCanBeDisabled() throws Exception {
    // Create model with constraints disabled
    String model = "{\n" +
        "  \"version\": \"1.0\",\n" +
        "  \"defaultSchema\": \"GEO\",\n" +
        "  \"schemas\": [{\n" +
        "    \"name\": \"GEO\",\n" +
        "    \"type\": \"custom\",\n" +
        "    \"factory\": \"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\",\n" +
        "    \"operand\": {\n" +
        "      \"dataSource\": \"geo\",\n" +
        "      \"cacheDir\": \"/tmp/geo-test\",\n" +
        "      \"enableConstraints\": false,\n" +
        "      \"enabledSources\": [\"tiger\"],\n" +
        "      \"dataYear\": 2024,\n" +
        "      \"autoDownload\": false\n" +
        "    }\n" +
        "  }]\n" +
        "}";

    // Write model to temp file
    java.nio.file.Path modelFile = java.nio.file.Files.createTempFile("geo-no-constraints", ".json");
    java.nio.file.Files.writeString(modelFile, model);

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");

    try (Connection conn = DriverManager.getConnection(
            "jdbc:calcite:model=" + modelFile, props)) {

      DatabaseMetaData metadata = conn.getMetaData();

      // When constraints are disabled, no primary keys should be reported
      try (ResultSet rs = metadata.getPrimaryKeys(null, "GEO", "tiger_states")) {
        int count = 0;
        while (rs.next()) {
          count++;
        }
        assertEquals(0, count, "Should not find primary keys when constraints are disabled");
      }

      // When constraints are disabled, no foreign keys should be reported
      try (ResultSet rs = metadata.getImportedKeys(null, "GEO", "tiger_counties")) {
        int count = 0;
        while (rs.next()) {
          count++;
        }
        assertEquals(0, count, "Should not find foreign keys when constraints are disabled");
      }
    }

    // Clean up
    java.nio.file.Files.deleteIfExists(modelFile);
  }
}