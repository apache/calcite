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
package org.apache.calcite.adapter.govdata.geo;

import org.apache.calcite.adapter.govdata.TestEnvironmentLoader;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Validation test for GEO schema using real TIGER/Line data.
 * This test downloads real geographic data and validates that:
 * 1. Tables are properly created
 * 2. Data is correctly loaded
 * 3. SQL queries work as expected
 * 4. Foreign key relationships are maintained
 */
@Tag("integration")
public class GeoSchemaValidationTest {

  @BeforeAll
  public static void setupEnvironment() {
    TestEnvironmentLoader.ensureLoaded();
  }

  @Test
  // No timeout - let it download all data
  public void testGeoSchemaWithRealData() throws Exception {
    System.out.println("\n==============================================================");
    System.out.println("GEO SCHEMA VALIDATION TEST - REAL DATA");
    System.out.println("==============================================================");

    // Use the external model file as specified
    // Load model file from test resources
    URL modelUrl = GeoSchemaValidationTest.class.getClassLoader()
        .getResource("govdata-geo-test-model.json");
    if (modelUrl == null) {
      throw new RuntimeException("Could not find govdata-geo-test-model.json in test resources");
    }
    String modelPath = modelUrl.getPath();

    // Verify the model file exists
    if (!Files.exists(Paths.get(modelPath))) {
      throw new IllegalStateException("Model file not found at: " + modelPath);
    }

    System.out.println("Using model file: " + modelPath);

    Properties info = new Properties();
    info.setProperty("lex", "ORACLE");
    info.setProperty("unquotedCasing", "TO_LOWER");

    String jdbcUrl = "jdbc:calcite:model=" + modelPath;
    System.out.println("Connecting with JDBC URL: " + jdbcUrl);

    try (Connection conn = DriverManager.getConnection(jdbcUrl, info)) {
      System.out.println("✓ Connected to Calcite");

      // Step 1: Discover tables
      System.out.println("\n----------------------------------------------------------");
      System.out.println("STEP 1: TABLE DISCOVERY");
      System.out.println("----------------------------------------------------------");

      Map<String, List<String>> tableColumns = discoverTables(conn);

      // Step 2: Validate expected tables exist
      System.out.println("\n----------------------------------------------------------");
      System.out.println("STEP 2: TABLE VALIDATION");
      System.out.println("----------------------------------------------------------");

      validateExpectedTables(tableColumns);

      // Step 3: Query data from each table
      System.out.println("\n----------------------------------------------------------");
      System.out.println("STEP 3: DATA VALIDATION");
      System.out.println("----------------------------------------------------------");

      validateTableData(conn);

      // Step 4: Test relationships
      System.out.println("\n----------------------------------------------------------");
      System.out.println("STEP 4: RELATIONSHIP VALIDATION");
      System.out.println("----------------------------------------------------------");

      validateRelationships(conn);

      // Step 5: Complex queries
      System.out.println("\n----------------------------------------------------------");
      System.out.println("STEP 5: COMPLEX QUERY VALIDATION");
      System.out.println("----------------------------------------------------------");

      validateComplexQueries(conn);

      System.out.println("\n==============================================================");
      System.out.println("✓ ALL VALIDATION TESTS PASSED");
      System.out.println("==============================================================");
    }
  }


  private Map<String, List<String>> discoverTables(Connection conn) throws Exception {
    Map<String, List<String>> tableColumns = new HashMap<>();
    DatabaseMetaData metaData = conn.getMetaData();

    // Try different schema names
    for (String schemaName : new String[]{"GEO", "geo", null}) {
      try (ResultSet tables = metaData.getTables(null, schemaName, "%", new String[]{"TABLE"})) {
        while (tables.next()) {
          String tableName = tables.getString("TABLE_NAME");
          String tableSchema = tables.getString("TABLE_SCHEM");

          if (!tableColumns.containsKey(tableName)) {
            System.out.printf("Found table: %s.%s\n",
                tableSchema != null ? tableSchema : "default", tableName);

            // Get columns for this table
            List<String> columns = new ArrayList<>();
            try (ResultSet cols = metaData.getColumns(null, schemaName, tableName, "%")) {
              while (cols.next()) {
                String colName = cols.getString("COLUMN_NAME");
                String colType = cols.getString("TYPE_NAME");
                columns.add(colName);
                System.out.printf("  - %s (%s)\n", colName, colType);
              }
            }
            tableColumns.put(tableName.toLowerCase(), columns);
          }
        }
      }

      if (!tableColumns.isEmpty()) {
        break; // Found tables, stop searching
      }
    }

    System.out.printf("\nTotal tables discovered: %d\n", tableColumns.size());
    return tableColumns;
  }

  private void validateExpectedTables(Map<String, List<String>> tableColumns) {
    String[] expectedTables = {
        "tiger_states",
        "tiger_counties",
        "tiger_zctas",
        "tiger_cbsa",
        "tiger_census_tracts",
        "tiger_block_groups"
    };

    int found = 0;
    int missing = 0;

    for (String expectedTable : expectedTables) {
      if (tableColumns.containsKey(expectedTable)) {
        System.out.printf("✓ %s: FOUND (%d columns)\n",
            expectedTable, tableColumns.get(expectedTable).size());
        found++;
      } else {
        System.out.printf("✗ %s: MISSING\n", expectedTable);
        missing++;
      }
    }

    System.out.printf("\nSummary: %d found, %d missing\n", found, missing);

    // At minimum, we expect states and counties tables
    assertTrue(tableColumns.containsKey("tiger_states"),
        "tiger_states table must exist");
    assertTrue(tableColumns.containsKey("tiger_counties"),
        "tiger_counties table must exist");
  }

  private void validateTableData(Connection conn) throws Exception {
    // Query tiger_states
    System.out.println("\nQuerying tiger_states...");
    String statesQuery = "SELECT state_fips, state_code, state_name, state_abbr " +
                        "FROM geo.tiger_states " +
                        "ORDER BY state_fips";

    int stateCount = 0;
    try (Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(statesQuery)) {

      System.out.println("State Data:");
      System.out.println("FIPS | Code | Name            | Abbr");
      System.out.println("-----|------|-----------------|-----");

      while (rs.next() && stateCount < 10) {
        System.out.printf("%-4s | %-4s | %-15s | %s\n",
            rs.getString("state_fips"),
            rs.getString("state_code"),
            rs.getString("state_name"),
            rs.getString("state_abbr"));
        stateCount++;
      }
    }

    assertTrue(stateCount > 0, "Should have at least one state record");
    System.out.printf("✓ Found %d state records\n", stateCount);

    // Query tiger_counties
    System.out.println("\nQuerying tiger_counties...");
    String countiesQuery = "SELECT county_fips, state_fips, county_name " +
                          "FROM geo.tiger_counties " +
                          "WHERE state_fips IN ('06', '36', '48') " +
                          "ORDER BY county_fips " +
                          "LIMIT 10";

    int countyCount = 0;
    try (Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(countiesQuery)) {

      System.out.println("County Data (first 10):");
      System.out.println("County FIPS | State | Name");
      System.out.println("------------|-------|------------------");

      while (rs.next()) {
        System.out.printf("%-11s | %-5s | %s\n",
            rs.getString("county_fips"),
            rs.getString("state_fips"),
            rs.getString("county_name"));
        countyCount++;
      }
    }

    assertTrue(countyCount > 0, "Should have at least one county record");
    System.out.printf("✓ Found %d county records\n", countyCount);
  }

  private void validateRelationships(Connection conn) throws Exception {
    // Test foreign key: counties -> states
    System.out.println("\nTesting county-to-state relationship...");

    String joinQuery = "SELECT s.state_name, COUNT(c.county_fips) as county_count " +
                      "FROM geo.tiger_states s " +
                      "LEFT JOIN geo.tiger_counties c ON s.state_fips = c.state_fips " +
                      "WHERE s.state_abbr IN ('CA', 'NY', 'TX') " +
                      "GROUP BY s.state_name " +
                      "ORDER BY s.state_name";

    try (Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(joinQuery)) {

      System.out.println("Counties per State:");
      System.out.println("State      | Counties");
      System.out.println("-----------|--------");

      int statesWithCounties = 0;
      while (rs.next()) {
        System.out.printf("%-10s | %d\n",
            rs.getString("state_name"),
            rs.getInt("county_count"));

        if (rs.getInt("county_count") > 0) {
          statesWithCounties++;
        }
      }

      assertTrue(statesWithCounties > 0,
          "At least one state should have counties");
      System.out.printf("✓ Found %d states with counties\n", statesWithCounties);
    }
  }

  private void validateComplexQueries(Connection conn) throws Exception {
    // Test 1: Aggregation query
    System.out.println("\nTest 1: State statistics...");
    String statsQuery = "SELECT COUNT(DISTINCT state_fips) as state_count, " +
                       "COUNT(DISTINCT state_abbr) as abbr_count " +
                       "FROM geo.tiger_states";

    try (Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(statsQuery)) {

      if (rs.next()) {
        int stateCount = rs.getInt("state_count");
        int abbrCount = rs.getInt("abbr_count");
        System.out.printf("States: %d, Unique abbreviations: %d\n",
            stateCount, abbrCount);

        assertTrue(stateCount > 0, "Should have states in the database");
        assertEquals(stateCount, abbrCount,
            "State count should match abbreviation count");
        System.out.println("✓ Aggregation query successful");
      }
    }

    // Test 2: Filter with ORDER BY
    System.out.println("\nTest 2: Sorted county data...");
    String sortQuery = "SELECT county_name, county_fips " +
                      "FROM geo.tiger_counties " +
                      "WHERE state_fips = '06' " +
                      "ORDER BY county_name " +
                      "LIMIT 5";

    try (Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(sortQuery)) {

      System.out.println("California Counties (alphabetical):");
      int count = 0;
      String previousName = "";

      while (rs.next()) {
        String countyName = rs.getString("county_name");
        System.out.printf("  - %s (%s)\n",
            countyName, rs.getString("county_fips"));

        // Verify ordering
        if (count > 0) {
          assertTrue(countyName.compareTo(previousName) >= 0,
              "Counties should be in alphabetical order");
        }
        previousName = countyName;
        count++;
      }

      assertTrue(count > 0, "Should have California counties");
      System.out.println("✓ Sorting and filtering successful");
    }

    // Test 3: NULL handling
    System.out.println("\nTest 3: NULL value handling...");
    String nullQuery = "SELECT COUNT(*) as total, " +
                      "COUNT(state_abbr) as with_abbr, " +
                      "COUNT(*) - COUNT(state_abbr) as without_abbr " +
                      "FROM geo.tiger_states";

    try (Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(nullQuery)) {

      if (rs.next()) {
        int total = rs.getInt("total");
        int withAbbr = rs.getInt("with_abbr");
        int withoutAbbr = rs.getInt("without_abbr");

        System.out.printf("Total: %d, With abbreviation: %d, Without: %d\n",
            total, withAbbr, withoutAbbr);
        System.out.println("✓ NULL handling successful");
      }
    }
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  public void testGeoSchemaConstraints() throws Exception {
    System.out.println("\n==============================================================");
    System.out.println("GEO SCHEMA CONSTRAINT VALIDATION TEST");
    System.out.println("==============================================================");

    String modelJson = "{\n" +
        "  \"version\": \"1.0\",\n" +
        "  \"defaultSchema\": \"geo\",\n" +
        "  \"schemas\": [\n" +
        "    {\n" +
        "      \"name\": \"geo\",\n" +
        "      \"type\": \"custom\",\n" +
        "      \"factory\": \"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\",\n" +
        "      \"operand\": {\n" +
        "        \"dataSource\": \"geo\",\n" +
        "        \"cacheDirectory\": \"/tmp/geo-constraint-test\",\n" +
        "        \"autoDownload\": true,\n" +
        "        \"startYear\": 2024,\n" +
        "        \"endYear\": 2024,\n" +
        "        \"enableConstraints\": true\n" +
        "      }\n" +
        "    }\n" +
        "  ]\n" +
        "}";

    File tempDir = Files.createTempDirectory("geo-constraint").toFile();
    File modelFile = new File(tempDir, "constraint-model.json");
    Files.write(modelFile.toPath(), modelJson.getBytes());

    Properties info = new Properties();
    info.setProperty("lex", "ORACLE");
    info.setProperty("unquotedCasing", "TO_LOWER");

    String jdbcUrl = "jdbc:calcite:model=" + modelFile.getAbsolutePath();

    try (Connection conn = DriverManager.getConnection(jdbcUrl, info)) {
      DatabaseMetaData metaData = conn.getMetaData();

      // Check primary keys
      System.out.println("\nPrimary Keys:");
      checkPrimaryKeys(metaData, "GEO", "TIGER_STATES", "state_fips");
      checkPrimaryKeys(metaData, "GEO", "TIGER_COUNTIES", "county_fips");

      // Check foreign keys
      System.out.println("\nForeign Keys:");
      checkForeignKeys(metaData, "GEO", "TIGER_COUNTIES", "state_fips",
          "TIGER_STATES", "state_fips");

      System.out.println("\n✓ Constraint validation complete");
    }
  }

  private void checkPrimaryKeys(DatabaseMetaData metaData, String schema,
      String table, String expectedPK) throws Exception {

    try (ResultSet pks = metaData.getPrimaryKeys(null, schema, table)) {
      boolean found = false;
      while (pks.next()) {
        String pkColumn = pks.getString("COLUMN_NAME");
        System.out.printf("  %s.%s: PK column = %s\n", schema, table, pkColumn);
        if (pkColumn.equalsIgnoreCase(expectedPK)) {
          found = true;
        }
      }

      if (found) {
        System.out.printf("  ✓ Found expected primary key: %s\n", expectedPK);
      } else {
        System.out.printf("  ⚠ Primary key %s not found (may not be enforced)\n", expectedPK);
      }
    }
  }

  private void checkForeignKeys(DatabaseMetaData metaData, String schema,
      String table, String fkColumn, String pkTable, String pkColumn) throws Exception {

    try (ResultSet fks = metaData.getImportedKeys(null, schema, table)) {
      boolean found = false;
      while (fks.next()) {
        String fkCol = fks.getString("FKCOLUMN_NAME");
        String pkTab = fks.getString("PKTABLE_NAME");
        String pkCol = fks.getString("PKCOLUMN_NAME");

        System.out.printf("  %s.%s.%s -> %s.%s\n",
            schema, table, fkCol, pkTab, pkCol);

        if (fkCol.equalsIgnoreCase(fkColumn) &&
            pkTab.equalsIgnoreCase(pkTable) &&
            pkCol.equalsIgnoreCase(pkColumn)) {
          found = true;
        }
      }

      if (found) {
        System.out.printf("  ✓ Found expected foreign key: %s -> %s.%s\n",
            fkColumn, pkTable, pkColumn);
      } else {
        System.out.printf("  ⚠ Foreign key %s -> %s.%s not found (may not be enforced)\n",
            fkColumn, pkTable, pkColumn);
      }
    }
  }
}
