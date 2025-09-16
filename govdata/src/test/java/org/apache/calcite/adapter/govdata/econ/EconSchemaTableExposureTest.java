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
package org.apache.calcite.adapter.govdata.econ;

import org.apache.calcite.adapter.govdata.TestEnvironmentLoader;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Simple test to verify all ECON tables are exposed and queryable.
 * Requires GOVDATA_CACHE_DIR and GOVDATA_PARQUET_DIR environment variables.
 */
@Tag("integration")
public class EconSchemaTableExposureTest {
  
  @BeforeAll
  static void setUp() {
    TestEnvironmentLoader.ensureLoaded();
  }
  
  @Test
  public void testAllTablesAreQueryable() throws Exception {
    // Create model JSON using GovDataSchemaFactory
    String modelJson = 
        "{"
        + "  \"version\": \"1.0\","
        + "  \"defaultSchema\": \"econ\","
        + "  \"schemas\": ["
        + "    {"
        + "      \"name\": \"econ\","
        + "      \"type\": \"custom\","
        + "      \"factory\": \"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\","
        + "      \"operand\": {"
        + "        \"dataSource\": \"econ\","
        + "        \"executionEngine\": \"DUCKDB\""
        + "      }"
        + "    }"
        + "  ]"
        + "}";
    
    // Write model file
    Path modelFile = Files.createTempFile("model", ".json");
    Files.write(modelFile, modelJson.getBytes());
    
    // Connect and query
    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    
    Set<String> expectedTables = new HashSet<>();
    expectedTables.add("employment_statistics");
    expectedTables.add("inflation_metrics");
    expectedTables.add("wage_growth");
    expectedTables.add("regional_employment");
    expectedTables.add("treasury_yields");
    expectedTables.add("federal_debt");
    expectedTables.add("world_indicators");
    expectedTables.add("fred_indicators");
    expectedTables.add("gdp_components");
    expectedTables.add("regional_income");
    expectedTables.add("trade_statistics");
    expectedTables.add("ita_data");
    expectedTables.add("industry_gdp");
    
    try (Connection conn = DriverManager.getConnection(
        "jdbc:calcite:model=" + modelFile, props);
         Statement stmt = conn.createStatement()) {
      
      // Test each table with EXPLAIN PLAN
      for (String table : expectedTables) {
        String query = "EXPLAIN PLAN FOR SELECT * FROM econ." + table + " LIMIT 1";
        
        try (ResultSet rs = stmt.executeQuery(query)) {
          assertTrue(rs.next(), "EXPLAIN PLAN should work for " + table);
        } catch (Exception e) {
          fail("Table " + table + " is not queryable: " + e.getMessage());
        }
      }
      
      System.out.println("✅ All " + expectedTables.size() + " ECON tables are exposed and queryable!");
    }
  }
}