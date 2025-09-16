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
package org.apache.calcite.adapter.govdata.sec;

// import org.apache.calcite.adapter.govdata.GovDataTestModels;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Basic tests for SEC schema creation and table discovery.
 * Requires file downloads to populate tables.
 */
@Tag("integration")
public class BasicSecSchemaTest {

  @Test
  @org.junit.jupiter.api.Disabled("GovDataTestModels not available")
  public void testSchemaWithAllTables() throws Exception {

    // Load test model from resources
    String modelPath = null; // GovDataTestModels.loadTestModel("basic-test-model");

    // Connection properties
    Properties info = new Properties();
    info.setProperty("lex", "ORACLE");
    info.setProperty("unquotedCasing", "TO_LOWER");

    String jdbcUrl = "jdbc:calcite:model=" + modelPath;

    try (Connection connection = DriverManager.getConnection(jdbcUrl, info)) {

      // Get database metadata
      DatabaseMetaData metaData = connection.getMetaData();
      
      // List all tables in SEC schema
      Set<String> tables = new HashSet<>();
      
      try (ResultSet rs = metaData.getTables(null, "sec", "%", null)) {
        while (rs.next()) {
          String tableName = rs.getString("TABLE_NAME");
          tables.add(tableName.toLowerCase());
        }
      }
      
      // Verify expected tables exist
      assertTrue(tables.contains("financial_line_items"), "financial_line_items table should exist");
      assertTrue(tables.contains("financial_facts"), "financial_facts table should exist");
      assertTrue(tables.contains("management_discussion"), "management_discussion table should exist");
      assertTrue(tables.contains("company_metadata"), "company_metadata table should exist");
      assertTrue(tables.contains("insider_transactions"), "insider_transactions table should exist");
      assertTrue(tables.contains("earnings_transcripts"), "earnings_transcripts table should exist");
      
      assertTrue(tables.size() >= 6, "Should have at least 6 expected tables, found " + tables.size());
    }
  }
}