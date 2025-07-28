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
package org.apache.calcite.adapter.file;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Locale;
import java.util.Map;

/**
 * Test to show actual tables and columns extracted from lots_of_tables.xlsx.
 */
public class ShowActualTablesTest {

  @Test public void showActualTablesFromExcel() throws Exception {
    File testDir = new File("src/test/resources/testdata");

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:");
         CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class)) {

      SchemaPlus rootSchema = calciteConnection.getRootSchema();

      // Create FileSchema with multi-table Excel detection enabled
      FileSchema fileSchema =
          new FileSchema(rootSchema, "excel", testDir, null, new ExecutionEngineConfig(), false, null, null);

      rootSchema.add("excel", fileSchema);

      System.out.println("\n=== ACTUAL TABLES FROM lots_of_tables.xlsx ===\n");

      // Get table map to force conversion
      Map<String, Table> tableMap = fileSchema.getTableMap();

      // List all tables
      for (Map.Entry<String, Table> entry : tableMap.entrySet()) {
        String tableName = entry.getKey();
        if (tableName.toLowerCase(Locale.ROOT).startsWith("lots_of_tables")) {
          System.out.println("TABLE: " + tableName);

          // Query the table to get column information
          try (Statement stmt = connection.createStatement()) {
            String query = String.format(Locale.ROOT, "SELECT * FROM excel.\"%s\" WHERE 1=0", tableName);
            try (ResultSet rs = stmt.executeQuery(query)) {
              ResultSetMetaData metadata = rs.getMetaData();
              int columnCount = metadata.getColumnCount();

              System.out.print("COLUMNS: ");
              for (int i = 1; i <= columnCount; i++) {
                if (i > 1) System.out.print(", ");
                System.out.print(metadata.getColumnName(i));
              }
              System.out.println("\n");
            }
          } catch (Exception e) {
            System.out.println("Error querying table: " + e.getMessage() + "\n");
          }
        }
      }

      // Also check JSON files created
      System.out.println("\nJSON FILES CREATED:");
      File[] jsonFiles = testDir.listFiles((dir, name) -> name.endsWith(".json"));
      if (jsonFiles != null) {
        for (File jsonFile : jsonFiles) {
          if (jsonFile.getName().startsWith("lots_of_tables")) {
            System.out.println("  - " + jsonFile.getName());
          }
        }
      }
    }
  }
}
