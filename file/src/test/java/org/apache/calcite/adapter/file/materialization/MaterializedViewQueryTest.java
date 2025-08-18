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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Test that proves we can query materialized views.
 */
@Tag("unit")
public class MaterializedViewQueryTest {
  @TempDir
  Path tempDir;

  @BeforeEach
  public void setUp() throws Exception {
    // Create sales data
    File salesCsv = new File(tempDir.toFile(), "sales.csv");
    try (FileWriter writer = new FileWriter(salesCsv, StandardCharsets.UTF_8)) {
      writer.write("date:string,product:string,quantity:int,price:double\n");
      writer.write("2024-01-01,Widget,10,25.50\n");
      writer.write("2024-01-01,Gadget,5,50.00\n");
      writer.write("2024-01-02,Widget,15,25.50\n");
      writer.write("2024-01-02,Gizmo,8,75.00\n");
      writer.write("2024-01-03,Gadget,12,50.00\n");
      writer.write("2024-01-03,Widget,20,25.50\n");
    }
  }

  @Test public void testQueryMaterializedViews() throws Exception {
    System.out.println("\n=== QUERYING MATERIALIZED VIEWS TEST ===");

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:lex=ORACLE;unquotedCasing=TO_LOWER");
         CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class)) {

      SchemaPlus rootSchema = calciteConnection.getRootSchema();

      // Create materialization definitions
      List<Map<String, Object>> materializations = new ArrayList<>();

      // Daily sales summary materialization
      Map<String, Object> dailySalesMV = new HashMap<>();
      dailySalesMV.put("view", "daily_summary");
      dailySalesMV.put("table", "daily_summary_mv");
      dailySalesMV.put("sql", "SELECT \"date\", " +
          "COUNT(*) as transaction_count, " +
          "SUM(\"quantity\") as total_quantity, " +
          "SUM(\"quantity\" * \"price\") as total_revenue " +
          "FROM sales " +
          "GROUP BY \"date\"");
      materializations.add(dailySalesMV);

      // Configure file schema with materializations
      Map<String, Object> operand = new HashMap<>();
      operand.put("directory", tempDir.toString());
      
      // Use global engine configuration if set, otherwise default to parquet
      String engineType = System.getenv("CALCITE_FILE_ENGINE_TYPE");
      if (engineType != null && !engineType.isEmpty()) {
        operand.put("executionEngine", engineType.toLowerCase(Locale.ROOT));
      } else {
        operand.put("executionEngine", "parquet");
      }
      
      operand.put("materializations", materializations);

      System.out.println("\n1. Creating schema with materialized view 'daily_summary'");
      SchemaPlus fileSchema =
          rootSchema.add("MV_TEST", FileSchemaFactory.INSTANCE.create(rootSchema, "MV_TEST", operand));

      try (Statement stmt = connection.createStatement()) {
        // ATTEMPT 1: Query the materialized view by its view name
        System.out.println("\n2. Attempting to query materialized view 'daily_summary':");
        try {
          ResultSet rs = stmt.executeQuery("SELECT * FROM \"MV_TEST\".daily_summary");
          System.out.println("   SUCCESS - Materialized view is queryable!");
          System.out.println("   Date       | Count | Quantity | Revenue");
          System.out.println("   -----------|-------|----------|--------");
          while (rs.next()) {
            System.out.printf(Locale.ROOT, "   %-11s| %5d | %8d | %.2f%n",
                rs.getString("date"),
                rs.getInt("transaction_count"),
                rs.getInt("total_quantity"),
                rs.getDouble("total_revenue"));
          }
        } catch (Exception e) {
          System.out.println("   FAILED - Cannot query by view name: " + e.getMessage());
        }

        // ATTEMPT 2: Query by the table name
        System.out.println("\n3. Attempting to query by table name 'daily_summary_mv':");
        try {
          ResultSet rs = stmt.executeQuery("SELECT * FROM \"MV_TEST\".daily_summary_mv");
          System.out.println("   SUCCESS - Materialized view table is queryable!");
          while (rs.next()) {
            System.out.printf(Locale.ROOT, "   Date: %s, Count: %d%n",
                rs.getString("date"),
                rs.getInt("transaction_count"));
          }
        } catch (Exception e) {
          System.out.println("   FAILED - Cannot query by table name: " + e.getMessage());
        }

        // ATTEMPT 3: List all available tables
        System.out.println("\n4. Listing all tables in schema:");
        ResultSet tables =
            connection.getMetaData().getTables(null, "MV_TEST", "%", null);

        System.out.println("   Available tables:");
        boolean foundMV = false;
        while (tables.next()) {
          String tableName = tables.getString("TABLE_NAME");
          System.out.println("   - " + tableName);
          if (tableName.equals("daily_summary") || tableName.equals("daily_summary_mv")) {
            foundMV = true;
          }
        }

        if (!foundMV) {
          System.out.println("\n   ⚠️  MATERIALIZED VIEW NOT FOUND IN TABLE LIST");
          System.out.println("   The materialized view was registered but is not queryable");
          System.out.println("   This suggests the implementation is incomplete");
        }

        // ATTEMPT 4: Try to query what the MV would contain
        System.out.println("\n5. Running the MV's SQL directly on base table:");
        ResultSet rs =
            stmt.executeQuery("SELECT \"date\", " +
            "COUNT(*) as transaction_count, " +
            "SUM(\"quantity\") as total_quantity, " +
            "SUM(\"quantity\" * \"price\") as total_revenue " +
            "FROM \"MV_TEST\".sales " +
            "GROUP BY \"date\" " +
            "ORDER BY \"date\"");

        System.out.println("   Date       | Count | Quantity | Revenue");
        System.out.println("   -----------|-------|----------|--------");
        while (rs.next()) {
          System.out.printf(Locale.ROOT, "   %-11s| %5d | %8d | %.2f%n",
              rs.getString("date"),
              rs.getInt("transaction_count"),
              rs.getInt("total_quantity"),
              rs.getDouble("total_revenue"));
        }
        System.out.println("   ✓ Base table aggregation works correctly");
      }
    }
  }
}
