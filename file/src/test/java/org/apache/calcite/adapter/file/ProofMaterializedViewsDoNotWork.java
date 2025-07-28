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
import java.util.Map;

/**
 * PROOF THAT MATERIALIZED VIEWS DO NOT WORK IN FILE ADAPTER
 *
 * This test demonstrates that while FileSchemaFactory accepts the "materializations"
 * operand and prints "Materialized view registered" messages, the materialized views
 * are NOT actually created and CANNOT be queried.
 *
 * The implementation in FileSchema.java line 336-347 is just a TODO placeholder.
 */
public class ProofMaterializedViewsDoNotWork {
  @TempDir
  Path tempDir;

  @Test public void proofThatMaterializedViewsAreNotImplemented() throws Exception {
    System.out.println("\n=== PROOF THAT MATERIALIZED VIEWS DO NOT WORK ===\n");

    // Step 1: Create test data
    File salesCsv = new File(tempDir.toFile(), "sales.csv");
    try (FileWriter writer = new FileWriter(salesCsv, StandardCharsets.UTF_8)) {
      writer.write("date:string,product:string,quantity:int,price:double\n");
      writer.write("2024-01-01,Widget,10,25.50\n");
      writer.write("2024-01-01,Gadget,5,50.00\n");
      writer.write("2024-01-02,Widget,15,25.50\n");
    }
    System.out.println("Created test data: " + salesCsv.getAbsolutePath());

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:");
         CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class)) {

      SchemaPlus rootSchema = calciteConnection.getRootSchema();

      // Step 2: Create materialized view definition
      List<Map<String, Object>> materializations = new ArrayList<>();
      Map<String, Object> mv = new HashMap<>();
      mv.put("view", "daily_summary");
      mv.put("table", "daily_summary_table");
      mv.put("sql", "SELECT \"date\", COUNT(*) as cnt, SUM(\"quantity\") as total " +
                    "FROM \"sales\" GROUP BY \"date\"");
      materializations.add(mv);

      // Step 3: Create schema with materializations
      Map<String, Object> operand = new HashMap<>();
      operand.put("directory", tempDir.toString());
      operand.put("materializations", materializations);  // <-- THIS IS THE KEY PART

      System.out.println("\nCreating FileSchema with materializations operand...");
      System.out.println("Expected: A queryable materialized view named 'daily_summary'");
      System.out.println("Actual: Watch what happens...\n");

      SchemaPlus fileSchema =
          rootSchema.add("TEST", FileSchemaFactory.INSTANCE.create(rootSchema, "TEST", operand));

      // The console will show: "Materialized view registered: daily_summary -> daily_summary_table"
      // But this is MISLEADING - it's not actually created!

      try (Statement stmt = connection.createStatement()) {
        // Step 4: Try to query the materialized view
        System.out.println("\n--- ATTEMPTING TO QUERY MATERIALIZED VIEW ---");

        try {
          System.out.println("Query 1: SELECT * FROM TEST.\"daily_summary\"");
          stmt.executeQuery("SELECT * FROM TEST.\"daily_summary\"");
          System.out.println("SUCCESS - Materialized view exists!");
        } catch (Exception e) {
          System.out.println("FAILED: " + e.getMessage());
        }

        try {
          System.out.println("\nQuery 2: SELECT * FROM TEST.\"daily_summary_table\"");
          stmt.executeQuery("SELECT * FROM TEST.\"daily_summary_table\"");
          System.out.println("SUCCESS - Materialized view table exists!");
        } catch (Exception e) {
          System.out.println("FAILED: " + e.getMessage());
        }

        // Step 5: Show what tables actually exist
        System.out.println("\n--- ACTUAL TABLES IN SCHEMA ---");
        ResultSet tables = connection.getMetaData().getTables(null, "TEST", "%", null);
        int count = 0;
        while (tables.next()) {
          System.out.println("Table: " + tables.getString("TABLE_NAME"));
          count++;
        }
        System.out.println("Total tables found: " + count);

        // Step 6: Show the materialized view SQL works on base table
        System.out.println("\n--- RUNNING MV SQL DIRECTLY ---");
        ResultSet rs =
            stmt.executeQuery("SELECT \"date\", COUNT(*) as cnt, SUM(\"quantity\") as total " +
            "FROM TEST.\"sales\" GROUP BY \"date\"");
        while (rs.next()) {
          System.out.println("Date: " + rs.getString("date") +
                           ", Count: " + rs.getInt("cnt") +
                           ", Total: " + rs.getInt("total"));
        }

        System.out.println("\n=== CONCLUSION ===");
        System.out.println("1. FileSchemaFactory accepts 'materializations' operand");
        System.out.println("2. FileSchema prints 'Materialized view registered' message");
        System.out.println("3. BUT the materialized view is NOT created");
        System.out.println("4. The materialized view CANNOT be queried");
        System.out.println("5. Only the base CSV file is accessible");
        System.out.println("\nThe implementation is incomplete - see FileSchema.java lines 336-347:");
        System.out.println("  // TODO: Implement actual materialized view execution and storage");
      }
    }
  }

  /**
   * Run this test with: ./gradlew :file:test --tests ProofMaterializedViewsDoNotWork
   *
   * Expected output shows:
   * - "Materialized view registered: daily_summary -> daily_summary_table" (misleading!)
   * - Both queries fail with "Object 'daily_summary' not found"
   * - Only 'sales' table exists
   * - The MV SQL works when run directly on base table
   */
  public static void main(String[] args) throws Exception {
    ProofMaterializedViewsDoNotWork test = new ProofMaterializedViewsDoNotWork();
    test.tempDir = Path.of(System.getProperty("java.io.tmpdir"), "mv_test_" + System.currentTimeMillis());
    test.tempDir.toFile().mkdirs();
    test.proofThatMaterializedViewsAreNotImplemented();
  }
}
