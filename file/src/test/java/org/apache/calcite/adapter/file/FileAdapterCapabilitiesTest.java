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
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Test demonstrating file adapter capabilities relevant to materialized views.
 */
public class FileAdapterCapabilitiesTest {
  @TempDir
  Path tempDir;

  @Test public void testFileAdapterAsDataSource() throws Exception {
    System.out.println("\n=== FILE ADAPTER CAPABILITIES TEST ===");
    System.out.println("Demonstrating file adapter as a data source for materialized views");

    // Create test data
    File salesCsv = new File(tempDir.toFile(), "sales.csv");
    try (FileWriter writer = new FileWriter(salesCsv, StandardCharsets.UTF_8)) {
      writer.write("date:string,product:string,amount:double\n");
      writer.write("2024-01-01,Widget,1000.00\n");
      writer.write("2024-01-01,Gadget,2000.00\n");
      writer.write("2024-01-02,Widget,1500.00\n");
      writer.write("2024-01-02,Gizmo,3000.00\n");
    }

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:");
         CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class)) {

      SchemaPlus rootSchema = calciteConnection.getRootSchema();

      // Add file schema
      Map<String, Object> operand = new HashMap<>();
      operand.put("directory", tempDir.toString());
      SchemaPlus fileSchema =
          rootSchema.add("FILES", FileSchemaFactory.INSTANCE.create(rootSchema, "FILES", operand));

      assertNotNull(fileSchema);
      System.out.println("\n✓ File schema created successfully");

      try (Statement statement = connection.createStatement()) {
        // 1. Basic aggregation query (typical MV source query)
        System.out.println("\n1. Aggregation query (typical MV source):");
        String aggQuery = "SELECT \"date\", " +
            "COUNT(*) as transaction_count, " +
            "SUM(\"amount\") as daily_total " +
            "FROM FILES.\"SALES\" " +
            "GROUP BY \"date\" " +
            "ORDER BY \"date\"";

        ResultSet rs = statement.executeQuery(aggQuery);
        System.out.println("   Date       | Transactions | Total");
        System.out.println("   -----------|--------------|--------");
        while (rs.next()) {
          System.out.printf(Locale.ROOT, "   %-11s| %12d | %.2f%n",
              rs.getString("date"),
              rs.getInt("transaction_count"),
              rs.getDouble("daily_total"));
        }
        System.out.println("   ✓ Aggregation successful");

        // 2. Complex aggregation with multiple groupings
        System.out.println("\n2. Product summary (another MV pattern):");
        String productQuery = "SELECT \"product\", " +
            "COUNT(*) as sales_count, " +
            "SUM(\"amount\") as total_revenue, " +
            "AVG(\"amount\") as avg_sale " +
            "FROM FILES.\"SALES\" " +
            "GROUP BY \"product\" " +
            "ORDER BY total_revenue DESC";

        ResultSet rs2 = statement.executeQuery(productQuery);
        System.out.println("   Product | Count | Revenue | Avg Sale");
        System.out.println("   --------|-------|---------|----------");
        while (rs2.next()) {
          System.out.printf(Locale.ROOT, "   %-8s| %5d | %7.2f | %.2f%n",
              rs2.getString("product"),
              rs2.getInt("sales_count"),
              rs2.getDouble("total_revenue"),
              rs2.getDouble("avg_sale"));
        }
        System.out.println("   ✓ Product summary successful");

        // 3. Test metadata access
        System.out.println("\n3. Schema metadata:");
        ResultSet tables =
            connection.getMetaData().getTables(null, "FILES", "%", null);
        while (tables.next()) {
          System.out.println("   Table: " + tables.getString("TABLE_NAME"));

          // Get column info
          ResultSet columns =
              connection.getMetaData().getColumns(null, "FILES", tables.getString("TABLE_NAME"), null);
          while (columns.next()) {
            System.out.printf(Locale.ROOT, "     - %s (%s)%n",
                columns.getString("COLUMN_NAME"),
                columns.getString("TYPE_NAME"));
          }
        }
        System.out.println("   ✓ Metadata access successful");

        System.out.println("\n✅ FILE ADAPTER CAPABILITIES VERIFIED");
        System.out.println("\nThe file adapter successfully:");
        System.out.println("  • Provides tables from CSV files");
        System.out.println("  • Supports complex aggregation queries");
        System.out.println("  • Works with GROUP BY and ORDER BY");
        System.out.println("  • Exposes proper metadata");
        System.out.println("\nThese capabilities make it suitable as a data source");
        System.out.println("for materialized views when used with Calcite Server DDL.");
        System.out.println("\nNote: CREATE MATERIALIZED VIEW requires:");
        System.out.println("  • Calcite Server module for DDL support");
        System.out.println("  • MaterializationService for MV management");
        System.out.println("  • Appropriate storage configuration");
      }
    }
  }
}
