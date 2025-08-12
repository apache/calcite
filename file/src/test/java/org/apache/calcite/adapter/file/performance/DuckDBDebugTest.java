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
package org.apache.calcite.adapter.file.performance;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Debug test for DuckDB Parquet reading.
 */
public class DuckDBDebugTest {
  
  @TempDir
  java.nio.file.Path tempDir;
  
  @Test
  public void testDuckDBParquetReading() throws Exception {
    System.out.println("\n=== DuckDB Parquet Debug Test ===");
    
    // Create a simple CSV file first
    File csvFile = new File(tempDir.toFile(), "test.csv");
    try (PrintWriter writer = new PrintWriter(new FileWriter(csvFile, StandardCharsets.UTF_8))) {
      writer.println("id,name,value");
      writer.println("1,Alice,100");
      writer.println("2,Bob,200");
      writer.println("3,Charlie,300");
    }
    System.out.println("Created CSV file: " + csvFile.getAbsolutePath());
    
    String url = "jdbc:duckdb:";
    try (Connection conn = DriverManager.getConnection(url);
         Statement stmt = conn.createStatement()) {
      
      // Test 1: Read CSV directly
      System.out.println("\n1. Testing CSV reading:");
      String csvPath = csvFile.getAbsolutePath();
      String csvQuery = "SELECT * FROM read_csv_auto('" + csvPath + "')";
      System.out.println("Query: " + csvQuery);
      
      try (ResultSet rs = stmt.executeQuery(csvQuery)) {
        int count = 0;
        while (rs.next()) {
          count++;
          System.out.println("  Row " + count + ": id=" + rs.getInt("id") + 
                           ", name=" + rs.getString("name") + 
                           ", value=" + rs.getInt("value"));
        }
        System.out.println("✓ Read " + count + " rows from CSV");
      }
      
      // Test 2: Create a Parquet file from CSV
      System.out.println("\n2. Creating Parquet file from CSV:");
      File parquetFile = new File(tempDir.toFile(), "test.parquet");
      String createParquetQuery = "COPY (SELECT * FROM read_csv_auto('" + csvPath + "')) " +
                                 "TO '" + parquetFile.getAbsolutePath() + "' (FORMAT PARQUET)";
      System.out.println("Query: " + createParquetQuery);
      stmt.execute(createParquetQuery);
      System.out.println("✓ Created Parquet file: " + parquetFile.getAbsolutePath());
      
      // Test 3: Read Parquet directly
      System.out.println("\n3. Testing Parquet reading:");
      String parquetQuery = "SELECT * FROM '" + parquetFile.getAbsolutePath() + "'";
      System.out.println("Query: " + parquetQuery);
      
      try (ResultSet rs = stmt.executeQuery(parquetQuery)) {
        int count = 0;
        while (rs.next()) {
          count++;
          System.out.println("  Row " + count + ": id=" + rs.getInt("id") + 
                           ", name=" + rs.getString("name") + 
                           ", value=" + rs.getInt("value"));
        }
        System.out.println("✓ Read " + count + " rows from Parquet");
      }
      
      // Test 4: Create a view from Parquet
      System.out.println("\n4. Creating view from Parquet:");
      stmt.execute("CREATE OR REPLACE VIEW test_view AS SELECT * FROM '" + 
                  parquetFile.getAbsolutePath() + "'");
      System.out.println("✓ Created view: test_view");
      
      // Test 5: Query the view
      System.out.println("\n5. Querying view:");
      try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as cnt FROM test_view")) {
        if (rs.next()) {
          System.out.println("✓ COUNT(*) = " + rs.getInt("cnt"));
        }
      }
      
      // Test 6: Test with table name that would be used in comparison
      System.out.println("\n6. Testing with comparison table names:");
      File salesFile = new File(tempDir.toFile(), "sales_1000.parquet");
      
      // Create a simple sales Parquet file
      stmt.execute("CREATE OR REPLACE TABLE sales_temp(order_id INT, customer_id INT, total DOUBLE)");
      stmt.execute("INSERT INTO sales_temp VALUES (1, 100, 99.99), (2, 101, 149.99), (3, 100, 79.99)");
      stmt.execute("COPY sales_temp TO '" + salesFile.getAbsolutePath() + "' (FORMAT PARQUET)");
      System.out.println("✓ Created sales Parquet: " + salesFile.getAbsolutePath());
      
      // Create view as the comparison test does
      stmt.execute("CREATE OR REPLACE VIEW sales_1000 AS SELECT * FROM '" + 
                  salesFile.getAbsolutePath() + "'");
      
      // Test COUNT query
      try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM sales_1000")) {
        if (rs.next()) {
          System.out.println("✓ sales_1000 COUNT(*) = " + rs.getInt(1));
        }
      }
      
      // Test COUNT(DISTINCT)
      try (ResultSet rs = stmt.executeQuery("SELECT COUNT(DISTINCT customer_id) FROM sales_1000")) {
        if (rs.next()) {
          System.out.println("✓ sales_1000 COUNT(DISTINCT customer_id) = " + rs.getInt(1));
        }
      }
      
      System.out.println("\n✅ All DuckDB tests passed!");
    } catch (Exception e) {
      System.err.println("❌ Error: " + e.getMessage());
      e.printStackTrace();
      throw e;
    }
  }
}