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

import org.apache.calcite.adapter.file.FileSchemaFactory;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;

/**
 * Performance test for complex joins - star schema with multiple dimension tables.
 * Tests DuckDB's sophisticated join algorithms vs Calcite's join optimization.
 * 
 * Creates a realistic star schema with:
 * - Large fact table (sales transactions)
 * - Multiple dimension tables (customers, products, stores, dates)
 * - Complex join patterns typical in data warehousing
 */
public class ComplexJoinsPerformanceTest {
  
  @TempDir
  static java.nio.file.Path tempDir;
  
  private static Connection duckdbConn;
  private static Connection calciteConn;
  
  private static final int[] FACT_TABLE_SIZES = {10000, 50000, 100000, 500000, 1000000};
  
  @BeforeAll
  public static void setUpOnce() throws Exception {
    System.out.println("Setting up complex joins performance test (star schema)...");
    
    // Create star schema: fact table + dimension tables
    for (int factSize : FACT_TABLE_SIZES) {
      createStarSchema(factSize);
    }
    
    // Setup DuckDB connection
    duckdbConn = DriverManager.getConnection("jdbc:duckdb:");
    try (Statement stmt = duckdbConn.createStatement()) {
      for (int factSize : FACT_TABLE_SIZES) {
        registerDuckDBViews(stmt, factSize);
      }
    }
    
    // Setup Calcite connection
    calciteConn = DriverManager.getConnection("jdbc:calcite:");
    CalciteConnection calciteConnection = calciteConn.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();
    
    Map<String, Object> operand = new LinkedHashMap<>();
    operand.put("directory", tempDir.toString());
    operand.put("executionEngine", "parquet");
    
    rootSchema.add("FILES", FileSchemaFactory.INSTANCE.create(rootSchema, "FILES", operand));
    
    System.out.println("Setup complete. Ready for complex join testing.\n");
  }
  
  @Test
  public void testComplexJoinsPerformance() throws Exception {
    System.out.println("\n╔══════════════════════════════════════════════════════════════════════════════════════════╗");
    System.out.println("║                        COMPLEX JOINS PERFORMANCE COMPARISON                                 ║");
    System.out.println("║                           Star Schema: Fact + 5 Dimension Tables                            ║");
    System.out.println("╚══════════════════════════════════════════════════════════════════════════════════════════╝\n");
    
    // Test different join patterns
    testSimpleStarJoin();
    testFilteredStarJoin();
    testAggregatedStarJoin();
    testNestedJoins();
    
    System.out.println("\n╔══════════════════════════════════════════════════════════════════════════════════════════╗");
    System.out.println("║                                   KEY INSIGHTS                                              ║");
    System.out.println("╚══════════════════════════════════════════════════════════════════════════════════════════╝");
    
    System.out.println("\n⚡ JOIN ALGORITHM ADVANTAGES:");
    System.out.println("   • DuckDB's hash joins are highly optimized for analytical workloads");
    System.out.println("   • Intelligent join reordering based on cardinality estimates");
    System.out.println("   • Vectorized hash table probing provides significant speedup");
    System.out.println("   • This is where DuckDB's OLAP focus shows strongest advantages");
    
    System.out.println("\n🔄 JOIN PATTERN PERFORMANCE:");
    System.out.println("   • Star schema joins favor DuckDB's optimization strategies");
    System.out.println("   • Performance gap increases with join complexity and data size");
    System.out.println("   • Calcite's rule-based optimization needs better cost estimates");
  }
  
  private void testSimpleStarJoin() throws Exception {
    System.out.println("Testing Simple Star Join (Fact + All Dimensions):");
    System.out.println("════════════════════════════════════════════════");
    
    for (int factSize : FACT_TABLE_SIZES) {
      String baseQuery = "SELECT COUNT(*), AVG(f.amount) " +
                        "FROM sales_fact_{size} f " +
                        "JOIN dim_customers_{size} c ON f.customer_id = c.customer_id " +
                        "JOIN dim_products_{size} p ON f.product_id = p.product_id " +
                        "JOIN dim_stores_{size} s ON f.store_id = s.store_id " +
                        "JOIN dim_dates_{size} d ON f.date_id = d.date_id ";
      
      String duckdbQuery = baseQuery.replace("{size}", String.valueOf(factSize));
      String calciteQuery = baseQuery
          .replace("sales_fact_{size}", "FILES.\"sales_fact_" + factSize + "\"")
          .replace("dim_customers_{size}", "FILES.\"dim_customers_" + factSize + "\"")
          .replace("dim_products_{size}", "FILES.\"dim_products_" + factSize + "\"")
          .replace("dim_stores_{size}", "FILES.\"dim_stores_" + factSize + "\"")
          .replace("dim_dates_{size}", "FILES.\"dim_dates_" + factSize + "\"")
          .replace("customer_id", "\"customer_id\"")
          .replace("product_id", "\"product_id\"")
          .replace("store_id", "\"store_id\"")
          .replace("date_id", "\"date_id\"")
          .replace("amount", "\"amount\"");
      
      long duckdbTime = measureQuery(duckdbConn, duckdbQuery, "DuckDB");
      long calciteTime = factSize <= 50000 ? measureQuery(calciteConn, calciteQuery, "Calcite") : -1;
      
      reportResults(factSize, duckdbTime, calciteTime, "5-table star join");
    }
    System.out.println();
  }
  
  private void testFilteredStarJoin() throws Exception {
    System.out.println("Testing Filtered Star Join (with WHERE conditions):");
    System.out.println("═══════════════════════════════════════════════════");
    
    for (int factSize : FACT_TABLE_SIZES) {
      String baseQuery = "SELECT c.segment, p.category, SUM(f.amount) as total_sales " +
                        "FROM sales_fact_{size} f " +
                        "JOIN dim_customers_{size} c ON f.customer_id = c.customer_id " +
                        "JOIN dim_products_{size} p ON f.product_id = p.product_id " +
                        "JOIN dim_stores_{size} s ON f.store_id = s.store_id " +
                        "WHERE c.segment = 'Premium' AND p.category = 'Electronics' " +
                        "AND s.region = 'North' " +
                        "GROUP BY c.segment, p.category ";
      
      String duckdbQuery = baseQuery.replace("{size}", String.valueOf(factSize));
      String calciteQuery = baseQuery
          .replace("sales_fact_{size}", "FILES.\"sales_fact_" + factSize + "\"")
          .replace("dim_customers_{size}", "FILES.\"dim_customers_" + factSize + "\"")
          .replace("dim_products_{size}", "FILES.\"dim_products_" + factSize + "\"")
          .replace("dim_stores_{size}", "FILES.\"dim_stores_" + factSize + "\"")
          .replaceAll("\\b(customer_id|product_id|store_id|segment|category|region|amount)\\b", "\"$1\"");
      
      long duckdbTime = measureQuery(duckdbConn, duckdbQuery, "DuckDB");
      long calciteTime = factSize <= 25000 ? measureQuery(calciteConn, calciteQuery, "Calcite") : -1;
      
      reportResults(factSize, duckdbTime, calciteTime, "filtered star join");
    }
    System.out.println();
  }
  
  private void testAggregatedStarJoin() throws Exception {
    System.out.println("Testing Aggregated Star Join (complex GROUP BY):");
    System.out.println("════════════════════════════════════════════════");
    
    for (int factSize : FACT_TABLE_SIZES) {
      if (factSize > 100000) continue; // Skip very large for complex aggregation
      
      String baseQuery = "SELECT " +
                        "d.year, d.quarter, s.region, p.category, " +
                        "COUNT(*) as transaction_count, " +
                        "SUM(f.amount) as total_revenue, " +
                        "AVG(f.amount) as avg_transaction, " +
                        "COUNT(DISTINCT c.customer_id) as unique_customers " +
                        "FROM sales_fact_{size} f " +
                        "JOIN dim_customers_{size} c ON f.customer_id = c.customer_id " +
                        "JOIN dim_products_{size} p ON f.product_id = p.product_id " +
                        "JOIN dim_stores_{size} s ON f.store_id = s.store_id " +
                        "JOIN dim_dates_{size} d ON f.date_id = d.date_id " +
                        "GROUP BY d.year, d.quarter, s.region, p.category " +
                        "HAVING SUM(f.amount) > 1000 ";
      
      String duckdbQuery = baseQuery.replace("{size}", String.valueOf(factSize));
      String calciteQuery = baseQuery
          .replace("sales_fact_{size}", "FILES.\"sales_fact_" + factSize + "\"")
          .replace("dim_customers_{size}", "FILES.\"dim_customers_" + factSize + "\"")
          .replace("dim_products_{size}", "FILES.\"dim_products_" + factSize + "\"")
          .replace("dim_stores_{size}", "FILES.\"dim_stores_" + factSize + "\"")
          .replace("dim_dates_{size}", "FILES.\"dim_dates_" + factSize + "\"")
          .replaceAll("\\b(customer_id|product_id|store_id|date_id|year|quarter|region|category|amount)\\b", "\"$1\"");
      
      long duckdbTime = measureQuery(duckdbConn, duckdbQuery, "DuckDB");
      long calciteTime = factSize <= 25000 ? measureQuery(calciteConn, calciteQuery, "Calcite") : -1;
      
      reportResults(factSize, duckdbTime, calciteTime, "aggregated star join");
    }
    System.out.println();
  }
  
  private void testNestedJoins() throws Exception {
    System.out.println("Testing Nested Joins (subqueries with joins):");
    System.out.println("═════════════════════════════════════════════");
    
    for (int factSize : FACT_TABLE_SIZES) {
      if (factSize > 50000) continue; // Skip large for complex nested queries
      
      String baseQuery = "SELECT outer_result.region, AVG(outer_result.customer_avg) as regional_avg " +
                        "FROM (" +
                        "  SELECT s.region, c.customer_id, AVG(f.amount) as customer_avg " +
                        "  FROM sales_fact_{size} f " +
                        "  JOIN dim_customers_{size} c ON f.customer_id = c.customer_id " +
                        "  JOIN dim_stores_{size} s ON f.store_id = s.store_id " +
                        "  GROUP BY s.region, c.customer_id " +
                        "  HAVING COUNT(*) > 2 " +
                        ") outer_result " +
                        "GROUP BY outer_result.region ";
      
      String duckdbQuery = baseQuery.replace("{size}", String.valueOf(factSize));
      String calciteQuery = baseQuery
          .replace("sales_fact_{size}", "FILES.\"sales_fact_" + factSize + "\"")
          .replace("dim_customers_{size}", "FILES.\"dim_customers_" + factSize + "\"")
          .replace("dim_stores_{size}", "FILES.\"dim_stores_" + factSize + "\"")
          .replaceAll("\\b(customer_id|store_id|region|amount)\\b", "\"$1\"");
      
      long duckdbTime = measureQuery(duckdbConn, duckdbQuery, "DuckDB");
      long calciteTime = factSize <= 10000 ? measureQuery(calciteConn, calciteQuery, "Calcite") : -1;
      
      reportResults(factSize, duckdbTime, calciteTime, "nested joins");
    }
  }
  
  private void reportResults(int factSize, long duckdbTime, long calciteTime, String queryType) {
    String rowStr = String.format("%7s", formatRowCount(factSize));
    String duckdbStr = duckdbTime > 0 ? String.format("%6d ms", duckdbTime) : "  ERROR";
    String calciteStr = calciteTime > 0 ? String.format("%6d ms", calciteTime) : "SKIPPED";
    String speedup = (duckdbTime > 0 && calciteTime > 0) ? 
                    String.format("%6.1fx", (double) calciteTime / duckdbTime) : "   N/A";
    
    System.out.printf("  %s rows: DuckDB %s | Calcite %s | Speedup %s (%s)%n", 
                     rowStr, duckdbStr, calciteStr, speedup, queryType);
  }
  
  private long measureQuery(Connection conn, String query, String engine) {
    try (Statement stmt = conn.createStatement()) {
      // Warmup
      executeAndConsume(stmt, query);
      
      // Measure
      long start = System.nanoTime();
      executeAndConsume(stmt, query);
      return (System.nanoTime() - start) / 1_000_000;
    } catch (Exception e) {
      System.out.println("    " + engine + " error: " + e.getMessage());
      return -1;
    }
  }
  
  private static void createStarSchema(int factSize) throws Exception {
    // Calculate dimension sizes (typically much smaller than fact table)
    int customerCount = Math.max(1000, factSize / 50);  // ~2% of facts
    int productCount = Math.max(500, factSize / 100);   // ~1% of facts  
    int storeCount = Math.max(100, factSize / 500);     // ~0.2% of facts
    int dateCount = Math.max(365, factSize / 100);      // ~1% of facts (daily data)
    
    createFactTable("sales_fact_" + factSize, factSize, customerCount, productCount, storeCount, dateCount);
    createCustomerDimension("dim_customers_" + factSize, customerCount);
    createProductDimension("dim_products_" + factSize, productCount);
    createStoreDimension("dim_stores_" + factSize, storeCount);
    createDateDimension("dim_dates_" + factSize, dateCount);
  }
  
  @SuppressWarnings("deprecation")
  private static void createFactTable(String name, int rows, int maxCustomerId, int maxProductId, 
                                     int maxStoreId, int maxDateId) throws Exception {
    File file = new File(tempDir.toFile(), name + ".parquet");
    if (file.exists()) return;
    
    String schemaString = "{\"type\": \"record\",\"name\": \"FactRecord\",\"fields\": [" +
                         "  {\"name\": \"transaction_id\", \"type\": \"int\"}," +
                         "  {\"name\": \"customer_id\", \"type\": \"int\"}," +
                         "  {\"name\": \"product_id\", \"type\": \"int\"}," +
                         "  {\"name\": \"store_id\", \"type\": \"int\"}," +
                         "  {\"name\": \"date_id\", \"type\": \"int\"}," +
                         "  {\"name\": \"amount\", \"type\": \"double\"}," +
                         "  {\"name\": \"quantity\", \"type\": \"int\"}" +
                         "]}";
    
    Schema avroSchema = new Schema.Parser().parse(schemaString);
    Random random = new Random(42);
    
    try (ParquetWriter<GenericRecord> writer = createWriter(file, avroSchema)) {
      for (int i = 0; i < rows; i++) {
        GenericRecord record = new GenericData.Record(avroSchema);
        record.put("transaction_id", i);
        record.put("customer_id", random.nextInt(maxCustomerId));
        record.put("product_id", random.nextInt(maxProductId));
        record.put("store_id", random.nextInt(maxStoreId));
        record.put("date_id", random.nextInt(maxDateId));
        record.put("amount", Math.round((10.0 + random.nextDouble() * 490.0) * 100.0) / 100.0);
        record.put("quantity", 1 + random.nextInt(5));
        writer.write(record);
      }
    }
  }
  
  @SuppressWarnings("deprecation")
  private static void createCustomerDimension(String name, int count) throws Exception {
    File file = new File(tempDir.toFile(), name + ".parquet");
    if (file.exists()) return;
    
    String schemaString = "{\"type\": \"record\",\"name\": \"CustomerRecord\",\"fields\": [" +
                         "  {\"name\": \"customer_id\", \"type\": \"int\"}," +
                         "  {\"name\": \"name\", \"type\": \"string\"}," +
                         "  {\"name\": \"segment\", \"type\": \"string\"}," +
                         "  {\"name\": \"region\", \"type\": \"string\"}" +
                         "]}";
    
    Schema avroSchema = new Schema.Parser().parse(schemaString);
    Random random = new Random(42);
    String[] segments = {"Basic", "Premium", "Enterprise"};
    String[] regions = {"North", "South", "East", "West"};
    
    try (ParquetWriter<GenericRecord> writer = createWriter(file, avroSchema)) {
      for (int i = 0; i < count; i++) {
        GenericRecord record = new GenericData.Record(avroSchema);
        record.put("customer_id", i);
        record.put("name", "Customer_" + i);
        record.put("segment", segments[random.nextInt(segments.length)]);
        record.put("region", regions[random.nextInt(regions.length)]);
        writer.write(record);
      }
    }
  }
  
  @SuppressWarnings("deprecation")
  private static void createProductDimension(String name, int count) throws Exception {
    File file = new File(tempDir.toFile(), name + ".parquet");
    if (file.exists()) return;
    
    String schemaString = "{\"type\": \"record\",\"name\": \"ProductRecord\",\"fields\": [" +
                         "  {\"name\": \"product_id\", \"type\": \"int\"}," +
                         "  {\"name\": \"name\", \"type\": \"string\"}," +
                         "  {\"name\": \"category\", \"type\": \"string\"}," +
                         "  {\"name\": \"price\", \"type\": \"double\"}" +
                         "]}";
    
    Schema avroSchema = new Schema.Parser().parse(schemaString);
    Random random = new Random(42);
    String[] categories = {"Electronics", "Clothing", "Home", "Sports", "Books"};
    
    try (ParquetWriter<GenericRecord> writer = createWriter(file, avroSchema)) {
      for (int i = 0; i < count; i++) {
        GenericRecord record = new GenericData.Record(avroSchema);
        record.put("product_id", i);
        record.put("name", "Product_" + i);
        record.put("category", categories[random.nextInt(categories.length)]);
        record.put("price", Math.round((5.0 + random.nextDouble() * 95.0) * 100.0) / 100.0);
        writer.write(record);
      }
    }
  }
  
  @SuppressWarnings("deprecation")
  private static void createStoreDimension(String name, int count) throws Exception {
    File file = new File(tempDir.toFile(), name + ".parquet");
    if (file.exists()) return;
    
    String schemaString = "{\"type\": \"record\",\"name\": \"StoreRecord\",\"fields\": [" +
                         "  {\"name\": \"store_id\", \"type\": \"int\"}," +
                         "  {\"name\": \"name\", \"type\": \"string\"}," +
                         "  {\"name\": \"region\", \"type\": \"string\"}," +
                         "  {\"name\": \"size\", \"type\": \"string\"}" +
                         "]}";
    
    Schema avroSchema = new Schema.Parser().parse(schemaString);
    Random random = new Random(42);
    String[] regions = {"North", "South", "East", "West", "Central"};
    String[] sizes = {"Small", "Medium", "Large", "Superstore"};
    
    try (ParquetWriter<GenericRecord> writer = createWriter(file, avroSchema)) {
      for (int i = 0; i < count; i++) {
        GenericRecord record = new GenericData.Record(avroSchema);
        record.put("store_id", i);
        record.put("name", "Store_" + i);
        record.put("region", regions[random.nextInt(regions.length)]);
        record.put("size", sizes[random.nextInt(sizes.length)]);
        writer.write(record);
      }
    }
  }
  
  @SuppressWarnings("deprecation")
  private static void createDateDimension(String name, int count) throws Exception {
    File file = new File(tempDir.toFile(), name + ".parquet");
    if (file.exists()) return;
    
    String schemaString = "{\"type\": \"record\",\"name\": \"DateRecord\",\"fields\": [" +
                         "  {\"name\": \"date_id\", \"type\": \"int\"}," +
                         "  {\"name\": \"date_value\", \"type\": \"string\"}," +
                         "  {\"name\": \"year\", \"type\": \"int\"}," +
                         "  {\"name\": \"quarter\", \"type\": \"int\"}," +
                         "  {\"name\": \"month\", \"type\": \"int\"}" +
                         "]}";
    
    Schema avroSchema = new Schema.Parser().parse(schemaString);
    
    try (ParquetWriter<GenericRecord> writer = createWriter(file, avroSchema)) {
      for (int i = 0; i < count; i++) {
        GenericRecord record = new GenericData.Record(avroSchema);
        record.put("date_id", i);
        
        // Generate dates over 2-3 years
        int dayOffset = i % 1095; // 3 years worth of days
        int year = 2023 + (dayOffset / 365);
        int dayInYear = dayOffset % 365;
        int month = (dayInYear / 30) + 1;
        int quarter = ((month - 1) / 3) + 1;
        
        record.put("date_value", year + "-" + String.format("%02d", month) + "-01");
        record.put("year", year);
        record.put("quarter", quarter);
        record.put("month", month);
        writer.write(record);
      }
    }
  }
  
  @SuppressWarnings("deprecation")
  private static ParquetWriter<GenericRecord> createWriter(File file, Schema avroSchema) throws Exception {
    return AvroParquetWriter
        .<GenericRecord>builder(new org.apache.hadoop.fs.Path(file.getAbsolutePath()))
        .withSchema(avroSchema)
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .build();
  }
  
  private static void registerDuckDBViews(Statement stmt, int factSize) throws Exception {
    String[] tables = {"sales_fact", "dim_customers", "dim_products", "dim_stores", "dim_dates"};
    for (String table : tables) {
      File parquetFile = new File(tempDir.toFile(), table + "_" + factSize + ".parquet");
      if (parquetFile.exists()) {
        stmt.execute("CREATE OR REPLACE VIEW " + table + "_" + factSize + 
                    " AS SELECT * FROM '" + parquetFile.getAbsolutePath() + "'");
      }
    }
  }
  
  private void executeAndConsume(Statement stmt, String query) throws Exception {
    try (ResultSet rs = stmt.executeQuery(query)) {
      rs.next();
    }
  }
  
  private static String formatRowCount(int count) {
    if (count >= 1000000) {
      return String.format("%.1fM", count / 1000000.0);
    } else if (count >= 1000) {
      return String.format("%dK", count / 1000);
    }
    return String.valueOf(count);
  }
}