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
package org.apache.calcite.adapter.file.debug;

import org.apache.calcite.adapter.file.FileSchemaFactory;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import org.junit.jupiter.api.BeforeEach;
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
 * Debug test to examine exact node types in query plans to understand rule matching issues.
 */
public class NodeTypeDebugTest {
  
  @TempDir
  java.nio.file.Path tempDir;
  
  private Connection calciteConn;
  
  @BeforeEach
  public void setUp() throws Exception {
    // Enable all optimizations for testing
    System.setProperty("calcite.file.statistics.filter.enabled", "true"); 
    System.setProperty("calcite.file.statistics.column.pruning.enabled", "true");
    
    createTestData();
    setupCalciteConnection();
  }
  
  @Test
  public void testNodeTypesInQueryPlans() throws Exception {
    System.out.println("\n╔═══════════════════════════════════════════════════════════════════════════════════╗");
    System.out.println("║                            NODE TYPE ANALYSIS                                        ║");
    System.out.println("║                    Understanding exact node types in plans                          ║");
    System.out.println("╚═══════════════════════════════════════════════════════════════════════════════════╝\n");
    
    // Test various query types to see exact node structures
    testFilterQuery();
    testProjectionQuery();
    testJoinQuery();
    testCountQuery();
  }
  
  private void testFilterQuery() throws Exception {
    System.out.println("FILTER Query Analysis:");
    System.out.println("======================");
    String query = "SELECT * FROM FILES.\"test_table\" WHERE \"amount\" > 500";
    analyzeQueryPlan(query);
  }
  
  private void testProjectionQuery() throws Exception {
    System.out.println("PROJECTION Query Analysis:");
    System.out.println("==========================");  
    String query = "SELECT \"order_id\", \"amount\" FROM FILES.\"test_table\"";
    analyzeQueryPlan(query);
  }
  
  private void testJoinQuery() throws Exception {
    System.out.println("JOIN Query Analysis:");
    System.out.println("===================");
    String query = "SELECT * FROM FILES.\"orders\" o JOIN FILES.\"customers\" c ON o.\"customer_id\" = c.\"customer_id\"";
    analyzeQueryPlan(query);
  }
  
  private void testCountQuery() throws Exception {
    System.out.println("COUNT Query Analysis:");
    System.out.println("====================");
    String query = "SELECT COUNT(*) FROM FILES.\"test_table\"";
    analyzeQueryPlan(query);
  }
  
  private void analyzeQueryPlan(String query) throws Exception {
    System.out.println("Query: " + query);
    
    try (Statement stmt = calciteConn.createStatement()) {
      // Get execution plan
      ResultSet rs = stmt.executeQuery("EXPLAIN PLAN FOR " + query);
      StringBuilder plan = new StringBuilder();
      while (rs.next()) {
        String line = rs.getString(1);
        plan.append(line).append("\\n");
        
        // Extract node type from each line
        if (line.trim().length() > 0) {
          String nodeType = extractNodeType(line.trim());
          System.out.println("  Node: " + nodeType);
        }
      }
      rs.close();
      
      System.out.println("\\nFull plan:");
      System.out.println(plan.toString());
      System.out.println();
    }
  }
  
  private String extractNodeType(String planLine) {
    // Extract the first word before any parenthesis
    if (planLine.contains("(")) {
      return planLine.substring(0, planLine.indexOf("("));
    }
    return planLine.split("\\\\s+")[0];
  }
  
  @SuppressWarnings("deprecation") 
  private void createTestData() throws Exception {
    createTable("test_table", 100, new String[]{"order_id", "customer_id", "amount"});
    createTable("orders", 50, new String[]{"order_id", "customer_id", "amount"});
    createTable("customers", 20, new String[]{"customer_id", "customer_name"});
  }
  
  @SuppressWarnings("deprecation")
  private void createTable(String tableName, int rows, String[] fields) throws Exception {
    File file = new File(tempDir.toFile(), tableName + ".parquet");
    
    StringBuilder schemaBuilder = new StringBuilder();
    schemaBuilder.append("{\"type\": \"record\",\"name\": \"TestRecord\",\"fields\": [");
    for (int i = 0; i < fields.length; i++) {
      if (i > 0) schemaBuilder.append(",");
      String type = fields[i].contains("name") ? "string" : 
                   fields[i].equals("amount") ? "double" : "int";
      schemaBuilder.append("{\"name\": \"").append(fields[i]).append("\", \"type\": \"").append(type).append("\"}");
    }
    schemaBuilder.append("]}");
    
    Schema avroSchema = new Schema.Parser().parse(schemaBuilder.toString());
    Random random = new Random(42);
    
    try (ParquetWriter<GenericRecord> writer = 
         AvroParquetWriter
             .<GenericRecord>builder(new org.apache.hadoop.fs.Path(file.getAbsolutePath()))
             .withSchema(avroSchema)
             .withCompressionCodec(CompressionCodecName.SNAPPY)
             .build()) {
      
      for (int i = 0; i < rows; i++) {
        GenericRecord record = new GenericData.Record(avroSchema);
        for (String field : fields) {
          if (field.equals("order_id")) {
            record.put(field, i);
          } else if (field.equals("customer_id")) {
            record.put(field, i % 20);
          } else if (field.equals("amount")) {
            record.put(field, 100.0 + random.nextDouble() * 900.0);
          } else if (field.equals("customer_name")) {
            record.put(field, "Customer_" + i);
          }
        }
        writer.write(record);
      }
    }
  }
  
  private void setupCalciteConnection() throws Exception {
    calciteConn = DriverManager.getConnection("jdbc:calcite:");
    CalciteConnection calciteConnection = calciteConn.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();
    
    Map<String, Object> operand = new LinkedHashMap<>();
    operand.put("directory", tempDir.toString());
    operand.put("executionEngine", "parquet");
    
    rootSchema.add("FILES", FileSchemaFactory.INSTANCE.create(rootSchema, "FILES", operand));
  }
}