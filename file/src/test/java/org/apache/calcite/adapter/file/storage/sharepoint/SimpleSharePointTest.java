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
package org.apache.calcite.adapter.file.storage.sharepoint;

import org.apache.calcite.adapter.file.FileSchema;
import org.apache.calcite.adapter.file.FileSchemaFactory;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Tag;
/**
 * Simple test to debug FileSchema table discovery.
 */
@Tag("integration")public class SimpleSharePointTest {

  @Test
  void testFileSchemaTableDiscovery() throws Exception {
    System.out.println("=== Simple FileSchema Test ===");

    // Create a connection to get the root schema
    java.util.Properties info = new java.util.Properties();
    info.setProperty("lex", "ORACLE");
    info.put("unquotedCasing", "TO_LOWER");

    try (java.sql.Connection conn = java.sql.DriverManager.getConnection("jdbc:calcite:", info)) {
      CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class);
      SchemaPlus parentSchema = calciteConn.getRootSchema();

      // Create FileSchema with the simple configuration
      Map<String, Object> operand = new HashMap<>();
      operand.put("directory", "/tmp");  // Use a simple directory that exists

    System.out.println("Creating FileSchema with directory: /tmp");
    FileSchemaFactory factory = FileSchemaFactory.INSTANCE;
    Schema fileSchema = factory.create(parentSchema, "TEST", operand);

    System.out.println("FileSchema created: " + fileSchema.getClass().getName());

    // This should trigger getTableMap()
    System.out.println("Calling getTableMap() via reflection...");
    if (fileSchema instanceof FileSchema) {
      FileSchema fs = (FileSchema) fileSchema;
      // Access protected method via reflection
      try {
        java.lang.reflect.Method method = FileSchema.class.getDeclaredMethod("getTableMap");
        method.setAccessible(true);
        Map<String, ?> tables = (Map<String, ?>) method.invoke(fs);
        System.out.println("Tables found: " + tables.size() + " - " + tables.keySet());
      } catch (Exception e) {
        System.err.println("Error accessing getTableMap: " + e.getMessage());
        e.printStackTrace();
      }
    }
    }
  }
}
