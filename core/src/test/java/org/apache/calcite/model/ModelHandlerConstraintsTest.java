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
package org.apache.calcite.model;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.ConstraintCapableSchemaFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for ModelHandler constraint processing functionality.
 */
@Tag("unit")
public class ModelHandlerConstraintsTest {

  /**
   * Test that ModelHandler properly extracts and passes constraint metadata 
   * to constraint-capable schema factories.
   */
  @Test
  public void testModelHandlerConstraintExtraction() throws Exception {
    // Create a model with constraint definitions
    String modelJson = "{\n" +
        "  \"version\": \"1.0\",\n" +
        "  \"defaultSchema\": \"TEST\",\n" +
        "  \"schemas\": [{\n" +
        "    \"name\": \"TEST\",\n" +
        "    \"type\": \"custom\",\n" +
        "    \"factory\": \"org.apache.calcite.model.ModelHandlerConstraintsTest$TestConstraintSchemaFactory\",\n" +
        "    \"tables\": [{\n" +
        "      \"name\": \"customers\",\n" +
        "      \"type\": \"custom\",\n" +
        "      \"factory\": \"org.apache.calcite.model.ModelHandlerConstraintsTest$TestTableFactory\",\n" +
        "      \"constraints\": {\n" +
        "        \"primaryKey\": [\"customer_id\"],\n" +
        "        \"uniqueKeys\": [[\"email\"]]\n" +
        "      }\n" +
        "    }, {\n" +
        "      \"name\": \"orders\",\n" +
        "      \"type\": \"custom\",\n" +
        "      \"factory\": \"org.apache.calcite.model.ModelHandlerConstraintsTest$TestTableFactory\",\n" +
        "      \"constraints\": {\n" +
        "        \"primaryKey\": [\"order_id\"],\n" +
        "        \"foreignKeys\": [{\n" +
        "          \"columns\": [\"customer_id\"],\n" +
        "          \"targetTable\": [\"customers\"],\n" +
        "          \"targetColumns\": [\"customer_id\"]\n" +
        "        }]\n" +
        "      }\n" +
        "    }]\n" +
        "  }]\n" +
        "}";

    // Create temporary model file
    Path tempModel = Files.createTempFile("model-constraints-test", ".json");
    Files.write(tempModel, modelJson.getBytes());

    try {
      // Clear static test state
      TestConstraintSchemaFactory.lastReceivedConstraints = null;
      TestConstraintSchemaFactory.lastReceivedTables = null;

      // Create connection using the model
      String jdbcUrl = "jdbc:calcite:model=" + tempModel.toString();
      Connection connection = DriverManager.getConnection(jdbcUrl);
      CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);

      // Verify schema was created
      SchemaPlus rootSchema = calciteConnection.getRootSchema();
      Schema testSchema = rootSchema.getSubSchema("TEST");
      assertNotNull(testSchema, "TEST schema should be created");

      // Verify constraint metadata was passed to factory
      assertNotNull(TestConstraintSchemaFactory.lastReceivedConstraints, 
                   "Factory should have received constraint metadata");
      assertNotNull(TestConstraintSchemaFactory.lastReceivedTables, 
                   "Factory should have received table definitions");

      // Verify specific constraint content
      Map<String, Map<String, Object>> constraints = TestConstraintSchemaFactory.lastReceivedConstraints;
      assertEquals(2, constraints.size(), "Should have constraints for 2 tables");
      
      assertTrue(constraints.containsKey("customers"), "Should have constraints for customers table");
      assertTrue(constraints.containsKey("orders"), "Should have constraints for orders table");

      Map<String, Object> customerConstraints = constraints.get("customers");
      assertEquals(List.of("customer_id"), customerConstraints.get("primaryKey"), 
                  "Customers primary key should be customer_id");
      assertEquals(List.of(List.of("email")), customerConstraints.get("uniqueKeys"), 
                  "Customers should have unique constraint on email");

      Map<String, Object> orderConstraints = constraints.get("orders");
      assertEquals(List.of("order_id"), orderConstraints.get("primaryKey"), 
                  "Orders primary key should be order_id");
      assertNotNull(orderConstraints.get("foreignKeys"), "Orders should have foreign key constraints");

      connection.close();
    } finally {
      Files.deleteIfExists(tempModel);
    }
  }

  /**
   * Test that ModelHandler handles schemas without constraints properly.
   */
  @Test
  public void testModelHandlerWithoutConstraints() throws Exception {
    // Create a model WITHOUT constraint definitions
    String modelJson = "{\n" +
        "  \"version\": \"1.0\",\n" +
        "  \"defaultSchema\": \"TEST\",\n" +
        "  \"schemas\": [{\n" +
        "    \"name\": \"TEST\",\n" +
        "    \"type\": \"custom\",\n" +
        "    \"factory\": \"org.apache.calcite.model.ModelHandlerConstraintsTest$TestConstraintSchemaFactory\"\n" +
        "  }]\n" +
        "}";

    // Create temporary model file
    Path tempModel = Files.createTempFile("model-no-constraints-test", ".json");
    Files.write(tempModel, modelJson.getBytes());

    try {
      // Clear static test state
      TestConstraintSchemaFactory.lastReceivedConstraints = null;
      TestConstraintSchemaFactory.lastReceivedTables = null;

      // Create connection using the model
      String jdbcUrl = "jdbc:calcite:model=" + tempModel.toString();
      Connection connection = DriverManager.getConnection(jdbcUrl);
      CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);

      // Verify schema was created
      SchemaPlus rootSchema = calciteConnection.getRootSchema();
      Schema testSchema = rootSchema.getSubSchema("TEST");
      assertNotNull(testSchema, "TEST schema should be created");

      // Verify empty constraints were passed to factory
      assertNotNull(TestConstraintSchemaFactory.lastReceivedConstraints, 
                   "Factory should have received constraint metadata (even if empty)");
      assertTrue(TestConstraintSchemaFactory.lastReceivedConstraints.isEmpty(), 
                "Constraint metadata should be empty when no constraints are defined");

      connection.close();
    } finally {
      Files.deleteIfExists(tempModel);
    }
  }

  /**
   * Test schema factory that captures constraint metadata for testing.
   */
  public static class TestConstraintSchemaFactory implements ConstraintCapableSchemaFactory {
    // Static fields to capture test data across instances
    public static Map<String, Map<String, Object>> lastReceivedConstraints;
    public static List<JsonTable> lastReceivedTables;

    @Override
    public boolean supportsConstraints() {
      return true;
    }

    @Override
    public void setTableConstraints(Map<String, Map<String, Object>> tableConstraints, 
                                   List<JsonTable> tableDefinitions) {
      lastReceivedConstraints = new HashMap<>(tableConstraints);
      lastReceivedTables = List.copyOf(tableDefinitions);
    }

    @Override
    public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
      // Return a simple schema for testing
      return new TestSchema();
    }
  }

  /**
   * Simple test schema implementation.
   */
  private static class TestSchema extends AbstractSchema {
    // Empty implementation for testing
  }

  /**
   * Test table factory for creating test tables.
   */
  public static class TestTableFactory implements org.apache.calcite.schema.TableFactory {
    @Override
    public org.apache.calcite.schema.Table create(
        org.apache.calcite.schema.SchemaPlus schema, 
        String name, 
        Map<String, Object> operand, 
        org.apache.calcite.rel.type.@org.checkerframework.checker.nullness.qual.Nullable RelDataType rowType) {
      return new TestTable();
    }
  }

  /**
   * Simple test table implementation.
   */
  private static class TestTable extends org.apache.calcite.schema.impl.AbstractTable {
    @Override
    public org.apache.calcite.rel.type.RelDataType getRowType(org.apache.calcite.rel.type.RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          .add("id", typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.INTEGER))
          .add("name", typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.VARCHAR, 255))
          .build();
    }
  }
}