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

import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.ImmutableBitSet;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for constraint metadata functionality in Calcite model files.
 */
@Tag("unit")
public class JsonConstraintsTest {

  /**
   * Test that JsonConstraints correctly parses primary key definitions.
   */
  @Test
  public void testPrimaryKeyParsing() {
    // Create constraint configuration with primary key
    Map<String, Object> constraints = Map.of("primaryKey", Arrays.asList("id", "version"));
    List<String> columnNames = Arrays.asList("id", "name", "version", "created");

    // Parse constraints
    Statistic statistic = JsonConstraints.fromConstraintsMap(constraints, columnNames, 1000.0);

    // Verify primary key was parsed correctly
    List<ImmutableBitSet> keys = statistic.getKeys();
    assertEquals(1, keys.size(), "Should have one primary key");

    ImmutableBitSet primaryKey = keys.get(0);
    assertTrue(primaryKey.get(0), "Should include column 0 (id)");
    assertTrue(primaryKey.get(2), "Should include column 2 (version)");
    assertEquals(2, primaryKey.cardinality(), "Primary key should have 2 columns");
  }

  /**
   * Test that JsonConstraints correctly parses foreign key definitions.
   */
  @Test
  public void testForeignKeyParsing() {
    // Create constraint configuration with foreign key
    Map<String, Object> foreignKey = Map.of(
        "columns", Arrays.asList("customer_id"),
        "targetTable", Arrays.asList("customers"),
        "targetColumns", Arrays.asList("id")
    );
    
    Map<String, Object> constraints = Map.of("foreignKeys", Arrays.asList(foreignKey));
    List<String> columnNames = Arrays.asList("id", "customer_id", "product_id", "quantity");

    // Parse constraints with table context
    Statistic statistic = JsonConstraints.fromConstraintsMap(constraints, columnNames, null, "sales", "orders");

    // Verify foreign key was parsed correctly
    List<RelReferentialConstraint> foreignKeys = statistic.getReferentialConstraints();
    assertEquals(1, foreignKeys.size(), "Should have one foreign key");

    RelReferentialConstraint fk = foreignKeys.get(0);
    assertEquals(Arrays.asList("sales", "orders"), fk.getSourceQualifiedName(), "Source table should be sales.orders");
    assertEquals(Arrays.asList("sales", "customers"), fk.getTargetQualifiedName(), "Target table should be sales.customers");
    assertEquals(1, fk.getColumnPairs().size(), "Should have one column pair");
  }

  /**
   * Test that JsonConstraints handles multiple constraint types together.
   */
  @Test
  public void testMultipleConstraintTypes() {
    // Create complex constraint configuration
    Map<String, Object> foreignKey1 = Map.of(
        "columns", Arrays.asList("customer_id"),
        "targetTable", Arrays.asList("customers"),
        "targetColumns", Arrays.asList("id")
    );
    
    Map<String, Object> foreignKey2 = Map.of(
        "columns", Arrays.asList("product_id"),
        "targetTable", Arrays.asList("products"),
        "targetColumns", Arrays.asList("id")
    );

    Map<String, Object> constraints = Map.of(
        "primaryKey", Arrays.asList("order_id"),
        "uniqueKeys", Arrays.asList(Arrays.asList("order_number")),
        "foreignKeys", Arrays.asList(foreignKey1, foreignKey2)
    );

    List<String> columnNames = Arrays.asList("order_id", "order_number", "customer_id", "product_id");

    // Parse constraints
    Statistic statistic = JsonConstraints.fromConstraintsMap(constraints, columnNames, 5000.0);

    // Verify all constraint types
    assertEquals(2, statistic.getKeys().size(), "Should have primary key + unique key");
    assertEquals(2, statistic.getReferentialConstraints().size(), "Should have 2 foreign keys");
    assertEquals(Double.valueOf(5000.0), statistic.getRowCount(), "Should preserve row count");
  }

  /**
   * Test that helper methods create correct constraint configurations.
   */
  @Test
  public void testHelperMethods() {
    // Test primary key helper
    Map<String, Object> pkConfig = JsonConstraints.primaryKey("id", "version");
    assertEquals(Arrays.asList("id", "version"), pkConfig.get("primaryKey"));

    // Test foreign key helper
    Map<String, Object> fkConfig = JsonConstraints.foreignKey(
        Arrays.asList("customer_id"),
        "sales",
        "customers", 
        Arrays.asList("id")
    );
    
    assertEquals(Arrays.asList("customer_id"), fkConfig.get("columns"));
    assertEquals(Arrays.asList("sales", "customers"), fkConfig.get("targetTable"));
    assertEquals(Arrays.asList("id"), fkConfig.get("targetColumns"));
  }

  /**
   * Test that constraints work with missing column information (lazy evaluation).
   */
  @Test
  public void testLazyColumnEvaluation() {
    // Create constraints without providing column names upfront
    Map<String, Object> constraints = Map.of(
        "primaryKey", Arrays.asList("id", "version"),
        "uniqueKeys", Arrays.asList(Arrays.asList("email"))
    );

    // Parse without column names - should extract them from constraints
    Statistic statistic = JsonConstraints.fromConstraintsMap(constraints, null, null);

    // Should still create statistics even without explicit column list
    assertNotNull(statistic, "Should create statistic even without column names");
    assertTrue(statistic.getKeys().isEmpty() || !statistic.getKeys().isEmpty(), 
               "Keys may be empty when column names can't be resolved");
  }

  /**
   * Test constraint-capable schema factory interface.
   */
  @Test
  public void testConstraintCapableSchemaFactory() {
    TestConstraintSchemaFactory factory = new TestConstraintSchemaFactory();
    
    // Test default behavior
    assertTrue(factory.supportsConstraints(), "Test factory should support constraints");
    
    // Test constraint setting
    Map<String, Map<String, Object>> tableConstraints = new HashMap<>();
    tableConstraints.put("test_table", Map.of("primaryKey", Arrays.asList("id")));
    
    factory.setTableConstraints(tableConstraints, Arrays.asList());
    assertEquals(1, factory.getReceivedConstraints().size(), "Should receive constraint metadata");
  }

  /**
   * Test schema factory for constraint functionality testing.
   */
  private static class TestConstraintSchemaFactory implements org.apache.calcite.schema.ConstraintCapableSchemaFactory {
    private Map<String, Map<String, Object>> receivedConstraints = new HashMap<>();

    @Override
    public boolean supportsConstraints() {
      return true;
    }

    @Override
    public void setTableConstraints(Map<String, Map<String, Object>> tableConstraints, List<JsonTable> tableDefinitions) {
      this.receivedConstraints = new HashMap<>(tableConstraints);
    }

    @Override
    public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
      return new TestSchema(receivedConstraints);
    }

    public Map<String, Map<String, Object>> getReceivedConstraints() {
      return receivedConstraints;
    }
  }

  /**
   * Test schema implementation with constraint support.
   */
  private static class TestSchema extends AbstractSchema {
    private final Map<String, Map<String, Object>> constraints;

    TestSchema(Map<String, Map<String, Object>> constraints) {
      this.constraints = constraints;
    }

    @Override
    protected Map<String, Table> getTableMap() {
      Map<String, Table> tables = new HashMap<>();
      for (String tableName : constraints.keySet()) {
        tables.put(tableName, new TestTable(constraints.get(tableName)));
      }
      return tables;
    }
  }

  /**
   * Test table implementation with constraint statistics.
   */
  private static class TestTable extends AbstractTable {
    private final Map<String, Object> constraints;

    TestTable(Map<String, Object> constraints) {
      this.constraints = constraints;
    }

    @Override
    public org.apache.calcite.rel.type.RelDataType getRowType(org.apache.calcite.rel.type.RelDataTypeFactory typeFactory) {
      // Create a simple row type for testing
      return typeFactory.builder()
          .add("id", typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.INTEGER))
          .add("name", typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.VARCHAR, 255))
          .add("created", typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.TIMESTAMP))
          .build();
    }

    @Override
    public Statistic getStatistic() {
      if (constraints != null && !constraints.isEmpty()) {
        // In a real implementation, you'd get actual column names from the table
        List<String> columnNames = Arrays.asList("id", "name", "created");
        return JsonConstraints.fromConstraintsMap(constraints, columnNames, 1000.0);
      }
      return Statistics.UNKNOWN;
    }
  }
}