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
package org.apache.calcite.adapter.sharepoint;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for SharePointListTableFactory.
 */
public class SharePointListTableFactoryTest {

  @Test public void testCreateWithValidConfig() {
    SharePointListTableFactory factory = SharePointListTableFactory.INSTANCE;
    SchemaPlus schema = null;
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);
    RelDataType rowType = typeFactory.createSqlType(SqlTypeName.VARCHAR);

    Map<String, Object> operand = new HashMap<>();
    operand.put("siteUrl", "https://test.sharepoint.com/sites/test");
    operand.put("listName", "TestList");
    operand.put("authType", "CLIENT_CREDENTIALS");
    operand.put("clientId", "test-client-id");
    operand.put("clientSecret", "test-client-secret");
    operand.put("tenantId", "test-tenant-id");

    // This will fail with authentication error, but we're testing the factory logic
    Exception exception = assertThrows(RuntimeException.class, () -> {
      Table table = factory.create(schema, "test_table", operand, rowType);
    });

    // Should fail with connection or auth error, not configuration error
    assertTrue(exception.getMessage().contains("Failed to create SharePoint table"));
  }

  @Test public void testCreateWithMissingSiteUrl() {
    SharePointListTableFactory factory = SharePointListTableFactory.INSTANCE;
    SchemaPlus schema = null;
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);
    RelDataType rowType = typeFactory.createSqlType(SqlTypeName.VARCHAR);

    Map<String, Object> operand = new HashMap<>();
    operand.put("listName", "TestList");
    operand.put("authType", "CLIENT_CREDENTIALS");
    operand.put("clientId", "test-client-id");
    operand.put("clientSecret", "test-client-secret");
    operand.put("tenantId", "test-tenant-id");

    Exception exception = assertThrows(RuntimeException.class, () -> {
      factory.create(schema, "test_table", operand, rowType);
    });

    assertTrue(exception.getMessage().contains("siteUrl is required"));
  }

  @Test public void testCreateWithMissingListName() {
    SharePointListTableFactory factory = SharePointListTableFactory.INSTANCE;
    SchemaPlus schema = null;
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);
    RelDataType rowType = typeFactory.createSqlType(SqlTypeName.VARCHAR);

    Map<String, Object> operand = new HashMap<>();
    operand.put("siteUrl", "https://test.sharepoint.com/sites/test");
    operand.put("authType", "CLIENT_CREDENTIALS");
    operand.put("clientId", "test-client-id");
    operand.put("clientSecret", "test-client-secret");
    operand.put("tenantId", "test-tenant-id");

    Exception exception = assertThrows(RuntimeException.class, () -> {
      factory.create(schema, "test_table", operand, rowType);
    });

    assertTrue(exception.getMessage().contains("Either listId or listName is required"));
  }

  @Test public void testCreateWithNullOperand() {
    SharePointListTableFactory factory = SharePointListTableFactory.INSTANCE;
    SchemaPlus schema = null;
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);
    RelDataType rowType = typeFactory.createSqlType(SqlTypeName.VARCHAR);

    Exception exception = assertThrows(Exception.class, () -> {
      factory.create(schema, "test_table", null, rowType);
    });

    // Will throw NullPointerException when trying to access operand.get()
    assertNotNull(exception);
  }

  @Test public void testCreateWithEmptyOperand() {
    SharePointListTableFactory factory = SharePointListTableFactory.INSTANCE;
    SchemaPlus schema = null;
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);
    RelDataType rowType = typeFactory.createSqlType(SqlTypeName.VARCHAR);

    Map<String, Object> operand = new HashMap<>();

    Exception exception = assertThrows(RuntimeException.class, () -> {
      factory.create(schema, "test_table", operand, rowType);
    });

    assertTrue(exception.getMessage().contains("siteUrl is required"));
  }

  @Test public void testCreateWithMissingAuthConfig() {
    SharePointListTableFactory factory = SharePointListTableFactory.INSTANCE;
    SchemaPlus schema = null;
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);
    RelDataType rowType = typeFactory.createSqlType(SqlTypeName.VARCHAR);

    Map<String, Object> operand = new HashMap<>();
    operand.put("siteUrl", "https://test.sharepoint.com/sites/test");
    operand.put("listName", "TestList");
    // Missing auth configuration

    Exception exception = assertThrows(RuntimeException.class, () -> {
      factory.create(schema, "test_table", operand, rowType);
    });

    // Should fail during auth setup
    assertNotNull(exception.getMessage());
  }
}
