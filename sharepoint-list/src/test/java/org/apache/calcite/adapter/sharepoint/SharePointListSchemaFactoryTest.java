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

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for SharePointListSchemaFactory.
 */
public class SharePointListSchemaFactoryTest {

  @Test public void testCreateWithValidConfig() {
    SharePointListSchemaFactory factory = SharePointListSchemaFactory.INSTANCE;
    SchemaPlus parentSchema = null; // Not actually used by the factory

    Map<String, Object> operand = new HashMap<>();
    operand.put("siteUrl", "https://test.sharepoint.com/sites/test");
    operand.put("authType", "CLIENT_CREDENTIALS");
    operand.put("clientId", "test-client-id");
    operand.put("clientSecret", "test-client-secret");
    operand.put("tenantId", "test-tenant-id");

    // This will fail with authentication error, but we're testing the factory logic
    Exception exception = assertThrows(RuntimeException.class, () -> {
      Schema schema = factory.create(parentSchema, "sharepoint", operand);
    });

    // Should fail with connection error, not configuration error
    assertTrue(exception.getMessage().contains("Failed to connect to SharePoint"));
  }

  @Test public void testCreateWithMissingSiteUrl() {
    SharePointListSchemaFactory factory = SharePointListSchemaFactory.INSTANCE;
    SchemaPlus parentSchema = null;

    Map<String, Object> operand = new HashMap<>();
    operand.put("authType", "CLIENT_CREDENTIALS");
    operand.put("clientId", "test-client-id");
    operand.put("clientSecret", "test-client-secret");
    operand.put("tenantId", "test-tenant-id");

    Exception exception = assertThrows(RuntimeException.class, () -> {
      factory.create(parentSchema, "sharepoint", operand);
    });

    assertTrue(exception.getMessage().contains("siteUrl is required"));
  }

  @Test public void testCreateWithNullOperand() {
    SharePointListSchemaFactory factory = SharePointListSchemaFactory.INSTANCE;
    SchemaPlus parentSchema = null;

    Exception exception = assertThrows(Exception.class, () -> {
      factory.create(parentSchema, "sharepoint", null);
    });

    // Will throw NullPointerException when trying to access operand.get()
    assertNotNull(exception);
  }

  @Test public void testCreateWithEmptyOperand() {
    SharePointListSchemaFactory factory = SharePointListSchemaFactory.INSTANCE;
    SchemaPlus parentSchema = null;

    Map<String, Object> operand = new HashMap<>();

    Exception exception = assertThrows(RuntimeException.class, () -> {
      factory.create(parentSchema, "sharepoint", operand);
    });

    assertTrue(exception.getMessage().contains("siteUrl is required"));
  }

  @Test public void testCreateWithInvalidSiteUrl() {
    SharePointListSchemaFactory factory = SharePointListSchemaFactory.INSTANCE;
    SchemaPlus parentSchema = null;

    Map<String, Object> operand = new HashMap<>();
    operand.put("siteUrl", "invalid-url");
    operand.put("authType", "CLIENT_CREDENTIALS");
    operand.put("clientId", "test-client-id");
    operand.put("clientSecret", "test-client-secret");
    operand.put("tenantId", "test-tenant-id");

    Exception exception = assertThrows(RuntimeException.class, () -> {
      factory.create(parentSchema, "sharepoint", operand);
    });

    // Should fail with connection or URL parsing error
    assertNotNull(exception.getMessage());
  }
}
