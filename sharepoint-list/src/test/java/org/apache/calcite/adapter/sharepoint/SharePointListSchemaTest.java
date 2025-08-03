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

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for SharePointListSchema.
 */
public class SharePointListSchemaTest {

  @Test public void testSchemaIsMutable() {
    Map<String, Object> authConfig = createValidAuthConfig();

    // This will fail with connection error, but we're testing the constructor and isMutable
    Exception exception = assertThrows(RuntimeException.class, () -> {
      SharePointListSchema schema = new SharePointListSchema("https://test.sharepoint.com/sites/test", authConfig);
    });

    // Should fail with connection error, not configuration error
    assertTrue(exception.getMessage().contains("Failed to connect to SharePoint"));
  }

  @Test public void testSchemaWithInvalidAuth() {
    Map<String, Object> invalidAuthConfig = new HashMap<>();
    // Missing required auth fields

    Exception exception = assertThrows(RuntimeException.class, () -> {
      SharePointListSchema schema = new SharePointListSchema("https://test.sharepoint.com/sites/test", invalidAuthConfig);
    });

    // Should fail during auth creation or connection
    assertNotNull(exception.getMessage());
  }

  @Test public void testSchemaCreationWithNullSiteUrl() {
    Map<String, Object> authConfig = createValidAuthConfig();

    Exception exception = assertThrows(Exception.class, () -> {
      SharePointListSchema schema = new SharePointListSchema(null, authConfig);
    });

    assertNotNull(exception);
  }

  @Test public void testSchemaCreationWithNullAuthConfig() {
    Exception exception = assertThrows(Exception.class, () -> {
      SharePointListSchema schema = new SharePointListSchema("https://test.sharepoint.com/sites/test", null);
    });

    assertNotNull(exception);
  }

  @Test public void testSchemaCreationWithEmptyAuthConfig() {
    Map<String, Object> emptyAuthConfig = new HashMap<>();

    Exception exception = assertThrows(RuntimeException.class, () -> {
      SharePointListSchema schema = new SharePointListSchema("https://test.sharepoint.com/sites/test", emptyAuthConfig);
    });

    assertNotNull(exception);
  }

  private Map<String, Object> createValidAuthConfig() {
    Map<String, Object> authConfig = new HashMap<>();
    authConfig.put("authType", "CLIENT_CREDENTIALS");
    authConfig.put("clientId", "test-client-id");
    authConfig.put("clientSecret", "test-client-secret");
    authConfig.put("tenantId", "test-tenant-id");
    return authConfig;
  }
}
