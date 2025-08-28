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

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for SharePoint attachment functions.
 * These tests verify the function signatures and method availability.
 */
public class AttachmentFunctionsTest {

  private DataContext dataContext;

  @BeforeEach
  public void setUp() {
    // Create a simple mock data context for testing
    dataContext = new DataContext() {
      @Override public org.apache.calcite.schema.SchemaPlus getRootSchema() {
        return null;
      }

      @Override public Object get(String name) {
        return null;
      }

      @Override public org.apache.calcite.linq4j.QueryProvider getQueryProvider() {
        return null;
      }

      @Override public org.apache.calcite.adapter.java.JavaTypeFactory getTypeFactory() {
        return null;
      }
    };
  }

  @Test public void testGetAttachmentsMethodExists() {
    // Test that the getAttachments method exists with correct signature
    assertDoesNotThrow(() -> {
      Method method =
          SharePointAttachmentFunctions.class.getMethod("getAttachments", DataContext.class, String.class, String.class);
      assertNotNull(method);
      assertEquals(Enumerable.class, method.getReturnType());
    });
  }

  @Test public void testAddAttachmentMethodExists() {
    // Test that the addAttachment method exists with correct signature
    assertDoesNotThrow(() -> {
      Method method =
          SharePointAttachmentFunctions.class.getMethod("addAttachment", DataContext.class, String.class, String.class, String.class, byte[].class);
      assertNotNull(method);
      assertEquals(boolean.class, method.getReturnType());
    });
  }

  @Test public void testDeleteAttachmentMethodExists() {
    // Test that the deleteAttachment method exists with correct signature
    assertDoesNotThrow(() -> {
      Method method =
          SharePointAttachmentFunctions.class.getMethod("deleteAttachment", DataContext.class, String.class, String.class, String.class);
      assertNotNull(method);
      assertEquals(boolean.class, method.getReturnType());
    });
  }

  @Test public void testGetAttachmentContentMethodExists() {
    // Test that the getAttachmentContent method exists with correct signature
    assertDoesNotThrow(() -> {
      Method method =
          SharePointAttachmentFunctions.class.getMethod("getAttachmentContent", DataContext.class, String.class, String.class, String.class);
      assertNotNull(method);
      assertEquals(byte[].class, method.getReturnType());
    });
  }

  @Test public void testCountAttachmentsMethodExists() {
    // Test that the countAttachments method exists with correct signature
    assertDoesNotThrow(() -> {
      Method method =
          SharePointAttachmentFunctions.class.getMethod("countAttachments", DataContext.class, String.class, String.class);
      assertNotNull(method);
      assertEquals(int.class, method.getReturnType());
    });
  }

  @Test public void testMethodsArePublicAndStatic() {
    // Verify all attachment methods are public and static (required for SQL functions)
    Method[] methods = SharePointAttachmentFunctions.class.getDeclaredMethods();

    for (Method method : methods) {
      if (method.getName().equals("getAttachments") ||
          method.getName().equals("addAttachment") ||
          method.getName().equals("deleteAttachment") ||
          method.getName().equals("getAttachmentContent") ||
          method.getName().equals("countAttachments")) {

        assertTrue(java.lang.reflect.Modifier.isPublic(method.getModifiers()),
            "Method " + method.getName() + " should be public");
        assertTrue(java.lang.reflect.Modifier.isStatic(method.getModifiers()),
            "Method " + method.getName() + " should be static");
      }
    }
  }

  @Test public void testBasicMethodInvocation() {
    // Test that methods can be called without throwing compilation errors
    // We don't test functionality here, just that the signatures work

    try {
      // These calls may fail at runtime due to missing schema, but they should compile
      SharePointAttachmentFunctions.getAttachments(dataContext, "TestList", "123");
    } catch (Exception e) {
      // Expected - method exists and is callable
    }

    try {
      byte[] content = "test".getBytes(StandardCharsets.UTF_8);
      SharePointAttachmentFunctions.addAttachment(dataContext, "TestList", "123", "test.txt", content);
    } catch (Exception e) {
      // Expected - method exists and is callable
    }

    try {
      SharePointAttachmentFunctions.deleteAttachment(dataContext, "TestList", "123", "test.txt");
    } catch (Exception e) {
      // Expected - method exists and is callable
    }

    try {
      SharePointAttachmentFunctions.getAttachmentContent(dataContext, "TestList", "123", "test.txt");
    } catch (Exception e) {
      // Expected - method exists and is callable
    }

    // Count attachments should not throw (returns 0 on error)
    int count = SharePointAttachmentFunctions.countAttachments(dataContext, "TestList", "123");
    assertTrue(count >= 0, "Count should be non-negative");
  }
}
