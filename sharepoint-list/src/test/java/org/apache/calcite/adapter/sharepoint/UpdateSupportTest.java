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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for UPDATE support in SharePoint List adapter.
 */
public class UpdateSupportTest {

  @Test public void testModifiableCollectionSupportsUpdate() {
    // Create mock metadata
    List<SharePointColumn> columns = new ArrayList<>();
    columns.add(new SharePointColumn("Title", "Title", "Text", false));
    columns.add(new SharePointColumn("Description", "Description", "Note", false));
    columns.add(new SharePointColumn("Priority", "Priority", "Number", false));

    SharePointListMetadata metadata =
        new SharePointListMetadata("test-list-id", "Test List", "TestListEntity", columns);

    // Create mock client
    MockMicrosoftGraphListClient client = new MockMicrosoftGraphListClient();

    // Create table with modifiable collection
    SharePointListTable table = new SharePointListTable(metadata, client);
    Object collection = table.getModifiableCollection();

    assertNotNull(collection);
    assertTrue(collection instanceof ArrayList);

    @SuppressWarnings("unchecked")
    ArrayList<Object[]> modifiableCollection = (ArrayList<Object[]>) collection;

    // Test that we can call set method (UPDATE operation)
    Object[] testRow = new Object[]{"123", "Updated Title", "Updated Description", 2};

    // This should not throw an exception
    assertDoesNotThrow(() -> {
      // Add a row first
      modifiableCollection.add(new Object[]{"123", "Original Title", "Original Description", 1});
      // Then update it
      modifiableCollection.set(0, testRow);
    });
  }

  @Test public void testUpdateRequiresId() {
    List<SharePointColumn> columns = new ArrayList<>();
    columns.add(new SharePointColumn("Title", "Title", "Text", false));

    SharePointListMetadata metadata =
        new SharePointListMetadata("test-list-id", "Test List", "TestListEntity", columns);

    MockMicrosoftGraphListClient client = new MockMicrosoftGraphListClient();
    SharePointListTable table = new SharePointListTable(metadata, client);

    @SuppressWarnings("unchecked")
    ArrayList<Object[]> collection = (ArrayList<Object[]>) table.getModifiableCollection();

    // Test UPDATE without ID should fail
    Object[] rowWithoutId = new Object[]{null, "Title"};
    assertThrows(IllegalArgumentException.class, () -> {
      collection.set(0, rowWithoutId);
    });

    // Test UPDATE with empty array should fail
    assertThrows(IllegalArgumentException.class, () -> {
      collection.set(0, new Object[]{});
    });
  }

  @Test public void testUpdateConvertsFieldsCorrectly() {
    List<SharePointColumn> columns = new ArrayList<>();
    columns.add(new SharePointColumn("Title", "Title", "Text", false));
    columns.add(new SharePointColumn("Priority", "Priority", "Number", false));
    columns.add(new SharePointColumn("IsComplete", "IsComplete", "Boolean", false));

    SharePointListMetadata metadata =
        new SharePointListMetadata("test-list-id", "Test List", "TestListEntity", columns);

    MockMicrosoftGraphListClient client = new MockMicrosoftGraphListClient();
    SharePointListTable table = new SharePointListTable(metadata, client);

    @SuppressWarnings("unchecked")
    ArrayList<Object[]> collection = (ArrayList<Object[]>) table.getModifiableCollection();

    // Add a row first
    collection.add(new Object[]{"123", "Original Title", 1, false});

    // Update with new values including null
    Object[] updatedRow = new Object[]{"123", "Updated Title", null, true};

    assertDoesNotThrow(() -> {
      collection.set(0, updatedRow);
    });

    // Verify the mock client received the update call
    assertTrue(client.updateCalled);
    assertEquals("123", client.lastUpdatedItemId);

    Map<String, Object> expectedFields = client.lastUpdateFields;
    assertNotNull(expectedFields);
    assertEquals("Updated Title", expectedFields.get("Title"));
    assertNull(expectedFields.get("Priority"));
    assertEquals(true, expectedFields.get("IsComplete"));

    // ID should not be in the fields map
    assertFalse(expectedFields.containsKey("Id"));
  }

  /**
   * Mock implementation of MicrosoftGraphListClient for testing.
   */
  private static class MockMicrosoftGraphListClient extends MicrosoftGraphListClient {

    boolean updateCalled = false;
    String lastUpdatedItemId;
    Map<String, Object> lastUpdateFields;

    public MockMicrosoftGraphListClient() {
      super("https://test.sharepoint.com", new MockSharePointAuth());
    }

    @Override public void updateListItem(String listId, String itemId, Map<String, Object> fields)
        throws IOException, InterruptedException {
      updateCalled = true;
      lastUpdatedItemId = itemId;
      lastUpdateFields = new HashMap<>(fields);
    }

    @Override public String createListItem(String listId, Map<String, Object> fields)
        throws IOException, InterruptedException {
      return "mock-id-" + System.currentTimeMillis();
    }

    @Override public void deleteListItem(String listId, String itemId)
        throws IOException, InterruptedException {
      // Mock implementation
    }

    @Override public List<Map<String, Object>> getListItems(String listId)
        throws IOException, InterruptedException {
      return new ArrayList<>();
    }

    @Override public Map<String, SharePointListMetadata> getAvailableLists()
        throws IOException, InterruptedException {
      return new HashMap<>();
    }
  }

  /**
   * Mock SharePoint authentication for testing.
   */
  private static class MockSharePointAuth implements org.apache.calcite.adapter.sharepoint.auth.SharePointAuth {

    @Override public String getAccessToken() throws IOException, InterruptedException {
      return "mock-token";
    }
  }
}
