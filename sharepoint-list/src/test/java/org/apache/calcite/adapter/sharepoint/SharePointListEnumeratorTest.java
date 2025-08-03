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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for SharePointListEnumerator.
 */
public class SharePointListEnumeratorTest {

  private MockMicrosoftGraphListClient mockClient;
  private SharePointListMetadata mockMetadata;
  private List<SharePointColumn> columns;

  @BeforeEach
  public void setUp() {
    // Create test columns with realistic SharePoint types
    columns = new ArrayList<>();
    columns.add(new SharePointColumn("Title", "title", "Title", false) {
      @Override public String getType() { return "text"; }
    });
    columns.add(new SharePointColumn("NumberField", "number_field", "Number Field", false) {
      @Override public String getType() { return "number"; }
    });
    columns.add(new SharePointColumn("BooleanField", "boolean_field", "Boolean Field", false) {
      @Override public String getType() { return "boolean"; }
    });
    columns.add(new SharePointColumn("DateField", "date_field", "Date Field", false) {
      @Override public String getType() { return "datetime"; }
    });
    columns.add(new SharePointColumn("LookupField", "lookup_field", "Lookup Field", false) {
      @Override public String getType() { return "lookup"; }
    });
    columns.add(new SharePointColumn("MultiChoiceField", "multi_choice_field", "Multi Choice Field", false) {
      @Override public String getType() { return "multichoice"; }
    });

    // Create mock metadata
    mockMetadata = new SharePointListMetadata("test-list-id", "TestList", "Test List", columns);

    // Create mock client
    mockClient = new MockMicrosoftGraphListClient();
  }

  @Test public void testEnumeratorWithValidData() {
    // Set up test data
    List<Map<String, Object>> testData = new ArrayList<>();

    Map<String, Object> item1 = new HashMap<>();
    item1.put("id", "1");
    item1.put("Title", "Test Item 1");
    item1.put("NumberField", 42.5);
    item1.put("BooleanField", true);
    item1.put("DateField", "2023-01-01T00:00:00Z");

    Map<String, Object> lookupValue = new HashMap<>();
    lookupValue.put("Title", "Lookup Value");
    item1.put("LookupField", lookupValue);

    List<String> multiChoice = new ArrayList<>();
    multiChoice.add("Choice1");
    multiChoice.add("Choice2");
    item1.put("MultiChoiceField", multiChoice);

    testData.add(item1);

    Map<String, Object> item2 = new HashMap<>();
    item2.put("id", "2");
    item2.put("Title", "Test Item 2");
    item2.put("NumberField", 100);
    item2.put("BooleanField", false);
    testData.add(item2);

    mockClient.setTestData(testData);

    SharePointListEnumerator enumerator = new SharePointListEnumerator(mockMetadata, mockClient);

    // Test first item
    assertTrue(enumerator.moveNext());
    Object[] row1 = enumerator.current();
    assertNotNull(row1);
    assertEquals(7, row1.length); // 6 columns + ID
    assertEquals("1", row1[0]); // ID
    assertEquals("Test Item 1", row1[1]); // Title
    assertEquals(42.5, row1[2]); // NumberField
    assertEquals(true, row1[3]); // BooleanField
    assertNotNull(row1[4]); // DateField
    assertEquals("Lookup Value", row1[5]); // LookupField
    assertEquals("Choice1, Choice2", row1[6]); // MultiChoiceField

    // Test second item
    assertTrue(enumerator.moveNext());
    Object[] row2 = enumerator.current();
    assertNotNull(row2);
    assertEquals("2", row2[0]); // ID
    assertEquals("Test Item 2", row2[1]); // Title
    assertEquals(100.0, row2[2]); // NumberField (converted to double)
    assertEquals(false, row2[3]); // BooleanField
    assertNull(row2[4]); // DateField (null)

    // No more items
    assertFalse(enumerator.moveNext());

    enumerator.close(); // Should not throw
  }

  @Test public void testEnumeratorWithEmptyData() {
    mockClient.setTestData(new ArrayList<>());

    SharePointListEnumerator enumerator = new SharePointListEnumerator(mockMetadata, mockClient);

    assertFalse(enumerator.moveNext());
    assertNull(enumerator.current()); // Should be null when no data
  }

  @Test public void testReset() {
    List<Map<String, Object>> testData = new ArrayList<>();
    Map<String, Object> item = new HashMap<>();
    item.put("id", "1");
    item.put("Title", "Test Item");
    testData.add(item);

    mockClient.setTestData(testData);

    SharePointListEnumerator enumerator = new SharePointListEnumerator(mockMetadata, mockClient);

    // Move through data
    assertTrue(enumerator.moveNext());
    assertFalse(enumerator.moveNext());

    // Reset and move through again
    enumerator.reset();
    assertTrue(enumerator.moveNext());
    assertEquals("Test Item", enumerator.current()[1]);
  }

  @Test public void testValueConversions() {
    List<Map<String, Object>> testData = new ArrayList<>();

    Map<String, Object> item = new HashMap<>();
    item.put("id", "1");
    item.put("Title", "Text Value");
    item.put("NumberField", "123.45"); // String number
    item.put("BooleanField", "true"); // String boolean
    item.put("DateField", "2023-12-25T15:30:00Z");

    // Test lookup with LookupValue instead of Title
    Map<String, Object> lookupValue = new HashMap<>();
    lookupValue.put("LookupValue", "Alternative Lookup");
    item.put("LookupField", lookupValue);

    // Test multi-choice as string
    item.put("MultiChoiceField", "Single Choice");

    testData.add(item);
    mockClient.setTestData(testData);

    SharePointListEnumerator enumerator = new SharePointListEnumerator(mockMetadata, mockClient);

    assertTrue(enumerator.moveNext());
    Object[] row = enumerator.current();

    assertEquals("Text Value", row[1]);
    assertEquals(123.45, row[2]); // Converted from string
    assertEquals(true, row[3]); // Converted from string
    assertNotNull(row[4]); // Date converted
    assertEquals("Alternative Lookup", row[5]); // LookupValue used
    assertEquals("Single Choice", row[6]); // String multi-choice
  }

  @Test public void testIntegerConversion() {
    // Test with integer/counter type columns - we'll simulate by using a custom mock client
    List<SharePointColumn> intColumns = new ArrayList<>();
    intColumns.add(new SharePointColumn("Title", "title", "Title", false));
    intColumns.add(new SharePointColumn("IntField", "int_field", "Integer Field", false));
    intColumns.add(new SharePointColumn("CounterField", "counter_field", "Counter Field", false));

    // Create custom metadata that returns integer/counter types
    SharePointListMetadata intMetadata = new SharePointListMetadata("test-list-id", "TestList", "Test List", intColumns) {
      @Override public List<SharePointColumn> getColumns() {
        List<SharePointColumn> cols = new ArrayList<>();
        cols.add(new SharePointColumn("Title", "title", "Title", false) {
          @Override public String getType() { return "text"; }
        });
        cols.add(new SharePointColumn("IntField", "int_field", "Integer Field", false) {
          @Override public String getType() { return "integer"; }
        });
        cols.add(new SharePointColumn("CounterField", "counter_field", "Counter Field", false) {
          @Override public String getType() { return "counter"; }
        });
        return cols;
      }
    };

    List<Map<String, Object>> testData = new ArrayList<>();
    Map<String, Object> item = new HashMap<>();
    item.put("id", "1");
    item.put("Title", "Test");
    item.put("IntField", 42.7); // Double that should become int
    item.put("CounterField", "100"); // String that should become int
    testData.add(item);

    mockClient.setTestData(testData);

    SharePointListEnumerator enumerator = new SharePointListEnumerator(intMetadata, mockClient);

    assertTrue(enumerator.moveNext());
    Object[] row = enumerator.current();
    assertEquals("Test", row[1]);
    assertEquals(42, row[2]); // Converted to int
    assertEquals(100, row[3]); // Converted to int
  }

  @Test public void testCurrencyConversion() {
    List<SharePointColumn> currencyColumns = new ArrayList<>();
    currencyColumns.add(new SharePointColumn("Title", "title", "Title", false));
    currencyColumns.add(new SharePointColumn("CurrencyField", "currency_field", "Currency Field", false));

    SharePointListMetadata currencyMetadata = new SharePointListMetadata("test-list-id", "TestList", "Test List", currencyColumns) {
      @Override public List<SharePointColumn> getColumns() {
        List<SharePointColumn> cols = new ArrayList<>();
        cols.add(new SharePointColumn("Title", "title", "Title", false) {
          @Override public String getType() { return "text"; }
        });
        cols.add(new SharePointColumn("CurrencyField", "currency_field", "Currency Field", false) {
          @Override public String getType() { return "currency"; }
        });
        return cols;
      }
    };

    List<Map<String, Object>> testData = new ArrayList<>();
    Map<String, Object> item = new HashMap<>();
    item.put("id", "1");
    item.put("Title", "Test");
    item.put("CurrencyField", "99.99");
    testData.add(item);

    mockClient.setTestData(testData);

    SharePointListEnumerator enumerator = new SharePointListEnumerator(currencyMetadata, mockClient);

    assertTrue(enumerator.moveNext());
    Object[] row = enumerator.current();
    assertEquals("Test", row[1]);
    assertEquals(99.99, row[2]);
  }

  @Test public void testExceptionHandling() {
    mockClient.setShouldThrowException(true);

    assertThrows(RuntimeException.class, () -> {
      new SharePointListEnumerator(mockMetadata, mockClient);
    });
  }

  @Test public void testNullValueHandling() {
    List<Map<String, Object>> testData = new ArrayList<>();
    Map<String, Object> item = new HashMap<>();
    item.put("id", "1");
    item.put("Title", null);
    item.put("NumberField", null);
    item.put("BooleanField", null);
    testData.add(item);

    mockClient.setTestData(testData);

    SharePointListEnumerator enumerator = new SharePointListEnumerator(mockMetadata, mockClient);

    assertTrue(enumerator.moveNext());
    Object[] row = enumerator.current();

    assertEquals("1", row[0]); // ID should not be null
    assertNull(row[1]); // Title
    assertNull(row[2]); // NumberField
    assertNull(row[3]); // BooleanField
  }

  /**
   * Mock implementation of MicrosoftGraphListClient for testing.
   */
  private static class MockMicrosoftGraphListClient extends MicrosoftGraphListClient {
    private List<Map<String, Object>> testData;
    private boolean shouldThrowException = false;

    public MockMicrosoftGraphListClient() {
      super("mock-site-url", null);
    }

    public void setTestData(List<Map<String, Object>> testData) {
      this.testData = testData;
    }

    public void setShouldThrowException(boolean shouldThrowException) {
      this.shouldThrowException = shouldThrowException;
    }

    @Override public List<Map<String, Object>> getListItems(String listId) {
      if (shouldThrowException) {
        throw new RuntimeException("Mock exception for testing");
      }
      return testData != null ? testData : new ArrayList<>();
    }
  }
}
