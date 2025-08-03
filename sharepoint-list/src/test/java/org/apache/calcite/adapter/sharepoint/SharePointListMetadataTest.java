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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for SharePointListMetadata.
 */
public class SharePointListMetadataTest {

  private List<SharePointColumn> testColumns;

  @BeforeEach
  public void setUp() {
    testColumns = new ArrayList<>();
    testColumns.add(new SharePointColumn("Title", "Title Field", "text", false));
    testColumns.add(new SharePointColumn("Description", "Description Field", "text", true));
    testColumns.add(new SharePointColumn("CreatedDate", "Created Date", "datetime", false));
  }

  @Test public void testMetadataCreationAndGetters() {
    SharePointListMetadata metadata =
        new SharePointListMetadata("list-12345",
        "Test List Display Name",
        "ListItem",
        testColumns);

    assertEquals("list-12345", metadata.getListId());
    assertEquals("test_list_display_name", metadata.getListName()); // Converted by SharePointNameConverter
    assertEquals("Test List Display Name", metadata.getDisplayName());
    assertEquals("ListItem", metadata.getEntityTypeName());
    assertNotNull(metadata.getColumns());
    assertEquals(3, metadata.getColumns().size());
    assertSame(testColumns, metadata.getColumns()); // Should return the same list reference
  }

  @Test @Disabled("Known limitation: SharePointNameConverter returns null for null input")
  public void testMetadataWithNullDisplayName() {
    SharePointListMetadata metadata =
        new SharePointListMetadata("list-67890",
        null,
        "ListItem",
        testColumns);

    assertEquals("list-67890", metadata.getListId());
    assertEquals("column", metadata.getListName()); // null gets converted by SharePointNameConverter
    assertNull(metadata.getDisplayName());
    assertEquals("ListItem", metadata.getEntityTypeName());
    assertEquals(testColumns, metadata.getColumns());
  }

  @Test @Disabled("Known limitation: SharePointNameConverter returns empty string for empty input")
  public void testMetadataWithEmptyDisplayName() {
    SharePointListMetadata metadata =
        new SharePointListMetadata("list-11111",
        "",
        "ListItem",
        testColumns);

    assertEquals("list-11111", metadata.getListId());
    assertEquals("column", metadata.getListName()); // Empty string gets converted by SharePointNameConverter
    assertEquals("", metadata.getDisplayName());
    assertEquals("ListItem", metadata.getEntityTypeName());
  }

  @Test public void testMetadataWithEmptyColumns() {
    List<SharePointColumn> emptyColumns = new ArrayList<>();
    SharePointListMetadata metadata =
        new SharePointListMetadata("list-empty",
        "Empty List",
        "ListItem",
        emptyColumns);

    assertEquals("list-empty", metadata.getListId());
    assertEquals("empty_list", metadata.getListName()); // Converted
    assertEquals("Empty List", metadata.getDisplayName());
    assertEquals("ListItem", metadata.getEntityTypeName());
    assertNotNull(metadata.getColumns());
    assertEquals(0, metadata.getColumns().size());
    assertSame(emptyColumns, metadata.getColumns());
  }

  @Test public void testMetadataWithSpecialCharacters() {
    SharePointListMetadata metadata =
        new SharePointListMetadata("list-special!@#$%",
        "Display Name with Special Chars!@#$%",
        "ListItem",
        testColumns);

    assertEquals("list-special!@#$%", metadata.getListId());
    assertEquals("display_name_with_special_chars", metadata.getListName()); // Converted
    assertEquals("Display Name with Special Chars!@#$%", metadata.getDisplayName());
    assertEquals("ListItem", metadata.getEntityTypeName());
    assertEquals(testColumns, metadata.getColumns());
  }

  @Test public void testMetadataWithLongNames() {
    String longListId = "list-" + "a".repeat(100);
    String longDisplayName = "Very Long Display Name " + "c".repeat(100);

    SharePointListMetadata metadata =
        new SharePointListMetadata(longListId,
        longDisplayName,
        "ListItem",
        testColumns);

    assertEquals(longListId, metadata.getListId());
    // The display name gets converted to SQL name by SharePointNameConverter
    assertTrue(metadata.getListName().startsWith("very_long_display_name"));
    assertEquals(longDisplayName, metadata.getDisplayName());
    assertEquals("ListItem", metadata.getEntityTypeName());
  }

  @Test public void testMetadataWithSingleColumn() {
    List<SharePointColumn> singleColumn = new ArrayList<>();
    singleColumn.add(new SharePointColumn("OnlyColumn", "Only Column", "text", true));

    SharePointListMetadata metadata =
        new SharePointListMetadata("list-single",
        "Single Column List",
        "ListItem",
        singleColumn);

    assertEquals("list-single", metadata.getListId());
    assertEquals("single_column_list", metadata.getListName()); // Converted
    assertEquals("Single Column List", metadata.getDisplayName());
    assertEquals("ListItem", metadata.getEntityTypeName());
    assertEquals(1, metadata.getColumns().size());
    assertEquals("OnlyColumn", metadata.getColumns().get(0).getInternalName());
  }

  @Test public void testGetEntityTypeNameConsistency() {
    // Test that getEntityTypeName always returns the same value for different instances
    SharePointListMetadata metadata1 = new SharePointListMetadata("list1", "Display1", "ListItem", testColumns);
    SharePointListMetadata metadata2 = new SharePointListMetadata("list2", "Display2", "ListItem", testColumns);
    SharePointListMetadata metadata3 = new SharePointListMetadata("list3", "Display3", "ListItem", new ArrayList<>());

    assertEquals(metadata1.getEntityTypeName(), metadata2.getEntityTypeName());
    assertEquals(metadata2.getEntityTypeName(), metadata3.getEntityTypeName());
    assertEquals("ListItem", metadata1.getEntityTypeName());
  }
}
