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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for SharePointColumn.
 */
public class SharePointColumnTest {

  @Test public void testColumnCreationAndGetters() {
    SharePointColumn column = new SharePointColumn("Title", "Title Field", "text", false);

    assertEquals("Title", column.getInternalName());
    assertEquals("title_field", column.getName()); // Converted by SharePointNameConverter
    assertEquals("Title Field", column.getDisplayName());
    assertEquals("text", column.getType());
    assertFalse(column.isRequired());
  }

  @Test public void testRequiredColumn() {
    SharePointColumn column = new SharePointColumn("RequiredField", "Required Field", "text", true);

    assertEquals("RequiredField", column.getInternalName());
    assertEquals("required_field", column.getName()); // Converted by SharePointNameConverter
    assertEquals("Required Field", column.getDisplayName());
    assertTrue(column.isRequired());
  }

  @Test public void testColumnWithSpecialCharacters() {
    SharePointColumn column = new SharePointColumn("Field!@#$%", "Field with Special Chars!", "text", false);

    assertEquals("Field!@#$%", column.getInternalName());
    assertEquals("field_with_special_chars", column.getName()); // Converted by SharePointNameConverter
    assertEquals("Field with Special Chars!", column.getDisplayName());
    assertFalse(column.isRequired());
  }

  @Test public void testColumnWithNullValues() {
    // Test with null displayName - should use internalName as fallback
    SharePointColumn column1 = new SharePointColumn("InternalName", null, "text", false);
    assertEquals("InternalName", column1.getInternalName());
    assertEquals("internalname", column1.getName()); // Converted by SharePointNameConverter
    assertEquals("InternalName", column1.getDisplayName()); // Fallback to internal name

    // Test with empty displayName - should use internalName as fallback
    SharePointColumn column2 = new SharePointColumn("InternalName2", "", "text", true);
    assertEquals("InternalName2", column2.getInternalName());
    assertEquals("internalname2", column2.getName()); // Converted by SharePointNameConverter
    assertEquals("InternalName2", column2.getDisplayName()); // Fallback to internal name
    assertTrue(column2.isRequired());
  }

  @Test public void testColumnEquality() {
    SharePointColumn column1 = new SharePointColumn("Title", "Title Field", "text", false);
    SharePointColumn column2 = new SharePointColumn("Title", "Title Field", "text", false);
    SharePointColumn column3 = new SharePointColumn("Description", "Description Field", "text", false);

    // Test that columns with same properties have same getter values
    assertEquals(column1.getInternalName(), column2.getInternalName());
    assertEquals(column1.getName(), column2.getName());
    assertEquals(column1.getDisplayName(), column2.getDisplayName());
    assertEquals(column1.isRequired(), column2.isRequired());

    // Test that different columns have different values
    assertEquals("Title", column1.getInternalName());
    assertEquals("Description", column3.getInternalName());
  }

  @Test public void testColumnWithLongNames() {
    String longInternalName = "VeryLongInternalNameThatExceedsNormalLimits";
    String longDisplayName = "Very Long Display Name That Exceeds Normal Limits";

    SharePointColumn column = new SharePointColumn(longInternalName, longDisplayName, "text", true);

    assertEquals(longInternalName, column.getInternalName());
    assertEquals("very_long_display_name_that_exceeds_normal_limits", column.getName()); // Converted
    assertEquals(longDisplayName, column.getDisplayName());
    assertTrue(column.isRequired());
  }
}
