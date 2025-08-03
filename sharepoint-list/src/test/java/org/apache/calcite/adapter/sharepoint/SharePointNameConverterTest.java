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

/**
 * Unit tests for SharePointNameConverter.
 */
public class SharePointNameConverterTest {

  @Test public void testToSqlName() {
    // Test basic conversion
    assertEquals("project_tasks", SharePointNameConverter.toSqlName("Project Tasks"));
    assertEquals("due_date", SharePointNameConverter.toSqlName("Due Date"));
    assertEquals("is_complete", SharePointNameConverter.toSqlName("Is Complete?"));

    // Test edge cases
    assertEquals("task_title", SharePointNameConverter.toSqlName("Task Title"));
    assertEquals("simple", SharePointNameConverter.toSqlName("Simple"));
    assertEquals("already_lower", SharePointNameConverter.toSqlName("already_lower"));
    assertEquals("with_numbers_123", SharePointNameConverter.toSqlName("With Numbers 123"));
    assertEquals("specialchars", SharePointNameConverter.toSqlName("Special!@#$%^&*()Chars"));

    // Test null and empty
    assertEquals("", SharePointNameConverter.toSqlName(""));
    assertEquals(null, SharePointNameConverter.toSqlName(null));

    // Test multiple consecutive spaces/special chars
    assertEquals("multiple_spaces", SharePointNameConverter.toSqlName("Multiple   Spaces"));
    assertEquals("leading_trailing", SharePointNameConverter.toSqlName("  Leading Trailing  "));
    assertEquals("mixed_case_with_numbers_42", SharePointNameConverter.toSqlName("Mixed Case With Numbers 42"));
  }

  @Test public void testToSharePointName() {
    // Test basic conversion
    assertEquals("Project Tasks", SharePointNameConverter.toSharePointName("project_tasks"));
    assertEquals("Due Date", SharePointNameConverter.toSharePointName("due_date"));
    assertEquals("Is Complete", SharePointNameConverter.toSharePointName("is_complete"));

    // Test edge cases
    assertEquals("Task Title", SharePointNameConverter.toSharePointName("task_title"));
    assertEquals("Simple", SharePointNameConverter.toSharePointName("simple"));
    assertEquals("Already upper", SharePointNameConverter.toSharePointName("Already Upper"));
    assertEquals("With Numbers 123", SharePointNameConverter.toSharePointName("with_numbers_123"));

    // Test null and empty
    assertEquals("", SharePointNameConverter.toSharePointName(""));
    assertEquals(null, SharePointNameConverter.toSharePointName(null));

    // Test multiple underscores and edge cases
    assertEquals("Multiple   Underscores", SharePointNameConverter.toSharePointName("multiple___underscores"));
    assertEquals(" Leading Trailing", SharePointNameConverter.toSharePointName("_leading_trailing_"));
    assertEquals("Mixed Case Input", SharePointNameConverter.toSharePointName("Mixed_Case_Input"));
  }

  @Test public void testRoundTrip() {
    // Test that converting back and forth is consistent
    String[] testCases = {
      "Project Tasks",
      "Due Date",
      "Is Complete",
      "Simple List",
      "Task With Numbers 123",
      "Mixed Case Example"
    };

    for (String original : testCases) {
      String sqlName = SharePointNameConverter.toSqlName(original);
      String backToSharePoint = SharePointNameConverter.toSharePointName(sqlName);

      // Should preserve the basic structure (special characters like '?' will be lost)
      String normalized = original.replaceAll("[^a-zA-Z0-9\\s]", "").trim();
      assertEquals(normalized, backToSharePoint,
          "Round trip conversion failed for: " + original);
    }
  }

  @Test public void testSpecialCharacterHandling() {
    // Test handling of various special characters - they get removed, no underscores between words
    assertEquals("test_column", SharePointNameConverter.toSqlName("Test Column!"));
    assertEquals("testcolumn", SharePointNameConverter.toSqlName("Test@Column"));
    assertEquals("testcolumn", SharePointNameConverter.toSqlName("Test#Column"));
    assertEquals("testcolumn", SharePointNameConverter.toSqlName("Test$Column%"));
    assertEquals("testcolumn", SharePointNameConverter.toSqlName("Test^Column&"));
    assertEquals("testcolumn", SharePointNameConverter.toSqlName("Test*Column()"));
    assertEquals("test_column", SharePointNameConverter.toSqlName("Test-Column+")); // Dash becomes underscore
    assertEquals("testcolumn", SharePointNameConverter.toSqlName("Test=Column[]"));
    assertEquals("testcolumn", SharePointNameConverter.toSqlName("Test{Column}"));
    assertEquals("testcolumn", SharePointNameConverter.toSqlName("Test|Column\\"));
    assertEquals("testcolumn", SharePointNameConverter.toSqlName("Test:Column;"));
    assertEquals("testcolumn", SharePointNameConverter.toSqlName("Test\"Column'"));
    assertEquals("testcolumn", SharePointNameConverter.toSqlName("Test<Column>"));
    assertEquals("testcolumn", SharePointNameConverter.toSqlName("Test,Column."));
    assertEquals("testcolumn", SharePointNameConverter.toSqlName("Test?Column/"));
  }
}
