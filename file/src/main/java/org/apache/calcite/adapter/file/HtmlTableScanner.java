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
package org.apache.calcite.adapter.file;

import org.apache.calcite.util.Source;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Scans HTML files to discover tables and their selectors.
 */
public class HtmlTableScanner {
  private static final Pattern WHITESPACE_PATTERN = Pattern.compile("\\s+");
  private static final Pattern INVALID_NAME_CHARS = Pattern.compile("[^a-zA-Z0-9_]");

  /**
   * Information about a discovered HTML table.
   */
  public static class TableInfo {
    public final String name;
    public final String selector;
    public final int index;

    TableInfo(String name, String selector, int index) {
      this.name = name;
      this.selector = selector;
      this.index = index;
    }
  }

  /**
   * Scans an HTML source and returns information about all tables found.
   *
   * @param source The HTML source to scan
   * @return List of table information
   * @throws IOException If the source cannot be read
   */
  public static List<TableInfo> scanTables(Source source) throws IOException {
    Document doc;
    String proto = source.protocol();

    if ("file".equals(proto)) {
      doc = Jsoup.parse(source.file(), StandardCharsets.UTF_8.name());
    } else {
      // For URLs, use openStream to handle all protocols
      doc = Jsoup.parse(source.openStream(), StandardCharsets.UTF_8.name(), "");
    }

    Elements tables = doc.select("table");
    List<TableInfo> tableInfos = new ArrayList<>();
    Map<String, Integer> nameCount = new LinkedHashMap<>();

    for (int i = 0; i < tables.size(); i++) {
      Element table = tables.get(i);
      String baseName = getTableName(table, i);

      // Ensure unique names
      Integer count = nameCount.get(baseName);
      String finalName;
      if (count != null) {
        finalName = baseName + "_" + (count + 1);
        nameCount.put(baseName, count + 1);
      } else {
        finalName = baseName;
        nameCount.put(baseName, 1);
      }

      // Create selector for this specific table
      String selector = null;
      if (table.hasAttr("id") && isValidCssIdentifier(table.attr("id"))) {
        selector = "#" + table.attr("id");
      } else {
        // Use index-based selector as fallback for IDs with special characters
        selector = "table:nth-of-type(" + (i + 1) + ")";
      }

      tableInfos.add(new TableInfo(finalName, selector, i));
    }

    return tableInfos;
  }

  /**
   * Gets a name for the table based on its id, nearby headings, or caption.
   */
  private static String getTableName(Element table, int index) {
    // First try table id attribute
    String id = table.attr("id");
    if (!id.isEmpty()) {
      return sanitizeName(id);
    }

    // Try table caption
    Element caption = table.selectFirst("caption");
    if (caption != null && !caption.text().trim().isEmpty()) {
      return sanitizeName(caption.text().trim());
    }

    // Look for preceding heading (h1-h6) within 3 elements
    Element current = table;
    for (int i = 0; i < 3; i++) {
      current = current.previousElementSibling();
      if (current == null) {
        break;
      }
      if (current.tagName().matches("h[1-6]")) {
        String headingText = current.text().trim();
        if (!headingText.isEmpty()) {
          return sanitizeName(headingText);
        }
      }
    }

    // Look for preceding element with class or id containing "title" or "header"
    current = table;
    for (int i = 0; i < 3; i++) {
      current = current.previousElementSibling();
      if (current == null) {
        break;
      }
      String classAttr = current.attr("class").toLowerCase(Locale.ROOT);
      String idAttr = current.attr("id").toLowerCase(Locale.ROOT);
      if (classAttr.contains("title") || classAttr.contains("header")
          || idAttr.contains("title") || idAttr.contains("header")) {
        String text = current.text().trim();
        if (!text.isEmpty()) {
          return sanitizeName(text);
        }
      }
    }

    // Default to T1, T2, etc.
    return "T" + (index + 1);
  }

  /**
   * Sanitizes a string to be a valid table name.
   */
  private static String sanitizeName(String name) {
    // Replace whitespace with underscores
    name = WHITESPACE_PATTERN.matcher(name).replaceAll("_");
    // Remove invalid characters
    name = INVALID_NAME_CHARS.matcher(name).replaceAll("");
    // Limit length
    if (name.length() > 50) {
      name = name.substring(0, 50);
    }
    // Ensure it starts with a letter or underscore
    if (!name.isEmpty() && !Character.isLetter(name.charAt(0)) && name.charAt(0) != '_') {
      name = "_" + name;
    }
    return name.isEmpty() ? "Table" : name;
  }

  /**
   * Checks if an identifier can be safely used in a CSS selector without escaping.
   */
  private static boolean isValidCssIdentifier(String identifier) {
    if (identifier == null || identifier.isEmpty()) {
      return false;
    }

    // Check for problematic characters that JSoup's CSS selector parser doesn't handle well
    for (int i = 0; i < identifier.length(); i++) {
      char c = identifier.charAt(i);
      
      // Disallow special characters that cause CSS selector parsing issues
      if (c == ' ' || c == '!' || c == '"' || c == '#' || c == '$' || c == '%' || c == '&' ||
          c == '\'' || c == '(' || c == ')' || c == '*' || c == '+' || c == ',' || c == '.' ||
          c == '/' || c == ':' || c == ';' || c == '<' || c == '=' || c == '>' || c == '?' ||
          c == '@' || c == '[' || c == '\\' || c == ']' || c == '^' || c == '`' || c == '{' ||
          c == '|' || c == '}' || c == '~') {
        return false;
      }
    }
    return true;
  }
}
