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
package org.apache.calcite.adapter.file.converters;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Safe wrapper around ExcelToJsonConverter that prevents conflicts.
 */
public final class SafeExcelToJsonConverter {

  // Track which Excel files have been converted in this session
  private static final Set<String> CONVERTED_FILES = new HashSet<>();

  private SafeExcelToJsonConverter() {
    // Prevent instantiation
  }

  /**
   * Converts Excel file to JSON only if not already converted or if Excel file is newer.
   * This prevents repeated conversions and checks for file freshness.
   */
  public static void convertIfNeeded(File excelFile) throws IOException {
    convertIfNeeded(excelFile, false);
  }

  /**
   * Converts Excel file to JSON only if not already converted or if Excel file is newer.
   * This prevents repeated conversions and checks for file freshness.
   * Uses SMART_CASING defaults.
   *
   * @param excelFile Excel file to convert
   * @param detectMultipleTables If true, uses enhanced converter to detect multiple tables
   */
  public static void convertIfNeeded(File excelFile, boolean detectMultipleTables)
      throws IOException {
    convertIfNeeded(excelFile, detectMultipleTables, "SMART_CASING", "SMART_CASING");
  }

  /**
   * Converts Excel file to JSON only if not already converted or if Excel file is newer.
   * This prevents repeated conversions and checks for file freshness.
   *
   * @param excelFile Excel file to convert
   * @param detectMultipleTables If true, uses enhanced converter to detect multiple tables
   * @param tableNameCasing The casing strategy for table names
   * @param columnNameCasing The casing strategy for column names
   */
  public static void convertIfNeeded(File excelFile, boolean detectMultipleTables,
      String tableNameCasing, String columnNameCasing)
      throws IOException {
    String canonicalPath = excelFile.getCanonicalPath();
    long excelLastModified = excelFile.lastModified();

    // Check if we need to convert
    boolean needsConversion = false;

    // Get expected JSON file names
    String fileName = excelFile.getName();
    String baseName = fileName.substring(0, fileName.lastIndexOf('.'));
    File parentDir = excelFile.getParentFile();

    // Check if any converted JSON files exist
    File[] existingJsonFiles = parentDir.listFiles((dir, name) ->
        name.startsWith(baseName + "__") && name.endsWith(".json"));

    if (existingJsonFiles == null || existingJsonFiles.length == 0) {
      // No JSON files exist, need to convert
      needsConversion = true;
    } else {
      // Check if Excel file is newer than existing JSON files
      for (File jsonFile : existingJsonFiles) {
        if (excelLastModified > jsonFile.lastModified()) {
          needsConversion = true;
          break;
        }
      }
    }

    // Only convert if needed and not already converted in this session
    if (needsConversion && !CONVERTED_FILES.contains(canonicalPath)) {
      if (detectMultipleTables) {
        MultiTableExcelToJsonConverter.convertFileToJson(excelFile, true, tableNameCasing, columnNameCasing);
      } else {
        ExcelToJsonConverter.convertFileToJson(excelFile, tableNameCasing, columnNameCasing);
      }
      CONVERTED_FILES.add(canonicalPath);
    }
  }

  /**
   * Clears the conversion cache. Useful for testing or when files need to be re-converted.
   */
  public static void clearCache() {
    CONVERTED_FILES.clear();
  }
}
