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
package org.apache.calcite.adapter.splunk;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility class for processing custom table configuration from environment variables.
 * Supports two configuration approaches:
 * 1. SPLUNK_CUSTOM_TABLES: JSON array with complete table definitions
 * 2. Individual table environment variables: SPLUNK_TABLE_{NAME}_* pattern
 */
public class CustomTableConfigProcessor {
  private static final ObjectMapper objectMapper = new ObjectMapper();

  /**
   * Processes custom table configuration from environment variables and adds
   * the resulting table definitions to the operand map.
   *
   * @param operand The operand map to modify
   */
  public static void processEnvironmentVariables(Map<String, Object> operand) {
    // First, try to process SPLUNK_CUSTOM_TABLES (JSON array approach)
    List<Map<String, Object>> customTables = processCustomTablesJson();

    // Then, process individual table environment variables
    List<Map<String, Object>> individualTables = processIndividualTableVars();

    // Combine both approaches
    if (!customTables.isEmpty() || !individualTables.isEmpty()) {
      List<Map<String, Object>> allTables = new ArrayList<>();
      allTables.addAll(customTables);
      allTables.addAll(individualTables);

      // Add to operand, potentially merging with existing tables
      mergeTablesIntoOperand(operand, allTables);
    }
  }

  /**
   * Processes the SPLUNK_CUSTOM_TABLES environment variable.
   */
  private static List<Map<String, Object>> processCustomTablesJson() {
    String customTablesJson = System.getenv("SPLUNK_CUSTOM_TABLES");
    if (customTablesJson == null || customTablesJson.trim().isEmpty()) {
      return new ArrayList<>();
    }

    try {
      TypeReference<List<Map<String, Object>>> typeRef =
          new TypeReference<List<Map<String, Object>>>() {};
      return objectMapper.readValue(customTablesJson, typeRef);
    } catch (JsonProcessingException e) {
      System.err.println("Warning: Failed to parse SPLUNK_CUSTOM_TABLES JSON: " + e.getMessage());
      return new ArrayList<>();
    }
  }

  /**
   * Processes individual table environment variables (SPLUNK_TABLE_*).
   */
  private static List<Map<String, Object>> processIndividualTableVars() {
    String tableNamesEnv = System.getenv("SPLUNK_TABLE_NAMES");
    if (tableNamesEnv == null || tableNamesEnv.trim().isEmpty()) {
      return new ArrayList<>();
    }

    List<String> tableNames = Arrays.asList(tableNamesEnv.split(","));
    List<Map<String, Object>> tables = new ArrayList<>();

    for (String tableName : tableNames) {
      String normalizedName = tableName.trim().toUpperCase();
      if (normalizedName.isEmpty()) {
        continue;
      }

      Map<String, Object> tableConfig = processIndividualTable(tableName.trim(), normalizedName);
      if (tableConfig != null) {
        tables.add(tableConfig);
      }
    }

    return tables;
  }

  /**
   * Processes configuration for a single table from environment variables.
   */
  private static Map<String, Object> processIndividualTable(String tableName, String normalizedName) {
    Map<String, Object> tableConfig = new HashMap<>();
    tableConfig.put("name", tableName);

    // Process search string
    String searchKey = "SPLUNK_TABLE_" + normalizedName + "_SEARCH";
    String search = System.getenv(searchKey);
    if (search != null && !search.trim().isEmpty()) {
      tableConfig.put("search", search);
    }

    // Process field definitions
    String fieldsKey = "SPLUNK_TABLE_" + normalizedName + "_FIELDS";
    String fieldsJson = System.getenv(fieldsKey);
    if (fieldsJson != null && !fieldsJson.trim().isEmpty()) {
      try {
        TypeReference<List<Object>> typeRef = new TypeReference<List<Object>>() {};
        List<Object> fields = objectMapper.readValue(fieldsJson, typeRef);
        tableConfig.put("fields", fields);
      } catch (JsonProcessingException e) {
        System.err.println("Warning: Failed to parse " + fieldsKey + " JSON: " + e.getMessage());
      }
    }

    // Process field mapping (object format)
    String fieldMappingKey = "SPLUNK_TABLE_" + normalizedName + "_FIELD_MAPPING";
    String fieldMappingJson = System.getenv(fieldMappingKey);
    if (fieldMappingJson != null && !fieldMappingJson.trim().isEmpty()) {
      try {
        TypeReference<Map<String, String>> typeRef =
            new TypeReference<Map<String, String>>() {};
        Map<String, String> fieldMapping = objectMapper.readValue(fieldMappingJson, typeRef);
        tableConfig.put("fieldMapping", fieldMapping);
      } catch (JsonProcessingException e) {
        System.err.println("Warning: Failed to parse " + fieldMappingKey + " JSON: " + e.getMessage());
      }
    }

    // Process field mappings (array format)
    String fieldMappingsKey = "SPLUNK_TABLE_" + normalizedName + "_FIELD_MAPPINGS";
    String fieldMappingsJson = System.getenv(fieldMappingsKey);
    if (fieldMappingsJson != null && !fieldMappingsJson.trim().isEmpty()) {
      try {
        TypeReference<List<String>> typeRef = new TypeReference<List<String>>() {};
        List<String> fieldMappings = objectMapper.readValue(fieldMappingsJson, typeRef);
        tableConfig.put("fieldMappings", fieldMappings);
      } catch (JsonProcessingException e) {
        System.err.println("Warning: Failed to parse " + fieldMappingsKey + " JSON: " + e.getMessage());
      }
    }

    // Only return the table config if it has meaningful configuration beyond just the name
    if (tableConfig.size() > 1) {
      return tableConfig;
    }

    return null;
  }

  /**
   * Merges environment variable tables with existing tables in the operand.
   */
  @SuppressWarnings("unchecked")
  private static void mergeTablesIntoOperand(Map<String, Object> operand,
      List<Map<String, Object>> envTables) {

    Object existingTablesObj = operand.get("tables");
    List<Map<String, Object>> allTables = new ArrayList<>();

    // Add existing tables from operand first
    if (existingTablesObj instanceof List) {
      try {
        List<Map<String, Object>> existingTables =
            (List<Map<String, Object>>) existingTablesObj;
        allTables.addAll(existingTables);
      } catch (ClassCastException e) {
        System.err.println("Warning: Existing tables in operand are not in expected format");
      }
    }

    // Add environment variable tables
    allTables.addAll(envTables);

    // Update operand
    operand.put("tables", allTables);

    // Log configuration summary
    System.out.println("Processed " + envTables.size() + " custom tables from environment variables");
    for (Map<String, Object> table : envTables) {
      String name = (String) table.get("name");
      String search = (String) table.getOrDefault("search", "search");
      System.out.println("  - Table: " + name + " (search: " + search + ")");
    }
  }
}
