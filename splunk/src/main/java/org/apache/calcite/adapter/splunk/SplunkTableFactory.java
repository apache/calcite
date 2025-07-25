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

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TableFactory;
import org.apache.calcite.sql.type.SqlTypeName;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Factory for creating custom Splunk tables with user-defined schemas.
 * Used when tables are explicitly defined in JSON rather than
 * auto-generated from CIM models.
 *
 * Supports the following operand parameters:
 * - "search": Custom Splunk search string (defaults to "search")
 * - "field_mapping": Map of schema field names to Splunk field names
 * - "field_mappings": List of "schema_field:splunk_field" mappings (alternative format)
 */
public class SplunkTableFactory implements TableFactory<SplunkTable> {

  @Override public SplunkTable create(SchemaPlus schema, String name,
                           @Nullable Map<String, Object> operand,
                           @Nullable RelDataType rowType) {

    // Handle nullable rowType parameter
    if (rowType == null) {
      throw new IllegalArgumentException("rowType cannot be null for SplunkTable");
    }

    // Ensure we always have an _extra field for unmapped data
    RelDataType enhancedRowType = ensureExtraField(rowType);

    // Extract custom search string from operand
    String searchString = extractSearchString(operand);

    // Extract field mapping from operand
    Map<String, String> fieldMapping = extractFieldMapping(operand);

    return new SplunkTable(enhancedRowType, fieldMapping, searchString);
  }

  /**
   * Ensures the table schema includes an "_extra" field for capturing
   * unmapped Splunk fields as JSON. If already present, returns the
   * original type unchanged.
   */
  private RelDataType ensureExtraField(RelDataType originalType) {
    if (originalType.getFieldNames().contains("_extra")) {
      return originalType; // Already has _extra field
    }

    // Add _extra field for overflow data
    RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataTypeFactory.Builder builder = typeFactory.builder();

    // Add all original fields
    for (RelDataTypeField field : originalType.getFieldList()) {
      builder.add(field.getName(), field.getType());
    }

    // Add _extra field for unmapped fields as JSON
    RelDataType anyType = typeFactory.createSqlType(SqlTypeName.ANY);
    builder.add("_extra", anyType);

    return builder.build();
  }

  /**
   * Extracts the custom search string from the operand map.
   * Defaults to "search" if not specified.
   */
  private String extractSearchString(@Nullable Map<String, Object> operand) {
    if (operand == null) {
      return "search";
    }

    Object searchObj = operand.get("search");
    if (searchObj instanceof String) {
      String search = (String) searchObj;
      return search.trim().isEmpty() ? "search" : search;
    }

    return "search";
  }

  /**
   * Extracts field mapping from the operand map.
   * Supports two formats:
   * 1. field_mapping: Map<String, String> where key=schema field, value=splunk field
   * 2. field_mappings: List<String> with "schema_field:splunk_field" format
   */
  @SuppressWarnings("unchecked")
  private Map<String, String> extractFieldMapping(@Nullable Map<String, Object> operand) {
    Map<String, String> fieldMapping = new HashMap<>();

    if (operand == null) {
      return fieldMapping;
    }

    // Handle field_mapping as Map<String, String>
    Object mappingObj = operand.get("fieldMapping");
    if (mappingObj instanceof Map) {
      try {
        Map<String, Object> rawMapping = (Map<String, Object>) mappingObj;
        for (Map.Entry<String, Object> entry : rawMapping.entrySet()) {
          String key = entry.getKey();
          Object value = entry.getValue();
          if (value instanceof String) {
            fieldMapping.put(key, (String) value);
          }
        }
      } catch (ClassCastException e) {
        // Log warning and continue with empty mapping
        System.err.println("Warning: Invalid field_mapping format in table operand. " +
            "Expected Map<String, String>, got: " + mappingObj.getClass());
      }
    }

    // Handle field_mappings as List<String> with "key:value" format
    Object mappingListObj = operand.get("fieldMappings");
    if (mappingListObj instanceof List) {
      try {
        List<String> mappingList = (List<String>) mappingListObj;
        for (String mapping : mappingList) {
          String[] parts = mapping.split(":", 2);
          if (parts.length == 2) {
            String schemaField = parts[0].trim();
            String splunkField = parts[1].trim();
            if (!schemaField.isEmpty() && !splunkField.isEmpty()) {
              fieldMapping.put(schemaField, splunkField);
            }
          } else {
            System.err.println("Warning: Invalid field mapping format: '" + mapping +
                "'. Expected 'schema_field:splunk_field'");
          }
        }
      } catch (ClassCastException e) {
        // Log warning and continue
        System.err.println("Warning: Invalid field_mappings format in table operand. " +
            "Expected List<String>, got: " + mappingListObj.getClass());
      }
}

    return fieldMapping;
  }
}
