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

import org.apache.calcite.adapter.splunk.search.SplunkConnection;
import org.apache.calcite.adapter.splunk.util.StringUtils;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.ArrayList;

/**
 * Query against Splunk using JSON output for simplified processing.
 *
 * @param <T> Element type
 */
public class SplunkQuery<T> extends AbstractEnumerable<T> {
  private static final Logger LOGGER = LoggerFactory.getLogger(SplunkQuery.class);

  private final SplunkConnection splunkConnection;
  private final String search;
  private final String earliest;
  private final String latest;
  private final List<String> fieldList;
  private final Set<String> explicitFields;
  private final Map<String, String> fieldMapping;
  private final RelDataType schema;

  /** Creates a SplunkQuery. */
  public SplunkQuery(
      SplunkConnection splunkConnection,
      String search,
      String earliest,
      String latest,
      List<String> fieldList) {
    this(splunkConnection, search, earliest, latest, fieldList,
        Collections.emptySet(), Collections.emptyMap(), null);
  }

  /** Creates a SplunkQuery with explicit field information. */
  public SplunkQuery(
      SplunkConnection splunkConnection,
      String search,
      String earliest,
      String latest,
      List<String> fieldList,
      Set<String> explicitFields) {
    this(splunkConnection, search, earliest, latest, fieldList,
        explicitFields, Collections.emptyMap(), null);
  }

  /** Creates a SplunkQuery with explicit field information and field mapping. */
  public SplunkQuery(
      SplunkConnection splunkConnection,
      String search,
      String earliest,
      String latest,
      List<String> fieldList,
      Set<String> explicitFields,
      Map<String, String> fieldMapping) {
    this(splunkConnection, search, earliest, latest, fieldList,
        explicitFields, fieldMapping, null);
  }

  /** Creates a SplunkQuery with explicit field information, field mapping, and schema. */
  public SplunkQuery(
      SplunkConnection splunkConnection,
      String search,
      String earliest,
      String latest,
      List<String> fieldList,
      Set<String> explicitFields,
      Map<String, String> fieldMapping,
      RelDataType schema) {
    this.splunkConnection = splunkConnection;
    this.search = search;
    this.earliest = earliest;
    this.latest = latest;
    this.fieldList = fieldList;
    this.explicitFields = explicitFields;
    this.fieldMapping = fieldMapping != null ? fieldMapping : Collections.emptyMap();
    this.schema = schema;
  }

  @Override public String toString() {
    return "SplunkQuery {" + search + "}";
  }

  @SuppressWarnings("unchecked")
  @Override public Enumerator<T> enumerator() {
    // Create a reverse mapping for result processing (Splunk field -> schema field)
    Map<String, String> reverseMapping = createReverseMapping();

    // Get the raw enumerator from the connection
    // With JSON, we pass schema field names directly - much simpler!
    Enumerator<T> rawEnumerator = (Enumerator<T>) splunkConnection.getSearchResultEnumerator(
        search, getArgs(), fieldList, explicitFields, reverseMapping);

    LOGGER.debug("DEBUG: SplunkQuery.enumerator() - JSON Mode");
    LOGGER.debug("  Query field list: {}", fieldList);
    LOGGER.debug("  Field mapping: {}", fieldMapping);
    LOGGER.debug("  Schema is null? {}", schema == null);

    // Type conversion is simpler with JSON since types are better preserved
    if (schema != null) {
      LOGGER.debug("  Creating SimpleTypeConverter...");
      // CRITICAL FIX: Pass the actual query field list so SimpleTypeConverter knows the array order
      return (Enumerator<T>) new SimpleTypeConverter((Enumerator<Object>) rawEnumerator, schema, fieldList);
    }

    LOGGER.debug("  Using raw enumerator (no schema conversion)");
    return rawEnumerator;
  }

  /**
   * Creates a reverse mapping from Splunk field names to schema field names.
   */
  private Map<String, String> createReverseMapping() {
    Map<String, String> reverse = new HashMap<>();
    for (Map.Entry<String, String> entry : fieldMapping.entrySet()) {
      reverse.put(entry.getValue(), entry.getKey());
    }
    return reverse;
  }

  private Map<String, String> getArgs() {
    Map<String, String> args = new HashMap<>();

    // For JSON mode, we can pass schema field names directly
    // The JSON enumerator will handle the mapping to Splunk field names
    List<String> fieldsForQuery = new ArrayList<>(fieldList);

    LOGGER.debug("=== SPLUNK QUERY ARGS DEBUG ===");
    LOGGER.debug("Original fieldList: {}", fieldList);
    LOGGER.debug("Contains _extra? {}", fieldList.contains("_extra"));

    // If _extra is requested, add wildcard to get all fields
    if (fieldList.contains("_extra")) {
      LOGGER.debug("_extra detected - requesting ALL fields from Splunk");
      fieldsForQuery.remove("_extra");
      fieldsForQuery.add("*");
      LOGGER.debug("Modified fieldsForQuery: {}", fieldsForQuery);
    }

    String fields = StringUtils.encodeList(fieldsForQuery, ',').toString();
    LOGGER.debug("Final field_list sent to Splunk: '{}'", fields);

    args.put("field_list", fields);
    args.put("earliest_time", earliest);
    args.put("latest_time", latest);

    LOGGER.debug("Complete Splunk args: {}", args);
    LOGGER.debug("=== END SPLUNK QUERY ARGS DEBUG ===");
    return args;
  }

  /**
   * Returns the field mapping for external use.
   */
  public Map<String, String> getFieldMapping() {
    return fieldMapping;
  }

  /**
   * Simple type converter for JSON data.
   * Much simpler than the CSV version since JSON preserves types better.
   *
   * FIXED: Now accepts the actual query field list so it knows the array order.
   */
  private static class SimpleTypeConverter implements Enumerator<Object> {
    private final Enumerator<Object> underlying;
    private final RelDataType schema;
    private final List<String> queryFieldList; // ADDED: The actual field order in the array
    private int rowCount = 0;

    public SimpleTypeConverter(Enumerator<Object> underlying, RelDataType schema, List<String> queryFieldList) {
      this.underlying = underlying;
      this.schema = schema;
      this.queryFieldList = queryFieldList;

      LOGGER.debug("DEBUG: SimpleTypeConverter created for JSON mode");
      LOGGER.debug("  Full schema field count: " + schema.getFieldCount());
      LOGGER.debug("  Query field count: " + queryFieldList.size());
      LOGGER.debug("  Query field list: " + queryFieldList);

      // Show the mapping between array indices and field names
      LOGGER.debug("  Array index mapping:");
      for (int i = 0; i < queryFieldList.size(); i++) {
        String fieldName = queryFieldList.get(i);
        RelDataTypeField schemaField = schema.getField(fieldName, false, false);
        if (schemaField != null) {
          LOGGER.debug(String.format("    [%d] = %s (%s)\n", i, fieldName, schemaField.getType().getSqlTypeName()));
        } else {
          LOGGER.debug(String.format("    [%d] = %s (field not found in schema)\n", i, fieldName));
        }
      }
    }

    @Override
    public Object current() {
      Object current = underlying.current();
      rowCount++;

      if (current instanceof Object[]) {
        Object[] inputRow = (Object[]) current;

        if (rowCount <= 3) {
          LOGGER.debug("DEBUG: SimpleTypeConverter - Row " + rowCount);
          LOGGER.debug("  Input row length: " + inputRow.length);
          LOGGER.debug("  Query field count: " + queryFieldList.size());

          // Show what we got from JSON using the CORRECT field mapping
          for (int i = 0; i < Math.min(inputRow.length, queryFieldList.size()); i++) {
            String fieldName = queryFieldList.get(i);
            RelDataTypeField field = schema.getField(fieldName, false, false);
            Object value = inputRow[i];
            String valueType = value != null ? value.getClass().getSimpleName() : "null";

            if (field != null) {
              String expectedType = field.getType().getSqlTypeName().toString();
              LOGGER.debug(String.format("  [%d] %s: '%s' (%s) -> expected %s\n",
                  i, fieldName, value, valueType, expectedType));
            } else {
              LOGGER.debug(String.format("  [%d] %s: '%s' (%s) -> field not in schema\n",
                  i, fieldName, value, valueType));
            }
          }
        }

        // Convert types as needed - most should already be correct from JSON
        Object[] convertedRow = convertRowWithFieldMapping(inputRow, schema, queryFieldList);

        if (rowCount <= 3) {
          LOGGER.debug("  Post-conversion:");
          for (int i = 0; i < Math.min(convertedRow.length, queryFieldList.size()); i++) {
            String fieldName = queryFieldList.get(i);
            RelDataTypeField field = schema.getField(fieldName, false, false);
            Object value = convertedRow[i];
            String valueType = value != null ? value.getClass().getSimpleName() : "null";

            LOGGER.debug("  [{}] {}: '{}' ({})", i, fieldName, value, valueType);

            // Flag any type mismatches
            if (field != null && field.getType().getSqlTypeName() == SqlTypeName.INTEGER && value instanceof String) {
              LOGGER.warn("    *** WARNING: INTEGER field has String value ***");
            }
          }
        }

        return convertedRow;
      }

      return current;
    }

    /**
     * Convert a row using the query field mapping instead of assuming schema order.
     */
    private Object[] convertRowWithFieldMapping(Object[] inputRow, RelDataType schema, List<String> queryFieldList) {
      Object[] result = new Object[inputRow.length];

      for (int i = 0; i < Math.min(inputRow.length, queryFieldList.size()); i++) {
        String fieldName = queryFieldList.get(i);
        RelDataTypeField field = schema.getField(fieldName, false, false);
        Object value = inputRow[i];

        if (field != null) {
          // Convert using the actual field type
          result[i] = SplunkDataConverter.convertValue(value, field.getType().getSqlTypeName());
        } else {
          // Field not in schema, pass through as-is
          result[i] = value;
        }
      }

      return result;
    }

    @Override
    public boolean moveNext() {
      return underlying.moveNext();
    }

    @Override
    public void reset() {
      underlying.reset();
    }

    @Override
    public void close() {
      underlying.close();
    }
  }
}
