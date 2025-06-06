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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.ArrayList;

/**
 * Query against Splunk.
 *
 * @param <T> Element type
 */
public class SplunkQuery<T> extends AbstractEnumerable<T> {
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
    // Map schema field names to Splunk field names for the query
    List<String> mappedFieldList = mapFieldList(fieldList);

    // Create a reverse mapping for result processing (Splunk field -> schema field)
    Map<String, String> reverseMapping = createReverseMapping();

    // Get the raw enumerator from the connection
    Enumerator<T> rawEnumerator = (Enumerator<T>) splunkConnection.getSearchResultEnumerator(
        search, getArgs(), mappedFieldList, explicitFields, reverseMapping);

    // If we have schema information, wrap with type converter
    if (schema != null) {
      return (Enumerator<T>) new TypeConvertingEnumerator((Enumerator<Object>) rawEnumerator, schema);
    }

    return rawEnumerator;
  }

  /**
   * Maps schema field names to Splunk field names using the field mapping.
   */
  private List<String> mapFieldList(List<String> schemaFieldList) {
    return schemaFieldList.stream()
        .map(field -> fieldMapping.getOrDefault(field, field))
        .collect(Collectors.toList());
  }

  /**
   * Creates a reverse mapping from Splunk field names to schema field names.
   * This is used when processing results to map back to schema field names.
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

    // Map the field list to Splunk field names for the field_list parameter
    List<String> mappedFieldList = mapFieldList(fieldList);

    // If _extra is requested, we need both the mapped fields AND additional fields
    if (fieldList.contains("_extra")) {
      // Keep the mapped fields for CIM extraction, but add additional raw fields
      mappedFieldList = new ArrayList<>(mappedFieldList);
      mappedFieldList.remove("_extra"); // Remove _extra from the field list
      mappedFieldList.add("*");
    }

    String fields = StringUtils.encodeList(mappedFieldList, ',').toString();
    args.put("field_list", fields);
    args.put("earliest_time", earliest);
    args.put("latest_time", latest);
    return args;
  }

  /**
   * Returns the field mapping for external use.
   */
  public Map<String, String> getFieldMapping() {
    return fieldMapping;
  }

  /**
   * Wrapper enumerator that applies type conversion to each row based on the expected schema.
   * This ensures that string values from Splunk are converted to the appropriate Java types
   * before being processed by Calcite.
   */
  private static class TypeConvertingEnumerator implements Enumerator<Object> {
    private final Enumerator<Object> underlying;
    private final RelDataType schema;
    private int rowCount = 0;

    public TypeConvertingEnumerator(Enumerator<Object> underlying, RelDataType schema) {
      this.underlying = underlying;
      this.schema = schema;

      // Print schema info once
//      System.out.println("=== Schema Info ===");
//      for (int i = 0; i < schema.getFieldList().size(); i++) {
//        RelDataTypeField field = schema.getFieldList().get(i);
//        System.out.println("Field[" + i + "]: " + field.getName() + " -> " + field.getType().getSqlTypeName());
//      }
//      System.out.println("===================");
    }

    @Override
    public Object current() {
      Object current = underlying.current();
      rowCount++;

      if (current instanceof Object[]) {
        Object[] inputRow = (Object[]) current;

        // Only debug first row to avoid spam
//        if (rowCount == 1) {
//          System.out.println("=== First Row Debug ===");
//          for (int i = 0; i < Math.min(inputRow.length, 3); i++) { // Only first 3 fields
//            Object value = inputRow[i];
//            System.out.println("Input[" + i + "]: " + value + " (" + (value != null ? value.getClass().getSimpleName() : "null") + ")");
//          }
//        }

        Object[] convertedRow = SplunkDataConverter.convertRow(inputRow, schema);

//        if (rowCount == 1) {
//          for (int i = 0; i < Math.min(convertedRow.length, 3); i++) { // Only first 3 fields
//            Object value = convertedRow[i];
//            System.out.println("Output[" + i + "]: " + value + " (" + (value != null ? value.getClass().getSimpleName() : "null") + ")");
//          }
//          System.out.println("====================");
//        }

        return convertedRow;
      }

      return current;
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
