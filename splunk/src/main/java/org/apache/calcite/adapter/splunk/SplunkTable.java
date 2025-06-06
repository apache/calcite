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

import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTableQueryable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Table based on Splunk.
 */
public class SplunkTable extends AbstractQueryableTable implements TranslatableTable {
  private final RelDataType rowType;
  private final Set<String> explicitFields;
  private final Map<String, String> fieldMapping;
  private final String searchString;

  public SplunkTable(RelDataType rowType) {
    this(rowType, Collections.emptyMap(), "search");
  }

  public SplunkTable(RelDataType rowType, Map<String, String> fieldMapping, String searchString) {
    super(Object[].class);
    this.rowType = rowType;
    this.fieldMapping = fieldMapping != null ? fieldMapping : Collections.emptyMap();
    this.searchString = searchString != null ? searchString : "search";
    // Extract the explicit field names, excluding "_extra" which is our catch-all field
    this.explicitFields = rowType.getFieldNames().stream()
        .filter(name -> !name.equals("_extra"))
        .collect(Collectors.toSet());
  }

  @Override public String toString() {
    return "SplunkTable";
  }

  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return rowType;
  }

  /**
   * Returns the set of explicitly defined field names (excluding "_extra").
   * This is used by the query processor to distinguish between fields that should
   * be extracted normally and fields that should be collected into the "_extra" JSON field.
   */
  public Set<String> getExplicitFields() {
    return explicitFields;
  }

  /**
   * Returns the field mapping from schema field names to Splunk field names.
   * For example: "reason" -> "Authentication.reason"
   */
  public Map<String, String> getFieldMapping() {
    return fieldMapping;
  }

  /**
   * Returns the search string for this table.
   */
  public String getSearchString() {
    return searchString;
  }

  /**
   * Maps a schema field name to the corresponding Splunk field name.
   * Returns the original name if no mapping exists.
   */
  public String mapSchemaFieldToSplunkField(String schemaFieldName) {
    return fieldMapping.getOrDefault(schemaFieldName, schemaFieldName);
  }

  /**
   * Maps a list of schema field names to Splunk field names.
   */
  public List<String> mapSchemaFieldsToSplunkFields(List<String> schemaFieldNames) {
    return schemaFieldNames.stream()
        .map(this::mapSchemaFieldToSplunkField)
        .collect(Collectors.toList());
  }

  /**
   * Creates a reverse mapping from Splunk field names to schema field names.
   * This is used when processing results to map back to schema field names.
   */
  public Map<String, String> createReverseFieldMapping() {
    Map<String, String> reverse = new HashMap<>();
    for (Map.Entry<String, String> entry : fieldMapping.entrySet()) {
      reverse.put(entry.getValue(), entry.getKey());
    }
    return reverse;
  }

  @Override public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
      SchemaPlus schema, String tableName) {
    return new SplunkTableQueryable<>(queryProvider, schema, this, tableName);
  }

  @Override public RelNode toRel(
      RelOptTable.ToRelContext context,
      RelOptTable relOptTable) {
    return new SplunkTableScan(
        context.getCluster(),
        relOptTable,
        this,
        searchString,
        "", // Use empty string instead of null for earliest
        "", // Use empty string instead of null for latest
        relOptTable.getRowType().getFieldNames());
  }

  /** Implementation of {@link Queryable} backed by a {@link SplunkTable}.
   * Generated code uses this get a Splunk connection for executing arbitrary
   * Splunk queries.
   *
   * @param <T> element type */
  public static class SplunkTableQueryable<T>
      extends AbstractTableQueryable<T> {

    SplunkTableQueryable(QueryProvider queryProvider, SchemaPlus schema,
        SplunkTable table, String tableName) {
      super(queryProvider, schema, table, tableName);
    }

    @Override public Enumerator<T> enumerator() {
      final SplunkTable splunkTable = (SplunkTable) table;
      final SplunkQuery<T> query = createQuery(
          splunkTable.getSearchString(), "", "", new ArrayList<>());
      return query.enumerator();
    }

    public SplunkQuery<T> createQuery(String search, String earliest,
        String latest, List<String> fieldList) {
      final SplunkSchema splunkSchema = schema.unwrap(SplunkSchema.class);
      if (splunkSchema == null) {
        throw new IllegalStateException("Schema is not a SplunkSchema");
      }
      final SplunkTable splunkTable = (SplunkTable) table;

      return new SplunkQuery<>(
          splunkSchema.splunkConnection,
          search,
          earliest,
          latest,
          fieldList,
          splunkTable.getExplicitFields(),
          splunkTable.getFieldMapping(),
          splunkTable.rowType);  // Pass the table's row type as schema
    }
  }
}
