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
package org.apache.calcite.adapter.openapi;

import static java.util.Objects.requireNonNull;

import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.calcite.sql.type.SqlTypeName;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;

/**
 * Table based on an OpenAPI endpoint.
 */
public class OpenAPITable extends AbstractQueryableTable implements TranslatableTable {

  private final String tableName;
  private final OpenAPITransport transport;
  private final ObjectMapper mapper;
  private final OpenAPIConfig config;

  /**
   * Creates an OpenAPITable.
   */
  OpenAPITable(String tableName, OpenAPITransport transport, OpenAPIConfig config) {
    super(Object[].class);
    this.tableName = requireNonNull(tableName, "tableName");
    this.transport = requireNonNull(transport, "transport");
    this.config = requireNonNull(config, "config");
    this.mapper = transport.mapper();
  }

  /**
   * Executes a "find" operation on the underlying API endpoint.
   * This method signature matches what the converter expects.
   *
   * @param filters Map of filter conditions (field name -> value)
   * @param fields List of fields to project; or null to return all
   * @param sort list of fields to sort and their direction (asc/desc)
   * @param offset Starting offset for pagination
   * @param fetch Maximum number of results to fetch
   * @return Enumerable of results
   */
  private Enumerable<Object> find(Map<String, Object> filters,
      List<Map.Entry<String, Class>> fields,
      List<Map.Entry<String, RelFieldCollation.Direction>> sort,
      Long offset, Long fetch) throws IOException {

    // Find matching variant based on available filters
    OpenAPIConfig.Variant variant = findMatchingVariant(filters);
    if (variant == null) {
      throw new IllegalArgumentException(
          "No endpoint variant supports filtering by: " + filters.keySet() +
              ". Available variants require: " +
              config.getVariants().stream()
                  .map(v -> v.getRequiredFilters().toString())
                  .reduce((a, b) -> a + ", " + b)
                  .orElse("none"));
    }

    // Build and execute API request
    OpenAPIRequest request = buildRequest(variant, filters, fields, sort, offset, fetch);
    JsonNode response = transport.execute(request);

    // Extract data array from response
    JsonNode dataArray = extractDataArray(response, variant);

    // Convert to enumerable using appropriate getter function
    Function1<JsonNode, Object> getter = OpenAPIEnumerators.getter(fields);

    return Linq4j.asEnumerable(() -> dataArray.elements())
        .select(getter);
  }

  private OpenAPIConfig.Variant findMatchingVariant(Map<String, Object> filters) {
    return config.getVariants().stream()
        .filter(variant -> variant.getRequiredFilters().stream()
            .allMatch(filters::containsKey))
        .max(
            (v1, v2) -> Integer.compare(
            v1.getRequiredFilters().size(),
            v2.getRequiredFilters().size()))
        .orElse(null);
  }

  private OpenAPIRequest buildRequest(OpenAPIConfig.Variant variant,
      Map<String, Object> filters,
      List<Map.Entry<String, Class>> fields,
      List<Map.Entry<String, RelFieldCollation.Direction>> sort,
      Long offset, Long fetch) {

    OpenAPIRequest.Builder builder = OpenAPIRequest.builder()
        .variant(variant)
        .transport(transport);

    // Add required path parameters
    for (String requiredFilter : variant.getRequiredFilters()) {
      if (filters.containsKey(requiredFilter)) {
        builder.pathParam(requiredFilter, filters.get(requiredFilter));
      }
    }

    // Add optional query parameters for filters
    for (String optionalFilter : variant.getOptionalFilters()) {
      if (filters.containsKey(optionalFilter)) {
        builder.queryParam(optionalFilter, filters.get(optionalFilter));
      }
    }

    // Add projection pushdown if this variant supports it
    if (fields != null && variant.getProjectionPushdown() != null) {
      builder.projection(fields, variant.getProjectionPushdown());
    }

    // Add sort pushdown if this variant supports it (only first sort column)
    if (sort != null && !sort.isEmpty() && variant.getSortPushdown() != null) {
      builder.sort(sort.get(0), variant.getSortPushdown());
    }

    // Add pagination
    if (offset != null || fetch != null) {
      builder.pagination(offset, fetch);
    }

    return builder.build();
  }

  private JsonNode extractDataArray(JsonNode response, OpenAPIConfig.Variant variant) {
    String arrayPath = variant.getArrayPath();
    if (arrayPath == null || arrayPath.isEmpty()) {
      return response; // Response is the array itself
    }

    // Simple JSONPath-like navigation: "data.items"
    JsonNode current = response;
    for (String segment : arrayPath.split("\\.")) {
      current = current.get(segment);
      if (current == null) {
        throw new IllegalStateException("Array path not found: " + arrayPath);
      }
    }
    return current;
  }

  @Override public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
    // Return a generic map type - actual schema would be derived from OpenAPI spec
    final RelDataType mapType =
        relDataTypeFactory.createMapType(
            relDataTypeFactory.createSqlType(SqlTypeName.VARCHAR),
            relDataTypeFactory.createTypeWithNullability(
                relDataTypeFactory.createSqlType(SqlTypeName.ANY),
                true));
    return relDataTypeFactory.builder().add("_MAP", mapType).build();
  }

  @Override public String toString() {
    return "OpenAPITable{" + tableName + "}";
  }

  @Override public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema,
      String tableName) {
    return new OpenAPIQueryable<>(queryProvider, schema, this, tableName);
  }

  @Override public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
    final RelOptCluster cluster = context.getCluster();
    return new OpenAPITableScan(cluster, cluster.traitSetOf(OpenAPIRel.CONVENTION),
        relOptTable, this, null);
  }

  /**
   * Implementation of {@link Queryable} based on a {@link OpenAPITable}.
   *
   * @param <T> element type
   */
  public static class OpenAPIQueryable<T> extends AbstractTableQueryable<T> {
    OpenAPIQueryable(QueryProvider queryProvider, SchemaPlus schema,
        OpenAPITable table, String tableName) {
      super(queryProvider, schema, table, tableName);
    }

    @Override public Enumerator<T> enumerator() {
      return null;
    }

    private OpenAPITable getTable() {
      return (OpenAPITable) table;
    }

    /** Called via code-generation.
     *
     * @param filters map of filter conditions
     * @param fields projection
     * @param sort sort specifications
     * @param offset pagination offset
     * @param fetch pagination limit
     * @see OpenAPIMethod#OPENAPI_QUERYABLE_FIND
     * @return result as enumerable
     */
    @SuppressWarnings("UnusedDeclaration")
    public Enumerable<Object> find(Map<String, Object> filters,
        List<Map.Entry<String, Class>> fields,
        List<Map.Entry<String, RelFieldCollation.Direction>> sort,
        Long offset, Long fetch) {
      try {
        return getTable().find(filters, fields, sort, offset, fetch);
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to query " + getTable().tableName, e);
      }
    }
  }
}
