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
package org.apache.calcite.adapter.elasticsearch;

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
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.calcite.sql.type.SqlTypeName;

import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Table based on an Elasticsearch type.
 */
public class ElasticsearchTable extends AbstractQueryableTable implements TranslatableTable {

  private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchTable.class);

  /**
   * Used for constructing (possibly nested) Elastic aggregation nodes.
   */
  private static final String AGGREGATIONS = "aggregations";

  private final RestClient restClient;
  private final ElasticsearchVersion version;
  private final String indexName;
  private final String typeName;
  final ObjectMapper mapper;

  /**
   * Creates an ElasticsearchTable.
   * @param client low-level ES rest client
   * @param mapper Jackson API
   * @param indexName elastic search index
   * @param typeName elastic searh index type
   */
  ElasticsearchTable(RestClient client, ObjectMapper mapper, String indexName, String typeName) {
    super(Object[].class);
    this.restClient = Objects.requireNonNull(client, "client");
    try {
      this.version = detectVersion(client, mapper);
    } catch (IOException e) {
      final String message = String.format(Locale.ROOT, "Couldn't detect ES version "
          + "for %s/%s", indexName, typeName);
      throw new UncheckedIOException(message, e);
    }
    this.indexName = Objects.requireNonNull(indexName, "indexName");
    this.typeName = Objects.requireNonNull(typeName, "typeName");
    this.mapper = Objects.requireNonNull(mapper, "mapper");

  }

  /**
   * Detects current Elastic Search version by connecting to a existing instance.
   * It is a {@code GET} request to {@code /}. Returned JSON has server information
   * (including version).
   *
   * @param client low-level rest client connected to ES instance
   * @param mapper Jackson mapper instance used to parse responses
   * @return parsed version from ES, or {@link ElasticsearchVersion#UNKNOWN}
   * @throws IOException if couldn't connect to ES
   */
  private static ElasticsearchVersion detectVersion(RestClient client, ObjectMapper mapper)
      throws IOException {
    HttpEntity entity = client.performRequest("GET", "/").getEntity();
    JsonNode node = mapper.readTree(EntityUtils.toString(entity));
    return ElasticsearchVersion.fromString(node.get("version").get("number").asText());
  }

  /**
   * In ES 5.x scripted fields start with {@code params._source.foo} while in ES2.x
   * {@code _source.foo}. Helper method to build correct query based on runtime version of elastic.
   * Used to keep backwards compatibility with ES2.
   *
   * @see <a href="https://github.com/elastic/elasticsearch/issues/20068">_source variable</a>
   * @see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/master/modules-scripting-fields.html">Scripted Fields</a>
   * @return string to be used for scripted fields
   */
  String scriptedFieldPrefix() {
    // ES2 vs ES5 scripted field difference
    return version == ElasticsearchVersion.ES2
        ? ElasticsearchConstants.SOURCE_GROOVY
        : ElasticsearchConstants.SOURCE_PAINLESS;
  }

  /**
   * Executes a "find" operation on the underlying type.
   *
   * <p>For example,
   * <code>client.prepareSearch(index).setTypes(type)
   * .setSource("{\"fields\" : [\"state\"]}")</code></p>
   *
   * @param ops List of operations represented as Json strings.
   * @param fields List of fields to project; or null to return map
   * @param sort list of fields to sort and their direction (asc/desc)
   * @param aggregations aggregation functions
   * @return Enumerator of results
   */
  protected Enumerable<Object> find(List<String> ops,
      List<Map.Entry<String, Class>> fields,
      List<Map.Entry<String, RelFieldCollation.Direction>> sort,
      List<String> groupBy,
      List<Map.Entry<String, String>> aggregations,
      Long offset, Long fetch) throws IOException {

    if (!aggregations.isEmpty()) {
      // process aggregations separately
      return aggregate(ops, fields, sort, groupBy, aggregations, offset, fetch);
    }

    final ObjectNode query = mapper.createObjectNode();
    // manually parse from previously concatenated string
    for (String op: ops) {
      query.setAll((ObjectNode) mapper.readTree(op));
    }

    if (!sort.isEmpty()) {
      ArrayNode sortNode = query.withArray("sort");
      sort.forEach(e ->
          sortNode.add(
              mapper.createObjectNode().put(e.getKey(),
                  e.getValue().isDescending() ? "desc" : "asc")));
    }

    if (offset != null) {
      query.put("from", offset);
    }

    if (fetch != null) {
      query.put("size", fetch);
    }

    try {
      ElasticsearchJson.Result search = httpRequest(query);
      final Function1<ElasticsearchJson.SearchHit, Object> getter =
          ElasticsearchEnumerators.getter(fields);
      return Linq4j.asEnumerable(search.searchHits().hits()).select(getter);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private Enumerable<Object> aggregate(List<String> ops,
      List<Map.Entry<String, Class>> fields,
      List<Map.Entry<String, RelFieldCollation.Direction>> sort,
      List<String> groupBy,
      List<Map.Entry<String, String>> aggregations,
      Long offset, Long fetch) throws IOException {

    if (aggregations.isEmpty()) {
      throw new IllegalArgumentException("Missing Aggregations");
    }

    if (!groupBy.isEmpty() && offset != null) {
      String message = "Currently ES doesn't support generic pagination "
          + "with aggregations. You can still use LIMIT keyword (without OFFSET). "
          + "For more details see https://github.com/elastic/elasticsearch/issues/4915";
      throw new IllegalStateException(message);
    }

    final ObjectNode query = mapper.createObjectNode();
    // manually parse into JSON from previously concatenated strings
    for (String op: ops) {
      query.setAll((ObjectNode) mapper.readTree(op));
    }

    // remove / override attributes which are not applicable to aggregations
    query.put("_source", false);
    query.put("size", 0);
    query.remove("script_fields");

    // allows to detect aggregation for count(*)
    final Predicate<Map.Entry<String, String>> isCountStar = e -> e.getValue()
            .contains("\"" + ElasticsearchConstants.ID + "\"");

    // list of expressions which are count(*)
    final Set<String> countAll = aggregations.stream()
            .filter(isCountStar)
        .map(Map.Entry::getKey).collect(Collectors.toSet());

    final Map<String, String> fieldMap = new HashMap<>();

    // due to ES aggregation format. fields in "order by" clause should go first
    // if "order by" is missing. order in "group by" is un-important
    final Set<String> orderedGroupBy = new LinkedHashSet<>();
    orderedGroupBy.addAll(sort.stream().map(Map.Entry::getKey).collect(Collectors.toList()));
    orderedGroupBy.addAll(groupBy);

    // construct nested aggregations node(s)
    ObjectNode parent = query.with(AGGREGATIONS);
    for (String name: orderedGroupBy) {
      final String aggName = "g_" + name;
      fieldMap.put(aggName, name);

      final ObjectNode section = parent.with(aggName);
      final ObjectNode terms = section.with("terms");
      terms.put("field", name);
      terms.set("missing", ElasticsearchJson.MISSING_VALUE); // expose missing terms

      if (fetch != null) {
        terms.put("size", fetch);
      }

      sort.stream().filter(e -> e.getKey().equals(name)).findAny().ifPresent(s -> {
        terms.with("order").put("_key", s.getValue().isDescending() ? "desc" : "asc");
      });

      parent = section.with(AGGREGATIONS);
    }

    // simple version for queries like "select count(*), max(col1) from table" (no GROUP BY cols)
    if (!groupBy.isEmpty() || !aggregations.stream().allMatch(isCountStar)) {
      for (Map.Entry<String, String> aggregation : aggregations) {
        JsonNode value = mapper.readTree(aggregation.getValue());
        parent.set(aggregation.getKey(), value);
      }
    }

    // cleanup query. remove empty AGGREGATIONS element (if empty)
    JsonNode agg = query;
    while (agg.has(AGGREGATIONS) && agg.get(AGGREGATIONS).elements().hasNext()) {
      agg = agg.get(AGGREGATIONS);
    }
    ((ObjectNode) agg).remove(AGGREGATIONS);

    ElasticsearchJson.Result res = httpRequest(query);

    final List<Map<String, Object>> result = new ArrayList<>();
    if (res.aggregations() != null) {
      // collect values
      ElasticsearchJson.visitValueNodes(res.aggregations(), m -> {
        Map<String, Object> newMap = new LinkedHashMap<>();
        for (String key: m.keySet()) {
          newMap.put(fieldMap.getOrDefault(key, key), m.get(key));
        }
        result.add(newMap);
      });
    } else {
      // probably no group by. add single result
      result.add(new LinkedHashMap<>());
    }

    // elastic exposes total number of documents matching a query in "/hits/total" path
    // this can be used for simple "select count(*) from table"
    final long total = res.searchHits().total();

    if (groupBy.isEmpty()) {
      // put totals automatically for count(*) expression(s), unless they contain group by
      for (String expr : countAll) {
        result.forEach(m -> m.put(expr, total));
      }
    }

    final Function1<ElasticsearchJson.SearchHit, Object> getter =
        ElasticsearchEnumerators.getter(fields);

    ElasticsearchJson.SearchHits hits =
        new ElasticsearchJson.SearchHits(total, result.stream()
            .map(r -> new ElasticsearchJson.SearchHit("_id", r, null))
            .collect(Collectors.toList()));

    return Linq4j.asEnumerable(hits.hits()).select(getter);
  }

  private ElasticsearchJson.Result httpRequest(ObjectNode query) throws IOException {
    Objects.requireNonNull(query, "query");
    String uri = String.format(Locale.ROOT, "/%s/%s/_search", indexName, typeName);

    Hook.QUERY_PLAN.run(query);
    final String json = mapper.writeValueAsString(query);

    LOGGER.debug("Elasticsearch Query: {}", json);

    HttpEntity entity = new StringEntity(json, ContentType.APPLICATION_JSON);
    Response response = restClient.performRequest("POST", uri, Collections.emptyMap(), entity);
    if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
      final String error = EntityUtils.toString(response.getEntity());
      final String message = String.format(Locale.ROOT,
          "Error while querying Elastic (on %s/%s) status: %s\nQuery:\n%s\nError:\n%s\n",
          response.getHost(), response.getRequestLine(), response.getStatusLine(), query, error);
      throw new RuntimeException(message);
    }

    try (InputStream is = response.getEntity().getContent()) {
      return mapper.readValue(is, ElasticsearchJson.Result.class);
    }
  }

  @Override public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
    final RelDataType mapType = relDataTypeFactory.createMapType(
        relDataTypeFactory.createSqlType(SqlTypeName.VARCHAR),
        relDataTypeFactory.createTypeWithNullability(
            relDataTypeFactory.createSqlType(SqlTypeName.ANY),
            true));
    return relDataTypeFactory.builder().add("_MAP", mapType).build();
  }

  @Override public String toString() {
    return "ElasticsearchTable{" + indexName + "/" + typeName + "}";
  }

  @Override public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema,
      String tableName) {
    return new ElasticsearchQueryable<>(queryProvider, schema, this, tableName);
  }

  @Override public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
    final RelOptCluster cluster = context.getCluster();
    return new ElasticsearchTableScan(cluster, cluster.traitSetOf(ElasticsearchRel.CONVENTION),
        relOptTable, this, null);
  }

  /**
   * Implementation of {@link Queryable} based on
   * a {@link ElasticsearchTable}.
   *
   * @param <T> element type
   */
  public static class ElasticsearchQueryable<T> extends AbstractTableQueryable<T> {
    ElasticsearchQueryable(QueryProvider queryProvider, SchemaPlus schema,
        ElasticsearchTable table, String tableName) {
      super(queryProvider, schema, table, tableName);
    }

    public Enumerator<T> enumerator() {
      return null;
    }

    private ElasticsearchTable getTable() {
      return (ElasticsearchTable) table;
    }

    /** Called via code-generation.
     * @param ops list of queries (as strings)
     * @param fields projection
     * @see ElasticsearchMethod#ELASTICSEARCH_QUERYABLE_FIND
     * @return result as enumerable
     */
    @SuppressWarnings("UnusedDeclaration")
    public Enumerable<Object> find(List<String> ops,
         List<Map.Entry<String, Class>> fields,
         List<Map.Entry<String, RelFieldCollation.Direction>> sort,
         List<String> groupBy,
         List<Map.Entry<String, String>> aggregations,
         Long offset, Long fetch) {
      try {
        return getTable().find(ops, fields, sort, groupBy, aggregations, offset, fetch);
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to query " + getTable().indexName, e);
      }
    }

  }
}

// End ElasticsearchTable.java
