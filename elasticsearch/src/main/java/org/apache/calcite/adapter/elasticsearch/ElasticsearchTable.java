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
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.calcite.sql.type.SqlTypeName;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Table based on an Elasticsearch index.
 */
public class ElasticsearchTable extends AbstractQueryableTable implements TranslatableTable {

  private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchTable.class);

  /**
   * Used for constructing (possibly nested) Elastic aggregation nodes.
   */
  private static final String AGGREGATIONS = "aggregations";

  private final ElasticsearchVersion version;
  private final String indexName;
  final ObjectMapper mapper;
  final ElasticsearchTransport transport;

  /**
   * Creates an ElasticsearchTable.
   */
  ElasticsearchTable(ElasticsearchTransport transport) {
    super(Object[].class);
    this.transport = Objects.requireNonNull(transport, "transport");
    this.version = transport.version;
    this.indexName = transport.indexName;
    this.mapper = transport.mapper();
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
   * Executes a "find" operation on the underlying index.
   *
   * @param ops List of operations represented as Json strings.
   * @param fields List of fields to project; or null to return map
   * @param sort list of fields to sort and their direction (asc/desc)
   * @param aggregations aggregation functions
   * @return Enumerator of results
   */
  private Enumerable<Object> find(List<String> ops,
      List<Map.Entry<String, Class>> fields,
      List<Map.Entry<String, RelFieldCollation.Direction>> sort,
      List<String> groupBy,
      List<Map.Entry<String, String>> aggregations,
      Map<String, String> mappings,
      Long offset, Long fetch) throws IOException {

    if (!aggregations.isEmpty() || !groupBy.isEmpty()) {
      // process aggregations separately
      return aggregate(ops, fields, sort, groupBy, aggregations, mappings, offset, fetch);
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

    final Function1<ElasticsearchJson.SearchHit, Object> getter =
        ElasticsearchEnumerators.getter(fields, ImmutableMap.copyOf(mappings));

    Iterable<ElasticsearchJson.SearchHit> iter;
    if (offset == null) {
      // apply scrolling when there is no offsets
      iter = () -> new Scrolling(transport).query(query);
    } else {
      final ElasticsearchJson.Result search = transport.search().apply(query);
      iter = () -> search.searchHits().hits().iterator();
    }

    return Linq4j.asEnumerable(iter).select(getter);
  }

  private Enumerable<Object> aggregate(List<String> ops,
      List<Map.Entry<String, Class>> fields,
      List<Map.Entry<String, RelFieldCollation.Direction>> sort,
      List<String> groupBy,
      List<Map.Entry<String, String>> aggregations,
      Map<String, String> mapping,
      Long offset, Long fetch) throws IOException {

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

      transport.mapping.missingValueFor(name).ifPresent(m -> {
        // expose missing terms. each type has a different missing value
        terms.set("missing", m);
      });

      if (fetch != null) {
        terms.put("size", fetch);
      }

      sort.stream().filter(e -> e.getKey().equals(name)).findAny()
          .ifPresent(s ->
              terms.with("order")
                  .put("_key", s.getValue().isDescending() ? "desc" : "asc"));

      parent = section.with(AGGREGATIONS);
    }

    // simple version for queries like "select count(*), max(col1) from table" (no GROUP BY cols)
    if (!groupBy.isEmpty() || !aggregations.stream().allMatch(isCountStar)) {
      for (Map.Entry<String, String> aggregation : aggregations) {
        JsonNode value = mapper.readTree(aggregation.getValue());
        parent.set(aggregation.getKey(), value);
      }
    }

    final Consumer<JsonNode> emptyAggRemover = new Consumer<JsonNode>() {
      @Override public void accept(JsonNode node) {
        if (!node.has(AGGREGATIONS)) {
          node.elements().forEachRemaining(this);
          return;
        }
        JsonNode agg = node.get(AGGREGATIONS);
        if (agg.size() == 0) {
          ((ObjectNode) node).remove(AGGREGATIONS);
        } else {
          this.accept(agg);
        }
      }
    };

    // cleanup query. remove empty AGGREGATIONS element (if empty)
    emptyAggRemover.accept(query);

    // This must be set to true or else in 7.X and 6/7 mixed clusters
    // will return lower bounded count values instead of an accurate count.
    if (groupBy.isEmpty()
        && version.elasticVersionMajor() >= ElasticsearchVersion.ES6.elasticVersionMajor()) {
      query.put("track_total_hits", true);
    }

    ElasticsearchJson.Result res = transport.search(Collections.emptyMap()).apply(query);

    final List<Map<String, Object>> result = new ArrayList<>();
    if (res.aggregations() != null) {
      // collect values
      ElasticsearchJson.visitValueNodes(res.aggregations(), m -> {
        // using 'Collectors.toMap' will trigger Java 8 bug here
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
    final long total = res.searchHits().total().value();

    if (groupBy.isEmpty()) {
      // put totals automatically for count(*) expression(s), unless they contain group by
      for (String expr : countAll) {
        result.forEach(m -> m.put(expr, total));
      }
    }

    final Function1<ElasticsearchJson.SearchHit, Object> getter =
        ElasticsearchEnumerators.getter(fields, ImmutableMap.copyOf(mapping));

    ElasticsearchJson.SearchHits hits =
        new ElasticsearchJson.SearchHits(res.searchHits().total(), result.stream()
            .map(r -> new ElasticsearchJson.SearchHit("_id", r, null))
            .collect(Collectors.toList()));

    return Linq4j.asEnumerable(hits.hits()).select(getter);
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
    return "ElasticsearchTable{" + indexName + "}";
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
         Map<String, String> mappings,
         Long offset, Long fetch) {
      try {
        return getTable().find(ops, fields, sort, groupBy, aggregations, mappings, offset, fetch);
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to query " + getTable().indexName, e);
      }
    }

  }
}

// End ElasticsearchTable.java
