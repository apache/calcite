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

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Relational expression that uses Elasticsearch calling convention.
 */
public interface ElasticsearchRel extends RelNode {
  void implement(Implementor implementor);

  /**
   * Calling convention for relational operations that occur in Elasticsearch.
   */
  Convention CONVENTION = new Convention.Impl("ELASTICSEARCH", ElasticsearchRel.class);

  /**
   * Callback for the implementation process that converts a tree of
   * {@link ElasticsearchRel} nodes into an Elasticsearch query.
   */
  class Implementor {

    final List<String> list = new ArrayList<>();

    /**
     * Sorting clauses.
     * @see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-sort.html">Sort</a>
     */
    final List<Map.Entry<String, RelFieldCollation.Direction>> sort = new ArrayList<>();

    /**
     * Elastic aggregation ({@code MIN / MAX / COUNT} etc.) statements (functions).
     * @see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations.html">aggregations</a>
     */
    final List<Map.Entry<String, String>> aggregations = new ArrayList<>();

    /**
     * Allows bucketing documents together. Similar to {@code select ... from table group by field1}
     * @see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/6.3/search-aggregations-bucket.html">Bucket Aggregrations</a>
     */
    final List<String> groupBy = new ArrayList<>();

    /**
     * Keeps mapping between calcite expression identifier (like {@code EXPR$0}) and
     * original item call like {@code _MAP['foo.bar']} ({@code foo.bar} really).
     * This information otherwise might be lost during query translation.
     *
     * @see SqlStdOperatorTable#ITEM
     */
    final Map<String, String> expressionItemMap = new LinkedHashMap<>();

    /**
     * Starting index (default {@code 0}). Equivalent to {@code start} in ES query.
     * @see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-from-size.html">From/Size</a>
     */
    Long offset;

    /**
     * Number of records to return. Equivalent to {@code size} in ES query.
     * @see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-from-size.html">From/Size</a>
     */
    Long fetch;

    RelOptTable table;
    ElasticsearchTable elasticsearchTable;

    void add(String findOp) {
      list.add(findOp);
    }

    void addGroupBy(String field) {
      Objects.requireNonNull(field, "field");
      groupBy.add(field);
    }

    void addSort(String field, RelFieldCollation.Direction direction) {
      Objects.requireNonNull(field, "field");
      sort.add(new Pair<>(field, direction));
    }

    void addAggregation(String field, String expression) {
      Objects.requireNonNull(field, "field");
      Objects.requireNonNull(expression, "expression");
      aggregations.add(new Pair<>(field, expression));
    }

    void addExpressionItemMapping(String expressionId, String item) {
      Objects.requireNonNull(expressionId, "expressionId");
      Objects.requireNonNull(item, "item");
      expressionItemMap.put(expressionId, item);
    }

    void offset(long offset) {
      this.offset = offset;
    }

    void fetch(long fetch) {
      this.fetch = fetch;
    }

    void visitChild(int ordinal, RelNode input) {
      assert ordinal == 0;
      ((ElasticsearchRel) input).implement(this);
    }

  }
}

// End ElasticsearchRel.java
