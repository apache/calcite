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

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Utility class to generate elastic search queries. Most query builders have
 * been copied from ES distribution. The reason we have separate definition is
 * high-level client dependency on core modules (like lucene, netty, XContent etc.) which
 * is not compatible between different major versions.
 *
 * <p>The goal of ES adapter is to
 * be compatible with any elastic version or even to connect to clusters with different
 * versions simultaneously.
 *
 * <p>Jackson API is used to generate ES query as JSON document.
 */
class QueryBuilders {

  private QueryBuilders() {}

  /**
   * A Query that matches documents containing a term.
   *
   * @param name  The name of the field
   * @param value The value of the term
   */
  static TermQueryBuilder termQuery(String name, String value) {
    return new TermQueryBuilder(name, value);
  }

  /**
   * A Query that matches documents containing a term.
   *
   * @param name  The name of the field
   * @param value The value of the term
   */
  static TermQueryBuilder termQuery(String name, int value) {
    return new TermQueryBuilder(name, value);
  }

  /**
   * A Query that matches documents containing a single character term.
   *
   * @param name  The name of the field
   * @param value The value of the term
   */
  static TermQueryBuilder termQuery(String name, char value) {
    return new TermQueryBuilder(name, value);
  }

  /**
   * A Query that matches documents containing a term.
   *
   * @param name  The name of the field
   * @param value The value of the term
   */
  static TermQueryBuilder termQuery(String name, long value) {
    return new TermQueryBuilder(name, value);
  }

  /**
   * A Query that matches documents containing a term.
   *
   * @param name  The name of the field
   * @param value The value of the term
   */
  static TermQueryBuilder termQuery(String name, float value) {
    return new TermQueryBuilder(name, value);
  }

  /**
   * A Query that matches documents containing a term.
   *
   * @param name  The name of the field
   * @param value The value of the term
   */
  static TermQueryBuilder termQuery(String name, double value) {
    return new TermQueryBuilder(name, value);
  }

  /**
   * A Query that matches documents containing a term.
   *
   * @param name  The name of the field
   * @param value The value of the term
   */
  static TermQueryBuilder termQuery(String name, boolean value) {
    return new TermQueryBuilder(name, value);
  }

  /**
   * A Query that matches documents containing a term.
   *
   * @param name  The name of the field
   * @param value The value of the term
   */
  static TermQueryBuilder termQuery(String name, Object value) {
    return new TermQueryBuilder(name, value);
  }

  /**
   * A filer for a field based on several terms matching on any of them.
   *
   * @param name   The field name
   * @param values The terms
   */
  static TermsQueryBuilder termsQuery(String name, Iterable<?> values) {
    return new TermsQueryBuilder(name, values);
  }

  /**
   * A Query that matches documents within an range of terms.
   *
   * @param name The field name
   */
  static RangeQueryBuilder rangeQuery(String name) {
    return new RangeQueryBuilder(name);
  }

  /**
   * A Query that matches documents containing terms with a specified regular expression.
   *
   * @param name   The name of the field
   * @param regexp The regular expression
   */
  static RegexpQueryBuilder regexpQuery(String name, String regexp) {
    return new RegexpQueryBuilder(name, regexp);
  }


  /**
   * A Query that matches documents matching boolean combinations of other queries.
   */
  static BoolQueryBuilder boolQuery() {
    return new BoolQueryBuilder();
  }

  /**
   * A query that wraps another query and simply returns a constant score equal to the
   * query boost for every document in the query.
   *
   * @param queryBuilder The query to wrap in a constant score query
   */
  static ConstantScoreQueryBuilder constantScoreQuery(QueryBuilder queryBuilder) {
    return new ConstantScoreQueryBuilder(queryBuilder);
  }

  /**
   * A filter to filter only documents where a field exists in them.
   *
   * @param name The name of the field
   */
  static ExistsQueryBuilder existsQuery(String name) {
    return new ExistsQueryBuilder(name);
  }

  /**
   * A query that matches on all documents.
   */
  static MatchAllQueryBuilder matchAll() {
    return new MatchAllQueryBuilder();
  }

  /**
   * Base class to build ES queries
   */
  abstract static class QueryBuilder {

    /**
     * Convert existing query to JSON format using jackson API.
     * @param generator used to generate JSON elements
     * @throws IOException if IO error occurred
     */
    abstract void writeJson(JsonGenerator generator) throws IOException;
  }

  /**
   * Query for boolean logic
   */
  static class BoolQueryBuilder extends QueryBuilder {
    private final List<QueryBuilder> mustClauses = new ArrayList<>();
    private final List<QueryBuilder> mustNotClauses = new ArrayList<>();
    private final List<QueryBuilder> filterClauses = new ArrayList<>();
    private final List<QueryBuilder> shouldClauses = new ArrayList<>();

    BoolQueryBuilder must(QueryBuilder queryBuilder) {
      Objects.requireNonNull(queryBuilder, "queryBuilder");
      mustClauses.add(queryBuilder);
      return this;
    }

    BoolQueryBuilder filter(QueryBuilder queryBuilder) {
      Objects.requireNonNull(queryBuilder, "queryBuilder");
      filterClauses.add(queryBuilder);
      return this;
    }

    BoolQueryBuilder mustNot(QueryBuilder queryBuilder) {
      Objects.requireNonNull(queryBuilder, "queryBuilder");
      mustNotClauses.add(queryBuilder);
      return this;
    }

    BoolQueryBuilder should(QueryBuilder queryBuilder) {
      Objects.requireNonNull(queryBuilder, "queryBuilder");
      shouldClauses.add(queryBuilder);
      return this;
    }

    @Override protected void writeJson(JsonGenerator gen) throws IOException {
      gen.writeStartObject();
      gen.writeFieldName("bool");
      gen.writeStartObject();
      writeJsonArray("must", mustClauses, gen);
      writeJsonArray("filter", filterClauses, gen);
      writeJsonArray("must_not", mustNotClauses, gen);
      writeJsonArray("should", shouldClauses, gen);
      gen.writeEndObject();
      gen.writeEndObject();
    }

    private void writeJsonArray(String field, List<QueryBuilder> clauses, JsonGenerator gen)
        throws IOException {
      if (clauses.isEmpty()) {
        return;
      }

      if (clauses.size() == 1) {
        gen.writeFieldName(field);
        clauses.get(0).writeJson(gen);
      } else {
        gen.writeArrayFieldStart(field);
        for (QueryBuilder clause: clauses) {
          clause.writeJson(gen);
        }
        gen.writeEndArray();
      }
    }
  }

  /**
   * A Query that matches documents containing a term.
   */
  static class TermQueryBuilder extends QueryBuilder {
    private final String fieldName;
    private final Object value;

    private TermQueryBuilder(final String fieldName, final Object value) {
      this.fieldName = Objects.requireNonNull(fieldName, "fieldName");
      this.value = Objects.requireNonNull(value, "value");
    }

    @Override void writeJson(final JsonGenerator generator) throws IOException {
      generator.writeStartObject();
      generator.writeFieldName("term");
      generator.writeStartObject();
      generator.writeFieldName(fieldName);
      writeObject(generator, value);
      generator.writeEndObject();
      generator.writeEndObject();
    }
  }

  /**
   * A filter for a field based on several terms matching on any of them.
   */
  private static class TermsQueryBuilder extends QueryBuilder {
    private final String fieldName;
    private final Iterable<?> values;

    private TermsQueryBuilder(final String fieldName, final Iterable<?> values) {
      this.fieldName = Objects.requireNonNull(fieldName, "fieldName");
      this.values = Objects.requireNonNull(values, "values");
    }

    @Override void writeJson(final JsonGenerator generator) throws IOException {
      generator.writeStartObject();
      generator.writeFieldName("terms");
      generator.writeStartObject();
      generator.writeFieldName(fieldName);
      generator.writeStartArray();
      for (Object value: values) {
        writeObject(generator, value);
      }
      generator.writeEndArray();
      generator.writeEndObject();
      generator.writeEndObject();
    }
  }

  /**
   * Write usually simple (scalar) value (string, number, boolean or null) to json output.
   * In case of complex objects delegates to jackson serialization.
   *
   * @param generator api to generate JSON document
   * @param value JSON value to write
   * @throws IOException if can't write to output
   */
  private static void writeObject(JsonGenerator generator, Object value) throws IOException {
    generator.writeObject(value);
  }

  /**
   * A Query that matches documents within an range of terms.
   */
  static class RangeQueryBuilder extends QueryBuilder {
    private final String fieldName;

    private Object lt;
    private boolean lte;
    private Object gt;
    private boolean gte;

    private String format;

    private RangeQueryBuilder(final String fieldName) {
      this.fieldName = Objects.requireNonNull(fieldName, "fieldName");
    }

    private RangeQueryBuilder to(Object value, boolean lte) {
      this.lt = Objects.requireNonNull(value, "value");
      this.lte = lte;
      return this;
    }

    private RangeQueryBuilder from(Object value, boolean gte) {
      this.gt = Objects.requireNonNull(value, "value");
      this.gte = gte;
      return this;
    }

    RangeQueryBuilder lt(Object value) {
      return to(value, false);
    }

    RangeQueryBuilder lte(Object value) {
      return to(value, true);
    }

    RangeQueryBuilder gt(Object value) {
      return from(value, false);
    }

    RangeQueryBuilder gte(Object value) {
      return from(value, true);
    }

    RangeQueryBuilder format(String format) {
      this.format = format;
      return this;
    }

    @Override void writeJson(final JsonGenerator generator) throws IOException {
      if (lt == null && gt == null) {
        throw new IllegalStateException("Either lower or upper bound should be provided");
      }

      generator.writeStartObject();
      generator.writeFieldName("range");
      generator.writeStartObject();
      generator.writeFieldName(fieldName);
      generator.writeStartObject();

      if (gt != null) {
        final String op = gte ? "gte" : "gt";
        generator.writeFieldName(op);
        writeObject(generator, gt);
      }

      if (lt != null) {
        final String op = lte ? "lte" : "lt";
        generator.writeFieldName(op);
        writeObject(generator, lt);
      }

      if (format != null) {
        generator.writeStringField("format", format);
      }

      generator.writeEndObject();
      generator.writeEndObject();
      generator.writeEndObject();
    }
  }

  /**
   * A Query that does fuzzy matching for a specific value.
   */
  static class RegexpQueryBuilder extends QueryBuilder {
    private final String fieldName;
    private final String value;

    RegexpQueryBuilder(final String fieldName, final String value) {
      this.fieldName = fieldName;
      this.value = value;
    }

    @Override void writeJson(final JsonGenerator generator) throws IOException {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * Constructs a query that only match on documents that the field has a value in them.
   */
  static class ExistsQueryBuilder extends QueryBuilder {
    private final String fieldName;

    ExistsQueryBuilder(final String fieldName) {
      this.fieldName = Objects.requireNonNull(fieldName, "fieldName");
    }

    @Override void writeJson(final JsonGenerator generator) throws IOException {
      generator.writeStartObject();
      generator.writeFieldName("exists");
      generator.writeStartObject();
      generator.writeStringField("field", fieldName);
      generator.writeEndObject();
      generator.writeEndObject();
    }
  }

  /**
   * A query that wraps a filter and simply returns a constant score equal to the
   * query boost for every document in the filter.
   */
  static class ConstantScoreQueryBuilder extends QueryBuilder {

    private final QueryBuilder builder;

    private ConstantScoreQueryBuilder(final QueryBuilder builder) {
      this.builder = Objects.requireNonNull(builder, "builder");
    }

    @Override void writeJson(final JsonGenerator generator) throws IOException {
      generator.writeStartObject();
      generator.writeFieldName("constant_score");
      generator.writeStartObject();
      generator.writeFieldName("filter");
      builder.writeJson(generator);
      generator.writeEndObject();
      generator.writeEndObject();
    }
  }

  /**
   * A query that matches on all documents.
   * <pre>
   *   {
   *     "match_all": {}
   *   }
   * </pre>
   */
  static class MatchAllQueryBuilder extends QueryBuilder {

    private MatchAllQueryBuilder() {}

    @Override void writeJson(final JsonGenerator generator) throws IOException {
      generator.writeStartObject();
      generator.writeFieldName("match_all");
      generator.writeStartObject();
      generator.writeEndObject();
      generator.writeEndObject();
    }
  }
}

// End QueryBuilders.java
