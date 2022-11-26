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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.google.common.collect.ImmutableMap;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Stores Elasticsearch
 * <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping.html">
 * mapping</a> information for particular index. This information is
 * extracted from {@code /$index/_mapping} endpoint.
 *
 * <p>Instances of this class are immutable.
 */
class ElasticsearchMapping {

  private final String index;

  private final Map<String, Datatype> mapping;

  ElasticsearchMapping(final String index,
      final Map<String, String> mapping) {
    this.index = Objects.requireNonNull(index, "index");
    Objects.requireNonNull(mapping, "mapping");

    final Map<String, Datatype> transformed = mapping.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> new Datatype(e.getValue())));
    this.mapping = ImmutableMap.copyOf(transformed);
  }

  /**
   * Returns ES schema for each field. Mapping is represented as field name
   * {@code foo.bar.qux} and type ({@code keyword}, {@code boolean},
   * {@code long}).
   *
   * @return immutable mapping between field and ES type
   *
   * @see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-types.html">Mapping Types</a>
   */
  Map<String, Datatype> mapping() {
    return this.mapping;
  }

  /**
   * Used as special aggregation key for missing values (documents that are
   * missing a field). MAX and MIN value are chosen to determine nulls sort direction.
   *
   * <p>Buckets with that value are then converted to {@code null}s in flat
   * tabular format.
   *
   * @see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-sum-aggregation.html">Missing Value</a>
   */
  Optional<JsonNode> missingValueFor(String fieldName, boolean isMin) {
    if (!mapping().containsKey(fieldName)) {
      final String message = String.format(Locale.ROOT,
          "Field %s not defined for %s", fieldName, index);
      throw new IllegalArgumentException(message);
    }

    if (isMin) {
      return mapping().get(fieldName).minMissingValue();
    }
    return mapping().get(fieldName).maxMissingValue();
  }

  String index() {
    return this.index;
  }

  /**
   * Represents elastic data-type, like {@code long}, {@code keyword},
   * {@code date} etc.
   *
   * @see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-types.html">Mapping Types</a>
   */
  static class Datatype {
    private static final JsonNodeFactory FACTORY = JsonNodeFactory.instance;

    // pre-cache missing values
    private static final Set<JsonNode> MIN_MISSING_VALUES =
        Stream.of("string", // for ES2
            "text", "keyword",
            "date", "long", "integer", "double", "float")
            .map(Datatype::minMissingValueForType)
            .collect(Collectors.toSet());

    private static final Set<JsonNode> MAX_MISSING_VALUES =
        Stream.of("string", // for ES2
                "text", "keyword",
                "date", "long", "integer", "double", "float")
            .map(Datatype::maxMissingValueForType)
            .collect(Collectors.toSet());

    private final String name;
    private final JsonNode minMissingValue;
    private final JsonNode maxMissingValue;

    private Datatype(final String name) {
      this.name = Objects.requireNonNull(name, "name");
      this.minMissingValue = minMissingValueForType(name);
      this.maxMissingValue = maxMissingValueForType(name);
    }

    /**
     * Mapping between ES type and json value that represents
     * {@code missing value} during aggregations. This value can't be
     * {@code null} and should match type or the field (for ES long type it
     * also has to be json integer, for date it has to match date format or be
     * integer (millis epoch) etc.
     *
     * <p>It is used for terms aggregations to represent SQL {@code null}.
     *
     * @param name name of the type ({@code long}, {@code keyword} ...)
     *
     * @return json that will be used in elastic search terms aggregation for
     * missing value
     *
     * @see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-terms-aggregation.html#_missing_value_13">Missing Value</a>
     */
    private static @Nullable JsonNode minMissingValueForType(String name) {
      switch (name) {
      case "string": // for ES2
      case "text":
      case "keyword":
        return FACTORY.textNode("");
      case "long":
        return FACTORY.numberNode(Long.MIN_VALUE);
      case "integer":
        return FACTORY.numberNode(Integer.MIN_VALUE);
      case "short":
        return FACTORY.numberNode(Short.MIN_VALUE);
      case "double":
        return FACTORY.numberNode(Double.MIN_VALUE);
      case "float":
        return FACTORY.numberNode(Float.MIN_VALUE);
      case "date":
        // sentinel for missing dates: 1-1-1
        final long millisEpoch = LocalDate.of(1, 1, 1)
            .atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli();
        // by default elastic returns dates as longs
        return FACTORY.numberNode(millisEpoch);
      default:
        break;
      }

      // this is unknown type
      return null;
    }

    private static @Nullable JsonNode maxMissingValueForType(String name) {
      switch (name) {
      case "string": // for ES2
      case "text":
      case "keyword":
        return FACTORY.textNode("~~~~~~~~");
      case "long":
        return FACTORY.numberNode(Long.MAX_VALUE);
      case "integer":
        return FACTORY.numberNode(Integer.MAX_VALUE);
      case "short":
        return FACTORY.numberNode(Short.MAX_VALUE);
      case "double":
        return FACTORY.numberNode(Double.MAX_VALUE);
      case "float":
        return FACTORY.numberNode(Float.MAX_VALUE);
      case "date":
        // sentinel for missing dates: 9999-12-30
        final long millisEpoch = LocalDate.of(9999, 12, 31)
            .atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli();
        // by default elastic returns dates as longs
        return FACTORY.numberNode(millisEpoch);
      default:
        break;
      }

      // this is unknown type
      return null;
    }

    /**
     * Name of the type: {@code text}, {@code integer}, {@code float} etc.
     */
    String name() {
      return this.name;
    }

    Optional<JsonNode> minMissingValue() {
      return Optional.ofNullable(minMissingValue);
    }

    Optional<JsonNode> maxMissingValue() {
      return Optional.ofNullable(maxMissingValue);
    }

    static boolean isMissingValue(JsonNode node) {
      return MIN_MISSING_VALUES.contains(node) || MAX_MISSING_VALUES.contains(node);
    }
  }

}
