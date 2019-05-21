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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableSet;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.StreamSupport;

import static java.util.Collections.unmodifiableMap;

/**
 * Internal objects (and deserializers) used to parse Elasticsearch results
 * (which are in JSON format).
 *
 * <p>Since we're using basic row-level rest client http response has to be
 * processed manually using JSON (jackson) library.
 */
final class ElasticsearchJson {

  private ElasticsearchJson() {}

  /**
   * Visits leaves of the aggregation where all values are stored.
   */
  static void visitValueNodes(Aggregations aggregations, Consumer<Map<String, Object>> consumer) {
    Objects.requireNonNull(aggregations, "aggregations");
    Objects.requireNonNull(consumer, "consumer");

    Map<RowKey, List<MultiValue>> rows = new LinkedHashMap<>();

    BiConsumer<RowKey, MultiValue> cons = (r, v) ->
        rows.computeIfAbsent(r, ignore -> new ArrayList<>()).add(v);
    aggregations.forEach(a -> visitValueNodes(a, new ArrayList<>(), cons));
    rows.forEach((k, v) -> {
      if (v.stream().allMatch(val -> val instanceof GroupValue)) {
        v.forEach(tuple -> {
          Map<String, Object> groupRow = new LinkedHashMap<>(k.keys);
          groupRow.put(tuple.getName(), tuple.value());
          consumer.accept(groupRow);
        });
      } else {
        Map<String, Object> row = new LinkedHashMap<>(k.keys);
        v.forEach(val -> row.put(val.getName(), val.value()));
        consumer.accept(row);
      }
    });
  }

  /**
   * Visits Elasticsearch
   * <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping.html">mapping
   * properties</a> and calls consumer for each {@code field / type} pair.
   * Nested fields are represented as {@code foo.bar.qux}.
   */
  static void visitMappingProperties(ObjectNode mapping,
      BiConsumer<String, String> consumer) {
    Objects.requireNonNull(mapping, "mapping");
    Objects.requireNonNull(consumer, "consumer");
    visitMappingProperties(new ArrayDeque<>(), mapping, consumer);
  }

  private static void visitMappingProperties(Deque<String> path,
      ObjectNode mapping, BiConsumer<String, String> consumer) {
    Objects.requireNonNull(mapping, "mapping");
    if (mapping.isMissingNode()) {
      return;
    }

    if (mapping.has("properties")) {
      // recurse
      visitMappingProperties(path, (ObjectNode) mapping.get("properties"), consumer);
      return;
    }

    if (mapping.has("type")) {
      // this is leaf (register field / type mapping)
      consumer.accept(String.join(".", path), mapping.get("type").asText());
      return;
    }

    // otherwise continue visiting mapping(s)
    Iterable<Map.Entry<String, JsonNode>> iter = mapping::fields;
    for (Map.Entry<String, JsonNode> entry : iter) {
      final String name = entry.getKey();
      final ObjectNode node = (ObjectNode) entry.getValue();
      path.add(name);
      visitMappingProperties(path, node, consumer);
      path.removeLast();
    }
  }


  /**
   * Identifies a calcite row (as in relational algebra)
   */
  private static class RowKey {
    private final Map<String, Object> keys;
    private final int hashCode;

    private RowKey(final Map<String, Object> keys) {
      this.keys = Objects.requireNonNull(keys, "keys");
      this.hashCode = Objects.hashCode(keys);
    }

    private RowKey(List<Bucket> buckets) {
      this(toMap(buckets));
    }

    private static Map<String, Object> toMap(Iterable<Bucket> buckets) {
      return StreamSupport.stream(buckets.spliterator(), false)
          .collect(LinkedHashMap::new,
              (m, v) -> m.put(v.getName(), v.key()),
              LinkedHashMap::putAll);
    }

    @Override public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final RowKey rowKey = (RowKey) o;
      return hashCode == rowKey.hashCode
          && Objects.equals(keys, rowKey.keys);
    }

    @Override public int hashCode() {
      return this.hashCode;
    }
  }

  private static void visitValueNodes(Aggregation aggregation, List<Bucket> parents,
      BiConsumer<RowKey, MultiValue> consumer) {

    if (aggregation instanceof MultiValue) {
      // this is a leaf. publish value of the row.
      RowKey key = new RowKey(parents);
      consumer.accept(key, (MultiValue) aggregation);
      return;
    }

    if (aggregation instanceof Bucket) {
      Bucket bucket = (Bucket) aggregation;
      if (bucket.hasNoAggregations()) {
        // bucket with no aggregations is also considered a leaf node
        visitValueNodes(GroupValue.of(bucket.getName(), bucket.key()), parents, consumer);
        return;
      }
      parents.add(bucket);
      bucket.getAggregations().forEach(a -> visitValueNodes(a, parents, consumer));
      parents.remove(parents.size() - 1);
    } else if (aggregation instanceof HasAggregations) {
      HasAggregations children = (HasAggregations) aggregation;
      children.getAggregations().forEach(a -> visitValueNodes(a, parents, consumer));
    } else if (aggregation instanceof MultiBucketsAggregation) {
      MultiBucketsAggregation multi = (MultiBucketsAggregation) aggregation;
      multi.buckets().forEach(b -> visitValueNodes(b, parents, consumer));
    }

  }

  /**
   * Response from Elastic
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  static class Result {
    private final SearchHits hits;
    private final Aggregations aggregations;
    private final String scrollId;
    private final long took;

    /**
     * Constructor for this instance.
     * @param hits list of matched documents
     * @param took time taken (in took) for this query to execute
     */
    @JsonCreator
    Result(@JsonProperty("hits") SearchHits hits,
        @JsonProperty("aggregations") Aggregations aggregations,
        @JsonProperty("_scroll_id") String scrollId,
        @JsonProperty("took") long took) {
      this.hits = Objects.requireNonNull(hits, "hits");
      this.aggregations = aggregations;
      this.scrollId = scrollId;
      this.took = took;
    }

    SearchHits searchHits() {
      return hits;
    }

    Aggregations aggregations() {
      return aggregations;
    }

    Duration took() {
      return Duration.ofMillis(took);
    }

    Optional<String> scrollId() {
      return Optional.ofNullable(scrollId);
    }

  }

  /**
   * Similar to {@code SearchHits} in ES. Container for {@link SearchHit}
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  static class SearchHits {

    private final SearchTotal total;
    private final List<SearchHit> hits;

    @JsonCreator
    SearchHits(@JsonProperty("total")final SearchTotal total,
               @JsonProperty("hits") final List<SearchHit> hits) {
      this.total = total;
      this.hits = Objects.requireNonNull(hits, "hits");
    }

    public List<SearchHit> hits() {
      return this.hits;
    }

    public SearchTotal total() {
      return total;
    }

  }

  /**
   * Container for total hits
   */
  @JsonDeserialize(using = SearchTotalDeserializer.class)
  static class SearchTotal {

    private final long value;

    SearchTotal(final long value) {
      this.value = value;
    }

    public long value() {
      return value;
    }

  }

  /**
   * Allows to de-serialize total hits structures.
   */
  static class SearchTotalDeserializer extends StdDeserializer<SearchTotal> {

    SearchTotalDeserializer() {
      super(SearchTotal.class);
    }

    @Override public SearchTotal deserialize(final JsonParser parser,
                                             final DeserializationContext ctxt)
        throws IOException  {

      JsonNode node = parser.getCodec().readTree(parser);
      return parseSearchTotal(node);
    }

    private static SearchTotal parseSearchTotal(JsonNode node) {

      final Number value;
      if (node.isNumber()) {
        value = node.numberValue();
      } else {
        value = node.get("value").numberValue();
      }

      return new SearchTotal(value.longValue());
    }

  }

  /**
   * Concrete result record which matched the query. Similar to {@code SearchHit} in ES.
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  static class SearchHit {

    /**
     * ID of the document (not available in aggregations)
     */
    private final String id;
    private final Map<String, Object> source;
    private final Map<String, Object> fields;

    @JsonCreator
    SearchHit(@JsonProperty(ElasticsearchConstants.ID) final String id,
                      @JsonProperty("_source") final Map<String, Object> source,
                      @JsonProperty("fields") final Map<String, Object> fields) {
      this.id = Objects.requireNonNull(id, "id");

      // both can't be null
      if (source == null && fields == null) {
        final String message = String.format(Locale.ROOT,
            "Both '_source' and 'fields' are missing for %s", id);
        throw new IllegalArgumentException(message);
      }

      // both can't be non-null
      if (source != null && fields != null) {
        final String message = String.format(Locale.ROOT,
            "Both '_source' and 'fields' are populated (non-null) for %s", id);
        throw new IllegalArgumentException(message);
      }

      this.source = source;
      this.fields = fields;
    }

    /**
     * Returns id of this hit (usually document id)
     * @return unique id
     */
    public String id() {
      return id;
    }

    Object valueOrNull(String name) {
      Objects.requireNonNull(name, "name");

      // for "select *" return whole document
      if (ElasticsearchConstants.isSelectAll(name)) {
        return sourceOrFields();
      }

      if (fields != null && fields.containsKey(name)) {
        Object field = fields.get(name);
        if (field instanceof Iterable) {
          // return first element (or null)
          Iterator<?> iter = ((Iterable<?>) field).iterator();
          return iter.hasNext() ? iter.next() : null;
        }

        return field;
      }

      return valueFromPath(source, name);
    }

    /**
     * Returns property from nested maps given a path like {@code a.b.c}.
     * @param map current map
     * @param path field path(s), optionally with dots ({@code a.b.c}).
     * @return value located at path {@code path} or {@code null} if not found.
     */
    private static Object valueFromPath(Map<String, Object> map, String path) {
      if (map == null) {
        return null;
      }

      if (map.containsKey(path)) {
        return map.get(path);
      }

      // maybe pattern of type a.b.c
      final int index = path.indexOf('.');
      if (index == -1) {
        return null;
      }

      final String prefix = path.substring(0, index);
      final String suffix = path.substring(index + 1);

      Object maybeMap = map.get(prefix);
      if (maybeMap instanceof Map) {
        return valueFromPath((Map<String, Object>) maybeMap, suffix);
      }

      return null;
    }

    Map<String, Object> source() {
      return source;
    }

    Map<String, Object> fields() {
      return fields;
    }

    Map<String, Object> sourceOrFields() {
      return source != null ? source : fields;
    }
  }


  /**
   * {@link Aggregation} container.
   */
  @JsonDeserialize(using = AggregationsDeserializer.class)
  static class Aggregations implements Iterable<Aggregation> {

    private final List<? extends Aggregation> aggregations;
    private Map<String, Aggregation> aggregationsAsMap;

    Aggregations(List<? extends Aggregation> aggregations) {
      this.aggregations = Objects.requireNonNull(aggregations, "aggregations");
    }

    /**
     * Iterates over the {@link Aggregation}s.
     */
    @Override public final Iterator<Aggregation> iterator() {
      return asList().iterator();
    }

    /**
     * The list of {@link Aggregation}s.
     */
    final List<Aggregation> asList() {
      return Collections.unmodifiableList(aggregations);
    }

    /**
     * Returns the {@link Aggregation}s keyed by aggregation name. Lazy init.
     */
    final Map<String, Aggregation> asMap() {
      if (aggregationsAsMap == null) {
        Map<String, Aggregation> map = new LinkedHashMap<>(aggregations.size());
        for (Aggregation aggregation : aggregations) {
          map.put(aggregation.getName(), aggregation);
        }
        this.aggregationsAsMap = unmodifiableMap(map);
      }
      return aggregationsAsMap;
    }

    /**
     * Returns the aggregation that is associated with the specified name.
     */
    @SuppressWarnings("unchecked")
    public final <A extends Aggregation> A get(String name) {
      return (A) asMap().get(name);
    }

    @Override public final boolean equals(Object obj) {
      if (obj == null || getClass() != obj.getClass()) {
        return false;
      }
      return aggregations.equals(((Aggregations) obj).aggregations);
    }

    @Override public final int hashCode() {
      return Objects.hash(getClass(), aggregations);
    }

  }

  /**
   * Identifies all aggregations
   */
  interface Aggregation {

    /**
     * @return The name of this aggregation.
     */
    String getName();

  }

  /**
   * Allows traversing aggregations tree
   */
  interface HasAggregations {
    Aggregations getAggregations();
  }

  /**
   * An aggregation that returns multiple buckets
   */
  static class MultiBucketsAggregation implements Aggregation {

    private final String name;
    private final List<Bucket> buckets;

    MultiBucketsAggregation(final String name,
        final List<Bucket> buckets) {
      this.name = name;
      this.buckets = buckets;
    }

    /**
     * @return  The buckets of this aggregation.
     */
    List<Bucket> buckets() {
      return buckets;
    }

    @Override public String getName() {
      return name;
    }
  }

  /**
   * A bucket represents a criteria to which all documents that fall in it adhere to.
   * It is also uniquely identified
   * by a key, and can potentially hold sub-aggregations computed over all documents in it.
   */
  static class Bucket implements HasAggregations, Aggregation {
    private final Object key;
    private final String name;
    private final Aggregations aggregations;

    Bucket(final Object key,
        final String name,
        final Aggregations aggregations) {
      this.key = key; // key can be set after construction
      this.name = Objects.requireNonNull(name, "name");
      this.aggregations = Objects.requireNonNull(aggregations, "aggregations");
    }

    /**
     * @return The key associated with the bucket
     */
    Object key() {
      return key;
    }

    /**
     * @return The key associated with the bucket as a string
     */
    String keyAsString() {
      return Objects.toString(key());
    }

    /**
     * Means current bucket has no aggregations.
     */
    boolean hasNoAggregations() {
      return aggregations.asList().isEmpty();
    }

    /**
     * @return  The sub-aggregations of this bucket
     */
    @Override public Aggregations getAggregations() {
      return aggregations;
    }

    @Override public String getName() {
      return name;
    }
  }

  /**
   * Multi value aggregatoin like
   * <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-stats-aggregation.html">Stats</a>
   */
  static class MultiValue implements Aggregation {
    private final String name;
    private final Map<String, Object> values;

    MultiValue(final String name, final Map<String, Object> values) {
      this.name = Objects.requireNonNull(name, "name");
      this.values = Objects.requireNonNull(values, "values");
    }

    @Override public String getName() {
      return name;
    }

    Map<String, Object> values() {
      return values;
    }

    /**
     * For single value. Returns single value represented by this leaf aggregation.
     * @return value corresponding to {@code value}
     */
    Object value() {
      if (!values().containsKey("value")) {
        String message = String.format(Locale.ROOT, "'value' field not present in "
            + "%s aggregation", getName());

        throw new IllegalStateException(message);
      }

      return values().get("value");
    }

  }

  /**
   * Distinguishes from {@link MultiValue}.
   * In order that rows which have the same key can be put into result map.
   */
  static class GroupValue extends MultiValue {
    GroupValue(String name, Map<String, Object> values) {
      super(name, values);
    }

    /**
     * Constructs a {@link GroupValue} instance with a single value.
     */
    static GroupValue of(String name, Object value) {
      return new GroupValue(name, Collections.singletonMap("value", value));
    }
  }

  /**
   * Allows to de-serialize nested aggregation structures.
   */
  static class AggregationsDeserializer extends StdDeserializer<Aggregations> {

    private static final Set<String> IGNORE_TOKENS =
        ImmutableSet.of("meta", "buckets", "value", "values", "value_as_string",
            "doc_count", "key", "key_as_string");

    AggregationsDeserializer() {
      super(Aggregations.class);
    }

    @Override public Aggregations deserialize(final JsonParser parser,
        final DeserializationContext ctxt)
        throws IOException  {

      ObjectNode node = parser.getCodec().readTree(parser);
      return parseAggregations(parser, node);
    }

    private static Aggregations parseAggregations(JsonParser parser, ObjectNode node)
        throws JsonProcessingException {

      List<Aggregation> aggregations = new ArrayList<>();

      Iterable<Map.Entry<String, JsonNode>> iter = node::fields;
      for (Map.Entry<String, JsonNode> entry : iter) {
        final String name = entry.getKey();
        final JsonNode value = entry.getValue();

        Aggregation agg = null;
        if (value.has("buckets")) {
          agg = parseBuckets(parser, name, (ArrayNode) value.get("buckets"));
        } else if (value.isObject() && !IGNORE_TOKENS.contains(name)) {
          // leaf
          agg = parseValue(parser, name, (ObjectNode) value);
        }

        if (agg != null) {
          aggregations.add(agg);
        }
      }

      return new Aggregations(aggregations);
    }



    private static MultiValue parseValue(JsonParser parser, String name, ObjectNode node)
        throws JsonProcessingException {

      return new MultiValue(name, parser.getCodec().treeToValue(node, Map.class));
    }

    private static Aggregation parseBuckets(JsonParser parser, String name, ArrayNode nodes)
        throws JsonProcessingException {

      List<Bucket> buckets = new ArrayList<>(nodes.size());
      for (JsonNode b: nodes) {
        buckets.add(parseBucket(parser, name, (ObjectNode) b));
      }

      return new MultiBucketsAggregation(name, buckets);
    }

    /**
     * Determines if current key is a missing field key. Missing key is returned when document
     * does not have pivoting attribute (example {@code GROUP BY _MAP['a.b.missing']}). It helps
     * grouping documents which don't have a field. In relational algebra this
     * would normally be {@code null}.
     *
     * <p>Please note that missing value is different for each type.
     *
     * @param key current {@code key} (usually string) as returned by ES
     * @return {@code true} if this value
     */
    private static boolean isMissingBucket(JsonNode key) {
      return ElasticsearchMapping.Datatype.isMissingValue(key);
    }

    private static Bucket parseBucket(JsonParser parser, String name, ObjectNode node)
        throws JsonProcessingException  {

      if (!node.has("key")) {
        throw new IllegalArgumentException("No 'key' attribute for " + node);
      }

      final JsonNode keyNode = node.get("key");
      final Object key;
      if (isMissingBucket(keyNode) || keyNode.isNull()) {
        key = null;
      } else if (keyNode.isTextual()) {
        key = keyNode.textValue();
      } else if (keyNode.isNumber()) {
        key = keyNode.numberValue();
      } else if (keyNode.isBoolean()) {
        key = keyNode.booleanValue();
      } else {
        // don't usually expect keys to be Objects
        key = parser.getCodec().treeToValue(node, Map.class);
      }

      return new Bucket(key, name, parseAggregations(parser, node));
    }

  }

}

// End ElasticsearchJson.java
