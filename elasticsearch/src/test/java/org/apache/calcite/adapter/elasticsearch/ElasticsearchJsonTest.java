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
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Testing correct parsing of JSON (elasticsearch) response.
 */
class ElasticsearchJsonTest {

  private ObjectMapper mapper;

  @BeforeEach
  public void setUp() {
    this.mapper = new ObjectMapper()
        .configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true)
        .configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
  }

  @Test void aggEmpty() throws Exception {
    String json = "{}";

    ElasticsearchJson.Aggregations a = mapper.readValue(json, ElasticsearchJson.Aggregations.class);
    assertNotNull(a);
    assertThat(a.asList(), hasSize(0));
    assertThat(a.asMap(), aMapWithSize(0));
  }

  @Test void aggSingle1() throws Exception {
    String json = "{agg1: {value: '111'}}";

    ElasticsearchJson.Aggregations a = mapper.readValue(json, ElasticsearchJson.Aggregations.class);
    assertNotNull(a);
    assertThat(a.asList(), hasSize(1));
    assertThat(a.asMap(), aMapWithSize(1));
    assertThat(a.asList().get(0).getName(), is("agg1"));
    assertThat(a.asMap().keySet().iterator().next(), is("agg1"));
    assertThat(((ElasticsearchJson.MultiValue) a.asList().get(0)).value(),
        is("111"));

    List<Map<String, Object>> rows = new ArrayList<>();
    ElasticsearchJson.visitValueNodes(a, rows::add);
    assertThat(rows, hasSize(1));
    assertThat(rows.get(0).get("agg1"), is("111"));
  }

  @Test void aggMultiValues() throws Exception {
    String json = "{ agg1: {min: 0, max: 2, avg: 2.33}}";
    ElasticsearchJson.Aggregations a = mapper.readValue(json, ElasticsearchJson.Aggregations.class);
    assertNotNull(a);
    assertThat(a.asList(), hasSize(1));
    assertThat(a.asMap(), aMapWithSize(1));
    assertThat(a.asList().get(0).getName(), is("agg1"));

    Map<String, Object> values = ((ElasticsearchJson.MultiValue) a.get("agg1")).values();
    assertThat(values.keySet(), hasItems("min", "max", "avg"));
  }

  @Test void aggSingle2() throws Exception {
    String json = "{ agg1: {value: 'foo'}, agg2: {value: 42}}";

    ElasticsearchJson.Aggregations a = mapper.readValue(json, ElasticsearchJson.Aggregations.class);
    assertNotNull(a);
    assertThat(a.asList(), hasSize(2));
    assertThat(a.asMap(), aMapWithSize(2));
    assertThat(a.asMap().keySet(), hasItems("agg1", "agg2"));
  }

  @Test void aggBuckets1() throws Exception {
    String json = "{ groupby: {buckets: [{key:'k1', doc_count:0, myagg:{value: 1.1}},"
        + " {key:'k2', myagg:{value: 2.2}}] }}";

    ElasticsearchJson.Aggregations a =
        mapper.readValue(json, ElasticsearchJson.Aggregations.class);

    assertThat(a.asMap().keySet(), hasItem("groupby"));
    assertThat(a.get("groupby"),
        instanceOf(ElasticsearchJson.MultiBucketsAggregation.class));
    ElasticsearchJson.MultiBucketsAggregation multi = a.get("groupby");
    assertThat(multi.buckets(), hasSize(2));
    assertThat(multi.getName(), is("groupby"));
    assertThat(multi.buckets().get(0).key(), is("k1"));
    assertThat(multi.buckets().get(0).keyAsString(), is("k1"));
    assertThat(multi.buckets().get(1).key(), is("k2"));
    assertThat(multi.buckets().get(1).keyAsString(), is("k2"));
  }

  @Test void aggManyAggregations() throws Exception {
    String json = "{groupby:{buckets:["
        + "{key:'k1', a1:{value:1}, a2:{value:2}},"
        + "{key:'k2', a1:{value:3}, a2:{value:4}}"
        + "]}}";

    ElasticsearchJson.Aggregations a =
        mapper.readValue(json, ElasticsearchJson.Aggregations.class);
    ElasticsearchJson.MultiBucketsAggregation multi = a.get("groupby");

    assertThat(multi.buckets().get(0).getAggregations().asMap(),
        aMapWithSize(2));
    assertThat(multi.buckets().get(0).getName(), is("groupby"));
    assertThat(multi.buckets().get(0).key(), is("k1"));
    assertThat(multi.buckets().get(0).getAggregations().asMap().keySet(),
        hasItems("a1", "a2"));
    assertThat(multi.buckets().get(1).getAggregations().asMap(),
        aMapWithSize(2));
    assertThat(multi.buckets().get(1).getName(), is("groupby"));
    assertThat(multi.buckets().get(1).key(), is("k2"));
    assertThat(multi.buckets().get(1).getAggregations().asMap().keySet(),
        hasItems("a1", "a2"));
    List<Map<String, Object>> rows = new ArrayList<>();
    ElasticsearchJson.visitValueNodes(a, rows::add);
    assertThat(rows, hasSize(2));
    assertThat(rows.get(0).get("groupby"), is("k1"));
    assertThat(rows.get(0).get("a1"), is(1));
    assertThat(rows.get(0).get("a2"), is(2));
  }

  @Test void aggMultiBuckets() throws Exception {
    String json = "{col1: {buckets: ["
        + "{col2: {doc_count:1, buckets:[{key:'k3', max:{value:41}}]}, key:'k1'},"
        + "{col2: {buckets:[{key:'k4', max:{value:42}}], doc_count:1}, key:'k2'}"
        + "]}}";

    ElasticsearchJson.Aggregations a = mapper.readValue(json, ElasticsearchJson.Aggregations.class);
    assertNotNull(a);

    assertThat(a.asMap().keySet(), hasItem("col1"));
    assertThat(a.get("col1"),
        instanceOf(ElasticsearchJson.MultiBucketsAggregation.class));
    ElasticsearchJson.MultiBucketsAggregation m = a.get("col1");
    assertThat(m.getName(), is("col1"));
    assertThat(m.buckets(), hasSize(2));
    assertThat(m.buckets().get(0).key(), is("k1"));
    assertThat(m.buckets().get(0).getName(), is("col1"));
    assertThat(m.buckets().get(0).getAggregations().asMap().keySet(), hasItem("col2"));
    assertThat(m.buckets().get(1).key(), is("k2"));
    List<Map<String, Object>> rows = new ArrayList<>();
    ElasticsearchJson.visitValueNodes(a, rows::add);
    assertThat(rows, hasSize(2));

    assertThat(rows.get(0).keySet(), hasItems("col1", "col2", "max"));
    assertThat(rows.get(0).get("col1"), is("k1"));
    assertThat(rows.get(0).get("col2"), is("k3"));
    assertThat(rows.get(0).get("max"), is(41));

    assertThat(rows.get(1).keySet(), hasItems("col1", "col2", "max"));
    assertThat(rows.get(1).get("col1"), is("k2"));
    assertThat(rows.get(1).get("col2"), is("k4"));
    assertThat(rows.get(1).get("max"), is(42));
  }

  /**
   * Validate that property names which are reserved keywords ES
   * are correctly mapped (e.g. {@code type} or {@code properties})
   */
  @Test void reservedKeywordMapping() throws Exception {
    // have special property names: type and properties
    ObjectNode mapping = mapper.readValue("{properties:{"
        + "type:{type:'text'},"
        + "keyword:{type:'keyword'},"
        + "properties:{type:'long'}"
        + "}}", ObjectNode.class);
    Map<String, String> result = new HashMap<>();
    ElasticsearchJson.visitMappingProperties(mapping, result::put);

    assertThat(result.get("type"), is("text"));
    assertThat(result.get("keyword"), is("keyword"));
    assertThat(result.get("properties"), is("long"));
  }


  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5974">[CALCITE-5974]
   * Elasticsearch adapter throws ClassCastException when index mapping sets
   * dynamic_templates without properties</a>. */
  @Test void reservedEmptyPropertiesMapping() throws Exception {
    // have special property names: type and properties
    ObjectNode mapping =
        mapper.readValue("{dynamic_templates:["
            + "{integers:"
            + "{match_mapping_type:'long',mapping:{type:'integer'}}"
            + "}]}", ObjectNode.class);

    // The 'dynamic_templates' object has no 'properties' field,
    // so the result is empty.
    Map<String, String> result = new HashMap<>();
    ElasticsearchJson.visitMappingProperties(mapping, result::put);
    assertThat(result, anEmptyMap());
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6498">[CALCITE-6498]
   * Elasticsearch multi-field mappings do not work</a>. */
  @Test void testVisitMappingPropertiesWithMultipleSingleFieldMappings()
      throws JsonProcessingException {
    ObjectNode mapping =
        mapper.readValue("{'properties':{"
            + "'title':{'type':'text'},"
            + "'name':{'type':'keyword'}"
            + "}}", ObjectNode.class);

    Map<String, String> result = getMappingAsMap(mapping);

    assertThat(result.get("title"), is("text"));
    assertThat(result.get("name"), is("keyword"));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6498">[CALCITE-6498]
   * Elasticsearch multi-field mappings do not work</a>. */
  @Test void testVisitMappingPropertiesWithMultipleMultiFieldMappings()
      throws Exception {
    ObjectNode mapping =
        mapper.readValue("{'properties':{"
            + "'title':{'type':'text',"
            +   "'fields':{'keyword':{'type': 'keyword'}}"
            + "},"
            + "'name':{'type':'text',"
            +   "'fields':{'name_keyword':{'type': 'keyword'}}"
            + "}"
            + "}}", ObjectNode.class);

    Map<String, String> result = getMappingAsMap(mapping);

    assertThat(result.get("title"), is("text"));
    assertThat(result.get("title.keyword"), is("keyword"));
    assertThat(result.get("name"), is("text"));
    assertThat(result.get("name.name_keyword"), is("keyword"));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6498">[CALCITE-6498]
   * Elasticsearch multi-field mappings do not work</a>. */
  @Test void testVisitMappingPropertiesWithMultipleNestedFieldMappings()
      throws Exception {
    ObjectNode mapping =
        mapper.readValue("{properties:{"
        + "'author':{'type':'nested',"
        +   "'properties':{"
        +     "'name':{'type':'text'},"
        +     "'age':{'type':'integer'}"
        +   "}},"
        + "'address':{'type':'nested',"
        +   "'properties':{"
        +     "'street':{'type':'keyword'},"
        +     "'zip':{'type':'integer'}"
        +   "}}"
        + "}}", ObjectNode.class);

    Map<String, String> result = getMappingAsMap(mapping);

    assertThat(result.get("author"), is("nested"));
    assertThat(result.get("author.name"), is("text"));
    assertThat(result.get("author.age"), is("integer"));

    assertThat(result.get("address"), is("nested"));
    assertThat(result.get("address.street"), is("keyword"));
    assertThat(result.get("address.zip"), is("integer"));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6498">[CALCITE-6498]
   * Elasticsearch multi-field mappings do not work</a>. */
  @Test void testVisitMappingPropertiesWithNestedAndMultiFieldMappings()
      throws Exception {
    // 'title' is a multi-mapped field
    // 'author' is a nested field ('author.name' is multi-mapped)
    ObjectNode mapping =
        mapper.readValue("{properties:{"
            + "'title':{'type':'text',"
            +   "'fields':{'keyword':{'type': 'keyword'}}"
            + "},"
            + "'author':{'type':'nested',"
            +   "'properties':{"
            +     "'name':{'type':'text',"
            +       "'fields':{'keyword':{'type': 'keyword'}}},"
            +     "'age':{'type':'integer'}"
            +   "}"
            + "}}}", ObjectNode.class);

    Map<String, String> result = getMappingAsMap(mapping);

    // Checking the multi-field mapping
    assertThat(result.get("title"), is("text"));
    assertThat(result.get("title.keyword"), is("keyword"));

    // Checking the nested mapping
    assertThat(result.get("author"), is("nested"));
    assertThat(result.get("author.name"), is("text"));
    assertThat(result.get("author.name.keyword"), is("keyword"));
    assertThat(result.get("author.age"), is("integer"));
  }

  private static Map<String, String> getMappingAsMap(ObjectNode mapping) {
    // ImmutableMap.Builder makes sure that we don't add the same key twice
    // (would throw exception otherwise)
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    ElasticsearchJson.visitMappingProperties(mapping, builder::put);

    return builder.build();
  }
}
