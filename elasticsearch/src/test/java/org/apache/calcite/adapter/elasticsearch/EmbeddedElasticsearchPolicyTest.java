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

import org.apache.http.HttpEntity;
import org.apache.http.util.EntityUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceAccessMode;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Tests for {@link EmbeddedElasticsearchPolicy}.
 */
@ResourceLock(value = "elasticsearch-scrolls", mode = ResourceAccessMode.READ)
public class EmbeddedElasticsearchPolicyTest {

  private static final EmbeddedElasticsearchPolicy NODE =
      EmbeddedElasticsearchPolicy.create();

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6498">[CALCITE-6498]
   * Elasticsearch multi-field mappings do not work</a>. */
  @Test void testCreateIndexWithSimpleFieldMappings() throws Exception {
    final Map<String, String> mapping =
        ImmutableMap.of("a", "keyword", "b", "text", "c", "long");
    final String simpleMappingIndex = "index_simple_mapping";

    NODE.createIndex(simpleMappingIndex, mapping);

    final JsonNode properties = getMappings(simpleMappingIndex);

    assertThat(properties.path("a").path("type").asText(), is("keyword"));
    assertThat(properties.path("b").path("type").asText(), is("text"));
    assertThat(properties.path("c").path("type").asText(), is("long"));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6498">[CALCITE-6498]
   * Elasticsearch multi-field mappings do not work</a>. */
  @Test void testCreateIndexWithNestedFieldMappings() throws Exception {
    final Map<String, String> mapping =
        ImmutableMap.of("a", "nested", "a.b", "text", "a.c", "long");
    final String index = "index_nested_field_mappings";

    NODE.createIndex(index, mapping);

    final JsonNode properties = getMappings(index);

    assertThat(properties.path("a").path("type").asText(), is("nested"));
    assertThat(properties.path("a")
                          .path("properties")
                            .path("b").path("type").asText(), is("text"));
    assertThat(properties.path("a")
                          .path("properties")
                            .path("c").path("type").asText(), is("long"));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6498">[CALCITE-6498]
   * Elasticsearch multi-field mappings do not work</a>. */
  @Test void testCreateIndexWithMultiFieldMappings() throws Exception {
    final Map<String, String> mapping =
        ImmutableMap.of("a", "text", "a.keyword", "keyword");
    final String index = "index_multi_field_mappings";

    NODE.createIndex(index, mapping);

    final JsonNode properties = getMappings(index);

    assertThat(properties.path("a").path("type").asText(), is("text"));
    assertThat(properties.path("a")
                          .path("fields")
                            .path("keyword").path("type").asText(),
        is("keyword"));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6498">[CALCITE-6498]
   * Elasticsearch multi-field mappings do not work</a>. */
  @Test void testCreateIndexWithNestedFieldMappingsAndMultiFieldMappings()
      throws Exception {
    final Map<String, String> mapping =
        ImmutableMap.of("a", "nested", "a.b", "text", "a.b.keyword", "keyword");
    final String index = "index_nested_and_multi_field_mappings";

    NODE.createIndex(index, mapping);

    final JsonNode properties = getMappings(index);

    assertThat(properties.path("a").path("type").asText(), is("nested"));
    assertThat(properties.path("a")
                          .path("properties")
                            .path("b").path("type").asText(), is("text"));
    assertThat(properties.path("a")
                          .path("properties")
                            .path("b").path("fields")
                              .path("keyword")
                                .path("type").asText(), is("keyword"));
  }

  private static JsonNode getMappings(String index) throws IOException {
    Response indexMappingsResponse = NODE.restClient()
        .performRequest(new Request("GET", "/" + index + "/_mapping"));
    HttpEntity entity = indexMappingsResponse.getEntity();
    String responseBody = EntityUtils.toString(entity);
    JsonNode responseJson = new ObjectMapper().readTree(responseBody);

    // It's more readable to assert on a JsonNode than on a map, where you need
    // to cast a lot
    return responseJson.path(index).path("mappings").path("properties");
  }
}
