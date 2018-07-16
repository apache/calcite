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

import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.util.Util;

import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Table based on an Elasticsearch type.
 */
public class ElasticsearchTable extends AbstractElasticsearchTable {
  private final RestClient restClient;
  private final ElasticsearchVersion version;


  /**
   * Creates an ElasticsearchTable.
   * @param client low-level ES rest client
   * @param mapper Jackson API
   * @param indexName elastic search index
   * @param typeName elastic searh index type
   */
  ElasticsearchTable(RestClient client, ObjectMapper mapper, String indexName, String typeName) {
    super(indexName, typeName, Objects.requireNonNull(mapper, "mapper"));
    this.restClient = Objects.requireNonNull(client, "client");
    try {
      this.version = detectVersion(client, mapper);
    } catch (IOException e) {
      final String message = String.format(Locale.ROOT, "Couldn't detect ES version "
          + "for %s/%s", indexName, typeName);
      throw new UncheckedIOException(message, e);
    }

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

  @Override protected String scriptedFieldPrefix() {
    // ES2 vs ES5 scripted field difference
    return version == ElasticsearchVersion.ES2
        ? ElasticsearchConstants.SOURCE_GROOVY
        : ElasticsearchConstants.SOURCE_PAINLESS;
  }

  @Override protected Enumerable<Object> find(String index, List<String> ops,
      List<Map.Entry<String, Class>> fields) {

    final String query;
    if (!ops.isEmpty()) {
      query = "{" + Util.toString(ops, "", ", ", "") + "}";
    } else {
      query = "{}";
    }

    try {
      ElasticsearchSearchResult result = httpRequest(query);
      final Function1<ElasticsearchSearchResult.SearchHit, Object> getter =
          ElasticsearchEnumerators.getter(fields);
      return Linq4j.asEnumerable(result.searchHits().hits()).select(getter);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private ElasticsearchSearchResult httpRequest(String query) throws IOException {
    Objects.requireNonNull(query, "query");
    String uri = String.format(Locale.ROOT, "/%s/%s/_search", indexName, typeName);
    HttpEntity entity = new StringEntity(query, ContentType.APPLICATION_JSON);
    Response response = restClient.performRequest("POST", uri, Collections.emptyMap(), entity);
    if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
      final String error = EntityUtils.toString(response.getEntity());
      final String message = String.format(Locale.ROOT,
          "Error while querying Elastic (on %s/%s) status: %s\nQuery:\n%s\nError:\n%s\n",
          response.getHost(), response.getRequestLine(), response.getStatusLine(), query, error);
      throw new RuntimeException(message);
    }

    try (InputStream is = response.getEntity().getContent()) {
      return mapper.readValue(is, ElasticsearchSearchResult.class);
    }
  }
}

// End ElasticsearchTable.java
