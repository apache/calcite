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

import org.apache.calcite.runtime.Hook;

import org.apache.http.HttpEntity;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpRequest;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.collect.ImmutableMap;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.StreamSupport;

/**
 * Set of predefined functions for REST interaction with elastic search API. Performs
 * HTTP requests and JSON (de)serialization.
 */
final class ElasticsearchTransport {

  private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchTable.class);

  static final int DEFAULT_FETCH_SIZE = 5196;

  private final ObjectMapper mapper;
  private final RestClient restClient;

  final String indexName;

  final ElasticsearchVersion version;

  final ElasticsearchMapping mapping;

  /**
   * Default batch size
   * @see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-scroll.html">Scrolling API</a>
   */
  final int fetchSize;

  ElasticsearchTransport(final RestClient restClient,
                         final ObjectMapper mapper,
                         final String indexName,
                         final int fetchSize) {
    this.mapper = Objects.requireNonNull(mapper, "mapper");
    this.restClient = Objects.requireNonNull(restClient, "restClient");
    this.indexName = Objects.requireNonNull(indexName, "indexName");
    this.fetchSize = fetchSize;
    this.version = version(); // cache version
    this.mapping = fetchAndCreateMapping(); // cache mapping
  }

  RestClient restClient() {
    return this.restClient;
  }

  /**
   * Detects current Elastic Search version by connecting to a existing instance.
   * It is a {@code GET} request to {@code /}. Returned JSON has server information
   * (including version).
   *
   * @return parsed version from ES, or {@link ElasticsearchVersion#UNKNOWN}
   */
  private ElasticsearchVersion version() {
    final HttpRequest request = new HttpGet("/");
    // version extract function
    final Function<ObjectNode, ElasticsearchVersion> fn = node -> ElasticsearchVersion.fromString(
        node.get("version").get("number").asText());
    return rawHttp(ObjectNode.class)
        .andThen(fn)
        .apply(request);
  }

  /**
   * Build index mapping returning new instance of {@link ElasticsearchMapping}.
   */
  private ElasticsearchMapping fetchAndCreateMapping() {
    final String uri = String.format(Locale.ROOT, "/%s/_mapping", indexName);
    final ObjectNode root = rawHttp(ObjectNode.class).apply(new HttpGet(uri));
    ObjectNode properties = (ObjectNode) root.elements().next().get("mappings");

    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    ElasticsearchJson.visitMappingProperties(properties, builder::put);
    return new ElasticsearchMapping(indexName, builder.build());
  }

  ObjectMapper mapper() {
    return mapper;
  }

  Function<HttpRequest, Response> rawHttp() {
    return new HttpFunction(restClient);
  }

  <T> Function<HttpRequest, T> rawHttp(Class<T> responseType) {
    Objects.requireNonNull(responseType, "responseType");
    return rawHttp().andThen(new JsonParserFn<>(mapper, responseType));
  }

  /**
   * Fetches search results given a scrollId.
   */
  Function<String, ElasticsearchJson.Result> scroll() {
    return scrollId -> {
      // fetch next scroll
      final HttpPost request = new HttpPost(URI.create("/_search/scroll"));
      final ObjectNode payload = mapper.createObjectNode()
          .put("scroll", "1m")
          .put("scroll_id", scrollId);

      try {
        final String json = mapper.writeValueAsString(payload);
        request.setEntity(new StringEntity(json, ContentType.APPLICATION_JSON));
        return rawHttp(ElasticsearchJson.Result.class).apply(request);
      } catch (IOException e) {
        String message = String.format(Locale.ROOT, "Couldn't fetch next scroll %s", scrollId);
        throw new UncheckedIOException(message, e);
      }
    };

  }

  void closeScroll(Iterable<String> scrollIds) {
    Objects.requireNonNull(scrollIds, "scrollIds");

    // delete current scroll
    final URI uri = URI.create("/_search/scroll");
    // http DELETE with payload
    final HttpEntityEnclosingRequestBase request = new HttpEntityEnclosingRequestBase() {
      @Override public String getMethod() {
        return HttpDelete.METHOD_NAME;
      }
    };

    request.setURI(uri);
    final ObjectNode payload = mapper().createObjectNode();
    // ES2 expects json array for DELETE scroll API
    final ArrayNode array = payload.withArray("scroll_id");

    StreamSupport.stream(scrollIds.spliterator(), false)
        .map(TextNode::new)
        .forEach(array::add);

    try {
      final String json = mapper().writeValueAsString(payload);
      request.setEntity(new StringEntity(json, ContentType.APPLICATION_JSON));
      rawHttp().apply(request);
    } catch (IOException | UncheckedIOException e) {
      LOGGER.warn("Failed to close scroll(s): {}", scrollIds, e);
    }
  }

  Function<ObjectNode, ElasticsearchJson.Result> search() {
    return search(Collections.emptyMap());
  }

  /**
   * Search request using HTTP post.
   */
  Function<ObjectNode, ElasticsearchJson.Result> search(final Map<String, String> httpParams) {
    Objects.requireNonNull(httpParams, "httpParams");
    return query -> {
      Hook.QUERY_PLAN.run(query);
      String path = String.format(Locale.ROOT, "/%s/_search", indexName);
      final HttpPost post;
      try {
        URIBuilder builder = new URIBuilder(path);
        httpParams.forEach(builder::addParameter);
        post = new HttpPost(builder.build());
        final String json = mapper.writeValueAsString(query);
        LOGGER.debug("Elasticsearch Query: {}", json);
        post.setEntity(new StringEntity(json, ContentType.APPLICATION_JSON));
      } catch (URISyntaxException e) {
        throw new RuntimeException(e);
      } catch (JsonProcessingException e) {
        throw new UncheckedIOException(e);
      }

      return rawHttp(ElasticsearchJson.Result.class).apply(post);
    };
  }

  /**
   * Parses HTTP response into some class using jackson API.
   * @param <T> result type
   */
  private static class JsonParserFn<T> implements Function<Response, T> {
    private final ObjectMapper mapper;
    private final Class<T> klass;

    JsonParserFn(final ObjectMapper mapper, final Class<T> klass) {
      this.mapper = mapper;
      this.klass = klass;
    }

    @Override public T apply(final Response response) {
      try (InputStream is = response.getEntity().getContent()) {
        return mapper.readValue(is, klass);
      } catch (IOException e) {
        final String message = String.format(Locale.ROOT,
            "Couldn't parse HTTP response %s into %s", response, klass);
        throw new UncheckedIOException(message, e);
      }
    }
  }

  /**
   * Basic rest operations interacting with elastic cluster.
   */
  private static class HttpFunction implements Function<HttpRequest, Response> {

    private final RestClient restClient;

    HttpFunction(final RestClient restClient) {
      this.restClient = Objects.requireNonNull(restClient, "restClient");
    }

    @Override public Response apply(final HttpRequest request) {
      try {
        return applyInternal(request);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    private Response applyInternal(final HttpRequest request)
        throws IOException  {

      Objects.requireNonNull(request, "request");
      final HttpEntity entity = request instanceof HttpEntityEnclosingRequest
          ? ((HttpEntityEnclosingRequest) request).getEntity() : null;

      final Request r = new Request(
          request.getRequestLine().getMethod(),
          request.getRequestLine().getUri());
      r.setEntity(entity);
      final Response response = restClient.performRequest(r);

      final String payload = entity != null && entity.isRepeatable()
          ? EntityUtils.toString(entity) : "<empty>";

      if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
        final String error = EntityUtils.toString(response.getEntity());

        final String message = String.format(Locale.ROOT,
            "Error while querying Elastic (on %s/%s) status: %s\nPayload:\n%s\nError:\n%s\n",
            response.getHost(), response.getRequestLine(),
            response.getStatusLine(), payload, error);
        throw new RuntimeException(message);
      }

      return response;
    }
  }
}

// End ElasticsearchTransport.java
