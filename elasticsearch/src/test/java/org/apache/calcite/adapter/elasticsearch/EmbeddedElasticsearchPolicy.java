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

import org.apache.calcite.util.Closer;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.transport.TransportAddress;
import org.junit.rules.ExternalResource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Used to initialize a single elastic node. For performance reasons (node startup costs),
 * same instance is usually shared across multiple tests.
 *
 * <p>This rule should be used as follows:
 * <pre>
 *  public class MyTest {
 *    &#64;ClassRule
 *    public static final EmbeddedElasticsearchPolicy RULE = EmbeddedElasticsearchPolicy.create();
 *
 *    &#64;BeforeClass
 *    public static void setup() {
 *       // ... populate instance
 *    }
 *
 *    &#64;Test
 *    public void myTest() {
 *      RestClient client = RULE.restClient();
 *      // ....
 *    }
 *  }
 *  </pre>
 * @see ExternalResource
 */
class EmbeddedElasticsearchPolicy extends ExternalResource {

  private final EmbeddedElasticsearchNode node;
  private final ObjectMapper mapper;
  private final Closer closer;
  private RestClient client;

  private EmbeddedElasticsearchPolicy(EmbeddedElasticsearchNode resource) {
    this.node = Objects.requireNonNull(resource, "resource");
    this.mapper = new ObjectMapper();
    this.closer = new Closer();
    closer.add(node);
  }

  @Override protected void before() throws Throwable {
    node.start();
  }

  @Override protected void after() {
    closer.close();
  }

  /**
   * Factory method to create this rule.
   * @return managed resource to be used in unit tests
   */
  public static EmbeddedElasticsearchPolicy create() {
    return new EmbeddedElasticsearchPolicy(EmbeddedElasticsearchNode.create());
  }

  /**
   * Creates index in elastic search given a mapping. Mapping can contain nested fields expressed
   * as dots({@code .}).
   *
   * <p>Example
   * <pre>
   *  {@code
   *     b.a: long
   *     b.b: keyword
   *  }
   * </pre>
   *
   * @param index index of the index
   * @param mapping field and field type mapping
   * @throws IOException if there is an error
   */
  void createIndex(String index, Map<String, String> mapping) throws IOException {
    Objects.requireNonNull(index, "index");
    Objects.requireNonNull(mapping, "mapping");

    ObjectNode mappings = mapper().createObjectNode();

    ObjectNode properties = mappings.with("mappings").with("properties");
    for (Map.Entry<String, String> entry: mapping.entrySet()) {
      applyMapping(properties, entry.getKey(), entry.getValue());
    }

    // create index and mapping
    final HttpEntity entity = new StringEntity(mapper().writeValueAsString(mappings),
        ContentType.APPLICATION_JSON);
    final Request r = new Request("PUT", "/" + index);
    r.setEntity(entity);
    restClient().performRequest(r);
  }

  /**
   * Creates nested mappings for an index. This function is called recursively for each level.
   *
   * @param parent current parent
   * @param key field name
   * @param type ES mapping type ({@code keyword}, {@code long} etc.)
   */
  private static void applyMapping(ObjectNode parent, String key, String type) {
    final int index = key.indexOf('.');
    if (index > -1) {
      String prefix  = key.substring(0, index);
      String suffix = key.substring(index + 1, key.length());
      applyMapping(parent.with(prefix).with("properties"), suffix, type);
    } else {
      parent.with(key).put("type", type);
    }
  }

  void insertDocument(String index, ObjectNode document) throws IOException {
    Objects.requireNonNull(index, "index");
    Objects.requireNonNull(document, "document");
    String uri = String.format(Locale.ROOT,
          "/%s/_doc?refresh", index);
    StringEntity entity = new StringEntity(mapper().writeValueAsString(document),
        ContentType.APPLICATION_JSON);
    final Request r = new Request("POST", uri);
    r.setEntity(entity);
    restClient().performRequest(r);
  }

  void insertBulk(String index, List<ObjectNode> documents) throws IOException {
    Objects.requireNonNull(index, "index");
    Objects.requireNonNull(documents, "documents");

    if (documents.isEmpty()) {
      // nothing to process
      return;
    }

    List<String> bulk = new ArrayList<>(documents.size() * 2);
    for (ObjectNode doc: documents) {
      bulk.add(String.format(Locale.ROOT, "{\"index\": {\"_index\":\"%s\"}}", index));
      bulk.add(mapper().writeValueAsString(doc));
    }

    final StringEntity entity = new StringEntity(String.join("\n", bulk) + "\n",
        ContentType.APPLICATION_JSON);

    final Request r = new Request("POST", "/_bulk?refresh");
    r.setEntity(entity);
    restClient().performRequest(r);
  }

  /**
   * Exposes Jackson API to be used to parse search results.
   * @return existing instance of ObjectMapper
   */
  ObjectMapper mapper() {
    return mapper;
  }

  /**
   * Low-level http rest client connected to current embedded elastic search instance.
   * @return http client connected to ES cluster
   */
  RestClient restClient() {
    if (client != null) {
      return client;
    }

    final RestClient client = RestClient.builder(httpHost()).build();
    closer.add(client);
    this.client = client;
    return client;
  }

  HttpHost httpHost() {
    final TransportAddress address = httpAddress();
    return new HttpHost(address.getAddress(), address.getPort());
  }

  /**
   * HTTP address for rest clients (can be ES native or any other).
   * @return http address to connect to
   */
  private TransportAddress httpAddress() {
    return node.httpAddress();
  }

}

// End EmbeddedElasticsearchPolicy.java
