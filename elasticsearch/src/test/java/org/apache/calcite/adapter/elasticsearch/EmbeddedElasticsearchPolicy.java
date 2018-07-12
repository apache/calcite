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

import org.apache.http.HttpHost;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.transport.TransportAddress;
import org.junit.rules.ExternalResource;

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
   * Exposes Jackson API to be used for low-level ES.
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
    TransportAddress address = httpAddress();
    RestClient client = RestClient.builder(new HttpHost(address.getAddress(), address.getPort()))
            .build();
    closer.add(client);
    this.client = client;
    return client;
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
