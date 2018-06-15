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
package org.apache.calcite.adapter.elasticsearch2;

import com.google.common.base.Preconditions;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.transport.TransportAddress;
import org.junit.rules.ExternalResource;

/**
 * Junit rule that is used to initialize a single Elasticsearch node for tests.
 *
 * <p>For performance reasons (node startup costs),
 * the same instance is usually shared across multiple tests.
 *
 * <p>This rule should be used as follows:
 * <pre>
 *
 *  public class MyTest {
 *    &#64;ClassRule
 *    public static final EmbeddedElasticsearchPolicy POLICY =
 *        EmbeddedElasticsearchPolicy.create();
 *
 *    &#64;BeforeClass
 *    public static void setup() {
 *       // ... populate instance
 *    }
 *
 *    &#64;Test
 *    public void myTest() {
 *      TransportAddress address = POLICY.httpAddress();
 *      // .... (connect)
 *    }
 * }
 * </pre>
 *
 * @see ExternalResource
 */
class EmbeddedElasticsearchPolicy extends ExternalResource {

  private final EmbeddedElasticsearchNode node;

  private EmbeddedElasticsearchPolicy(EmbeddedElasticsearchNode resource) {
    this.node = Preconditions.checkNotNull(resource, "resource");
  }

  @Override protected void before() throws Throwable {
    node.start();
  }

  @Override protected void after() {
    try {
      node.close();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Factory method to create this rule.
   *
   * @return new rule instance to be used in unit tests
   */
  public static EmbeddedElasticsearchPolicy create() {
    return new EmbeddedElasticsearchPolicy(EmbeddedElasticsearchNode.create());
  }

  /**
   * Exposes current ES transport client.
   *
   * @return initialized instance of ES
   */
  Client client() {
    return node.client();
  }

  /**
   * HTTP address for rest clients (can be ES native or any other).
   *
   * @return HTTP hostname/port to connect to this ES instance
   */
  TransportAddress httpAddress() {
    return node.httpAddress();
  }
}

// End EmbeddedElasticsearchPolicy.java
