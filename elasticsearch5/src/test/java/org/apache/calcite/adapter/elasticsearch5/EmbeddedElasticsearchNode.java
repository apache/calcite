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
package org.apache.calcite.adapter.elasticsearch5;

import com.google.common.base.Preconditions;
import com.google.common.io.Files;

import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.node.InternalSettingsPreparer;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeValidationException;
import org.elasticsearch.painless.PainlessPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.Netty3Plugin;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;

/**
 * Represents a single elastic search node which can run embedded in a java application.
 * Intended for unit and integration tests. Settings and plugins are crafted for Calcite.
 */
class EmbeddedElasticsearchNode implements AutoCloseable {

  private final Node node;
  private volatile boolean  isStarted;

  private EmbeddedElasticsearchNode(Node node) {
    this.node = Preconditions.checkNotNull(node, "node");
  }


  /**
   * Creates an instance with existing settings
   *
   * @param settings ES configuration
   * @return un-initialized node. Use {@link #start()} explicitly.
   */
  private static EmbeddedElasticsearchNode create(Settings settings) {
    // ensure GroovyPlugin is installed or otherwise scripted fields would not work
    Node node = new LocalNode(settings, Arrays.asList(Netty3Plugin.class, PainlessPlugin.class));
    return new EmbeddedElasticsearchNode(node);
  }

  /**
   * Creates elastic node as single member of a cluster. Node will not be started
   * unless {@link #start()} is explicitly called.
   *
   * @return un-initialized node. Use {@link #start()} explicitly.
   */
  public static EmbeddedElasticsearchNode create() {
    File data = Files.createTempDir();
    data.deleteOnExit();
    File home = Files.createTempDir();
    home.deleteOnExit();

    Settings settings = Settings.builder()
            .put("node.name", "fake-elastic")
            .put("path.home", home.getAbsolutePath())
            .put("path.data", data.getAbsolutePath())
            .put("transport.type", "local")
            .put("http.type", "netty3")
            .put("script.inline", true)  // requires groovy or painless
            .put("network.host", "localhost")
            .build();

    return create(settings);
  }

  /**
   * Starts current node
   */
  public void start() {
    Preconditions.checkState(!isStarted, "already started");
    try {
      node.start();
      this.isStarted = true;
    } catch (NodeValidationException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Returns the current address to connect to with HTTP client.
   *
   * @return {@code HTTP} protocol connection parameters
   */
  TransportAddress httpAddress() {
    Preconditions.checkState(isStarted, "node is not started");

    NodesInfoResponse response =  client().admin().cluster().prepareNodesInfo()
            .execute().actionGet();
    if (response.getNodes().size() != 1) {
      throw new IllegalStateException("Expected single node but got "
              + response.getNodes().size());
    }
    NodeInfo node = response.getNodes().get(0);
    return node.getHttp().address().boundAddresses()[0];
  }

  /**
   * Exposes an Elasticsearch
   * <a href="https://www.elastic.co/guide/en/elasticsearch/client/java-api/current/transport-client.html">transport client</a>
   * (use of HTTP client is preferred).
   *
   * @return ES client API on a running instance
   */
  public Client client() {
    Preconditions.checkState(isStarted, "node is not started");
    return node.client();
  }

  @Override public void close() throws Exception {
    node.close();
    // cleanup data dirs
    for (String name: Arrays.asList("path.data", "path.home")) {
      if (node.settings().get(name) != null) {
        File file = new File(node.settings().get(name));
        if (file.exists()) {
          file.delete();
        }
      }
    }
  }

  /**
   * Having separate class to expose (protected) constructor which allows to install
   * different plugins. In our case it is {@code GroovyPlugin} for scripted fields
   * like {@code loc[0]} or {@code loc[1]['foo']}.
   *
   * <p>This class is intended solely for tests
   */
  private static class LocalNode extends Node {

    private LocalNode(Settings settings, Collection<Class<? extends Plugin>> classpathPlugins) {
      super(InternalSettingsPreparer.prepareEnvironment(settings, null),
              classpathPlugins);
    }
  }
}

// End EmbeddedElasticsearchNode.java
