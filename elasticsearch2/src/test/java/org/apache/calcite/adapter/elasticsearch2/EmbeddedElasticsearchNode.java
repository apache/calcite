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
import com.google.common.io.Files;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.internal.InternalSettingsPreparer;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.groovy.GroovyPlugin;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

/**
 * Represents a single Elasticsearch node that can run embedded in a java application.
 * Intended for unit and integration tests. Settings and plugins are crafted for Calcite.
 */
class EmbeddedElasticsearchNode implements AutoCloseable {

  private final LocalNode node;
  private volatile boolean  isStarted;

  private EmbeddedElasticsearchNode(LocalNode node) {
    this.node = Preconditions.checkNotNull(node, "node");
  }

  /**
   * Having separate class to expose (protected) constructor which allows to install
   * different plugins. In our case it is {@code GroovyPlugin} for scripted fields like
   * {@code loc[0]} or {@code loc[1]['foo']}.
   */
  private static class LocalNode extends Node {
    private LocalNode(Settings settings, Collection<Class<? extends Plugin>> classpathPlugins) {
      super(InternalSettingsPreparer.prepareEnvironment(settings, null),
          Version.CURRENT, classpathPlugins);
    }
  }

  /**
   * Creates an instance with existing settings
   *
   * @param settings ES settings for the node
   * @return un-started node; call {@link #start()} to start the instance
   */
  private static EmbeddedElasticsearchNode create(Settings settings) {
    // ensure GroovyPlugin is installed or otherwise scripted fields would not work
    LocalNode node = new LocalNode(settings, Collections.singleton(GroovyPlugin.class));
    return new EmbeddedElasticsearchNode(node);
  }

  /**
   * Creates elastic node as single member of a cluster. Node will not be started
   * unless {@link #start()} is explicitly called.
   *
   * @return node with default configuration
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
            .put("script.inline", true)  // requires GroovyPlugin
            .put("script.indexed", true) // requires GroovyPlugin
            .put("cluster.routing.allocation.disk.threshold_enabled", false)
            .put("node.local", true)
            .put("node.data", true)
            .put("network.host", "localhost")
            .build();

    return create(settings);
  }

  /**
   * Starts current node
   */
  public void start() {
    Preconditions.checkState(!isStarted, "already started");
    node.start();
    this.isStarted = true;
  }

  /**
   * Returns the current address to connect to with HTTP client.
   *
   * @return {@code HTTP} address (hostname / port)
   */
  public TransportAddress httpAddress() {
    Preconditions.checkState(isStarted, "node is not started");

    NodesInfoResponse response =  client().admin().cluster().prepareNodesInfo()
            .execute().actionGet();
    if (response.getNodes().length != 1) {
      throw new IllegalStateException("Expected single node but got "
              + response.getNodes().length);
    }
    NodeInfo node = response.getNodes()[0];
    return node.getHttp().address().boundAddresses()[0];
  }

  /**
   * Exposes an Elasticsearch
   * <a href="https://www.elastic.co/guide/en/elasticsearch/client/java-api/current/transport-client.html">transport client</a>
   * (use of HTTP client is preferred).
   *
   * @return client API to access ES functionality
   */
  public Client client() {
    Preconditions.checkState(isStarted, "node is not started");
    return node.client();
  }

  @Override public void close() throws Exception {
    node.close();
    // cleanup data dirs
    File data = new File(node.settings().get("path.data"));
    File home = new File(node.settings().get("path.home"));
    for (File file: Arrays.asList(data, home)) {
      if (file.exists()) {
        file.delete();
      }
    }
  }
}

// End EmbeddedElasticsearchNode.java
