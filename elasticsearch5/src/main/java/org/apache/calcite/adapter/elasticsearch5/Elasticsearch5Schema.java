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

import org.apache.calcite.adapter.elasticsearch.ElasticsearchSchema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Schema mapped onto an index of ELASTICSEARCH types.
 *
 * <p>Each table in the schema is an ELASTICSEARCH type in that index.
 */
public class Elasticsearch5Schema extends AbstractSchema
    implements ElasticsearchSchema {
  final String index;

  private transient Client client;

  /**
   * Creates an Elasticsearch5 schema.
   *
   * @param coordinates Map of Elasticsearch node locations (host, port)
   * @param userConfig Map of user-specified configurations
   * @param indexName Elasticsearch database name, e.g. "usa".
   */
  Elasticsearch5Schema(Map<String, Integer> coordinates,
      Map<String, String> userConfig, String indexName) {
    super();

    final List<InetSocketAddress> transportAddresses = new ArrayList<>();
    for (Map.Entry<String, Integer> coordinate: coordinates.entrySet()) {
      transportAddresses.add(
          new InetSocketAddress(coordinate.getKey(), coordinate.getValue()));
    }

    open(transportAddresses, userConfig);

    if (client != null) {
      final String[] indices = client.admin().indices()
          .getIndex(new GetIndexRequest().indices(indexName))
          .actionGet().getIndices();
      if (indices.length == 1) {
        index = indices[0];
      } else {
        index = null;
      }
    } else {
      index = null;
    }
  }

  /**
   * Allows schema to be instantiated from existing elastic search client.
   * This constructor is used in tests.
   * @param client existing client instance
   * @param index name of ES index
   */
  @VisibleForTesting
  Elasticsearch5Schema(Client client, String index) {
    this.client = Preconditions.checkNotNull(client, "client");
    this.index = Preconditions.checkNotNull(index, "index");
  }


  @Override protected Map<String, Table> getTableMap() {
    final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();

    try {
      final GetMappingsResponse response = client.admin().indices()
          .getMappings(new GetMappingsRequest().indices(index))
          .get();
      ImmutableOpenMap<String, MappingMetaData> mapping =
          response.getMappings().get(index);
      for (ObjectObjectCursor<String, MappingMetaData> c : mapping) {
        builder.put(c.key, new Elasticsearch5Table(client, index, c.key));
      }
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return builder.build();
  }

  private void open(List<InetSocketAddress> transportAddresses,
      Map<String, String> userConfig) {
    final List<TransportAddress> transportNodes = new ArrayList<>(transportAddresses.size());
    for (InetSocketAddress address : transportAddresses) {
      transportNodes.add(new InetSocketTransportAddress(address));
    }

    Settings settings = Settings.builder().put(userConfig).build();

    final TransportClient transportClient = new PreBuiltTransportClient(settings);
    for (TransportAddress transport : transportNodes) {
      transportClient.addTransportAddress(transport);
    }

    final List<DiscoveryNode> nodes =
        ImmutableList.copyOf(transportClient.connectedNodes());

    if (nodes.isEmpty()) {
      throw new IllegalStateException("Cannot connect to any elasticsearch node from "
              + transportNodes);
    }

    client = transportClient;
  }

  @Override public String getIndex() {
    return index;
  }
}

// End Elasticsearch5Schema.java
