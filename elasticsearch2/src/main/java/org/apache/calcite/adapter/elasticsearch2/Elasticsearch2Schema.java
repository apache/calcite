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

import org.apache.calcite.adapter.elasticsearch.ElasticsearchSchema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;

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

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Schema mapped onto an index of ELASTICSEARCH types.
 *
 * <p>Each table in the schema is an ELASTICSEARCH type in that index.
 */
public class Elasticsearch2Schema extends AbstractSchema
    implements ElasticsearchSchema {
  final String index;

  private transient Client client;

  /**
   * Creates an Elasticsearch2 schema.
   *
   * @param coordinates Map of Elasticsearch node locations (host, port)
   * @param userConfig Map of user-specified configurations
   * @param indexName Elasticsearch database name, e.g. "usa".
   */
  Elasticsearch2Schema(Map<String, Integer> coordinates,
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

  @Override protected Map<String, Table> getTableMap() {
    final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();

    try {
      GetMappingsResponse response = client.admin().indices()
          .getMappings(new GetMappingsRequest().indices(index))
          .get();
      ImmutableOpenMap<String, MappingMetaData> mapping = response.getMappings().get(index);
      for (ObjectObjectCursor<String, MappingMetaData> c: mapping) {
        builder.put(c.key, new Elasticsearch2Table(client, index, c.key));
      }
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return builder.build();
  }

  private void open(List<InetSocketAddress> transportAddresses, Map<String, String> userConfig) {
    final List<TransportAddress> transportNodes = new ArrayList<>(transportAddresses.size());
    for (InetSocketAddress address : transportAddresses) {
      transportNodes.add(new InetSocketTransportAddress(address));
    }

    Settings settings = Settings.settingsBuilder().put(userConfig).build();

    final TransportClient transportClient = TransportClient.builder().settings(settings).build();
    for (TransportAddress transport : transportNodes) {
      transportClient.addTransportAddress(transport);
    }

    final List<DiscoveryNode> nodes = ImmutableList.copyOf(transportClient.connectedNodes());
    if (nodes.isEmpty()) {
      throw new RuntimeException("Cannot connect to any elasticsearch nodes");
    }

    client = transportClient;
  }

  @Override public String getIndex() {
    return index;
  }
}

// End Elasticsearch2Schema.java
