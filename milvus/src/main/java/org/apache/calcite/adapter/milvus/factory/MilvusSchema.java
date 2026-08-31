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
package org.apache.calcite.adapter.milvus.factory;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DescribeCollectionReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.collection.response.ListCollectionsResp;

import java.util.HashMap;
import java.util.Map;

/**
 * Schema implementation for Milvus database.
 * Uses a global singleton MilvusClientV2 for all operations.
 */
public class MilvusSchema extends AbstractSchema {
  private final Map<String, Table> tableMap = new HashMap<>();

  private final String host;
  private final Integer port;
  private final String databaseName;
  private final String user;
  private final String password;


  public MilvusSchema(String host, Integer port, String databaseName, String user,
      String password) {
    super();
    this.host = host;
    this.port = port;
    this.databaseName = databaseName;
    this.user = user;
    this.password = password;
  }

  private ConnectConfig buildConnectConfig() {
    ConnectConfig.ConnectConfigBuilder builder = ConnectConfig.builder()
        .uri("http://" + host + ":" + port);
    if (databaseName != null) {
      builder.dbName(databaseName);
    }
    if (user != null) {
      builder.username(user);
    }
    if (password != null) {
      builder.password(password);
    }
    return builder.build();
  }

  /**
   * Borrows a MilvusClientV2 for this schema's database.
   * Creates a new client with the specific database context.
   */
  public MilvusClientV2 borrowClient() {
    // Create a client with specific database context
    return new MilvusClientV2(buildConnectConfig());
  }

  /**
   * Returns a MilvusClientV2 back to the pool.
   * Closes the client as we create a new one for each borrow.
   */
  public void returnClient(MilvusClientV2 client) {
    if (client != null) {
      try {
        client.close();
      } catch (Exception e) {
        // Ignore close errors
      }
    }
  }

  @Override protected synchronized Map<String, Table> getTableMap() {
    MilvusClientV2 client = borrowClient();
    try {
      ListCollectionsResp list = client.listCollections();
      if (list.getCollectionNames() != null) {
        for (String name : list.getCollectionNames()) {
          tableMap.computeIfAbsent(name, n ->
              new MilvusTranslatableTable(this, n, getCollectionSchema(n, client)));
        }
      }
    } finally {
      returnClient(client);
    }
    return tableMap;
  }

  private CreateCollectionReq.CollectionSchema getCollectionSchema(String collectionName,
      MilvusClientV2 milvusClient) {
    DescribeCollectionReq req =
        DescribeCollectionReq.builder()
            .collectionName(collectionName)
            .databaseName(databaseName)
            .build();
    DescribeCollectionResp describeCollectionResp =
        milvusClient.describeCollection(req);
    return describeCollectionResp.getCollectionSchema();
  }
}
