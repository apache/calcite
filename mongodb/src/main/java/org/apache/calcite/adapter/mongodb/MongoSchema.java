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
package org.apache.calcite.adapter.mongodb;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Schema mapped onto a directory of MONGO files. Each table in the schema
 * is a MONGO file in that directory.
 */
public class MongoSchema extends AbstractSchema {
  final MongoDatabase mongoDb;

  /**
   * Creates a MongoDB schema.
   *
   * @param host Mongo host, e.g. "localhost"
   * @param credential Optional credentials (null for none)
   * @param database Mongo database name, e.g. "foodmart"
   */
  MongoSchema(String host, String database, MongoCredential credential) {
    super();
    try {
      MongoClientSettings.Builder mongoSettings = MongoClientSettings.builder()
          .applyToClusterSettings(clusterSettingsBuilder ->
              clusterSettingsBuilder.hosts(Collections.singletonList(new ServerAddress(host))));

      if (null != credential) {
        mongoSettings.credential(credential);
      }
      final MongoClient mongo = MongoClients.create(mongoSettings.build());

      this.mongoDb = mongo.getDatabase(database);
    } catch (Exception exception) {
      throw new RuntimeException("Failed to created the Mongo client using the host and credential",
          exception);
    }
  }

  /**
   * Creates a MongoDB schema using the new recommended way using connection string.
   * @param uri MongoDB connection string
   * @param database Mongo database name provided in the configuration
   */
  MongoSchema(String uri, String database) {
    super();
    try {
      final MongoClient mongo = MongoClients.create(uri);
      this.mongoDb = mongo.getDatabase(database);
    } catch (Exception exception) {
      throw new RuntimeException("Failed to create Mongo client using the connection string",
          exception);
    }
  }

  /**
   * Allows tests to inject their instance of the database.
   *
   * @param mongoDb existing mongo database instance
   */
  @VisibleForTesting
  MongoSchema(MongoDatabase mongoDb) {
    super();
    this.mongoDb = Objects.requireNonNull(mongoDb, "mongoDb");
  }

  @Override protected Map<String, Table> getTableMap() {
    final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
    for (String collectionName : mongoDb.listCollectionNames()) {
      builder.put(collectionName, new MongoTable(collectionName));
    }
    return builder.build();
  }
}
