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

import org.apache.calcite.test.MongoAssertions;
import org.apache.calcite.util.Closer;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.Closeable;
import java.net.InetSocketAddress;

import de.bwaldvogel.mongo.MongoServer;
import de.bwaldvogel.mongo.backend.memory.MemoryBackend;

import static java.util.Objects.requireNonNull;

/**
 * Instantiates a new connection to a embedded (but fake) or real mongo database
 * depending on current profile (unit or integration tests).
 *
 * <p>By default, this rule is executed as part of a unit test and
 * <a href="https://github.com/bwaldvogel/mongo-java-server">in-memory database</a> is used.
 *
 * <p>However, if the maven profile is set to {@code IT} (eg. via command line
 * {@code $ mvn -Pit install}) this rule will connect to an existing (external)
 * Mongo instance ({@code localhost}).
 */
class MongoDatabasePolicy implements AfterAllCallback {

  private static final String DB_NAME = "test";

  private final MongoDatabase database;
  private final Closer closer;

  private MongoDatabasePolicy(MongoClient client, Closer closer) {
    MongoClient client1 = requireNonNull(client, "client");
    this.database = client.getDatabase(DB_NAME);
    this.closer = requireNonNull(closer, "closer");
    closer.add(client);
  }

  @Override public void afterAll(ExtensionContext context) {
    closer.close();
  }

  /**
   * Creates an instance based on current maven profile (as defined by {@code -Pit}).
   *
   * @return new instance of the policy to be used by unit tests
   */
  static MongoDatabasePolicy create() {
    final MongoClient client;
    final Closer closer = new Closer();
    if (MongoAssertions.useMongo()) {
      // use to real client (connects to default mongo instance)
      client = MongoClients.create();
    } else if (MongoAssertions.useFake()) {
      final MongoServer server = new MongoServer(new MemoryBackend());
      final InetSocketAddress address = server.bind();

      closer.add((Closeable) server::shutdownNow);
      client =
              MongoClients.create(MongoClientSettings
                      .builder()
                      .applyConnectionString(
                               new ConnectionString("mongodb://127.0.0.1:" + address.getPort())
                      )
                      .build());
    } else {
      throw new UnsupportedOperationException("I can only connect to Mongo or Fake instances");
    }

    return new MongoDatabasePolicy(client, closer);
  }

  MongoDatabase database() {
    return database;
  }
}
