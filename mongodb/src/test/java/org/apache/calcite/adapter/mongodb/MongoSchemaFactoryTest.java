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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.Closeable;
import java.net.InetSocketAddress;

import de.bwaldvogel.mongo.MongoServer;
import de.bwaldvogel.mongo.backend.memory.MemoryBackend;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Testing Mongo Schema factory for creation of Mongo client either using a host name and port
 * or a connection string.
 */
public class MongoSchemaFactoryTest {
  private static int port;
  private static Closer closer;

  @BeforeAll
  public static void setUp() {
    closer = new Closer();
    if (MongoAssertions.useMongo()) {
      port = 27017;
    } else if (MongoAssertions.useFake()) {
      final MongoServer server = new MongoServer(new MemoryBackend());
      final InetSocketAddress address = server.bind();

      port = address.getPort();
      closer.add((Closeable) server::shutdownNow);
    } else {
      throw new UnsupportedOperationException("I can only connect to Mongo or Fake instances");
    }
  }

  @AfterAll
  public static void cleanup() {
    closer.close();
  }

  @Test void testMongoConnectionUsingHostnameAndPort() {
    final String host = "127.0.0.1";
    final String database = "test";
    final MongoSchema mongo = MongoAssertions.useMongo()
        ? new MongoSchema(host, database, null)
        : new MongoSchema(host, database, null);

    assertThat(database, is(mongo.mongoDb.getName()));
  }

  @Test void testMongoConnectionUsingConnectionString() {
    final String uri = "mongodb://127.0.0.1";
    final String database = "test";
    final MongoSchema mongo = new MongoSchema(uri, database);

    assertThat(database, is(mongo.mongoDb.getName()));
  }

}
