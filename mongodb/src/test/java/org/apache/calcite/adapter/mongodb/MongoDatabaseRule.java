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

import com.github.fakemongo.Fongo;

import com.google.common.base.Preconditions;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;

import org.junit.rules.ExternalResource;

/**
 * Instantiates new connection to fongo (or mongo) database depending on current profile
 * (unit or integration tests).
 *
 * By default, this rule is executed as part of a unit test and in-memory database
 * <a href="https://github.com/fakemongo/fongo">fongo</a> is used.
 *
 * <p>However, if maven profile is set to {@code IT} (eg. via command line
 * {@code $ mvn -Pit install}) this rule will connect to existing (external)
 * mongo instance ({@code localhost})</p>
 *
 */
class MongoDatabaseRule extends ExternalResource {

  private static final String DB_NAME = "test";

  private final MongoDatabase database;
  private final MongoClient client;

  private MongoDatabaseRule(MongoClient client) {
    this.client = Preconditions.checkNotNull(client, "client");
    this.database = client.getDatabase(DB_NAME);
  }

  /**
   * Create an instance based on current maven profile (as defined by {@code -Pit}).
   */
  static MongoDatabaseRule create() {
    final MongoClient client;
    if (MongoAssertions.useMongo()) {
      // use to real client (connects to mongo)
      client = new MongoClient();
    } else if (MongoAssertions.useFongo()) {
      // in-memory DB (fake Mongo)
      client = new Fongo(MongoDatabaseRule.class.getSimpleName()).getMongo();
    } else {
      throw new UnsupportedOperationException("I can only connect to Mongo or Fongo instances");
    }

    return new MongoDatabaseRule(client);
  }


  MongoDatabase database() {
    return database;
  }

  @Override protected void after() {
    client.close();
  }

}

// End MongoDatabaseRule.java
