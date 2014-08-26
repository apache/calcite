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
package net.hydromatic.optiq.impl.mongodb;

import net.hydromatic.optiq.*;
import net.hydromatic.optiq.impl.AbstractSchema;

import com.google.common.collect.ImmutableMap;

import com.mongodb.DB;
import com.mongodb.MongoClient;

import java.util.*;

/**
 * Schema mapped onto a directory of MONGO files. Each table in the schema
 * is a MONGO file in that directory.
 */
public class MongoSchema extends AbstractSchema {
  final DB mongoDb;

  /**
   * Creates a MongoDB schema.
   *
   * @param host Mongo host, e.g. "localhost"
   * @param database Mongo database name, e.g. "foodmart"
   */
  public MongoSchema(String host, String database) {
    super();
    try {
      MongoClient mongo = new MongoClient(host);
      this.mongoDb = mongo.getDB(database);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected Map<String, Table> getTableMap() {
    final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
    for (String collectionName : mongoDb.getCollectionNames()) {
      builder.put(collectionName, new MongoTable(collectionName));
    }
    return builder.build();
  }
}

// End MongoSchema.java
