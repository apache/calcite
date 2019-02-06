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
package org.apache.calcite.adapter.cassandra;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.util.Map;

/**
 * Factory that creates a {@link CassandraSchema}
 */
@SuppressWarnings("UnusedDeclaration")
public class CassandraSchemaFactory implements SchemaFactory {
  public CassandraSchemaFactory() {
  }

  public Schema create(SchemaPlus parentSchema, String name,
      Map<String, Object> operand) {
    Map map = (Map) operand;
    String host = (String) map.get("host");
    String keyspace = (String) map.get("keyspace");
    String username = (String) map.get("username");
    String password = (String) map.get("password");

    if (map.containsKey("port")) {
      Object portObj = map.get("port");
      int port;
      if (portObj instanceof String) {
        port = Integer.parseInt((String) portObj);
      } else {
        port = (int) portObj;
      }
      return new CassandraSchema(host, port, keyspace, username, password, parentSchema, name);
    } else {
      return new CassandraSchema(host, keyspace, username, password, parentSchema, name);
    }
  }
}

// End CassandraSchemaFactory.java
