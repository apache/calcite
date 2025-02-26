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
package org.apache.calcite.adapter.redis;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

import static java.lang.Integer.parseInt;

/**
 * Factory that creates a {@link RedisSchema}.
 *
 * <p>Allows a custom schema to be included in a redis-test-model.json file.
 * See <a href="http://calcite.apache.org/docs/file_adapter.html">File adapter</a>.
 */
@SuppressWarnings("UnusedDeclaration")
public class RedisSchemaFactory implements SchemaFactory {
  // public constructor, per factory contract
  public RedisSchemaFactory() {
  }

  @Override public Schema create(SchemaPlus schema, String name,
      Map<String, Object> operand) {
    checkArgument(operand.get("tables") != null,
        "tables must be specified");
    checkArgument(operand.get("host") != null,
        "host must be specified");
    checkArgument(operand.get("port") != null,
        "port must be specified");
    checkArgument(operand.get("database") != null,
        "database must be specified");

    @SuppressWarnings("unchecked") List<Map<String, Object>> tables =
        (List) operand.get("tables");
    String host = operand.get("host").toString();
    int port = (int) operand.get("port");
    int database = parseInt(operand.get("database").toString());
    String password = operand.get("password") == null ? null
        : operand.get("password").toString();
    return new RedisSchema(host, port, database, password, tables);
  }
}
