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
package org.apache.calcite.adapter.kvrocks;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

import static java.lang.Integer.parseInt;

/**
 * Factory that creates a {@link KvrocksSchema}.
 *
 * <p>Reads connection parameters and table definitions from the model JSON.
 *
 * <p>Required operand keys: {@code host}, {@code port}, {@code database},
 * {@code tables}. Optional: {@code password}, {@code namespace}.
 */
@SuppressWarnings("UnusedDeclaration")
public class KvrocksSchemaFactory implements SchemaFactory {

  public KvrocksSchemaFactory() {
  }

  @Override public Schema create(SchemaPlus parentSchema, String name,
      Map<String, Object> operand) {
    checkArgument(operand.get("host") != null, "host must be specified");
    checkArgument(operand.get("port") != null, "port must be specified");
    checkArgument(operand.get("database") != null, "database must be specified");
    checkArgument(operand.get("tables") != null, "tables must be specified");

    String host = operand.get("host").toString();
    int port = (int) operand.get("port");
    int database = parseInt(operand.get("database").toString());

    String password = operand.get("password") == null
        ? null : operand.get("password").toString();
    String namespace = operand.get("namespace") == null
        ? null : operand.get("namespace").toString();

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> tables = (List) operand.get("tables");

    return new KvrocksSchema(host, port, database, password, namespace, tables);
  }
}
