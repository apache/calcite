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

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.util.Map;

/**
 * Schema Factory for milvus database.
 */
public class MilvusSchemaFactory implements SchemaFactory {
  @Override public Schema create(SchemaPlus parentSchema, String name,
      Map<String, Object> operand) {
    String host = (String) operand.get("host");
    Integer milvusPort = (Integer) operand.get("port");
    String databaseName = (String) operand.get("databaseName");
    String user = (String) operand.get("user");
    String password = (String) operand.get("password");
    return new MilvusSchema(host, milvusPort, databaseName, user, password);
  }
}
