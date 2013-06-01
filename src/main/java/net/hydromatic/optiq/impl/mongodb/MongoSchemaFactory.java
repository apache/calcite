/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package net.hydromatic.optiq.impl.mongodb;

import net.hydromatic.optiq.*;

import java.util.Map;

/**
 * Factory that creates a {@link MongoSchema}.
 *
 * <p>Allows a custom schema to be included in a model.json file.</p>
 */
@SuppressWarnings("UnusedDeclaration")
public class MongoSchemaFactory implements SchemaFactory {
  // public constructor, per factory contract
  public MongoSchemaFactory() {
  }

  public Schema create(MutableSchema parentSchema, String name,
      Map<String, Object> operand) {
    Map map = (Map) operand;
    String host = (String) map.get("host");
    String database = (String) map.get("database");
    final MongoSchema schema =
        new MongoSchema(
            parentSchema,
            host,
            database,
            parentSchema.getSubSchemaExpression(name, MongoSchema.class));
    parentSchema.addSchema(name, schema);
    return schema;
  }
}

// End MongoSchemaFactory.java
