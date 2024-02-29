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
package org.apache.calcite.adapter.gremlin;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.util.List;
import java.util.Map;

public class GremlinSchemaFactory implements SchemaFactory {

  /**GraphDatabase Gremlin Server Url **/
  public static final String DEFAULT_URL = "http://localhost:8182";

  @Override public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {

    final String url = operand.get("url") instanceof String
        ? (String) operand.get("url")
        : DEFAULT_URL;
    final boolean containsTables = operand.get("tables") instanceof List
        && ((List) operand.get("tables")).size() > 0;
    return new GremlinSchema(url, !containsTables);
  }
}
