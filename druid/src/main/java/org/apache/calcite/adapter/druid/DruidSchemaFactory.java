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
package org.apache.calcite.adapter.druid;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.util.List;
import java.util.Map;

/**
 * Schema factory that creates Druid schemas.
 *
 * <table>
 *   <caption>Druid schema operands</caption>
 *   <tr>
 *     <th>Operand</th>
 *     <th>Description</th>
 *     <th>Required</th>
 *   </tr>
 *   <tr>
 *     <td>url</td>
 *     <td>URL of Druid's query node.
 *     The default is "http://localhost:8082".</td>
 *     <td>No</td>
 *   </tr>
 *   <tr>
 *     <td>coordinatorUrl</td>
 *     <td>URL of Druid's coordinator node.
 *     The default is <code>url</code>, replacing "8082" with "8081",
 *     for example "http://localhost:8081".</td>
 *     <td>No</td>
 *   </tr>
 * </table>
 */
public class DruidSchemaFactory implements SchemaFactory {
  /** Default Druid URL. */
  public static final String DEFAULT_URL = "http://localhost:8082";

  public Schema create(SchemaPlus parentSchema, String name,
      Map<String, Object> operand) {
    final String url = operand.get("url") instanceof String
        ? (String) operand.get("url")
        : DEFAULT_URL;
    final String coordinatorUrl = operand.get("coordinatorUrl") instanceof String
        ? (String) operand.get("coordinatorUrl")
        : url.replace(":8082", ":8081");
    // "tables" is a hidden attribute, copied in from the enclosing custom
    // schema
    final boolean containsTables = operand.get("tables") instanceof List
        && ((List) operand.get("tables")).size() > 0;
    return new DruidSchema(url, coordinatorUrl, !containsTables);
  }
}

// End DruidSchemaFactory.java
