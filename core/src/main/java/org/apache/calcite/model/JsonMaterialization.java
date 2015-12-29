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
package org.apache.calcite.model;

import java.util.List;

/**
 * Element that describes how a table is a materialization of a query.
 *
 * <p>Occurs within {@link JsonSchema#materializations}.
 *
 * @see JsonRoot Description of schema elements
 */
public class JsonMaterialization {
  public String view;
  public String table;

  /** SQL query that defines the materialization.
   *
   * <p>Must be a string or a list of strings (which are concatenated into a
   * multi-line SQL string, separated by newlines).
   */
  public Object sql;

  public List<String> viewSchemaPath;

  public void accept(ModelHandler handler) {
    handler.visit(this);
  }

  @Override public String toString() {
    return "JsonMaterialization(table=" + table + ", view=" + view + ")";
  }

  /** Returns the SQL query as a string, concatenating a list of lines if
   * necessary. */
  public String getSql() {
    return JsonLattice.toString(sql);
  }
}

// End JsonMaterialization.java
