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
 * View schema element.
 *
 * <p>Like base class {@link JsonTable},
 * occurs within {@link JsonMapSchema#tables}.
 *
 * <h2>Modifiable views</h2>
 *
 * <p>A view is modifiable if contains only SELECT, FROM, WHERE (no JOIN,
 * aggregation or sub-queries) and every column:
 *
 * <ul>
 *   <li>is specified once in the SELECT clause; or
 *   <li>occurs in the WHERE clause with a column = literal predicate; or
 *   <li>is nullable.
 * </ul>
 *
 * <p>The second clause allows Calcite to automatically provide the correct
 * value for hidden columns. It is useful in, say, a multi-tenant environment,
 * where the {@code tenantId} column is hidden, mandatory (NOT NULL), and has a
 * constant value for a particular view.
 *
 * <p>Errors regarding modifiable views:
 *
 * <ul>
 *   <li>If a view is marked modifiable: true and is not modifiable, Calcite
 *   throws an error while reading the schema.
 *   <li>If you submit an INSERT, UPDATE or UPSERT command to a non-modifiable
 *   view, Calcite throws an error when validating the statement.
 *   <li>If a DML statement creates a row that would not appear in the view
 *   (for example, a row in female_emps, above, with gender = 'M'), Calcite
 *   throws an error when executing the statement.
 * </ul>
 *
 * @see JsonRoot Description of schema elements
 */
public class JsonView extends JsonTable {
  /** SQL query that is the definition of the view.
   *
   * <p>Must be a string or a list of strings (which are concatenated into a
   * multi-line SQL string, separated by newlines).
   */
  public Object sql;

  /** Schema name(s) to use when resolving query.
   *
   * <p>If not specified, defaults to current schema.
   */
  public List<String> path;

  /** Whether this view should allow INSERT requests.
   *
   * <p>The values have the following meanings:
   * <ul>
   * <li>If true, Calcite throws an error when validating the schema if the
   *     view is not modifiable.
   * <li>If null, Calcite deduces whether the view is modifiable.
   * <li>If false, Calcite will not allow inserts.
   * </ul>
   *
   * <p>The default value is {@code null}.
   */
  public Boolean modifiable;

  public void accept(ModelHandler handler) {
    handler.visit(this);
  }

  @Override public String toString() {
    return "JsonView(name=" + name + ")";
  }

  /** Returns the SQL query as a string, concatenating a list of lines if
   * necessary. */
  public String getSql() {
    return JsonLattice.toString(sql);
  }
}

// End JsonView.java
