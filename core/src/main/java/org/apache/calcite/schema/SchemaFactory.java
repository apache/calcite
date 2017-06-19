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
package org.apache.calcite.schema;

import java.util.Map;

/**
 * Factory for {@link Schema} objects.
 *
 * <p>A schema factory allows you to include a custom schema in a model file.
 * For example, here is a model that contains a custom schema whose tables
 * read CSV files. (See the
 * <a href="http://calcite.apache.org/apidocs/org/apache/calcite/adapter/csv/package-summary.html">example CSV adapter</a>
 * for more details about this particular adapter.)
 *
 * <blockquote><pre>{
 *   "version": "1.0",
 *   "defaultSchema": "SALES",
 *   "schemas": [
 *     {
 *       "name": "SALES",
 *       "type": "custom",
 *       "factory": "org.apache.calcite.adapter.csv.CsvSchemaFactory",
 *       "mutable": true,
 *       "operand": {
 *         directory: "target/test-classes/sales"
 *       },
 *       "tables": [
 *         {
 *           "name": "FEMALE_EMPS",
 *           "type": "view",
 *           "sql": "SELECT * FROM emps WHERE gender = 'F'"
 *          }
 *       ]
 *     }
 *   ]
 * }</pre></blockquote>
 *
 * <p>If you do not wish to allow model authors to add additional tables
 * (including views) to an instance of your schema, specify
 * 'mutable: false'.
 *
 * <p>A class that implements SchemaFactory specified in a schema must have a
 * public default constructor.
 */
public interface SchemaFactory {
  /** Creates a Schema.
   *
   * @param parentSchema Parent schema
   * @param name Name of this schema
   * @param operand The "operand" JSON property
   * @return Created schema
   */
  Schema create(
      SchemaPlus parentSchema,
      String name,
      Map<String, Object> operand);
}

// End SchemaFactory.java
