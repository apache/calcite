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

import org.apache.calcite.model.JsonTable;

import java.util.List;
import java.util.Map;

/**
 * Extension interface for {@link SchemaFactory} implementations that want to
 * support constraint metadata defined in model files.
 *
 * <p>Schema factories that implement this interface can opt-in to constraint
 * metadata support by implementing the {@link #supportsConstraints()} method
 * to return {@code true}. When enabled, the ModelHandler will pass table
 * constraint definitions from model files to the factory through the operand map.
 *
 * <p>The constraint metadata includes:
 * <ul>
 *   <li>Primary keys - for query optimization and JDBC metadata
 *   <li>Foreign keys - for join optimization and referential integrity metadata
 *   <li>Unique keys - for additional optimization hints
 * </ul>
 *
 * <p>These constraints are metadata-only and are not enforced by the underlying
 * storage engines. They serve as hints for query optimization, JDBC metadata
 * support, and logical data model documentation.
 *
 * <h3>Example Model File Configuration</h3>
 * <pre>
 * {
 *   "schemas": [{
 *     "name": "myschema",
 *     "type": "custom",
 *     "factory": "com.example.MySchemaFactory",
 *     "tables": [{
 *       "name": "customers",
 *       "constraints": {
 *         "primaryKey": ["customer_id"],
 *         "foreignKeys": [{
 *           "columns": ["region_id"],
 *           "targetTable": ["regions"],
 *           "targetColumns": ["id"]
 *         }]
 *       }
 *     }]
 *   }]
 * }
 * </pre>
 *
 * @see org.apache.calcite.model.JsonConstraints
 */
public interface ConstraintCapableSchemaFactory extends SchemaFactory {

  /**
   * Returns whether this schema factory supports constraint metadata processing.
   *
   * <p>When {@code true}, the ModelHandler will:
   * <ul>
   *   <li>Extract constraint definitions from table configurations in model files
   *   <li>Add constraint metadata to the operand map under the "tableConstraints" key
   *   <li>Allow tables created by this factory to access constraint information
   * </ul>
   *
   * <p>Default implementation returns {@code false} for backward compatibility.
   *
   * @return {@code true} if this factory supports constraint metadata
   */
  default boolean supportsConstraints() {
    return false;
  }

  /**
   * Called by ModelHandler to provide constraint metadata for tables.
   *
   * <p>This method is called during schema creation to provide constraint
   * definitions that were specified in the model file. The constraints
   * are organized by table name.
   *
   * <p>The constraint format follows the same structure as used in model files:
   * <pre>
   * Map.of(
   *   "tableName", Map.of(
   *     "primaryKey", List.of("column1", "column2"),
   *     "foreignKeys", List.of(
   *       Map.of(
   *         "columns", List.of("fk_column"),
   *         "targetTable", List.of("target_table"),
   *         "targetColumns", List.of("target_column")
   *       )
   *     )
   *   )
   * )
   * </pre>
   *
   * <p>Default implementation does nothing, allowing factories to opt-in by
   * overriding this method.
   *
   * @param tableConstraints Map from table name to constraint definitions
   * @param tableDefinitions List of table definitions from model file
   */
  default void setTableConstraints(
      Map<String, Map<String, Object>> tableConstraints,
      List<JsonTable> tableDefinitions) {
    // Default implementation - do nothing
  }
}