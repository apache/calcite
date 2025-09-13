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

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Extension to Schema that can provide comments for business documentation.
 *
 * <p>Similar to {@link CommentableTable}, this interface allows schemas to
 * provide business descriptions that can be queried via SQL. Comments are
 * accessible through the information schema following PostgreSQL conventions.
 *
 * <p>Example usage:
 * <pre>
 * public class MySchema extends AbstractSchema implements CommentableSchema {
 *   &#64;Override public String getComment() {
 *     return "Business domain schema for financial analytics";
 *   }
 * }
 * </pre>
 *
 * <p>Comments can then be queried via SQL:
 * <pre>
 * SELECT schema_name, schema_comment 
 * FROM information_schema.schemata 
 * WHERE schema_comment IS NOT NULL;
 * </pre>
 */
public interface CommentableSchema extends Schema {
  /**
   * Returns the business description/comment for this schema.
   *
   * <p>This comment should describe the business purpose, data domain,
   * or use cases for the schema. It will be exposed through the
   * information_schema.schemata view for documentation and discovery.
   *
   * @return Schema comment, or null if none defined
   */
  @Nullable String getComment();
}