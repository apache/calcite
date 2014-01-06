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
package net.hydromatic.optiq;

/**
 * Extension to the {@link Schema} interface.
 *
 * <p>Given a user-defined schema that implements the {@link Schema} interface,
 * Optiq creates a wrapper that implements the {@code SchemaPlus} interface.
 * This provides extra functionality, such as access to tables that have been
 * added explicitly.</p>
 *
 * <p>A user-defined schema does not need to implement this interface, but by
 * the time a schema is passed to a method in a user-defined schema or
 * user-defined table, it will have been wrapped in this interface.</p>
 */
public interface SchemaPlus extends Schema {
  // override with stricter return
  SchemaPlus getSubSchema(String name);

  /** Adds a schema as a sub-schema of this schema, and returns the wrapped
   * object. */
  SchemaPlus add(Schema schema);

  /** Adds a table to this schema. */
  void add(String name, Table table);

  /** Adds a table function to this schema. */
  void add(String name, TableFunction table);

  boolean isMutable();

  /** Returns an underlying object. */
  <T> T unwrap(Class<T> clazz);

  SchemaPlus addRecursive(Schema schema);
}

// End SchemaPlus.java
