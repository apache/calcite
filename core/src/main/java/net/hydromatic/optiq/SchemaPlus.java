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
package net.hydromatic.optiq;

import net.hydromatic.optiq.materialize.Lattice;

import com.google.common.collect.ImmutableList;

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
  /**
   * Returns the parent schema, or null if this schema has no parent.
   */
  SchemaPlus getParentSchema();

  /**
   * Returns the name of this schema.
   *
   * <p>The name must not be null, and must be unique within its parent.
   * The root schema is typically named "".
   */
  String getName();

  // override with stricter return
  SchemaPlus getSubSchema(String name);

  /** Adds a schema as a sub-schema of this schema, and returns the wrapped
   * object. */
  SchemaPlus add(String name, Schema schema);

  /** Adds a table to this schema. */
  void add(String name, Table table);

  /** Adds a function to this schema. */
  void add(String name, Function function);

  /** Adds a lattice to this schema. */
  void add(String name, Lattice lattice);

  boolean isMutable();

  /** Returns an underlying object. */
  <T> T unwrap(Class<T> clazz);

  void setPath(ImmutableList<ImmutableList<String>> path);

  void setCacheEnabled(boolean cache);

  boolean isCacheEnabled();
}

// End SchemaPlus.java
