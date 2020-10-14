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

import org.apache.calcite.materialize.Lattice;
import org.apache.calcite.rel.type.RelProtoDataType;

import com.google.common.collect.ImmutableList;

/**
 * Extension to the {@link Schema} interface.
 * 对 Schema 接口的拓展。
 *
 * <p>
 *   Given a user-defined schema that implements the {@link Schema} interface,
 *   Calcite creates a wrapper that implements the {@code SchemaPlus} interface.
 *   This provides extra functionality, such as access to tables that have been added explicitly(明确的、明白的).
 *   fixme
 *        给定一个实现了 Schema 的用户自定义的schema，Calcite会创建一个实现了SchemaPlus接口的包装。
 *        SchemaPlus 提供了额外的功能，例如获取明确添加的表。
 *
 * <p>
 *   A user-defined schema does not need to implement this interface,
 *   but by the time a schema is passed to a method in a user-defined schema or user-defined table,
 *   it will have been wrapped in this interface.
 *   fixme
 *        用户自定义的schema不用实现此接口，
 *        但是当 schema 传递给 用户自定义的schema 或 用户自定义的表 中的 方法 时，schema将会被此接口包装。
 *
 * <p>SchemaPlus is intended to be used by users but not instantiated by them.
 * Users should only use the SchemaPlus they are given by the system.
 * The purpose of SchemaPlus is to expose to user code, in a read only manner,
 * some of the extra information about schemas that Calcite builds up when a
 * schema is registered. It appears in several SPI calls as context; for example
 * {@link SchemaFactory#create(SchemaPlus, String, java.util.Map)} contains a
 * parent schema that might be a wrapped instance of a user-defined
 * {@link Schema}, or indeed might not.
 */
public interface SchemaPlus extends Schema {

  /**
   * Returns the parent schema, or null if this schema has no parent.
   *
   * 获取父schema，不存在则返回null。
   */
  SchemaPlus getParentSchema();

  /**
   * Returns the name of this schema.
   * fixme 返回schema的名称。
   *
   * <p>
   *   The name must not be null, and must be unique within its parent.
   *   The root schema is typically named "".
   */
  String getName();

  // override with stricter return
  @Override SchemaPlus getSubSchema(String name);

  /**
   * Adds a schema as a sub-schema of this schema, and returns the wrapped object.
   *
   * 讲一个schema作为一个子schema加入此schema，并返回被包装的对象。
   */
  SchemaPlus add(String name, Schema schema);

  /**
   * Adds a table to this schema.
   *
   * 添加此schema的表。
   */
  void add(String name, Table table);

  /** Adds a function to this schema. */
  void add(String name, Function function);

  /** Adds a type to this schema.  */
  void add(String name, RelProtoDataType type);

  /** Adds a lattice to this schema. */
  void add(String name, Lattice lattice);

  @Override boolean isMutable();

  /** Returns an underlying object. */
  <T> T unwrap(Class<T> clazz);

  void setPath(ImmutableList<ImmutableList<String>> path);

  void setCacheEnabled(boolean cache);

  boolean isCacheEnabled();
}
