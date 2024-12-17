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
package org.apache.calcite.schema.impl;

import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.lookup.CompatibilityLookup;
import org.apache.calcite.schema.lookup.Lookup;
import org.apache.calcite.util.LazyReference;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Abstract implementation of {@link Schema}.
 *
 * <p>Behavior is as follows:
 * <ul>
 *   <li>The schema has no tables unless you override
 *       {@link #getTableMap()}.</li>
 *   <li>The schema has no functions unless you override
 *       {@link #getFunctionMultimap()}.</li>
 *   <li>The schema has no sub-schemas unless you override
 *       {@link #getSubSchemaMap()}.</li>
 *   <li>The schema is mutable unless you override
 *       {@link #isMutable()}.</li>
 *   <li>The name and parent schema are as specified in the constructor
 *       arguments.</li>
 * </ul>
 */
public class AbstractSchema implements Schema {

  private LazyReference<Lookup<Table>> tables = new LazyReference<>();
  private LazyReference<Lookup<Schema>> subSchemas = new LazyReference<>();

  @Override public Lookup<Table> tables() {
    return tables.getOrCompute(
        () -> new CompatibilityLookup<>(this::getTable, this::getTableNames));
  }

  @Override public Lookup<? extends Schema> subSchemas() {
    return subSchemas.getOrCompute(
        () -> new CompatibilityLookup<>(this::getSubSchema, this::getSubSchemaNames));
  }

  @Override public boolean isMutable() {
    return true;
  }

  @Override public Schema snapshot(SchemaVersion version) {
    return this;
  }

  @Override public Expression getExpression(@Nullable SchemaPlus parentSchema, String name) {
    requireNonNull(parentSchema, "parentSchema");
    return Schemas.subSchemaExpression(parentSchema, name, getClass());
  }

  /**
   * Returns a map of tables in this schema by name.
   *
   * <p>The implementations of {@link #getTableNames()}
   * and {@link #getTable(String)} depend on this map.
   * The default implementation of this method returns the empty map.
   * Override this method to change their behavior.
   *
   * @return Map of tables in this schema by name
   */
  protected Map<String, Table> getTableMap() {
    return ImmutableMap.of();
  }

  @Deprecated @Override public final Set<String> getTableNames() {
    //noinspection RedundantCast
    return (Set<String>) getTableMap().keySet();
  }

  @Deprecated @Override public final @Nullable Table getTable(String name) {
    return getTableMap().get(name);
  }

  /**
   * Returns a map of types in this schema by name.
   *
   * <p>The implementations of {@link #getTypeNames()}
   * and {@link #getType(String)} depend on this map.
   * The default implementation of this method returns the empty map.
   * Override this method to change their behavior.
   *
   * @return Map of types in this schema by name
   */
  protected Map<String, RelProtoDataType> getTypeMap() {
    return ImmutableMap.of();
  }

  @Override public @Nullable RelProtoDataType getType(String name) {
    return getTypeMap().get(name);
  }

  @Override public Set<String> getTypeNames() {
    //noinspection RedundantCast
    return (Set<String>) getTypeMap().keySet();
  }

  /**
   * Returns a multi-map of functions in this schema by name.
   * It is a multi-map because functions are overloaded; there may be more than
   * one function in a schema with a given name (as long as they have different
   * parameter lists).
   *
   * <p>The implementations of {@link #getFunctionNames()}
   * and {@link Schema#getFunctions(String)} depend on this map.
   * The default implementation of this method returns the empty multi-map.
   * Override this method to change their behavior.
   *
   * @return Multi-map of functions in this schema by name
   */
  protected Multimap<String, Function> getFunctionMultimap() {
    return ImmutableMultimap.of();
  }

  @Override public final Collection<Function> getFunctions(String name) {
    return getFunctionMultimap().get(name); // never null
  }

  @Override public final Set<String> getFunctionNames() {
    return getFunctionMultimap().keySet();
  }

  /**
   * Returns a map of sub-schemas in this schema by name.
   *
   * <p>The implementations of {@link #getSubSchemaNames()}
   * and {@link #getSubSchema(String)} depend on this map.
   * The default implementation of this method returns the empty map.
   * Override this method to change their behavior.
   *
   * @return Map of sub-schemas in this schema by name
   */
  protected Map<String, Schema> getSubSchemaMap() {
    return ImmutableMap.of();
  }

  @Deprecated @Override public final Set<String> getSubSchemaNames() {
    //noinspection RedundantCast
    return (Set<String>) getSubSchemaMap().keySet();
  }

  @Deprecated @Override public final @Nullable Schema getSubSchema(String name) {
    return getSubSchemaMap().get(name);
  }

  /** Schema factory that creates an
   * {@link org.apache.calcite.schema.impl.AbstractSchema}. */
  public static class Factory implements SchemaFactory {
    public static final Factory INSTANCE = new Factory();

    private Factory() {}

    @Override public Schema create(SchemaPlus parentSchema, String name,
        Map<String, Object> operand) {
      return new AbstractSchema();
    }
  }
}
