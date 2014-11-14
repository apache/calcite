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
package net.hydromatic.optiq.impl;

import net.hydromatic.linq4j.expressions.Expression;

import net.hydromatic.optiq.*;
import net.hydromatic.optiq.Table;

import com.google.common.collect.*;

import java.util.*;

/**
 * Abstract implementation of {@link Schema}.
 *
 * <p>Behavior is as follows:</p>
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
 *
 * <p>For constructing custom maps and multi-maps, we recommend
 * {@link com.google.common.base.Suppliers} and
 * {@link com.google.common.collect.Maps}.</p>
 */
public class AbstractSchema implements Schema {
  public AbstractSchema() {
  }

  public boolean isMutable() {
    return true;
  }

  public boolean contentsHaveChangedSince(long lastCheck, long now) {
    return false;
  }

  public Expression getExpression(SchemaPlus parentSchema, String name) {
    return Schemas.subSchemaExpression(parentSchema, name, getClass());
  }

  /**
   * Returns a map of tables in this schema by name.
   *
   * <p>The implementations of {@link #getTableNames()}
   * and {@link #getTable(String)} depend on this map.
   * The default implementation of this method returns the empty map.
   * Override this method to change their behavior.</p>
   */
  protected Map<String, Table> getTableMap() {
    return ImmutableMap.of();
  }

  public final Set<String> getTableNames() {
    return getTableMap().keySet();
  }

  public final Table getTable(String name) {
    return getTableMap().get(name);
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
   * Override this method to change their behavior.</p>
   */
  protected Multimap<String, Function> getFunctionMultimap() {
    return ImmutableMultimap.of();
  }

  public final Collection<Function> getFunctions(String name) {
    return getFunctionMultimap().get(name); // never null
  }

  public final Set<String> getFunctionNames() {
    return getFunctionMultimap().keySet();
  }

  /**
   * Returns a map of tables in this schema by name.
   *
   * <p>The implementations of {@link #getTableNames()}
   * and {@link #getTable(String)} depend on this map.
   * The default implementation of this method returns the empty map.
   * Override this method to change their behavior.</p>
   */
  protected Map<String, Schema> getSubSchemaMap() {
    return ImmutableMap.of();
  }

  public final Set<String> getSubSchemaNames() {
    return getSubSchemaMap().keySet();
  }

  public final Schema getSubSchema(String name) {
    return getSubSchemaMap().get(name);
  }
}

// End AbstractSchema.java
