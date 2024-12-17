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
package org.apache.calcite.adapter.jdbc;

import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.lookup.LikePattern;
import org.apache.calcite.schema.lookup.Lookup;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Base class for JDBC schemas.
 */
public abstract class JdbcBaseSchema implements Schema {

  @Override public abstract Lookup<Table> tables();


  @Deprecated @Override public @Nullable Table getTable(String name) {
    return tables().get(name);
  }

  @Deprecated @Override public Set<String> getTableNames() {
    return tables().getNames(LikePattern.any());
  }

  @Override public abstract Lookup<? extends Schema> subSchemas();

  @Deprecated @Override public @Nullable Schema getSubSchema(String name) {
    return subSchemas().get(name);
  }

  @Deprecated @Override public Set<String> getSubSchemaNames() {
    return subSchemas().getNames(LikePattern.any());
  }


  @Override public @Nullable RelProtoDataType getType(String name) {
    return null;
  }

  @Override public Set<String> getTypeNames() {
    return Collections.emptySet();
  }

  @Override public final Collection<Function> getFunctions(String name) {
    return Collections.emptyList();
  }

  @Override public final Set<String> getFunctionNames() {
    return Collections.emptySet();
  }

  @Override public Expression getExpression(final @Nullable SchemaPlus parentSchema,
      final String name) {
    requireNonNull(parentSchema, "parentSchema");
    return Schemas.subSchemaExpression(parentSchema, name, getClass());
  }

  @Override public boolean isMutable() {
    return false;
  }

  @Override public Schema snapshot(final SchemaVersion version) {
    return this;
  }
}
