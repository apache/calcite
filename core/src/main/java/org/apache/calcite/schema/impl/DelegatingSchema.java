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
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Table;

import java.util.Collection;
import java.util.Set;

/**
 * Implementation of {@link org.apache.calcite.schema.Schema} that delegates to
 * an underlying schema.
 */
public class DelegatingSchema implements Schema {
  protected final Schema schema;

  /**
   * Creates a DelegatingSchema.
   *
   * @param schema Underlying schema
   */
  public DelegatingSchema(Schema schema) {
    this.schema = schema;
  }

  @Override public String toString() {
    return "DelegatingSchema(delegate=" + schema + ")";
  }

  public boolean isMutable() {
    return schema.isMutable();
  }

  public Schema snapshot(SchemaVersion version) {
    return schema.snapshot(version);
  }

  public Expression getExpression(SchemaPlus parentSchema, String name) {
    return schema.getExpression(parentSchema, name);
  }

  public Table getTable(String name) {
    return schema.getTable(name);
  }

  public Set<String> getTableNames() {
    return schema.getTableNames();
  }

  public RelProtoDataType getType(String name) {
    return schema.getType(name);
  }

  public Set<String> getTypeNames() {
    return schema.getTypeNames();
  }

  public Collection<Function> getFunctions(String name) {
    return schema.getFunctions(name);
  }

  public Set<String> getFunctionNames() {
    return schema.getFunctionNames();
  }

  public Schema getSubSchema(String name) {
    return schema.getSubSchema(name);
  }

  public Set<String> getSubSchemaNames() {
    return schema.getSubSchemaNames();
  }
}

// End DelegatingSchema.java
