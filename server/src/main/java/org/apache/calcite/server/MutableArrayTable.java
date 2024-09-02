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
package org.apache.calcite.server;

import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.Wrapper;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.calcite.sql2rel.InitializerExpressionFactory;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static java.util.Objects.requireNonNull;

/** Table backed by a Java list. */
class MutableArrayTable extends AbstractModifiableTable
    implements Wrapper {
  final List rows = new ArrayList();
  private final RelProtoDataType protoRowType;
  private final InitializerExpressionFactory initializerExpressionFactory;

  /** Creates a MutableArrayTable.
   *
   * @param name Name of table within its schema
   * @param protoStoredRowType Prototype of row type of stored columns (all
   *     columns except virtual columns)
   * @param protoRowType Prototype of row type (all columns)
   * @param initializerExpressionFactory How columns are populated
   */
  MutableArrayTable(String name, RelProtoDataType protoStoredRowType,
      RelProtoDataType protoRowType,
      InitializerExpressionFactory initializerExpressionFactory) {
    super(name);
    requireNonNull(protoStoredRowType, "protoStoredRowType");
    this.protoRowType = requireNonNull(protoRowType, "protoRowType");
    this.initializerExpressionFactory =
        requireNonNull(initializerExpressionFactory,
            "initializerExpressionFactory");
  }

  @Override public Collection getModifiableCollection() {
    return rows;
  }

  @Override public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
      SchemaPlus schema, String tableName) {
    return new AbstractTableQueryable<T>(queryProvider, schema, this,
        tableName) {
      @Override public Enumerator<T> enumerator() {
        //noinspection unchecked
        return (Enumerator<T>) Linq4j.enumerator(rows);
      }
    };
  }

  @Override public Type getElementType() {
    return Object[].class;
  }

  @Override public Expression getExpression(SchemaPlus schema, String tableName,
      Class clazz) {
    return Schemas.tableExpression(schema, getElementType(),
        tableName, clazz);
  }

  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return protoRowType.apply(typeFactory);
  }

  @Override public <C extends Object> @Nullable C unwrap(Class<C> aClass) {
    if (aClass.isInstance(initializerExpressionFactory)) {
      return aClass.cast(initializerExpressionFactory);
    }
    return super.unwrap(aClass);
  }
}
