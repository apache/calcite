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

import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TranslatableTable;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Type;
import java.util.List;

/**
 * Table whose contents are defined using an SQL statement.
 *
 * <p>It is not evaluated; it is expanded during query planning.</p>
 */
public class ViewTable
    extends AbstractQueryableTable
    implements TranslatableTable {
  private final String viewSql;
  private final List<String> schemaPath;
  private final RelProtoDataType protoRowType;
  private final @Nullable List<String> viewPath;

  public ViewTable(Type elementType, RelProtoDataType rowType, String viewSql,
      List<String> schemaPath, @Nullable List<String> viewPath) {
    super(elementType);
    this.viewSql = viewSql;
    this.schemaPath = ImmutableList.copyOf(schemaPath);
    this.protoRowType = rowType;
    this.viewPath = viewPath == null ? null : ImmutableList.copyOf(viewPath);
  }

  @Deprecated // to be removed before 2.0
  public static ViewTableMacro viewMacro(SchemaPlus schema,
      final String viewSql, final List<String> schemaPath) {
    return viewMacro(schema, viewSql, schemaPath, null, Boolean.TRUE);
  }

  @Deprecated // to be removed before 2.0
  public static ViewTableMacro viewMacro(SchemaPlus schema, String viewSql,
      List<String> schemaPath, @Nullable Boolean modifiable) {
    return viewMacro(schema, viewSql, schemaPath, null, modifiable);
  }

  /** Table macro that returns a view.
   *
   * @param schema Schema the view will belong to
   * @param viewSql SQL query
   * @param schemaPath Path of schema
   * @param modifiable Whether view is modifiable, or null to deduce it
   */
  public static ViewTableMacro viewMacro(SchemaPlus schema, String viewSql,
      List<String> schemaPath, @Nullable List<String> viewPath,
      @Nullable Boolean modifiable) {
    return new ViewTableMacro(CalciteSchema.from(schema), viewSql, schemaPath,
        viewPath, modifiable);
  }

  /** Returns the view's SQL definition. */
  public String getViewSql() {
    return viewSql;
  }

  /** Returns the schema path of the view. */
  public List<String> getSchemaPath() {
    return schemaPath;
  }

  /** Returns the path of the view. */
  public @Nullable List<String> getViewPath() {
    return viewPath;
  }

  @Override public Schema.TableType getJdbcTableType() {
    return Schema.TableType.VIEW;
  }

  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return protoRowType.apply(typeFactory);
  }

  @Override public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
      SchemaPlus schema, String tableName) {
    return queryProvider.createQuery(
        getExpression(schema, tableName, Queryable.class), elementType);
  }

  @Override public RelNode toRel(
      RelOptTable.ToRelContext context,
      RelOptTable relOptTable) {
    return expandView(context, relOptTable.getRowType(), viewSql).rel;
  }

  private RelRoot expandView(RelOptTable.ToRelContext context,
      RelDataType rowType, String queryString) {
    try {
      final RelRoot root =
          context.expandView(rowType, queryString, schemaPath, viewPath);
      final RelNode rel = RelOptUtil.createCastRel(root.rel, rowType, true);
      // Expand any views
      final RelNode rel2 = rel.accept(
          new RelShuttleImpl() {
            @Override public RelNode visit(TableScan scan) {
              final RelOptTable table = scan.getTable();
              final TranslatableTable translatableTable =
                  table.unwrap(TranslatableTable.class);
              if (translatableTable != null) {
                return translatableTable.toRel(context, table);
              }
              return super.visit(scan);
            }
          });
      return root.withRel(rel2);
    } catch (Exception e) {
      throw new RuntimeException("Error while parsing view definition: "
          + queryString, e);
    }
  }
}
