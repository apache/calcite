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
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.ModifiableView;
import org.apache.calcite.schema.Path;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TableMacro;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;

import java.lang.reflect.Type;
import java.util.Collections;
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

  public ViewTable(Type elementType, RelProtoDataType rowType, String viewSql,
      List<String> schemaPath) {
    super(elementType);
    this.viewSql = viewSql;
    this.schemaPath = ImmutableList.copyOf(schemaPath);
    this.protoRowType = rowType;
  }

  /** Table macro that returns a view. */
  @Deprecated // to be removed before 2.0
  public static ViewTableMacro viewMacro(SchemaPlus schema,
      final String viewSql, final List<String> schemaPath) {
    return viewMacro(schema, viewSql, schemaPath, Boolean.TRUE);
  }

  /** Table macro that returns a view.
   *
   * @param schema Schema the view will belong to
   * @param viewSql SQL query
   * @param schemaPath Path of schema
   * @param modifiable Whether view is modifiable, or null to deduce it
   */
  public static ViewTableMacro viewMacro(SchemaPlus schema, String viewSql,
      List<String> schemaPath, Boolean modifiable) {
    return new ViewTableMacro(CalciteSchema.from(schema), viewSql, schemaPath,
        modifiable);
  }

  /** Returns the view's SQL definition. */
  public String getViewSql() {
    return viewSql;
  }

  /** Returns the the schema path of the view. */
  public List<String> getSchemaPath() {
    return schemaPath;
  }

  @Override public Schema.TableType getJdbcTableType() {
    return Schema.TableType.VIEW;
  }

  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return protoRowType.apply(typeFactory);
  }

  public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
      SchemaPlus schema, String tableName) {
    return queryProvider.createQuery(
        getExpression(schema, tableName, Queryable.class), elementType);
  }

  public RelNode toRel(
      RelOptTable.ToRelContext context,
      RelOptTable relOptTable) {
    return expandView(context, relOptTable.getRowType(), viewSql);
  }

  private RelNode expandView(
      RelOptTable.ToRelContext preparingStmt,
      RelDataType rowType,
      String queryString) {
    try {
      RelNode rel = preparingStmt.expandView(rowType, queryString, schemaPath);

      rel = RelOptUtil.createCastRel(rel, rowType, true);
      //rel = viewExpander.flattenTypes(rel, false);
      return rel;
    } catch (Throwable e) {
      throw Util.newInternal(
          e, "Error while parsing view definition:  " + queryString);
    }
  }

  /** Table function that implements a view. It returns the operator
   * tree of the view's SQL query. */
  static class ViewTableMacro implements TableMacro {
    protected final String viewSql;
    protected final CalciteSchema schema;
    private final Boolean modifiable;
    /** Typically null. If specified, overrides the path of the schema as the
     * context for validating {@code viewSql}. */
    protected final List<String> schemaPath;

    ViewTableMacro(CalciteSchema schema, String viewSql, List<String> schemaPath,
        Boolean modifiable) {
      this.viewSql = viewSql;
      this.schema = schema;
      this.modifiable = modifiable;
      this.schemaPath =
          schemaPath == null ? null : ImmutableList.copyOf(schemaPath);
    }

    public List<FunctionParameter> getParameters() {
      return Collections.emptyList();
    }

    public TranslatableTable apply(List<Object> arguments) {
      CalcitePrepare.AnalyzeViewResult parsed =
          Schemas.analyzeView(MaterializedViewTable.MATERIALIZATION_CONNECTION,
              schema, schemaPath, viewSql, modifiable != null && modifiable);
      final List<String> schemaPath1 =
          schemaPath != null ? schemaPath : schema.path(null);
      final JavaTypeFactory typeFactory = (JavaTypeFactory) parsed.typeFactory;
      final Type elementType = typeFactory.getJavaClass(parsed.rowType);
      if ((modifiable == null || modifiable) && parsed.table != null) {
        return new ModifiableViewTable(elementType,
            RelDataTypeImpl.proto(parsed.rowType), viewSql, schemaPath1,
            parsed.table, Schemas.path(schema.root(), parsed.tablePath),
            parsed.constraint, parsed.columnMapping);
      } else {
        return new ViewTable(elementType,
            RelDataTypeImpl.proto(parsed.rowType), viewSql, schemaPath1);
      }
    }
  }

  /** Extension to {@link ViewTable} that is modifiable. */
  static class ModifiableViewTable extends ViewTable
      implements ModifiableView {
    private final Table table;
    private final Path tablePath;
    private final RexNode constraint;
    private final ImmutableIntList columnMapping;

    public ModifiableViewTable(Type elementType, RelProtoDataType rowType,
        String viewSql, List<String> schemaPath, Table table,
        Path tablePath, RexNode constraint,
        ImmutableIntList columnMapping) {
      super(elementType, rowType, viewSql, schemaPath);
      this.table = table;
      this.tablePath = tablePath;
      this.constraint = constraint;
      this.columnMapping = columnMapping;
    }

    public RexNode getConstraint(RexBuilder rexBuilder,
        RelDataType tableRowType) {
      return rexBuilder.copy(constraint);
    }

    public ImmutableIntList getColumnMapping() {
      return columnMapping;
    }

    public Table getTable() {
      return table;
    }

    public Path getTablePath() {
      return tablePath;
    }
  }
}

// End ViewTable.java
