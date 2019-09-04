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
package org.apache.calcite.prepare;

import org.apache.calcite.adapter.enumerable.EnumerableTableScan;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.materialize.Lattice;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.schema.FilterableTable;
import org.apache.calcite.schema.ModifiableTable;
import org.apache.calcite.schema.Path;
import org.apache.calcite.schema.ProjectableFilterableTable;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.StreamableTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TemporalTable;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.Wrapper;
import org.apache.calcite.sql.SqlAccessType;
import org.apache.calcite.sql.validate.SqlModality;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.sql2rel.InitializerExpressionFactory;
import org.apache.calcite.sql2rel.NullInitializerExpressionFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;

import java.util.AbstractList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/**
 * Implementation of {@link org.apache.calcite.plan.RelOptTable}.
 */
public class RelOptTableImpl extends Prepare.AbstractPreparingTable {
  private final RelOptSchema schema;
  private final RelDataType rowType;
  private final Table table;
  private final Function<Class, Expression> expressionFunction;
  private final ImmutableList<String> names;

  /** Estimate for the row count, or null.
   *
   * <p>If not null, overrides the estimate from the actual table.
   *
   * <p>Useful when a table that contains a materialized query result is being
   * used to replace a query expression that wildly underestimates the row
   * count. Now the materialized table can tell the same lie. */
  private final Double rowCount;

  private RelOptTableImpl(
      RelOptSchema schema,
      RelDataType rowType,
      List<String> names,
      Table table,
      Function<Class, Expression> expressionFunction,
      Double rowCount) {
    this.schema = schema;
    this.rowType = Objects.requireNonNull(rowType);
    this.names = ImmutableList.copyOf(names);
    this.table = table; // may be null
    this.expressionFunction = expressionFunction; // may be null
    this.rowCount = rowCount; // may be null
  }

  public static RelOptTableImpl create(
      RelOptSchema schema,
      RelDataType rowType,
      List<String> names,
      Expression expression) {
    return new RelOptTableImpl(schema, rowType, names, null,
        c -> expression, null);
  }

  public static RelOptTableImpl create(
      RelOptSchema schema,
      RelDataType rowType,
      List<String> names,
      Table table,
      Expression expression) {
    return new RelOptTableImpl(schema, rowType, names, table,
        c -> expression, table.getStatistic().getRowCount());
  }

  public static RelOptTableImpl create(RelOptSchema schema, RelDataType rowType,
      Table table, Path path) {
    final SchemaPlus schemaPlus = MySchemaPlus.create(path);
    return new RelOptTableImpl(schema, rowType, Pair.left(path), table,
        getClassExpressionFunction(schemaPlus, Util.last(path).left, table),
        table.getStatistic().getRowCount());
  }

  public static RelOptTableImpl create(RelOptSchema schema, RelDataType rowType,
      final CalciteSchema.TableEntry tableEntry, Double rowCount) {
    final Table table = tableEntry.getTable();
    return new RelOptTableImpl(schema, rowType, tableEntry.path(),
        table, getClassExpressionFunction(tableEntry, table), rowCount);
  }

  /**
   * Creates a copy of this RelOptTable. The new RelOptTable will have newRowType.
   */
  public RelOptTableImpl copy(RelDataType newRowType) {
    return new RelOptTableImpl(this.schema, newRowType, this.names, this.table,
        this.expressionFunction, this.rowCount);
  }

  private static Function<Class, Expression> getClassExpressionFunction(
      CalciteSchema.TableEntry tableEntry, Table table) {
    return getClassExpressionFunction(tableEntry.schema.plus(), tableEntry.name,
        table);
  }

  private static Function<Class, Expression> getClassExpressionFunction(
      final SchemaPlus schema, final String tableName, final Table table) {
    if (table instanceof QueryableTable) {
      final QueryableTable queryableTable = (QueryableTable) table;
      return clazz -> queryableTable.getExpression(schema, tableName, clazz);
    } else if (table instanceof ScannableTable
        || table instanceof FilterableTable
        || table instanceof ProjectableFilterableTable) {
      return clazz -> Schemas.tableExpression(schema, Object[].class, tableName,
          table.getClass());
    } else if (table instanceof StreamableTable) {
      return getClassExpressionFunction(schema, tableName,
          ((StreamableTable) table).stream());
    } else {
      return input -> {
        throw new UnsupportedOperationException();
      };
    }
  }

  public static RelOptTableImpl create(RelOptSchema schema,
      RelDataType rowType, Table table, ImmutableList<String> names) {
    assert table instanceof TranslatableTable
        || table instanceof ScannableTable
        || table instanceof ModifiableTable;
    return new RelOptTableImpl(schema, rowType, names, table, null, null);
  }

  public <T> T unwrap(Class<T> clazz) {
    if (clazz.isInstance(this)) {
      return clazz.cast(this);
    }
    if (clazz.isInstance(table)) {
      return clazz.cast(table);
    }
    if (table instanceof Wrapper) {
      final T t = ((Wrapper) table).unwrap(clazz);
      if (t != null) {
        return t;
      }
    }
    if (clazz == CalciteSchema.class) {
      return clazz.cast(
          Schemas.subSchema(((CalciteCatalogReader) schema).rootSchema,
              Util.skipLast(getQualifiedName())));
    }
    return null;
  }

  public Expression getExpression(Class clazz) {
    if (expressionFunction == null) {
      return null;
    }
    return expressionFunction.apply(clazz);
  }

  @Override protected RelOptTable extend(Table extendedTable) {
    final RelDataType extendedRowType =
        extendedTable.getRowType(getRelOptSchema().getTypeFactory());
    return new RelOptTableImpl(getRelOptSchema(), extendedRowType, getQualifiedName(),
        extendedTable, expressionFunction, getRowCount());
  }

  @Override public boolean equals(Object obj) {
    return obj instanceof RelOptTableImpl
        && this.rowType.equals(((RelOptTableImpl) obj).getRowType())
        && this.table == ((RelOptTableImpl) obj).table;
  }

  @Override public int hashCode() {
    return (this.table == null)
        ? super.hashCode() : this.table.hashCode();
  }
  public double getRowCount() {
    if (rowCount != null) {
      return rowCount;
    }
    if (table != null) {
      final Double rowCount = table.getStatistic().getRowCount();
      if (rowCount != null) {
        return rowCount;
      }
    }
    return 100d;
  }

  public RelOptSchema getRelOptSchema() {
    return schema;
  }

  public RelNode toRel(ToRelContext context) {
    // Make sure rowType's list is immutable. If rowType is DynamicRecordType, creates a new
    // RelOptTable by replacing with immutable RelRecordType using the same field list.
    if (this.getRowType().isDynamicStruct()) {
      final RelDataType staticRowType = new RelRecordType(getRowType().getFieldList());
      final RelOptTable relOptTable = this.copy(staticRowType);
      return relOptTable.toRel(context);
    }

    // If there are any virtual columns, create a copy of this table without
    // those virtual columns.
    final List<ColumnStrategy> strategies = getColumnStrategies();
    if (strategies.contains(ColumnStrategy.VIRTUAL)) {
      final RelDataTypeFactory.Builder b =
          context.getCluster().getTypeFactory().builder();
      for (RelDataTypeField field : rowType.getFieldList()) {
        if (strategies.get(field.getIndex()) != ColumnStrategy.VIRTUAL) {
          b.add(field.getName(), field.getType());
        }
      }
      final RelOptTable relOptTable =
          new RelOptTableImpl(this.schema, b.build(), this.names, this.table,
              this.expressionFunction, this.rowCount) {
            @Override public <T> T unwrap(Class<T> clazz) {
              if (clazz.isAssignableFrom(InitializerExpressionFactory.class)) {
                return clazz.cast(NullInitializerExpressionFactory.INSTANCE);
              }
              return super.unwrap(clazz);
            }
          };
      return relOptTable.toRel(context);
    }

    if (table instanceof TranslatableTable) {
      return ((TranslatableTable) table).toRel(context, this);
    }
    final RelOptCluster cluster = context.getCluster();
    if (Hook.ENABLE_BINDABLE.get(false)) {
      return LogicalTableScan.create(cluster, this);
    }
    if (CalciteSystemProperty.ENABLE_ENUMERABLE.value()
        && table instanceof QueryableTable) {
      return EnumerableTableScan.create(cluster, this);
    }
    if (table instanceof ScannableTable
        || table instanceof FilterableTable
        || table instanceof ProjectableFilterableTable) {
      return LogicalTableScan.create(cluster, this);
    }
    if (CalciteSystemProperty.ENABLE_ENUMERABLE.value()) {
      return EnumerableTableScan.create(cluster, this);
    }
    throw new AssertionError();
  }

  public List<RelCollation> getCollationList() {
    if (table != null) {
      return table.getStatistic().getCollations();
    }
    return ImmutableList.of();
  }

  public RelDistribution getDistribution() {
    if (table != null) {
      return table.getStatistic().getDistribution();
    }
    return RelDistributionTraitDef.INSTANCE.getDefault();
  }

  public boolean isKey(ImmutableBitSet columns) {
    if (table != null) {
      return table.getStatistic().isKey(columns);
    }
    return false;
  }

  public List<RelReferentialConstraint> getReferentialConstraints() {
    if (table != null) {
      return table.getStatistic().getReferentialConstraints();
    }
    return ImmutableList.of();
  }

  public RelDataType getRowType() {
    return rowType;
  }

  public boolean supportsModality(SqlModality modality) {
    switch (modality) {
    case STREAM:
      return table instanceof StreamableTable;
    default:
      return !(table instanceof StreamableTable);
    }
  }

  @Override public boolean isTemporal() {
    return table instanceof TemporalTable;
  }

  public List<String> getQualifiedName() {
    return names;
  }

  public SqlMonotonicity getMonotonicity(String columnName) {
    for (RelCollation collation : table.getStatistic().getCollations()) {
      final RelFieldCollation fieldCollation =
          collation.getFieldCollations().get(0);
      final int fieldIndex = fieldCollation.getFieldIndex();
      if (fieldIndex < rowType.getFieldCount()
          && rowType.getFieldNames().get(fieldIndex).equals(columnName)) {
        return fieldCollation.direction.monotonicity();
      }
    }
    return SqlMonotonicity.NOT_MONOTONIC;
  }

  public SqlAccessType getAllowedAccess() {
    return SqlAccessType.ALL;
  }

  /** Helper for {@link #getColumnStrategies()}. */
  public static List<ColumnStrategy> columnStrategies(final RelOptTable table) {
    final int fieldCount = table.getRowType().getFieldCount();
    final InitializerExpressionFactory ief =
        Util.first(table.unwrap(InitializerExpressionFactory.class),
            NullInitializerExpressionFactory.INSTANCE);
    return new AbstractList<ColumnStrategy>() {
      public int size() {
        return fieldCount;
      }

      public ColumnStrategy get(int index) {
        return ief.generationStrategy(table, index);
      }
    };
  }

  /** Converts the ordinal of a field into the ordinal of a stored field.
   * That is, it subtracts the number of virtual fields that come before it. */
  public static int realOrdinal(final RelOptTable table, int i) {
    List<ColumnStrategy> strategies = table.getColumnStrategies();
    int n = 0;
    for (int j = 0; j < i; j++) {
      switch (strategies.get(j)) {
      case VIRTUAL:
        ++n;
      }
    }
    return i - n;
  }

  /** Returns the row type of a table after any {@link ColumnStrategy#VIRTUAL}
   * columns have been removed. This is the type of the records that are
   * actually stored. */
  public static RelDataType realRowType(RelOptTable table) {
    final RelDataType rowType = table.getRowType();
    final List<ColumnStrategy> strategies = columnStrategies(table);
    if (!strategies.contains(ColumnStrategy.VIRTUAL)) {
      return rowType;
    }
    final RelDataTypeFactory.Builder builder =
        table.getRelOptSchema().getTypeFactory().builder();
    for (RelDataTypeField field : rowType.getFieldList()) {
      if (strategies.get(field.getIndex()) != ColumnStrategy.VIRTUAL) {
        builder.add(field);
      }
    }
    return builder.build();
  }

  /** Implementation of {@link SchemaPlus} that wraps a regular schema and knows
   * its name and parent.
   *
   * <p>It is read-only, and functionality is limited in other ways, it but
   * allows table expressions to be generated. */
  private static class MySchemaPlus implements SchemaPlus {
    private final SchemaPlus parent;
    private final String name;
    private final Schema schema;

    MySchemaPlus(SchemaPlus parent, String name, Schema schema) {
      this.parent = parent;
      this.name = name;
      this.schema = schema;
    }

    public static MySchemaPlus create(Path path) {
      final Pair<String, Schema> pair = Util.last(path);
      final SchemaPlus parent;
      if (path.size() == 1) {
        parent = null;
      } else {
        parent = create(path.parent());
      }
      return new MySchemaPlus(parent, pair.left, pair.right);
    }

    @Override public SchemaPlus getParentSchema() {
      return parent;
    }

    @Override public String getName() {
      return name;
    }

    @Override public SchemaPlus getSubSchema(String name) {
      final Schema subSchema = schema.getSubSchema(name);
      return subSchema == null ? null : new MySchemaPlus(this, name, subSchema);
    }

    @Override public SchemaPlus add(String name, Schema schema) {
      throw new UnsupportedOperationException();
    }

    @Override public void add(String name, Table table) {
      throw new UnsupportedOperationException();
    }

    @Override public void add(String name,
        org.apache.calcite.schema.Function function) {
      throw new UnsupportedOperationException();
    }

    @Override public void add(String name, RelProtoDataType type) {
      throw new UnsupportedOperationException();
    }

    @Override public void add(String name, Lattice lattice) {
      throw new UnsupportedOperationException();
    }

    @Override public boolean isMutable() {
      return schema.isMutable();
    }

    @Override public <T> T unwrap(Class<T> clazz) {
      return null;
    }

    @Override public void setPath(ImmutableList<ImmutableList<String>> path) {
      throw new UnsupportedOperationException();
    }

    @Override public void setCacheEnabled(boolean cache) {
      throw new UnsupportedOperationException();
    }

    @Override public boolean isCacheEnabled() {
      return false;
    }

    @Override public Table getTable(String name) {
      return schema.getTable(name);
    }

    @Override public Set<String> getTableNames() {
      return schema.getTableNames();
    }

    @Override public RelProtoDataType getType(String name) {
      return schema.getType(name);
    }

    @Override public Set<String> getTypeNames() {
      return schema.getTypeNames();
    }

    @Override public Collection<org.apache.calcite.schema.Function>
    getFunctions(String name) {
      return schema.getFunctions(name);
    }

    @Override public Set<String> getFunctionNames() {
      return schema.getFunctionNames();
    }

    @Override public Set<String> getSubSchemaNames() {
      return schema.getSubSchemaNames();
    }

    @Override public Expression getExpression(SchemaPlus parentSchema,
        String name) {
      return schema.getExpression(parentSchema, name);
    }

    @Override public Schema snapshot(SchemaVersion version) {
      throw new UnsupportedOperationException();
    }
  }
}

// End RelOptTableImpl.java
