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

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.TableExpressionFactory;
import org.apache.calcite.materialize.Lattice;
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
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.schema.ModifiableTable;
import org.apache.calcite.schema.Path;
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

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.AbstractList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Implementation of {@link org.apache.calcite.plan.RelOptTable}.
 */
public class RelOptTableImpl extends Prepare.AbstractPreparingTable {
  private final @Nullable RelOptSchema schema;
  private final RelDataType rowType;
  private final @Nullable Table table;
  private final @Nullable TableExpressionFactory tableExpressionFactory;
  private final ImmutableList<String> names;

  /** Estimate for the row count, or null.
   *
   * <p>If not null, overrides the estimate from the actual table.
   *
   * <p>Useful when a table that contains a materialized query result is being
   * used to replace a query expression that wildly underestimates the row
   * count. Now the materialized table can tell the same lie. */
  private final @Nullable Double rowCount;

  private RelOptTableImpl(
      @Nullable RelOptSchema schema,
      RelDataType rowType,
      List<String> names,
      @Nullable Table table,
      @Nullable TableExpressionFactory tableExpressionFactory,
      @Nullable Double rowCount) {
    this.schema = schema;
    this.rowType = requireNonNull(rowType, "rowType");
    this.names = ImmutableList.copyOf(names);
    this.table = table; // may be null
    this.tableExpressionFactory = tableExpressionFactory; // may be null
    this.rowCount = rowCount; // may be null
  }

  public static RelOptTableImpl create(
      @Nullable RelOptSchema schema,
      RelDataType rowType,
      List<String> names,
      Expression expression) {
    return new RelOptTableImpl(schema, rowType, names, null,
        c -> expression, null);
  }

  @Deprecated // to be removed before 2.0
  public static RelOptTableImpl create(
      @Nullable RelOptSchema schema,
      RelDataType rowType,
      List<String> names,
      Table table,
      Expression expression) {
    return create(schema, rowType, names, table, c -> expression);
  }

  /**
   * Creates {@link RelOptTableImpl} instance with specified arguments
   * and row count obtained from table statistic.
   *
   * @param schema table schema
   * @param rowType table row type
   * @param names full table path
   * @param table table
   * @param expressionFactory expression function for accessing table data
   *                          in the generated code
   *
   * @return {@link RelOptTableImpl} instance
   */
  public static RelOptTableImpl create(
      @Nullable RelOptSchema schema,
      RelDataType rowType,
      List<String> names,
      Table table,
      TableExpressionFactory expressionFactory) {
    return new RelOptTableImpl(schema, rowType, names, table,
        expressionFactory, table.getStatistic().getRowCount());
  }

  public static RelOptTableImpl create(@Nullable RelOptSchema schema, RelDataType rowType,
      Table table, Path path) {
    final SchemaPlus schemaPlus = MySchemaPlus.create(path);
    return new RelOptTableImpl(schema, rowType, Pair.left(path), table,
        c -> Schemas.getTableExpression(schemaPlus, Util.last(path).left, table, c),
        table.getStatistic().getRowCount());
  }

  public static RelOptTableImpl create(@Nullable RelOptSchema schema, RelDataType rowType,
      final CalciteSchema.TableEntry tableEntry, @Nullable Double rowCount) {
    final Table table = tableEntry.getTable();
    return new RelOptTableImpl(schema, rowType, tableEntry.path(), table,
        c -> Schemas.getTableExpression(tableEntry.schema.plus(), tableEntry.name, table, c),
        rowCount);
  }

  /**
   * Creates a copy of this RelOptTable. The new RelOptTable will have newRowType.
   */
  public RelOptTableImpl copy(RelDataType newRowType) {
    return new RelOptTableImpl(this.schema, newRowType, this.names, this.table,
        this.tableExpressionFactory, this.rowCount);
  }

  @Override public String toString() {
    return "RelOptTableImpl{"
        + "schema=" + schema
        + ", names= " + names
        + ", table=" + table
        + ", rowType=" + rowType
        + '}';
  }

  public static RelOptTableImpl create(@Nullable RelOptSchema schema,
      RelDataType rowType, Table table, ImmutableList<String> names) {
    assert table instanceof TranslatableTable
        || table instanceof ScannableTable
        || table instanceof ModifiableTable;
    return new RelOptTableImpl(schema, rowType, names, table, null, null);
  }

  @Override public <T extends Object> @Nullable T unwrap(Class<T> clazz) {
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
    if (clazz == CalciteSchema.class && schema != null) {
      return clazz.cast(
          Schemas.subSchema(((CalciteCatalogReader) schema).rootSchema,
              Util.skipLast(getQualifiedName())));
    }
    return null;
  }

  @Override public @Nullable Expression getExpression(Class clazz) {
    if (tableExpressionFactory == null) {
      return null;
    }
    return tableExpressionFactory.create(clazz);
  }

  @Override protected RelOptTable extend(Table extendedTable) {
    RelOptSchema schema = requireNonNull(getRelOptSchema(), "relOptSchema");
    final RelDataType extendedRowType =
        extendedTable.getRowType(schema.getTypeFactory());
    return new RelOptTableImpl(schema, extendedRowType, getQualifiedName(),
        extendedTable, tableExpressionFactory, getRowCount());
  }

  @Override public boolean equals(@Nullable Object obj) {
    return obj instanceof RelOptTableImpl
        && this.rowType.equals(((RelOptTableImpl) obj).getRowType())
        && this.table == ((RelOptTableImpl) obj).table;
  }

  @Override public int hashCode() {
    return (this.table == null)
        ? super.hashCode() : this.table.hashCode();
  }
  @Override public double getRowCount() {
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

  @Override public @Nullable RelOptSchema getRelOptSchema() {
    return schema;
  }

  @Override public RelNode toRel(ToRelContext context) {
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
              this.tableExpressionFactory, this.rowCount) {
            @Override public <T extends Object> @Nullable T unwrap(Class<T> clazz) {
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
    return LogicalTableScan.create(context.getCluster(), this, context.getTableHints());
  }

  @Override public @Nullable List<RelCollation> getCollationList() {
    if (table != null) {
      return table.getStatistic().getCollations();
    }
    return ImmutableList.of();
  }

  @Override public @Nullable RelDistribution getDistribution() {
    if (table != null) {
      return table.getStatistic().getDistribution();
    }
    return RelDistributionTraitDef.INSTANCE.getDefault();
  }

  @Override public boolean isKey(ImmutableBitSet columns) {
    if (table != null) {
      return table.getStatistic().isKey(columns);
    }
    return false;
  }

  @Override public @Nullable List<ImmutableBitSet> getKeys() {
    if (table != null) {
      return table.getStatistic().getKeys();
    }
    return ImmutableList.of();
  }

  @Override public @Nullable List<RelReferentialConstraint> getReferentialConstraints() {
    if (table != null) {
      return table.getStatistic().getReferentialConstraints();
    }
    return ImmutableList.of();
  }

  @Override public RelDataType getRowType() {
    return rowType;
  }

  @Override public boolean supportsModality(SqlModality modality) {
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

  @Override public List<String> getQualifiedName() {
    return names;
  }

  @Override public SqlMonotonicity getMonotonicity(String columnName) {
    if (table == null) {
      return SqlMonotonicity.NOT_MONOTONIC;
    }
    List<RelCollation> collations = table.getStatistic().getCollations();
    if (collations == null) {
      return SqlMonotonicity.NOT_MONOTONIC;
    }
    for (RelCollation collation : collations) {
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

  @Override public SqlAccessType getAllowedAccess() {
    return SqlAccessType.ALL;
  }

  /** Helper for {@link #getColumnStrategies()}. */
  public static List<ColumnStrategy> columnStrategies(final RelOptTable table) {
    final int fieldCount = table.getRowType().getFieldCount();
    final InitializerExpressionFactory ief =
        Util.first(table.unwrap(InitializerExpressionFactory.class),
            NullInitializerExpressionFactory.INSTANCE);
    return new AbstractList<ColumnStrategy>() {
      @Override public int size() {
        return fieldCount;
      }

      @Override public ColumnStrategy get(int index) {
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
        break;
      default:
        break;
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
        requireNonNull(table.getRelOptSchema(),
            () -> "relOptSchema for table " + table).getTypeFactory().builder();
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
    private final @Nullable SchemaPlus parent;
    private final String name;
    private final Schema schema;

    MySchemaPlus(@Nullable SchemaPlus parent, String name, Schema schema) {
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

    @Override public @Nullable SchemaPlus getParentSchema() {
      return parent;
    }

    @Override public String getName() {
      return name;
    }

    @Override public @Nullable SchemaPlus getSubSchema(String name) {
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

    @Override public <T extends Object> @Nullable T unwrap(Class<T> clazz) {
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

    @Override public @Nullable Table getTable(String name) {
      return schema.getTable(name);
    }

    @Override public Set<String> getTableNames() {
      return schema.getTableNames();
    }

    @Override public @Nullable RelProtoDataType getType(String name) {
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

    @Override public Expression getExpression(@Nullable SchemaPlus parentSchema,
        String name) {
      return schema.getExpression(parentSchema, name);
    }

    @Override public Schema snapshot(SchemaVersion version) {
      throw new UnsupportedOperationException();
    }
  }
}
