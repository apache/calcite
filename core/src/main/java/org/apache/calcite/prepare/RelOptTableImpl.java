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
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.ExtensibleTable;
import org.apache.calcite.schema.FilterableTable;
import org.apache.calcite.schema.ProjectableFilterableTable;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.StreamableTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.sql.SqlAccessType;
import org.apache.calcite.sql.validate.SqlModality;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Implementation of {@link org.apache.calcite.plan.RelOptTable}.
 */
public class RelOptTableImpl implements Prepare.PreparingTable {
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
    this.rowType = Preconditions.checkNotNull(rowType);
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
    //noinspection unchecked
    final Function<Class, Expression> expressionFunction =
        (Function) Functions.constant(expression);
    return new RelOptTableImpl(schema, rowType, names, null,
        expressionFunction, null);
  }

  public static RelOptTableImpl create(RelOptSchema schema, RelDataType rowType,
      final CalciteSchema.TableEntry tableEntry, Double rowCount) {
    final Table table = tableEntry.getTable();
    Function<Class, Expression> expressionFunction =
        getClassExpressionFunction(tableEntry, table);
    return new RelOptTableImpl(schema, rowType, tableEntry.path(),
        table, expressionFunction, rowCount);
  }

  private static Function<Class, Expression> getClassExpressionFunction(
      final CalciteSchema.TableEntry tableEntry, final Table table) {
    if (table instanceof QueryableTable) {
      final QueryableTable queryableTable = (QueryableTable) table;
      return new Function<Class, Expression>() {
        public Expression apply(Class clazz) {
          return queryableTable.getExpression(tableEntry.schema.plus(),
              tableEntry.name, clazz);
        }
      };
    } else if (table instanceof ScannableTable
        || table instanceof FilterableTable
        || table instanceof ProjectableFilterableTable) {
      return new Function<Class, Expression>() {
        public Expression apply(Class clazz) {
          return Schemas.tableExpression(tableEntry.schema.plus(),
              Object[].class,
              tableEntry.name,
              table.getClass());
        }
      };
    } else if (table instanceof StreamableTable) {
      return getClassExpressionFunction(tableEntry,
          ((StreamableTable) table).stream());
    } else {
      return new Function<Class, Expression>() {
        public Expression apply(Class input) {
          throw new UnsupportedOperationException();
        }
      };
    }
  }

  public static RelOptTableImpl create(
      RelOptSchema schema,
      RelDataType rowType,
      Table table) {
    assert table instanceof TranslatableTable
        || table instanceof ScannableTable;
    return new RelOptTableImpl(schema, rowType, ImmutableList.<String>of(),
        table, null, null);
  }

  public <T> T unwrap(Class<T> clazz) {
    if (clazz.isInstance(this)) {
      return clazz.cast(this);
    }
    if (clazz.isInstance(table)) {
      return clazz.cast(table);
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

  public RelOptTable extend(List<RelDataTypeField> extendedFields) {
    if (table instanceof ExtensibleTable) {
      final Table extendedTable =
          ((ExtensibleTable) table).extend(extendedFields);
      final RelDataType extendedRowType =
          extendedTable.getRowType(schema.getTypeFactory());
      return new RelOptTableImpl(schema, extendedRowType, names, extendedTable,
          expressionFunction, rowCount);
    }
    throw new RuntimeException("Cannot extend " + table); // TODO: user error
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
    if (table instanceof TranslatableTable) {
      return ((TranslatableTable) table).toRel(context, this);
    }
    final RelOptCluster cluster = context.getCluster();
    if (CalcitePrepareImpl.ENABLE_BINDABLE) {
      return LogicalTableScan.create(cluster, this);
    }
    if (CalcitePrepareImpl.ENABLE_ENUMERABLE
        && table instanceof QueryableTable) {
      return EnumerableTableScan.create(cluster, this);
    }
    if (table instanceof ScannableTable
        || table instanceof FilterableTable
        || table instanceof ProjectableFilterableTable) {
      return LogicalTableScan.create(cluster, this);
    }
    if (CalcitePrepareImpl.ENABLE_ENUMERABLE) {
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

  public List<String> getQualifiedName() {
    return names;
  }

  public SqlMonotonicity getMonotonicity(String columnName) {
    final int i = rowType.getFieldNames().indexOf(columnName);
    if (i >= 0) {
      for (RelCollation collation : table.getStatistic().getCollations()) {
        final RelFieldCollation fieldCollation =
            collation.getFieldCollations().get(0);
        if (fieldCollation.getFieldIndex() == i) {
          return monotonicity(fieldCollation.direction);
        }
      }
    }
    return SqlMonotonicity.NOT_MONOTONIC;
  }

  /** Converts a {@link org.apache.calcite.rel.RelFieldCollation.Direction}
   * value to a {@link org.apache.calcite.sql.validate.SqlMonotonicity}. */
  private static SqlMonotonicity
  monotonicity(RelFieldCollation.Direction direction) {
    switch (direction) {
    case ASCENDING:
      return SqlMonotonicity.INCREASING;
    case STRICTLY_ASCENDING:
      return SqlMonotonicity.STRICTLY_INCREASING;
    case DESCENDING:
      return SqlMonotonicity.DECREASING;
    case STRICTLY_DESCENDING:
      return SqlMonotonicity.STRICTLY_DECREASING;
    case CLUSTERED:
      return SqlMonotonicity.MONOTONIC;
    default:
      throw new AssertionError("unknown: " + direction);
    }
  }

  public SqlAccessType getAllowedAccess() {
    return SqlAccessType.ALL;
  }
}

// End RelOptTableImpl.java
