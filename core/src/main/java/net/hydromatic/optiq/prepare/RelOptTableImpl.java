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
package net.hydromatic.optiq.prepare;

import net.hydromatic.linq4j.expressions.Expression;

import net.hydromatic.optiq.*;
import net.hydromatic.optiq.jdbc.OptiqSchema;
import net.hydromatic.optiq.rules.java.EnumerableConvention;
import net.hydromatic.optiq.rules.java.JavaRules;

import org.eigenbase.rel.RelCollation;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptSchema;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.sql.SqlAccessType;
import org.eigenbase.sql.validate.SqlMonotonicity;
import org.eigenbase.util.Util;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;

import java.lang.reflect.Type;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;

/**
 * Implementation of {@link org.eigenbase.relopt.RelOptTable}.
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
    this.rowType = rowType;
    this.names = ImmutableList.copyOf(names);
    this.table = table; // may be null
    this.expressionFunction = expressionFunction;
    this.rowCount = rowCount;
    assert expressionFunction != null;
    assert rowType != null;
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
      final OptiqSchema.TableEntry tableEntry, Double rowCount) {
    Function<Class, Expression> expressionFunction;
    final Table table = tableEntry.getTable();
    if (table instanceof QueryableTable) {
      final QueryableTable queryableTable = (QueryableTable) table;
      expressionFunction = new Function<Class, Expression>() {
        public Expression apply(Class clazz) {
          return queryableTable.getExpression(tableEntry.schema.plus(),
              tableEntry.name, clazz);
        }
      };
    } else if (table instanceof ScannableTable
        || table instanceof FilterableTable
        || table instanceof ProjectableFilterableTable) {
      expressionFunction = new Function<Class, Expression>() {
        public Expression apply(Class clazz) {
          return Schemas.tableExpression(tableEntry.schema.plus(),
              Object[].class, tableEntry.name,
              table.getClass());
        }
      };
    } else {
      expressionFunction = new Function<Class, Expression>() {
        public Expression apply(Class input) {
          throw new UnsupportedOperationException();
        }
      };
    }
    return new RelOptTableImpl(schema, rowType, tableEntry.path(),
        table, expressionFunction, rowCount);
  }

  public static RelOptTableImpl create(
      RelOptSchema schema,
      RelDataType rowType,
      TranslatableTable table) {
    final Function<Class, Expression> expressionFunction =
        new Function<Class, Expression>() {
          public Expression apply(Class input) {
            throw new UnsupportedOperationException();
          }
        };
    return new RelOptTableImpl(schema, rowType, ImmutableList.<String>of(),
        table, expressionFunction, null);
  }

  public <T> T unwrap(Class<T> clazz) {
    if (clazz.isInstance(this)) {
      return clazz.cast(this);
    }
    if (clazz.isInstance(table)) {
      return clazz.cast(table);
    }
    if (clazz == OptiqSchema.class) {
      return clazz.cast(
          Schemas.subSchema(((OptiqCatalogReader) schema).rootSchema,
              Util.skipLast(getQualifiedName())));
    }
    return null;
  }

  public Expression getExpression(Class clazz) {
    return expressionFunction.apply(clazz);
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
    RelOptCluster cluster = context.getCluster();
    Class elementType = deduceElementType();
    final RelNode scan = new JavaRules.EnumerableTableAccessRel(cluster,
        cluster.traitSetOf(EnumerableConvention.INSTANCE), this, elementType);
    if (table instanceof FilterableTable
        || table instanceof ProjectableFilterableTable) {
      return new JavaRules.EnumerableInterpreterRel(cluster, scan.getTraitSet(),
          scan, 1d);
    }
    return scan;
  }

  private Class deduceElementType() {
    if (table instanceof QueryableTable) {
      final QueryableTable queryableTable = (QueryableTable) table;
      final Type type = queryableTable.getElementType();
      if (type instanceof Class) {
        return (Class) type;
      } else {
        return Object[].class;
      }
    } else if (table instanceof ScannableTable
        || table instanceof FilterableTable
        || table instanceof ProjectableFilterableTable) {
      return Object[].class;
    } else {
      return Object.class;
    }
  }

  public List<RelCollation> getCollationList() {
    return Collections.emptyList();
  }

  public boolean isKey(BitSet columns) {
    return table.getStatistic().isKey(columns);
  }

  public RelDataType getRowType() {
    return rowType;
  }

  public List<String> getQualifiedName() {
    return names;
  }

  public SqlMonotonicity getMonotonicity(String columnName) {
    return SqlMonotonicity.NOT_MONOTONIC;
  }

  public SqlAccessType getAllowedAccess() {
    return SqlAccessType.ALL;
  }
}

// End RelOptTableImpl.java
