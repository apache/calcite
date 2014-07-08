/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package org.apache.optiq.prepare;

import org.apache.linq4j.expressions.Expression;

import org.apache.optiq.QueryableTable;
import org.apache.optiq.Table;
import org.apache.optiq.TranslatableTable;
import org.apache.optiq.jdbc.OptiqSchema;
import org.apache.optiq.impl.enumerable.EnumerableConvention;
import org.apache.optiq.impl.enumerable.JavaRules;

import org.apache.optiq.rel.RelCollation;
import org.apache.optiq.rel.RelNode;
import org.apache.optiq.relopt.RelOptCluster;
import org.apache.optiq.relopt.RelOptSchema;
import org.apache.optiq.reltype.RelDataType;
import org.apache.optiq.sql.SqlAccessType;
import org.apache.optiq.sql.validate.SqlMonotonicity;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;

import java.lang.reflect.Type;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;

/**
 * Implementation of {@link org.apache.optiq.relopt.RelOptTable}.
 */
public class RelOptTableImpl implements Prepare.PreparingTable {
  private final RelOptSchema schema;
  private final RelDataType rowType;
  private final Table table;
  private final Function<Class, Expression> expressionFunction;
  private final ImmutableList<String> names;

  private RelOptTableImpl(
      RelOptSchema schema,
      RelDataType rowType,
      List<String> names,
      Table table,
      Function<Class, Expression> expressionFunction) {
    this.schema = schema;
    this.rowType = rowType;
    this.names = ImmutableList.copyOf(names);
    this.table = table; // may be null
    this.expressionFunction = expressionFunction;
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
        expressionFunction);
  }

  public static RelOptTableImpl create(
      RelOptSchema schema,
      RelDataType rowType,
      final OptiqSchema.TableEntry tableEntry) {
    Function<Class, Expression> expressionFunction;
    if (tableEntry.getTable() instanceof QueryableTable) {
      final QueryableTable table = (QueryableTable) tableEntry.getTable();
      expressionFunction = new Function<Class, Expression>() {
        public Expression apply(Class clazz) {
          return table.getExpression(tableEntry.schema.plus(),
              tableEntry.name, clazz);
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
      tableEntry.getTable(), expressionFunction);
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
        table, expressionFunction);
  }

  public <T> T unwrap(Class<T> clazz) {
    if (clazz.isInstance(this)) {
      return clazz.cast(this);
    }
    if (clazz.isInstance(table)) {
      return clazz.cast(table);
    }
    return null;
  }

  public Expression getExpression(Class clazz) {
    return expressionFunction.apply(clazz);
  }

  public double getRowCount() {
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
    return new JavaRules.EnumerableTableAccessRel(
        cluster, cluster.traitSetOf(EnumerableConvention.INSTANCE),
        this, elementType);
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
