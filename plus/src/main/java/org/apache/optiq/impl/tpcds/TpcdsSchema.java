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
package org.apache.optiq.impl.tpcds;

import org.apache.linq4j.Enumerator;
import org.apache.linq4j.Linq4j;
import org.apache.linq4j.QueryProvider;
import org.apache.linq4j.Queryable;

import org.apache.optiq.SchemaPlus;
import org.apache.optiq.Table;
import org.apache.optiq.impl.AbstractSchema;
import org.apache.optiq.impl.AbstractTableQueryable;
import org.apache.optiq.impl.enumerable.AbstractQueryableTable;

import org.apache.optiq.reltype.RelDataType;
import org.apache.optiq.reltype.RelDataTypeFactory;

import com.google.common.collect.ImmutableMap;

import java.sql.Date;
import java.util.List;
import java.util.Map;

import net.hydromatic.tpcds.TpcdsColumn;
import net.hydromatic.tpcds.TpcdsEntity;
import net.hydromatic.tpcds.TpcdsTable;

/** Schema that provides TPC-H tables, populated according to a
 * particular scale factor. */
public class TpcdsSchema extends AbstractSchema {
  private final double scaleFactor;
  private final int part;
  private final int partCount;
  private final ImmutableMap<String, Table> tableMap;

  public TpcdsSchema(double scaleFactor, int part, int partCount) {
    this.scaleFactor = scaleFactor;
    this.part = part;
    this.partCount = partCount;

    final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
    for (TpcdsTable<?> tpcdsTable : TpcdsTable.getTables()) {
      builder.put(tpcdsTable.getTableName().toUpperCase(),
          new TpcdsQueryableTable(tpcdsTable));
    }
    this.tableMap = builder.build();
  }

  @Override
  protected Map<String, Table> getTableMap() {
    return tableMap;
  }

  /** Definition of a table in the TPC-DS schema. */
  private class TpcdsQueryableTable<E extends TpcdsEntity>
      extends AbstractQueryableTable {
    private final TpcdsTable<E> tpcdsTable;

    TpcdsQueryableTable(TpcdsTable<E> tpcdsTable) {
      super(Object[].class);
      this.tpcdsTable = tpcdsTable;
    }

    public <T> Queryable<T> asQueryable(final QueryProvider queryProvider,
        final SchemaPlus schema, final String tableName) {
      //noinspection unchecked
      return (Queryable) new AbstractTableQueryable<Object[]>(queryProvider,
          schema, this, tableName) {
        public Enumerator<Object[]> enumerator() {
          final Enumerator<E> iterator =
              Linq4j.iterableEnumerator(
                  tpcdsTable.createGenerator(scaleFactor, part, partCount));
          return new Enumerator<Object[]>() {
            public Object[] current() {
              final List<TpcdsColumn<E>> columns = tpcdsTable.getColumns();
              final Object[] objects = new Object[columns.size()];
              int i = 0;
              for (TpcdsColumn<E> column : columns) {
                objects[i++] = value(column, iterator.current());
              }
              return objects;
            }

            private Object value(TpcdsColumn<E> tpcdsColumn, E current) {
              final Class<?> type = realType(tpcdsColumn);
              if (type == String.class) {
                return tpcdsColumn.getString(current);
              } else if (type == Double.class) {
                return tpcdsColumn.getDouble(current);
              } else if (type == Date.class) {
                return Date.valueOf(tpcdsColumn.getString(current));
              } else {
                return tpcdsColumn.getLong(current);
              }
            }

            public boolean moveNext() {
              return iterator.moveNext();
            }

            public void reset() {
              iterator.reset();
            }

            public void close() {
            }
          };
        }
      };
    }

    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      final RelDataTypeFactory.FieldInfoBuilder builder = typeFactory.builder();
      for (TpcdsColumn<E> column : tpcdsTable.getColumns()) {
        builder.add(column.getColumnName().toUpperCase(),
            typeFactory.createJavaType(realType(column)));
      }
      return builder.build();
    }

    private Class<?> realType(TpcdsColumn<E> column) {
      if (column.getColumnName().endsWith("date")) {
        return Date.class;
      }
      return column.getType();
    }
  }
}

// End TpcdsSchema.java

