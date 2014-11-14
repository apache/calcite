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
package net.hydromatic.optiq.impl.tpch;

import net.hydromatic.linq4j.Enumerator;
import net.hydromatic.linq4j.Linq4j;
import net.hydromatic.linq4j.QueryProvider;
import net.hydromatic.linq4j.Queryable;

import net.hydromatic.optiq.SchemaPlus;
import net.hydromatic.optiq.Table;
import net.hydromatic.optiq.impl.AbstractSchema;
import net.hydromatic.optiq.impl.AbstractTableQueryable;
import net.hydromatic.optiq.impl.java.AbstractQueryableTable;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;

import com.google.common.collect.ImmutableMap;

import java.sql.Date;
import java.util.List;
import java.util.Map;

import io.airlift.tpch.TpchColumn;
import io.airlift.tpch.TpchEntity;
import io.airlift.tpch.TpchTable;

/** Schema that provides TPC-H tables, populated according to a
 * particular scale factor. */
public class TpchSchema extends AbstractSchema {
  private final double scaleFactor;
  private final int part;
  private final int partCount;
  private final boolean columnPrefix;
  private final ImmutableMap<String, Table> tableMap;
  private final ImmutableMap<String, String> columnPrefixes;

  public TpchSchema(double scaleFactor, int part, int partCount,
      boolean columnPrefix) {
    this.scaleFactor = scaleFactor;
    this.part = part;
    this.partCount = partCount;
    this.columnPrefix = columnPrefix;

    final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
    for (TpchTable<?> tpchTable : TpchTable.getTables()) {
      builder.put(tpchTable.getTableName().toUpperCase(),
          new TpchQueryableTable(tpchTable));
    }
    this.tableMap = builder.build();

    this.columnPrefixes = ImmutableMap.<String, String>builder()
        .put("LINEITEM", "L_")
        .put("CUSTOMER", "C_")
        .put("SUPPLIER", "S_")
        .put("PARTSUPP", "PS_")
        .put("PART", "P_")
        .put("ORDERS", "O_")
        .put("NATION", "N_")
        .put("REGION", "R_")
        .build();
  }

  @Override
  protected Map<String, Table> getTableMap() {
    return tableMap;
  }

  /** Definition of a table in the TPC-H schema. */
  private class TpchQueryableTable<E extends TpchEntity>
      extends AbstractQueryableTable {
    private final TpchTable<E> tpchTable;

    TpchQueryableTable(TpchTable<E> tpchTable) {
      super(Object[].class);
      this.tpchTable = tpchTable;
    }

    public <T> Queryable<T> asQueryable(final QueryProvider queryProvider,
        final SchemaPlus schema, final String tableName) {
      //noinspection unchecked
      return (Queryable) new AbstractTableQueryable<Object[]>(queryProvider,
          schema, this, tableName) {
        public Enumerator<Object[]> enumerator() {
          final Enumerator<E> iterator =
              Linq4j.iterableEnumerator(
                  tpchTable.createGenerator(scaleFactor, part, partCount));
          return new Enumerator<Object[]>() {
            public Object[] current() {
              final List<TpchColumn<E>> columns = tpchTable.getColumns();
              final Object[] objects = new Object[columns.size()];
              int i = 0;
              for (TpchColumn<E> column : columns) {
                objects[i++] = value(column, iterator.current());
              }
              return objects;
            }

            private Object value(TpchColumn<E> tpchColumn, E current) {
              final Class<?> type = realType(tpchColumn);
              if (type == String.class) {
                return tpchColumn.getString(current);
              } else if (type == Double.class) {
                return tpchColumn.getDouble(current);
              } else if (type == Date.class) {
                return Date.valueOf(tpchColumn.getString(current));
              } else {
                return tpchColumn.getLong(current);
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
      String prefix = "";
      if (columnPrefix) {
        prefix = columnPrefixes.get(tpchTable.getTableName().toUpperCase());
        assert prefix != null : tpchTable.getTableName().toUpperCase();
      }
      for (TpchColumn<E> column : tpchTable.getColumns()) {
        builder.add((prefix + column.getColumnName()).toUpperCase(),
            typeFactory.createJavaType(realType(column)));
      }
      return builder.build();
    }

    private Class<?> realType(TpchColumn<E> column) {
      if (column.getColumnName().endsWith("date")) {
        return java.sql.Date.class;
      }
      return column.getType();
    }
  }
}

// End TpchSchema.java

