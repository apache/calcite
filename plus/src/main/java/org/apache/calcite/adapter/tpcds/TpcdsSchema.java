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
package org.apache.calcite.adapter.tpcds;

import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.calcite.util.Bug;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableMap;

import net.hydromatic.tpcds.TpcdsColumn;
import net.hydromatic.tpcds.TpcdsEntity;
import net.hydromatic.tpcds.TpcdsTable;

import java.sql.Date;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/** Schema that provides TPC-DS tables, populated according to a
 * particular scale factor. */
public class TpcdsSchema extends AbstractSchema {
  private final double scaleFactor;
  private final int part;
  private final int partCount;
  private final ImmutableMap<String, Table> tableMap;

  // From TPC-DS spec, table 3-2 "Database Row Counts", for 1G sizing.
  private static final ImmutableMap<String, Integer> TABLE_ROW_COUNTS =
      ImmutableMap.<String, Integer>builder()
          .put("call_center", 8)
          .put("catalog_page", 11718)
          .put("catalog_returns", 144067)
          .put("catalog_sales", 1441548)
          .put("customer", 100000)
          .put("customer_address", 50000)
          .put("customer_demographics", 1920800)
          .put("date_dim", 73049)
          .put("household_demographics", 7200)
          .put("income_band", 20)
          .put("inventory", 11745000)
          .put("item", 18000)
          .put("promotion", 300)
          .put("reason", 35)
          .put("ship_mode", 20)
          .put("store", 12)
          .put("store_returns", 287514)
          .put("store_sales", 2880404)
          .put("time_dim", 86400)
          .put("warehouse", 5)
          .put("web_page", 60)
          .put("web_returns", 71763)
          .put("web_sales", 719384)
          .put("web_site", 1)
          .build();

  public TpcdsSchema(double scaleFactor, int part, int partCount) {
    this.scaleFactor = scaleFactor;
    this.part = part;
    this.partCount = partCount;

    final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
    for (TpcdsTable<?> tpcdsTable : TpcdsTable.getTables()) {
      //noinspection unchecked
      builder.put(tpcdsTable.getTableName().toUpperCase(Locale.ROOT),
          new TpcdsQueryableTable(tpcdsTable));
    }
    this.tableMap = builder.build();
  }

  @Override protected Map<String, Table> getTableMap() {
    return tableMap;
  }

  /** Definition of a table in the TPC-DS schema.
   *
   * @param <E> entity type */
  private class TpcdsQueryableTable<E extends TpcdsEntity>
      extends AbstractQueryableTable {
    private final TpcdsTable<E> tpcdsTable;

    TpcdsQueryableTable(TpcdsTable<E> tpcdsTable) {
      super(Object[].class);
      this.tpcdsTable = tpcdsTable;
    }

    @Override public Statistic getStatistic() {
      Bug.upgrade("add row count estimate to TpcdsTable, and use it");
      Integer rowCount = TABLE_ROW_COUNTS.get(tpcdsTable.name);
      assert rowCount != null : tpcdsTable.name;
      return Statistics.of(rowCount, Collections.<ImmutableBitSet>emptyList());
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
      final RelDataTypeFactory.Builder builder = typeFactory.builder();
      for (TpcdsColumn<E> column : tpcdsTable.getColumns()) {
        builder.add(column.getColumnName().toUpperCase(Locale.ROOT),
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
