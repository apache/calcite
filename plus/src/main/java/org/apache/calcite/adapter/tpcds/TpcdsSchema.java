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
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Bug;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.teradata.tpcds.Results;
import com.teradata.tpcds.Session;
import com.teradata.tpcds.column.Column;
import com.teradata.tpcds.column.ColumnType;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/** Schema that provides TPC-DS tables, populated according to a
 * particular scale factor. */
public class TpcdsSchema extends AbstractSchema {
  private final double scaleFactor;
  private final ImmutableMap<String, Table> tableMap;

  // From TPC-DS spec, table 3-2 "Database Row Counts", for 1G sizing.
  private static final ImmutableMap<String, Integer> TABLE_ROW_COUNTS =
      ImmutableMap.<String, Integer>builder()
          .put("CALL_CENTER", 8)
          .put("CATALOG_PAGE", 11718)
          .put("CATALOG_RETURNS", 144067)
          .put("CATALOG_SALES", 1441548)
          .put("CUSTOMER", 100000)
          .put("CUSTOMER_ADDRESS", 50000)
          .put("CUSTOMER_DEMOGRAPHICS", 1920800)
          .put("DATE_DIM", 73049)
          .put("DBGEN_VERSION", 1)
          .put("HOUSEHOLD_DEMOGRAPHICS", 7200)
          .put("INCOME_BAND", 20)
          .put("INVENTORY", 11745000)
          .put("ITEM", 18000)
          .put("PROMOTION", 300)
          .put("REASON", 35)
          .put("SHIP_MODE", 20)
          .put("STORE", 12)
          .put("STORE_RETURNS", 287514)
          .put("STORE_SALES", 2880404)
          .put("TIME_DIM", 86400)
          .put("WAREHOUSE", 5)
          .put("WEB_PAGE", 60)
          .put("WEB_RETURNS", 71763)
          .put("WEB_SALES", 719384)
          .put("WEB_SITE", 1)
          .build();

  @Deprecated
  public TpcdsSchema(double scaleFactor, int part, int partCount) {
    this(scaleFactor);
    Util.discard(part);
    Util.discard(partCount);
  }

  /** Creates a TpcdsSchema. */
  public TpcdsSchema(double scaleFactor) {
    this.scaleFactor = scaleFactor;

    final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
    for (com.teradata.tpcds.Table tpcdsTable
        : com.teradata.tpcds.Table.getBaseTables()) {
      builder.put(tpcdsTable.name().toUpperCase(Locale.ROOT),
          new TpcdsQueryableTable(tpcdsTable));
    }
    this.tableMap = builder.build();
  }

  @Override protected Map<String, Table> getTableMap() {
    return tableMap;
  }

  private static Object convert(String string, Column column) {
    if (string == null) {
      return null;
    }
    switch (column.getType().getBase()) {
    case IDENTIFIER:
      return Long.valueOf(string);
    case INTEGER:
      return Integer.valueOf(string);
    case CHAR:
    case VARCHAR:
      return string;
    case DATE:
      return DateTimeUtils.dateStringToUnixDate(string);
    case TIME:
      return DateTimeUtils.timeStringToUnixDate(string);
    case DECIMAL:
      return new BigDecimal(string);
    default:
      throw new AssertionError(column);
    }
  }

  /** Definition of a table in the TPC-DS schema.
   *
   * @param <E> entity type */
  private class TpcdsQueryableTable<E extends com.teradata.tpcds.Table>
      extends AbstractQueryableTable {
    private final com.teradata.tpcds.Table tpcdsTable;

    TpcdsQueryableTable(com.teradata.tpcds.Table tpcdsTable) {
      super(Object[].class);
      this.tpcdsTable = tpcdsTable;
    }

    @Override public Statistic getStatistic() {
      Bug.upgrade("add row count estimate to TpcdsTable, and use it");
      Integer rowCount = TABLE_ROW_COUNTS.get(tpcdsTable.name());
      assert rowCount != null : tpcdsTable;
      return Statistics.of(rowCount, ImmutableList.of());
    }

    public <T> Queryable<T> asQueryable(final QueryProvider queryProvider,
        final SchemaPlus schema, final String tableName) {
      //noinspection unchecked
      return (Queryable) new AbstractTableQueryable<Object[]>(queryProvider,
          schema, this, tableName) {
        public Enumerator<Object[]> enumerator() {
          final Session session =
              Session.getDefaultSession()
                  .withTable(tpcdsTable)
                  .withScale(scaleFactor);
          final Results results = Results.constructResults(tpcdsTable, session);
          return Linq4j.asEnumerable(results)
              .selectMany(
                  new Function1<List<List<String>>, Enumerable<Object[]>>() {
                    final Column[] columns = tpcdsTable.getColumns();

                    public Enumerable<Object[]> apply(
                        List<List<String>> inRows) {
                      final List<Object[]> rows = new ArrayList<>();
                      for (List<String> strings : inRows) {
                        final Object[] values = new Object[columns.length];
                        for (int i = 0; i < strings.size(); i++) {
                          values[i] = convert(strings.get(i), columns[i]);
                        }
                        rows.add(values);
                      }
                      return Linq4j.asEnumerable(rows);
                    }

                  })
              .enumerator();
        }
      };
    }

    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      final RelDataTypeFactory.Builder builder = typeFactory.builder();
      for (Column column : tpcdsTable.getColumns()) {
        builder.add(column.getName().toUpperCase(Locale.ROOT),
            type(typeFactory, column));
      }
      return builder.build();
    }

    private RelDataType type(RelDataTypeFactory typeFactory, Column column) {
      final ColumnType type = column.getType();
      switch (type.getBase()) {
      case DATE:
        return typeFactory.createSqlType(SqlTypeName.DATE);
      case TIME:
        return typeFactory.createSqlType(SqlTypeName.TIME);
      case INTEGER:
        return typeFactory.createSqlType(SqlTypeName.INTEGER);
      case IDENTIFIER:
        return typeFactory.createSqlType(SqlTypeName.BIGINT);
      case DECIMAL:
        return typeFactory.createSqlType(SqlTypeName.DECIMAL,
            type.getPrecision().get(), type.getScale().get());
      case VARCHAR:
        return typeFactory.createSqlType(SqlTypeName.VARCHAR,
            type.getPrecision().get());
      case CHAR:
        return typeFactory.createSqlType(SqlTypeName.CHAR,
            type.getPrecision().get());
      default:
        throw new AssertionError(type.getBase() + ": " + column);
      }
    }
  }
}

// End TpcdsSchema.java
