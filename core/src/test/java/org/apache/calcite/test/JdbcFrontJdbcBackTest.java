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
package org.apache.calcite.test;

import org.apache.calcite.avatica.MetaImpl.MetaTable;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteMetaImpl;
import org.apache.calcite.jdbc.CalciteMetaImpl.CalciteMetaTable;
import org.apache.calcite.jdbc.CalciteMetaTableFactory;
import org.apache.calcite.schema.Table;
import org.apache.calcite.util.TestUtil;

import com.google.common.collect.ImmutableList;

import org.hamcrest.Matcher;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static org.apache.calcite.test.CalciteAssert.that;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasToString;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for a JDBC front-end and JDBC back-end.
 *
 * <p>The idea is that as much as possible of the query is pushed down
 * to the JDBC data source, in the form of a large (and hopefully efficient)
 * SQL statement.
 *
 * @see JdbcFrontJdbcBackLinqMiddleTest
 */
class JdbcFrontJdbcBackTest {
  @Test void testWhere2() {
    that()
        .with(CalciteAssert.Config.JDBC_FOODMART)
        .query("select * from \"foodmart\".\"days\" where \"day\" < 3")
        .returns("day=1; week_day=Sunday\n"
            + "day=2; week_day=Monday\n");
  }

  @Disabled
  @Test void testTables() throws Exception {
    that()
        .with(CalciteAssert.Config.JDBC_FOODMART)
        .doWithConnection(connection -> {
          try {
            ResultSet rset =
                connection.getMetaData().getTables(
                    null, null, null, null);
            StringBuilder buf = new StringBuilder();
            while (rset.next()) {
              buf.append(rset.getString(3)).append(';');
            }
            assertThat(buf,
                hasToString("account;agg_c_10_sales_fact_1997;"
                    + "agg_c_14_sales_fact_1997;agg_c_special_sales_fact_1997;"
                    + "agg_g_ms_pcat_sales_fact_1997;agg_l_03_sales_fact_1997;"
                    + "agg_l_04_sales_fact_1997;agg_l_05_sales_fact_1997;"
                    + "agg_lc_06_sales_fact_1997;agg_lc_100_sales_fact_1997;"
                    + "agg_ll_01_sales_fact_1997;agg_pl_01_sales_fact_1997;"
                    + "category;currency;customer;days;department;employee;"
                    + "employee_closure;expense_fact;inventory_fact_1997;"
                    + "inventory_fact_1998;position;product;product_class;"
                    + "products;promotion;region;reserve_employee;salary;"
                    + "sales_fact_1997;sales_fact_1998;sales_fact_dec_1998;"
                    + "store;store_ragged;time_by_day;warehouse;"
                    + "warehouse_class;COLUMNS;TABLES;"));
          } catch (SQLException e) {
            throw TestUtil.rethrow(e);
          }
        });
  }

  @Test void testTablesExtraColumn() throws Exception {
    that()
        .with(CalciteAssert.Config.JDBC_FOODMART)
        .with(
            CalciteConnectionProperty.META_TABLE_FACTORY.camelName(),
            MetaExtraTableFactoryImpl.class.getName())
        .doWithConnection(connection -> {
          try {
            ResultSet rset =
                connection.getMetaData().getTables(
                    null, null, null, null);
            assertTrue(rset.next());
            // Asserts that the number of columns in the result set equals
            // MetaExtraTableFactoryImpl's expected number of columns.
            assertThat(rset.getMetaData().getColumnCount(), is(11));
            assertThat("EXTRA_LABEL", is(rset.getMetaData().getColumnName(11)));
          } catch (SQLException e) {
            throw TestUtil.rethrow(e);
          }
        });
  }

  @Test void testTablesByType() throws Exception {
    // check with the form recommended by JDBC
    checkTablesByType("SYSTEM TABLE", is("COLUMNS;TABLES;"));
    // the form we used until 1.14 no longer generates results
    checkTablesByType("SYSTEM_TABLE", is(""));
  }

  private void checkTablesByType(final String tableType,
      final Matcher<String> matcher) throws Exception {
    that()
        .with(CalciteAssert.Config.REGULAR_PLUS_METADATA)
        .doWithConnection(connection -> {
          try (ResultSet rset =
                   connection.getMetaData().getTables(null, null, null,
                       new String[] {tableType})) {
            StringBuilder buf = new StringBuilder();
            while (rset.next()) {
              buf.append(rset.getString(3)).append(';');
            }
            assertThat(buf, hasToString(matcher));
          } catch (SQLException e) {
            throw TestUtil.rethrow(e);
          }
        });
  }

  @Test void testColumns() throws Exception {
    that()
        .with(CalciteAssert.Config.JDBC_FOODMART)
        .doWithConnection(connection -> {
          try {
            ResultSet resultSet =
                connection.getMetaData().getColumns(
                    null, null, "sales_fact_1997", null);
            StringBuilder buf = new StringBuilder();
            while (resultSet.next()) {
              buf.append(resultSet.getString(4)).append(';');
            }
            assertThat(buf,
                hasToString("product_id;time_id;customer_id;promotion_id;"
                    + "store_id;store_sales;store_cost;unit_sales;"));
          } catch (SQLException e) {
            throw TestUtil.rethrow(e);
          }
        });
  }

  /** Tests a JDBC method known to be not implemented (as it happens,
   * {@link java.sql.DatabaseMetaData#getPrimaryKeys}) that therefore uses
   * empty result set. */
  @Test void testEmpty() throws Exception {
    that()
        .with(CalciteAssert.Config.JDBC_FOODMART)
        .doWithConnection(connection -> {
          try {
            ResultSet rset =
                connection.getMetaData().getPrimaryKeys(
                    null, null, "sales_fact_1997");
            assertFalse(rset.next());
          } catch (SQLException e) {
            throw TestUtil.rethrow(e);
          }
        });
  }

  @Test void testCase() {
    that()
        .with(CalciteAssert.Config.JDBC_FOODMART)
        .query("select\n"
            + "  case when \"sales_fact_1997\".\"promotion_id\" = 1 then 0\n"
            + "  else \"sales_fact_1997\".\"store_sales\" end as \"c0\"\n"
            + "from \"sales_fact_1997\" as \"sales_fact_1997\""
            + "where \"product_id\" = 1\n"
            + "and \"time_id\" < 400")
        .returns2("c0=11.4\n"
            + "c0=8.55\n");
  }

  /** Mock implementation of {@link CalciteMetaTable}. */
  private static class MetaExtraTable extends CalciteMetaTable {
    final String extraLabel;

    MetaExtraTable(Table calciteTable, String tableCat,
        String tableSchem, String tableName) {
      super(calciteTable, tableCat, tableSchem, tableName);
      this.extraLabel = "extraLabel1";
    }
  }

  /** Mock implementation of {@link CalciteMetaTableFactory} that creates
   * instances of {@link MetaExtraTable}. Must be public, otherwise it is
   * inaccessible from {@link org.apache.calcite.jdbc.Driver}. */
  public static class MetaExtraTableFactoryImpl
      implements CalciteMetaTableFactory {
    public static final MetaExtraTableFactoryImpl INSTANCE =
        new MetaExtraTableFactoryImpl();

    MetaExtraTableFactoryImpl() {}

    @Override public MetaTable createTable(Table table, String tableCat,
        String tableSchem, String tableName) {
      return new MetaExtraTable(table, tableCat, tableSchem, tableName);
    }

    @Override public List<String> getColumnNames() {
      // 11 columns total.
      return ImmutableList.<String>builder()
          .addAll(CalciteMetaImpl.TABLE_COLUMNS)
          .add("EXTRA_LABEL")
          .build();
    }

    @Override public Class<? extends MetaTable> getMetaTableClass() {
      return MetaExtraTable.class;
    }
  }
}
