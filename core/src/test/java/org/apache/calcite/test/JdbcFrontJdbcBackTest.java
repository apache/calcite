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

import org.apache.calcite.util.TestUtil;

import org.hamcrest.Matcher;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;

import static org.apache.calcite.test.CalciteAssert.that;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

/**
 * Tests for a JDBC front-end and JDBC back-end.
 *
 * <p>The idea is that as much as possible of the query is pushed down
 * to the JDBC data source, in the form of a large (and hopefully efficient)
 * SQL statement.</p>
 *
 * @see JdbcFrontJdbcBackLinqMiddleTest
 */
public class JdbcFrontJdbcBackTest {
  @Test public void testWhere2() {
    that()
        .with(CalciteAssert.Config.JDBC_FOODMART)
        .query("select * from \"foodmart\".\"days\" where \"day\" < 3")
        .returns("day=1; week_day=Sunday\n"
            + "day=2; week_day=Monday\n");
  }

  @Ignore
  @Test public void testTables() throws Exception {
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
            assertEquals(
                "account;agg_c_10_sales_fact_1997;agg_c_14_sales_fact_1997;agg_c_special_sales_fact_1997;agg_g_ms_pcat_sales_fact_1997;agg_l_03_sales_fact_1997;agg_l_04_sales_fact_1997;agg_l_05_sales_fact_1997;agg_lc_06_sales_fact_1997;agg_lc_100_sales_fact_1997;agg_ll_01_sales_fact_1997;agg_pl_01_sales_fact_1997;category;currency;customer;days;department;employee;employee_closure;expense_fact;inventory_fact_1997;inventory_fact_1998;position;product;product_class;products;promotion;region;reserve_employee;salary;sales_fact_1997;sales_fact_1998;sales_fact_dec_1998;store;store_ragged;time_by_day;warehouse;warehouse_class;COLUMNS;TABLES;",
                buf.toString());
          } catch (SQLException e) {
            throw TestUtil.rethrow(e);
          }
        });
  }

  @Test public void testTablesByType() throws Exception {
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
          try (ResultSet rset = connection.getMetaData().getTables(null, null,
              null, new String[] {tableType})) {
            StringBuilder buf = new StringBuilder();
            while (rset.next()) {
              buf.append(rset.getString(3)).append(';');
            }
            assertThat(buf.toString(), matcher);
          } catch (SQLException e) {
            throw TestUtil.rethrow(e);
          }
        });
  }

  @Test public void testColumns() throws Exception {
    that()
        .with(CalciteAssert.Config.JDBC_FOODMART)
        .doWithConnection(connection -> {
          try {
            ResultSet rset =
                connection.getMetaData().getColumns(
                    null, null, "sales_fact_1997", null);
            StringBuilder buf = new StringBuilder();
            while (rset.next()) {
              buf.append(rset.getString(4)).append(';');
            }
            assertEquals(
                "product_id;time_id;customer_id;promotion_id;store_id;store_sales;store_cost;unit_sales;",
                buf.toString());
          } catch (SQLException e) {
            throw TestUtil.rethrow(e);
          }
        });
  }

  /** Tests a JDBC method known to be not implemented (as it happens,
   * {@link java.sql.DatabaseMetaData#getPrimaryKeys}) that therefore uses
   * empty result set. */
  @Test public void testEmpty() throws Exception {
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

  @Test public void testCase() {
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
}

// End JdbcFrontJdbcBackTest.java
