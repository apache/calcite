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

import org.apache.calcite.schema.TableFactory;
import org.apache.calcite.test.schemata.orderstream.InfiniteOrdersStreamTableFactory;
import org.apache.calcite.test.schemata.orderstream.OrdersStreamTableFactory;
import org.apache.calcite.test.schemata.orderstream.ProductsTableFactory;
import org.apache.calcite.util.TestUtil;

import com.google.common.collect.ImmutableList;

import org.hamcrest.comparator.ComparatorMatcherBuilder;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests for streaming queries.
 */
public class StreamTest {
  public static final String STREAM_SCHEMA_NAME = "STREAMS";
  public static final String INFINITE_STREAM_SCHEMA_NAME = "INFINITE_STREAMS";
  public static final String STREAM_JOINS_SCHEMA_NAME = "STREAM_JOINS";

  private static String schemaFor(String name, Class<? extends TableFactory> clazz) {
    return "     {\n"
      + "       name: '" + name + "',\n"
      + "       tables: [ {\n"
      + "         type: 'custom',\n"
      + "         name: 'ORDERS',\n"
      + "         stream: {\n"
      + "           stream: true\n"
      + "         },\n"
      + "         factory: '" + clazz.getName() + "'\n"
      + "       } ]\n"
      + "     }";
  }

  private static final String STREAM_JOINS_MODEL = "{\n"
      + "  version: '1.0',\n"
      + "  defaultSchema: 'STREAM_JOINS',\n"
      + "   schemas: [\n"
      + "     {\n"
      + "       name: 'STREAM_JOINS',\n"
      + "       tables: [ {\n"
      + "         type: 'custom',\n"
      + "         name: 'ORDERS',\n"
      + "         stream: {\n"
      + "           stream: true\n"
      + "         },\n"
      + "         factory: '" + OrdersStreamTableFactory.class.getName() + "'\n"
      + "       },\n"
      + "       {\n"
      + "         type: 'custom',\n"
      + "         name: 'PRODUCTS',\n"
      + "         factory: '" + ProductsTableFactory.class.getName() + "'\n"
      + "       }]\n"
      + "     }]}";

  public static final String STREAM_MODEL = "{\n"
      + "  version: '1.0',\n"
      + "  defaultSchema: 'foodmart',\n"
      + "   schemas: [\n"
      + schemaFor(STREAM_SCHEMA_NAME, OrdersStreamTableFactory.class)
      + ",\n"
      + schemaFor(INFINITE_STREAM_SCHEMA_NAME, InfiniteOrdersStreamTableFactory.class)
      + "\n"
      + "   ]\n"
      + "}";

  @Test void testStream() {
    CalciteAssert.model(STREAM_MODEL)
        .withDefaultSchema("STREAMS")
        .query("select stream * from orders")
        .convertContains("LogicalDelta\n"
            + "  LogicalProject(ROWTIME=[$0], ID=[$1], PRODUCT=[$2], UNITS=[$3])\n"
            + "    LogicalTableScan(table=[[STREAMS, ORDERS]])\n")
        .explainContains("EnumerableInterpreter\n"
            + "  BindableTableScan(table=[[STREAMS, ORDERS, (STREAM)]])")
        .returns(
            startsWith(
                "ROWTIME=2015-02-15 10:15:00; ID=1; PRODUCT=paint; UNITS=10",
                "ROWTIME=2015-02-15 10:24:15; ID=2; PRODUCT=paper; UNITS=5"));
  }

  @Test void testStreamFilterProject() {
    CalciteAssert.model(STREAM_MODEL)
        .withDefaultSchema("STREAMS")
        .query("select stream product from orders where units > 6")
        .convertContains(
            "LogicalDelta\n"
                + "  LogicalProject(PRODUCT=[$1])\n"
                + "    LogicalFilter(condition=[>($2, 6)])\n"
                + "      LogicalProject(ROWTIME=[$0], PRODUCT=[$2], UNITS=[$3])\n"
                + "        LogicalTableScan(table=[[STREAMS, ORDERS]])\n")
        .explainContains(
            "EnumerableCalc(expr#0..3=[{inputs}], expr#4=[6], expr#5=[>($t3, $t4)], PRODUCT=[$t2], $condition=[$t5])\n"
                + "  EnumerableInterpreter\n"
                + "    BindableTableScan(table=[[STREAMS, ORDERS, (STREAM)]])")
        .returns(
            startsWith("PRODUCT=paint",
                "PRODUCT=brush"));
  }

  @Test void testStreamGroupByHaving() {
    CalciteAssert.model(STREAM_MODEL)
        .withDefaultSchema("STREAMS")
        .query("select stream floor(rowtime to hour) as rowtime,\n"
            + "  product, count(*) as c\n"
            + "from orders\n"
            + "group by floor(rowtime to hour), product\n"
            + "having count(*) > 1")
        .convertContains(
            "LogicalDelta\n"
                + "  LogicalFilter(condition=[>($2, 1)])\n"
                + "    LogicalAggregate(group=[{0, 1}], C=[COUNT()])\n"
                + "      LogicalProject(ROWTIME=[FLOOR($0, FLAG(HOUR))], PRODUCT=[$2])\n"
                + "        LogicalTableScan(table=[[STREAMS, ORDERS]])\n")
        .explainContains(
            "EnumerableCalc(expr#0..2=[{inputs}], expr#3=[1], expr#4=[>($t2, $t3)], proj#0..2=[{exprs}], $condition=[$t4])\n"
                + "  EnumerableAggregate(group=[{0, 1}], C=[COUNT()])\n"
                + "    EnumerableCalc(expr#0..3=[{inputs}], expr#4=[FLAG(HOUR)], expr#5=[FLOOR($t0, $t4)], ROWTIME=[$t5], PRODUCT=[$t2])\n"
                + "      EnumerableInterpreter\n"
                + "        BindableTableScan(table=[[STREAMS, ORDERS, (STREAM)]])")
        .returns(
            startsWith("ROWTIME=2015-02-15 10:00:00; PRODUCT=paint; C=2"));
  }

  @Test void testStreamOrderBy() {
    CalciteAssert.model(STREAM_MODEL)
        .withDefaultSchema("STREAMS")
        .query("select stream floor(rowtime to hour) as rowtime,\n"
            + "  product, units\n"
            + "from orders\n"
            + "order by floor(orders.rowtime to hour), product desc")
        .convertContains(
            "LogicalDelta\n"
                + "  LogicalSort(sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[DESC])\n"
                + "    LogicalProject(ROWTIME=[FLOOR($0, FLAG(HOUR))], PRODUCT=[$2], UNITS=[$3])\n"
                + "      LogicalTableScan(table=[[STREAMS, ORDERS]])\n")
        .explainContains(
            "EnumerableSort(sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[DESC])\n"
                + "  EnumerableCalc(expr#0..3=[{inputs}], expr#4=[FLAG(HOUR)], expr#5=[FLOOR($t0, $t4)], ROWTIME=[$t5], PRODUCT=[$t2], UNITS=[$t3])\n"
                + "    EnumerableInterpreter\n"
                + "      BindableTableScan(table=[[STREAMS, ORDERS, (STREAM)]])")
        .returns(
            startsWith("ROWTIME=2015-02-15 10:00:00; PRODUCT=paper; UNITS=5",
                "ROWTIME=2015-02-15 10:00:00; PRODUCT=paint; UNITS=10",
                "ROWTIME=2015-02-15 10:00:00; PRODUCT=paint; UNITS=3"));
  }

  @Disabled
  @Test void testStreamUnionAllOrderBy() {
    CalciteAssert.model(STREAM_MODEL)
        .withDefaultSchema("STREAMS")
        .query("select stream *\n"
            + "from (\n"
            + "  select rowtime, product\n"
            + "  from orders\n"
            + "  union all\n"
            + "  select rowtime, product\n"
            + "  from orders)\n"
            + "order by rowtime\n")
        .convertContains(
            "LogicalDelta\n"
                + "  LogicalSort(sort0=[$0], dir0=[ASC])\n"
                + "    LogicalProject(ROWTIME=[$0], PRODUCT=[$1])\n"
                + "      LogicalUnion(all=[true])\n"
                + "        LogicalProject(ROWTIME=[$0], PRODUCT=[$2])\n"
                + "          EnumerableTableScan(table=[[STREAMS, ORDERS]])\n"
                + "        LogicalProject(ROWTIME=[$0], PRODUCT=[$2])\n"
                + "          EnumerableTableScan(table=[[STREAMS, ORDERS]])\n")
        .explainContains(
            "EnumerableSort(sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[DESC])\n"
                + "  EnumerableCalc(expr#0..3=[{inputs}], expr#4=[FLAG(HOUR)], expr#5=[FLOOR($t0, $t4)], ROWTIME=[$t5], PRODUCT=[$t2], UNITS=[$t3])\n"
                + "    EnumerableInterpreter\n"
                + "      BindableTableScan(table=[[]])")
        .returns(
            startsWith("ROWTIME=2015-02-15 10:00:00; PRODUCT=paper; UNITS=5",
                "ROWTIME=2015-02-15 10:00:00; PRODUCT=paint; UNITS=10",
                "ROWTIME=2015-02-15 10:00:00; PRODUCT=paint; UNITS=3"));
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-809">[CALCITE-809]
   * TableScan does not support large/infinite scans</a>.
   */
  @Test void testInfiniteStreamsDoNotBufferInMemory() {
    CalciteAssert.model(STREAM_MODEL)
        .withDefaultSchema(INFINITE_STREAM_SCHEMA_NAME)
        .query("select stream * from orders")
        .limit(100)
        .explainContains("EnumerableInterpreter\n"
            + "  BindableTableScan(table=[[INFINITE_STREAMS, ORDERS, (STREAM)]])")
        .returnsCount(100);
  }

  @Test @Timeout(10) public void testStreamCancel() {
    final String explain = "EnumerableInterpreter\n"
        + "  BindableTableScan(table=[[INFINITE_STREAMS, ORDERS, (STREAM)]])";
    CalciteAssert.model(STREAM_MODEL)
        .withDefaultSchema(INFINITE_STREAM_SCHEMA_NAME)
        .query("select stream * from orders")
        .explainContains(explain)
        .returns(resultSet -> {
          int n = 0;
          try {
            while (resultSet.next()) {
              if (++n == 5) {
                new Thread(() -> {
                  try {
                    Thread.sleep(3);
                    resultSet.getStatement().cancel();
                  } catch (InterruptedException | SQLException e) {
                    // ignore
                  }
                }).start();
              }
            }
            fail("expected cancel, got end-of-data");
          } catch (SQLException e) {
            assertThat(e.getMessage(), is("Statement canceled"));
          }
          // With a 3 millisecond delay, typically n is between 200 - 400
          // before cancel takes effect.
          assertThat(n,
              ComparatorMatcherBuilder.<Integer>usingNaturalOrdering().greaterThan(5));
        });
  }

  @Test void testStreamToRelationJoin() {
    CalciteAssert.model(STREAM_JOINS_MODEL)
        .withDefaultSchema(STREAM_JOINS_SCHEMA_NAME)
        .query("select stream "
            + "orders.rowtime as rowtime, orders.id as orderId, products.supplier as supplierId "
            + "from orders join products on orders.product = products.id")
        .convertContains("LogicalDelta\n"
            + "  LogicalProject(ROWTIME=[$0], ORDERID=[$1], SUPPLIERID=[$4])\n"
            + "    LogicalJoin(condition=[=($2, $3)], joinType=[inner])\n"
            + "      LogicalProject(ROWTIME=[$0], ID=[$1], PRODUCT0=[CAST($2):VARCHAR(32) NOT NULL])\n"
            + "        LogicalTableScan(table=[[STREAM_JOINS, ORDERS]])\n"
            + "      LogicalTableScan(table=[[STREAM_JOINS, PRODUCTS]])\n")
        .explainContains(""
            + "EnumerableCalc(expr#0..4=[{inputs}], proj#0..1=[{exprs}], SUPPLIERID=[$t4])\n"
            + "  EnumerableMergeJoin(condition=[=($2, $3)], joinType=[inner])\n"
            + "    EnumerableSort(sort0=[$2], dir0=[ASC])\n"
            + "      EnumerableCalc(expr#0..3=[{inputs}], expr#4=[CAST($t2):VARCHAR(32) NOT NULL], proj#0..1=[{exprs}], PRODUCT0=[$t4])\n"
            + "        EnumerableInterpreter\n"
            + "          BindableTableScan(table=[[STREAM_JOINS, ORDERS, (STREAM)]])\n"
            + "    EnumerableSort(sort0=[$0], dir0=[ASC])\n"
            + "      EnumerableTableScan(table=[[STREAM_JOINS, PRODUCTS]])\n")
        .returns(
            startsWith("ROWTIME=2015-02-15 10:24:45; ORDERID=3; SUPPLIERID=1",
                "ROWTIME=2015-02-15 10:15:00; ORDERID=1; SUPPLIERID=1",
                "ROWTIME=2015-02-15 10:58:00; ORDERID=4; SUPPLIERID=1"));
  }

  @Disabled
  @Test void testTumbleViaOver() {
    String sql = "WITH HourlyOrderTotals (rowtime, productId, c, su) AS (\n"
        + "  SELECT FLOOR(rowtime TO HOUR),\n"
        + "    productId,\n"
        + "    COUNT(*),\n"
        + "    SUM(units)\n"
        + "  FROM Orders\n"
        + "  GROUP BY FLOOR(rowtime TO HOUR), productId)\n"
        + "SELECT STREAM rowtime,\n"
        + "  productId,\n"
        + "  SUM(su) OVER w AS su,\n"
        + "  SUM(c) OVER w AS c\n"
        + "FROM HourlyTotals\n"
        + "WINDOW w AS (\n"
        + "  ORDER BY rowtime\n"
        + "  PARTITION BY productId\n"
        + "  RANGE INTERVAL '2' HOUR PRECEDING)\n";
    String sql2 = ""
        + "SELECT STREAM rowtime, productId, SUM(units) AS su, COUNT(*) AS c\n"
        + "FROM Orders\n"
        + "GROUP BY TUMBLE(rowtime, INTERVAL '1' HOUR)";
    // sql and sql2 should give same result
    CalciteAssert.model(STREAM_JOINS_MODEL)
        .query(sql);
  }

  private Consumer<ResultSet> startsWith(String... rows) {
    final ImmutableList<String> rowList = ImmutableList.copyOf(rows);
    return resultSet -> {
      try {
        final CalciteAssert.ResultSetFormatter formatter =
            new CalciteAssert.ResultSetFormatter();
        final ResultSetMetaData metaData = resultSet.getMetaData();
        for (String expectedRow : rowList) {
          if (!resultSet.next()) {
            throw new AssertionError("input ended too soon");
          }
          formatter.rowToString(resultSet, metaData);
          String actualRow = formatter.string();
          assertThat(actualRow, equalTo(expectedRow));
        }
      } catch (SQLException e) {
        throw TestUtil.rethrow(e);
      }
    };
  }


}
