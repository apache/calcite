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

import org.apache.calcite.DataContext;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.StreamableTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TableFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

import org.junit.Ignore;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * Tests for streaming queries.
 */
public class StreamTest {
  public static final String STREAM_SCHEMA = "     {\n"
      + "       name: 'STREAMS',\n"
      + "       tables: [ {\n"
      + "         type: 'custom',\n"
      + "         name: 'ORDERS',\n"
      + "         stream: {\n"
      + "           stream: true\n"
      + "         },\n"
      + "         factory: '" + OrdersStreamTableFactory.class.getName() + "'\n"
      + "       } ]\n"
      + "     }\n";

  public static final String STREAM_MODEL = "{\n"
      + "  version: '1.0',\n"
      + "  defaultSchema: 'foodmart',\n"
      + "   schemas: [\n"
      + STREAM_SCHEMA
      + "   ]\n"
      + "}";

  @Test public void testStream() {
    CalciteAssert.model(STREAM_MODEL)
        .withDefaultSchema("STREAMS")
        .query("select stream * from orders")
        .convertContains("LogicalDelta\n"
            + "  LogicalProject(ROWTIME=[$0], ID=[$1], PRODUCT=[$2], UNITS=[$3])\n"
            + "    EnumerableTableScan(table=[[STREAMS, ORDERS]])\n")
        .explainContains("EnumerableInterpreter\n"
            + "  BindableTableScan(table=[[]])")
        .returns(
            startsWith(
                "ROWTIME=2015-02-15 10:15:00; ID=1; PRODUCT=paint; UNITS=10",
                "ROWTIME=2015-02-15 10:24:15; ID=2; PRODUCT=paper; UNITS=5"));
  }

  @Test public void testStreamFilterProject() {
    CalciteAssert.model(STREAM_MODEL)
        .withDefaultSchema("STREAMS")
        .query("select stream product from orders where units > 6")
        .convertContains(
            "LogicalDelta\n"
                + "  LogicalProject(PRODUCT=[$2])\n"
                + "    LogicalFilter(condition=[>($3, 6)])\n"
                + "      EnumerableTableScan(table=[[STREAMS, ORDERS]])\n")
        .explainContains(
            "EnumerableCalc(expr#0..3=[{inputs}], expr#4=[6], expr#5=[>($t3, $t4)], PRODUCT=[$t2], $condition=[$t5])\n"
                + "  EnumerableInterpreter\n"
                + "    BindableTableScan(table=[[]])")
        .returns(
            startsWith("PRODUCT=paint",
                "PRODUCT=brush"));
  }

  @Test public void testStreamGroupByHaving() {
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
                + "        EnumerableTableScan(table=[[STREAMS, ORDERS]])\n")
        .explainContains(
            "EnumerableCalc(expr#0..2=[{inputs}], expr#3=[1], expr#4=[>($t2, $t3)], proj#0..2=[{exprs}], $condition=[$t4])\n"
                + "  EnumerableAggregate(group=[{0, 1}], C=[COUNT()])\n"
                + "    EnumerableCalc(expr#0..3=[{inputs}], expr#4=[FLAG(HOUR)], expr#5=[FLOOR($t0, $t4)], ROWTIME=[$t5], PRODUCT=[$t2])\n"
                + "      EnumerableInterpreter\n"
                + "        BindableTableScan(table=[[]])")
        .returns(
            startsWith("ROWTIME=2015-02-15 10:00:00; PRODUCT=paint; C=2"));
  }

  @Test public void testStreamOrderBy() {
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
                + "      EnumerableTableScan(table=[[STREAMS, ORDERS]])\n")
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

  @Ignore
  @Test public void testStreamUnionAllOrderBy() {
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

  private Function<ResultSet, Void> startsWith(String... rows) {
    final ImmutableList<String> rowList = ImmutableList.copyOf(rows);
    return new Function<ResultSet, Void>() {
      public Void apply(ResultSet input) {
        try {
          final StringBuilder buf = new StringBuilder();
          final ResultSetMetaData metaData = input.getMetaData();
          for (String expectedRow : rowList) {
            if (!input.next()) {
              throw new AssertionError("input ended too soon");
            }
            CalciteAssert.rowToString(input, buf, metaData);
            String actualRow = buf.toString();
            buf.setLength(0);
            assertThat(actualRow, equalTo(expectedRow));
          }
          return null;
        } catch (SQLException e) {
          throw Throwables.propagate(e);
        }
      }
    };
  }

  /** Mock table that returns a stream of orders from a fixed array. */
  @SuppressWarnings("UnusedDeclaration")
  public static class OrdersStreamTableFactory implements TableFactory<Table> {
    // public constructor, per factory contract
    public OrdersStreamTableFactory() {
    }

    public Table create(SchemaPlus schema, String name,
        Map<String, Object> operand, RelDataType rowType) {
      final RelProtoDataType protoRowType = new RelProtoDataType() {
        public RelDataType apply(RelDataTypeFactory a0) {
          return a0.builder()
              .add("ROWTIME", SqlTypeName.TIMESTAMP)
              .add("ID", SqlTypeName.INTEGER)
              .add("PRODUCT", SqlTypeName.VARCHAR, 10)
              .add("UNITS", SqlTypeName.INTEGER)
              .build();
        }
      };
      final ImmutableList<Object[]> rows = ImmutableList.of(
          new Object[] {ts(10, 15, 0), 1, "paint", 10},
          new Object[] {ts(10, 24, 15), 2, "paper", 5},
          new Object[] {ts(10, 24, 45), 3, "brush", 12},
          new Object[] {ts(10, 58, 0), 4, "paint", 3},
          new Object[] {ts(11, 10, 0), 5, "paint", 3});

      return new StreamableTable() {
        public Table stream() {
          return new OrdersTable(protoRowType, rows);
        }

        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
          return protoRowType.apply(typeFactory);
        }

        public Statistic getStatistic() {
          return Statistics.of(100d,
              ImmutableList.<ImmutableBitSet>of(),
              RelCollations.createSingleton(0));
        }

        public Schema.TableType getJdbcTableType() {
          return Schema.TableType.TABLE;
        }
      };
    }

    private Object ts(int h, int m, int s) {
      return DateTimeUtils.unixTimestamp(2015, 2, 15, h, m, s);
    }
  }

  /** Table representing the ORDERS stream. */
  public static class OrdersTable implements ScannableTable {
    private final RelProtoDataType protoRowType;
    private final ImmutableList<Object[]> rows;

    public OrdersTable(RelProtoDataType protoRowType,
        ImmutableList<Object[]> rows) {
      this.protoRowType = protoRowType;
      this.rows = rows;
    }

    public Enumerable<Object[]> scan(DataContext root) {
      return Linq4j.asEnumerable(rows);
    }

    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return protoRowType.apply(typeFactory);
    }

    public Statistic getStatistic() {
      return Statistics.of(100d,
          ImmutableList.<ImmutableBitSet>of(),
          RelCollations.createSingleton(0));
    }

    public Schema.TableType getJdbcTableType() {
      return Schema.TableType.STREAM;
    }
  }
}

// End StreamTest.java
