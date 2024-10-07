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
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.DelegatingEnumerator;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.runtime.PairList;
import org.apache.calcite.schema.FilterableTable;
import org.apache.calcite.schema.ProjectableFilterableTable;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.test.CalciteAssert.ConnectionPostProcessor;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Pair;

import com.google.common.collect.ImmutableMap;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.hasToString;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import static java.util.Objects.requireNonNull;

/**
 * Unit test for {@link org.apache.calcite.schema.ScannableTable}.
 */
public class ScannableTableTest {
  @Test void testTens() {
    try (Enumerator<Object[]> cursor = tens()) {
      assertTrue(cursor.moveNext());
      assertThat(cursor.current()[0], equalTo(0));
      assertThat(cursor.current(), arrayWithSize(1));
      assertTrue(cursor.moveNext());
      assertThat(cursor.current()[0], equalTo(10));
      assertTrue(cursor.moveNext());
      assertThat(cursor.current()[0], equalTo(20));
      assertTrue(cursor.moveNext());
      assertThat(cursor.current()[0], equalTo(30));
      assertFalse(cursor.moveNext());
    }
  }

  /** A table with one column. */
  @Test void testSimple() {
    CalciteAssert.that()
        .with(newSchema("s", PairList.of("simple", new SimpleTable())))
        .query("select * from \"s\".\"simple\"")
        .returnsUnordered("i=0", "i=10", "i=20", "i=30");
  }

  /** A table with two columns. */
  @Test void testSimple2() {
    CalciteAssert.that()
        .with(newSchema("s", PairList.of("beatles", new BeatlesTable())))
        .query("select * from \"s\".\"beatles\"")
        .returnsUnordered("i=4; j=John; k=1940",
            "i=4; j=Paul; k=1942",
            "i=6; j=George; k=1943",
            "i=5; j=Ringo; k=1940");
  }

  /** A filter on a {@link FilterableTable} with two columns (cooperative). */
  @Test void testFilterableTableCooperative() {
    final StringBuilder buf = new StringBuilder();
    final Table table = new BeatlesFilterableTable(buf, true);
    final String explain = "PLAN="
        + "EnumerableInterpreter\n"
        + "  BindableTableScan(table=[[s, beatles]], filters=[[=($0, 4)]])";
    CalciteAssert.that()
        .with(newSchema("s", PairList.of("beatles", table)))
        .query("select * from \"s\".\"beatles\" where \"i\" = 4")
        .explainContains(explain)
        .returnsUnordered("i=4; j=John; k=1940",
            "i=4; j=Paul; k=1942");
    // Only 2 rows came out of the table. If the value is 4, it means that the
    // planner did not pass the filter down.
    assertThat(buf, hasToString("returnCount=2, filter=<0, 4>"));
  }

  /** A filter on a {@link FilterableTable} with two columns (noncooperative). */
  @Test void testFilterableTableNonCooperative() {
    final StringBuilder buf = new StringBuilder();
    final Table table = new BeatlesFilterableTable(buf, false);
    final String explain = "PLAN="
        + "EnumerableInterpreter\n"
        + "  BindableTableScan(table=[[s, beatles2]], filters=[[=($0, 4)]])";
    CalciteAssert.that()
        .with(newSchema("s", PairList.of("beatles2", table)))
        .query("select * from \"s\".\"beatles2\" where \"i\" = 4")
        .explainContains(explain)
        .returnsUnordered("i=4; j=John; k=1940",
            "i=4; j=Paul; k=1942");
    assertThat(buf, hasToString("returnCount=4"));
  }

  /** A filter on a {@link org.apache.calcite.schema.ProjectableFilterableTable}
   * with two columns (cooperative). */
  @Test void testProjectableFilterableCooperative() {
    final StringBuilder buf = new StringBuilder();
    final Table table = new BeatlesProjectableFilterableTable(buf, true);
    final String explain = "PLAN="
        + "EnumerableInterpreter\n"
        + "  BindableTableScan(table=[[s, beatles]], filters=[[=($0, 4)]], projects=[[1]])";
    CalciteAssert.that()
        .with(newSchema("s", PairList.of("beatles", table)))
        .query("select \"j\" from \"s\".\"beatles\" where \"i\" = 4")
        .explainContains(explain)
        .returnsUnordered("j=John",
            "j=Paul");
    // Only 2 rows came out of the table. If the value is 4, it means that the
    // planner did not pass the filter down.
    assertThat(buf,
        hasToString("returnCount=2, filter=<0, 4>, projects=[1, 0]"));
  }

  @Test void testProjectableFilterableNonCooperative() {
    final StringBuilder buf = new StringBuilder();
    final Table table = new BeatlesProjectableFilterableTable(buf, false);
    final String explain = "PLAN="
        + "EnumerableInterpreter\n"
        + "  BindableTableScan(table=[[s, beatles2]], filters=[[=($0, 4)]], projects=[[1]]";
    CalciteAssert.that()
        .with(newSchema("s", PairList.of("beatles2", table)))
        .query("select \"j\" from \"s\".\"beatles2\" where \"i\" = 4")
        .explainContains(explain)
        .returnsUnordered("j=John",
            "j=Paul");
    assertThat(buf, hasToString("returnCount=4, projects=[1, 0]"));
  }

  /** A filter on a {@link org.apache.calcite.schema.ProjectableFilterableTable}
   * with two columns, and a project in the query. (Cooperative)*/
  @Test void testProjectableFilterableWithProjectAndFilter() {
    final StringBuilder buf = new StringBuilder();
    final Table table = new BeatlesProjectableFilterableTable(buf, true);
    final String explain = "PLAN="
        + "EnumerableInterpreter\n"
        + "  BindableTableScan(table=[[s, beatles]], filters=[[=($0, 4)]], projects=[[2, 1]]";
    CalciteAssert.that()
        .with(newSchema("s", PairList.of("beatles", table)))
        .query("select \"k\",\"j\" from \"s\".\"beatles\" where \"i\" = 4")
        .explainContains(explain)
        .returnsUnordered("k=1940; j=John",
            "k=1942; j=Paul");
    assertThat(buf,
        hasToString("returnCount=2, filter=<0, 4>, projects=[2, 1, 0]"));
  }

  /** A filter on a {@link org.apache.calcite.schema.ProjectableFilterableTable}
   * with two columns, and a project in the query (NonCooperative). */
  @Test void testProjectableFilterableWithProjectFilterNonCooperative() {
    final StringBuilder buf = new StringBuilder();
    final Table table = new BeatlesProjectableFilterableTable(buf, false);
    final String explain = "PLAN="
        + "EnumerableInterpreter\n"
        + "  BindableTableScan(table=[[s, beatles]], filters=[[>($2, 1941)]], "
        + "projects=[[0, 2]])";
    CalciteAssert.that()
        .with(newSchema("s", PairList.of("beatles", table)))
        .query("select \"i\",\"k\" from \"s\".\"beatles\" where \"k\" > 1941")
        .explainContains(explain)
        .returnsUnordered("i=4; k=1942",
            "i=6; k=1943");
    assertThat(buf,
        hasToString("returnCount=4, projects=[0, 2]"));
  }

  /** A filter and project on a
   * {@link org.apache.calcite.schema.ProjectableFilterableTable}. The table
   * refuses to execute the filter, so Calcite should add a pull up and
   * transform the filter (projecting the column needed by the filter). */
  @Test void testPFTableRefusesFilterCooperative() {
    final StringBuilder buf = new StringBuilder();
    final Table table = new BeatlesProjectableFilterableTable(buf, false);
    final String explain = "PLAN=EnumerableInterpreter\n"
        + "  BindableTableScan(table=[[s, beatles2]], filters=[[=($0, 4)]], projects=[[2]])";
    CalciteAssert.that()
        .with(newSchema("s", PairList.of("beatles2", table)))
        .query("select \"k\" from \"s\".\"beatles2\" where \"i\" = 4")
        .explainContains(explain)
        .returnsUnordered("k=1940",
            "k=1942");
    assertThat(buf,
        hasToString("returnCount=4, projects=[2, 0]"));
  }

  @Test void testPFPushDownProjectFilterInAggregateNoGroup() {
    final StringBuilder buf = new StringBuilder();
    final Table table = new BeatlesProjectableFilterableTable(buf, false);
    final String explain = "PLAN=EnumerableAggregate(group=[{}], M=[MAX($0)])\n"
        + "  EnumerableInterpreter\n"
        + "    BindableTableScan(table=[[s, beatles]], filters=[[>($0, 1)]], projects=[[2]])";
    CalciteAssert.that()
        .with(newSchema("s", PairList.of("beatles", table)))
        .query("select max(\"k\") as m from \"s\".\"beatles\" where \"i\" > 1")
        .explainContains(explain)
        .returnsUnordered("M=1943");
  }

  @Test void testPFPushDownProjectFilterAggregateGroup() {
    final String sql = "select \"i\", count(*) as c\n"
        + "from \"s\".\"beatles\"\n"
        + "where \"k\" > 1900\n"
        + "group by \"i\"";
    final StringBuilder buf = new StringBuilder();
    final Table table = new BeatlesProjectableFilterableTable(buf, false);
    final String explain = "PLAN="
        + "EnumerableAggregate(group=[{0}], C=[COUNT()])\n"
        + "  EnumerableInterpreter\n"
        + "    BindableTableScan(table=[[s, beatles]], filters=[[>($2, 1900)]], "
        + "projects=[[0]])";
    CalciteAssert.that()
        .with(newSchema("s", PairList.of("beatles", table)))
        .query(sql)
        .explainContains(explain)
        .returnsUnordered("i=4; C=2",
            "i=5; C=1",
            "i=6; C=1");
  }

  @Test void testPFPushDownProjectFilterAggregateNested() {
    final StringBuilder buf = new StringBuilder();
    final String sql = "select \"k\", count(*) as c\n"
        + "from (\n"
        + "  select \"k\", \"i\" from \"s\".\"beatles\" group by \"k\", \"i\") t\n"
        + "where \"k\" = 1940\n"
        + "group by \"k\"";
    final Table table = new BeatlesProjectableFilterableTable(buf, false);
    final String explain = "PLAN="
        + "EnumerableAggregate(group=[{0}], C=[COUNT()])\n"
        + "  EnumerableCalc(expr#0=[{inputs}], expr#1=[1940], k=[$t1], i=[$t0])\n"
        + "    EnumerableAggregate(group=[{1}])\n"
        + "      EnumerableInterpreter\n"
        + "        BindableTableScan(table=[[s, beatles]], filters=[[=($2, 1940)]], projects=[[2, 0]])";
    CalciteAssert.that()
        .with(newSchema("s", PairList.of("beatles", table)))
        .query(sql)
        .explainContains(explain)
        .returnsUnordered("k=1940; C=2");
  }

  private static @Nullable Pair<Integer, Object> getFilter(boolean cooperative,
      List<RexNode> filters) {
    final Iterator<RexNode> filterIter = filters.iterator();
    while (filterIter.hasNext()) {
      final RexNode node = filterIter.next();
      if (cooperative
          && node instanceof RexCall
          && ((RexCall) node).getOperator() == SqlStdOperatorTable.EQUALS
          && ((RexCall) node).getOperands().get(0) instanceof RexInputRef
          && ((RexCall) node).getOperands().get(1) instanceof RexLiteral) {
        filterIter.remove();
        final int pos = ((RexInputRef) ((RexCall) node).getOperands().get(0)).getIndex();
        final RexLiteral op1 = (RexLiteral) ((RexCall) node).getOperands().get(1);
        switch (pos) {
        case 0:
        case 2:
          return Pair.of(pos, ((BigDecimal) op1.getValue()).intValue());
        case 1:
          return Pair.of(pos, ((NlsString) op1.getValue()).getValue());
        }
      }
    }
    return null;
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-458">[CALCITE-458]
   * ArrayIndexOutOfBoundsException when using just a single column in
   * interpreter</a>. */
  @Test void testPFTableRefusesFilterSingleColumn() {
    final StringBuilder buf = new StringBuilder();
    final Table table = new BeatlesProjectableFilterableTable(buf, false);
    final String explain = "PLAN="
        + "EnumerableInterpreter\n"
        + "  BindableTableScan(table=[[s, beatles2]], filters=[[>($2, 1941)]], projects=[[2]])";
    CalciteAssert.that()
        .with(newSchema("s", PairList.of("beatles2", table)))
        .query("select \"k\" from \"s\".\"beatles2\" where \"k\" > 1941")
        .explainContains(explain)
        .returnsUnordered("k=1942",
            "k=1943");
    assertThat(buf, hasToString("returnCount=4, projects=[2]"));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3405">[CALCITE-3405]
   * Prune columns for ProjectableFilterable when project is not simple mapping</a>. */
  @Test void testPushNonSimpleMappingProject() {
    final StringBuilder buf = new StringBuilder();
    final Table table = new BeatlesProjectableFilterableTable(buf, true);
    final String explain = "PLAN="
        + "EnumerableCalc(expr#0..1=[{inputs}], expr#2=[+($t1, $t1)], expr#3=[3],"
        + " proj#0..1=[{exprs}], k0=[$t0], $f3=[$t2], $f4=[$t3])\n"
        + "  EnumerableInterpreter\n"
        + "    BindableTableScan(table=[[s, beatles]], projects=[[2, 0]])";
    CalciteAssert.that()
        .with(newSchema("s", PairList.of("beatles", table)))
        .query("select \"k\", \"i\", \"k\", \"i\"+\"i\" \"ii\", 3 from \"s\".\"beatles\"")
        .explainContains(explain)
        .returnsUnordered(
            "k=1940; i=4; k=1940; ii=8; EXPR$3=3",
            "k=1940; i=5; k=1940; ii=10; EXPR$3=3",
            "k=1942; i=4; k=1942; ii=8; EXPR$3=3",
            "k=1943; i=6; k=1943; ii=12; EXPR$3=3");
    assertThat(buf, hasToString("returnCount=4, projects=[2, 0]"));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3405">[CALCITE-3405]
   * Prune columns for ProjectableFilterable when project is not simple mapping</a>. */
  @Test void testPushSimpleMappingProject() {
    final StringBuilder buf = new StringBuilder();
    final Table table = new BeatlesProjectableFilterableTable(buf, true);
    // Note that no redundant Project on EnumerableInterpreter
    final String explain = "PLAN="
        + "EnumerableInterpreter\n"
        + "  BindableTableScan(table=[[s, beatles]], projects=[[2, 0]])";
    CalciteAssert.that()
        .with(newSchema("s", PairList.of("beatles", table)))
        .query("select \"k\", \"i\" from \"s\".\"beatles\"")
        .explainContains(explain)
        .returnsUnordered(
            "k=1940; i=4",
            "k=1940; i=5",
            "k=1942; i=4",
            "k=1943; i=6");
    assertThat(buf, hasToString("returnCount=4, projects=[2, 0]"));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3479">[CALCITE-3479]
   * Stack overflow error thrown when running join query</a>
   * Test two ProjectableFilterableTable can join and produce right plan.
   */
  @Test void testProjectableFilterableTableJoin() {
    final StringBuilder buf = new StringBuilder();
    final String explain = "PLAN="
        + "EnumerableNestedLoopJoin(condition=[true], joinType=[inner])\n"
        + "  EnumerableInterpreter\n"
        + "    BindableTableScan(table=[[s, b1]], filters=[[=($0, 10)]])\n"
        + "  EnumerableInterpreter\n"
        + "    BindableTableScan(table=[[s, b2]], filters=[[=($0, 10)]])";
    CalciteAssert.that()
            .with(
              newSchema("s",
                  PairList.<String, Table>builder()
                      .add("b1",
                          new BeatlesProjectableFilterableTable(buf, true))
                      .add("b2",
                          new BeatlesProjectableFilterableTable(buf, true))
                      .build()))
            .query("select * from \"s\".\"b1\", \"s\".\"b2\" "
                    + "where \"s\".\"b1\".\"i\" = 10 and \"s\".\"b2\".\"i\" = 10 "
                    + "and \"s\".\"b1\".\"i\" = \"s\".\"b2\".\"i\"")
            .explainContains(explain);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5019">[CALCITE-5019]
   * Avoid multiple scans when table is ProjectableFilterableTable</a>.*/
  @Test void testProjectableFilterableWithScanCounter() {
    final StringBuilder buf = new StringBuilder();
    final BeatlesProjectableFilterableTable table =
        new BeatlesProjectableFilterableTable(buf, false);
    final String explain = "PLAN="
        + "EnumerableInterpreter\n"
        + "  BindableTableScan(table=[[s, beatles]], filters=[[=($0, 4)]], projects=[[1]]";
    CalciteAssert.that()
        .with(newSchema("s", PairList.of("beatles", table)))
        .query("select \"j\" from \"s\".\"beatles\" where \"i\" = 4")
        .explainContains(explain)
        .returnsUnordered("j=John", "j=Paul");
    assertThat(table.getScanCount(), is(1));
    assertThat(buf, hasToString("returnCount=4, projects=[1, 0]"));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1031">[CALCITE-1031]
   * In prepared statement, CsvScannableTable.scan is called twice</a>. */
  @Test void testPrepared2() throws SQLException {
    final Properties properties = new Properties();
    properties.setProperty("caseSensitive", "true");
    try (Connection connection =
             DriverManager.getConnection("jdbc:calcite:", properties)) {
      final CalciteConnection calciteConnection =
          connection.unwrap(CalciteConnection.class);

      final AtomicInteger scanCount = new AtomicInteger();
      final AtomicInteger enumerateCount = new AtomicInteger();
      final AtomicInteger closeCount = new AtomicInteger();
      final Schema schema =
          new AbstractSchema() {
            @Override protected Map<String, Table> getTableMap() {
              return ImmutableMap.of("TENS",
                  countingTable(scanCount, enumerateCount, closeCount));
            }
          };
      calciteConnection.getRootSchema().add("TEST", schema);
      final String sql = "select * from \"TEST\".\"TENS\" where \"i\" < ?";
      final PreparedStatement statement =
          calciteConnection.prepareStatement(sql);
      assertThat(scanCount.get(), is(0));
      assertThat(enumerateCount.get(), is(0));

      // First execute
      statement.setInt(1, 20);
      assertThat(scanCount.get(), is(0));
      ResultSet resultSet = statement.executeQuery();
      assertThat(scanCount.get(), is(1));
      assertThat(enumerateCount.get(), is(1));
      assertThat(resultSet,
          Matchers.returnsUnordered("i=0", "i=10"));
      assertThat(scanCount.get(), is(1));
      assertThat(enumerateCount.get(), is(1));

      // Second execute
      resultSet = statement.executeQuery();
      assertThat(scanCount.get(), is(2));
      assertThat(resultSet,
          Matchers.returnsUnordered("i=0", "i=10"));
      assertThat(scanCount.get(), is(2));

      // Third execute
      statement.setInt(1, 30);
      resultSet = statement.executeQuery();
      assertThat(scanCount.get(), is(3));
      assertThat(resultSet,
          Matchers.returnsUnordered("i=0", "i=10", "i=20"));
      assertThat(scanCount.get(), is(3));
    }
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3758">[CALCITE-3758]
   * FilterTableScanRule generate wrong mapping for filter condition
   * when underlying is BindableTableScan</a>. */
  @Test void testPFTableInBindableConvention() {
    final StringBuilder buf = new StringBuilder();
    final Table table = new BeatlesProjectableFilterableTable(buf, true);
    try (Hook.Closeable ignored = Hook.ENABLE_BINDABLE.addThread(Hook.propertyJ(true))) {
      final String explain = "PLAN="
          + "BindableTableScan(table=[[s, beatles]], filters=[[=($1, 'John')]], projects=[[1]])";
      CalciteAssert.that()
          .with(newSchema("s", PairList.of("beatles", table)))
          .query("select \"j\" from \"s\".\"beatles\" where \"j\" = 'John'")
          .explainContains(explain)
          .returnsUnordered("j=John");
      assertThat(buf,
          hasToString("returnCount=1, filter=<1, John>, projects=[1]"));
    }
  }

  protected ConnectionPostProcessor newSchema(final String schemaName,
      PairList<String, Table> tables) {
    return connection -> {
      CalciteConnection con = connection.unwrap(CalciteConnection.class);
      SchemaPlus rootSchema = con.getRootSchema();
      SchemaPlus schema = rootSchema.add(schemaName, new AbstractSchema());
      tables.forEach(schema::add);
      connection.setSchema(schemaName);
      return connection;
    };
  }

  /** Table that returns one column via the {@link ScannableTable} interface. */
  public static class SimpleTable extends AbstractTable
      implements ScannableTable {
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          .add("i", SqlTypeName.INTEGER)
          .build();
    }

    public Enumerable<@Nullable Object[]> scan(DataContext root) {
      return new AbstractEnumerable<Object[]>() {
        public Enumerator<Object[]> enumerator() {
          return tens();
        }
      };
    }

  }

  /** Table that returns two columns via the ScannableTable interface. */
  public static class BeatlesTable extends AbstractTable
      implements ScannableTable {
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          .add("i", SqlTypeName.INTEGER)
          .add("j", SqlTypeName.VARCHAR)
          .add("k", SqlTypeName.INTEGER)
          .build();
    }

    public Enumerable<@Nullable Object[]> scan(DataContext root) {
      return new AbstractEnumerable<Object[]>() {
        public Enumerator<Object[]> enumerator() {
          return beatles(new StringBuilder(), null, null);
        }
      };
    }
  }

  /** Table that returns two columns via the {@link FilterableTable}
   * interface. */
  public static class BeatlesFilterableTable extends AbstractTable
      implements FilterableTable {
    private final StringBuilder buf;
    private final boolean cooperative;

    public BeatlesFilterableTable(StringBuilder buf, boolean cooperative) {
      this.buf = buf;
      this.cooperative = cooperative;
    }

    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          .add("i", SqlTypeName.INTEGER)
          .add("j", SqlTypeName.VARCHAR)
          .add("k", SqlTypeName.INTEGER)
          .build();
    }

    public Enumerable<@Nullable Object[]> scan(DataContext root, List<RexNode> filters) {
      final Pair<Integer, Object> filter = getFilter(cooperative, filters);
      return new AbstractEnumerable<Object[]>() {
        public Enumerator<Object[]> enumerator() {
          return beatles(buf, filter, null);
        }
      };
    }
  }

  /** Table that returns two columns via the {@link FilterableTable}
   * interface. */
  public static class BeatlesProjectableFilterableTable
      extends AbstractTable implements ProjectableFilterableTable {
    private final AtomicInteger scanCounter = new AtomicInteger();
    private final StringBuilder buf;
    private final boolean cooperative;

    BeatlesProjectableFilterableTable(StringBuilder buf,
        boolean cooperative) {
      this.buf = buf;
      this.cooperative = cooperative;
    }

    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          .add("i", SqlTypeName.INTEGER)
          .add("j", SqlTypeName.VARCHAR)
          .add("k", SqlTypeName.INTEGER)
          .build();
    }

    public Enumerable<@Nullable Object[]> scan(DataContext root, List<RexNode> filters,
        final int @Nullable [] projects) {
      scanCounter.incrementAndGet();
      final Pair<Integer, Object> filter = getFilter(cooperative, filters);
      return new AbstractEnumerable<Object[]>() {
        public Enumerator<Object[]> enumerator() {
          return beatles(buf, filter, projects);
        }
      };
    }

    public int getScanCount() {
      return this.scanCounter.get();
    }
  }

  private static Enumerator<Object[]> tens() {
    return new Enumerator<Object[]>() {
      int row = -1;
      Object @Nullable[] current;

      public Object[] current() {
        return requireNonNull(current, "current");
      }

      public boolean moveNext() {
        if (++row < 4) {
          current = new Object[] {row * 10};
          return true;
        } else {
          return false;
        }
      }

      public void reset() {
        row = -1;
      }

      public void close() {
        current = null;
      }
    };
  }

  /** Returns a table that counts the number of calls to
   * {@link ScannableTable#scan}, {@link Enumerable#enumerator()},
   * and {@link Enumerator#close()}. */
  static SimpleTable countingTable(AtomicInteger scanCount,
      AtomicInteger enumerateCount, AtomicInteger closeCount) {
    return new SimpleTable() {
      private Enumerable<Object[]> superScan(DataContext root) {
        return super.scan(root);
      }

      @Override public Enumerable<@Nullable Object[]> scan(DataContext root) {
        scanCount.incrementAndGet();
        return new AbstractEnumerable<Object[]>() {
          @Override public Enumerator<Object[]> enumerator() {
            enumerateCount.incrementAndGet();
            final Enumerator<Object[]> enumerator =
                superScan(root).enumerator();
            return new DelegatingEnumerator<Object[]>(enumerator) {
              @Override public void close() {
                closeCount.incrementAndGet();
                super.close();
              }
            };
          }
        };
      }
    };
  }

  private static final Object[][] BEATLES = {
      {4, "John", 1940},
      {4, "Paul", 1942},
      {6, "George", 1943},
      {5, "Ringo", 1940}
  };

  private static Enumerator<Object[]> beatles(final StringBuilder buf,
      @Nullable final Pair<Integer, Object> filter,
      final int @Nullable[] projects) {
    return new Enumerator<Object[]>() {
      int row = -1;
      int returnCount = 0;
      Object @Nullable[] current;

      public Object[] current() {
        return requireNonNull(current, "current");
      }

      public boolean moveNext() {
        while (++row < 4) {
          Object[] current = BEATLES[row % 4];
          if (filter == null || filter.right.equals(current[filter.left])) {
            if (projects == null) {
              this.current = current;
            } else {
              Object[] newCurrent = new Object[projects.length];
              for (int i = 0; i < projects.length; i++) {
                newCurrent[i] = current[projects[i]];
              }
              this.current = newCurrent;
            }
            ++returnCount;
            return true;
          }
        }
        return false;
      }

      public void reset() {
        row = -1;
      }

      public void close() {
        current = null;
        buf.append("returnCount=").append(returnCount);
        if (filter != null) {
          buf.append(", filter=").append(filter);
        }
        if (projects != null) {
          buf.append(", projects=").append(Arrays.toString(projects));
        }
      }
    };
  }
}
