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
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
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

import com.google.common.collect.ImmutableMap;

import org.junit.Assert;
import org.junit.Test;

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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for {@link org.apache.calcite.schema.ScannableTable}.
 */
public class ScannableTableTest {
  @Test public void testTens() throws SQLException {
    final Enumerator<Object[]> cursor = tens();
    assertTrue(cursor.moveNext());
    Assert.assertThat(cursor.current()[0], equalTo((Object) 0));
    Assert.assertThat(cursor.current().length, equalTo(1));
    assertTrue(cursor.moveNext());
    Assert.assertThat(cursor.current()[0], equalTo((Object) 10));
    assertTrue(cursor.moveNext());
    Assert.assertThat(cursor.current()[0], equalTo((Object) 20));
    assertTrue(cursor.moveNext());
    Assert.assertThat(cursor.current()[0], equalTo((Object) 30));
    assertFalse(cursor.moveNext());
  }

  /** A table with one column. */
  @Test public void testSimple() throws Exception {
    CalciteAssert.that()
        .with(newSchema("s", "simple", new SimpleTable()))
        .query("select * from \"s\".\"simple\"")
        .returnsUnordered("i=0", "i=10", "i=20", "i=30");
  }

  /** A table with two columns. */
  @Test public void testSimple2() throws Exception {
    CalciteAssert.that()
        .with(newSchema("s", "beatles", new BeatlesTable()))
        .query("select * from \"s\".\"beatles\"")
        .returnsUnordered("i=4; j=John",
            "i=4; j=Paul",
            "i=6; j=George",
            "i=5; j=Ringo");
  }

  /** A filter on a {@link FilterableTable} with two columns (cooperative). */
  @Test public void testFilterableTableCooperative() throws Exception {
    final StringBuilder buf = new StringBuilder();
    final Table table = new BeatlesFilterableTable(buf, true);
    final String explain = "PLAN="
        + "EnumerableInterpreter\n"
        + "  BindableTableScan(table=[[s, beatles]], filters=[[=($0, 4)]])";
    CalciteAssert.that()
        .with(newSchema("s", "beatles", table))
        .query("select * from \"s\".\"beatles\" where \"i\" = 4")
        .explainContains(explain)
        .returnsUnordered("i=4; j=John; k=1940",
            "i=4; j=Paul; k=1942");
    // Only 2 rows came out of the table. If the value is 4, it means that the
    // planner did not pass the filter down.
    assertThat(buf.toString(), is("returnCount=2, filter=4"));
  }

  /** A filter on a {@link FilterableTable} with two columns (noncooperative). */
  @Test public void testFilterableTableNonCooperative() throws Exception {
    final StringBuilder buf = new StringBuilder();
    final Table table = new BeatlesFilterableTable(buf, false);
    final String explain = "PLAN="
        + "EnumerableInterpreter\n"
        + "  BindableTableScan(table=[[s, beatles2]], filters=[[=($0, 4)]])";
    CalciteAssert.that()
        .with(newSchema("s", "beatles2", table))
        .query("select * from \"s\".\"beatles2\" where \"i\" = 4")
        .explainContains(explain)
        .returnsUnordered("i=4; j=John; k=1940",
            "i=4; j=Paul; k=1942");
    assertThat(buf.toString(), is("returnCount=4"));
  }

  /** A filter on a {@link org.apache.calcite.schema.ProjectableFilterableTable}
   * with two columns (cooperative). */
  @Test public void testProjectableFilterableCooperative() throws Exception {
    final StringBuilder buf = new StringBuilder();
    final Table table = new BeatlesProjectableFilterableTable(buf, true);
    final String explain = "PLAN="
        + "EnumerableInterpreter\n"
        + "  BindableTableScan(table=[[s, beatles]], filters=[[=($0, 4)]], projects=[[1]])";
    CalciteAssert.that()
        .with(newSchema("s", "beatles", table))
        .query("select \"j\" from \"s\".\"beatles\" where \"i\" = 4")
        .explainContains(explain)
        .returnsUnordered("j=John",
            "j=Paul");
    // Only 2 rows came out of the table. If the value is 4, it means that the
    // planner did not pass the filter down.
    assertThat(buf.toString(), is("returnCount=2, filter=4, projects=[1]"));
  }

  @Test public void testProjectableFilterableNonCooperative() throws Exception {
    final StringBuilder buf = new StringBuilder();
    final Table table = new BeatlesProjectableFilterableTable(buf, false);
    final String explain = "PLAN="
        + "EnumerableInterpreter\n"
        + "  BindableTableScan(table=[[s, beatles2]], filters=[[=($0, 4)]], projects=[[1]]";
    CalciteAssert.that()
        .with(newSchema("s", "beatles2", table))
        .query("select \"j\" from \"s\".\"beatles2\" where \"i\" = 4")
        .explainContains(explain)
        .returnsUnordered("j=John",
            "j=Paul");
    assertThat(buf.toString(), is("returnCount=4, projects=[1, 0]"));
  }

  /** A filter on a {@link org.apache.calcite.schema.ProjectableFilterableTable}
   * with two columns, and a project in the query. (Cooperative)*/
  @Test public void testProjectableFilterableWithProjectAndFilter() throws Exception {
    final StringBuilder buf = new StringBuilder();
    final Table table = new BeatlesProjectableFilterableTable(buf, true);
    final String explain = "PLAN="
        + "EnumerableInterpreter\n"
        + "  BindableTableScan(table=[[s, beatles]], filters=[[=($0, 4)]], projects=[[2, 1]]";
    CalciteAssert.that()
        .with(newSchema("s", "beatles", table))
        .query("select \"k\",\"j\" from \"s\".\"beatles\" where \"i\" = 4")
        .explainContains(explain)
        .returnsUnordered("k=1940; j=John",
            "k=1942; j=Paul");
    assertThat(buf.toString(),
        is("returnCount=2, filter=4, projects=[2, 1]"));
  }

  /** A filter on a {@link org.apache.calcite.schema.ProjectableFilterableTable}
   * with two columns, and a project in the query (NonCooperative). */
  @Test public void testProjectableFilterableWithProjectFilterNonCooperative()
      throws Exception {
    final StringBuilder buf = new StringBuilder();
    final Table table = new BeatlesProjectableFilterableTable(buf, false);
    final String explain = "PLAN="
        + "EnumerableInterpreter\n"
        + "  BindableTableScan(table=[[s, beatles]], filters=[[>($2, 1941)]], "
        + "projects=[[0, 2]])";
    CalciteAssert.that()
        .with(newSchema("s", "beatles", table))
        .query("select \"i\",\"k\" from \"s\".\"beatles\" where \"k\" > 1941")
        .explainContains(explain)
        .returnsUnordered("i=4; k=1942",
            "i=6; k=1943");
    assertThat(buf.toString(),
        is("returnCount=4, projects=[0, 2]"));
  }

  /** A filter and project on a
   * {@link org.apache.calcite.schema.ProjectableFilterableTable}. The table
   * refuses to execute the filter, so Calcite should add a pull up and
   * transform the filter (projecting the column needed by the filter). */
  @Test public void testPFTableRefusesFilterCooperative() throws Exception {
    final StringBuilder buf = new StringBuilder();
    final Table table = new BeatlesProjectableFilterableTable(buf, false);
    final String explain = "PLAN=EnumerableInterpreter\n"
        + "  BindableTableScan(table=[[s, beatles2]], filters=[[=($0, 4)]], projects=[[2]])";
    CalciteAssert.that()
        .with(newSchema("s", "beatles2", table))
        .query("select \"k\" from \"s\".\"beatles2\" where \"i\" = 4")
        .explainContains(explain)
        .returnsUnordered("k=1940",
            "k=1942");
    assertThat(buf.toString(),
        is("returnCount=4, projects=[2, 0]"));
  }

  @Test public void testPFPushDownProjectFilterInAggregateNoGroup() {
    final StringBuilder buf = new StringBuilder();
    final Table table = new BeatlesProjectableFilterableTable(buf, false);
    final String explain = "PLAN=EnumerableAggregate(group=[{}], M=[MAX($0)])\n"
        + "  EnumerableInterpreter\n"
        + "    BindableTableScan(table=[[s, beatles]], filters=[[>($0, 1)]], projects=[[2]])";
    CalciteAssert.that()
        .with(newSchema("s", "beatles", table))
        .query("select max(\"k\") as m from \"s\".\"beatles\" where \"i\" > 1")
        .explainContains(explain)
        .returnsUnordered("M=1943");
  }

  @Test public void testPFPushDownProjectFilterAggregateGroup() {
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
        .with(newSchema("s", "beatles", table))
        .query(sql)
        .explainContains(explain)
        .returnsUnordered("i=4; C=2",
            "i=5; C=1",
            "i=6; C=1");
  }

  @Test public void testPFPushDownProjectFilterAggregateNested() {
    final StringBuilder buf = new StringBuilder();
    final String sql = "select \"k\", count(*) as c\n"
        + "from (\n"
        + "  select \"k\", \"i\" from \"s\".\"beatles\" group by \"k\", \"i\") t\n"
        + "where \"k\" = 1940\n"
        + "group by \"k\"";
    final Table table = new BeatlesProjectableFilterableTable(buf, false);
    final String explain = "PLAN="
        + "EnumerableAggregate(group=[{0}], C=[COUNT()])\n"
        + "  EnumerableAggregate(group=[{0, 1}])\n"
        + "    EnumerableInterpreter\n"
        + "      BindableTableScan(table=[[s, beatles]], "
        + "filters=[[=($2, 1940)]], projects=[[2, 0]])";
    CalciteAssert.that()
        .with(newSchema("s", "beatles", table))
        .query(sql)
        .explainContains(explain)
        .returnsUnordered("k=1940; C=2");
  }

  private static Integer getFilter(boolean cooperative, List<RexNode> filters) {
    final Iterator<RexNode> filterIter = filters.iterator();
    while (filterIter.hasNext()) {
      final RexNode node = filterIter.next();
      if (cooperative
          && node instanceof RexCall
          && ((RexCall) node).getOperator() == SqlStdOperatorTable.EQUALS
          && ((RexCall) node).getOperands().get(0) instanceof RexInputRef
          && ((RexInputRef) ((RexCall) node).getOperands().get(0)).getIndex()
          == 0
          && ((RexCall) node).getOperands().get(1) instanceof RexLiteral) {
        final RexNode op1 = ((RexCall) node).getOperands().get(1);
        filterIter.remove();
        return ((BigDecimal) ((RexLiteral) op1).getValue()).intValue();
      }
    }
    return null;
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-458">[CALCITE-458]
   * ArrayIndexOutOfBoundsException when using just a single column in
   * interpreter</a>. */
  @Test public void testPFTableRefusesFilterSingleColumn() throws Exception {
    final StringBuilder buf = new StringBuilder();
    final Table table = new BeatlesProjectableFilterableTable(buf, false);
    final String explain = "PLAN="
        + "EnumerableInterpreter\n"
        + "  BindableTableScan(table=[[s, beatles2]], filters=[[>($2, 1941)]], projects=[[2]])";
    CalciteAssert.that()
        .with(newSchema("s", "beatles2", table))
        .query("select \"k\" from \"s\".\"beatles2\" where \"k\" > 1941")
        .explainContains(explain)
        .returnsUnordered("k=1942",
            "k=1943");
    assertThat(buf.toString(), is("returnCount=4, projects=[2]"));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2039">[CALCITE-2039]
   * AssertionError when pushing project to ProjectableFilterableTable</a>.
   * Cannot push down a project if it is not a permutation of columns; in this
   * case, it contains a literal. */
  @Test public void testCannotPushProject() throws Exception {
    final StringBuilder buf = new StringBuilder();
    final Table table = new BeatlesProjectableFilterableTable(buf, true);
    final String explain = "PLAN="
        + "EnumerableCalc(expr#0..2=[{inputs}], expr#3=[3], k=[$t2], j=[$t1], "
        + "i=[$t0], EXPR$3=[$t3])\n"
        + "  EnumerableInterpreter\n"
        + "    BindableTableScan(table=[[s, beatles]])";
    CalciteAssert.that()
        .with(newSchema("s", "beatles", table))
        .query("select \"k\",\"j\",\"i\",3 from \"s\".\"beatles\"")
        .explainContains(explain)
        .returnsUnordered("k=1940; j=John; i=4; EXPR$3=3",
            "k=1940; j=Ringo; i=5; EXPR$3=3",
            "k=1942; j=Paul; i=4; EXPR$3=3",
            "k=1943; j=George; i=6; EXPR$3=3");
    assertThat(buf.toString(), is("returnCount=4"));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1031">[CALCITE-1031]
   * In prepared statement, CsvScannableTable.scan is called twice</a>. */
  @Test public void testPrepared2() throws SQLException {
    final Properties properties = new Properties();
    properties.setProperty("caseSensitive", "true");
    try (Connection connection =
             DriverManager.getConnection("jdbc:calcite:", properties)) {
      final CalciteConnection calciteConnection = connection.unwrap(
          CalciteConnection.class);

      final AtomicInteger scanCount = new AtomicInteger();
      final AtomicInteger enumerateCount = new AtomicInteger();
      final Schema schema =
          new AbstractSchema() {
            @Override protected Map<String, Table> getTableMap() {
              return ImmutableMap.of("TENS",
                  new SimpleTable() {
                    private Enumerable<Object[]> superScan(DataContext root) {
                      return super.scan(root);
                    }

                    @Override public Enumerable<Object[]>
                    scan(final DataContext root) {
                      scanCount.incrementAndGet();
                      return new AbstractEnumerable<Object[]>() {
                        public Enumerator<Object[]> enumerator() {
                          enumerateCount.incrementAndGet();
                          return superScan(root).enumerator();
                        }
                      };
                    }
                  });
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

  protected ConnectionPostProcessor newSchema(final String schemaName,
      final String tableName, final Table table) {
    return connection -> {
      CalciteConnection con = connection.unwrap(CalciteConnection.class);
      SchemaPlus rootSchema = con.getRootSchema();
      SchemaPlus schema = rootSchema.add(schemaName, new AbstractSchema());
      schema.add(tableName, table);
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

    public Enumerable<Object[]> scan(DataContext root) {
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
          .build();
    }

    public Enumerable<Object[]> scan(DataContext root) {
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

    public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters) {
      final Integer filter = getFilter(cooperative, filters);
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
    private final StringBuilder buf;
    private final boolean cooperative;

    public BeatlesProjectableFilterableTable(StringBuilder buf,
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

    public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters,
        final int[] projects) {
      final Integer filter = getFilter(cooperative, filters);
      return new AbstractEnumerable<Object[]>() {
        public Enumerator<Object[]> enumerator() {
          return beatles(buf, filter, projects);
        }
      };
    }
  }

  private static Enumerator<Object[]> tens() {
    return new Enumerator<Object[]>() {
      int row = -1;
      Object[] current;

      public Object[] current() {
        return current;
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

  private static final Object[][] BEATLES = {
      {4, "John", 1940},
      {4, "Paul", 1942},
      {6, "George", 1943},
      {5, "Ringo", 1940}
  };

  private static Enumerator<Object[]> beatles(final StringBuilder buf,
      final Integer filter, final int[] projects) {
    return new Enumerator<Object[]>() {
      int row = -1;
      int returnCount = 0;
      Object[] current;

      public Object[] current() {
        return current;
      }

      public boolean moveNext() {
        while (++row < 4) {
          Object[] current = BEATLES[row % 4];
          if (filter == null || filter.equals(current[0])) {
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

// End ScannableTableTest.java
