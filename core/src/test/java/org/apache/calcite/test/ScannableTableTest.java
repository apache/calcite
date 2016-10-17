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
import org.apache.calcite.adapter.java.ReflectiveSchema;
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
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;

import com.google.common.collect.ImmutableMap;

import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
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
    Connection connection =
        DriverManager.getConnection("jdbc:calcite:");
    CalciteConnection calciteConnection =
        connection.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();
    SchemaPlus schema = rootSchema.add("s", new AbstractSchema());
    schema.add("simple", new SimpleTable());
    rootSchema.add("hr", new ReflectiveSchema(new JdbcTest.HrSchema()));
    ResultSet resultSet = connection.createStatement().executeQuery(
        "select * from \"s\".\"simple\"");
    assertThat(CalciteAssert.toString(resultSet),
        equalTo("i=0\ni=10\ni=20\ni=30\n"));
  }

  /** A table with two columns. */
  @Test public void testSimple2() throws Exception {
    Connection connection =
        DriverManager.getConnection("jdbc:calcite:");
    CalciteConnection calciteConnection =
        connection.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();
    SchemaPlus schema = rootSchema.add("s", new AbstractSchema());
    schema.add("beatles", new BeatlesTable());
    rootSchema.add("hr", new ReflectiveSchema(new JdbcTest.HrSchema()));
    ResultSet resultSet = connection.createStatement().executeQuery(
        "select * from \"s\".\"beatles\"");
    assertThat(CalciteAssert.toString(resultSet),
        equalTo("i=4; j=John\ni=4; j=Paul\ni=6; j=George\ni=5; j=Ringo\n"));
  }

  /** A filter on a {@link FilterableTable} with two columns. */
  @Test public void testSimpleFilter2() throws Exception {
    Connection connection =
        DriverManager.getConnection("jdbc:calcite:");
    CalciteConnection calciteConnection =
        connection.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();
    SchemaPlus schema = rootSchema.add("s", new AbstractSchema());
    final StringBuilder buf = new StringBuilder();
    schema.add("beatles", new BeatlesFilterableTable(buf, true));
    final Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery(
        "select * from \"s\".\"beatles\" where \"i\" = 4");
    assertThat(CalciteAssert.toString(resultSet),
        equalTo("i=4; j=John; k=1940\ni=4; j=Paul; k=1942\n"));
    resultSet.close();
    // Only 2 rows came out of the table. If the value is 4, it means that the
    // planner did not pass the filter down.
    assertThat(buf.toString(), equalTo("returnCount=2, filter=4"));
    buf.setLength(0);

    // Now with an "uncooperative" filterable table that refuses to accept
    // filters.
    schema.add("beatles2", new BeatlesFilterableTable(buf, false));
    resultSet = statement.executeQuery(
        "select * from \"s\".\"beatles2\" where \"i\" = 4");
    assertThat(CalciteAssert.toString(resultSet),
        equalTo("i=4; j=John; k=1940\ni=4; j=Paul; k=1942\n"));
    resultSet.close();
    assertThat(buf.toString(), equalTo("returnCount=4"));
    buf.setLength(0);
  }

  /** A filter on a {@link org.apache.calcite.schema.ProjectableFilterableTable}
   * with two columns. */
  @Test public void testProjectableFilterable2() throws Exception {
    Connection connection =
        DriverManager.getConnection("jdbc:calcite:");
    CalciteConnection calciteConnection =
        connection.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();
    SchemaPlus schema = rootSchema.add("s", new AbstractSchema());
    final StringBuilder buf = new StringBuilder();
    schema.add("beatles", new BeatlesProjectableFilterableTable(buf, true));
    final Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery(
        "select * from \"s\".\"beatles\" where \"i\" = 4");
    assertThat(CalciteAssert.toString(resultSet),
        equalTo("i=4; j=John; k=1940\ni=4; j=Paul; k=1942\n"));
    resultSet.close();
    // Only 2 rows came out of the table. If the value is 4, it means that the
    // planner did not pass the filter down.
    assertThat(buf.toString(), equalTo("returnCount=2, filter=4"));
    buf.setLength(0);

    // Now with an "uncooperative" filterable table that refuses to accept
    // filters.
    schema.add("beatles2", new BeatlesProjectableFilterableTable(buf, false));
    resultSet = statement.executeQuery(
        "select * from \"s\".\"beatles2\" where \"i\" = 4");
    assertThat(CalciteAssert.toString(resultSet),
        equalTo("i=4; j=John; k=1940\ni=4; j=Paul; k=1942\n"));
    resultSet.close();
    assertThat(buf.toString(), equalTo("returnCount=4"));
    buf.setLength(0);
  }

  /** A filter on a {@link org.apache.calcite.schema.ProjectableFilterableTable}
   * with two columns, and a project in the query. */
  @Test public void testProjectableFilterable2WithProject() throws Exception {
    Connection connection =
        DriverManager.getConnection("jdbc:calcite:");
    CalciteConnection calciteConnection =
        connection.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();
    SchemaPlus schema = rootSchema.add("s", new AbstractSchema());
    final StringBuilder buf = new StringBuilder();
    schema.add("beatles", new BeatlesProjectableFilterableTable(buf, true));

    // Now with a project.
    final Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery(
        "select \"k\",\"j\" from \"s\".\"beatles\" where \"i\" = 4");
    assertThat(CalciteAssert.toString(resultSet),
        equalTo("k=1940; j=John\nk=1942; j=Paul\n"));
    resultSet.close();
    assertThat(buf.toString(),
        equalTo("returnCount=2, filter=4, projects=[2, 1]"));
    buf.setLength(0);

    // Filter on one of the projected columns.
    resultSet = statement.executeQuery(
        "select \"i\",\"k\" from\n"
            + "\"s\".\"beatles\" where \"k\" > 1941");
    assertThat(CalciteAssert.toString(resultSet),
        equalTo("i=4; k=1942\n"
            + "i=6; k=1943\n"));
    resultSet.close();
    assertThat(buf.toString(),
        equalTo("returnCount=4, projects=[0, 2]"));
    buf.setLength(0);
  }

  /** A filter and project on a
   * {@link org.apache.calcite.schema.ProjectableFilterableTable}. The table
   * refuses to execute the filter, so Calcite should add a pull up and
   * transform the filter (projecting the column needed by the filter). */
  @Test public void testPFTableRefusesFilter() throws Exception {
    Connection connection =
        DriverManager.getConnection("jdbc:calcite:");
    CalciteConnection calciteConnection =
        connection.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();
    SchemaPlus schema = rootSchema.add("s", new AbstractSchema());
    final StringBuilder buf = new StringBuilder();
    schema.add("beatles2", new BeatlesProjectableFilterableTable(buf, false));

    // Now with an "uncooperative" filterable table that refuses to accept
    // filters.
    final Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery(
        "select \"k\" from \"s\".\"beatles2\" where \"i\" = 4");
    assertThat(CalciteAssert.toString(resultSet), equalTo("k=1940\nk=1942\n"));
    resultSet.close();
    assertThat(buf.toString(),
        equalTo("returnCount=4, projects=[2, 0]"));
    buf.setLength(0);
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
    Connection connection =
        DriverManager.getConnection("jdbc:calcite:");
    CalciteConnection calciteConnection =
        connection.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();
    SchemaPlus schema = rootSchema.add("s", new AbstractSchema());
    final StringBuilder buf = new StringBuilder();
    schema.add("beatles2", new BeatlesProjectableFilterableTable(buf, false));

    // Now with an "uncooperative" filterable table that refuses to accept
    // filters.
    final Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery(
        "select \"k\" from \"s\".\"beatles2\" where \"k\" > 1941");
    assertThat(CalciteAssert.toString(resultSet), equalTo("k=1942\nk=1943\n"));
    // have to iterate (CalciteAssert.toString) and then close the result set b/c it is backed by
    // an enumerable that only populates the info buffer (buf) on close
    resultSet.close();
    assertThat(buf.toString(),
      equalTo("returnCount=4, projects=[2]"));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1031">[CALCITE-1031]
   * In prepared statement, CsvScannableTable.scan is called twice</a>. */
  @Test public void testPrepared2() throws SQLException {
    final Properties properties = new Properties();
    properties.setProperty("caseSensitive", "true");
    try (final Connection connection =
             DriverManager.getConnection("jdbc:calcite:", properties)) {
      final CalciteConnection calciteConnection = connection.unwrap(
          CalciteConnection.class);

      final AtomicInteger scanCount = new AtomicInteger();
      final AtomicInteger enumerateCount = new AtomicInteger();
      final Schema schema =
          new AbstractSchema() {
            @Override protected Map<String, Table> getTableMap() {
              return ImmutableMap.<String, Table>of("TENS",
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

  /** Table that returns one column via the {@link ScannableTable} interface. */
  public static class SimpleTable implements ScannableTable {
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder().add("i", SqlTypeName.INTEGER).build();
    }

    public Statistic getStatistic() {
      return Statistics.UNKNOWN;
    }

    public Schema.TableType getJdbcTableType() {
      return Schema.TableType.TABLE;
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
  public static class BeatlesTable implements ScannableTable {
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          .add("i", SqlTypeName.INTEGER)
          .add("j", SqlTypeName.VARCHAR)
          .build();
    }

    public Statistic getStatistic() {
      return Statistics.UNKNOWN;
    }

    public Schema.TableType getJdbcTableType() {
      return Schema.TableType.TABLE;
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
