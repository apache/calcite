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
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.type.SqlTypeName;

import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test cases for
 * <a href="https://issues.apache.org/jira/browse/CALCITE-1386">[CALCITE-1386]
 * ITEM operator seems to ignore the value type of collection and assign the value to Object</a>.
 */
public class CollectionTypeTest {
  @Test public void testAccessNestedMap() throws Exception {
    Connection connection = setupConnectionAwaringNestedTable();

    final Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery(
        "select \"ID\", \"MAPFIELD\"['c'] AS \"MAPFIELD_C\", \"NESTEDMAPFIELD\", \"ARRAYFIELD\" "
        + "from \"s\".\"nested\" "
        + "where \"NESTEDMAPFIELD\"['a']['b'] = 2 AND \"ARRAYFIELD\"[2] = 200");

    List<String> resultStrings = new ArrayList<>();
    CalciteAssert.toStringList(resultSet, resultStrings);

    assertEquals(1, resultStrings.size());

    // JDBC doesn't support Map / Nested Map so just relying on string representation
    String expectedRow = "ID=2; MAPFIELD_C=4; NESTEDMAPFIELD={a={b=2, c=4}}; "
        + "ARRAYFIELD=[100, 200, 300]";
    assertEquals(expectedRow, resultStrings.get(0));
  }

  @Test public void testAccessNonExistKeyFromMap() throws Exception {
    Connection connection = setupConnectionAwaringNestedTable();

    final Statement statement = connection.createStatement();

    // this shouldn't throw any Exceptions on runtime, just don't return any rows.
    ResultSet resultSet = statement.executeQuery(
        "select \"ID\", \"MAPFIELD\", \"NESTEDMAPFIELD\", \"ARRAYFIELD\" "
        + "from \"s\".\"nested\" "
        + "where \"MAPFIELD\"['a'] = 2"
    );

    List<String> resultStrings = new ArrayList<>();
    CalciteAssert.toStringList(resultSet, resultStrings);

    assertEquals(0, resultStrings.size());
  }

  @Test public void testAccessNonExistKeyFromNestedMap() throws Exception {
    Connection connection = setupConnectionAwaringNestedTable();

    final Statement statement = connection.createStatement();

    // this shouldn't throw any Exceptions on runtime, just don't return any rows.
    ResultSet resultSet = statement.executeQuery(
        "select \"ID\", \"MAPFIELD\", \"NESTEDMAPFIELD\", \"ARRAYFIELD\" "
        + "from \"s\".\"nested\" "
        + "where \"NESTEDMAPFIELD\"['b']['c'] = 4"
    );

    List<String> resultStrings = new ArrayList<>();
    CalciteAssert.toStringList(resultSet, resultStrings);

    assertEquals(0, resultStrings.size());
  }

  @Test
  public void testInvalidAccessUseStringForIndexOnArray() throws Exception {
    Connection connection = setupConnectionAwaringNestedTable();

    final Statement statement = connection.createStatement();

    try {
      statement.executeQuery("select \"ID\", \"MAPFIELD\", \"NESTEDMAPFIELD\", \"ARRAYFIELD\" "
          + "from \"s\".\"nested\" "
          + "where \"ARRAYFIELD\"['a'] = 200");

      fail("This query shouldn't be evaluated properly");
    } catch (SQLException e) {
      Throwable e2 = e.getCause();
      assertTrue(e2 instanceof CalciteContextException);
    }
  }

  @Test
  public void testNestedArrayOutOfBoundAccess() throws Exception {
    Connection connection = setupConnectionAwaringNestedTable();

    final Statement statement = connection.createStatement();

    ResultSet resultSet = statement.executeQuery(
        "select \"ID\", \"MAPFIELD\", \"NESTEDMAPFIELD\", \"ARRAYFIELD\" "
        + "from \"s\".\"nested\" "
        + "where \"ARRAYFIELD\"[10] = 200");

    List<String> resultStrings = new ArrayList<>();
    CalciteAssert.toStringList(resultSet, resultStrings);

    // this is against SQL standard definition...
    // SQL standard states that data exception should be occurred
    // when accessing array with out of bound index.
    // but PostgreSQL breaks it, and this is more convenient since it guarantees runtime safety.
    assertEquals(0, resultStrings.size());
  }

  @Test public void testAccessNestedMapWithANYType() throws Exception {
    Connection connection = setupConnectionAwaringNestedANYTypeTable();

    final Statement statement = connection.createStatement();

    ResultSet resultSet = statement.executeQuery(
        "select \"ID\", \"MAPFIELD\"['c'] AS \"MAPFIELD_C\", \"NESTEDMAPFIELD\", \"ARRAYFIELD\" "
        + "from \"s\".\"nested\" "
        + "where CAST(\"NESTEDMAPFIELD\"['a']['b'] AS INTEGER) = 2 AND CAST(\"ARRAYFIELD\"[2] AS INTEGER) = 200");

    List<String> resultStrings = new ArrayList<>();
    CalciteAssert.toStringList(resultSet, resultStrings);

    assertEquals(1, resultStrings.size());

    // JDBC doesn't support Map / Nested Map so just relying on string representation
    String expectedRow = "ID=2; MAPFIELD_C=4; NESTEDMAPFIELD={a={b=2, c=4}}; "
        + "ARRAYFIELD=[100, 200, 300]";
    assertEquals(expectedRow, resultStrings.get(0));
  }

  @Test public void testAccessNestedMapWithANYTypeWithoutCAST() throws Exception {
    Connection connection = setupConnectionAwaringNestedANYTypeTable();

    final Statement statement = connection.createStatement();

    // Since the value type is ANY, we need to wrap the value to CAST in order to
    // compare with literal. if it doesn't, Exception is thrown at Runtime.
    // This is only occurred with primitive type because of providing overloaded methods
    try {
      statement.executeQuery(
          "select \"ID\", \"MAPFIELD\"['c'] AS \"MAPFIELD_C\", \"NESTEDMAPFIELD\", \"ARRAYFIELD\" "
              + "from \"s\".\"nested\" "
              + "where \"NESTEDMAPFIELD\"['a']['b'] = 2 AND \"ARRAYFIELD\"[2] = 200");

      fail("Without CAST, comparing result of ITEM() and primitive type should throw Exception "
          + "in Runtime");
    } catch (SQLException e) {
      Throwable e2 = e.getCause();
      assertTrue(e2 instanceof RuntimeException);
      Throwable e3 = e2.getCause();
      assertTrue(e3 instanceof NoSuchMethodException);
    }
  }

  @Test public void testAccessNonExistKeyFromMapWithANYType() throws Exception {
    Connection connection = setupConnectionAwaringNestedTable();

    final Statement statement = connection.createStatement();

    // this shouldn't throw any Exceptions on runtime, just don't return any rows.
    ResultSet resultSet = statement.executeQuery(
        "select \"ID\", \"MAPFIELD\", \"NESTEDMAPFIELD\", \"ARRAYFIELD\" "
        + "from \"s\".\"nested\" "
        + "where CAST(\"MAPFIELD\"['a'] AS INTEGER) = 2"
    );

    List<String> resultStrings = new ArrayList<>();
    CalciteAssert.toStringList(resultSet, resultStrings);

    assertEquals(0, resultStrings.size());
  }

  @Test public void testAccessNonExistKeyFromNestedMapWithANYType() throws Exception {
    Connection connection = setupConnectionAwaringNestedTable();

    final Statement statement = connection.createStatement();

    // this shouldn't throw any Exceptions on runtime, just don't return any rows.
    ResultSet resultSet = statement.executeQuery(
        "select \"ID\", \"MAPFIELD\", \"NESTEDMAPFIELD\", \"ARRAYFIELD\" "
         + "from \"s\".\"nested\" "
         + "where CAST(\"NESTEDMAPFIELD\"['b']['c'] AS INTEGER) = 4"
    );

    List<String> resultStrings = new ArrayList<>();
    CalciteAssert.toStringList(resultSet, resultStrings);

    assertEquals(0, resultStrings.size());
  }

  @Test
  public void testInvalidAccessUseStringForIndexOnArrayWithANYType() throws Exception {
    Connection connection = setupConnectionAwaringNestedTable();

    final Statement statement = connection.createStatement();

    try {
      statement.executeQuery("select \"ID\", \"MAPFIELD\", \"NESTEDMAPFIELD\", \"ARRAYFIELD\" "
          + "from \"s\".\"nested\" "
          + "where CAST(\"ARRAYFIELD\"['a'] AS INTEGER) = 200");

      fail("This query shouldn't be evaluated properly");
    } catch (SQLException e) {
      Throwable e2 = e.getCause();
      assertTrue(e2 instanceof CalciteContextException);
    }
  }

  @Test
  public void testNestedArrayOutOfBoundAccessWithANYType() throws Exception {
    Connection connection = setupConnectionAwaringNestedTable();

    final Statement statement = connection.createStatement();

    ResultSet resultSet = statement.executeQuery(
        "select \"ID\", \"MAPFIELD\", \"NESTEDMAPFIELD\", \"ARRAYFIELD\" "
        + "from \"s\".\"nested\" "
        + "where CAST(\"ARRAYFIELD\"[10] AS INTEGER) = 200");

    List<String> resultStrings = new ArrayList<>();
    CalciteAssert.toStringList(resultSet, resultStrings);

    // this is against SQL standard definition...
    // SQL standard states that data exception should be occurred
    // when accessing array with out of bound index.
    // but PostgreSQL breaks it, and this is more convenient since it guarantees runtime safety.
    assertEquals(0, resultStrings.size());
  }

  private Connection setupConnectionAwaringNestedTable() throws SQLException {
    Connection connection =
        DriverManager.getConnection("jdbc:calcite:");
    CalciteConnection calciteConnection =
        connection.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();
    SchemaPlus schema = rootSchema.add("s", new AbstractSchema());
    schema.add("nested", new NestedCollectionTable());
    return connection;
  }

  private Connection setupConnectionAwaringNestedANYTypeTable() throws SQLException {
    Connection connection =
        DriverManager.getConnection("jdbc:calcite:");
    CalciteConnection calciteConnection =
        connection.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();
    SchemaPlus schema = rootSchema.add("s", new AbstractSchema());
    schema.add("nested", new NestedCollectionWithANYTypeTable());
    return connection;
  }

  public static Enumerator<Object[]> nestedRecordsEnumerator() {
    final Object[][] records = setupNestedRecords();

    return new Enumerator<Object[]>() {
      int row = -1;
      int returnCount = 0;
      Object[] current;

      @Override public Object[] current() {
        return current;
      }

      @Override public boolean moveNext() {
        while (++row < 5) {
          this.current = records[row];
          ++returnCount;
          return true;
        }
        return false;
      }

      @Override public void reset() {
        row = -1;
      }

      @Override public void close() {
        current = null;
      }
    };
  }

  private static Object[][] setupNestedRecords() {
    List<Integer> ints = Arrays.asList(100, 200, 300);

    Object[][] records = new Object[5][];

    for (int i = 0; i < 5; ++i) {
      Map<String, Integer> map = new HashMap<>();
      map.put("b", i);
      map.put("c", i * i);
      Map<String, Map<String, Integer>> mm = new HashMap<>();
      mm.put("a", map);
      records[i] = new Object[] {i, map, mm, ints};
    }

    return records;
  }

  /** Table that returns columns which include complicated collection type via the ScannableTable
   * interface. */
  public static class NestedCollectionTable implements ScannableTable {
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {

      RelDataType nullableKeyType = typeFactory
          .createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true);
      RelDataType nullableValueType = typeFactory
          .createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true);
      RelDataType nullableMapType = typeFactory
          .createTypeWithNullability(typeFactory.createMapType(nullableKeyType, nullableValueType),
              true);
      return typeFactory.builder()
          .add("ID", SqlTypeName.INTEGER)
          .add("MAPFIELD",
              typeFactory.createTypeWithNullability(
                typeFactory.createMapType(nullableKeyType, nullableValueType), true))
          .add("NESTEDMAPFIELD", typeFactory
              .createTypeWithNullability(
                  typeFactory.createMapType(nullableKeyType, nullableMapType), true))
          .add("ARRAYFIELD", typeFactory
              .createTypeWithNullability(
                  typeFactory.createArrayType(nullableValueType, -1L), true))
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
          return nestedRecordsEnumerator();
        }
      };
    }
  }

  /** Table that returns columns which include complicated collection type via the ScannableTable
   * interface. */
  public static class NestedCollectionWithANYTypeTable implements ScannableTable {
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          .add("ID", SqlTypeName.INTEGER)
          .add("MAPFIELD", SqlTypeName.ANY)
          .add("NESTEDMAPFIELD", SqlTypeName.ANY)
          .add("ARRAYFIELD", SqlTypeName.ANY)
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
          return nestedRecordsEnumerator();
        }
      };
    }
  }
}

// End CollectionTypeTest.java
