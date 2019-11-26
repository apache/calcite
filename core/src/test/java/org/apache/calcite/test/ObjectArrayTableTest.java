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

import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.EnumerableDefaults;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;

import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


/**
 * Test for querying Java Object array table.
 */
public class ObjectArrayTableTest {

  /**
   * A functional interface to implement a test.
   */
  @FunctionalInterface private interface TestWithConnection {
    void test(Connection connection) throws SQLException;
  }

  /**
   * Execute a test with an in-memory table having java.sql.Date, Time, Timestamp columns.
   * @param test the actual test code
   */
  private void withJavaSqlDateTypes(TestWithConnection test) throws SQLException {
    try (
        Connection connection = DriverManager.getConnection("jdbc:calcite:");
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class)
    ) {
      final SchemaPlus rootSchema = calciteConnection.getRootSchema();

      final JavaTypeFactoryImpl typeFactory =
          new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

      final RelDataType rowType = typeFactory.createStructType(
          Arrays.asList(
              typeFactory.createJavaType(java.sql.Date.class),
              typeFactory.createJavaType(java.sql.Time.class),
              typeFactory.createJavaType(java.sql.Timestamp.class)
          ),
          Arrays.asList("dt", "tm", "ts")
      );

      final Enumerable<Object[]> enumerable =
          Linq4j.asEnumerable(
              Collections.singletonList(new Object[]{
                  java.sql.Date.valueOf("2018-12-14"),
                  java.sql.Time.valueOf("18:29:34"),
                  java.sql.Timestamp.valueOf("2018-12-14 18:29:34.123")}));

      final Table table = new AbstractQueryableTable(Object[].class) {
        @SuppressWarnings({"unchecked", "rawtypes"})
        @Override public Queryable<Object[]> asQueryable(QueryProvider queryProvider,
                                                         SchemaPlus schema, String tableName) {
          return EnumerableDefaults.asOrderedQueryable(enumerable);
        }

        @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
          return rowType;
        }
      };

      rootSchema.add("java_sql_date_types", table);

      test.test(connection);
    }
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1703">[CALCITE-1703]
   * Before the fix, functions on TIME and TIMESTAMP column can throw ClassCastException</a>. */
  @Test public void testJavaSqlDateColumnReference() throws SQLException {
    withJavaSqlDateTypes(connection -> {
      // 'extract' function expects numeric date representation.
      // RexToLixTranslator.convert() handles Java type to numeric storage type conversion.
      // Storage types are defined at EnumUtils.toInternal.
      final Statement statement = connection.createStatement();
      String sql = "select extract(year from \"dt\"),"
          + " extract(hour from \"tm\"),"
          + " extract(day from \"ts\")"
          + " from \"java_sql_date_types\"";

      ResultSet resultSet = statement.executeQuery(sql);
      assertTrue(resultSet.next());
      assertEquals("extract(year from java.sql.Date.valueOf(\"2018-12-14\"))",
          2018, resultSet.getInt(1));
      assertEquals("extract(hour from java.sql.Time.valueOf(\"18:29:34\"))",
          18, resultSet.getInt(2));
      assertEquals("extract(day from"
              + " java.sql.Timestamp.valueOf(\"2018-12-14 18:29:34.123\"))",
          14, resultSet.getInt(3));
    });
  }

  /**
   * Test to confirm java.sql.Time column can be used by a filter condition.
   */
  @Test public void testTimeFilter() throws SQLException {
    withJavaSqlDateTypes(connection -> {
      String sql = "select 'result'"
          + " from \"java_sql_date_types\"";

      Statement statement = connection.createStatement();
      ResultSet resultSet = statement.executeQuery(sql + " where \"tm\" > TIME '18:29:33'");
      assertTrue("java.sql.Time.valueOf(\"18:29:34\") > TIME '18:29:33'", resultSet.next());

      statement = connection.createStatement();
      resultSet = statement.executeQuery(sql + " where \"tm\" = TIME '18:29:34'");
      assertTrue("java.sql.Time.valueOf(\"18:29:34\") = TIME '18:29:34'", resultSet.next());

      statement = connection.createStatement();
      resultSet = statement.executeQuery(sql + " where \"tm\" > TIME '18:29:34'");
      assertFalse("java.sql.Time.valueOf(\"18:29:34\") > TIME '18:29:34'", resultSet.next());

      statement = connection.createStatement();
      resultSet = statement.executeQuery(sql
          + " where \"tm\" between TIME '18:29:34' and TIME '18:29:35'");
      assertTrue("java.sql.Time.valueOf(\"18:29:34\")"
          + " between TIME '18:29:34' and  TIME '18:29:35'", resultSet.next());
    });
  }

  /**
   * Test to confirm java.sql.Date column can be used by a filter condition.
   */
  @Test public void testDateFilter() throws SQLException {
    withJavaSqlDateTypes(connection -> {
      String sql = "select 'result'"
          + " from \"java_sql_date_types\"";

      Statement statement = connection.createStatement();
      ResultSet resultSet = statement.executeQuery(sql
          + " where \"dt\" > DATE '2018-12-13'");
      assertTrue("java.sql.Date.valueOf(\"2018-12-14\") > DATE '2018-12-13'", resultSet.next());

      statement = connection.createStatement();
      resultSet = statement.executeQuery(sql + " where \"dt\" = DATE '2018-12-14'");
      assertTrue("java.sql.Date.valueOf(\"2018-12-14\") = DATE '2018-12-14'", resultSet.next());

      statement = connection.createStatement();
      resultSet = statement.executeQuery(sql + " where \"dt\" > DATE '2018-12-14'");
      assertFalse("java.sql.Date.valueOf(\"2018-12-14\") > DATE '2018-12-14'", resultSet.next());

      statement = connection.createStatement();
      resultSet = statement.executeQuery(sql
          + " where \"dt\" between DATE '2018-12-14' and DATE '2018-12-15'");
      assertTrue("java.sql.Date.valueOf(\"2018-12-14\")"
          + " between DATE '2018-12-14' and DATE '2018-12-14'", resultSet.next());
    });
  }

  /**
   * Test to confirm java.sql.Timestamp column can be used by a filter condition.
   */
  @Test public void testTimestampFilter() throws SQLException {
    withJavaSqlDateTypes(connection -> {
      String sql = "select 'result'"
          + " from \"java_sql_date_types\"";

      Statement statement = connection.createStatement();
      ResultSet resultSet = statement.executeQuery(sql
          + " where \"ts\" > TIMESTAMP '2018-12-14 18:29:34.122'");
      assertTrue("java.sql.Timestamp.valueOf(\"2018-12-14 18:29:34.123\")"
          + " > TIMESTAMP '2018-12-14 18:29:34.122'", resultSet.next());


      statement = connection.createStatement();
      resultSet = statement.executeQuery(sql
          + " where \"ts\" = TIMESTAMP '2018-12-14 18:29:34.123'");
      assertTrue("java.sql.Timestamp.valueOf(\"2018-12-14 18:29:34.123\")"
          + " = TIMESTAMP '2018-12-14 18:29:34.123'", resultSet.next());


      statement = connection.createStatement();
      resultSet = statement.executeQuery(sql
          + " where \"ts\" > TIMESTAMP '2018-12-14 18:29:34.123'");
      assertFalse("java.sql.Timestamp.valueOf(\"2018-12-14 18:29:34.123\")"
          + " > TIMESTAMP '2018-12-14 18:29:34.123'", resultSet.next());

      statement = connection.createStatement();
      resultSet = statement.executeQuery(sql
          + " where \"ts\" between TIMESTAMP '2018-12-14 18:29:34.122'"
          + " and TIMESTAMP '2018-12-14 18:29:34.123'");
      assertTrue("java.sql.Timestamp.valueOf(\"2018-12-14 18:29:34.123\")"
          + " between TIMESTAMP '2018-12-14 18:29:34.122'"
          + " and TIMESTAMP '2018-12-14 18:29:34.123'", resultSet.next());
    });
  }

  /**
   * Test to confirm java.sql.Timestamp can be casted to Timestamp with different precision.
   * The cast should be done the same as with TIMESTAMP literal.
   */
  @Test public void testCastTimestampToTimestamp() throws SQLException {
    withJavaSqlDateTypes(connection -> {
      Statement statement = connection.createStatement();
      final Timestamp[] expectedTimestamps = {
          // precision = 0
          Timestamp.valueOf("2018-12-14 18:29:34"),
          // precision = 1
          Timestamp.valueOf("2018-12-14 18:29:34.1"),
          // precision = 2
          Timestamp.valueOf("2018-12-14 18:29:34.12"),
          // precision = 3
          Timestamp.valueOf("2018-12-14 18:29:34.123")
      };
      for (int i = 3; i >= 0; i--) {
        final String castTo = "TIMESTAMP(" + i + ")";
        final String sql = "select cast(\"ts\" as " + castTo + ") from \"java_sql_date_types\"";
        final ResultSet resultSet = statement.executeQuery(sql);
        resultSet.next();
        assertEquals("cast(java.sql.Timestamp.valueOf(\"2018-12-14 18:29:34.123\")"
                + " as " + castTo + ")",
            expectedTimestamps[i], resultSet.getTimestamp(1));
      }
    });
  }

}

// End ObjectArrayTableTest.java
