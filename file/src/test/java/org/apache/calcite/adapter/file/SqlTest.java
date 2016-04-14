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
package org.apache.calcite.adapter.file;

import com.google.common.base.Function;

import org.junit.Assume;
import org.junit.Test;

import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

/**
 * System test of the Calcite file adapter, which can also read and parse
 * HTML tables over HTTP.
 */
public class SqlTest {
  // helper functions

  private void checkSql(String model, String sql) throws SQLException {
    checkSql(sql, model, new Function<ResultSet, Void>() {
      public Void apply(ResultSet resultSet) {
        try {
          output(resultSet, System.out);
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
        return null;
      }
    });
  }

  private void checkSql(String model, String sql, final String expected)
      throws SQLException {
    checkSql(sql, model, new Function<ResultSet, Void>() {
      public Void apply(ResultSet resultSet) {
        try {
          String actual = SqlTest.toString(resultSet);
          if (!expected.equals(actual)) {
            System.out.println("Assertion failure:");
            System.out.println("\tExpected: '" + expected + "'");
            System.out.println("\tActual: '" + actual + "'");
          }
          assertEquals(expected, actual);
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
        return null;
      }
    });
  }

  private void checkSql(String sql, String model, Function<ResultSet, Void> fn)
      throws SQLException {
    Connection connection = null;
    Statement statement = null;
    try {
      Properties info = new Properties();
      info.put("model", "target/test-classes/" + model + ".json");
      connection = DriverManager.getConnection("jdbc:calcite:", info);
      statement = connection.createStatement();
      final ResultSet resultSet =
          statement.executeQuery(
              sql);
      fn.apply(resultSet);
    } finally {
      close(connection, statement);
    }
  }

  private static String toString(ResultSet resultSet) throws SQLException {
    StringBuilder buf = new StringBuilder();
    while (resultSet.next()) {
      int n = resultSet.getMetaData().getColumnCount();
      String sep = "";
      for (int i = 1; i <= n; i++) {
        buf.append(sep)
            .append(resultSet.getMetaData().getColumnLabel(i))
            .append("=")
            .append(resultSet.getObject(i));
        sep = "; ";
      }
      buf.append("\n");
    }
    return buf.toString();
  }

  private void output(ResultSet resultSet, PrintStream out)
      throws SQLException {
    final ResultSetMetaData metaData = resultSet.getMetaData();
    final int columnCount = metaData.getColumnCount();
    while (resultSet.next()) {
      for (int i = 1;; i++) {
        out.print(resultSet.getString(i));
        if (i < columnCount) {
          out.print(", ");
        } else {
          out.println();
          break;
        }
      }
    }
  }

  private void close(Connection connection, Statement statement) {
    if (statement != null) {
      try {
        statement.close();
      } catch (SQLException e) {
        // ignore
      }
    }
    if (connection != null) {
      try {
        connection.close();
      } catch (SQLException e) {
        // ignore
      }
    }
  }

  // tests

  /**
   * Reads from a local file and checks the result
   */
  @Test
  public void testFileSelect() throws SQLException {
    checkSql("testModel", "select H1 from T1 where H0 = 'R1C0'", "H1=R1C1\n");
  }

  /**
   * Reads from a local file without table headers <TH> and checks the result
   */
  @Test
  public void testNoTHSelect() throws SQLException {
    Assume.assumeTrue(FileSuite.hazNetwork());
    checkSql("testModel",
        "select \"col1\" from T1_NO_TH where \"col0\" like 'R0%'",
        "col1=R0C1\n");
  }

  /**
   * Reads from a local file - finds larger table even without <TH> elements
   */
  @Test
  public void testFindBiggerNoTH() throws SQLException {
    checkSql("testModel",
        "select \"col4\" from TABLEX2 where \"col0\" like 'R1%'",
        "col4=R1C4\n");
  }

  /**
   * Reads from a URL and checks the result
   */
  @Test
  public void testURLSelect() throws SQLException {
    Assume.assumeTrue(FileSuite.hazNetwork());
    final String sql = "select \"State\", \"Statehood\" from \"States\"\n"
        + "where \"State\" = 'California'";
    checkSql("wiki", sql,
        "State=California; Statehood=1850-09-09\n");
  }

}

// End SqlTest.java
