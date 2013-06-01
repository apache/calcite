/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package net.hydromatic.optiq.test;

import net.hydromatic.linq4j.function.Function1;

import junit.framework.TestCase;

import java.io.PrintStream;
import java.sql.*;
import java.util.Properties;

/**
 * Unit test of the Optiq adapter for CSV.
 */
public class CsvTest extends TestCase {
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

  /**
   * Tests the vanity driver.
   */
  public void _testVanityDriver() throws SQLException {
    Properties info = new Properties();
    Connection connection =
        DriverManager.getConnection("jdbc:csv:", info);
    connection.close();
  }

  /**
   * Tests the vanity driver with properties in the URL.
   */
  public void _testVanityDriverArgsInUrl() throws SQLException {
    Connection connection =
        DriverManager.getConnection(
            "jdbc:csv:"
            + "directory='foo'");
    connection.close();
  }

  /**
   * Reads from a table.
   */
  public void testSelect() throws SQLException {
    checkSql("model", "select * from EMPS");
  }

  public void testCustomTable() throws SQLException {
    checkSql("model-with-custom-table", "select * from CUSTOM_TABLE.EMPS");
  }

  public void testPushDownProjectDumb() throws SQLException {
    // rule does not fire, because we're using 'dumb' tables in simple model
    checkSql("model", "explain plan for select * from EMPS",
        "PLAN=EnumerableCalcRel(expr#0..9=[{inputs}], proj#0..9=[{exprs}])\n"
        + "  EnumerableTableAccessRel(table=[[SALES, EMPS]])\n"
        + "\n");
  }

  public void testPushDownProject() throws SQLException {
    checkSql("smart", "explain plan for select * from EMPS",
        "PLAN=CsvTableScan(table=[[SALES, EMPS]], fields=[[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]])\n"
        + "\n");
  }

  public void testPushDownProject2() throws SQLException {
    checkSql("smart", "explain plan for select name, empno from EMPS",
        "PLAN=CsvTableScan(table=[[SALES, EMPS]], fields=[[1, 0]])\n"
        + "\n");
    // make sure that it works...
    checkSql("smart", "select name, empno from EMPS",
        "NAME=Fred; EMPNO=100\n"
        + "NAME=Eric; EMPNO=110\n"
        + "NAME=John; EMPNO=110\n"
        + "NAME=Wilma; EMPNO=120\n"
        + "NAME=Alice; EMPNO=130\n");
  }

  private void checkSql(String model, String sql) throws SQLException {
    checkSql(sql, model, new Function1<ResultSet, Void>() {
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
    checkSql(sql, model, new Function1<ResultSet, Void>() {
      public Void apply(ResultSet resultSet) {
        try {
          String actual = CsvTest.toString(resultSet);
          assertEquals(expected, actual);
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
        return null;
      }
    });
  }

  private void checkSql(String sql, String model, Function1<ResultSet, Void> fn)
      throws SQLException {
    Connection connection = null;
    Statement statement = null;
    try {
      Properties info = new Properties();
      info.put("model", "target/test-classes/" + model + ".json");
      connection = DriverManager.getConnection("jdbc:optiq:", info);
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
}

// End CsvTest.java
