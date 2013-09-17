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

import org.junit.Test;

import java.io.PrintStream;
import java.sql.*;
import java.util.Properties;

/**
 * Unit test of the Optiq adapter for Splunk.
 */
public class SplunkAdapterTest {
  public static final String SPLUNK_URL = "https://localhost:8089";
  public static final String SPLUNK_USER = "admin";
  public static final String SPLUNK_PASSWORD = "changeme";

  /** Whether this test is enabled. Tests are disabled unless we know that
   * Splunk is present and loaded with the requisite data. */
  private boolean enabled() {
    return false;
  }

  private void loadDriverClass() {
    try {
      Class.forName("net.hydromatic.optiq.impl.splunk.SplunkDriver");
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("driver not found", e);
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

  /**
   * Tests the vanity driver.
   */
  @Test public void testVanityDriver() throws SQLException {
    loadDriverClass();
    Properties info = new Properties();
    info.setProperty("url", SPLUNK_URL);
    info.put("user", SPLUNK_USER);
    info.put("password", SPLUNK_PASSWORD);
    Connection connection =
        DriverManager.getConnection("jdbc:splunk:", info);
    connection.close();
  }

  /**
   * Tests the vanity driver with properties in the URL.
   */
  @Test public void testVanityDriverArgsInUrl() throws SQLException {
    loadDriverClass();
    Connection connection =
        DriverManager.getConnection(
            "jdbc:splunk:"
            + "url='" + SPLUNK_URL + "'"
            + ";user='" + SPLUNK_USER + "'"
            + ";password='" + SPLUNK_PASSWORD + "'");
    connection.close();
  }

  static final String[] sqls = {
      "select \"source\", \"sourcetype\"\n"
      + "from \"splunk\".\"splunk\"",

      "select \"sourcetype\"\n"
      + "from \"splunk\".\"splunk\"",

      "select distinct \"sourcetype\"\n"
      + "from \"splunk\".\"splunk\"",

      "select count(\"sourcetype\")\n"
      + "from \"splunk\".\"splunk\"",

      // gives wrong answer, not error. currently returns same as count.
      "select count(distinct \"sourcetype\")\n"
      + "from \"splunk\".\"splunk\"",

      "select \"sourcetype\", count(\"source\")\n"
      + "from \"splunk\".\"splunk\"\n"
      + "group by \"sourcetype\"",

      "select \"sourcetype\", count(\"source\") as c\n"
      + "from \"splunk\".\"splunk\"\n"
      + "group by \"sourcetype\"\n"
      + "order by c desc\n",

      // group + order
      "select s.\"product_id\", count(\"source\") as c\n"
      + "from \"splunk\".\"splunk\" as s\n"
      + "where s.\"sourcetype\" = 'access_combined_wcookie'\n"
      + "group by s.\"product_id\"\n"
      + "order by c desc\n",

      // non-advertised field
      "select s.\"sourcetype\", s.\"action\" from \"splunk\".\"splunk\" as s",

      "select s.\"source\", s.\"product_id\", s.\"product_name\", s.\"method\"\n"
      + "from \"splunk\".\"splunk\" as s\n"
      + "where s.\"sourcetype\" = 'access_combined_wcookie'\n",

      "select p.\"product_name\", s.\"action\"\n"
      + "from \"splunk\".\"splunk\" as s\n"
      + "  join \"mysql\".\"products\" as p\n"
      + "on s.\"product_id\" = p.\"product_id\"",

      "select s.\"source\", s.\"product_id\", p.\"product_name\", p.\"price\"\n"
      + "from \"splunk\".\"splunk\" as s\n"
      + "    join \"mysql\".\"products\" as p\n"
      + "    on s.\"product_id\" = p.\"product_id\"\n"
      + "where s.\"sourcetype\" = 'access_combined_wcookie'\n",
  };

  String[] errSqls = {
      // gives error in SplunkPushDownRule
      "select count(*) from \"splunk\".\"splunk\"",

      // gives no rows; suspect off-by-one because no base fields are
      // referenced
      "select s.\"product_id\", s.\"product_name\", s.\"method\"\n"
      + "from \"splunk\".\"splunk\" as s\n"
      + "where s.\"sourcetype\" = 'access_combined_wcookie'\n",

      // horrible error if you access a field that doesn't exist
      "select s.\"sourcetype\", s.\"access\"\n"
      + "from \"splunk\".\"splunk\" as s\n",
  };

  // Fields:
  // sourcetype=access_*
  // action (purchase | update)
  // method (GET | POST)

  /**
   * Reads from a table.
   */
  @Test public void testSelect() throws SQLException {
    checkSql(
        "select \"source\", \"sourcetype\"\n"
        + "from \"splunk\".\"splunk\"");
  }

  @Test public void testSelectDistinct() throws SQLException {
    checkSql(
        "select distinct \"sourcetype\"\n"
        + "from \"splunk\".\"splunk\"");
  }

  @Test public void testSql() throws SQLException {
    checkSql(
        "select p.\"product_name\", /*s.\"product_id\",*/ s.\"action\"\n"
        + "from \"splunk\".\"splunk\" as s\n"
        + "join \"mysql\".\"products\" as p\n"
        + "on s.\"product_id\" = p.\"product_id\"\n"
        + "where s.\"action\" = 'PURCHASE'");

/*
            "select s.\"eventtype\", count(\"source\") as c\n"
            + "from \"splunk\".\"splunk\" as s\n"
            + "group by s.\"eventtype\"\n"
            + "order by c desc\n";
*/
  }

  private void checkSql(String sql) throws SQLException {
    loadDriverClass();
    Connection connection = null;
    Statement statement = null;
    try {
      Properties info = new Properties();
      info.put("url", SPLUNK_URL);
      info.put("user", SPLUNK_USER);
      info.put("password", SPLUNK_PASSWORD);
      connection = DriverManager.getConnection("jdbc:splunk:", info);
      statement = connection.createStatement();
      if (!enabled()) {
        return;
      }
      final ResultSet resultSet =
          statement.executeQuery(
              sql);
      output(resultSet, System.out);
    } finally {
      close(connection, statement);
    }
  }

  private void output(
      ResultSet resultSet, PrintStream out) throws SQLException {
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

// End SplunkAdapterTest.java
