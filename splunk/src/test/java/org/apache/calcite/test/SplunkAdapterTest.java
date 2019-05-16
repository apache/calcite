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

import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.util.TestUtil;

import com.google.common.collect.ImmutableSet;

import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * Unit test of the Calcite adapter for Splunk.
 */
public class SplunkAdapterTest {
  public static final String SPLUNK_URL = "https://localhost:8089";
  public static final String SPLUNK_USER = "admin";
  public static final String SPLUNK_PASSWORD = "changeme";

  /** Whether this test is enabled. Tests are disabled unless we know that
   * Splunk is present and loaded with the requisite data. */
  private boolean enabled() {
    return CalciteSystemProperty.TEST_SPLUNK.value();
  }

  private void loadDriverClass() {
    try {
      Class.forName("org.apache.calcite.adapter.splunk.SplunkDriver");
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
    if (!enabled()) {
      return;
    }
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
    if (!enabled()) {
      return;
    }
    Connection connection =
        DriverManager.getConnection("jdbc:splunk:"
            + "url='" + SPLUNK_URL + "'"
            + ";user='" + SPLUNK_USER + "'"
            + ";password='" + SPLUNK_PASSWORD + "'");
    connection.close();
  }

  static final String[] SQL_STRINGS = {
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

  static final String[] ERROR_SQL_STRINGS = {
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
    final String sql = "select \"source\", \"sourcetype\"\n"
        + "from \"splunk\".\"splunk\"";
    checkSql(sql, resultSet -> {
      try {
        if (!(resultSet.next() && resultSet.next() && resultSet.next())) {
          throw new AssertionError("expected at least 3 rows");
        }
        return null;
      } catch (SQLException e) {
        throw TestUtil.rethrow(e);
      }
    });
  }

  @Test public void testSelectDistinct() throws SQLException {
    checkSql(
        "select distinct \"sourcetype\"\n"
        + "from \"splunk\".\"splunk\"",
        expect("sourcetype=access_combined_wcookie",
            "sourcetype=vendor_sales",
            "sourcetype=secure"));
  }

  private static Function<ResultSet, Void> expect(final String... lines) {
    final Collection<String> expected = ImmutableSet.copyOf(lines);
    return a0 -> {
      try {
        Collection<String> actual =
            CalciteAssert.toStringList(a0, new HashSet<>());
        assertThat(actual, equalTo(expected));
        return null;
      } catch (SQLException e) {
        throw TestUtil.rethrow(e);
      }
    };
  }

  /** "status" is not a built-in column but we know it has some values in the
   * test data. */
  @Test public void testSelectNonBuiltInColumn() throws SQLException {
    checkSql(
        "select \"status\"\n"
        + "from \"splunk\".\"splunk\"", a0 -> {
          final Set<String> actual = new HashSet<>();
          try {
            while (a0.next()) {
              actual.add(a0.getString(1));
            }
            assertThat(actual.contains("404"), is(true));
            return null;
          } catch (SQLException e) {
            throw TestUtil.rethrow(e);
          }
        });
  }

  @Ignore("cannot plan due to CAST in ON clause")
  @Test public void testJoinToJdbc() throws SQLException {
    checkSql(
        "select p.\"product_name\", /*s.\"product_id\",*/ s.\"action\"\n"
            + "from \"splunk\".\"splunk\" as s\n"
            + "join \"foodmart\".\"product\" as p\n"
            + "on cast(s.\"product_id\" as integer) = p.\"product_id\"\n"
            + "where s.\"action\" = 'PURCHASE'",
        null);
  }

  @Test public void testGroupBy() throws SQLException {
    checkSql(
        "select s.\"host\", count(\"source\") as c\n"
            + "from \"splunk\".\"splunk\" as s\n"
            + "group by s.\"host\"\n"
            + "order by c desc\n",
        expect("host=vendor_sales; C=30244",
            "host=www1; C=24221",
            "host=www3; C=22975",
            "host=www2; C=22595",
            "host=mailsv; C=9829"));
  }

  private void checkSql(String sql, Function<ResultSet, Void> f)
      throws SQLException {
    if (!enabled()) {
      return;
    }
    loadDriverClass();
    Connection connection = null;
    Statement statement = null;
    try {
      Properties info = new Properties();
      info.put("url", SPLUNK_URL);
      info.put("user", SPLUNK_USER);
      info.put("password", SPLUNK_PASSWORD);
      info.put("model", "inline:" + JdbcTest.FOODMART_MODEL);
      connection = DriverManager.getConnection("jdbc:splunk:", info);
      statement = connection.createStatement();
      final ResultSet resultSet = statement.executeQuery(sql);
      f.apply(resultSet);
      resultSet.close();
    } finally {
      close(connection, statement);
    }
  }
}

// End SplunkAdapterTest.java
