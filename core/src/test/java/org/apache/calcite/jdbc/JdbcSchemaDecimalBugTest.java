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
package org.apache.calcite.jdbc;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.schema.SchemaPlus;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.Properties;
import java.util.logging.Logger;
import javax.sql.DataSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Integration tests for
 * <a href="https://issues.apache.org/jira/browse/CALCITE-6654">[CALCITE-6654]
 * JdbcSchema throws error for NUMERIC/DECIMAL columns without explicit
 * precision (Oracle, PostgreSQL, MSSQL)</a>.
 *
 * <p>These are <b>integration tests</b> that require live database instances.
 * Run with:
 ** {@code ./gradlew :core:integTestCalcite6654}
 *
 * <p>Required Docker containers (see docker-compose in src/test/resources):
 * <ul>
 *   <li>PostgreSQL - localhost:5432/testdb  (user: testuser / testpass)</li>
 *   <li>SQL Server  - localhost:1433/master  (user: sa / TestPass123!)</li>
 *   <li>Oracle Free - localhost:1521/FREEPDB1 (user: system / testpass) [optional]</li>
 * </ul>
 *
 * <p>If a database is unreachable the test is skipped, not failed.
 */
@Tag("integration")
@EnabledIfSystemProperty(named = "calcite.integration.tests", matches = "true")
public class JdbcSchemaDecimalBugTest {

  // ---------------------------------------------------------------------------
  // DataSource factory
  // ---------------------------------------------------------------------------

  /**
   * Minimal {@link DataSource} backed by a JDBC URL.
   * Sufficient for integration tests; not for production use.
   */
  private static class DriverManagerDataSource implements DataSource {
    private final String url;
    private final String user;
    private final String password;

    DriverManagerDataSource(String url, String user, String password) {
      this.url = url;
      this.user = user;
      this.password = password;
    }

    @Override public Connection getConnection() throws SQLException {
      return DriverManager.getConnection(url, user, password);
    }

    @Override public Connection getConnection(String u, String p) throws SQLException {
      return getConnection();
    }

    @Override public PrintWriter getLogWriter() {
      return null;
    }

    @Override public void setLogWriter(PrintWriter w) {
    }

    @Override public void setLoginTimeout(int seconds) {
    }

    @Override public int getLoginTimeout() {
      return 0;
    }

    @Override public Logger getParentLogger() throws SQLFeatureNotSupportedException {
      throw new SQLFeatureNotSupportedException("getParentLogger");
    }

    @Override public <T> T unwrap(Class<T> iface) throws SQLException {
      throw new SQLException("Not a wrapper for " + iface);
    }

    @Override public boolean isWrapperFor(Class<?> iface) {
      return false;
    }
  }

  private static DataSource oracleDataSource() {
    return new DriverManagerDataSource(
        "jdbc:oracle:thin:@localhost:1521/FREEPDB1", "system", "testpass");
  }

  private static DataSource postgresDataSource() {
    return new DriverManagerDataSource(
        "jdbc:postgresql://localhost:5432/testdb", "testuser", "testpass");
  }

  private static DataSource mssqlDataSource() {
    return new DriverManagerDataSource(
        "jdbc:sqlserver://localhost:1433;databaseName=master;encrypt=false",
        "sa", "TestPass123!");
  }

  // ---------------------------------------------------------------------------
  // Helper methods
  // ---------------------------------------------------------------------------

  /** Skips the test if the database is not reachable. */
  private static void assumeReachable(DataSource ds, String dbName) {
    try (Connection ignored = ds.getConnection()) {
      // connection succeeded - proceed with the test
    } catch (SQLException e) {
      assumeTrue(false,
          dbName + " is not reachable - skipping test. Cause: " + e.getMessage());
    }
  }

  /**
   * Creates a table with a DECIMAL/NUMERIC column that has no explicit
   * precision. This is the exact scenario that triggered CALCITE-6654.
   * Drops the table first if it already exists.
   */
  private static void createTestTable(DataSource ds, String dropSql,
      String createSql, String insertSql) throws SQLException {
    try (Connection conn = ds.getConnection();
         Statement st = conn.createStatement()) {
      try {
        st.execute(dropSql);
      } catch (SQLException ignored) {
        // table did not exist yet - that is fine
      }
      st.execute(createSql);
      st.execute(insertSql);
    }
    // connection is closed here via try-with-resources
  }

  // ---------------------------------------------------------------------------
  // Tests - Bug fix: columns without explicit precision must not throw
  // ---------------------------------------------------------------------------

  /**
   * Oracle {@code NUMBER} without precision (e.g. {@code col NUMBER}) must
   * not cause Calcite to throw during schema registration or query execution.
   *
   * <p>Oracle reports {@code precision=0, scale=-127} for such columns.
   */
  @Test void oracleNumberWithoutPrecisionShouldNotThrow() throws Exception {
    DataSource ds = oracleDataSource();
    assumeReachable(ds, "Oracle");

    // Oracle stores unquoted identifiers in UPPERCASE
    createTestTable(ds,
        "DROP TABLE TEST_NUMBERS",
        "CREATE TABLE TEST_NUMBERS (id NUMBER, val NUMBER)",
        "INSERT INTO TEST_NUMBERS VALUES (1, 42.5)");

    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", new Properties())) {
      CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class);
      SchemaPlus root = calciteConn.getRootSchema();
      root.add("oracle", JdbcSchema.create(root, "oracle", ds, null, "SYSTEM"));
      try (Statement st = conn.createStatement();
           ResultSet rs =
               st.executeQuery("SELECT * FROM \"oracle\".\"TEST_NUMBERS\"")) {
        while (rs.next()) {
          // drain - no exception = fix is working
        }
      }
    }
  }

  /**
   * PostgreSQL {@code NUMERIC} without precision (e.g. {@code col NUMERIC})
   * must not cause Calcite to throw.
   *
   * <p>PostgreSQL reports {@code precision=0, scale=0} for such columns.
   */
  @Test void postgresNumericWithoutPrecisionShouldNotThrow() throws Exception {
    DataSource ds = postgresDataSource();
    assumeReachable(ds, "PostgreSQL");

    // Create the test table directly in the test — with NUMERIC (no precision)
    createTestTable(ds,
        "DROP TABLE IF EXISTS test_numbers",
        "CREATE TABLE test_numbers (id INTEGER, val NUMERIC)",
        "INSERT INTO test_numbers VALUES (1, 42.5)");

    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", new Properties())) {
      CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class);
      SchemaPlus root = calciteConn.getRootSchema();
      root.add("postgres",
          JdbcSchema.create(root, "postgres", ds, "testdb", "public"));
      try (Statement st = conn.createStatement();
           ResultSet rs =
               st.executeQuery("SELECT * FROM \"postgres\".\"test_numbers\"")) {
        while (rs.next()) {
          // drain
        }
      }
    }
  }

  /**
   * SQL Server {@code DECIMAL} without precision (e.g. {@code col DECIMAL})
   * must not cause Calcite to throw.
   *
   * <p>MSSQL reports {@code precision=0, scale=0} for such columns.
   */
  @Test void mssqlDecimalWithoutPrecisionShouldNotThrow() throws Exception {
    DataSource ds = mssqlDataSource();
    assumeReachable(ds, "MSSQL");

    // Create the test table directly in the test — with DECIMAL (no precision)
    createTestTable(ds,
        "DROP TABLE IF EXISTS test_numbers",
        "CREATE TABLE test_numbers (id INT, val DECIMAL)",
        "INSERT INTO test_numbers VALUES (1, 42.5)");

    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", new Properties())) {
      CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class);
      SchemaPlus root = calciteConn.getRootSchema();
      root.add("mssql", JdbcSchema.create(root, "mssql", ds, null, "dbo"));
      try (Statement st = conn.createStatement();
           ResultSet rs =
               st.executeQuery("SELECT * FROM \"mssql\".\"test_numbers\"")) {
        while (rs.next()) {
          // drain
        }
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Tests - Regression: explicit precision/scale must be preserved
  // ---------------------------------------------------------------------------

  /**
   * A column declared as {@code DECIMAL(10,2)} must still report the correct
   * type, precision, and scale after the fix — i.e. the fix must not break
   * columns that have explicit precision.
   */
  @Test void postgresDecimalWithExplicitPrecisionShouldRemainUnchanged()
      throws Exception {
    DataSource ds = postgresDataSource();
    assumeReachable(ds, "PostgreSQL");

    // Create a table with EXPLICIT precision — regression test
    createTestTable(ds,
        "DROP TABLE IF EXISTS test_explicit",
        "CREATE TABLE test_explicit (amount DECIMAL(10, 2))",
        "INSERT INTO test_explicit VALUES (12345.67)");

    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", new Properties())) {
      CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class);
      SchemaPlus root = calciteConn.getRootSchema();
      root.add("postgres",
          JdbcSchema.create(root, "postgres", ds, "testdb", "public"));
      try (Statement st = conn.createStatement();
           ResultSet rs =
               st.executeQuery("SELECT * FROM \"postgres\".\"test_explicit\"")) {
        ResultSetMetaData meta = rs.getMetaData();
        assertEquals(10, meta.getPrecision(1),
            "Precision of DECIMAL(10,2) column must be 10");
        assertEquals(2, meta.getScale(1),
            "Scale of DECIMAL(10,2) column must be 2");
        while (rs.next()) {
          // drain
        }
      }
    }
  }
}
