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

import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelRunner;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.function.Function;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Test cases to check that necessary information from underlying exceptions
 * is correctly propagated via {@link SQLException}s.
 */
public class ExceptionMessageTest {
  /**
   * Simple reflective schema that provides valid and invalid entries.
   */
  @SuppressWarnings("UnusedDeclaration")
  public static class TestSchema {
    public Entry[] entries = {
        new Entry(1, "name1"),
        new Entry(2, "name2")
    };

    public Iterable<Entry> badEntries = () -> {
      throw new IllegalStateException("Can't iterate over badEntries");
    };
  }

  /**
   * Entries made available in the reflective TestSchema.
   */
  public static class Entry {
    public final int id;
    public final String name;

    public Entry(int id, String name) {
      this.id = id;
      this.name = name;
    }
  }

  /** Fixture. */
  private static class Fixture implements AutoCloseable {
    private final CalciteConnection conn;

    Fixture() throws SQLException {
      Connection connection = DriverManager.getConnection("jdbc:calcite:");
      this.conn = connection.unwrap(CalciteConnection.class);
      SchemaPlus rootSchema = conn.getRootSchema();
      rootSchema.add("test", new ReflectiveSchema(new TestSchema()));
      conn.setSchema("test");
    }

    @Override public void close() throws SQLException {
      conn.close();
    }

    private void runQuery(String sql) throws SQLException {
      Statement stmt = conn.createStatement();
      try {
        stmt.executeQuery(sql);
      } finally {
        try {
          stmt.close();
        } catch (Exception e) {
          // We catch a possible exception on close so that we know we're not
          // masking the query exception with the close exception
          fail("Error on close");
        }
      }
    }

    /** Performs an action that requires a {@link RelBuilder}, and returns the
     * result. */
    private <T> T withRelBuilder(Function<RelBuilder, T> fn)
        throws SQLException {
      final SchemaPlus rootSchema =
          conn.unwrap(CalciteConnection.class).getRootSchema();
      final FrameworkConfig config = Frameworks.newConfigBuilder()
          .defaultSchema(rootSchema)
          .build();
      final RelBuilder relBuilder = RelBuilder.create(config);
      return fn.apply(relBuilder);
    }

    private void runQuery(Function<RelBuilder, RelNode> relFn)
        throws SQLException {
      final RelRunner relRunner = conn.unwrap(RelRunner.class);
      final RelNode relNode = withRelBuilder(relFn);
      final PreparedStatement preparedStatement =
          relRunner.prepareStatement(relNode);
      try {
        preparedStatement.executeQuery();
      } finally {
        try {
          preparedStatement.close();
        } catch (Exception e) {
          fail("Error on close");
        }
      }
    }
  }

  @Test void testValidQuery() throws SQLException {
    try (Fixture f = new Fixture()) {
      // Just ensure that we're actually dealing with a valid connection
      // to be sure that the results of the other tests can be trusted
      f.runQuery("select * from \"entries\"");
    }
  }

  @Test void testNonSqlException() {
    try (Fixture f = new Fixture()) {
      f.runQuery("select * from \"badEntries\"");
      fail("Query badEntries should result in an exception");
    } catch (SQLException e) {
      assertThat(e.getMessage(),
          equalTo("Error while executing SQL \"select * from \"badEntries\"\": "
              + "Can't iterate over badEntries"));
    }
  }

  @Test void testSyntaxError() {
    try (Fixture f = new Fixture()) {
      f.runQuery("invalid sql");
      fail("Query should fail");
    } catch (SQLException e) {
      assertThat(e.getMessage(),
          equalTo("Error while executing SQL \"invalid sql\": parse failed: "
              + "Non-query expression encountered in illegal context"));
    }
  }

  @Test void testSemanticError() {
    try (Fixture f = new Fixture()) {
      // implicit type coercion.
      f.runQuery("select \"name\" - \"id\" from \"entries\"");
    } catch (SQLException e) {
      assertThat(e.getMessage(),
          containsString("Cannot apply '-' to arguments"));
    }
  }

  @Test void testNonexistentTable() {
    try (Fixture f = new Fixture()) {
      f.runQuery("select name from \"nonexistentTable\"");
      fail("Query should fail");
    } catch (SQLException e) {
      assertThat(e.getMessage(),
          containsString("Object 'nonexistentTable' not found"));
    }
  }

  /** Runs a query via {@link RelRunner}. */
  @Test void testValidRelNodeQuery() throws SQLException {
    try (Fixture f = new Fixture()) {
      final Function<RelBuilder, RelNode> relFn = b ->
          b.scan("test", "entries")
              .project(b.field("name"))
              .build();
      f.runQuery(relFn);
    }
  }

  /** Runs a query via {@link RelRunner} that is expected to fail,
   * and checks that the exception correctly describes the RelNode tree.
   *
   * <p>Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4585">[CALCITE-4585]
   * If a query is executed via RelRunner.prepare(RelNode) and fails, the
   * exception should report the RelNode plan, not the SQL</a>. */
  @Test void testRelNodeQueryException() {
    try (Fixture f = new Fixture()) {
      final Function<RelBuilder, RelNode> relFn = b ->
          b.scan("test", "entries")
              .project(b.call(SqlStdOperatorTable.ABS, b.field("name")))
              .build();
      f.runQuery(relFn);
      fail("RelNode query about entries should result in an exception");
    } catch (SQLException e) {
      String message = "Error while preparing plan ["
          + "LogicalProject($f0=[ABS($1)])\n"
          + "  LogicalTableScan(table=[[test, entries]])\n"
          + "]";
      assertThat(e.getMessage(), Matchers.isLinux(message));
    }
  }
}
