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
import org.apache.calcite.schema.SchemaPlus;

import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Test cases to check that necessary information from underlying exceptions
 * is correctly propagated via {@link SQLException}s.
 */
public class ExceptionMessageTest {
  private Connection conn;

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
    public int id;
    public String name;

    public Entry(int id, String name) {
      this.id = id;
      this.name = name;
    }
  }

  @Before
  public void setUp() throws SQLException {
    Connection connection = DriverManager.getConnection("jdbc:calcite:");
    CalciteConnection calciteConnection =
        connection.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();
    rootSchema.add("test", new ReflectiveSchema(new TestSchema()));
    calciteConnection.setSchema("test");
    this.conn = calciteConnection;
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

  @Test public void testValidQuery() throws SQLException {
    // Just ensure that we're actually dealing with a valid connection
    // to be sure that the results of the other tests can be trusted
    runQuery("select * from \"entries\"");
  }

  @Test public void testNonSqlException() throws SQLException {
    try {
      runQuery("select * from \"badEntries\"");
      fail("Query badEntries should result in an exception");
    } catch (SQLException e) {
      assertThat(e.getMessage(),
          equalTo("Error while executing SQL \"select * from \"badEntries\"\": "
              + "Can't iterate over badEntries"));
    }
  }

  @Test public void testSyntaxError() {
    try {
      runQuery("invalid sql");
      fail("Query should fail");
    } catch (SQLException e) {
      assertThat(e.getMessage(),
          equalTo("Error while executing SQL \"invalid sql\": parse failed: "
              + "Non-query expression encountered in illegal context"));
    }
  }

  @Test public void testSemanticError() {
    try {
      // implicit type coercion.
      runQuery("select \"name\" - \"id\" from \"entries\"");
    } catch (SQLException e) {
      assertThat(e.getMessage(),
          containsString("Cannot apply '-' to arguments"));
    }
  }

  @Test public void testNonexistentTable() {
    try {
      runQuery("select name from \"nonexistentTable\"");
      fail("Query should fail");
    } catch (SQLException e) {
      assertThat(e.getMessage(),
          containsString("Object 'nonexistentTable' not found"));
    }
  }
}

// End ExceptionMessageTest.java
