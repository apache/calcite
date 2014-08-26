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
package net.hydromatic.optiq.test;

import net.hydromatic.optiq.SchemaPlus;
import net.hydromatic.optiq.impl.java.ReflectiveSchema;
import net.hydromatic.optiq.jdbc.OptiqConnection;

import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

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

    public Iterable<Entry> badEntries = new Iterable<Entry>() {
      public Iterator<Entry> iterator() {
        throw new IllegalStateException("Can't iterate over badEntries");
      }
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
    Connection connection = DriverManager.getConnection("jdbc:optiq:");
    OptiqConnection optiqConnection = connection.unwrap(OptiqConnection.class);
    SchemaPlus rootSchema = optiqConnection.getRootSchema();
    rootSchema.add("test", new ReflectiveSchema(new TestSchema()));
    optiqConnection.setSchema("test");
    this.conn = optiqConnection;
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
          equalTo(
              "exception while executing query: Can't iterate over badEntries"));
    }
  }

  @Test public void testSyntaxError() {
    try {
      runQuery("invalid sql");
      fail("Query should fail");
    } catch (SQLException e) {
      assertThat(e.getMessage(),
          equalTo("error while executing SQL \"invalid sql\": parse failed: "
              + "Non-query expression encountered in illegal context"));
    }
  }

  @Test public void testSemanticError() {
    try {
      runQuery("select \"name\" - \"id\" from \"entries\"");
      fail("Query with semantic error should fail");
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
          containsString("Table 'nonexistentTable' not found"));
    }
  }
}

// End ExceptionMessageTest.java
