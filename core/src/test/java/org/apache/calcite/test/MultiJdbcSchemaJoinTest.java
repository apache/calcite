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
import org.apache.calcite.adapter.jdbc.JdbcCatalogSchema;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalciteJdbc41Factory;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.Driver;
import org.apache.calcite.schema.SchemaPlus;

import org.apache.commons.dbcp2.BasicDataSource;

import com.google.common.collect.Sets;

import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/** Test case for joining tables from two different JDBC databases. */
public class MultiJdbcSchemaJoinTest {
  @Test public void test() throws SQLException, ClassNotFoundException {
    // Create two databases
    // It's two times hsqldb, but imagine they are different rdbms's
    final String db1 = TempDb.INSTANCE.getUrl();
    Connection c1 = DriverManager.getConnection(db1, "", "");
    Statement stmt1 = c1.createStatement();
    stmt1.execute("create table table1(id varchar(10) not null primary key, "
        + "field1 varchar(10))");
    stmt1.execute("insert into table1 values('a', 'aaaa')");
    c1.close();

    final String db2 = TempDb.INSTANCE.getUrl();
    Connection c2 = DriverManager.getConnection(db2, "", "");
    Statement stmt2 = c2.createStatement();
    stmt2.execute("create table table2(id varchar(10) not null primary key, "
        + "field1 varchar(10))");
    stmt2.execute("insert into table2 values('a', 'aaaa')");
    c2.close();

    // Connect via calcite to these databases
    Connection connection = DriverManager.getConnection("jdbc:calcite:");
    CalciteConnection calciteConnection =
        connection.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();
    final DataSource ds1 =
        JdbcSchema.dataSource(db1, "org.hsqldb.jdbcDriver", "", "");
    rootSchema.add("DB1",
        JdbcSchema.create(rootSchema, "DB1", ds1, null, null));
    final DataSource ds2 =
        JdbcSchema.dataSource(db2, "org.hsqldb.jdbcDriver", "", "");
    rootSchema.add("DB2",
        JdbcSchema.create(rootSchema, "DB2", ds2, null, null));

    Statement stmt3 = connection.createStatement();
    ResultSet rs = stmt3.executeQuery("select table1.id, table1.field1 "
        + "from db1.table1 join db2.table2 on table1.id = table2.id");
    assertThat(CalciteAssert.toString(rs), equalTo("ID=a; FIELD1=aaaa\n"));
  }

  /** Makes sure that {@link #test} is re-entrant.
   * Effectively a test for {@code TempDb}. */
  @Test public void test2() throws SQLException, ClassNotFoundException {
    test();
  }

  /** Tests {@link org.apache.calcite.adapter.jdbc.JdbcCatalogSchema}. */
  @Test public void test3() throws SQLException {
    final BasicDataSource dataSource = new BasicDataSource();
    dataSource.setUrl(TempDb.INSTANCE.getUrl());
    dataSource.setUsername("");
    dataSource.setPassword("");
    final JdbcCatalogSchema schema =
        JdbcCatalogSchema.create(null, "", dataSource, "PUBLIC");
    assertThat(schema.getSubSchemaNames(),
        is(Sets.newHashSet("INFORMATION_SCHEMA", "PUBLIC", "SYSTEM_LOBS")));
    final CalciteSchema rootSchema0 =
        CalciteSchema.createRootSchema(false, false, "", schema);
    final Driver driver = new Driver();
    final CalciteJdbc41Factory factory = new CalciteJdbc41Factory();
    final String sql = "select count(*) as c from information_schema.schemata";
    try (Connection connection =
             factory.newConnection(driver, factory,
                 "jdbc:calcite:", new Properties(), rootSchema0, null);
         Statement stmt3 = connection.createStatement();
         ResultSet rs = stmt3.executeQuery(sql)) {
      assertThat(CalciteAssert.toString(rs), equalTo("C=3\n"));
    }
  }

  private Connection setup() throws SQLException {
    // Create a jdbc database & table
    final String db = TempDb.INSTANCE.getUrl();
    Connection c1 = DriverManager.getConnection(db, "", "");
    Statement stmt1 = c1.createStatement();
    // This is a table we can join with the emps from the hr schema
    stmt1.execute("create table table1(id integer not null primary key, "
        + "field1 varchar(10))");
    stmt1.execute("insert into table1 values(100, 'foo')");
    stmt1.execute("insert into table1 values(200, 'bar')");
    c1.close();

    // Make a Calcite schema with both a jdbc schema and a non-jdbc schema
    Connection connection = DriverManager.getConnection("jdbc:calcite:");
    CalciteConnection calciteConnection =
        connection.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();
    rootSchema.add("DB",
        JdbcSchema.create(rootSchema, "DB",
            JdbcSchema.dataSource(db, "org.hsqldb.jdbcDriver", "", ""),
            null, null));
    rootSchema.add("hr", new ReflectiveSchema(new JdbcTest.HrSchema()));
    return connection;
  }

  @Test public void testJdbcWithEnumerableHashJoin() throws SQLException {
    // This query works correctly
    String query = "select t.id, t.field1 "
        + "from db.table1 t join \"hr\".\"emps\" e on e.\"empid\" = t.id";
    final Set<Integer> expected = Sets.newHashSet(100, 200);
    assertThat(runQuery(setup(), query), equalTo(expected));
  }

  @Test public void testEnumerableWithJdbcJoin() throws SQLException {
    //  * compared to testJdbcWithEnumerableHashJoin, the join order is reversed
    //  * the query fails with a CannotPlanException
    String query = "select t.id, t.field1 "
        + "from \"hr\".\"emps\" e join db.table1 t on e.\"empid\" = t.id";
    final Set<Integer> expected = Sets.newHashSet(100, 200);
    assertThat(runQuery(setup(), query), equalTo(expected));
  }

  @Test public void testEnumerableWithJdbcJoinWithWhereClause()
      throws SQLException {
    // Same query as above but with a where condition added:
    //  * the good: this query does not give a CannotPlanException
    //  * the bad: the result is wrong: there is only one emp called Bill.
    //             The query plan shows the join condition is always true,
    //             afaics, the join condition is pushed down to the non-jdbc
    //             table. It might have something to do with the cast that
    //             is introduced in the join condition.
    String query = "select t.id, t.field1 "
        + "from \"hr\".\"emps\" e join db.table1 t on e.\"empid\" = t.id"
        + " where e.\"name\" = 'Bill'";
    final Set<Integer> expected = Sets.newHashSet(100);
    assertThat(runQuery(setup(), query), equalTo(expected));
  }

  private Set<Integer> runQuery(Connection calciteConnection, String query)
      throws SQLException {
    // Print out the plan
    Statement stmt = calciteConnection.createStatement();
    try {
      ResultSet rs;
      if (CalciteSystemProperty.DEBUG.value()) {
        rs = stmt.executeQuery("explain plan for " + query);
        rs.next();
        System.out.println(rs.getString(1));
      }

      // Run the actual query
      rs = stmt.executeQuery(query);
      Set<Integer> ids = new HashSet<>();
      while (rs.next()) {
        ids.add(rs.getInt(1));
      }
      return ids;
    } finally {
      stmt.close();
    }
  }

  @Test public void testSchemaConsistency() throws Exception {
    // Create a database
    final String db = TempDb.INSTANCE.getUrl();
    Connection c1 = DriverManager.getConnection(db, "", "");
    Statement stmt1 = c1.createStatement();
    stmt1.execute("create table table1(id varchar(10) not null primary key, "
        + "field1 varchar(10))");

    // Connect via calcite to these databases
    Connection connection = DriverManager.getConnection("jdbc:calcite:");
    CalciteConnection calciteConnection =
        connection.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();
    final DataSource ds =
        JdbcSchema.dataSource(db, "org.hsqldb.jdbcDriver", "", "");
    rootSchema.add("DB", JdbcSchema.create(rootSchema, "DB", ds, null, null));

    Statement stmt3 = connection.createStatement();
    ResultSet rs;

    // fails, table does not exist
    try {
      rs = stmt3.executeQuery("select * from db.table2");
      fail("expected error, got " + rs);
    } catch (SQLException e) {
      assertThat(e.getCause().getCause().getMessage(),
          equalTo("Object 'TABLE2' not found within 'DB'"));
    }

    stmt1.execute("create table table2(id varchar(10) not null primary key, "
        + "field1 varchar(10))");
    stmt1.execute("insert into table2 values('a', 'aaaa')");

    PreparedStatement stmt2 =
        connection.prepareStatement("select * from db.table2");

    stmt1.execute("alter table table2 add column field2 varchar(10)");

    // "field2" not visible to stmt2
    rs = stmt2.executeQuery();
    assertThat(CalciteAssert.toString(rs), equalTo("ID=a; FIELD1=aaaa\n"));

    // "field2" visible to a new query
    rs = stmt3.executeQuery("select * from db.table2");
    assertThat(CalciteAssert.toString(rs),
        equalTo("ID=a; FIELD1=aaaa; FIELD2=null\n"));
    c1.close();
  }

  /** Pool of temporary databases. */
  static class TempDb {
    public static final TempDb INSTANCE = new TempDb();

    private final AtomicInteger id = new AtomicInteger(1);

    TempDb() {}

    /** Allocates a URL for a new Hsqldb database. */
    public String getUrl() {
      return "jdbc:hsqldb:mem:db" + id.getAndIncrement();
    }
  }
}

// End MultiJdbcSchemaJoinTest.java
