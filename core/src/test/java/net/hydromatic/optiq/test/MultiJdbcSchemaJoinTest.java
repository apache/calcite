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
import net.hydromatic.optiq.impl.jdbc.JdbcSchema;
import net.hydromatic.optiq.jdbc.OptiqConnection;
import net.hydromatic.optiq.prepare.OptiqPrepareImpl;

import com.google.common.collect.Sets;

import org.junit.Test;

import java.sql.*;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

/** Test case for joining tables from two different JDBC databases. */
public class MultiJdbcSchemaJoinTest {
  @Test public void test() throws SQLException, ClassNotFoundException {
    // Create two databases
    // It's two times hsqldb, but imagine they are different rdbms's
    final String db1 = TempDb.INSTANCE.getUrl();
    Connection c1 = DriverManager.getConnection(db1, "", "");
    Statement stmt1 = c1.createStatement();
    stmt1.execute(
        "create table table1(id varchar(10) not null primary key, "
            + "field1 varchar(10))");
    stmt1.execute("insert into table1 values('a', 'aaaa')");
    c1.close();

    final String db2 = TempDb.INSTANCE.getUrl();
    Connection c2 = DriverManager.getConnection(db2, "", "");
    Statement stmt2 = c2.createStatement();
    stmt2.execute(
        "create table table2(id varchar(10) not null primary key, "
            + "field1 varchar(10))");
    stmt2.execute("insert into table2 values('a', 'aaaa')");
    c2.close();

    // Connect via optiq to these databases
    Connection connection = DriverManager.getConnection("jdbc:optiq:");
    OptiqConnection optiqConnection = connection.unwrap(OptiqConnection.class);
    SchemaPlus rootSchema = optiqConnection.getRootSchema();
    final DataSource ds1 =
        JdbcSchema.dataSource(db1, "org.hsqldb.jdbcDriver", "", "");
    rootSchema.add("DB1",
        JdbcSchema.create(rootSchema, "DB1", ds1, null, null));
    final DataSource ds2 =
        JdbcSchema.dataSource(db2, "org.hsqldb.jdbcDriver", "", "");
    rootSchema.add("DB2",
        JdbcSchema.create(rootSchema, "DB2", ds2, null, null));

    Statement stmt3 = connection.createStatement();
    ResultSet rs = stmt3.executeQuery(
        "select table1.id, table1.field1 "
            + "from db1.table1 join db2.table2 on table1.id = table2.id");
    assertThat(OptiqAssert.toString(rs), equalTo("ID=a; FIELD1=aaaa\n"));
  }

  /** Makes sure that {@link #test} is re-entrant.
   * Effectively a test for {@link TempDb}. */
  @Test public void test2() throws SQLException, ClassNotFoundException {
    test();
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

    // Make an optiq schema with both a jdbc schema and a non-jdbc schema
    Connection optiqConn = DriverManager.getConnection("jdbc:optiq:");
    OptiqConnection optiqConnection =
        optiqConn.unwrap(OptiqConnection.class);
    SchemaPlus rootSchema = optiqConnection.getRootSchema();
    rootSchema.add("DB",
        JdbcSchema.create(rootSchema, "DB",
            JdbcSchema.dataSource(db, "org.hsqldb.jdbcDriver", "", ""),
            null, null));
    rootSchema.add("hr", new ReflectiveSchema(new JdbcTest.HrSchema()));
    return optiqConn;
  }

  @Test public void testJdbcWithEnumerableJoin() throws SQLException {
    // This query works correctly
    String query = "select t.id, t.field1 "
        + "from db.table1 t join \"hr\".\"emps\" e on e.\"empid\" = t.id";
    final Set<Integer> expected = Sets.newHashSet(100, 200);
    assertThat(runQuery(setup(), query), equalTo(expected));
  }

  @Test public void testEnumerableWithJdbcJoin() throws SQLException {
    //  * compared to testJdbcWithEnumerableJoin, the join order is reversed
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

  private Set<Integer> runQuery(Connection optiqConn, String query)
      throws SQLException {
    // Print out the plan
    Statement stmt = optiqConn.createStatement();
    try {
      ResultSet rs;
      if (OptiqPrepareImpl.DEBUG) {
        rs = stmt.executeQuery("explain plan for " + query);
        rs.next();
        System.out.println(rs.getString(1));
      }

      // Run the actual query
      rs = stmt.executeQuery(query);
      Set<Integer> ids = Sets.newHashSet();
      while (rs.next()) {
        ids.add(rs.getInt(1));
      }
      return ids;
    } finally {
      stmt.close();
    }
  }

  @Test public void testSchemaCache() throws Exception {
    // Create a database
    final String db = TempDb.INSTANCE.getUrl();
    Connection c1 = DriverManager.getConnection(db, "", "");
    Statement stmt1 = c1.createStatement();
    stmt1.execute(
        "create table table1(id varchar(10) not null primary key, "
            + "field1 varchar(10))");

    // Connect via optiq to these databases
    Connection connection = DriverManager.getConnection("jdbc:optiq:");
    OptiqConnection optiqConnection = connection.unwrap(OptiqConnection.class);
    SchemaPlus rootSchema = optiqConnection.getRootSchema();
    final DataSource ds =
        JdbcSchema.dataSource(db, "org.hsqldb.jdbcDriver", "", "");
    final SchemaPlus s =
        rootSchema.add("DB",
            JdbcSchema.create(rootSchema, "DB", ds, null, null));

    Statement stmt3 = connection.createStatement();
    ResultSet rs;

    // fails, table does not exist
    try {
      rs = stmt3.executeQuery("select * from db.table2");
      fail("expected error, got " + rs);
    } catch (SQLException e) {
      assertThat(e.getCause().getCause().getMessage(),
          equalTo("Table 'DB.TABLE2' not found"));
    }

    stmt1.execute(
        "create table table2(id varchar(10) not null primary key, "
            + "field1 varchar(10))");
    stmt1.execute("insert into table2 values('a', 'aaaa')");

    // fails, table not visible due to caching
    try {
      rs = stmt3.executeQuery("select * from db.table2");
      fail("expected error, got " + rs);
    } catch (SQLException e) {
      assertThat(e.getCause().getCause().getMessage(),
          equalTo("Table 'DB.TABLE2' not found"));
    }

    // disable caching and table becomes visible
    s.setCacheEnabled(false);
    rs = stmt3.executeQuery("select * from db.table2");
    assertThat(OptiqAssert.toString(rs), equalTo("ID=a; FIELD1=aaaa\n"));
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
