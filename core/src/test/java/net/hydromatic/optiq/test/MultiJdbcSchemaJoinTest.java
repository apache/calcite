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

import net.hydromatic.optiq.SchemaPlus;
import net.hydromatic.optiq.impl.jdbc.JdbcSchema;
import net.hydromatic.optiq.jdbc.OptiqConnection;

import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
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
