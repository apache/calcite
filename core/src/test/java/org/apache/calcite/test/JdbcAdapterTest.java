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

import org.apache.calcite.jdbc.CalciteConnection;

import com.google.common.base.Function;
import com.google.common.base.Throwables;

import org.hsqldb.jdbcDriver;

import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

/**
 * Tests for the {@code org.apache.calcite.adapter.jdbc} package.
 */
public class JdbcAdapterTest {
  @Test public void testUnionPlan() {
    CalciteAssert.model(JdbcTest.FOODMART_MODEL)
        .query("select * from \"sales_fact_1997\"\n"
            + "union all\n"
            + "select * from \"sales_fact_1998\"")
        .explainContains("PLAN=JdbcToEnumerableConverter\n"
            + "  JdbcUnion(all=[true])\n"
            + "    JdbcTableScan(table=[[foodmart, sales_fact_1997]])\n"
            + "    JdbcTableScan(table=[[foodmart, sales_fact_1998]])")
        .runs()
        .enable(CalciteAssert.DB == CalciteAssert.DatabaseInstance.HSQLDB)
        .planHasSql("SELECT *\n"
            + "FROM \"foodmart\".\"sales_fact_1997\"\n"
            + "UNION ALL\n"
            + "SELECT *\n"
            + "FROM \"foodmart\".\"sales_fact_1998\"");
  }

  @Test public void testFilterUnionPlan() {
    CalciteAssert.model(JdbcTest.FOODMART_MODEL)
        .query("select * from (\n"
            + "  select * from \"sales_fact_1997\"\n"
            + "  union all\n"
            + "  select * from \"sales_fact_1998\")\n"
            + "where \"product_id\" = 1")
        .runs()
        .enable(CalciteAssert.DB == CalciteAssert.DatabaseInstance.HSQLDB)
        .planHasSql("SELECT *\n"
            + "FROM \"foodmart\".\"sales_fact_1997\"\n"
            + "WHERE \"product_id\" = 1\n"
            + "UNION ALL\n"
            + "SELECT *\n"
            + "FROM \"foodmart\".\"sales_fact_1998\"\n"
            + "WHERE \"product_id\" = 1");
  }

  @Test public void testInPlan() {
    CalciteAssert.model(JdbcTest.FOODMART_MODEL)
        .query("select \"store_id\", \"store_name\" from \"store\"\n"
            + "where \"store_name\" in ('Store 1', 'Store 10', 'Store 11', 'Store 15', 'Store 16', 'Store 24', 'Store 3', 'Store 7')")
        .runs()
        .enable(CalciteAssert.DB == CalciteAssert.DatabaseInstance.HSQLDB)
        .planHasSql(
            "SELECT \"store_id\", \"store_name\"\n"
            + "FROM \"foodmart\".\"store\"\n"
            + "WHERE \"store_name\" = 'Store 1' OR \"store_name\" = 'Store 10' OR \"store_name\" = 'Store 11' OR \"store_name\" = 'Store 15' OR \"store_name\" = 'Store 16' OR \"store_name\" = 'Store 24' OR \"store_name\" = 'Store 3' OR \"store_name\" = 'Store 7'")
        .returns("store_id=1; store_name=Store 1\n"
            + "store_id=3; store_name=Store 3\n"
            + "store_id=7; store_name=Store 7\n"
            + "store_id=10; store_name=Store 10\n"
            + "store_id=11; store_name=Store 11\n"
            + "store_id=15; store_name=Store 15\n"
            + "store_id=16; store_name=Store 16\n"
            + "store_id=24; store_name=Store 24\n");
  }

  @Test public void testEquiJoinPlan() {
    CalciteAssert.model(JdbcTest.SCOTT_MODEL)
        .query("select empno, ename, e.deptno, dname \n"
            + "from scott.emp e inner join scott.dept d \n"
            + "on e.deptno = d.deptno")
        .explainContains("PLAN=JdbcToEnumerableConverter\n"
            + "  JdbcProject(EMPNO=[$2], ENAME=[$3], DEPTNO=[$4], DNAME=[$1])\n"
            + "    JdbcJoin(condition=[=($4, $0)], joinType=[inner])\n"
            + "      JdbcProject(DEPTNO=[$0], DNAME=[$1])\n"
            + "        JdbcTableScan(table=[[SCOTT, DEPT]])\n"
            + "      JdbcProject(EMPNO=[$0], ENAME=[$1], DEPTNO=[$7])\n"
            + "        JdbcTableScan(table=[[SCOTT, EMP]])")
        .runs()
        .enable(CalciteAssert.DB == CalciteAssert.DatabaseInstance.HSQLDB)
        .planHasSql("SELECT \"t0\".\"EMPNO\", \"t0\".\"ENAME\", "
            + "\"t0\".\"DEPTNO\", \"t\".\"DNAME\"\n"
            + "FROM (SELECT \"DEPTNO\", \"DNAME\"\n"
            + "FROM \"SCOTT\".\"DEPT\") AS \"t\"\n"
            + "INNER JOIN (SELECT \"EMPNO\", \"ENAME\", \"DEPTNO\"\n"
            + "FROM \"SCOTT\".\"EMP\") AS \"t0\" "
            + "ON \"t\".\"DEPTNO\" = \"t0\".\"DEPTNO\"");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-631">[CALCITE-631]
   * Push theta joins down to JDBC adapter</a>. */
  @Test public void testNonEquiJoinPlan() {
    CalciteAssert.model(JdbcTest.SCOTT_MODEL)
        .query("select empno, ename, grade \n"
            + "from scott.emp e inner join scott.salgrade s \n"
            + "on e.sal > s.losal and e.sal < s.hisal")
        .explainContains("PLAN=JdbcToEnumerableConverter\n"
            + "  JdbcProject(EMPNO=[$3], ENAME=[$4], GRADE=[$0])\n"
            + "    JdbcJoin(condition=[AND(>($5, $1), <($5, $2))], joinType=[inner])\n"
            + "      JdbcTableScan(table=[[SCOTT, SALGRADE]])\n"
            + "      JdbcProject(EMPNO=[$0], ENAME=[$1], SAL=[$5])\n"
            + "        JdbcTableScan(table=[[SCOTT, EMP]])")
        .runs()
        .enable(CalciteAssert.DB == CalciteAssert.DatabaseInstance.HSQLDB)
        .planHasSql("SELECT \"t\".\"EMPNO\", \"t\".\"ENAME\", "
            + "\"SALGRADE\".\"GRADE\"\nFROM \"SCOTT\".\"SALGRADE\"\n"
            + "INNER JOIN (SELECT \"EMPNO\", \"ENAME\", \"SAL\"\n"
            + "FROM \"SCOTT\".\"EMP\") AS \"t\" "
            + "ON \"SALGRADE\".\"LOSAL\" < \"t\".\"SAL\" "
            + "AND \"SALGRADE\".\"HISAL\" > \"t\".\"SAL\"");
  }

  @Test public void testNonEquiJoinReverseConditionPlan() {
    CalciteAssert.model(JdbcTest.SCOTT_MODEL)
        .query("select empno, ename, grade \n"
            + "from scott.emp e inner join scott.salgrade s \n"
            + "on s.losal <= e.sal and s.hisal >= e.sal")
        .explainContains("PLAN=JdbcToEnumerableConverter\n"
            + "  JdbcProject(EMPNO=[$3], ENAME=[$4], GRADE=[$0])\n"
            + "    JdbcJoin(condition=[AND(<=($1, $5), >=($2, $5))], joinType=[inner])\n"
            + "      JdbcTableScan(table=[[SCOTT, SALGRADE]])\n"
            + "      JdbcProject(EMPNO=[$0], ENAME=[$1], SAL=[$5])\n"
            + "        JdbcTableScan(table=[[SCOTT, EMP]])")
        .runs()
        .enable(CalciteAssert.DB == CalciteAssert.DatabaseInstance.HSQLDB)
        .planHasSql("SELECT \"t\".\"EMPNO\", \"t\".\"ENAME\", "
            + "\"SALGRADE\".\"GRADE\"\nFROM \"SCOTT\".\"SALGRADE\"\n"
            + "INNER JOIN (SELECT \"EMPNO\", \"ENAME\", \"SAL\"\n"
            + "FROM \"SCOTT\".\"EMP\") AS \"t\" "
            + "ON \"SALGRADE\".\"LOSAL\" <= \"t\".\"SAL\" AND \"SALGRADE\".\"HISAL\" >= \"t\".\"SAL\"");
  }

  @Test public void testMixedJoinPlan() {
    CalciteAssert.model(JdbcTest.SCOTT_MODEL)
        .query("select e.empno, e.ename, e.empno, e.ename  \n"
            + "from scott.emp e inner join scott.emp m on  \n"
            + "e.mgr = m.empno and e.sal > m.sal")
        .explainContains("PLAN=JdbcToEnumerableConverter\n"
            + "  JdbcProject(EMPNO=[$2], ENAME=[$3], EMPNO0=[$2], ENAME0=[$3])\n"
            + "    JdbcJoin(condition=[AND(=($4, $0), >($5, $1))], joinType=[inner])\n"
            + "      JdbcProject(EMPNO=[$0], SAL=[$5])\n"
            + "        JdbcTableScan(table=[[SCOTT, EMP]])\n"
            + "      JdbcProject(EMPNO=[$0], ENAME=[$1], MGR=[$3], SAL=[$5])\n"
            + "        JdbcTableScan(table=[[SCOTT, EMP]])")
        .runs()
        .enable(CalciteAssert.DB == CalciteAssert.DatabaseInstance.HSQLDB)
        .planHasSql("SELECT \"t0\".\"EMPNO\", \"t0\".\"ENAME\", "
            + "\"t0\".\"EMPNO\" AS \"EMPNO0\", \"t0\".\"ENAME\" AS \"ENAME0\"\n"
            + "FROM (SELECT \"EMPNO\", \"SAL\"\n"
            + "FROM \"SCOTT\".\"EMP\") AS \"t\"\n"
            + "INNER JOIN (SELECT \"EMPNO\", \"ENAME\", \"MGR\", \"SAL\"\n"
            + "FROM \"SCOTT\".\"EMP\") AS \"t0\" "
            + "ON \"t\".\"EMPNO\" = \"t0\".\"MGR\" AND \"t\".\"SAL\" < \"t0\".\"SAL\"");
  }

  @Test public void testMixedJoinWithOrPlan() {
    CalciteAssert.model(JdbcTest.SCOTT_MODEL)
        .query("select e.empno, e.ename, e.empno, e.ename  \n"
            + "from scott.emp e inner join scott.emp m on  \n"
            + "e.mgr = m.empno and (e.sal > m.sal or m.hiredate > e.hiredate)")
        .explainContains("PLAN=JdbcToEnumerableConverter\n"
            + "  JdbcProject(EMPNO=[$3], ENAME=[$4], EMPNO0=[$3], ENAME0=[$4])\n"
            + "    JdbcJoin(condition=[AND(=($5, $0), OR(>($7, $2), >($1, $6)))], joinType=[inner])\n"
            + "      JdbcProject(EMPNO=[$0], HIREDATE=[$4], SAL=[$5])\n"
            + "        JdbcTableScan(table=[[SCOTT, EMP]])\n"
            + "      JdbcProject(EMPNO=[$0], ENAME=[$1], MGR=[$3], HIREDATE=[$4], SAL=[$5])\n"
            + "        JdbcTableScan(table=[[SCOTT, EMP]])")
        .runs()
        .enable(CalciteAssert.DB == CalciteAssert.DatabaseInstance.HSQLDB)
        .planHasSql("SELECT \"t0\".\"EMPNO\", \"t0\".\"ENAME\", "
            + "\"t0\".\"EMPNO\" AS \"EMPNO0\", \"t0\".\"ENAME\" AS \"ENAME0\"\n"
            + "FROM (SELECT \"EMPNO\", \"HIREDATE\", \"SAL\"\n"
            + "FROM \"SCOTT\".\"EMP\") AS \"t\"\n"
            + "INNER JOIN (SELECT \"EMPNO\", \"ENAME\", \"MGR\", \"HIREDATE\", \"SAL\"\n"
            + "FROM \"SCOTT\".\"EMP\") AS \"t0\" "
            + "ON \"t\".\"EMPNO\" = \"t0\".\"MGR\" "
            + "AND (\"t\".\"SAL\" < \"t0\".\"SAL\" OR \"t\".\"HIREDATE\" > \"t0\".\"HIREDATE\")");
  }

  @Test public void testJoin3TablesPlan() {
    CalciteAssert.model(JdbcTest.SCOTT_MODEL)
        .query("select  empno, ename, dname, grade \n"
            + "from scott.emp e inner join scott.dept d \n"
            + "on e.deptno = d.deptno \n"
            + "inner join scott.salgrade s \n"
            + "on e.sal > s.losal and e.sal < s.hisal")
        .explainContains("PLAN=JdbcToEnumerableConverter\n"
            + "  JdbcProject(EMPNO=[$3], ENAME=[$4], DNAME=[$8], GRADE=[$0])\n"
            + "    JdbcJoin(condition=[AND(>($5, $1), <($5, $2))], joinType=[inner])\n"
            + "      JdbcTableScan(table=[[SCOTT, SALGRADE]])\n"
            + "      JdbcJoin(condition=[=($3, $4)], joinType=[inner])\n"
            + "        JdbcProject(EMPNO=[$0], ENAME=[$1], SAL=[$5], DEPTNO=[$7])\n"
            + "          JdbcTableScan(table=[[SCOTT, EMP]])\n"
            + "        JdbcProject(DEPTNO=[$0], DNAME=[$1])\n"
            + "          JdbcTableScan(table=[[SCOTT, DEPT]])")
        .runs()
        .enable(CalciteAssert.DB == CalciteAssert.DatabaseInstance.HSQLDB)
        .planHasSql("SELECT \"t\".\"EMPNO\", \"t\".\"ENAME\", "
            + "\"t0\".\"DNAME\", \"SALGRADE\".\"GRADE\"\n"
            + "FROM \"SCOTT\".\"SALGRADE\"\n"
            + "INNER JOIN ((SELECT \"EMPNO\", \"ENAME\", \"SAL\", \"DEPTNO\"\n"
            + "FROM \"SCOTT\".\"EMP\") AS \"t\"\n"
            + "INNER JOIN (SELECT \"DEPTNO\", \"DNAME\"\n"
            + "FROM \"SCOTT\".\"DEPT\") AS \"t0\" ON \"t\".\"DEPTNO\" = \"t0\".\"DEPTNO\")"
            + " ON \"SALGRADE\".\"LOSAL\" < \"t\".\"SAL\" AND \"SALGRADE\".\"HISAL\" > \"t\".\"SAL\"");
  }

  @Test public void testCrossJoinWithJoinKeyPlan() {
    CalciteAssert.model(JdbcTest.SCOTT_MODEL)
        .query("select empno, ename, d.deptno, dname \n"
            + "from scott.emp e,scott.dept d \n"
            + "where e.deptno = d.deptno")
        .explainContains("PLAN=JdbcToEnumerableConverter\n"
            + "  JdbcProject(EMPNO=[$0], ENAME=[$1], DEPTNO=[$3], DNAME=[$4])\n"
            + "    JdbcJoin(condition=[=($2, $3)], joinType=[inner])\n"
            + "      JdbcProject(EMPNO=[$0], ENAME=[$1], DEPTNO=[$7])\n"
            + "        JdbcTableScan(table=[[SCOTT, EMP]])\n"
            + "      JdbcProject(DEPTNO=[$0], DNAME=[$1])\n"
            + "        JdbcTableScan(table=[[SCOTT, DEPT]])")
        .runs()
        .enable(CalciteAssert.DB == CalciteAssert.DatabaseInstance.HSQLDB)
        .planHasSql("SELECT \"t\".\"EMPNO\", \"t\".\"ENAME\", "
            + "\"t0\".\"DEPTNO\", \"t0\".\"DNAME\"\n"
            + "FROM (SELECT \"EMPNO\", \"ENAME\", \"DEPTNO\"\nFROM \"SCOTT\".\"EMP\") AS \"t\"\n"
            + "INNER JOIN (SELECT \"DEPTNO\", \"DNAME\"\n"
            + "FROM \"SCOTT\".\"DEPT\") AS \"t0\" ON \"t\".\"DEPTNO\" = \"t0\".\"DEPTNO\"");
  }

  // JdbcJoin not used for this
  @Test public void testCartesianJoinWithoutKeyPlan() {
    CalciteAssert.model(JdbcTest.SCOTT_MODEL)
        .query("select empno, ename, d.deptno, dname \n"
            + "from scott.emp e,scott.dept d")
        .explainContains("PLAN=EnumerableJoin(condition=[true], "
            + "joinType=[inner])\n"
            + "  JdbcToEnumerableConverter\n"
            + "    JdbcProject(EMPNO=[$0], ENAME=[$1])\n"
            + "      JdbcTableScan(table=[[SCOTT, EMP]])\n"
            + "  JdbcToEnumerableConverter\n"
            + "    JdbcProject(DEPTNO=[$0], DNAME=[$1])\n"
            + "      JdbcTableScan(table=[[SCOTT, DEPT]])")
        .runs()
        .enable(CalciteAssert.DB == CalciteAssert.DatabaseInstance.HSQLDB);
  }

  @Test public void testCrossJoinWithJoinKeyAndFilterPlan() {
    CalciteAssert.model(JdbcTest.SCOTT_MODEL)
        .query("select empno, ename, d.deptno, dname \n"
            + "from scott.emp e,scott.dept d \n"
            + "where e.deptno = d.deptno \n"
            + "and e.deptno=20")
        .explainContains("PLAN=JdbcToEnumerableConverter\n"
            + "  JdbcProject(EMPNO=[$0], ENAME=[$1], DEPTNO=[$3], DNAME=[$4])\n"
            + "    JdbcJoin(condition=[=($2, $3)], joinType=[inner])\n"
            + "      JdbcProject(EMPNO=[$0], ENAME=[$1], DEPTNO=[$7])\n"
            + "        JdbcFilter(condition=[=(CAST($7):INTEGER, 20)])\n"
            + "          JdbcTableScan(table=[[SCOTT, EMP]])\n"
            + "      JdbcProject(DEPTNO=[$0], DNAME=[$1])\n"
            + "        JdbcTableScan(table=[[SCOTT, DEPT]])")
        .runs()
        .enable(CalciteAssert.DB == CalciteAssert.DatabaseInstance.HSQLDB)
        .planHasSql("SELECT \"t0\".\"EMPNO\", \"t0\".\"ENAME\", "
            + "\"t1\".\"DEPTNO\", \"t1\".\"DNAME\"\n"
            + "FROM (SELECT \"EMPNO\", \"ENAME\", \"DEPTNO\"\n"
            + "FROM \"SCOTT\".\"EMP\"\n"
            + "WHERE CAST(\"DEPTNO\" AS INTEGER) = 20) AS \"t0\"\n"
            + "INNER JOIN (SELECT \"DEPTNO\", \"DNAME\"\n"
            + "FROM \"SCOTT\".\"DEPT\") AS \"t1\" ON \"t0\".\"DEPTNO\" = \"t1\".\"DEPTNO\"");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-893">[CALCITE-893]
   * Theta join in JdbcAdapter</a>. */
  @Test public void testJoinPlan() {
    final String sql = "SELECT T1.\"brand_name\"\n"
        + "FROM \"foodmart\".\"product\" AS T1\n"
        + " INNER JOIN \"foodmart\".\"product_class\" AS T2\n"
        + " ON T1.\"product_class_id\" = T2.\"product_class_id\"\n"
        + "WHERE T2.\"product_department\" = 'Frozen Foods'\n"
        + " OR T2.\"product_department\" = 'Baking Goods'\n"
        + " AND T1.\"brand_name\" <> 'King'";
    CalciteAssert.model(JdbcTest.FOODMART_MODEL)
        .query(sql).runs()
        .returnsCount(275);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-657">[CALCITE-657]
   * NullPointerException when executing JdbcAggregate implement method</a>. */
  @Test public void testJdbcAggregate() throws Exception {
    final String url = MultiJdbcSchemaJoinTest.TempDb.INSTANCE.getUrl();
    Connection baseConnection = DriverManager.getConnection(url);
    Statement baseStmt = baseConnection.createStatement();
    baseStmt.execute("CREATE TABLE T2 (\n"
            + "ID INTEGER,\n"
            + "VALS INTEGER)");
    baseStmt.execute("INSERT INTO T2 VALUES (1, 1)");
    baseStmt.execute("INSERT INTO T2 VALUES (2, null)");
    baseStmt.close();
    baseConnection.commit();

    Properties info = new Properties();
    info.put("model",
        "inline:"
            + "{\n"
            + "  version: '1.0',\n"
            + "  defaultSchema: 'BASEJDBC',\n"
            + "  schemas: [\n"
            + "     {\n"
            + "       type: 'jdbc',\n"
            + "       name: 'BASEJDBC',\n"
            + "       jdbcDriver: '" + jdbcDriver.class.getName() + "',\n"
            + "       jdbcUrl: '" + url + "',\n"
            + "       jdbcCatalog: null,\n"
            + "       jdbcSchema: null\n"
            + "     }\n"
            + "  ]\n"
            + "}");

    final Connection calciteConnection =
        DriverManager.getConnection("jdbc:calcite:", info);
    ResultSet rs = calciteConnection
        .prepareStatement("select 10 * count(ID) from t2").executeQuery();

    assertThat(rs.next(), is(true));
    assertThat((Long) rs.getObject(1), equalTo(20L));
    assertThat(rs.next(), is(false));

    rs.close();
    calciteConnection.close();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-259">[CALCITE-259]
   * Using sub-queries in CASE statement against JDBC tables generates invalid
   * Oracle SQL</a>. */
  @Test public void testSubQueryWithSingleValue() {
    final String expected;
    switch (CalciteAssert.DB) {
    case MYSQL:
      expected = "Subquery returns more than 1 row";
      break;
    default:
      expected = "more than one value in agg SINGLE_VALUE";
    }
    CalciteAssert.model(JdbcTest.FOODMART_MODEL)
        .query("SELECT \"full_name\" FROM \"employee\" WHERE "
                + "\"employee_id\" = (SELECT \"employee_id\" FROM \"salary\")")
        .explainContains("SINGLE_VALUE")
        .throws_(expected);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-865">[CALCITE-865]
   * Unknown table type causes NullPointerException in JdbcSchema</a>. The issue
   * occurred because of the "SYSTEM_INDEX" table type when run against
   * PostgreSQL. */
  @Test public void testMetadataTables() throws Exception {
    // The troublesome tables occur in PostgreSQL's system schema.
    final String model =
        JdbcTest.FOODMART_MODEL.replace("jdbcSchema: 'foodmart'",
            "jdbcSchema: null");
    CalciteAssert.model(
        model)
        .doWithConnection(
            new Function<CalciteConnection, Void>() {
              public Void apply(CalciteConnection connection) {
                try {
                  final ResultSet resultSet =
                      connection.getMetaData().getTables(null, null, "%", null);
                  assertFalse(CalciteAssert.toString(resultSet).isEmpty());
                  return null;
                } catch (SQLException e) {
                  throw Throwables.propagate(e);
                }
              }
            });
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-666">[CALCITE-666]
   * Anti-semi-joins against JDBC adapter give wrong results</a>. */
  @Test public void testScalarSubquery() {
    CalciteAssert.model(JdbcTest.SCOTT_MODEL)
        .query("SELECT COUNT(empno) AS cEmpNo FROM \"SCOTT\".\"EMP\" "
            + "WHERE DEPTNO <> (SELECT * FROM (VALUES 1))")
        .enable(CalciteAssert.DB == CalciteAssert.DatabaseInstance.HSQLDB)
        .returns("CEMPNO=14\n");

    CalciteAssert.model(JdbcTest.SCOTT_MODEL)
        .query("SELECT ename FROM \"SCOTT\".\"EMP\" "
            + "WHERE DEPTNO = (SELECT deptno FROM \"SCOTT\".\"DEPT\" "
            + "WHERE dname = 'ACCOUNTING')")
        .enable(CalciteAssert.DB == CalciteAssert.DatabaseInstance.HSQLDB)
        .returns("ENAME=CLARK\nENAME=KING\nENAME=MILLER\n");

    CalciteAssert.model(JdbcTest.SCOTT_MODEL)
        .query("SELECT COUNT(ename) AS cEname FROM \"SCOTT\".\"EMP\" "
            + "WHERE DEPTNO > (SELECT deptno FROM \"SCOTT\".\"DEPT\" "
            + "WHERE dname = 'ACCOUNTING')")
        .enable(CalciteAssert.DB == CalciteAssert.DatabaseInstance.HSQLDB)
        .returns("CENAME=11\n");

    CalciteAssert.model(JdbcTest.SCOTT_MODEL)
        .query("SELECT COUNT(ename) AS cEname FROM \"SCOTT\".\"EMP\" "
            + "WHERE DEPTNO < (SELECT deptno FROM \"SCOTT\".\"DEPT\" "
            + "WHERE dname = 'ACCOUNTING')")
        .enable(CalciteAssert.DB == CalciteAssert.DatabaseInstance.HSQLDB)
        .returns("CENAME=0\n");
  }
}

// End JdbcAdapterTest.java
