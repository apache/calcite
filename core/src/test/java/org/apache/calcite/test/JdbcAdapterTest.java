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

import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.test.CalciteAssert.AssertThat;
import org.apache.calcite.test.CalciteAssert.DatabaseInstance;
import org.apache.calcite.test.schemata.foodmart.FoodmartSchema;
import org.apache.calcite.test.schemata.hr.HrSchema;
import org.apache.calcite.util.Smalls;
import org.apache.calcite.util.TestUtil;

import org.hsqldb.jdbcDriver;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Tests for the {@code org.apache.calcite.adapter.jdbc} package.
 */
class JdbcAdapterTest {

  /** Ensures that tests that are modifying data (doing DML) do not run at the
   * same time. */
  private static final ReentrantLock LOCK = new ReentrantLock();

  /** VALUES is pushed down. */
  @Test void testValuesPlan() {
    final String sql = "select * from \"days\", (values 1, 2) as t(c)";
    final String explain = "PLAN=JdbcToEnumerableConverter\n"
        + "  JdbcJoin(condition=[true], joinType=[inner])\n"
        + "    JdbcTableScan(table=[[foodmart, days]])\n"
        + "    JdbcValues(tuples=[[{ 1 }, { 2 }]])";
    final String jdbcSql = "SELECT *\n"
        + "FROM \"foodmart\".\"days\",\n"
        + "(VALUES (1),\n"
        + "(2)) AS \"t\" (\"C\")";
    CalciteAssert.model(FoodmartSchema.FOODMART_MODEL)
        .query(sql)
        .explainContains(explain)
        .runs()
        .enable(CalciteAssert.DB == CalciteAssert.DatabaseInstance.HSQLDB
            || CalciteAssert.DB == DatabaseInstance.POSTGRESQL)
        .planHasSql(jdbcSql)
        .returnsCount(14);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6462">[CALCITE-6462]
   * VolcanoPlanner internal valid may throw exception when log trace is enabled</a>. */
  @Test void testVolcanoPlannerInternalValid() {
    CalciteAssert.model(JdbcTest.SCOTT_MODEL)
        .query("select *\n"
            + "from scott.emp e left join scott.dept d\n"
            + "on 'job' in (select job from scott.bonus b)")
        .explainContains("PLAN=JdbcToEnumerableConverter\n"
            + "  JdbcProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5], COMM=[$6], DEPTNO=[$7], DEPTNO0=[$8], DNAME=[$9], LOC=[$10])\n"
            + "    JdbcJoin(condition=[true], joinType=[left])\n"
            + "      JdbcTableScan(table=[[SCOTT, EMP]])\n"
            + "      JdbcJoin(condition=[true], joinType=[inner])\n"
            + "        JdbcTableScan(table=[[SCOTT, DEPT]])\n"
            + "        JdbcAggregate(group=[{0}])\n"
            + "          JdbcProject(cs=[true])\n"
            + "            JdbcFilter(condition=[=('job', $1)])\n"
            + "              JdbcTableScan(table=[[SCOTT, BONUS]])\n\n")
        .runs()
        .enable(CalciteAssert.DB == DatabaseInstance.HSQLDB)
        .returnsCount(14);
  }

  @Test void testUnionPlan() {
    CalciteAssert.model(FoodmartSchema.FOODMART_MODEL)
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

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3115">[CALCITE-3115]
   * Cannot add JdbcRules which have different JdbcConvention
   * to same VolcanoPlanner's RuleSet</a>. */
  @Test void testUnionPlan2() {
    CalciteAssert.model(JdbcTest.FOODMART_SCOTT_MODEL)
        .query("select \"store_name\" from \"foodmart\".\"store\" where \"store_id\" < 10\n"
            + "union all\n"
            + "select ename from SCOTT.emp where empno > 10")
        .explainContains("PLAN=EnumerableUnion(all=[true])\n"
            + "  JdbcToEnumerableConverter\n"
            + "    JdbcProject(store_name=[$3])\n"
            + "      JdbcFilter(condition=[<($0, 10)])\n"
            + "        JdbcTableScan(table=[[foodmart, store]])\n"
            + "  JdbcToEnumerableConverter\n"
            + "    JdbcProject(ENAME=[CAST($1):VARCHAR(30)])\n"
            + "      JdbcFilter(condition=[>(CAST($0):INTEGER NOT NULL, 10)])\n"
            + "        JdbcTableScan(table=[[SCOTT, EMP]])")
        .runs()
        .enable(CalciteAssert.DB == CalciteAssert.DatabaseInstance.HSQLDB)
        .planHasSql("SELECT \"store_name\"\n"
                + "FROM \"foodmart\".\"store\"\n"
                + "WHERE \"store_id\" < 10")
        .planHasSql("SELECT CAST(\"ENAME\" AS VARCHAR(30)) AS \"ENAME\"\n"
            + "FROM \"SCOTT\".\"EMP\"\n"
            + "WHERE CAST(\"EMPNO\" AS INTEGER) > 10");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6268">[CALCITE-6268]
   * Support implementing custom JdbcSchema</a>. */
  @Test void testCustomJdbc() {
    CalciteAssert.model(JdbcTest.FOODMART_SCOTT_CUSTOM_MODEL)
        .query("select * from SCOTT.emp\n")
        .enable(CalciteAssert.DB == CalciteAssert.DatabaseInstance.HSQLDB)
        .planHasSql("SELECT *\nFROM \"SCOTT\".\"EMP\"")
        .returnsCount(14);
  }

  @Test void testFilterUnionPlan() {
    CalciteAssert.model(FoodmartSchema.FOODMART_MODEL)
        .query("select * from (\n"
            + "  select * from \"sales_fact_1997\"\n"
            + "  union all\n"
            + "  select * from \"sales_fact_1998\")\n"
            + "where \"product_id\" = 1")
        .runs()
        .enable(CalciteAssert.DB == CalciteAssert.DatabaseInstance.HSQLDB)
        .planHasSql("SELECT 1 AS \"product_id\", \"time_id\", \"customer_id\", "
            + "\"promotion_id\", \"store_id\", \"store_sales\", "
            + "\"store_cost\", \"unit_sales\"\n"
            + "FROM (SELECT \"time_id\", \"customer_id\", \"promotion_id\", \"store_id\", "
            + "\"store_sales\", \"store_cost\", \"unit_sales\"\n"
            + "FROM \"foodmart\".\"sales_fact_1997\"\n"
            + "WHERE \"product_id\" = 1\n"
            + "UNION ALL\n"
            + "SELECT \"time_id\", \"customer_id\", \"promotion_id\", \"store_id\", "
            + "\"store_sales\", \"store_cost\", \"unit_sales\"\n"
            + "FROM \"foodmart\".\"sales_fact_1998\"\n"
            + "WHERE \"product_id\" = 1) AS \"t3\"");
  }

  @Test void testInPlan() {
    CalciteAssert.model(FoodmartSchema.FOODMART_MODEL)
        .query("select \"store_id\", \"store_name\" from \"store\"\n"
            + "where \"store_name\" in ('Store 1', 'Store 10', 'Store 11', 'Store 15', 'Store 16', 'Store 24', 'Store 3', 'Store 7')")
        .runs()
        .enable(CalciteAssert.DB == CalciteAssert.DatabaseInstance.HSQLDB)
        .planHasSql("SELECT \"store_id\", \"store_name\"\n"
            + "FROM \"foodmart\".\"store\"\n"
            + "WHERE \"store_name\" IN ('Store 1', 'Store 10', 'Store 11',"
            + " 'Store 15', 'Store 16', 'Store 24', 'Store 3', 'Store 7')")
        .returns("store_id=1; store_name=Store 1\n"
            + "store_id=3; store_name=Store 3\n"
            + "store_id=7; store_name=Store 7\n"
            + "store_id=10; store_name=Store 10\n"
            + "store_id=11; store_name=Store 11\n"
            + "store_id=15; store_name=Store 15\n"
            + "store_id=16; store_name=Store 16\n"
            + "store_id=24; store_name=Store 24\n");
  }

  @Test void testEquiJoinPlan() {
    CalciteAssert.model(JdbcTest.SCOTT_MODEL)
        .query("select empno, ename, e.deptno, dname\n"
            + "from scott.emp e inner join scott.dept d\n"
            + "on e.deptno = d.deptno")
        .explainContains("PLAN=JdbcToEnumerableConverter\n"
            + "  JdbcProject(EMPNO=[$0], ENAME=[$1], DEPTNO=[$2], DNAME=[$4])\n"
            + "    JdbcJoin(condition=[=($2, $3)], joinType=[inner])\n"
            + "      JdbcProject(EMPNO=[$0], ENAME=[$1], DEPTNO=[$7])\n"
            + "        JdbcTableScan(table=[[SCOTT, EMP]])\n"
            + "      JdbcProject(DEPTNO=[$0], DNAME=[$1])\n"
            + "        JdbcTableScan(table=[[SCOTT, DEPT]])")
        .runs()
        .enable(CalciteAssert.DB == CalciteAssert.DatabaseInstance.HSQLDB)
        .planHasSql("SELECT \"t\".\"EMPNO\", \"t\".\"ENAME\", "
            + "\"t\".\"DEPTNO\", \"t0\".\"DNAME\"\n"
            + "FROM (SELECT \"EMPNO\", \"ENAME\", \"DEPTNO\"\n"
            + "FROM \"SCOTT\".\"EMP\") AS \"t\"\n"
            + "INNER JOIN (SELECT \"DEPTNO\", \"DNAME\"\n"
            + "FROM \"SCOTT\".\"DEPT\") AS \"t0\" "
            + "ON \"t\".\"DEPTNO\" = \"t0\".\"DEPTNO\"");
  }

  @Test void testPushDownSort() {
    CalciteAssert.model(JdbcTest.SCOTT_MODEL)
        .with(CalciteConnectionProperty.TOPDOWN_OPT.camelName(), false)
        .query("select ename\n"
            + "from scott.emp\n"
            + "order by empno")
        .explainContains("PLAN=JdbcToEnumerableConverter\n"
            + "  JdbcSort(sort0=[$1], dir0=[ASC])\n"
            + "    JdbcProject(ENAME=[$1], EMPNO=[$0])\n"
            + "      JdbcTableScan(table=[[SCOTT, EMP]])")
        .runs()
        .enable(CalciteAssert.DB == CalciteAssert.DatabaseInstance.HSQLDB)
        .planHasSql("SELECT \"ENAME\", \"EMPNO\"\n"
            + "FROM \"SCOTT\".\"EMP\"\n"
            + "ORDER BY \"EMPNO\" NULLS LAST");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6401">[CALCITE-6401]
   * JdbcAdapter cannot push down NOT IN sub-queries</a>. */
  @Test void testPushDownNotIn() {
    CalciteAssert.model(JdbcTest.SCOTT_MODEL)
        .query("select * from dept where deptno not in (select deptno from emp)")
        .explainContains("PLAN=JdbcToEnumerableConverter\n"
            + "  JdbcProject(DEPTNO=[$0], DNAME=[$1], LOC=[$2])\n"
            + "    JdbcFilter(condition=[OR(=($3, 0), AND(IS NULL($6), >=($4, $3)))])\n"
            + "      JdbcJoin(condition=[=($0, $5)], joinType=[left])\n"
            + "        JdbcJoin(condition=[true], joinType=[inner])\n"
            + "          JdbcTableScan(table=[[SCOTT, DEPT]])\n"
            + "          JdbcAggregate(group=[{}], c=[COUNT()], ck=[COUNT($7)])\n"
            + "            JdbcTableScan(table=[[SCOTT, EMP]])\n"
            + "        JdbcAggregate(group=[{7}], i=[LITERAL_AGG(true)])\n"
            + "          JdbcTableScan(table=[[SCOTT, EMP]])\n\n")
        .runs()
        .enable(CalciteAssert.DB == CalciteAssert.DatabaseInstance.HSQLDB)
        .planHasSql("SELECT \"DEPT\".\"DEPTNO\", \"DEPT\".\"DNAME\", \"DEPT\".\"LOC\"\n"
            + "FROM \"SCOTT\".\"DEPT\"\nCROSS JOIN (SELECT COUNT(*) AS \"c\", COUNT(\"DEPTNO\") AS \"ck\"\n"
            + "FROM \"SCOTT\".\"EMP\") AS \"t\"\n"
            + "LEFT JOIN (SELECT \"DEPTNO\", TRUE AS \"i\"\n"
            + "FROM \"SCOTT\".\"EMP\"\nGROUP BY \"DEPTNO\") AS \"t0\" ON \"DEPT\".\"DEPTNO\" = \"t0\".\"DEPTNO\"\n"
            + "WHERE \"t\".\"c\" = 0 OR \"t0\".\"i\" IS NULL AND \"t\".\"ck\" >= \"t\".\"c\"");
  }

  @Test void testNotPushDownNotIn() {
    CalciteAssert.model(JdbcTest.SCOTT_MODEL)
        .query("select * from dept where (deptno, dname) not in (select deptno, ename from emp)")
        .explainContains("PLAN=EnumerableCalc(expr#0..7=[{inputs}], expr#8=[0], "
            + "expr#9=[=($t3, $t8)], expr#10=[IS NULL($t7)], expr#11=[>=($t4, $t3)], "
            + "expr#12=[IS NOT NULL($t1)], expr#13=[AND($t10, $t11, $t12)], "
            + "expr#14=[OR($t9, $t13)], proj#0..2=[{exprs}], $condition=[$t14])\n"
            + "  EnumerableMergeJoin(condition=[AND(=($0, $5), =($1, $6))], joinType=[left])\n"
            + "    EnumerableSort(sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[ASC])\n"
            + "      EnumerableNestedLoopJoin(condition=[true], joinType=[inner])\n"
            + "        JdbcToEnumerableConverter\n"
            + "          JdbcTableScan(table=[[SCOTT, DEPT]])\n"
            + "        EnumerableAggregate(group=[{}], c=[COUNT()], ck=[COUNT() FILTER $0])\n"
            + "          JdbcToEnumerableConverter\n"
            + "            JdbcProject($f2=[OR(IS NOT NULL($7), IS NOT NULL($1))])\n"
            + "              JdbcTableScan(table=[[SCOTT, EMP]])\n"
            + "    JdbcToEnumerableConverter\n"
            + "      JdbcSort(sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[ASC])\n"
            + "        JdbcAggregate(group=[{0, 1}], i=[LITERAL_AGG(true)])\n"
            + "          JdbcProject(DEPTNO=[$7], ENAME=[CAST($1):VARCHAR(14)])\n"
            + "            JdbcTableScan(table=[[SCOTT, EMP]])\n\n");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6401">[CALCITE-6401]
   * JDBC adapter cannot push down JOIN with condition
   * includes IS TRUE、IS NULL、Dynamic parameter、CAST、Literal comparison</a>. */
  @Test void testJoinConditionPushDownIsTrue() {
    CalciteAssert.model(JdbcTest.SCOTT_MODEL)
        .query("select emp1.empno\n"
            + "from scott.emp as emp1 join scott.emp as emp2 on\n"
            + "(emp1.empno = emp2.empno) is true\n"
            + " and (emp1.ename = emp2.ename) is not true")
        .explainContains("PLAN=JdbcToEnumerableConverter\n"
            + "  JdbcProject(EMPNO=[$0])\n"
            + "    JdbcJoin(condition=[AND(=($0, $2), IS NOT TRUE(=($1, $3)))], joinType=[inner])\n"
            + "      JdbcProject(EMPNO=[$0], ENAME=[$1])\n"
            + "        JdbcTableScan(table=[[SCOTT, EMP]])\n"
            + "      JdbcProject(EMPNO=[$0], ENAME=[$1])\n"
            + "        JdbcTableScan(table=[[SCOTT, EMP]])\n\n")
        .runs()
        .enable(CalciteAssert.DB == CalciteAssert.DatabaseInstance.HSQLDB)
        .planHasSql("SELECT \"t\".\"EMPNO\"\n"
            + "FROM (SELECT \"EMPNO\", \"ENAME\"\n"
            + "FROM \"SCOTT\".\"EMP\") AS \"t\"\n"
            + "INNER JOIN (SELECT \"EMPNO\", \"ENAME\"\n"
            + "FROM \"SCOTT\".\"EMP\") AS \"t0\" ON \"t\".\"EMPNO\" = \"t0\".\"EMPNO\" AND \"t\".\"ENAME\" = \"t0\".\"ENAME\" IS NOT TRUE");
  }

  @Test void testJoinConditionPushDownIsFalse() {
    CalciteAssert.model(JdbcTest.SCOTT_MODEL)
        .query("select emp1.empno\n"
            + "from scott.emp as emp1 join scott.emp as emp2 on\n"
            + "(emp1.empno = emp2.empno) is false\n"
            + " and (emp1.ename = emp2.ename) is not false")
        .explainContains("PLAN=JdbcToEnumerableConverter\n"
            + "  JdbcProject(EMPNO=[$0])\n"
            + "    JdbcJoin(condition=[AND(<>($0, $2), IS NOT FALSE(=($1, $3)))], joinType=[inner])\n"
            + "      JdbcProject(EMPNO=[$0], ENAME=[$1])\n"
            + "        JdbcTableScan(table=[[SCOTT, EMP]])\n"
            + "      JdbcProject(EMPNO=[$0], ENAME=[$1])\n"
            + "        JdbcTableScan(table=[[SCOTT, EMP]])\n\n")
        .runs()
        .enable(CalciteAssert.DB == CalciteAssert.DatabaseInstance.HSQLDB)
        .planHasSql("SELECT \"t\".\"EMPNO\"\n"
            + "FROM (SELECT \"EMPNO\", \"ENAME\"\n"
            + "FROM \"SCOTT\".\"EMP\") AS \"t\"\n"
            + "INNER JOIN (SELECT \"EMPNO\", \"ENAME\"\n"
            + "FROM \"SCOTT\".\"EMP\") AS \"t0\" ON \"t\".\"EMPNO\" <> \"t0\".\"EMPNO\" AND \"t\".\"ENAME\" = \"t0\".\"ENAME\" IS NOT FALSE");
  }

  @Test void testJoinConditionPushDownIsNull() {
    CalciteAssert.model(JdbcTest.SCOTT_MODEL)
        .query("select emp1.empno\n"
            + "from scott.emp as emp1 join scott.emp as emp2 on\n"
            + "(emp1.empno = emp2.empno or emp1.ename is null)\n"
            + " and (emp1.ename = emp2.ename or emp2.empno is null)")
        .explainContains("PLAN=JdbcToEnumerableConverter\n"
            + "  JdbcProject(EMPNO=[$0])\n"
            + "    JdbcJoin(condition=[AND(OR(IS NULL($1), =($0, $2)), =($1, $3))], joinType=[inner])\n"
            + "      JdbcProject(EMPNO=[$0], ENAME=[$1])\n"
            + "        JdbcTableScan(table=[[SCOTT, EMP]])\n"
            + "      JdbcProject(EMPNO=[$0], ENAME=[$1])\n"
            + "        JdbcTableScan(table=[[SCOTT, EMP]])\n\n")
        .runs()
        .enable(CalciteAssert.DB == CalciteAssert.DatabaseInstance.HSQLDB)
        .planHasSql("SELECT \"t\".\"EMPNO\"\n"
            + "FROM (SELECT \"EMPNO\", \"ENAME\"\n"
            + "FROM \"SCOTT\".\"EMP\") AS \"t\"\n"
            + "INNER JOIN (SELECT \"EMPNO\", \"ENAME\"\n"
            + "FROM \"SCOTT\".\"EMP\") AS \"t0\" ON (\"t\".\"ENAME\" IS NULL OR \"t\".\"EMPNO\" = \"t0\".\"EMPNO\") AND \"t\".\"ENAME\" = \"t0\".\"ENAME\"");
  }

  @Test void testJoinConditionPushDownIsNotNull() {
    CalciteAssert.model(JdbcTest.SCOTT_MODEL)
        .query("select emp1.empno\n"
            + "from scott.emp as emp1 join scott.emp as emp2 on\n"
            + "(emp1.empno = emp2.empno and emp1.ename is not null)\n"
            + " or (emp1.ename = emp2.ename and emp2.empno is not null)")
        .explainContains("PLAN=JdbcToEnumerableConverter\n"
            + "  JdbcProject(EMPNO=[$0])\n"
            + "    JdbcJoin(condition=[OR(AND(=($0, $2), IS NOT NULL($1)), =($1, $3))], joinType=[inner])\n"
            + "      JdbcProject(EMPNO=[$0], ENAME=[$1])\n"
            + "        JdbcTableScan(table=[[SCOTT, EMP]])\n"
            + "      JdbcProject(EMPNO=[$0], ENAME=[$1])\n"
            + "        JdbcTableScan(table=[[SCOTT, EMP]])\n\n")
        .runs()
        .enable(CalciteAssert.DB == CalciteAssert.DatabaseInstance.HSQLDB)
        .planHasSql("SELECT \"t\".\"EMPNO\"\n"
            + "FROM (SELECT \"EMPNO\", \"ENAME\"\n"
            + "FROM \"SCOTT\".\"EMP\") AS \"t\"\n"
            + "INNER JOIN (SELECT \"EMPNO\", \"ENAME\"\n"
            + "FROM \"SCOTT\".\"EMP\") AS \"t0\" ON \"t\".\"EMPNO\" = \"t0\".\"EMPNO\" AND \"t\".\"ENAME\" IS NOT NULL OR \"t\".\"ENAME\" = \"t0\".\"ENAME\"");
  }

  @Test void testJoinConditionPushDownLiteral() {
    CalciteAssert.model(JdbcTest.SCOTT_MODEL)
        .query("select emp1.empno\n"
            + "from scott.emp as emp1 join scott.emp as emp2 on\n"
            + "(emp1.empno = emp2.empno and emp1.ename = 'empename') or\n"
            + "(emp1.ename = emp2.ename and emp2.empno = 5)")
        .explainContains("PLAN=JdbcToEnumerableConverter\n"
            + "  JdbcProject(EMPNO=[$0])\n"
            + "    JdbcJoin(condition=[OR(AND(=($0, $2), =($1, 'empename')), AND(=($1, $3), =(CAST($2):INTEGER NOT NULL, 5)))], joinType=[inner])\n"
            + "      JdbcProject(EMPNO=[$0], ENAME=[$1])\n"
            + "        JdbcTableScan(table=[[SCOTT, EMP]])\n"
            + "      JdbcProject(EMPNO=[$0], ENAME=[$1])\n"
            + "        JdbcTableScan(table=[[SCOTT, EMP]])\n\n")
        .runs()
        .enable(CalciteAssert.DB == CalciteAssert.DatabaseInstance.HSQLDB)
        .planHasSql("SELECT \"t\".\"EMPNO\"\n"
            + "FROM (SELECT \"EMPNO\", \"ENAME\"\nFROM \"SCOTT\".\"EMP\") AS \"t\"\n"
            + "INNER JOIN (SELECT \"EMPNO\", \"ENAME\"\n"
            + "FROM \"SCOTT\".\"EMP\") AS \"t0\" ON \"t\".\"EMPNO\" = \"t0\".\"EMPNO\" AND \"t\".\"ENAME\" = 'empename' OR \"t\".\"ENAME\" = \"t0\".\"ENAME\" AND CAST(\"t0\".\"EMPNO\" AS INTEGER) = 5");
  }

  @Test void testJoinConditionPushDownCast() {
    CalciteAssert.model(JdbcTest.SCOTT_MODEL)
        .query("select emp1.empno\n"
            + "from scott.emp as emp1 join scott.emp as emp2 on\n"
            + "(emp1.empno = emp2.empno and emp1.ename = 'empename') or\n"
            + "(emp1.ename = emp2.ename and emp2.empno = 5)")
        .explainContains("PLAN=JdbcToEnumerableConverter\n"
            + "  JdbcProject(EMPNO=[$0])\n"
            + "    JdbcJoin(condition=[OR(AND(=($0, $2), =($1, 'empename')), AND(=($1, $3), =(CAST($2):INTEGER NOT NULL, 5)))], joinType=[inner])\n"
            + "      JdbcProject(EMPNO=[$0], ENAME=[$1])\n"
            + "        JdbcTableScan(table=[[SCOTT, EMP]])\n"
            + "      JdbcProject(EMPNO=[$0], ENAME=[$1])\n"
            + "        JdbcTableScan(table=[[SCOTT, EMP]])\n\n")
        .runs()
        .enable(CalciteAssert.DB == CalciteAssert.DatabaseInstance.HSQLDB)
        .planHasSql("SELECT \"t\".\"EMPNO\"\n"
            + "FROM (SELECT \"EMPNO\", \"ENAME\"\n"
            + "FROM \"SCOTT\".\"EMP\") AS \"t\"\n"
            + "INNER JOIN (SELECT \"EMPNO\", \"ENAME\"\n"
            + "FROM \"SCOTT\".\"EMP\") AS \"t0\" ON \"t\".\"EMPNO\" = \"t0\".\"EMPNO\" AND \"t\".\"ENAME\" = 'empename' OR \"t\".\"ENAME\" = \"t0\".\"ENAME\" AND CAST(\"t0\".\"EMPNO\" AS INTEGER) = 5");
  }

  @Test void testJoinConditionPushDownDynamicParam() {
    CalciteAssert.model(JdbcTest.SCOTT_MODEL)
        .query("select emp1.empno\n"
            + "from scott.emp as emp1 join scott.emp as emp2 on\n"
            + "(emp1.empno = emp2.empno and emp1.ename = 'empename') or\n"
            + "(emp1.ename = emp2.ename and emp2.empno = ?)")
        .consumesPreparedStatement(p -> p.setInt(1, 5))
        .explainContains("PLAN=JdbcToEnumerableConverter\n"
            + "  JdbcProject(EMPNO=[$0])\n"
            + "    JdbcJoin(condition=[OR(AND(=($0, $2), =($1, 'empename')), AND(=($1, $3), =($2, ?0)))], joinType=[inner])\n"
            + "      JdbcProject(EMPNO=[$0], ENAME=[$1])\n"
            + "        JdbcTableScan(table=[[SCOTT, EMP]])\n"
            + "      JdbcProject(EMPNO=[$0], ENAME=[$1])\n"
            + "        JdbcTableScan(table=[[SCOTT, EMP]])\n\n")
        .runs()
        .enable(CalciteAssert.DB == CalciteAssert.DatabaseInstance.HSQLDB)
        .planHasSql("SELECT \"t\".\"EMPNO\"\n"
            + "FROM (SELECT \"EMPNO\", \"ENAME\"\n"
            + "FROM \"SCOTT\".\"EMP\") AS \"t\"\n"
            + "INNER JOIN (SELECT \"EMPNO\", \"ENAME\"\n"
            + "FROM \"SCOTT\".\"EMP\") AS \"t0\" ON \"t\".\"EMPNO\" = \"t0\".\"EMPNO\" AND \"t\".\"ENAME\" = 'empename' OR \"t\".\"ENAME\" = \"t0\".\"ENAME\" AND \"t0\".\"EMPNO\" = ?");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3751">[CALCITE-3751]
   * JDBC adapter wrongly pushes ORDER BY into sub-query</a>. */
  @Test void testOrderByPlan() {
    final String sql = "select deptno, job, sum(sal)\n"
        + "from \"EMP\"\n"
        + "group by deptno, job\n"
        + "order by 1, 2";
    final String explain = "PLAN=JdbcToEnumerableConverter\n"
        + "  JdbcSort(sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[ASC])\n"
        + "    JdbcProject(DEPTNO=[$1], JOB=[$0], EXPR$2=[$2])\n"
        + "      JdbcAggregate(group=[{2, 7}], EXPR$2=[SUM($5)])\n"
        + "        JdbcTableScan(table=[[SCOTT, EMP]])";
    final String sqlHsqldb = "SELECT \"DEPTNO\", \"JOB\", SUM(\"SAL\")\n"
        + "FROM \"SCOTT\".\"EMP\"\n"
        + "GROUP BY \"JOB\", \"DEPTNO\"\n"
        + "ORDER BY \"DEPTNO\" NULLS LAST, \"JOB\" NULLS LAST";
    CalciteAssert.model(JdbcTest.SCOTT_MODEL)
        .with(CalciteConnectionProperty.TOPDOWN_OPT.camelName(), false)
        .query(sql)
        .explainContains(explain)
        .runs()
        .enable(CalciteAssert.DB == CalciteAssert.DatabaseInstance.HSQLDB)
        .planHasSql(sqlHsqldb);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-631">[CALCITE-631]
   * Push theta joins down to JDBC adapter</a>. */
  @Test void testNonEquiJoinPlan() {
    CalciteAssert.model(JdbcTest.SCOTT_MODEL)
        .query("select empno, ename, grade\n"
            + "from scott.emp e inner join scott.salgrade s\n"
            + "on e.sal > s.losal and e.sal < s.hisal")
        .explainContains("PLAN=JdbcToEnumerableConverter\n"
            + "  JdbcProject(EMPNO=[$0], ENAME=[$1], GRADE=[$3])\n"
            + "    JdbcJoin(condition=[AND(>($2, $4), <($2, $5))], joinType=[inner])\n"
            + "      JdbcProject(EMPNO=[$0], ENAME=[$1], SAL=[$5])\n"
            + "        JdbcTableScan(table=[[SCOTT, EMP]])\n"
            + "      JdbcTableScan(table=[[SCOTT, SALGRADE]])")
        .runs()
        .enable(CalciteAssert.DB == CalciteAssert.DatabaseInstance.HSQLDB)
        .planHasSql("SELECT \"t\".\"EMPNO\", \"t\".\"ENAME\", \"SALGRADE\".\"GRADE\"\n"
            + "FROM (SELECT \"EMPNO\", \"ENAME\", \"SAL\"\n"
            + "FROM \"SCOTT\".\"EMP\") AS \"t\"\n"
            + "INNER JOIN \"SCOTT\".\"SALGRADE\" "
            + "ON \"t\".\"SAL\" > \"SALGRADE\".\"LOSAL\" "
            + "AND \"t\".\"SAL\" < \"SALGRADE\".\"HISAL\"");
  }

  @Test void testNonEquiJoinReverseConditionPlan() {
    CalciteAssert.model(JdbcTest.SCOTT_MODEL)
        .query("select empno, ename, grade\n"
            + "from scott.emp e inner join scott.salgrade s\n"
            + "on s.losal <= e.sal and s.hisal >= e.sal")
        .explainContains("PLAN=JdbcToEnumerableConverter\n"
            + "  JdbcProject(EMPNO=[$0], ENAME=[$1], GRADE=[$3])\n"
            + "    JdbcJoin(condition=[AND(<=($4, $2), >=($5, $2))], joinType=[inner])\n"
            + "      JdbcProject(EMPNO=[$0], ENAME=[$1], SAL=[$5])\n"
            + "        JdbcTableScan(table=[[SCOTT, EMP]])\n"
            + "      JdbcTableScan(table=[[SCOTT, SALGRADE]])")
        .runs()
        .enable(CalciteAssert.DB == CalciteAssert.DatabaseInstance.HSQLDB)
        .planHasSql("SELECT \"t\".\"EMPNO\", \"t\".\"ENAME\", \"SALGRADE\".\"GRADE\"\n"
            + "FROM (SELECT \"EMPNO\", \"ENAME\", \"SAL\"\n"
            + "FROM \"SCOTT\".\"EMP\") AS \"t\"\n"
            + "INNER JOIN \"SCOTT\".\"SALGRADE\" ON \"t\".\"SAL\" >= \"SALGRADE\".\"LOSAL\" "
            + "AND \"t\".\"SAL\" <= \"SALGRADE\".\"HISAL\"");
  }

  @Test void testMixedJoinPlan() {
    CalciteAssert.model(JdbcTest.SCOTT_MODEL)
        .query("select e.empno, e.ename, e.empno, e.ename\n"
            + "from scott.emp e inner join scott.emp m on\n"
            + "e.mgr = m.empno and e.sal > m.sal")
        .explainContains("PLAN=JdbcToEnumerableConverter\n"
            + "  JdbcProject(EMPNO=[$0], ENAME=[$1], EMPNO0=[$0], ENAME0=[$1])\n"
            + "    JdbcJoin(condition=[AND(=($2, $4), >($3, $5))], joinType=[inner])\n"
            + "      JdbcProject(EMPNO=[$0], ENAME=[$1], MGR=[$3], SAL=[$5])\n"
            + "        JdbcTableScan(table=[[SCOTT, EMP]])\n"
            + "      JdbcProject(EMPNO=[$0], SAL=[$5])\n"
            + "        JdbcTableScan(table=[[SCOTT, EMP]])")
        .runs()
        .enable(CalciteAssert.DB == CalciteAssert.DatabaseInstance.HSQLDB)
        .planHasSql("SELECT \"t\".\"EMPNO\", \"t\".\"ENAME\", "
            + "\"t\".\"EMPNO\" AS \"EMPNO0\", \"t\".\"ENAME\" AS \"ENAME0\"\n"
            + "FROM (SELECT \"EMPNO\", \"ENAME\", \"MGR\", \"SAL\"\n"
            + "FROM \"SCOTT\".\"EMP\") AS \"t\"\n"
            + "INNER JOIN (SELECT \"EMPNO\", \"SAL\"\n"
            + "FROM \"SCOTT\".\"EMP\") AS \"t0\" "
            + "ON \"t\".\"MGR\" = \"t0\".\"EMPNO\" AND \"t\".\"SAL\" > \"t0\".\"SAL\"");
  }

  @Test void testMixedJoinWithOrPlan() {
    CalciteAssert.model(JdbcTest.SCOTT_MODEL)
        .query("select e.empno, e.ename, e.empno, e.ename\n"
            + "from scott.emp e inner join scott.emp m on\n"
            + "e.mgr = m.empno and (e.sal > m.sal or m.hiredate > e.hiredate)")
        .explainContains("PLAN=JdbcToEnumerableConverter\n"
            + "  JdbcProject(EMPNO=[$0], ENAME=[$1], EMPNO0=[$0], ENAME0=[$1])\n"
            + "    JdbcJoin(condition=[AND(=($2, $5), OR(>($4, $7), >($6, $3)))], joinType=[inner])\n"
            + "      JdbcProject(EMPNO=[$0], ENAME=[$1], MGR=[$3], HIREDATE=[$4], SAL=[$5])\n"
            + "        JdbcTableScan(table=[[SCOTT, EMP]])\n"
            + "      JdbcProject(EMPNO=[$0], HIREDATE=[$4], SAL=[$5])\n"
            + "        JdbcTableScan(table=[[SCOTT, EMP]])")
        .runs()
        .enable(CalciteAssert.DB == CalciteAssert.DatabaseInstance.HSQLDB)
        .planHasSql("SELECT \"t\".\"EMPNO\", \"t\".\"ENAME\", "
            + "\"t\".\"EMPNO\" AS \"EMPNO0\", \"t\".\"ENAME\" AS \"ENAME0\"\n"
            + "FROM (SELECT \"EMPNO\", \"ENAME\", \"MGR\", \"HIREDATE\", \"SAL\"\n"
            + "FROM \"SCOTT\".\"EMP\") AS \"t\"\n"
            + "INNER JOIN (SELECT \"EMPNO\", \"HIREDATE\", \"SAL\"\n"
            + "FROM \"SCOTT\".\"EMP\") AS \"t0\" "
            + "ON \"t\".\"MGR\" = \"t0\".\"EMPNO\" "
            + "AND (\"t\".\"SAL\" > \"t0\".\"SAL\" OR \"t\".\"HIREDATE\" < \"t0\".\"HIREDATE\")");
  }


  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6436">[CALCITE-6436]
   * JDBC adapter generates SQL missing parentheses when comparing 3 values with
   * the same precedence like (a=b)=c</a>. */
  @Test void testMissingParentheses() {
    CalciteAssert.model(FoodmartSchema.FOODMART_MODEL)
        .query("select * from \"sales_fact_1997\" "
            + "where (\"product_id\" = 1) = ?")
        .consumesPreparedStatement(p -> p.setBoolean(1, true))
        .returnsCount(26)
        .planHasSql("SELECT *\nFROM \"foodmart\".\"sales_fact_1997\"\n"
            + "WHERE (\"product_id\" = 1) = ?");
  }

  @Test void testJoin3TablesPlan() {
    CalciteAssert.model(JdbcTest.SCOTT_MODEL)
        .query("select  empno, ename, dname, grade\n"
            + "from scott.emp e inner join scott.dept d\n"
            + "on e.deptno = d.deptno\n"
            + "inner join scott.salgrade s\n"
            + "on e.sal > s.losal and e.sal < s.hisal")
        .explainContains("PLAN=JdbcToEnumerableConverter\n"
            + "  JdbcProject(EMPNO=[$0], ENAME=[$1], DNAME=[$5], GRADE=[$6])\n"
            + "    JdbcJoin(condition=[AND(>($2, $7), <($2, $8))], joinType=[inner])\n"
            + "      JdbcJoin(condition=[=($3, $4)], joinType=[inner])\n"
            + "        JdbcProject(EMPNO=[$0], ENAME=[$1], SAL=[$5], DEPTNO=[$7])\n"
            + "          JdbcTableScan(table=[[SCOTT, EMP]])\n"
            + "        JdbcProject(DEPTNO=[$0], DNAME=[$1])\n"
            + "          JdbcTableScan(table=[[SCOTT, DEPT]])\n"
            + "      JdbcTableScan(table=[[SCOTT, SALGRADE]])\n")
        .runs()
        .enable(CalciteAssert.DB == CalciteAssert.DatabaseInstance.HSQLDB)
        .planHasSql("SELECT \"t\".\"EMPNO\", \"t\".\"ENAME\", "
            + "\"t0\".\"DNAME\", \"SALGRADE\".\"GRADE\"\n"
            + "FROM (SELECT \"EMPNO\", \"ENAME\", \"SAL\", \"DEPTNO\"\n"
            + "FROM \"SCOTT\".\"EMP\") AS \"t\"\n"
            + "INNER JOIN (SELECT \"DEPTNO\", \"DNAME\"\n"
            + "FROM \"SCOTT\".\"DEPT\") AS \"t0\" ON \"t\".\"DEPTNO\" = \"t0\".\"DEPTNO\"\n"
            + "INNER JOIN \"SCOTT\".\"SALGRADE\" "
            + "ON \"t\".\"SAL\" > \"SALGRADE\".\"LOSAL\" "
            + "AND \"t\".\"SAL\" < \"SALGRADE\".\"HISAL\"");
  }

  @Test void testCrossJoinWithJoinKeyPlan() {
    CalciteAssert.model(JdbcTest.SCOTT_MODEL)
        .query("select empno, ename, d.deptno, dname\n"
            + "from scott.emp e,scott.dept d\n"
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

  @Test void testCartesianJoinWithoutKeyPlan() {
    CalciteAssert.model(JdbcTest.SCOTT_MODEL)
        .query("select empno, ename, d.deptno, dname\n"
            + "from scott.emp e,scott.dept d")
        .explainContains("PLAN=JdbcToEnumerableConverter\n"
            + "  JdbcJoin(condition=[true], joinType=[inner])\n"
            + "    JdbcProject(EMPNO=[$0], ENAME=[$1])\n"
            + "      JdbcTableScan(table=[[SCOTT, EMP]])\n"
            + "    JdbcProject(DEPTNO=[$0], DNAME=[$1])\n"
            + "      JdbcTableScan(table=[[SCOTT, DEPT]])")
        .runs()
        .enable(CalciteAssert.DB == CalciteAssert.DatabaseInstance.HSQLDB);
  }

  @Test void testCrossJoinWithJoinKeyAndFilterPlan() {
    CalciteAssert.model(JdbcTest.SCOTT_MODEL)
        .query("select empno, ename, d.deptno, dname\n"
            + "from scott.emp e,scott.dept d\n"
            + "where e.deptno = d.deptno\n"
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

  @Test void testJoinConditionAlwaysTruePushDown() {
    CalciteAssert.model(JdbcTest.SCOTT_MODEL)
        .query("select empno, ename, d.deptno, dname\n"
                + "from scott.emp e,scott.dept d\n"
                + "where true")
        .explainContains("PLAN=JdbcToEnumerableConverter\n"
                + "  JdbcJoin(condition=[true], joinType=[inner])\n"
                + "    JdbcProject(EMPNO=[$0], ENAME=[$1])\n"
                + "      JdbcTableScan(table=[[SCOTT, EMP]])\n"
                + "    JdbcProject(DEPTNO=[$0], DNAME=[$1])\n"
                + "      JdbcTableScan(table=[[SCOTT, DEPT]])")
        .runs()
        .enable(CalciteAssert.DB == CalciteAssert.DatabaseInstance.HSQLDB)
        .planHasSql("SELECT *\n"
                + "FROM (SELECT \"EMPNO\", \"ENAME\"\n"
                + "FROM \"SCOTT\".\"EMP\") AS \"t\",\n"
                + "(SELECT \"DEPTNO\", \"DNAME\"\n"
                + "FROM \"SCOTT\".\"DEPT\") AS \"t0\"");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-893">[CALCITE-893]
   * Theta join in JdbcAdapter</a>. */
  @Test void testJoinPlan() {
    final String sql = "SELECT T1.\"brand_name\"\n"
        + "FROM \"foodmart\".\"product\" AS T1\n"
        + " INNER JOIN \"foodmart\".\"product_class\" AS T2\n"
        + " ON T1.\"product_class_id\" = T2.\"product_class_id\"\n"
        + "WHERE T2.\"product_department\" = 'Frozen Foods'\n"
        + " OR T2.\"product_department\" = 'Baking Goods'\n"
        + " AND T1.\"brand_name\" <> 'King'";
    CalciteAssert.model(FoodmartSchema.FOODMART_MODEL)
        .query(sql).runs()
        .returnsCount(275);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1372">[CALCITE-1372]
   * JDBC adapter generates SQL with wrong field names</a>. */
  @Test void testJoinPlan2() {
    final String sql = "SELECT v1.deptno, v2.deptno\n"
        + "FROM Scott.dept v1 LEFT JOIN Scott.emp v2 ON v1.deptno = v2.deptno\n"
        + "WHERE v2.job LIKE 'PRESIDENT'";
    CalciteAssert.model(JdbcTest.SCOTT_MODEL)
        .with(Lex.MYSQL)
        .query(sql).runs()
        .returnsCount(1);
  }

  @Test void testJoinCartesian() {
    final String sql = "SELECT *\n"
        + "FROM Scott.dept, Scott.emp";
    CalciteAssert.model(JdbcTest.SCOTT_MODEL).query(sql).returnsCount(56);
  }

  @Test void testJoinCartesianCount() {
    final String sql = "SELECT count(*) as c\n"
        + "FROM Scott.dept, Scott.emp";
    CalciteAssert.model(JdbcTest.SCOTT_MODEL).query(sql).returns("C=56\n");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1382">[CALCITE-1382]
   * ClassCastException in JDBC adapter</a>. */
  @Test void testJoinPlan3() {
    final String sql = "SELECT count(*) AS c FROM (\n"
        + "  SELECT count(emp.empno) `Count Emp`,\n"
        + "      dept.dname `Department Name`\n"
        + "  FROM emp emp\n"
        + "  JOIN dept dept ON emp.deptno = dept.deptno\n"
        + "  JOIN salgrade salgrade ON emp.comm = salgrade.hisal\n"
        + "  WHERE dept.dname LIKE '%A%'\n"
        + "  GROUP BY emp.deptno, dept.dname)";
    final String expected = "c=1\n";
    final String expectedSql = "SELECT COUNT(*) AS \"c\"\n"
        + "FROM (SELECT \"t0\".\"DEPTNO\", \"t2\".\"DNAME\"\n"
        + "FROM (SELECT \"HISAL\"\n"
        + "FROM \"SCOTT\".\"SALGRADE\") AS \"t\"\n"
        + "INNER JOIN ((SELECT \"COMM\", \"DEPTNO\"\n"
        + "FROM \"SCOTT\".\"EMP\") AS \"t0\" "
        + "INNER JOIN (SELECT \"DEPTNO\", \"DNAME\"\n"
        + "FROM \"SCOTT\".\"DEPT\"\n"
        + "WHERE \"DNAME\" LIKE '%A%') AS \"t2\" "
        + "ON \"t0\".\"DEPTNO\" = \"t2\".\"DEPTNO\") "
        + "ON \"t\".\"HISAL\" = \"t0\".\"COMM\"\n"
        + "GROUP BY \"t0\".\"DEPTNO\", \"t2\".\"DNAME\") AS \"t3\"";
    CalciteAssert.model(JdbcTest.SCOTT_MODEL)
        .with(Lex.MYSQL)
        .query(sql)
        .returns(expected)
        .planHasSql(expectedSql);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-657">[CALCITE-657]
   * NullPointerException when executing JdbcAggregate implement method</a>. */
  @Test void testJdbcAggregate() throws Exception {
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
    assertThat(rs.getObject(1), equalTo(20L));
    assertThat(rs.next(), is(false));

    rs.close();
    calciteConnection.close();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2206">[CALCITE-2206]
   * JDBC adapter incorrectly pushes windowed aggregates down to HSQLDB</a>. */
  @Test void testOverNonSupportedDialect() {
    final String sql = "select \"store_id\", \"account_id\", \"exp_date\",\n"
        + " \"time_id\", \"category_id\", \"currency_id\", \"amount\",\n"
        + " last_value(\"time_id\") over () as \"last_version\"\n"
        + "from \"expense_fact\"";
    final String explain = "PLAN="
        + "EnumerableWindow(window#0=[window(aggs [LAST_VALUE($3)])])\n"
        + "  JdbcToEnumerableConverter\n"
        + "    JdbcTableScan(table=[[foodmart, expense_fact]])\n";
    CalciteAssert
        .model(FoodmartSchema.FOODMART_MODEL)
        .enable(CalciteAssert.DB == DatabaseInstance.HSQLDB)
        .query(sql)
        .explainContains(explain)
        .runs()
        .planHasSql("SELECT *\n"
            + "FROM \"foodmart\".\"expense_fact\"");
  }

  @Test void testTablesNoCatalogSchema() {
    // Switch from "FOODMART" user, whose default schema is 'foodmart',
    // to "sa", whose default schema is the root, and therefore cannot
    // see the table unless directed to look in a particular schema.
    final String model =
        FoodmartSchema.FOODMART_MODEL
            .replace("jdbcUser: 'FOODMART'", "jdbcUser: 'sa'")
            .replace("jdbcPassword: 'FOODMART'", "jdbcPassword: ''")
            .replace("jdbcCatalog: 'foodmart'", "jdbcCatalog: null")
            .replace("jdbcSchema: 'foodmart'", "jdbcSchema: null");
    // Since Calcite uses PostgreSQL JDBC driver version >= 4.1,
    // catalog/schema can be retrieved from JDBC connection and
    // this test succeeds
    CalciteAssert.model(model)
        // Calcite uses PostgreSQL JDBC driver version >= 4.1
        .enable(CalciteAssert.DB == DatabaseInstance.POSTGRESQL)
        .query("select \"store_id\", \"account_id\", \"exp_date\","
            + " \"time_id\", \"category_id\", \"currency_id\", \"amount\","
            + " last_value(\"time_id\") over ()"
            + " as \"last_version\" from \"expense_fact\"")
        .runs();
    // Since Calcite uses HSQLDB JDBC driver version < 4.1,
    // catalog/schema cannot be retrieved from JDBC connection and
    // this test fails
    CalciteAssert.model(model)
        .enable(CalciteAssert.DB == DatabaseInstance.HSQLDB)
        .query("select \"store_id\", \"account_id\", \"exp_date\","
            + " \"time_id\", \"category_id\", \"currency_id\", \"amount\","
            + " last_value(\"time_id\") over ()"
            + " as \"last_version\" from \"expense_fact\"")
        .throws_("'expense_fact' not found");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1506">[CALCITE-1506]
   * Push OVER Clause to underlying SQL via JDBC adapter</a>.
   *
   * <p>Test runs only on Postgres; the default database, Hsqldb, does not
   * support OVER. */
  @Test void testOverDefault() {
    CalciteAssert
        .model(FoodmartSchema.FOODMART_MODEL)
        .enable(CalciteAssert.DB == CalciteAssert.DatabaseInstance.POSTGRESQL)
        .query("select \"store_id\", \"account_id\", \"exp_date\","
            + " \"time_id\", \"category_id\", \"currency_id\", \"amount\","
            + " last_value(\"time_id\") over ()"
            + " as \"last_version\" from \"expense_fact\"")
        .explainContains("PLAN=JdbcToEnumerableConverter\n"
            + "  JdbcProject(store_id=[$0], account_id=[$1], exp_date=[$2], "
            + "time_id=[$3], category_id=[$4], currency_id=[$5], amount=[$6],"
            + " last_version=[LAST_VALUE($3) OVER (RANGE BETWEEN UNBOUNDED"
            + " PRECEDING AND UNBOUNDED FOLLOWING)])\n"
            + "    JdbcTableScan(table=[[foodmart, expense_fact]])\n")
        .runs()
        .planHasSql("SELECT \"store_id\", \"account_id\", \"exp_date\","
            + " \"time_id\", \"category_id\", \"currency_id\", \"amount\","
            + " LAST_VALUE(\"time_id\") OVER (RANGE BETWEEN UNBOUNDED"
            + " PRECEDING AND UNBOUNDED FOLLOWING) AS \"last_version\"\n"
            + "FROM \"foodmart\".\"expense_fact\"");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2305">[CALCITE-2305]
   * JDBC adapter generates invalid casts on PostgreSQL, because PostgreSQL does
   * not have TINYINT and DOUBLE types</a>. */
  @Test void testCast() {
    CalciteAssert
        .model(FoodmartSchema.FOODMART_MODEL)
        .enable(CalciteAssert.DB == CalciteAssert.DatabaseInstance.POSTGRESQL)
        .query("select cast(\"store_id\" as TINYINT),"
            + "cast(\"store_id\" as DOUBLE)"
            + " from \"expense_fact\"")
        .runs()
        .planHasSql("SELECT CAST(\"store_id\" AS SMALLINT),"
            + " CAST(\"store_id\" AS DOUBLE PRECISION)\n"
            + "FROM \"foodmart\".\"expense_fact\"");
  }

  @Test void testOverRowsBetweenBoundFollowingAndFollowing() {
    CalciteAssert
        .model(FoodmartSchema.FOODMART_MODEL)
        .enable(CalciteAssert.DB == CalciteAssert.DatabaseInstance.POSTGRESQL)
        .query("select \"store_id\", \"account_id\", \"exp_date\","
            + " \"time_id\", \"category_id\", \"currency_id\", \"amount\","
            + " last_value(\"time_id\") over (partition by \"account_id\""
            + " order by \"time_id\" rows between 1 following and 10 following)"
            + " as \"last_version\" from \"expense_fact\"")
        .explainContains("PLAN=JdbcToEnumerableConverter\n"
            + "  JdbcProject(store_id=[$0], account_id=[$1], exp_date=[$2], "
            + "time_id=[$3], category_id=[$4], currency_id=[$5], amount=[$6],"
            + " last_version=[LAST_VALUE($3) OVER (PARTITION BY $1"
            + " ORDER BY $3 ROWS BETWEEN 1 FOLLOWING AND 10 FOLLOWING)])\n"
            + "    JdbcTableScan(table=[[foodmart, expense_fact]])\n")
        .runs()
        .planHasSql("SELECT \"store_id\", \"account_id\", \"exp_date\","
            + " \"time_id\", \"category_id\", \"currency_id\", \"amount\","
            + " LAST_VALUE(\"time_id\") OVER (PARTITION BY \"account_id\""
            + " ORDER BY \"time_id\" ROWS BETWEEN 1 FOLLOWING"
            + " AND 10 FOLLOWING) AS \"last_version\"\n"
            + "FROM \"foodmart\".\"expense_fact\"");
  }

  @Test void testOverRowsBetweenBoundPrecedingAndCurrent() {
    CalciteAssert
        .model(FoodmartSchema.FOODMART_MODEL)
        .enable(CalciteAssert.DB == CalciteAssert.DatabaseInstance.POSTGRESQL)
        .query("select \"store_id\", \"account_id\", \"exp_date\","
            + " \"time_id\", \"category_id\", \"currency_id\", \"amount\","
            + " last_value(\"time_id\") over (partition by \"account_id\""
            + " order by \"time_id\" rows between 3 preceding and current row)"
            + " as \"last_version\" from \"expense_fact\"")
        .explainContains("PLAN=JdbcToEnumerableConverter\n"
            + "  JdbcProject(store_id=[$0], account_id=[$1], exp_date=[$2], "
            + "time_id=[$3], category_id=[$4], currency_id=[$5], amount=[$6],"
            + " last_version=[LAST_VALUE($3) OVER (PARTITION BY $1"
            + " ORDER BY $3 ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)])\n"
            + "    JdbcTableScan(table=[[foodmart, expense_fact]])\n")
        .runs()
        .planHasSql("SELECT \"store_id\", \"account_id\", \"exp_date\","
            + " \"time_id\", \"category_id\", \"currency_id\", \"amount\","
            + " LAST_VALUE(\"time_id\") OVER (PARTITION BY \"account_id\""
            + " ORDER BY \"time_id\" ROWS BETWEEN 3 PRECEDING"
            + " AND CURRENT ROW) AS \"last_version\"\n"
            + "FROM \"foodmart\".\"expense_fact\"");
  }

  @Test void testOverDisallowPartial() {
    CalciteAssert
        .model(FoodmartSchema.FOODMART_MODEL)
        .enable(CalciteAssert.DB == CalciteAssert.DatabaseInstance.POSTGRESQL)
        .query("select \"store_id\", \"account_id\", \"exp_date\","
            + " \"time_id\", \"category_id\", \"currency_id\", \"amount\","
            + " last_value(\"time_id\") over (partition by \"account_id\""
            + " order by \"time_id\" rows 3 preceding disallow partial)"
            + " as \"last_version\" from \"expense_fact\"")
        .explainContains("PLAN=JdbcToEnumerableConverter\n"
            + "  JdbcProject(store_id=[$0], account_id=[$1], exp_date=[$2],"
            + " time_id=[$3], category_id=[$4], currency_id=[$5],"
            + " amount=[$6], last_version=[CASE(>=(COUNT() OVER"
            + " (PARTITION BY $1 ORDER BY $3 ROWS BETWEEN 3 PRECEDING AND"
            + " CURRENT ROW), 2), LAST_VALUE($3) OVER (PARTITION BY $1"
            + " ORDER BY $3 ROWS BETWEEN 3 PRECEDING AND CURRENT ROW),"
            + " null)])\n    JdbcTableScan(table=[[foodmart,"
            + " expense_fact]])\n")
        .runs()
        .planHasSql("SELECT \"store_id\", \"account_id\", \"exp_date\","
            + " \"time_id\", \"category_id\", \"currency_id\", \"amount\","
            + " CASE WHEN (COUNT(*) OVER (PARTITION BY \"account_id\""
            + " ORDER BY \"time_id\" ROWS BETWEEN 3 PRECEDING"
            + " AND CURRENT ROW)) >= 2 THEN LAST_VALUE(\"time_id\")"
            + " OVER (PARTITION BY \"account_id\" ORDER BY \"time_id\""
            + " ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)"
            + " ELSE NULL END AS \"last_version\"\n"
            + "FROM \"foodmart\".\"expense_fact\"");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6346">[CALCITE-6346]
   * JdbcAdapter: Cast for dynamic filter arguments is lost</a>. */
  @Test void testCastDynamic() {
    CalciteAssert.that()
        .with(CalciteAssert.Config.FOODMART_CLONE)
        .query("SELECT * FROM \"foodmart\".\"sales_fact_1997\""
            + " WHERE cast (? as varchar(10)) = cast(? as varchar(10))")
        .planHasSql("SELECT *\n"
            + "FROM \"foodmart\".\"sales_fact_1997\"\n"
            + "WHERE CAST(? AS VARCHAR(10)) = CAST(? AS VARCHAR(10))")
        .consumesPreparedStatement(p -> {
          p.setInt(1, 10);
          p.setLong(2, 10);
        })
        .runs();
  }

  @Test void testLastValueOver() {
    CalciteAssert
        .model(FoodmartSchema.FOODMART_MODEL)
        .enable(CalciteAssert.DB == CalciteAssert.DatabaseInstance.POSTGRESQL)
        .query("select \"store_id\", \"account_id\", \"exp_date\","
            + " \"time_id\", \"category_id\", \"currency_id\", \"amount\","
            + " last_value(\"time_id\") over (partition by \"account_id\""
            + " order by \"time_id\") as \"last_version\""
            + " from \"expense_fact\"")
        .explainContains("PLAN=JdbcToEnumerableConverter\n"
            + "  JdbcProject(store_id=[$0], account_id=[$1], exp_date=[$2],"
            + " time_id=[$3], category_id=[$4], currency_id=[$5], amount=[$6],"
            + " last_version=[LAST_VALUE($3) OVER (PARTITION BY $1 ORDER BY $3"
            + " RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)])\n"
            + "    JdbcTableScan(table=[[foodmart, expense_fact]])\n")
        .runs()
        .planHasSql("SELECT \"store_id\", \"account_id\", \"exp_date\","
            + " \"time_id\", \"category_id\", \"currency_id\", \"amount\","
            + " LAST_VALUE(\"time_id\") OVER (PARTITION BY \"account_id\""
            + " ORDER BY \"time_id\" RANGE BETWEEN UNBOUNDED PRECEDING AND"
            + " CURRENT ROW) AS \"last_version\""
            + "\nFROM \"foodmart\".\"expense_fact\"");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-259">[CALCITE-259]
   * Using sub-queries in CASE statement against JDBC tables generates invalid
   * Oracle SQL</a>. */
  @Test void testSubQueryWithSingleValue() {
    final String expected;
    switch (CalciteAssert.DB) {
    case MYSQL:
      expected = "Sub"
          + "query returns more than 1 row";
      break;
    default:
      expected = "more than one value in agg SINGLE_VALUE";
    }
    CalciteAssert.model(FoodmartSchema.FOODMART_MODEL)
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
  @Test void testMetadataTables() throws Exception {
    // The troublesome tables occur in PostgreSQL's system schema.
    final String model =
        FoodmartSchema.FOODMART_MODEL.replace("jdbcSchema: 'foodmart'",
            "jdbcSchema: null");
    CalciteAssert.model(
        model)
        .doWithConnection(connection -> {
          try {
            final ResultSet resultSet =
                connection.getMetaData().getTables(null, null, "%", null);
            assertFalse(CalciteAssert.toString(resultSet).isEmpty());
          } catch (SQLException e) {
            throw TestUtil.rethrow(e);
          }
        });
  }

  @Test void testMetadataFunctions() {
    final String model = ""
        + "{\n"
        + "  version: '1.0',\n"
        + "   schemas: [\n"
        + "     {\n"
        + "       name: 'adhoc',\n"
        + "       functions: [\n"
        + "         {\n"
        + "           name: 'MY_STR',\n"
        + "           className: '" + Smalls.MyToStringFunction.class.getName() + "'\n"
        + "         },\n"
        + "         {\n"
        + "           name: 'FIBONACCI_TABLE',\n"
        + "           className: '" + Smalls.class.getName() + "',\n"
        + "           methodName: 'fibonacciTable'\n"
        + "         }\n"
        + "       ],\n"
        + "       materializations: [\n"
        + "         {\n"
        + "           table: 'TEST_VIEW',\n"
        + "           sql: 'SELECT 1'\n"
        + "         }\n"
        + "       ]\n"
        + "     }\n"
        + "   ]\n"
        + "}";
    CalciteAssert.model(model)
        .withDefaultSchema("adhoc")
        .metaData(connection -> {
          try {
            return connection.getMetaData().getFunctions(null, "adhoc", "%");
          } catch (SQLException e) {
            throw TestUtil.rethrow(e);
          }
        })
        .returns(""
            + "FUNCTION_CAT=null; FUNCTION_SCHEM=adhoc; FUNCTION_NAME=FIBONACCI_TABLE; REMARKS=null; FUNCTION_TYPE=0; SPECIFIC_NAME=FIBONACCI_TABLE\n"
            + "FUNCTION_CAT=null; FUNCTION_SCHEM=adhoc; FUNCTION_NAME=MY_STR; REMARKS=null; FUNCTION_TYPE=0; SPECIFIC_NAME=MY_STR\n");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-666">[CALCITE-666]
   * Anti-semi-joins against JDBC adapter give wrong results</a>. */
  @Test void testScalarSubQuery() {
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

  /**
   * Acquires an exclusive connection to a test database, and cleans it.
   *
   * <p>Cleans any previous TableModify states and creates
   * one expense_fact instance with store_id = 666.
   *
   * <p>Caller must close the returned wrapper, so that the next test can
   * acquire the lock and use the database.
   *
   * @param c JDBC connection
   */
  private LockWrapper exclusiveCleanDb(Connection c) throws SQLException {
    final LockWrapper wrapper = LockWrapper.lock(LOCK);
    try (Statement statement = c.createStatement()) {
      final String dSql = "DELETE FROM \"foodmart\".\"expense_fact\""
          + " WHERE \"store_id\"=666\n";
      final String iSql = "INSERT INTO \"foodmart\".\"expense_fact\"(\n"
          + " \"store_id\", \"account_id\", \"exp_date\", \"time_id\","
          + " \"category_id\", \"currency_id\", \"amount\")\n"
          + " VALUES (666, 666, TIMESTAMP '1997-01-01 00:00:00',"
          + " 666, '666', 666, 666)";
      statement.executeUpdate(dSql);
      int rowCount = statement.executeUpdate(iSql);
      assertThat(rowCount, is(1));
      return wrapper;
    } catch (SQLException | RuntimeException | Error e) {
      wrapper.close();
      throw e;
    }
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1527">[CALCITE-1527]
   * Support DML in the JDBC adapter</a>. */
  @Test void testTableModifyInsert() throws Exception {
    final String sql = "INSERT INTO \"foodmart\".\"expense_fact\"(\n"
        + " \"store_id\", \"account_id\", \"exp_date\", \"time_id\","
        + " \"category_id\", \"currency_id\", \"amount\")\n"
        + "VALUES (666, 666, TIMESTAMP '1997-01-01 00:00:00',"
        + " 666, '666', 666, 666)";
    final String explain = "PLAN=JdbcToEnumerableConverter\n"
        + "  JdbcTableModify(table=[[foodmart, expense_fact]], "
        + "operation=[INSERT], flattened=[false])\n"
        + "    JdbcValues(tuples=[[{ 666, 666, 1997-01-01 00:00:00, 666, "
        + "'666', 666, 666.0000 }]])\n\n";
    final String jdbcSql = "INSERT INTO \"foodmart\".\"expense_fact\" (\"store_id\", "
        + "\"account_id\", \"exp_date\", \"time_id\", \"category_id\", \"currency_id\", "
        + "\"amount\")\n"
        + "VALUES (666, 666, TIMESTAMP '1997-01-01 00:00:00', 666, '666', "
        + "666, 666.0000)";
    final AssertThat that =
        CalciteAssert.model(FoodmartSchema.FOODMART_MODEL)
            .enable(CalciteAssert.DB == DatabaseInstance.HSQLDB
                || CalciteAssert.DB == DatabaseInstance.POSTGRESQL);
    that.doWithConnection(connection -> {
      try (LockWrapper ignore = exclusiveCleanDb(connection)) {
        that.query(sql)
            .explainContains(explain)
            .planUpdateHasSql(jdbcSql, 1);
      } catch (SQLException e) {
        throw TestUtil.rethrow(e);
      }
    });
  }

  @Test void testTableModifyInsertMultiValues() throws Exception {
    final String sql = "INSERT INTO \"foodmart\".\"expense_fact\"(\n"
        + " \"store_id\", \"account_id\", \"exp_date\", \"time_id\","
        + " \"category_id\", \"currency_id\", \"amount\")\n"
        + "VALUES (666, 666, TIMESTAMP '1997-01-01 00:00:00',"
        + "   666, '666', 666, 666),\n"
        + " (666, 777, TIMESTAMP '1997-01-01 00:00:00',"
        + "   666, '666', 666, 666)";
    final String explain = "PLAN=JdbcToEnumerableConverter\n"
        + "  JdbcTableModify(table=[[foodmart, expense_fact]], "
        + "operation=[INSERT], flattened=[false])\n"
        + "    JdbcValues(tuples=[["
        + "{ 666, 666, 1997-01-01 00:00:00, 666, '666', 666, 666.0000 }, "
        + "{ 666, 777, 1997-01-01 00:00:00, 666, '666', 666, 666.0000 }]])\n\n";
    final String jdbcSql = "INSERT INTO \"foodmart\".\"expense_fact\""
        + " (\"store_id\", \"account_id\", \"exp_date\", \"time_id\", "
        + "\"category_id\", \"currency_id\", \"amount\")\n"
        + "VALUES "
        + "(666, 666, TIMESTAMP '1997-01-01 00:00:00', 666, '666', 666, 666.0000),\n"
        + "(666, 777, TIMESTAMP '1997-01-01 00:00:00', 666, '666', 666, 666.0000)";
    final AssertThat that =
        CalciteAssert.model(FoodmartSchema.FOODMART_MODEL)
            .enable(CalciteAssert.DB == DatabaseInstance.HSQLDB
                || CalciteAssert.DB == DatabaseInstance.POSTGRESQL);
    that.doWithConnection(connection -> {
      try (LockWrapper ignore = exclusiveCleanDb(connection)) {
        that.query(sql)
            .explainContains(explain)
            .planUpdateHasSql(jdbcSql, 2);
      } catch (SQLException e) {
        throw TestUtil.rethrow(e);
      }
    });
  }

  @Test void testTableModifyInsertWithSubQuery() throws Exception {
    final AssertThat that = CalciteAssert
        .model(FoodmartSchema.FOODMART_MODEL)
        .enable(CalciteAssert.DB == DatabaseInstance.HSQLDB);

    that.doWithConnection(connection -> {
      try (LockWrapper ignore = exclusiveCleanDb(connection)) {
        final String sql = "INSERT INTO \"foodmart\".\"expense_fact\"(\n"
            + " \"store_id\", \"account_id\", \"exp_date\", \"time_id\","
            + " \"category_id\", \"currency_id\", \"amount\")\n"
            + "SELECT  \"store_id\", \"account_id\", \"exp_date\","
            + " \"time_id\" + 1, \"category_id\", \"currency_id\","
            + " \"amount\"\n"
            + "FROM \"foodmart\".\"expense_fact\"\n"
            + "WHERE \"store_id\" = 666";
        final String explain = "PLAN=JdbcToEnumerableConverter\n"
            + "  JdbcTableModify(table=[[foodmart, expense_fact]], operation=[INSERT], flattened=[false])\n"
            + "    JdbcProject(store_id=[$0], account_id=[$1], exp_date=[$2], time_id=[+($3, 1)], category_id=[$4], currency_id=[$5], amount=[$6])\n"
            + "      JdbcFilter(condition=[=($0, 666)])\n"
            + "        JdbcTableScan(table=[[foodmart, expense_fact]])\n";
        final String jdbcSql = "INSERT INTO \"foodmart\".\"expense_fact\""
            + " (\"store_id\", \"account_id\", \"exp_date\", \"time_id\","
            + " \"category_id\", \"currency_id\", \"amount\")\n"
            + "SELECT \"store_id\", \"account_id\", \"exp_date\","
            + " \"time_id\" + 1 AS \"time_id\", \"category_id\","
            + " \"currency_id\", \"amount\"\n"
            + "FROM \"foodmart\".\"expense_fact\"\n"
            + "WHERE \"store_id\" = 666";
        that.query(sql)
            .explainContains(explain)
            .planUpdateHasSql(jdbcSql, 1);
      } catch (SQLException e) {
        throw TestUtil.rethrow(e);
      }
    });
  }

  @Test void testTableModifyUpdate() throws Exception {
    final AssertThat that = CalciteAssert
        .model(FoodmartSchema.FOODMART_MODEL)
        .enable(CalciteAssert.DB == DatabaseInstance.HSQLDB);

    that.doWithConnection(connection -> {
      try (LockWrapper ignore = exclusiveCleanDb(connection)) {
        final String sql = "UPDATE \"foodmart\".\"expense_fact\"\n"
            + " SET \"account_id\"=888\n"
            + " WHERE \"store_id\"=666\n";
        final String explain = "PLAN=JdbcToEnumerableConverter\n"
            + "  JdbcTableModify(table=[[foodmart, expense_fact]], operation=[UPDATE], updateColumnList=[[account_id]], sourceExpressionList=[[888]], flattened=[false])\n"
            + "    JdbcProject(store_id=[$0], account_id=[$1], exp_date=[$2], time_id=[$3], category_id=[$4], currency_id=[$5], amount=[$6], EXPR$0=[888])\n"
            + "      JdbcFilter(condition=[=($0, 666)])\n"
            + "        JdbcTableScan(table=[[foodmart, expense_fact]])";
        final String jdbcSql = "UPDATE \"foodmart\".\"expense_fact\""
            + " SET \"account_id\" = 888\n"
            + "WHERE \"store_id\" = 666";
        that.query(sql)
            .explainContains(explain)
            .planUpdateHasSql(jdbcSql, 1);
        return null;
      } catch (SQLException e) {
        throw TestUtil.rethrow(e);
      }
    });
  }

  @Test void testTableModifyDelete() throws Exception {
    final AssertThat that = CalciteAssert
        .model(FoodmartSchema.FOODMART_MODEL)
        .enable(CalciteAssert.DB == DatabaseInstance.HSQLDB);

    that.doWithConnection(connection -> {
      try (LockWrapper ignore = exclusiveCleanDb(connection)) {
        final String sql = "DELETE FROM \"foodmart\".\"expense_fact\"\n"
            + "WHERE \"store_id\"=666\n";
        final String explain = "PLAN=JdbcToEnumerableConverter\n"
            + "  JdbcTableModify(table=[[foodmart, expense_fact]], operation=[DELETE], flattened=[false])\n"
            + "    JdbcFilter(condition=[=($0, 666)])\n"
            + "      JdbcTableScan(table=[[foodmart, expense_fact]]";
        final String jdbcSql = "DELETE FROM \"foodmart\".\"expense_fact\"\n"
            + "WHERE \"store_id\" = 666";
        that.query(sql)
            .explainContains(explain)
            .planUpdateHasSql(jdbcSql, 1);
      } catch (SQLException e) {
        throw TestUtil.rethrow(e);
      }
    });
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1572">[CALCITE-1572]
   * JdbcSchema throws exception when detecting nullable columns</a>. */
  @Test void testColumnNullability() {
    final String sql = "select \"employee_id\", \"position_id\"\n"
        + "from \"foodmart\".\"employee\" limit 10";
    CalciteAssert.model(FoodmartSchema.FOODMART_MODEL)
        .query(sql)
        .runs()
        .returnsCount(10)
        .typeIs("[employee_id INTEGER NOT NULL, position_id INTEGER]");
  }

  @Test void pushBindParameters() {
    final String sql = "select empno, ename from emp where empno = ?";
    CalciteAssert.model(JdbcTest.SCOTT_MODEL)
        .query(sql)
        .consumesPreparedStatement(p -> p.setInt(1, 7566))
        .returnsCount(1)
        .planHasSql("SELECT \"EMPNO\", \"ENAME\"\nFROM \"SCOTT\".\"EMP\"\nWHERE \"EMPNO\" = ?");
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4619">[CALCITE-4619]
   * "Full join" generates an incorrect execution plan under mysql</a>. */
  @Test void testFullJoinNonSupportedDialect() {
    CalciteAssert.model(JdbcTest.SCOTT_MODEL)
        .enable(CalciteAssert.DB == CalciteAssert.DatabaseInstance.H2
            || CalciteAssert.DB == CalciteAssert.DatabaseInstance.MYSQL)
        .query("select empno, ename, e.deptno, dname\n"
            + "from scott.emp e full join scott.dept d\n"
            + "on e.deptno = d.deptno")
        .explainContains("PLAN=EnumerableCalc(expr#0..4=[{inputs}], proj#0..2=[{exprs}],"
            + " DNAME=[$t4])\n"
            + "  EnumerableHashJoin(condition=[=($2, $3)], joinType=[full])\n"
            + "    JdbcToEnumerableConverter\n"
            + "      JdbcProject(EMPNO=[$0], ENAME=[$1], DEPTNO=[$7])\n"
            + "        JdbcTableScan(table=[[SCOTT, EMP]])\n"
            + "    JdbcToEnumerableConverter\n"
            + "      JdbcProject(DEPTNO=[$0], DNAME=[$1])\n"
            + "        JdbcTableScan(table=[[SCOTT, DEPT]])")
        .runs();
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6995">[CALCITE-6995]
   * Support FULL JOIN in StarRocks/Doris Dialect</a>. */
  @Test void testFullJoinSupportedDialect() {
    CalciteAssert.model(JdbcTest.SCOTT_MODEL)
        .enable(CalciteAssert.DB != CalciteAssert.DatabaseInstance.H2
            && CalciteAssert.DB != CalciteAssert.DatabaseInstance.MYSQL)
        .query("select empno, ename, e.deptno, dname\n"
            + "from scott.emp e full join scott.dept d\n"
            + "on e.deptno = d.deptno")
        .explainContains("PLAN=JdbcToEnumerableConverter\n"
            + "  JdbcProject(EMPNO=[$0], ENAME=[$1], DEPTNO=[$2], DNAME=[$4])\n"
            + "    JdbcJoin(condition=[=($2, $3)], joinType=[full])\n"
            + "      JdbcProject(EMPNO=[$0], ENAME=[$1], DEPTNO=[$7])\n"
            + "        JdbcTableScan(table=[[SCOTT, EMP]])\n"
            + "      JdbcProject(DEPTNO=[$0], DNAME=[$1])\n"
            + "        JdbcTableScan(table=[[SCOTT, DEPT]])\n\n")
        .runs();
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5243">[CALCITE-5243]
   * "SELECT NULL AS C causes NoSuchMethodException: java.sql.ResultSet.getVoid(int)</a>. */
  @Test void testNullSelect() {
    final String sql = "select NULL AS C from \"days\"";
    CalciteAssert.model(FoodmartSchema.FOODMART_MODEL)
        .query(sql)
        .runs()
        .returnsCount(7)
        .returns("C=null\nC=null\nC=null\nC=null\nC=null\nC=null\nC=null\n");
  }

  @Test void testMerge() throws Exception {
    final String sql = "merge into \"foodmart\".\"expense_fact\"\n"
        + "using (values(666, 42)) as vals(store_id, amount)\n"
        + "on \"expense_fact\".\"store_id\" = vals.store_id\n"
        + "when matched then update\n"
        + "set \"amount\" = vals.amount\n"
        + "when not matched then insert\n"
        + "values (vals.store_id, 666, TIMESTAMP '1997-01-01 00:00:00', 666, '666', 666,"
        + " vals.amount)";
    final String explain = "PLAN=JdbcToEnumerableConverter\n"
        + "  JdbcTableModify(table=[[foodmart, expense_fact]], operation=[MERGE],"
        + " updateColumnList=[[amount]], flattened=[false])\n"
        + "    JdbcProject(STORE_ID=[$0], $f1=[666], $f2=[1997-01-01 00:00:00], $f3=[666],"
        + " $f4=['666':VARCHAR(30)], $f5=[666], AMOUNT=[CAST($1):DECIMAL(10, 4) NOT NULL],"
        + " store_id=[$2],"
        + " account_id=[$3], exp_date=[$4], time_id=[$5], category_id=[$6], currency_id=[$7],"
        + " amount=[$8], AMOUNT0=[$1])\n"
        + "      JdbcJoin(condition=[=($2, $0)], joinType=[left])\n"
        + "        JdbcValues(tuples=[[{ 666, 42 }]])\n"
        + "        JdbcTableScan(table=[[foodmart, expense_fact]])\n";
    final String jdbcSql = "MERGE INTO \"foodmart\".\"expense_fact\"\n"
        + "USING (VALUES (666, 42)) AS \"t\" (\"STORE_ID\", \"AMOUNT\")\n"
        + "ON \"t\".\"STORE_ID\" = \"expense_fact\".\"store_id\"\n"
        + "WHEN MATCHED THEN UPDATE SET \"amount\" = \"t\".\"AMOUNT\"\n"
        + "WHEN NOT MATCHED THEN INSERT (\"store_id\", \"account_id\", \"exp_date\", \"time_id\", "
        + "\"category_id\", \"currency_id\", \"amount\") VALUES \"t\".\"STORE_ID\",\n"
        + "666,\nTIMESTAMP '1997-01-01 00:00:00',\n666,\n'666',\n666,\n"
        + "CAST(\"t\".\"AMOUNT\" AS DECIMAL(10, 4))";
    final AssertThat that =
        CalciteAssert.model(FoodmartSchema.FOODMART_MODEL)
            .enable(CalciteAssert.DB == DatabaseInstance.HSQLDB);
    that.doWithConnection(connection -> {
      try (LockWrapper ignore = exclusiveCleanDb(connection)) {
        that.query(sql)
            .explainContains(explain)
            .planUpdateHasSql(jdbcSql, 1);
      } catch (SQLException e) {
        throw TestUtil.rethrow(e);
      }
    });
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6221">[CALCITE-6221]</a>.*/
  @Test void testUnknownColumn() {
    CalciteAssert.model(JdbcTest.SCOTT_MODEL)
        .query("SELECT\n"
          + "    \"content-format-owner\",\n"
          + "    \"content-owner\"\n"
          + "FROM\n"
          + "    (\n"
          + "        SELECT\n"
          + "            d1.dname AS \"content-format-owner\",\n"
          + "            d2.dname || ' ' AS \"content-owner\"\n"
          + "        FROM\n"
          + "            scott.emp e1\n"
          + "            left outer join scott.dept d1 on e1.deptno = d1.deptno\n"
          + "            left outer join scott.dept d2 on e1.deptno = d2.deptno\n"
          + "            left outer join scott.emp e2 on e1.deptno = e2.deptno\n"
          + "        GROUP BY\n"
          + "            d1.dname,\n"
          + "            d2.dname\n"
          + "    )\n"
          + "WHERE\n"
          + "    \"content-owner\" IN (?)")
        .planHasSql("SELECT "
            + "\"t2\".\"DNAME\" AS \"content-format-owner\", "
            + "\"t2\".\"DNAME0\" || ' ' AS \"content-owner\"\n"
            + "FROM (SELECT \"t\".\"DEPTNO\" AS \"DEPTNO\", "
            + "\"t0\".\"DEPTNO\" AS \"DEPTNO0\", "
            + "\"t0\".\"DNAME\" AS \"DNAME\", "
            + "\"t1\".\"DEPTNO\" AS \"DEPTNO1\", "
            + "\"t1\".\"DNAME\" AS \"DNAME0\"\n"
            + "FROM (SELECT \"DEPTNO\"\n"
            + "FROM \"SCOTT\".\"EMP\") AS \"t\"\n"
            + "LEFT JOIN (SELECT \"DEPTNO\", \"DNAME\"\n"
            + "FROM \"SCOTT\".\"DEPT\") AS \"t0\" ON \"t\".\"DEPTNO\" = \"t0\".\"DEPTNO\"\n"
            + "LEFT JOIN (SELECT \"DEPTNO\", \"DNAME\"\n"
            + "FROM \"SCOTT\".\"DEPT\") AS \"t1\" "
            + "ON \"t\".\"DEPTNO\" = \"t1\".\"DEPTNO\"\n"
            + "WHERE \"t1\".\"DNAME\" || ' ' = ?) AS \"t2\"\n"
            + "LEFT JOIN (SELECT \"DEPTNO\"\n"
            + "FROM \"SCOTT\".\"EMP\") AS \"t3\" "
            + "ON \"t2\".\"DEPTNO\" = \"t3\".\"DEPTNO\"\n"
            + "GROUP BY \"t2\".\"DNAME\", \"t2\".\"DNAME0\"")
        .runs();
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4188">[CALCITE-4188]
   * Support EnumerableBatchNestedLoopJoin for JDBC</a>. */
  @Test void testBatchNestedLoopJoinPlan() {
    final String sql = "SELECT *\n"
        + "FROM \"s\".\"emps\" A\n"
        + "LEFT OUTER JOIN \"foodmart\".\"store\" B ON A.\"empid\" = B.\"store_id\"";
    final String explain = "JdbcFilter(condition=[OR(=($cor0.empid0, $0), =($cor1.empid0, $0)";
    final String jdbcSql = "SELECT *\n"
        + "FROM \"foodmart\".\"store\"\n"
        + "WHERE ? = \"store_id\" OR (? = \"store_id\" OR ? = \"store_id\") OR (? = \"store_id\" OR"
        + " (? = \"store_id\" OR ? = \"store_id\")) OR (? = \"store_id\" OR (? = \"store_id\" OR ? "
        + "= \"store_id\") OR (? = \"store_id\" OR (? = \"store_id\" OR ? = \"store_id\"))) OR (? ="
        + " \"store_id\" OR (? = \"store_id\" OR ? = \"store_id\") OR (? = \"store_id\" OR (? = "
        + "\"store_id\" OR ? = \"store_id\")) OR (? = \"store_id\" OR (? = \"store_id\" OR ? = "
        + "\"store_id\") OR (? = \"store_id\" OR ? = \"store_id\" OR (? = \"store_id\" OR ? = "
        + "\"store_id\")))) OR (? = \"store_id\" OR (? = \"store_id\" OR ? = \"store_id\") OR (? = "
        + "\"store_id\" OR (? = \"store_id\" OR ? = \"store_id\")) OR (? = \"store_id\" OR (? = "
        + "\"store_id\" OR ? = \"store_id\") OR (? = \"store_id\" OR (? = \"store_id\" OR ? = "
        + "\"store_id\"))) OR (? = \"store_id\" OR (? = \"store_id\" OR ? = \"store_id\") OR (? = "
        + "\"store_id\" OR (? = \"store_id\" OR ? = \"store_id\")) OR (? = \"store_id\" OR (? = "
        + "\"store_id\" OR ? = \"store_id\") OR (? = \"store_id\" OR ? = \"store_id\" OR (? = "
        + "\"store_id\" OR ? = \"store_id\"))))) OR (? = \"store_id\" OR (? = \"store_id\" OR ? = "
        + "\"store_id\") OR (? = \"store_id\" OR (? = \"store_id\" OR ? = \"store_id\")) OR (? = "
        + "\"store_id\" OR (? = \"store_id\" OR ? = \"store_id\") OR (? = \"store_id\" OR (? = "
        + "\"store_id\" OR ? = \"store_id\"))) OR (? = \"store_id\" OR (? = \"store_id\" OR ? = "
        + "\"store_id\") OR (? = \"store_id\" OR (? = \"store_id\" OR ? = \"store_id\")) OR (? = "
        + "\"store_id\" OR (? = \"store_id\" OR ? = \"store_id\") OR (? = \"store_id\" OR ? = "
        + "\"store_id\" OR (? = \"store_id\" OR ? = \"store_id\")))) OR (? = \"store_id\" OR (? = "
        + "\"store_id\" OR ? = \"store_id\") OR (? = \"store_id\" OR (? = \"store_id\" OR ? = "
        + "\"store_id\")) OR (? = \"store_id\" OR (? = \"store_id\" OR ? = \"store_id\") OR (? = "
        + "\"store_id\" OR (? = \"store_id\" OR ? = \"store_id\"))) OR (? = \"store_id\" OR (? = "
        + "\"store_id\" OR ? = \"store_id\") OR (? = \"store_id\" OR (? = \"store_id\" OR ? = "
        + "\"store_id\")) OR (? = \"store_id\" OR (? = \"store_id\" OR ? = \"store_id\") OR (? = "
        + "\"store_id\" OR ? = \"store_id\" OR (? = \"store_id\" OR ? = \"store_id\"))))))";
    CalciteAssert.model(FoodmartSchema.FOODMART_MODEL)
        .withSchema("s", new ReflectiveSchema(new HrSchema()))
        .withHook(Hook.PLANNER, (Consumer<RelOptPlanner>) planner -> {
          planner.addRule(EnumerableRules.ENUMERABLE_BATCH_NESTED_LOOP_JOIN_RULE);
        })
        .query(sql)
        .explainContains(explain)
        .runs()
        .enable(CalciteAssert.DB == CalciteAssert.DatabaseInstance.HSQLDB
            || CalciteAssert.DB == DatabaseInstance.POSTGRESQL)
        .planHasSql(jdbcSql)
        .returnsCount(4);
  }

  /** Acquires a lock, and releases it when closed. */
  static class LockWrapper implements AutoCloseable {
    private final Lock lock;

    LockWrapper(Lock lock) {
      this.lock = lock;
    }

    /** Acquires a lock and returns a closeable wrapper. */
    static LockWrapper lock(Lock lock) {
      lock.lock();
      return new LockWrapper(lock);
    }

    public void close() {
      lock.unlock();
    }
  }
}
