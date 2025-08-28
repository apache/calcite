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

import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.RepeatUnion;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.TableSpool;
import org.apache.calcite.rel.externalize.RelDotWriter;
import org.apache.calcite.rel.externalize.RelXmlWriter;
import org.apache.calcite.rel.logical.LogicalAsofJoin;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalRepeatUnion;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.fun.SqlLibrary;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlDelegatingConformance;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql.validate.implicit.TypeCoercionImpl;
import org.apache.calcite.test.catalog.MockCatalogReaderExtended;
import org.apache.calcite.util.Bug;
import org.apache.calcite.util.TestUtil;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;

/**
 * Unit test for {@link org.apache.calcite.sql2rel.SqlToRelConverter}.
 * See {@link RelOptRulesTest} for an explanation of how to add tests;
 */
@ResourceLock(value = "SqlToRelConverterTest.xml")
class SqlToRelConverterTest extends SqlToRelTestBase {

  private static final SqlToRelFixture LOCAL_FIXTURE =
      SqlToRelFixture.DEFAULT
          .withDiffRepos(DiffRepository.lookup(SqlToRelConverterTest.class));

  @Nullable
  private static DiffRepository diffRepos = null;

  @AfterAll
  public static void checkActualAndReferenceFiles() {
    if (diffRepos != null) {
      diffRepos.checkActualAndReferenceFiles();
    }
  }

  @Override public SqlToRelFixture fixture() {
    diffRepos = LOCAL_FIXTURE.diffRepos();
    return LOCAL_FIXTURE;
  }

  /** Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-6350">[CALCITE-6350]
   * Unexpected result from UNION with literals expression</a>. */
  @Test void testUnionLiterals() {
    final String sql = "select * from (select 'word' i union all select 'w' i) t1 where i='w'";
    sql(sql).ok();
  }

  @Test void testDotLiteralAfterNestedRow() {
    final String sql = "select ((1,2),(3,4,5)).\"EXPR$1\".\"EXPR$2\" from emp";
    sql(sql).ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3679">[CALCITE-3679]
   * Allow lambda expressions in SQL queries</a>. */
  @Test void testLambdaExpression() {
    final String sql = "select higher_order_function(1, (x, y) -> y + 1)";
    fixture()
        .withFactory(c ->
            c.withOperatorTable(t -> MockSqlOperatorTable.standard().extend()))
        .withSql(sql)
        .ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3679">[CALCITE-3679]
   * Allow lambda expressions in SQL queries</a>. */
  @Test void testLambdaExpression2() {
    final String sql = "select higher_order_function2(1, () -> -1)";
    fixture()
        .withFactory(c ->
            c.withOperatorTable(t -> MockSqlOperatorTable.standard().extend()))
        .withSql(sql)
        .ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3679">[CALCITE-3679]
   * Allow lambda expressions in SQL queries</a>. */
  @Test void testLambdaExpression3() {
    final String sql = "select higher_order_function(deptno, (x, deptno) -> deptno + 1) from emp";
    fixture()
        .withFactory(c ->
            c.withOperatorTable(t -> MockSqlOperatorTable.standard().extend()))
        .withSql(sql)
        .ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6116">[CALCITE-6116]
   * Add EXISTS function (enabled in Spark library)</a>. */
  @Test void testExistsFunctionInSpark() {
    final String sql = "select \"EXISTS\"(array(1,2,3), x -> false)";
    fixture()
        .withFactory(c ->
            c.withOperatorTable(t -> SqlValidatorTest.operatorTableFor(SqlLibrary.SPARK)))
        .withSql(sql)
        .ok();
  }

  @Test void testDotLiteralAfterRow() {
    final String sql = "select row(1,2).\"EXPR$1\" from emp";
    sql(sql).ok();
  }

  @Test void testDotAfterParenthesizedIdentifier() {
    final String sql = "select (home_address).city from emp_address";
    sql(sql).ok();
  }

  @Test void testRowValueConstructorWithSubQuery() {
    final String sql = "select ROW("
        + "(select deptno\n"
        + "from dept\n"
        + "where dept.deptno = emp.deptno), emp.ename)\n"
        + "from emp";
    sql(sql).ok();
  }

  @Test void testIntegerLiteral() {
    final String sql = "select 1 from emp";
    sql(sql).ok();
  }

  @Test void testIntervalLiteralYearToMonth() {
    final String sql = "select\n"
        + "  cast(empno as Integer) * (INTERVAL '1-1' YEAR TO MONTH)\n"
        + "from emp";
    sql(sql).ok();
  }

  @Test void testIntervalLiteralHourToMinute() {
    final String sql = "select\n"
        + " cast(empno as Integer) * (INTERVAL '1:1' HOUR TO MINUTE)\n"
        + "from emp";
    sql(sql).ok();
  }

  @Test void testIntervalExpression() {
    sql("select interval mgr hour as h from emp").ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6115">[CALCITE-6115]
   * Interval type specifier with zero fractional second precision does not pass validation</a>.
   */
  @Test void testIntervalSecondNoFractionalPart() {
    sql("select interval '1' second(1,0) as h from emp").ok();
  }

  @Test void testAliasList() {
    final String sql = "select a + b from (\n"
        + "  select deptno, 1 as uno, name from dept\n"
        + ") as d(a, b, c)\n"
        + "where c like 'X%'";
    sql(sql).ok();
  }

  @Test void testAliasList2() {
    final String sql = "select * from (\n"
        + "  select a, b, c from (values (1, 2, 3)) as t (c, b, a)\n"
        + ") join dept on dept.deptno = c\n"
        + "order by c + a";
    sql(sql).ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2468">[CALCITE-2468]
   * struct type alias should not cause IndexOutOfBoundsException</a>.
   */
  @Test void testStructTypeAlias() {
    final String sql = "select t.r AS myRow\n"
        + "from (select row(row(1)) r from dept) t";
    sql(sql).ok();
  }

  @Test void testJoinUsingDynamicTable() {
    final String sql = "select * from SALES.NATION t1\n"
        + "join SALES.NATION t2\n"
        + "using (n_nationkey)";
    sql(sql).withDynamicTable().ok();
  }

  /**
   * Tests that AND(x, AND(y, z)) gets flattened to AND(x, y, z).
   */
  @Test void testMultiAnd() {
    final String sql = "select * from emp\n"
        + "where deptno < 10\n"
        + "and deptno > 5\n"
        + "and (deptno = 8 or empno < 100)";
    sql(sql).ok();
  }

  @Test void testJoinOn() {
    final String sql = "SELECT * FROM emp\n"
        + "JOIN dept on emp.deptno = dept.deptno";
    sql(sql).ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6770">[CALCITE-6770]
   * Preserve column names when casts are inserted in projects</a>. */
  @Test void testCastNames() {
    final String sql = "SELECT * FROM (SELECT empno, 'x' AS X FROM emp) "
        + "UNION ALL (SELECT empno, 'xx' AS X from emp)";
    sql(sql).ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-245">[CALCITE-245]
   * Off-by-one translation of ON clause of JOIN</a>. */
  @Test void testConditionOffByOne() {
    // Bug causes the plan to contain
    //   LogicalJoin(condition=[=($9, $9)], joinType=[inner])
    final String sql = "SELECT * FROM emp\n"
        + "JOIN dept on emp.deptno + 0 = dept.deptno";
    sql(sql).ok();
  }

  @Test void testConditionOffByOneReversed() {
    final String sql = "SELECT * FROM emp\n"
        + "JOIN dept on dept.deptno = emp.deptno + 0";
    sql(sql).ok();
  }

  @Test void testJoinOnExpression() {
    final String sql = "SELECT * FROM emp\n"
        + "JOIN dept on emp.deptno + 1 = dept.deptno - 2";
    sql(sql).ok();
  }

  @Test void testJoinOnIn() {
    final String sql = "select * from emp join dept\n"
        + " on emp.deptno = dept.deptno and emp.empno in (1, 3)";
    sql(sql).ok();
  }

  @Test void testAsOfJoin() {
    final String sql = "select emp.empno from emp asof join dept\n"
        + "match_condition emp.deptno <= dept.deptno\n"
        + "on ename = name";
    sql(sql).ok();
  }

  /** Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-6540">
   * RelOptUtil.pushDownJoinConditions does not correctly adjust ASOF joins match conditions</a>.
   */
  @Test void testAsOfCast() {
    final String sql = "SELECT * "
        + "FROM (SELECT CAST(deptno % 10 AS BIGINT) as m, CAST(deptno AS BIGINT) as deptno FROM dept) D\n"
        + "LEFT ASOF JOIN (SELECT CAST(empno as BIGINT) as empno, CAST(deptno AS BIGINT) AS deptno FROM emp) E\n"
        + "MATCH_CONDITION D.deptno >= E.deptno\n"
        + "ON D.m = E.empno";
    sql(sql).withConformance(SqlConformanceEnum.LENIENT).ok();
  }

  @Test void testJoinOnInSubQuery() {
    final String sql = "select * from emp left join dept\n"
        + "on emp.empno = 1\n"
        + "or dept.deptno in (select deptno from emp where empno > 5)";
    sql(sql).withExpand(false).ok();
  }

  @Test void testJoinOnExists() {
    final String sql = "select * from emp left join dept\n"
        + "on emp.empno = 1\n"
        + "or exists (select deptno from emp where empno > dept.deptno + 5)";
    sql(sql).withExpand(false).ok();
  }

  @Test void testJoinUsing() {
    sql("SELECT * FROM emp JOIN dept USING (deptno)").ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-74">[CALCITE-74]
   * JOIN ... USING fails in 3-way join with
   * UnsupportedOperationException</a>. */
  @Test void testJoinUsingThreeWay() {
    final String sql = "select *\n"
        + "from emp as e\n"
        + "join dept as d using (deptno)\n"
        + "join emp as e2 using (empno)";
    sql(sql).ok();
  }

  @Test void testJoinUsingCompound() {
    final String sql = "SELECT * FROM emp LEFT JOIN ("
        + "SELECT *, deptno * 5 as empno FROM dept) "
        + "USING (deptno,empno)";
    sql(sql).ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-801">[CALCITE-801]
   * NullPointerException using USING on table alias with column aliases</a>. */
  @Test void testValuesUsing() {
    final String sql = "select d.deptno, min(e.empid) as empid\n"
        + "from (values (100, 'Bill', 1)) as e(empid, name, deptno)\n"
        + "join (values (1, 'LeaderShip')) as d(deptno, name)\n"
        + "  using (deptno)\n"
        + "group by d.deptno";
    sql(sql).ok();
  }

  @Test void testJoinNatural() {
    sql("SELECT * FROM emp NATURAL JOIN dept").ok();
  }

  @Test void testJoinNaturalNoCommonColumn() {
    final String sql = "SELECT *\n"
        + "FROM emp NATURAL JOIN (SELECT deptno AS foo, name FROM dept) AS d";
    sql(sql).ok();
  }

  @Test void testJoinNaturalMultipleCommonColumn() {
    final String sql = "SELECT *\n"
        + "FROM emp\n"
        + "NATURAL JOIN (SELECT deptno, name AS ename FROM dept) AS d";
    sql(sql).ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4915">[CALCITE-4915]
   * Query with unqualified common column and NATURAL JOIN fails</a>. */
  @Test void testJoinNaturalWithUnqualifiedCommonColumn() {
    final String sql = "SELECT deptno, name\n"
        + "FROM emp\n"
        + "NATURAL JOIN dept";
    sql(sql).ok();
  }

  /** Similar to {@link #testJoinNaturalWithUnqualifiedCommonColumn()},
   * but with nested common column. */
  @Test void testJoinNaturalWithUnqualifiedNestedCommonColumn() {
    final String sql =
        "select (coord).x\n"
            + "from customer.contact_peek t1\n"
            + "natural join customer.contact_peek t2";
    sql(sql).ok();
  }

  /** Similar to {@link #testJoinNaturalWithUnqualifiedCommonColumn()},
   * but with aggregate. */
  @Test void testJoinNaturalWithAggregate() {
    final String sql = "select deptno, count(*)\n"
        + "from emp\n"
        + "natural join dept\n"
        + "group by deptno";
    sql(sql).ok();
  }

  /** Similar to {@link #testJoinNaturalWithUnqualifiedCommonColumn()},
   * but with grouping sets. */
  @Test void testJoinNaturalWithGroupingSets() {
    final String sql = "select deptno, grouping(deptno),\n"
        + "grouping(deptno, job), count(*)\n"
        + "from emp\n"
        + "natural join dept\n"
        + "group by grouping sets ((deptno), (deptno, job))";
    sql(sql).ok();
  }

  /** Similar to {@link #testJoinNaturalWithUnqualifiedCommonColumn()},
   * but with multiple join. */
  @Test void testJoinNaturalWithMultipleJoin() {
    final String sql = "SELECT deptno, ename\n"
        + "FROM emp\n"
        + "NATURAL JOIN dept\n"
        + "NATURAL JOIN (values ('Calcite', 200)) as s(ename, salary)";
    sql(sql).ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3387">[CALCITE-3387]
   * Query with GROUP BY and JOIN ... USING wrongly fails with
   * "Column 'DEPTNO' is ambiguous"</a>. */
  @Test void testJoinUsingWithUnqualifiedCommonColumn() {
    final String sql = "SELECT deptno, name\n"
        + "FROM emp JOIN dept using (deptno)";
    sql(sql).ok();
  }

  /** Similar to {@link #testJoinUsingWithUnqualifiedCommonColumn()},
   * but with nested common column. */
  @Test void testJoinUsingWithUnqualifiedNestedCommonColumn() {
    final String sql =
        "select (coord).x from\n"
            + "customer.contact_peek t1\n"
            + "join customer.contact_peek t2\n"
            + "using (coord)";
    sql(sql).ok();
  }

  /** Similar to {@link #testJoinUsingWithUnqualifiedCommonColumn()},
   * but with aggregate. */
  @Test void testJoinUsingWithAggregate() {
    final String sql = "select deptno, count(*)\n"
        + "from emp\n"
        + "full join dept using (deptno)\n"
        + "group by deptno";
    sql(sql).ok();
  }

  /** Similar to {@link #testJoinUsingWithUnqualifiedCommonColumn()},
   * but with grouping sets. */
  @Test void testJoinUsingWithGroupingSets() {
    final String sql = "select deptno, grouping(deptno),\n"
        + "grouping(deptno, job), count(*)\n"
        + "from emp\n"
        + "join dept using (deptno)\n"
        + "group by grouping sets ((deptno), (deptno, job))";
    sql(sql).ok();
  }

  /** Similar to {@link #testJoinUsingWithUnqualifiedCommonColumn()},
   * but with multiple join. */
  @Test void testJoinUsingWithMultipleJoin() {
    final String sql = "SELECT deptno, ename\n"
        + "FROM emp "
        + "JOIN dept using (deptno)\n"
        + "JOIN (values ('Calcite', 200)) as s(ename, salary) using (ename)";
    sql(sql).ok();
  }

  @Test void testJoinWithUnion() {
    final String sql = "select grade\n"
        + "from (select empno from emp union select deptno from dept),\n"
        + "  salgrade";
    sql(sql).ok();
  }

  @Test void testGroup() {
    sql("select deptno from emp group by deptno").ok();
  }

  @Test void testGroupByAlias() {
    sql("select empno as d from emp group by d")
        .withConformance(SqlConformanceEnum.LENIENT).ok();
  }

  @Test void testGroupByAliasOfSubExpressionsInProject() {
    final String sql = "select deptno+empno as d, deptno+empno+mgr\n"
        + "from emp group by d,mgr";
    sql(sql)
        .withConformance(SqlConformanceEnum.LENIENT).ok();
  }

  @Test void testGroupByAliasEqualToColumnName() {
    // If the alias (deptno) matches an existing column, it is not used in the GROUP BY
    sql("select empno, ename as deptno from emp group by empno, deptno")
        .withConformance(SqlConformanceEnum.LENIENT)
        .throws_("Expression 'ENAME' is not being grouped");
    // If the alias is a new one, it is used in the GROUP BY
    sql("select empno, ename as x from emp group by empno, x")
        .withConformance(SqlConformanceEnum.LENIENT).ok();
  }

  @Test void testGroupByOrdinal() {
    sql("select empno from emp group by 1")
        .withConformance(SqlConformanceEnum.LENIENT).ok();
  }

  @Test void testGroupByContainsLiterals() {
    final String sql = "select count(*) from (\n"
        + "  select 1 from emp group by substring(ename from 2 for 3))";
    sql(sql)
        .withConformance(SqlConformanceEnum.LENIENT).ok();
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4512">[CALCITE-4512]
   * GROUP BY expression with argument name same with SELECT field and alias causes
   * validation error</a>.
   */
  @Test void testGroupByExprArgFieldSameWithAlias() {
    final String sql = "SELECT floor(deptno / 2) AS deptno\n"
        + "FROM emp\n"
        + "GROUP BY floor(deptno / 2)";
    sql(sql)
        .withConformance(SqlConformanceEnum.LENIENT)
        .ok();
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4512">[CALCITE-4512]
   * GROUP BY expression with argument name same with SELECT field and alias causes
   * validation error</a>.
   */
  @Test void testGroupByExprArgFieldSameWithAlias2() {
    final String sql = "SELECT deptno / 2 AS deptno, deptno / 2 as empno, sum(sal)\n"
        + "FROM emp\n"
        + "GROUP BY GROUPING SETS "
        + "((deptno), (empno, deptno / 2), (2, 1), ((1, 2), (deptno, deptno / 2)))";
    sql(sql)
        .withConformance(SqlConformanceEnum.LENIENT)
        .ok();
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4512">[CALCITE-4512]
   * GROUP BY expression with argument name same with SELECT field and alias causes
   * validation error</a>.
   */
  @Test void testGroupByExprArgFieldSameWithAlias3() {
    // Same as the test above, but different conformance.
    // Must produce the exact same plan.
    final String sql = "SELECT deptno / 2 AS deptno, deptno / 2 as empno, sum(sal)\n"
        + "FROM emp\n"
        + "GROUP BY GROUPING SETS "
        + "((deptno), (empno, deptno / 2), (2, 1), ((1, 2), (deptno, deptno / 2)))";
    sql(sql)
        .withConformance(
            // This ensures that numbers in grouping sets are interpreted as column numbers
            new SqlDelegatingConformance(SqlConformanceEnum.DEFAULT) {
              @Override public boolean isGroupByOrdinal() {
                return true;
              }
            })
        .ok();
  }

  @Test void testAliasInHaving() {
    sql("select count(empno) as e from emp having e > 1")
        .withConformance(SqlConformanceEnum.LENIENT).ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5507">[CALCITE-5507]
   * HAVING alias failed when aggregate function in condition</a>. */
  @Test void testAggregateFunAndAliasInHaving1() {
    sql("select count(empno) as e\n"
        + "from emp\n"
        + "having e > 10 and count(empno) > 10")
        .withConformance(SqlConformanceEnum.LENIENT).ok();
  }

  @Test void testAggregateFunAndAliasInHaving2() {
    sql("select count(empno) as e\n"
        + "from emp\n"
        + "having e > 10 or count(empno) < 5")
        .withConformance(SqlConformanceEnum.LENIENT).ok();
  }

  @Test void testGroupJustOneAgg() {
    // just one agg
    final String sql =
        "select deptno, sum(sal) as sum_sal from emp group by deptno";
    sql(sql).ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4549">[CALCITE-4549]
   * IndexOutOfBoundsException when group view by a sub query</a>. */
  @Test void testGroupView() {
    final String sql = "SELECT case when ENAME in( 'a', 'b') then 'c' else 'd' end\n"
        + "from EMP_20\n"
        + "group by case when ENAME in( 'a', 'b') then 'c' else 'd' end";
    sql(sql).ok();
  }

  @Test void testGroupExpressionsInsideAndOut() {
    // Expressions inside and outside aggs. Common sub-expressions should be
    // eliminated: 'sal' always translates to expression #2.
    final String sql = "select\n"
        + "  deptno + 4, sum(sal), sum(3 + sal), 2 * count(sal)\n"
        + "from emp group by deptno";
    sql(sql).ok();
  }

  @Test void testAggregateNoGroup() {
    sql("select sum(deptno) from emp").ok();
  }

  @Test void testGroupEmpty() {
    sql("select sum(deptno) from emp group by ()").ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5717">[CALCITE-5717]
   * RelBuilder.project should generate a Values if all expressions are literals
   * and the input is an Aggregate that returns exactly one row</a>. */
  @Test void testGroupEmptyYieldLiteral() {
    // Expected plan is "VALUES 42". The result is one row even if EMP is empty.
    sql("select 42 from emp group by ()").ok();
  }

  // Same effect as writing "GROUP BY deptno"
  @Test void testSingletonGroupingSet() {
    sql("select sum(sal) from emp group by grouping sets (deptno)").ok();
  }

  @Test void testGroupingSets() {
    final String sql = "select deptno, ename, sum(sal) from emp\n"
        + "group by grouping sets ((deptno), (ename, deptno))\n"
        + "order by 2";
    sql(sql).ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2147">[CALCITE-2147]
   * Incorrect plan in with with ROLLUP inside GROUPING SETS</a>.
   *
   * <p>Equivalence example:
   *
   * <blockquote>GROUP BY GROUPING SETS (ROLLUP(A, B), CUBE(C,D))</blockquote>
   *
   * <p>is equal to
   *
   * <blockquote>GROUP BY GROUPING SETS ((A,B), (A), (),
   * (C,D), (C), (D) )</blockquote>
   */
  @Test void testGroupingSetsWithRollup() {
    final String sql = "select deptno, ename, sum(sal) from emp\n"
        + "group by grouping sets ( rollup(deptno), (ename, deptno))\n"
        + "order by 2";
    sql(sql).ok();
  }

  @Test void testGroupingSetsWithCube() {
    final String sql = "select deptno, ename, sum(sal) from emp\n"
        + "group by grouping sets ( (deptno), CUBE(ename, deptno))\n"
        + "order by 2";
    sql(sql).ok();
  }

  @Test void testGroupingSetsWithRollupCube() {
    final String sql = "select deptno, ename, sum(sal) from emp\n"
        + "group by grouping sets ( CUBE(deptno), ROLLUP(ename, deptno))\n"
        + "order by 2";
    sql(sql).ok();
  }

  @Test void testGroupingSetsProduct() {
    // Example in SQL:2011:
    //   GROUP BY GROUPING SETS ((A, B), (C)), GROUPING SETS ((X, Y), ())
    // is transformed to
    //   GROUP BY GROUPING SETS ((A, B, X, Y), (A, B), (C, X, Y), (C))
    final String sql = "select 1\n"
        + "from (values (0, 1, 2, 3, 4)) as t(a, b, c, x, y)\n"
        + "group by grouping sets ((a, b), c), grouping sets ((x, y), ())";
    sql(sql).ok();
  }

  /** When the GROUPING function occurs with GROUP BY (effectively just one
   * grouping set), we can translate it directly to 1. */
  @Test void testGroupingFunctionWithGroupBy() {
    final String sql = "select\n"
        + "  deptno, grouping(deptno), count(*), grouping(empno)\n"
        + "from emp\n"
        + "group by empno, deptno\n"
        + "order by 2";
    sql(sql).ok();
  }

  @Test void testGroupingFunction() {
    final String sql = "select\n"
        + "  deptno, grouping(deptno), count(*), grouping(empno)\n"
        + "from emp\n"
        + "group by rollup(empno, deptno)";
    sql(sql).ok();
  }

  /**
   * GROUP BY with duplicates.
   *
   * <p>From SQL spec:
   * <blockquote>NOTE 190 &mdash; That is, a simple <em>group by clause</em>
   * that is not primitive may be transformed into a primitive <em>group by
   * clause</em> by deleting all parentheses, and deleting extra commas as
   * necessary for correct syntax. If there are no grouping columns at all (for
   * example, GROUP BY (), ()), this is transformed to the canonical form GROUP
   * BY ().
   * </blockquote> */
  // Same effect as writing "GROUP BY ()"
  @Test void testGroupByWithDuplicates() {
    sql("select sum(sal) from emp group by (), ()").ok();
  }

  /** GROUP BY with duplicate (and heavily nested) GROUPING SETS. */
  @Test void testDuplicateGroupingSets() {
    final String sql = "select sum(sal) from emp\n"
        + "group by sal,\n"
        + "  grouping sets (deptno,\n"
        + "    grouping sets ((deptno, ename), ename),\n"
        + "      (ename)),\n"
        + "  ()";
    sql(sql).ok();
  }

  @Test void testGroupingSetsCartesianProduct() {
    // Equivalent to (a, c), (a, d), (b, c), (b, d)
    final String sql = "select 1\n"
        + "from (values (1, 2, 3, 4)) as t(a, b, c, d)\n"
        + "group by grouping sets (a, b), grouping sets (c, d)";
    sql(sql).ok();
  }

  @Test void testGroupingSetsCartesianProduct2() {
    final String sql = "select 1\n"
        + "from (values (1, 2, 3, 4)) as t(a, b, c, d)\n"
        + "group by grouping sets (a, (a, b)), grouping sets (c), d";
    sql(sql).ok();
  }

  @Test void testRollupSimple() {
    // a is nullable so is translated as just "a"
    // b is not null, so is represented as 0 inside Aggregate, then
    // using "CASE WHEN i$b THEN NULL ELSE b END"
    final String sql = "select a, b, count(*) as c\n"
        + "from (values (cast(null as integer), 2)) as t(a, b)\n"
        + "group by rollup(a, b)";
    sql(sql).ok();
  }

  @Test void testRollup() {
    // Equivalent to {(a, b), (a), ()}  * {(c, d), (c), ()}
    final String sql = "select 1\n"
        + "from (values (1, 2, 3, 4)) as t(a, b, c, d)\n"
        + "group by rollup(a, b), rollup(c, d)";
    sql(sql).ok();
  }

  @Test void testRollupTuples() {
    // rollup(b, (a, d)) is (b, a, d), (b), ()
    final String sql = "select 1\n"
        + "from (values (1, 2, 3, 4)) as t(a, b, c, d)\n"
        + "group by rollup(b, (a, d))";
    sql(sql).ok();
  }

  @Test void testCube() {
    // cube(a, b) is {(a, b), (a), (b), ()}
    final String sql = "select 1\n"
        + "from (values (1, 2, 3, 4)) as t(a, b, c, d)\n"
        + "group by cube(a, b)";
    sql(sql).ok();
  }

  @Test void testGroupingSetsRepeated() {
    final String sql = "select deptno, group_id()\n"
        + "from emp\n"
        + "group by grouping sets (deptno, (), job, (deptno, job), deptno,\n"
        + "  job, deptno)";
    sql(sql).ok();
  }

  /** As {@link #testGroupingSetsRepeated()} but with no {@code GROUP_ID}
   * function. (We still need the plan to contain a Union.) */
  @Test void testGroupingSetsRepeatedNoGroupId() {
    final String sql = "select deptno, job\n"
        + "from emp\n"
        + "group by grouping sets (deptno, (), job, (deptno, job), deptno,\n"
        + "  job, deptno)";
    sql(sql).ok();
  }

  /** As {@link #testGroupingSetsRepeated()} but grouping sets are distinct.
   * The {@code GROUP_ID} is replaced by 0.*/
  @Test void testGroupingSetsWithGroupId() {
    final String sql = "select deptno, group_id()\n"
        + "from emp\n"
        + "group by grouping sets (deptno, (), job)";
    sql(sql).ok();
  }

  @Test void testRecursiveQuery() {
    final String sql = "WITH RECURSIVE aux(i) AS (\n"
        + "  VALUES (1)\n"
        + "  UNION ALL\n"
        + "  SELECT i+1 FROM aux WHERE i < 10\n"
        + ")\n"
        + "SELECT * FROM aux";
    sql(sql).ok();
  }

  @Test void testGroupingSetsWith() {
    final String sql = "with t(a, b, c, d) as (values (1, 2, 3, 4))\n"
        + "select 1 from t\n"
        + "group by rollup(a, b), rollup(c, d)";
    sql(sql).ok();
  }

  @Test void testHaving() {
    // empty group-by clause, having
    final String sql = "select sum(sal + sal) from emp having sum(sal) > 10";
    sql(sql).ok();
  }

  @Test void testGroupBug281() {
    // Dtbug 281 gives:
    //   Internal error:
    //   Type 'RecordType(VARCHAR(128) $f0)' has no field 'NAME'
    final String sql =
        "select name from (select name from dept group by name)";
    sql(sql).ok();
  }

  @Test void testGroupBug281b() {
    // Try to confuse it with spurious columns.
    final String sql = "select name, foo from (\n"
        + "select deptno, name, count(deptno) as foo\n"
        + "from dept\n"
        + "group by name, deptno, name)";
    sql(sql).ok();
  }

  @Test void testGroupByExpression() {
    // This used to cause an infinite loop,
    // SqlValidatorImpl.getValidatedNodeType
    // calling getValidatedNodeTypeIfKnown
    // calling getValidatedNodeType.
    final String sql = "select count(*)\n"
        + "from emp\n"
        + "group by substring(ename FROM 1 FOR 1)";
    sql(sql).ok();
  }

  @Test void testAggDistinct() {
    final String sql = "select deptno, sum(sal), sum(distinct sal), count(*)\n"
        + "from emp\n"
        + "group by deptno";
    sql(sql).ok();
  }

  @Test void testAggFilter() {
    final String sql = "select\n"
        + "  deptno, sum(sal * 2) filter (where empno < 10), count(*)\n"
        + "from emp\n"
        + "group by deptno";
    sql(sql).ok();
  }

  @Test void testAggFilterWithIn() {
    final String sql = "select\n"
        + "  deptno, sum(sal * 2) filter (where empno not in (1, 2)), count(*)\n"
        + "from emp\n"
        + "group by deptno";
    sql(sql).ok();
  }

  @Test void testAggFilterWithInSubQuery() {
    final String sql = "select\n"
        + "  count(*) filter (where empno in (select deptno from empnullables))\n"
        + "from empnullables";
    sql(sql).withExpand(false).ok();
  }

  @Test void testFakeStar() {
    sql("SELECT * FROM (VALUES (0, 0)) AS T(A, \"*\")").ok();
  }

  @Test void testSelectNull() {
    sql("select null from emp").ok();
  }

  @Test void testSelectNullWithAlias() {
    sql("select null as dummy from emp").ok();
  }

  @Test void testSelectNullWithCast() {
    sql("select cast(null as timestamp) dummy from emp").ok();
  }

  @Test void testSelectDistinct() {
    sql("select distinct sal + 5 from emp").ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-476">[CALCITE-476]
   * DISTINCT flag in windowed aggregates</a>. */
  @Test void testSelectOverDistinct() {
    // Checks to see if <aggregate>(DISTINCT x) is set and preserved
    // as a flag for the aggregate call.
    final String sql = "select SUM(DISTINCT deptno)\n"
        + "over (ORDER BY empno ROWS BETWEEN 10 PRECEDING AND CURRENT ROW)\n"
        + "from emp\n";
    sql(sql).ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6742">[CALCITE-6742]
   * StandardConvertletTable.convertCall loses casts from ROW comparisons</a>. */
  @Test void testStructCast() {
    final String sql = "select ROW(1, 'x') = ROW('y', 1)";
    sql(sql).ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6742">[CALCITE-6742]
   * StandardConvertletTable.convertCall loses casts from ROW comparisons</a>. */
  @Test void testStructCast1() {
    final String sql = "select CAST(CAST(ROW('x', 1) AS "
        + "ROW(l INTEGER, r DOUBLE)) AS ROW(l BIGINT, r INTEGER)) = ROW(RAND(), RAND())";
    sql(sql).ok();
  }

  /** As {@link #testSelectOverDistinct()} but for streaming queries. */
  @Test void testSelectStreamPartitionDistinct() {
    final String sql = "select stream\n"
        + "  count(distinct orderId) over (partition by productId\n"
        + "    order by rowtime\n"
        + "    range interval '1' second preceding) as c,\n"
        + "  count(distinct orderId) over w as c2,\n"
        + "  count(orderId) over w as c3\n"
        + "from orders\n"
        + "window w as (partition by productId)";
    sql(sql).ok();
  }

  @Test void testSelectDistinctGroup() {
    sql("select distinct sum(sal) from emp group by deptno").ok();
  }

  /**
   * Tests that if the clause of SELECT DISTINCT contains duplicate
   * expressions, they are only aggregated once.
   */
  @Test void testSelectDistinctDup() {
    final String sql =
        "select distinct sal + 5, deptno, sal + 5 from emp where deptno < 10";
    sql(sql).ok();
  }

  @Test void testSelectWithoutFrom() {
    final String sql = "select 2+2";
    sql(sql).ok();
  }

  /** Tests referencing columns from a sub-query that has duplicate column
   * names. I think the standard says that this is illegal. We roll with it,
   * and rename the second column to "e0". */
  @Test void testDuplicateColumnsInSubQuery() {
    String sql = "select \"e\" from (\n"
        + "select empno as \"e\", deptno as d, 1 as \"e0\" from EMP)";
    sql(sql).ok();
  }

  @Test void testOrder() {
    final String sql = "select empno from emp order by empno";
    sql(sql).ok();

    // duplicate field is dropped, so plan is same
    final String sql2 = "select empno from emp order by empno, empno asc";
    sql(sql2).ok();

    // ditto
    final String sql3 = "select empno from emp order by empno, empno desc";
    sql(sql3).ok();
  }

  /** Tests that if a column occurs twice in ORDER BY, only the first key is
   * kept. */
  @Test void testOrderBasedRepeatFields() {
    final String sql = "select empno from emp order by empno DESC, empno ASC";
    sql(sql).ok();
  }

  @Test void testOrderDescNullsLast() {
    final String sql = "select empno from emp order by empno desc nulls last";
    sql(sql).ok();
  }

  @Test void testOrderByOrdinalDesc() {
    // This test requires a conformance that sorts by ordinal
    final SqlToRelFixture f = fixture()
        .ensuring(f2 -> f2.getConformance().isSortByOrdinal(),
            f2 -> f2.withConformance(SqlConformanceEnum.ORACLE_10));
    final String sql =
        "select empno + 1, deptno, empno from emp order by 2 desc";
    f.withSql(sql).ok();

    // ordinals rounded down, so 2.5 should have the same effect as 2, and
    // generate identical plan
    final String sql2 =
        "select empno + 1, deptno, empno from emp order by 2.5 desc";
    f.withSql(sql2).ok();
  }

  @Test void testOrderDistinct() {
    // The relexp aggregates by 3 expressions - the 2 select expressions
    // plus the one to sort on. A little inefficient, but acceptable.
    final String sql = "select distinct empno, deptno + 1\n"
            + "from emp order by deptno + 1 + empno";
    sql(sql).ok();
  }

  @Test void testOrderByNegativeOrdinal() {
    // Regardless of whether sort-by-ordinals is enabled, negative ordinals
    // are treated like ordinary numbers.
    final String sql =
        "select empno + 1, deptno, empno from emp order by -1 desc";
    sql(sql).ok();
  }

  @Test void testOrderByOrdinalInExpr() {
    // Regardless of whether sort-by-ordinals is enabled, ordinals
    // inside expressions are treated like integers.
    final String sql =
        "select empno + 1, deptno, empno from emp order by 1 + 2 desc";
    sql(sql).ok();
  }

  @Test void testOrderByIdenticalExpr() {
    // Expression in ORDER BY clause is identical to expression in SELECT
    // clause, so plan should not need an extra project.
    final String sql =
        "select empno + 1 from emp order by deptno asc, empno + 1 desc";
    sql(sql).ok();
  }

  @Test void testOrderByAlias() {
    final String sql =
        "select empno + 1 as x, empno - 2 as y from emp order by y";
    sql(sql).ok();
  }

  @Test void testOrderByAliasInExpr() {
    final String sql = "select empno + 1 as x, empno - 2 as y\n"
        + "from emp order by y + 3";
    sql(sql).ok();
  }

  @Test void testOrderByAliasOverrides() {
    // This test requires a conformance that sorts by alias
    final SqlToRelFixture f = fixture()
        .ensuring(f2 -> f2.getConformance().isSortByAlias(),
            f2 -> f2.withConformance(SqlConformanceEnum.ORACLE_10));

    // plan should contain '(empno + 1) + 3'
    final String sql = "select empno + 1 as empno, empno - 2 as y\n"
        + "from emp order by empno + 3";
    f.withSql(sql).ok();
  }

  @Test void testOrderByAliasDoesNotOverride() {
    // This test requires a conformance that does not sort by alias
    final SqlToRelFixture f = fixture()
        .ensuring(f2 -> !f2.getConformance().isSortByAlias(),
            f2 -> f2.withConformance(SqlConformanceEnum.PRAGMATIC_2003));

    // plan should contain 'empno + 3', not '(empno + 1) + 3'
    final String sql = "select empno + 1 as empno, empno - 2 as y\n"
        + "from emp order by empno + 3";
    f.withSql(sql).ok();
  }

  @Test void testOrderBySameExpr() {
    final String sql = "select empno from emp, dept\n"
        + "order by sal + empno desc, sal * empno, sal + empno desc";
    sql(sql).ok();
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5468">[CALCITE-5468]
   * SqlToRelConverter throws if ORDER BY contains IN</a>.
   */
  @Test void testOrderByWithIn() {
    String sql = "SELECT empno\n"
        + "FROM emp\n"
        + "ORDER BY\n"
        + "CASE WHEN empno IN (1,2) THEN 0 ELSE 1 END";
    sql(sql).ok();
  }

  @Test void testOrderByWithSubQuery() {
    String sql = "SELECT empno\n"
        + "FROM emp\n"
        + "ORDER BY\n"
        + "(SELECT empno FROM emp LIMIT 1)";
    sql(sql).ok();
  }

  @Test void testOrderUnion() {
    final String sql = "select empno, sal from emp\n"
        + "union all\n"
        + "select deptno, deptno from dept\n"
        + "order by sal desc, empno asc";
    sql(sql).ok();
  }

  @Test void testOrderUnionOrdinal() {
    // This test requires a conformance that sorts by ordinal
    final SqlToRelFixture f = fixture()
        .ensuring(f2 -> f2.getConformance().isSortByOrdinal(),
            f2 -> f2.withConformance(SqlConformanceEnum.ORACLE_10));
    final String sql = "select empno, sal from emp\n"
        + "union all\n"
        + "select deptno, deptno from dept\n"
        + "order by 2";
    f.withSql(sql).ok();
  }

  @Test void testOrderUnionExprs() {
    final String sql = "select empno, sal from emp\n"
        + "union all\n"
        + "select deptno, deptno from dept\n"
        + "order by empno * sal + 2";
    sql(sql).ok();
  }

  @Test void testOrderOffsetFetch() {
    final String sql = "select empno from emp\n"
        + "order by empno offset 10 rows fetch next 5 rows only";
    sql(sql).ok();
  }

  @Test void testOrderOffsetFetchWithDynamicParameter() {
    final String sql = "select empno from emp\n"
        + "order by empno offset ? rows fetch next ? rows only";
    sql(sql).ok();
  }

  @Test void testOffsetFetch() {
    final String sql = "select empno from emp\n"
        + "offset 10 rows fetch next 5 rows only";
    sql(sql).ok();
  }

  @Test void testOffsetFetchWithDynamicParameter() {
    final String sql = "select empno from emp\n"
        + "offset ? rows fetch next ? rows only";
    sql(sql).ok();
  }

  @Test void testOffset() {
    final String sql = "select empno from emp offset 10 rows";
    sql(sql).ok();
  }

  @Test void testOffsetWithDynamicParameter() {
    final String sql = "select empno from emp offset ? rows";
    sql(sql).ok();
  }

  @Test void testFetch() {
    final String sql = "select empno from emp fetch next 5 rows only";
    sql(sql).ok();
  }

  @Test void testFetchWithDynamicParameter() {
    final String sql = "select empno from emp fetch next ? rows only";
    sql(sql).ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-439">[CALCITE-439]
   * SqlValidatorUtil.uniquify() may not terminate under some conditions</a>. */
  @Test void testGroupAlias() {
    final String sql = "select \"$f2\", max(x), max(x + 1)\n"
        + "from (values (1, 2)) as t(\"$f2\", x)\n"
        + "group by \"$f2\"";
    sql(sql).ok();
  }

  @Test void testOrderGroup() {
    final String sql = "select deptno, count(*)\n"
        + "from emp\n"
        + "group by deptno\n"
        + "order by deptno * sum(sal) desc, min(empno)";
    sql(sql).ok();
  }

  @Test void testCountNoGroup() {
    final String sql = "select count(*), sum(sal)\n"
        + "from emp\n"
        + "where empno > 10";
    sql(sql).ok();
  }

  @Test void testWith() {
    final String sql = "with emp2 as (select * from emp)\n"
        + "select * from emp2";
    sql(sql).ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-309">[CALCITE-309]
   * WITH ... ORDER BY query gives AssertionError</a>. */
  @Test void testWithOrder() {
    final String sql = "with emp2 as (select * from emp)\n"
        + "select * from emp2 order by deptno";
    sql(sql).ok();
  }

  @Test void testWithUnionOrder() {
    final String sql = "with emp2 as (select empno, deptno as x from emp)\n"
        + "select * from emp2\n"
        + "union all\n"
        + "select * from emp2\n"
        + "order by empno + x";
    sql(sql).ok();
  }

  @Test void testWithUnion() {
    final String sql = "with emp2 as (select * from emp where deptno > 10)\n"
        + "select empno from emp2 where deptno < 30\n"
        + "union all\n"
        + "select deptno from emp";
    sql(sql).ok();
  }

  @Test void testWithAlias() {
    final String sql = "with w(x, y) as\n"
        + "  (select * from dept where deptno > 10)\n"
        + "select x from w where x < 30 union all select deptno from dept";
    sql(sql).ok();
  }

  @Test void testWithInsideWhereExists() {
    final String sql = "select * from emp\n"
        + "where exists (\n"
        + "  with dept2 as (select * from dept where dept.deptno >= emp.deptno)\n"
        + "  select 1 from dept2 where deptno <= emp.deptno)";
    sql(sql).withDecorrelate(false).ok();
  }

  @Test void testWithInsideWhereExistsRex() {
    final String sql = "select * from emp\n"
        + "where exists (\n"
        + "  with dept2 as (select * from dept where dept.deptno >= emp.deptno)\n"
        + "  select 1 from dept2 where deptno <= emp.deptno)";
    sql(sql).withDecorrelate(false).withExpand(false).ok();
  }

  @Test void testWithInsideWhereExistsDecorrelate() {
    final String sql = "select * from emp\n"
        + "where exists (\n"
        + "  with dept2 as (select * from dept where dept.deptno >= emp.deptno)\n"
        + "  select 1 from dept2 where deptno <= emp.deptno)";
    sql(sql).withDecorrelate(true).ok();
  }

  @Test void testWithInsideWhereExistsDecorrelateRex() {
    final String sql = "select * from emp\n"
        + "where exists (\n"
        + "  with dept2 as (select * from dept where dept.deptno >= emp.deptno)\n"
        + "  select 1 from dept2 where deptno <= emp.deptno)";
    sql(sql).withDecorrelate(true).withExpand(false).ok();
  }

  @Test void testWithInsideScalarSubQuery() {
    final String sql = "select (\n"
        + " with dept2 as (select * from dept where deptno > 10)"
        + " select count(*) from dept2) as c\n"
        + "from emp";
    sql(sql).ok();
  }

  @Test void testWithInsideScalarSubQueryRex() {
    final String sql = "select (\n"
        + " with dept2 as (select * from dept where deptno > 10)"
        + " select count(*) from dept2) as c\n"
        + "from emp";
    sql(sql).withExpand(false).ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-365">[CALCITE-365]
   * AssertionError while translating query with WITH and correlated
   * sub-query</a>. */
  @Test void testWithExists() {
    final String sql = "with t (a, b) as (select * from (values (1, 2)))\n"
        + "select * from t where exists (\n"
        + "  select 1 from emp where deptno = t.a)";
    sql(sql).ok();
  }

  @Test void testTableSubset() {
    final String sql = "select deptno, name from dept";
    sql(sql).ok();
  }

  @Test void testTableExpression() {
    final String sql = "select deptno + deptno from dept";
    sql(sql).ok();
  }

  @Test void testTableExtend() {
    final String sql = "select * from dept extend (x varchar(5) not null)";
    sql(sql).ok();
  }

  @Test void testTableExtendSubset() {
    final String sql = "select deptno, x from dept extend (x int)";
    sql(sql).ok();
  }

  @Test void testTableExtendExpression() {
    final String sql = "select deptno + x from dept extend (x int not null)";
    sql(sql).ok();
  }

  @Test void testModifiableViewExtend() {
    final String sql = "select *\n"
        + "from EMP_MODIFIABLEVIEW extend (x varchar(5) not null)";
    sql(sql).withExtendedTester().ok();
  }

  @Test void testModifiableViewExtendSubset() {
    final String sql = "select x, empno\n"
        + "from EMP_MODIFIABLEVIEW extend (x varchar(5) not null)";
    sql(sql).withExtendedTester().ok();
  }

  @Test void testModifiableViewExtendExpression() {
    final String sql = "select empno + x\n"
        + "from EMP_MODIFIABLEVIEW extend (x int not null)";
    sql(sql).withExtendedTester().ok();
  }

  @Test void testSelectViewExtendedColumnCollision() {
    sql("select ENAME, EMPNO, JOB, SLACKER, SAL, HIREDATE, MGR\n"
        + " from EMP_MODIFIABLEVIEW3\n"
        + " where SAL = 20").withExtendedTester().ok();
    sql("select ENAME, EMPNO, JOB, SLACKER, SAL, HIREDATE, MGR\n"
        + " from EMP_MODIFIABLEVIEW3 extend (SAL int)\n"
        + " where SAL = 20").withExtendedTester().ok();
  }

  @Test void testSelectViewExtendedColumnCaseSensitiveCollision() {
    sql("select ENAME, EMPNO, JOB, SLACKER, \"sal\", HIREDATE, MGR\n"
        + " from EMP_MODIFIABLEVIEW3 extend (\"sal\" boolean)\n"
        + " where \"sal\" = true").withExtendedTester().ok();
  }

  @Test void testSelectViewExtendedColumnExtendedCollision() {
    sql("select ENAME, EMPNO, JOB, SLACKER, SAL, HIREDATE, EXTRA\n"
        + " from EMP_MODIFIABLEVIEW2\n"
        + " where SAL = 20").withExtendedTester().ok();
    sql("select ENAME, EMPNO, JOB, SLACKER, SAL, HIREDATE, EXTRA\n"
        + " from EMP_MODIFIABLEVIEW2 extend (EXTRA boolean)\n"
        + " where SAL = 20").withExtendedTester().ok();
  }

  @Test void testSelectViewExtendedColumnCaseSensitiveExtendedCollision() {
    sql("select ENAME, EMPNO, JOB, SLACKER, SAL, HIREDATE, \"extra\"\n"
        + " from EMP_MODIFIABLEVIEW2 extend (\"extra\" boolean)\n"
        + " where \"extra\" = false").withExtendedTester().ok();
  }

  @Test void testSelectViewExtendedColumnUnderlyingCollision() {
    sql("select ENAME, EMPNO, JOB, SLACKER, SAL, HIREDATE, MGR, COMM\n"
        + " from EMP_MODIFIABLEVIEW3 extend (COMM int)\n"
        + " where SAL = 20").withExtendedTester().ok();
  }

  @Test void testSelectViewExtendedColumnCaseSensitiveUnderlyingCollision() {
    sql("select ENAME, EMPNO, JOB, SLACKER, SAL, HIREDATE, MGR, \"comm\"\n"
        + " from EMP_MODIFIABLEVIEW3 extend (\"comm\" int)\n"
        + " where \"comm\" = 20").withExtendedTester().ok();
  }

  @Test void testUpdateExtendedColumnCollision() {
    sql("update empdefaults(empno INTEGER NOT NULL, deptno INTEGER)"
        + " set deptno = 1, empno = 20, ename = 'Bob'"
        + " where deptno = 10").ok();
  }

  @Test void testUpdateExtendedColumnCaseSensitiveCollision() {
    sql("update empdefaults(\"slacker\" INTEGER, deptno INTEGER)"
        + " set deptno = 1, \"slacker\" = 100"
        + " where ename = 'Bob'").ok();
  }

  @Test void testUpdateExtendedColumnModifiableViewCollision() {
    sql("update EMP_MODIFIABLEVIEW3(empno INTEGER NOT NULL, deptno INTEGER)"
        + " set deptno = 20, empno = 20, ename = 'Bob'"
        + " where empno = 10").withExtendedTester().ok();
  }

  @Test void testUpdateExtendedColumnModifiableViewCaseSensitiveCollision() {
    sql("update EMP_MODIFIABLEVIEW2(\"slacker\" INTEGER, deptno INTEGER)"
        + " set deptno = 20, \"slacker\" = 100"
        + " where ename = 'Bob'").withExtendedTester().ok();
  }

  @Test void testUpdateExtendedColumnModifiableViewExtendedCollision() {
    sql("update EMP_MODIFIABLEVIEW2(\"slacker\" INTEGER, extra BOOLEAN)"
        + " set deptno = 20, \"slacker\" = 100, extra = true"
        + " where ename = 'Bob'").withExtendedTester().ok();
  }

  @Test void testUpdateExtendedColumnModifiableViewExtendedCaseSensitiveCollision() {
    sql("update EMP_MODIFIABLEVIEW2(\"extra\" INTEGER, extra BOOLEAN)"
        + " set deptno = 20, \"extra\" = 100, extra = true"
        + " where ename = 'Bob'").withExtendedTester().ok();
  }

  @Test void testUpdateExtendedColumnModifiableViewUnderlyingCollision() {
    sql("update EMP_MODIFIABLEVIEW3(extra BOOLEAN, comm INTEGER)"
        + " set empno = 20, comm = 123, extra = true"
        + " where ename = 'Bob'").withExtendedTester().ok();
  }

  @Test void testSelectModifiableViewConstraint() {
    final String sql = "select deptno from EMP_MODIFIABLEVIEW2\n"
        + "where deptno = ?";
    sql(sql).withExtendedTester().ok();
  }

  @Test void testModifiableViewDdlExtend() {
    final String sql = "select extra from EMP_MODIFIABLEVIEW2";
    sql(sql).withExtendedTester().ok();
  }

  @Test void testExplicitTable() {
    sql("table emp").ok();
  }

  @Test void testCollectionTable() {
    sql("select * from table(ramp(3))").ok();
  }

  @Test void testCollectionTableWithLateral() {
    sql("select * from dept, lateral table(ramp(dept.deptno))").ok();
  }

  @Test void testCollectionTableWithLateral2() {
    sql("select * from dept, lateral table(ramp(deptno))").ok();
  }

  @Test void testSnapshotOnTemporalTable1() {
    final String sql = "select * from products_temporal "
        + "for system_time as of TIMESTAMP '2011-01-02 00:00:00'";
    sql(sql).ok();
  }

  @Test void testSnapshotOnTemporalTable2() {
    // Test temporal table with virtual columns.
    final String sql = "select * from VIRTUALCOLUMNS.VC_T1 "
        + "for system_time as of TIMESTAMP '2011-01-02 00:00:00'";
    sql(sql).withExtendedTester().ok();
  }

  @Test void testJoinTemporalTableOnSpecificTime1() {
    final String sql = "select stream *\n"
        + "from orders,\n"
        + "  products_temporal for system_time as of\n"
        + "    TIMESTAMP '2011-01-02 00:00:00'";
    sql(sql).ok();
  }

  @Test void testJoinTemporalTableOnSpecificTimestampWithLocalTimeZone() {
    final String sql = "select stream *\n"
        + "from orders,\n"
        + "  products_temporal for system_time as of\n"
        + "    TIMESTAMP WITH LOCAL TIME ZONE '2011-01-02 00:00:00'";
    sql(sql).ok();
  }

  @Test void testJoinTemporalTableOnSpecificTime2() {
    // Test temporal table with virtual columns.
    final String sql = "select stream *\n"
        + "from orders,\n"
        + "  VIRTUALCOLUMNS.VC_T1 for system_time as of\n"
        + "    TIMESTAMP '2011-01-02 00:00:00'";
    sql(sql).withExtendedTester().ok();
  }

  @Test void testJoinTemporalTableOnColumnReference1() {
    final String sql = "select stream *\n"
        + "from orders\n"
        + "join products_temporal for system_time as of orders.rowtime\n"
        + "on orders.productid = products_temporal.productid";
    sql(sql).ok();
  }

  @Test void testJoinTemporalTableOnColumnReference2() {
    // Test temporal table with virtual columns.
    final String sql = "select stream *\n"
        + "from orders\n"
        + "join VIRTUALCOLUMNS.VC_T1 for system_time as of orders.rowtime\n"
        + "on orders.productid = VIRTUALCOLUMNS.VC_T1.a";
    sql(sql).withExtendedTester().ok();
  }

  /**
   * Lateral join with temporal table, both snapshot's input scan
   * and snapshot's period reference outer columns. Should not
   * decorrelate join.
   */
  @Test void testCrossJoinTemporalTable1() {
    final String sql = "select stream *\n"
        + "from orders\n"
        + "cross join lateral (\n"
        + "  select * from products_temporal for system_time\n"
        + "  as of orders.rowtime\n"
        + "  where orders.productid = products_temporal.productid)\n";
    sql(sql).ok();
  }

  /**
   * Lateral join with temporal table, snapshot's input scan
   * reference outer columns, but snapshot's period is static.
   * Should be able to decorrelate join.
   */
  @Test void testCrossJoinTemporalTable2() {
    final String sql = "select stream *\n"
        + "from orders\n"
        + "cross join lateral (\n"
        + "  select * from products_temporal for system_time\n"
        + "  as of TIMESTAMP '2011-01-02 00:00:00'\n"
        + "  where orders.productid = products_temporal.productid)\n";
    sql(sql).ok();
  }

  /**
   * Lateral join with temporal table, snapshot's period reference
   * outer columns. Should not decorrelate join.
   */
  @Test void testCrossJoinTemporalTable3() {
    final String sql = "select stream *\n"
        + "from orders\n"
        + "cross join lateral (\n"
        + "  select * from products_temporal for system_time\n"
        + "  as of orders.rowtime\n"
        + "  where products_temporal.productid > 1)\n";
    sql(sql).ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1732">[CALCITE-1732]
   * IndexOutOfBoundsException when using LATERAL TABLE with more than one
   * field</a>. */
  @Test void testCollectionTableWithLateral3() {
    sql("select * from dept, lateral table(DEDUP(dept.deptno, dept.name))").ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4673">[CALCITE-4673]
   * If arguments to a table function use correlation variables,
   * SqlToRelConverter should eliminate duplicate variables</a>.
   *
   * <p>The {@code LogicalTableFunctionScan} should have two identical
   * correlation variables like "{@code $cor0.DEPTNO}", but before this bug was
   * fixed, we have different ones: "{@code $cor0.DEPTNO}" and
   * "{@code $cor1.DEPTNO}". */
  @Test void testCorrelationCollectionTableInSubQuery() {
    Consumer<String> fn = sql -> {
      sql(sql).withExpand(true).withDecorrelate(true)
          .convertsTo("${planExpanded}");
      sql(sql).withExpand(false).withDecorrelate(false)
          .convertsTo("${planNotExpanded}");
    };
    fn.accept("select e.deptno,\n"
        + "  (select * from lateral table(DEDUP(e.deptno, e.deptno)))\n"
        + "from emp e");
    // same effect without LATERAL
    fn.accept("select e.deptno,\n"
        + "  (select * from table(DEDUP(e.deptno, e.deptno)))\n"
        + "from emp e");
  }

  @Test void testCorrelatedScalarSubQueryInSelectList() {
    Consumer<String> fn = sql -> {
      sql(sql).withExpand(true).withDecorrelate(false)
          .convertsTo("${planExpanded}");
      sql(sql).withExpand(false).withDecorrelate(false)
          .convertsTo("${planNotExpanded}");
    };
    fn.accept("select deptno,\n"
        + "  (select min(1) from emp where empno > d.deptno) as i0,\n"
        + "  (select min(0) from emp where deptno = d.deptno "
        + "                            and ename = 'SMITH'"
        + "                            and d.deptno > 0) as i1\n"
        + "from dept as d");
  }

  @Test void testCorrelationLateralSubQuery() {
    String sql = "SELECT deptno, ename\n"
        + "FROM\n"
        + "  (SELECT DISTINCT deptno FROM emp) t1,\n"
        + "  LATERAL (\n"
        + "    SELECT ename, sal\n"
        + "    FROM emp\n"
        + "    WHERE deptno IN (t1.deptno, t1.deptno)\n"
        + "    AND   deptno = t1.deptno\n"
        + "    ORDER BY sal\n"
        + "    DESC LIMIT 3)";
    sql(sql).withExpand(false).withDecorrelate(false).ok();
  }

  @Test void testCorrelationExistsWithSubQuery() {
    String sql = "select emp.deptno, dept.deptno\n"
        + "from emp, dept\n"
        + "where exists (select * from emp\n"
        + "  where emp.deptno = dept.deptno\n"
        + "  and emp.deptno = dept.deptno\n"
        + "  and emp.deptno in (dept.deptno, dept.deptno))";
    sql(sql).withExpand(false).withDecorrelate(false).ok();
  }

  @Test void testCorrelationInWithSubQuery() {
    String sql = "select deptno\n"
        + "from emp\n"
        + "where deptno in (select deptno\n"
        + "    from dept\n"
        + "    where emp.deptno = dept.deptno\n"
        + "    and emp.deptno = dept.deptno)";
    sql(sql).withExpand(false).withDecorrelate(false).ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3847">[CALCITE-3847]
   * Decorrelation for join with lateral table outputs wrong plan if the join
   * condition contains correlation variables</a>. */
  @Test void testJoinLateralTableWithConditionCorrelated() {
    final String sql = "select deptno, r.num from dept join\n"
        + " lateral table(ramp(dept.deptno)) as r(num)\n"
        + " on deptno=num";
    sql(sql).ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4206">[CALCITE-4206]
   * RelDecorrelator outputs wrong plan for correlate sort with fetch
   * limit</a>. */
  @Test void testCorrelateSortWithLimit() {
    final String sql = "SELECT deptno, ename\n"
        + "FROM\n"
        + "  (SELECT DISTINCT deptno FROM emp) t1,\n"
        + "  LATERAL (\n"
        + "    SELECT ename, sal\n"
        + "    FROM emp\n"
        + "    WHERE deptno = t1.deptno\n"
        + "    ORDER BY sal\n"
        + "    DESC LIMIT 3\n"
        + "  )";
    sql(sql).ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4333">[CALCITE-4333]
   * The Sort rel should be decorrelated even though it has fetch or limit
   * when its parent is not a Correlate</a>. */
  @Test void testSortLimitWithCorrelateInput() {
    final String sql = ""
        + "SELECT deptno, ename\n"
        + "    FROM\n"
        + "        (SELECT DISTINCT deptno FROM emp) t1,\n"
        + "          LATERAL (\n"
        + "            SELECT ename, sal\n"
        + "            FROM emp\n"
        + "            WHERE deptno = t1.deptno)\n"
        + "    ORDER BY ename DESC\n"
        + "    LIMIT 3";
    sql(sql).ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4437">[CALCITE-4437]
   * The Sort rel should be decorrelated even though it has fetch or limit
   * when it is not inside a Correlate</a>.
   */
  @Test void testProjectSortLimitWithCorrelateInput() {
    final String sql = ""
        + "SELECT ename||deptno FROM\n"
        + "    (SELECT deptno, ename\n"
        + "    FROM\n"
        + "        (SELECT DISTINCT deptno FROM emp) t1,\n"
        + "          LATERAL (\n"
        + "            SELECT ename, sal\n"
        + "            FROM emp\n"
        + "            WHERE deptno = t1.deptno)\n"
        + "    ORDER BY ename DESC\n"
        + "    LIMIT 3)";
    sql(sql).ok();
  }

  @Test void testSample() {
    final String sql =
        "select * from emp tablesample substitute('DATASET1') where empno > 5";
    sql(sql).ok();
  }

  @Test void testSampleQuery() {
    final String sql = "select * from (\n"
        + " select * from emp as e tablesample substitute('DATASET1')\n"
        + " join dept on e.deptno = dept.deptno\n"
        + ") tablesample substitute('DATASET2')\n"
        + "where empno > 5";
    sql(sql).ok();
  }

  @Test void testSampleBernoulli() {
    final String sql =
        "select * from emp tablesample bernoulli(50) where empno > 5";
    sql(sql).ok();
  }

  @Test void testSampleBernoulliQuery() {
    final String sql = "select * from (\n"
        + " select * from emp as e tablesample bernoulli(10) repeatable(1)\n"
        + " join dept on e.deptno = dept.deptno\n"
        + ") tablesample bernoulli(50) repeatable(99)\n"
        + "where empno > 5";
    sql(sql).ok();
  }

  @Test void testSampleBernoulliWithRateZero() {
    final String sql = "select *\n"
        + "from (\n"
        + "  select * from emp limit 10\n"
        + ") as e tablesample bernoulli(0)";
    sql(sql).ok();
  }

  @Test void testSampleSystem() {
    final String sql =
        "select * from emp tablesample system(50) where empno > 5";
    sql(sql).ok();
  }

  @Test void testSampleSystemQuery() {
    final String sql = "select * from (\n"
        + " select * from emp as e tablesample system(10) repeatable(1)\n"
        + " join dept on e.deptno = dept.deptno\n"
        + ") tablesample system(50) repeatable(99)\n"
        + "where empno > 5";
    sql(sql).ok();
  }

  @Test void testSampleSystemWithRateZero() {
    final String sql = "select * from (\n"
        + " select * from emp as e\n"
        + " join dept on e.deptno = dept.deptno\n"
        + ") tablesample system(0)\n"
        + "where empno > 5";
    sql(sql).ok();
  }

  @Test void testCollectionTableWithCursorParam() {
    final String sql = "select * from table(dedup("
        + "cursor(select ename from emp),"
        + " cursor(select name from dept), 'NAME'))";
    sql(sql).withDecorrelate(false).ok();
  }

  @Test void testUnnest() {
    final String sql = "select*from unnest(multiset[1,2])";
    sql(sql).ok();
  }

  @Test void testUnnestSubQuery() {
    final String sql = "select*from unnest(multiset(select*from dept))";
    sql(sql).ok();
  }

  @Test void testUnnestArrayAggPlan() {
    final String sql = "select d.deptno, e2.empno_avg\n"
        + "from dept_nested as d outer apply\n"
        + " (select avg(e.empno) as empno_avg from UNNEST(d.employees) as e) e2";
    sql(sql).withConformance(SqlConformanceEnum.LENIENT).ok();
  }

  @Test void testUnnestArrayPlan() {
    final String sql = "select d.deptno, e2.empno\n"
        + "from dept_nested as d,\n"
        + " UNNEST(d.employees) e2";
    sql(sql).withExtendedTester().ok();
  }

  @Test void testUnnestArrayPlanAs() {
    final String sql = "select d.deptno, e2.empno\n"
        + "from dept_nested as d,\n"
        + " UNNEST(d.employees) as e2(empno, y, z)";
    sql(sql).ok();
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3789">[CALCITE-3789]
   * Support validation of UNNEST multiple array columns like Presto</a>.
   */
  @Test void testAliasUnnestArrayPlanWithSingleColumn() {
    final String sql = "select d.deptno, employee.empno\n"
        + "from dept_nested_expanded as d,\n"
        + " UNNEST(d.employees) as t(employee)";
    sql(sql).withConformance(SqlConformanceEnum.PRESTO).ok();
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3789">[CALCITE-3789]
   * Support validation of UNNEST multiple array columns like Presto</a>.
   */
  @Test void testAliasUnnestArrayPlanWithDoubleColumn() {
    final String sql = "select d.deptno, e, k.empno\n"
        + "from dept_nested_expanded as d CROSS JOIN\n"
        + " UNNEST(d.admins, d.employees) as t(e, k)";
    sql(sql).withConformance(SqlConformanceEnum.PRESTO).ok();
  }

  @Test void testArrayOfRecord() {
    sql("select employees[1].detail.skills[2+3].desc from dept_nested").ok();
  }

  @Test void testFlattenRecords() {
    sql("select employees[1] from dept_nested").ok();
  }

  @Test void testUnnestArray() {
    sql("select*from unnest(array(select*from dept))").ok();
  }

  @Test void testUnnestArrayNoExpand() {
    final String sql = "select name,\n"
        + "    array (select *\n"
        + "        from emp\n"
        + "        where deptno = dept.deptno) as emp_array,\n"
        + "    multiset (select *\n"
        + "        from emp\n"
        + "        where deptno = dept.deptno) as emp_multiset,\n"
        + "    map (select empno, job\n"
        + "        from emp\n"
        + "        where deptno = dept.deptno) as job_map\n"
        + "from dept";
    sql(sql).withExpand(false).ok();
  }

  @Test void testUnnestWithOrdinality() {
    final String sql =
        "select*from unnest(array(select*from dept)) with ordinality";
    sql(sql).ok();
  }

  @Test void testMultisetSubQuery() {
    final String sql =
        "select multiset(select deptno from dept) from (values(true))";
    sql(sql).ok();
  }

  @Test void testMultiset() {
    final String sql = "select 'a',multiset[10] from dept";
    sql(sql).ok();
  }

  @Test void testMultisetOfColumns() {
    final String sql = "select 'abc',multiset[deptno,sal] from emp";
    sql(sql).withExpand(true).ok();
  }

  @Test void testMultisetOfColumnsRex() {
    sql("select 'abc',multiset[deptno,sal] from emp").ok();
  }

  @Test void testCorrelationJoin() {
    checkCorrelationJoin(true);
  }

  @Test void testCorrelationJoinRex() {
    checkCorrelationJoin(false);
  }

  void checkCorrelationJoin(boolean expand) {
    final String sql = "select *,\n"
        + "  multiset(select * from emp where deptno=dept.deptno) as empset\n"
        + "from dept";
    sql(sql).withExpand(expand).ok();
  }

  @Test void testCorrelatedArraySubQuery() {
    checkCorrelatedArraySubQuery(true);
  }

  @Test void testCorrelatedArraySubQueryRex() {
    checkCorrelatedArraySubQuery(false);
  }

  void checkCorrelatedArraySubQuery(boolean expand) {
    final String sql = "select *,\n"
        + "    array (select * from emp\n"
        + "        where deptno = dept.deptno) as empset\n"
        + "from dept";
    sql(sql).withExpand(expand).ok();
  }

  @Test void testCorrelatedMapSubQuery() {
    checkCorrelatedMapSubQuery(true);
  }

  @Test void testCorrelatedMapSubQueryRex() {
    checkCorrelatedMapSubQuery(false);
  }

  void checkCorrelatedMapSubQuery(boolean expand) {
    final String sql = "select *,\n"
        + "  map (select empno, job\n"
        + "       from emp where deptno = dept.deptno) as jobMap\n"
        + "from dept";
    sql(sql).withExpand(expand).ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-864">[CALCITE-864]
   * Correlation variable has incorrect row type if it is populated by right
   * side of a Join</a>. */
  @Test void testCorrelatedSubQueryInJoin() {
    final String sql = "select *\n"
        + "from emp as e\n"
        + "join dept as d using (deptno)\n"
        + "where d.name = (\n"
        + "  select max(name)\n"
        + "  from dept as d2\n"
        + "  where d2.deptno = d.deptno)";
    sql(sql).withExpand(false).ok();
  }

  @Test void testMultipleCorrelatedSubQueriesInSelectReferencingDifferentTablesInFrom() {
    final String sql = "select\n"
        + "(select ename from emp where empno = empnos.empno) as emp_name,\n"
        + "(select name from dept where deptno = deptnos.deptno) as dept_name\n"
        + " from (values (1), (2)) as empnos(empno), (values (1), (2)) as deptnos(deptno)";
    sql(sql).withExpand(false).ok();
  }

  @Test void testExists() {
    final String sql = "select*from emp\n"
        + "where exists (select 1 from dept where deptno=55)";
    sql(sql).ok();
  }

  @Test void testExistsCorrelated() {
    final String sql = "select*from emp where exists (\n"
        + "  select 1 from dept where emp.deptno=dept.deptno)";
    sql(sql).withDecorrelate(false).ok();
  }

  @Test void testNotExistsCorrelated() {
    final String sql = "select * from emp where not exists (\n"
        + "  select 1 from dept where emp.deptno=dept.deptno)";
    sql(sql).withDecorrelate(false).ok();
  }

  @Test void testExistsCorrelatedDecorrelate() {
    final String sql = "select*from emp where exists (\n"
        + "  select 1 from dept where emp.deptno=dept.deptno)";
    sql(sql).withDecorrelate(true).ok();
  }

  /**
   * Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-4560">[CALCITE-4560]
   * Wrong plan when decorrelating EXISTS subquery with COALESCE in the predicate</a>. */
  @Test void testExistsDecorrelateComplexCorrelationPredicate() {
    final String sql = "select e1.empno from empnullables e1 where exists (\n"
        + "  select 1 from empnullables e2 where COALESCE(e1.ename,'M')=COALESCE(e2.ename,'M'))";
    sql(sql).withDecorrelate(true).ok();
  }

  @Test void testExistsCorrelatedDecorrelateRex() {
    final String sql = "select*from emp where exists (\n"
        + "  select 1 from dept where emp.deptno=dept.deptno)";
    sql(sql).withDecorrelate(true).withExpand(false).ok();
  }

  @Test void testExistsCorrelatedLimit() {
    final String sql = "select*from emp where exists (\n"
        + "  select 1 from dept where emp.deptno=dept.deptno limit 1)";
    sql(sql).withDecorrelate(false).ok();
  }

  @Test void testExistsCorrelatedLimitDecorrelate() {
    final String sql = "select*from emp where exists (\n"
        + "  select 1 from dept where emp.deptno=dept.deptno limit 1)";
    sql(sql).withDecorrelate(true).withExpand(true).ok();
  }

  @Test void testExistsCorrelatedLimitDecorrelateRex() {
    final String sql = "select*from emp where exists (\n"
        + "  select 1 from dept where emp.deptno=dept.deptno limit 1)";
    sql(sql).withDecorrelate(true).withExpand(false).ok();
  }

  @Test void testUniqueWithExpand() {
    final String sql = "select * from emp\n"
        + "where unique (select 1 from dept where deptno=55)";
    sql(sql).withExpand(true)
        .throws_("UNIQUE is only supported if expand = false");
  }

  @Test void testUniqueWithProjectLateral() {
    final String sql = "select * from emp\n"
        + "where unique (select 1 from dept where deptno=55)";
    sql(sql).withExpand(false).ok();
  }

  @Test void testUniqueWithOneProject() {
    final String sql = "select * from emp\n"
        + "where unique (select name from dept where deptno=55)";
    sql(sql).withExpand(false).ok();
  }

  @Test void testUniqueWithManyProject() {
    final String sql = "select * from emp\n"
        + "where unique (select * from dept)";
    sql(sql).withExpand(false).ok();
  }

  @Test void testNotUnique() {
    final String sql = "select * from emp\n"
        + "where not unique (select 1 from dept where deptno=55)";
    sql(sql).withExpand(false).ok();
  }

  @Test void testNotUniqueCorrelated() {
    final String sql = "select * from emp where not unique (\n"
        + "  select 1 from dept where emp.deptno=dept.deptno)";
    sql(sql).withExpand(false).ok();
  }

  @Test void testInValueListShort() {
    final String sql = "select empno from emp where deptno in (10, 20)";
    sql(sql).ok();
    sql(sql).withExpand(false).ok();
  }

  @Test void testInValueListLong() {
    // Go over the default threshold of 20 to force a sub-query.
    final String sql = "select empno from emp where deptno in"
        + " (10, 20, 30, 40, 50, 60, 70, 80, 90, 100"
        + ", 110, 120, 130, 140, 150, 160, 170, 180, 190"
        + ", 200, 210, 220, 230)";
    sql(sql).ok();
  }

  @Test void testInUncorrelatedSubQuery() {
    final String sql = "select empno from emp where deptno in"
        + " (select deptno from dept)";
    sql(sql).ok();
  }

  @Test void testInUncorrelatedSubQueryRex() {
    final String sql = "select empno from emp where deptno in"
        + " (select deptno from dept)";
    sql(sql).withExpand(false).ok();
  }

  @Test void testCompositeInUncorrelatedSubQueryRex() {
    final String sql = "select empno from emp where (empno, deptno) in"
        + " (select deptno - 10, deptno from dept)";
    sql(sql).withExpand(false).ok();
  }

  @Test void testNotInUncorrelatedSubQuery() {
    final String sql = "select empno from emp where deptno not in"
        + " (select deptno from dept)";
    sql(sql).ok();
  }

  @Test void testAllValueList() {
    final String sql = "select empno from emp where deptno > all (10, 20)";
    sql(sql).withExpand(false).ok();
  }

  @Test void testSomeValueList() {
    final String sql = "select empno from emp where deptno > some (10, 20)";
    sql(sql).withExpand(false).ok();
  }

  @Test void testSome() {
    final String sql = "select empno from emp where deptno > some (\n"
        + "  select deptno from dept)";
    sql(sql).withExpand(false).ok();
  }

  @Test void testSomeWithEquality() {
    final String sql = "select empno from emp where deptno = some (\n"
        + "  select deptno from dept)";
    sql(sql).withExpand(false).ok();
  }

  @Test void testSomeWithNotEquality() {
    final String sql = "select empno from emp where deptno <> some (\n"
        + "  select deptno from dept)";
    sql(sql).withExpand(false).ok();
  }

  @Test void testNotInUncorrelatedSubQueryRex() {
    final String sql = "select empno from emp where deptno not in"
        + " (select deptno from dept)";
    sql(sql).withExpand(false).ok();
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5532">[CALCITE-5532]
   * CompositeOperandTypeChecker should check operands without type coercion first</a>.
   */
  @Test void testNoTypeCoercionForExactMatchInCompositeTypeChecker() {
    String sql = "SELECT COMPARE_STRINGS_OR_NUMERIC_VALUES(1, 1)";
    sql(sql).ok();
  }

  @Test void testNotCaseInThreeClause() {
    final String sql = "select empno from emp where not case when "
        + "true then deptno in (10,20) else true end";
    sql(sql).withExpand(false).ok();
  }

  @Test void testNotCaseInMoreClause() {
    final String sql = "select empno from emp where not case when "
        + "true then deptno in (10,20) when false then false else deptno in (30,40) end";
    sql(sql).withExpand(false).ok();
  }

  @Test void testNotCaseInWithoutElse() {
    final String sql = "select empno from emp where not case when "
        + "true then deptno in (10,20)  end";
    sql(sql).withExpand(false).ok();
  }

  @Test void testWhereInCorrelated() {
    final String sql = "select empno from emp as e\n"
        + "join dept as d using (deptno)\n"
        + "where e.sal in (\n"
        + "  select e2.sal from emp as e2 where e2.deptno > e.deptno)";
    sql(sql).withExpand(false).ok();
  }

  @Test void testInUncorrelatedSubQueryInSelect() {
    // In the SELECT clause, the value of IN remains in 3-valued logic
    // -- it's not forced into 2-valued by the "... IS TRUE" wrapper as in the
    // WHERE clause -- so the translation is more complicated.
    final String sql = "select name, deptno in (\n"
        + "  select case when true then deptno else null end from emp)\n"
        + "from dept";
    sql(sql).ok();
  }

  @Test void testInUncorrelatedSubQueryInSelectRex() {
    // In the SELECT clause, the value of IN remains in 3-valued logic
    // -- it's not forced into 2-valued by the "... IS TRUE" wrapper as in the
    // WHERE clause -- so the translation is more complicated.
    final String sql = "select name, deptno in (\n"
        + "  select case when true then deptno else null end from emp)\n"
        + "from dept";
    sql(sql).withExpand(false).ok();
  }

  @Test void testInUncorrelatedSubQueryInHavingRex() {
    final String sql = "select sum(sal) as s\n"
        + "from emp\n"
        + "group by deptno\n"
        + "having count(*) > 2\n"
        + "and deptno in (\n"
        + "  select case when true then deptno else null end from emp)";
    sql(sql).withExpand(false).ok();
  }

  @Test void testUncorrelatedScalarSubQueryInOrderRex() {
    final String sql = "select ename\n"
        + "from emp\n"
        + "order by (select case when true then deptno else null end from emp) desc,\n"
        + "  ename";
    sql(sql).withExpand(false).ok();
  }

  @Test void testUncorrelatedScalarSubQueryInGroupOrderRex() {
    final String sql = "select sum(sal) as s\n"
        + "from emp\n"
        + "group by deptno\n"
        + "order by (select case when true then deptno else null end from emp) desc,\n"
        + "  count(*)";
    sql(sql).withExpand(false).ok();
  }

  @Test void testUncorrelatedScalarSubQueryInAggregateRex() {
    final String sql = "select sum((select min(deptno) from emp)) as s\n"
        + "from emp\n"
        + "group by deptno\n";
    sql(sql).withExpand(false).ok();
  }

  /** Plan should be as {@link #testInUncorrelatedSubQueryInSelect}, but with
   * an extra NOT. Both queries require 3-valued logic. */
  @Test void testNotInUncorrelatedSubQueryInSelect() {
    final String sql = "select empno, deptno not in (\n"
        + "  select case when true then deptno else null end from dept)\n"
        + "from emp";
    sql(sql).ok();
  }

  @Test void testNotInUncorrelatedSubQueryInSelectRex() {
    final String sql = "select empno, deptno not in (\n"
        + "  select case when true then deptno else null end from dept)\n"
        + "from emp";
    sql(sql).withExpand(false).ok();
  }

  /** Since 'deptno NOT IN (SELECT deptno FROM dept)' can not be null, we
   * generate a simpler plan. */
  @Test void testNotInUncorrelatedSubQueryInSelectNotNull() {
    final String sql = "select empno, deptno not in (\n"
        + "  select deptno from dept)\n"
        + "from emp";
    sql(sql).ok();
  }

  /** Since 'deptno NOT IN (SELECT mgr FROM emp)' can be null, we need a more
   * complex plan, including counts of null and not-null keys. */
  @Test void testNotInUncorrelatedSubQueryInSelectMayBeNull() {
    final String sql = "select empno, deptno not in (\n"
        + "  select mgr from emp)\n"
        + "from emp";
    sql(sql).ok();
  }

  /** Even though "mgr" allows nulls, we can deduce from the WHERE clause that
   * it will never be null. Therefore we can generate a simpler plan. */
  @Test void testNotInUncorrelatedSubQueryInSelectDeduceNotNull() {
    final String sql = "select empno, deptno not in (\n"
        + "  select mgr from emp where mgr > 5)\n"
        + "from emp";
    sql(sql).ok();
  }

  /** Similar to {@link #testNotInUncorrelatedSubQueryInSelectDeduceNotNull()},
   * using {@code IS NOT NULL}. */
  @Test void testNotInUncorrelatedSubQueryInSelectDeduceNotNull2() {
    final String sql = "select empno, deptno not in (\n"
        + "  select mgr from emp where mgr is not null)\n"
        + "from emp";
    sql(sql).ok();
  }

  /** Similar to {@link #testNotInUncorrelatedSubQueryInSelectDeduceNotNull()},
   * using {@code IN}. */
  @Test void testNotInUncorrelatedSubQueryInSelectDeduceNotNull3() {
    final String sql = "select empno, deptno not in (\n"
        + "  select mgr from emp where mgr in (\n"
        + "    select mgr from emp where deptno = 10))\n"
        + "from emp";
    sql(sql).ok();
  }

  @Test void testNotInUncorrelatedSubQueryInSelectNotNullRex() {
    final String sql = "select empno, deptno not in (\n"
        + "  select deptno from dept)\n"
        + "from emp";
    sql(sql).withExpand(false).ok();
  }

  @Test void testUnnestSelect() {
    final String sql = "select*from unnest(select multiset[deptno] from dept)";
    sql(sql).withExpand(true).ok();
  }

  @Test void testUnnestSelectRex() {
    final String sql = "select*from unnest(select multiset[deptno] from dept)";
    sql(sql).withExpand(false).ok();
  }

  @Test void testJoinUnnest() {
    final String sql = "select*from dept as d, unnest(multiset[d.deptno * 2])";
    sql(sql).ok();
  }

  @Test void testJoinUnnestRex() {
    final String sql = "select*from dept as d, unnest(multiset[d.deptno * 2])";
    sql(sql).withExpand(false).ok();
  }

  @Test void testLateral() {
    final String sql = "select * from emp,\n"
        + "  LATERAL (select * from dept where emp.deptno=dept.deptno)";
    sql(sql).withDecorrelate(false).ok();
  }

  @Test void testLateralDecorrelate() {
    final String sql = "select * from emp,\n"
        + " LATERAL (select * from dept where emp.deptno=dept.deptno)";
    sql(sql).withDecorrelate(true).withExpand(true).ok();
  }

  @Test void testLateralDecorrelateRex() {
    final String sql = "select * from emp,\n"
        + " LATERAL (select * from dept where emp.deptno=dept.deptno)";
    sql(sql).withDecorrelate(true).ok();
  }

  @Test void testLateralDecorrelateThetaRex() {
    final String sql = "select * from emp,\n"
        + " LATERAL (select * from dept where emp.deptno < dept.deptno)";
    sql(sql).withDecorrelate(true).ok();
  }

  @Test void testNestedCorrelations() {
    final String sql = "select *\n"
        + "from (select 2+deptno d2, 3+deptno d3 from emp) e\n"
        + " where exists (select 1 from (select deptno+1 d1 from dept) d\n"
        + " where d1=e.d2 and exists (select 2 from (select deptno+4 d4, deptno+5 d5, deptno+6 d6 from dept)\n"
        + " where d4=d.d1 and d5=d.d1 and d6=e.d3))";
    sql(sql).withDecorrelate(false).ok();
  }

  @Test void testNestedCorrelationsDecorrelated() {
    final String sql = "select *\n"
        + "from (select 2+deptno d2, 3+deptno d3 from emp) e\n"
        + " where exists (select 1 from (select deptno+1 d1 from dept) d\n"
        + " where d1=e.d2 and exists (select 2 from (select deptno+4 d4, deptno+5 d5, deptno+6 d6 from dept)\n"
        + " where d4=d.d1 and d5=d.d1 and d6=e.d3))";
    sql(sql).withDecorrelate(true).withExpand(true).ok();
  }

  @Test void testNestedCorrelationsDecorrelatedRex() {
    final String sql = "select *\n"
        + "from (select 2+deptno d2, 3+deptno d3 from emp) e\n"
        + " where exists (select 1 from (select deptno+1 d1 from dept) d\n"
        + " where d1=e.d2 and exists (select 2 from (select deptno+4 d4, deptno+5 d5, deptno+6 d6 from dept)\n"
        + " where d4=d.d1 and d5=d.d1 and d6=e.d3))";
    sql(sql).withDecorrelate(true).ok();
  }

  @Test void testElement() {
    sql("select element(multiset[5]) from emp").ok();
  }

  @Test void testElementInValues() {
    sql("values element(multiset[5])").ok();
  }

  @Test void testUnionAll() {
    final String sql =
        "select empno from emp union all select deptno from dept";
    sql(sql).ok();
  }

  @Test void testUnion() {
    final String sql =
        "select empno from emp union select deptno from dept";
    sql(sql).ok();
  }

  @Test void testUnionValues() {
    // union with values
    final String sql = "values (10), (20)\n"
        + "union all\n"
        + "select 34 from emp\n"
        + "union all values (30), (45 + 10)";
    sql(sql).ok();
  }

  @Test void testUnionSubQuery() {
    // union of sub-query, inside from list, also values
    final String sql = "select deptno from emp as emp0 cross join\n"
        + " (select empno from emp union all\n"
        + "  select deptno from dept where deptno > 20 union all\n"
        + "  values (45), (67))";
    sql(sql).ok();
  }

  @Test void testIsDistinctFrom() {
    final String sql = "select empno is distinct from deptno\n"
        + "from (values (cast(null as int), 1),\n"
        + "             (2, cast(null as int))) as emp(empno, deptno)";
    sql(sql).ok();
  }

  @Test void testIsNotDistinctFrom() {
    final String sql = "select empno is not distinct from deptno\n"
        + "from (values (cast(null as int), 1),\n"
        + "             (2, cast(null as int))) as emp(empno, deptno)";
    sql(sql).ok();
  }

  @Test void testNotLike() {
    // note that 'x not like y' becomes 'not(x like y)'
    final String sql = "values ('a' not like 'b' escape 'c')";
    sql(sql).ok();
  }

  @Test void testTumble() {
    final String sql = "select STREAM\n"
        + "  TUMBLE_START(rowtime, INTERVAL '1' MINUTE) AS s,\n"
        + "  TUMBLE_END(rowtime, INTERVAL '1' MINUTE) AS e\n"
        + "from Shipments\n"
        + "GROUP BY TUMBLE(rowtime, INTERVAL '1' MINUTE)";
    sql(sql).ok();
  }

  @Test void testTableFunctionTumble() {
    final String sql = "select *\n"
        + "from table(tumble(table Shipments, descriptor(rowtime), INTERVAL '1' MINUTE))";
    sql(sql).ok();
  }

  @Test void testTableFunctionTumbleWithParamNames() {
    final String sql = "select *\n"
        + "from table(\n"
        + "tumble(\n"
        + "  DATA => table Shipments,\n"
        + "  TIMECOL => descriptor(rowtime),\n"
        + "  SIZE => INTERVAL '1' MINUTE))";
    sql(sql).ok();
  }

  @Test void testTableFunctionTumbleWithParamReordered() {
    final String sql = "select *\n"
        + "from table(\n"
        + "tumble(\n"
        + "  DATA => table Shipments,\n"
        + "  SIZE => INTERVAL '1' MINUTE,\n"
        + "  TIMECOL => descriptor(rowtime)))";
    sql(sql).ok();
  }

  @Test void testTableFunctionTumbleWithInnerJoin() {
    final String sql = "select *\n"
        + "from table(tumble(table Shipments, descriptor(rowtime), INTERVAL '1' MINUTE)) a\n"
        + "join table(tumble(table Shipments, descriptor(rowtime), INTERVAL '1' MINUTE)) b\n"
        + "on a.orderid = b.orderid";
    sql(sql).ok();
  }

  @Test void testTableFunctionTumbleWithOffset() {
    final String sql = "select *\n"
        + "from table(tumble(table Shipments, descriptor(rowtime),\n"
        + "  INTERVAL '10' MINUTE, INTERVAL '1' MINUTE))";
    sql(sql).ok();
  }

  @Test void testTableFunctionHop() {
    final String sql = "select *\n"
        + "from table(hop(table Shipments, descriptor(rowtime), "
        + "INTERVAL '1' MINUTE, INTERVAL '2' MINUTE))";
    sql(sql).ok();
  }

  @Test void testTableFunctionHopWithOffset() {
    final String sql = "select *\n"
        + "from table(hop(table Shipments, descriptor(rowtime), "
        + "INTERVAL '1' MINUTE, INTERVAL '5' MINUTE, INTERVAL '3' MINUTE))";
    sql(sql).ok();
  }

  @Test void testTableFunctionHopWithParamNames() {
    final String sql = "select *\n"
        + "from table(\n"
        + "hop(\n"
        + "  DATA => table Shipments,\n"
        + "  TIMECOL => descriptor(rowtime),\n"
        + "  SLIDE => INTERVAL '1' MINUTE,\n"
        + "  SIZE => INTERVAL '2' MINUTE))";
    sql(sql).ok();
  }

  @Test void testTableFunctionHopWithParamReordered() {
    final String sql = "select *\n"
        + "from table(\n"
        + "hop(\n"
        + "  DATA => table Shipments,\n"
        + "  SLIDE => INTERVAL '1' MINUTE,\n"
        + "  TIMECOL => descriptor(rowtime),\n"
        + "  SIZE => INTERVAL '2' MINUTE))";
    sql(sql).ok();
  }

  @Test void testTableFunctionSession() {
    final String sql = "select *\n"
        + "from table(session(table Shipments, descriptor(rowtime), "
        + "descriptor(orderId), INTERVAL '10' MINUTE))";
    sql(sql).ok();
  }

  @Test void testTableFunctionSessionWithParamNames() {
    final String sql = "select *\n"
        + "from table(\n"
        + "session(\n"
        + "  DATA => table Shipments,\n"
        + "  TIMECOL => descriptor(rowtime),\n"
        + "  KEY => descriptor(orderId),\n"
        + "  SIZE => INTERVAL '10' MINUTE))";
    sql(sql).ok();
  }

  @Test void testTableFunctionSessionWithParamReordered() {
    final String sql = "select *\n"
        + "from table(\n"
        + "session(\n"
        + "  DATA => table Shipments,\n"
        + "  KEY => descriptor(orderId),\n"
        + "  TIMECOL => descriptor(rowtime),\n"
        + "  SIZE => INTERVAL '10' MINUTE))";
    sql(sql).ok();
  }

  @Test void testTableFunctionTumbleWithSubQueryParam() {
    final String sql = "select *\n"
        + "from table(tumble((select * from Shipments), descriptor(rowtime), INTERVAL '1' MINUTE))";
    sql(sql).ok();
  }

  @Test void testTableFunctionHopWithSubQueryParam() {
    final String sql = "select *\n"
        + "from table(hop((select * from Shipments), descriptor(rowtime), "
        + "INTERVAL '1' MINUTE, INTERVAL '2' MINUTE))";
    sql(sql).ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5779">[CALCITE-5779]
   * Implicit column alias for single-column table function should work</a>. */
  @Test void testTableFunctionSingleColumnAlias() {
    String sql = "select rmp1.i, rmp1, rmp2, rmp3.i, j\n"
        + "from table(ramp(1)) as rmp1,\n"
        + "table(ramp(1)) as rmp2,\n"
        + "table(ramp(1)) as rmp3,\n"
        + "table(ramp(1)) as rmp4(j)";
    fixture()
        .withFactory(c ->
            c.withOperatorTable(t -> MockSqlOperatorTable.standard().extend()))
        .withCatalogReader(MockCatalogReaderExtended::create)
        .withSql(sql)
        .ok();
  }

  @Test void testTableFunctionSessionWithSubQueryParam() {
    final String sql = "select *\n"
        + "from table(session((select * from Shipments), descriptor(rowtime), "
        + "descriptor(orderId), INTERVAL '10' MINUTE))";
    sql(sql).ok();
  }

  @Test void testTableFunctionSessionCompoundSessionKey() {
    final String sql = "select *\n"
        + "from table(session(table Orders, descriptor(rowtime), "
        + "descriptor(orderId, productId), INTERVAL '10' MINUTE))";
    sql(sql).ok();
  }

  @Test void testTableFunctionWithPartitionKey() {
    final String sql = "select *\n"
        + "from table(topn(table orders partition by productid, 3))";
    sql(sql).ok();
  }

  @Test void testTableFunctionWithMultiplePartitionKeys() {
    final String sql = "select *\n"
        + "from table(topn(table orders partition by (orderId, productid), 3))";
    sql(sql).ok();
  }

  @Test void testTableFunctionWithOrderKey() {
    final String sql = "select *\n"
        + "from table(topn(table orders order by orderId, 3))";
    sql(sql).ok();
  }

  @Test void testTableFunctionWithMultipleOrderKeys() {
    final String sql = "select *\n"
        + "from table(topn(table orders order by (orderId, productid), 3))";
    sql(sql).ok();
  }

  @Test void testTableFunctionWithComplexOrderBy() {
    final String sql = "select *\n"
        + "from table(topn(table orders order by (orderId desc, productid desc nulls last), 3))";
    sql(sql).ok();
  }

  @Test void testTableFunctionWithOrderByWithNullLast() {
    final String sql = "select *\n"
        + "from table(topn(table orders order by orderId desc nulls last, 3))";
    sql(sql).ok();
  }

  @Test void testTableFunctionWithPartitionKeyAndOrderKey() {
    final String sql = "select *\n"
        + "from table(topn(table orders partition by productid order by orderId, 3))";
    sql(sql).ok();
  }

  @Test void testTableFunctionWithParamNames() {
    final String sql = "select *\n"
        + "from table(\n"
        + "topn(\n"
        + "  DATA => table orders partition by productid order by orderId,\n"
        + "  COL => 3))";
    sql(sql).ok();
  }

  @Test void testTableFunctionWithSubQuery() {
    final String sql = "select *\n"
        + "from table(topn("
        + "select * from orders partition by productid order by orderId desc nulls last, 3))";
    sql(sql).ok();
  }

  @Test void testTableFunctionWithSubQueryWithParamNames() {
    final String sql = "select *\n"
        + "from table(\n"
        + "topn(\n"
        + "  DATA => select * from orders partition by productid order by orderId nulls first,\n"
        + "  COL => 3))";
    sql(sql).ok();
  }

  @Test void testTableFunctionWithMultipleInputTables() {
    final String sql = "select *\n"
        + "from table(\n"
        + "similarlity(\n"
        + "  table emp partition by deptno order by empno nulls first,\n"
        + "  table emp_b partition by deptno order by empno nulls first))";
    sql(sql).ok();
  }

  @Test void testTableFunctionWithMultipleInputTablesWithParamNames() {
    final String sql = "select *\n"
        + "from table(\n"
        + "similarlity(\n"
        + "  LTABLE => table emp partition by deptno order by empno nulls first,\n"
        + "  RTABLE => table emp_b partition by deptno order by empno nulls first))";
    sql(sql).ok();
  }

  @Test void testNotNotIn() {
    final String sql = "select * from EMP where not (ename not in ('Fred') )";
    sql(sql).ok();
  }

  @Test void testOverMultiple() {
    final String sql = "select sum(sal) over w1,\n"
        + "  sum(deptno) over w1,\n"
        + "  sum(deptno) over w2\n"
        + "from emp\n"
        + "where deptno - sal > 999\n"
        + "window w1 as (partition by job order by hiredate rows 2 preceding),\n"
        + "  w2 as (partition by job order by hiredate rows 3 preceding disallow partial),\n"
        + "  w3 as (partition by job order by hiredate range interval '1' second preceding)";
    sql(sql).ok();
  }

  @Test void testOverDefaultBracket() {
    // c2 and c3 are equivalent to c1;
    // c5 is equivalent to c4;
    // c7 is equivalent to c6.
    final String sql = "select\n"
        + "  count(*) over (order by deptno) c1,\n"
        + "  count(*) over (order by deptno\n"
        + "    range unbounded preceding) c2,\n"
        + "  count(*) over (order by deptno\n"
        + "    range between unbounded preceding and current row) c3,\n"
        + "  count(*) over (order by deptno\n"
        + "    rows unbounded preceding) c4,\n"
        + "  count(*) over (order by deptno\n"
        + "    rows between unbounded preceding and current row) c5,\n"
        + "  count(*) over (order by deptno\n"
        + "    range between unbounded preceding and unbounded following) c6,\n"
        + " count(*) over (order by deptno\n"
        + "    rows between unbounded preceding and unbounded following) c7\n"
        + "from emp";
    sql(sql).ok();
  }

  /** As {@link #testOverDefaultBracket()}, but no {@code ORDER BY},
   * which makes more things equivalent. */
  @Test void testOverDefaultBracketNoOrderBy() {
    // c2 is invalid (therefore commented out);
    // c3, c6, c7 are equivalent to c1;
    // c5 is equivalent to c4.
    final String sql = "select\n"
        + "  count(*) over () c1,\n"
        + "--count(*) over (\n"
        + "--  range unbounded preceding) c2,\n"
        + "  count(*) over (\n"
        + "    range between unbounded preceding and current row) c3,\n"
        + "  count(*) over (\n"
        + "    rows unbounded preceding) c4,\n"
        + "  count(*) over (\n"
        + "    rows between unbounded preceding and current row) c5,\n"
        + "  count(*) over (\n"
        + "    range between unbounded preceding and unbounded following) c6,\n"
        + " count(*) over (\n"
        + "    rows between unbounded preceding and unbounded following) c7\n"
        + "from emp";
    sql(sql).ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-750">[CALCITE-750]
   * Allow windowed aggregate on top of regular aggregate</a>. */
  @Test void testNestedAggregates() {
    final String sql = "SELECT\n"
        + "  avg(sum(sal) + 2 * min(empno) + 3 * avg(empno))\n"
        + "  over (partition by deptno)\n"
        + "from emp\n"
        + "group by deptno";
    sql(sql).ok();
  }

  /**
   * Test one of the custom conversions which is recognized by the class of the
   * operator (in this case,
   * {@link org.apache.calcite.sql.fun.SqlCaseOperator}).
   */
  @Test void testCase() {
    sql("values (case 'a' when 'a' then 1 end)").ok();
  }

  /**
   * Tests one of the custom conversions which is recognized by the identity
   * of the operator (in this case,
   * {@link org.apache.calcite.sql.fun.SqlStdOperatorTable#CHARACTER_LENGTH}).
   */
  @Test void testCharLength() {
    // Note that CHARACTER_LENGTH becomes CHAR_LENGTH.
    sql("values (character_length('foo'))").ok();
  }

  @Test void testOverAvg() {
    // AVG(x) gets translated to SUM(x)/COUNT(x).  Because COUNT controls
    // the return type there usually needs to be a final CAST to get the
    // result back to match the type of x.
    final String sql = "select sum(sal) over w1,\n"
        + "  avg(sal) over w1\n"
        + "from emp\n"
        + "window w1 as (partition by job order by hiredate rows 2 preceding)";
    sql(sql).ok();
  }

  @Test void testOverAvg2() {
    // Check to see if extra CAST is present.  Because CAST is nested
    // inside AVG it passed to both SUM and COUNT so the outer final CAST
    // isn't needed.
    final String sql = "select sum(sal) over w1,\n"
        + "  avg(CAST(sal as real)) over w1\n"
        + "from emp\n"
        + "window w1 as (partition by job order by hiredate rows 2 preceding)";
    sql(sql).ok();
  }

  @Test void testOverCountStar() {
    final String sql = "select count(sal) over w1,\n"
        + "  count(*) over w1\n"
        + "from emp\n"
        + "window w1 as (partition by job order by hiredate rows 2 preceding)";
    sql(sql).ok();
  }

  /**
   * Tests that a window containing only ORDER BY is implicitly CURRENT ROW.
   */
  @Test void testOverOrderWindow() {
    final String sql = "select last_value(deptno) over w\n"
        + "from emp\n"
        + "window w as (order by empno)";
    sql(sql).ok();

    // Same query using inline window
    final String sql2 = "select last_value(deptno) over (order by empno)\n"
        + "from emp\n";
    sql(sql2).ok();
  }

  /**
   * Tests that a window with specifying null treatment.
   */
  @Test void testOverNullTreatmentWindow() {
    final String sql = "select\n"
        + "lead(deptno, 1) over w,\n "
        + "lead(deptno, 2) ignore nulls over w,\n"
        + "lead(deptno, 3) respect nulls over w,\n"
        + "lead(deptno, 1) over w,\n"
        + "lag(deptno, 2) ignore nulls over w,\n"
        + "lag(deptno, 2) respect nulls over w,\n"
        + "first_value(deptno) over w,\n"
        + "first_value(deptno) ignore nulls over w,\n"
        + "first_value(deptno) respect nulls over w,\n"
        + "last_value(deptno) over w,\n"
        + "last_value(deptno) ignore nulls over w,\n"
        + "last_value(deptno) respect nulls over w\n"
        + " from emp\n"
        + "window w as (order by empno)";
    sql(sql).ok();
  }

  /**
   * Tests that a window with a FOLLOWING bound becomes BETWEEN CURRENT ROW
   * AND FOLLOWING.
   */
  @Test void testOverOrderFollowingWindow() {
    // Window contains only ORDER BY (implicitly CURRENT ROW).
    final String sql = "select last_value(deptno) over w\n"
        + "from emp\n"
        + "window w as (order by empno rows 2 following)";
    sql(sql).ok();

    // Same query using inline window
    final String sql2 = "select\n"
        + "  last_value(deptno) over (order by empno rows 2 following)\n"
        + "from emp\n";
    sql(sql2).ok();
  }

  @Test void testTumbleTable() {
    final String sql = "select stream"
        + " tumble_end(rowtime, interval '2' hour) as rowtime, productId\n"
        + "from orders\n"
        + "group by tumble(rowtime, interval '2' hour), productId";
    sql(sql).ok();
  }

  /** As {@link #testTumbleTable()} but on a table where "rowtime" is at
   * position 1 not 0. */
  @Test void testTumbleTableRowtimeNotFirstColumn() {
    final String sql = "select stream\n"
        + "   tumble_end(rowtime, interval '2' hour) as rowtime, orderId\n"
        + "from shipments\n"
        + "group by tumble(rowtime, interval '2' hour), orderId";
    sql(sql).ok();
  }

  @Test void testHopTable() {
    final String sql = "select stream hop_start(rowtime, interval '1' hour,"
        + " interval '3' hour) as rowtime,\n"
        + "  count(*) as c\n"
        + "from orders\n"
        + "group by hop(rowtime, interval '1' hour, interval '3' hour)";
    sql(sql).ok();
  }

  @Test void testSessionTable() {
    final String sql = "select stream session_start(rowtime, interval '1' hour)"
        + " as rowtime,\n"
        + "  session_end(rowtime, interval '1' hour),\n"
        + "  count(*) as c\n"
        + "from orders\n"
        + "group by session(rowtime, interval '1' hour)";
    sql(sql).ok();
  }

  @Test void testInterval() {
    // temporarily disabled per DTbug 1212
    if (!Bug.DT785_FIXED) {
      return;
    }
    final String sql =
        "values(cast(interval '1' hour as interval hour to second))";
    sql(sql).ok();
  }

  @Test void testStream() {
    final String sql =
        "select stream productId from orders where productId = 10";
    sql(sql).ok();
  }

  @Test void testStreamGroupBy() {
    final String sql = "select stream\n"
        + " floor(rowtime to second) as rowtime, count(*) as c\n"
        + "from orders\n"
        + "group by floor(rowtime to second)";
    sql(sql).ok();
  }

  @Test void testStreamWindowedAggregation() {
    final String sql = "select stream *,\n"
        + "  count(*) over (partition by productId\n"
        + "    order by rowtime\n"
        + "    range interval '1' second preceding) as c\n"
        + "from orders";
    sql(sql).ok();
  }

  @Test void testExplainAsXml() {
    String sql = "select 1 + 2, 3 from (values (true))";
    final RelNode rel = sql(sql).toRel();
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    RelXmlWriter planWriter =
        new RelXmlWriter(pw, SqlExplainLevel.EXPPLAN_ATTRIBUTES);
    rel.explain(planWriter);
    pw.flush();
    TestUtil.assertEqualsVerbose(
        "<RelNode type=\"LogicalProject\">\n"
            + "\t<Property name=\"EXPR$0\">\n"
            + "\t\t+(1, 2)\n"
            + "\t</Property>\n"
            + "\t<Property name=\"EXPR$1\">\n"
            + "\t\t3\n"
            + "\t</Property>\n"
            + "\t<Inputs>\n"
            + "\t\t<RelNode type=\"LogicalValues\">\n"
            + "\t\t\t<Property name=\"tuples\">\n"
            + "\t\t\t\t[{ true }]\n"
            + "\t\t\t</Property>\n"
            + "\t\t\t<Inputs/>\n"
            + "\t\t</RelNode>\n"
            + "\t</Inputs>\n"
            + "</RelNode>\n",
        Util.toLinux(sw.toString()));
  }

  @Test void testExplainAsDot() {
    String sql = "select 1 + 2, 3 from (values (true))";
    final RelNode rel = sql(sql).toRel();
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    RelDotWriter planWriter =
        new RelDotWriter(pw, SqlExplainLevel.EXPPLAN_ATTRIBUTES, false);
    rel.explain(planWriter);
    pw.flush();
    TestUtil.assertEqualsVerbose(
        "digraph {\n"
            + "\"LogicalValues\\ntuples = [{ true }]\\n\" -> \"LogicalProject\\nEXPR$0 = +(1, 2)"
            + "\\nEXPR$1 = 3\\n\" [label=\"0\"]\n"
            + "}\n",
        Util.toLinux(sw.toString()));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-412">[CALCITE-412]
   * RelFieldTrimmer: when trimming Sort, the collation and trait set don't
   * match</a>. */
  @Test void testSortWithTrim() {
    final String sql = "select ename from (select * from emp order by sal) a";
    sql(sql).withTrim(true).ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5377">[CALCITE-5377]
   * RelFieldTrimmer support Sort with dynamic param</a>. */
  @Test void testDynamicParameterSortWithTrim() {
    final String sql = "select ename from "
        + "(select * from emp order by sal limit ? offset ?) a";
    sql(sql).withTrim(true).ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3183">[CALCITE-3183]
   * Trimming method for Filter rel uses wrong traitSet</a>. */
  @SuppressWarnings("rawtypes")
  @Test void testFilterAndSortWithTrim() {
    // Run query and save plan after trimming
    final String sql = "select count(a.EMPNO)\n"
        + "from (select * from emp order by sal limit 3) a\n"
        + "where a.EMPNO > 10 group by 2";
    RelNode afterTrim = sql(sql)
        .withDecorrelate(false)
        .withFactory(t ->
            // Create a customized test with RelCollation trait in the test
            // cluster.
            t.withPlannerFactory(context ->
                new MockRelOptPlanner(Contexts.empty()) {
                  @Override public List<RelTraitDef> getRelTraitDefs() {
                    return ImmutableList.of(RelCollationTraitDef.INSTANCE);
                  }
                  @Override public RelTraitSet emptyTraitSet() {
                    return RelTraitSet.createEmpty().plus(
                        RelCollationTraitDef.INSTANCE.getDefault());
                  }
                }))
        .toRel();

    // Get Sort and Filter operators
    final List<RelNode> rels = new ArrayList<>();
    final RelShuttleImpl visitor = new RelShuttleImpl() {
      @Override public RelNode visit(LogicalSort sort) {
        rels.add(sort);
        return super.visit(sort);
      }
      @Override public RelNode visit(LogicalFilter filter) {
        rels.add(filter);
        return super.visit(filter);
      }
    };
    visitor.visit(afterTrim);

    // Ensure sort and filter operators have consistent traitSet after trimming
    assertThat(rels, hasSize(2));
    RelTrait filterCollation = rels.get(0).getTraitSet()
        .getTrait(RelCollationTraitDef.INSTANCE);
    RelTrait sortCollation = rels.get(1).getTraitSet()
        .getTrait(RelCollationTraitDef.INSTANCE);
    assertThat(filterCollation, notNullValue());
    assertThat(sortCollation, notNullValue());
    assertThat(filterCollation.satisfies(sortCollation), is(true));
  }

  @Test void testRelShuttleForLogicalCalc() {
    final String sql = "select ename from emp";
    final RelNode rel = sql(sql).toRel();
    final HepProgramBuilder programBuilder = HepProgram.builder();
    programBuilder.addRuleInstance(CoreRules.PROJECT_TO_CALC);
    final HepPlanner planner = new HepPlanner(programBuilder.build());
    planner.setRoot(rel);
    final RelNode calc = planner.findBestExp();
    final List<RelNode> rels = new ArrayList<>();
    final RelShuttleImpl visitor = new RelShuttleImpl() {
      @Override public RelNode visit(LogicalCalc calc) {
        RelNode visitedRel = super.visit(calc);
        rels.add(visitedRel);
        return visitedRel;
      }
    };
    calc.accept(visitor);
    assertThat(rels, hasSize(1));
    assertThat(rels.get(0), instanceOf(LogicalCalc.class));
  }

  @Test void testRelShuttleForLogicalTableModify() {
    final String sql = "insert into emp select * from emp";
    final RelNode rel = sql(sql).toRel();
    final List<RelNode> rels = new ArrayList<>();
    final RelShuttleImpl visitor = new RelShuttleImpl() {
      @Override public RelNode visit(LogicalTableModify modify) {
        RelNode visitedRel = super.visit(modify);
        rels.add(visitedRel);
        return visitedRel;
      }
    };
    rel.accept(visitor);
    assertThat(rels, hasSize(1));
    assertThat(rels.get(0), instanceOf(LogicalTableModify.class));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6959">[CALCITE-6959]
   * Support LogicalAsofJoin in RelShuttle</a>. */
  @Test void testRelShuttleForLogicalAsofJoin() {
    final String sql = "select emp.empno from emp asof join dept\n"
        + "match_condition emp.deptno <= dept.deptno\n"
        + "on ename = name";
    final RelNode rel = sql(sql).toRel();
    final List<RelNode> rels = new ArrayList<>();
    final RelShuttleImpl visitor = new RelShuttleImpl() {
      @Override public RelNode visit(LogicalAsofJoin asofJoin) {
        RelNode visitedRel = super.visit(asofJoin);
        rels.add(visitedRel);
        return visitedRel;
      }
    };
    rel.accept(visitor);
    assertThat(rels, hasSize(1));
    assertThat(rels.get(0), instanceOf(LogicalAsofJoin.class));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6961">[CALCITE-6961]
   * Support LogicalRepeatUnion in RelShuttle</a>. */
  @Test void testRelShuttleForLogicalRepeatUnion() {
    final String sql = "WITH RECURSIVE delta(n) AS (\n"
        + "VALUES (1)\n"
        + "UNION ALL\n"
        + "SELECT n+1 FROM delta WHERE n < 10\n"
        + ")\n"
        + "SELECT * FROM delta";
    final RelNode rel = sql(sql).toRel();
    final List<RelNode> rels = new ArrayList<>();
    final RelShuttleImpl visitor = new RelShuttleImpl() {
      @Override public RelNode visit(LogicalRepeatUnion repeatUnion) {
        RelNode visitedRel = super.visit(repeatUnion);
        rels.add(visitedRel);
        return visitedRel;
      }
    };
    rel.accept(visitor);
    assertThat(rels, hasSize(1));
    assertThat(rels.get(0), instanceOf(LogicalRepeatUnion.class));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7090">[CALCITE-7090]
   * Support LogicalRepeatUnion in RelHomogeneousShuttle</a>. */
  @Test void testRelHomogeneousShuttleForLogicalRepeatUnion() {
    final String sql = "WITH RECURSIVE delta(n) AS (\n"
        + "VALUES (1)\n"
        + "UNION ALL\n"
        + "SELECT n+1 FROM delta WHERE n < 10\n"
        + ")\n"
        + "SELECT * FROM delta";
    final RelNode rel = sql(sql).toRel();
    final List<RelNode> rels = new ArrayList<>();
    // RelHomogeneousShuttle delegates all calls to visit(RelNode)
    final RelShuttleImpl visitor = new RelHomogeneousShuttle() {
      @Override public RelNode visit(RelNode node) {
        // Collect all nodes applied to visit(RelNode)
        rels.add(node);
        super.visit(node);
        return node;
      }
    };
    rel.accept(visitor);
    // Check existence and proper count of RepeatUnion, TableSpools and TableScans
    RepeatUnion repeatUnion = null;
    int spoolCount = 0;
    TableScan transientScan = null;
    for (RelNode node : rels) {
      if (node instanceof RepeatUnion) {
        assertThat("Should not have encountered a prior RepeatUnion", repeatUnion, nullValue());
        repeatUnion = (RepeatUnion) node;
      } else if (node instanceof TableSpool) {
        spoolCount += 1;
      } else if (node instanceof TableScan) {
        assertThat("Should not have encountered a prior TableScan", transientScan, nullValue());
        transientScan = (TableScan) node;
      }
    }

    assertThat("Should have encountered a RepeatUnion", repeatUnion, notNullValue());
    assertThat("Should have encountered 2 TableSpools", spoolCount, is(2));
    assertThat("Should have encountered a TableScan", transientScan, notNullValue());
  }

  @Test void testOffset0() {
    final String sql = "select * from emp offset 0";
    sql(sql).ok();
  }

  /** Tests group-by CASE expression involving a non-query IN. */
  @Test void testGroupByCaseSubQuery() {
    final String sql = "SELECT CASE WHEN emp.empno IN (3) THEN 0 ELSE 1 END\n"
        + "FROM emp\n"
        + "GROUP BY (CASE WHEN emp.empno IN (3) THEN 0 ELSE 1 END)";
    sql(sql).ok();
  }

  /** Tests an aggregate function on a CASE expression involving a non-query
   * IN. */
  @Test void testAggCaseSubQuery() {
    final String sql =
        "SELECT SUM(CASE WHEN empno IN (3) THEN 0 ELSE 1 END) FROM emp";
    sql(sql).ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-753">[CALCITE-753]
   * Test aggregate operators do not derive row types with duplicate column
   * names</a>. */
  @Test void testAggNoDuplicateColumnNames() {
    final String sql = "SELECT  empno, EXPR$2, COUNT(empno) FROM (\n"
        + "    SELECT empno, deptno AS EXPR$2\n"
        + "    FROM emp)\n"
        + "GROUP BY empno, EXPR$2";
    sql(sql).ok();
  }

  @Test void testAggScalarSubQuery() {
    final String sql = "SELECT SUM(SELECT min(deptno) FROM dept) FROM emp";
    sql(sql).ok();
  }

  /** Test aggregate function on a CASE expression involving IN with a
   * sub-query.
   *
   * <p>Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-551">[CALCITE-551]
   * Sub-query inside aggregate function</a>. */
  @Test void testAggCaseInSubQuery() {
    final String sql = "SELECT SUM(\n"
        + "  CASE WHEN deptno IN (SELECT deptno FROM dept) THEN 1 ELSE 0 END)\n"
        + "FROM emp";
    sql(sql).withExpand(false).ok();
  }

  @Test void testCorrelatedSubQueryInAggregate() {
    final String sql = "SELECT SUM(\n"
        + "  (select char_length(name) from dept\n"
        + "   where dept.deptno = emp.empno))\n"
        + "FROM emp";
    sql(sql).withExpand(false).ok();
  }

  @Test void testCorrelatedForOuterFields() {
    final String sql = "SELECT ARRAY(SELECT dept.deptno)\n"
        + "FROM emp\n"
        + "LEFT OUTER JOIN dept\n"
        + "ON emp.empno = dept.deptno";
    sql(sql).ok();
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-614">[CALCITE-614]
   * IN within CASE within GROUP BY gives AssertionError</a>.
   */
  @Test void testGroupByCaseIn() {
    final String sql = "select\n"
        + " (CASE WHEN (deptno IN (10, 20)) THEN 0 ELSE deptno END),\n"
        + " min(empno) from EMP\n"
        + "group by (CASE WHEN (deptno IN (10, 20)) THEN 0 ELSE deptno END)";
    sql(sql).ok();
  }

  @Test void testInsert() {
    final String sql = "insert into empnullables (deptno, empno, ename)\n"
        + "values (10, 150, 'Fred')";
    sql(sql).ok();
  }

  @Test void testInsertSubset() {
    final String sql = "insert into empnullables\n"
        + "values (50, 'Fred')";
    sql(sql).withConformance(SqlConformanceEnum.PRAGMATIC_2003).ok();
  }

  @Test void testInsertWithCustomInitializerExpressionFactory() {
    final String sql = "insert into empdefaults (deptno) values (300)";
    sql(sql).ok();
  }

  @Test void testInsertSubsetWithCustomInitializerExpressionFactory() {
    final String sql = "insert into empdefaults values (100)";
    sql(sql).withConformance(SqlConformanceEnum.PRAGMATIC_2003).ok();
  }

  @Test void testInsertBind() {
    final String sql = "insert into empnullables (deptno, empno, ename)\n"
        + "values (?, ?, ?)";
    sql(sql).ok();
  }

  @Test void testInsertBindSubset() {
    final String sql = "insert into empnullables\n"
        + "values (?, ?)";
    sql(sql).withConformance(SqlConformanceEnum.PRAGMATIC_2003).ok();
  }

  @Test void testInsertBindWithCustomInitializerExpressionFactory() {
    final String sql = "insert into empdefaults (deptno) values (?)";
    sql(sql).ok();
  }

  @Test void testInsertBindSubsetWithCustomInitializerExpressionFactory() {
    final String sql = "insert into empdefaults values (?)";
    sql(sql).withConformance(SqlConformanceEnum.PRAGMATIC_2003).ok();
  }

  @Test void testInsertSubsetView() {
    final String sql = "insert into empnullables_20\n"
        + "values (10, 'Fred')";
    sql(sql).withConformance(SqlConformanceEnum.PRAGMATIC_2003).ok();
  }

  @Test void testInsertExtendedColumn() {
    final String sql = "insert into empdefaults(updated TIMESTAMP)\n"
        + " (ename, deptno, empno, updated, sal)\n"
        + " values ('Fred', 456, 44, timestamp '2017-03-12 13:03:05', 999999)";
    sql(sql).ok();
  }

  @Test void testInsertBindExtendedColumn() {
    final String sql = "insert into empdefaults(updated TIMESTAMP)\n"
        + " (ename, deptno, empno, updated, sal)\n"
        + " values ('Fred', 456, 44, ?, 999999)";
    sql(sql).ok();
  }

  @Test void testInsertExtendedColumnModifiableView() {
    final String sql = "insert into EMP_MODIFIABLEVIEW2(updated TIMESTAMP)\n"
        + " (ename, deptno, empno, updated, sal)\n"
        + " values ('Fred', 20, 44, timestamp '2017-03-12 13:03:05', 999999)";
    sql(sql).withExtendedTester().ok();
  }

  @Test void testInsertBindExtendedColumnModifiableView() {
    final String sql = "insert into EMP_MODIFIABLEVIEW2(updated TIMESTAMP)\n"
        + " (ename, deptno, empno, updated, sal)\n"
        + " values ('Fred', 20, 44, ?, 999999)";
    sql(sql).withExtendedTester().ok();
  }

  @Test void testInsertWithSort() {
    final String sql = "insert into empnullables (empno, ename)\n"
        + "select deptno, ename from emp order by ename";
    sql(sql).ok();
  }

  @Test void testInsertWithLimit() {
    final String sql = "insert into empnullables (empno, ename)\n"
        + "select deptno, ename from emp order by ename limit 10";
    sql(sql).ok();
  }

  @Test void testDelete() {
    final String sql = "delete from emp";
    sql(sql).ok();
  }

  /** Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-7132">[CALCITE-7132]
   * Inconsistency with type coercion and character types</a>. */
  @Test void testCoercion() {
    sql("WITH c AS (SELECT CAST('x' as VARCHAR(2)) AS X, CAST('y' AS CHAR(2)) AS Y)"
        + "SELECT X = Y AND Y = X FROM c")
        .ok();
  }

  @Test void testDeleteWhere() {
    final String sql = "delete from emp where deptno = 10";
    sql(sql).ok();
  }

  @Test void testDeleteBind() {
    final String sql = "delete from emp where deptno = ?";
    sql(sql).ok();
  }

  @Test void testDeleteBindExtendedColumn() {
    final String sql = "delete from emp(enddate TIMESTAMP) where enddate < ?";
    sql(sql).ok();
  }

  @Test void testDeleteBindModifiableView() {
    final String sql = "delete from EMP_MODIFIABLEVIEW2 where empno = ?";
    sql(sql).withExtendedTester().ok();
  }

  @Test void testDeleteBindExtendedColumnModifiableView() {
    final String sql = "delete from EMP_MODIFIABLEVIEW2(note VARCHAR)\n"
        + "where note = ?";
    sql(sql).withExtendedTester().ok();
  }

  @Test void testUpdate() {
    final String sql = "update emp set empno = empno + 1";
    sql(sql).ok();
  }

  @Test void testUpdateSubQuery() {
    final String sql = "update emp\n"
        + "set empno = (\n"
        + "  select min(empno) from emp as e where e.deptno = emp.deptno)";
    sql(sql).ok();
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3229">[CALCITE-3229]
   * UnsupportedOperationException for UPDATE with IN query</a>.
   */
  @Test void testUpdateSubQueryWithIn() {
    final String sql = "update emp\n"
            + "set empno = 1 where empno in (\n"
            + "  select empno from emp where empno=2)";
    sql(sql).ok();
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3292">[CALCITE-3292]
   * NPE for UPDATE with IN query</a>.
   */
  @Test void testUpdateSubQueryWithIn1() {
    final String sql = "update emp\n"
            + "set empno = 1 where emp.empno in (\n"
            + "  select emp.empno from emp where emp.empno=2)";
    sql(sql).ok();
  }

  /** Similar to {@link #testUpdateSubQueryWithIn()} but with not in instead of in. */
  @Test void testUpdateSubQueryWithNotIn() {
    final String sql = "update emp\n"
            + "set empno = 1 where empno not in (\n"
            + "  select empno from emp where empno=2)";
    sql(sql).ok();
  }

  @Test void testUpdateWhere() {
    final String sql = "update emp set empno = empno + 1 where deptno = 10";
    sql(sql).ok();
  }

  @Test void testUpdateModifiableView() {
    final String sql = "update EMP_MODIFIABLEVIEW2\n"
        + "set sal = sal + 5000 where slacker = false";
    sql(sql).withExtendedTester().ok();
  }

  @Test void testUpdateExtendedColumn() {
    final String sql = "update empdefaults(updated TIMESTAMP)"
        + " set deptno = 1, updated = timestamp '2017-03-12 13:03:05', empno = 20, ename = 'Bob'"
        + " where deptno = 10";
    sql(sql).ok();
  }

  @Test void testUpdateExtendedColumnModifiableView() {
    final String sql = "update EMP_MODIFIABLEVIEW2(updated TIMESTAMP)\n"
        + "set updated = timestamp '2017-03-12 13:03:05', sal = sal + 5000\n"
        + "where slacker = false";
    sql(sql).withExtendedTester().ok();
  }

  @Test void testUpdateBind() {
    final String sql = "update emp"
        + " set sal = sal + ? where slacker = false";
    sql(sql).ok();
  }

  @Test void testUpdateBind2() {
    final String sql = "update emp"
        + " set sal = ? where slacker = false";
    sql(sql).ok();
  }

  @Disabled("CALCITE-1708")
  @Test void testUpdateBindExtendedColumn() {
    final String sql = "update emp(test INT)"
        + " set test = ?, sal = sal + 5000 where slacker = false";
    sql(sql).ok();
  }

  @Disabled("CALCITE-1708")
  @Test void testUpdateBindExtendedColumnModifiableView() {
    final String sql = "update EMP_MODIFIABLEVIEW2(test INT)"
        + " set test = ?, sal = sal + 5000 where slacker = false";
    sql(sql).ok();
  }

  @Test void testMerge() {
    final String sql = "merge into empnullables e\n"
        + "using (select * from emp where deptno is null) t\n"
        + "on e.empno = t.empno\n"
        + "when matched then update\n"
        + "set ename = t.ename, deptno = t.deptno, sal = t.sal * .1\n"
        + "when not matched then insert (empno, ename, deptno, sal)\n"
        + "values(t.empno, t.ename, 10, t.sal * .15)";
    sql(sql).ok();
  }

  @Test void testSelectView() {
    // translated condition: deptno = 20 and sal > 1000 and empno > 100
    final String sql = "select * from emp_20 where empno > 100";
    sql(sql).ok();
  }

  @Test void testInsertView() {
    final String sql = "insert into empnullables_20 (empno, ename)\n"
        + "values (150, 'Fred')";
    sql(sql).ok();
  }

  @Test void testInsertModifiableView() {
    final String sql = "insert into EMP_MODIFIABLEVIEW (EMPNO, ENAME, JOB)"
        + " values (34625, 'nom', 'accountant')";
    sql(sql).withExtendedTester().ok();
  }

  @Test void testInsertSubsetModifiableView() {
    final String sql = "insert into EMP_MODIFIABLEVIEW "
        + "values (10, 'Fred')";
    sql(sql).withExtendedTester()
        .withConformance(SqlConformanceEnum.PRAGMATIC_2003).ok();
  }

  @Test void testInsertBindModifiableView() {
    final String sql = "insert into EMP_MODIFIABLEVIEW (empno, job)"
        + " values (?, ?)";
    sql(sql).withExtendedTester().ok();
  }

  @Test void testInsertBindSubsetModifiableView() {
    final String sql = "insert into EMP_MODIFIABLEVIEW"
        + " values (?, ?)";
    sql(sql).withConformance(SqlConformanceEnum.PRAGMATIC_2003)
        .withExtendedTester().ok();
  }

  @Test void testInsertWithCustomColumnResolving() {
    final String sql = "insert into struct.t values (?, ?, ?, ?, ?, ?, ?, ?, ?)";
    sql(sql).ok();
  }

  @Test void testInsertWithCustomColumnResolving2() {
    final String sql = "insert into struct.t_nullables (f0.c0, f1.c2, c1)\n"
        + "values (?, ?, ?)";
    sql(sql).ok();
  }

  @Test void testInsertViewWithCustomColumnResolving() {
    final String sql = "insert into struct.t_10 (f0.c0, f1.c2, c1, k0,\n"
        + "  f1.a0, f2.a0, f0.c1, f2.c3)\n"
        + "values (?, ?, ?, ?, ?, ?, ?, ?)";
    sql(sql).ok();
  }

  @Test void testUpdateWithCustomColumnResolving() {
    final String sql = "update struct.t set c0 = c0 + 1";
    sql(sql).ok();
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2936">[CALCITE-2936]
   * Existential sub-query that has aggregate without grouping key
   * should be simplified to constant boolean expression</a>.
   */
  @Test void testSimplifyExistsAggregateSubQuery() {
    final String sql = "SELECT e1.empno\n"
        + "FROM emp e1 where exists\n"
        + "(select avg(sal) from emp e2 where e1.empno = e2.empno)";
    sql(sql).withDecorrelate(true).ok();
  }

  @Test void testSimplifyNotExistsAggregateSubQuery() {
    final String sql = "SELECT e1.empno\n"
        + "FROM emp e1 where not exists\n"
        + "(select avg(sal) from emp e2 where e1.empno = e2.empno)";
    sql(sql).withDecorrelate(true).ok();
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2936">[CALCITE-2936]
   * Existential sub-query that has Values with at least 1 tuple
   * should be simplified to constant boolean expression</a>.
   */
  @Test void testSimplifyExistsValuesSubQuery() {
    final String sql = "select deptno\n"
        + "from EMP\n"
        + "where exists (values 10)";
    sql(sql).withDecorrelate(true).ok();
  }

  @Test void testSimplifyNotExistsValuesSubQuery() {
    final String sql = "select deptno\n"
        + "from EMP\n"
        + "where not exists (values 10)";
    sql(sql).withDecorrelate(true).ok();
  }

  @Test void testReduceConstExpr() {
    final String sql = "select sum(case when 'y' = 'n' then ename else 0.1 end) from emp";
    sql(sql).ok();
  }

  @Test void testSubQueryNoExpand() {
    final String sql = "select (select empno from EMP where 1 = 0)";
    sql(sql).withExpand(false).ok();
  }

  @Test void testConvertFunc() {
    final String sql = "select convert(ename, latin1, utf8) as new_ename\n"
        + "from emp";
    sql(sql).withTrim(true).ok();
  }

  @Test void testTranslateFunc1() {
    final String sql = "select translate(ename using utf8) as new_ename\n"
        + "from emp";
    sql(sql).withTrim(true).ok();
  }

  @Test void testTranslateFunc2() {
    final String sql = "select convert(ename using utf8) as new_ename\n"
        + "from emp";
    sql(sql).withTrim(true).ok();
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-695">[CALCITE-695]
   * SqlSingleValueAggFunction is created when it may not be needed</a>.
   */
  @Test void testSubQueryAggregateFunctionFollowedBySimpleOperation() {
    final String sql = "select deptno\n"
        + "from EMP\n"
        + "where deptno > (select min(deptno) * 2 + 10 from EMP)";
    sql(sql).ok();
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1799">[CALCITE-1799]
   * "OR .. IN" sub-query conversion wrong</a>.
   *
   * <p>The problem is only fixed if you have {@code expand = false}.
   */
  @Test void testSubQueryOr() {
    final String sql = "select * from emp where deptno = 10 or deptno in (\n"
        + "    select dept.deptno from dept where deptno < 5)\n";
    sql(sql).withExpand(false).ok();
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-695">[CALCITE-695]
   * SqlSingleValueAggFunction is created when it may not be needed</a>.
   */
  @Test void testSubQueryValues() {
    final String sql = "select deptno\n"
        + "from EMP\n"
        + "where deptno > (values 10)";
    sql(sql).ok();
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-695">[CALCITE-695]
   * SqlSingleValueAggFunction is created when it may not be needed</a>.
   */
  @Test void testSubQueryLimitOne() {
    final String sql = "select deptno\n"
        + "from EMP\n"
        + "where deptno > (select deptno\n"
        + "from EMP order by deptno limit 1)";
    sql(sql).ok();
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-710">[CALCITE-710]
   * When look up sub-queries, perform the same logic as the way when ones were
   * registered</a>.
   */
  @Test void testIdenticalExpressionInSubQuery() {
    final String sql = "select deptno\n"
        + "from EMP\n"
        + "where deptno in (1, 2) or deptno in (1, 2)";
    sql(sql).ok();
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-694">[CALCITE-694]
   * Scan HAVING clause for sub-queries and IN-lists</a> relating to IN.
   */
  @Test void testHavingAggrFunctionIn() {
    final String sql = "select deptno\n"
        + "from emp\n"
        + "group by deptno\n"
        + "having sum(case when deptno in (1, 2) then 0 else 1 end) +\n"
        + "sum(case when deptno in (3, 4) then 0 else 1 end) > 10";
    sql(sql).ok();
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-694">[CALCITE-694]
   * Scan HAVING clause for sub-queries and IN-lists</a>, with a sub-query in
   * the HAVING clause.
   */
  @Test void testHavingInSubQueryWithAggrFunction() {
    final String sql = "select sal\n"
        + "from emp\n"
        + "group by sal\n"
        + "having sal in (\n"
        + "  select deptno\n"
        + "  from dept\n"
        + "  group by deptno\n"
        + "  having sum(deptno) > 0)";
    sql(sql).ok();
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-716">[CALCITE-716]
   * Scalar sub-query and aggregate function in SELECT or HAVING clause gives
   * AssertionError</a>; variant involving HAVING clause.
   */
  @Test void testAggregateAndScalarSubQueryInHaving() {
    final String sql = "select deptno\n"
        + "from emp\n"
        + "group by deptno\n"
        + "having max(emp.empno) > (SELECT min(emp.empno) FROM emp)\n";
    sql(sql).ok();
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-716">[CALCITE-716]
   * Scalar sub-query and aggregate function in SELECT or HAVING clause gives
   * AssertionError</a>; variant involving SELECT clause.
   */
  @Test void testAggregateAndScalarSubQueryInSelect() {
    final String sql = "select deptno,\n"
        + "  max(emp.empno) > (SELECT min(emp.empno) FROM emp) as b\n"
        + "from emp\n"
        + "group by deptno\n";
    sql(sql).ok();
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-770">[CALCITE-770]
   * window aggregate and ranking functions with grouped aggregates</a>.
   */
  @Test void testWindowAggWithGroupBy() {
    final String sql = "select min(deptno), rank() over (order by empno),\n"
        + "max(empno) over (partition by deptno)\n"
        + "from emp group by deptno, empno\n";
    sql(sql).ok();
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-847">[CALCITE-847]
   * AVG window function in GROUP BY gives AssertionError</a>.
   */
  @Test void testWindowAverageWithGroupBy() {
    final String sql = "select avg(deptno) over ()\n"
        + "from emp\n"
        + "group by deptno";
    sql(sql).ok();
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-770">[CALCITE-770]
   * variant involving joins</a>.
   */
  @Test void testWindowAggWithGroupByAndJoin() {
    final String sql = "select min(d.deptno), rank() over (order by e.empno),\n"
        + " max(e.empno) over (partition by e.deptno)\n"
        + "from emp e, dept d\n"
        + "where e.deptno = d.deptno\n"
        + "group by d.deptno, e.empno, e.deptno\n";
    sql(sql).ok();
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-770">[CALCITE-770]
   * variant involving HAVING clause</a>.
   */
  @Test void testWindowAggWithGroupByAndHaving() {
    final String sql = "select min(deptno), rank() over (order by empno),\n"
        + "max(empno) over (partition by deptno)\n"
        + "from emp group by deptno, empno\n"
        + "having empno < 10 and min(deptno) < 20\n";
    sql(sql).ok();
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-770">[CALCITE-770]
   * variant involving join with sub-query that contains window function and
   * GROUP BY</a>.
   */
  @Test void testWindowAggInSubQueryJoin() {
    final String sql = "select T.x, T.y, T.z, emp.empno\n"
        + "from (select min(deptno) as x,\n"
        + "   rank() over (order by empno) as y,\n"
        + "   max(empno) over (partition by deptno) as z\n"
        + "   from emp group by deptno, empno) as T\n"
        + " inner join emp on T.x = emp.deptno\n"
        + " and T.y = emp.empno\n";
    sql(sql).ok();
  }

  /** Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-6376">[CALCITE-6376]
   * Selecting 6 columns with QUALIFY operation results in exception</a>. */
  @Test void testQualifyWindow() {
    sql("SELECT empno, ename, deptno, job, mgr, hiredate\n"
        + "FROM emp\n"
        + "QUALIFY ROW_NUMBER() over (partition by ename order by deptno) = 1")
        .ok();
  }

  @Test void testQualifyWithoutReferences() {
    sql("SELECT empno, ename, deptno\n"
        + "FROM emp\n"
        + "QUALIFY ROW_NUMBER() over (partition by ename order by deptno) = 1")
        .ok();
  }

  @Test void testQualifyWithoutReferencesAndFilter() {
    sql("SELECT empno, ename, deptno\n"
        + "FROM emp\n"
        + "WHERE deptno > 5\n"
        + "QUALIFY ROW_NUMBER() over (partition by ename order by deptno) = 1")
        .ok();
  }

  @Test void testQualifyWithReferences() {
    sql("SELECT empno, ename, deptno,\n"
        + "   ROW_NUMBER() over (partition by ename order by deptno) as row_num\n"
        + "FROM emp\n"
        + "QUALIFY row_num = 1")
        .ok();
  }

  @Test void testQualifyWithMultipleReferences() {
    sql("SELECT empno, ename, deptno + 1 as derived_deptno,\n"
        + "   ROW_NUMBER() over (partition by ename order by deptno) as row_num\n"
        + "FROM emp\n"
        + "QUALIFY row_num = derived_deptno")
        .ok();
  }

  @Test void testQualifyWithDerivedColumn() {
    sql("SELECT empno, ename, deptno, SUBSTRING(ename,1,1) as DERIVED_COLUMN "
        + "FROM emp "
        + "QUALIFY ROW_NUMBER() OVER (PARTITION BY deptno\n"
        + "                           ORDER BY DERIVED_COLUMN) = 1")
        .ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6691">[CALCITE-6691]
   * QUALIFY on subquery that projects</a>. */
  @Test void testQualifyOnProject() {
    sql("WITH t0 AS (SELECT deptno, sal FROM emp),\n"
        + "t1 AS (SELECT deptno\n"
        + "    FROM t0\n"
        + "    QUALIFY row_number() OVER (PARTITION BY deptno\n"
        + "                               ORDER BY sal DESC) = 1)\n"
        + "SELECT deptno FROM t1")
        .ok();
  }

  @Test void testQualifyAfterGroupBy() {
    sql("WITH t0 AS (SELECT deptno, sal FROM emp),\n"
        + "t1 AS (SELECT deptno, sal, COUNT(*)\n"
        + "    FROM t0\n"
        + "    GROUP BY deptno, sal\n"
        + "    QUALIFY row_number() OVER (PARTITION BY deptno\n"
        + "                               ORDER BY COUNT(*) DESC) = 1)\n"
        + "SELECT deptno FROM t1")
        .ok();
  }

  @Test void testQualifyWithWindowClause() {
    sql("SELECT empno, ename, SUM(deptno) OVER myWindow as sumDeptNo\n"
        + "FROM emp\n"
        + "WINDOW myWindow AS (PARTITION BY ename ORDER BY empno)\n"
        + "QUALIFY sumDeptNo = 1")
        .ok();
  }

  @Test void testQualifyInDdl() {
    sql("INSERT INTO dept(deptno, name)\n"
        + "SELECT DISTINCT empno, ename\n"
        + "FROM emp\n"
        + "WHERE deptno > 5\n"
        + "QUALIFY RANK() OVER (PARTITION BY ename\n"
        + "                     ORDER BY slacker DESC) = 1")
        .ok();
  }

  @Test void testQualifyInSubQuery() {
    sql("SELECT *\n"
        + "FROM (\n"
        + " SELECT DISTINCT empno, ename, deptno\n"
        + " FROM emp\n"
        + " QUALIFY RANK() OVER (PARTITION BY ename\n"
        + "                      ORDER BY deptno DESC) = 1)")
        .ok();
  }

  @Test void testQualifyWithSubQueryFilter() {
    sql("SELECT empno, ename, deptno,\n"
        + "    RANK() OVER (PARTITION BY ename\n"
        + "                 ORDER BY deptno DESC) as rank_val\n"
        + "FROM emp\n"
        + "QUALIFY rank_val = (SELECT COUNT(*) FROM emp)")
        .ok();
  }

  @Test void testQualifyWithEverything() {
    sql("SELECT DISTINCT empno, ename, deptno,\n"
        + "    RANK() OVER (PARTITION BY ename\n"
        + "                 ORDER BY deptno DESC) as rank_val\n"
        + "FROM emp\n"
        + "WHERE sal > 1000\n"
        + "QUALIFY rank_val = (SELECT COUNT(*) FROM emp)\n"
        + "ORDER BY deptno\n"
        + "LIMIT 5")
        .ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6950">[CALCITE-6950]
   * Use ANY operator to check if an element exists in an array throws exception</a>. */
  @Test void testQuantifyOperatorsWithTypeCoercion() {
    sql("SELECT 1.0 = some (ARRAY[2,null,3])")
        .withExpand(false)
        .ok();
  }

  @Test void testQuantifyOperatorsWithTypeCoercion2() {
    sql("SELECT 3 = some (ARRAY[1.0, 2.0])")
        .withExpand(false)
        .ok();
  }

  @Test void testQuantifyOperatorsWithTypeCoercion3() {
    sql("SELECT '1970-01-01 01:23:45' = any (array[timestamp '1970-01-01 01:23:45',"
        + "timestamp '1970-01-01 01:23:46'])")
        .withExpand(false)
        .ok();
  }

  @Test void testQuantifyOperatorsWithTypeCoercion4() {
    sql("SELECT timestamp '1970-01-01 01:23:45' = any (array['1970-01-01 01:23:45',"
        + "'1970-01-01 01:23:46'])")
        .withExpand(false)
        .ok();
  }

  @Test void testQualifyInCorrelatedSubQuery() {
    // The QUALIFY clause is inside a WHERE EXISTS and references columns from
    // the enclosing query.
    sql("SELECT *\n"
        + "FROM emp\n"
        + "WHERE EXISTS(\n"
        + " SELECT name\n"
        + " FROM dept\n"
        + " QUALIFY RANK() OVER (PARTITION BY name\n"
        + "                      ORDER BY dept.deptno DESC) = emp.deptno\n"
        + ")")
        .ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1313">[CALCITE-1313]
   * Validator should derive type of expression in ORDER BY</a>.
   */
  @Test void testOrderByOver() {
    String sql = "select deptno, rank() over(partition by empno order by deptno)\n"
        + "from emp order by row_number() over(partition by empno order by deptno)";
    sql(sql).ok();
  }

  /**
   * Test case (correlated scalar aggregate sub-query) for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-714">[CALCITE-714]
   * When de-correlating, push join condition into sub-query</a>.
   */
  @Test void testCorrelationScalarAggAndFilter() {
    final String sql = "SELECT e1.empno\n"
        + "FROM emp e1, dept d1 where e1.deptno = d1.deptno\n"
        + "and e1.deptno < 10 and d1.deptno < 15\n"
        + "and e1.sal > (select avg(sal) from emp e2 where e1.empno = e2.empno)";
    sql(sql).withDecorrelate(true).withExpand(true).ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1543">[CALCITE-1543]
   * Correlated scalar sub-query with multiple aggregates gives
   * AssertionError</a>. */
  @Test void testCorrelationMultiScalarAggregate() {
    final String sql = "select sum(e1.empno)\n"
        + "from emp e1, dept d1\n"
        + "where e1.deptno = d1.deptno\n"
        + "and e1.sal > (select avg(e2.sal) from emp e2\n"
        + "  where e2.deptno = d1.deptno)";
    sql(sql).withDecorrelate(true).withExpand(true).ok();
  }

  @Test void testCorrelationScalarAggAndFilterRex() {
    final String sql = "SELECT e1.empno\n"
        + "FROM emp e1, dept d1 where e1.deptno = d1.deptno\n"
        + "and e1.deptno < 10 and d1.deptno < 15\n"
        + "and e1.sal > (select avg(sal) from emp e2 where e1.empno = e2.empno)";
    sql(sql).withDecorrelate(true).withExpand(false).ok();
  }

  /**
   * Test case (correlated EXISTS sub-query) for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-714">[CALCITE-714]
   * When de-correlating, push join condition into sub-query</a>.
   */
  @Test void testCorrelationExistsAndFilter() {
    final String sql = "SELECT e1.empno\n"
        + "FROM emp e1, dept d1 where e1.deptno = d1.deptno\n"
        + "and e1.deptno < 10 and d1.deptno < 15\n"
        + "and exists (select * from emp e2 where e1.empno = e2.empno)";
    sql(sql).withDecorrelate(true).withExpand(true).ok();
  }

  @Test void testCorrelationExistsAndFilterRex() {
    final String sql = "SELECT e1.empno\n"
        + "FROM emp e1, dept d1 where e1.deptno = d1.deptno\n"
        + "and e1.deptno < 10 and d1.deptno < 15\n"
        + "and exists (select * from emp e2 where e1.empno = e2.empno)";
    sql(sql).withDecorrelate(true).ok();
  }

  /** A theta join condition, unlike the equi-join condition in
   * {@link #testCorrelationExistsAndFilterRex()}, requires a value
   * generator. */
  @Test void testCorrelationExistsAndFilterThetaRex() {
    final String sql = "SELECT e1.empno\n"
        + "FROM emp e1, dept d1 where e1.deptno = d1.deptno\n"
        + "and e1.deptno < 10 and d1.deptno < 15\n"
        + "and exists (select * from emp e2 where e1.empno < e2.empno)";
    sql(sql).withDecorrelate(true).ok();
  }

  /**
   * Test case (correlated NOT EXISTS sub-query) for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-714">[CALCITE-714]
   * When de-correlating, push join condition into sub-query</a>.
   */
  @Test void testCorrelationNotExistsAndFilter() {
    final String sql = "SELECT e1.empno\n"
        + "FROM emp e1, dept d1 where e1.deptno = d1.deptno\n"
        + "and e1.deptno < 10 and d1.deptno < 15\n"
        + "and not exists (select * from emp e2 where e1.empno = e2.empno)";
    sql(sql).withDecorrelate(true).ok();
  }

  /**
   * Test case for decorrelating sub-query that has aggregate with
   * grouping sets.
   */
  @Test void testCorrelationAggregateGroupSets() {
    final String sql = "select sum(e1.empno)\n"
        + "from emp e1, dept d1\n"
        + "where e1.deptno = d1.deptno\n"
        + "and e1.sal > (select avg(e2.sal) from emp e2\n"
        + "  where e2.deptno = d1.deptno group by cube(comm, mgr))";
    sql(sql).withDecorrelate(true).ok();
  }

  @Test void testCorrelationInProjectionWithScan() {
    final String sql = "select array(select e.deptno) from emp e";
    sql(sql).withExpand(false).withDecorrelate(false).ok();
  }

  @Test void testCorrelationInProjectionWithProjection() {
    final String sql = "select array(select e.deptno)\n"
        + "from (select deptno, ename from emp) e";
    sql(sql).withExpand(false).withDecorrelate(false).ok();
  }

  @Test void testMultiCorrelationInProjectionWithProjection() {
    final String sql = "select cardinality(array(select e.deptno)), array(select e.ename)[0]\n"
        + "from (select deptno, ename from emp) e";
    sql(sql).withExpand(false).withDecorrelate(false).ok();
  }

  @Test void testCorrelationInProjectionWithCorrelatedProjection() {
    final String sql = "select cardinality(arr) from (\n"
        + "  select array(select e.deptno) arr from (\n"
        + "    select deptno, ename from emp) e)";
    sql(sql).withExpand(false).withDecorrelate(false).ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6554">[CALCITE-6554]
   * Nested correlated sub-query in aggregation does not have inner correlation variable bound
   * to inner projection</a>. */
  @Test void testCorrelationInProjectionWith1xNestedCorrelatedProjection() {
    final String sql = "select e1.empno,\n"
          + "  (select sum(e2.sal +\n"
          + "    (select sum(e3.sal) from emp e3 where e3.mgr = e2.empno)\n"
          + "   ) from emp e2 where e2.mgr = e1.empno)\n"
          + "from emp e1";
    sql(sql).withExpand(false).withDecorrelate(false).ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6554">[CALCITE-6554]
   * Nested correlated sub-query in aggregation does not have inner correlation variable bound
   * to inner projection</a>. */
  @Test void testCorrelationInProjectionWith2xNestedCorrelatedProjection() {
    final String sql = "select e1.empno,\n"
        + "  (select sum(e2.sal +\n"
        + "    (select sum(e3.sal + (select sum(e4.sal) from emp e4 where e4.mgr = e3.empno)\n"
        + "      ) from emp e3 where e3.mgr = e2.empno)\n"
        + "   ) from emp e2 where e2.mgr = e1.empno)\n"
        + "from emp e1";
    sql(sql).withExpand(false).withDecorrelate(false).ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6554">[CALCITE-6554]
   * Nested correlated sub-query in aggregation does not have inner correlation variable bound
   * to inner projection</a>. */
  @Test void testCorrelationInProjectionWithCorrelatedProjectionWithNestedNonCorrelatedSubquery() {
    final String sql = "select e1.empno,\n"
        + "  (select sum(e2.sal +\n"
        + "    (select sum(e3.sal) from emp e3 where e3.mgr = e1.empno)\n"
        + "   ) from emp e2 where e2.mgr = e1.empno)\n"
        + "from emp e1";
    sql(sql).withExpand(false).withDecorrelate(false).ok();
  }

  @Test void testCustomColumnResolving() {
    final String sql = "select k0 from struct.t";
    sql(sql).ok();
  }

  @Test void testCustomColumnResolving2() {
    final String sql = "select c2 from struct.t";
    sql(sql).ok();
  }

  @Test void testCustomColumnResolving3() {
    final String sql = "select f1.c2 from struct.t";
    sql(sql).ok();
  }

  @Test void testCustomColumnResolving4() {
    final String sql = "select c1 from struct.t order by f0.c1";
    sql(sql).ok();
  }

  @Test void testCustomColumnResolving5() {
    final String sql = "select count(c1) from struct.t group by f0.c1";
    sql(sql)
        .withConfig(c ->
            // Don't prune the Project. We want to see columns "FO"."C1" & "C1".
            c.addRelBuilderConfigTransform(c2 ->
                c2.withPruneInputOfAggregate(false)))
        .ok();
  }

  @Test void testCustomColumnResolvingWithSelectStar() {
    final String sql = "select * from struct.t";
    sql(sql).ok();
  }

  @Test void testCustomColumnResolvingWithSelectFieldNameDotStar() {
    final String sql = "select f1.* from struct.t";
    sql(sql).ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1150">[CALCITE-1150]
   * Dynamic Table / Dynamic Star support</a>. */
  @Test void testSelectFromDynamicTable() {
    final String sql = "select n_nationkey, n_name from SALES.NATION";
    sql(sql).withDynamicTable().ok();
  }

  /** As {@link #testSelectFromDynamicTable} but "SELECT *". */
  @Test void testSelectStarFromDynamicTable() {
    final String sql = "select * from SALES.NATION";
    sql(sql).withDynamicTable().ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2080">[CALCITE-2080]
   * Query with NOT IN operator and literal fails throws AssertionError: 'Cast
   * for just nullability not allowed'</a>. */
  @Test void testNotInWithLiteral() {
    final String sql = "SELECT *\n"
        + "FROM SALES.NATION\n"
        + "WHERE n_name NOT IN\n"
        + "    (SELECT ''\n"
        + "     FROM SALES.NATION)";
    sql(sql).withDynamicTable().ok();
  }

  /** As {@link #testSelectFromDynamicTable} but with ORDER BY. */
  @Test void testReferDynamicStarInSelectOB() {
    final String sql = "select n_nationkey, n_name\n"
        + "from (select * from SALES.NATION)\n"
        + "order by n_regionkey";
    sql(sql).withDynamicTable().ok();
  }

  /** As {@link #testSelectFromDynamicTable} but with join. */
  @Test void testDynamicStarInTableJoin() {
    final String sql = "select * from "
        + " (select * from SALES.NATION) T1, "
        + " (SELECT * from SALES.CUSTOMER) T2 "
        + " where T1.n_nationkey = T2.c_nationkey";
    sql(sql).withDynamicTable().ok();
  }

  @Test void testDynamicNestedColumn() {
    final String sql = "select t3.fake_q1['fake_col2'] as fake2\n"
        + "from (\n"
        + "  select t2.fake_col as fake_q1\n"
        + "  from SALES.CUSTOMER as t2) as t3";
    sql(sql).withDynamicTable().ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2900">[CALCITE-2900]
   * RelStructuredTypeFlattener generates wrong types on nested columns</a>. */
  @Test void testNestedColumnType() {
    final String sql = "select empa.home_address.zip\n"
        + "from sales.emp_address empa\n"
        + "where empa.home_address.city = 'abc'";
    sql(sql).ok();
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2962">[CALCITE-2962]
   * RelStructuredTypeFlattener generates wrong types for nested column when
   * flattenProjection</a>.
   */
  @Test void testSelectNestedColumnType() {
    final String sql = "select\n"
        + "  char_length(coord.\"unit\") as unit_length\n"
        + "from\n"
        + "  (\n"
        + "    select\n"
        + "      fname,\n"
        + "      coord\n"
        + "    from\n"
        + "      customer.contact_peek\n"
        + "    where\n"
        + "      coord.x > 1\n"
        + "      and coord.y > 1\n"
        + "  ) as view\n"
        + "where\n"
        + "  fname = 'john'";
    sql(sql).ok();
  }

  @Test void testNestedStructFieldAccess() {
    final String sql = "select dn.skill['others']\n"
        + "from sales.dept_nested dn";
    sql(sql).ok();
  }

  @Test void testNestedStructPrimitiveFieldAccess() {
    final String sql = "select dn.skill['others']['a']\n"
        + "from sales.dept_nested dn";
    sql(sql).ok();
  }

  @Test void testNestedStructSingleFieldAccessWhere() {
    final String sql = "select dn.skill\n"
        + "from sales.dept_single dn WHERE dn.skill.type = ''";
    sql(sql).ok();
  }

  @Test void testFunctionWithStructInput() {
    final String sql = "select json_type(skill)\n"
        + "from sales.dept_nested";
    sql(sql).ok();
  }

  @Test void testAggregateFunctionForStructInput() {
    final String sql = "select collect(skill) as collect_skill,\n"
        + "  count(skill) as count_skill, count(*) as count_star,\n"
        + "  approx_count_distinct(skill) as approx_count_distinct_skill,\n"
        + "  max(skill) as max_skill, min(skill) as min_skill,\n"
        + "  any_value(skill) as any_value_skill\n"
        + "from sales.dept_nested";
    sql(sql).ok();
  }

  @Test void testAggregateFunctionForStructInputByName() {
    final String sql = "select collect(skill) as collect_skill,\n"
        + "  count(skill) as count_skill, count(*) as count_star,\n"
        + "  approx_count_distinct(skill) as approx_count_distinct_skill,\n"
        + "  max(skill) as max_skill, min(skill) as min_skill,\n"
        + "  any_value(skill) as any_value_skill\n"
        + "from sales.dept_nested group by name";
    sql(sql).ok();
  }

  @Test void testNestedPrimitiveFieldAccess() {
    final String sql = "select dn.skill['desc']\n"
        + "from sales.dept_nested dn";
    sql(sql).ok();
  }

  @Test void testArrayElementNestedPrimitive() {
    final String sql = "select dn.employees[0]['empno']\n"
        + "from sales.dept_nested dn";
    sql(sql).ok();
  }

  @Test void testArrayElementDoublyNestedPrimitive() {
    final String sql = "select dn.employees[0]['detail']['skills'][0]['type']\n"
        + "from sales.dept_nested dn";
    sql(sql).ok();
  }

  @Test void testArrayElementDoublyNestedStruct() {
    final String sql = "select dn.employees[0]['detail']['skills'][0]\n"
        + "from sales.dept_nested dn";
    sql(sql).ok();
  }

  @Test void testArrayElementThreeTimesNestedStruct() {
    final String sql = ""
        + "select dn.employees[0]['detail']['skills'][0]['others']\n"
        + "from sales.dept_nested dn";
    sql(sql).ok();
  }

  @Test void testArraySubquery() {
    final String sql = "SELECT ARRAY(SELECT empno FROM emp)";
    sql(sql).ok();
  }

  @Test void testArraySubqueryOrderByProjectedField() {
    final String sql = "SELECT ARRAY(SELECT empno FROM emp ORDER BY empno)";
    sql(sql).ok();
  }

  /** Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-7135">[CALCITE-7135]
   * SqlToRelConverter throws AssertionError on ARRAY subquery order by a field that
   * is not present on the final projection</a>. */
  @Test void testArraySubqueryOrderByNonProjectedField() {
    final String sql = "SELECT ARRAY(SELECT empno FROM emp ORDER BY ename)";
    sql(sql).ok();
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3003">[CALCITE-3003]
   * AssertionError when GROUP BY nested field</a>.
   */
  @Test void testGroupByNestedColumn() {
    final String sql =
        "select\n"
            + "  coord.x,\n"
            + "  coord_ne.sub.a,\n"
            + "  avg(coord.y)\n"
            + "from\n"
            + "  customer.contact_peek\n"
            + "group by\n"
            + "  coord_ne.sub.a,\n"
            + "  coord.x";
    sql(sql).ok();
  }

  /**
   * Similar to {@link #testGroupByNestedColumn()},
   * but with grouping sets.
   */
  @Test void testGroupingSetsWithNestedColumn() {
    final String sql =
        "select\n"
            + "  coord.x,\n"
            + "  coord.\"unit\",\n"
            + "  coord_ne.sub.a,\n"
            + "  avg(coord.y)\n"
            + "from\n"
            + "  customer.contact_peek\n"
            + "group by\n"
            + "  grouping sets (\n"
            + "    (coord_ne.sub.a, coord.x, coord.\"unit\"),\n"
            + "    (coord.x, coord.\"unit\")\n"
            + "  )";
    sql(sql).ok();
  }

  /**
   * Similar to {@link #testGroupByNestedColumn()},
   * but with cube.
   */
  @Test void testGroupByCubeWithNestedColumn() {
    final String sql =
        "select\n"
            + "  coord.x,\n"
            + "  coord.\"unit\",\n"
            + "  coord_ne.sub.a,\n"
            + "  avg(coord.y)\n"
            + "from\n"
            + "  customer.contact_peek\n"
            + "group by\n"
            + "  cube (coord_ne.sub.a, coord.x, coord.\"unit\")";
    sql(sql).ok();
  }

  @Test void testDynamicSchemaUnnest() {
    final String sql = "select t1.c_nationkey, t3.fake_col3\n"
        + "from SALES.CUSTOMER as t1,\n"
        + "lateral (select t2 as fake_col3\n"
        + "         from unnest(t1.fake_col) as t2) as t3";
    sql(sql).withDynamicTable().ok();
  }

  @Test void testStarDynamicSchemaUnnest() {
    final String sql = "select *\n"
        + "from SALES.CUSTOMER as t1,\n"
        + "lateral (select t2 as fake_col3\n"
        + "         from unnest(t1.fake_col) as t2) as t3";
    sql(sql).withDynamicTable().ok();
  }

  @Test void testStarDynamicSchemaUnnest2() {
    final String sql = "select *\n"
        + "from SALES.CUSTOMER as t1,\n"
        + "unnest(t1.fake_col) as t2";
    sql(sql).withDynamicTable().ok();
  }

  @Test void testStarDynamicSchemaUnnestNestedSubQuery() {
    String sql = "select t2.c1\n"
        + "from (select * from SALES.CUSTOMER) as t1,\n"
        + "unnest(t1.fake_col) as t2(c1)";
    sql(sql).withDynamicTable().ok();
  }

  @Test void testReferDynamicStarInSelectWhereGB() {
    final String sql = "select n_regionkey, count(*) as cnt from "
        + "(select * from SALES.NATION) where n_nationkey > 5 "
        + "group by n_regionkey";
    sql(sql).withDynamicTable().ok();
  }

  @Test void testDynamicStarInJoinAndSubQ() {
    final String sql = "select * from "
        + " (select * from SALES.NATION T1, "
        + " SALES.CUSTOMER T2 where T1.n_nationkey = T2.c_nationkey)";
    sql(sql).withDynamicTable().ok();
  }

  @Test void testStarJoinStaticDynTable() {
    final String sql = "select * from SALES.NATION N, SALES.REGION as R "
        + "where N.n_regionkey = R.r_regionkey";
    sql(sql).withDynamicTable().ok();
  }

  @Test void testGrpByColFromStarInSubQuery() {
    final String sql = "SELECT n.n_nationkey AS col "
        + " from (SELECT * FROM SALES.NATION) as n "
        + " group by n.n_nationkey";
    sql(sql).withDynamicTable().ok();
  }

  @Test void testDynStarInExistSubQ() {
    final String sql = "select *\n"
        + "from SALES.REGION where exists (select * from SALES.NATION)";
    sql(sql).withDynamicTable().ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1150">[CALCITE-1150]
   * Create the a new DynamicRecordType, avoiding star expansion when working
   * with this type</a>. */
  @Test void testSelectDynamicStarOrderBy() {
    final String sql = "SELECT * from SALES.NATION order by n_nationkey";
    sql(sql).withDynamicTable().ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5156">[CALCITE-5156]
   * Support implicit integer types cast for IN Sub-query</a>. */
  @Test void testInSubQueryWithTypeCast() {
    final String sql = "select *\n"
        + "from dept\n"
        + "where cast(deptno + 20 as bigint) in (select deptno from dept)";
    sql(sql).withExpand(false).ok();
  }

  @Test void testInSubQueryWithTypeCast2() {
    final String sql = "select *\n"
        + "from dept\n"
        + "where cast(deptno as bigint) in (select deptno + 20 from dept)";
    sql(sql).withExpand(false).ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1321">[CALCITE-1321]
   * Configurable IN list size when converting IN clause to join</a>. */
  @Test void testInToSemiJoin() {
    final String sql = "SELECT empno\n"
        + "FROM emp AS e\n"
        + "WHERE cast(e.empno as bigint) in (130, 131, 132, 133, 134)";
    // No conversion to join since less than IN-list size threshold 10
    sql(sql).withConfig(b -> b.withInSubQueryThreshold(10))
        .convertsTo("${planNotConverted}");
    // Conversion to join since greater than IN-list size threshold 2
    sql(sql).withConfig(b -> b.withInSubQueryThreshold(2))
        .convertsTo("${planConverted}");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4683">[CALCITE-4683]
   * IN-list converted to JOIN throws type mismatch exception</a>. */
  @Test void testInToSemiJoinWithNewProject() {
    final String sql = "SELECT * FROM (\n"
        + "SELECT '20210101' AS dt, deptno\n"
        + "FROM emp\n"
        + "GROUP BY deptno\n"
        + ") t\n"
        + "WHERE cast(deptno as varchar) in ('1')";
    sql(sql).withConfig(c -> c.withInSubQueryThreshold(0)).ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1944">[CALCITE-1944]
   * Window function applied to sub-query with dynamic star gets wrong
   * plan</a>. */
  @Test void testWindowOnDynamicStar() {
    final String sql = "SELECT SUM(n_nationkey) OVER w\n"
        + "FROM (SELECT * FROM SALES.NATION) subQry\n"
        + "WINDOW w AS (PARTITION BY REGION ORDER BY n_nationkey)";
    sql(sql).withDynamicTable().ok();
  }

  @Test void testWindowAndGroupByWithDynamicStar() {
    final String sql = "SELECT\n"
        + "n_regionkey,\n"
        + "MAX(MIN(n_nationkey)) OVER (PARTITION BY n_regionkey)\n"
        + "FROM (SELECT * FROM SALES.NATION)\n"
        + "GROUP BY n_regionkey";
    final SqlConformance conformance =
        new SqlDelegatingConformance(SqlConformanceEnum.DEFAULT) {
      @Override public boolean isGroupByAlias() {
        return true;
      }
    };
    sql(sql).withConformance(conformance).withDynamicTable().ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2366">[CALCITE-2366]
   * Add support for ANY_VALUE aggregate function</a>. */
  @Test void testAnyValueAggregateFunctionNoGroupBy() {
    final String sql = "SELECT any_value(empno) as anyempno FROM emp AS e";
    sql(sql).ok();
  }

  @Test void testAnyValueAggregateFunctionGroupBy() {
    final String sql = "SELECT any_value(empno) as anyempno FROM emp AS e group by e.sal";
    sql(sql).ok();
  }

  @Test void testSomeAndEveryAggregateFunctions() {
    final String sql = "SELECT some(empno = 130) as someempnoexists,\n"
        + " every(empno > 0) as everyempnogtzero\n"
        + " FROM emp AS e group by e.sal";
    sql(sql).ok();
  }

  @Test void testLarge() {
    // Size factor used to be 400, but lambdas use a lot of stack
    final int x = 300;
    final SqlToRelFixture fixture = fixture();
    SqlValidatorTest.checkLarge(x, input -> {
      final RelRoot root = fixture.withSql(input).toRoot();
      final String s = RelOptUtil.toString(root.project());
      assertThat(s, notNullValue());
    });
  }

  @Test void testUnionInFrom() {
    final String sql = "select x0, x1 from (\n"
        + "  select 'a' as x0, 'a' as x1, 'a' as x2 from emp\n"
        + "  union all\n"
        + "  select 'bb' as x0, 'bb' as x1, 'bb' as x2 from dept)";
    sql(sql).ok();
  }

  @Test void testPivot() {
    final String sql = "SELECT *\n"
        + "FROM (SELECT mgr, deptno, job, sal FROM emp)\n"
        + "PIVOT (SUM(sal) AS ss, COUNT(*)\n"
        + "    FOR (job, deptno)\n"
        + "    IN (('CLERK', 10) AS c10, ('MANAGER', 20) AS m20))";
    sql(sql).ok();
  }

  @Test void testPivot2() {
    final String sql = "SELECT *\n"
        + "FROM   (SELECT deptno, job, sal\n"
        + "        FROM   emp)\n"
        + "PIVOT  (SUM(sal) AS sum_sal, COUNT(*) AS \"COUNT\"\n"
        + "        FOR (job) IN ('CLERK', 'MANAGER' mgr, 'ANALYST' AS \"a\"))\n"
        + "ORDER BY deptno";
    sql(sql).ok();
  }

  @Test void testUnpivot() {
    final String sql = "SELECT * FROM emp\n"
        + "UNPIVOT INCLUDE NULLS (remuneration\n"
        + "  FOR remuneration_type IN (comm AS 'commission',\n"
        + "                            sal as 'salary'))";
    sql(sql).ok();
  }

  @Test void testMatchRecognize1() {
    final String sql = "select *\n"
        + "  from emp match_recognize\n"
        + "  (\n"
        + "    partition by job, sal\n"
        + "    order by job asc, sal desc, empno\n"
        + "    pattern (strt down+ up+)\n"
        + "    define\n"
        + "      down as down.mgr < PREV(down.mgr),\n"
        + "      up as up.mgr > prev(up.mgr)) as mr";
    sql(sql).ok();
  }

  @Test void testMatchRecognizeMeasures1() {
    final String sql = "select *\n"
        + "from emp match_recognize (\n"
        + "  partition by job, sal\n"
        + "  order by job asc, sal desc\n"
        + "  measures MATCH_NUMBER() as match_num,\n"
        + "    CLASSIFIER() as var_match,\n"
        + "    STRT.mgr as start_nw,\n"
        + "    LAST(DOWN.mgr) as bottom_nw,\n"
        + "    LAST(up.mgr) as end_nw\n"
        + "  pattern (strt down+ up+)\n"
        + "  define\n"
        + "    down as down.mgr < PREV(down.mgr),\n"
        + "    up as up.mgr > prev(up.mgr)) as mr";
    sql(sql).ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1909">[CALCITE-1909]
   * Output rowType of Match should include PARTITION BY and ORDER BY
   * columns</a>. */
  @Test void testMatchRecognizeMeasures2() {
    final String sql = "select *\n"
        + "from emp match_recognize (\n"
        + "  partition by job\n"
        + "  order by sal\n"
        + "  measures MATCH_NUMBER() as match_num,\n"
        + "    CLASSIFIER() as var_match,\n"
        + "    STRT.mgr as start_nw,\n"
        + "    LAST(DOWN.mgr) as bottom_nw,\n"
        + "    LAST(up.mgr) as end_nw\n"
        + "  pattern (strt down+ up+)\n"
        + "  define\n"
        + "    down as down.mgr < PREV(down.mgr),\n"
        + "    up as up.mgr > prev(up.mgr)) as mr";
    sql(sql).ok();
  }

  @Test void testMatchRecognizeMeasures3() {
    final String sql = "select *\n"
        + "from emp match_recognize (\n"
        + "  partition by job\n"
        + "  order by sal\n"
        + "  measures MATCH_NUMBER() as match_num,\n"
        + "    CLASSIFIER() as var_match,\n"
        + "    STRT.mgr as start_nw,\n"
        + "    LAST(DOWN.mgr) as bottom_nw,\n"
        + "    LAST(up.mgr) as end_nw\n"
        + "  ALL ROWS PER MATCH\n"
        + "  pattern (strt down+ up+)\n"
        + "  define\n"
        + "    down as down.mgr < PREV(down.mgr),\n"
        + "    up as up.mgr > prev(up.mgr)) as mr";
    sql(sql).ok();
  }

  @Test void testMatchRecognizePatternSkip1() {
    final String sql = "select *\n"
        + "  from emp match_recognize\n"
        + "  (\n"
        + "    after match skip to next row\n"
        + "    pattern (strt down+ up+)\n"
        + "    define\n"
        + "      down as down.mgr < PREV(down.mgr),\n"
        + "      up as up.mgr > NEXT(up.mgr)\n"
        + "  ) mr";
    sql(sql).ok();
  }

  @Test void testMatchRecognizeSubset1() {
    final String sql = "select *\n"
        + "  from emp match_recognize\n"
        + "  (\n"
        + "    after match skip to down\n"
        + "    pattern (strt down+ up+)\n"
        + "    subset stdn = (strt, down)\n"
        + "    define\n"
        + "      down as down.mgr < PREV(down.mgr),\n"
        + "      up as up.mgr > NEXT(up.mgr)\n"
        + "  ) mr";
    sql(sql).ok();
  }

  @Test void testMatchRecognizePrevLast() {
    final String sql = "SELECT *\n"
        + "FROM emp\n"
        + "MATCH_RECOGNIZE (\n"
        + "  MEASURES\n"
        + "    STRT.mgr AS start_mgr,\n"
        + "    LAST(DOWN.mgr) AS bottom_mgr,\n"
        + "    LAST(UP.mgr) AS end_mgr\n"
        + "  ONE ROW PER MATCH\n"
        + "  PATTERN (STRT DOWN+ UP+)\n"
        + "  DEFINE\n"
        + "    DOWN AS DOWN.mgr < PREV(DOWN.mgr),\n"
        + "    UP AS UP.mgr > PREV(LAST(DOWN.mgr, 1), 1)\n"
        + ") AS T";
    sql(sql).ok();
  }

  @Test void testMatchRecognizePrevDown() {
    final String sql = "SELECT *\n"
        + "FROM emp\n"
        + "MATCH_RECOGNIZE (\n"
        + "  MEASURES\n"
        + "    STRT.mgr AS start_mgr,\n"
        + "    LAST(DOWN.mgr) AS up_days,\n"
        + "    LAST(UP.mgr) AS total_days\n"
        + "  PATTERN (STRT DOWN+ UP+)\n"
        + "  DEFINE\n"
        + "    DOWN AS DOWN.mgr < PREV(DOWN.mgr),\n"
        + "    UP AS UP.mgr > PREV(DOWN.mgr)\n"
        + ") AS T";
    sql(sql).ok();
  }

  @Test void testPrevClassifier() {
    final String sql = "SELECT *\n"
        + "FROM emp\n"
        + "MATCH_RECOGNIZE (\n"
        + "  MEASURES\n"
        + "    STRT.mgr AS start_mgr,\n"
        + "    LAST(DOWN.mgr) AS up_days,\n"
        + "    LAST(UP.mgr) AS total_days\n"
        + "  PATTERN (STRT DOWN? UP+)\n"
        + "  DEFINE\n"
        + "    DOWN AS DOWN.mgr < PREV(DOWN.mgr),\n"
        + "    UP AS CASE\n"
        + "            WHEN PREV(CLASSIFIER()) = 'STRT'\n"
        + "              THEN UP.mgr > 15\n"
        + "            ELSE\n"
        + "              UP.mgr > 20\n"
        + "            END\n"
        + ") AS T";
    sql(sql).ok();
  }

  @Test void testMatchRecognizeIn() {
    final String sql = "select *\n"
        + "  from emp match_recognize\n"
        + "  (\n"
        + "    partition by job, sal\n"
        + "    order by job asc, sal desc, empno\n"
        + "    pattern (strt down+ up+)\n"
        + "    define\n"
        + "      down as down.mgr in (0, 1),\n"
        + "      up as up.mgr > prev(up.mgr)) as mr";
    sql(sql).ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2323">[CALCITE-2323]
   * Validator should allow alternative nullCollations for ORDER BY in
   * OVER</a>. */
  @Test void testUserDefinedOrderByOverLow() {
    checkUserDefinedOrderByOver(NullCollation.LOW);
  }

  @Test void testUserDefinedOrderByOverHigh() {
    checkUserDefinedOrderByOver(NullCollation.HIGH);
  }

  @Test void testUserDefinedOrderByOverFirst() {
    checkUserDefinedOrderByOver(NullCollation.FIRST);
  }

  @Test void testUserDefinedOrderByOverLast() {
    checkUserDefinedOrderByOver(NullCollation.LAST);
  }

  void checkUserDefinedOrderByOver(NullCollation nullCollation) {
    String sql = "select deptno,\n"
        + "  rank() over (partition by empno order by comm desc)\n"
        + "from emp\n"
        + "order by row_number() over (partition by empno order by comm)";
    Properties properties = new Properties();
    properties.setProperty(
        CalciteConnectionProperty.DEFAULT_NULL_COLLATION.camelName(),
        nullCollation.name());
    CalciteConnectionConfigImpl connectionConfig =
        new CalciteConnectionConfigImpl(properties);
    sql(sql)
        .withDecorrelate(false)
        .withTrim(false)
        .withFactory(f ->
                f.withValidatorConfig(c ->
                    c.withDefaultNullCollation(
                        connectionConfig.defaultNullCollation())))
        .ok();
  }

  @Test void testJsonValueExpressionOperator() {
    final String sql = "select ename format json,\n"
        + "ename format json encoding utf8,\n"
        + "ename format json encoding utf16,\n"
        + "ename format json encoding utf32\n"
        + "from emp";
    sql(sql).ok();
  }

  @Test void testJsonExists() {
    final String sql = "select json_exists(ename, 'lax $')\n"
        + "from emp";
    sql(sql).ok();
  }

  @Test void testJsonValue() {
    final String sql = "select json_value(ename, 'lax $')\n"
        + "from emp";
    sql(sql).ok();
  }

  @Test void testJsonQuery() {
    final String sql = "select json_query(ename, 'lax $')\n"
        + "from emp";
    sql(sql).ok();
  }

  @Test void testJsonType() {
    final String sql = "select json_type(ename)\n"
        + "from emp";
    sql(sql).ok();
  }

  @Test void testJsonPretty() {
    final String sql = "select json_pretty(ename)\n"
        + "from emp";
    sql(sql).ok();
  }

  @Test void testJsonDepth() {
    final String sql = "select json_depth(ename)\n"
        + "from emp";
    sql(sql).ok();
  }

  @Test void testJsonLength() {
    final String sql = "select json_length(ename, 'strict $')\n"
        + "from emp";
    sql(sql).ok();
  }

  @Test void testJsonKeys() {
    final String sql = "select json_keys(ename, 'strict $')\n"
        + "from emp";
    sql(sql).ok();
  }

  @Test void testJsonArray() {
    final String sql = "select json_array(ename, ename)\n"
        + "from emp";
    sql(sql).ok();
  }

  @Test void testJsonArrayAgg1() {
    final String sql = "select json_arrayagg(ename)\n"
        + "from emp";
    sql(sql).ok();
  }

  @Test void testJsonArrayAgg2() {
    final String sql = "select json_arrayagg(ename order by ename)\n"
        + "from emp";
    sql(sql).ok();
  }

  @Test void testJsonArrayAgg3() {
    final String sql = "select json_arrayagg(ename order by ename null on null)\n"
        + "from emp";
    sql(sql).ok();
  }

  @Test void testJsonArrayAgg4() {
    final String sql = "select json_arrayagg(ename null on null) within group (order by ename)\n"
        + "from emp";
    sql(sql).ok();
  }

  @Test void testJsonObject() {
    final String sql = "select json_object(ename: deptno, ename: deptno)\n"
        + "from emp";
    sql(sql).ok();
  }

  @Test void testJsonObjectAgg() {
    final String sql = "select json_objectagg(ename: deptno)\n"
        + "from emp";
    sql(sql).ok();
  }

  @Test void testJsonPredicate() {
    final String sql = "select\n"
        + "ename is json,\n"
        + "ename is json value,\n"
        + "ename is json object,\n"
        + "ename is json array,\n"
        + "ename is json scalar,\n"
        + "ename is not json,\n"
        + "ename is not json value,\n"
        + "ename is not json object,\n"
        + "ename is not json array,\n"
        + "ename is not json scalar\n"
        + "from emp";
    sql(sql).ok();
  }

  @Test void testJsonNestedJsonObjectConstructor() {
    final String sql = "select\n"
        + "json_object(\n"
        + "  'key1' :\n"
        + "  json_object(\n"
        + "    'key2' :\n"
        + "    ename)),\n"
        + "  json_object(\n"
        + "    'key3' :\n"
        + "    json_array(12, 'hello', deptno))\n"
        + "from emp";
    sql(sql).ok();
  }

  @Test void testJsonNestedJsonArrayConstructor() {
    final String sql = "select\n"
        + "json_array(\n"
        + "  json_object(\n"
        + "    'key1' :\n"
        + "    json_object(\n"
        + "      'key2' :\n"
        + "       ename)),\n"
        + "  json_array(12, 'hello', deptno))\n"
        + "from emp";
    sql(sql).ok();
  }

  @Test void testJsonNestedJsonObjectAggConstructor() {
    final String sql = "select\n"
        + "json_object(\n"
        + "  'k2' :\n"
        + "  json_objectagg(\n"
        + "    ename :\n"
        + "    json_object(\n"
        + "      'k1' :\n"
        + "      deptno)))\n"
        + "from emp";
    sql(sql).ok();
  }

  @Test void testJsonNestedJsonArrayAggConstructor() {
    final String sql = "select\n"
        + "json_object(\n"
        + "  'k2' :\n"
        + "  json_arrayagg(\n"
        + "    json_object(\n"
        + "      ename :\n"
        + "      deptno)))\n"
        + "from emp";
    sql(sql).ok();
  }

  @Test void testWithinGroup1() {
    final String sql = "select deptno,\n"
        + " collect(empno) within group (order by deptno, hiredate desc)\n"
        + "from emp\n"
        + "group by deptno";
    sql(sql).ok();
  }

  @Test void testWithinGroup2() {
    final String sql = "select dept.deptno,\n"
        + " collect(sal) within group (order by sal desc) as s,\n"
        + " collect(sal) within group (order by 1)as s1,\n"
        + " collect(sal) within group (order by sal)\n"
        + "  filter (where sal > 2000) as s2\n"
        + "from emp\n"
        + "join dept using (deptno)\n"
        + "group by dept.deptno";
    sql(sql).ok();
  }

  @Test void testWithinGroup3() {
    final String sql = "select deptno,\n"
        + " collect(empno) within group (order by empno not in (1, 2)), count(*)\n"
        + "from emp\n"
        + "group by deptno";
    sql(sql).ok();
  }

  @Test void testArgMinFunction() {
    final String sql = "select arg_min(ename, deptno)\n"
        + "from emp";
    sql(sql).withTrim(true).ok();
  }

  @Test void testArgMinFunctionWithWinAgg() {
    final String sql = "select job,\n"
        + "  arg_min(ename, deptno) over (partition by job order by sal)\n"
        + "from emp";
    sql(sql).withTrim(true).ok();
  }

  @Test void testArgMaxFunction() {
    final String sql = "select arg_max(ename, deptno)\n"
        + "from emp";
    sql(sql).withTrim(true).ok();
  }

  @Test void testArgMaxFunctionWithWinAgg() {
    final String sql = "select job,\n"
        + "  arg_max(ename, deptno) over (partition by job order by sal)\n"
        + "from emp";
    sql(sql).withTrim(true).ok();
  }

  @Test void testModeFunction() {
    final String sql = "select mode(deptno)\n"
        + "from emp";
    sql(sql).withTrim(true).ok();
  }

  @Test void testModeFunctionWithWinAgg() {
    final String sql = "select deptno, ename,\n"
        + "  mode(job) over (partition by deptno order by ename)\n"
        + "from emp";
    sql(sql).withTrim(true).ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4644">[CALCITE-4644]
   * Add PERCENTILE_CONT and PERCENTILE_DISC aggregate functions</a>. */
  @Test void testPercentileCont() {
    final String sql = "select\n"
        + " percentile_cont(0.25) within group (order by deptno)\n"
        + "from emp";
    sql(sql).ok();
  }

  @Test void testPercentileContWithGroupBy() {
    final String sql = "select deptno,\n"
        + " percentile_cont(0.25) within group (order by empno desc)\n"
        + "from emp\n"
        + "group by deptno";
    sql(sql).ok();
  }

  @Test void testPercentileDisc() {
    final String sql = "select\n"
        + " percentile_disc(0.25) within group (order by deptno)\n"
        + "from emp";
    sql(sql).ok();
  }

  @Test void testPercentileDiscWithGroupBy() {
    final String sql = "select deptno,\n"
        + " percentile_disc(0.25) within group (order by empno)\n"
        + "from emp\n"
        + "group by deptno";
    sql(sql).ok();
  }

  @Test void testOrderByRemoval1() {
    final String sql = "select * from (\n"
        + "  select empno from emp order by deptno offset 0) t\n"
        + "order by empno desc";
    sql(sql).ok();
  }

  @Test void testOrderByRemoval2() {
    final String sql = "select * from (\n"
        + "  select empno from emp order by deptno offset 1) t\n"
        + "order by empno desc";
    sql(sql).ok();
  }

  @Test void testOrderByRemoval3() {
    final String sql = "select * from (\n"
        + "  select empno from emp order by deptno limit 10) t\n"
        + "order by empno";
    sql(sql).ok();
  }

  /** Tests LEFT JOIN LATERAL with USING. */
  @Test void testLeftJoinLateral1() {
    final String sql = "select * from (values 4) as t(c)\n"
        + " left join lateral\n"
        + " (select c,a*c from (values 2) as s(a)) as r(d,c)\n"
        + " using(c)";
    sql(sql).ok();
  }

  /** Tests LEFT JOIN LATERAL with NATURAL JOIN. */
  @Test void testLeftJoinLateral2() {
    final String sql = "select * from (values 4) as t(c)\n"
        + " natural left join lateral\n"
        + " (select c,a*c from (values 2) as s(a)) as r(d,c)";
    sql(sql).ok();
  }

  /** Tests LEFT JOIN LATERAL with ON condition. */
  @Test void testLeftJoinLateral3() {
    final String sql = "select * from (values 4) as t(c)\n"
        + " left join lateral\n"
        + " (select c,a*c from (values 2) as s(a)) as r(d,c)\n"
        + " on t.c=r.c";
    sql(sql).ok();
  }

  /** Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-6413">[CALCITE-6413]
   * SqlValidator does not invoke TypeCoercionImpl::binaryComparisonCoercion for both NATURAL
   * and USING join conditions</a>. */
  @Test void testNaturalJoinCast() {
    final String sql = "WITH t1(x) AS (VALUES('x')), t2(x) AS (VALUES(0.0))\n"
        + "SELECT * FROM t1 NATURAL JOIN t2";
    sql(sql).ok();
  }

  /** Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-6885">[CALCITE-6885]
   * SqlToRelConverter#convertUsing should not fail if commonTypeForBinaryComparison
   * returns null</a>. */
  @Test void testNaturalJoinCastNoCoercion() {
    final String sql = "WITH t1(x) AS (VALUES('x')), t2(x) AS (VALUES(0.0))\n"
        + "SELECT * FROM t1 NATURAL JOIN t2";
    sql(sql)
        // Default factory, except for the TypeCoercion
        .withFactory(f ->
            f
                .withValidator((opTab, catalogReader, typeFactory, config) ->
                    SqlValidatorUtil.newValidator(opTab, catalogReader, typeFactory,
                        config.withIdentifierExpansion(true)
                            // Ad-hoc coercion that returns null for commonTypeForBinaryComparison
                            .withTypeCoercionFactory((t, v) -> new TypeCoercionImpl(t, v) {
                              @Override public @Nullable RelDataType commonTypeForBinaryComparison(
                                  @Nullable RelDataType type1, @Nullable RelDataType type2) {
                                return null;
                              }
                            })))
                .withSqlToRelConfig(c ->
                    c.withTrimUnusedFields(true).withExpand(true)
                        .addRelBuilderConfigTransform(b ->
                            b.withAggregateUnique(true).withPruneInputOfAggregate(false))))
        .ok();
  }

  /** Tests LEFT JOIN LATERAL with multiple columns from outer. */
  @Test void testLeftJoinLateral4() {
    final String sql = "select * from (values (4,5)) as t(c,d)\n"
        + " left join lateral\n"
        + " (select c,a*c from (values 2) as s(a)) as r(d,c)\n"
        + " on t.c+t.d=r.c";
    sql(sql).ok();
  }

  /** Tests LEFT JOIN LATERAL with correlating variable coming
   * from one level up join scope. */
  @Test void testLeftJoinLateral5() {
    final String sql = "select * from (values 4) as t (c)\n"
        + "left join lateral\n"
        + "  (select f1+b1 from (values 2) as foo(f1)\n"
        + "    join\n"
        + "  (select c+1 from (values 3)) as bar(b1)\n"
        + "  on f1=b1)\n"
        + "as r(n) on c=n";
    sql(sql).ok();
  }

  /** Tests CROSS JOIN LATERAL with multiple columns from outer. */
  @Test void testCrossJoinLateral1() {
    final String sql = "select * from (values (4,5)) as t(c,d)\n"
        + " cross join lateral\n"
        + " (select c,a*c as f from (values 2) as s(a)\n"
        + " where c+d=a*c)";
    sql(sql).ok();
  }

  /** Tests CROSS JOIN LATERAL with correlating variable coming
   * from one level up join scope. */
  @Test void testCrossJoinLateral2() {
    final String sql = "select * from (values 4) as t (c)\n"
        + "cross join lateral\n"
        + "(select * from (\n"
        + "  select f1+b1 from (values 2) as foo(f1)\n"
        + "    join\n"
        + "  (select c+1 from (values 3)) as bar(b1)\n"
        + "  on f1=b1\n"
        + ") as r(n) where c=n)";
    sql(sql).ok();
  }

  @Test void testWithinDistinct1() {
    final String sql = "select avg(empno) within distinct (deptno)\n"
        + "from emp";
    sql(sql).ok();
  }

  /** Tests a reference to a measure column in an underlying table. The measure
   * is treated as a black box: it is not expanded, just wrapped in a call to
   * AGGREGATE. */
  @Test void testMeasureRef() {
    final String sql = "select deptno, aggregate(count_plus_100) as c\n"
        + "from empm\n"
        + "group by deptno";
    fixture()
        .withFactory(c ->
            c.withOperatorTable(t ->
                SqlValidatorTest.operatorTableFor(SqlLibrary.CALCITE)))
        .withCatalogReader(MockCatalogReaderExtended::create)
        .withSql(sql)
        .ok();
  }

  /** A query that references a measure that does not contain any aggregate
   * functions. The measure is fully expanded in the plan. */
  @Test void testMeasure1() {
    final String sql = "select * from (\n"
        + "  select deptno,\n"
        + "    empno + 1 as measure e1,\n"
        + "    e1 + deptno as measure e2\n"
        + "  from emp)";
    sql(sql).ok();
  }

  /** As {@link #testMeasure1()} but references a non-measure. */
  @Test void testMeasure2() {
    final String sql = "select * from (\n"
        + "  select deptno,\n"
        + "    empno + 1 as e1,\n"
        + "    e1 + deptno as measure e2\n"
        + "  from emp)";
    sql(sql).ok();
  }

  /** As {@link #testMeasure1()} but uses an aggregate measure. The plan
   * contains a call to {@code AGG_M2V} on top of a call to {@code V2M}. */
  @Test void testMeasure3() {
    final String sql = "select deptno, count_plus_10, min(job) as min_job\n"
        + "from (\n"
        + "  select deptno,\n"
        + "    job,\n"
        + "    count(*) + 10 as measure count_plus_10,\n"
        + "    count_plus_10 + deptno as measure e2\n"
        + "  from emp)\n"
        + "group by deptno";
    sql(sql).ok();
  }

  /** As {@link #testMeasure3()} but no {@code GROUP BY}.
   * The measure is expanded to {@code OVER}. */
  @Test void testMeasure3b() {
    final String sql = "select deptno, count_plus_10\n"
        + "from (\n"
        + "  select deptno,\n"
        + "    job,\n"
        + "    count(*) + 10 as measure count_plus_10,\n"
        + "    count_plus_10 + deptno as measure e2\n"
        + "  from emp)";
    sql(sql).ok();
  }

  /** Measures defined in the outermost query are converted to values. */
  @Test void testMeasure4() {
    final String sql = "select deptno, count(*) as measure c,\n"
        + "  t.uno as measure uno, 2 as measure two\n"
        + "from (select deptno, job, 1 as measure uno from emp) as t";
    sql(sql).ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6343">[CALCITE-6343]
   * Ensure that AS operator doesn't change return type of measures</a>. */
  @Test void testMeasureRefWithAlias() {
    final String sql = "select count_plus_100 as c\n"
        + "from empm";
    fixture()
        .withFactory(c ->
            c.withOperatorTable(t ->
              SqlValidatorTest.operatorTableFor(SqlLibrary.CALCITE)))
        .withCatalogReader(MockCatalogReaderExtended::create)
        .withSql(sql)
        .ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6013">[CALCITE-6013]
   * Unnecessary measures added as projects during rel construction</a>. */
  @Test void testAvoidUnnecessaryMeasureProject() {
    final String sql = "select deptno\n"
        + "from empm\n"
        + "group by deptno";
    fixture()
        .withFactory(c ->
            c.withOperatorTable(t ->
                SqlValidatorTest.operatorTableFor(SqlLibrary.CALCITE)))
        .withCatalogReader(MockCatalogReaderExtended::create)
        .withSql(sql)
        .ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3310">[CALCITE-3310]
   * Approximate and exact aggregate calls are recognized as the same
   * during sql-to-rel conversion</a>. */
  @Test void testProjectApproximateAndExactAggregates() {
    final String sql = "SELECT empno, count(distinct ename),\n"
            + "approx_count_distinct(ename)\n"
            + "FROM emp\n"
            + "GROUP BY empno";
    sql(sql).ok();
  }

  @Test void testProjectInSubQueryWithIsTruePredicate() {
    final String sql = "select deptno in (select deptno from empnullables) is true\n"
        + "from empnullables";
    sql(sql).withExpand(false).ok();
  }

  @Test void testProjectAggregatesIgnoreNullsAndNot() {
    final String sql = "select lead(sal, 4) IGNORE NULLS, lead(sal, 4) over (w)\n"
        + "from emp window w as (order by empno)";
    sql(sql).ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3456">[CALCITE-3456]
   * AssertionError throws when aggregation same digest in sub-query in same
   * scope</a>. */
  @Test void testAggregateWithSameDigestInSubQueries() {
    final String sql = "select\n"
        + "  CASE WHEN job IN ('810000', '820000') THEN job\n"
        + "  ELSE 'error'\n"
        + "  END AS job_name,\n"
        + "  count(empno)\n"
        + "FROM emp\n"
        + "where job <> '' or job IN ('810000', '820000')\n"
        + "GROUP by deptno, job";
    sql(sql)
        .withConfig(c ->
            c.addRelBuilderConfigTransform(c2 ->
                c2.withPruneInputOfAggregate(false)))
        .ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3575">[CALCITE-3575]
   * IndexOutOfBoundsException when converting SQL to rel</a>. */
  @Test void testPushDownJoinConditionWithProjectMerge() {
    final String sql = "select * from\n"
        + " (select empno, deptno from emp) a\n"
        + " join dept b\n"
        + "on a.deptno + 20 = b.deptno";
    sql(sql).ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2997">[CALCITE-2997]
   * Avoid pushing down join condition in SqlToRelConverter</a>. */
  @Test void testDoNotPushDownJoinCondition() {
    final String sql = "select *\n"
        + "from emp as e\n"
        + "join dept as d on e.deptno + 20 = d.deptno / 2";
    sql(sql).withConfig(c ->
        c.addRelBuilderConfigTransform(b ->
            b.withPushJoinCondition(false)))
        .ok();
  }

  /** As {@link #testDoNotPushDownJoinCondition()}. */
  @Test void testPushDownJoinCondition() {
    final String sql = "select *\n"
        + "from emp as e\n"
        + "join dept as d on e.deptno + 20 = d.deptno / 2";
    sql(sql).ok();
  }

  @Test void testCoalesceOnNullableField() {
    final String sql = "select coalesce(mgr, 0) from emp";
    sql(sql).ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7044">[CALCITE-7044]
   * Add internal operator CAST NOT NULL to enhance rewrite COALESCE operator</a>. */
  @Test void testCoalesceSubquery() {
    final String sql = "SELECT"
        + "  deptno, "
        + "  coalesce((select sum(empno) from emp "
        + "  where deptno = emp.deptno limit 1), 0) as w "
        + "FROM dept";
    sql(sql).ok();
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4145">[CALCITE-4145]
   * Exception when query from UDF field with structured type</a>.
   */
  @Test void testUdfWithStructuredReturnType() {
    final String sql = "SELECT deptno, tmp.r.f0, tmp.r.f1 FROM\n"
        + "(SELECT deptno, STRUCTURED_FUNC() AS r from dept)tmp";
    sql(sql).ok();
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3826">[CALCITE-3826]
   * UPDATE assigns wrong type to bind variables</a>.
   */
  @Test void testDynamicParamTypesInUpdate() {
    RelNode rel =
        sql("update emp set sal = ?, ename = ? where empno = ?").toRel();
    LogicalTableModify modify = (LogicalTableModify) rel;
    List<RexNode> parameters = modify.getSourceExpressionList();
    assertThat(parameters, notNullValue());
    assertThat(parameters, hasSize(2));
    assertThat(parameters.get(0).getType().getSqlTypeName(), is(SqlTypeName.INTEGER));
    assertThat(parameters.get(1).getType().getSqlTypeName(), is(SqlTypeName.VARCHAR));
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4167">[CALCITE-4167]
   * Group by COALESCE IN throws NullPointerException</a>.
   */
  @Test void testGroupByCoalesceIn() {
    final String sql = "select case when coalesce(ename, 'a') in ('1', '2')\n"
        + "then 'CKA' else 'QT' END, count(distinct deptno) from emp\n"
        + "group by case when coalesce(ename, 'a') in ('1', '2') then 'CKA' else 'QT' END";
    sql(sql).ok();
  }

  @Test void testSortInSubQuery() {
    final String sql = "select * from (select empno from emp order by empno)";
    sql(sql).convertsTo("${planRemoveSort}");
    sql(sql).withConfig(c -> c.withRemoveSortInSubQuery(false)).convertsTo("${planKeepSort}");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6759">[CALCITE-6759]
   * SqlToRelConverter should not remove ORDER BY in subquery if it has an
   * OFFSET</a>.
   *
   * <p>While an ORDER BY on its own can be ignored, an ORDER BY with an OFFSET
   * or FETCH cannot be removed from the subquery without changing the
   * semantics. */
  @Test void testSortWithOffsetInSubQuery() {
    final String sql = "select count(*) from (\n"
        + "  select *\n"
        + "  from emp\n"
        + "  order by empno offset 10)";
    sql(sql).ok();
  }

  @Test void testTrimUnionAll() {
    final String sql = ""
        + "select deptno from\n"
        + "(select ename, deptno from emp\n"
        + "union all\n"
        + "select name, deptno from dept)";
    sql(sql).withTrim(true).ok();
  }

  @Test void testTrimUnionDistinct() {
    final String sql = ""
        + "select deptno from\n"
        + "(select ename, deptno from emp\n"
        + "union\n"
        + "select name, deptno from dept)";
    sql(sql).withTrim(true).ok();
  }

  @Test void testTrimIntersectAll() {
    final String sql = ""
        + "select deptno from\n"
        + "(select ename, deptno from emp\n"
        + "intersect all\n"
        + "select name, deptno from dept)";
    sql(sql).withTrim(true).ok();
  }

  @Test void testTrimIntersectDistinct() {
    final String sql = ""
        + "select deptno from\n"
        + "(select ename, deptno from emp\n"
        + "intersect\n"
        + "select name, deptno from dept)";
    sql(sql).withTrim(true).ok();
  }

  @Test void testTrimExceptAll() {
    final String sql = ""
        + "select deptno from\n"
        + "(select ename, deptno from emp\n"
        + "except all\n"
        + "select name, deptno from dept)";
    sql(sql).withTrim(true).ok();
  }

  @Test void testTrimExceptDistinct() {
    final String sql = ""
        + "select deptno from\n"
        + "(select ename, deptno from emp\n"
        + "except\n"
        + "select name, deptno from dept)";
    sql(sql).withTrim(true).ok();
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6199">[CALCITE-6199]
   * Trim unused fields for SNAPSHOT and SAMPLE if table has VIRTUAL column</a>.
   */
  @Test void testTrimSnapshotOnTemporalTable1() {
    // Test temporal table with virtual columns.
    final String sql = "select D, E from VIRTUALCOLUMNS.VC_T1 "
        + "for system_time as of TIMESTAMP '2011-01-02 00:00:00'";
    sql(sql).withExtendedTester().withTrim(true).ok();
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6199">[CALCITE-6199]
   * Trim unused fields for SNAPSHOT and SAMPLE if table has VIRTUAL column</a>.
   */
  @Test void testTrimSnapshotOnTemporalTable2() {
    // Test temporal table with virtual columns.
    final String sql = "select * from VIRTUALCOLUMNS.VC_T1 "
        + "for system_time as of TIMESTAMP '2011-01-02 00:00:00'";
    sql(sql).withExtendedTester().withTrim(true).ok();
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6199">[CALCITE-6199]
   * Trim unused fields for SNAPSHOT and SAMPLE if table has VIRTUAL column</a>.
   */
  @Test void testTrimSampleOnTemporalTable1() {
    // Test temporal table with virtual columns.
    final String sql = "select D, E from VIRTUALCOLUMNS.VC_T1 "
        + " tablesample bernoulli(50)";
    sql(sql).withExtendedTester().withTrim(true).ok();
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6199">[CALCITE-6199]
   * Trim unused fields for SNAPSHOT and SAMPLE if table has VIRTUAL column</a>.
   */
  @Test void testTrimSampleOnTemporalTable2() {
    // Test temporal table with virtual columns.
    final String sql = "select * from VIRTUALCOLUMNS.VC_T1 "
        + " tablesample bernoulli(50)";
    sql(sql).withExtendedTester().withTrim(true).ok();
  }

  @Test void testJoinWithOnConditionQuery() {
    String sql = ""
        + "SELECT emp.deptno, emp.sal\n"
        + "FROM dept\n"
        + "JOIN emp\n"
        + "ON (SELECT AVG(emp.sal) > 0 FROM emp)";
    sql(sql).ok();
  }

  @Test void testJoinExpandAndDecorrelation() {
    String sql = ""
        + "SELECT emp.deptno, emp.sal\n"
        + "FROM dept\n"
        + "JOIN emp ON emp.deptno = dept.deptno AND emp.sal < (\n"
        + "  SELECT AVG(emp.sal)\n"
        + "  FROM emp\n"
        + "  WHERE  emp.deptno = dept.deptno\n"
        + ")";
    sql(sql)
        .withConfig(configBuilder -> configBuilder
            .withExpand(true)
            .withDecorrelationEnabled(true))
        .convertsTo("${planExpanded}");
    sql(sql)
        .withConfig(configBuilder -> configBuilder
            .withExpand(false)
            .withDecorrelationEnabled(false))
        .convertsTo("${planNotExpanded}");
  }

  @Test void testImplicitJoinExpandAndDecorrelation() {
    String sql = ""
        + "SELECT emp.deptno, emp.sal\n"
        + "FROM dept, emp "
        + "WHERE emp.deptno = dept.deptno AND emp.sal < (\n"
        + "  SELECT AVG(emp.sal)\n"
        + "  FROM emp\n"
        + "  WHERE  emp.deptno = dept.deptno\n"
        + ")";
    sql(sql).withExpand(true).withDecorrelate(true)
        .convertsTo("${planExpanded}");
    sql(sql).withExpand(false).withDecorrelate(false)
        .convertsTo("${planNotExpanded}");
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4295">[CALCITE-4295]
   * Composite of two checker with SqlOperandCountRange throws IllegalArgumentException</a>.
   */
  @Test void testCompositeOfCountRange() {
    final String sql = ""
        + "select COMPOSITE(deptno)\n"
        + "from dept";
    sql(sql).withTrim(true).ok();
  }

  @Test void testInWithConstantList() {
    String expr = "1 in (1,2,3)";
    expr(expr).ok();
  }

  @Test void testFunctionExprInOver() {
    String sql = "select ename, row_number() over(partition by char_length(ename)\n"
        + " order by deptno desc) as rn\n"
        + "from emp\n"
        + "where deptno = 10";
    sql(sql)
        .withFactory(t ->
            t.withValidatorConfig(config ->
                config.withIdentifierExpansion(false)))
        .withTrim(false)
        .ok();
  }

  @Test void testNestedWindowAggWithIdentifierExpansionDisabled() {
    String sql = "select sum(sum(sal)) over() from emp";
    sql(sql)
        .withFactory(f ->
            f.withValidator((opTab, catalogReader, typeFactory, config)
                -> SqlValidatorUtil.newValidator(opTab, catalogReader,
                typeFactory, config.withIdentifierExpansion(false))))
        .withTrim(false)
        .ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6214">[CALCITE-6214]
   * Remove DISTINCT in COUNT if field is unique</a>. */
  @Test void testRemoveDistinctIfUnique1() {
    final String sql = "SELECT\n"
        + "    deptno,\n"
        + "    COUNT(DISTINCT sal) as cds,\n"
        + "    COUNT(sal) as cs,\n"
        + "    SUM(DISTINCT sal) AS sds,\n"
        + "    SUM(sal) AS ss\n"
        + "FROM (\n"
        + "    SELECT DISTINCT deptno, sal\n"
        + "    FROM emp)\n"
        + "GROUP BY deptno";
    sql(sql)
        .withConfig(c ->
            c.addRelBuilderConfigTransform(c2 ->
                c2.withRemoveRedundantDistinct(true))).ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6214">[CALCITE-6214]
   * Remove DISTINCT in COUNT if field is unique</a>. */
  @Test void testRemoveDistinctIfUnique2() {
    final String sql = "SELECT\n"
        + "    COUNT(DISTINCT sal) as cds,\n"
        + "    COUNT(sal) as cs,\n"
        + "    SUM(DISTINCT sal) AS sds,\n"
        + "    SUM(sal) AS ss\n"
        + "FROM (\n"
        + "    SELECT deptno, 1 as sal\n"
        + "    FROM emp"
        + "    GROUP BY deptno)"
        + "GROUP BY deptno\n";
    sql(sql)
        .withConfig(c ->
            c.addRelBuilderConfigTransform(c2 ->
                c2.withRemoveRedundantDistinct(true))).ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6214">[CALCITE-6214]
   * Remove DISTINCT in COUNT if field is unique</a>. */
  @Test void testRemoveDistinctIfUnique3() {
    final String sql = "SELECT\n"
        + "    COUNT(DISTINCT sal) as cds,\n"
        + "    COUNT(sal) as cs,\n"
        + "    SUM(DISTINCT sal) AS sds,\n"
        + "    SUM(sal) AS ss\n"
        + "FROM (\n"
        + "    SELECT DISTINCT deptno, sal\n"
        + "    FROM emp)\n";
    sql(sql)
        .withConfig(c ->
            c.addRelBuilderConfigTransform(c2 ->
                c2.withRemoveRedundantDistinct(true))).ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6214">[CALCITE-6214]
   * Remove DISTINCT in COUNT if field is unique</a>. */
  @Test void testRemoveDistinctIfUnique4() {
    final String sql = "SELECT\n"
        + "    COUNT(DISTINCT sal) as cds,\n"
        + "    COUNT(sal) as cs,\n"
        + "    SUM(DISTINCT sal) AS sds,\n"
        + "    SUM(sal) AS ss\n"
        + "FROM (\n"
        + "    SELECT deptno, sal\n"
        + "    FROM emp"
        + "    GROUP BY deptno, sal)"
        + "GROUP BY deptno\n";
    // Default save redundant distinct
    sql(sql).ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6214">[CALCITE-6214]
   * Remove DISTINCT in COUNT if field is unique</a>. */
  @Test void testRemoveDistinctIfUnique5() {
    // empno is unique key
    final String sql = "SELECT COUNT(DISTINCT empno)\n"
        + "FROM emp\n";
    // Default save redundant distinct
    sql(sql)
        .withConfig(c ->
            c.addRelBuilderConfigTransform(c2 ->
                c2.withRemoveRedundantDistinct(true))).ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6214">[CALCITE-6214]
   * Remove DISTINCT in COUNT if field is unique</a>.
   * See {@link org.apache.calcite.test.catalog.MockCatalogReaderSimple#registerTableEmp}
   * */
  @Test void testRemoveDistinctIfUnique6() {
    // empno is unique key in emp table
    final String sql = "SELECT deptno, COUNT(DISTINCT empno)\n"
        + "FROM emp\n"
        + "GROUP BY deptno";
    // Default save redundant distinct
    sql(sql)
        .withConfig(c ->
            c.addRelBuilderConfigTransform(c2 ->
                c2.withRemoveRedundantDistinct(true))).ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6214">[CALCITE-6214]
   * Remove DISTINCT in COUNT if field is unique</a>. */
  @Test void testRemoveDistinctIfUnique7() {
    // empno is unique key
    final String sql = "SELECT deptno, COUNT(DISTINCT empno)\n"
        + "FROM emp\n"
        + "GROUP BY ROLLUP(deptno)";
    // Default save redundant distinct
    sql(sql)
        .withConfig(c ->
            c.addRelBuilderConfigTransform(c2 ->
                c2.withRemoveRedundantDistinct(true))).ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5089">[CALCITE-5089]
   * Allow GROUP BY ALL or DISTINCT set quantifier on GROUPING SETS</a>. */
  @Test void testGroupByDistinct() {
    final String sql = "SELECT deptno, job, count(*)\n"
        + "FROM emp\n"
        + "GROUP BY DISTINCT\n"
        + "CUBE (deptno, job),\n"
        + "ROLLUP (deptno, job)";
    sql(sql).ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5089">[CALCITE-5089]
   * Allow GROUP BY ALL or DISTINCT set quantifier on GROUPING SETS</a>. */
  @Test void testGroupByAll() {
    final String sql = "SELECT deptno, job, count(*)\n"
        + "FROM emp\n"
        + "GROUP BY ALL\n"
        + "CUBE (deptno, job),\n"
        + "ROLLUP (deptno, job)";
    sql(sql).ok();
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5045">[CALCITE-5045]
   * Alias within GroupingSets throws type mis-match exception</a>.
   */
  @Test void testAliasWithinGroupingSets() {
    final String sql = "SELECT empno / 2 AS x\n"
        + "FROM emp\n"
        + "GROUP BY ROLLUP(x)";
    sql(sql)
        .withConformance(SqlConformanceEnum.LENIENT)
        .ok();
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5145">[CALCITE-5145]
   * CASE statement within GROUPING SETS throws type mis-match exception</a>.
   */
  @Test void testCaseAliasWithinGroupingSets() {
    sql("SELECT empno,\n"
        + "CASE\n"
        + "WHEN ename in ('Fred','Eric') THEN 'CEO'\n"
        + "ELSE 'Other'\n"
        + "END AS derived_col\n"
        + "FROM emp\n"
        + "GROUP BY GROUPING SETS ((empno, derived_col),(empno))")
        .withConformance(SqlConformanceEnum.LENIENT).ok();
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5145">[CALCITE-5145]
   * CASE statement within GROUPING SETS throws type mis-match exception</a>.
   */
  @Test void testCaseWithinGroupingSets() {
    String sql = "SELECT empno,\n"
        + "CASE WHEN ename IN ('Fred','Eric') THEN 'Manager' ELSE 'Other' END\n"
        + "FROM emp\n"
        + "GROUP BY GROUPING SETS (\n"
        + "(empno, CASE WHEN ename IN ('Fred','Eric') THEN 'Manager' ELSE 'Other' END),\n"
        + "(empno)\n"
        + ")";
    sql(sql)
        .withConformance(SqlConformanceEnum.LENIENT)
        .ok();
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5297">[CALCITE-5297]
   * Casting dynamic variable twice throws exception</a>.
   */
  @Test void testDynamicParameterDoubleCast() {
    String sql = "SELECT CAST(CAST(? AS INTEGER) AS CHAR)";
    sql(sql).ok();
  }
}
