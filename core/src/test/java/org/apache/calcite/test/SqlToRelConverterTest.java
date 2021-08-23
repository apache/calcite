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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.externalize.RelDotWriter;
import org.apache.calcite.rel.externalize.RelXmlWriter;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlDelegatingConformance;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.sql2rel.StandardConvertletTableConfig;
import org.apache.calcite.test.catalog.MockCatalogReaderExtended;
import org.apache.calcite.util.Bug;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.TestUtil;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.function.UnaryOperator;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.Is.isA;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit test for {@link org.apache.calcite.sql2rel.SqlToRelConverter}.
 */
class SqlToRelConverterTest extends SqlToRelTestBase {
  protected DiffRepository getDiffRepos() {
    return DiffRepository.lookup(SqlToRelConverterTest.class);
  }

  /** Sets the SQL statement for a test. */
  public final Sql sql(String sql) {
    return new Sql(sql, true, tester, false, UnaryOperator.identity(),
        tester.getConformance(), true);
  }

  public final Sql expr(String expr) {
    return new Sql(expr, true, tester, false, UnaryOperator.identity(),
            tester.getConformance(), false);
  }

  @Test void testDotLiteralAfterNestedRow() {
    final String sql = "select ((1,2),(3,4,5)).\"EXPR$1\".\"EXPR$2\" from emp";
    sql(sql).ok();
  }

  @Test void testDotLiteralAfterRow() {
    final String sql = "select row(1,2).\"EXPR$1\" from emp";
    sql(sql).ok();
  }

  @Test void testRowValueConstructorWithSubquery() {
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
    sql(sql).with(getTesterWithDynamicTable()).ok();
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

  @Test void testJoinOnInSubQuery() {
    final String sql = "select * from emp left join dept\n"
        + "on emp.empno = 1\n"
        + "or dept.deptno in (select deptno from emp where empno > 5)";
    sql(sql).expand(false).ok();
  }

  @Test void testJoinOnExists() {
    final String sql = "select * from emp left join dept\n"
        + "on emp.empno = 1\n"
        + "or exists (select deptno from emp where empno > dept.deptno + 5)";
    sql(sql).expand(false).ok();
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
        .conformance(SqlConformanceEnum.LENIENT).ok();
  }

  @Test void testGroupByAliasOfSubExpressionsInProject() {
    final String sql = "select deptno+empno as d, deptno+empno+mgr\n"
        + "from emp group by d,mgr";
    sql(sql)
        .conformance(SqlConformanceEnum.LENIENT).ok();
  }

  @Test void testGroupByAliasEqualToColumnName() {
    sql("select empno, ename as deptno from emp group by empno, deptno")
        .conformance(SqlConformanceEnum.LENIENT).ok();
  }

  @Test void testGroupByOrdinal() {
    sql("select empno from emp group by 1")
        .conformance(SqlConformanceEnum.LENIENT).ok();
  }

  @Test void testGroupByContainsLiterals() {
    final String sql = "select count(*) from (\n"
        + "  select 1 from emp group by substring(ename from 2 for 3))";
    sql(sql)
        .conformance(SqlConformanceEnum.LENIENT).ok();
  }

  @Test void testAliasInHaving() {
    sql("select count(empno) as e from emp having e > 1")
        .conformance(SqlConformanceEnum.LENIENT).ok();
  }

  @Test void testGroupJustOneAgg() {
    // just one agg
    final String sql =
        "select deptno, sum(sal) as sum_sal from emp group by deptno";
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
   * <blockquote>GROUP BY GROUPING SETS (ROLLUP(A, B), CUBE(C,D))</blockquote>
   * <p>is equal to
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
        + "group by grouping sets (deptno, (), deptno)";
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
    // FRG-98
    if (!tester.getConformance().isSortByOrdinal()) {
      return;
    }
    final String sql =
        "select empno + 1, deptno, empno from emp order by 2 desc";
    sql(sql).ok();

    // ordinals rounded down, so 2.5 should have same effect as 2, and
    // generate identical plan
    final String sql2 =
        "select empno + 1, deptno, empno from emp order by 2.5 desc";
    sql(sql2).ok();
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
    if (!tester.getConformance().isSortByAlias()) {
      return;
    }

    // plan should contain '(empno + 1) + 3'
    final String sql = "select empno + 1 as empno, empno - 2 as y\n"
        + "from emp order by empno + 3";
    sql(sql).ok();
  }

  @Test void testOrderByAliasDoesNotOverride() {
    if (tester.getConformance().isSortByAlias()) {
      return;
    }

    // plan should contain 'empno + 3', not '(empno + 1) + 3'
    final String sql = "select empno + 1 as empno, empno - 2 as y\n"
        + "from emp order by empno + 3";
    sql(sql).ok();
  }

  @Test void testOrderBySameExpr() {
    final String sql = "select empno from emp, dept\n"
        + "order by sal + empno desc, sal * empno, sal + empno desc";
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
    if (!tester.getConformance().isSortByOrdinal()) {
      return;
    }
    final String sql = "select empno, sal from emp\n"
        + "union all\n"
        + "select deptno, deptno from dept\n"
        + "order by 2";
    sql(sql).ok();
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
    sql(sql).decorrelate(false).ok();
  }

  @Test void testWithInsideWhereExistsRex() {
    final String sql = "select * from emp\n"
        + "where exists (\n"
        + "  with dept2 as (select * from dept where dept.deptno >= emp.deptno)\n"
        + "  select 1 from dept2 where deptno <= emp.deptno)";
    sql(sql).decorrelate(false).expand(false).ok();
  }

  @Test void testWithInsideWhereExistsDecorrelate() {
    final String sql = "select * from emp\n"
        + "where exists (\n"
        + "  with dept2 as (select * from dept where dept.deptno >= emp.deptno)\n"
        + "  select 1 from dept2 where deptno <= emp.deptno)";
    sql(sql).decorrelate(true).ok();
  }

  @Test void testWithInsideWhereExistsDecorrelateRex() {
    final String sql = "select * from emp\n"
        + "where exists (\n"
        + "  with dept2 as (select * from dept where dept.deptno >= emp.deptno)\n"
        + "  select 1 from dept2 where deptno <= emp.deptno)";
    sql(sql).decorrelate(true).expand(false).ok();
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
    sql(sql).expand(false).ok();
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
    sql(sql).with(getExtendedTester()).ok();
  }

  @Test void testModifiableViewExtendSubset() {
    final String sql = "select x, empno\n"
        + "from EMP_MODIFIABLEVIEW extend (x varchar(5) not null)";
    sql(sql).with(getExtendedTester()).ok();
  }

  @Test void testModifiableViewExtendExpression() {
    final String sql = "select empno + x\n"
        + "from EMP_MODIFIABLEVIEW extend (x int not null)";
    sql(sql).with(getExtendedTester()).ok();
  }

  @Test void testSelectViewExtendedColumnCollision() {
    sql("select ENAME, EMPNO, JOB, SLACKER, SAL, HIREDATE, MGR\n"
        + " from EMP_MODIFIABLEVIEW3\n"
        + " where SAL = 20").with(getExtendedTester()).ok();
    sql("select ENAME, EMPNO, JOB, SLACKER, SAL, HIREDATE, MGR\n"
        + " from EMP_MODIFIABLEVIEW3 extend (SAL int)\n"
        + " where SAL = 20").with(getExtendedTester()).ok();
  }

  @Test void testSelectViewExtendedColumnCaseSensitiveCollision() {
    sql("select ENAME, EMPNO, JOB, SLACKER, \"sal\", HIREDATE, MGR\n"
        + " from EMP_MODIFIABLEVIEW3 extend (\"sal\" boolean)\n"
        + " where \"sal\" = true").with(getExtendedTester()).ok();
  }

  @Test void testSelectViewExtendedColumnExtendedCollision() {
    sql("select ENAME, EMPNO, JOB, SLACKER, SAL, HIREDATE, EXTRA\n"
        + " from EMP_MODIFIABLEVIEW2\n"
        + " where SAL = 20").with(getExtendedTester()).ok();
    sql("select ENAME, EMPNO, JOB, SLACKER, SAL, HIREDATE, EXTRA\n"
        + " from EMP_MODIFIABLEVIEW2 extend (EXTRA boolean)\n"
        + " where SAL = 20").with(getExtendedTester()).ok();
  }

  @Test void testSelectViewExtendedColumnCaseSensitiveExtendedCollision() {
    sql("select ENAME, EMPNO, JOB, SLACKER, SAL, HIREDATE, \"extra\"\n"
        + " from EMP_MODIFIABLEVIEW2 extend (\"extra\" boolean)\n"
        + " where \"extra\" = false").with(getExtendedTester()).ok();
  }

  @Test void testSelectViewExtendedColumnUnderlyingCollision() {
    sql("select ENAME, EMPNO, JOB, SLACKER, SAL, HIREDATE, MGR, COMM\n"
        + " from EMP_MODIFIABLEVIEW3 extend (COMM int)\n"
        + " where SAL = 20").with(getExtendedTester()).ok();
  }

  @Test void testSelectViewExtendedColumnCaseSensitiveUnderlyingCollision() {
    sql("select ENAME, EMPNO, JOB, SLACKER, SAL, HIREDATE, MGR, \"comm\"\n"
        + " from EMP_MODIFIABLEVIEW3 extend (\"comm\" int)\n"
        + " where \"comm\" = 20").with(getExtendedTester()).ok();
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
        + " where empno = 10").with(getExtendedTester()).ok();
  }

  @Test void testUpdateExtendedColumnModifiableViewCaseSensitiveCollision() {
    sql("update EMP_MODIFIABLEVIEW2(\"slacker\" INTEGER, deptno INTEGER)"
        + " set deptno = 20, \"slacker\" = 100"
        + " where ename = 'Bob'").with(getExtendedTester()).ok();
  }

  @Test void testUpdateExtendedColumnModifiableViewExtendedCollision() {
    sql("update EMP_MODIFIABLEVIEW2(\"slacker\" INTEGER, extra BOOLEAN)"
        + " set deptno = 20, \"slacker\" = 100, extra = true"
        + " where ename = 'Bob'").with(getExtendedTester()).ok();
  }

  @Test void testUpdateExtendedColumnModifiableViewExtendedCaseSensitiveCollision() {
    sql("update EMP_MODIFIABLEVIEW2(\"extra\" INTEGER, extra BOOLEAN)"
        + " set deptno = 20, \"extra\" = 100, extra = true"
        + " where ename = 'Bob'").with(getExtendedTester()).ok();
  }

  @Test void testUpdateExtendedColumnModifiableViewUnderlyingCollision() {
    sql("update EMP_MODIFIABLEVIEW3(extra BOOLEAN, comm INTEGER)"
        + " set empno = 20, comm = 123, extra = true"
        + " where ename = 'Bob'").with(getExtendedTester()).ok();
  }

  @Test void testSelectModifiableViewConstraint() {
    final String sql = "select deptno from EMP_MODIFIABLEVIEW2\n"
        + "where deptno = ?";
    sql(sql).with(getExtendedTester()).ok();
  }

  @Test void testModifiableViewDdlExtend() {
    final String sql = "select extra from EMP_MODIFIABLEVIEW2";
    sql(sql).with(getExtendedTester()).ok();
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
    sql(sql).with(getExtendedTester()).ok();
  }

  @Test void testJoinTemporalTableOnSpecificTime1() {
    final String sql = "select stream *\n"
        + "from orders,\n"
        + "  products_temporal for system_time as of\n"
        + "    TIMESTAMP '2011-01-02 00:00:00'";
    sql(sql).ok();
  }

  @Test void testJoinTemporalTableOnSpecificTime2() {
    // Test temporal table with virtual columns.
    final String sql = "select stream *\n"
        + "from orders,\n"
        + "  VIRTUALCOLUMNS.VC_T1 for system_time as of\n"
        + "    TIMESTAMP '2011-01-02 00:00:00'";
    sql(sql).with(getExtendedTester()).ok();
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
    sql(sql).with(getExtendedTester()).ok();
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

  @Test void testCollectionTableWithCursorParam() {
    final String sql = "select * from table(dedup("
        + "cursor(select ename from emp),"
        + " cursor(select name from dept), 'NAME'))";
    sql(sql).decorrelate(false).ok();
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
    sql(sql).conformance(SqlConformanceEnum.LENIENT).ok();
  }

  @Test void testUnnestArrayPlan() {
    final String sql = "select d.deptno, e2.empno\n"
        + "from dept_nested as d,\n"
        + " UNNEST(d.employees) e2";
    sql(sql).with(getExtendedTester()).ok();
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
    sql(sql).conformance(SqlConformanceEnum.PRESTO).ok();
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
    sql(sql).conformance(SqlConformanceEnum.PRESTO).ok();
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
    sql(sql).expand(true).ok();
  }

  @Test void testMultisetOfColumnsRex() {
    sql("select 'abc',multiset[deptno,sal] from emp").ok();
  }

  @Test void testCorrelationJoin() {
    final String sql = "select *,\n"
        + "  multiset(select * from emp where deptno=dept.deptno) as empset\n"
        + "from dept";
    sql(sql).ok();
  }

  @Test void testCorrelationJoinRex() {
    final String sql = "select *,\n"
        + "  multiset(select * from emp where deptno=dept.deptno) as empset\n"
        + "from dept";
    sql(sql).expand(false).ok();
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
    sql(sql).expand(false).ok();
  }

  @Test void testExists() {
    final String sql = "select*from emp\n"
        + "where exists (select 1 from dept where deptno=55)";
    sql(sql).ok();
  }

  @Test void testExistsCorrelated() {
    final String sql = "select*from emp where exists (\n"
        + "  select 1 from dept where emp.deptno=dept.deptno)";
    sql(sql).decorrelate(false).ok();
  }

  @Test void testNotExistsCorrelated() {
    final String sql = "select * from emp where not exists (\n"
        + "  select 1 from dept where emp.deptno=dept.deptno)";
    sql(sql).decorrelate(false).ok();
  }

  @Test void testExistsCorrelatedDecorrelate() {
    final String sql = "select*from emp where exists (\n"
        + "  select 1 from dept where emp.deptno=dept.deptno)";
    sql(sql).decorrelate(true).ok();
  }

  /**
   * Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-4560">[CALCITE-4560]
   * Wrong plan when decorrelating EXISTS subquery with COALESCE in the predicate</a>. */
  @Test void testExistsDecorrelateComplexCorrelationPredicate() {
    final String sql = "select e1.empno from empnullables e1 where exists (\n"
        + "  select 1 from empnullables e2 where COALESCE(e1.ename,'M')=COALESCE(e2.ename,'M'))";
    sql(sql).decorrelate(true).ok();
  }

  @Test void testExistsCorrelatedDecorrelateRex() {
    final String sql = "select*from emp where exists (\n"
        + "  select 1 from dept where emp.deptno=dept.deptno)";
    sql(sql).decorrelate(true).expand(false).ok();
  }

  @Test void testExistsCorrelatedLimit() {
    final String sql = "select*from emp where exists (\n"
        + "  select 1 from dept where emp.deptno=dept.deptno limit 1)";
    sql(sql).decorrelate(false).ok();
  }

  @Test void testExistsCorrelatedLimitDecorrelate() {
    final String sql = "select*from emp where exists (\n"
        + "  select 1 from dept where emp.deptno=dept.deptno limit 1)";
    sql(sql).decorrelate(true).expand(true).ok();
  }

  @Test void testExistsCorrelatedLimitDecorrelateRex() {
    final String sql = "select*from emp where exists (\n"
        + "  select 1 from dept where emp.deptno=dept.deptno limit 1)";
    sql(sql).decorrelate(true).expand(false).ok();
  }

  @Test void testInValueListShort() {
    final String sql = "select empno from emp where deptno in (10, 20)";
    sql(sql).ok();
    sql(sql).expand(false).ok();
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
    sql(sql).expand(false).ok();
  }

  @Test void testCompositeInUncorrelatedSubQueryRex() {
    final String sql = "select empno from emp where (empno, deptno) in"
        + " (select deptno - 10, deptno from dept)";
    sql(sql).expand(false).ok();
  }

  @Test void testNotInUncorrelatedSubQuery() {
    final String sql = "select empno from emp where deptno not in"
        + " (select deptno from dept)";
    sql(sql).ok();
  }

  @Test void testAllValueList() {
    final String sql = "select empno from emp where deptno > all (10, 20)";
    sql(sql).expand(false).ok();
  }

  @Test void testSomeValueList() {
    final String sql = "select empno from emp where deptno > some (10, 20)";
    sql(sql).expand(false).ok();
  }

  @Test void testSome() {
    final String sql = "select empno from emp where deptno > some (\n"
        + "  select deptno from dept)";
    sql(sql).expand(false).ok();
  }

  @Test void testSomeWithEquality() {
    final String sql = "select empno from emp where deptno = some (\n"
        + "  select deptno from dept)";
    sql(sql).expand(false).ok();
  }

  @Test void testNotInUncorrelatedSubQueryRex() {
    final String sql = "select empno from emp where deptno not in"
        + " (select deptno from dept)";
    sql(sql).expand(false).ok();
  }

  @Test void testNotCaseInThreeClause() {
    final String sql = "select empno from emp where not case when "
        + "true then deptno in (10,20) else true end";
    sql(sql).expand(false).ok();
  }

  @Test void testNotCaseInMoreClause() {
    final String sql = "select empno from emp where not case when "
        + "true then deptno in (10,20) when false then false else deptno in (30,40) end";
    sql(sql).expand(false).ok();
  }

  @Test void testNotCaseInWithoutElse() {
    final String sql = "select empno from emp where not case when "
        + "true then deptno in (10,20)  end";
    sql(sql).expand(false).ok();
  }

  @Test void testWhereInCorrelated() {
    final String sql = "select empno from emp as e\n"
        + "join dept as d using (deptno)\n"
        + "where e.sal in (\n"
        + "  select e2.sal from emp as e2 where e2.deptno > e.deptno)";
    sql(sql).expand(false).ok();
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
    sql(sql).expand(false).ok();
  }

  @Test void testInUncorrelatedSubQueryInHavingRex() {
    final String sql = "select sum(sal) as s\n"
        + "from emp\n"
        + "group by deptno\n"
        + "having count(*) > 2\n"
        + "and deptno in (\n"
        + "  select case when true then deptno else null end from emp)";
    sql(sql).expand(false).ok();
  }

  @Test void testUncorrelatedScalarSubQueryInOrderRex() {
    final String sql = "select ename\n"
        + "from emp\n"
        + "order by (select case when true then deptno else null end from emp) desc,\n"
        + "  ename";
    sql(sql).expand(false).ok();
  }

  @Test void testUncorrelatedScalarSubQueryInGroupOrderRex() {
    final String sql = "select sum(sal) as s\n"
        + "from emp\n"
        + "group by deptno\n"
        + "order by (select case when true then deptno else null end from emp) desc,\n"
        + "  count(*)";
    sql(sql).expand(false).ok();
  }

  @Test void testUncorrelatedScalarSubQueryInAggregateRex() {
    final String sql = "select sum((select min(deptno) from emp)) as s\n"
        + "from emp\n"
        + "group by deptno\n";
    sql(sql).expand(false).ok();
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
    sql(sql).expand(false).ok();
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
    sql(sql).expand(false).ok();
  }

  @Test void testUnnestSelect() {
    final String sql = "select*from unnest(select multiset[deptno] from dept)";
    sql(sql).expand(true).ok();
  }

  @Test void testUnnestSelectRex() {
    final String sql = "select*from unnest(select multiset[deptno] from dept)";
    sql(sql).expand(false).ok();
  }

  @Test void testJoinUnnest() {
    final String sql = "select*from dept as d, unnest(multiset[d.deptno * 2])";
    sql(sql).ok();
  }

  @Test void testJoinUnnestRex() {
    final String sql = "select*from dept as d, unnest(multiset[d.deptno * 2])";
    sql(sql).expand(false).ok();
  }

  @Test void testLateral() {
    final String sql = "select * from emp,\n"
        + "  LATERAL (select * from dept where emp.deptno=dept.deptno)";
    sql(sql).decorrelate(false).ok();
  }

  @Test void testLateralDecorrelate() {
    final String sql = "select * from emp,\n"
        + " LATERAL (select * from dept where emp.deptno=dept.deptno)";
    sql(sql).decorrelate(true).expand(true).ok();
  }

  @Test void testLateralDecorrelateRex() {
    final String sql = "select * from emp,\n"
        + " LATERAL (select * from dept where emp.deptno=dept.deptno)";
    sql(sql).decorrelate(true).ok();
  }

  @Test void testLateralDecorrelateThetaRex() {
    final String sql = "select * from emp,\n"
        + " LATERAL (select * from dept where emp.deptno < dept.deptno)";
    sql(sql).decorrelate(true).ok();
  }

  @Test void testNestedCorrelations() {
    final String sql = "select *\n"
        + "from (select 2+deptno d2, 3+deptno d3 from emp) e\n"
        + " where exists (select 1 from (select deptno+1 d1 from dept) d\n"
        + " where d1=e.d2 and exists (select 2 from (select deptno+4 d4, deptno+5 d5, deptno+6 d6 from dept)\n"
        + " where d4=d.d1 and d5=d.d1 and d6=e.d3))";
    sql(sql).decorrelate(false).ok();
  }

  @Test void testNestedCorrelationsDecorrelated() {
    final String sql = "select *\n"
        + "from (select 2+deptno d2, 3+deptno d3 from emp) e\n"
        + " where exists (select 1 from (select deptno+1 d1 from dept) d\n"
        + " where d1=e.d2 and exists (select 2 from (select deptno+4 d4, deptno+5 d5, deptno+6 d6 from dept)\n"
        + " where d4=d.d1 and d5=d.d1 and d6=e.d3))";
    sql(sql).decorrelate(true).expand(true).ok();
  }

  @Test void testNestedCorrelationsDecorrelatedRex() {
    final String sql = "select *\n"
        + "from (select 2+deptno d2, 3+deptno d3 from emp) e\n"
        + " where exists (select 1 from (select deptno+1 d1 from dept) d\n"
        + " where d1=e.d2 and exists (select 2 from (select deptno+4 d4, deptno+5 d5, deptno+6 d6 from dept)\n"
        + " where d4=d.d1 and d5=d.d1 and d6=e.d3))";
    sql(sql).decorrelate(true).ok();
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
    final RelNode rel = tester.convertSqlToRel(sql).rel;
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
    final RelNode rel = tester.convertSqlToRel(sql).rel;
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
    sql(sql).trim(true).ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3183">[CALCITE-3183]
   * Trimming method for Filter rel uses wrong traitSet</a>. */
  @Test void testFilterAndSortWithTrim() {
    // Create a customized test with RelCollation trait in the test cluster.
    Tester tester =
        new TesterImpl(getDiffRepos())
            .withDecorrelation(false)
            .withPlannerFactory(context ->
                new MockRelOptPlanner(Contexts.empty()) {
                  @Override public List<RelTraitDef> getRelTraitDefs() {
                    return ImmutableList.of(RelCollationTraitDef.INSTANCE);
                  }
                  @Override public RelTraitSet emptyTraitSet() {
                    return RelTraitSet.createEmpty().plus(
                        RelCollationTraitDef.INSTANCE.getDefault());
                  }
                });

    // Run query and save plan after trimming
    final String sql = "select count(a.EMPNO)\n"
        + "from (select * from emp order by sal limit 3) a\n"
        + "where a.EMPNO > 10 group by 2";
    RelNode afterTrim = tester.convertSqlToRel(sql).rel;

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
    assertThat(rels.size(), is(2));
    RelTrait filterCollation = rels.get(0).getTraitSet()
        .getTrait(RelCollationTraitDef.INSTANCE);
    RelTrait sortCollation = rels.get(1).getTraitSet()
        .getTrait(RelCollationTraitDef.INSTANCE);
    assertTrue(filterCollation.satisfies(sortCollation));
  }

  @Test void testRelShuttleForLogicalCalc() {
    final String sql = "select ename from emp";
    final RelNode rel = tester.convertSqlToRel(sql).rel;
    final HepProgramBuilder programBuilder = HepProgram.builder();
    programBuilder.addRuleInstance(CoreRules.PROJECT_TO_CALC);
    final HepPlanner planner = new HepPlanner(programBuilder.build());
    planner.setRoot(rel);
    final LogicalCalc calc = (LogicalCalc) planner.findBestExp();
    final List<RelNode> rels = new ArrayList<>();
    final RelShuttleImpl visitor = new RelShuttleImpl() {
      @Override public RelNode visit(LogicalCalc calc) {
        RelNode visitedRel = super.visit(calc);
        rels.add(visitedRel);
        return visitedRel;
      }
    };
    visitor.visit(calc);
    assertThat(rels.size(), is(1));
    assertThat(rels.get(0), isA(LogicalCalc.class));
  }

  @Test void testRelShuttleForLogicalTableModify() {
    final String sql = "insert into emp select * from emp";
    final LogicalTableModify rel = (LogicalTableModify) tester.convertSqlToRel(sql).rel;
    final List<RelNode> rels = new ArrayList<>();
    final RelShuttleImpl visitor = new RelShuttleImpl() {
      @Override public RelNode visit(LogicalTableModify modify) {
        RelNode visitedRel = super.visit(modify);
        rels.add(visitedRel);
        return visitedRel;
      }
    };
    visitor.visit(rel);
    assertThat(rels.size(), is(1));
    assertThat(rels.get(0), isA(LogicalTableModify.class));
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
    sql(sql).expand(false).ok();
  }

  @Test void testCorrelatedSubQueryInAggregate() {
    final String sql = "SELECT SUM(\n"
        + "  (select char_length(name) from dept\n"
        + "   where dept.deptno = emp.empno))\n"
        + "FROM emp";
    sql(sql).expand(false).ok();
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
    sql(sql).conformance(SqlConformanceEnum.PRAGMATIC_2003).ok();
  }

  @Test void testInsertWithCustomInitializerExpressionFactory() {
    final String sql = "insert into empdefaults (deptno) values (300)";
    sql(sql).ok();
  }

  @Test void testInsertSubsetWithCustomInitializerExpressionFactory() {
    final String sql = "insert into empdefaults values (100)";
    sql(sql).conformance(SqlConformanceEnum.PRAGMATIC_2003).ok();
  }

  @Test void testInsertBind() {
    final String sql = "insert into empnullables (deptno, empno, ename)\n"
        + "values (?, ?, ?)";
    sql(sql).ok();
  }

  @Test void testInsertBindSubset() {
    final String sql = "insert into empnullables\n"
        + "values (?, ?)";
    sql(sql).conformance(SqlConformanceEnum.PRAGMATIC_2003).ok();
  }

  @Test void testInsertBindWithCustomInitializerExpressionFactory() {
    final String sql = "insert into empdefaults (deptno) values (?)";
    sql(sql).ok();
  }

  @Test void testInsertBindSubsetWithCustomInitializerExpressionFactory() {
    final String sql = "insert into empdefaults values (?)";
    sql(sql).conformance(SqlConformanceEnum.PRAGMATIC_2003).ok();
  }

  @Test void testInsertSubsetView() {
    final String sql = "insert into empnullables_20\n"
        + "values (10, 'Fred')";
    sql(sql).conformance(SqlConformanceEnum.PRAGMATIC_2003).ok();
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
    sql(sql).with(getExtendedTester()).ok();
  }

  @Test void testInsertBindExtendedColumnModifiableView() {
    final String sql = "insert into EMP_MODIFIABLEVIEW2(updated TIMESTAMP)\n"
        + " (ename, deptno, empno, updated, sal)\n"
        + " values ('Fred', 20, 44, ?, 999999)";
    sql(sql).with(getExtendedTester()).ok();
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
    sql(sql).with(getExtendedTester()).ok();
  }

  @Test void testDeleteBindExtendedColumnModifiableView() {
    final String sql = "delete from EMP_MODIFIABLEVIEW2(note VARCHAR)\n"
        + "where note = ?";
    sql(sql).with(getExtendedTester()).ok();
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
    sql(sql).with(getExtendedTester()).ok();
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
    sql(sql).with(getExtendedTester()).ok();
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

  @Disabled("CALCITE-985")
  @Test void testMerge() {
    final String sql = "merge into emp as target\n"
        + "using (select * from emp where deptno = 30) as source\n"
        + "on target.empno = source.empno\n"
        + "when matched then\n"
        + "  update set sal = sal + source.sal\n"
        + "when not matched then\n"
        + "  insert (empno, deptno, sal)\n"
        + "  values (source.empno, source.deptno, source.sal)";
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
    sql(sql).with(getExtendedTester()).ok();
  }

  @Test void testInsertSubsetModifiableView() {
    final String sql = "insert into EMP_MODIFIABLEVIEW "
        + "values (10, 'Fred')";
    sql(sql).with(getExtendedTester())
        .conformance(SqlConformanceEnum.PRAGMATIC_2003).ok();
  }

  @Test void testInsertBindModifiableView() {
    final String sql = "insert into EMP_MODIFIABLEVIEW (empno, job)"
        + " values (?, ?)";
    sql(sql).with(getExtendedTester()).ok();
  }

  @Test void testInsertBindSubsetModifiableView() {
    final String sql = "insert into EMP_MODIFIABLEVIEW"
        + " values (?, ?)";
    sql(sql).conformance(SqlConformanceEnum.PRAGMATIC_2003)
        .with(getExtendedTester()).ok();
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
    sql(sql).decorrelate(true).ok();
  }

  @Test void testSimplifyNotExistsAggregateSubQuery() {
    final String sql = "SELECT e1.empno\n"
        + "FROM emp e1 where not exists\n"
        + "(select avg(sal) from emp e2 where e1.empno = e2.empno)";
    sql(sql).decorrelate(true).ok();
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
    sql(sql).decorrelate(true).ok();
  }

  @Test void testSimplifyNotExistsValuesSubQuery() {
    final String sql = "select deptno\n"
        + "from EMP\n"
        + "where not exists (values 10)";
    sql(sql).decorrelate(true).ok();
  }

  @Test void testReduceConstExpr() {
    final String sql = "select sum(case when 'y' = 'n' then ename else 0.1 end) from emp";
    sql(sql).ok();
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
    sql(sql).expand(false).ok();
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
    sql(sql).decorrelate(true).expand(true).ok();
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
    sql(sql).decorrelate(true).expand(true).ok();
  }

  @Test void testCorrelationScalarAggAndFilterRex() {
    final String sql = "SELECT e1.empno\n"
        + "FROM emp e1, dept d1 where e1.deptno = d1.deptno\n"
        + "and e1.deptno < 10 and d1.deptno < 15\n"
        + "and e1.sal > (select avg(sal) from emp e2 where e1.empno = e2.empno)";
    sql(sql).decorrelate(true).expand(false).ok();
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
    sql(sql).decorrelate(true).expand(true).ok();
  }

  @Test void testCorrelationExistsAndFilterRex() {
    final String sql = "SELECT e1.empno\n"
        + "FROM emp e1, dept d1 where e1.deptno = d1.deptno\n"
        + "and e1.deptno < 10 and d1.deptno < 15\n"
        + "and exists (select * from emp e2 where e1.empno = e2.empno)";
    sql(sql).decorrelate(true).ok();
  }

  /** A theta join condition, unlike the equi-join condition in
   * {@link #testCorrelationExistsAndFilterRex()}, requires a value
   * generator. */
  @Test void testCorrelationExistsAndFilterThetaRex() {
    final String sql = "SELECT e1.empno\n"
        + "FROM emp e1, dept d1 where e1.deptno = d1.deptno\n"
        + "and e1.deptno < 10 and d1.deptno < 15\n"
        + "and exists (select * from emp e2 where e1.empno < e2.empno)";
    sql(sql).decorrelate(true).ok();
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
    sql(sql).decorrelate(true).ok();
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
    sql(sql).decorrelate(true).ok();
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
    sql(sql).with(getTesterWithDynamicTable()).ok();
  }

  /** As {@link #testSelectFromDynamicTable} but "SELECT *". */
  @Test void testSelectStarFromDynamicTable() {
    final String sql = "select * from SALES.NATION";
    sql(sql).with(getTesterWithDynamicTable()).ok();
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
    sql(sql).with(getTesterWithDynamicTable()).ok();
  }

  /** As {@link #testSelectFromDynamicTable} but with ORDER BY. */
  @Test void testReferDynamicStarInSelectOB() {
    final String sql = "select n_nationkey, n_name\n"
        + "from (select * from SALES.NATION)\n"
        + "order by n_regionkey";
    sql(sql).with(getTesterWithDynamicTable()).ok();
  }

  /** As {@link #testSelectFromDynamicTable} but with join. */
  @Test void testDynamicStarInTableJoin() {
    final String sql = "select * from "
        + " (select * from SALES.NATION) T1, "
        + " (SELECT * from SALES.CUSTOMER) T2 "
        + " where T1.n_nationkey = T2.c_nationkey";
    sql(sql).with(getTesterWithDynamicTable()).ok();
  }

  @Test void testDynamicNestedColumn() {
    final String sql = "select t3.fake_q1['fake_col2'] as fake2\n"
        + "from (\n"
        + "  select t2.fake_col as fake_q1\n"
        + "  from SALES.CUSTOMER as t2) as t3";
    sql(sql).with(getTesterWithDynamicTable()).ok();
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
    final String sql3 = "select t1.c_nationkey, t3.fake_col3\n"
        + "from SALES.CUSTOMER as t1,\n"
        + "lateral (select t2.\"$unnest\" as fake_col3\n"
        + "         from unnest(t1.fake_col) as t2) as t3";
    sql(sql3).with(getTesterWithDynamicTable()).ok();
  }

  @Test void testStarDynamicSchemaUnnest() {
    final String sql3 = "select *\n"
        + "from SALES.CUSTOMER as t1,\n"
        + "lateral (select t2.\"$unnest\" as fake_col3\n"
        + "         from unnest(t1.fake_col) as t2) as t3";
    sql(sql3).with(getTesterWithDynamicTable()).ok();
  }

  @Test void testStarDynamicSchemaUnnest2() {
    final String sql3 = "select *\n"
        + "from SALES.CUSTOMER as t1,\n"
        + "unnest(t1.fake_col) as t2";
    sql(sql3).with(getTesterWithDynamicTable()).ok();
  }

  @Test void testStarDynamicSchemaUnnestNestedSubQuery() {
    String sql3 = "select t2.c1\n"
        + "from (select * from SALES.CUSTOMER) as t1,\n"
        + "unnest(t1.fake_col) as t2(c1)";
    sql(sql3).with(getTesterWithDynamicTable()).ok();
  }

  @Test void testReferDynamicStarInSelectWhereGB() {
    final String sql = "select n_regionkey, count(*) as cnt from "
        + "(select * from SALES.NATION) where n_nationkey > 5 "
        + "group by n_regionkey";
    sql(sql).with(getTesterWithDynamicTable()).ok();
  }

  @Test void testDynamicStarInJoinAndSubQ() {
    final String sql = "select * from "
        + " (select * from SALES.NATION T1, "
        + " SALES.CUSTOMER T2 where T1.n_nationkey = T2.c_nationkey)";
    sql(sql).with(getTesterWithDynamicTable()).ok();
  }

  @Test void testStarJoinStaticDynTable() {
    final String sql = "select * from SALES.NATION N, SALES.REGION as R "
        + "where N.n_regionkey = R.r_regionkey";
    sql(sql).with(getTesterWithDynamicTable()).ok();
  }

  @Test void testGrpByColFromStarInSubQuery() {
    final String sql = "SELECT n.n_nationkey AS col "
        + " from (SELECT * FROM SALES.NATION) as n "
        + " group by n.n_nationkey";
    sql(sql).with(getTesterWithDynamicTable()).ok();
  }

  @Test void testDynStarInExistSubQ() {
    final String sql = "select *\n"
        + "from SALES.REGION where exists (select * from SALES.NATION)";
    sql(sql).with(getTesterWithDynamicTable()).ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1150">[CALCITE-1150]
   * Create the a new DynamicRecordType, avoiding star expansion when working
   * with this type</a>. */
  @Test void testSelectDynamicStarOrderBy() {
    final String sql = "SELECT * from SALES.NATION order by n_nationkey";
    sql(sql).with(getTesterWithDynamicTable()).ok();
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
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1944">[CALCITE-1944]
   * Window function applied to sub-query with dynamic star gets wrong
   * plan</a>. */
  @Test void testWindowOnDynamicStar() {
    final String sql = "SELECT SUM(n_nationkey) OVER w\n"
        + "FROM (SELECT * FROM SALES.NATION) subQry\n"
        + "WINDOW w AS (PARTITION BY REGION ORDER BY n_nationkey)";
    sql(sql).with(getTesterWithDynamicTable()).ok();
  }

  @Test void testWindowAndGroupByWithDynamicStar() {
    final String sql = "SELECT\n"
        + "n_regionkey,\n"
        + "MAX(MIN(n_nationkey)) OVER (PARTITION BY n_regionkey)\n"
        + "FROM (SELECT * FROM SALES.NATION)\n"
        + "GROUP BY n_regionkey";

    sql(sql).conformance(new SqlDelegatingConformance(SqlConformanceEnum.DEFAULT) {
      @Override public boolean isGroupByAlias() {
        return true;
      }
    }).with(getTesterWithDynamicTable()).ok();
  }

  @Test public void testConvertletConfigNoWindowedAggDecomposeAvgSimple() {
    String query = "SELECT AVG(emp.sal) OVER (PARTITION BY emp.deptno) from emp";
    sql(query).with(getNoWindowedAggDecompositionTester()).ok();
  }

  @Test public void testConvertletConfigNoWindowedAggDecomposeAvg() {
    String query = "SELECT emp.sal, AVG(emp.sal) OVER (PARTITION BY emp.deptno ORDER BY emp.sal"
        +
        " ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM emp";
    sql(query).with(getNoWindowedAggDecompositionTester()).ok();
  }
  @Test public void testConvertletConfigNoWindowedAggDecomposeStd() {
    String query = "SELECT emp.sal, STDDEV(emp.sal) OVER (PARTITION BY emp.deptno ORDER BY emp.sal"
        +
        " ROWS BETWEEN 1 PRECEDING and 1 FOLLOWING) FROM emp";
    sql(query).with(getNoWindowedAggDecompositionTester()).ok();
  }
  @Test public void testConvertletConfigNoWindowedAggDecomposeStdPop() {
    String query = "SELECT emp.sal, STDDEV_POP(emp.sal) OVER (PARTITION BY emp.deptno "
        +
        "ORDER BY emp.sal ROWS BETWEEN 1 PRECEDING and 1 FOLLOWING) FROM emp";
    sql(query).with(getNoWindowedAggDecompositionTester()).ok();
  }
  @Test public void testConvertletConfigNoWindowedAggDecomposeVar() {
    String query = "SELECT emp.sal, VARIANCE(emp.sal) OVER (PARTITION BY emp.deptno ORDER BY"
        +
        " emp.sal ROWS BETWEEN 1 PRECEDING and 1 FOLLOWING) FROM emp";
    sql(query).with(getNoWindowedAggDecompositionTester()).ok();
  }
  @Test public void testConvertletConfigNoWindowedAggDecomposeVarPop() {
    String query = "SELECT emp.sal, VAR_POP(emp.sal) OVER (PARTITION BY emp.deptno ORDER BY emp.sal"
        +
        " ROWS BETWEEN 1 PRECEDING and 1 FOLLOWING) FROM emp";
    sql(query).with(getNoWindowedAggDecompositionTester()).ok();
  }

  @Test public void testConvertletConfigTimestampdiffDecompose() {
    String query = "SELECT TIMESTAMPDIFF(DAY, TIMESTAMP '2021-02-02', TIMESTAMP '2022-02-01')";
    sql(query).ok();
  }
  @Test public void testConvertletConfigNoTimestampdiffDecompose() {
    String query = "SELECT TIMESTAMPDIFF(DAY, TIMESTAMP '2021-02-02', TIMESTAMP '2022-02-01')";
    sql(query).with(getNoTimestampdiffDecompositionTester()).ok();
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

  private Tester getExtendedTester() {
    return tester.withCatalogReaderFactory(MockCatalogReaderExtended::new);
  }

  private Tester getNoWindowedAggDecompositionTester() {
    //changes the sql2rel config to not decompose windowed aggregations
    return tester.withConvertletTable(
        new StandardConvertletTable(new StandardConvertletTableConfig(false, true)));
  }

  private Tester getNoTimestampdiffDecompositionTester() {
    //changes the sql2rel config to not decompose windowed aggregations
    return tester.withConvertletTable(
        new StandardConvertletTable(new StandardConvertletTableConfig(true, false)));
  }

  @Test void testLarge() {
    // Size factor used to be 400, but lambdas use a lot of stack
    final int x = 300;
    SqlValidatorTest.checkLarge(x, input -> {
      final RelRoot root = tester.convertSqlToRel(input);
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
  @Test void testUserDefinedOrderByOver() {
    String sql = "select deptno,\n"
        + "  rank() over(partition by empno order by deptno)\n"
        + "from emp\n"
        + "order by row_number() over(partition by empno order by deptno)";
    Properties properties = new Properties();
    properties.setProperty(
        CalciteConnectionProperty.DEFAULT_NULL_COLLATION.camelName(),
        NullCollation.LOW.name());
    CalciteConnectionConfigImpl connectionConfig =
        new CalciteConnectionConfigImpl(properties);
    final TesterImpl tester = new TesterImpl(getDiffRepos())
        .withDecorrelation(false)
        .withTrim(false)
        .withContext(c -> Contexts.of(connectionConfig, c));
    sql(sql).with(tester).ok();
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

  /** Test case for:
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3310">[CALCITE-3310]
   * Approximate and exact aggregate calls are recognized as the same
   * during sql-to-rel conversion</a>.
   */
  @Test void testProjectApproximateAndExactAggregates() {
    final String sql = "SELECT empno, count(distinct ename),\n"
            + "approx_count_distinct(ename)\n"
            + "FROM emp\n"
            + "GROUP BY empno";
    sql(sql).ok();
  }

  @Test void testProjectAggregatesIgnoreNullsAndNot() {
    final String sql = "select lead(sal, 4) IGNORE NULLS, lead(sal, 4) over (w)\n"
        + "from emp window w as (order by empno)";
    sql(sql).ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3456">[CALCITE-3456]
   * AssertionError throws when aggregation same digest in sub-query in same
   * scope</a>.
   */
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
    RelNode rel = tester.convertSqlToRel("update emp set sal = ?, ename = ? where empno = ?").rel;
    LogicalTableModify modify = (LogicalTableModify) rel;
    List<RexNode> parameters = modify.getSourceExpressionList();
    assertThat(parameters.size(), is(2));
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

  @Test public void testSortInSubQuery() {
    final String sql = "select * from (select empno from emp order by empno)";
    sql(sql).convertsTo("${planRemoveSort}");
    sql(sql).withConfig(c -> c.withRemoveSortInSubQuery(false)).convertsTo("${planKeepSort}");
  }

  @Test public void testTrimUnionAll() {
    final String sql = ""
        + "select deptno from\n"
        + "(select ename, deptno from emp\n"
        + "union all\n"
        + "select name, deptno from dept)";
    sql(sql).trim(true).ok();
  }

  @Test public void testTrimUnionDistinct() {
    final String sql = ""
        + "select deptno from\n"
        + "(select ename, deptno from emp\n"
        + "union\n"
        + "select name, deptno from dept)";
    sql(sql).trim(true).ok();
  }

  @Test public void testTrimIntersectAll() {
    final String sql = ""
        + "select deptno from\n"
        + "(select ename, deptno from emp\n"
        + "intersect all\n"
        + "select name, deptno from dept)";
    sql(sql).trim(true).ok();
  }

  @Test public void testTrimIntersectDistinct() {
    final String sql = ""
        + "select deptno from\n"
        + "(select ename, deptno from emp\n"
        + "intersect\n"
        + "select name, deptno from dept)";
    sql(sql).trim(true).ok();
  }

  @Test public void testTrimExceptAll() {
    final String sql = ""
        + "select deptno from\n"
        + "(select ename, deptno from emp\n"
        + "except all\n"
        + "select name, deptno from dept)";
    sql(sql).trim(true).ok();
  }

  @Test public void testTrimExceptDistinct() {
    final String sql = ""
        + "select deptno from\n"
        + "(select ename, deptno from emp\n"
        + "except\n"
        + "select name, deptno from dept)";
    sql(sql).trim(true).ok();
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
        .convertsTo("${plan_extended}");
    sql(sql)
        .withConfig(configBuilder -> configBuilder
            .withExpand(false)
            .withDecorrelationEnabled(false))
        .convertsTo("${plan_not_extended}");
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
    sql(sql)
        .withConfig(configBuilder -> configBuilder
            .withDecorrelationEnabled(true)
            .withExpand(true))
        .convertsTo("${plan_extended}");
    sql(sql)
        .withConfig(configBuilder -> configBuilder
            .withDecorrelationEnabled(false)
            .withExpand(false))
        .convertsTo("${plan_not_extended}");
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4295">[CALCITE-4295]
   * Composite of two checker with SqlOperandCountRange throws IllegalArgumentException</a>.
   */
  @Test public void testCompositeOfCountRange() {
    final String sql = ""
        + "select COMPOSITE(deptno)\n"
        + "from dept";
    sql(sql).trim(true).ok();
  }


  @Test public void testInWithConstantList() {
    String expr = "1 in (1,2,3)";
    expr(expr).ok();
  }

  /**
   * Visitor that checks that every {@link RelNode} in a tree is valid.
   *
   * @see RelNode#isValid(Litmus, RelNode.Context)
   */
  public static class RelValidityChecker extends RelVisitor
      implements RelNode.Context {
    int invalidCount;
    final Deque<RelNode> stack = new ArrayDeque<>();

    public Set<CorrelationId> correlationIds() {
      final ImmutableSet.Builder<CorrelationId> builder =
          ImmutableSet.builder();
      for (RelNode r : stack) {
        builder.addAll(r.getVariablesSet());
      }
      return builder.build();
    }

    public void visit(RelNode node, int ordinal, @Nullable RelNode parent) {
      try {
        stack.push(node);
        if (!node.isValid(Litmus.THROW, this)) {
          ++invalidCount;
        }
        super.visit(node, ordinal, parent);
      } finally {
        stack.pop();
      }
    }
  }

  /** Allows fluent testing. */
  public class Sql {
    private final String sql;
    private final boolean decorrelate;
    private final Tester tester;
    private final boolean trim;
    private final UnaryOperator<SqlToRelConverter.Config> config;
    private final SqlConformance conformance;
    private final boolean query;


    Sql(String sql, boolean decorrelate, Tester tester, boolean trim,
        UnaryOperator<SqlToRelConverter.Config> config,
        SqlConformance conformance, boolean query) {
      this.sql = Objects.requireNonNull(sql, "sql");
      if (sql.contains(" \n")) {
        throw new AssertionError("trailing whitespace");
      }
      this.decorrelate = decorrelate;
      this.tester = Objects.requireNonNull(tester, "tester");
      this.trim = trim;
      this.config = Objects.requireNonNull(config, "config");
      this.conformance = Objects.requireNonNull(conformance, "conformance");
      this.query = query;
    }

    public void ok() {
      convertsTo("${plan}");
    }

    public void convertsTo(String plan) {
      tester.withDecorrelation(decorrelate)
          .withConformance(conformance)
          .withConfig(config)
          .withConfig(c -> c.withTrimUnusedFields(true))
          .assertConvertsTo(sql, plan, trim, query);
    }

    public Sql withConfig(UnaryOperator<SqlToRelConverter.Config> config) {
      final UnaryOperator<SqlToRelConverter.Config> config2 =
          this.config.andThen(Objects.requireNonNull(config, "config"))::apply;
      return new Sql(sql, decorrelate, tester, trim, config2, conformance, query);
    }

    public Sql expand(boolean expand) {
      return withConfig(b -> b.withExpand(expand));
    }

    public Sql decorrelate(boolean decorrelate) {
      return new Sql(sql, decorrelate, tester, trim, config, conformance, query);
    }

    public Sql with(Tester tester) {
      return new Sql(sql, decorrelate, tester, trim, config, conformance, query);
    }

    public Sql trim(boolean trim) {
      return new Sql(sql, decorrelate, tester, trim, config, conformance, query);
    }

    public Sql conformance(SqlConformance conformance) {
      return new Sql(sql, decorrelate, tester, trim, config, conformance, query);
    }
  }
}
