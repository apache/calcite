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
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.externalize.RelXmlWriter;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.util.Bug;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.TestUtil;
import org.apache.calcite.util.Util;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;

import org.junit.Ignore;
import org.junit.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Properties;
import java.util.Set;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

/**
 * Unit test for {@link org.apache.calcite.sql2rel.SqlToRelConverter}.
 */
public class SqlToRelConverterTest extends SqlToRelTestBase {
  //~ Methods ----------------------------------------------------------------

  public SqlToRelConverterTest() {
    super();
  }

  protected DiffRepository getDiffRepos() {
    return DiffRepository.lookup(SqlToRelConverterTest.class);
  }

  /** Sets the SQL statement for a test. */
  public final Sql sql(String sql) {
    return new Sql(sql, true, true, tester, false,
        SqlToRelConverter.Config.DEFAULT, tester.getConformance());
  }

  protected final void check(
      String sql,
      String plan) {
    sql(sql).convertsTo(plan);
  }

  @Test public void testIntegerLiteral() {
    final String sql = "select 1 from emp";
    sql(sql).ok();
  }

  @Test public void testIntervalLiteralYearToMonth() {
    final String sql = "select\n"
        + "  cast(empno as Integer) * (INTERVAL '1-1' YEAR TO MONTH)\n"
        + "from emp";
    sql(sql).ok();
  }

  @Test public void testIntervalLiteralHourToMinute() {
    final String sql = "select\n"
        + " cast(empno as Integer) * (INTERVAL '1:1' HOUR TO MINUTE)\n"
        + "from emp";
    sql(sql).ok();
  }

  @Test public void testAliasList() {
    final String sql = "select a + b from (\n"
        + "  select deptno, 1 as uno, name from dept\n"
        + ") as d(a, b, c)\n"
        + "where c like 'X%'";
    sql(sql).ok();
  }

  @Test public void testAliasList2() {
    final String sql = "select * from (\n"
        + "  select a, b, c from (values (1, 2, 3)) as t (c, b, a)\n"
        + ") join dept on dept.deptno = c\n"
        + "order by c + a";
    sql(sql).ok();
  }

  /**
   * Tests that AND(x, AND(y, z)) gets flattened to AND(x, y, z).
   */
  @Test public void testMultiAnd() {
    final String sql = "select * from emp\n"
        + "where deptno < 10\n"
        + "and deptno > 5\n"
        + "and (deptno = 8 or empno < 100)";
    sql(sql).ok();
  }

  @Test public void testJoinOn() {
    final String sql = "SELECT * FROM emp\n"
        + "JOIN dept on emp.deptno = dept.deptno";
    sql(sql).ok();
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-245">[CALCITE-245]
   * Off-by-one translation of ON clause of JOIN</a>.
   */
  @Test public void testConditionOffByOne() {
    // Bug causes the plan to contain
    //   LogicalJoin(condition=[=($9, $9)], joinType=[inner])
    final String sql = "SELECT * FROM emp\n"
        + "JOIN dept on emp.deptno + 0 = dept.deptno";
    sql(sql).ok();
  }

  @Test public void testConditionOffByOneReversed() {
    final String sql = "SELECT * FROM emp\n"
        + "JOIN dept on dept.deptno = emp.deptno + 0";
    sql(sql).ok();
  }

  @Test public void testJoinOnExpression() {
    final String sql = "SELECT * FROM emp\n"
        + "JOIN dept on emp.deptno + 1 = dept.deptno - 2";
    sql(sql).ok();
  }

  @Test public void testJoinOnIn() {
    final String sql = "select * from emp join dept\n"
        + " on emp.deptno = dept.deptno and emp.empno in (1, 3)";
    sql(sql).ok();
  }

  @Test public void testJoinOnInSubQuery() {
    final String sql = "select * from emp left join dept\n"
        + "on emp.empno = 1\n"
        + "or dept.deptno in (select deptno from emp where empno > 5)";
    sql(sql).expand(false).ok();
  }

  @Test public void testJoinOnExists() {
    final String sql = "select * from emp left join dept\n"
        + "on emp.empno = 1\n"
        + "or exists (select deptno from emp where empno > dept.deptno + 5)";
    sql(sql).expand(false).ok();
  }

  @Test public void testJoinUsing() {
    sql("SELECT * FROM emp JOIN dept USING (deptno)").ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-74">[CALCITE-74]
   * JOIN ... USING fails in 3-way join with
   * UnsupportedOperationException</a>. */
  @Test public void testJoinUsingThreeWay() {
    final String sql = "select *\n"
        + "from emp as e\n"
        + "join dept as d using (deptno)\n"
        + "join emp as e2 using (empno)";
    sql(sql).ok();
  }

  @Test public void testJoinUsingCompound() {
    final String sql = "SELECT * FROM emp LEFT JOIN ("
        + "SELECT *, deptno * 5 as empno FROM dept) "
        + "USING (deptno,empno)";
    sql(sql).ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-801">[CALCITE-801]
   * NullPointerException using USING on table alias with column
   * aliases</a>. */
  @Test public void testValuesUsing() {
    final String sql = "select d.deptno, min(e.empid) as empid\n"
        + "from (values (100, 'Bill', 1)) as e(empid, name, deptno)\n"
        + "join (values (1, 'LeaderShip')) as d(deptno, name)\n"
        + "  using (deptno)\n"
        + "group by d.deptno";
    sql(sql).ok();
  }

  @Test public void testJoinNatural() {
    sql("SELECT * FROM emp NATURAL JOIN dept").ok();
  }

  @Test public void testJoinNaturalNoCommonColumn() {
    final String sql = "SELECT *\n"
        + "FROM emp NATURAL JOIN (SELECT deptno AS foo, name FROM dept) AS d";
    sql(sql).ok();
  }

  @Test public void testJoinNaturalMultipleCommonColumn() {
    final String sql = "SELECT *\n"
        + "FROM emp\n"
        + "NATURAL JOIN (SELECT deptno, name AS ename FROM dept) AS d";
    sql(sql).ok();
  }

  @Test public void testJoinWithUnion() {
    final String sql = "select grade\n"
        + "from (select empno from emp union select deptno from dept),\n"
        + "  salgrade";
    sql(sql).ok();
  }

  @Test public void testGroup() {
    sql("select deptno from emp group by deptno").ok();
  }

  @Test public void testGroupByAlias() {
    sql("select empno as d from emp group by d")
        .conformance(SqlConformanceEnum.LENIENT).ok();
  }

  @Test public void testGroupByAliasOfSubExpressionsInProject() {
    final String sql = "select deptno+empno as d, deptno+empno+mgr\n"
        + "from emp group by d,mgr";
    sql(sql)
        .conformance(SqlConformanceEnum.LENIENT).ok();
  }

  @Test public void testGroupByAliasEqualToColumnName() {
    sql("select empno, ename as deptno from emp group by empno, deptno")
        .conformance(SqlConformanceEnum.LENIENT).ok();
  }

  @Test public void testGroupByOrdinal() {
    sql("select empno from emp group by 1")
        .conformance(SqlConformanceEnum.LENIENT).ok();
  }

  @Test public void testGroupByContainsLiterals() {
    final String sql = "select count(*) from (\n"
        + "  select 1 from emp group by substring(ename from 2 for 3))";
    sql(sql)
        .conformance(SqlConformanceEnum.LENIENT).ok();
  }

  @Test public void testAliasInHaving() {
    sql("select count(empno) as e from emp having e > 1")
        .conformance(SqlConformanceEnum.LENIENT).ok();
  }

  @Test public void testGroupJustOneAgg() {
    // just one agg
    final String sql =
        "select deptno, sum(sal) as sum_sal from emp group by deptno";
    sql(sql).ok();
  }

  @Test public void testGroupExpressionsInsideAndOut() {
    // Expressions inside and outside aggs. Common sub-expressions should be
    // eliminated: 'sal' always translates to expression #2.
    final String sql = "select\n"
        + "  deptno + 4, sum(sal), sum(3 + sal), 2 * count(sal)\n"
        + "from emp group by deptno";
    sql(sql).ok();
  }

  @Test public void testAggregateNoGroup() {
    sql("select sum(deptno) from emp").ok();
  }

  @Test public void testGroupEmpty() {
    sql("select sum(deptno) from emp group by ()").ok();
  }

  // Same effect as writing "GROUP BY deptno"
  @Test public void testSingletonGroupingSet() {
    sql("select sum(sal) from emp group by grouping sets (deptno)").ok();
  }

  @Test public void testGroupingSets() {
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
  @Test public void testGroupingSetsWithRollup() {
    final String sql = "select deptno, ename, sum(sal) from emp\n"
        + "group by grouping sets ( rollup(deptno), (ename, deptno))\n"
        + "order by 2";
    sql(sql).ok();
  }

  @Test public void testGroupingSetsWithCube() {
    final String sql = "select deptno, ename, sum(sal) from emp\n"
        + "group by grouping sets ( (deptno), CUBE(ename, deptno))\n"
        + "order by 2";
    sql(sql).ok();
  }

  @Test public void testGroupingSetsWithRollupCube() {
    final String sql = "select deptno, ename, sum(sal) from emp\n"
        + "group by grouping sets ( CUBE(deptno), ROLLUP(ename, deptno))\n"
        + "order by 2";
    sql(sql).ok();
  }

  @Test public void testGroupingSetsProduct() {
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
  @Test public void testGroupingFunctionWithGroupBy() {
    final String sql = "select\n"
        + "  deptno, grouping(deptno), count(*), grouping(empno)\n"
        + "from emp\n"
        + "group by empno, deptno\n"
        + "order by 2";
    sql(sql).ok();
  }

  @Test public void testGroupingFunction() {
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
  @Test public void testGroupByWithDuplicates() {
    sql("select sum(sal) from emp group by (), ()").ok();
  }

  /** GROUP BY with duplicate (and heavily nested) GROUPING SETS. */
  @Test public void testDuplicateGroupingSets() {
    final String sql = "select sum(sal) from emp\n"
        + "group by sal,\n"
        + "  grouping sets (deptno,\n"
        + "    grouping sets ((deptno, ename), ename),\n"
        + "      (ename)),\n"
        + "  ()";
    sql(sql).ok();
  }

  @Test public void testGroupingSetsCartesianProduct() {
    // Equivalent to (a, c), (a, d), (b, c), (b, d)
    final String sql = "select 1\n"
        + "from (values (1, 2, 3, 4)) as t(a, b, c, d)\n"
        + "group by grouping sets (a, b), grouping sets (c, d)";
    sql(sql).ok();
  }

  @Test public void testGroupingSetsCartesianProduct2() {
    final String sql = "select 1\n"
        + "from (values (1, 2, 3, 4)) as t(a, b, c, d)\n"
        + "group by grouping sets (a, (a, b)), grouping sets (c), d";
    sql(sql).ok();
  }

  @Test public void testRollupSimple() {
    // a is nullable so is translated as just "a"
    // b is not null, so is represented as 0 inside Aggregate, then
    // using "CASE WHEN i$b THEN NULL ELSE b END"
    final String sql = "select a, b, count(*) as c\n"
        + "from (values (cast(null as integer), 2)) as t(a, b)\n"
        + "group by rollup(a, b)";
    sql(sql).ok();
  }

  @Test public void testRollup() {
    // Equivalent to {(a, b), (a), ()}  * {(c, d), (c), ()}
    final String sql = "select 1\n"
        + "from (values (1, 2, 3, 4)) as t(a, b, c, d)\n"
        + "group by rollup(a, b), rollup(c, d)";
    sql(sql).ok();
  }

  @Test public void testRollupTuples() {
    // rollup(b, (a, d)) is (b, a, d), (b), ()
    final String sql = "select 1\n"
        + "from (values (1, 2, 3, 4)) as t(a, b, c, d)\n"
        + "group by rollup(b, (a, d))";
    sql(sql).ok();
  }

  @Test public void testCube() {
    // cube(a, b) is {(a, b), (a), (b), ()}
    final String sql = "select 1\n"
        + "from (values (1, 2, 3, 4)) as t(a, b, c, d)\n"
        + "group by cube(a, b)";
    sql(sql).ok();
  }

  @Test public void testGroupingSetsWith() {
    final String sql = "with t(a, b, c, d) as (values (1, 2, 3, 4))\n"
        + "select 1 from t\n"
        + "group by rollup(a, b), rollup(c, d)";
    sql(sql).ok();
  }

  @Test public void testHaving() {
    // empty group-by clause, having
    final String sql = "select sum(sal + sal) from emp having sum(sal) > 10";
    sql(sql).ok();
  }

  @Test public void testGroupBug281() {
    // Dtbug 281 gives:
    //   Internal error:
    //   Type 'RecordType(VARCHAR(128) $f0)' has no field 'NAME'
    final String sql =
        "select name from (select name from dept group by name)";
    sql(sql).ok();
  }

  @Test public void testGroupBug281b() {
    // Try to confuse it with spurious columns.
    final String sql = "select name, foo from (\n"
        + "select deptno, name, count(deptno) as foo\n"
        + "from dept\n"
        + "group by name, deptno, name)";
    sql(sql).ok();
  }

  @Test public void testGroupByExpression() {
    // This used to cause an infinite loop,
    // SqlValidatorImpl.getValidatedNodeType
    // calling getValidatedNodeTypeIfKnown
    // calling getValidatedNodeType.
    final String sql = "select count(*)\n"
        + "from emp\n"
        + "group by substring(ename FROM 1 FOR 1)";
    sql(sql).ok();
  }

  @Test public void testAggDistinct() {
    final String sql = "select deptno, sum(sal), sum(distinct sal), count(*)\n"
        + "from emp\n"
        + "group by deptno";
    sql(sql).ok();
  }

  @Test public void testAggFilter() {
    final String sql = "select\n"
        + "  deptno, sum(sal * 2) filter (where empno < 10), count(*)\n"
        + "from emp\n"
        + "group by deptno";
    sql(sql).ok();
  }

  @Test public void testAggFilterWithIn() {
    final String sql = "select\n"
        + "  deptno, sum(sal * 2) filter (where empno not in (1, 2)), count(*)\n"
        + "from emp\n"
        + "group by deptno";
    sql(sql).ok();
  }

  @Test public void testFakeStar() {
    sql("SELECT * FROM (VALUES (0, 0)) AS T(A, \"*\")").ok();
  }

  @Test public void testSelectDistinct() {
    sql("select distinct sal + 5 from emp").ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-476">[CALCITE-476]
   * DISTINCT flag in windowed aggregates</a>. */
  @Test public void testSelectOverDistinct() {
    // Checks to see if <aggregate>(DISTINCT x) is set and preserved
    // as a flag for the aggregate call.
    final String sql = "select SUM(DISTINCT deptno)\n"
        + "over (ROWS BETWEEN 10 PRECEDING AND CURRENT ROW)\n"
        + "from emp\n";
    sql(sql).ok();
  }

  /** As {@link #testSelectOverDistinct()} but for streaming queries. */
  @Test public void testSelectStreamPartitionDistinct() {
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

  @Test public void testSelectDistinctGroup() {
    sql("select distinct sum(sal) from emp group by deptno").ok();
  }

  /**
   * Tests that if the clause of SELECT DISTINCT contains duplicate
   * expressions, they are only aggregated once.
   */
  @Test public void testSelectDistinctDup() {
    final String sql =
        "select distinct sal + 5, deptno, sal + 5 from emp where deptno < 10";
    sql(sql).ok();
  }

  @Test public void testSelectWithoutFrom() {
    final String sql = "select 2+2";
    sql(sql).ok();
  }

  /** Tests referencing columns from a sub-query that has duplicate column
   * names. I think the standard says that this is illegal. We roll with it,
   * and rename the second column to "e0". */
  @Test public void testDuplicateColumnsInSubQuery() {
    String sql = "select \"e\" from (\n"
        + "select empno as \"e\", deptno as d, 1 as \"e\" from EMP)";
    sql(sql).ok();
  }

  @Test public void testOrder() {
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
  @Test public void testOrderBasedRepeatFields() {
    final String sql = "select empno from emp order by empno DESC, empno ASC";
    sql(sql).ok();
  }

  @Test public void testOrderDescNullsLast() {
    final String sql = "select empno from emp order by empno desc nulls last";
    sql(sql).ok();
  }

  @Test public void testOrderByOrdinalDesc() {
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

  @Test public void testOrderDistinct() {
    // The relexp aggregates by 3 expressions - the 2 select expressions
    // plus the one to sort on. A little inefficient, but acceptable.
    final String sql = "select distinct empno, deptno + 1\n"
            + "from emp order by deptno + 1 + empno";
    sql(sql).ok();
  }

  @Test public void testOrderByNegativeOrdinal() {
    // Regardless of whether sort-by-ordinals is enabled, negative ordinals
    // are treated like ordinary numbers.
    final String sql =
        "select empno + 1, deptno, empno from emp order by -1 desc";
    sql(sql).ok();
  }

  @Test public void testOrderByOrdinalInExpr() {
    // Regardless of whether sort-by-ordinals is enabled, ordinals
    // inside expressions are treated like integers.
    final String sql =
        "select empno + 1, deptno, empno from emp order by 1 + 2 desc";
    sql(sql).ok();
  }

  @Test public void testOrderByIdenticalExpr() {
    // Expression in ORDER BY clause is identical to expression in SELECT
    // clause, so plan should not need an extra project.
    final String sql =
        "select empno + 1 from emp order by deptno asc, empno + 1 desc";
    sql(sql).ok();
  }

  @Test public void testOrderByAlias() {
    final String sql =
        "select empno + 1 as x, empno - 2 as y from emp order by y";
    sql(sql).ok();
  }

  @Test public void testOrderByAliasInExpr() {
    final String sql = "select empno + 1 as x, empno - 2 as y\n"
        + "from emp order by y + 3";
    sql(sql).ok();
  }

  @Test public void testOrderByAliasOverrides() {
    if (!tester.getConformance().isSortByAlias()) {
      return;
    }

    // plan should contain '(empno + 1) + 3'
    final String sql = "select empno + 1 as empno, empno - 2 as y\n"
        + "from emp order by empno + 3";
    sql(sql).ok();
  }

  @Test public void testOrderByAliasDoesNotOverride() {
    if (tester.getConformance().isSortByAlias()) {
      return;
    }

    // plan should contain 'empno + 3', not '(empno + 1) + 3'
    final String sql = "select empno + 1 as empno, empno - 2 as y\n"
        + "from emp order by empno + 3";
    sql(sql).ok();
  }

  @Test public void testOrderBySameExpr() {
    final String sql = "select empno from emp, dept\n"
        + "order by sal + empno desc, sal * empno, sal + empno desc";
    sql(sql).ok();
  }

  @Test public void testOrderUnion() {
    final String sql = "select empno, sal from emp\n"
        + "union all\n"
        + "select deptno, deptno from dept\n"
        + "order by sal desc, empno asc";
    sql(sql).ok();
  }

  @Test public void testOrderUnionOrdinal() {
    if (!tester.getConformance().isSortByOrdinal()) {
      return;
    }
    final String sql = "select empno, sal from emp\n"
        + "union all\n"
        + "select deptno, deptno from dept\n"
        + "order by 2";
    sql(sql).ok();
  }

  @Test public void testOrderUnionExprs() {
    final String sql = "select empno, sal from emp\n"
        + "union all\n"
        + "select deptno, deptno from dept\n"
        + "order by empno * sal + 2";
    sql(sql).ok();
  }

  @Test public void testOrderOffsetFetch() {
    final String sql = "select empno from emp\n"
        + "order by empno offset 10 rows fetch next 5 rows only";
    sql(sql).ok();
  }

  @Test public void testOrderOffsetFetchWithDynamicParameter() {
    final String sql = "select empno from emp\n"
        + "order by empno offset ? rows fetch next ? rows only";
    sql(sql).ok();
  }

  @Test public void testOffsetFetch() {
    final String sql = "select empno from emp\n"
        + "offset 10 rows fetch next 5 rows only";
    sql(sql).ok();
  }

  @Test public void testOffsetFetchWithDynamicParameter() {
    final String sql = "select empno from emp\n"
        + "offset ? rows fetch next ? rows only";
    sql(sql).ok();
  }

  @Test public void testOffset() {
    final String sql = "select empno from emp offset 10 rows";
    sql(sql).ok();
  }

  @Test public void testOffsetWithDynamicParameter() {
    final String sql = "select empno from emp offset ? rows";
    sql(sql).ok();
  }

  @Test public void testFetch() {
    final String sql = "select empno from emp fetch next 5 rows only";
    sql(sql).ok();
  }

  @Test public void testFetchWithDynamicParameter() {
    final String sql = "select empno from emp fetch next ? rows only";
    sql(sql).ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-439">[CALCITE-439]
   * SqlValidatorUtil.uniquify() may not terminate under some
   * conditions</a>. */
  @Test public void testGroupAlias() {
    final String sql = "select \"$f2\", max(x), max(x + 1)\n"
        + "from (values (1, 2)) as t(\"$f2\", x)\n"
        + "group by \"$f2\"";
    sql(sql).ok();
  }

  @Test public void testOrderGroup() {
    final String sql = "select deptno, count(*)\n"
        + "from emp\n"
        + "group by deptno\n"
        + "order by deptno * sum(sal) desc, min(empno)";
    sql(sql).ok();
  }

  @Test public void testCountNoGroup() {
    final String sql = "select count(*), sum(sal)\n"
        + "from emp\n"
        + "where empno > 10";
    sql(sql).ok();
  }

  @Test public void testWith() {
    final String sql = "with emp2 as (select * from emp)\n"
        + "select * from emp2";
    sql(sql).ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-309">[CALCITE-309]
   * WITH ... ORDER BY query gives AssertionError</a>. */
  @Test public void testWithOrder() {
    final String sql = "with emp2 as (select * from emp)\n"
        + "select * from emp2 order by deptno";
    sql(sql).ok();
  }

  @Test public void testWithUnionOrder() {
    final String sql = "with emp2 as (select empno, deptno as x from emp)\n"
        + "select * from emp2\n"
        + "union all\n"
        + "select * from emp2\n"
        + "order by empno + x";
    sql(sql).ok();
  }

  @Test public void testWithUnion() {
    final String sql = "with emp2 as (select * from emp where deptno > 10)\n"
        + "select empno from emp2 where deptno < 30\n"
        + "union all\n"
        + "select deptno from emp";
    sql(sql).ok();
  }

  @Test public void testWithAlias() {
    final String sql = "with w(x, y) as\n"
        + "  (select * from dept where deptno > 10)\n"
        + "select x from w where x < 30 union all select deptno from dept";
    sql(sql).ok();
  }

  @Test public void testWithInsideWhereExists() {
    final String sql = "select * from emp\n"
        + "where exists (\n"
        + "  with dept2 as (select * from dept where dept.deptno >= emp.deptno)\n"
        + "  select 1 from dept2 where deptno <= emp.deptno)";
    sql(sql).decorrelate(false).ok();
  }

  @Test public void testWithInsideWhereExistsRex() {
    final String sql = "select * from emp\n"
        + "where exists (\n"
        + "  with dept2 as (select * from dept where dept.deptno >= emp.deptno)\n"
        + "  select 1 from dept2 where deptno <= emp.deptno)";
    sql(sql).decorrelate(false).expand(false).ok();
  }

  @Test public void testWithInsideWhereExistsDecorrelate() {
    final String sql = "select * from emp\n"
        + "where exists (\n"
        + "  with dept2 as (select * from dept where dept.deptno >= emp.deptno)\n"
        + "  select 1 from dept2 where deptno <= emp.deptno)";
    sql(sql).decorrelate(true).ok();
  }

  @Test public void testWithInsideWhereExistsDecorrelateRex() {
    final String sql = "select * from emp\n"
        + "where exists (\n"
        + "  with dept2 as (select * from dept where dept.deptno >= emp.deptno)\n"
        + "  select 1 from dept2 where deptno <= emp.deptno)";
    sql(sql).decorrelate(true).expand(false).ok();
  }

  @Test public void testWithInsideScalarSubQuery() {
    final String sql = "select (\n"
        + " with dept2 as (select * from dept where deptno > 10)"
        + " select count(*) from dept2) as c\n"
        + "from emp";
    sql(sql).ok();
  }

  @Test public void testWithInsideScalarSubQueryRex() {
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
  @Test public void testWithExists() {
    final String sql = "with t (a, b) as (select * from (values (1, 2)))\n"
        + "select * from t where exists (\n"
        + "  select 1 from emp where deptno = t.a)";
    sql(sql).ok();
  }

  @Test public void testTableSubset() {
    final String sql = "select deptno, name from dept";
    sql(sql).ok();
  }

  @Test public void testTableExpression() {
    final String sql = "select deptno + deptno from dept";
    sql(sql).ok();
  }

  @Test public void testTableExtend() {
    final String sql = "select * from dept extend (x varchar(5) not null)";
    sql(sql).ok();
  }

  @Test public void testTableExtendSubset() {
    final String sql = "select deptno, x from dept extend (x int)";
    sql(sql).ok();
  }

  @Test public void testTableExtendExpression() {
    final String sql = "select deptno + x from dept extend (x int not null)";
    sql(sql).ok();
  }

  @Test public void testModifiableViewExtend() {
    final String sql = "select *\n"
        + "from EMP_MODIFIABLEVIEW extend (x varchar(5) not null)";
    sql(sql).with(getExtendedTester()).ok();
  }

  @Test public void testModifiableViewExtendSubset() {
    final String sql = "select x, empno\n"
        + "from EMP_MODIFIABLEVIEW extend (x varchar(5) not null)";
    sql(sql).with(getExtendedTester()).ok();
  }

  @Test public void testModifiableViewExtendExpression() {
    final String sql = "select empno + x\n"
        + "from EMP_MODIFIABLEVIEW extend (x int not null)";
    sql(sql).with(getExtendedTester()).ok();
  }

  @Test public void testSelectViewExtendedColumnCollision() {
    sql("select ENAME, EMPNO, JOB, SLACKER, SAL, HIREDATE, MGR\n"
        + " from EMP_MODIFIABLEVIEW3\n"
        + " where SAL = 20").with(getExtendedTester()).ok();
    sql("select ENAME, EMPNO, JOB, SLACKER, SAL, HIREDATE, MGR\n"
        + " from EMP_MODIFIABLEVIEW3 extend (SAL int)\n"
        + " where SAL = 20").with(getExtendedTester()).ok();
  }

  @Test public void testSelectViewExtendedColumnCaseSensitiveCollision() {
    sql("select ENAME, EMPNO, JOB, SLACKER, \"sal\", HIREDATE, MGR\n"
        + " from EMP_MODIFIABLEVIEW3 extend (\"sal\" boolean)\n"
        + " where \"sal\" = true").with(getExtendedTester()).ok();
  }

  @Test public void testSelectViewExtendedColumnExtendedCollision() {
    sql("select ENAME, EMPNO, JOB, SLACKER, SAL, HIREDATE, EXTRA\n"
        + " from EMP_MODIFIABLEVIEW2\n"
        + " where SAL = 20").with(getExtendedTester()).ok();
    sql("select ENAME, EMPNO, JOB, SLACKER, SAL, HIREDATE, EXTRA\n"
        + " from EMP_MODIFIABLEVIEW2 extend (EXTRA boolean)\n"
        + " where SAL = 20").with(getExtendedTester()).ok();
  }

  @Test public void testSelectViewExtendedColumnCaseSensitiveExtendedCollision() {
    sql("select ENAME, EMPNO, JOB, SLACKER, SAL, HIREDATE, \"extra\"\n"
        + " from EMP_MODIFIABLEVIEW2 extend (\"extra\" boolean)\n"
        + " where \"extra\" = false").with(getExtendedTester()).ok();
  }

  @Test public void testSelectViewExtendedColumnUnderlyingCollision() {
    sql("select ENAME, EMPNO, JOB, SLACKER, SAL, HIREDATE, MGR, COMM\n"
        + " from EMP_MODIFIABLEVIEW3 extend (COMM int)\n"
        + " where SAL = 20").with(getExtendedTester()).ok();
  }

  @Test public void testSelectViewExtendedColumnCaseSensitiveUnderlyingCollision() {
    sql("select ENAME, EMPNO, JOB, SLACKER, SAL, HIREDATE, MGR, \"comm\"\n"
        + " from EMP_MODIFIABLEVIEW3 extend (\"comm\" int)\n"
        + " where \"comm\" = 20").with(getExtendedTester()).ok();
  }

  @Test public void testUpdateExtendedColumnCollision() {
    sql("update empdefaults(empno INTEGER NOT NULL, deptno INTEGER)"
        + " set deptno = 1, empno = 20, ename = 'Bob'"
        + " where deptno = 10").ok();
  }

  @Test public void testUpdateExtendedColumnCaseSensitiveCollision() {
    sql("update empdefaults(\"slacker\" INTEGER, deptno INTEGER)"
        + " set deptno = 1, \"slacker\" = 100"
        + " where ename = 'Bob'").ok();
  }

  @Test public void testUpdateExtendedColumnModifiableViewCollision() {
    sql("update EMP_MODIFIABLEVIEW3(empno INTEGER NOT NULL, deptno INTEGER)"
        + " set deptno = 20, empno = 20, ename = 'Bob'"
        + " where empno = 10").with(getExtendedTester()).ok();
  }

  @Test public void testUpdateExtendedColumnModifiableViewCaseSensitiveCollision() {
    sql("update EMP_MODIFIABLEVIEW2(\"slacker\" INTEGER, deptno INTEGER)"
        + " set deptno = 20, \"slacker\" = 100"
        + " where ename = 'Bob'").with(getExtendedTester()).ok();
  }

  @Test public void testUpdateExtendedColumnModifiableViewExtendedCollision() {
    sql("update EMP_MODIFIABLEVIEW2(\"slacker\" INTEGER, extra BOOLEAN)"
        + " set deptno = 20, \"slacker\" = 100, extra = true"
        + " where ename = 'Bob'").with(getExtendedTester()).ok();
  }

  @Test public void testUpdateExtendedColumnModifiableViewExtendedCaseSensitiveCollision() {
    sql("update EMP_MODIFIABLEVIEW2(\"extra\" INTEGER, extra BOOLEAN)"
        + " set deptno = 20, \"extra\" = 100, extra = true"
        + " where ename = 'Bob'").with(getExtendedTester()).ok();
  }

  @Test public void testUpdateExtendedColumnModifiableViewUnderlyingCollision() {
    sql("update EMP_MODIFIABLEVIEW3(extra BOOLEAN, comm INTEGER)"
        + " set empno = 20, comm = true, extra = true"
        + " where ename = 'Bob'").with(getExtendedTester()).ok();
  }

  @Test public void testSelectModifiableViewConstraint() {
    final String sql = "select deptno from EMP_MODIFIABLEVIEW2\n"
        + "where deptno = ?";
    sql(sql).with(getExtendedTester()).ok();
  }

  @Test public void testModifiableViewDdlExtend() {
    final String sql = "select extra from EMP_MODIFIABLEVIEW2";
    sql(sql).with(getExtendedTester()).ok();
  }

  @Test public void testExplicitTable() {
    sql("table emp").ok();
  }

  @Test public void testCollectionTable() {
    sql("select * from table(ramp(3))").ok();
  }

  @Test public void testCollectionTableWithLateral() {
    sql("select * from dept, lateral table(ramp(dept.deptno))").ok();
  }

  @Test public void testCollectionTableWithLateral2() {
    sql("select * from dept, lateral table(ramp(deptno))").ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1732">[CALCITE-1732]
   * IndexOutOfBoundsException when using LATERAL TABLE with more than one
   * field</a>. */
  @Test public void testCollectionTableWithLateral3() {
    sql("select * from dept, lateral table(DEDUP(dept.deptno, dept.name))").ok();
  }

  @Test public void testSample() {
    final String sql =
        "select * from emp tablesample substitute('DATASET1') where empno > 5";
    sql(sql).ok();
  }

  @Test public void testSampleQuery() {
    final String sql = "select * from (\n"
        + " select * from emp as e tablesample substitute('DATASET1')\n"
        + " join dept on e.deptno = dept.deptno\n"
        + ") tablesample substitute('DATASET2')\n"
        + "where empno > 5";
    sql(sql).ok();
  }

  @Test public void testSampleBernoulli() {
    final String sql =
        "select * from emp tablesample bernoulli(50) where empno > 5";
    sql(sql).ok();
  }

  @Test public void testSampleBernoulliQuery() {
    final String sql = "select * from (\n"
        + " select * from emp as e tablesample bernoulli(10) repeatable(1)\n"
        + " join dept on e.deptno = dept.deptno\n"
        + ") tablesample bernoulli(50) repeatable(99)\n"
        + "where empno > 5";
    sql(sql).ok();
  }

  @Test public void testSampleSystem() {
    final String sql =
        "select * from emp tablesample system(50) where empno > 5";
    sql(sql).ok();
  }

  @Test public void testSampleSystemQuery() {
    final String sql = "select * from (\n"
        + " select * from emp as e tablesample system(10) repeatable(1)\n"
        + " join dept on e.deptno = dept.deptno\n"
        + ") tablesample system(50) repeatable(99)\n"
        + "where empno > 5";
    sql(sql).ok();
  }

  @Test public void testCollectionTableWithCursorParam() {
    final String sql = "select * from table(dedup("
        + "cursor(select ename from emp),"
        + " cursor(select name from dept), 'NAME'))";
    sql(sql).decorrelate(false).ok();
  }

  @Test public void testUnnest() {
    final String sql = "select*from unnest(multiset[1,2])";
    sql(sql).ok();
  }

  @Test public void testUnnestSubQuery() {
    final String sql = "select*from unnest(multiset(select*from dept))";
    sql(sql).ok();
  }

  @Test public void testUnnestArrayAggPlan() {
    final String sql = "select d.deptno, e2.empno_avg\n"
        + "from dept_nested as d outer apply\n"
        + " (select avg(e.empno) as empno_avg from UNNEST(d.employees) as e) e2";
    sql(sql).conformance(SqlConformanceEnum.LENIENT).ok();
  }

  @Test public void testUnnestArrayPlan() {
    final String sql = "select d.deptno, e2.empno\n"
        + "from dept_nested as d,\n"
        + " UNNEST(d.employees) e2";
    sql(sql).with(getExtendedTester()).ok();
  }

  @Test public void testUnnestArrayPlanAs() {
    final String sql = "select d.deptno, e2.empno\n"
        + "from dept_nested as d,\n"
        + " UNNEST(d.employees) as e2(empno, y, z)";
    sql(sql).with(getExtendedTester()).ok();
  }

  @Test public void testArrayOfRecord() {
    sql("select employees[1].detail.skills[2+3].desc from dept_nested").ok();
  }

  @Test public void testFlattenRecords() {
    sql("select employees[1] from dept_nested").ok();
  }

  @Test public void testUnnestArray() {
    sql("select*from unnest(array(select*from dept))").ok();
  }

  @Test public void testUnnestWithOrdinality() {
    final String sql =
        "select*from unnest(array(select*from dept)) with ordinality";
    sql(sql).ok();
  }

  @Test public void testMultisetSubQuery() {
    final String sql =
        "select multiset(select deptno from dept) from (values(true))";
    sql(sql).ok();
  }

  @Test public void testMultiset() {
    final String sql = "select 'a',multiset[10] from dept";
    sql(sql).ok();
  }

  @Test public void testMultisetOfColumns() {
    final String sql = "select 'abc',multiset[deptno,sal] from emp";
    sql(sql).expand(true).ok();
  }

  @Test public void testMultisetOfColumnsRex() {
    sql("select 'abc',multiset[deptno,sal] from emp").ok();
  }

  @Test public void testCorrelationJoin() {
    final String sql = "select *,\n"
        + "  multiset(select * from emp where deptno=dept.deptno) as empset\n"
        + "from dept";
    sql(sql).ok();
  }

  @Test public void testCorrelationJoinRex() {
    final String sql = "select *,\n"
        + "  multiset(select * from emp where deptno=dept.deptno) as empset\n"
        + "from dept";
    sql(sql).expand(false).ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-864">[CALCITE-864]
   * Correlation variable has incorrect row type if it is populated by right
   * side of a Join</a>. */
  @Test public void testCorrelatedSubQueryInJoin() {
    final String sql = "select *\n"
        + "from emp as e\n"
        + "join dept as d using (deptno)\n"
        + "where d.name = (\n"
        + "  select max(name)\n"
        + "  from dept as d2\n"
        + "  where d2.deptno = d.deptno)";
    sql(sql).expand(false).ok();
  }

  @Test public void testExists() {
    final String sql = "select*from emp\n"
        + "where exists (select 1 from dept where deptno=55)";
    sql(sql).ok();
  }

  @Test public void testExistsCorrelated() {
    final String sql = "select*from emp where exists (\n"
        + "  select 1 from dept where emp.deptno=dept.deptno)";
    sql(sql).decorrelate(false).ok();
  }

  @Test public void testNotExistsCorrelated() {
    final String sql = "select * from emp where not exists (\n"
        + "  select 1 from dept where emp.deptno=dept.deptno)";
    sql(sql).decorrelate(false).ok();
  }

  @Test public void testExistsCorrelatedDecorrelate() {
    final String sql = "select*from emp where exists (\n"
        + "  select 1 from dept where emp.deptno=dept.deptno)";
    sql(sql).decorrelate(true).ok();
  }

  @Test public void testExistsCorrelatedDecorrelateRex() {
    final String sql = "select*from emp where exists (\n"
        + "  select 1 from dept where emp.deptno=dept.deptno)";
    sql(sql).decorrelate(true).expand(false).ok();
  }

  @Test public void testExistsCorrelatedLimit() {
    final String sql = "select*from emp where exists (\n"
        + "  select 1 from dept where emp.deptno=dept.deptno limit 1)";
    sql(sql).decorrelate(false).ok();
  }

  @Test public void testExistsCorrelatedLimitDecorrelate() {
    final String sql = "select*from emp where exists (\n"
        + "  select 1 from dept where emp.deptno=dept.deptno limit 1)";
    sql(sql).decorrelate(true).expand(true).ok();
  }

  @Test public void testExistsCorrelatedLimitDecorrelateRex() {
    final String sql = "select*from emp where exists (\n"
        + "  select 1 from dept where emp.deptno=dept.deptno limit 1)";
    sql(sql).decorrelate(true).expand(false).ok();
  }

  @Test public void testInValueListShort() {
    final String sql = "select empno from emp where deptno in (10, 20)";
    sql(sql).ok();
    sql(sql).expand(false).ok();
  }

  @Test public void testInValueListLong() {
    // Go over the default threshold of 20 to force a sub-query.
    final String sql = "select empno from emp where deptno in"
        + " (10, 20, 30, 40, 50, 60, 70, 80, 90, 100"
        + ", 110, 120, 130, 140, 150, 160, 170, 180, 190"
        + ", 200, 210, 220, 230)";
    sql(sql).ok();
  }

  @Test public void testInUncorrelatedSubQuery() {
    final String sql = "select empno from emp where deptno in"
        + " (select deptno from dept)";
    sql(sql).ok();
  }

  @Test public void testInUncorrelatedSubQueryRex() {
    final String sql = "select empno from emp where deptno in"
        + " (select deptno from dept)";
    sql(sql).expand(false).ok();
  }

  @Test public void testCompositeInUncorrelatedSubQueryRex() {
    final String sql = "select empno from emp where (empno, deptno) in"
        + " (select deptno - 10, deptno from dept)";
    sql(sql).expand(false).ok();
  }

  @Test public void testNotInUncorrelatedSubQuery() {
    final String sql = "select empno from emp where deptno not in"
        + " (select deptno from dept)";
    sql(sql).ok();
  }

  @Test public void testAllValueList() {
    final String sql = "select empno from emp where deptno > all (10, 20)";
    sql(sql).expand(false).ok();
  }

  @Test public void testSomeValueList() {
    final String sql = "select empno from emp where deptno > some (10, 20)";
    sql(sql).expand(false).ok();
  }

  @Test public void testSome() {
    final String sql = "select empno from emp where deptno > some (\n"
        + "  select deptno from dept)";
    sql(sql).expand(false).ok();
  }

  @Test public void testNotInUncorrelatedSubQueryRex() {
    final String sql = "select empno from emp where deptno not in"
        + " (select deptno from dept)";
    sql(sql).expand(false).ok();
  }

  @Test public void testWhereInCorrelated() {
    final String sql = "select empno from emp as e\n"
        + "join dept as d using (deptno)\n"
        + "where e.sal in (\n"
        + "  select e2.sal from emp as e2 where e2.deptno > e.deptno)";
    sql(sql).expand(false).ok();
  }

  @Test public void testInUncorrelatedSubQueryInSelect() {
    // In the SELECT clause, the value of IN remains in 3-valued logic
    // -- it's not forced into 2-valued by the "... IS TRUE" wrapper as in the
    // WHERE clause -- so the translation is more complicated.
    final String sql = "select name, deptno in (\n"
        + "  select case when true then deptno else null end from emp)\n"
        + "from dept";
    sql(sql).ok();
  }

  @Test public void testInUncorrelatedSubQueryInSelectRex() {
    // In the SELECT clause, the value of IN remains in 3-valued logic
    // -- it's not forced into 2-valued by the "... IS TRUE" wrapper as in the
    // WHERE clause -- so the translation is more complicated.
    final String sql = "select name, deptno in (\n"
        + "  select case when true then deptno else null end from emp)\n"
        + "from dept";
    sql(sql).expand(false).ok();
  }

  @Test public void testInUncorrelatedSubQueryInHavingRex() {
    final String sql = "select sum(sal) as s\n"
        + "from emp\n"
        + "group by deptno\n"
        + "having count(*) > 2\n"
        + "and deptno in (\n"
        + "  select case when true then deptno else null end from emp)";
    sql(sql).expand(false).ok();
  }

  @Test public void testUncorrelatedScalarSubQueryInOrderRex() {
    final String sql = "select ename\n"
        + "from emp\n"
        + "order by (select case when true then deptno else null end from emp) desc,\n"
        + "  ename";
    sql(sql).expand(false).ok();
  }

  @Test public void testUncorrelatedScalarSubQueryInGroupOrderRex() {
    final String sql = "select sum(sal) as s\n"
        + "from emp\n"
        + "group by deptno\n"
        + "order by (select case when true then deptno else null end from emp) desc,\n"
        + "  count(*)";
    sql(sql).expand(false).ok();
  }

  @Test public void testUncorrelatedScalarSubQueryInAggregateRex() {
    final String sql = "select sum((select min(deptno) from emp)) as s\n"
        + "from emp\n"
        + "group by deptno\n";
    sql(sql).expand(false).ok();
  }

  /** Plan should be as {@link #testInUncorrelatedSubQueryInSelect}, but with
   * an extra NOT. Both queries require 3-valued logic. */
  @Test public void testNotInUncorrelatedSubQueryInSelect() {
    final String sql = "select empno, deptno not in (\n"
        + "  select case when true then deptno else null end from dept)\n"
        + "from emp";
    sql(sql).ok();
  }

  @Test public void testNotInUncorrelatedSubQueryInSelectRex() {
    final String sql = "select empno, deptno not in (\n"
        + "  select case when true then deptno else null end from dept)\n"
        + "from emp";
    sql(sql).expand(false).ok();
  }

  /** Since 'deptno NOT IN (SELECT deptno FROM dept)' can not be null, we
   * generate a simpler plan. */
  @Test public void testNotInUncorrelatedSubQueryInSelectNotNull() {
    final String sql = "select empno, deptno not in (\n"
        + "  select deptno from dept)\n"
        + "from emp";
    sql(sql).ok();
  }

  /** Since 'deptno NOT IN (SELECT mgr FROM emp)' can be null, we need a more
   * complex plan, including counts of null and not-null keys. */
  @Test public void testNotInUncorrelatedSubQueryInSelectMayBeNull() {
    final String sql = "select empno, deptno not in (\n"
        + "  select mgr from emp)\n"
        + "from emp";
    sql(sql).ok();
  }

  /** Even though "mgr" allows nulls, we can deduce from the WHERE clause that
   * it will never be null. Therefore we can generate a simpler plan. */
  @Test public void testNotInUncorrelatedSubQueryInSelectDeduceNotNull() {
    final String sql = "select empno, deptno not in (\n"
        + "  select mgr from emp where mgr > 5)\n"
        + "from emp";
    sql(sql).ok();
  }

  /** Similar to {@link #testNotInUncorrelatedSubQueryInSelectDeduceNotNull()},
   * using {@code IS NOT NULL}. */
  @Test public void testNotInUncorrelatedSubQueryInSelectDeduceNotNull2() {
    final String sql = "select empno, deptno not in (\n"
        + "  select mgr from emp where mgr is not null)\n"
        + "from emp";
    sql(sql).ok();
  }

  /** Similar to {@link #testNotInUncorrelatedSubQueryInSelectDeduceNotNull()},
   * using {@code IN}. */
  @Test public void testNotInUncorrelatedSubQueryInSelectDeduceNotNull3() {
    final String sql = "select empno, deptno not in (\n"
        + "  select mgr from emp where mgr in (\n"
        + "    select mgr from emp where deptno = 10))\n"
        + "from emp";
    sql(sql).ok();
  }

  @Test public void testNotInUncorrelatedSubQueryInSelectNotNullRex() {
    final String sql = "select empno, deptno not in (\n"
        + "  select deptno from dept)\n"
        + "from emp";
    sql(sql).expand(false).ok();
  }

  @Test public void testUnnestSelect() {
    final String sql = "select*from unnest(select multiset[deptno] from dept)";
    sql(sql).expand(true).ok();
  }

  @Test public void testUnnestSelectRex() {
    final String sql = "select*from unnest(select multiset[deptno] from dept)";
    sql(sql).expand(false).ok();
  }

  @Test public void testJoinUnnest() {
    final String sql = "select*from dept as d, unnest(multiset[d.deptno * 2])";
    sql(sql).ok();
  }

  @Test public void testJoinUnnestRex() {
    final String sql = "select*from dept as d, unnest(multiset[d.deptno * 2])";
    sql(sql).expand(false).ok();
  }

  @Test public void testLateral() {
    final String sql = "select * from emp,\n"
        + "  LATERAL (select * from dept where emp.deptno=dept.deptno)";
    sql(sql).decorrelate(false).ok();
  }

  @Test public void testLateralDecorrelate() {
    final String sql = "select * from emp,\n"
        + " LATERAL (select * from dept where emp.deptno=dept.deptno)";
    sql(sql).decorrelate(true).expand(true).ok();
  }

  @Test public void testLateralDecorrelateRex() {
    final String sql = "select * from emp,\n"
        + " LATERAL (select * from dept where emp.deptno=dept.deptno)";
    sql(sql).decorrelate(true).ok();
  }

  @Test public void testLateralDecorrelateThetaRex() {
    final String sql = "select * from emp,\n"
        + " LATERAL (select * from dept where emp.deptno < dept.deptno)";
    sql(sql).decorrelate(true).ok();
  }

  @Test public void testNestedCorrelations() {
    final String sql = "select *\n"
        + "from (select 2+deptno d2, 3+deptno d3 from emp) e\n"
        + " where exists (select 1 from (select deptno+1 d1 from dept) d\n"
        + " where d1=e.d2 and exists (select 2 from (select deptno+4 d4, deptno+5 d5, deptno+6 d6 from dept)\n"
        + " where d4=d.d1 and d5=d.d1 and d6=e.d3))";
    sql(sql).decorrelate(false).ok();
  }

  @Test public void testNestedCorrelationsDecorrelated() {
    final String sql = "select *\n"
        + "from (select 2+deptno d2, 3+deptno d3 from emp) e\n"
        + " where exists (select 1 from (select deptno+1 d1 from dept) d\n"
        + " where d1=e.d2 and exists (select 2 from (select deptno+4 d4, deptno+5 d5, deptno+6 d6 from dept)\n"
        + " where d4=d.d1 and d5=d.d1 and d6=e.d3))";
    sql(sql).decorrelate(true).expand(true).ok();
  }

  @Test public void testNestedCorrelationsDecorrelatedRex() {
    final String sql = "select *\n"
        + "from (select 2+deptno d2, 3+deptno d3 from emp) e\n"
        + " where exists (select 1 from (select deptno+1 d1 from dept) d\n"
        + " where d1=e.d2 and exists (select 2 from (select deptno+4 d4, deptno+5 d5, deptno+6 d6 from dept)\n"
        + " where d4=d.d1 and d5=d.d1 and d6=e.d3))";
    sql(sql).decorrelate(true).ok();
  }

  @Test public void testElement() {
    sql("select element(multiset[5]) from emp").ok();
  }

  @Test public void testElementInValues() {
    sql("values element(multiset[5])").ok();
  }

  @Test public void testUnionAll() {
    final String sql =
        "select empno from emp union all select deptno from dept";
    sql(sql).ok();
  }

  @Test public void testUnion() {
    final String sql =
        "select empno from emp union select deptno from dept";
    sql(sql).ok();
  }

  @Test public void testUnionValues() {
    // union with values
    final String sql = "values (10), (20)\n"
        + "union all\n"
        + "select 34 from emp\n"
        + "union all values (30), (45 + 10)";
    sql(sql).ok();
  }

  @Test public void testUnionSubQuery() {
    // union of sub-query, inside from list, also values
    final String sql = "select deptno from emp as emp0 cross join\n"
        + " (select empno from emp union all\n"
        + "  select deptno from dept where deptno > 20 union all\n"
        + "  values (45), (67))";
    sql(sql).ok();
  }

  @Test public void testIsDistinctFrom() {
    final String sql = "select empno is distinct from deptno\n"
        + "from (values (cast(null as int), 1),\n"
        + "             (2, cast(null as int))) as emp(empno, deptno)";
    sql(sql).ok();
  }

  @Test public void testIsNotDistinctFrom() {
    final String sql = "select empno is not distinct from deptno\n"
        + "from (values (cast(null as int), 1),\n"
        + "             (2, cast(null as int))) as emp(empno, deptno)";
    sql(sql).ok();
  }

  @Test public void testNotLike() {
    // note that 'x not like y' becomes 'not(x like y)'
    final String sql = "values ('a' not like 'b' escape 'c')";
    sql(sql).ok();
  }

  @Test public void testTumble() {
    final String sql = "select STREAM\n"
        + "  TUMBLE_START(rowtime, INTERVAL '1' MINUTE) AS s,\n"
        + "  TUMBLE_END(rowtime, INTERVAL '1' MINUTE) AS e\n"
        + "from Shipments\n"
        + "GROUP BY TUMBLE(rowtime, INTERVAL '1' MINUTE)";
    sql(sql).ok();
  }

  @Test public void testNotNotIn() {
    final String sql = "select * from EMP where not (ename not in ('Fred') )";
    sql(sql).ok();
  }

  @Test public void testOverMultiple() {
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

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-750">[CALCITE-750]
   * Allow windowed aggregate on top of regular aggregate</a>. */
  @Test public void testNestedAggregates() {
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
  @Test public void testCase() {
    sql("values (case 'a' when 'a' then 1 end)").ok();
  }

  /**
   * Tests one of the custom conversions which is recognized by the identity
   * of the operator (in this case,
   * {@link org.apache.calcite.sql.fun.SqlStdOperatorTable#CHARACTER_LENGTH}).
   */
  @Test public void testCharLength() {
    // Note that CHARACTER_LENGTH becomes CHAR_LENGTH.
    sql("values (character_length('foo'))").ok();
  }

  @Test public void testOverAvg() {
    // AVG(x) gets translated to SUM(x)/COUNT(x).  Because COUNT controls
    // the return type there usually needs to be a final CAST to get the
    // result back to match the type of x.
    final String sql = "select sum(sal) over w1,\n"
        + "  avg(sal) over w1\n"
        + "from emp\n"
        + "window w1 as (partition by job order by hiredate rows 2 preceding)";
    sql(sql).ok();
  }

  @Test public void testOverAvg2() {
    // Check to see if extra CAST is present.  Because CAST is nested
    // inside AVG it passed to both SUM and COUNT so the outer final CAST
    // isn't needed.
    final String sql = "select sum(sal) over w1,\n"
        + "  avg(CAST(sal as real)) over w1\n"
        + "from emp\n"
        + "window w1 as (partition by job order by hiredate rows 2 preceding)";
    sql(sql).ok();
  }

  @Test public void testOverCountStar() {
    final String sql = "select count(sal) over w1,\n"
        + "  count(*) over w1\n"
        + "from emp\n"
        + "window w1 as (partition by job order by hiredate rows 2 preceding)";
    sql(sql).ok();
  }

  /**
   * Tests that a window containing only ORDER BY is implicitly CURRENT ROW.
   */
  @Test public void testOverOrderWindow() {
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
   * Tests that a window with a FOLLOWING bound becomes BETWEEN CURRENT ROW
   * AND FOLLOWING.
   */
  @Test public void testOverOrderFollowingWindow() {
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

  @Test public void testTumbleTable() {
    final String sql = "select stream"
        + " tumble_end(rowtime, interval '2' hour) as rowtime, productId\n"
        + "from orders\n"
        + "group by tumble(rowtime, interval '2' hour), productId";
    sql(sql).ok();
  }

  /** As {@link #testTumbleTable()} but on a table where "rowtime" is at
   * position 1 not 0. */
  @Test public void testTumbleTableRowtimeNotFirstColumn() {
    final String sql = "select stream\n"
        + "   tumble_end(rowtime, interval '2' hour) as rowtime, orderId\n"
        + "from shipments\n"
        + "group by tumble(rowtime, interval '2' hour), orderId";
    sql(sql).ok();
  }

  @Test public void testHopTable() {
    final String sql = "select stream hop_start(rowtime, interval '1' hour,"
        + " interval '3' hour) as rowtime,\n"
        + "  count(*) as c\n"
        + "from orders\n"
        + "group by hop(rowtime, interval '1' hour, interval '3' hour)";
    sql(sql).ok();
  }

  @Test public void testSessionTable() {
    final String sql = "select stream session_start(rowtime, interval '1' hour)"
        + " as rowtime,\n"
        + "  session_end(rowtime, interval '1' hour),\n"
        + "  count(*) as c\n"
        + "from orders\n"
        + "group by session(rowtime, interval '1' hour)";
    sql(sql).ok();
  }

  @Test public void testInterval() {
    // temporarily disabled per DTbug 1212
    if (!Bug.DT785_FIXED) {
      return;
    }
    final String sql =
        "values(cast(interval '1' hour as interval hour to second))";
    sql(sql).ok();
  }

  @Test public void testStream() {
    final String sql =
        "select stream productId from orders where productId = 10";
    sql(sql).ok();
  }

  @Test public void testStreamGroupBy() {
    final String sql = "select stream\n"
        + " floor(rowtime to second) as rowtime, count(*) as c\n"
        + "from orders\n"
        + "group by floor(rowtime to second)";
    sql(sql).ok();
  }

  @Test public void testStreamWindowedAggregation() {
    final String sql = "select stream *,\n"
        + "  count(*) over (partition by productId\n"
        + "    order by rowtime\n"
        + "    range interval '1' second preceding) as c\n"
        + "from orders";
    sql(sql).ok();
  }

  @Test public void testExplainAsXml() {
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
            + "\t\t+(1, 2)\t</Property>\n"
            + "\t<Property name=\"EXPR$1\">\n"
            + "\t\t3\t</Property>\n"
            + "\t<Inputs>\n"
            + "\t\t<RelNode type=\"LogicalValues\">\n"
            + "\t\t\t<Property name=\"tuples\">\n"
            + "\t\t\t\t[{ true }]\t\t\t</Property>\n"
            + "\t\t\t<Inputs/>\n"
            + "\t\t</RelNode>\n"
            + "\t</Inputs>\n"
            + "</RelNode>\n",
        Util.toLinux(sw.toString()));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-412">[CALCITE-412]
   * RelFieldTrimmer: when trimming Sort, the collation and trait set don't
   * match</a>. */
  @Test public void testSortWithTrim() {
    final String sql = "select ename from (select * from emp order by sal) a";
    sql(sql).trim(true).ok();
  }

  @Test public void testOffset0() {
    final String sql = "select * from emp offset 0";
    sql(sql).ok();
  }

  /**
   * Test group-by CASE expression involving a non-query IN
   */
  @Test public void testGroupByCaseSubQuery() {
    final String sql = "SELECT CASE WHEN emp.empno IN (3) THEN 0 ELSE 1 END\n"
        + "FROM emp\n"
        + "GROUP BY (CASE WHEN emp.empno IN (3) THEN 0 ELSE 1 END)";
    sql(sql).ok();
  }

  /**
   * Test aggregate function on a CASE expression involving a non-query IN
   */
  @Test public void testAggCaseSubQuery() {
    final String sql =
        "SELECT SUM(CASE WHEN empno IN (3) THEN 0 ELSE 1 END) FROM emp";
    sql(sql).ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-753">[CALCITE-753]
   * Test aggregate operators do not derive row types with duplicate column
   * names</a>. */
  @Test public void testAggNoDuplicateColumnNames() {
    final String sql = "SELECT  empno, EXPR$2, COUNT(empno) FROM (\n"
        + "    SELECT empno, deptno AS EXPR$2\n"
        + "    FROM emp)\n"
        + "GROUP BY empno, EXPR$2";
    sql(sql).ok();
  }

  @Test public void testAggScalarSubQuery() {
    final String sql = "SELECT SUM(SELECT min(deptno) FROM dept) FROM emp";
    sql(sql).ok();
  }

  /** Test aggregate function on a CASE expression involving IN with a
   * sub-query.
   *
   * <p>Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-551">[CALCITE-551]
   * Sub-query inside aggregate function</a>.
   */
  @Test public void testAggCaseInSubQuery() {
    final String sql = "SELECT SUM(\n"
        + "  CASE WHEN deptno IN (SELECT deptno FROM dept) THEN 1 ELSE 0 END)\n"
        + "FROM emp";
    sql(sql).expand(false).ok();
  }

  @Test public void testCorrelatedSubQueryInAggregate() {
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
  @Test public void testGroupByCaseIn() {
    final String sql = "select\n"
        + " (CASE WHEN (deptno IN (10, 20)) THEN 0 ELSE deptno END),\n"
        + " min(empno) from EMP\n"
        + "group by (CASE WHEN (deptno IN (10, 20)) THEN 0 ELSE deptno END)";
    sql(sql).ok();
  }

  @Test public void testInsert() {
    final String sql = "insert into empnullables (deptno, empno, ename)\n"
        + "values (10, 150, 'Fred')";
    sql(sql).ok();
  }

  @Test public void testInsertSubset() {
    final String sql = "insert into empnullables\n"
        + "values (50, 'Fred')";
    sql(sql).conformance(SqlConformanceEnum.PRAGMATIC_2003).ok();
  }

  @Test public void testInsertWithCustomInitializerExpressionFactory() {
    final String sql = "insert into empdefaults (deptno) values (300)";
    sql(sql).ok();
  }

  @Test public void testInsertSubsetWithCustomInitializerExpressionFactory() {
    final String sql = "insert into empdefaults values (100)";
    sql(sql).conformance(SqlConformanceEnum.PRAGMATIC_2003).ok();
  }

  @Test public void testInsertBind() {
    final String sql = "insert into empnullables (deptno, empno, ename)\n"
        + "values (?, ?, ?)";
    sql(sql).ok();
  }

  @Test public void testInsertBindSubset() {
    final String sql = "insert into empnullables\n"
        + "values (?, ?)";
    sql(sql).conformance(SqlConformanceEnum.PRAGMATIC_2003).ok();
  }

  @Test public void testInsertBindWithCustomInitializerExpressionFactory() {
    final String sql = "insert into empdefaults (deptno) values (?)";
    sql(sql).ok();
  }

  @Test public void testInsertBindSubsetWithCustomInitializerExpressionFactory() {
    final String sql = "insert into empdefaults values (?)";
    sql(sql).conformance(SqlConformanceEnum.PRAGMATIC_2003).ok();
  }

  @Test public void testInsertSubsetView() {
    final String sql = "insert into empnullables_20\n"
        + "values (10, 'Fred')";
    sql(sql).conformance(SqlConformanceEnum.PRAGMATIC_2003).ok();
  }

  @Test public void testInsertExtendedColumn() {
    final String sql = "insert into empdefaults(updated TIMESTAMP)"
        + " (ename, deptno, empno, updated, sal)"
        + " values ('Fred', 456, 44, timestamp '2017-03-12 13:03:05', 999999)";
    sql(sql).ok();
  }

  @Test public void testInsertBindExtendedColumn() {
    final String sql = "insert into empdefaults(updated TIMESTAMP)"
        + " (ename, deptno, empno, updated, sal)"
        + " values ('Fred', 456, 44, ?, 999999)";
    sql(sql).ok();
  }

  @Test public void testInsertExtendedColumnModifiableView() {
    final String sql = "insert into EMP_MODIFIABLEVIEW2(updated TIMESTAMP)"
        + " (ename, deptno, empno, updated, sal)"
        + " values ('Fred', 20, 44, timestamp '2017-03-12 13:03:05', 999999)";
    sql(sql).with(getExtendedTester()).ok();
  }

  @Test public void testInsertBindExtendedColumnModifiableView() {
    final String sql = "insert into EMP_MODIFIABLEVIEW2(updated TIMESTAMP)"
        + " (ename, deptno, empno, updated, sal)"
        + " values ('Fred', 20, 44, ?, 999999)";
    sql(sql).with(getExtendedTester()).ok();
  }

  @Test public void testDelete() {
    final String sql = "delete from emp";
    sql(sql).ok();
  }

  @Test public void testDeleteWhere() {
    final String sql = "delete from emp where deptno = 10";
    sql(sql).ok();
  }

  @Test public void testDeleteBind() {
    final String sql = "delete from emp where deptno = ?";
    sql(sql).ok();
  }

  @Test public void testDeleteBindExtendedColumn() {
    final String sql = "delete from emp(enddate TIMESTAMP) where enddate < ?";
    sql(sql).ok();
  }

  @Test public void testDeleteBindModifiableView() {
    final String sql = "delete from EMP_MODIFIABLEVIEW2 where empno = ?";
    sql(sql).with(getExtendedTester()).ok();
  }

  @Test public void testDeleteBindExtendedColumnModifiableView() {
    final String sql = "delete from EMP_MODIFIABLEVIEW2(note VARCHAR)\n"
        + "where note = ?";
    sql(sql).with(getExtendedTester()).ok();
  }

  @Test public void testUpdate() {
    final String sql = "update emp set empno = empno + 1";
    sql(sql).ok();
  }

  @Ignore("CALCITE-1527")
  @Test public void testUpdateSubQuery() {
    final String sql = "update emp\n"
        + "set empno = (\n"
        + "  select min(empno) from emp as e where e.deptno = emp.deptno)";
    sql(sql).ok();
  }

  @Test public void testUpdateWhere() {
    final String sql = "update emp set empno = empno + 1 where deptno = 10";
    sql(sql).ok();
  }

  @Test public void testUpdateModifiableView() {
    final String sql = "update EMP_MODIFIABLEVIEW2\n"
        + "set sal = sal + 5000 where slacker = false";
    sql(sql).with(getExtendedTester()).ok();
  }

  @Test public void testUpdateExtendedColumn() {
    final String sql = "update empdefaults(updated TIMESTAMP)"
        + " set deptno = 1, updated = timestamp '2017-03-12 13:03:05', empno = 20, ename = 'Bob'"
        + " where deptno = 10";
    sql(sql).ok();
  }

  @Test public void testUpdateExtendedColumnModifiableView() {
    final String sql = "update EMP_MODIFIABLEVIEW2(updated TIMESTAMP)\n"
        + "set updated = timestamp '2017-03-12 13:03:05', sal = sal + 5000\n"
        + "where slacker = false";
    sql(sql).with(getExtendedTester()).ok();
  }

  @Test public void testUpdateBind() {
    final String sql = "update emp"
        + " set sal = sal + ? where slacker = false";
    sql(sql).ok();
  }

  @Test public void testUpdateBind2() {
    final String sql = "update emp"
        + " set sal = ? where slacker = false";
    sql(sql).ok();
  }

  @Ignore("CALCITE-1708")
  @Test public void testUpdateBindExtendedColumn() {
    final String sql = "update emp(test INT)"
        + " set test = ?, sal = sal + 5000 where slacker = false";
    sql(sql).ok();
  }

  @Ignore("CALCITE-1708")
  @Test public void testUpdateBindExtendedColumnModifiableView() {
    final String sql = "update EMP_MODIFIABLEVIEW2(test INT)"
        + " set test = ?, sal = sal + 5000 where slacker = false";
    sql(sql).ok();
  }

  @Ignore("CALCITE-985")
  @Test public void testMerge() {
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

  @Test public void testSelectView() {
    // translated condition: deptno = 20 and sal > 1000 and empno > 100
    final String sql = "select * from emp_20 where empno > 100";
    sql(sql).ok();
  }

  @Test public void testInsertView() {
    final String sql = "insert into empnullables_20 (empno, ename)\n"
        + "values (150, 'Fred')";
    sql(sql).ok();
  }

  @Test public void testInsertModifiableView() {
    final String sql = "insert into EMP_MODIFIABLEVIEW (EMPNO, ENAME, JOB)"
        + " values (34625, 'nom', 'accountant')";
    sql(sql).with(getExtendedTester()).ok();
  }

  @Test public void testInsertSubsetModifiableView() {
    final String sql = "insert into EMP_MODIFIABLEVIEW "
        + "values (10, 'Fred')";
    sql(sql).with(getExtendedTester())
        .conformance(SqlConformanceEnum.PRAGMATIC_2003).ok();
  }

  @Test public void testInsertBindModifiableView() {
    final String sql = "insert into EMP_MODIFIABLEVIEW (empno, job)"
        + " values (?, ?)";
    sql(sql).with(getExtendedTester()).ok();
  }

  @Test public void testInsertBindSubsetModifiableView() {
    final String sql = "insert into EMP_MODIFIABLEVIEW"
        + " values (?, ?)";
    sql(sql).conformance(SqlConformanceEnum.PRAGMATIC_2003)
        .with(getExtendedTester()).ok();
  }

  @Test public void testInsertWithCustomColumnResolving() {
    final String sql = "insert into struct.t values (?, ?, ?, ?, ?, ?, ?, ?, ?)";
    sql(sql).ok();
  }

  @Test public void testInsertWithCustomColumnResolving2() {
    final String sql = "insert into struct.t_nullables (f0.c0, f1.c2, c1)\n"
        + "values (?, ?, ?)";
    sql(sql).ok();
  }

  @Test public void testInsertViewWithCustomColumnResolving() {
    final String sql = "insert into struct.t_10 (f0.c0, f1.c2, c1, k0,\n"
        + "  f1.a0, f2.a0, f0.c1, f2.c3)\n"
        + "values (?, ?, ?, ?, ?, ?, ?, ?)";
    sql(sql).ok();
  }

  @Test public void testUpdateWithCustomColumnResolving() {
    final String sql = "update struct.t set c0 = c0 + 1";
    sql(sql).ok();
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-695">[CALCITE-695]
   * SqlSingleValueAggFunction is created when it may not be needed</a>.
   */
  @Test public void testSubQueryAggregateFunctionFollowedBySimpleOperation() {
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
  @Test public void testSubQueryOr() {
    final String sql = "select * from emp where deptno = 10 or deptno in (\n"
        + "    select dept.deptno from dept where deptno < 5)\n";
    sql(sql).expand(false).ok();
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-695">[CALCITE-695]
   * SqlSingleValueAggFunction is created when it may not be needed</a>.
   */
  @Test public void testSubQueryValues() {
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
  @Test public void testSubQueryLimitOne() {
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
  @Test public void testIdenticalExpressionInSubQuery() {
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
  @Test public void testHavingAggrFunctionIn() {
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
  @Test public void testHavingInSubQueryWithAggrFunction() {
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
  @Test public void testAggregateAndScalarSubQueryInHaving() {
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
  @Test public void testAggregateAndScalarSubQueryInSelect() {
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
  @Test public void testWindowAggWithGroupBy() {
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
  @Test public void testWindowAverageWithGroupBy() {
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
  @Test public void testWindowAggWithGroupByAndJoin() {
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
  @Test public void testWindowAggWithGroupByAndHaving() {
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
  @Test public void testWindowAggInSubQueryJoin() {
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
  @Test public void testOrderByOver() {
    String sql = "select deptno, rank() over(partition by empno order by deptno)\n"
        + "from emp order by row_number() over(partition by empno order by deptno)";
    sql(sql).ok();
  }

  /**
   * Test case (correlated scalar aggregate sub-query) for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-714">[CALCITE-714]
   * When de-correlating, push join condition into sub-query</a>.
   */
  @Test public void testCorrelationScalarAggAndFilter() {
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
  @Test public void testCorrelationMultiScalarAggregate() {
    final String sql = "select sum(e1.empno)\n"
        + "from emp e1, dept d1\n"
        + "where e1.deptno = d1.deptno\n"
        + "and e1.sal > (select avg(e2.sal) from emp e2\n"
        + "  where e2.deptno = d1.deptno)";
    sql(sql).decorrelate(true).expand(true).ok();
  }

  @Test public void testCorrelationScalarAggAndFilterRex() {
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
  @Test public void testCorrelationExistsAndFilter() {
    final String sql = "SELECT e1.empno\n"
        + "FROM emp e1, dept d1 where e1.deptno = d1.deptno\n"
        + "and e1.deptno < 10 and d1.deptno < 15\n"
        + "and exists (select * from emp e2 where e1.empno = e2.empno)";
    sql(sql).decorrelate(true).expand(true).ok();
  }

  @Test public void testCorrelationExistsAndFilterRex() {
    final String sql = "SELECT e1.empno\n"
        + "FROM emp e1, dept d1 where e1.deptno = d1.deptno\n"
        + "and e1.deptno < 10 and d1.deptno < 15\n"
        + "and exists (select * from emp e2 where e1.empno = e2.empno)";
    sql(sql).decorrelate(true).ok();
  }

  /** A theta join condition, unlike the equi-join condition in
   * {@link #testCorrelationExistsAndFilterRex()}, requires a value
   * generator. */
  @Test public void testCorrelationExistsAndFilterThetaRex() {
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
  @Test public void testCorrelationNotExistsAndFilter() {
    final String sql = "SELECT e1.empno\n"
        + "FROM emp e1, dept d1 where e1.deptno = d1.deptno\n"
        + "and e1.deptno < 10 and d1.deptno < 15\n"
        + "and not exists (select * from emp e2 where e1.empno = e2.empno)";
    sql(sql).decorrelate(true).ok();
  }

  @Test public void testCustomColumnResolving() {
    final String sql = "select k0 from struct.t";
    sql(sql).ok();
  }

  @Test public void testCustomColumnResolving2() {
    final String sql = "select c2 from struct.t";
    sql(sql).ok();
  }

  @Test public void testCustomColumnResolving3() {
    final String sql = "select f1.c2 from struct.t";
    sql(sql).ok();
  }

  @Test public void testCustomColumnResolving4() {
    final String sql = "select c1 from struct.t order by f0.c1";
    sql(sql).ok();
  }

  @Test public void testCustomColumnResolving5() {
    final String sql = "select count(c1) from struct.t group by f0.c1";
    sql(sql).ok();
  }

  @Test public void testCustomColumnResolvingWithSelectStar() {
    final String sql = "select * from struct.t";
    sql(sql).ok();
  }

  @Test public void testCustomColumnResolvingWithSelectFieldNameDotStar() {
    final String sql = "select f1.* from struct.t";
    sql(sql).ok();
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1150">[CALCITE-1150]
   * Dynamic Table / Dynamic Star support</a>
   */
  @Test
  public void testSelectFromDynamicTable() throws Exception {
    final String sql = "select n_nationkey, n_name from SALES.NATION";
    sql(sql).with(getTesterWithDynamicTable()).ok();
  }

  /**
   * Test case for Dynamic Table / Dynamic Star support
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1150">[CALCITE-1150]</a>
   */
  @Test
  public void testSelectStarFromDynamicTable() throws Exception {
    final String sql = "select * from SALES.NATION";
    sql(sql).with(getTesterWithDynamicTable()).ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2080">[CALCITE-2080]
   * Query with NOT IN operator and literal fails throws AssertionError: 'Cast
   * for just nullability not allowed'</a>. */
  @Test public void testNotInWithLiteral() {
    final String sql = "SELECT *\n"
        + "FROM SALES.NATION\n"
        + "WHERE n_name NOT IN\n"
        + "    (SELECT ''\n"
        + "     FROM SALES.NATION)";
    sql(sql).with(getTesterWithDynamicTable()).ok();
  }

  /**
   * Test case for Dynamic Table / Dynamic Star support
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1150">[CALCITE-1150]</a>
   */
  @Test
  public void testReferDynamicStarInSelectOB() throws Exception {
    final String sql = "select n_nationkey, n_name\n"
        + "from (select * from SALES.NATION)\n"
        + "order by n_regionkey";
    sql(sql).with(getTesterWithDynamicTable()).ok();
  }

  /**
   * Test case for Dynamic Table / Dynamic Star support
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1150">[CALCITE-1150]</a>
   */
  @Test
  public void testDynamicStarInTableJoin() throws Exception {
    final String sql = "select * from "
        + " (select * from SALES.NATION) T1, "
        + " (SELECT * from SALES.CUSTOMER) T2 "
        + " where T1.n_nationkey = T2.c_nationkey";
    sql(sql).with(getTesterWithDynamicTable()).ok();
  }

  @Test public void testDynamicNestedColumn() {
    final String sql = "select t3.fake_q1['fake_col2'] as fake2\n"
        + "from (\n"
        + "  select t2.fake_col as fake_q1\n"
        + "  from SALES.CUSTOMER as t2) as t3";
    sql(sql).with(getTesterWithDynamicTable()).ok();
  }

  @Test public void testDynamicSchemaUnnest() {
    final String sql3 = "select t1.c_nationkey, t3.fake_col3\n"
        + "from SALES.CUSTOMER as t1,\n"
        + "lateral (select t2.\"$unnest\" as fake_col3\n"
        + "         from unnest(t1.fake_col) as t2) as t3";
    sql(sql3).with(getTesterWithDynamicTable()).ok();
  }

  @Test public void testStarDynamicSchemaUnnest() {
    final String sql3 = "select * \n"
        + "from SALES.CUSTOMER as t1,\n"
        + "lateral (select t2.\"$unnest\" as fake_col3\n"
        + "         from unnest(t1.fake_col) as t2) as t3";
    sql(sql3).with(getTesterWithDynamicTable()).ok();
  }

  @Test public void testStarDynamicSchemaUnnest2() {
    final String sql3 = "select * \n"
        + "from SALES.CUSTOMER as t1,\n"
        + "unnest(t1.fake_col) as t2";
    sql(sql3).with(getTesterWithDynamicTable()).ok();
  }
  /**
   * Test case for Dynamic Table / Dynamic Star support
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1150">[CALCITE-1150]</a>
   */
  @Test
  public void testReferDynamicStarInSelectWhereGB() throws Exception {
    final String sql = "select n_regionkey, count(*) as cnt from "
        + "(select * from SALES.NATION) where n_nationkey > 5 "
        + "group by n_regionkey";
    sql(sql).with(getTesterWithDynamicTable()).ok();
  }

  /**
   * Test case for Dynamic Table / Dynamic Star support
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1150">[CALCITE-1150]</a>
   */
  @Test
  public void testDynamicStarInJoinAndSubQ() throws Exception {
    final String sql = "select * from "
        + " (select * from SALES.NATION T1, "
        + " SALES.CUSTOMER T2 where T1.n_nationkey = T2.c_nationkey)";
    sql(sql).with(getTesterWithDynamicTable()).ok();
  }

  /**
   * Test case for Dynamic Table / Dynamic Star support
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1150">[CALCITE-1150]</a>
   */
  @Test
  public void testStarJoinStaticDynTable() throws Exception {
    final String sql = "select * from SALES.NATION N, SALES.REGION as R "
        + "where N.n_regionkey = R.r_regionkey";
    sql(sql).with(getTesterWithDynamicTable()).ok();
  }

  /**
   * Test case for Dynamic Table / Dynamic Star support
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1150">[CALCITE-1150]</a>
   */
  @Test
  public void testGrpByColFromStarInSubQuery() throws Exception {
    final String sql = "SELECT n.n_nationkey AS col "
        + " from (SELECT * FROM SALES.NATION) as n "
        + " group by n.n_nationkey";
    sql(sql).with(getTesterWithDynamicTable()).ok();
  }

  /**
   * Test case for Dynamic Table / Dynamic Star support
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1150">[CALCITE-1150]</a>
   */
  @Test
  public void testDynStarInExistSubQ() throws Exception {
    final String sql = "select *\n"
        + "from SALES.REGION where exists (select * from SALES.NATION)";
    sql(sql).with(getTesterWithDynamicTable()).ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1150">[CALCITE-1150]
   * Create the a new DynamicRecordType, avoiding star expansion when working
   * with this type</a>. */
  @Test public void testSelectDynamicStarOrderBy() throws Exception {
    final String sql = "SELECT * from SALES.NATION order by n_nationkey";
    sql(sql).with(getTesterWithDynamicTable()).ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1321">[CALCITE-1321]
   * Configurable IN list size when converting IN clause to join</a>. */
  @Test public void testInToSemiJoin() {
    final String sql = "SELECT empno\n"
        + "FROM emp AS e\n"
        + "WHERE cast(e.empno as bigint) in (130, 131, 132, 133, 134)";
    // No conversion to join since less than IN-list size threshold 10
    SqlToRelConverter.Config noConvertConfig =
        SqlToRelConverter.configBuilder().withInSubQueryThreshold(10).build();
    sql(sql).withConfig(noConvertConfig).convertsTo("${planNotConverted}");
    // Conversion to join since greater than IN-list size threshold 2
    SqlToRelConverter.Config convertConfig =
        SqlToRelConverter.configBuilder().withInSubQueryThreshold(2).build();
    sql(sql).withConfig(convertConfig).convertsTo("${planConverted}");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1944">[CALCITE-1944]
   * Window function applied to sub-query with dynamic star gets wrong
   * plan</a>. */
  @Test public void testWindowOnDynamicStar() throws Exception {
    final String sql = "SELECT SUM(n_nationkey) OVER w\n"
        + "FROM (SELECT * FROM SALES.NATION) subQry\n"
        + "WINDOW w AS (PARTITION BY REGION ORDER BY n_nationkey)";
    sql(sql).with(getTesterWithDynamicTable()).ok();
  }

  private Tester getExtendedTester() {
    return tester.withCatalogReaderFactory(
      new Function<RelDataTypeFactory, Prepare.CatalogReader>() {
        public Prepare.CatalogReader apply(RelDataTypeFactory typeFactory) {
          return new MockCatalogReader(typeFactory, true)
              .init().init2();
        }
      });
  }

  @Test public void testLarge() {
    SqlValidatorTest.checkLarge(400,
        new Function<String, Void>() {
          public Void apply(String input) {
            final RelRoot root = tester.convertSqlToRel(input);
            final String s = RelOptUtil.toString(root.project());
            assertThat(s, notNullValue());
            return null;
          }
        });
  }

  @Test public void testUnionInFrom() {
    final String sql = "select x0, x1 from (\n"
        + "  select 'a' as x0, 'a' as x1, 'a' as x2 from emp\n"
        + "  union all\n"
        + "  select 'bb' as x0, 'bb' as x1, 'bb' as x2 from dept)";
    sql(sql).ok();
  }

  @Test public void testMatchRecognize1() {
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

  @Test public void testMatchRecognizeMeasures1() {
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
  @Test public void testMatchRecognizeMeasures2() {
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

  @Test public void testMatchRecognizeMeasures3() {
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

  @Test public void testMatchRecognizePatternSkip1() {
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

  @Test public void testMatchRecognizeSubset1() {
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

  @Test public void testMatchRecognizePrevLast() {
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

  @Test public void testMatchRecognizePrevDown() {
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

  @Test public void testPrevClassifier() {
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

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2323">[CALCITE-2323]
   * Validator should allow alternative nullCollations for ORDER BY in
   * OVER</a>. */
  @Test public void testUserDefinedOrderByOver() {
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
    TesterImpl tester = new TesterImpl(getDiffRepos(), false, false, true, false,
        null, null, SqlToRelConverter.Config.DEFAULT,
        SqlConformanceEnum.DEFAULT, Contexts.of(connectionConfig));
    sql(sql).with(tester).ok();
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

    public void visit(RelNode node, int ordinal, RelNode parent) {
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
    private final boolean expand;
    private final boolean decorrelate;
    private final Tester tester;
    private final boolean trim;
    private final SqlToRelConverter.Config config;
    private final SqlConformance conformance;

    Sql(String sql, boolean expand, boolean decorrelate, Tester tester,
        boolean trim, SqlToRelConverter.Config config,
        SqlConformance conformance) {
      this.sql = sql;
      this.expand = expand;
      this.decorrelate = decorrelate;
      this.tester = tester;
      this.trim = trim;
      this.config = config;
      this.conformance = conformance;
    }

    public void ok() {
      convertsTo("${plan}");
    }

    public void convertsTo(String plan) {
      tester.withExpand(expand)
          .withDecorrelation(decorrelate)
          .withConformance(conformance)
          .withConfig(config)
          .assertConvertsTo(sql, plan, trim);
    }

    public Sql withConfig(SqlToRelConverter.Config config) {
      return new Sql(sql, expand, decorrelate, tester, trim, config,
          conformance);
    }

    public Sql expand(boolean expand) {
      return new Sql(sql, expand, decorrelate, tester, trim, config,
          conformance);
    }

    public Sql decorrelate(boolean decorrelate) {
      return new Sql(sql, expand, decorrelate, tester, trim, config,
          conformance);
    }

    public Sql with(Tester tester) {
      return new Sql(sql, expand, decorrelate, tester, trim, config,
          conformance);
    }

    public Sql trim(boolean trim) {
      return new Sql(sql, expand, decorrelate, tester, trim, config,
          conformance);
    }

    public Sql conformance(SqlConformance conformance) {
      return new Sql(sql, expand, decorrelate, tester, trim, config,
          conformance);
    }
  }
}

// End SqlToRelConverterTest.java
