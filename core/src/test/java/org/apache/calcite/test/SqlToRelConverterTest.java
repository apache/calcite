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

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.externalize.RelXmlWriter;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.Bug;
import org.apache.calcite.util.TestUtil;
import org.apache.calcite.util.Util;

import org.junit.Ignore;
import org.junit.Test;

import java.io.PrintWriter;
import java.io.StringWriter;

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
    return new Sql(sql);
  }

  protected final void check(
      String sql,
      String plan) {
    sql(sql).convertsTo(plan);
  }

  @Test public void testIntegerLiteral() {
    check("select 1 from emp", "${plan}");
  }

  @Test public void testAliasList() {
    check(
        "select a + b from (\n"
            + "  select deptno, 1 as one, name from dept\n"
            + ") as d(a, b, c)\n"
            + "where c like 'X%'",
        "${plan}");
  }

  @Test public void testAliasList2() {
    check(
        "select * from (\n"
            + "  select a, b, c from (values (1, 2, 3)) as t (c, b, a)\n"
            + ") join dept on dept.deptno = c\n"
            + "order by c + a",
        "${plan}");
  }

  /**
   * Tests that AND(x, AND(y, z)) gets flattened to AND(x, y, z).
   */
  @Test public void testMultiAnd() {
    check(
        "select * from emp\n"
            + "where deptno < 10\n"
            + "and deptno > 5\n"
            + "and (deptno = 8 or empno < 100)",
        "${plan}");
  }

  @Test public void testJoinOn() {
    check(
        "SELECT * FROM emp JOIN dept on emp.deptno = dept.deptno",
        "${plan}");
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-245">[CALCITE-245]
   * Off-by-one translation of ON clause of JOIN</a>.
   */
  @Test public void testConditionOffByOne() {
    // Bug causes the plan to contain
    //   LogicalJoin(condition=[=($9, $9)], joinType=[inner])
    check(
        "SELECT * FROM emp JOIN dept on emp.deptno + 0 = dept.deptno",
        "${plan}");
  }

  @Test public void testConditionOffByOneReversed() {
    check(
        "SELECT * FROM emp JOIN dept on dept.deptno = emp.deptno + 0",
        "${plan}");
  }

  @Test public void testJoinOnExpression() {
    check(
        "SELECT * FROM emp JOIN dept on emp.deptno + 1 = dept.deptno - 2",
        "${plan}");
  }

  @Test public void testJoinOnIn() {
    check(
        "select * from emp join dept\n"
            + " on emp.deptno = dept.deptno and emp.empno in (1, 3)",
        "${plan}");
  }

  @Test public void testJoinUsing() {
    check("SELECT * FROM emp JOIN dept USING (deptno)", "${plan}");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-74">[CALCITE-74]
   * JOIN ... USING fails in 3-way join with
   * UnsupportedOperationException</a>. */
  @Test public void testJoinUsingThreeWay() {
    check(
        "select *\n"
            + "from emp as e\n"
            + "join dept as d using (deptno)\n"
            + "join emp as e2 using (empno)", "${plan}");
  }

  @Test public void testJoinUsingCompound() {
    check(
        "SELECT * FROM emp LEFT JOIN ("
            + "SELECT *, deptno * 5 as empno FROM dept) "
            + "USING (deptno,empno)",
        "${plan}");
  }

  @Test public void testJoinNatural() {
    check(
        "SELECT * FROM emp NATURAL JOIN dept",
        "${plan}");
  }

  @Test public void testJoinNaturalNoCommonColumn() {
    check(
        "SELECT * FROM emp NATURAL JOIN (SELECT deptno AS foo, name FROM dept) AS d",
        "${plan}");
  }

  @Test public void testJoinNaturalMultipleCommonColumn() {
    check(
        "SELECT * FROM emp NATURAL JOIN (SELECT deptno, name AS ename FROM dept) AS d",
        "${plan}");
  }

  @Test public void testJoinWithUnion() {
    check(
        "select grade from "
            + "(select empno from emp union select deptno from dept), "
            + "salgrade",
        "${plan}");
  }

  @Test public void testGroup() {
    check(
        "select deptno from emp group by deptno",
        "${plan}");
  }

  @Test public void testGroupJustOneAgg() {
    // just one agg
    check(
        "select deptno, sum(sal) as sum_sal from emp group by deptno",
        "${plan}");
  }

  @Test public void testGroupExpressionsInsideAndOut() {
    // Expressions inside and outside aggs. Common sub-expressions should be
    // eliminated: 'sal' always translates to expression #2.
    check(
        "select deptno + 4, sum(sal), sum(3 + sal), 2 * count(sal) from emp group by deptno",
        "${plan}");
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
    sql("select deptno, ename, sum(sal) from emp\n"
        + "group by grouping sets ((deptno), (ename, deptno))\n"
        + "order by 2").ok();
  }

  @Test public void testGroupingSetsProduct() {
    // Example in SQL:2011:
    //   GROUP BY GROUPING SETS ((A, B), (C)), GROUPING SETS ((X, Y), ())
    // is transformed to
    //   GROUP BY GROUPING SETS ((A, B, X, Y), (A, B), (C, X, Y), (C))
    sql("select 1\n"
        + "from (values (0, 1, 2, 3, 4)) as t(a, b, c, x, y)\n"
        + "group by grouping sets ((a, b), c), grouping sets ((x, y), ())")
        .ok();
  }

  /** When the GROUPING function occurs with GROUP BY (effectively just one
   * grouping set), we can translate it directly to 1. */
  @Test public void testGroupingFunctionWithGroupBy() {
    sql("select deptno, grouping(deptno), count(*), grouping(empno)\n"
        + "from emp\n"
        + "group by empno, deptno\n"
        + "order by 2").ok();
  }

  @Test public void testGroupingFunction() {
    sql("select deptno, grouping(deptno), count(*), grouping(empno)\n"
        + "from emp\n"
        + "group by rollup(empno, deptno)").ok();
  }

  /**
   * GROUP BY with duplicates
   *
   * <p>From SQL spec:
   * <blockquote>NOTE 190 — That is, a simple <em>group by clause</em> that is
   * not primitive may be transformed into a primitive <em>group by clause</em>
   * by deleting all parentheses, and deleting extra commas as necessary for
   * correct syntax. If there are no grouping columns at all (for example,
   * GROUP BY (), ()), this is transformed to the canonical form GROUP BY ().
   * </blockquote> */
  // Same effect as writing "GROUP BY ()"
  @Test public void testGroupByWithDuplicates() {
    sql("select sum(sal) from emp group by (), ()").ok();
  }

  /** GROUP BY with duplicate (and heavily nested) GROUPING SETS. */
  @Test public void testDuplicateGroupingSets() {
    sql("select sum(sal) from emp\n"
        + "group by sal,\n"
        + "  grouping sets (deptno,\n"
        + "    grouping sets ((deptno, ename), ename),\n"
        + "      (ename)),\n"
        + "  ()").ok();
  }

  @Test public void testGroupingSetsCartesianProduct() {
    // Equivalent to (a, c), (a, d), (b, c), (b, d)
    sql("select 1 from (values (1, 2, 3, 4)) as t(a, b, c, d)\n"
        + "group by grouping sets (a, b), grouping sets (c, d)").ok();
  }

  @Test public void testGroupingSetsCartesianProduct2() {
    sql("select 1 from (values (1, 2, 3, 4)) as t(a, b, c, d)\n"
      + "group by grouping sets (a, (a, b)), grouping sets (c), d").ok();
  }

  @Test public void testRollupSimple() {
    // a is nullable so is translated as just "a"
    // b is not null, so is represented as 0 inside Aggregate, then
    // using "CASE WHEN i$b THEN NULL ELSE b END"
    sql("select a, b, count(*) as c\n"
        + "from (values (cast(null as integer), 2)) as t(a, b)\n"
        + "group by rollup(a, b)").ok();
  }

  @Test public void testRollup() {
    // Equivalent to {(a, b), (a), ()}  * {(c, d), (c), ()}
    sql("select 1 from (values (1, 2, 3, 4)) as t(a, b, c, d)\n"
        + "group by rollup(a, b), rollup(c, d)").ok();
  }

  @Test public void testRollupTuples() {
    // rollup(b, (a, d)) is (b, a, d), (b), ()
    sql("select 1 from (values (1, 2, 3, 4)) as t(a, b, c, d)\n"
        + "group by rollup(b, (a, d))").ok();
  }

  @Test public void testCube() {
    // cube(a, b) is {(a, b), (a), (b), ()}
    sql("select 1 from (values (1, 2, 3, 4)) as t(a, b, c, d)\n"
        + "group by cube(a, b)").ok();
  }

  @Test public void testGroupingSetsWith() {
    sql("with t(a, b, c, d) as (values (1, 2, 3, 4))\n"
        + "select 1 from t\n"
        + "group by rollup(a, b), rollup(c, d)").ok();
  }

  @Test public void testHaving() {
    // empty group-by clause, having
    check(
        "select sum(sal + sal) from emp having sum(sal) > 10",
        "${plan}");
  }

  @Test public void testGroupBug281() {
    // Dtbug 281 gives:
    //   Internal error:
    //   Type 'RecordType(VARCHAR(128) $f0)' has no field 'NAME'
    check(
        "select name from (select name from dept group by name)",
        "${plan}");
  }

  @Test public void testGroupBug281b() {
    // Try to confuse it with spurious columns.
    sql("select name, foo from ("
        + "select deptno, name, count(deptno) as foo "
        + "from dept "
        + "group by name, deptno, name)").ok();
  }

  @Test public void testGroupByExpression() {
    // This used to cause an infinite loop,
    // SqlValidatorImpl.getValidatedNodeType
    // calling getValidatedNodeTypeIfKnown
    // calling getValidatedNodeType.
    sql("select count(*) from emp group by substring(ename FROM 1 FOR 1)").ok();
  }

  @Test public void testAggDistinct() {
    sql("select deptno, sum(sal), sum(distinct sal), count(*) "
        + "from emp "
        + "group by deptno").ok();
  }

  @Test public void testAggFilter() {
    sql("select deptno, sum(sal * 2) filter (where empno < 10), count(*) "
        + "from emp "
        + "group by deptno").ok();
  }

  @Test public void testSelectDistinct() {
    sql("select distinct sal + 5 from emp").ok();
  }

  @Test public void testSelectDistinctGroup() {
    sql("select distinct sum(sal) from emp group by deptno").ok();
  }

  /**
   * Tests that if the clause of SELECT DISTINCT contains duplicate
   * expressions, they are only aggregated once.
   */
  @Test public void testSelectDistinctDup() {
    check(
        "select distinct sal + 5, deptno, sal + 5 from emp where deptno < 10",
        "${plan}");
  }

  /** Tests referencing columns from a sub-query that has duplicate column
   * names. I think the standard says that this is illegal. We roll with it,
   * and rename the second column to "e0". */
  @Test public void testDuplicateColumnsInSubQuery() {
    String sql = "select \"e\" from (\n"
        + "select empno as \"e\", deptno as d, 1 as \"e\" from EMP)";
    tester.assertConvertsTo(sql, "${plan}");
  }

  @Test public void testOrder() {
    check(
        "select empno from emp order by empno", "${plan}");
  }

  @Test public void testOrderDescNullsLast() {
    check(
        "select empno from emp order by empno desc nulls last",
        "${plan}");
  }

  @Test public void testOrderByOrdinalDesc() {
    // FRG-98
    if (!tester.getConformance().isSortByOrdinal()) {
      return;
    }
    check(
        "select empno + 1, deptno, empno from emp order by 2 desc",
        "${plan}");

    // ordinals rounded down, so 2.5 should have same effect as 2, and
    // generate identical plan
    check(
        "select empno + 1, deptno, empno from emp order by 2.5 desc",
        "${plan}");
  }

  @Test public void testOrderDistinct() {
    // The relexp aggregates by 3 expressions - the 2 select expressions
    // plus the one to sort on. A little inefficient, but acceptable.
    check(
        "select distinct empno, deptno + 1 from emp order by deptno + 1 + empno",
        "${plan}");
  }

  @Test public void testOrderByNegativeOrdinal() {
    // Regardless of whether sort-by-ordinals is enabled, negative ordinals
    // are treated like ordinary numbers.
    check(
        "select empno + 1, deptno, empno from emp order by -1 desc",
        "${plan}");
  }

  @Test public void testOrderByOrdinalInExpr() {
    // Regardless of whether sort-by-ordinals is enabled, ordinals
    // inside expressions are treated like integers.
    check(
        "select empno + 1, deptno, empno from emp order by 1 + 2 desc",
        "${plan}");
  }

  @Test public void testOrderByIdenticalExpr() {
    // Expression in ORDER BY clause is identical to expression in SELECT
    // clause, so plan should not need an extra project.
    check(
        "select empno + 1 from emp order by deptno asc, empno + 1 desc",
        "${plan}");
  }

  @Test public void testOrderByAlias() {
    check(
        "select empno + 1 as x, empno - 2 as y from emp order by y",
        "${plan}");
  }

  @Test public void testOrderByAliasInExpr() {
    check(
        "select empno + 1 as x, empno - 2 as y from emp order by y + 3",
        "${plan}");
  }

  @Test public void testOrderByAliasOverrides() {
    if (!tester.getConformance().isSortByAlias()) {
      return;
    }

    // plan should contain '(empno + 1) + 3'
    check(
        "select empno + 1 as empno, empno - 2 as y from emp order by empno + 3",
        "${plan}");
  }

  @Test public void testOrderByAliasDoesNotOverride() {
    if (tester.getConformance().isSortByAlias()) {
      return;
    }

    // plan should contain 'empno + 3', not '(empno + 1) + 3'
    check(
        "select empno + 1 as empno, empno - 2 as y from emp order by empno + 3",
        "${plan}");
  }

  @Test public void testOrderBySameExpr() {
    check(
        "select empno from emp, dept order by sal + empno desc, sal * empno, sal + empno",
        "${plan}");
  }

  @Test public void testOrderUnion() {
    check(
        "select empno, sal from emp "
            + "union all "
            + "select deptno, deptno from dept "
            + "order by sal desc, empno asc",
        "${plan}");
  }

  @Test public void testOrderUnionOrdinal() {
    if (!tester.getConformance().isSortByOrdinal()) {
      return;
    }
    check(
        "select empno, sal from emp "
            + "union all "
            + "select deptno, deptno from dept "
            + "order by 2",
        "${plan}");
  }

  @Test public void testOrderUnionExprs() {
    check(
        "select empno, sal from emp "
            + "union all "
            + "select deptno, deptno from dept "
            + "order by empno * sal + 2",
        "${plan}");
  }

  @Test public void testOrderOffsetFetch() {
    check(
        "select empno from emp order by empno offset 10 rows fetch next 5 rows only",
        "${plan}");
  }

  @Test public void testOffsetFetch() {
    check(
        "select empno from emp offset 10 rows fetch next 5 rows only",
        "${plan}");
  }

  @Test public void testOffset() {
    check(
        "select empno from emp offset 10 rows",
        "${plan}");
  }

  @Test public void testFetch() {
    check(
        "select empno from emp fetch next 5 rows only",
        "${plan}");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-439">[CALCITE-439]
   * SqlValidatorUtil.uniquify() may not terminate under some
   * conditions</a>. */
  @Test public void testGroupAlias() {
    check("select \"$f2\", max(x), max(x + 1)\n"
            + "from (values (1, 2)) as t(\"$f2\", x)\n"
            + "group by \"$f2\"",
        "${plan}");
  }

  @Test public void testOrderGroup() {
    check(
        "select deptno, count(*) "
            + "from emp "
            + "group by deptno "
            + "order by deptno * sum(sal) desc, min(empno)",
        "${plan}");
  }

  @Test public void testCountNoGroup() {
    check(
        "select count(*), sum(sal)\n"
            + "from emp\n"
            + "where empno > 10",
        "${plan}");
  }

  @Test public void testWith() {
    check("with emp2 as (select * from emp)\n"
            + "select * from emp2",
        "${plan}");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-309">[CALCITE-309]
   * WITH ... ORDER BY query gives AssertionError</a>. */
  @Test public void testWithOrder() {
    check("with emp2 as (select * from emp)\n"
            + "select * from emp2 order by deptno",
        "${plan}");
  }

  @Test public void testWithUnionOrder() {
    check("with emp2 as (select empno, deptno as x from emp)\n"
            + "select * from emp2\n"
            + "union all\n"
            + "select * from emp2\n"
            + "order by empno + x",
        "${plan}");
  }

  @Test public void testWithUnion() {
    check("with emp2 as (select * from emp where deptno > 10)\n"
            + "select empno from emp2 where deptno < 30 union all select deptno from emp",
        "${plan}");
  }

  @Test public void testWithAlias() {
    check("with w(x, y) as (select * from dept where deptno > 10)\n"
            + "select x from w where x < 30 union all select deptno from dept",
        "${plan}");
  }

  @Test public void testWithInsideWhereExists() {
    tester.withDecorrelation(false).assertConvertsTo("select * from emp\n"
            + "where exists (\n"
            + "  with dept2 as (select * from dept where dept.deptno >= emp.deptno)\n"
            + "  select 1 from dept2 where deptno <= emp.deptno)",
        "${plan}");
  }

  @Test public void testWithInsideWhereExistsDecorrelate() {
    tester.withDecorrelation(true).assertConvertsTo("select * from emp\n"
            + "where exists (\n"
            + "  with dept2 as (select * from dept where dept.deptno >= emp.deptno)\n"
            + "  select 1 from dept2 where deptno <= emp.deptno)",
        "${plan}");
  }

  @Test public void testWithInsideScalarSubquery() {
    check("select (\n"
            + " with dept2 as (select * from dept where deptno > 10)"
            + " select count(*) from dept2) as c\n"
            + "from emp",
        "${plan}");
  }

  @Test public void testTableExtend() {
    sql("select * from dept extend (x varchar(5) not null)")
        .convertsTo("${plan}");
  }

  @Test public void testExplicitTable() {
    check(
        "table emp",
        "${plan}");
  }

  @Test public void testCollectionTable() {
    check(
        "select * from table(ramp(3))",
        "${plan}");
  }

  @Test public void testSample() {
    check(
        "select * from emp tablesample substitute('DATASET1') where empno > 5",
        "${plan}");
  }

  @Test public void testSampleQuery() {
    check(
        "select * from (\n"
            + " select * from emp as e tablesample substitute('DATASET1')\n"
            + " join dept on e.deptno = dept.deptno\n"
            + ") tablesample substitute('DATASET2')\n"
            + "where empno > 5",
        "${plan}");
  }

  @Test public void testSampleBernoulli() {
    check(
        "select * from emp tablesample bernoulli(50) where empno > 5",
        "${plan}");
  }

  @Test public void testSampleBernoulliQuery() {
    check(
        "select * from (\n"
            + " select * from emp as e tablesample bernoulli(10) repeatable(1)\n"
            + " join dept on e.deptno = dept.deptno\n"
            + ") tablesample bernoulli(50) repeatable(99)\n"
            + "where empno > 5",
        "${plan}");
  }

  @Test public void testSampleSystem() {
    check(
        "select * from emp tablesample system(50) where empno > 5",
        "${plan}");
  }

  @Test public void testSampleSystemQuery() {
    check(
        "select * from (\n"
            + " select * from emp as e tablesample system(10) repeatable(1)\n"
            + " join dept on e.deptno = dept.deptno\n"
            + ") tablesample system(50) repeatable(99)\n"
            + "where empno > 5",
        "${plan}");
  }

  @Test public void testCollectionTableWithCursorParam() {
    tester.withDecorrelation(false).assertConvertsTo(
        "select * from table(dedup(" + "cursor(select ename from emp),"
            + " cursor(select name from dept), 'NAME'))",
        "${plan}");
  }

  @Test public void testUnnest() {
    check(
        "select*from unnest(multiset[1,2])",
        "${plan}");
  }

  @Test public void testUnnestSubquery() {
    check("select*from unnest(multiset(select*from dept))", "${plan}");
  }

  @Test public void testMultisetSubquery() {
    check(
        "select multiset(select deptno from dept) from (values(true))",
        "${plan}");
  }

  @Test public void testMultiset() {
    check(
        "select 'a',multiset[10] from dept",
        "${plan}");
  }

  @Test public void testMultisetOfColumns() {
    check(
        "select 'abc',multiset[deptno,sal] from emp",
        "${plan}");
  }

  @Test public void testCorrelationJoin() {
    check(
        "select *,"
            + "         multiset(select * from emp where deptno=dept.deptno) "
            + "               as empset"
            + "      from dept",
        "${plan}");
  }

  @Test public void testExists() {
    check(
        "select*from emp where exists (select 1 from dept where deptno=55)",
        "${plan}");
  }

  @Test public void testExistsCorrelated() {
    tester.withDecorrelation(false).assertConvertsTo(
        "select*from emp where exists (select 1 from dept where emp.deptno=dept.deptno)",
        "${plan}");
  }

  @Test public void testExistsCorrelatedDecorrelate() {
    tester.withDecorrelation(true).assertConvertsTo(
        "select*from emp where exists (select 1 from dept where emp.deptno=dept.deptno)",
        "${plan}");
  }

  @Test public void testExistsCorrelatedLimit() {
    tester.withDecorrelation(false).assertConvertsTo(
        "select*from emp where exists (\n"
            + "  select 1 from dept where emp.deptno=dept.deptno limit 1)",
        "${plan}");
  }

  @Test public void testExistsCorrelatedLimitDecorrelate() {
    tester.withDecorrelation(true).assertConvertsTo(
        "select*from emp where exists (\n"
            + "  select 1 from dept where emp.deptno=dept.deptno limit 1)",
        "${plan}");
  }

  @Test public void testInValueListShort() {
    check("select empno from emp where deptno in (10, 20)", "${plan}");
  }

  @Test public void testInValueListLong() {
    // Go over the default threshold of 20 to force a subquery.
    check("select empno from emp where deptno in"
            + " (10, 20, 30, 40, 50, 60, 70, 80, 90, 100"
            + ", 110, 120, 130, 140, 150, 160, 170, 180, 190"
            + ", 200, 210, 220, 230)", "${plan}");
  }

  @Test public void testInUncorrelatedSubquery() {
    check(
        "select empno from emp where deptno in"
            + " (select deptno from dept)",
        "${plan}");
  }

  @Test public void testNotInUncorrelatedSubquery() {
    check(
        "select empno from emp where deptno not in"
            + " (select deptno from dept)",
        "${plan}");
  }

  @Test public void testInUncorrelatedSubqueryInSelect() {
    // In the SELECT clause, the value of IN remains in 3-valued logic
    // -- it's not forced into 2-valued by the "... IS TRUE" wrapper as in the
    // WHERE clause -- so the translation is more complicated.
    check(
        "select name, deptno in (\n"
            + "  select case when true then deptno else null end from emp)\n"
            + "from dept",
        "${plan}");
  }

  /** Plan should be as {@link #testInUncorrelatedSubqueryInSelect}, but with
   * an extra NOT. Both queries require 3-valued logic. */
  @Test public void testNotInUncorrelatedSubqueryInSelect() {
    check(
        "select empno, deptno not in (\n"
            + "  select case when true then deptno else null end from dept)\n"
            + "from emp",
        "${plan}");
  }

  /** Since 'deptno NOT IN (SELECT deptno FROM dept)' can not be null, we
   * generate a simpler plan. */
  @Test public void testNotInUncorrelatedSubqueryInSelectNotNull() {
    check(
        "select empno, deptno not in (\n"
            + "  select deptno from dept)\n"
            + "from emp",
        "${plan}");
  }

  @Test public void testUnnestSelect() {
    check(
        "select*from unnest(select multiset[deptno] from dept)",
        "${plan}");
  }

  @Test public void testJoinUnnest() {
    check(
        "select*from dept as d, unnest(multiset[d.deptno * 2])",
        "${plan}");
  }

  @Test public void testLateral() {
    tester.withDecorrelation(false).assertConvertsTo(
        "select * from emp, LATERAL (select * from dept where emp.deptno=dept.deptno)",
        "${plan}");
  }

  @Test public void testLateralDecorrelate() {
    tester.withDecorrelation(true).assertConvertsTo(
        "select * from emp, LATERAL (select * from dept where emp.deptno=dept.deptno)",
        "${plan}");
  }

  @Test public void testNestedCorrelations() {
    tester.withDecorrelation(false).assertConvertsTo(
        "select * from (select 2+deptno d2, 3+deptno d3 from emp) e\n"
            + " where exists (select 1 from (select deptno+1 d1 from dept) d\n"
            + " where d1=e.d2 and exists (select 2 from (select deptno+4 d4, deptno+5 d5, deptno+6 d6 from dept)\n"
            + " where d4=d.d1 and d5=d.d1 and d6=e.d3))",
        "${plan}");
  }

  @Test public void testNestedCorrelationsDecorrelated() {
    tester.withDecorrelation(true).assertConvertsTo(
        "select * from (select 2+deptno d2, 3+deptno d3 from emp) e\n"
            + " where exists (select 1 from (select deptno+1 d1 from dept) d\n"
            + " where d1=e.d2 and exists (select 2 from (select deptno+4 d4, deptno+5 d5, deptno+6 d6 from dept)\n"
            + " where d4=d.d1 and d5=d.d1 and d6=e.d3))",
        "${plan}");
  }

  @Test public void testElement() {
    check("select element(multiset[5]) from emp", "${plan}");
  }

  @Test public void testElementInValues() {
    check("values element(multiset[5])", "${plan}");
  }

  @Test public void testUnionAll() {
    // union all
    check(
        "select empno from emp union all select deptno from dept",
        "${plan}");
  }

  @Test public void testUnion() {
    // union without all
    check(
        "select empno from emp union select deptno from dept",
        "${plan}");
  }

  @Test public void testUnionValues() {
    // union with values
    check(
        "values (10), (20)\n"
            + "union all\n"
            + "select 34 from emp\n"
            + "union all values (30), (45 + 10)",
        "${plan}");
  }

  @Test public void testUnionSubquery() {
    // union of subquery, inside from list, also values
    check(
        "select deptno from emp as emp0 cross join\n"
            + " (select empno from emp union all\n"
            + "  select deptno from dept where deptno > 20 union all\n"
            + "  values (45), (67))",
        "${plan}");
  }

  @Test public void testIsDistinctFrom() {
    check(
        "select 1 is distinct from 2 from (values(true))",
        "${plan}");
  }

  @Test public void testIsNotDistinctFrom() {
    check(
        "select 1 is not distinct from 2 from (values(true))",
        "${plan}");
  }

  @Test public void testNotLike() {
    // note that 'x not like y' becomes 'not(x like y)'
    check(
        "values ('a' not like 'b' escape 'c')",
        "${plan}");
  }

  @Test public void testOverMultiple() {
    check(
        "select sum(sal) over w1,\n"
            + "  sum(deptno) over w1,\n"
            + "  sum(deptno) over w2\n"
            + "from emp\n"
            + "where deptno - sal > 999\n"
            + "window w1 as (partition by job order by hiredate rows 2 preceding),\n"
            + "  w2 as (partition by job order by hiredate rows 3 preceding disallow partial),\n"
            + "  w3 as (partition by job order by hiredate range interval '1' second preceding)",
        "${plan}");
  }

  /**
   * Test one of the custom conversions which is recognized by the class of the
   * operator (in this case,
   * {@link org.apache.calcite.sql.fun.SqlCaseOperator}).
   */
  @Test public void testCase() {
    check(
        "values (case 'a' when 'a' then 1 end)",
        "${plan}");
  }

  /**
   * Tests one of the custom conversions which is recognized by the identity
   * of the operator (in this case,
   * {@link org.apache.calcite.sql.fun.SqlStdOperatorTable#CHARACTER_LENGTH}).
   */
  @Test public void testCharLength() {
    // Note that CHARACTER_LENGTH becomes CHAR_LENGTH.
    check(
        "values (character_length('foo'))",
        "${plan}");
  }

  @Test public void testOverAvg() {
    // AVG(x) gets translated to SUM(x)/COUNT(x).  Because COUNT controls
    // the return type there usually needs to be a final CAST to get the
    // result back to match the type of x.
    check(
        "select sum(sal) over w1,\n"
            + "  avg(sal) over w1\n"
            + "from emp\n"
            + "window w1 as (partition by job order by hiredate rows 2 preceding)",
        "${plan}");
  }

  @Test public void testOverAvg2() {
    // Check to see if extra CAST is present.  Because CAST is nested
    // inside AVG it passed to both SUM and COUNT so the outer final CAST
    // isn't needed.
    check(
        "select sum(sal) over w1,\n"
            + "  avg(CAST(sal as real)) over w1\n"
            + "from emp\n"
            + "window w1 as (partition by job order by hiredate rows 2 preceding)",
        "${plan}");
  }

  @Test public void testOverCountStar() {
    check(
        "select count(sal) over w1,\n"
            + "  count(*) over w1\n"
            + "from emp\n"
            + "window w1 as (partition by job order by hiredate rows 2 preceding)",

        "${plan}");
  }

  /**
   * Tests that a window containing only ORDER BY is implicitly CURRENT ROW.
   */
  @Test public void testOverOrderWindow() {
    check(
        "select last_value(deptno) over w\n"
            + "from emp\n"
            + "window w as (order by empno)",
        "${plan}");

    // Same query using inline window
    check(
        "select last_value(deptno) over (order by empno)\n"
            + "from emp\n",
        "${plan}");
  }

  /**
   * Tests that a window with a FOLLOWING bound becomes BETWEEN CURRENT ROW
   * AND FOLLOWING.
   */
  @Test public void testOverOrderFollowingWindow() {
    // Window contains only ORDER BY (implicitly CURRENT ROW).
    check(
        "select last_value(deptno) over w\n"
            + "from emp\n"
            + "window w as (order by empno rows 2 following)",
        "${plan}");

    // Same query using inline window
    check(
        "select last_value(deptno) over (order by empno rows 2 following)\n"
            + "from emp\n",
        "${plan}");
  }

  @Test public void testInterval() {
    // temporarily disabled per DTbug 1212
    if (Bug.DT785_FIXED) {
      check(
          "values(cast(interval '1' hour as interval hour to second))",
          "${plan}");
    }
  }

  @Test public void testStream() {
    sql("select stream productId from orders where productId = 10")
        .convertsTo("${plan}");
  }

  @Test public void testStreamGroupBy() {
    sql("select stream floor(rowtime to second) as rowtime, count(*) as c\n"
            + "from orders\n"
            + "group by floor(rowtime to second)")
        .convertsTo("${plan}");
  }

  @Test public void testStreamWindowedAggregation() {
    sql("select stream *,\n"
            + "  count(*) over (partition by productId\n"
            + "    order by rowtime\n"
            + "    range interval '1' second preceding) as c\n"
            + "from orders")
        .convertsTo("${plan}");
  }

  @Test public void testExplainAsXml() {
    String sql = "select 1 + 2, 3 from (values (true))";
    final RelNode rel = tester.convertSqlToRel(sql);
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
    tester.assertConvertsTo(
        "select ename from (select * from emp order by sal) a", "${plan}",
        true);
  }

  /**
   * Test group-by CASE expression involving a non-query IN
   */
  @Test public void testGroupByCaseSubquery() {
    sql("SELECT CASE WHEN emp.empno IN (3) THEN 0 ELSE 1 END\n"
        + "FROM emp\n"
        + "GROUP BY (CASE WHEN emp.empno IN (3) THEN 0 ELSE 1 END)")
        .convertsTo("${plan}");
  }

  /**
   * Test aggregate function on a CASE expression involving a non-query IN
   */
  @Test public void testAggCaseSubquery() {
    sql("SELECT SUM(CASE WHEN empno IN (3) THEN 0 ELSE 1 END) FROM emp")
        .convertsTo("${plan}");
  }

  @Test public void testAggScalarSubquery() {
    sql("SELECT SUM(SELECT min(deptno) FROM dept) FROM emp")
        .convertsTo("${plan}");
  }

  /** Test aggregate function on a CASE expression involving IN with a
   * sub-query */
  @Ignore("[CALCITE-551] Sub-query inside aggregate function")
  @Test public void testAggCaseInSubquery() {
    sql("SELECT SUM(\n"
        + "  CASE WHEN deptno IN (SELECT deptno FROM dept) THEN 1 ELSE 0 END)\n"
        + "FROM emp")
        .convertsTo("${plan}");
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-614">[CALCITE-614]
   * IN within CASE within GROUP BY gives AssertionError</a>.
   */
  @Test public void testGroupByCaseIn() {
    sql("select (CASE WHEN (deptno IN (10, 20)) THEN 0 ELSE deptno END),\n"
        + " min(empno) from EMP\n"
        + "group by (CASE WHEN (deptno IN (10, 20)) THEN 0 ELSE deptno END)")
        .convertsTo("${plan}");
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-695">[CALCITE-695]
   * SqlSingleValueAggFunction is created when it may not be needed</a>.
   */
  @Test public void testSubqueryAggreFunctionFollowedBySimpleOperation() {
    sql("select deptno\n"
        + "from EMP\n"
        + "where deptno > (select min(deptno) * 2 + 10 from EMP)")
        .convertsTo("${plan}");
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-695">[CALCITE-695]
   * SqlSingleValueAggFunction is created when it may not be needed</a>.
   */
  @Test public void testSubqueryValues() {
    sql("select deptno\n"
        + "from EMP\n"
        + "where deptno > (values 10)")
        .convertsTo("${plan}");
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-695">[CALCITE-695]
   * SqlSingleValueAggFunction is created when it may not be needed</a>.
   */
  @Test public void testSubqueryLimitOne() {
    sql("select deptno\n"
        + "from EMP\n"
        + "where deptno > (select deptno \n"
        + "from EMP order by deptno limit 1)")
        .convertsTo("${plan}");
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-710">[CALCITE-710]
   * When look up subqueries, perform the same logic as the way when ones were
   * registered</a>.
   */
  @Test public void testIdenticalExpressionInSubquery() {
    sql("select deptno\n"
        + "from EMP\n"
        + "where deptno in (1, 2) or deptno in (1, 2)")
        .convertsTo("${plan}");
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-694">[CALCITE-694]
   * Scan HAVING clause for sub-queries and IN-lists</a> relating to IN.
   */
  @Test public void testHavingAggrFunctionIn() {
    sql("select deptno \n"
        + "from emp \n"
        + "group by deptno \n"
        + "having sum(case when deptno in (1, 2) then 0 else 1 end) + \n"
        + "sum(case when deptno in (3, 4) then 0 else 1 end) > 10")
        .convertsTo("${plan}");
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-694">[CALCITE-694]
   * Scan HAVING clause for sub-queries and IN-lists</a>, with a sub-query in
   * the HAVING clause.
   */
  @Test public void testHavingInSubqueryWithAggrFunction() {
    sql("select sal \n"
        + "from emp \n"
        + "group by sal \n"
        + "having sal in \n"
            + "(select deptno \n"
            + "from dept \n"
            + "group by deptno \n"
            + "having sum(deptno) > 0)")
        .convertsTo("${plan}");
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-716">[CALCITE-716]
   * Scalar sub-query and aggregate function in SELECT or HAVING clause gives
   * AssertionError</a>; variant involving HAVING clause.
   */
  @Test public void testAggregateAndScalarSubQueryInHaving() {
    sql("select deptno\n"
            + "from emp\n"
            + "group by deptno\n"
            + "having max(emp.empno) > (SELECT min(emp.empno) FROM emp)\n")
        .convertsTo("${plan}");
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-716">[CALCITE-716]
   * Scalar sub-query and aggregate function in SELECT or HAVING clause gives
   * AssertionError</a>; variant involving SELECT clause.
   */
  @Test public void testAggregateAndScalarSubQueryInSelect() {
    sql("select deptno,\n"
            + "  max(emp.empno) > (SELECT min(emp.empno) FROM emp) as b\n"
            + "from emp\n"
            + "group by deptno\n")
        .convertsTo("${plan}");
  }

  /**
   * Visitor that checks that every {@link RelNode} in a tree is valid.
   *
   * @see RelNode#isValid(boolean)
   */
  public static class RelValidityChecker extends RelVisitor {
    int invalidCount;

    public void visit(RelNode node, int ordinal, RelNode parent) {
      if (!node.isValid(true)) {
        ++invalidCount;
      }
      super.visit(node, ordinal, parent);
    }
  }

  /** Allows fluent testing. */
  public class Sql {
    private final String sql;

    Sql(String sql) {
      this.sql = sql;
    }

    public void ok() {
      convertsTo("${plan}");
    }

    public void convertsTo(String plan) {
      tester.assertConvertsTo(sql, plan);
    }
  }
}

// End SqlToRelConverterTest.java
