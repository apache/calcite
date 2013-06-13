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
package org.eigenbase.test;

import java.io.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.sql.*;
import org.eigenbase.util.*;

import org.junit.Test;


/**
 * Unit test for {@link org.eigenbase.sql2rel.SqlToRelConverter}.
 */
public class SqlToRelConverterTest
    extends SqlToRelTestBase
{
    //~ Methods ----------------------------------------------------------------

    public SqlToRelConverterTest() {
        super();
    }

    protected DiffRepository getDiffRepos()
    {
        return DiffRepository.lookup(SqlToRelConverterTest.class);
    }

    protected final void check(
        String sql,
        String plan)
    {
        tester.assertConvertsTo(sql, plan);
    }

    @Test public void testIntegerLiteral() {
        check(
            "select 1 from emp",
            "${plan}");
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

    @Test public void testJoinOn() {
        check(
            "SELECT * FROM emp JOIN dept on emp.deptno = dept.deptno",
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
        check(
            "select name, foo from ("
            + "select deptno, name, count(deptno) as foo "
            + "from dept "
            + "group by name, deptno, name)",
            "${plan}");
    }

    @Test public void testAggDistinct() {
        check(
            "select deptno, sum(sal), sum(distinct sal), count(*) "
            + "from emp "
            + "group by deptno",
            "${plan}");
    }

    @Test public void testSelectDistinct() {
        check(
            "select distinct sal + 5 from emp",
            "${plan}");
    }

    @Test public void testSelectDistinctGroup() {
        check(
            "select distinct sum(sal) from emp group by deptno",
            "${plan}");
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

    @Test public void testOrder() {
        check(
            "select empno from emp order by empno",
            "${plan}");
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
        check(
            "select * from table(dedup("
            + "cursor(select ename from emp),"
            + " cursor(select name from dept), 'NAME'))",
            "${plan}");
    }

    @Test public void testUnnest() {
        check(
            "select*from unnest(multiset[1,2])",
            "${plan}");
    }

    @Test public void testUnnestSubquery() {
        check(
            "select*from unnest(multiset(select*from dept))",
            "${plan}");
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
        check(
            "select*from emp where exists (select 1 from dept where emp.deptno=dept.deptno)",
            "${plan}");
    }

    @Test public void testInValueListShort() {
        check("select empno from emp where deptno in (10, 20)", "${plan}");
    }

    @Test public void testInValueListLong() {
        // Go over the default threshold of 20 to force a subquery.
        check(
            "select empno from emp where deptno in"
            + " (10, 20, 30, 40, 50, 60, 70, 80, 90, 100"
            + ", 110, 120, 130, 140, 150, 160, 170, 180, 190"
            + ", 200, 210, 220, 230)",
            "${plan}");
    }

    @Test public void testInUncorrelatedSubquery() {
        check(
            "select empno from emp where deptno in"
            + " (select deptno from dept)",
            "${plan}");
    }

    @Test public void testUnnestSelect() {
        check(
            "select*from unnest(select multiset[deptno] from dept)",
            "${plan}");
    }

    @Test public void testLateral() {
        check(
            "select * from emp, LATERAL (select * from dept where emp.deptno=dept.deptno)",
            "${plan}");
    }

    @Test public void testElement() {
        check(
            "select element(multiset[5]) from emp",
            "${plan}");
    }

    @Test public void testElementInValues() {
        check(
            "values element(multiset[5])",
            "${plan}");
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
            + " (select empno from emp union all \n"
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
     * Test one of the custom conversions which is recognized by the class of
     * the operator (in this case, {@link
     * org.eigenbase.sql.fun.SqlCaseOperator}).
     */
    @Test public void testCase() {
        check(
            "values (case 'a' when 'a' then 1 end)",
            "${plan}");
    }

    /**
     * Tests one of the custom conversions which is recognized by the identity
     * of the operator (in this case, {@link
     * org.eigenbase.sql.fun.SqlStdOperatorTable#characterLengthFunc}).
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
        if (Bug.Dt785Fixed) {
            check(
                "values(cast(interval '1' hour as interval hour to second))",
                "${plan}");
        }
    }

    @Test public void testExplainAsXml() {
        String sql = "select 1 + 2, 3 from (values (true))";
        final RelNode rel = tester.convertSqlToRel(sql);
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        RelOptXmlPlanWriter planWriter =
            new RelOptXmlPlanWriter(pw, SqlExplainLevel.EXPPLAN_ATTRIBUTES);
        rel.explain(planWriter);
        pw.flush();
        TestUtil.assertEqualsVerbose(
            TestUtil.fold(
                "<RelNode type=\"ProjectRel\">\n"
                + "\t<Property name=\"EXPR$0\">\n"
                + "\t\t+(1, 2)\t</Property>\n"
                + "\t<Property name=\"EXPR$1\">\n"
                + "\t\t3\t</Property>\n"
                + "\t<Inputs>\n"
                + "\t\t<RelNode type=\"ValuesRel\">\n"
                + "\t\t\t<Property name=\"tuples\">\n"
                + "\t\t\t\t[{ true }]\t\t\t</Property>\n"
                + "\t\t\t<Inputs/>\n"
                + "\t\t</RelNode>\n"
                + "\t</Inputs>\n"
                + "</RelNode>\n"
                + ""),
            sw.toString());
    }

    /**
     * Visitor that checks that every {@link RelNode} in a tree is valid.
     *
     * @see RelNode#isValid(boolean)
     */
    public static class RelValidityChecker extends RelVisitor
    {
        int invalidCount;

        public void visit(RelNode node, int ordinal, RelNode parent)
        {
            if (!node.isValid(true)) {
                ++invalidCount;
            }
            super.visit(node, ordinal, parent);
        }
    }
}

// End SqlToRelConverterTest.java
