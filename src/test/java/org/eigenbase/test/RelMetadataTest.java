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

import java.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;

import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Unit test for {@link DefaultRelMetadataProvider}. See {@link
 * SqlToRelTestBase} class comments for details on the schema used. Note that no
 * optimizer rules are fired on the translation of the SQL into relational
 * algebra (e.g. join conditions in the WHERE clause will look like filters), so
 * it's necessary to phrase the SQL carefully.
 */
public class RelMetadataTest
    extends SqlToRelTestBase
{
    //~ Static fields/initializers ---------------------------------------------

    private static final double EPSILON = 1.0e-5;

    private static final double DEFAULT_EQUAL_SELECTIVITY = 0.15;

    private static final double DEFAULT_EQUAL_SELECTIVITY_SQUARED =
        DEFAULT_EQUAL_SELECTIVITY * DEFAULT_EQUAL_SELECTIVITY;

    private static final double DEFAULT_COMP_SELECTIVITY = 0.5;

    private static final double DEFAULT_NOTNULL_SELECTIVITY = 0.9;

    private static final double DEFAULT_SELECTIVITY = 0.25;

    private static final double EMP_SIZE = 1000.0;

    private static final double DEPT_SIZE = 100.0;

    //~ Methods ----------------------------------------------------------------

    // ----------------------------------------------------------------------
    // Tests for getPercentageOriginalRows
    // ----------------------------------------------------------------------

    private RelNode convertSql(String sql)
    {
        RelNode rel = tester.convertSqlToRel(sql);
        DefaultRelMetadataProvider provider = new DefaultRelMetadataProvider();
        rel.getCluster().setMetadataProvider(provider);
        return rel;
    }

    private void checkPercentageOriginalRows(String sql, double expected)
    {
        checkPercentageOriginalRows(sql, expected, EPSILON);
    }

    private void checkPercentageOriginalRows(
        String sql,
        double expected,
        double epsilon)
    {
        RelNode rel = convertSql(sql);
        Double result = RelMetadataQuery.getPercentageOriginalRows(rel);
        assertTrue(result != null);
        assertEquals(
            expected,
            result.doubleValue(),
            epsilon);
    }

    @Test public void testPercentageOriginalRowsTableOnly() {
        checkPercentageOriginalRows(
            "select * from dept",
            1.0);
    }

    @Test public void testPercentageOriginalRowsAgg() {
        checkPercentageOriginalRows(
            "select deptno from dept group by deptno",
            1.0);
    }

    @Ignore
    @Test public void testPercentageOriginalRowsOneFilter() {
        checkPercentageOriginalRows(
            "select * from dept where deptno = 20",
            DEFAULT_EQUAL_SELECTIVITY);
    }

    @Ignore
    @Test public void testPercentageOriginalRowsTwoFilters() {
        checkPercentageOriginalRows(
            "select * from (select * from dept where name='X')"
            + " where deptno = 20",
            DEFAULT_EQUAL_SELECTIVITY_SQUARED);
    }

    @Ignore
    @Test public void testPercentageOriginalRowsRedundantFilter() {
        checkPercentageOriginalRows(
            "select * from (select * from dept where deptno=20)"
            + " where deptno = 20",
            DEFAULT_EQUAL_SELECTIVITY);
    }

    @Test public void testPercentageOriginalRowsJoin() {
        checkPercentageOriginalRows(
            "select * from emp inner join dept on emp.deptno=dept.deptno",
            1.0);
    }

    @Ignore
    @Test public void testPercentageOriginalRowsJoinTwoFilters() {
        checkPercentageOriginalRows(
            "select * from (select * from emp where deptno=10) e"
            + " inner join (select * from dept where deptno=10) d"
            + " on e.deptno=d.deptno",
            DEFAULT_EQUAL_SELECTIVITY_SQUARED);
    }

    @Test public void testPercentageOriginalRowsUnionNoFilter() {
        checkPercentageOriginalRows(
            "select name from dept union all select ename from emp",
            1.0);
    }

    @Ignore
    @Test public void testPercentageOriginalRowsUnionLittleFilter() {
        checkPercentageOriginalRows(
            "select name from dept where deptno=20"
            + " union all select ename from emp",
            ((DEPT_SIZE * DEFAULT_EQUAL_SELECTIVITY) + EMP_SIZE)
            / (DEPT_SIZE + EMP_SIZE));
    }

    @Ignore
    @Test public void testPercentageOriginalRowsUnionBigFilter() {
        checkPercentageOriginalRows(
            "select name from dept"
            + " union all select ename from emp where deptno=20",
            ((EMP_SIZE * DEFAULT_EQUAL_SELECTIVITY) + DEPT_SIZE)
            / (DEPT_SIZE + EMP_SIZE));
    }

    // ----------------------------------------------------------------------
    // Tests for getColumnOrigins
    // ----------------------------------------------------------------------

    private Set<RelColumnOrigin> checkColumnOrigin(String sql)
    {
        RelNode rel = convertSql(sql);
        return RelMetadataQuery.getColumnOrigins(rel, 0);
    }

    private void checkNoColumnOrigin(String sql)
    {
        Set<RelColumnOrigin> result = checkColumnOrigin(sql);
        assertTrue(result != null);
        assertTrue(result.isEmpty());
    }

    public static void checkColumnOrigin(
        RelColumnOrigin rco,
        String expectedTableName,
        String expectedColumnName,
        boolean expectedDerived)
    {
        RelOptTable actualTable = rco.getOriginTable();
        String [] actualTableName = actualTable.getQualifiedName();
        assertEquals(
            actualTableName[actualTableName.length - 1],
            expectedTableName);
        assertEquals(
            actualTable.getRowType().getFields()[rco.getOriginColumnOrdinal()]
            .getName(),
            expectedColumnName);
        assertEquals(
            rco.isDerived(),
            expectedDerived);
    }

    private void checkSingleColumnOrigin(
        String sql,
        String expectedTableName,
        String expectedColumnName,
        boolean expectedDerived)
    {
        Set<RelColumnOrigin> result = checkColumnOrigin(sql);
        assertTrue(result != null);
        assertEquals(
            1,
            result.size());
        RelColumnOrigin rco = result.iterator().next();
        checkColumnOrigin(
            rco,
            expectedTableName,
            expectedColumnName,
            expectedDerived);
    }

    // WARNING:  this requires the two table names to be different
    private void checkTwoColumnOrigin(
        String sql,
        String expectedTableName1,
        String expectedColumnName1,
        String expectedTableName2,
        String expectedColumnName2,
        boolean expectedDerived)
    {
        Set<RelColumnOrigin> result = checkColumnOrigin(sql);
        assertTrue(result != null);
        assertEquals(
            2,
            result.size());
        for (RelColumnOrigin rco : result) {
            RelOptTable actualTable = rco.getOriginTable();
            String [] actualTableName = actualTable.getQualifiedName();
            String actualUnqualifiedName =
                actualTableName[actualTableName.length - 1];
            if (actualUnqualifiedName.equals(expectedTableName1)) {
                checkColumnOrigin(
                    rco,
                    expectedTableName1,
                    expectedColumnName1,
                    expectedDerived);
            } else {
                checkColumnOrigin(
                    rco,
                    expectedTableName2,
                    expectedColumnName2,
                    expectedDerived);
            }
        }
    }

    @Test public void testColumnOriginsTableOnly() {
        checkSingleColumnOrigin(
            "select name as dname from dept",
            "DEPT",
            "NAME",
            false);
    }

    @Test public void testColumnOriginsExpression() {
        checkSingleColumnOrigin(
            "select upper(name) as dname from dept",
            "DEPT",
            "NAME",
            true);
    }

    @Test public void testColumnOriginsDyadicExpression() {
        checkTwoColumnOrigin(
            "select name||ename from dept,emp",
            "DEPT",
            "NAME",
            "EMP",
            "ENAME",
            true);
    }

    @Test public void testColumnOriginsConstant() {
        checkNoColumnOrigin(
            "select 'Minstrelsy' as dname from dept");
    }

    @Test public void testColumnOriginsFilter() {
        checkSingleColumnOrigin(
            "select name as dname from dept where deptno=10",
            "DEPT",
            "NAME",
            false);
    }

    @Test public void testColumnOriginsJoinLeft() {
        checkSingleColumnOrigin(
            "select ename from emp,dept",
            "EMP",
            "ENAME",
            false);
    }

    @Test public void testColumnOriginsJoinRight() {
        checkSingleColumnOrigin(
            "select name as dname from emp,dept",
            "DEPT",
            "NAME",
            false);
    }

    @Test public void testColumnOriginsJoinOuter() {
        checkSingleColumnOrigin(
            "select name as dname from emp left outer join dept"
            + " on emp.deptno = dept.deptno",
            "DEPT",
            "NAME",
            true);
    }

    @Test public void testColumnOriginsJoinFullOuter() {
        checkSingleColumnOrigin(
            "select name as dname from emp full outer join dept"
            + " on emp.deptno = dept.deptno",
            "DEPT",
            "NAME",
            true);
    }

    @Test public void testColumnOriginsAggKey() {
        checkSingleColumnOrigin(
            "select name,count(deptno) from dept group by name",
            "DEPT",
            "NAME",
            false);
    }

    @Test public void testColumnOriginsAggMeasure() {
        checkSingleColumnOrigin(
            "select count(deptno),name from dept group by name",
            "DEPT",
            "DEPTNO",
            true);
    }

    @Test public void testColumnOriginsAggCountStar() {
        checkNoColumnOrigin(
            "select count(*),name from dept group by name");
    }

    @Test public void testColumnOriginsValues() {
        checkNoColumnOrigin(
            "values(1,2,3)");
    }

    @Test public void testColumnOriginsUnion() {
        checkTwoColumnOrigin(
            "select name from dept union all select ename from emp",
            "DEPT",
            "NAME",
            "EMP",
            "ENAME",
            false);
    }

    @Test public void testColumnOriginsSelfUnion() {
        checkSingleColumnOrigin(
            "select ename from emp union all select ename from emp",
            "EMP",
            "ENAME",
            false);
    }

    private void checkRowCount(
        String sql,
        double expected)
    {
        RelNode rel = convertSql(sql);
        Double result = RelMetadataQuery.getRowCount(rel);
        assertTrue(result != null);
        assertEquals(
            expected,
            result.doubleValue(),
            0d);
    }

    @Ignore
    @Test public void testRowCountEmp() {
        checkRowCount(
            "select * from emp",
            EMP_SIZE);
    }

    @Ignore
    @Test public void testRowCountDept() {
        checkRowCount(
            "select * from dept",
            DEPT_SIZE);
    }

    @Ignore
    @Test public void testRowCountCartesian() {
        checkRowCount(
            "select * from emp,dept",
            EMP_SIZE * DEPT_SIZE);
    }

    @Ignore
    @Test public void testRowCountJoin() {
        checkRowCount(
            "select * from emp inner join dept on emp.deptno = dept.deptno",
            EMP_SIZE * DEPT_SIZE * DEFAULT_EQUAL_SELECTIVITY);
    }

    @Ignore
    @Test public void testRowCountUnion() {
        checkRowCount(
            "select ename from emp union all select name from dept",
            EMP_SIZE + DEPT_SIZE);
    }

    @Ignore
    @Test public void testRowCountFilter() {
        checkRowCount(
            "select * from emp where ename='Mathilda'",
            EMP_SIZE * DEFAULT_EQUAL_SELECTIVITY);
    }

    @Ignore
    @Test public void testRowCountSort() {
        checkRowCount(
            "select * from emp order by ename",
            EMP_SIZE);
    }

    private void checkFilterSelectivity(
        String sql,
        double expected)
    {
        RelNode rel = convertSql(sql);
        Double result = RelMetadataQuery.getSelectivity(rel, null);
        assertTrue(result != null);
        assertEquals(
            expected,
            result.doubleValue(),
            EPSILON);
    }

    @Test public void testSelectivityIsNotNullFilter() {
        checkFilterSelectivity(
            "select * from emp where deptno is not null",
            DEFAULT_NOTNULL_SELECTIVITY);
    }

    @Test public void testSelectivityComparisonFilter() {
        checkFilterSelectivity(
            "select * from emp where deptno > 10",
            DEFAULT_COMP_SELECTIVITY);
    }

    @Test public void testSelectivityAndFilter() {
        checkFilterSelectivity(
            "select * from emp where ename = 'foo' and deptno = 10",
            DEFAULT_EQUAL_SELECTIVITY_SQUARED);
    }

    @Test public void testSelectivityOrFilter() {
        checkFilterSelectivity(
            "select * from emp where ename = 'foo' or deptno = 10",
            DEFAULT_SELECTIVITY);
    }

    private void checkRelSelectivity(
        RelNode rel,
        double expected)
    {
        Double result = RelMetadataQuery.getSelectivity(rel, null);
        assertTrue(result != null);
        assertEquals(
            expected,
            result.doubleValue(),
            EPSILON);
    }

    @Test public void testSelectivityRedundantFilter() {
        RelNode rel = convertSql("select * from emp where deptno = 10");
        checkRelSelectivity(rel, DEFAULT_EQUAL_SELECTIVITY);
    }

    @Test public void testSelectivitySort() {
        RelNode rel =
            convertSql(
                "select * from emp where deptno = 10"
                + "order by ename");
        checkRelSelectivity(rel, DEFAULT_EQUAL_SELECTIVITY);
    }

    @Test public void testSelectivityUnion() {
        RelNode rel =
            convertSql(
                "select * from (select * from emp union all select * from emp) "
                + "where deptno = 10");
        checkRelSelectivity(rel, DEFAULT_EQUAL_SELECTIVITY);
    }

    @Test public void testSelectivityAgg() {
        RelNode rel =
            convertSql(
                "select deptno, count(*) from emp where deptno > 10 "
                + "group by deptno having count(*) = 0");
        checkRelSelectivity(
            rel,
            DEFAULT_COMP_SELECTIVITY * DEFAULT_EQUAL_SELECTIVITY);
    }

    @Test public void testDistinctRowCountTable() {
        // no unique key information is available so return null
        RelNode rel = convertSql("select * from emp where deptno = 10");
        BitSet groupKey = new BitSet();
        Double result =
            RelMetadataQuery.getDistinctRowCount(
                rel,
                groupKey,
                null);
        assertTrue(result == null);
    }
}

// End RelMetadataTest.java
