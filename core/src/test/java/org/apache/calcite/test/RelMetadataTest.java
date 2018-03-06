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

import org.apache.calcite.adapter.enumerable.EnumerableMergeJoin;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Minus;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.SemiJoin;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalExchange;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.CachingRelMetadataProvider;
import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.Metadata;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMdColumnUniqueness;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexTableInputRef.RelTableRef;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.SaffronProperties;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import org.hamcrest.CoreMatchers;
import org.hamcrest.CustomTypeSafeMatcher;
import org.hamcrest.Matcher;
import org.hamcrest.core.Is;
import org.junit.Assume;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.calcite.test.Matchers.within;

import static org.hamcrest.CoreMatchers.endsWith;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.isA;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit test for {@link DefaultRelMetadataProvider}. See
 * {@link SqlToRelTestBase} class comments for details on the schema used. Note
 * that no optimizer rules are fired on the translation of the SQL into
 * relational algebra (e.g. join conditions in the WHERE clause will look like
 * filters), so it's necessary to phrase the SQL carefully.
 */
public class RelMetadataTest extends SqlToRelTestBase {
  //~ Static fields/initializers ---------------------------------------------

  private static final double EPSILON = 1.0e-5;

  private static final double DEFAULT_EQUAL_SELECTIVITY = 0.15;

  private static final double DEFAULT_EQUAL_SELECTIVITY_SQUARED =
      DEFAULT_EQUAL_SELECTIVITY * DEFAULT_EQUAL_SELECTIVITY;

  private static final double DEFAULT_COMP_SELECTIVITY = 0.5;

  private static final double DEFAULT_NOTNULL_SELECTIVITY = 0.9;

  private static final double DEFAULT_SELECTIVITY = 0.25;

  private static final double EMP_SIZE = 14d;

  private static final double DEPT_SIZE = 4d;

  private static final List<String> EMP_QNAME = ImmutableList.of("CATALOG", "SALES", "EMP");

  /** Ensures that tests that use a lot of memory do not run at the same
   * time. */
  private static final ReentrantLock LOCK = new ReentrantLock();

  //~ Methods ----------------------------------------------------------------

  // ----------------------------------------------------------------------
  // Tests for getPercentageOriginalRows
  // ----------------------------------------------------------------------

  private RelNode convertSql(String sql) {
    final RelRoot root = tester.convertSqlToRel(sql);
    root.rel.getCluster().setMetadataProvider(DefaultRelMetadataProvider.INSTANCE);
    return root.rel;
  }

  private void checkPercentageOriginalRows(String sql, double expected) {
    checkPercentageOriginalRows(sql, expected, EPSILON);
  }

  private void checkPercentageOriginalRows(
      String sql,
      double expected,
      double epsilon) {
    RelNode rel = convertSql(sql);
    final RelMetadataQuery mq = RelMetadataQuery.instance();
    Double result = mq.getPercentageOriginalRows(rel);
    assertTrue(result != null);
    assertEquals(expected, result, epsilon);
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
    checkPercentageOriginalRows("select * from (\n"
        + "  select * from dept where name='X')\n"
        + "where deptno = 20",
        DEFAULT_EQUAL_SELECTIVITY_SQUARED);
  }

  @Ignore
  @Test public void testPercentageOriginalRowsRedundantFilter() {
    checkPercentageOriginalRows("select * from (\n"
        + "  select * from dept where deptno=20)\n"
        + "where deptno = 20",
        DEFAULT_EQUAL_SELECTIVITY);
  }

  @Test public void testPercentageOriginalRowsJoin() {
    checkPercentageOriginalRows(
        "select * from emp inner join dept on emp.deptno=dept.deptno",
        1.0);
  }

  @Ignore
  @Test public void testPercentageOriginalRowsJoinTwoFilters() {
    checkPercentageOriginalRows("select * from (\n"
        + "  select * from emp where deptno=10) e\n"
        + "inner join (select * from dept where deptno=10) d\n"
        + "on e.deptno=d.deptno",
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

  private Set<RelColumnOrigin> checkColumnOrigin(String sql) {
    RelNode rel = convertSql(sql);
    final RelMetadataQuery mq = RelMetadataQuery.instance();
    return mq.getColumnOrigins(rel, 0);
  }

  private void checkNoColumnOrigin(String sql) {
    Set<RelColumnOrigin> result = checkColumnOrigin(sql);
    assertTrue(result != null);
    assertTrue(result.isEmpty());
  }

  public static void checkColumnOrigin(
      RelColumnOrigin rco,
      String expectedTableName,
      String expectedColumnName,
      boolean expectedDerived) {
    RelOptTable actualTable = rco.getOriginTable();
    List<String> actualTableName = actualTable.getQualifiedName();
    assertEquals(
        Iterables.getLast(actualTableName),
        expectedTableName);
    assertEquals(
        actualTable.getRowType()
            .getFieldList()
            .get(rco.getOriginColumnOrdinal())
            .getName(), expectedColumnName);
    assertEquals(
        rco.isDerived(), expectedDerived);
  }

  private void checkSingleColumnOrigin(
      String sql,
      String expectedTableName,
      String expectedColumnName,
      boolean expectedDerived) {
    Set<RelColumnOrigin> result = checkColumnOrigin(sql);
    assertTrue(result != null);
    assertEquals(
        1,
        result.size());
    RelColumnOrigin rco = result.iterator().next();
    checkColumnOrigin(
        rco, expectedTableName, expectedColumnName, expectedDerived);
  }

  // WARNING:  this requires the two table names to be different
  private void checkTwoColumnOrigin(
      String sql,
      String expectedTableName1,
      String expectedColumnName1,
      String expectedTableName2,
      String expectedColumnName2,
      boolean expectedDerived) {
    Set<RelColumnOrigin> result = checkColumnOrigin(sql);
    assertTrue(result != null);
    assertEquals(
        2,
        result.size());
    for (RelColumnOrigin rco : result) {
      RelOptTable actualTable = rco.getOriginTable();
      List<String> actualTableName = actualTable.getQualifiedName();
      String actualUnqualifiedName = Iterables.getLast(actualTableName);
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

  @Test public void testColumnOriginsAggReduced() {
    checkNoColumnOrigin(
        "select count(deptno),name from dept group by name");
  }

  @Test public void testColumnOriginsAggCountNullable() {
    checkSingleColumnOrigin(
        "select count(mgr),ename from emp group by ename",
        "EMP",
        "MGR",
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

  private void checkRowCount(String sql, double expected, double expectedMin,
      double expectedMax) {
    RelNode rel = convertSql(sql);
    final RelMetadataQuery mq = RelMetadataQuery.instance();
    final Double result = mq.getRowCount(rel);
    assertThat(result, notNullValue());
    assertEquals(expected, result, 0d);
    final Double max = mq.getMaxRowCount(rel);
    assertThat(max, notNullValue());
    assertEquals(expectedMax, max, 0d);
    final Double min = mq.getMinRowCount(rel);
    assertThat(max, notNullValue());
    assertEquals(expectedMin, min, 0d);
  }

  @Test public void testRowCountEmp() {
    final String sql = "select * from emp";
    checkRowCount(sql, EMP_SIZE, 0D, Double.POSITIVE_INFINITY);
  }

  @Test public void testRowCountDept() {
    final String sql = "select * from dept";
    checkRowCount(sql, DEPT_SIZE, 0D, Double.POSITIVE_INFINITY);
  }

  @Test public void testRowCountValues() {
    final String sql = "select * from (values (1), (2)) as t(c)";
    checkRowCount(sql, 2, 2, 2);
  }

  @Test public void testRowCountCartesian() {
    final String sql = "select * from emp,dept";
    checkRowCount(sql, EMP_SIZE * DEPT_SIZE, 0D, Double.POSITIVE_INFINITY);
  }

  @Test public void testRowCountJoin() {
    final String sql = "select * from emp\n"
        + "inner join dept on emp.deptno = dept.deptno";
    checkRowCount(sql, EMP_SIZE * DEPT_SIZE * DEFAULT_EQUAL_SELECTIVITY,
        0D, Double.POSITIVE_INFINITY);
  }

  @Test public void testRowCountJoinFinite() {
    final String sql = "select * from (select * from emp limit 14) as emp\n"
        + "inner join (select * from dept limit 4) as dept\n"
        + "on emp.deptno = dept.deptno";
    checkRowCount(sql, EMP_SIZE * DEPT_SIZE * DEFAULT_EQUAL_SELECTIVITY,
        0D, 56D); // 4 * 14
  }

  @Test public void testRowCountJoinEmptyFinite() {
    final String sql = "select * from (select * from emp limit 0) as emp\n"
        + "inner join (select * from dept limit 4) as dept\n"
        + "on emp.deptno = dept.deptno";
    checkRowCount(sql, 1D, // 0, rounded up to row count's minimum 1
        0D, 0D); // 0 * 4
  }

  @Test public void testRowCountLeftJoinEmptyFinite() {
    final String sql = "select * from (select * from emp limit 0) as emp\n"
        + "left join (select * from dept limit 4) as dept\n"
        + "on emp.deptno = dept.deptno";
    checkRowCount(sql, 1D, // 0, rounded up to row count's minimum 1
        0D, 0D); // 0 * 4
  }

  @Test public void testRowCountRightJoinEmptyFinite() {
    final String sql = "select * from (select * from emp limit 0) as emp\n"
        + "right join (select * from dept limit 4) as dept\n"
        + "on emp.deptno = dept.deptno";
    checkRowCount(sql, 1D, // 0, rounded up to row count's minimum 1
        0D, 4D); // 1 * 4
  }

  @Test public void testRowCountJoinFiniteEmpty() {
    final String sql = "select * from (select * from emp limit 7) as emp\n"
        + "inner join (select * from dept limit 0) as dept\n"
        + "on emp.deptno = dept.deptno";
    checkRowCount(sql, 1D, // 0, rounded up to row count's minimum 1
        0D, 0D); // 7 * 0
  }

  @Test public void testRowCountJoinEmptyEmpty() {
    final String sql = "select * from (select * from emp limit 0) as emp\n"
        + "inner join (select * from dept limit 0) as dept\n"
        + "on emp.deptno = dept.deptno";
    checkRowCount(sql, 1D, // 0, rounded up to row count's minimum 1
        0D, 0D); // 0 * 0
  }

  @Test public void testRowCountUnion() {
    final String sql = "select ename from emp\n"
        + "union all\n"
        + "select name from dept";
    checkRowCount(sql, EMP_SIZE + DEPT_SIZE, 0D, Double.POSITIVE_INFINITY);
  }

  @Test public void testRowCountUnionOnFinite() {
    final String sql = "select ename from (select * from emp limit 100)\n"
        + "union all\n"
        + "select name from (select * from dept limit 40)";
    checkRowCount(sql, EMP_SIZE + DEPT_SIZE, 0D, 140D);
  }

  @Test public void testRowCountIntersectOnFinite() {
    final String sql = "select ename from (select * from emp limit 100)\n"
        + "intersect\n"
        + "select name from (select * from dept limit 40)";
    checkRowCount(sql, Math.min(EMP_SIZE, DEPT_SIZE), 0D, 40D);
  }

  @Test public void testRowCountMinusOnFinite() {
    final String sql = "select ename from (select * from emp limit 100)\n"
        + "except\n"
        + "select name from (select * from dept limit 40)";
    checkRowCount(sql, 4D, 0D, 100D);
  }

  @Test public void testRowCountFilter() {
    final String sql = "select * from emp where ename='Mathilda'";
    checkRowCount(sql, EMP_SIZE * DEFAULT_EQUAL_SELECTIVITY,
        0D, Double.POSITIVE_INFINITY);
  }

  @Test public void testRowCountFilterOnFinite() {
    final String sql = "select * from (select * from emp limit 10)\n"
        + "where ename='Mathilda'";
    checkRowCount(sql, 10D * DEFAULT_EQUAL_SELECTIVITY, 0D, 10D);
  }

  @Test public void testRowCountFilterFalse() {
    final String sql = "select * from (values 'a', 'b') as t(x) where false";
    checkRowCount(sql, 1D, 0D, 0D);
  }

  @Test public void testRowCountSort() {
    final String sql = "select * from emp order by ename";
    checkRowCount(sql, EMP_SIZE, 0D, Double.POSITIVE_INFINITY);
  }

  @Test public void testRowCountSortHighLimit() {
    final String sql = "select * from emp order by ename limit 123456";
    checkRowCount(sql, EMP_SIZE, 0D, 123456D);
  }

  @Test public void testRowCountSortHighOffset() {
    final String sql = "select * from emp order by ename offset 123456";
    checkRowCount(sql, 1D, 0D, Double.POSITIVE_INFINITY);
  }

  @Test public void testRowCountSortHighOffsetLimit() {
    final String sql = "select * from emp order by ename limit 5 offset 123456";
    checkRowCount(sql, 1D, 0D, 5D);
  }

  @Test public void testRowCountSortLimit() {
    final String sql = "select * from emp order by ename limit 10";
    checkRowCount(sql, 10d, 0D, 10d);
  }

  @Test public void testRowCountSortLimit0() {
    final String sql = "select * from emp order by ename limit 10";
    checkRowCount(sql, 10d, 0D, 10d);
  }

  @Test public void testRowCountSortLimitOffset() {
    final String sql = "select * from emp order by ename limit 10 offset 5";
    checkRowCount(sql, 9D /* 14 - 5 */, 0D, 10d);
  }

  @Test public void testRowCountSortLimitOffsetOnFinite() {
    final String sql = "select * from (select * from emp limit 12)\n"
        + "order by ename limit 20 offset 5";
    checkRowCount(sql, 7d, 0D, 7d);
  }

  @Test public void testRowCountAggregate() {
    final String sql = "select deptno from emp group by deptno";
    checkRowCount(sql, 1.4D, 0D, Double.POSITIVE_INFINITY);
  }

  @Test public void testRowCountAggregateGroupingSets() {
    final String sql = "select deptno from emp\n"
        + "group by grouping sets ((deptno), (ename, deptno))";
    checkRowCount(sql, 2.8D, // EMP_SIZE / 10 * 2
        0D, Double.POSITIVE_INFINITY);
  }

  @Test public void testRowCountAggregateGroupingSetsOneEmpty() {
    final String sql = "select deptno from emp\n"
        + "group by grouping sets ((deptno), ())";
    checkRowCount(sql, 2.8D, 0D, Double.POSITIVE_INFINITY);
  }

  @Test public void testRowCountAggregateEmptyKey() {
    final String sql = "select count(*) from emp";
    checkRowCount(sql, 1D, 1D, 1D);
  }

  @Test public void testRowCountFilterAggregateEmptyKey() {
    final String sql = "select count(*) from emp where 1 = 0";
    checkRowCount(sql, 1D, 1D, 1D);
  }

  @Test public void testRowCountAggregateEmptyKeyOnEmptyTable() {
    final String sql = "select count(*) from (select * from emp limit 0)";
    checkRowCount(sql, 1D, 1D, 1D);
  }

  private void checkFilterSelectivity(
      String sql,
      double expected) {
    RelNode rel = convertSql(sql);
    final RelMetadataQuery mq = RelMetadataQuery.instance();
    Double result = mq.getSelectivity(rel, null);
    assertTrue(result != null);
    assertEquals(expected, result, EPSILON);
  }

  @Test public void testSelectivityIsNotNullFilter() {
    checkFilterSelectivity(
        "select * from emp where mgr is not null",
        DEFAULT_NOTNULL_SELECTIVITY);
  }

  @Test public void testSelectivityIsNotNullFilterOnNotNullColumn() {
    checkFilterSelectivity(
        "select * from emp where deptno is not null",
        1.0d);
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

  @Test public void testSelectivityJoin() {
    checkFilterSelectivity(
        "select * from emp join dept using (deptno) where ename = 'foo'",
        DEFAULT_EQUAL_SELECTIVITY);
  }

  private void checkRelSelectivity(
      RelNode rel,
      double expected) {
    final RelMetadataQuery mq = RelMetadataQuery.instance();
    Double result = mq.getSelectivity(rel, null);
    assertTrue(result != null);
    assertEquals(expected, result, EPSILON);
  }

  @Test public void testSelectivityRedundantFilter() {
    RelNode rel = convertSql("select * from emp where deptno = 10");
    checkRelSelectivity(rel, DEFAULT_EQUAL_SELECTIVITY);
  }

  @Test public void testSelectivitySort() {
    RelNode rel =
        convertSql("select * from emp where deptno = 10"
            + "order by ename");
    checkRelSelectivity(rel, DEFAULT_EQUAL_SELECTIVITY);
  }

  @Test public void testSelectivityUnion() {
    RelNode rel =
        convertSql("select * from (\n"
            + "  select * from emp union all select * from emp) "
            + "where deptno = 10");
    checkRelSelectivity(rel, DEFAULT_EQUAL_SELECTIVITY);
  }

  @Test public void testSelectivityAgg() {
    RelNode rel =
        convertSql("select deptno, count(*) from emp where deptno > 10 "
            + "group by deptno having count(*) = 0");
    checkRelSelectivity(
        rel,
        DEFAULT_COMP_SELECTIVITY * DEFAULT_EQUAL_SELECTIVITY);
  }

  /** Checks that we can cache a metadata request that includes a null
   * argument. */
  @Test public void testSelectivityAggCached() {
    RelNode rel =
        convertSql("select deptno, count(*) from emp where deptno > 10 "
            + "group by deptno having count(*) = 0");
    rel.getCluster().setMetadataProvider(
        new CachingRelMetadataProvider(
            rel.getCluster().getMetadataProvider(),
            rel.getCluster().getPlanner()));
    final RelMetadataQuery mq = RelMetadataQuery.instance();
    Double result = mq.getSelectivity(rel, null);
    assertThat(result,
        within(DEFAULT_COMP_SELECTIVITY * DEFAULT_EQUAL_SELECTIVITY, EPSILON));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1808">[CALCITE-1808]
   * JaninoRelMetadataProvider loading cache might cause
   * OutOfMemoryError</a>. */
  @Test public void testMetadataHandlerCacheLimit() {
    Assume.assumeTrue("If cache size is too large, this test may fail and the "
            + "test won't be to blame",
        SaffronProperties.INSTANCE.metadataHandlerCacheMaximumSize().get()
            < 10_000);
    final int iterationCount = 2_000;
    final RelNode rel = convertSql("select * from emp");
    final RelMetadataProvider metadataProvider =
        rel.getCluster().getMetadataProvider();
    final RelOptPlanner planner = rel.getCluster().getPlanner();
    for (int i = 0; i < iterationCount; i++) {
      RelMetadataQuery.THREAD_PROVIDERS.set(
          JaninoRelMetadataProvider.of(
              new CachingRelMetadataProvider(metadataProvider, planner)));
      final RelMetadataQuery mq = RelMetadataQuery.instance();
      final Double result = mq.getRowCount(rel);
      assertThat(result, within(14d, 0.1d));
    }
  }

  @Test public void testDistinctRowCountTable() {
    // no unique key information is available so return null
    RelNode rel = convertSql("select * from emp where deptno = 10");
    final RelMetadataQuery mq = RelMetadataQuery.instance();
    ImmutableBitSet groupKey =
        ImmutableBitSet.of(rel.getRowType().getFieldNames().indexOf("DEPTNO"));
    Double result = mq.getDistinctRowCount(rel, groupKey, null);
    assertThat(result, nullValue());
  }

  @Test public void testDistinctRowCountTableEmptyKey() {
    RelNode rel = convertSql("select * from emp where deptno = 10");
    ImmutableBitSet groupKey = ImmutableBitSet.of(); // empty key
    final RelMetadataQuery mq = RelMetadataQuery.instance();
    Double result = mq.getDistinctRowCount(rel, groupKey, null);
    assertThat(result, is(1D));
  }

  /** Asserts that {@link RelMetadataQuery#getUniqueKeys(RelNode)}
   * and {@link RelMetadataQuery#areColumnsUnique(RelNode, ImmutableBitSet)}
   * return consistent results. */
  private void assertUniqueConsistent(RelNode rel) {
    final RelMetadataQuery mq = RelMetadataQuery.instance();
    final Set<ImmutableBitSet> uniqueKeys = mq.getUniqueKeys(rel);
    final ImmutableBitSet allCols =
        ImmutableBitSet.range(0, rel.getRowType().getFieldCount());
    for (ImmutableBitSet key : allCols.powerSet()) {
      Boolean result2 = mq.areColumnsUnique(rel, key);
      assertTrue(result2 == null || result2 == isUnique(uniqueKeys, key));
    }
  }

  /** Returns whether {@code keys} is unique, that is, whether it or a superset
   * is in {@code keySets}. */
  private boolean isUnique(Set<ImmutableBitSet> uniqueKeys, ImmutableBitSet key) {
    for (ImmutableBitSet uniqueKey : uniqueKeys) {
      if (key.contains(uniqueKey)) {
        return true;
      }
    }
    return false;
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-509">[CALCITE-509]
   * "RelMdColumnUniqueness uses ImmutableBitSet.Builder twice, gets
   * NullPointerException"</a>. */
  @Test public void testJoinUniqueKeys() {
    RelNode rel = convertSql("select * from emp join bonus using (ename)");
    final RelMetadataQuery mq = RelMetadataQuery.instance();
    Set<ImmutableBitSet> result = mq.getUniqueKeys(rel);
    assertThat(result.isEmpty(), is(true));
    assertUniqueConsistent(rel);
  }

  @Test public void testCorrelateUniqueKeys() {
    final String sql = "select *\n"
        + "from (select distinct deptno from emp) as e,\n"
        + "  lateral (\n"
        + "    select * from dept where dept.deptno = e.deptno)";
    final RelNode rel = convertSql(sql);
    final RelMetadataQuery mq = RelMetadataQuery.instance();

    assertThat(rel, isA((Class) Project.class));
    final Project project = (Project) rel;
    final Set<ImmutableBitSet> result = mq.getUniqueKeys(project);
    assertThat(result, sortsAs("[{0}]"));
    if (false) {
      assertUniqueConsistent(project);
    }

    assertThat(project.getInput(), isA((Class) Correlate.class));
    final Correlate correlate = (Correlate) project.getInput();
    final Set<ImmutableBitSet> result2 = mq.getUniqueKeys(correlate);
    assertThat(result2, sortsAs("[{0}]"));
    if (false) {
      assertUniqueConsistent(correlate);
    }
  }

  @Test public void testGroupByEmptyUniqueKeys() {
    RelNode rel = convertSql("select count(*) from emp");
    final RelMetadataQuery mq = RelMetadataQuery.instance();
    Set<ImmutableBitSet> result = mq.getUniqueKeys(rel);
    assertThat(result,
        CoreMatchers.<Set<ImmutableBitSet>>equalTo(
            ImmutableSet.of(ImmutableBitSet.of())));
    assertUniqueConsistent(rel);
  }

  @Test public void testGroupByEmptyHavingUniqueKeys() {
    RelNode rel = convertSql("select count(*) from emp where 1 = 1");
    final RelMetadataQuery mq = RelMetadataQuery.instance();
    final Set<ImmutableBitSet> result = mq.getUniqueKeys(rel);
    assertThat(result,
        CoreMatchers.<Set<ImmutableBitSet>>equalTo(
            ImmutableSet.of(ImmutableBitSet.of())));
    assertUniqueConsistent(rel);
  }

  @Test public void testGroupBy() {
    RelNode rel = convertSql("select deptno, count(*), sum(sal) from emp\n"
            + "group by deptno");
    final RelMetadataQuery mq = RelMetadataQuery.instance();
    final Set<ImmutableBitSet> result = mq.getUniqueKeys(rel);
    assertThat(result,
        CoreMatchers.<Set<ImmutableBitSet>>equalTo(
            ImmutableSet.of(ImmutableBitSet.of(0))));
    assertUniqueConsistent(rel);
  }

  @Test public void testUnion() {
    RelNode rel = convertSql("select deptno from emp\n"
            + "union\n"
            + "select deptno from dept");
    final RelMetadataQuery mq = RelMetadataQuery.instance();
    final Set<ImmutableBitSet> result = mq.getUniqueKeys(rel);
    assertThat(result,
        CoreMatchers.<Set<ImmutableBitSet>>equalTo(
            ImmutableSet.of(ImmutableBitSet.of(0))));
    assertUniqueConsistent(rel);
  }

  @Test public void testBrokenCustomProvider() {
    final List<String> buf = Lists.newArrayList();
    ColTypeImpl.THREAD_LIST.set(buf);

    final String sql = "select deptno, count(*) from emp where deptno > 10 "
        + "group by deptno having count(*) = 0";
    final RelRoot root = tester
        .withClusterFactory(
            new Function<RelOptCluster, RelOptCluster>() {
              public RelOptCluster apply(RelOptCluster cluster) {
                cluster.setMetadataProvider(
                    ChainedRelMetadataProvider.of(
                        ImmutableList.of(BrokenColTypeImpl.SOURCE,
                            cluster.getMetadataProvider())));
                return cluster;
              }
            })
        .convertSqlToRel(sql);

    final RelNode rel = root.rel;
    assertThat(rel, instanceOf(LogicalFilter.class));
    final MyRelMetadataQuery mq = new MyRelMetadataQuery();

    try {
      assertThat(colType(mq, rel, 0), equalTo("DEPTNO-rel"));
      fail("expected error");
    } catch (IllegalArgumentException e) {
      final String value = "No handler for method [public abstract java.lang.String "
          + "org.apache.calcite.test.RelMetadataTest$ColType.getColType(int)] "
          + "applied to argument of type [interface org.apache.calcite.rel.RelNode]; "
          + "we recommend you create a catch-all (RelNode) handler";
      assertThat(e.getMessage(), is(value));
    }
  }

  public String colType(RelMetadataQuery mq, RelNode rel, int column) {
    if (mq instanceof MyRelMetadataQuery) {
      return ((MyRelMetadataQuery) mq).colType(rel, column);
    } else {
      return rel.metadata(ColType.class, mq).getColType(column);
    }
  }

  @Test public void testCustomProvider() {
    final List<String> buf = Lists.newArrayList();
    ColTypeImpl.THREAD_LIST.set(buf);

    final String sql = "select deptno, count(*) from emp where deptno > 10 "
        + "group by deptno having count(*) = 0";
    final RelRoot root = tester
        .withClusterFactory(
            new Function<RelOptCluster, RelOptCluster>() {
              public RelOptCluster apply(RelOptCluster cluster) {
                // Create a custom provider that includes ColType.
                // Include the same provider twice just to be devious.
                final ImmutableList<RelMetadataProvider> list =
                    ImmutableList.of(ColTypeImpl.SOURCE, ColTypeImpl.SOURCE,
                        cluster.getMetadataProvider());
                cluster.setMetadataProvider(
                    ChainedRelMetadataProvider.of(list));
                return cluster;
              }
            })
        .convertSqlToRel(sql);
    final RelNode rel = root.rel;

    // Top node is a filter. Its metadata uses getColType(RelNode, int).
    assertThat(rel, instanceOf(LogicalFilter.class));
    final RelMetadataQuery mq = RelMetadataQuery.instance();
    assertThat(colType(mq, rel, 0), equalTo("DEPTNO-rel"));
    assertThat(colType(mq, rel, 1), equalTo("EXPR$1-rel"));

    // Next node is an aggregate. Its metadata uses
    // getColType(LogicalAggregate, int).
    final RelNode input = rel.getInput(0);
    assertThat(input, instanceOf(LogicalAggregate.class));
    assertThat(colType(mq, input, 0), equalTo("DEPTNO-agg"));

    // There is no caching. Another request causes another call to the provider.
    assertThat(buf.toString(), equalTo("[DEPTNO-rel, EXPR$1-rel, DEPTNO-agg]"));
    assertThat(buf.size(), equalTo(3));
    assertThat(colType(mq, input, 0), equalTo("DEPTNO-agg"));
    assertThat(buf.size(), equalTo(4));

    // Now add a cache. Only the first request for each piece of metadata
    // generates a new call to the provider.
    final RelOptPlanner planner = rel.getCluster().getPlanner();
    rel.getCluster().setMetadataProvider(
        new CachingRelMetadataProvider(
            rel.getCluster().getMetadataProvider(), planner));
    assertThat(colType(mq, input, 0), equalTo("DEPTNO-agg"));
    assertThat(buf.size(), equalTo(5));
    assertThat(colType(mq, input, 0), equalTo("DEPTNO-agg"));
    assertThat(buf.size(), equalTo(5));
    assertThat(colType(mq, input, 1), equalTo("EXPR$1-agg"));
    assertThat(buf.size(), equalTo(6));
    assertThat(colType(mq, input, 1), equalTo("EXPR$1-agg"));
    assertThat(buf.size(), equalTo(6));
    assertThat(colType(mq, input, 0), equalTo("DEPTNO-agg"));
    assertThat(buf.size(), equalTo(6));

    // With a different timestamp, a metadata item is re-computed on first call.
    long timestamp = planner.getRelMetadataTimestamp(rel);
    assertThat(timestamp, equalTo(0L));
    ((MockRelOptPlanner) planner).setRelMetadataTimestamp(timestamp + 1);
    assertThat(colType(mq, input, 0), equalTo("DEPTNO-agg"));
    assertThat(buf.size(), equalTo(7));
    assertThat(colType(mq, input, 0), equalTo("DEPTNO-agg"));
    assertThat(buf.size(), equalTo(7));
  }

  /** Unit test for
   * {@link org.apache.calcite.rel.metadata.RelMdCollation#project}
   * and other helper functions for deducing collations. */
  @Test public void testCollation() {
    final Project rel = (Project) convertSql("select * from emp, dept");
    final Join join = (Join) rel.getInput();
    final RelOptTable empTable = join.getInput(0).getTable();
    final RelOptTable deptTable = join.getInput(1).getTable();
    Frameworks.withPlanner(
        new Frameworks.PlannerAction<Void>() {
          public Void apply(RelOptCluster cluster,
              RelOptSchema relOptSchema,
              SchemaPlus rootSchema) {
            checkCollation(cluster, empTable, deptTable);
            return null;
          }
        });
  }

  private void checkCollation(RelOptCluster cluster, RelOptTable empTable,
      RelOptTable deptTable) {
    final RexBuilder rexBuilder = cluster.getRexBuilder();
    final LogicalTableScan empScan = LogicalTableScan.create(cluster, empTable);

    List<RelCollation> collations =
        RelMdCollation.table(empScan.getTable());
    assertThat(collations.size(), equalTo(0));

    // ORDER BY field#0 ASC, field#1 ASC
    final RelCollation collation =
        RelCollations.of(new RelFieldCollation(0), new RelFieldCollation(1));
    collations = RelMdCollation.sort(collation);
    assertThat(collations.size(), equalTo(1));
    assertThat(collations.get(0).getFieldCollations().size(), equalTo(2));

    final Sort empSort = LogicalSort.create(empScan, collation, null, null);

    final List<RexNode> projects =
        ImmutableList.of(rexBuilder.makeInputRef(empSort, 1),
            rexBuilder.makeLiteral("foo"),
            rexBuilder.makeInputRef(empSort, 0),
            rexBuilder.makeCall(SqlStdOperatorTable.MINUS,
                rexBuilder.makeInputRef(empSort, 0),
                rexBuilder.makeInputRef(empSort, 3)));

    final RelMetadataQuery mq = RelMetadataQuery.instance();
    collations = RelMdCollation.project(mq, empSort, projects);
    assertThat(collations.size(), equalTo(1));
    assertThat(collations.get(0).getFieldCollations().size(), equalTo(2));
    assertThat(collations.get(0).getFieldCollations().get(0).getFieldIndex(),
        equalTo(2));
    assertThat(collations.get(0).getFieldCollations().get(1).getFieldIndex(),
        equalTo(0));

    final LogicalProject project = LogicalProject.create(empSort, projects,
        ImmutableList.of("a", "b", "c", "d"));

    final LogicalTableScan deptScan =
        LogicalTableScan.create(cluster, deptTable);

    final RelCollation deptCollation =
        RelCollations.of(new RelFieldCollation(0), new RelFieldCollation(1));
    final Sort deptSort =
        LogicalSort.create(deptScan, deptCollation, null, null);

    final ImmutableIntList leftKeys = ImmutableIntList.of(2);
    final ImmutableIntList rightKeys = ImmutableIntList.of(0);
    final EnumerableMergeJoin join;
    try {
      join = EnumerableMergeJoin.create(project, deptSort,
          rexBuilder.makeLiteral(true), leftKeys, rightKeys, JoinRelType.INNER);
    } catch (InvalidRelException e) {
      throw new RuntimeException(e);
    }
    collations =
        RelMdCollation.mergeJoin(mq, project, deptSort, leftKeys,
            rightKeys);
    assertThat(collations,
        equalTo(join.getTraitSet().getTraits(RelCollationTraitDef.INSTANCE)));

    // Values (empty)
    collations = RelMdCollation.values(mq, empTable.getRowType(),
        ImmutableList.<ImmutableList<RexLiteral>>of());
    assertThat(collations.toString(),
        equalTo("[[0, 1, 2, 3, 4, 5, 6, 7, 8], "
            + "[1, 2, 3, 4, 5, 6, 7, 8], "
            + "[2, 3, 4, 5, 6, 7, 8], "
            + "[3, 4, 5, 6, 7, 8], "
            + "[4, 5, 6, 7, 8], "
            + "[5, 6, 7, 8], "
            + "[6, 7, 8], "
            + "[7, 8], "
            + "[8]]"));

    final LogicalValues emptyValues =
        LogicalValues.createEmpty(cluster, empTable.getRowType());
    assertThat(mq.collations(emptyValues), equalTo(collations));

    // Values (non-empty)
    final RelDataType rowType = cluster.getTypeFactory().builder()
        .add("a", SqlTypeName.INTEGER)
        .add("b", SqlTypeName.INTEGER)
        .add("c", SqlTypeName.INTEGER)
        .add("d", SqlTypeName.INTEGER)
        .build();
    final ImmutableList.Builder<ImmutableList<RexLiteral>> tuples =
        ImmutableList.builder();
    // sort keys are [a], [a, b], [a, b, c], [a, b, c, d], [a, c], [b], [b, a],
    //   [b, d]
    // algorithm deduces [a, b, c, d], [b, d] which is a useful sub-set
    addRow(tuples, rexBuilder, 1, 1, 1, 1);
    addRow(tuples, rexBuilder, 1, 2, 0, 3);
    addRow(tuples, rexBuilder, 2, 3, 2, 2);
    addRow(tuples, rexBuilder, 3, 3, 1, 4);
    collations = RelMdCollation.values(mq, rowType, tuples.build());
    assertThat(collations.toString(),
        equalTo("[[0, 1, 2, 3], [1, 3]]"));

    final LogicalValues values =
        LogicalValues.create(cluster, rowType, tuples.build());
    assertThat(mq.collations(values), equalTo(collations));
  }

  /** Unit test for
   * {@link org.apache.calcite.rel.metadata.RelMdColumnUniqueness#areColumnsUnique}
   * applied to {@link Values}. */
  @Test public void testColumnUniquenessForValues() {
    Frameworks.withPlanner(
        new Frameworks.PlannerAction<Void>() {
          public Void apply(RelOptCluster cluster, RelOptSchema relOptSchema,
              SchemaPlus rootSchema) {
            final RexBuilder rexBuilder = cluster.getRexBuilder();
            final RelMetadataQuery mq = RelMetadataQuery.instance();
            final RelDataType rowType = cluster.getTypeFactory().builder()
                .add("a", SqlTypeName.INTEGER)
                .add("b", SqlTypeName.VARCHAR)
                .build();
            final ImmutableList.Builder<ImmutableList<RexLiteral>> tuples =
                ImmutableList.builder();
            addRow(tuples, rexBuilder, 1, "X");
            addRow(tuples, rexBuilder, 2, "Y");
            addRow(tuples, rexBuilder, 3, "X");
            addRow(tuples, rexBuilder, 4, "X");

            final LogicalValues values =
                LogicalValues.create(cluster, rowType, tuples.build());

            final ImmutableBitSet colNone = ImmutableBitSet.of();
            final ImmutableBitSet col0 = ImmutableBitSet.of(0);
            final ImmutableBitSet col1 = ImmutableBitSet.of(1);
            final ImmutableBitSet colAll = ImmutableBitSet.of(0, 1);

            assertThat(mq.areColumnsUnique(values, col0), is(true));
            assertThat(mq.areColumnsUnique(values, col1), is(false));
            assertThat(mq.areColumnsUnique(values, colAll), is(true));
            assertThat(mq.areColumnsUnique(values, colNone), is(false));

            // Repeat the above tests directly against the handler.
            final RelMdColumnUniqueness handler =
                (RelMdColumnUniqueness) RelMdColumnUniqueness.SOURCE
                    .handlers(BuiltInMetadata.ColumnUniqueness.DEF)
                    .get(BuiltInMethod.COLUMN_UNIQUENESS.method)
                    .iterator().next();
            assertThat(handler.areColumnsUnique(values, mq, col0, false),
                is(true));
            assertThat(handler.areColumnsUnique(values, mq, col1, false),
                is(false));
            assertThat(handler.areColumnsUnique(values, mq, colAll, false),
                is(true));
            assertThat(handler.areColumnsUnique(values, mq, colNone, false),
                is(false));

            return null;
          }
        });
  }

  private void addRow(ImmutableList.Builder<ImmutableList<RexLiteral>> builder,
      RexBuilder rexBuilder, Object... values) {
    ImmutableList.Builder<RexLiteral> b = ImmutableList.builder();
    final RelDataType varcharType =
        rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR);
    for (Object value : values) {
      final RexLiteral literal;
      if (value == null) {
        literal = rexBuilder.makeNullLiteral(varcharType);
      } else if (value instanceof Integer) {
        literal = rexBuilder.makeExactLiteral(
            BigDecimal.valueOf((Integer) value));
      } else {
        literal = rexBuilder.makeLiteral((String) value);
      }
      b.add(literal);
    }
    builder.add(b.build());
  }

  /** Unit test for
   * {@link org.apache.calcite.rel.metadata.RelMetadataQuery#getAverageColumnSizes(org.apache.calcite.rel.RelNode)},
   * {@link org.apache.calcite.rel.metadata.RelMetadataQuery#getAverageRowSize(org.apache.calcite.rel.RelNode)}. */
  @Test public void testAverageRowSize() {
    final Project rel = (Project) convertSql("select * from emp, dept");
    final Join join = (Join) rel.getInput();
    final RelOptTable empTable = join.getInput(0).getTable();
    final RelOptTable deptTable = join.getInput(1).getTable();
    Frameworks.withPlanner(
        new Frameworks.PlannerAction<Void>() {
          public Void apply(RelOptCluster cluster,
              RelOptSchema relOptSchema,
              SchemaPlus rootSchema) {
            checkAverageRowSize(cluster, empTable, deptTable);
            return null;
          }
        });
  }

  private void checkAverageRowSize(RelOptCluster cluster, RelOptTable empTable,
      RelOptTable deptTable) {
    final RexBuilder rexBuilder = cluster.getRexBuilder();
    final RelMetadataQuery mq = RelMetadataQuery.instance();
    final LogicalTableScan empScan = LogicalTableScan.create(cluster, empTable);

    Double rowSize = mq.getAverageRowSize(empScan);
    List<Double> columnSizes = mq.getAverageColumnSizes(empScan);

    assertThat(columnSizes.size(),
        equalTo(empScan.getRowType().getFieldCount()));
    assertThat(columnSizes,
        equalTo(Arrays.asList(4.0, 40.0, 20.0, 4.0, 8.0, 4.0, 4.0, 4.0, 1.0)));
    assertThat(rowSize, equalTo(89.0));

    // Empty values
    final LogicalValues emptyValues =
        LogicalValues.createEmpty(cluster, empTable.getRowType());
    rowSize = mq.getAverageRowSize(emptyValues);
    columnSizes = mq.getAverageColumnSizes(emptyValues);
    assertThat(columnSizes.size(),
        equalTo(emptyValues.getRowType().getFieldCount()));
    assertThat(columnSizes,
        equalTo(Arrays.asList(4.0, 40.0, 20.0, 4.0, 8.0, 4.0, 4.0, 4.0, 1.0)));
    assertThat(rowSize, equalTo(89.0));

    // Values
    final RelDataType rowType = cluster.getTypeFactory().builder()
        .add("a", SqlTypeName.INTEGER)
        .add("b", SqlTypeName.VARCHAR)
        .add("c", SqlTypeName.VARCHAR)
        .build();
    final ImmutableList.Builder<ImmutableList<RexLiteral>> tuples =
        ImmutableList.builder();
    addRow(tuples, rexBuilder, 1, "1234567890", "ABC");
    addRow(tuples, rexBuilder, 2, "1",          "A");
    addRow(tuples, rexBuilder, 3, "2",          null);
    final LogicalValues values =
        LogicalValues.create(cluster, rowType, tuples.build());
    rowSize = mq.getAverageRowSize(values);
    columnSizes = mq.getAverageColumnSizes(values);
    assertThat(columnSizes.size(),
        equalTo(values.getRowType().getFieldCount()));
    assertThat(columnSizes, equalTo(Arrays.asList(4.0, 8.0, 3.0)));
    assertThat(rowSize, equalTo(15.0));

    // Union
    final LogicalUnion union =
        LogicalUnion.create(ImmutableList.<RelNode>of(empScan, emptyValues),
            true);
    rowSize = mq.getAverageRowSize(union);
    columnSizes = mq.getAverageColumnSizes(union);
    assertThat(columnSizes.size(), equalTo(9));
    assertThat(columnSizes,
        equalTo(Arrays.asList(4.0, 40.0, 20.0, 4.0, 8.0, 4.0, 4.0, 4.0, 1.0)));
    assertThat(rowSize, equalTo(89.0));

    // Filter
    final LogicalTableScan deptScan =
        LogicalTableScan.create(cluster, deptTable);
    final LogicalFilter filter =
        LogicalFilter.create(deptScan,
            rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN,
                rexBuilder.makeInputRef(deptScan, 0),
                rexBuilder.makeExactLiteral(BigDecimal.TEN)));
    rowSize = mq.getAverageRowSize(filter);
    columnSizes = mq.getAverageColumnSizes(filter);
    assertThat(columnSizes.size(), equalTo(2));
    assertThat(columnSizes, equalTo(Arrays.asList(4.0, 20.0)));
    assertThat(rowSize, equalTo(24.0));

    // Project
    final LogicalProject deptProject =
        LogicalProject.create(filter,
            ImmutableList.of(
                rexBuilder.makeInputRef(filter, 0),
                rexBuilder.makeInputRef(filter, 1),
                rexBuilder.makeCall(SqlStdOperatorTable.PLUS,
                    rexBuilder.makeInputRef(filter, 0),
                    rexBuilder.makeExactLiteral(BigDecimal.ONE)),
                rexBuilder.makeCall(SqlStdOperatorTable.CHAR_LENGTH,
                    rexBuilder.makeInputRef(filter, 1))),
            (List<String>) null);
    rowSize = mq.getAverageRowSize(deptProject);
    columnSizes = mq.getAverageColumnSizes(deptProject);
    assertThat(columnSizes.size(), equalTo(4));
    assertThat(columnSizes, equalTo(Arrays.asList(4.0, 20.0, 4.0, 4.0)));
    assertThat(rowSize, equalTo(32.0));

    // Join
    final LogicalJoin join =
        LogicalJoin.create(empScan, deptProject, rexBuilder.makeLiteral(true),
            ImmutableSet.<CorrelationId>of(), JoinRelType.INNER);
    rowSize = mq.getAverageRowSize(join);
    columnSizes = mq.getAverageColumnSizes(join);
    assertThat(columnSizes.size(), equalTo(13));
    assertThat(columnSizes,
        equalTo(
            Arrays.asList(4.0, 40.0, 20.0, 4.0, 8.0, 4.0, 4.0, 4.0, 1.0, 4.0,
                20.0, 4.0, 4.0)));
    assertThat(rowSize, equalTo(121.0));

    // Aggregate
    final LogicalAggregate aggregate =
        LogicalAggregate.create(join, ImmutableBitSet.of(2, 0),
            ImmutableList.<ImmutableBitSet>of(),
            ImmutableList.of(
                AggregateCall.create(SqlStdOperatorTable.COUNT,
                    false, false, ImmutableIntList.of(),
                    -1, 2, join, null, null)));
    rowSize = mq.getAverageRowSize(aggregate);
    columnSizes = mq.getAverageColumnSizes(aggregate);
    assertThat(columnSizes.size(), equalTo(3));
    assertThat(columnSizes, equalTo(Arrays.asList(4.0, 20.0, 8.0)));
    assertThat(rowSize, equalTo(32.0));

    // Smoke test Parallelism and Memory metadata providers
    assertThat(mq.memory(aggregate), nullValue());
    assertThat(mq.cumulativeMemoryWithinPhase(aggregate),
        nullValue());
    assertThat(mq.cumulativeMemoryWithinPhaseSplit(aggregate),
        nullValue());
    assertThat(mq.isPhaseTransition(aggregate), is(false));
    assertThat(mq.splitCount(aggregate), is(1));
  }

  /** Unit test for
   * {@link org.apache.calcite.rel.metadata.RelMdPredicates#getPredicates(Join, RelMetadataQuery)}. */
  @Test public void testPredicates() {
    final Project rel = (Project) convertSql("select * from emp, dept");
    final Join join = (Join) rel.getInput();
    final RelOptTable empTable = join.getInput(0).getTable();
    final RelOptTable deptTable = join.getInput(1).getTable();
    Frameworks.withPlanner(
        new Frameworks.PlannerAction<Void>() {
          public Void apply(RelOptCluster cluster,
              RelOptSchema relOptSchema,
              SchemaPlus rootSchema) {
            checkPredicates(cluster, empTable, deptTable);
            return null;
          }
        });
  }

  private void checkPredicates(RelOptCluster cluster, RelOptTable empTable,
      RelOptTable deptTable) {
    final RelBuilder relBuilder = RelBuilder.proto().create(cluster, null);
    final RelMetadataQuery mq = RelMetadataQuery.instance();

    final LogicalTableScan empScan = LogicalTableScan.create(cluster, empTable);
    relBuilder.push(empScan);

    RelOptPredicateList predicates =
        mq.getPulledUpPredicates(empScan);
    assertThat(predicates.pulledUpPredicates.isEmpty(), is(true));

    relBuilder.filter(
        relBuilder.equals(relBuilder.field("EMPNO"),
            relBuilder.literal(BigDecimal.ONE)));

    final RelNode filter = relBuilder.peek();
    predicates = mq.getPulledUpPredicates(filter);
    assertThat(predicates.pulledUpPredicates.toString(), is("[=($0, 1)]"));

    final LogicalTableScan deptScan =
        LogicalTableScan.create(cluster, deptTable);
    relBuilder.push(deptScan);

    relBuilder.semiJoin(
        relBuilder.equals(relBuilder.field(2, 0, "DEPTNO"),
            relBuilder.field(2, 1, "DEPTNO")));
    final SemiJoin semiJoin = (SemiJoin) relBuilder.build();

    predicates = mq.getPulledUpPredicates(semiJoin);
    assertThat(predicates.pulledUpPredicates, sortsAs("[=($0, 1)]"));
    assertThat(predicates.leftInferredPredicates, sortsAs("[]"));
    assertThat(predicates.rightInferredPredicates.isEmpty(), is(true));

    // Create a Join similar to the previous SemiJoin
    relBuilder.push(filter);
    relBuilder.push(deptScan);
    relBuilder.join(JoinRelType.INNER,
        relBuilder.equals(relBuilder.field(2, 0, "DEPTNO"),
            relBuilder.field(2, 1, "DEPTNO")));

    relBuilder.project(relBuilder.field("DEPTNO"));
    final RelNode project = relBuilder.peek();
    predicates = mq.getPulledUpPredicates(project);
    // No inferred predicates, because we already know DEPTNO is NOT NULL
    assertThat(predicates.pulledUpPredicates, sortsAs("[]"));
    assertThat(project.getRowType().getFullTypeString(),
        is("RecordType(INTEGER NOT NULL DEPTNO) NOT NULL"));
    assertThat(predicates.leftInferredPredicates.isEmpty(), is(true));
    assertThat(predicates.rightInferredPredicates.isEmpty(), is(true));

    // Create a Join similar to the previous Join, but joining on MGR, which
    // is nullable. From the join condition "e.MGR = d.DEPTNO" we can deduce
    // the projected predicate "IS NOT NULL($0)".
    relBuilder.push(filter);
    relBuilder.push(deptScan);
    relBuilder.join(JoinRelType.INNER,
        relBuilder.equals(relBuilder.field(2, 0, "MGR"),
            relBuilder.field(2, 1, "DEPTNO")));

    relBuilder.project(relBuilder.field("MGR"));
    final RelNode project2 = relBuilder.peek();
    predicates = mq.getPulledUpPredicates(project2);
    assertThat(predicates.pulledUpPredicates, sortsAs("[IS NOT NULL($0)]"));
    assertThat(predicates.leftInferredPredicates.isEmpty(), is(true));
    assertThat(predicates.rightInferredPredicates.isEmpty(), is(true));

    // Create another similar Join. From the join condition
    //   e.MGR - e.EMPNO = d.DEPTNO + e.MGR_COMM
    // we can deduce the projected predicate
    //   MGR IS NOT NULL OR MGR_COMM IS NOT NULL
    //
    // EMPNO is omitted because it is NOT NULL.
    // MGR_COMM is a made-up nullable field.
    relBuilder.push(filter);
    relBuilder.project(
        Iterables.concat(relBuilder.fields(),
            ImmutableList.of(
                relBuilder.alias(
                    relBuilder.call(SqlStdOperatorTable.PLUS,
                        relBuilder.field("MGR"),
                        relBuilder.field("COMM")),
                    "MGR_COMM"))));
    relBuilder.push(deptScan);
    relBuilder.join(JoinRelType.INNER,
        relBuilder.equals(
            relBuilder.call(SqlStdOperatorTable.MINUS,
                relBuilder.field(2, 0, "MGR"),
                relBuilder.field(2, 0, "EMPNO")),
            relBuilder.call(SqlStdOperatorTable.PLUS,
                relBuilder.field(2, 1, "DEPTNO"),
                relBuilder.field(2, 0, "MGR_COMM"))));

    relBuilder.project(relBuilder.field("MGR"), relBuilder.field("NAME"),
        relBuilder.field("MGR_COMM"), relBuilder.field("COMM"));
    final RelNode project3 = relBuilder.peek();
    predicates = mq.getPulledUpPredicates(project3);
    assertThat(predicates.pulledUpPredicates,
        sortsAs("[OR(IS NOT NULL($0), IS NOT NULL($2))]"));
    assertThat(predicates.leftInferredPredicates.isEmpty(), is(true));
    assertThat(predicates.rightInferredPredicates.isEmpty(), is(true));
  }

  /**
   * Unit test for
   * {@link org.apache.calcite.rel.metadata.RelMdPredicates#getPredicates(Aggregate, RelMetadataQuery)}.
   */
  @Test public void testPullUpPredicatesFromAggregation() {
    final String sql = "select a, max(b) from (\n"
        + "  select 1 as a, 2 as b from emp)subq\n"
        + "group by a";
    final Aggregate rel = (Aggregate) convertSql(sql);
    final RelMetadataQuery mq = RelMetadataQuery.instance();
    RelOptPredicateList inputSet = mq.getPulledUpPredicates(rel);
    ImmutableList<RexNode> pulledUpPredicates = inputSet.pulledUpPredicates;
    assertThat(pulledUpPredicates, sortsAs("[=($0, 1)]"));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1960">[CALCITE-1960]
   * RelMdPredicates.getPredicates is slow if there are many equivalent
   * columns</a>. There are much less duplicates after
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2205">[CALCITE-2205]</a>.
   * Since this is a performance problem, the test result does not
   * change, but takes over 15 minutes before the fix and 6 seconds after. */
  @Test(timeout = 20_000) public void testPullUpPredicatesForExprsItr() {
    // If we're running Windows, we are probably in a VM and the test may
    // exceed timeout by a small margin.
    Assume.assumeThat("Too slow to run on Windows",
        File.separatorChar, Is.is('/'));
    final String sql = "select a.EMPNO, a.ENAME\n"
        + "from (select * from sales.emp ) a\n"
        + "join (select * from sales.emp  ) b\n"
        + "on a.empno = b.deptno\n"
        + "  and a.comm = b.comm\n"
        + "  and a.mgr=b.mgr\n"
        + "  and (a.empno < 10 or a.comm < 3 or a.deptno < 10\n"
        + "    or a.job ='abc' or a.ename='abc' or a.sal='30' or a.mgr >3\n"
        + "    or a.slacker is not null  or a.HIREDATE is not null\n"
        + "    or b.empno < 9 or b.comm < 3 or b.deptno < 10 or b.job ='abc'\n"
        + "    or b.ename='abc' or b.sal='30' or b.mgr >3 or b.slacker )\n"
        + "join emp c\n"
        + "on b.mgr =a.mgr and a.empno =b.deptno and a.comm=b.comm\n"
        + "  and a.deptno=b.deptno and a.job=b.job and a.ename=b.ename\n"
        + "  and a.mgr=b.deptno and a.slacker=b.slacker";
    // Lock to ensure that only one test is using this method at a time.
    try (final JdbcAdapterTest.LockWrapper ignore =
             JdbcAdapterTest.LockWrapper.lock(LOCK)) {
      final RelNode rel = convertSql(sql);
      final RelMetadataQuery mq = RelMetadataQuery.instance();
      RelOptPredicateList inputSet = mq.getPulledUpPredicates(rel.getInput(0));
      assertThat(inputSet.pulledUpPredicates.size(), is(18));
    }
  }

  @Test public void testPullUpPredicatesOnConstant() {
    final String sql = "select deptno, mgr, x, 'y' as y, z from (\n"
        + "  select deptno, mgr, cast(null as integer) as x, cast('1' as int) as z\n"
        + "  from emp\n"
        + "  where mgr is null and deptno < 10)";
    final RelNode rel = convertSql(sql);
    final RelMetadataQuery mq = RelMetadataQuery.instance();
    RelOptPredicateList list = mq.getPulledUpPredicates(rel);
    assertThat(list.pulledUpPredicates,
        sortsAs("[<($0, 10), =($3, 'y'), =($4, 1), IS NULL($1), IS NULL($2)]"));
  }

  @Test public void testPullUpPredicatesOnNullableConstant() {
    final String sql = "select nullif(1, 1) as c\n"
        + "  from emp\n"
        + "  where mgr is null and deptno < 10";
    final RelNode rel = convertSql(sql);
    final RelMetadataQuery mq = RelMetadataQuery.instance();
    RelOptPredicateList list = mq.getPulledUpPredicates(rel);
    // Uses "IS NOT DISTINCT FROM" rather than "=" because cannot guarantee not null.
    assertThat(list.pulledUpPredicates,
        sortsAs("[IS NOT DISTINCT FROM($0, CASE(=(1, 1), null, 1))]"));
  }

  @Test public void testDistributionSimple() {
    RelNode rel = convertSql("select * from emp where deptno = 10");
    final RelMetadataQuery mq = RelMetadataQuery.instance();
    RelDistribution d = mq.getDistribution(rel);
    assertThat(d, is(RelDistributions.BROADCAST_DISTRIBUTED));
  }

  @Test public void testDistributionHash() {
    final RelNode rel = convertSql("select * from emp");
    final RelDistribution dist = RelDistributions.hash(ImmutableList.of(1));
    final LogicalExchange exchange = LogicalExchange.create(rel, dist);

    final RelMetadataQuery mq = RelMetadataQuery.instance();
    RelDistribution d = mq.getDistribution(exchange);
    assertThat(d, is(dist));
  }

  @Test public void testDistributionHashEmpty() {
    final RelNode rel = convertSql("select * from emp");
    final RelDistribution dist = RelDistributions.hash(ImmutableList.<Integer>of());
    final LogicalExchange exchange = LogicalExchange.create(rel, dist);

    final RelMetadataQuery mq = RelMetadataQuery.instance();
    RelDistribution d = mq.getDistribution(exchange);
    assertThat(d, is(dist));
  }

  @Test public void testDistributionSingleton() {
    final RelNode rel = convertSql("select * from emp");
    final RelDistribution dist = RelDistributions.SINGLETON;
    final LogicalExchange exchange = LogicalExchange.create(rel, dist);

    final RelMetadataQuery mq = RelMetadataQuery.instance();
    RelDistribution d = mq.getDistribution(exchange);
    assertThat(d, is(dist));
  }

  /** Unit test for {@link RelMdUtil#linear(int, int, int, double, double)}. */
  @Test public void testLinear() {
    assertThat(RelMdUtil.linear(0, 0, 10, 100, 200), is(100d));
    assertThat(RelMdUtil.linear(5, 0, 10, 100, 200), is(150d));
    assertThat(RelMdUtil.linear(6, 0, 10, 100, 200), is(160d));
    assertThat(RelMdUtil.linear(10, 0, 10, 100, 200), is(200d));
    assertThat(RelMdUtil.linear(-2, 0, 10, 100, 200), is(100d));
    assertThat(RelMdUtil.linear(12, 0, 10, 100, 200), is(200d));
  }

  @Test public void testExpressionLineageStar() {
    // All columns in output
    final RelNode tableRel = convertSql("select * from emp");
    final RelMetadataQuery mq = RelMetadataQuery.instance();

    final RexNode ref = RexInputRef.of(4, tableRel.getRowType().getFieldList());
    final Set<RexNode> r = mq.getExpressionLineage(tableRel, ref);
    final String inputRef = RexInputRef.of(4, tableRel.getRowType().getFieldList()).toString();
    assertThat(r.size(), is(1));
    final String resultString = r.iterator().next().toString();
    assertThat(resultString, startsWith(EMP_QNAME.toString()));
    assertThat(resultString, endsWith(inputRef));
  }

  @Test public void testExpressionLineageTwoColumns() {
    // mgr is column 3 in catalog.sales.emp
    // deptno is column 7 in catalog.sales.emp
    final RelNode rel = convertSql("select mgr, deptno from emp");
    final RelMetadataQuery mq = RelMetadataQuery.instance();

    final RexNode ref1 = RexInputRef.of(0, rel.getRowType().getFieldList());
    final Set<RexNode> r1 = mq.getExpressionLineage(rel, ref1);
    assertThat(r1.size(), is(1));
    final RexTableInputRef result1 = (RexTableInputRef) r1.iterator().next();
    assertTrue(result1.getQualifiedName().equals(EMP_QNAME));
    assertThat(result1.getIndex(), is(3));

    final RexNode ref2 = RexInputRef.of(1, rel.getRowType().getFieldList());
    final Set<RexNode> r2 = mq.getExpressionLineage(rel, ref2);
    assertThat(r2.size(), is(1));
    final RexTableInputRef result2 = (RexTableInputRef) r2.iterator().next();
    assertTrue(result2.getQualifiedName().equals(EMP_QNAME));
    assertThat(result2.getIndex(), is(7));

    assertThat(result1.getIdentifier(), is(result2.getIdentifier()));
  }

  @Test public void testExpressionLineageTwoColumnsSwapped() {
    // deptno is column 7 in catalog.sales.emp
    // mgr is column 3 in catalog.sales.emp
    final RelNode rel = convertSql("select deptno, mgr from emp");
    final RelMetadataQuery mq = RelMetadataQuery.instance();

    final RexNode ref1 = RexInputRef.of(0, rel.getRowType().getFieldList());
    final Set<RexNode> r1 = mq.getExpressionLineage(rel, ref1);
    assertThat(r1.size(), is(1));
    final RexTableInputRef result1 = (RexTableInputRef) r1.iterator().next();
    assertTrue(result1.getQualifiedName().equals(EMP_QNAME));
    assertThat(result1.getIndex(), is(7));

    final RexNode ref2 = RexInputRef.of(1, rel.getRowType().getFieldList());
    final Set<RexNode> r2 = mq.getExpressionLineage(rel, ref2);
    assertThat(r2.size(), is(1));
    final RexTableInputRef result2 = (RexTableInputRef) r2.iterator().next();
    assertTrue(result2.getQualifiedName().equals(EMP_QNAME));
    assertThat(result2.getIndex(), is(3));

    assertThat(result1.getIdentifier(), is(result2.getIdentifier()));
  }

  @Test public void testExpressionLineageCombineTwoColumns() {
    // empno is column 0 in catalog.sales.emp
    // deptno is column 7 in catalog.sales.emp
    final RelNode rel = convertSql("select empno + deptno from emp");
    final RelMetadataQuery mq = RelMetadataQuery.instance();

    final RexNode ref = RexInputRef.of(0, rel.getRowType().getFieldList());
    final Set<RexNode> r = mq.getExpressionLineage(rel, ref);

    assertThat(r.size(), is(1));
    final RexNode result = r.iterator().next();
    assertThat(result.getKind(), is(SqlKind.PLUS));
    final RexCall call = (RexCall) result;
    assertThat(call.getOperands().size(), is(2));
    final RexTableInputRef inputRef1 = (RexTableInputRef) call.getOperands().get(0);
    assertTrue(inputRef1.getQualifiedName().equals(EMP_QNAME));
    assertThat(inputRef1.getIndex(), is(0));
    final RexTableInputRef inputRef2 = (RexTableInputRef) call.getOperands().get(1);
    assertTrue(inputRef2.getQualifiedName().equals(EMP_QNAME));
    assertThat(inputRef2.getIndex(), is(7));
    assertThat(inputRef1.getIdentifier(), is(inputRef2.getIdentifier()));
  }

  @Test public void testExpressionLineageInnerJoinLeft() {
    // ename is column 1 in catalog.sales.emp
    final RelNode rel = convertSql("select ename from emp,dept");
    final RelMetadataQuery mq = RelMetadataQuery.instance();

    final RexNode ref = RexInputRef.of(0, rel.getRowType().getFieldList());
    final Set<RexNode> r = mq.getExpressionLineage(rel, ref);
    assertThat(r.size(), is(1));
    final RexTableInputRef result = (RexTableInputRef) r.iterator().next();
    assertTrue(result.getQualifiedName().equals(EMP_QNAME));
    assertThat(result.getIndex(), is(1));
  }

  @Test public void testExpressionLineageInnerJoinRight() {
    // ename is column 0 in catalog.sales.bonus
    final RelNode rel = convertSql("select bonus.ename from emp join bonus using (ename)");
    final RelMetadataQuery mq = RelMetadataQuery.instance();

    final RexNode ref = RexInputRef.of(0, rel.getRowType().getFieldList());
    final Set<RexNode> r = mq.getExpressionLineage(rel, ref);
    assertThat(r.size(), is(1));
    final RexTableInputRef result = (RexTableInputRef) r.iterator().next();
    assertTrue(result.getQualifiedName().equals(ImmutableList.of("CATALOG", "SALES", "BONUS")));
    assertThat(result.getIndex(), is(0));
  }

  @Test public void testExpressionLineageSelfJoin() {
    // deptno is column 7 in catalog.sales.emp
    // sal is column 5 in catalog.sales.emp
    final RelNode rel = convertSql("select a.deptno, b.sal from (select * from emp limit 7) as a\n"
        + "inner join (select * from emp limit 2) as b\n"
        + "on a.deptno = b.deptno");
    final RelNode tableRel = convertSql("select * from emp");
    final RelMetadataQuery mq = RelMetadataQuery.instance();

    final RexNode ref1 = RexInputRef.of(0, rel.getRowType().getFieldList());
    final Set<RexNode> r1 = mq.getExpressionLineage(rel, ref1);
    final String inputRef1 = RexInputRef.of(7, tableRel.getRowType().getFieldList()).toString();
    assertThat(r1.size(), is(1));
    final String resultString1 = r1.iterator().next().toString();
    assertThat(resultString1, startsWith(EMP_QNAME.toString()));
    assertThat(resultString1, endsWith(inputRef1));

    final RexNode ref2 = RexInputRef.of(1, rel.getRowType().getFieldList());
    final Set<RexNode> r2 = mq.getExpressionLineage(rel, ref2);
    final String inputRef2 = RexInputRef.of(5, tableRel.getRowType().getFieldList()).toString();
    assertThat(r2.size(), is(1));
    final String resultString2 = r2.iterator().next().toString();
    assertThat(resultString2, startsWith(EMP_QNAME.toString()));
    assertThat(resultString2, endsWith(inputRef2));

    assertThat(((RexTableInputRef) r1.iterator().next()).getIdentifier(),
        not(((RexTableInputRef) r2.iterator().next()).getIdentifier()));
  }

  @Test public void testExpressionLineageOuterJoin() {
    // lineage cannot be determined
    final RelNode rel = convertSql("select name as dname from emp left outer join dept"
        + " on emp.deptno = dept.deptno");
    final RelMetadataQuery mq = RelMetadataQuery.instance();

    final RexNode ref = RexInputRef.of(0, rel.getRowType().getFieldList());
    final Set<RexNode> r = mq.getExpressionLineage(rel, ref);
    assertNull(r);
  }

  @Test public void testExpressionLineageFilter() {
    // ename is column 1 in catalog.sales.emp
    final RelNode rel = convertSql("select ename from emp where deptno = 10");
    final RelNode tableRel = convertSql("select * from emp");
    final RelMetadataQuery mq = RelMetadataQuery.instance();

    final RexNode ref = RexInputRef.of(0, rel.getRowType().getFieldList());
    final Set<RexNode> r = mq.getExpressionLineage(rel, ref);
    final String inputRef = RexInputRef.of(1, tableRel.getRowType().getFieldList()).toString();
    assertThat(r.size(), is(1));
    final String resultString = r.iterator().next().toString();
    assertThat(resultString, startsWith(EMP_QNAME.toString()));
    assertThat(resultString, endsWith(inputRef));
  }

  @Test public void testExpressionLineageAggregateGroupColumn() {
    // deptno is column 7 in catalog.sales.emp
    final RelNode rel = convertSql("select deptno, count(*) from emp where deptno > 10 "
        + "group by deptno having count(*) = 0");
    final RelNode tableRel = convertSql("select * from emp");
    final RelMetadataQuery mq = RelMetadataQuery.instance();

    final RexNode ref = RexInputRef.of(0, rel.getRowType().getFieldList());
    final Set<RexNode> r = mq.getExpressionLineage(rel, ref);
    final String inputRef = RexInputRef.of(7, tableRel.getRowType().getFieldList()).toString();
    assertThat(r.size(), is(1));
    final String resultString = r.iterator().next().toString();
    assertThat(resultString, startsWith(EMP_QNAME.toString()));
    assertThat(resultString, endsWith(inputRef));
  }

  @Test public void testExpressionLineageAggregateAggColumn() {
    // lineage cannot be determined
    final RelNode rel = convertSql("select deptno, count(*) from emp where deptno > 10 "
        + "group by deptno having count(*) = 0");
    final RelMetadataQuery mq = RelMetadataQuery.instance();

    final RexNode ref = RexInputRef.of(1, rel.getRowType().getFieldList());
    final Set<RexNode> r = mq.getExpressionLineage(rel, ref);
    assertNull(r);
  }

  @Test public void testExpressionLineageUnion() {
    // sal is column 5 in catalog.sales.emp
    final RelNode rel = convertSql("select sal from (\n"
        + "  select * from emp union all select * from emp) "
        + "where deptno = 10");
    final RelNode tableRel = convertSql("select * from emp");
    final RelMetadataQuery mq = RelMetadataQuery.instance();

    final RexNode ref = RexInputRef.of(0, rel.getRowType().getFieldList());
    final Set<RexNode> r = mq.getExpressionLineage(rel, ref);
    final String inputRef = RexInputRef.of(5, tableRel.getRowType().getFieldList()).toString();
    assertThat(r.size(), is(2));
    for (RexNode result : r) {
      final String resultString = result.toString();
      assertThat(resultString, startsWith(EMP_QNAME.toString()));
      assertThat(resultString, endsWith(inputRef));
    }

    Iterator<RexNode> it = r.iterator();
    assertThat(((RexTableInputRef) it.next()).getIdentifier(),
        not(((RexTableInputRef) it.next()).getIdentifier()));
  }

  @Test public void testExpressionLineageMultiUnion() {
    // empno is column 0 in catalog.sales.emp
    // sal is column 5 in catalog.sales.emp
    final RelNode rel = convertSql("select a.empno + b.sal from \n"
        + " (select empno, ename from emp,dept) a join "
        + " (select * from emp union all select * from emp) b \n"
        + " on a.empno = b.empno \n"
        + " where b.deptno = 10");
    final RelMetadataQuery mq = RelMetadataQuery.instance();

    final RexNode ref = RexInputRef.of(0, rel.getRowType().getFieldList());
    final Set<RexNode> r = mq.getExpressionLineage(rel, ref);

    // With the union, we should get two origins
    // The first one should be the same one: join
    // The second should come from each union input
    final Set<List<String>> set = new HashSet<>();
    assertThat(r.size(), is(2));
    for (RexNode result : r) {
      assertThat(result.getKind(), is(SqlKind.PLUS));
      final RexCall call = (RexCall) result;
      assertThat(call.getOperands().size(), is(2));
      final RexTableInputRef inputRef1 = (RexTableInputRef) call.getOperands().get(0);
      assertTrue(inputRef1.getQualifiedName().equals(EMP_QNAME));
      // Add join alpha to set
      set.add(inputRef1.getQualifiedName());
      assertThat(inputRef1.getIndex(), is(0));
      final RexTableInputRef inputRef2 = (RexTableInputRef) call.getOperands().get(1);
      assertTrue(inputRef2.getQualifiedName().equals(EMP_QNAME));
      assertThat(inputRef2.getIndex(), is(5));
      assertThat(inputRef1.getIdentifier(), not(inputRef2.getIdentifier()));
    }
    assertThat(set.size(), is(1));
  }

  @Test public void testExpressionLineageValues() {
    // lineage cannot be determined
    final RelNode rel = convertSql("select * from (values (1), (2)) as t(c)");
    final RelMetadataQuery mq = RelMetadataQuery.instance();

    final RexNode ref = RexInputRef.of(0, rel.getRowType().getFieldList());
    final Set<RexNode> r = mq.getExpressionLineage(rel, ref);
    assertNull(r);
  }

  @Test public void testAllPredicates() {
    final Project rel = (Project) convertSql("select * from emp, dept");
    final Join join = (Join) rel.getInput();
    final RelOptTable empTable = join.getInput(0).getTable();
    final RelOptTable deptTable = join.getInput(1).getTable();
    Frameworks.withPlanner(
        new Frameworks.PlannerAction<Void>() {
          public Void apply(RelOptCluster cluster,
              RelOptSchema relOptSchema,
              SchemaPlus rootSchema) {
            checkAllPredicates(cluster, empTable, deptTable);
            return null;
          }
        });
  }

  private void checkAllPredicates(RelOptCluster cluster, RelOptTable empTable,
      RelOptTable deptTable) {
    final RelBuilder relBuilder = RelBuilder.proto().create(cluster, null);
    final RelMetadataQuery mq = RelMetadataQuery.instance();

    final LogicalTableScan empScan = LogicalTableScan.create(cluster, empTable);
    relBuilder.push(empScan);

    RelOptPredicateList predicates =
        mq.getAllPredicates(empScan);
    assertThat(predicates.pulledUpPredicates.isEmpty(), is(true));

    relBuilder.filter(
        relBuilder.equals(relBuilder.field("EMPNO"),
            relBuilder.literal(BigDecimal.ONE)));

    final RelNode filter = relBuilder.peek();
    predicates = mq.getAllPredicates(filter);
    assertThat(predicates.pulledUpPredicates.size(), is(1));
    RexCall call = (RexCall) predicates.pulledUpPredicates.get(0);
    assertThat(call.getOperands().size(), is(2));
    RexTableInputRef inputRef1 = (RexTableInputRef) call.getOperands().get(0);
    assertTrue(inputRef1.getQualifiedName().equals(EMP_QNAME));
    assertThat(inputRef1.getIndex(), is(0));

    final LogicalTableScan deptScan =
        LogicalTableScan.create(cluster, deptTable);
    relBuilder.push(deptScan);

    relBuilder.join(JoinRelType.INNER,
        relBuilder.equals(relBuilder.field(2, 0, "DEPTNO"),
            relBuilder.field(2, 1, "DEPTNO")));

    relBuilder.project(relBuilder.field("DEPTNO"));
    final RelNode project = relBuilder.peek();
    predicates = mq.getAllPredicates(project);
    assertThat(predicates.pulledUpPredicates.size(), is(2));
    // From Filter
    call = (RexCall) predicates.pulledUpPredicates.get(0);
    assertThat(call.getOperands().size(), is(2));
    inputRef1 = (RexTableInputRef) call.getOperands().get(0);
    assertTrue(inputRef1.getQualifiedName().equals(EMP_QNAME));
    assertThat(inputRef1.getIndex(), is(0));
    // From Join
    call = (RexCall) predicates.pulledUpPredicates.get(1);
    assertThat(call.getOperands().size(), is(2));
    inputRef1 = (RexTableInputRef) call.getOperands().get(0);
    assertTrue(inputRef1.getQualifiedName().equals(EMP_QNAME));
    assertThat(inputRef1.getIndex(), is(7));
    RexTableInputRef inputRef2 = (RexTableInputRef) call.getOperands().get(1);
    assertTrue(inputRef2.getQualifiedName().equals(ImmutableList.of("CATALOG", "SALES", "DEPT")));
    assertThat(inputRef2.getIndex(), is(0));
  }

  @Test public void testAllPredicatesAggregate1() {
    final String sql = "select a, max(b) from (\n"
        + "  select empno as a, sal as b from emp where empno = 5)subq\n"
        + "group by a";
    final RelNode rel = convertSql(sql);
    final RelMetadataQuery mq = RelMetadataQuery.instance();
    RelOptPredicateList inputSet = mq.getAllPredicates(rel);
    ImmutableList<RexNode> pulledUpPredicates = inputSet.pulledUpPredicates;
    assertThat(pulledUpPredicates.size(), is(1));
    RexCall call = (RexCall) pulledUpPredicates.get(0);
    assertThat(call.getOperands().size(), is(2));
    final RexTableInputRef inputRef1 = (RexTableInputRef) call.getOperands().get(0);
    assertTrue(inputRef1.getQualifiedName().equals(EMP_QNAME));
    assertThat(inputRef1.getIndex(), is(0));
    final RexLiteral constant = (RexLiteral) call.getOperands().get(1);
    assertThat(constant.toString(), is("5"));
  }

  @Test public void testAllPredicatesAggregate2() {
    final String sql = "select * from (select a, max(b) from (\n"
        + "  select empno as a, sal as b from emp)subq\n"
        + "group by a) \n"
        + "where a = 5";
    final RelNode rel = convertSql(sql);
    final RelMetadataQuery mq = RelMetadataQuery.instance();
    RelOptPredicateList inputSet = mq.getAllPredicates(rel);
    ImmutableList<RexNode> pulledUpPredicates = inputSet.pulledUpPredicates;
    assertThat(pulledUpPredicates.size(), is(1));
    RexCall call = (RexCall) pulledUpPredicates.get(0);
    assertThat(call.getOperands().size(), is(2));
    final RexTableInputRef inputRef1 = (RexTableInputRef) call.getOperands().get(0);
    assertTrue(inputRef1.getQualifiedName().equals(EMP_QNAME));
    assertThat(inputRef1.getIndex(), is(0));
    final RexLiteral constant = (RexLiteral) call.getOperands().get(1);
    assertThat(constant.toString(), is("5"));
  }

  @Test public void testAllPredicatesAggregate3() {
    final String sql = "select * from (select a, max(b) as b from (\n"
        + "  select empno as a, sal as b from emp)subq\n"
        + "group by a) \n"
        + "where b = 5";
    final RelNode rel = convertSql(sql);
    final RelMetadataQuery mq = RelMetadataQuery.instance();
    RelOptPredicateList inputSet = mq.getAllPredicates(rel);
    // Filter on aggregate, we cannot infer lineage
    assertNull(inputSet);
  }

  @Test public void testAllPredicatesAndTablesJoin() {
    final String sql = "select x.sal, y.deptno from\n"
        + "(select a.deptno, c.sal from (select * from emp limit 7) as a\n"
        + "cross join (select * from dept limit 1) as b\n"
        + "inner join (select * from emp limit 2) as c\n"
        + "on a.deptno = c.deptno) as x\n"
        + "inner join\n"
        + "(select a.deptno, c.sal from (select * from emp limit 7) as a\n"
        + "cross join (select * from dept limit 1) as b\n"
        + "inner join (select * from emp limit 2) as c\n"
        + "on a.deptno = c.deptno) as y\n"
        + "on x.deptno = y.deptno";
    final RelNode rel = convertSql(sql);
    final RelMetadataQuery mq = RelMetadataQuery.instance();
    final RelOptPredicateList inputSet = mq.getAllPredicates(rel);
    assertThat(inputSet.pulledUpPredicates.toString(),
        equalTo("[true, "
            + "=([CATALOG, SALES, EMP].#0.$7, [CATALOG, SALES, EMP].#1.$7), "
            + "true, "
            + "=([CATALOG, SALES, EMP].#2.$7, [CATALOG, SALES, EMP].#3.$7), "
            + "=([CATALOG, SALES, EMP].#0.$7, [CATALOG, SALES, EMP].#2.$7)]"));
    final Set<RelTableRef> tableReferences = Sets.newTreeSet(mq.getTableReferences(rel));
    assertThat(tableReferences.toString(),
        equalTo("[[CATALOG, SALES, DEPT].#0, [CATALOG, SALES, DEPT].#1, "
            + "[CATALOG, SALES, EMP].#0, [CATALOG, SALES, EMP].#1, "
            + "[CATALOG, SALES, EMP].#2, [CATALOG, SALES, EMP].#3]"));
  }

  @Test public void testAllPredicatesAndTableUnion() {
    final String sql = "select a.deptno, c.sal from (select * from emp limit 7) as a\n"
        + "cross join (select * from dept limit 1) as b\n"
        + "inner join (select * from emp limit 2) as c\n"
        + "on a.deptno = c.deptno\n"
        + "union all\n"
        + "select a.deptno, c.sal from (select * from emp limit 7) as a\n"
        + "cross join (select * from dept limit 1) as b\n"
        + "inner join (select * from emp limit 2) as c\n"
        + "on a.deptno = c.deptno";
    final RelNode rel = convertSql(sql);
    final RelMetadataQuery mq = RelMetadataQuery.instance();
    final RelOptPredicateList inputSet = mq.getAllPredicates(rel);
    assertThat(inputSet.pulledUpPredicates.toString(),
        equalTo("[true, "
            + "=([CATALOG, SALES, EMP].#0.$7, [CATALOG, SALES, EMP].#1.$7), "
            + "true, "
            + "=([CATALOG, SALES, EMP].#2.$7, [CATALOG, SALES, EMP].#3.$7)]"));
    final Set<RelTableRef> tableReferences = Sets.newTreeSet(mq.getTableReferences(rel));
    assertThat(tableReferences.toString(),
        equalTo("[[CATALOG, SALES, DEPT].#0, [CATALOG, SALES, DEPT].#1, "
            + "[CATALOG, SALES, EMP].#0, [CATALOG, SALES, EMP].#1, "
            + "[CATALOG, SALES, EMP].#2, [CATALOG, SALES, EMP].#3]"));
  }

  @Test public void testAllPredicatesCrossJoinMultiTable() {
    final String sql = "select x.sal from\n"
        + "(select a.deptno, c.sal from (select * from emp limit 7) as a\n"
        + "cross join (select * from dept limit 1) as b\n"
        + "cross join (select * from emp where empno = 5 limit 2) as c) as x";
    final RelNode rel = convertSql(sql);
    final RelMetadataQuery mq = RelMetadataQuery.instance();
    final Set<RelTableRef> tableReferences = Sets.newTreeSet(mq.getTableReferences(rel));
    assertThat(tableReferences.toString(),
        equalTo("[[CATALOG, SALES, DEPT].#0, "
            + "[CATALOG, SALES, EMP].#0, "
            + "[CATALOG, SALES, EMP].#1]"));
    final RelOptPredicateList inputSet = mq.getAllPredicates(rel);
    // Note that we reference [CATALOG, SALES, EMP].#1 rather than [CATALOG, SALES, EMP].#0
    assertThat(inputSet.pulledUpPredicates.toString(),
        equalTo("[true, =([CATALOG, SALES, EMP].#1.$0, 5), true]"));
  }

  @Test public void testAllPredicatesUnionMultiTable() {
    final String sql = "select x.sal from\n"
        + "(select a.deptno, a.sal from (select * from emp) as a\n"
        + "union all select emp.deptno, emp.sal from emp\n"
        + "union all select emp.deptno, emp.sal from emp where empno = 5) as x";
    final RelNode rel = convertSql(sql);
    final RelMetadataQuery mq = RelMetadataQuery.instance();
    final Set<RelTableRef> tableReferences = Sets.newTreeSet(mq.getTableReferences(rel));
    assertThat(tableReferences.toString(),
        equalTo("[[CATALOG, SALES, EMP].#0, "
            + "[CATALOG, SALES, EMP].#1, "
            + "[CATALOG, SALES, EMP].#2]"));
    // Note that we reference [CATALOG, SALES, EMP].#2 rather than
    // [CATALOG, SALES, EMP].#0 or [CATALOG, SALES, EMP].#1
    final RelOptPredicateList inputSet = mq.getAllPredicates(rel);
    assertThat(inputSet.pulledUpPredicates.toString(),
        equalTo("[=([CATALOG, SALES, EMP].#2.$0, 5)]"));
  }

  private void checkNodeTypeCount(String sql, Map<Class<? extends RelNode>, Integer> expected) {
    final RelNode rel = convertSql(sql);
    final RelMetadataQuery mq = RelMetadataQuery.instance();
    final Multimap<Class<? extends RelNode>, RelNode> result = mq.getNodeTypes(rel);
    assertThat(result, notNullValue());
    final Map<Class<? extends RelNode>, Integer> resultCount = new HashMap<>();
    for (Entry<Class<? extends RelNode>, Collection<RelNode>> e : result.asMap().entrySet()) {
      resultCount.put(e.getKey(), e.getValue().size());
    }
    assertEquals(expected, resultCount);
  }

  @Test public void testNodeTypeCountEmp() {
    final String sql = "select * from emp";
    final Map<Class<? extends RelNode>, Integer> expected = new HashMap<>();
    expected.put(TableScan.class, 1);
    expected.put(Project.class, 1);
    checkNodeTypeCount(sql, expected);
  }

  @Test public void testNodeTypeCountDept() {
    final String sql = "select * from dept";
    final Map<Class<? extends RelNode>, Integer> expected = new HashMap<>();
    expected.put(TableScan.class, 1);
    expected.put(Project.class, 1);
    checkNodeTypeCount(sql, expected);
  }

  @Test public void testNodeTypeCountValues() {
    final String sql = "select * from (values (1), (2)) as t(c)";
    final Map<Class<? extends RelNode>, Integer> expected = new HashMap<>();
    expected.put(Values.class, 1);
    expected.put(Project.class, 1);
    checkNodeTypeCount(sql, expected);
  }

  @Test public void testNodeTypeCountCartesian() {
    final String sql = "select * from emp,dept";
    final Map<Class<? extends RelNode>, Integer> expected = new HashMap<>();
    expected.put(TableScan.class, 2);
    expected.put(Join.class, 1);
    expected.put(Project.class, 1);
    checkNodeTypeCount(sql, expected);
  }

  @Test public void testNodeTypeCountJoin() {
    final String sql = "select * from emp\n"
        + "inner join dept on emp.deptno = dept.deptno";
    final Map<Class<? extends RelNode>, Integer> expected = new HashMap<>();
    expected.put(TableScan.class, 2);
    expected.put(Join.class, 1);
    expected.put(Project.class, 1);
    checkNodeTypeCount(sql, expected);
  }

  @Test public void testNodeTypeCountJoinFinite() {
    final String sql = "select * from (select * from emp limit 14) as emp\n"
        + "inner join (select * from dept limit 4) as dept\n"
        + "on emp.deptno = dept.deptno";
    final Map<Class<? extends RelNode>, Integer> expected = new HashMap<>();
    expected.put(TableScan.class, 2);
    expected.put(Join.class, 1);
    expected.put(Project.class, 3);
    expected.put(Sort.class, 2);
    checkNodeTypeCount(sql, expected);
  }

  @Test public void testNodeTypeCountJoinEmptyFinite() {
    final String sql = "select * from (select * from emp limit 0) as emp\n"
        + "inner join (select * from dept limit 4) as dept\n"
        + "on emp.deptno = dept.deptno";
    final Map<Class<? extends RelNode>, Integer> expected = new HashMap<>();
    expected.put(TableScan.class, 2);
    expected.put(Join.class, 1);
    expected.put(Project.class, 3);
    expected.put(Sort.class, 2);
    checkNodeTypeCount(sql, expected);
  }

  @Test public void testNodeTypeCountLeftJoinEmptyFinite() {
    final String sql = "select * from (select * from emp limit 0) as emp\n"
        + "left join (select * from dept limit 4) as dept\n"
        + "on emp.deptno = dept.deptno";
    final Map<Class<? extends RelNode>, Integer> expected = new HashMap<>();
    expected.put(TableScan.class, 2);
    expected.put(Join.class, 1);
    expected.put(Project.class, 3);
    expected.put(Sort.class, 2);
    checkNodeTypeCount(sql, expected);
  }

  @Test public void testNodeTypeCountRightJoinEmptyFinite() {
    final String sql = "select * from (select * from emp limit 0) as emp\n"
        + "right join (select * from dept limit 4) as dept\n"
        + "on emp.deptno = dept.deptno";
    final Map<Class<? extends RelNode>, Integer> expected = new HashMap<>();
    expected.put(TableScan.class, 2);
    expected.put(Join.class, 1);
    expected.put(Project.class, 3);
    expected.put(Sort.class, 2);
    checkNodeTypeCount(sql, expected);
  }

  @Test public void testNodeTypeCountJoinFiniteEmpty() {
    final String sql = "select * from (select * from emp limit 7) as emp\n"
        + "inner join (select * from dept limit 0) as dept\n"
        + "on emp.deptno = dept.deptno";
    final Map<Class<? extends RelNode>, Integer> expected = new HashMap<>();
    expected.put(TableScan.class, 2);
    expected.put(Join.class, 1);
    expected.put(Project.class, 3);
    expected.put(Sort.class, 2);
    checkNodeTypeCount(sql, expected);
  }

  @Test public void testNodeTypeCountJoinEmptyEmpty() {
    final String sql = "select * from (select * from emp limit 0) as emp\n"
        + "inner join (select * from dept limit 0) as dept\n"
        + "on emp.deptno = dept.deptno";
    final Map<Class<? extends RelNode>, Integer> expected = new HashMap<>();
    expected.put(TableScan.class, 2);
    expected.put(Join.class, 1);
    expected.put(Project.class, 3);
    expected.put(Sort.class, 2);
    checkNodeTypeCount(sql, expected);
  }

  @Test public void testNodeTypeCountUnion() {
    final String sql = "select ename from emp\n"
        + "union all\n"
        + "select name from dept";
    final Map<Class<? extends RelNode>, Integer> expected = new HashMap<>();
    expected.put(TableScan.class, 2);
    expected.put(Project.class, 2);
    expected.put(Union.class, 1);
    checkNodeTypeCount(sql, expected);
  }

  @Test public void testNodeTypeCountUnionOnFinite() {
    final String sql = "select ename from (select * from emp limit 100)\n"
        + "union all\n"
        + "select name from (select * from dept limit 40)";
    final Map<Class<? extends RelNode>, Integer> expected = new HashMap<>();
    expected.put(TableScan.class, 2);
    expected.put(Union.class, 1);
    expected.put(Project.class, 4);
    expected.put(Sort.class, 2);
    checkNodeTypeCount(sql, expected);
  }

  @Test public void testNodeTypeCountMinusOnFinite() {
    final String sql = "select ename from (select * from emp limit 100)\n"
        + "except\n"
        + "select name from (select * from dept limit 40)";
    final Map<Class<? extends RelNode>, Integer> expected = new HashMap<>();
    expected.put(TableScan.class, 2);
    expected.put(Minus.class, 1);
    expected.put(Project.class, 4);
    expected.put(Sort.class, 2);
    checkNodeTypeCount(sql, expected);
  }

  @Test public void testNodeTypeCountFilter() {
    final String sql = "select * from emp where ename='Mathilda'";
    final Map<Class<? extends RelNode>, Integer> expected = new HashMap<>();
    expected.put(TableScan.class, 1);
    expected.put(Project.class, 1);
    expected.put(Filter.class, 1);
    checkNodeTypeCount(sql, expected);
  }

  @Test public void testNodeTypeCountSort() {
    final String sql = "select * from emp order by ename";
    final Map<Class<? extends RelNode>, Integer> expected = new HashMap<>();
    expected.put(TableScan.class, 1);
    expected.put(Project.class, 1);
    expected.put(Sort.class, 1);
    checkNodeTypeCount(sql, expected);
  }

  @Test public void testNodeTypeCountSortLimit() {
    final String sql = "select * from emp order by ename limit 10";
    final Map<Class<? extends RelNode>, Integer> expected = new HashMap<>();
    expected.put(TableScan.class, 1);
    expected.put(Project.class, 1);
    expected.put(Sort.class, 1);
    checkNodeTypeCount(sql, expected);
  }

  @Test public void testNodeTypeCountSortLimitOffset() {
    final String sql = "select * from emp order by ename limit 10 offset 5";
    final Map<Class<? extends RelNode>, Integer> expected = new HashMap<>();
    expected.put(TableScan.class, 1);
    expected.put(Project.class, 1);
    expected.put(Sort.class, 1);
    checkNodeTypeCount(sql, expected);
  }

  @Test public void testNodeTypeCountSortLimitOffsetOnFinite() {
    final String sql = "select * from (select * from emp limit 12)\n"
        + "order by ename limit 20 offset 5";
    final Map<Class<? extends RelNode>, Integer> expected = new HashMap<>();
    expected.put(TableScan.class, 1);
    expected.put(Project.class, 2);
    expected.put(Sort.class, 2);
    checkNodeTypeCount(sql, expected);
  }

  @Test public void testNodeTypeCountAggregate() {
    final String sql = "select deptno from emp group by deptno";
    final Map<Class<? extends RelNode>, Integer> expected = new HashMap<>();
    expected.put(TableScan.class, 1);
    expected.put(Project.class, 1);
    expected.put(Aggregate.class, 1);
    checkNodeTypeCount(sql, expected);
  }

  @Test public void testNodeTypeCountAggregateGroupingSets() {
    final String sql = "select deptno from emp\n"
        + "group by grouping sets ((deptno), (ename, deptno))";
    final Map<Class<? extends RelNode>, Integer> expected = new HashMap<>();
    expected.put(TableScan.class, 1);
    expected.put(Project.class, 2);
    expected.put(Aggregate.class, 1);
    checkNodeTypeCount(sql, expected);
  }

  @Test public void testNodeTypeCountAggregateEmptyKeyOnEmptyTable() {
    final String sql = "select count(*) from (select * from emp limit 0)";
    final Map<Class<? extends RelNode>, Integer> expected = new HashMap<>();
    expected.put(TableScan.class, 1);
    expected.put(Project.class, 2);
    expected.put(Aggregate.class, 1);
    expected.put(Sort.class, 1);
    checkNodeTypeCount(sql, expected);
  }

  @Test public void testNodeTypeCountFilterAggregateEmptyKey() {
    final String sql = "select count(*) from emp where 1 = 0";
    final Map<Class<? extends RelNode>, Integer> expected = new HashMap<>();
    expected.put(TableScan.class, 1);
    expected.put(Project.class, 1);
    expected.put(Filter.class, 1);
    expected.put(Aggregate.class, 1);
    checkNodeTypeCount(sql, expected);
  }

  private static final SqlOperator NONDETERMINISTIC_OP = new SqlSpecialOperator(
          "NDC",
          SqlKind.OTHER_FUNCTION,
          0,
          false,
          ReturnTypes.BOOLEAN,
          null, null) {
    @Override public boolean isDeterministic() {
      return false;
    }
  };

  @Test public void testGetPredicatesForJoin() throws Exception {
    final FrameworkConfig config = RelBuilderTest.config().build();
    final RelBuilder builder = RelBuilder.create(config);
    RelNode join = builder
        .scan("EMP")
        .scan("DEPT")
        .join(JoinRelType.INNER, builder.call(NONDETERMINISTIC_OP))
        .build();
    RelMetadataQuery mq = RelMetadataQuery.instance();
    assertTrue(mq.getPulledUpPredicates(join).pulledUpPredicates.isEmpty());

    RelNode join1 = builder
        .scan("EMP")
        .scan("DEPT")
        .join(JoinRelType.INNER,
          builder.call(SqlStdOperatorTable.EQUALS,
            builder.field(2, 0, 0),
            builder.field(2, 1, 0)))
        .build();
    assertEquals("=($0, $8)",
        mq.getPulledUpPredicates(join1).pulledUpPredicates.get(0).toString());
  }

  @Test public void testGetPredicatesForFilter() throws Exception {
    final FrameworkConfig config = RelBuilderTest.config().build();
    final RelBuilder builder = RelBuilder.create(config);
    RelNode filter = builder
        .scan("EMP")
        .filter(builder.call(NONDETERMINISTIC_OP))
        .build();
    RelMetadataQuery mq = RelMetadataQuery.instance();
    assertTrue(mq.getPulledUpPredicates(filter).pulledUpPredicates.isEmpty());

    RelNode filter1 = builder
        .scan("EMP")
        .filter(
          builder.call(SqlStdOperatorTable.EQUALS,
            builder.field(1, 0, 0),
            builder.field(1, 0, 1)))
        .build();
    assertEquals("=($0, $1)",
        mq.getPulledUpPredicates(filter1).pulledUpPredicates.get(0).toString());
  }

  /**
   * Matcher that succeeds for any collection that, when converted to strings
   * and sorted on those strings, matches the given reference string.
   *
   * <p>Use it as an alternative to {@link CoreMatchers#is} if items in your
   * list might occur in any order.
   *
   * <p>For example:
   *
   * <blockquote><pre>List&lt;Integer&gt; ints = Arrays.asList(2, 500, 12);
   * assertThat(ints, sortsAs("[12, 2, 500]");</pre></blockquote>
   */
  static <T> Matcher<Iterable<? extends T>> sortsAs(final String value) {
    return new CustomTypeSafeMatcher<Iterable<? extends T>>(value) {
      protected boolean matchesSafely(Iterable<? extends T> item) {
        final List<String> strings = new ArrayList<>();
        for (T t : item) {
          strings.add(t.toString());
        }
        Collections.sort(strings);
        return value.equals(strings.toString());
      }
    };
  }

  /** Custom metadata interface. */
  public interface ColType extends Metadata {
    Method METHOD = Types.lookupMethod(ColType.class, "getColType", int.class);

    MetadataDef<ColType> DEF =
        MetadataDef.of(ColType.class, ColType.Handler.class, METHOD);

    String getColType(int column);

    /** Handler API. */
    interface Handler extends MetadataHandler<ColType> {
      String getColType(RelNode r, RelMetadataQuery mq, int column);
    }
  }

  /** A provider for {@link org.apache.calcite.test.RelMetadataTest.ColType} via
   * reflection. */
  public abstract static class PartialColTypeImpl
      implements MetadataHandler<ColType> {
    static final ThreadLocal<List<String>> THREAD_LIST = new ThreadLocal<>();

    public MetadataDef<ColType> getDef() {
      return ColType.DEF;
    }

    /** Implementation of {@link ColType#getColType(int)} for
     * {@link org.apache.calcite.rel.logical.LogicalAggregate}, called via
     * reflection. */
    @SuppressWarnings("UnusedDeclaration")
    public String getColType(Aggregate rel, RelMetadataQuery mq, int column) {
      final String name =
          rel.getRowType().getFieldList().get(column).getName() + "-agg";
      THREAD_LIST.get().add(name);
      return name;
    }
  }

  /** A provider for {@link org.apache.calcite.test.RelMetadataTest.ColType} via
   * reflection. */
  public static class ColTypeImpl extends PartialColTypeImpl {
    public static final RelMetadataProvider SOURCE =
        ReflectiveRelMetadataProvider.reflectiveSource(ColType.METHOD, new ColTypeImpl());

    /** Implementation of {@link ColType#getColType(int)} for
     * {@link RelNode}, called via reflection. */
    @SuppressWarnings("UnusedDeclaration")
    public String getColType(RelNode rel, RelMetadataQuery mq, int column) {
      final String name =
          rel.getRowType().getFieldList().get(column).getName() + "-rel";
      THREAD_LIST.get().add(name);
      return name;
    }
  }

  /** Implementation of {@link ColType} that has no fall-back for {@link RelNode}. */
  public static class BrokenColTypeImpl extends PartialColTypeImpl {
    public static final RelMetadataProvider SOURCE =
        ReflectiveRelMetadataProvider.reflectiveSource(ColType.METHOD,
            new BrokenColTypeImpl());
  }

  /** Extension to {@link RelMetadataQuery} to support {@link ColType}.
   *
   * <p>Illustrates how you would package up a user-defined metadata type. */
  private static class MyRelMetadataQuery extends RelMetadataQuery {
    private ColType.Handler colTypeHandler;

    MyRelMetadataQuery() {
      super(THREAD_PROVIDERS.get(), EMPTY);
      colTypeHandler = initialHandler(ColType.Handler.class);
    }

    public String colType(RelNode rel, int column) {
      for (;;) {
        try {
          return colTypeHandler.getColType(rel, this, column);
        } catch (JaninoRelMetadataProvider.NoHandler e) {
          colTypeHandler = revise(e.relClass, ColType.DEF);
        }
      }
    }
  }
}

// End RelMetadataTest.java
