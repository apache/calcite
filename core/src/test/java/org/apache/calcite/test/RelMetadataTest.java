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

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.metadata.CachingRelMetadataProvider;
import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.metadata.Metadata;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import org.hamcrest.CoreMatchers;
import org.hamcrest.Matcher;
import org.junit.Ignore;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

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

  private static final double EMP_SIZE = 1000.0;

  private static final double DEPT_SIZE = 100.0;

  //~ Methods ----------------------------------------------------------------

  private static Matcher<? super Number> nearTo(Number v, Number epsilon) {
    return CoreMatchers.equalTo(v); // TODO: use epsilon
  }

  // ----------------------------------------------------------------------
  // Tests for getPercentageOriginalRows
  // ----------------------------------------------------------------------

  private RelNode convertSql(String sql) {
    RelNode rel = tester.convertSqlToRel(sql);
    DefaultRelMetadataProvider provider = new DefaultRelMetadataProvider();
    rel.getCluster().setMetadataProvider(provider);
    return rel;
  }

  private void checkPercentageOriginalRows(String sql, double expected) {
    checkPercentageOriginalRows(sql, expected, EPSILON);
  }

  private void checkPercentageOriginalRows(
      String sql,
      double expected,
      double epsilon) {
    RelNode rel = convertSql(sql);
    Double result = RelMetadataQuery.getPercentageOriginalRows(rel);
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
    return RelMetadataQuery.getColumnOrigins(rel, 0);
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

  private void checkRowCount(
      String sql,
      double expected) {
    RelNode rel = convertSql(sql);
    Double result = RelMetadataQuery.getRowCount(rel);
    assertTrue(result != null);
    assertEquals(expected, result, 0d);
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
      double expected) {
    RelNode rel = convertSql(sql);
    Double result = RelMetadataQuery.getSelectivity(rel, null);
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

  private void checkRelSelectivity(
      RelNode rel,
      double expected) {
    Double result = RelMetadataQuery.getSelectivity(rel, null);
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
    Double result = RelMetadataQuery.getSelectivity(rel, null);
    assertThat(result,
        nearTo(DEFAULT_COMP_SELECTIVITY * DEFAULT_EQUAL_SELECTIVITY, EPSILON));
  }

  @Test public void testDistinctRowCountTable() {
    // no unique key information is available so return null
    RelNode rel = convertSql("select * from emp where deptno = 10");
    ImmutableBitSet groupKey = ImmutableBitSet.of();
    Double result =
        RelMetadataQuery.getDistinctRowCount(
            rel, groupKey, null);
    assertTrue(result == null);
  }

  @Test public void testCustomProvider() {
    final List<String> buf = new ArrayList<String>();
    ColTypeImpl.THREAD_LIST.set(buf);

    RelNode rel =
        convertSql("select deptno, count(*) from emp where deptno > 10 "
            + "group by deptno having count(*) = 0");
    rel.getCluster().setMetadataProvider(
        ChainedRelMetadataProvider.of(
            ImmutableList.of(
                ColTypeImpl.SOURCE, rel.getCluster().getMetadataProvider())));

    // Top node is a filter. Its metadata uses getColType(RelNode, int).
    assertThat(rel, instanceOf(LogicalFilter.class));
    assertThat(rel.metadata(ColType.class).getColType(0),
        equalTo("DEPTNO-rel"));
    assertThat(rel.metadata(ColType.class).getColType(1),
        equalTo("EXPR$1-rel"));

    // Next node is an aggregate. Its metadata uses
    // getColType(LogicalAggregate, int).
    final RelNode input = rel.getInput(0);
    assertThat(input, instanceOf(LogicalAggregate.class));
    assertThat(input.metadata(ColType.class).getColType(0),
        equalTo("DEPTNO-agg"));

    // There is no caching. Another request causes another call to the provider.
    assertThat(buf.toString(), equalTo("[DEPTNO-rel, EXPR$1-rel, DEPTNO-agg]"));
    assertThat(buf.size(), equalTo(3));
    assertThat(input.metadata(ColType.class).getColType(0),
        equalTo("DEPTNO-agg"));
    assertThat(buf.size(), equalTo(4));

    // Now add a cache. Only the first request for each piece of metadata
    // generates a new call to the provider.
    final RelOptPlanner planner = rel.getCluster().getPlanner();
    rel.getCluster().setMetadataProvider(
        new CachingRelMetadataProvider(
            rel.getCluster().getMetadataProvider(), planner));
    assertThat(input.metadata(ColType.class).getColType(0),
        equalTo("DEPTNO-agg"));
    assertThat(buf.size(), equalTo(5));
    assertThat(input.metadata(ColType.class).getColType(0),
        equalTo("DEPTNO-agg"));
    assertThat(buf.size(), equalTo(5));
    assertThat(input.metadata(ColType.class).getColType(1),
        equalTo("EXPR$1-agg"));
    assertThat(buf.size(), equalTo(6));
    assertThat(input.metadata(ColType.class).getColType(1),
        equalTo("EXPR$1-agg"));
    assertThat(buf.size(), equalTo(6));
    assertThat(input.metadata(ColType.class).getColType(0),
        equalTo("DEPTNO-agg"));
    assertThat(buf.size(), equalTo(6));

    // With a different timestamp, a metadata item is re-computed on first call.
    long timestamp = planner.getRelMetadataTimestamp(rel);
    assertThat(timestamp, equalTo(0L));
    ((MockRelOptPlanner) planner).setRelMetadataTimestamp(timestamp + 1);
    assertThat(input.metadata(ColType.class).getColType(0),
        equalTo("DEPTNO-agg"));
    assertThat(buf.size(), equalTo(7));
    assertThat(input.metadata(ColType.class).getColType(0),
        equalTo("DEPTNO-agg"));
    assertThat(buf.size(), equalTo(7));
  }

  /** Custom metadata interface. */
  public interface ColType extends Metadata {
    String getColType(int column);
  }

  /** A provider for {@link org.apache.calcite.test.RelMetadataTest.ColType} via
   * reflection. */
  public static class ColTypeImpl {
    static final ThreadLocal<List<String>> THREAD_LIST =
        new ThreadLocal<List<String>>();
    static final Method METHOD;
    static {
      try {
        METHOD = ColType.class.getMethod("getColType", int.class);
      } catch (NoSuchMethodException e) {
        throw new RuntimeException(e);
      }
    }

    public static final RelMetadataProvider SOURCE =
        ReflectiveRelMetadataProvider.reflectiveSource(
            METHOD, new ColTypeImpl());

    /** Implementation of {@link ColType#getColType(int)} for
     * {@link org.apache.calcite.rel.logical.LogicalAggregate}, called via
     * reflection. */
    @SuppressWarnings("UnusedDeclaration")
    public String getColType(Aggregate rel, int column) {
      final String name =
          rel.getRowType().getFieldList().get(column).getName() + "-agg";
      THREAD_LIST.get().add(name);
      return name;
    }

    /** Implementation of {@link ColType#getColType(int)} for
     * {@link RelNode}, called via reflection. */
    @SuppressWarnings("UnusedDeclaration")
    public String getColType(RelNode rel, int column) {
      final String name =
          rel.getRowType().getFieldList().get(column).getName() + "-rel";
      THREAD_LIST.get().add(name);
      return name;
    }
  }
}

// End RelMetadataTest.java
