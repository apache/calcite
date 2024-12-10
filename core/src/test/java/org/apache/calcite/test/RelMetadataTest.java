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

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableLimit;
import org.apache.calcite.adapter.enumerable.EnumerableMergeJoin;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Minus;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sample;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.hint.RelHint;
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
import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.Metadata;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.MetadataHandlerProvider;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMdColumnUniqueness;
import org.apache.calcite.rel.metadata.RelMdPopulationSize;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.metadata.UnboundMetadata;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexTableInputRef.RelTableRef;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlBasicFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.test.SqlTestFactory;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.test.catalog.MockCatalogReaderSimple;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Holder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

import static com.google.common.collect.ImmutableList.toImmutableList;

import static org.apache.calcite.test.Matchers.hasFieldNames;
import static org.apache.calcite.test.Matchers.isAlmost;
import static org.apache.calcite.test.Matchers.sortsAs;

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.endsWith;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.hasToString;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import static java.util.Objects.requireNonNull;

/**
 * Unit test for {@link DefaultRelMetadataProvider}. See
 * {@link SqlToRelTestBase} class comments for details on the schema used. Note
 * that no optimizer rules are fired on the translation of the SQL into
 * relational algebra (e.g. join conditions in the WHERE clause will look like
 * filters), so it's necessary to phrase the SQL carefully.
 */
public class RelMetadataTest {
  //~ Static fields/initializers ---------------------------------------------

  private static final double DEFAULT_EQUAL_SELECTIVITY = 0.15;

  private static final double DEFAULT_EQUAL_SELECTIVITY_SQUARED =
      DEFAULT_EQUAL_SELECTIVITY * DEFAULT_EQUAL_SELECTIVITY;

  private static final double DEFAULT_COMP_SELECTIVITY = 0.5;

  private static final double DEFAULT_NOTNULL_SELECTIVITY = 0.9;

  private static final double DEFAULT_SELECTIVITY = 0.25;

  private static final double EMP_SIZE = 14d;

  private static final double DEPT_SIZE = 4d;

  private static final List<String> EMP_QNAME =
      ImmutableList.of("CATALOG", "SALES", "EMP");

  /** Ensures that tests that use a lot of memory do not run at the same
   * time. */
  private static final ReentrantLock LOCK = new ReentrantLock();

  //~ Methods ----------------------------------------------------------------

  /** Creates a fixture. */
  protected RelMetadataFixture fixture() {
    return RelMetadataFixture.DEFAULT;
  }

  final RelMetadataFixture sql(String sql) {
    return fixture().withSql(sql);
  }

  // ----------------------------------------------------------------------
  // Tests for getPercentageOriginalRows
  // ----------------------------------------------------------------------

  @Test void testPercentageOriginalRowsTableOnly() {
    sql("select * from dept")
        .assertPercentageOriginalRows(isAlmost(1.0));
  }

  @Test void testPercentageOriginalRowsAgg() {
    sql("select deptno from dept group by deptno")
        .assertPercentageOriginalRows(isAlmost(1.0));
  }

  @Disabled
  @Test void testPercentageOriginalRowsOneFilter() {
    sql("select * from dept where deptno = 20")
        .assertPercentageOriginalRows(isAlmost(DEFAULT_EQUAL_SELECTIVITY));
  }

  @Disabled
  @Test void testPercentageOriginalRowsTwoFilters() {
    sql("select * from (\n"
        + "  select * from dept where name='X')\n"
        + "where deptno = 20")
        .assertPercentageOriginalRows(
            isAlmost(DEFAULT_EQUAL_SELECTIVITY_SQUARED));
  }

  @Disabled
  @Test void testPercentageOriginalRowsRedundantFilter() {
    sql("select * from (\n"
        + "  select * from dept where deptno=20)\n"
        + "where deptno = 20")
        .assertPercentageOriginalRows(
            isAlmost(DEFAULT_EQUAL_SELECTIVITY));
  }

  @Test void testPercentageOriginalRowsJoin() {
    sql("select * from emp inner join dept on emp.deptno=dept.deptno")
        .assertPercentageOriginalRows(isAlmost(1.0));
  }

  @Disabled
  @Test void testPercentageOriginalRowsJoinTwoFilters() {
    sql("select * from (\n"
        + "  select * from emp where deptno=10) e\n"
        + "inner join (select * from dept where deptno=10) d\n"
        + "on e.deptno=d.deptno")
        .assertPercentageOriginalRows(
            isAlmost(DEFAULT_EQUAL_SELECTIVITY_SQUARED));
  }

  @Test void testPercentageOriginalRowsUnionNoFilter() {
    sql("select name from dept union all select ename from emp")
        .assertPercentageOriginalRows(isAlmost(1.0));
  }

  @Disabled
  @Test void testPercentageOriginalRowsUnionLittleFilter() {
    sql("select name from dept where deptno=20"
        + " union all select ename from emp")
        .assertPercentageOriginalRows(
            isAlmost(((DEPT_SIZE * DEFAULT_EQUAL_SELECTIVITY) + EMP_SIZE)
                / (DEPT_SIZE + EMP_SIZE)));
  }

  @Disabled
  @Test void testPercentageOriginalRowsUnionBigFilter() {
    sql("select name from dept"
        + " union all select ename from emp where deptno=20")
        .assertPercentageOriginalRows(
            isAlmost(((EMP_SIZE * DEFAULT_EQUAL_SELECTIVITY) + DEPT_SIZE)
                / (DEPT_SIZE + EMP_SIZE)));
  }

  // ----------------------------------------------------------------------
  // Tests for getColumnOrigins
  // ----------------------------------------------------------------------

  @Test void testCalcColumnOriginsTable() {
    final String sql = "select name,deptno from dept where deptno > 10";
    final RelNode relNode = sql(sql).toRel();
    final HepProgram program = new HepProgramBuilder().
        addRuleInstance(CoreRules.PROJECT_TO_CALC).build();
    final HepPlanner planner = new HepPlanner(program);
    planner.setRoot(relNode);
    final RelNode calc = planner.findBestExp();
    final RelMetadataQuery mq = calc.getCluster().getMetadataQuery();
    final RelColumnOrigin nameColumn = mq.getColumnOrigin(calc, 0);
    assertThat(nameColumn, notNullValue());
    assertThat(nameColumn.getOriginColumnOrdinal(), is(1));
    final RelColumnOrigin deptnoColumn = mq.getColumnOrigin(calc, 1);
    assertThat(deptnoColumn, notNullValue());
    assertThat(deptnoColumn.getOriginColumnOrdinal(), is(0));
  }

  @Test void testDerivedColumnOrigins() {
    final String sql1 = ""
        + "select empno, sum(sal) as all_sal\n"
        + "from emp\n"
        + "group by empno";
    final RelNode relNode = sql(sql1).toRel();
    final HepProgram program = new HepProgramBuilder().
        addRuleInstance(CoreRules.PROJECT_TO_CALC).build();
    final HepPlanner planner = new HepPlanner(program);
    planner.setRoot(relNode);
    final RelNode rel = planner.findBestExp();
    final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
    final RelColumnOrigin allSal = mq.getColumnOrigin(rel, 1);
    assertThat(allSal, notNullValue());
    assertThat(allSal.getOriginColumnOrdinal(), is(5));
  }

  @Test void testColumnOriginsTableOnly() {
    sql("select name as dname from dept")
        .assertColumnOriginSingle("DEPT", "NAME", false);
  }

  @Test void testColumnOriginsExpression() {
    sql("select upper(name) as dname from dept")
        .assertColumnOriginSingle("DEPT", "NAME", true);
  }

  @Test void testColumnOriginsDyadicExpression() {
    sql("select name||ename from dept,emp")
        .assertColumnOriginDouble("DEPT", "NAME", "EMP", "ENAME", true);
  }

  @Test void testColumnOriginsConstant() {
    sql("select 'Minstrelsy' as dname from dept")
        .assertColumnOriginIsEmpty();
  }

  @Test void testColumnOriginsFilter() {
    sql("select name as dname from dept where deptno=10")
        .assertColumnOriginSingle("DEPT", "NAME", false);
  }

  @Test void testColumnOriginsJoinLeft() {
    sql("select ename from emp,dept")
        .assertColumnOriginSingle("EMP", "ENAME", false);
  }

  @Test void testColumnOriginsJoinRight() {
    sql("select name as dname from emp,dept")
        .assertColumnOriginSingle("DEPT", "NAME", false);
  }

  @Test void testColumnOriginsJoinOuter() {
    sql("select name as dname from emp left outer join dept"
        + " on emp.deptno = dept.deptno")
        .assertColumnOriginSingle("DEPT", "NAME", true);
  }

  @Test void testColumnOriginsJoinFullOuter() {
    sql("select name as dname from emp full outer join dept"
        + " on emp.deptno = dept.deptno")
        .assertColumnOriginSingle("DEPT", "NAME", true);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5944">[CALCITE-5944]
   * Add metadata for Sample</a>. */
  @Test void testColumnOriginsSample() {
    final String sql = "select productid from products_temporal\n"
        + "tablesample bernoulli(50) repeatable(1)";
    sql(sql)
        .assertColumnOriginSingle("PRODUCTS_TEMPORAL", "PRODUCTID", false);
  }

  @Test void testColumnOriginsSnapshot() {
    final String sql = "select productid from products_temporal\n"
        + "for system_time as of TIMESTAMP '2011-01-02 00:00:00'";
    sql(sql)
        .assertColumnOriginSingle("PRODUCTS_TEMPORAL", "PRODUCTID", false);
  }

  @Test void testColumnOriginsAggKey() {
    sql("select name,count(deptno) from dept group by name")
        .assertColumnOriginSingle("DEPT", "NAME", false);
  }

  @Test void testColumnOriginsAggReduced() {
    sql("select count(deptno),name from dept group by name")
        .assertColumnOriginIsEmpty();
  }

  @Test void testColumnOriginsAggCountNullable() {
    sql("select count(mgr),ename from emp group by ename")
        .assertColumnOriginSingle("EMP", "MGR", true);
  }

  @Test void testColumnOriginsAggCountStar() {
    sql("select count(*),name from dept group by name")
        .assertColumnOriginIsEmpty();
  }

  @Test void testColumnOriginsValues() {
    sql("values(1,2,3)")
        .assertColumnOriginIsEmpty();
  }

  @Test @Disabled("Plan contains casts, which inhibit metadata propagation")
  void testColumnOriginsUnion() {
    sql("select name from dept union all select ename from emp")
        .assertColumnOriginDouble("DEPT", "NAME", "EMP", "ENAME", false);
  }

  @Test void testColumnOriginsSelfUnion() {
    sql("select ename from emp union all select ename from emp")
        .assertColumnOriginSingle("EMP", "ENAME", false);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4192">[CALCITE-4192]
   * RelMdColumnOrigins get the wrong index of group by columns after RelNode
   * was optimized by AggregateProjectMergeRule rule</a>. */
  @Test void testColumnOriginAfterAggProjectMergeRule() {
    final String sql = "select count(ename), SAL from emp group by SAL";
    final RelMetadataFixture fixture = sql(sql);
    final RelNode rel = fixture.toRel();
    final HepProgramBuilder programBuilder = HepProgram.builder();
    programBuilder.addRuleInstance(CoreRules.AGGREGATE_PROJECT_MERGE);
    final HepPlanner planner = new HepPlanner(programBuilder.build());
    planner.setRoot(rel);
    final RelNode optimizedRel = planner.findBestExp();

    final RelMetadataFixture.MetadataConfig metadataConfig =
        fixture.metadataConfig;
    final RelMetadataQuery mq =
        new RelMetadataQuery(metadataConfig.getDefaultHandlerProvider());
    Set<RelColumnOrigin> origins = mq.getColumnOrigins(optimizedRel, 1);
    assertThat(origins, notNullValue());
    assertThat(origins, hasSize(1));

    RelColumnOrigin columnOrigin = origins.iterator().next();
    assertThat(columnOrigin.getOriginColumnOrdinal(), equalTo(5));
    assertThat(columnOrigin.getOriginTable().getRowType().getFieldNames().get(5),
        equalTo("SAL"));
  }

  // ----------------------------------------------------------------------
  // Tests for getRowCount, getMinRowCount, getMaxRowCount
  // ----------------------------------------------------------------------

  @Test void testRowCountEmp() {
    final String sql = "select * from emp";
    sql(sql)
        .assertThatRowCount(is(EMP_SIZE), is(0D), is(Double.POSITIVE_INFINITY));
  }

  @Test void testRowCountDept() {
    final String sql = "select * from dept";
    sql(sql)
        .assertThatRowCount(is(DEPT_SIZE), is(0D), is(Double.POSITIVE_INFINITY));
  }

  @Test void testRowCountValues() {
    final String sql = "select * from (values (1), (2)) as t(c)";
    sql(sql).assertThatRowCount(is(2d), is(2d), is(2d));
  }

  @Test void testRowCountCartesian() {
    final String sql = "select * from emp,dept";
    sql(sql)
        .assertThatRowCount(is(EMP_SIZE * DEPT_SIZE), is(0D),
            is(Double.POSITIVE_INFINITY));
  }

  @Test void testRowCountJoin() {
    final String sql = "select * from emp\n"
        + "inner join dept on emp.deptno = dept.deptno";
    sql(sql)
        .assertThatRowCount(is(EMP_SIZE * DEPT_SIZE * DEFAULT_EQUAL_SELECTIVITY),
            is(0D), is(Double.POSITIVE_INFINITY));
  }

  @Test void testRowCountJoinFinite() {
    final String sql = "select * from (select * from emp limit 14) as emp\n"
        + "inner join (select * from dept limit 4) as dept\n"
        + "on emp.deptno = dept.deptno";
    final double maxRowCount = 56D; // 4 * 14
    sql(sql)
        .assertThatRowCount(is(EMP_SIZE * DEPT_SIZE * DEFAULT_EQUAL_SELECTIVITY),
            is(0D), is(maxRowCount));
  }

  @Test void testRowCountJoinEmptyFinite() {
    final String sql = "select * from (select * from emp limit 0) as emp\n"
        + "inner join (select * from dept limit 4) as dept\n"
        + "on emp.deptno = dept.deptno";
    final double rowCount = 1D; // 0, rounded up to row count's minimum 1
    final double minRowCount = 0D; // 0 * 4
    sql(sql).assertThatRowCount(is(rowCount), is(minRowCount), is(0D));
  }

  @Test void testRowCountLeftJoinEmptyFinite() {
    final String sql = "select * from (select * from emp limit 0) as emp\n"
        + "left join (select * from dept limit 4) as dept\n"
        + "on emp.deptno = dept.deptno";
    final double rowCount = 1D; // 0, rounded up to row count's minimum 1
    final double minRowCount = 0D; // 0 * 4
    sql(sql).assertThatRowCount(is(rowCount), is(minRowCount), is(0D));
  }

  @Test void testRowCountRightJoinEmptyFinite() {
    final String sql = "select * from (select * from emp limit 0) as emp\n"
        + "right join (select * from dept limit 4) as dept\n"
        + "on emp.deptno = dept.deptno";
    sql(sql).assertThatRowCount(is(4D), is(0D), is(4D));
  }

  @Test void testRowCountJoinFiniteEmpty() {
    final String sql = "select * from (select * from emp limit 7) as emp\n"
        + "inner join (select * from dept limit 0) as dept\n"
        + "on emp.deptno = dept.deptno";
    final double rowCount = 1D; // 0, rounded up to row count's minimum 1
    final double minRowCount = 0D; // 7 * 0
    sql(sql).assertThatRowCount(is(rowCount), is(minRowCount), is(0D));
  }

  @Test void testRowCountLeftJoinFiniteEmpty() {
    final String sql = "select * from (select * from emp limit 4) as emp\n"
        + "left join (select * from dept limit 0) as dept\n"
        + "on emp.deptno = dept.deptno";
    sql(sql).assertThatRowCount(is(4D), is(0D), is(4D));
  }

  @Test void testRowCountRightJoinFiniteEmpty() {
    final String sql = "select * from (select * from emp limit 4) as emp\n"
        + "right join (select * from dept limit 0) as dept\n"
        + "on emp.deptno = dept.deptno";
    final double rowCount = 1D; // 0, rounded up to row count's minimum 1
    final double minRowCount = 0D; // 0 * 4
    sql(sql).assertThatRowCount(is(rowCount), is(minRowCount), is(0D));
  }

  @Test void testRowCountJoinEmptyEmpty() {
    final String sql = "select * from (select * from emp limit 0) as emp\n"
        + "inner join (select * from dept limit 0) as dept\n"
        + "on emp.deptno = dept.deptno";
    final double rowCount = 1D; // 0, rounded up to row count's minimum 1
    final double minRowCount = 0D; // 0 * 0
    sql(sql).assertThatRowCount(is(rowCount), is(minRowCount), is(0D));
  }

  @Test void testRowCountUnion() {
    final String sql = "select ename from emp\n"
        + "union all\n"
        + "select name from dept";
    sql(sql).assertThatRowCount(is(EMP_SIZE + DEPT_SIZE),
        is(0D), is(Double.POSITIVE_INFINITY));
  }

  @Test void testRowCountUnionOnFinite() {
    final String sql = "select ename from (select * from emp limit 100)\n"
        + "union all\n"
        + "select name from (select * from dept limit 40)";
    sql(sql).assertThatRowCount(is(EMP_SIZE + DEPT_SIZE), is(0D), is(140D));
  }

  @Test void testRowCountUnionDistinct() {
    String sql = "select x from (values 'a', 'b') as t(x)\n"
        + "union\n"
        + "select x from (values 'a', 'b') as t(x)";
    sql(sql).assertThatRowCount(is(2D), is(1D), is(4D));

    sql = "select x from (values 'a', 'a') as t(x)\n"
        + "union\n"
        + "select x from (values 'a', 'a') as t(x)";
    sql(sql).assertThatRowCount(is(2D), is(1D), is(4D));
  }

  @Test void testRowCountIntersectOnFinite() {
    final String sql = "select ename from (select * from emp limit 100)\n"
        + "intersect\n"
        + "select name from (select * from dept limit 40)";
    sql(sql)
        .assertThatRowCount(is(Math.min(EMP_SIZE, DEPT_SIZE)), is(0D), is(40D));
  }

  @Test void testRowCountMinusOnFinite() {
    final String sql = "select ename from (select * from emp limit 100)\n"
        + "except\n"
        + "select name from (select * from dept limit 40)";
    sql(sql).assertThatRowCount(is(4D), is(0D), is(100D));
  }

  @Test void testRowCountFilter() {
    final String sql = "select * from emp where ename='Mathilda'";
    sql(sql)
        .assertThatRowCount(is(EMP_SIZE * DEFAULT_EQUAL_SELECTIVITY),
            is(0D), is(Double.POSITIVE_INFINITY));
  }

  @Test void testRowCountFilterOnFinite() {
    final String sql = "select * from (select * from emp limit 10)\n"
        + "where ename='Mathilda'";
    sql(sql)
        .assertThatRowCount(is(10D * DEFAULT_EQUAL_SELECTIVITY),
            is(0D), is(10D));
  }

  @Test void testRowCountFilterFalse() {
    final String sql = "select * from (values 'a', 'b') as t(x) where false";
    sql(sql).assertThatRowCount(is(1D), is(0D), is(0D));
  }

  @Test void testRowCountSort() {
    final String sql = "select * from emp order by ename";
    sql(sql)
        .assertThatRowCount(is(EMP_SIZE), is(0D), is(Double.POSITIVE_INFINITY));
  }

  @Test void testRowCountExchange() {
    final String sql = "select * from emp order by ename limit 123456";
    sql(sql)
        .withRelTransform(rel ->
            LogicalExchange.create(rel,
                RelDistributions.hash(ImmutableList.<Integer>of())))
        .assertThatRowCount(is(EMP_SIZE), is(0D), is(123456D));
  }

  @Test void testRowCountTableModify() {
    final String sql = "insert into emp select * from emp order by ename limit 123456";
    final RelMetadataFixture fixture = sql(sql);
    fixture.assertThatRowCount(is(EMP_SIZE), is(0D), is(123456D));
  }

  @Test void testRowCountSortHighLimit() {
    final String sql = "select * from emp order by ename limit 123456";
    final RelMetadataFixture fixture = sql(sql);
    fixture.assertThatRowCount(is(EMP_SIZE), is(0D), is(123456D));
  }

  @Test void testRowCountSortHighOffset() {
    final String sql = "select * from emp order by ename offset 123456";
    final RelMetadataFixture fixture = sql(sql);
    fixture.assertThatRowCount(is(1D), is(0D), is(Double.POSITIVE_INFINITY));
  }

  @Test void testRowCountSortHighOffsetLimit() {
    final String sql = "select * from emp order by ename limit 5 offset 123456";
    final RelMetadataFixture fixture = sql(sql);
    fixture.assertThatRowCount(is(1D), is(0D), is(5D));
  }

  @Test void testRowCountSortLimit() {
    final String sql = "select * from emp order by ename limit 10";
    final RelMetadataFixture fixture = sql(sql);
    fixture.assertThatRowCount(is(10d), is(0D), is(10d));
  }

  @Test void testRowCountSortLimit0() {
    final String sql = "select * from emp order by ename limit 0";
    final RelMetadataFixture fixture = sql(sql);
    fixture.assertThatRowCount(is(1d), is(0D), is(0d));
  }

  @Test void testRowCountSortLimitOffset() {
    final String sql = "select * from emp order by ename limit 10 offset 5";
    /* 14 - 5 */
    final RelMetadataFixture fixture = sql(sql);
    fixture.assertThatRowCount(is(9D), is(0D), is(10d));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5286">[CALCITE-5286]
   * Join with parameterized LIMIT throws AssertionError "not a literal". </a>. */
  @Test void testRowCountJoinWithDynamicParameters() {
    final String sql = "select r.ename, s.sal from\n"
        + "(select * from emp limit ?) r join bonus s\n"
        + "on r.ename=s.ename where r.sal+1=s.sal";
    sql(sql)
        .withCluster(cluster -> {
          RelOptPlanner planner = new VolcanoPlanner();
          planner.addRule(EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE);
          planner.addRule(EnumerableRules.ENUMERABLE_PROJECT_RULE);
          planner.addRule(EnumerableRules.ENUMERABLE_FILTER_RULE);
          planner.addRule(EnumerableRules.ENUMERABLE_JOIN_RULE);
          planner.addRule(EnumerableRules.ENUMERABLE_LIMIT_SORT_RULE);
          planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
          return RelOptCluster.create(planner, cluster.getRexBuilder());
        })
        .withRelTransform(rel -> {
          RelOptPlanner planner = rel.getCluster().getPlanner();
          planner.setRoot(rel);
          RelTraitSet requiredOutputTraits =
              rel.getCluster().traitSet().replace(EnumerableConvention.INSTANCE);
          final RelNode rootRel2 = planner.changeTraits(rel, requiredOutputTraits);

          planner.setRoot(rootRel2);
          final RelOptPlanner planner2 = planner.chooseDelegate();
          final RelNode rootRel3 = planner2.findBestExp();
          return rootRel3;
        })
        .assertThatRowCount(is(1.0), is(0D), is(Double.POSITIVE_INFINITY));
  }

  @Test void testRowCountSortLimitOffsetDynamic() {
    sql("select * from emp order by ename limit ? offset ?")
        .assertThatRowCount(is(EMP_SIZE), is(0D), is(Double.POSITIVE_INFINITY));
    sql("select * from emp order by ename limit 1 offset ?")
        .assertThatRowCount(is(1D), is(0D), is(1D));
    sql("select * from emp order by ename limit ? offset 1")
        .assertThatRowCount(is(EMP_SIZE - 1), is(0D), is(Double.POSITIVE_INFINITY));
  }

  @Test void testRowCountSortLimitOffsetOnFinite() {
    final String sql = "select * from (select * from emp limit 12)\n"
        + "order by ename limit 20 offset 5";
    sql(sql).assertThatRowCount(is(7d), is(0D), is(7d));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5944">[CALCITE-5944]
   * Add metadata for Sample</a>. */
  @Test void testRowCountSample() {
    sql("select * from emp tablesample bernoulli(50) repeatable(1)")
        .assertThatRowCount(is(EMP_SIZE * 0.5), is(0D), is(Double.POSITIVE_INFINITY));
    sql("select * from emp tablesample system(20)")
        .assertThatRowCount(is(EMP_SIZE * 0.2), is(0D), is(Double.POSITIVE_INFINITY));
  }

  @Test void testRowCountAggregate() {
    final String sql = "select deptno from emp group by deptno";
    sql(sql).assertThatRowCount(is(1.4D), is(0D), is(Double.POSITIVE_INFINITY));
  }

  @Test void testRowCountAggregateGroupingSets() {
    final String sql = "select deptno from emp\n"
        + "group by grouping sets ((deptno), (ename, deptno))";
    final double rowCount = 2.8D; // EMP_SIZE / 10 * 2
    sql(sql)
        .assertThatRowCount(is(rowCount), is(0D), is(Double.POSITIVE_INFINITY));
  }

  @Test void testRowCountAggregateGroupingSetsOneEmpty() {
    final String sql = "select deptno from emp\n"
        + "group by grouping sets ((deptno), ())";
    sql(sql).assertThatRowCount(is(2.8D), is(0D), is(Double.POSITIVE_INFINITY));
  }

  @Test void testRowCountAggregateEmptyKey() {
    final String sql = "select count(*) from emp";
    sql(sql).assertThatRowCount(is(1D), is(1D), is(1D));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5050">[CALCITE-5050]
   * Aggregate with no GROUP BY always returns 1 row. </a>. */
  @Test void testRowCountAggregateEmptyGroupKey() {
    fixture()
        .withRelFn(b ->
            b.scan("EMP")
                .aggregate(
                    b.groupKey(),
                    b.count(false, "C"))
                .build())
        .assertThatRowCount(is(1D), is(1D), is(1D));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5050">[CALCITE-5050]
   * Aggregate with no GROUP BY always returns 1 row (even on empty table). </a>. */
  @Test void testRowCountAggregateEmptyGroupKeyWithEmptyTable() {
    fixture()
        .withRelFn(b ->
            b.scan("EMP")
                .filter(b.literal(false))
                .aggregate(
                    b.groupKey(),
                    b.count(false, "C"))
                .build())
        .assertThatRowCount(is(1D), is(1D), is(1D));
  }

  @Test void testRowCountAggregateConstantKey() {
    final String sql = "select count(*) from emp where deptno=2 and ename='emp1' "
        + "group by deptno, ename";
    sql(sql).assertThatRowCount(is(1D), is(0D), is(1D));
  }

  @Test void testRowCountAggregateConstantKeys() {
    final String sql = "select distinct deptno from emp where deptno=4";
    sql(sql).assertThatRowCount(is(1D), is(0D), is(1D));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6474">[CALCITE-6474]
   * Aggregate with constant key can get a RowCount greater than its MaxRowCount </a>. */
  @Test void testRowCountAggregateConstantKeysOnBigInput() {
    final String sql = ""
        + "select distinct deptno from ("
        + "select deptno from emp e1 union all "
        + "select deptno from emp e2 union all "
        + "select deptno from emp e3 union all "
        + "select deptno from emp e4 union all "
        + "select deptno from emp e5 union all "
        + "select deptno from emp e6 union all "
        + "select deptno from emp e7"
        + ") where deptno=4";
    sql(sql).assertThatRowCount(is(1D), is(0D), is(1D));
  }

  @Test void testRowCountFilterAggregateEmptyKey() {
    final String sql = "select count(*) from emp where 1 = 0";
    sql(sql).assertThatRowCount(is(1D), is(1D), is(1D));
  }

  @Test void testRowCountAggregateEmptyKeyOnEmptyTable() {
    final String sql = "select count(*) from (select * from emp limit 0)";
    sql(sql).assertThatRowCount(is(1D), is(1D), is(1D));
  }

  // ----------------------------------------------------------------------
  // Tests for computeSelfCost.cpu
  // ----------------------------------------------------------------------

  @Test void testSortCpuCostOffsetLimit() {
    final String sql = "select ename, deptno from emp\n"
        + "order by ename limit 5 offset 5";
    // inputRows = EMP_SIZE = 14
    // offset + fetch = 5 + 5 = 10
    // rowBytes = (2 real columns + 3 virtual columns) * 4 bytes per column
    //   = 5 * 4
    //   = 20
    double cpuCost = Util.nLogM(EMP_SIZE, 10) * 5 * 4;
    sql(sql).assertCpuCost(is(cpuCost), "offset + fetch smaller than table size "
        + "=> cpu cost should be: inputRows * log(offset + fetch) * rowBytes");
  }

  @Test void testSortCpuCostLimit() {
    final String sql = "select ename, deptno from emp limit 10";
    final double cpuCost = 10 * 5 * 4;
    sql(sql).assertCpuCost(is(cpuCost), "no order by clause "
        + "=> cpu cost should be min(fetch + offset, inputRows) * rowBytes");
  }

  @Test void testSortCpuCostOffset() {
    final String sql = "select ename from emp order by ename offset 10";
    double cpuCost = Util.nLogM(EMP_SIZE, EMP_SIZE) * 4 * 4;
    sql(sql).assertCpuCost(is(cpuCost), "offset smaller than table size "
        + "=> cpu cost should be: inputRows * log(inputRows) * rowBytes");
  }

  @Test void testSortCpuCostLargeOffset() {
    final String sql = "select ename from emp order by ename offset 100";
    double cpuCost = Util.nLogM(EMP_SIZE, EMP_SIZE) * 4 * 4;
    sql(sql).assertCpuCost(is(cpuCost), "offset larger than table size "
        + "=> cpu cost should be: inputRows * log(inputRows) * rowBytes");
  }

  @Test void testSortCpuCostLimit0() {
    final String sql = "select ename from emp order by ename limit 0";
    sql(sql).assertCpuCost(is(0d), "fetch zero => cpu cost should be 0");
  }

  @Test void testSortCpuCostLimit1() {
    final String sql = "select ename, deptno from emp\n"
        + "order by ename limit 1";
    double cpuCost = EMP_SIZE * 5 * 4;
    sql(sql).assertCpuCost(is(cpuCost), "fetch 1 "
        + "=> cpu cost should be inputRows * rowBytes");
  }

  @Test void testSortCpuCostLargeLimit() {
    final String sql = "select ename, deptno from emp\n"
        + "order by ename limit 10000";
    double cpuCost = Util.nLogM(EMP_SIZE, EMP_SIZE) * 5 * 4;
    sql(sql).assertCpuCost(is(cpuCost), "sort limit exceeds table size "
        + "=> cpu cost should be dominated by table size");
  }

  // ----------------------------------------------------------------------
  // Tests for getSelectivity
  // ----------------------------------------------------------------------

  @Test void testSelectivityIsNotNullFilter() {
    sql("select * from emp where mgr is not null")
        .assertThatSelectivity(isAlmost(DEFAULT_NOTNULL_SELECTIVITY));
  }

  @Test void testSelectivityIsNotNullFilterOnNotNullColumn() {
    sql("select * from emp where deptno is not null")
        .assertThatSelectivity(isAlmost(1.0d));
  }

  @Test void testSelectivityComparisonFilter() {
    sql("select * from emp where deptno > 10")
        .assertThatSelectivity(isAlmost(DEFAULT_COMP_SELECTIVITY));
  }

  @Test void testSelectivityAndFilter() {
    sql("select * from emp where ename = 'foo' and deptno = 10")
        .assertThatSelectivity(isAlmost(DEFAULT_EQUAL_SELECTIVITY_SQUARED));
  }

  @Test void testSelectivityOrFilter() {
    sql("select * from emp where ename = 'foo' or deptno = 10")
        .assertThatSelectivity(isAlmost(DEFAULT_SELECTIVITY));
  }

  @Test void testSelectivityJoin() {
    sql("select * from emp join dept using (deptno) where ename = 'foo'")
        .assertThatSelectivity(isAlmost(DEFAULT_EQUAL_SELECTIVITY));
  }

  @Test void testSelectivityRedundantFilter() {
    sql("select * from emp where deptno = 10")
        .assertThatSelectivity(isAlmost(DEFAULT_EQUAL_SELECTIVITY));
  }

  @Test void testSelectivitySort() {
    sql("select * from emp where deptno = 10\n"
        + "order by ename")
        .assertThatSelectivity(isAlmost(DEFAULT_EQUAL_SELECTIVITY));
  }

  @Test void testSelectivityUnion() {
    sql("select * from (\n"
        + "  select * from emp union all select * from emp)\n"
        + "where deptno = 10")
        .assertThatSelectivity(isAlmost(DEFAULT_EQUAL_SELECTIVITY));
  }

  @Test void testSelectivityAgg() {
    sql("select deptno, count(*) from emp where deptno > 10 "
        + "group by deptno having count(*) = 0")
        .assertThatSelectivity(
            isAlmost(DEFAULT_COMP_SELECTIVITY * DEFAULT_EQUAL_SELECTIVITY));
  }

  /** Checks that we can cache a metadata request that includes a null
   * argument. */
  @Test void testSelectivityAggCached() {
    sql("select deptno, count(*) from emp where deptno > 10\n"
        + "group by deptno having count(*) = 0")
        .assertThatSelectivity(
            isAlmost(DEFAULT_COMP_SELECTIVITY * DEFAULT_EQUAL_SELECTIVITY));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1808">[CALCITE-1808]
   * JaninoRelMetadataProvider loading cache might cause
   * OutOfMemoryError</a>.
   *
   * <p>Too slow to run every day, and it does not reproduce the issue. */
  @Tag("slow")
  @Test void testMetadataHandlerCacheLimit() {
    assumeTrue(CalciteSystemProperty.METADATA_HANDLER_CACHE_MAXIMUM_SIZE.value() < 10_000,
        "If cache size is too large, this test may fail and the test won't be to blame");
    final int iterationCount = 2_000;
    final RelNode rel = sql("select * from emp").toRel();
    final RelMetadataProvider metadataProvider =
        rel.getCluster().getMetadataProvider();
    assertThat(metadataProvider, notNullValue());
    for (int i = 0; i < iterationCount; i++) {
      RelMetadataProvider wrappedProvider = new RelMetadataProvider() {
        @Deprecated // to be removed before 2.0
        @Override public @Nullable <M extends @Nullable Metadata> UnboundMetadata<M> apply(
            Class<? extends RelNode> relClass, Class<? extends M> metadataClass) {
          return metadataProvider.apply(relClass, metadataClass);
        }

        @Deprecated // to be removed before 2.0
        @Override public <M extends Metadata> Multimap<Method, MetadataHandler<M>> handlers(
            MetadataDef<M> def) {
          return metadataProvider.handlers(def);
        }

        @Override public List<MetadataHandler<?>> handlers(
            Class<? extends MetadataHandler<?>> handlerClass) {
          return metadataProvider.handlers(handlerClass);
        }
      };
      RelMetadataQuery.THREAD_PROVIDERS.set(JaninoRelMetadataProvider.of(wrappedProvider));
      final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
      final Double result = mq.getRowCount(rel);
      assertThat(result, closeTo(14d, 0.1d));
    }
  }

  @Test void testDistinctRowCountTable() {
    // no unique key information is available so return null
    final String sql = "select * from (values "
        + "(1, 2, 3, null), "
        + "(3, 4, 5, 6), "
        + "(3, 4, null, 6), "
        + "(8, 4, 5, null) "
        + ") t(c1, c2, c3, c4)";
    sql(sql)
        // all rows are different
        .assertThatDistinctRowCount(bitSetOf(0, 1, 2, 3), is(4D))
        // rows 2 and 4 are the same in the specified columns
        .assertThatDistinctRowCount(bitSetOf(1, 2), is(3D))
        // rows 2 and 3 are the same in the specified columns
        .assertThatDistinctRowCount(bitSetOf(0), is(3D))
        // the last column has 2 distinct values: 6 and null
        .assertThatDistinctRowCount(bitSetOf(3), is(2D));
  }

  @Test void testDistinctRowCountValues() {
    sql("select * from emp where deptno = 10")
        .assertThatDistinctRowCount(
            rel -> bitSetOf(rel.getRowType().getFieldNames().indexOf("DEPTNO")),
            nullValue(Double.class));
  }

  @Test void testDistinctRowCountTableEmptyKey() {
    sql("select * from emp where deptno = 10")
        .assertThatDistinctRowCount(bitSetOf(), // empty key
            is(1D));
  }

  // ----------------------------------------------------------------------
  // Tests for getUniqueKeys
  // ----------------------------------------------------------------------


  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-509">[CALCITE-509]
   * "RelMdColumnUniqueness uses ImmutableBitSet.Builder twice, gets
   * NullPointerException"</a>. */
  @Test void testJoinUniqueKeys() {
    sql("select * from emp join bonus using (ename)")
        .assertThatUniqueKeysAre(); // no unique keys
  }

  @Test void testCorrelateUniqueKeys() {
    final String sql = "select *\n"
        + "from (select distinct deptno from emp) as e,\n"
        + "  lateral (\n"
        + "    select * from dept where dept.deptno = e.deptno)";
    sql(sql)
        .assertThatRel(is(instanceOf(Project.class)))
        .assertThatUniqueKeys(sortsAs("[{0}]"))
        .withRelTransform(r -> ((Project) r).getInput())
        .assertThatRel(is(instanceOf(Correlate.class)))
        .assertThatUniqueKeys(sortsAs("[{0}]"));
  }

  @Test void testGroupByEmptyUniqueKeys() {
    sql("select count(*) from emp")
        .assertThatUniqueKeysAre(bitSetOf());
  }

  @Test void testGroupByEmptyHavingUniqueKeys() {
    sql("select count(*) from emp where 1 = 1")
        .assertThatUniqueKeysAre(bitSetOf());
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5162">[CALCITE-5162]
   * RelMdUniqueKeys can return more precise unique keys for Aggregate</a>. */
  @Test void testGroupByPreciseUniqueKeys() {
    sql("select empno, ename from emp group by empno, ename")
        .assertThatUniqueKeysAre(bitSetOf(0));
  }

  @Test void testFullOuterJoinUniqueness1() {
    final String sql = "select e.empno, d.deptno\n"
        + "from (select cast(null as int) empno from sales.emp "
        + " where empno = 10 group by cast(null as int)) as e\n"
        + "full outer join (select cast (null as int) deptno from sales.dept "
        + "group by cast(null as int)) as d on e.empno = d.deptno\n"
        + "group by e.empno, d.deptno";
    sql(sql)
        .assertThatAreColumnsUnique(r ->
                ImmutableBitSet.range(0, r.getRowType().getFieldCount()),
            r -> r.getInput(0),
            is(false));
  }

  @Test void testColumnUniquenessForFilterWithConstantColumns() {
    checkColumnUniquenessForFilterWithConstantColumns(""
        + "select *\n"
        + "from (select distinct deptno, sal from emp)\n"
        + "where sal=1000");
    checkColumnUniquenessForFilterWithConstantColumns(""
        + "select *\n"
        + "from (select distinct deptno, sal from emp)\n"
        + "where 1000=sal");
  }

  private void checkColumnUniquenessForFilterWithConstantColumns(String sql) {
    sql(sql)
        .assertThatRel(hasFieldNames("[DEPTNO, SAL]"))
        .assertThatAreColumnsUnique(bitSetOf(0, 1), is(true))
        .assertThatAreColumnsUnique(bitSetOf(0), is(true))
        .assertThatAreColumnsUnique(bitSetOf(1), is(false));
  }

  @Test void testColumnUniquenessForUnionWithConstantColumns() {
    final String sql = ""
        + "select deptno, sal from emp where sal=1000\n"
        + "union\n"
        + "select deptno, sal from emp where sal=1000\n";
    sql(sql)
        .assertThatRel(hasFieldNames("[DEPTNO, SAL]"))
        .assertThatAreColumnsUnique(bitSetOf(0), is(true));
  }

  @Test void testColumnUniquenessForIntersectWithConstantColumns() {
    final String sql = ""
        + "select deptno, sal\n"
        + "from (select distinct deptno, sal from emp)\n"
        + "where sal=1000\n"
        + "intersect all\n"
        + "select deptno, sal from emp\n";
    sql(sql)
        .assertThatRel(hasFieldNames("[DEPTNO, SAL]"))
        .assertThatAreColumnsUnique(bitSetOf(0, 1), is(true));
  }

  @Test void testColumnUniquenessForMinusWithConstantColumns() {
    final String sql = ""
        + "select deptno, sal\n"
        + "from (select distinct deptno, sal from emp)\n"
        + "where sal=1000\n"
        + "except all\n"
        + "select deptno, sal from emp\n";
    sql(sql)
        .assertThatRel(hasFieldNames("[DEPTNO, SAL]"))
        .assertThatAreColumnsUnique(bitSetOf(0), is(true))
        .assertThatAreColumnsUnique(bitSetOf(0, 1), is(true));
  }

  @Test void testColumnUniquenessForSortWithConstantColumns() {
    final String sql = ""
        + "select *\n"
        + "from (select distinct deptno, sal from emp)\n"
        + "where sal=1000\n"
        + "order by deptno";
    sql(sql)
        .assertThatRel(hasFieldNames("[DEPTNO, SAL]"))
        .assertThatAreColumnsUnique(bitSetOf(0, 1), is(true));
  }

  @Test void testRowUniquenessForSortWithLimit() {
    final String sql = "select sal\n"
        + "from emp\n"
        + "limit 1";
    sql(sql)
        .assertThatAreRowsUnique(is(true));
  }

  @Test void testColumnUniquenessForJoinWithConstantColumns() {
    final String sql = ""
        + "select *\n"
        + "from (select distinct deptno, sal from emp) A\n"
        + "join (select distinct deptno, sal from emp) B\n"
        + "on A.deptno=B.deptno and A.sal=1000 and B.sal=1000";
    sql(sql)
        .assertThatRel(hasFieldNames("[DEPTNO, SAL, DEPTNO0, SAL0]"))
        .assertThatAreColumnsUnique(bitSetOf(0, 2), is(true))
        .assertThatAreColumnsUnique(bitSetOf(0, 1, 2), is(true))
        .assertThatAreColumnsUnique(bitSetOf(0, 2, 3), is(true))
        .assertThatAreColumnsUnique(bitSetOf(0, 1), is(false));
  }

  @Test void testColumnUniquenessForLimit1() {
    final String sql = ""
        + "select *\n"
        + "from emp\n"
        + "limit 1";
    sql(sql)
        .assertThatAreColumnsUnique(bitSetOf(0), is(true))
        .assertThatAreColumnsUnique(bitSetOf(1), is(true))
        .assertThatAreColumnsUnique(bitSetOf(), is(true))
        .assertThatUniqueKeysAre(bitSetOf());
  }

  @Test void testColumnUniquenessForJoinOnLimit1() {
    final String sql = ""
        + "select *\n"
        + "from emp A\n"
        + "join (\n"
        + "  select * from emp\n"
        + "  limit 1) B\n"
        + "on A.empno = B.empno";
    sql(sql)
        .assertThatAreColumnsUnique(bitSetOf(0), is(true))
        .assertThatAreColumnsUnique(bitSetOf(1), is(true))
        .assertThatAreColumnsUnique(bitSetOf(9), is(true))
        .assertThatAreColumnsUnique(bitSetOf(10), is(true))
        .assertThatAreColumnsUnique(bitSetOf(), is(true))
        .assertThatUniqueKeysAre(bitSetOf());
  }

  @Test void testColumnUniquenessForJoinOnAggregation() {
    final String sql = ""
        + "select *\n"
        + "from emp A\n"
        + "join (\n"
        + "  select max(empno) AS maxno from emp) B\n"
        + "on A.empno = B.maxno";
    sql(sql)
        .assertThatAreColumnsUnique(bitSetOf(0), is(true))
        .assertThatAreColumnsUnique(bitSetOf(9), is(true))
        .assertThatAreColumnsUnique(bitSetOf(1), is(true))
        .assertThatAreColumnsUnique(bitSetOf(1, 9), is(true))
        .assertThatAreColumnsUnique(bitSetOf(), is(true))
        .assertThatUniqueKeysAre(bitSetOf());
  }

  @Test void testColumnUniquenessForConstantKey() {
    final String sql = ""
        + "select *\n"
        + "from emp A\n"
        + "where empno = 1010";
    sql(sql)
        .assertThatAreColumnsUnique(bitSetOf(0), is(true))
        .assertThatAreColumnsUnique(bitSetOf(1), is(true))
        .assertThatAreColumnsUnique(bitSetOf(), is(true))
        .assertThatUniqueKeysAre(bitSetOf());
  }

  @Test void testColumnUniquenessForCorrelatedSubquery() {
    final String sql = ""
        + "select *\n"
        + "from emp A\n"
        + "where empno = (\n"
        + "  select max(empno) from emp)";
    sql(sql)
        .assertThatAreColumnsUnique(bitSetOf(0), is(true))
        .assertThatAreColumnsUnique(bitSetOf(1), is(true))
        .assertThatAreColumnsUnique(bitSetOf(), is(true))
        .assertThatUniqueKeysAre(bitSetOf());
  }

  @Test void testColumnUniquenessForSubqueryWithCorrelatingVars() {
    final String sql = ""
        + "select empno, deptno, slacker\n"
        + "from emp A\n"
        + "where empno = (\n"
        + "  select max(empno)\n"
        + "  from emp B\n"
        + "  where A.deptno = B.deptno\n"
        + ")";
    sql(sql)
        .assertThatAreColumnsUnique(bitSetOf(0), is(true))
        // This requires drilling into the subquery
//        .assertThatAreColumnsUnique(bitSetOf(1), is(true))
        .assertThatAreColumnsUnique(bitSetOf(2), is(false))
        .assertThatAreColumnsUnique(bitSetOf(), is(false))
        .assertThatUniqueKeysAre(bitSetOf(0));
  }

  @Test void testColumnUniquenessForAggregateWithConstantColumns() {
    final String sql = ""
        + "select deptno, ename, sum(sal)\n"
        + "from emp\n"
        + "where deptno=1010\n"
        + "group by deptno, ename";
    sql(sql)
        .assertThatAreColumnsUnique(bitSetOf(0, 1), is(true))
        .assertThatAreColumnsUnique(bitSetOf(0), is(false))
        .assertThatAreColumnsUnique(bitSetOf(1), is(true))
        .assertThatAreColumnsUnique(bitSetOf(), is(false))
        .assertThatUniqueKeysAre(bitSetOf(1));
  }

  @Test void testColumnUniquenessForExchangeWithConstantColumns() {
    fixture()
        .withRelFn(b ->
            b.scan("EMP")
                .project(b.field("DEPTNO"), b.field("SAL"))
                .distinct()
                .filter(b.equals(b.field("SAL"), b.literal(1)))
                .exchange(RelDistributions.hash(ImmutableList.of(1)))
                .build())
        .assertThatAreColumnsUnique(bitSetOf(0), is(true));
  }

  @Test void testColumnUniquenessForCorrelateWithConstantColumns() {
    fixture()
        .withRelFn(b -> {
          RelNode rel0 = b.scan("EMP")
              .project(b.field("DEPTNO"), b.field("SAL"))
              .distinct()
              .filter(b.equals(b.field("SAL"), b.literal(1)))
              .build();
          final Holder<@Nullable RexCorrelVariable> v = Holder.empty();
          final RelNode rel1 = b.scan("EMP")
              .variable(v::set)
              .project(b.field("DEPTNO"), b.field("SAL"))
              .filter(
                  b.equals(b.field(0), b.field(v.get(), "DEPTNO")))
              .build();
          return b.push(rel0)
              .variable(v::set)
              .push(rel1)
              .correlate(JoinRelType.SEMI, v.get().id, b.field(2, 0, "DEPTNO"))
              .build();
        })
        .assertThatAreColumnsUnique(bitSetOf(0), is(true));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5149">[CALCITE-5149]
   * Refine RelMdColumnUniqueness for Aggregate by considering intersect keys
   * between target keys and group keys</a>. */
  @Test void testColumnUniquenessForAggregate() {
    sql("select empno, ename, count(1) as cnt from emp group by empno, ename")
        .assertThatAreColumnsUnique(bitSetOf(0, 1), is(true));

    sql("select empno, ename, count(1) as cnt from emp group by empno, ename")
        .assertThatAreColumnsUnique(bitSetOf(0), is(true));

    sql("select ename, empno, count(1) as cnt from emp group by ename, empno")
        .assertThatAreColumnsUnique(bitSetOf(1), is(true));

    sql("select empno, ename, count(1) as cnt from emp group by empno, ename")
        .assertThatAreColumnsUnique(bitSetOf(2), is(false));
  }

  @Test void testGroupBy() {
    sql("select deptno, count(*), sum(sal) from emp group by deptno")
        .assertThatUniqueKeysAre(bitSetOf(0));
  }

  /**
   * The group by columns constitute a key, and the keys of the relation we are
   * aggregating over are retained.
   */
  @Test void testGroupByNonKey() {
    sql("select sal, max(deptno), max(empno) from emp group by sal")
        .assertThatAreColumnsUnique(bitSetOf(0), is(true))
        .assertThatAreColumnsUnique(bitSetOf(1), is(false))
        .assertThatAreColumnsUnique(bitSetOf(2), is(true))
        .assertThatUniqueKeysAre(bitSetOf(0), bitSetOf(2));
  }

  /**
   * All columns are unique. Should not include () because there are multiple rows.
   */
  @Test void testGroupByNonKeyNoAggs() {
    sql("select sal from emp group by sal")
        .assertThatAreColumnsUnique(bitSetOf(), is(false))
        .assertThatAreColumnsUnique(bitSetOf(0), is(true))
        .assertThatUniqueKeysAre(bitSetOf(0));
  }

// TODO: Enable when CALCITE-6126 fixed
/*
  @Test void testOverByNonKey() {
    sql("select sal,\n"
        + "max(deptno) over (partition BY sal rows between 2 preceding and 0 following) maxDept,\n"
        + "max(empno) over (partition BY sal rows between 2 preceding and 0 following) maxEmp\n"
        + "from emp")
        .assertThatAreColumnsUnique(bitSetOf(0), is(false))
        .assertThatAreColumnsUnique(bitSetOf(1), is(false))
        .assertThatAreColumnsUnique(bitSetOf(2), is(false))
        .assertThatUniqueKeysAre();
  }
*/

// TODO: Enable when CALCITE-6126 fixed
/*
  @Test void testOverNoPartitioning() {
    sql("select max(empno) over (rows between 2 preceding and 0 following) maxEmp from emp")
        .assertThatAreColumnsUnique(bitSetOf(0), is(false))
        .assertThatUniqueKeysAre();
  }
*/

  @Test void testNoGroupBy() {
    sql("select max(sal), count(*) from emp")
        .assertThatAreColumnsUnique(bitSetOf(0), is(true))
        .assertThatAreColumnsUnique(bitSetOf(1), is(true))
        .assertThatUniqueKeysAre(bitSetOf());
  }

  @Test void testGroupByNothing() {
    sql("select max(sal), count(*) from emp group by ()")
        .assertThatAreColumnsUnique(bitSetOf(0), is(true))
        .assertThatAreColumnsUnique(bitSetOf(1), is(true))
        .assertThatUniqueKeysAre(bitSetOf());
  }

  @Test void testGroupingSets() {
    sql("select deptno, sal, count(*) from emp\n"
        + "group by GROUPING SETS (deptno, sal)")
        .assertThatUniqueKeysAre();
  }

  @Test void testUnion() {
    sql("select deptno from emp\n"
        + "union\n"
        + "select deptno from dept")
        .assertThatUniqueKeysAre(bitSetOf(0));
  }

  @Test void testUniqueKeysMinus() {
    sql("select distinct deptno from emp\n"
        + "except all\n"
        + "select deptno from dept")
        .assertThatUniqueKeysAre(bitSetOf(0));
  }

  @Test void testUniqueKeysIntersect() {
    sql("select distinct deptno from emp\n"
        + "intersect all\n"
        + "select deptno from dept")
        .assertThatUniqueKeysAre(bitSetOf(0));
  }

  @Test void testSingleKeyTableScanUniqueKeys() {
    // select key column
    sql("select empno, ename from emp")
        .assertThatUniqueKeysAre(bitSetOf(0));

    // select non key column
    sql("select ename, deptno from emp")
        .assertThatUniqueKeysAre();
  }

  @Test void testCompositeKeysTableScanUniqueKeys() {
    SqlTestFactory.CatalogReaderFactory factory = (typeFactory, caseSensitive) -> {
      CompositeKeysCatalogReader catalogReader =
          new CompositeKeysCatalogReader(typeFactory, false);
      catalogReader.init();
      return catalogReader;
    };

    // all columns, contain composite keys
    sql("select * from s.composite_keys_table")
        .withCatalogReaderFactory(factory)
        .assertThatAreColumnsUnique(bitSetOf(0), is(false))
        .assertThatAreColumnsUnique(bitSetOf(1), is(false))
        .assertThatAreColumnsUnique(bitSetOf(2), is(false))
        .assertThatAreColumnsUnique(bitSetOf(0, 1), is(true))
        .assertThatAreColumnsUnique(bitSetOf(0, 1, 2), is(true))
        .assertThatUniqueKeysAre(bitSetOf(0, 1));

    // only contain composite keys
    sql("select key1, key2 from s.composite_keys_table")
        .withCatalogReaderFactory(factory)
        .assertThatUniqueKeysAre(bitSetOf(0, 1));

    // partial column of composite keys
    sql("select key1, value1 from s.composite_keys_table")
        .withCatalogReaderFactory(factory)
        .assertThatUniqueKeysAre();

    sql("select key1, key1, key2, value1 from s.composite_keys_table")
        .withCatalogReaderFactory(factory)
        .assertThatUniqueKeysAre(bitSetOf(0, 2), bitSetOf(1, 2));
    sql("select key1, key2, key2, value1 from s.composite_keys_table")
        .withCatalogReaderFactory(factory)
        .assertThatUniqueKeysAre(bitSetOf(0, 1), bitSetOf(0, 2));

    sql("select key1, key1, key2, key2, value1 from s.composite_keys_table")
        .withCatalogReaderFactory(factory)
        .assertThatUniqueKeysAre(bitSetOf(0, 2), bitSetOf(0, 3), bitSetOf(1, 2), bitSetOf(1, 3));

    // no column of composite keys
    sql("select value1 from s.composite_keys_table")
        .withCatalogReaderFactory(factory)
        .assertThatUniqueKeysAre();

    // One key set to constant
    sql("select key1, key2, value1 from s.composite_keys_table t\n"
        + "where t.key2 = 'constant'")
        .withCatalogReaderFactory(factory)
        .assertThatAreColumnsUnique(bitSetOf(0), is(true))
        .assertThatUniqueKeysAre(bitSetOf(0));

    // One key set to a value from correlated subquery
    sql("select * from s.composite_keys_table where key2 = ("
        + "select max(key2) from s.composite_keys_table)")
        .withCatalogReaderFactory(factory)
        .assertThatAreColumnsUnique(bitSetOf(0), is(true))
        .assertThatUniqueKeysAre(bitSetOf(0));

    // One key set to table-wide aggregation in join expression
    sql("select * from s.composite_keys_table t1\n"
        + "inner join (\n"
        + "  select max(key2) max_key2 from s.composite_keys_table) t2\n"
        + "on t1.key2 = t2.max_key2")
        .withCatalogReaderFactory(factory)
        .assertThatAreColumnsUnique(bitSetOf(0), is(true))
        .assertThatUniqueKeysAre(bitSetOf(0));

    // One key set to single value by limit in join expression
    sql("select * from s.composite_keys_table t1\n"
        + "inner join (\n"
        + "  select * from s.composite_keys_table limit 1) t2\n"
        + "on t1.key2 = t2.key2")
        .withCatalogReaderFactory(factory)
        .assertThatAreColumnsUnique(bitSetOf(0), is(true))
        .assertThatUniqueKeysAre(bitSetOf(0));

    // One key set to single constant by select in join expression
    sql("select * from s.composite_keys_table t1\n"
        + "inner join (\n"
        + "  select CAST('constant' AS VARCHAR) c) t2\n"
        + "on t1.key2 = t2.c")
        .withCatalogReaderFactory(factory)
        .assertThatAreColumnsUnique(bitSetOf(0), is(true))
        .assertThatUniqueKeysAre(bitSetOf(0));

    // One key set joined with single-row constant
    sql("select * from s.composite_keys_table t1\n"
        + "inner join (\n"
        + "values (CAST('constant' AS VARCHAR))) as t2 (c)\n"
        + "on t1.key2 = t2.c")
        .withCatalogReaderFactory(factory)
        .assertThatAreColumnsUnique(bitSetOf(0), is(true))
        .assertThatUniqueKeysAre(bitSetOf(0));

    // One key set joined with multi-row constant
    sql("select * from s.composite_keys_table t1\n"
        + "inner join (\n"
        + "values (CAST('constant' AS VARCHAR)),(CAST('constant' AS VARCHAR))) as t2 (c)\n"
        + "on t1.key2 = t2.c")
        .withCatalogReaderFactory(factory)
        .assertThatAreColumnsUnique(bitSetOf(0), is(false))
        .assertThatUniqueKeysAre();
  }

  @Test void testCompositeKeysAggregationUniqueKeys() {
    SqlTestFactory.CatalogReaderFactory factory = (typeFactory, caseSensitive) -> {
      CompositeKeysCatalogReader catalogReader =
          new CompositeKeysCatalogReader(typeFactory, false);
      catalogReader.init();
      return catalogReader;
    };

    // both keys in passthrough functions, no group by (single row)
    sql("select any_value(key1), any_value(key2) from s.composite_keys_table")
        .withCatalogReaderFactory(factory)
        .assertThatAreColumnsUnique(bitSetOf(0), is(true))
        .assertThatAreColumnsUnique(bitSetOf(1), is(true))
        .assertThatAreColumnsUnique(bitSetOf(0, 1), is(true))
        .assertThatUniqueKeysAre(bitSetOf());

    // one key in mutating function, no group by (single row)
    sql("select min(key1), avg(key2) from s.composite_keys_table")
        .withCatalogReaderFactory(factory)
        .assertThatAreColumnsUnique(bitSetOf(0), is(true))
        .assertThatAreColumnsUnique(bitSetOf(1), is(true))
        .assertThatAreColumnsUnique(bitSetOf(0, 1), is(true))
        .assertThatUniqueKeysAre(bitSetOf());

    // both keys in passthrough functions, group by non-key
    sql("select value1, min(key1), max(key2) from s.composite_keys_table group by value1")
        .withCatalogReaderFactory(factory)
        .assertThatAreColumnsUnique(bitSetOf(0), is(true))
        .assertThatAreColumnsUnique(bitSetOf(1), is(false))
        .assertThatAreColumnsUnique(bitSetOf(2), is(false))
        .assertThatAreColumnsUnique(bitSetOf(0, 1), is(true))
        .assertThatAreColumnsUnique(bitSetOf(1, 2), is(true))
        .assertThatAreColumnsUnique(bitSetOf(0, 1, 2), is(true))
        .assertThatUniqueKeysAre(bitSetOf(0), bitSetOf(1, 2));

    // keys passed through multiple functions, group by non-key
    sql("select min(key1), max(key1), avg(key1), min(key2), max(key2), avg(key2), value1\n"
        + "from s.composite_keys_table group by value1")
        .withCatalogReaderFactory(factory)
        .assertThatAreColumnsUnique(bitSetOf(0), is(false))
        .assertThatAreColumnsUnique(bitSetOf(1), is(false))
        .assertThatAreColumnsUnique(bitSetOf(2), is(false))
        .assertThatAreColumnsUnique(bitSetOf(3), is(false))
        .assertThatAreColumnsUnique(bitSetOf(4), is(false))
        .assertThatAreColumnsUnique(bitSetOf(5), is(false))
        .assertThatAreColumnsUnique(bitSetOf(6), is(true)) // group by
        .assertThatAreColumnsUnique(bitSetOf(0, 1), is(false))
        .assertThatAreColumnsUnique(bitSetOf(0, 2), is(false))
        .assertThatAreColumnsUnique(bitSetOf(0, 3), is(true))
        .assertThatAreColumnsUnique(bitSetOf(0, 4), is(true))
        .assertThatAreColumnsUnique(bitSetOf(0, 5), is(false))
        .assertThatAreColumnsUnique(bitSetOf(0, 6), is(true)) // group by
        .assertThatAreColumnsUnique(bitSetOf(1, 2), is(false))
        .assertThatAreColumnsUnique(bitSetOf(1, 3), is(true))
        .assertThatAreColumnsUnique(bitSetOf(1, 4), is(true))
        .assertThatAreColumnsUnique(bitSetOf(1, 5), is(false))
        .assertThatAreColumnsUnique(bitSetOf(1, 6), is(true)) // group by
        .assertThatAreColumnsUnique(bitSetOf(2, 3), is(false))
        .assertThatAreColumnsUnique(bitSetOf(2, 4), is(false))
        .assertThatAreColumnsUnique(bitSetOf(2, 5), is(false))
        .assertThatAreColumnsUnique(bitSetOf(2, 6), is(true)) // group by
        .assertThatAreColumnsUnique(bitSetOf(3, 4), is(false))
        .assertThatAreColumnsUnique(bitSetOf(3, 5), is(false))
        .assertThatAreColumnsUnique(bitSetOf(3, 6), is(true)) // group by
        .assertThatAreColumnsUnique(bitSetOf(4, 5), is(false))
        .assertThatAreColumnsUnique(bitSetOf(4, 6), is(true)) // group by
        .assertThatAreColumnsUnique(bitSetOf(5, 6), is(true)) // group by
        .assertThatAreColumnsUnique(bitSetOf(0, 1, 2, 3), is(true))
        .assertThatAreColumnsUnique(bitSetOf(1, 3, 4, 5), is(true))
        .assertThatUniqueKeysAre(bitSetOf(0, 3), bitSetOf(0, 4), bitSetOf(1, 3), bitSetOf(1, 4),
            bitSetOf(6));

    // one key in mutating function, group by non-key
    sql("select value1, min(key1), count(key2) from s.composite_keys_table group by value1")
        .withCatalogReaderFactory(factory)
        .assertThatAreColumnsUnique(bitSetOf(0), is(true))
        .assertThatAreColumnsUnique(bitSetOf(1), is(false))
        .assertThatAreColumnsUnique(bitSetOf(2), is(false))
        .assertThatAreColumnsUnique(bitSetOf(0, 1), is(true))
        .assertThatAreColumnsUnique(bitSetOf(1, 2), is(false))
        .assertThatAreColumnsUnique(bitSetOf(0, 1, 2), is(true))
        .assertThatUniqueKeysAre(bitSetOf(0));

    // one key part of group by, one in passthrough function
    sql("select key1, min(key2), value1 from s.composite_keys_table group by key1, value1")
        .withCatalogReaderFactory(factory)
        .assertThatAreColumnsUnique(bitSetOf(0), is(false))
        .assertThatAreColumnsUnique(bitSetOf(1), is(false))
        .assertThatAreColumnsUnique(bitSetOf(2), is(false))
        .assertThatAreColumnsUnique(bitSetOf(0, 1), is(true)) // passthroughs
        .assertThatAreColumnsUnique(bitSetOf(0, 2), is(true)) // group bys
        .assertThatAreColumnsUnique(bitSetOf(1, 2), is(false))
        .assertThatAreColumnsUnique(bitSetOf(0, 1, 2), is(true))
        .assertThatUniqueKeysAre(bitSetOf(0, 1), bitSetOf(0, 2));

    // one key part of group by, one in mutating function
    sql("select key1, value1, count(key2) from s.composite_keys_table group by key1, value1")
        .withCatalogReaderFactory(factory)
        .assertThatAreColumnsUnique(bitSetOf(0), is(false))
        .assertThatAreColumnsUnique(bitSetOf(1), is(false))
        .assertThatAreColumnsUnique(bitSetOf(2), is(false))
        .assertThatAreColumnsUnique(bitSetOf(0, 1), is(true)) // group bys
        .assertThatAreColumnsUnique(bitSetOf(0, 2), is(false))
        .assertThatAreColumnsUnique(bitSetOf(1, 2), is(false))
        .assertThatAreColumnsUnique(bitSetOf(0, 1, 2), is(true))
        .assertThatUniqueKeysAre(bitSetOf(0, 1));
  }

  private static ImmutableBitSet bitSetOf(int... bits) {
    return ImmutableBitSet.of(bits);
  }

  @Test void calcColumnsAreUniqueSimpleCalc() {
    sql("select empno, empno*0 from emp")
        .convertingProjectAsCalc()
        .assertThatUniqueKeysAre(bitSetOf(0));
  }

  @Test void calcColumnsAreUniqueCalcWithFirstConstant() {
    sql("select 1, empno, empno*0 from emp")
        .convertingProjectAsCalc()
        .assertThatUniqueKeysAre(bitSetOf(1));
  }

  @Test void calcMultipleColumnsAreUniqueCalc() {
    sql("select empno, empno from emp")
        .convertingProjectAsCalc()
        .assertThatUniqueKeysAre(bitSetOf(0), bitSetOf(1));
  }

  @Test void calcMultipleColumnsAreUniqueCalc2() {
    sql("select a1.empno, a2.empno\n"
        + "from emp a1 join emp a2 on (a1.empno=a2.empno)")
        .convertingProjectAsCalc()
        .assertThatUniqueKeysAre(bitSetOf(0), bitSetOf(1));
  }

  @Test void calcMultipleColumnsAreUniqueCalc3() {
    sql("select a1.empno, a2.empno, a2.empno\n"
        + " from emp a1 join emp a2\n"
        + " on (a1.empno=a2.empno)")
        .convertingProjectAsCalc()
        .assertThatUniqueKeysAre(bitSetOf(0), bitSetOf(1), bitSetOf(2));
  }

  @Test void calcColumnsAreNonUniqueCalc() {
    sql("select empno*0 from emp")
        .convertingProjectAsCalc()
        .assertThatUniqueKeysAre();
  }

  /** Unit test for
   * {@link org.apache.calcite.rel.metadata.RelMetadataQuery#areRowsUnique(RelNode)}. */
  @Test void testRowsUnique() {
    sql("select * from emp")
        .assertRowsUnique(is(true), "table has primary key");
    sql("select deptno from emp")
        .assertRowsUnique(is(false), "table has primary key");
    sql("select empno from emp")
        .assertRowsUnique(is(true), "primary key is unique");
    sql("select empno from emp, dept")
        .assertRowsUnique(is(false), "cartesian product destroys uniqueness");
    sql("select empno from emp join dept using (deptno)")
        .assertRowsUnique(is(true),
            "many-to-one join does not destroy uniqueness");
    sql("select empno, job from emp join dept using (deptno) order by job desc")
        .assertRowsUnique(is(true),
            "project and sort does not destroy uniqueness");
    sql("select deptno from emp limit 1")
        .assertRowsUnique(is(true), "1 row table is always unique");
    sql("select distinct deptno from emp")
        .assertRowsUnique(is(true), "distinct table is always unique");
    sql("select count(*) from emp")
        .assertRowsUnique(is(true), "grand total is always unique");
    sql("select count(*) from emp group by deptno")
        .assertRowsUnique(is(false), "several depts may have same count");
    sql("select deptno, count(*) from emp group by deptno")
        .assertRowsUnique(is(true), "group by keys are unique");
    sql("select deptno, count(*) from emp group by grouping sets ((), (deptno))")
        .assertRowsUnique(true, is(true),
            "group by keys are unique and not null");
    sql("select deptno, count(*) from emp group by grouping sets ((), (deptno))")
        .assertRowsUnique(false, nullValue(Boolean.class),
            "is actually unique; TODO: deduce it");
    sql("select distinct deptno from emp join dept using (deptno)")
        .assertRowsUnique(is(true), "distinct table is always unique");
    sql("select deptno from emp union select deptno from dept")
        .assertRowsUnique(is(true), "set query is always unique");
    sql("select deptno from emp intersect select deptno from dept")
        .assertRowsUnique(is(true), "set query is always unique");
    sql("select deptno from emp except select deptno from dept")
        .assertRowsUnique(is(true), "set query is always unique");
  }

  @Test void testBrokenCustomProviderWithMetadataFactory() {
    final List<String> buf = new ArrayList<>();
    ColTypeImpl.THREAD_LIST.set(buf);

    final String sql = "select deptno, count(*) from emp where deptno > 10 "
        + "group by deptno having count(*) = 0";
    final RelMetadataFixture.MetadataConfig metadataConfig =
        fixture().metadataConfig;
    final RelMetadataFixture fixture = sql(sql)
        .withCluster(cluster -> {
          metadataConfig.applyMetadata(cluster,
              ChainedRelMetadataProvider.of(
                  ImmutableList.of(BrokenColTypeImpl.SOURCE,
                      requireNonNull(cluster.getMetadataProvider(),
                          "cluster.metadataProvider"))));
          return cluster;
        });

    final RelNode rel = fixture.toRel();
    assertThat(rel, instanceOf(LogicalFilter.class));
    final MetadataHandlerProvider defaultHandlerProvider =
        fixture.metadataConfig.getDefaultHandlerProvider();
    final MyRelMetadataQuery mq =
        new MyRelMetadataQuery(defaultHandlerProvider);

    try {
      assertThat(colType(mq, rel, 0), equalTo("DEPTNO-rel"));
      fail("expected error");
    } catch (IllegalArgumentException e) {
      final String value = "No handler for method [public abstract "
          + "java.lang.String org.apache.calcite.test.RelMetadataTest$ColType$Handler.getColType("
          + "org.apache.calcite.rel.RelNode,org.apache.calcite.rel.metadata.RelMetadataQuery,int)] "
          + "applied to argument of type [class org.apache.calcite.rel.logical.LogicalFilter]; "
          + "we recommend you create a catch-all (RelNode) handler";
      assertThat(e.getMessage(), is(value));
    }
  }

  @Test void testBrokenCustomProviderWithMetadataQuery() {
    final List<String> buf = new ArrayList<>();
    ColTypeImpl.THREAD_LIST.set(buf);

    final String sql = "select deptno, count(*) from emp where deptno > 10 "
        + "group by deptno having count(*) = 0";
    final RelMetadataFixture.MetadataConfig metadataConfig =
        fixture().metadataConfig;
    final RelMetadataFixture fixture = sql(sql)
        .withMetadataConfig(RelMetadataFixture.MetadataConfig.NOP)
        .withCluster(cluster -> {
          metadataConfig.applyMetadata(cluster,
              ChainedRelMetadataProvider.of(
                  ImmutableList.of(BrokenColTypeImpl.SOURCE,
                      requireNonNull(cluster.getMetadataProvider(),
                          "cluster.metadataProvider"))),
              MyRelMetadataQuery::new);
          return cluster;
        });

    final RelNode rel = fixture.toRel();
    assertThat(rel, instanceOf(LogicalFilter.class));
    assertThat(rel.getCluster().getMetadataQuery(),
        instanceOf(MyRelMetadataQuery.class));
    final MyRelMetadataQuery mq =
        (MyRelMetadataQuery) rel.getCluster().getMetadataQuery();

    try {
      assertThat(colType(mq, rel, 0), equalTo("DEPTNO-rel"));
      fail("expected error");
    } catch (IllegalArgumentException e) {
      final String value = "No handler for method [public abstract java.lang.String "
          + "org.apache.calcite.test.RelMetadataTest$ColType$Handler.getColType("
          + "org.apache.calcite.rel.RelNode,org.apache.calcite.rel.metadata.RelMetadataQuery,int)]"
          + " applied to argument of type [class org.apache.calcite.rel.logical.LogicalFilter];"
          + " we recommend you create a catch-all (RelNode) handler";
      assertThat(e.getMessage(), is(value));
    }
  }

  @Deprecated // to be removed before 2.0
  public String colType(RelMetadataQuery mq, RelNode rel, int column) {
    return rel.metadata(ColType.class, mq).getColType(column);
  }

  public String colType(MyRelMetadataQuery myRelMetadataQuery, RelNode rel, int column) {
    return myRelMetadataQuery.colType(rel, column);
  }

  @Deprecated // to be removed before 2.0
  @Test void testCustomProviderWithRelMetadataFactory() {
    final List<String> buf = new ArrayList<>();
    ColTypeImpl.THREAD_LIST.set(buf);

    final String sql = "select deptno, count(*) from emp where deptno > 10 "
        + "group by deptno having count(*) = 0";
    final RelMetadataFixture.MetadataConfig metadataConfig =
        fixture().metadataConfig;
    final RelMetadataFixture fixture = sql(sql)
        .withMetadataConfig(RelMetadataFixture.MetadataConfig.NOP)
        .withCluster(cluster -> {
          // Create a custom provider that includes ColType.
          // Include the same provider twice just to be devious.
          final ImmutableList<RelMetadataProvider> list =
              ImmutableList.of(ColTypeImpl.SOURCE, ColTypeImpl.SOURCE,
                  DefaultRelMetadataProvider.INSTANCE);
          metadataConfig.applyMetadata(cluster,
              ChainedRelMetadataProvider.of(list));
          return cluster;
        });
    final RelNode rel = fixture.toRel();

    // Top node is a filter. Its metadata uses getColType(RelNode, int).
    assertThat(rel, instanceOf(LogicalFilter.class));
    final RelOptCluster cluster = rel.getCluster();
    final RelMetadataQuery mq = cluster.getMetadataQuery();
    assertThat(colType(mq, rel, 0), equalTo("DEPTNO-rel"));
    assertThat(colType(mq, rel, 1), equalTo("EXPR$1-rel"));

    // Next node is an aggregate. Its metadata uses
    // getColType(LogicalAggregate, int).
    final RelNode input = rel.getInput(0);
    assertThat(input, instanceOf(LogicalAggregate.class));
    assertThat(colType(mq, input, 0), equalTo("DEPTNO-agg"));

    // There is no caching. Another request causes another call to the provider.
    assertThat(buf, hasToString("[DEPTNO-rel, EXPR$1-rel, DEPTNO-agg]"));
    assertThat(buf, hasSize(3));
    assertThat(colType(mq, input, 0), equalTo("DEPTNO-agg"));
    assertThat(buf, hasSize(4));

    // Now add a cache. Only the first request for each piece of metadata
    // generates a new call to the provider.
    final RelOptPlanner planner = cluster.getPlanner();
    metadataConfig.applyMetadata(rel.getCluster(),
        new org.apache.calcite.rel.metadata.CachingRelMetadataProvider(
            requireNonNull(cluster.getMetadataProvider(),
                "cluster.metadataProvider"), planner));
    assertThat(colType(mq, input, 0), equalTo("DEPTNO-agg"));
    assertThat(buf, hasSize(5));
    assertThat(colType(mq, input, 0), equalTo("DEPTNO-agg"));
    assertThat(buf, hasSize(5));
    assertThat(colType(mq, input, 1), equalTo("EXPR$1-agg"));
    assertThat(buf, hasSize(6));
    assertThat(colType(mq, input, 1), equalTo("EXPR$1-agg"));
    assertThat(buf, hasSize(6));
    assertThat(colType(mq, input, 0), equalTo("DEPTNO-agg"));
    assertThat(buf, hasSize(6));

    // With a different timestamp, a metadata item is re-computed on first call.
    long timestamp = planner.getRelMetadataTimestamp(rel);
    assertThat(timestamp, equalTo(0L));
    ((MockRelOptPlanner) planner).setRelMetadataTimestamp(timestamp + 1);
    assertThat(colType(mq, input, 0), equalTo("DEPTNO-agg"));
    assertThat(buf, hasSize(7));
    assertThat(colType(mq, input, 0), equalTo("DEPTNO-agg"));
    assertThat(buf, hasSize(7));
  }

  @Test void testCustomProviderWithRelMetadataQuery() {
    final List<String> buf = new ArrayList<>();
    ColTypeImpl.THREAD_LIST.set(buf);

    final String sql = "select deptno, count(*) from emp where deptno > 10 "
        + "group by deptno having count(*) = 0";
    final RelMetadataFixture.MetadataConfig metadataConfig =
        fixture().metadataConfig;
    final RelMetadataFixture fixture = sql(sql)
        .withMetadataConfig(RelMetadataFixture.MetadataConfig.NOP)
        .withCluster(cluster -> {
          // Create a custom provider that includes ColType.
          // Include the same provider twice just to be devious.
          final ImmutableList<RelMetadataProvider> list =
              ImmutableList.of(ColTypeImpl.SOURCE, ColTypeImpl.SOURCE,
                  requireNonNull(cluster.getMetadataProvider(),
                      "cluster.metadataProvider"));
          metadataConfig.applyMetadata(cluster,
              ChainedRelMetadataProvider.of(list),
              MyRelMetadataQuery::new);
          return cluster;
        });
    final RelNode rel = fixture.toRel();

    // Top node is a filter. Its metadata uses getColType(RelNode, int).
    assertThat(rel, instanceOf(LogicalFilter.class));
    assertThat(rel.getCluster().getMetadataQuery(),
        instanceOf(MyRelMetadataQuery.class));
    final MyRelMetadataQuery mq =
        (MyRelMetadataQuery) rel.getCluster().getMetadataQuery();
    assertThat(colType(mq, rel, 0), equalTo("DEPTNO-rel"));
    assertThat(colType(mq, rel, 1), equalTo("EXPR$1-rel"));

    // Next node is an aggregate. Its metadata uses
    // getColType(LogicalAggregate, int).
    final RelNode input = rel.getInput(0);
    assertThat(input, instanceOf(LogicalAggregate.class));
    assertThat(colType(mq, input, 0), equalTo("DEPTNO-agg"));

    if (metadataConfig.isCaching()) {
      // The metadata query is caching, only the first request for each piece of metadata
      // generates a new call to the provider.
      assertThat(buf, hasToString("[DEPTNO-rel, EXPR$1-rel, DEPTNO-agg]"));
      assertThat(buf, hasSize(3));
      assertThat(colType(mq, input, 0), equalTo("DEPTNO-agg"));
      assertThat(buf, hasSize(3));
      assertThat(colType(mq, input, 0), equalTo("DEPTNO-agg"));
      assertThat(buf, hasSize(3));
      assertThat(colType(mq, input, 1), equalTo("EXPR$1-agg"));
      assertThat(buf, hasSize(4));
      assertThat(colType(mq, input, 1), equalTo("EXPR$1-agg"));
      assertThat(buf, hasSize(4));
      assertThat(colType(mq, input, 0), equalTo("DEPTNO-agg"));
      assertThat(buf, hasSize(4));
    }

    // Invalidate the metadata query triggers clearing of all the metadata.
    rel.getCluster().invalidateMetadataQuery();
    assertThat(rel.getCluster().getMetadataQuery(),
        instanceOf(MyRelMetadataQuery.class));
    final MyRelMetadataQuery mq1 =
        (MyRelMetadataQuery) rel.getCluster().getMetadataQuery();
    assertThat(colType(mq1, input, 0), equalTo("DEPTNO-agg"));
    if (metadataConfig.isCaching()) {
      assertThat(buf, hasSize(5));
    }
    assertThat(colType(mq1, input, 0), equalTo("DEPTNO-agg"));
    if (metadataConfig.isCaching()) {
      assertThat(buf, hasSize(5));
    }
    // Resets the RelMetadataQuery to default.
    metadataConfig.applyMetadata(rel.getCluster());
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5903">[CALCITE-5903]
   * RelMdCollation does not define collations for EnumerableLimit</a>.
   */
  @Test void testCollationEnumerableLimit() {
    final RelNode result = sql("select * from emp order by empno limit 10")
        .withCluster(cluster -> {
          final RelOptPlanner planner = new VolcanoPlanner();
          planner.addRule(CoreRules.PROJECT_TO_CALC);
          planner.addRule(EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE);
          planner.addRule(EnumerableRules.ENUMERABLE_CALC_RULE);
          planner.addRule(EnumerableRules.ENUMERABLE_SORT_RULE);
          planner.addRule(EnumerableRules.ENUMERABLE_LIMIT_RULE);
          planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
          return RelOptCluster.create(planner, cluster.getRexBuilder());
        })
        .withRelTransform(rel -> {
          final RelOptPlanner planner = rel.getCluster().getPlanner();
          planner.setRoot(rel);
          final RelTraitSet requiredOutputTraits =
              rel.getCluster().traitSet().replace(EnumerableConvention.INSTANCE);
          final RelNode rootRel = planner.changeTraits(rel, requiredOutputTraits);
          planner.setRoot(rootRel);
          return planner.findBestExp();
        }).toRel();

    assertThat(result, instanceOf(EnumerableLimit.class));
    final RelMetadataQuery mq = result.getCluster().getMetadataQuery();
    final ImmutableList<RelCollation> collations = mq.collations(result);
    assertThat(collations, notNullValue());
    assertThat(collations, hasToString("[[0]]"));
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6338">[CALCITE-6338]
   * RelMdCollation#project can return an incomplete list of collations
   * in the presence of aliasing</a>.
   */
  @Test void testCollationProjectAliasing() {
    final RelBuilder builder = RelBuilderTest.createBuilder();
    final RelNode relNode1 = builder
        .scan("EMP")
        .sort(2, 3)
        .project(builder.field(0), builder.field(2), builder.field(2), builder.field(3))
        .build();
    checkCollationProjectAliasing(relNode1, "[[1, 3], [2, 3]]");
    final RelNode relNode2 = builder
        .scan("EMP")
        .sort(0, 1)
        .project(builder.field(0), builder.field(0), builder.field(1), builder.field(1))
        .build();
    checkCollationProjectAliasing(relNode2, "[[0, 2], [0, 3], [1, 2], [1, 3]]");
    final RelNode relNode3 = builder
        .scan("EMP")
        .sort(0, 1, 2)
        .project(
            builder.field(0), builder.field(0),
            builder.field(1), builder.field(1), builder.field(1),
            builder.field(2))
        .build();
    checkCollationProjectAliasing(relNode3,
        "[[0, 2, 5], [0, 3, 5], [0, 4, 5], [1, 2, 5], [1, 3, 5], [1, 4, 5]]");
  }

  private void checkCollationProjectAliasing(RelNode relNode, String expectedCollation) {
    final RelMetadataQuery mq = relNode.getCluster().getMetadataQuery();
    assertThat(relNode, instanceOf(Project.class));
    final ImmutableList<RelCollation> collations = mq.collations(relNode);
    assertThat(collations, notNullValue());
    assertThat(collations, hasToString(expectedCollation));
  }

  /** Unit test for
   * {@link org.apache.calcite.rel.metadata.RelMdCollation#project}
   * and other helper functions for deducing collations. */
  @Test void testCollation() {
    final RelMetadataFixture.MetadataConfig metadataConfig =
        fixture().metadataConfig;
    final Project rel = (Project) sql("select * from emp, dept").toRel();
    final Join join = (Join) rel.getInput();
    final RelOptTable empTable = join.getInput(0).getTable();
    assertThat(empTable, notNullValue());
    final RelOptTable deptTable = join.getInput(1).getTable();
    assertThat(deptTable, notNullValue());
    Frameworks.withPlanner((cluster, relOptSchema, rootSchema) -> {
      metadataConfig.applyMetadata(cluster);
      checkCollation(cluster, empTable, deptTable);
      return null;
    });
  }

  private void checkCollation(RelOptCluster cluster, RelOptTable empTable,
      RelOptTable deptTable) {
    final RexBuilder rexBuilder = cluster.getRexBuilder();
    final LogicalTableScan empScan =
        LogicalTableScan.create(cluster, empTable, ImmutableList.of());

    List<RelCollation> collations =
        RelMdCollation.table(empScan.getTable());
    assertThat(collations, hasSize(0));

    // ORDER BY field#0 ASC, field#1 ASC
    final RelCollation collation =
        RelCollations.of(new RelFieldCollation(0), new RelFieldCollation(1));
    collations = RelMdCollation.sort(collation);
    assertThat(collations, hasSize(1));
    assertThat(collations.get(0).getFieldCollations(), hasSize(2));

    final Sort empSort = LogicalSort.create(empScan, collation, null, null);

    final List<RexNode> projects =
        ImmutableList.of(rexBuilder.makeInputRef(empSort, 1),
            rexBuilder.makeLiteral("foo"),
            rexBuilder.makeInputRef(empSort, 0),
            rexBuilder.makeCall(SqlStdOperatorTable.MINUS,
                rexBuilder.makeInputRef(empSort, 0),
                rexBuilder.makeInputRef(empSort, 3)));

    final RelMetadataQuery mq = cluster.getMetadataQuery();
    collations = RelMdCollation.project(mq, empSort, projects);
    assertThat(collations, hasSize(1));
    assertThat(collations.get(0).getFieldCollations(), hasSize(2));
    assertThat(collations.get(0).getFieldCollations().get(0).getFieldIndex(),
        equalTo(2));
    assertThat(collations.get(0).getFieldCollations().get(1).getFieldIndex(),
        equalTo(0));

    final LogicalProject project =
        LogicalProject.create(empSort, ImmutableList.of(),
            projects,
            ImmutableList.of("a", "b", "c", "d"),
            ImmutableSet.of());

    final LogicalTableScan deptScan =
        LogicalTableScan.create(cluster, deptTable, ImmutableList.of());

    final RelCollation deptCollation =
        RelCollations.of(new RelFieldCollation(0), new RelFieldCollation(1));
    final Sort deptSort =
        LogicalSort.create(deptScan, deptCollation, null, null);

    final ImmutableIntList leftKeys = ImmutableIntList.of(2);
    final ImmutableIntList rightKeys = ImmutableIntList.of(0);
    final EnumerableMergeJoin join =
        EnumerableMergeJoin.create(project, deptSort,
            rexBuilder.makeLiteral(true), leftKeys, rightKeys, JoinRelType.INNER);
    collations =
        RelMdCollation.mergeJoin(mq, project, deptSort, leftKeys,
            rightKeys, JoinRelType.INNER);
    assertThat(collations,
        equalTo(join.getTraitSet().getTraits(RelCollationTraitDef.INSTANCE)));
    final EnumerableMergeJoin semiJoin =
        EnumerableMergeJoin.create(project, deptSort,
            rexBuilder.makeLiteral(true), leftKeys, rightKeys,
            JoinRelType.SEMI);
    collations =
        RelMdCollation.mergeJoin(mq, project, deptSort, leftKeys,
            rightKeys, JoinRelType.SEMI);
    assertThat(collations,
        equalTo(semiJoin.getTraitSet().getTraits(RelCollationTraitDef.INSTANCE)));
    final EnumerableMergeJoin antiJoin =
        EnumerableMergeJoin.create(project, deptSort,
            rexBuilder.makeLiteral(true), leftKeys, rightKeys,
            JoinRelType.ANTI);
    collations =
        RelMdCollation.mergeJoin(mq, project, deptSort, leftKeys,
            rightKeys, JoinRelType.ANTI);
    assertThat(collations,
        equalTo(antiJoin.getTraitSet().getTraits(RelCollationTraitDef.INSTANCE)));

    // Values (empty)
    collations =
        RelMdCollation.values(mq, empTable.getRowType(), ImmutableList.of());
    assertThat(collations,
        hasToString("[[0, 1, 2, 3, 4, 5, 6, 7, 8], "
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
    assertThat(collations,
        hasToString("[[0, 1, 2, 3], [1, 3]]"));

    final LogicalValues values =
        LogicalValues.create(cluster, rowType, tuples.build());
    assertThat(mq.collations(values), equalTo(collations));
  }

  /** Unit test for
   * {@link org.apache.calcite.rel.metadata.RelMdColumnUniqueness#areColumnsUnique}
   * applied to {@link Values}. */
  @Test void testColumnUniquenessForValues() {
    Frameworks.withPlanner((cluster, relOptSchema, rootSchema) -> {
      final RexBuilder rexBuilder = cluster.getRexBuilder();
      final RelMetadataQuery mq = cluster.getMetadataQuery();
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

      final ImmutableBitSet colNone = bitSetOf();
      final ImmutableBitSet col0 = bitSetOf(0);
      final ImmutableBitSet col1 = bitSetOf(1);
      final ImmutableBitSet colAll = bitSetOf(0, 1);

      assertThat(mq.areColumnsUnique(values, col0), is(true));
      assertThat(mq.areColumnsUnique(values, col1), is(false));
      assertThat(mq.areColumnsUnique(values, colAll), is(true));
      assertThat(mq.areColumnsUnique(values, colNone), is(false));

      // Repeat the above tests directly against the handler.
      final RelMdColumnUniqueness handler =
          (RelMdColumnUniqueness) Iterables.getOnlyElement(RelMdColumnUniqueness.SOURCE
              .handlers(BuiltInMetadata.ColumnUniqueness.Handler.class));
      assertThat(handler.areColumnsUnique(values, mq, col0, false),
          is(true));
      assertThat(handler.areColumnsUnique(values, mq, col1, false),
          is(false));
      assertThat(handler.areColumnsUnique(values, mq, colAll, false),
          is(true));
      assertThat(handler.areColumnsUnique(values, mq, colNone, false),
          is(false));

      return null;
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
        literal =
            rexBuilder.makeExactLiteral(BigDecimal.valueOf((Integer) value));
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
  @Test void testAverageRowSize() {
    final Project rel = (Project) sql("select * from emp, dept").toRel();
    final Join join = (Join) rel.getInput();
    final RelOptTable empTable = join.getInput(0).getTable();
    assertThat(empTable, notNullValue());
    final RelOptTable deptTable = join.getInput(1).getTable();
    assertThat(deptTable, notNullValue());
    Frameworks.withPlanner((cluster, relOptSchema, rootSchema) -> {
      checkAverageRowSize(cluster, empTable, deptTable);
      return null;
    });
  }

  private void checkAverageRowSize(RelOptCluster cluster, RelOptTable empTable,
      RelOptTable deptTable) {
    final RexBuilder rexBuilder = cluster.getRexBuilder();
    final RelMetadataQuery mq = cluster.getMetadataQuery();
    final LogicalTableScan empScan =
        LogicalTableScan.create(cluster, empTable, ImmutableList.of());

    Double rowSize = mq.getAverageRowSize(empScan);
    List<Double> columnSizes = mq.getAverageColumnSizes(empScan);

    assertThat(columnSizes,
        hasSize(empScan.getRowType().getFieldCount()));
    assertThat(columnSizes,
        equalTo(Arrays.asList(4.0, 40.0, 20.0, 4.0, 8.0, 4.0, 4.0, 4.0, 1.0)));
    assertThat(rowSize, equalTo(89.0));

    // Empty values
    final LogicalValues emptyValues =
        LogicalValues.createEmpty(cluster, empTable.getRowType());
    rowSize = mq.getAverageRowSize(emptyValues);
    columnSizes = mq.getAverageColumnSizes(emptyValues);
    assertThat(columnSizes,
        hasSize(emptyValues.getRowType().getFieldCount()));
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
    assertThat(columnSizes,
        hasSize(values.getRowType().getFieldCount()));
    assertThat(columnSizes, equalTo(Arrays.asList(4.0, 8.0, 3.0)));
    assertThat(rowSize, equalTo(15.0));

    // Union
    final LogicalUnion union =
        LogicalUnion.create(ImmutableList.of(empScan, emptyValues),
            true);
    rowSize = mq.getAverageRowSize(union);
    columnSizes = mq.getAverageColumnSizes(union);
    assertThat(columnSizes, hasSize(9));
    assertThat(columnSizes,
        equalTo(Arrays.asList(4.0, 40.0, 20.0, 4.0, 8.0, 4.0, 4.0, 4.0, 1.0)));
    assertThat(rowSize, equalTo(89.0));

    // Filter
    final LogicalTableScan deptScan =
        LogicalTableScan.create(cluster, deptTable, ImmutableList.of());
    final LogicalFilter filter =
        LogicalFilter.create(deptScan,
            rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN,
                rexBuilder.makeInputRef(deptScan, 0),
                rexBuilder.makeExactLiteral(BigDecimal.TEN)));
    rowSize = mq.getAverageRowSize(filter);
    columnSizes = mq.getAverageColumnSizes(filter);
    assertThat(columnSizes, hasSize(2));
    assertThat(columnSizes, equalTo(Arrays.asList(4.0, 20.0)));
    assertThat(rowSize, equalTo(24.0));

    // Project
    final LogicalProject deptProject =
        LogicalProject.create(filter,
            ImmutableList.of(),
            ImmutableList.of(
                rexBuilder.makeInputRef(filter, 0),
                rexBuilder.makeInputRef(filter, 1),
                rexBuilder.makeCall(SqlStdOperatorTable.PLUS,
                    rexBuilder.makeInputRef(filter, 0),
                    rexBuilder.makeExactLiteral(BigDecimal.ONE)),
                rexBuilder.makeCall(SqlStdOperatorTable.CHAR_LENGTH,
                    rexBuilder.makeInputRef(filter, 1))),
            (List<String>) null,
            ImmutableSet.of());
    rowSize = mq.getAverageRowSize(deptProject);
    columnSizes = mq.getAverageColumnSizes(deptProject);
    assertThat(columnSizes, hasSize(4));
    assertThat(columnSizes, equalTo(Arrays.asList(4.0, 20.0, 4.0, 4.0)));
    assertThat(rowSize, equalTo(32.0));

    // Join
    final LogicalJoin join =
        LogicalJoin.create(empScan, deptProject, ImmutableList.of(),
            rexBuilder.makeLiteral(true), ImmutableSet.of(), JoinRelType.INNER);
    rowSize = mq.getAverageRowSize(join);
    columnSizes = mq.getAverageColumnSizes(join);
    assertThat(columnSizes, hasSize(13));
    assertThat(columnSizes,
        equalTo(
            Arrays.asList(4.0, 40.0, 20.0, 4.0, 8.0, 4.0, 4.0, 4.0, 1.0, 4.0,
                20.0, 4.0, 4.0)));
    assertThat(rowSize, equalTo(121.0));

    // Aggregate
    final LogicalAggregate aggregate =
        LogicalAggregate.create(join,
            ImmutableList.of(),
            bitSetOf(2, 0),
            ImmutableList.of(),
            ImmutableList.of(
                AggregateCall.create(SqlStdOperatorTable.COUNT, false, false,
                    false, ImmutableList.of(), ImmutableIntList.of(),
                    -1, null, RelCollations.EMPTY, 2, join, null, null)));
    rowSize = mq.getAverageRowSize(aggregate);
    columnSizes = mq.getAverageColumnSizes(aggregate);
    assertThat(columnSizes, hasSize(3));
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
  @Test void testPredicates() {
    final Project rel = (Project) sql("select * from emp, dept").toRel();
    final Join join = (Join) rel.getInput();
    final RelOptTable empTable = join.getInput(0).getTable();
    assertThat(empTable, notNullValue());
    final RelOptTable deptTable = join.getInput(1).getTable();
    assertThat(deptTable, notNullValue());
    Frameworks.withPlanner((cluster, relOptSchema, rootSchema) -> {
      checkPredicates(cluster, empTable, deptTable);
      return null;
    });
  }

  private void checkPredicates(RelOptCluster cluster, RelOptTable empTable,
      RelOptTable deptTable) {
    final RelBuilder relBuilder = RelBuilder.proto().create(cluster, null);
    final RelMetadataQuery mq = cluster.getMetadataQuery();

    final LogicalTableScan empScan =
        LogicalTableScan.create(cluster, empTable, ImmutableList.of());
    relBuilder.push(empScan);

    RelOptPredicateList predicates =
        mq.getPulledUpPredicates(empScan);
    assertThat(predicates.pulledUpPredicates.isEmpty(), is(true));

    relBuilder.filter(
        relBuilder.equals(relBuilder.field("EMPNO"),
            relBuilder.literal(BigDecimal.ONE)));

    final RelNode filter = relBuilder.peek();
    predicates = mq.getPulledUpPredicates(filter);
    assertThat(predicates.pulledUpPredicates, sortsAs("[=($0, 1)]"));

    final LogicalTableScan deptScan =
        LogicalTableScan.create(cluster, deptTable, ImmutableList.of());
    relBuilder.push(deptScan);

    relBuilder.semiJoin(
        relBuilder.equals(relBuilder.field(2, 0, "DEPTNO"),
            relBuilder.field(2, 1, "DEPTNO")));
    final LogicalJoin semiJoin = (LogicalJoin) relBuilder.build();

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
    assertThat(predicates.pulledUpPredicates,
        sortsAs("[IS NOT NULL($0)]"));
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
  @Test void testPullUpPredicatesFromAggregation() {
    final String sql = "select a, max(b) from (\n"
        + "  select 1 as a, 2 as b from emp)subq\n"
        + "group by a";
    final Aggregate rel = (Aggregate) sql(sql).toRel();
    final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
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
  @Test void testPullUpPredicatesForExprsItr() {
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
    try (JdbcAdapterTest.LockWrapper ignore =
             JdbcAdapterTest.LockWrapper.lock(LOCK)) {
      final RelNode rel = sql(sql).toRel();
      final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
      RelOptPredicateList inputSet = mq.getPulledUpPredicates(rel.getInput(0));
      assertThat(inputSet.pulledUpPredicates, hasSize(11));
    }
  }

  @Test void testPullUpPredicatesOnConstant() {
    final String sql = "select deptno, mgr, x, 'y' as y, z from (\n"
        + "  select deptno, mgr, cast(null as integer) as x, cast('1' as int) as z\n"
        + "  from emp\n"
        + "  where mgr is null and deptno < 10)";
    final RelNode rel = sql(sql).toRel();
    final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
    RelOptPredicateList list = mq.getPulledUpPredicates(rel);
    assertThat(list.pulledUpPredicates,
        sortsAs("[<($0, 10), =($3, 'y'), =($4, 1), IS NULL($1), IS NULL($2)]"));
  }

  @Test void testPullUpPredicatesOnNullableConstant() {
    final String sql = "select nullif(1, 1) as c\n"
        + "  from emp\n"
        + "  where mgr is null and deptno < 10";
    final RelNode rel = sql(sql).toRel();
    final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
    RelOptPredicateList list = mq.getPulledUpPredicates(rel);
    // Uses "IS NOT DISTINCT FROM" rather than "=" because cannot guarantee not null.
    assertThat(list.pulledUpPredicates,
        sortsAs("[IS NULL($0)]"));
  }

  @Test void testPullUpPredicatesFromUnion0() {
    final RelNode rel = sql(""
        + "select empno from emp where empno=1\n"
        + "union all\n"
        + "select empno from emp where empno=1").toRel();
    final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
    assertThat(mq.getPulledUpPredicates(rel).pulledUpPredicates,
        sortsAs("[=($0, 1)]"));
  }

  @Test void testPullUpPredicatesFromUnion1() {
    final RelNode rel = sql(""
        + "select empno, deptno from emp where empno=1 or deptno=2\n"
        + "union all\n"
        + "select empno, deptno from emp where empno=3 or deptno=4").toRel();
    final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
    assertThat(mq.getPulledUpPredicates(rel).pulledUpPredicates,
        sortsAs("[OR(SEARCH($0, Sarg[1, 3]), SEARCH($1, Sarg[2, 4]))]"));
  }

  @Test void testPullUpPredicatesFromUnion2() {
    final RelNode rel = sql(""
        + "select empno, comm, deptno from emp where empno=1 and comm=2 and deptno=3\n"
        + "union all\n"
        + "select empno, comm, deptno from emp where empno=1 and comm=4").toRel();
    final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
    assertThat(mq.getPulledUpPredicates(rel).pulledUpPredicates,
        // Because the hashCode for
        // OR(AND(=($1, 2), =($2, 3)) and
        // OR(AND(=($2, 3), =($1, 2)) are the same, the result is flipped and not stable,
        // but they both are correct.
        anyOf(sortsAs("[=($0, 1), OR(AND(=($1, 2), =($2, 3)), =($1, 4))]"),
            sortsAs("[=($0, 1), OR(AND(=($2, 3), =($1, 2)), =($1, 4))]")));

  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6592">[CALCITE-6592]
   * Add test for RelMdPredicates pull up predicate from UNION
   * when it's input predicates include NULL VALUE</a>. */
  @Test void testPullUpPredicatesFromUnionWithValues1() {
    final String sql = "select cast(null as integer) as a\n"
        + "union all\n"
        + "select 5 as a";
    final Union rel =
        (Union) sql(sql).withRelBuilderConfig(c -> c.withSimplifyValues(false)).toRel();
    final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
    RelOptPredicateList inputSet = mq.getPulledUpPredicates(rel);
    ImmutableList<RexNode> pulledUpPredicates = inputSet.pulledUpPredicates;
    assertThat(pulledUpPredicates, sortsAs("[SEARCH($0, Sarg[5; NULL AS TRUE])]"));
  }

  @Test void testPullUpPredicatesFromUnionWithValues2() {
    final String sql = "select 6 as a\n"
        + "union all\n"
        + "select 5 as a";
    final Union rel =
        (Union) sql(sql).withRelBuilderConfig(c -> c.withSimplifyValues(false)).toRel();
    final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
    RelOptPredicateList inputSet = mq.getPulledUpPredicates(rel);
    ImmutableList<RexNode> pulledUpPredicates = inputSet.pulledUpPredicates;
    assertThat(pulledUpPredicates, sortsAs("[SEARCH($0, Sarg[5, 6])]"));
  }

  @Test void testPullUpPredicatesFromUnionWithValues3() {
    final String sql = "select cast(null as integer) as a, 6 as b, 7 as c\n"
        + "union all\n"
        + "select 5 as a, cast(null as integer) as b, 7 as c";
    final Union rel =
        (Union) sql(sql).withRelBuilderConfig(c -> c.withSimplifyValues(false)).toRel();
    final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
    RelOptPredicateList inputSet = mq.getPulledUpPredicates(rel);
    ImmutableList<RexNode> pulledUpPredicates = inputSet.pulledUpPredicates;
    assertThat(pulledUpPredicates,
        anyOf(sortsAs("[=($2, 7), OR(AND(IS NULL($0), IS NULL($1)), AND(=($0, 5), =($1, 6)))]"),
            sortsAs("[=($2, 7), OR(AND(=($1, 6), IS NULL($0)), AND(=($0, 5), IS NULL($1)))]"),
            sortsAs("[=($2, 7), OR(AND(IS NULL($0), =($1, 6)), AND(=($0, 5), IS NULL($1)))]")));
  }

  @Test void testPullUpPredicatesFromUnionWithProject() {
    final String sql = "select null from emp where empno = 1\n"
        + "union all\n"
        + "select null from emp where comm = 2";
    final Union rel =
        (Union) sql(sql).withRelBuilderConfig(c -> c.withSimplifyValues(false)).toRel();
    final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
    RelOptPredicateList inputSet = mq.getPulledUpPredicates(rel);
    ImmutableList<RexNode> pulledUpPredicates = inputSet.pulledUpPredicates;
    assertThat(pulledUpPredicates,
        sortsAs("[IS NULL($0)]"));
  }

  @Test void testPullUpPredicatesFromUnionWithProject2() {
    final String sql = "select empno = null from emp where comm = 2\n"
        + "union all\n"
        + "select comm = 2 from emp where comm = 2";
    final Union rel =
        (Union) sql(sql).withRelBuilderConfig(c -> c.withSimplifyValues(false)).toRel();
    final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
    RelOptPredicateList inputSet = mq.getPulledUpPredicates(rel);
    ImmutableList<RexNode> pulledUpPredicates = inputSet.pulledUpPredicates;
    assertThat(pulledUpPredicates, sortsAs("[]"));
  }

  @Test void testPullUpPredicatesFromProject() {
    final RelNode rel = sql(""
        + "select null, comm = null from emp\n").toRel();
    final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
    assertThat(mq.getPulledUpPredicates(rel).pulledUpPredicates,
        sortsAs("[IS NULL($0), IS NULL($1)]"));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6599">[CALCITE-6599]
   * RelMdPredicates should pull up more predicates from VALUES
   * when there are several literals</a>. */
  @Test void testPullUpPredicatesFromValues1() {
    final String sql = "values(1, 2, 3)";
    final Values values = (Values) sql(sql).toRel();
    final RelMetadataQuery mq = values.getCluster().getMetadataQuery();
    RelOptPredicateList inputSet = mq.getPulledUpPredicates(values);
    ImmutableList<RexNode> pulledUpPredicates = inputSet.pulledUpPredicates;
    assertThat(pulledUpPredicates,
        sortsAs("[=($0, 1), =($1, 2), =($2, 3)]"));
  }

  @Test void testPullUpPredicatesFromValues2() {
    final String sql = "values(cast(null as integer), null)";
    final Values values = (Values) sql(sql).toRel();
    final RelMetadataQuery mq = values.getCluster().getMetadataQuery();
    RelOptPredicateList inputSet = mq.getPulledUpPredicates(values);
    ImmutableList<RexNode> pulledUpPredicates = inputSet.pulledUpPredicates;
    assertThat(pulledUpPredicates, sortsAs("[IS NULL($0), IS NULL($1)]"));
  }

  @Test void testPullUpPredicatesFromValues3() {
    final String sql = "values(1, 2, 3, null)";
    final Values values = (Values) sql(sql).toRel();
    final RelMetadataQuery mq = values.getCluster().getMetadataQuery();
    RelOptPredicateList inputSet = mq.getPulledUpPredicates(values);
    ImmutableList<RexNode> pulledUpPredicates = inputSet.pulledUpPredicates;
    assertThat(pulledUpPredicates,
        sortsAs("[=($0, 1), =($1, 2), =($2, 3), IS NULL($3)]"));
  }

  @Test void testPullUpPredicatesFromValues4() {
    final String sql = "values(1, 2, 3, null), (1, 2, null, null), (5, 2, 3, null)";
    final Values values = (Values) sql(sql).toRel();
    final RelMetadataQuery mq = values.getCluster().getMetadataQuery();
    RelOptPredicateList inputSet = mq.getPulledUpPredicates(values);
    ImmutableList<RexNode> pulledUpPredicates = inputSet.pulledUpPredicates;
    assertThat(pulledUpPredicates,
        sortsAs("[=($1, 2), IS NULL($3), "
            + "SEARCH($0, Sarg[1, 5]), SEARCH($2, Sarg[3; NULL AS TRUE])]"));
  }

  @Test void testPullUpPredicatesFromValues5() {
    final String sql =
        "values(TIMESTAMP '2005-01-03 12:34:56',\n"
            + "TIMESTAMP WITH LOCAL TIME ZONE '2012-12-30 12:30:00',\n"
            + "DATE '2005-01-03',\n"
            + "TIME '12:34:56.7')";
    final Values values = (Values) sql(sql).toRel();
    final RelMetadataQuery mq = values.getCluster().getMetadataQuery();
    RelOptPredicateList inputSet = mq.getPulledUpPredicates(values);
    ImmutableList<RexNode> pulledUpPredicates = inputSet.pulledUpPredicates;
    assertThat(pulledUpPredicates,
        sortsAs("[=($0, 2005-01-03 12:34:56), "
            + "=($1, 2012-12-30 12:30:00), "
            + "=($2, 2005-01-03), "
            + "=($3, 12:34:56.7)]"));
  }

  @Test void testPullUpPredicatesFromValues6() {
    final String sql =
        "values(TIMESTAMP '2005-01-03 12:34:56',\n"
            + "TIMESTAMP WITH LOCAL TIME ZONE '2012-12-30 12:30:00',\n"
            + "DATE '2005-01-03',\n"
            + "TIME '12:34:56.7'), (TIMESTAMP '2005-01-03 12:35:56',\n"
            + "TIMESTAMP WITH LOCAL TIME ZONE '2012-11-30 12:30:00',\n"
            + "DATE '2005-01-04',\n"
            + "TIME '12:35:56.7')";
    final Values values = (Values) sql(sql).toRel();
    final RelMetadataQuery mq = values.getCluster().getMetadataQuery();
    RelOptPredicateList inputSet = mq.getPulledUpPredicates(values);
    ImmutableList<RexNode> pulledUpPredicates = inputSet.pulledUpPredicates;
    assertThat(pulledUpPredicates,
        sortsAs("[SEARCH($0, Sarg[2005-01-03 12:34:56, 2005-01-03 12:35:56]), "
            + "SEARCH($1, Sarg[2012-11-30 12:30:00:TIMESTAMP_WITH_LOCAL_TIME_ZONE(0), "
            + "2012-12-30 12:30:00:TIMESTAMP_WITH_LOCAL_TIME_ZONE(0)]:TIMESTAMP_WITH_LOCAL_TIME_ZONE(0)), "
            + "SEARCH($2, Sarg[2005-01-03, 2005-01-04]), "
            + "SEARCH($3, Sarg[12:34:56.7:TIME(1), 12:35:56.7:TIME(1)]:TIME(1))]"));
  }

  @Test void testPullUpPredicatesFromIntersect0() {
    final RelNode rel = sql(""
        + "select empno from emp where empno=1\n"
        + "intersect all\n"
        + "select empno from emp where empno=1").toRel();
    final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
    assertThat(mq.getPulledUpPredicates(rel).pulledUpPredicates,
        sortsAs("[=($0, 1)]"));
  }

  @Test void testPullUpPredicatesFromIntersect1() {
    final RelNode rel = sql(""
        + "select empno, deptno, comm from emp where empno=1 and deptno=2\n"
        + "intersect all\n"
        + "select empno, deptno, comm from emp where empno=1 and comm=3").toRel();
    final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
    assertThat(mq.getPulledUpPredicates(rel).pulledUpPredicates,
        sortsAs("[=($0, 1), =($1, 2), =($2, 3)]"));

  }

  @Test void testPullUpPredicatesFromIntersect2() {
    final RelNode rel = sql(""
        + "select empno, deptno, comm from emp where empno=1 and deptno=2\n"
        + "intersect all\n"
        + "select empno, deptno, comm from emp where 1=empno and (deptno=2 or comm=3)").toRel();
    final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
    assertThat(mq.getPulledUpPredicates(rel).pulledUpPredicates,
        sortsAs("[=($0, 1), =($1, 2)]"));

  }

  @Test void testPullUpPredicatesFromIntersect3() {
    final RelNode rel = sql(""
        + "select empno, deptno, comm from emp where empno=1 or deptno=2\n"
        + "intersect all\n"
        + "select empno, deptno, comm from emp where deptno=2 or empno=1 or comm=3").toRel();
    final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
    assertThat(mq.getPulledUpPredicates(rel).pulledUpPredicates,
        sortsAs("[OR(=($0, 1), =($1, 2))]"));
  }

  @Test void testPullUpPredicatesFromMinus() {
    final RelNode rel = sql(""
        + "select empno, deptno, comm from emp where empno=1 and deptno=2\n"
        + "except all\n"
        + "select empno, deptno, comm from emp where comm=3").toRel();
    final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
    assertThat(mq.getPulledUpPredicates(rel).pulledUpPredicates,
        sortsAs("[=($0, 1), =($1, 2)]"));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5944">[CALCITE-5944]
   * Add metadata for Sample</a>. */
  @Test void testPullUpPredicatesFromSample() {
    final RelNode rel = sql("select * from("
        + "select empno, deptno, comm from emp\n"
        + "where empno=1 and deptno=2)\n"
        + "tablesample bernoulli(50) repeatable(1)").toRel();
    final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
    assertThat(mq.getPulledUpPredicates(rel).pulledUpPredicates,
        sortsAs("[=($0, 1), =($1, 2)]"));
  }

  @Test void testDistributionSimple() {
    RelNode rel = sql("select * from emp where deptno = 10").toRel();
    final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
    RelDistribution d = mq.getDistribution(rel);
    assertThat(d, is(RelDistributions.BROADCAST_DISTRIBUTED));
  }

  @Test void testDistributionHash() {
    final RelNode rel = sql("select * from emp").toRel();
    final RelDistribution dist = RelDistributions.hash(ImmutableList.of(1));
    final LogicalExchange exchange = LogicalExchange.create(rel, dist);

    final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
    RelDistribution d = mq.getDistribution(exchange);
    assertThat(d, is(dist));
  }

  @Test void testDistributionHashEmpty() {
    final RelNode rel = sql("select * from emp").toRel();
    final RelDistribution dist =
        RelDistributions.hash(ImmutableList.<Integer>of());
    final LogicalExchange exchange = LogicalExchange.create(rel, dist);

    final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
    RelDistribution d = mq.getDistribution(exchange);
    assertThat(d, is(dist));
  }

  @Test void testDistributionSingleton() {
    final RelNode rel = sql("select * from emp").toRel();
    final RelDistribution dist = RelDistributions.SINGLETON;
    final LogicalExchange exchange = LogicalExchange.create(rel, dist);

    final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
    RelDistribution d = mq.getDistribution(exchange);
    assertThat(d, is(dist));
  }

  /** Unit test for {@link RelMdUtil#linear(int, int, int, double, double)}. */
  @Test void testLinear() {
    assertThat(RelMdUtil.linear(0, 0, 10, 100, 200), is(100d));
    assertThat(RelMdUtil.linear(5, 0, 10, 100, 200), is(150d));
    assertThat(RelMdUtil.linear(6, 0, 10, 100, 200), is(160d));
    assertThat(RelMdUtil.linear(10, 0, 10, 100, 200), is(200d));
    assertThat(RelMdUtil.linear(-2, 0, 10, 100, 200), is(100d));
    assertThat(RelMdUtil.linear(12, 0, 10, 100, 200), is(200d));
  }

  // ----------------------------------------------------------------------
  // Tests for getExpressionLineage
  // ----------------------------------------------------------------------

  private void assertExpressionLineage(
      String sql, int columnIndex, String expected, String comment) {
    RelNode rel = sql(sql).toRel();
    RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
    RexNode ref = RexInputRef.of(columnIndex, rel.getRowType().getFieldList());
    Set<RexNode> r = mq.getExpressionLineage(rel, ref);

    assertThat("Lineage for expr '" + ref + "' in node '"
            + rel + "'" + " for query '" + sql + "': " + comment,
        String.valueOf(r), is(expected));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5944">[CALCITE-5944]
   * Add metadata for Sample</a>. */
  @Test void testExpressionLineageSample() {
    final String sql = "select productid from products_temporal\n"
        + "tablesample bernoulli(50) repeatable(1)";
    final String expected = "[[CATALOG, SALES, PRODUCTS_TEMPORAL].#0.$0]";
    final String comment = "'productid' is column 0 in 'catalog.sales.products_temporal'";
    assertExpressionLineage(sql, 0, expected, comment);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5392">[CALCITE-5392]
   * Support Snapshot in RelMdExpressionLineage</a>. */
  @Test void testExpressionLineageSnapshot() {
    String expected = "[[CATALOG, SALES, PRODUCTS_TEMPORAL].#0.$0]";
    String comment = "'productid' is column 0 in 'catalog.sales.products_temporal'";
    assertExpressionLineage("select productid from products_temporal\n"
        + "for system_time as of TIMESTAMP '2011-01-02 00:00:00'", 0, expected, comment);
  }

  @Test void testExpressionLineageStar() {
    // All columns in output
    final RelNode tableRel = sql("select * from emp").toRel();
    final RelMetadataQuery mq = tableRel.getCluster().getMetadataQuery();

    final RexNode ref = RexInputRef.of(4, tableRel.getRowType().getFieldList());
    final Set<RexNode> r = mq.getExpressionLineage(tableRel, ref);
    final String inputRef =
        RexInputRef.of(4, tableRel.getRowType().getFieldList()).toString();
    assertThat(r, hasSize(1));
    final String resultString = r.iterator().next().toString();
    assertThat(resultString, startsWith(EMP_QNAME.toString()));
    assertThat(resultString, endsWith(inputRef));
  }

  @Test void testExpressionLineageTwoColumns() {
    // mgr is column 3 in catalog.sales.emp
    // deptno is column 7 in catalog.sales.emp
    final RelNode rel = sql("select mgr, deptno from emp").toRel();
    final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();

    final RexNode ref1 = RexInputRef.of(0, rel.getRowType().getFieldList());
    final Set<RexNode> r1 = mq.getExpressionLineage(rel, ref1);
    assertThat(r1, hasSize(1));
    final RexTableInputRef result1 = (RexTableInputRef) r1.iterator().next();
    assertThat(result1.getQualifiedName(), is(EMP_QNAME));
    assertThat(result1.getIndex(), is(3));

    final RexNode ref2 = RexInputRef.of(1, rel.getRowType().getFieldList());
    final Set<RexNode> r2 = mq.getExpressionLineage(rel, ref2);
    assertThat(r2, hasSize(1));
    final RexTableInputRef result2 = (RexTableInputRef) r2.iterator().next();
    assertThat(result2.getQualifiedName(), is(EMP_QNAME));
    assertThat(result2.getIndex(), is(7));

    assertThat(result1.getIdentifier(), is(result2.getIdentifier()));
  }

  @Test void testExpressionLineageTwoColumnsSwapped() {
    // deptno is column 7 in catalog.sales.emp
    // mgr is column 3 in catalog.sales.emp
    final RelNode rel = sql("select deptno, mgr from emp").toRel();
    final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();

    final RexNode ref1 = RexInputRef.of(0, rel.getRowType().getFieldList());
    final Set<RexNode> r1 = mq.getExpressionLineage(rel, ref1);
    assertThat(r1, hasSize(1));
    final RexTableInputRef result1 = (RexTableInputRef) r1.iterator().next();
    assertThat(result1.getQualifiedName(), is(EMP_QNAME));
    assertThat(result1.getIndex(), is(7));

    final RexNode ref2 = RexInputRef.of(1, rel.getRowType().getFieldList());
    final Set<RexNode> r2 = mq.getExpressionLineage(rel, ref2);
    assertThat(r2, hasSize(1));
    final RexTableInputRef result2 = (RexTableInputRef) r2.iterator().next();
    assertThat(result2.getQualifiedName(), is(EMP_QNAME));
    assertThat(result2.getIndex(), is(3));

    assertThat(result1.getIdentifier(), is(result2.getIdentifier()));
  }

  @Test void testExpressionLineageCombineTwoColumns() {
    // empno is column 0 in catalog.sales.emp
    // deptno is column 7 in catalog.sales.emp
    final RelNode rel = sql("select empno + deptno from emp").toRel();
    final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();

    final RexNode ref = RexInputRef.of(0, rel.getRowType().getFieldList());
    final Set<RexNode> r = mq.getExpressionLineage(rel, ref);

    assertThat(r, hasSize(1));
    final RexNode result = r.iterator().next();
    assertThat(result.getKind(), is(SqlKind.PLUS));
    final RexCall call = (RexCall) result;
    assertThat(call.getOperands(), hasSize(2));
    final RexTableInputRef inputRef1 =
        (RexTableInputRef) call.getOperands().get(0);
    assertThat(inputRef1.getQualifiedName(), is(EMP_QNAME));
    assertThat(inputRef1.getIndex(), is(0));
    final RexTableInputRef inputRef2 =
        (RexTableInputRef) call.getOperands().get(1);
    assertThat(inputRef2.getQualifiedName(), is(EMP_QNAME));
    assertThat(inputRef2.getIndex(), is(7));
    assertThat(inputRef1.getIdentifier(), is(inputRef2.getIdentifier()));
  }

  @Test void testExpressionLineageConjuntiveExpression() {
    String sql = "select (empno = 1 or ename = 'abc') and deptno > 1 from emp";
    String expected = "[AND(OR(=([CATALOG, SALES, EMP].#0.$0, 1), "
        + "=([CATALOG, SALES, EMP].#0.$1, 'abc')), "
        + ">([CATALOG, SALES, EMP].#0.$7, 1))]";
    String comment = "'empno' is column 0 in 'catalog.sales.emp', "
        + "'ename' is column 1 in 'catalog.sales.emp', and "
        + "'deptno' is column 7 in 'catalog.sales.emp'";

    assertExpressionLineage(sql, 0, expected, comment);
  }

  @Test void testExpressionLineageBetweenExpressionWithJoin() {
    String sql = "select dept.deptno + empno between 1 and 2"
        + " from emp join dept on emp.deptno = dept.deptno";
    String expected = "[AND(>=(+([CATALOG, SALES, DEPT].#0.$0, [CATALOG, SALES, EMP].#0.$0), 1),"
        + " <=(+([CATALOG, SALES, DEPT].#0.$0, [CATALOG, SALES, EMP].#0.$0), 2))]";
    String comment = "'empno' is column 0 in 'catalog.sales.emp', "
        + "'deptno' is column 0 in 'catalog.sales.dept', and "
        + "'dept.deptno + empno between 1 and 2' is translated into "
        + "'dept.deptno + empno >= 1 and dept.deptno + empno <= 2'";

    assertExpressionLineage(sql, 0, expected, comment);
  }

  @Test void testExpressionLineageInnerJoinLeft() {
    // ename is column 1 in catalog.sales.emp
    final RelNode rel = sql("select ename from emp,dept").toRel();
    final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();

    final RexNode ref = RexInputRef.of(0, rel.getRowType().getFieldList());
    final Set<RexNode> r = mq.getExpressionLineage(rel, ref);
    assertThat(r, hasSize(1));
    final RexTableInputRef result = (RexTableInputRef) r.iterator().next();
    assertThat(result.getQualifiedName(), is(EMP_QNAME));
    assertThat(result.getIndex(), is(1));
  }

  @Test void testExpressionLineageInnerJoinRight() {
    // ename is column 0 in catalog.sales.bonus
    final RelNode rel =
        sql("select bonus.ename from emp join bonus using (ename)").toRel();
    final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();

    final RexNode ref = RexInputRef.of(0, rel.getRowType().getFieldList());
    final Set<RexNode> r = mq.getExpressionLineage(rel, ref);
    assertThat(r, hasSize(1));
    final RexTableInputRef result = (RexTableInputRef) r.iterator().next();
    assertThat(result.getQualifiedName(),
        equalTo(ImmutableList.of("CATALOG", "SALES", "BONUS")));
    assertThat(result.getIndex(), is(0));
  }

  @Test void testExpressionLineageLeftJoinLeft() {
    // ename is column 1 in catalog.sales.emp
    final RelNode rel =
        sql("select ename from emp left join dept using (deptno)").toRel();
    final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();

    final RexNode ref = RexInputRef.of(0, rel.getRowType().getFieldList());
    final Set<RexNode> r = mq.getExpressionLineage(rel, ref);
    assertThat(r, hasSize(1));
    final RexTableInputRef result = (RexTableInputRef) r.iterator().next();
    assertThat(result.getQualifiedName(), is(EMP_QNAME));
    assertThat(result.getIndex(), is(1));
  }

  @Test void testExpressionLineageRightJoinRight() {
    // ename is column 0 in catalog.sales.bonus
    final RelNode rel =
        sql("select bonus.ename from emp right join bonus using (ename)")
            .toRel();
    final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();

    final RexNode ref = RexInputRef.of(0, rel.getRowType().getFieldList());
    final Set<RexNode> r = mq.getExpressionLineage(rel, ref);
    assertThat(r, hasSize(1));
    final RexTableInputRef result = (RexTableInputRef) r.iterator().next();
    assertThat(result.getQualifiedName(),
        equalTo(ImmutableList.of("CATALOG", "SALES", "BONUS")));
    assertThat(result.getIndex(), is(0));
  }

  @Test void testExpressionLineageSelfJoin() {
    // deptno is column 7 in catalog.sales.emp
    // sal is column 5 in catalog.sales.emp
    final RelNode rel =
        sql("select a.deptno, b.sal from (select * from emp limit 7) as a\n"
            + "inner join (select * from emp limit 2) as b\n"
            + "on a.deptno = b.deptno").toRel();
    final RelNode tableRel = sql("select * from emp").toRel();
    final RelMetadataQuery mq = tableRel.getCluster().getMetadataQuery();

    final RexNode ref1 = RexInputRef.of(0, rel.getRowType().getFieldList());
    final Set<RexNode> r1 = mq.getExpressionLineage(rel, ref1);
    final String inputRef1 =
        RexInputRef.of(7, tableRel.getRowType().getFieldList()).toString();
    assertThat(r1, hasSize(1));
    final String resultString1 = r1.iterator().next().toString();
    assertThat(resultString1, startsWith(EMP_QNAME.toString()));
    assertThat(resultString1, endsWith(inputRef1));

    final RexNode ref2 = RexInputRef.of(1, rel.getRowType().getFieldList());
    final Set<RexNode> r2 = mq.getExpressionLineage(rel, ref2);
    final String inputRef2 =
        RexInputRef.of(5, tableRel.getRowType().getFieldList()).toString();
    assertThat(r2, hasSize(1));
    final String resultString2 = r2.iterator().next().toString();
    assertThat(resultString2, startsWith(EMP_QNAME.toString()));
    assertThat(resultString2, endsWith(inputRef2));

    assertThat(((RexTableInputRef) r1.iterator().next()).getIdentifier(),
        not(((RexTableInputRef) r2.iterator().next()).getIdentifier()));
  }

  @Test void testExpressionLineageOuterJoin() {
    // lineage cannot be determined
    final RelNode rel = sql("select name as dname from emp left outer join dept"
        + " on emp.deptno = dept.deptno").toRel();
    final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();

    final RexNode ref = RexInputRef.of(0, rel.getRowType().getFieldList());
    final Set<RexNode> r = mq.getExpressionLineage(rel, ref);
    assertNull(r);
  }

  @Test void testExpressionLineageFilter() {
    // ename is column 1 in catalog.sales.emp
    final RelNode rel = sql("select ename from emp where deptno = 10").toRel();
    final RelNode tableRel = sql("select * from emp").toRel();
    final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();

    final RexNode ref = RexInputRef.of(0, rel.getRowType().getFieldList());
    final Set<RexNode> r = mq.getExpressionLineage(rel, ref);
    final String inputRef =
        RexInputRef.of(1, tableRel.getRowType().getFieldList()).toString();
    assertThat(r, hasSize(1));
    final String resultString = r.iterator().next().toString();
    assertThat(resultString, startsWith(EMP_QNAME.toString()));
    assertThat(resultString, endsWith(inputRef));
  }

  @Test void testExpressionLineageAggregateGroupColumn() {
    // deptno is column 7 in catalog.sales.emp
    final RelNode rel = sql("select deptno, count(*) from emp where deptno > 10 "
        + "group by deptno having count(*) = 0").toRel();
    final RelNode tableRel = sql("select * from emp").toRel();
    final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();

    final RexNode ref = RexInputRef.of(0, rel.getRowType().getFieldList());
    final Set<RexNode> r = mq.getExpressionLineage(rel, ref);
    final String inputRef =
        RexInputRef.of(7, tableRel.getRowType().getFieldList()).toString();
    assertThat(r, hasSize(1));
    final String resultString = r.iterator().next().toString();
    assertThat(resultString, startsWith(EMP_QNAME.toString()));
    assertThat(resultString, endsWith(inputRef));
  }

  @Test void testExpressionLineageAggregateAggColumn() {
    // lineage cannot be determined
    final RelNode rel =
        sql("select deptno, count(*) from emp where deptno > 10 "
            + "group by deptno having count(*) = 0").toRel();
    final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();

    final RexNode ref = RexInputRef.of(1, rel.getRowType().getFieldList());
    final Set<RexNode> r = mq.getExpressionLineage(rel, ref);
    assertNull(r);
  }

  @Test void testExpressionLineageUnion() {
    // sal is column 5 in catalog.sales.emp
    final RelNode rel = sql("select sal from (\n"
        + "  select * from emp union all select * from emp) "
        + "where deptno = 10").toRel();
    final RelNode tableRel = sql("select * from emp").toRel();
    final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();

    final RexNode ref = RexInputRef.of(0, rel.getRowType().getFieldList());
    final Set<RexNode> r = mq.getExpressionLineage(rel, ref);
    final String inputRef =
        RexInputRef.of(5, tableRel.getRowType().getFieldList()).toString();
    assertThat(r, hasSize(2));
    for (RexNode result : r) {
      final String resultString = result.toString();
      assertThat(resultString, startsWith(EMP_QNAME.toString()));
      assertThat(resultString, endsWith(inputRef));
    }

    Iterator<RexNode> it = r.iterator();
    assertThat(((RexTableInputRef) it.next()).getIdentifier(),
        not(((RexTableInputRef) it.next()).getIdentifier()));
  }

  @Test void testExpressionLineageMultiUnion() {
    // empno is column 0 in catalog.sales.emp
    // sal is column 5 in catalog.sales.emp
    final RelNode rel = sql("select a.empno + b.sal from\n"
        + " (select empno, ename from emp,dept) a join "
        + " (select * from emp union all select * from emp) b\n"
        + " on a.empno = b.empno\n"
        + " where b.deptno = 10").toRel();
    final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();

    final RexNode ref = RexInputRef.of(0, rel.getRowType().getFieldList());
    final Set<RexNode> r = mq.getExpressionLineage(rel, ref);

    // With the union, we should get two origins
    // The first one should be the same one: join
    // The second should come from each union input
    final Set<List<String>> set = new HashSet<>();
    assertThat(r, hasSize(2));
    for (RexNode result : r) {
      assertThat(result.getKind(), is(SqlKind.PLUS));
      final RexCall call = (RexCall) result;
      assertThat(call.getOperands(), hasSize(2));
      final RexTableInputRef inputRef1 =
          (RexTableInputRef) call.getOperands().get(0);
      assertThat(inputRef1.getQualifiedName(), is(EMP_QNAME));
      // Add join alpha to set
      set.add(inputRef1.getQualifiedName());
      assertThat(inputRef1.getIndex(), is(0));
      final RexTableInputRef inputRef2 =
          (RexTableInputRef) call.getOperands().get(1);
      assertThat(inputRef2.getQualifiedName(), is(EMP_QNAME));
      assertThat(inputRef2.getIndex(), is(5));
      assertThat(inputRef1.getIdentifier(), not(inputRef2.getIdentifier()));
    }
    assertThat(set, hasSize(1));
  }

  @Test void testExpressionLineageValues() {
    // lineage cannot be determined
    final RelNode rel = sql("select * from (values (1), (2)) as t(c)").toRel();
    final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();

    final RexNode ref = RexInputRef.of(0, rel.getRowType().getFieldList());
    final Set<RexNode> r = mq.getExpressionLineage(rel, ref);
    assertNull(r);
  }

  @Test void testExpressionLineageCalc() {
    final RelNode rel = sql("select sal from (\n"
        + " select deptno, empno, sal + 1 as sal, job from emp) "
        + "where deptno = 10").toRel();
    final HepProgramBuilder programBuilder = HepProgram.builder();
    programBuilder.addRuleInstance(CoreRules.PROJECT_TO_CALC);
    programBuilder.addRuleInstance(CoreRules.FILTER_TO_CALC);
    programBuilder.addRuleInstance(CoreRules.CALC_MERGE);
    final HepPlanner planner = new HepPlanner(programBuilder.build());
    planner.setRoot(rel);
    final RelNode optimizedRel = planner.findBestExp();
    final RelMetadataQuery mq = optimizedRel.getCluster().getMetadataQuery();

    final RexNode ref =
        RexInputRef.of(0, optimizedRel.getRowType().getFieldList());
    final Set<RexNode> r = mq.getExpressionLineage(optimizedRel, ref);

    assertThat(r, hasSize(1));
    final String resultString = r.iterator().next().toString();
    assertThat(resultString, is("+([CATALOG, SALES, EMP].#0.$5, 1)"));
  }

  @Test void testAllPredicates() {
    final Project rel = (Project) sql("select * from emp, dept").toRel();
    final Join join = (Join) rel.getInput();
    final RelOptTable empTable = join.getInput(0).getTable();
    assertThat(empTable, notNullValue());
    final RelOptTable deptTable = join.getInput(1).getTable();
    assertThat(deptTable, notNullValue());
    Frameworks.withPlanner((cluster, relOptSchema, rootSchema) -> {
      checkAllPredicates(cluster, empTable, deptTable);
      return null;
    });
  }

  private void checkAllPredicates(RelOptCluster cluster, RelOptTable empTable,
      RelOptTable deptTable) {
    final RelBuilder relBuilder = RelBuilder.proto().create(cluster, null);
    final RelMetadataQuery mq = cluster.getMetadataQuery();

    final LogicalTableScan empScan =
        LogicalTableScan.create(cluster, empTable, ImmutableList.of());
    relBuilder.push(empScan);

    RelOptPredicateList predicates =
        mq.getAllPredicates(empScan);
    assertThat(predicates.pulledUpPredicates.isEmpty(), is(true));

    relBuilder.filter(
        relBuilder.equals(relBuilder.field("EMPNO"),
            relBuilder.literal(BigDecimal.ONE)));

    final RelNode filter = relBuilder.peek();
    predicates = mq.getAllPredicates(filter);
    assertThat(predicates.pulledUpPredicates, hasSize(1));
    RexCall call = (RexCall) predicates.pulledUpPredicates.get(0);
    assertThat(call.getOperands(), hasSize(2));
    RexTableInputRef inputRef1 = (RexTableInputRef) call.getOperands().get(0);
    assertThat(inputRef1.getQualifiedName(), is(EMP_QNAME));
    assertThat(inputRef1.getIndex(), is(0));

    final LogicalTableScan deptScan =
        LogicalTableScan.create(cluster, deptTable, ImmutableList.of());
    relBuilder.push(deptScan);

    relBuilder.join(JoinRelType.INNER,
        relBuilder.equals(relBuilder.field(2, 0, "DEPTNO"),
            relBuilder.field(2, 1, "DEPTNO")));

    relBuilder.project(relBuilder.field("DEPTNO"));
    final RelNode project = relBuilder.peek();
    predicates = mq.getAllPredicates(project);
    assertThat(predicates.pulledUpPredicates, hasSize(2));
    // From Filter
    call = (RexCall) predicates.pulledUpPredicates.get(0);
    assertThat(call.getOperands(), hasSize(2));
    inputRef1 = (RexTableInputRef) call.getOperands().get(0);
    assertThat(inputRef1.getQualifiedName(), is(EMP_QNAME));
    assertThat(inputRef1.getIndex(), is(0));
    // From Join
    call = (RexCall) predicates.pulledUpPredicates.get(1);
    assertThat(call.getOperands(), hasSize(2));
    inputRef1 = (RexTableInputRef) call.getOperands().get(0);
    assertThat(inputRef1.getQualifiedName(), is(EMP_QNAME));
    assertThat(inputRef1.getIndex(), is(7));
    RexTableInputRef inputRef2 = (RexTableInputRef) call.getOperands().get(1);
    assertThat(inputRef2.getQualifiedName(),
        equalTo(ImmutableList.of("CATALOG", "SALES", "DEPT")));
    assertThat(inputRef2.getIndex(), is(0));
  }

  @Test void testAllPredicatesAggregate1() {
    final String sql = "select a, max(b) from (\n"
        + "  select empno as a, sal as b from emp where empno = 5)subq\n"
        + "group by a";
    final RelNode rel = sql(sql).toRel();
    final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
    RelOptPredicateList inputSet = mq.getAllPredicates(rel);
    ImmutableList<RexNode> pulledUpPredicates = inputSet.pulledUpPredicates;
    assertThat(pulledUpPredicates, hasSize(1));
    RexCall call = (RexCall) pulledUpPredicates.get(0);
    assertThat(call.getOperands(), hasSize(2));
    final RexTableInputRef inputRef1 =
        (RexTableInputRef) call.getOperands().get(0);
    assertThat(inputRef1.getQualifiedName(), is(EMP_QNAME));
    assertThat(inputRef1.getIndex(), is(0));
    final RexLiteral constant = (RexLiteral) call.getOperands().get(1);
    assertThat(constant, hasToString("5"));
  }

  @Test void testAllPredicatesAggregate2() {
    final String sql = "select * from (select a, max(b) from (\n"
        + "  select empno as a, sal as b from emp)subq\n"
        + "group by a)\n"
        + "where a = 5";
    final RelNode rel = sql(sql).toRel();
    final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
    RelOptPredicateList inputSet = mq.getAllPredicates(rel);
    ImmutableList<RexNode> pulledUpPredicates = inputSet.pulledUpPredicates;
    assertThat(pulledUpPredicates, hasSize(1));
    RexCall call = (RexCall) pulledUpPredicates.get(0);
    assertThat(call.getOperands(), hasSize(2));
    final RexTableInputRef inputRef1 =
        (RexTableInputRef) call.getOperands().get(0);
    assertThat(inputRef1.getQualifiedName(), is(EMP_QNAME));
    assertThat(inputRef1.getIndex(), is(0));
    final RexLiteral constant = (RexLiteral) call.getOperands().get(1);
    assertThat(constant, hasToString("5"));
  }

  @Test void testAllPredicatesAggregate3() {
    final String sql = "select * from (select a, max(b) as b from (\n"
        + "  select empno as a, sal as b from emp)subq\n"
        + "group by a)\n"
        + "where b = 5";
    final RelNode rel = sql(sql).toRel();
    final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
    RelOptPredicateList inputSet = mq.getAllPredicates(rel);
    // Filter on aggregate, we cannot infer lineage
    assertNull(inputSet);
  }

  @Test void testAllPredicatesAndTablesJoin() {
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
    final RelNode rel = sql(sql).toRel();
    final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
    final RelOptPredicateList inputSet = mq.getAllPredicates(rel);
    assertThat(inputSet.pulledUpPredicates,
        sortsAs("[=([CATALOG, SALES, EMP].#0.$7, [CATALOG, SALES, EMP].#1.$7), "
            + "=([CATALOG, SALES, EMP].#0.$7, [CATALOG, SALES, EMP].#2.$7), "
            + "=([CATALOG, SALES, EMP].#2.$7, [CATALOG, SALES, EMP].#3.$7), "
            + "true, "
            + "true]"));
    final Set<RelTableRef> tableReferences =
        Sets.newTreeSet(mq.getTableReferences(rel));
    assertThat(tableReferences,
        hasToString("[[CATALOG, SALES, DEPT].#0, [CATALOG, SALES, DEPT].#1, "
            + "[CATALOG, SALES, EMP].#0, [CATALOG, SALES, EMP].#1, "
            + "[CATALOG, SALES, EMP].#2, [CATALOG, SALES, EMP].#3]"));
  }

  @Test void testAllPredicatesAndTablesCalc() {
    final String sql = "select empno as a, sal as b from emp where empno > 5";
    final RelNode relNode = sql(sql).toRel();
    final HepProgram hepProgram = new HepProgramBuilder()
        .addRuleInstance(CoreRules.PROJECT_TO_CALC)
        .addRuleInstance(CoreRules.FILTER_TO_CALC)
        .build();
    final HepPlanner planner = new HepPlanner(hepProgram);
    planner.setRoot(relNode);
    final RelNode rel = planner.findBestExp();
    final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
    final RelOptPredicateList inputSet = mq.getAllPredicates(rel);
    assertThat(inputSet.pulledUpPredicates,
        sortsAs("[>([CATALOG, SALES, EMP].#0.$0, 5)]"));
    final Set<RelTableRef> tableReferences =
        Sets.newTreeSet(mq.getTableReferences(rel));
    assertThat(tableReferences,
        hasToString("[[CATALOG, SALES, EMP].#0]"));
  }

  @Test void testAllPredicatesAndTableUnion() {
    final String sql = "select a.deptno, c.sal from (select * from emp limit 7) as a\n"
        + "cross join (select * from dept limit 1) as b\n"
        + "inner join (select * from emp limit 2) as c\n"
        + "on a.deptno = c.deptno\n"
        + "union all\n"
        + "select a.deptno, c.sal from (select * from emp limit 7) as a\n"
        + "cross join (select * from dept limit 1) as b\n"
        + "inner join (select * from emp limit 2) as c\n"
        + "on a.deptno = c.deptno";
    checkAllPredicatesAndTableSetOp(sql);
  }

  @Test void testAllPredicatesAndTableIntersect() {
    final String sql = "select a.deptno, c.sal from (select * from emp limit 7) as a\n"
        + "cross join (select * from dept limit 1) as b\n"
        + "inner join (select * from emp limit 2) as c\n"
        + "on a.deptno = c.deptno\n"
        + "intersect all\n"
        + "select a.deptno, c.sal from (select * from emp limit 7) as a\n"
        + "cross join (select * from dept limit 1) as b\n"
        + "inner join (select * from emp limit 2) as c\n"
        + "on a.deptno = c.deptno";
    checkAllPredicatesAndTableSetOp(sql);
  }

  @Test void testAllPredicatesAndTableMinus() {
    final String sql = "select a.deptno, c.sal from (select * from emp limit 7) as a\n"
        + "cross join (select * from dept limit 1) as b\n"
        + "inner join (select * from emp limit 2) as c\n"
        + "on a.deptno = c.deptno\n"
        + "except all\n"
        + "select a.deptno, c.sal from (select * from emp limit 7) as a\n"
        + "cross join (select * from dept limit 1) as b\n"
        + "inner join (select * from emp limit 2) as c\n"
        + "on a.deptno = c.deptno";
    checkAllPredicatesAndTableSetOp(sql);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5944">[CALCITE-5944]
   * Add metadata for Sample</a>. */
  @Test void testAllPredicatesSample() {
    final RelNode rel = sql("select * from("
        + "select empno, deptno, comm from emp\n"
        + "where empno=1 and deptno=2)\n"
        + "tablesample bernoulli(50) repeatable(1)").toRel();
    final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
    assertThat(mq.getAllPredicates(rel).pulledUpPredicates,
        sortsAs("[AND(=([CATALOG, SALES, EMP].#0.$0, 1), =([CATALOG, SALES, EMP].#0.$7, 2))]"));
  }

  public void checkAllPredicatesAndTableSetOp(String sql) {
    final RelNode rel = sql(sql).toRel();
    final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
    final RelOptPredicateList inputSet = mq.getAllPredicates(rel);
    assertThat(inputSet.pulledUpPredicates,
        sortsAs("[=([CATALOG, SALES, EMP].#0.$7, [CATALOG, SALES, EMP].#1.$7),"
            + " =([CATALOG, SALES, EMP].#2.$7, [CATALOG, SALES, EMP].#3.$7), "
            + "true, "
            + "true]"));
    final Set<RelTableRef> tableReferences =
        Sets.newTreeSet(mq.getTableReferences(rel));
    assertThat(tableReferences,
        hasToString("[[CATALOG, SALES, DEPT].#0, [CATALOG, SALES, DEPT].#1, "
            + "[CATALOG, SALES, EMP].#0, [CATALOG, SALES, EMP].#1, "
            + "[CATALOG, SALES, EMP].#2, [CATALOG, SALES, EMP].#3]"));
  }

  @Test void testTableReferenceForIntersect() {
    final String sql1 = "select a.deptno, a.sal from emp a\n"
        + "intersect all select b.deptno, b.sal from emp b where empno = 5";
    final RelNode rel1 = sql(sql1).toRel();
    final RelMetadataQuery mq1 = rel1.getCluster().getMetadataQuery();
    final Set<RelTableRef> tableReferences1 =
        Sets.newTreeSet(mq1.getTableReferences(rel1));
    assertThat(tableReferences1,
        hasToString("[[CATALOG, SALES, EMP].#0, [CATALOG, SALES, EMP].#1]"));

    final String sql2 = "select a.deptno from dept a intersect all select b.deptno from emp b";
    final RelNode rel2 = sql(sql2).toRel();
    final RelMetadataQuery mq2 = rel2.getCluster().getMetadataQuery();
    final Set<RelTableRef> tableReferences2 =
        Sets.newTreeSet(mq2.getTableReferences(rel2));
    assertThat(tableReferences2,
        hasToString("[[CATALOG, SALES, DEPT].#0, [CATALOG, SALES, EMP].#0]"));

  }

  @Test void testTableReferenceForMinus() {
    final String sql = "select emp.deptno, emp.sal from emp\n"
        + "except all select emp.deptno, emp.sal from emp where empno = 5";
    final RelNode rel = sql(sql).toRel();
    final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
    final Set<RelTableRef> tableReferences =
        Sets.newTreeSet(mq.getTableReferences(rel));
    assertThat(tableReferences,
        hasToString("[[CATALOG, SALES, EMP].#0, [CATALOG, SALES, EMP].#1]"));
  }

  @Test void testAllPredicatesCrossJoinMultiTable() {
    final String sql = "select x.sal from\n"
        + "(select a.deptno, c.sal from (select * from emp limit 7) as a\n"
        + "cross join (select * from dept limit 1) as b\n"
        + "cross join (select * from emp where empno = 5 limit 2) as c) as x";
    final RelNode rel = sql(sql).toRel();
    final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
    final Set<RelTableRef> tableReferences =
        Sets.newTreeSet(mq.getTableReferences(rel));
    assertThat(tableReferences,
        sortsAs("[[CATALOG, SALES, DEPT].#0, "
            + "[CATALOG, SALES, EMP].#0, "
            + "[CATALOG, SALES, EMP].#1]"));
    final RelOptPredicateList inputSet = mq.getAllPredicates(rel);
    // Note that we reference [CATALOG, SALES, EMP].#1 rather than [CATALOG, SALES, EMP].#0
    assertThat(inputSet.pulledUpPredicates,
        sortsAs("[=([CATALOG, SALES, EMP].#1.$0, 5), true, true]"));
  }

  @Test void testTableReferencesJoinUnknownNode() {
    final String sql = "select * from emp limit 10";
    final RelNode node = sql(sql).toRel();
    final RelNode nodeWithUnknown =
        new DummyRelNode(node.getCluster(), node.getTraitSet(), node);
    final RexBuilder rexBuilder = node.getCluster().getRexBuilder();
    // Join
    final LogicalJoin join =
        LogicalJoin.create(nodeWithUnknown, node, ImmutableList.of(),
            rexBuilder.makeLiteral(true), ImmutableSet.of(), JoinRelType.INNER);
    final RelMetadataQuery mq = node.getCluster().getMetadataQuery();
    final Set<RelTableRef> tableReferences = mq.getTableReferences(join);
    assertNull(tableReferences);
  }

  @Test void testAllPredicatesUnionMultiTable() {
    final String sql = "select x.sal from\n"
        + "(select a.deptno, a.sal from (select * from emp) as a\n"
        + "union all select emp.deptno, emp.sal from emp\n"
        + "union all select emp.deptno, emp.sal from emp where empno = 5) as x";
    final RelNode rel = sql(sql).toRel();
    final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
    final Set<RelTableRef> tableReferences =
        Sets.newTreeSet(mq.getTableReferences(rel));
    assertThat(tableReferences,
        sortsAs("[[CATALOG, SALES, EMP].#0, "
            + "[CATALOG, SALES, EMP].#1, "
            + "[CATALOG, SALES, EMP].#2]"));
    // Note that we reference [CATALOG, SALES, EMP].#2 rather than
    // [CATALOG, SALES, EMP].#0 or [CATALOG, SALES, EMP].#1
    final RelOptPredicateList inputSet = mq.getAllPredicates(rel);
    assertThat(inputSet.pulledUpPredicates,
        sortsAs("[=([CATALOG, SALES, EMP].#2.$0, 5)]"));
  }

  @Test void testTableReferencesUnionUnknownNode() {
    final String sql = "select * from emp limit 10";
    final RelNode node = sql(sql).toRel();
    final RelNode nodeWithUnknown =
        new DummyRelNode(node.getCluster(), node.getTraitSet(), node);
    // Union
    final LogicalUnion union =
        LogicalUnion.create(ImmutableList.of(nodeWithUnknown, node),
            true);
    final RelMetadataQuery mq = node.getCluster().getMetadataQuery();
    final Set<RelTableRef> tableReferences = mq.getTableReferences(union);
    assertNull(tableReferences);
  }

  @Test void testNodeTypeCountEmp() {
    final String sql = "select * from emp";
    sql(sql)
        .assertThatNodeTypeCountIs(TableScan.class, 1,
            Project.class, 1);
  }

  @Test void testNodeTypeCountDept() {
    final String sql = "select * from dept";
    sql(sql)
        .assertThatNodeTypeCountIs(TableScan.class, 1,
            Project.class, 1);
  }

  @Test void testNodeTypeCountValues() {
    final String sql = "select * from (values (1), (2)) as t(c)";
    sql(sql)
        .assertThatNodeTypeCountIs(Values.class, 1,
            Project.class, 1);
  }

  @Test void testNodeTypeCountCartesian() {
    final String sql = "select * from emp,dept";
    sql(sql)
        .assertThatNodeTypeCountIs(TableScan.class, 2,
            Join.class, 1,
            Project.class, 1);
  }

  @Test void testNodeTypeCountJoin() {
    final String sql = "select * from emp\n"
        + "inner join dept on emp.deptno = dept.deptno";
    sql(sql)
        .assertThatNodeTypeCountIs(TableScan.class, 2,
            Join.class, 1,
            Project.class, 1);
  }

  @Test void testNodeTypeCountTableModify() {
    final String sql = "insert into emp select * from emp";
    sql(sql)
        .assertThatNodeTypeCountIs(TableScan.class, 1,
            TableModify.class, 1,
            Project.class, 1);
  }

  @Test void testNodeTypeCountExchange() {
    final String sql = "select * from emp";
    sql(sql)
        .withRelTransform(rel ->
            LogicalExchange.create(rel,
                RelDistributions.hash(ImmutableList.of())))
        .assertThatNodeTypeCountIs(TableScan.class, 1,
            Exchange.class, 1,
            Project.class, 1);
  }

  @Test void testNodeTypeCountSample() {
    final String sql = "select * from emp tablesample system(50) where empno > 5";
    sql(sql)
        .assertThatNodeTypeCountIs(TableScan.class, 1,
            Filter.class, 1,
            Project.class, 1,
            Sample.class, 1);
  }

  @Test void testNodeTypeCountJoinFinite() {
    final String sql = "select * from (select * from emp limit 14) as emp\n"
        + "inner join (select * from dept limit 4) as dept\n"
        + "on emp.deptno = dept.deptno";
    sql(sql)
        .assertThatNodeTypeCountIs(TableScan.class, 2,
            Join.class, 1,
            Project.class, 3,
            Sort.class, 2);
  }

  @Test void testNodeTypeCountJoinEmptyFinite() {
    final String sql = "select * from (select * from emp limit 0) as emp\n"
        + "inner join (select * from dept limit 4) as dept\n"
        + "on emp.deptno = dept.deptno";
    sql(sql)
        .assertThatNodeTypeCountIs(TableScan.class, 2,
            Join.class, 1,
            Project.class, 3,
            Sort.class, 2);
  }

  @Test void testNodeTypeCountLeftJoinEmptyFinite() {
    final String sql = "select * from (select * from emp limit 0) as emp\n"
        + "left join (select * from dept limit 4) as dept\n"
        + "on emp.deptno = dept.deptno";
    sql(sql)
        .assertThatNodeTypeCountIs(TableScan.class, 2,
            Join.class, 1,
            Project.class, 3,
            Sort.class, 2);
  }

  @Test void testNodeTypeCountRightJoinEmptyFinite() {
    final String sql = "select * from (select * from emp limit 0) as emp\n"
        + "right join (select * from dept limit 4) as dept\n"
        + "on emp.deptno = dept.deptno";
    sql(sql)
        .assertThatNodeTypeCountIs(TableScan.class, 2,
            Join.class, 1,
            Project.class, 3,
            Sort.class, 2);
  }

  @Test void testNodeTypeCountJoinFiniteEmpty() {
    final String sql = "select * from (select * from emp limit 7) as emp\n"
        + "inner join (select * from dept limit 0) as dept\n"
        + "on emp.deptno = dept.deptno";
    sql(sql)
        .assertThatNodeTypeCountIs(TableScan.class, 2,
            Join.class, 1,
            Project.class, 3,
            Sort.class, 2);
  }

  @Test void testNodeTypeCountJoinEmptyEmpty() {
    final String sql = "select * from (select * from emp limit 0) as emp\n"
        + "inner join (select * from dept limit 0) as dept\n"
        + "on emp.deptno = dept.deptno";
    sql(sql)
        .assertThatNodeTypeCountIs(TableScan.class, 2,
            Join.class, 1,
            Project.class, 3,
            Sort.class, 2);
  }

  @Test void testNodeTypeCountUnion() {
    final String sql = "select ename from emp\n"
        + "union all\n"
        + "select name from dept";
    sql(sql)
        .assertThatNodeTypeCountIs(TableScan.class, 2,
            Project.class, 2,
            Union.class, 1);
  }

  @Test void testNodeTypeCountUnionOnFinite() {
    final String sql = "select ename from (select * from emp limit 100)\n"
        + "union all\n"
        + "select name from (select * from dept limit 40)";
    sql(sql)
        .assertThatNodeTypeCountIs(TableScan.class, 2,
            Union.class, 1,
            Project.class, 4,
            Sort.class, 2);
  }

  @Test void testNodeTypeCountMinusOnFinite() {
    final String sql = "select ename from (select * from emp limit 100)\n"
        + "except\n"
        + "select name from (select * from dept limit 40)";
    sql(sql)
        .assertThatNodeTypeCountIs(TableScan.class, 2,
            Minus.class, 1,
            Project.class, 4,
            Sort.class, 2);
  }

  @Test void testNodeTypeCountFilter() {
    final String sql = "select * from emp where ename='Mathilda'";
    sql(sql)
        .assertThatNodeTypeCountIs(TableScan.class, 1,
            Project.class, 1,
            Filter.class, 1);
  }

  @Test void testNodeTypeCountSort() {
    final String sql = "select * from emp order by ename";
    sql(sql)
        .assertThatNodeTypeCountIs(TableScan.class, 1,
            Project.class, 1,
            Sort.class, 1);
  }

  @Test void testNodeTypeCountSortLimit() {
    final String sql = "select * from emp order by ename limit 10";
    sql(sql)
        .assertThatNodeTypeCountIs(TableScan.class, 1,
            Project.class, 1,
            Sort.class, 1);
  }

  @Test void testNodeTypeCountSortLimitOffset() {
    final String sql = "select * from emp order by ename limit 10 offset 5";
    sql(sql)
        .assertThatNodeTypeCountIs(TableScan.class, 1,
            Project.class, 1,
            Sort.class, 1);
  }

  @Test void testNodeTypeCountSortLimitOffsetOnFinite() {
    final String sql = "select * from (select * from emp limit 12)\n"
        + "order by ename limit 20 offset 5";
    sql(sql)
        .assertThatNodeTypeCountIs(TableScan.class, 1,
            Project.class, 2,
            Sort.class, 2);
  }

  @Test void testNodeTypeCountAggregate() {
    final String sql = "select deptno from emp group by deptno";
    sql(sql)
        .assertThatNodeTypeCountIs(TableScan.class, 1,
            Project.class, 1,
            Aggregate.class, 1);
  }

  @Test void testNodeTypeCountAggregateGroupingSets() {
    final String sql = "select deptno from emp\n"
        + "group by grouping sets ((deptno), (ename, deptno))";
    sql(sql)
        .assertThatNodeTypeCountIs(TableScan.class, 1,
            Project.class, 2,
            Aggregate.class, 1);
  }

  @Test void testNodeTypeCountAggregateEmptyKeyOnEmptyTable() {
    final String sql = "select count(*) from (select * from emp limit 0)";
    sql(sql)
        .assertThatNodeTypeCountIs(TableScan.class, 1,
            Project.class, 2,
            Aggregate.class, 1,
            Sort.class, 1);
  }

  @Test void testNodeTypeCountFilterAggregateEmptyKey() {
    final String sql = "select count(*) from emp where 1 = 0";
    sql(sql)
        .assertThatNodeTypeCountIs(TableScan.class, 1,
            Project.class, 1,
            Filter.class, 1,
            Aggregate.class, 1);
  }

  @Test void testConstColumnsNdv() {
    final String sql = "select ename, 100, 200 from emp";
    final RelNode rel = sql(sql).toRel();
    RelMetadataQuery mq = rel.getCluster().getMetadataQuery();

    assertThat(rel, instanceOf(Project.class));

    Project project = (Project) rel;
    assertThat(project.getProjects(), hasSize(3));

    // a non-const column, followed by two constant columns.
    assertThat(RexUtil.isLiteral(project.getProjects().get(0), true), is(false));
    assertThat(RexUtil.isLiteral(project.getProjects().get(1), true), is(true));
    assertThat(RexUtil.isLiteral(project.getProjects().get(2), true), is(true));

    // the distinct row count of const columns should be 1
    assertThat(mq.getDistinctRowCount(rel, bitSetOf(), null), is(1.0));
    assertThat(mq.getDistinctRowCount(rel, bitSetOf(1), null), is(1.0));
    assertThat(mq.getDistinctRowCount(rel, bitSetOf(1, 2), null), is(1.0));

    // the population size of const columns should be 1
    assertThat(mq.getPopulationSize(rel, bitSetOf()), is(1.0));
    assertThat(mq.getPopulationSize(rel, bitSetOf(1)), is(1.0));
    assertThat(mq.getPopulationSize(rel, bitSetOf(1, 2)), is(1.0));

    // the distinct row count of mixed columns depends on the distinct row
    // count of non-const columns
    assertThat(mq.getDistinctRowCount(rel, bitSetOf(0, 1), null),
        is(mq.getDistinctRowCount(rel, bitSetOf(0), null)));
    assertThat(mq.getDistinctRowCount(rel, bitSetOf(0, 1, 2), null),
        is(mq.getDistinctRowCount(rel, bitSetOf(0), null)));

    // the population size of mixed columns depends on the population size of
    // non-const columns
    assertThat(mq.getPopulationSize(rel, bitSetOf(0, 1)),
        is(mq.getPopulationSize(rel, bitSetOf(0))));
    assertThat(mq.getPopulationSize(rel, bitSetOf(0, 1, 2)),
        is(mq.getPopulationSize(rel, bitSetOf(0))));
  }

  /**
   * Test that RelMdPopulationSize is calculated based on the RelMetadataQuery#getRowCount().
   *
   * @see <a href="https://issues.apache.org/jira/browse/CALCITE-5647">[CALCITE-5647]</a>
   */
  @Test public void testPopulationSizeFromValues() {
    final String sql = "values(1,2,3),(1,2,3),(1,2,3),(1,2,3)";
    final RelNode rel = sql(sql).toRel();
    assertThat(rel, instanceOf(Values.class));

    RelMetadataProvider provider = RelMdPopulationSize.SOURCE;

    List<MetadataHandler<?>> handlers =
        provider.handlers(BuiltInMetadata.PopulationSize.Handler.class);

    // The population size is calculated to be half the row count. (The assumption is that half
    // the rows are duplicated.) With the default handler it should evaluate to 2 since there
    // are 4 rows.
    RelMdPopulationSize populationSize = (RelMdPopulationSize) handlers.get(0);
    Double popSize =
        populationSize.getPopulationSize((Values) rel, rel.getCluster().getMetadataQuery(),
            bitSetOf(0, 1, 2));
    assertThat(popSize, is(2.0));

    // If we use a custom RelMetadataQuery and override the row count, the population size
    // should be half the reported row count. In this case we will have the RelMetadataQuery say
    // the row count is 12 for testing purposes, so we should expect a population size of 6.
    RelMetadataQuery customQuery = new RelMetadataQuery() {
      @Override public Double getRowCount(RelNode rel) {
        return 12.0;
      }
    };

    popSize = populationSize.getPopulationSize((Values) rel, customQuery, bitSetOf(0, 1, 2));
    assertThat(popSize, is(6.0));
  }

  private static final SqlOperator NONDETERMINISTIC_OP =
      SqlBasicFunction.create("NDC", ReturnTypes.BOOLEAN, OperandTypes.VARIADIC)
          .withDeterministic(false);

  /** Tests calling {@link RelMetadataQuery#getTableOrigin} for
   * an aggregate with no columns. Previously threw. */
  @Test void testEmptyAggregateTableOrigin() {
    final RelBuilder builder =
        RelBuilderTest.createBuilder(b -> b.withPreventEmptyFieldList(false));
    RelMetadataQuery mq = builder.getCluster().getMetadataQuery();
    RelNode agg = builder
        .scan("EMP")
        .aggregate(builder.groupKey())
        .build();
    final RelOptTable tableOrigin = mq.getTableOrigin(agg);
    assertThat(tableOrigin, nullValue());
  }

  @Test void testGetPredicatesForJoin() {
    final RelBuilder builder = RelBuilderTest.createBuilder();
    RelNode join = builder
        .scan("EMP")
        .scan("DEPT")
        .join(JoinRelType.INNER, builder.call(NONDETERMINISTIC_OP))
        .build();
    RelMetadataQuery mq = join.getCluster().getMetadataQuery();
    assertTrue(mq.getPulledUpPredicates(join).pulledUpPredicates.isEmpty());

    RelNode join1 = builder
        .scan("EMP")
        .scan("DEPT")
        .join(JoinRelType.INNER,
            builder.call(SqlStdOperatorTable.EQUALS,
                builder.field(2, 0, 0),
                builder.field(2, 1, 0)))
        .build();
    assertThat(mq.getPulledUpPredicates(join1)
            .pulledUpPredicates
            .get(0),
        hasToString("=($0, $8)"));
  }

  @Test void testGetPredicatesForFilter() {
    final RelBuilder builder = RelBuilderTest.createBuilder();
    RelNode filter = builder
        .scan("EMP")
        .filter(builder.call(NONDETERMINISTIC_OP))
        .build();
    RelMetadataQuery mq = filter.getCluster().getMetadataQuery();
    assertTrue(mq.getPulledUpPredicates(filter).pulledUpPredicates.isEmpty());

    RelNode filter1 = builder
        .scan("EMP")
        .filter(
            builder.call(SqlStdOperatorTable.EQUALS,
                builder.field(1, 0, 0),
                builder.field(1, 0, 1)))
        .build();
    assertThat(mq.getPulledUpPredicates(filter1)
            .pulledUpPredicates
            .get(0),
        hasToString("=($0, $1)"));
  }

  @Test void testGetPredicatesForLiteralAgg() {
    final RelBuilder b = RelBuilderTest.createBuilder();
    RelNode r = b
        .scan("EMP")
        .aggregate(b.groupKey("DEPTNO"),
            b.literalAgg(42),
            b.literalAgg(null))
        .build();
    RelMetadataQuery mq = r.getCluster().getMetadataQuery();
    final RelOptPredicateList predicateList = mq.getPulledUpPredicates(r);
    assertThat(predicateList.pulledUpPredicates,
        hasToString("[=($1, 42), IS NULL($2)]"));
    assertThat(toSortedStringList(predicateList.constantMap),
        hasToString("[$1=42, $2=null:NULL]"));
  }

  /** Converts a Map to a sorted list of its entries. */
  static <K, V> List<String> toSortedStringList(Map<K, V> map) {
    return map.entrySet().stream().map(Object::toString)
        .sorted().collect(toImmutableList());
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4315">[CALCITE-4315]
   * NPE in RelMdUtil#checkInputForCollationAndLimit</a>. */
  @Test void testCheckInputForCollationAndLimit() {
    final Project rel = (Project) sql("select * from emp, dept").toRel();
    final Join join = (Join) rel.getInput();
    final RelOptTable empTable = join.getInput(0).getTable();
    final RelOptTable deptTable = join.getInput(1).getTable();
    Frameworks.withPlanner((cluster, relOptSchema, rootSchema) -> {
      checkInputForCollationAndLimit(cluster, empTable, deptTable);
      return null;
    });
  }

  /** Unit test for
   * {@link org.apache.calcite.rel.metadata.RelMetadataQuery#getAverageColumnSizes(org.apache.calcite.rel.RelNode)}
   * with a table that has its own implementation of {@link BuiltInMetadata.Size}. */
  @Test void testCustomizedAverageColumnSizes() {
    SqlTestFactory.CatalogReaderFactory factory = (typeFactory, caseSensitive) -> {
      CompositeKeysCatalogReader catalogReader =
          new CompositeKeysCatalogReader(typeFactory, false);
      catalogReader.init();
      return catalogReader;
    };

    final RelNode rel = sql("select key1, key2 from s.composite_keys_table")
        .withCatalogReaderFactory(factory).toRel();
    final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
    List<Double> columnSizes = mq.getAverageColumnSizes(rel);
    assertThat(columnSizes, hasSize(2));
    assertThat(columnSizes.get(0), is(2.0));
    assertThat(columnSizes.get(1), is(3.0));
  }

  /** Unit test for
   * {@link org.apache.calcite.rel.metadata.RelMetadataQuery#getDistinctRowCount(RelNode, ImmutableBitSet, RexNode)}
   * with a table that has its own implementation of {@link BuiltInMetadata.Size}. */
  @Test void testCustomizedDistinctRowcount() {
    SqlTestFactory.CatalogReaderFactory factory = (typeFactory, caseSensitive) -> {
      CompositeKeysCatalogReader catalogReader =
          new CompositeKeysCatalogReader(typeFactory, false);
      catalogReader.init();
      return catalogReader;
    };

    final RelNode rel = sql("select key1, key2 from s.composite_keys_table")
        .withCatalogReaderFactory(factory).toRel();
    final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
    Double ndv = mq.getDistinctRowCount(rel, ImmutableBitSet.of(0, 1), null);
    assertThat(ndv, is(100.0));
  }

  private void checkInputForCollationAndLimit(RelOptCluster cluster, RelOptTable empTable,
      RelOptTable deptTable) {
    final RexBuilder rexBuilder = cluster.getRexBuilder();
    final RelMetadataQuery mq = cluster.getMetadataQuery();
    final List<RelHint> hints = ImmutableList.of();
    final LogicalTableScan empScan =
        LogicalTableScan.create(cluster, empTable, hints);
    final LogicalTableScan deptScan =
        LogicalTableScan.create(cluster, deptTable, hints);
    final LogicalJoin join =
        LogicalJoin.create(empScan, deptScan, ImmutableList.of(),
            rexBuilder.makeLiteral(true), ImmutableSet.of(), JoinRelType.INNER);
    assertTrue(
        RelMdUtil.checkInputForCollationAndLimit(mq, join,
            join.getTraitSet().getCollation(), null, null), () ->
            "we are checking a join against its own collation, fetch=null, "
                + "offset=null => checkInputForCollationAndLimit must be "
                + "true. join=" + join);
  }

  //~ Inner classes and interfaces -------------------------------------------

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

    @Deprecated
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
        ReflectiveRelMetadataProvider.reflectiveSource(new ColTypeImpl(),
            ColType.Handler.class);

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
        ReflectiveRelMetadataProvider.reflectiveSource(
            new BrokenColTypeImpl(), ColType.Handler.class);
  }

  /** Extension to {@link RelMetadataQuery} to support {@link ColType}.
   *
   * <p>Illustrates how you would package up a user-defined metadata type. */
  private static class MyRelMetadataQuery extends RelMetadataQuery {
    private ColType.Handler colTypeHandler;

    MyRelMetadataQuery(MetadataHandlerProvider provider) {
      super(provider);
      colTypeHandler = handler(ColType.Handler.class);
    }

    public String colType(RelNode rel, int column) {
      for (;;) {
        try {
          return colTypeHandler.getColType(rel, this, column);
        } catch (MetadataHandlerProvider.NoHandler e) {
          colTypeHandler = revise(ColType.Handler.class);
        }
      }
    }
  }

  /**
   * Dummy rel node used for testing.
   */
  private static class DummyRelNode extends SingleRel {
    /**
     * Creates a <code>DummyRelNode</code>.
     */
    DummyRelNode(RelOptCluster cluster, RelTraitSet traits, RelNode input) {
      super(cluster, traits, input);
    }
  }

  /** Mock catalog reader for registering a table with composite keys. */
  private static class CompositeKeysCatalogReader
      extends MockCatalogReaderSimple {
    CompositeKeysCatalogReader(RelDataTypeFactory typeFactory,
        boolean caseSensitive) {
      super(typeFactory, caseSensitive);
    }

    /** Creates and initializes a CompositeKeysCatalogReader. */
    public static @NonNull CompositeKeysCatalogReader create(
        RelDataTypeFactory typeFactory, boolean caseSensitive) {
      return new CompositeKeysCatalogReader(typeFactory, caseSensitive).init();
    }

    @Override public CompositeKeysCatalogReader init() {
      super.init();
      MockSchema tSchema = new MockSchema("s");
      registerSchema(tSchema);
      // Register "T1" table.
      final MockTable t1 =
          MockTable.create(this, tSchema, "composite_keys_table", false, 7.0, null);
      t1.addColumn("key1", typeFactory.createSqlType(SqlTypeName.VARCHAR));
      t1.addColumn("key2", typeFactory.createSqlType(SqlTypeName.VARCHAR));
      t1.addColumn("value1", typeFactory.createSqlType(SqlTypeName.INTEGER));
      t1.addKey("key1", "key2");
      addSizeHandler(t1);
      addDistinctRowcountHandler(t1);
      addUniqueKeyHandler(t1);
      registerTable(t1);
      return this;
    }

    private void addSizeHandler(MockTable table) {
      table.addWrap(
          new BuiltInMetadata.Size.Handler() {
            @Override public @Nullable Double averageRowSize(RelNode r, RelMetadataQuery mq) {
              return null;
            }

            @Override public @Nullable List<@Nullable Double> averageColumnSizes(RelNode r,
                RelMetadataQuery mq) {
              List<Double> colSize = new ArrayList<>();
              colSize.add(2D);
              colSize.add(3D);
              return colSize;
            }
          });
    }

    private void addDistinctRowcountHandler(MockTable table) {
      table.addWrap(
          new BuiltInMetadata.DistinctRowCount.Handler() {
            @Override public @Nullable Double getDistinctRowCount(RelNode r, RelMetadataQuery mq,
                ImmutableBitSet groupKey, @Nullable RexNode predicate) {
              return 100D;
            }
          });
    }

    private void addUniqueKeyHandler(MockTable table) {
      table.addWrap(
          new BuiltInMetadata.UniqueKeys.Handler() {
            @Override public @Nullable Set<ImmutableBitSet> getUniqueKeys(RelNode r,
                RelMetadataQuery mq, boolean ignoreNulls) {
              return ImmutableSet.of(ImmutableBitSet.of(0, 1));
            }
          });
    }
  }
}
