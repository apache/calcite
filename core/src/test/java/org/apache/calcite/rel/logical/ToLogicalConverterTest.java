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
package org.apache.calcite.rel.logical;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.test.RelBuilderTest;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RuleSets;
import org.apache.calcite.util.Holder;
import org.apache.calcite.util.TestUtil;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.Test;

import java.util.Locale;

import static org.apache.calcite.test.Matchers.hasTree;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Tests for {@link ToLogicalConverter}.
 */
class ToLogicalConverterTest {
  private static final ImmutableSet<RelOptRule> RULE_SET =
      ImmutableSet.of(
          CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW,
          EnumerableRules.ENUMERABLE_VALUES_RULE,
          EnumerableRules.ENUMERABLE_JOIN_RULE,
          EnumerableRules.ENUMERABLE_CORRELATE_RULE,
          EnumerableRules.ENUMERABLE_PROJECT_RULE,
          EnumerableRules.ENUMERABLE_FILTER_RULE,
          EnumerableRules.ENUMERABLE_AGGREGATE_RULE,
          EnumerableRules.ENUMERABLE_SORT_RULE,
          EnumerableRules.ENUMERABLE_LIMIT_RULE,
          EnumerableRules.ENUMERABLE_COLLECT_RULE,
          EnumerableRules.ENUMERABLE_UNCOLLECT_RULE,
          EnumerableRules.ENUMERABLE_UNION_RULE,
          EnumerableRules.ENUMERABLE_INTERSECT_RULE,
          EnumerableRules.ENUMERABLE_MINUS_RULE,
          EnumerableRules.ENUMERABLE_WINDOW_RULE,
          EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE,
          EnumerableRules.TO_INTERPRETER);

  private static final SqlToRelConverter.Config DEFAULT_REL_CONFIG =
      SqlToRelConverter.config().withTrimUnusedFields(false);

  private static FrameworkConfig frameworkConfig() {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    final SchemaPlus schema =
        CalciteAssert.addSchema(rootSchema,
            CalciteAssert.SchemaSpec.JDBC_FOODMART);
    return Frameworks.newConfigBuilder()
        .defaultSchema(schema)
        .sqlToRelConverterConfig(DEFAULT_REL_CONFIG)
        .build();
  }

  private static RelBuilder builder() {
    return RelBuilder.create(RelBuilderTest.config().build());
  }

  private static RelNode rel(String sql) {
    final Planner planner = Frameworks.getPlanner(frameworkConfig());
    try {
      SqlNode parse = planner.parse(sql);
      SqlNode validate = planner.validate(parse);
      return planner.rel(validate).rel;
    } catch (Exception e) {
      throw TestUtil.rethrow(e);
    }
  }

  private static RelNode toPhysical(RelNode rel) {
    final RelOptPlanner planner = rel.getCluster().getPlanner();
    planner.clear();
    for (RelOptRule rule : RULE_SET) {
      planner.addRule(rule);
    }

    final Program program = Programs.of(RuleSets.ofList(planner.getRules()));
    return program.run(planner, rel, rel.getTraitSet().replace(EnumerableConvention.INSTANCE),
        ImmutableList.of(), ImmutableList.of());
  }

  private static RelNode toLogical(RelNode rel) {
    return rel.accept(new ToLogicalConverter(builder()));
  }

  private void verify(RelNode rel, String expectedPhysical, String expectedLogical) {
    RelNode physical = toPhysical(rel);
    RelNode logical = toLogical(physical);
    assertThat(physical, hasTree(expectedPhysical));
    assertThat(logical, hasTree(expectedLogical));
  }

  @Test void testValues() {
    // Equivalent SQL:
    //   VALUES (true, 1), (false, -50) AS t(a, b)
    final RelBuilder builder = builder();
    final RelNode rel =
        builder
            .values(new String[]{"a", "b"}, true, 1, false, -50)
            .build();
    verify(rel,
        "EnumerableValues(tuples=[[{ true, 1 }, { false, -50 }]])\n",
        "LogicalValues(tuples=[[{ true, 1 }, { false, -50 }]])\n");
  }

  @Test void testScan() {
    // Equivalent SQL:
    //   SELECT *
    //   FROM emp
    final RelNode rel =
        builder()
            .scan("EMP")
            .build();
    verify(rel,
        "EnumerableTableScan(table=[[scott, EMP]])\n",
        "LogicalTableScan(table=[[scott, EMP]])\n");
  }

  @Test void testProject() {
    // Equivalent SQL:
    //   SELECT deptno
    //   FROM emp
    final RelBuilder builder = builder();
    final RelNode rel =
        builder.scan("EMP")
            .project(builder.field("DEPTNO"))
            .build();
    String expectedPhysical = ""
        + "EnumerableProject(DEPTNO=[$7])\n"
        + "  EnumerableTableScan(table=[[scott, EMP]])\n";
    String expectedLogical = ""
        + "LogicalProject(DEPTNO=[$7])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verify(rel, expectedPhysical, expectedLogical);
  }

  @Test void testFilter() {
    // Equivalent SQL:
    //   SELECT *
    //   FROM emp
    //   WHERE deptno = 10
    final RelBuilder builder = builder();
    final RelNode rel =
        builder.scan("EMP")
            .filter(
                builder.equals(
                    builder.field("DEPTNO"),
                    builder.literal(10)))
            .build();
    String expectedPhysical = ""
        + "EnumerableFilter(condition=[=($7, 10)])\n"
        + "  EnumerableTableScan(table=[[scott, EMP]])\n";
    String expectedLogical = ""
        + "LogicalFilter(condition=[=($7, 10)])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verify(rel, expectedPhysical, expectedLogical);
  }

  @Test void testSort() {
    // Equivalent SQL:
    //   SELECT *
    //   FROM emp
    //   ORDER BY 3
    final RelBuilder builder = builder();
    final RelNode rel =
        builder.scan("EMP")
            .sort(builder.field(2))
            .build();
    String expectedPhysical = ""
        + "EnumerableSort(sort0=[$2], dir0=[ASC])\n"
        + "  EnumerableTableScan(table=[[scott, EMP]])\n";
    String expectedLogical = ""
        + "LogicalSort(sort0=[$2], dir0=[ASC])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verify(rel, expectedPhysical, expectedLogical);
  }

  @Test void testLimit() {
    // Equivalent SQL:
    //   SELECT *
    //   FROM emp
    //   FETCH 10
    final RelBuilder builder = builder();
    final RelNode rel =
        builder.scan("EMP")
            .limit(0, 10)
            .build();
    String expectedPhysical = ""
        + "EnumerableLimit(fetch=[10])\n"
        + "  EnumerableTableScan(table=[[scott, EMP]])\n";
    String expectedLogical = ""
        + "LogicalSort(fetch=[10])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verify(rel, expectedPhysical, expectedLogical);
  }

  @Test void testSortLimit() {
    // Equivalent SQL:
    //   SELECT *
    //   FROM emp
    //   ORDER BY deptno DESC FETCH 10
    final RelBuilder builder = builder();
    final RelNode rel =
        builder.scan("EMP")
            .sortLimit(-1, 10, builder.desc(builder.field("DEPTNO")))
            .build();
    String expectedPhysical = ""
        + "EnumerableLimit(fetch=[10])\n"
        + "  EnumerableSort(sort0=[$7], dir0=[DESC])\n"
        + "    EnumerableTableScan(table=[[scott, EMP]])\n";
    String expectedLogical = ""
        + "LogicalSort(sort0=[$7], dir0=[DESC], fetch=[10])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verify(rel, expectedPhysical, expectedLogical);
  }

  @Test void testAggregate() {
    // Equivalent SQL:
    //   SELECT deptno, COUNT(sal) AS c
    //   FROM emp
    //   GROUP BY deptno
    final RelBuilder builder = builder();
    final RelNode rel =
        builder.scan("EMP")
            .aggregate(builder.groupKey(builder.field("DEPTNO")),
                builder.count(false, "C", builder.field("SAL")))
            .build();
    String expectedPhysical = ""
        + "EnumerableAggregate(group=[{7}], C=[COUNT($5)])\n"
        + "  EnumerableTableScan(table=[[scott, EMP]])\n";
    String expectedLogical = ""
        + "LogicalAggregate(group=[{7}], C=[COUNT($5)])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verify(rel, expectedPhysical, expectedLogical);
  }

  @Test void testJoin() {
    // Equivalent SQL:
    //   SELECT *
    //   FROM emp
    //   JOIN dept ON emp.deptno = dept.deptno
    final RelBuilder builder = builder();
    final RelNode rel =
        builder.scan("EMP")
            .scan("DEPT")
            .join(JoinRelType.INNER,
                builder.call(SqlStdOperatorTable.EQUALS,
                    builder.field(2, 0, "DEPTNO"),
                    builder.field(2, 1, "DEPTNO")))
            .build();
    String expectedPhysical = ""
        + "EnumerableHashJoin(condition=[=($7, $8)], joinType=[inner])\n"
        + "  EnumerableTableScan(table=[[scott, EMP]])\n"
        + "  EnumerableTableScan(table=[[scott, DEPT]])\n";
    String expectedLogical = ""
        + "LogicalJoin(condition=[=($7, $8)], joinType=[inner])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n"
        + "  LogicalTableScan(table=[[scott, DEPT]])\n";
    verify(rel, expectedPhysical, expectedLogical);
  }

  @Test void testDeepEquals() {
    // Equivalent SQL:
    //   SELECT *
    //   FROM emp
    //   JOIN dept ON emp.deptno = dept.deptno
    final RelBuilder builder = builder();
    RelNode[] rels = new RelNode[2];
    for (int i = 0; i < 2; i++) {
      rels[i] = builder.scan("EMP")
          .scan("DEPT")
          .join(JoinRelType.INNER,
              builder.call(SqlStdOperatorTable.EQUALS,
                  builder.field(2, 0, "DEPTNO"),
                  builder.field(2, 1, "DEPTNO")))
          .build();
    }

    // Currently, default implementation uses identity equals
    assertThat(rels[0].equals(rels[1]), is(false));
    assertThat(rels[0].getInput(0).equals(rels[1].getInput(0)), is(false));

    // Deep equals and hashCode check
    assertThat(rels[0].deepEquals(rels[1]), is(true));
    assertThat(rels[0].deepHashCode() == rels[1].deepHashCode(), is(true));
  }

  /** Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-7159">[CALCITE-7159]
   * LogicalAsofJoin deepEquals can throw for legal expressions</a>. */
  @Test void testAsofDeepEquals() {
    final RelBuilder builder = builder();
    RelNode[] rels = new RelNode[2];
    rels[0] = builder.scan("EMP")
        .scan("DEPT")
        .asofJoin(JoinRelType.ASOF,
            builder.equals(
                builder.field(2, 0, "DEPTNO"),
                builder.field(2, 1, "DEPTNO")),
            builder.lessThan(
                builder.field(2, 1, "DEPTNO"),
                builder.field(2, 0, "DEPTNO")))
        .build();
    rels[1] = builder.scan("EMP").build();
    assertThat(rels[0].deepEquals(rels[1]), is(false));
  }

  @Test void testCorrelation() {
    final RelBuilder builder = builder();
    final Holder<@Nullable RexCorrelVariable> v = Holder.empty();
    final RelNode rel = builder.scan("EMP")
        .variable(v::set)
        .scan("DEPT")
        .filter(
            builder.equals(builder.field(0), builder.field(v.get(), "DEPTNO")))
        .join(JoinRelType.LEFT,
            builder.equals(builder.field(2, 0, "SAL"),
                builder.literal(1000)),
            ImmutableSet.of(v.get().id))
        .build();
    String expectedPhysical = ""
        + "EnumerableCorrelate(correlation=[$cor0], joinType=[left], requiredColumns=[{5, 7}])\n"
        + "  EnumerableTableScan(table=[[scott, EMP]])\n"
        + "  EnumerableFilter(condition=[=($cor0.SAL, 1000)])\n"
        + "    EnumerableFilter(condition=[=($0, $cor0.DEPTNO)])\n"
        + "      EnumerableTableScan(table=[[scott, DEPT]])\n";
    String expectedLogical = ""
        + "LogicalCorrelate(correlation=[$cor0], joinType=[left], requiredColumns=[{5, 7}])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n"
        + "  LogicalFilter(condition=[=($cor0.SAL, 1000)])\n"
        + "    LogicalFilter(condition=[=($0, $cor0.DEPTNO)])\n"
        + "      LogicalTableScan(table=[[scott, DEPT]])\n";
    verify(rel, expectedPhysical, expectedLogical);
  }

  @Test void testUnion() {
    // Equivalent SQL:
    //   SELECT deptno FROM emp
    //   UNION ALL
    //   SELECT deptno FROM dept
    final RelBuilder builder = builder();
    RelNode rel =
        builder.scan("DEPT")
            .project(builder.field("DEPTNO"))
            .scan("EMP")
            .project(builder.field("DEPTNO"))
            .union(true)
            .build();
    String expectedPhysical = ""
        + "EnumerableUnion(all=[true])\n"
        + "  EnumerableProject(DEPTNO=[$0])\n"
        + "    EnumerableTableScan(table=[[scott, DEPT]])\n"
        + "  EnumerableProject(DEPTNO=[$7])\n"
        + "    EnumerableTableScan(table=[[scott, EMP]])\n";
    String expectedLogical = ""
        + "LogicalUnion(all=[true])\n"
        + "  LogicalProject(DEPTNO=[$0])\n"
        + "    LogicalTableScan(table=[[scott, DEPT]])\n"
        + "  LogicalProject(DEPTNO=[$7])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verify(rel, expectedPhysical, expectedLogical);
  }

  @Test void testIntersect() {
    // Equivalent SQL:
    //   SELECT deptno FROM emp
    //   INTERSECT ALL
    //   SELECT deptno FROM dept
    final RelBuilder builder = builder();
    RelNode rel =
        builder.scan("DEPT")
            .project(builder.field("DEPTNO"))
            .scan("EMP")
            .project(builder.field("DEPTNO"))
            .intersect(true)
            .build();
    String expectedPhysical = ""
        + "EnumerableIntersect(all=[true])\n"
        + "  EnumerableProject(DEPTNO=[$0])\n"
        + "    EnumerableTableScan(table=[[scott, DEPT]])\n"
        + "  EnumerableProject(DEPTNO=[$7])\n"
        + "    EnumerableTableScan(table=[[scott, EMP]])\n";
    String expectedLogical = ""
        + "LogicalIntersect(all=[true])\n"
        + "  LogicalProject(DEPTNO=[$0])\n"
        + "    LogicalTableScan(table=[[scott, DEPT]])\n"
        + "  LogicalProject(DEPTNO=[$7])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verify(rel, expectedPhysical, expectedLogical);
  }

  @Test void testMinus() {
    // Equivalent SQL:
    //   SELECT deptno FROM emp
    //   EXCEPT ALL
    //   SELECT deptno FROM dept
    final RelBuilder builder = builder();
    RelNode rel =
        builder.scan("DEPT")
            .project(builder.field("DEPTNO"))
            .scan("EMP")
            .project(builder.field("DEPTNO"))
            .minus(true)
            .build();
    String expectedPhysical = ""
        + "EnumerableMinus(all=[true])\n"
        + "  EnumerableProject(DEPTNO=[$0])\n"
        + "    EnumerableTableScan(table=[[scott, DEPT]])\n"
        + "  EnumerableProject(DEPTNO=[$7])\n"
        + "    EnumerableTableScan(table=[[scott, EMP]])\n";
    String expectedLogical = ""
        + "LogicalMinus(all=[true])\n"
        + "  LogicalProject(DEPTNO=[$0])\n"
        + "    LogicalTableScan(table=[[scott, DEPT]])\n"
        + "  LogicalProject(DEPTNO=[$7])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verify(rel, expectedPhysical, expectedLogical);
  }

  @Test void testUncollect() {
    final String sql = ""
        + "select did\n"
        + "from unnest(select collect(\"department_id\") as deptid"
        + "            from \"department\") as t(did)";
    String expectedPhysical = ""
        + "EnumerableUncollect\n"
        + "  EnumerableAggregate(group=[{}], DEPTID=[COLLECT($0)])\n"
        + "    JdbcToEnumerableConverter\n"
        + "      JdbcProject(department_id=[$0])\n"
        + "        JdbcTableScan(table=[[foodmart, department]])\n";
    String expectedLogical = ""
        + "Uncollect\n"
        + "  LogicalAggregate(group=[{}], DEPTID=[COLLECT($0)])\n"
        + "    LogicalProject(department_id=[$0])\n"
        + "      LogicalTableScan(table=[[foodmart, department]])\n";
    verify(rel(sql), expectedPhysical, expectedLogical);
  }

  @Test void testWindow() {
    String sql = "SELECT rank() over (order by \"hire_date\") FROM \"employee\"";
    String expectedPhysical = ""
        + "EnumerableProject($0=[$17])\n"
        + "  EnumerableWindow(window#0=[window(order by [9] aggs [RANK()])])\n"
        + "    JdbcToEnumerableConverter\n"
        + "      JdbcTableScan(table=[[foodmart, employee]])\n";
    String expectedLogical = ""
        + "LogicalProject($0=[$17])\n"
        + "  LogicalWindow(window#0=[window(order by [9] aggs [RANK()])])\n"
        + "    LogicalTableScan(table=[[foodmart, employee]])\n";
    verify(rel(sql), expectedPhysical, expectedLogical);
  }

  void testWindowExcludeImp(String excludeClause, String expectedExcludeString) {
    // forbiddenApiTest requires to use Locale even though it's no needed here.
    String sql = String.format(Locale.ROOT, "SELECT sum(\"salary\") over (order by \"hire_date\" "
        + "rows between unbounded preceding and current row %s) FROM \"employee\"", excludeClause);
    String expectedPhysical =
        String.format(Locale.ROOT, "EnumerableProject($0=[$17])\n"
            + "  EnumerableWindow(window#0=[window(order by [9] rows between"
            + " UNBOUNDED PRECEDING and CURRENT ROW %saggs [SUM($11)])])\n"
            + "    JdbcToEnumerableConverter\n"
            + "      JdbcTableScan(table=[[foodmart, employee]])\n", expectedExcludeString);
    String expectedLogical =
        String.format(Locale.ROOT, "LogicalProject($0=[$17])\n"
            + "  LogicalWindow(window#0=[window(order by [9] rows between"
            + " UNBOUNDED PRECEDING and CURRENT ROW %saggs [SUM($11)])])\n"
            + "    LogicalTableScan(table=[[foodmart, employee]])\n", expectedExcludeString);
    verify(rel(sql), expectedPhysical, expectedLogical);
  }

  @Test void testWindowExclude() {
    testWindowExcludeImp("exclude current row", "EXCLUDE CURRENT ROW ");
    testWindowExcludeImp("exclude group", "EXCLUDE GROUP ");
    testWindowExcludeImp("exclude ties", "EXCLUDE TIES ");
    testWindowExcludeImp("exclude no others", "");
  }

  @Test void testTableModify() {
    final String sql = "insert into \"employee\" select * from \"employee\"";
    final String expectedPhysical = ""
        + "JdbcToEnumerableConverter\n"
        + "  JdbcTableModify(table=[[foodmart, employee]], operation=[INSERT], flattened=[true])\n"
        + "    JdbcTableScan(table=[[foodmart, employee]])\n";
    final String expectedLogical = ""
        + "LogicalTableModify(table=[[foodmart, employee]], "
        + "operation=[INSERT], flattened=[true])\n"
        + "  LogicalTableScan(table=[[foodmart, employee]])\n";
    verify(rel(sql), expectedPhysical, expectedLogical);
  }

}
