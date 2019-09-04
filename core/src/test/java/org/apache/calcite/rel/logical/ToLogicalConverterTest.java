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
import org.apache.calcite.adapter.enumerable.EnumerableInterpreterRule;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.rules.ProjectToWindowRule;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
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

import org.junit.Test;

import static org.apache.calcite.test.Matchers.hasTree;

import static org.junit.Assert.assertThat;

/**
 * Tests for {@link ToLogicalConverter}.
 */
public class ToLogicalConverterTest {
  private static final ImmutableSet<RelOptRule> RULE_SET =
      ImmutableSet.of(
          ProjectToWindowRule.PROJECT,
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
          EnumerableRules.ENUMERABLE_WINDOW_RULE,
          EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE,
          EnumerableInterpreterRule.INSTANCE);

  private static final SqlToRelConverter.Config DEFAULT_REL_CONFIG =
      SqlToRelConverter.configBuilder()
          .withTrimUnusedFields(false)
          .withConvertTableAccess(false)
          .build();

  private static FrameworkConfig frameworkConfig() {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    final SchemaPlus schema = CalciteAssert.addSchema(rootSchema,
        CalciteAssert.SchemaSpec.JDBC_FOODMART);
    return Frameworks.newConfigBuilder()
               .parserConfig(SqlParser.Config.DEFAULT)
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

  @Test public void testValues() {
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

  @Test public void testScan() {
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

  @Test public void testProject() {
    // Equivalent SQL:
    //   SELECT deptno
    //   FROM emp
    final RelBuilder builder = builder();
    final RelNode rel =
        builder.scan("EMP")
            .project(builder.field("DEPTNO"))
            .build();
    String expectedPhysial =
        ""
            + "EnumerableProject(DEPTNO=[$7])\n"
            + "  EnumerableTableScan(table=[[scott, EMP]])\n";
    String expectedLogical =
        ""
            + "LogicalProject(DEPTNO=[$7])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verify(rel, expectedPhysial, expectedLogical);
  }

  @Test public void testFilter() {
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
    String expectedPhysial =
        ""
            + "EnumerableFilter(condition=[=($7, 10)])\n"
            + "  EnumerableTableScan(table=[[scott, EMP]])\n";
    String expectedLogical =
        ""
            + "LogicalFilter(condition=[=($7, 10)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verify(rel, expectedPhysial, expectedLogical);
  }

  @Test public void testSort() {
    // Equivalent SQL:
    //   SELECT *
    //   FROM emp
    //   ORDER BY 3
    final RelBuilder builder = builder();
    final RelNode rel =
        builder.scan("EMP")
            .sort(builder.field(2))
            .build();
    String expectedPhysial =
        ""
            + "EnumerableSort(sort0=[$2], dir0=[ASC])\n"
            + "  EnumerableTableScan(table=[[scott, EMP]])\n";
    String expectedLogical =
        ""
            + "LogicalSort(sort0=[$2], dir0=[ASC])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verify(rel, expectedPhysial, expectedLogical);
  }

  @Test public void testLimit() {
    // Equivalent SQL:
    //   SELECT *
    //   FROM emp
    //   FETCH 10
    final RelBuilder builder = builder();
    final RelNode rel =
        builder.scan("EMP")
            .limit(0, 10)
            .build();
    String expectedPhysial =
        ""
            + "EnumerableLimit(fetch=[10])\n"
            + "  EnumerableTableScan(table=[[scott, EMP]])\n";
    String expectedLogical =
        ""
            + "LogicalSort(fetch=[10])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verify(rel, expectedPhysial, expectedLogical);
  }

  @Test public void testSortLimit() {
    // Equivalent SQL:
    //   SELECT *
    //   FROM emp
    //   ORDER BY deptno DESC FETCH 10
    final RelBuilder builder = builder();
    final RelNode rel =
        builder.scan("EMP")
            .sortLimit(-1, 10, builder.desc(builder.field("DEPTNO")))
            .build();
    String expectedPhysial =
        ""
            + "EnumerableLimit(fetch=[10])\n"
            + "  EnumerableSort(sort0=[$7], dir0=[DESC])\n"
            + "    EnumerableTableScan(table=[[scott, EMP]])\n";
    String expectedLogical =
        ""
            + "LogicalSort(sort0=[$7], dir0=[DESC], fetch=[10])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verify(rel, expectedPhysial, expectedLogical);
  }

  @Test public void testAggregate() {
    // Equivalent SQL:
    //   SELECT COUNT(empno) AS c
    //   FROM emp
    //   GROUP BY deptno
    final RelBuilder builder = builder();
    final RelNode rel =
        builder.scan("EMP")
            .aggregate(builder.groupKey(builder.field("DEPTNO")),
                builder.count(false, "C", builder.field("EMPNO")))
            .build();
    String expectedPhysial =
        ""
            + "EnumerableAggregate(group=[{7}], C=[COUNT($0)])\n"
            + "  EnumerableTableScan(table=[[scott, EMP]])\n";
    String expectedLogical =
        ""
            + "LogicalAggregate(group=[{7}], C=[COUNT($0)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verify(rel, expectedPhysial, expectedLogical);
  }

  @Test public void testJoin() {
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
    String expectedPhysial =
        ""
            + "EnumerableHashJoin(condition=[=($7, $8)], joinType=[inner])\n"
            + "  EnumerableTableScan(table=[[scott, EMP]])\n"
            + "  EnumerableTableScan(table=[[scott, DEPT]])\n";
    String expectedLogical =
        ""
            + "LogicalJoin(condition=[=($7, $8)], joinType=[inner])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n"
            + "  LogicalTableScan(table=[[scott, DEPT]])\n";
    verify(rel, expectedPhysial, expectedLogical);
  }

  @Test public void testCorrelation() {
    final RelBuilder builder = builder();
    final Holder<RexCorrelVariable> v = Holder.of(null);
    final RelNode rel = builder.scan("EMP")
                       .variable(v)
                       .scan("DEPT")
                       .join(JoinRelType.LEFT,
                           builder.equals(builder.field(2, 0, "SAL"),
                               builder.literal(1000)),
                           ImmutableSet.of(v.get().id))
                       .build();
    String expectedPhysial =
        ""
            + "EnumerableCorrelate(correlation=[$cor0], joinType=[left], requiredColumns=[{}])\n"
            + "  EnumerableTableScan(table=[[scott, EMP]])\n"
            + "  EnumerableFilter(condition=[=($cor0.SAL, 1000)])\n"
            + "    EnumerableTableScan(table=[[scott, DEPT]])\n";
    String expectedLogical =
        ""
            + "LogicalCorrelate(correlation=[$cor0], joinType=[left], requiredColumns=[{5}])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n"
            + "  LogicalFilter(condition=[=($cor0.SAL, 1000)])\n"
            + "    LogicalTableScan(table=[[scott, DEPT]])\n";
    verify(rel, expectedPhysial, expectedLogical);
  }

  @Test public void testUnion() {
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
    String expectedPhysial =
        ""
            + "EnumerableUnion(all=[true])\n"
            + "  EnumerableProject(DEPTNO=[$0])\n"
            + "    EnumerableTableScan(table=[[scott, DEPT]])\n"
            + "  EnumerableProject(DEPTNO=[$7])\n"
            + "    EnumerableTableScan(table=[[scott, EMP]])\n";
    String expectedLogical =
        ""
            + "LogicalUnion(all=[true])\n"
            + "  LogicalProject(DEPTNO=[$0])\n"
            + "    LogicalTableScan(table=[[scott, DEPT]])\n"
            + "  LogicalProject(DEPTNO=[$7])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verify(rel, expectedPhysial, expectedLogical);
  }

  @Test public void testUncollect() {
    final String sql =
        ""
            + "select did \n"
            + "from unnest(select collect(\"department_id\") as deptid"
            + "            from \"department\") as t(did)";
    String expectedPhysial =
        ""
            + "EnumerableUncollect\n"
            + "  EnumerableAggregate(group=[{}], DEPTID=[COLLECT($0)])\n"
            + "    JdbcToEnumerableConverter\n"
            + "      JdbcProject(department_id=[$0])\n"
            + "        JdbcTableScan(table=[[foodmart, department]])\n";
    String expectedLogical =
        ""
            + "Uncollect\n"
            + "  LogicalAggregate(group=[{}], DEPTID=[COLLECT($0)])\n"
            + "    LogicalProject(department_id=[$0])\n"
            + "      LogicalTableScan(table=[[foodmart, department]])\n";
    verify(rel(sql), expectedPhysial, expectedLogical);
  }

  @Test public void testWindow() {
    String sql = "SELECT rank() over (order by \"hire_date\") FROM \"employee\"";
    String expectedPhysial =
        ""
            + "EnumerableProject($0=[$17])\n"
            + "  EnumerableWindow(window#0=[window(partition {} order by [9] range between "
            + "UNBOUNDED PRECEDING and CURRENT ROW aggs [RANK()])])\n"
            + "    JdbcToEnumerableConverter\n"
            + "      JdbcTableScan(table=[[foodmart, employee]])\n";
    String expectedLogical =
        ""
            + "LogicalProject($0=[$17])\n"
            + "  LogicalWindow(window#0=[window(partition {} order by [9] range between UNBOUNDED"
            + " PRECEDING and CURRENT ROW aggs [RANK()])])\n"
            + "    LogicalTableScan(table=[[foodmart, employee]])\n";
    verify(rel(sql), expectedPhysial, expectedLogical);
  }
}

// End ToLogicalConverterTest.java
